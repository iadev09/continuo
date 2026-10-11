//! Process-local typed event bus.
//!
//! A pub/sub primitive where **anyone can define a new event type and
//! emit / subscribe to it** without modifying this crate. Each event
//! type gets its own `tokio::sync::broadcast` channel created on
//! demand the first time someone emits or subscribes.
//!
//! # Why typed registration instead of a fixed enum
//!
//! A closed event enum forces every new event into
//! one central definition — every crate that wants a new variant has
//! to come back and edit the runtime crate. With an [`Event`] trait,
//! any consumer can
//! introduce its own event type locally and emit it through the bus.
//! Event producers and consumers remain decoupled by their shared event type.
//!
//! # Scope
//!
//! Process-local. Each process (master, worker, standalone) has its
//! own bus. Cross-process and fleet delivery belong to an adapter over a
//! shared transport, outside this primitive.
//!
//! # Atomicity
//!
//! `ProcessEventBus` is the **atomic primitive** for process-scoped pubsub —
//! analogous to `AtomicU64` in the storage tier ladder. Higher layers can
//! retain this typed shape while providing broader delivery.
//!
//! # Usage
//!
//! ```ignore
//! // Define an event type anywhere
//! #[derive(Clone, Debug)]
//! pub struct CertReloaded { pub server_id: i32 }
//! impl continuo::events::Event for CertReloaded {}
//!
//! // Emit
//! state.events().emit(CertReloaded { server_id: 5 });
//!
//! // Subscribe (typed, only this event)
//! let mut rx = state.events().subscribe::<CertReloaded>();
//! while let Ok(evt) = rx.recv().await {
//!     // ...
//! }
//! ```

use std::any::{Any, TypeId};
use std::sync::{Arc, Mutex, OnceLock};

use dashmap::DashMap;
use tokio::sync::broadcast;

/// Default per-type channel capacity. Subscribers that fall more than
/// `DEFAULT_CAPACITY` events behind on a single type receive
/// `RecvError::Lagged` on the next `recv()`.
const DEFAULT_CAPACITY: usize = 64;

/// Marker trait for any value that can flow through the bus.
///
/// `Clone` is required because `broadcast` channels deliver the same
/// value to every subscriber. `Send + Sync + 'static` enables storage
/// in the type-erased registry.
pub trait Event: Clone + Send + Sync + 'static {}

/// Typed, on-demand event channel registry.
///
/// Cheap to clone — internally backed by `Arc<DashMap>` plus per-type
/// `broadcast::Sender`s. Embed in `AppState` and expose via
/// `state.events()`.
#[derive(Clone)]
pub struct ProcessEventBus {
    channels: Arc<DashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    capacity: usize,
}

type EventFilter<E> = Box<dyn Fn(&E) -> bool + Send + Sync>;

struct FilteredSender<E: Event> {
    accepts: EventFilter<E>,
    sender: broadcast::Sender<E>,
}

struct Channel<E: Event> {
    sender: broadcast::Sender<E>,
    // Ordinary typed channels never allocate or lock a filtered subscriber list.
    filtered: OnceLock<Mutex<Vec<FilteredSender<E>>>>,
}

impl<E: Event> Channel<E> {
    fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender, filtered: OnceLock::new() }
    }

    fn emit(
        &self,
        event: E,
    ) {
        if let Some(filtered) = self.filtered.get() {
            let mut subscribers =
                filtered.lock().expect("event filters lock poisoned");
            subscribers.retain(|subscriber| {
                if subscriber.sender.receiver_count() == 0 {
                    return false;
                }
                if (subscriber.accepts)(&event) {
                    let _ = subscriber.sender.send(event.clone());
                }
                true
            });
        }
        let _ = self.sender.send(event);
    }
}

impl ProcessEventBus {
    /// Create a bus with the default per-type capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a bus with explicit per-type channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { channels: Arc::new(DashMap::new()), capacity }
    }

    /// Publish an event of type `E`.
    ///
    /// If no channel for `E` exists yet (no subscriber has registered),
    /// the call is a no-op — fire-and-forget semantics. Once a
    /// subscriber appears, future emits of `E` are delivered.
    pub fn emit<E: Event>(
        &self,
        event: E,
    ) {
        let key = TypeId::of::<E>();
        tracing::debug!(
            event_type = std::any::type_name::<E>(),
            "process event emitted"
        );

        if let Some(entry) = self.channels.get(&key)
            && let Some(channel) = entry.downcast_ref::<Arc<Channel<E>>>()
        {
            channel.emit(event);
        }
        // No subscribers (or no channel registered) → drop silently.
    }

    /// Subscribe to events of type `E`.
    ///
    /// The channel for `E` is created on demand on first call. Each
    /// receiver sees only events emitted AFTER it subscribed —
    /// no replay of historical events.
    pub fn subscribe<E: Event>(&self) -> broadcast::Receiver<E> {
        self.channel::<E>().sender.subscribe()
    }

    /// Subscribe to matching values of `E`, filtering before queue admission.
    ///
    /// The receiver has the same capacity and lag/closure semantics as an
    /// ordinary subscription, but rejected events consume no slots. Publication
    /// applies the predicate synchronously; no task, forwarding queue or polling
    /// loop is started. Predicates must be cheap, non-blocking and must not
    /// publish or subscribe through this bus. Mutable selection may be captured
    /// through shared state; selection is evaluated when an event is emitted.
    ///
    /// Only events emitted after registration are eligible. Dropping the receiver
    /// stops delivery; the next publication or filtered registration releases
    /// its predicate. Dropping the last bus closes all receivers.
    pub fn subscribe_filtered<E: Event>(
        &self,
        accepts: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> broadcast::Receiver<E> {
        self.subscribe_filtered_with_capacity(self.capacity, accepts)
    }

    /// A filtered subscription with a consumer-owned backlog bound.
    ///
    /// This changes only this receiver, not ordinary typed channels or other
    /// subscribers. As with Tokio broadcast, capacity must be positive and at
    /// most `usize::MAX / 2`; storage rounds up to the next power of two.
    /// No earlier publications are replayed.
    pub fn subscribe_filtered_with_capacity<E: Event>(
        &self,
        capacity: usize,
        accepts: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> broadcast::Receiver<E> {
        let (sender, receiver) = broadcast::channel(capacity);
        let channel = self.channel::<E>();
        let mut subscribers = channel
            .filtered
            .get_or_init(Mutex::default)
            .lock()
            .expect("event filters lock poisoned");
        subscribers
            .retain(|subscriber| subscriber.sender.receiver_count() != 0);
        subscribers.push(FilteredSender { accepts: Box::new(accepts), sender });
        receiver
    }

    fn channel<E: Event>(&self) -> Arc<Channel<E>> {
        let entry =
            self.channels.entry(TypeId::of::<E>()).or_insert_with(|| {
                Box::new(Arc::new(Channel::<E>::new(self.capacity)))
                    as Box<dyn Any + Send + Sync>
            });
        Arc::clone(
            entry
                .downcast_ref::<Arc<Channel<E>>>()
                .expect("channel for TypeId is always Channel<E>"),
        )
    }

    /// Number of registered event types (one channel per type).
    pub fn registered_type_count(&self) -> usize {
        self.channels.len()
    }
}

impl Default for ProcessEventBus {
    fn default() -> Self {
        Self::new()
    }
}

// =====================================================================
// Built-in events
//
// Shared lifecycle markers; publishing and handling belong to applications.
// Consumers define additional event types in their own domain modules.
// =====================================================================

/// Process-wide marker for runtimes that publish shutdown initiation alongside
/// cancellation of their shutdown token.
#[derive(Clone, Debug)]
pub struct ShutdownInitiated;
impl Event for ShutdownInitiated {}

/// Application-owned reload request. This crate neither emits nor handles it.
/// The application defines the reload scope and execution policy.
#[derive(Clone, Debug)]
pub struct ReloadRequired;
impl Event for ReloadRequired {}
