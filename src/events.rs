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
use std::sync::Arc;

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
        tracing::debug!(event_type = std::any::type_name::<E>(), "process event emitted");

        if let Some(entry) = self.channels.get(&key)
            && let Some(sender) = entry.downcast_ref::<broadcast::Sender<E>>()
        {
            let _ = sender.send(event);
        }
        // No subscribers (or no channel registered) → drop silently.
    }

    /// Subscribe to events of type `E`.
    ///
    /// The channel for `E` is created on demand on first call. Each
    /// receiver sees only events emitted AFTER it subscribed —
    /// no replay of historical events.
    pub fn subscribe<E: Event>(&self) -> broadcast::Receiver<E> {
        let key = TypeId::of::<E>();

        if let Some(entry) = self.channels.get(&key)
            && let Some(sender) = entry.downcast_ref::<broadcast::Sender<E>>()
        {
            return sender.subscribe();
        }

        // Slow path: create the channel for this type. Race with
        // another subscriber is harmless — `entry().or_insert_with`
        // gives us a single winner; both end up with the same
        // `Sender` clone.
        let entry = self.channels.entry(key).or_insert_with(|| {
            let (sender, _) = broadcast::channel::<E>(self.capacity);
            Box::new(sender) as Box<dyn Any + Send + Sync>
        });
        entry
            .downcast_ref::<broadcast::Sender<E>>()
            .expect("channel for TypeId is always Sender<E>")
            .subscribe()
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
// Continuo only ships the shutdown marker used by runtimes that want a
// push-style notification alongside their cancellation token. Consumers define
// their own event types in their own domain modules.
// =====================================================================

/// Process-wide marker for runtimes that publish shutdown initiation alongside
/// cancellation of their shutdown token.
#[derive(Clone, Debug)]
pub struct ShutdownInitiated;
impl Event for ShutdownInitiated {}
