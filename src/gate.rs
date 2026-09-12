//! Graceful shutdown gate primitive.
//!
//! The design is inspired by axum's server handle pattern
//! (<https://github.com/tokio-rs/axum>): a sticky shutdown signal, tracked
//! in-flight work, and a graceful drain phase before forced shutdown. This
//! module is not tied to HTTP; it lifts that operational shape into a small
//! runtime primitive that any service loop can use.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Sentinel value for AtomicU64 representing None (infinite grace period)
const NANOS_NONE: u64 = u64::MAX;

#[derive(Clone, Debug)]
pub struct Gate {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    graceful: NotifyOnce,
    shutdown: NotifyOnce,
    released: Notify,
    count: AtomicUsize,
    all_done: NotifyOnce,
    /// Grace period for shutdown: NANOS_NONE = None (infinite), otherwise nanoseconds
    grace_period_nanos: AtomicU64,
    /// Timeout for acquiring connection slot when at max_connections (during normal operation)
    acquire_timeout: Duration,
    max_count: Option<usize>,
}

#[derive(Debug)]
pub enum Error {
    ShuttingDown, // GracefulShutdown(Duration),
    AcquireTimeout(Duration),
    AtCapacity,
}

/// Result of waiting for accepted work to leave a [`Gate`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "forced gate shutdown must be handled by the caller"]
pub enum GateDrainOutcome {
    /// Every permit was released without forcing shutdown.
    Drained,
    /// The grace period elapsed and the hard-shutdown signal was sent.
    Forced { remaining: usize },
}

impl Gate {
    /// Create a new gate with optional max connections and acquire timeout.
    ///
    /// - `max_count`: Maximum concurrent connections (None = unlimited)
    /// - `acquire_timeout`: How long to wait for a slot when at capacity
    pub fn new(
        max_count: Option<usize>,
        acquire_timeout: Duration,
    ) -> Self {
        let inner = Inner {
            graceful: NotifyOnce::default(),
            shutdown: NotifyOnce::default(),
            released: Notify::new(),
            count: AtomicUsize::new(0),
            all_done: NotifyOnce::default(),
            grace_period_nanos: AtomicU64::new(NANOS_NONE),
            acquire_timeout,
            max_count,
        };
        Self { inner: Arc::new(inner) }
    }

    /// Returns the current grace period duration (if any).
    /// Lock-free read using atomic operation.
    pub fn grace_period(&self) -> Option<Duration> {
        match self.inner.grace_period_nanos.load(Ordering::Acquire) {
            NANOS_NONE => None,
            nanos => Some(Duration::from_nanos(nanos)),
        }
    }

    /// Get the number of connections.
    pub fn count(&self) -> usize {
        self.inner.count.load(Ordering::Acquire)
    }

    /// Trigger a **forced** (hard) shutdown. Wakes all waiters of `wait_shutdown()`.
    /// This is only called by `wait_all_done()` when the grace period elapses.
    /// External code should not call this directly.
    pub fn force_shutdown(&self) {
        self.inner.shutdown.notify_waiters();
    }

    /// Begin a graceful shutdown phase. Notifies `graceful` and records the optional grace period.
    /// This does **not** imply a forced/Hard shutdown signal.
    ///
    /// `None` means indefinite grace period (critical tasks mode).
    /// Lock-free write using atomic operation.
    pub fn graceful_shutdown(
        &self,
        duration: Option<Duration>,
    ) {
        let nanos = duration.map_or(NANOS_NONE, |d| d.as_nanos() as u64);
        self.inner.grace_period_nanos.store(nanos, Ordering::Release);

        self.inner.graceful.notify_waiters();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.graceful.is_notified()
    }

    /// Wait for the **forced** (hard) shutdown signal. This does *not* fire for a clean drain.
    /// Private to this module; external code should use `Permit::wait_forced_shutdown`.
    pub async fn wait_forced_shutdown(&self) {
        self.inner.shutdown.notified().await;
    }

    pub async fn wait_graceful_shutdown(&self) {
        self.inner.graceful.notified().await;
    }

    /// Enter the gate, waiting up to the configured acquire timeout if the
    /// gate is currently at capacity.
    ///
    /// Returns a [`Permit`] token. Dropping the permit leaves the gate.
    pub async fn enter(&self) -> Result<Permit, Error> {
        let wait_timeout = self.inner.acquire_timeout;
        let start = tokio::time::Instant::now();

        loop {
            // Claiming the slot is `try_enter`'s job rather than a second
            // implementation here. This loop used to read the count and *then*
            // take a permit, which is two operations: every task that read
            // `max - 1` took one, so the gate admitted more than the limit it
            // exists to enforce — under exactly the load that makes it matter.
            // `try_enter` increments first and gives the slot back if it was
            // not there, which is the only shape that is safe without a lock.
            match self.try_enter() {
                Ok(permit) => return Ok(permit),
                Err(Error::AtCapacity) => {
                    if let Some(max_count) = self.inner.max_count {
                        debug!(
                            "Connection limit reached: {}/{} connections in use",
                            self.count(),
                            max_count
                        );
                    }
                }
                Err(error) => return Err(error),
            }

            // Calculate remaining timeout
            let elapsed = start.elapsed();
            if elapsed >= wait_timeout {
                return Err(Error::AcquireTimeout(wait_timeout));
            }

            let remaining = wait_timeout - elapsed;

            // Wait until a connection is freed, but let shutdown interrupt
            // overload backpressure immediately.
            tokio::select! {
                biased;

                _ = self.inner.graceful.notified() => {
                    return Err(Error::ShuttingDown);
                }
                _ = self.inner.released.notified() => {
                    // A connection was released, loop again to try acquiring.
                    continue;
                }
                _ = sleep(remaining) => {
                    return Err(Error::AcquireTimeout(wait_timeout));
                }
            }
        }
    }

    /// Try to enter the gate immediately without waiting for capacity.
    ///
    /// This is where the limit is enforced, for both doors: [`enter`] waits and
    /// retries around this rather than testing the count itself.
    ///
    /// [`enter`]: Self::enter
    pub fn try_enter(&self) -> Result<Permit, Error> {
        if self.inner.graceful.is_notified() {
            return Err(Error::ShuttingDown);
        }

        // Take the slot first and give it back if it was not there. Reading
        // the count and then incrementing is two operations with a window
        // between them, and every task in that window sees room that is
        // already spoken for.
        //
        // The count can therefore sit above `max` for as long as a rejected
        // task takes to back out. That is a property of `count()` as an
        // observation, not of admission: `prev >= max` is what decides, and
        // only one task per slot can read a `prev` below it.
        if let Some(max) = self.inner.max_count {
            let prev = self.inner.count.fetch_add(1, Ordering::AcqRel);
            if prev >= max {
                self.inner.count.fetch_sub(1, Ordering::AcqRel);
                return Err(Error::AtCapacity);
            }
        } else {
            self.inner.count.fetch_add(1, Ordering::AcqRel);
        }

        // The slot was already counted above; construct the permit directly.
        Ok(Permit { gate: self.clone() })
    }

    /// Wait until all permits are dropped, respecting the configured grace period.
    /// If the grace period elapses, this triggers `force_shutdown()` and reports
    /// the number of permits that remained when the hard signal was sent.
    /// Note: when returning via the forced path, `count()` may still be > 0 for a short time
    /// until connection tasks observe the hard signal and drop.
    ///
    /// # Call [`graceful_shutdown`] first
    ///
    /// Before that signal this call does not return, even once the last permit
    /// is gone: `all_done` is a one-shot, and a permit released during ordinary
    /// operation would latch it permanently, so [`Permit::drop`] only fires it
    /// when graceful shutdown has already been asked for. Waiting without
    /// asking is therefore waiting for a signal nobody will send — which is
    /// also the right behaviour, since the grace-then-force path is only
    /// meaningful once the work has been told to wind down.
    ///
    /// [`graceful_shutdown`]: Self::graceful_shutdown
    pub async fn wait_all_done(&self) -> GateDrainOutcome {
        if self.inner.count.load(Ordering::Acquire) == 0 {
            return GateDrainOutcome::Drained;
        }

        // Lock-free read of grace period
        let deadline = self.grace_period();

        match deadline {
            Some(duration) => tokio::select! {
                biased;
                _ = sleep(duration) => {
                    let remaining = self.count();
                    self.force_shutdown();
                    GateDrainOutcome::Forced { remaining }
                },
                _ = self.inner.all_done.notified() => {
                    debug!("🍺 All connections finished before graceful timeout");
                    GateDrainOutcome::Drained
                },
            },
            None => {
                self.inner.all_done.notified().await;
                GateDrainOutcome::Drained
            }
        }
    }
}

pub struct Permit {
    gate: Gate,
}

impl Permit {
    pub async fn wait_graceful_shutdown(&self) {
        self.gate.wait_graceful_shutdown().await
    }

    pub async fn wait_forced_shutdown(&self) {
        self.gate.wait_forced_shutdown().await
    }

    pub fn is_shutting_down(&self) -> bool {
        self.gate.is_shutting_down()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let count = self.gate.inner.count.fetch_sub(1, Ordering::AcqRel) - 1;

        if count == 0 && self.gate.inner.graceful.is_notified() {
            self.gate.inner.all_done.notify_waiters();
        }

        // permit isn't dropped yet.
        if let Some(max_count) = self.gate.inner.max_count
            && count < max_count
        {
            // Notify waiters that a slot is available
            self.gate.inner.released.notify_waiters();
        }
    }
}

/// Create a gate that automatically initiates graceful shutdown when the token is cancelled.
///
/// # Parameters
/// - `token`: Cancellation token that triggers shutdown
/// - `graceful_timeout`: Grace period for shutdown (None = infinite, for critical tasks)
/// - `max_count`: Maximum concurrent connections (None = unlimited)
/// - `acquire_timeout`: Timeout for acquiring a connection slot
pub fn create_gate(
    token: CancellationToken,
    graceful_timeout: Option<Duration>,
    max_count: Option<usize>,
    acquire_timeout: Duration,
) -> Gate {
    let gate = Gate::new(max_count, acquire_timeout);
    let shutdown_gate = gate.clone();
    tokio::spawn(async move {
        token.cancelled().await;
        shutdown_gate.graceful_shutdown(graceful_timeout);
    });
    gate
}

#[inline]
pub fn default_acquire_timeout() -> Duration {
    Duration::from_millis(100)
}

#[derive(Debug)]
struct NotifyOnce {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl Default for NotifyOnce {
    fn default() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }
}

impl NotifyOnce {
    fn notify_waiters(&self) {
        self.tx.send_replace(true);
    }

    fn is_notified(&self) -> bool {
        *self.rx.borrow()
    }

    async fn notified(&self) {
        let mut rx = self.rx.clone();

        loop {
            if *rx.borrow_and_update() {
                return;
            }

            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Error, Gate, GateDrainOutcome};

    #[test]
    fn try_enter_counts_one_slot_per_permit() {
        let gate = Gate::new(Some(2), Duration::from_millis(10));

        let first = gate.try_enter().unwrap();
        assert_eq!(gate.count(), 1);

        let second = gate.try_enter().unwrap();
        assert_eq!(gate.count(), 2);

        assert!(matches!(gate.try_enter(), Err(Error::AtCapacity)));

        drop(first);
        assert_eq!(gate.count(), 1);

        drop(second);
        assert_eq!(gate.count(), 0);
    }

    #[tokio::test]
    async fn empty_gate_reports_clean_drain() {
        let gate = Gate::new(None, Duration::from_millis(10));

        assert_eq!(gate.wait_all_done().await, GateDrainOutcome::Drained);
    }

    #[tokio::test]
    async fn elapsed_grace_period_reports_forced_drain() {
        let gate = Gate::new(None, Duration::from_millis(10));
        let _permit = gate.try_enter().expect("permit should be admitted");
        gate.graceful_shutdown(Some(Duration::from_millis(1)));

        assert_eq!(gate.wait_all_done().await, GateDrainOutcome::Forced { remaining: 1 });
    }

    /// `enter` under contention, which is the only way its limit can be wrong.
    ///
    /// Single-threaded tests cannot see this: the fault was reading the count
    /// and then taking a permit, so every task that observed `max - 1` in the
    /// window between the two got one. Against the load-then-increment version
    /// this reaches 5 live permits against a max of 4 within a few rounds.
    ///
    /// It counts live permits rather than `count()`, because `count()` is
    /// briefly above the limit by design — a rejected task increments before it
    /// learns there was no room.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn enter_never_admits_more_than_max() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        for _ in 0..200 {
            let gate = Gate::new(Some(4), Duration::from_secs(5));
            let live = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));

            let entrants: Vec<_> = (0..32)
                .map(|_| {
                    let gate = gate.clone();
                    let live = Arc::clone(&live);
                    let peak = Arc::clone(&peak);
                    tokio::spawn(async move {
                        let Ok(permit) = gate.enter().await else {
                            return;
                        };
                        let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        live.fetch_sub(1, Ordering::SeqCst);
                        drop(permit);
                    })
                })
                .collect();

            for entrant in entrants {
                entrant.await.unwrap();
            }

            let observed = peak.load(Ordering::SeqCst);
            assert!(observed <= 4, "gate held {observed} permits at once against max 4");
        }
    }
}
