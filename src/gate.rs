//! Graceful shutdown gate primitive.
//!
//! The design is inspired by axum's server handle pattern
//! (<https://github.com/tokio-rs/axum>): a sticky shutdown signal, tracked
//! in-flight work, and a graceful drain phase before forced shutdown. This
//! module is not tied to HTTP; it lifts that operational shape into a small
//! runtime primitive that any service loop can use.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[derive(Clone, Debug)]
pub struct Gate {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    graceful: NotifyOnce,
    shutdown: NotifyOnce,
    released: Notify,
    admission: Mutex<Admission>,
    all_done: NotifyOnce,
    /// Timeout for acquiring connection slot when at max_connections (during normal operation)
    acquire_timeout: Duration,
    max_count: Option<usize>,
}

// Closing admission and counting accepted permits share one linearization point.
// Refused entrants never change the count observed by drain.
#[derive(Debug, Default)]
struct Admission {
    closed: bool,
    count: usize,
    grace_period: Option<Duration>,
}

/// Why a [`Gate`] refused a permit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Error {
    /// Graceful shutdown has been requested; the gate takes no new work.
    ShuttingDown,
    /// The gate was at capacity for the whole acquire timeout.
    AcquireTimeout(Duration),
    /// The gate was at capacity and the caller asked not to wait.
    AtCapacity,
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::ShuttingDown => f.write_str("gate is shutting down"),
            Self::AcquireTimeout(timeout) => {
                write!(f, "gate was at capacity for {timeout:?}")
            }
            Self::AtCapacity => f.write_str("gate is at capacity"),
        }
    }
}

impl std::error::Error for Error {}

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
            admission: Mutex::new(Admission::default()),
            all_done: NotifyOnce::default(),
            acquire_timeout,
            max_count,
        };
        Self { inner: Arc::new(inner) }
    }

    /// Returns the complete caller-declared grace period (`None` is indefinite).
    pub fn grace_period(&self) -> Option<Duration> {
        self.inner
            .admission
            .lock()
            .expect("gate admission lock poisoned")
            .grace_period
    }

    /// Get the number of connections.
    pub fn count(&self) -> usize {
        self.inner.admission.lock().expect("gate admission lock poisoned").count
    }

    /// Admission capacity; None means unlimited.
    pub fn max_count(&self) -> Option<usize> {
        self.inner.max_count
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
    pub fn graceful_shutdown(
        &self,
        duration: Option<Duration>,
    ) {
        let mut admission =
            self.inner.admission.lock().expect("gate admission lock poisoned");
        admission.grace_period = duration;
        admission.closed = true;
        let empty = admission.count == 0;
        drop(admission);
        self.inner.graceful.notify_waiters();
        if empty {
            self.inner.all_done.notify_waiters();
        }
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
            // Register before checking capacity: notify_waiters does not retain
            // releases for futures created after the release.
            let released = self.inner.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
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
                _ = &mut released => {
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
        let mut admission =
            self.inner.admission.lock().expect("gate admission lock poisoned");
        if admission.closed {
            return Err(Error::ShuttingDown);
        }
        if self.inner.max_count.is_some_and(|max| admission.count >= max)
            || admission.count == usize::MAX
        {
            return Err(Error::AtCapacity);
        }
        admission.count += 1;
        drop(admission);

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
        if self.count() == 0 {
            return GateDrainOutcome::Drained;
        }

        // Read the declared grace period
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
        let mut admission = self
            .gate
            .inner
            .admission
            .lock()
            .expect("gate admission lock poisoned");
        admission.count -= 1;
        let count = admission.count;
        let closed = admission.closed;
        drop(admission);
        if count == 0 && closed {
            self.gate.inner.all_done.notify_waiters();
        }

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
    Duration::from_millis(200)
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
#[path = "../tests/gate/unit.rs"]
mod tests;
