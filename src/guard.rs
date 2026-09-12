use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Small RAII in-flight counter.
///
/// [`GuardGroup::guard`] increments the counter and returns a [`Guard`].
/// Dropping the guard decrements the counter. [`GuardGroup::wait_empty`]
/// resolves when the counter reaches zero.
#[derive(Clone, Debug)]
pub struct GuardGroup(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    count: AtomicUsize,
    notify: Notify,
}

impl GuardGroup {
    pub fn new() -> Self {
        Self(Arc::new(Inner { count: AtomicUsize::new(0), notify: Notify::new() }))
    }

    pub fn guard(&self) -> Guard {
        self.0.count.fetch_add(1, Ordering::Relaxed);
        Guard(self.0.clone())
    }

    pub fn count(&self) -> usize {
        self.0.count.load(Ordering::Acquire)
    }

    /// Resolves once every outstanding [`Guard`] has been dropped.
    pub async fn wait_empty(&self) {
        loop {
            // Registered before the count is read, not after. The other order
            // loses the wakeup: between observing a non-zero count and
            // registering, the last guard can drop and its `notify_waiters`
            // reaches nobody, so this parks for good on a group that is already
            // empty. Creating the future is not enough — `Notified` registers
            // when it is first polled, so `enable` is what makes it happen
            // here, ahead of the read.
            let notified = self.0.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.count() == 0 {
                return;
            }

            notified.await;
        }
    }
}

impl Default for GuardGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct Guard(Arc<Inner>);

impl Drop for Guard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.notify.notify_waiters();
        }
    }
}
