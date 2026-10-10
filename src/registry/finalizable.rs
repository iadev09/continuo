use async_trait::async_trait;

use super::{Error, Result};

/// Independent capability for releasing non-running
/// resources at the end of the process lifecycle.
///
/// This does not require `Runnable` or `Component`. A lifecycle component
/// exposes the resource through `Component::as_finalizable`; the resource
/// itself does not need to be stored in DI or started as a task.
///
/// `finalize()` runs after process shutdown has started and after the
/// runnable tasks have drained — runnables end their own futures (and
/// any protocol-level graceful drain) inside `Runnable::run()`;
/// `finalize()` is NOT the place to stop them.
///
/// This is not a replacement for `Drop`: it is the lifecycle point for
/// externally named resources whose stale presence can break the next
/// boot, such as shm segments or lock files. Implementations should be
/// idempotent because the finalize path may be re-entered.
#[async_trait]
pub trait Finalizable<S>: Send + Sync {
    /// Releases non-running resources after runnables drain.
    async fn finalize(
        &self,
        state: &S,
    ) -> Result<()>;
}

/// One component failure retained by best-effort finalization.
#[derive(Debug)]
pub struct FinalizeFailure {
    pub(super) component: &'static str,
    pub(super) error: Error,
}

impl FinalizeFailure {
    pub fn component(&self) -> &'static str {
        self.component
    }

    pub fn error(&self) -> &Error {
        &self.error
    }

    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of a completed best-effort finalization pass.
#[derive(Debug, Default)]
#[must_use = "component finalization failures are reported through FinalizeOutcome"]
pub struct FinalizeOutcome {
    pub(super) finalized_count: usize,
    pub(super) failures: Vec<FinalizeFailure>,
}

impl FinalizeOutcome {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn finalized_count(&self) -> usize {
        self.finalized_count
    }

    pub fn failed_count(&self) -> usize {
        self.failures.len()
    }

    pub fn failures(&self) -> &[FinalizeFailure] {
        &self.failures
    }

    pub fn into_failures(self) -> Vec<FinalizeFailure> {
        self.failures
    }
}
