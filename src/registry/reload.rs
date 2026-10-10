use async_trait::async_trait;

use super::{Error, Result};

#[async_trait]
pub trait ReloadState: Send + Sync + Sized + 'static {
    /// Optional configuration catalog for explicit broadcast reloads.
    /// The registry prepares all owners before calling application/component
    /// hooks, and acknowledges each candidate only after successful application.
    /// States without configuration retain the ordinary lifecycle-only path.
    #[cfg(feature = "config")]
    fn reload_config_reader(&self) -> Option<&congif::ConfigReader> {
        None
    }

    async fn reload(&self) -> Result<()>;
}

/// Anything that can hot-reload itself when config changes.
///
/// Configurable components consume the candidate already prepared by Congif;
/// they do not read sources again. The hook applies component-owned resources
/// and runtime values. A successful return acknowledges that application,
/// while a failure leaves the candidate unacknowledged for a later retry.
/// Components outside Congif retain ownership of their own reload inputs.
/// Reload must NOT change which components are registered; it only
/// refreshes state of an already-registered component.
#[async_trait]
pub trait Reloadable<S>: Send + Sync {
    /// Optional reload priority.
    ///
    /// Lower values run earlier among otherwise-ready components. `None`
    /// means `priority::NORMAL`. Prefer `Component::order()` for real
    /// dependency relationships.
    fn priority(&self) -> Option<u8> {
        None
    }

    /// Perform a synchronous reload using the current shared state.
    /// Implementations may spawn async work internally if needed.
    ///
    /// Do not call back into [`ServiceManager`] from here. A reload driven by
    /// the manager runs inside the runtime loop's own await, so a command sent
    /// from this method waits for a reply the loop cannot produce until this
    /// method returns. See [`ServiceManager`] for the whole of that rule.
    ///
    /// [`ServiceManager`]: crate::ServiceManager
    async fn reload(
        &self,
        state: &S,
    ) -> Result<()>;
}

/// One component failure retained by a broadcast reload.
#[derive(Debug)]
pub struct ReloadFailure {
    pub(super) component: &'static str,
    pub(super) error: Error,
}

impl ReloadFailure {
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

/// Result of a completed best-effort reload broadcast.
///
/// The outer [`Result`] returned by [`super::Registry::reload_all`] remains reserved
/// for failures that prevent the broadcast itself, such as state reload or
/// lifecycle-plan errors. Individual component failures are retained here so
/// callers choose their own logging, metrics, or transport representation.
#[derive(Debug, Default)]
#[must_use = "component reload failures are reported through ReloadOutcome"]
pub struct ReloadOutcome {
    pub(super) reloaded_count: usize,
    pub(super) failures: Vec<ReloadFailure>,
}

impl ReloadOutcome {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn reloaded_count(&self) -> usize {
        self.reloaded_count
    }

    pub fn failed_count(&self) -> usize {
        self.failures.len()
    }

    pub fn failures(&self) -> &[ReloadFailure] {
        &self.failures
    }

    pub fn into_failures(self) -> Vec<ReloadFailure> {
        self.failures
    }
}
