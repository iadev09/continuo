use std::sync::Arc;

use async_trait::async_trait;

use super::{Provider, Result};

/// Capability trait for providers that produce a long-running runtime task.
///
/// `run()` is the ONLY place in the lifecycle for long-running work
/// (accept loops, listeners, periodic tickers). It must NOT appear in
/// `register()` or `Provider::boot()`.
///
/// The supplied [`crate::RunContext`] belongs to one live service generation.
/// Observe its cancellation instead of only the process shutdown token: the
/// runtime uses the same path for targeted stop/restart and whole-process
/// shutdown.
///
#[async_trait]
pub trait Runnable<S>: Provider<S> {
    /// Declare the service's current activation policy.
    ///
    /// The default is [`crate::ServiceStartPolicy::Automatic`] because
    /// exposing `Runnable` is already the provider's explicit opt-in to
    /// runtime ownership. Providers may inspect their booted configuration
    /// and the process state here without making the registry configuration
    /// aware. The runtime re-evaluates this synchronous method when observing
    /// the service and before starting a generation.
    fn start_policy(
        &self,
        _state: &S,
    ) -> crate::ServiceStartPolicy {
        crate::ServiceStartPolicy::Automatic
    }

    /// Run one runtime-owned generation of this long-lived provider.
    ///
    /// NOTICE (convention):
    /// If this future returns `Err`, implementation should log contextual
    /// failure details itself (provider/task specific metadata).
    ///
    /// Reason:
    /// - Runtime layer handles lifecycle/control-flow only.
    /// - Runtime cannot reliably attach provider-specific business context.
    /// - Non-critical runnable errors are not centrally logged to avoid
    ///   duplicate/no-context error lines.
    async fn run(
        self: Arc<Self>,
        state: S,
        context: crate::RunContext,
    ) -> Result<()>;
}
