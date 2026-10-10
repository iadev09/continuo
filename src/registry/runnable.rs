use std::sync::Arc;

use async_trait::async_trait;

use super::Result;

/// Capability trait for components that produce a long-running runtime task.
///
/// `run()` is the only place in the lifecycle for long-running work: accept
/// loops, listeners, periodic tickers. It does not appear in `register()` or
/// `Bootable::boot()`.
///
/// `D` is the component's `Deps`. The runtime resolves it before each admitted
/// generation, after checking scope and activation policy. Dormant services
/// do not require runtime dependencies until started. Each execution receives
/// shared handles; fleet dispatch clones those handles across its slots.
///
/// The supplied [`crate::RunContext`] belongs to one live service generation.
/// Observe its cancellation rather than the process shutdown token alone: the
/// runtime uses the same path for a targeted stop or restart and for
/// whole-process shutdown.
#[async_trait]
pub trait Runnable<S, D = ()>: Send + Sync + 'static {
    /// Declare the service's current activation policy.
    ///
    /// The default is [`crate::ServiceStartPolicy::Automatic`]: exposing
    /// `Runnable` is already the component's opt-in to runtime ownership. The
    /// runtime re-evaluates this synchronous method when observing the
    /// service and before starting a generation.
    fn start_policy(
        &self,
        _state: &S,
    ) -> crate::ServiceStartPolicy {
        crate::ServiceStartPolicy::Automatic
    }

    /// Run one runtime-owned generation of this component.
    ///
    /// An implementation that returns `Err` logs its own contextual details;
    /// the runtime attaches the component name and the lifecycle phase, and
    /// does not log non-critical runnable errors a second time.
    async fn run(
        self: Arc<Self>,
        deps: D,
        state: S,
        context: crate::RunContext,
    ) -> Result<()>;
}
