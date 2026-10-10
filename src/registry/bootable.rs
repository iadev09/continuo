use async_trait::async_trait;

use super::Result;

/// Ordered, finite preparation before runtime work is admitted.
///
/// `D` is the component's `Deps`, resolved by the registry and passed in; the
/// hook does not look its dependencies up. It consumes validated
/// configuration, constructs complete objects and publishes their handles. A
/// preparation component may prepare several related objects; those objects
/// need no boot hook of their own.
///
/// No listener, detached task or operational loop starts here; that work
/// belongs to a runtime-owned `Runnable`. Expose this capability through
/// `Component::as_bootable`; implementing the trait alone neither inserts an
/// object into DI nor registers it for execution.
#[async_trait]
pub trait Bootable<S, D = ()>: Send + Sync {
    async fn boot(
        &self,
        deps: D,
        state: &S,
    ) -> Result<()>;
}
