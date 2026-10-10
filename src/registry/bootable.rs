use async_trait::async_trait;

use super::Result;

/// Ordered, finite preparation before runtime work is admitted.
///
/// This is not a running service. Resolve already-prepared dependencies,
/// consume validated configuration, construct complete objects and publish
/// their handles here. A registration component may prepare several related
/// objects; those objects do not need a boot hook themselves.
///
/// Do not serve requests, start a listener, spawn detached tasks or run an
/// operational loop here. Such work belongs to a runtime-owned `Runnable`.
/// Expose this capability through `Component::as_bootable`; implementing this
/// trait does not insert an object into DI or register it for execution.
#[async_trait]
pub trait Bootable<S>: Send + Sync {
    async fn boot(
        &self,
        state: &S,
    ) -> Result<()>;
}
