use std::any::Any;
use std::sync::Arc;

use super::{
    Bootable, ComponentOrder, Finalizable, Reloadable, Result, Runnable,
};

/// An explicitly managed lifecycle participant, not a DI requirement.
///
/// Plain values use `Registry::insert` and need not implement this trait.
/// `Register` composes the application; `Component` describes participants in
/// its ordered lifecycle. A preparation component can construct and register
/// several fully initialized services from its `Bootable` hook. It need not
/// itself be exposed through DI.
///
/// # Lifecycle boundaries
///
/// `Register` composes the application synchronously. It may insert ordinary
/// DI values and register lifecycle participants; it does not require an empty
/// instance of every eventual service.
///
/// `Bootable::boot` performs finite, dependency-ordered preparation. A wrapper
/// can consume validated configuration, construct a fully initialized service
/// and publish its shared handle. No listener or long-lived task starts here.
/// `validate` then checks readiness before runnable work is admitted.
///
/// `Runnable::run` owns operational work and its graceful drain. The runtime
/// retains and controls that future independently of DI. A component that
/// exposes a runnable need not be resolvable, and the runnable may be a separate
/// object rather than the component itself.
///
/// `Finalizable::finalize` releases prepared, non-running resources in reverse
/// order after runnable tasks have drained. It does not require a runnable
/// and must not be used as a task's stop hook.
///
/// All capabilities are optional and independent. A component may prepare or
/// manage several related objects; it exposes only the capabilities it needs
/// through `as_*` hooks. Plain injected values implement none of these traits.
pub trait Component<S>: Any + Send + Sync + 'static {
    /// Human-readable label for logs/diagnostics.
    fn name(&self) -> &'static str;

    /// Optional boot priority. Lower values run earlier among otherwise-ready
    /// components. `None` means `priority::NORMAL`. Prefer `Component::order()`
    /// for dependency relationships; priority is only a coarse tie-breaker.
    fn boot_priority(&self) -> Option<u8> {
        None
    }

    /// Optional runtime task start priority. Lower values run earlier.
    /// `None` means `priority::NORMAL`.
    fn run_priority(&self) -> Option<u8> {
        None
    }

    /// Optional type-based boot/reload ordering hints.
    ///
    /// The registry builds one ordered lifecycle plan and uses it for boot,
    /// validate, finalize, and reload. Reload skips providers that are not
    /// `Reloadable`, but dependency relationships remain the same: reload is
    /// a boot emulation on a live process.
    fn order(&self) -> ComponentOrder {
        ComponentOrder::default()
    }

    /// Optional ordered preparation. Operational work belongs to `Runnable`.
    fn as_bootable(&self) -> Option<&dyn Bootable<S>> {
        None
    }

    /// Synchronous readiness validation after `Bootable::boot()` and before runnable
    /// tasks are spawned.
    ///
    /// Use this for cheap checks over boot-published state, missing files, or
    /// conflicting settings that should fail before long-running work starts.
    fn validate(
        &self,
        _state: &S,
    ) -> Result<()> {
        Ok(())
    }

    /// Downcast hook for typed resolve APIs.
    fn as_any(&self) -> &dyn Any
    where
        Self: Sized,
    {
        self
    }

    /// Optional capability hook.
    fn as_reloadable(&self) -> Option<&dyn Reloadable<S>> {
        None
    }

    /// Optional placement metadata for an application-owned fleet adapter.
    /// The default is treated as `Scope::Worker`. Runtime consults this before
    /// creating local runnable entries; provider/configuration registration
    /// remains intact. Scope is sampled at initial runnable submission.
    #[cfg(feature = "fleet")]
    fn as_scoped(&self) -> Option<&dyn crate::fleet::Scoped> {
        None
    }

    /// Optional dispatch metadata for a fleet execution adapter.
    /// An absent capability means `Dispatch::Each`. The adapter selects among
    /// scope-eligible processes; this hook neither arbitrates ownership nor
    /// changes local service activation. It applies to runnable execution,
    /// not provider construction, boot or configuration.
    #[cfg(feature = "fleet")]
    fn as_dispatched(&self) -> Option<&dyn crate::fleet::Dispatched> {
        None
    }

    /// Optional config-source discovery for application-owned notification.
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        None
    }

    /// Optional declaration of the environments this provider's programs are
    /// started with. Asked after `configure` and after a reload; the reader
    /// keeps one node per program and the runtime delivers a re-read file
    /// through `Environmental::environment_changed`.
    fn as_environmental(&self) -> Option<&dyn congif::Environmental> {
        None
    }

    /// Optional capability hook.
    fn as_finalizable(&self) -> Option<&dyn Finalizable<S>> {
        None
    }

    /// Optional capability hook.
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<S>>> {
        None
    }
}
