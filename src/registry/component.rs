use std::any::Any;
use std::sync::Arc;

use super::{
    Bootable, ComponentOrder, Finalizable, Reloadable, Resolve, Result,
    Runnable,
};

/// A lifecycle participant.
///
/// Plain values use `Registry::insert` and implement nothing. `Register`
/// composes the application; `Component` describes the participants in its
/// ordered lifecycle. A component exposes only the capabilities it has,
/// through `as_*` hooks, either on itself or on a separate object.
///
/// # Dependencies
///
/// `Deps` names what the component requires in order to boot and run: a
/// tuple of `Arc<T>` values, `Option<Arc<T>>` for a requirement that may be
/// absent, or `()`. The registry resolves it before `Bootable::boot` and
/// before each run generation and passes the result in; a missing dependency
/// is one error naming this component and the missing type. Registered
/// components named in `Deps` boot before this one.
///
/// A component that only uses another when it happens to be present, and
/// serves without it, does not list it here. It resolves the value at the
/// point of use and states a preferred start order through `run_priority`.
///
/// # Ordering
///
/// `order` places the component relative to layers or preparation components:
/// `after::<Network>()` where `Network` is a marker type
/// nobody registers. Such a type acts as a target, complete once everything
/// ordered `before` it has completed. Priorities order components that are
/// otherwise independent and never override a dependency or an `order` edge.
/// When boot publishes a plain DI value, `Deps` cannot infer its producer;
/// retain an explicit order after the component that prepares that value.
///
/// # Lifecycle boundaries
///
/// `Register` composes the application synchronously. `Bootable::boot`
/// performs finite preparation in dependency order; no listener or long-lived
/// task starts there. `validate` checks readiness before runnables are
/// admitted. `Runnable::run` owns operational work and its graceful drain.
/// `Finalizable::finalize` releases prepared, non-running resources in reverse
/// order after runnables have drained; it is not a stop hook.
pub trait Component<S>: Any + Send + Sync + 'static {
    /// What this component requires. See the trait documentation.
    type Deps: Resolve<S>;

    /// Human-readable label for logs and diagnostics, and the management
    /// name of its runnable.
    fn name(&self) -> &'static str;

    /// Boot order among components that are otherwise independent. Lower
    /// runs earlier; `None` means `priority::NORMAL`.
    fn boot_priority(&self) -> Option<u8> {
        None
    }

    /// Run start order among components that are otherwise independent.
    /// Lower starts earlier; `None` means `priority::NORMAL`.
    fn run_priority(&self) -> Option<u8> {
        None
    }

    /// Placement relative to layers or boot-time producers of plain DI values.
    /// Dependencies on other components belong in `Deps`.
    fn order(&self) -> ComponentOrder {
        ComponentOrder::default()
    }

    /// Optional ordered preparation. Operational work belongs to `Runnable`.
    fn as_bootable(&self) -> Option<&dyn Bootable<S, Self::Deps>> {
        None
    }

    /// Synchronous readiness validation after boot and before runnables are
    /// spawned: cheap checks over boot-published state, missing files or
    /// conflicting settings.
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
    /// The default is treated as `Scope::Worker`. The runtime consults this
    /// before creating local runnable entries; registration is unaffected.
    #[cfg(feature = "fleet")]
    fn as_scoped(&self) -> Option<&dyn crate::fleet::Scoped> {
        None
    }

    /// Optional dispatch metadata for a fleet execution adapter. Absent means
    /// `Dispatch::Each`. It applies to runnable execution, not to
    /// construction, boot or configuration.
    #[cfg(feature = "fleet")]
    fn as_dispatched(&self) -> Option<&dyn crate::fleet::Dispatched> {
        None
    }

    /// Optional typed configuration owned by this component.
    #[cfg(feature = "config")]
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        None
    }

    /// Optional declaration of the environments this component's programs
    /// start with. Asked after `configure` and after a reload; a re-read file
    /// is delivered through `Environmental::environment_changed`.
    #[cfg(feature = "config")]
    fn as_environmental(&self) -> Option<&dyn congif::Environmental> {
        None
    }

    /// Optional capability hook.
    fn as_finalizable(&self) -> Option<&dyn Finalizable<S>> {
        None
    }

    /// Optional capability hook. The runnable receives this component's
    /// resolved `Deps` on every generation.
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<S, Self::Deps>>> {
        None
    }
}
