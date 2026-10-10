//! Type-erased views of components and runnables.
//!
//! `Component<S>` carries an associated `Deps` type, so the registry cannot
//! hold `dyn Component<S>` values of different components in one map. It
//! holds `dyn AnyComponent<S>` instead: the same capabilities, with dependency
//! resolution folded in, implemented once for every `Component<S>`.

use std::any::{Any, TypeId};
use std::sync::Arc;

use async_trait::async_trait;

use super::{
    Component, ComponentOrder, Finalizable, Registry, Reloadable, Resolve,
    Result, Runnable,
};

/// A registered component with its dependency type erased.
///
/// Every `Component<S>` implements this trait. The registry stores and hands
/// out `Arc<dyn AnyComponent<S>>`; application code implements `Component`.
#[async_trait]
pub trait AnyComponent<S>: Any + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn boot_priority(&self) -> Option<u8>;
    fn run_priority(&self) -> Option<u8>;
    fn order(&self) -> ComponentOrder;

    /// The `TypeId`s named by the component's `Deps`.
    fn dependencies(&self) -> Vec<TypeId>;

    /// Whether the component exposes a `Bootable`.
    fn has_bootable(&self) -> bool;

    /// Resolve the component's dependencies and run its `Bootable`, if any.
    async fn boot(
        &self,
        registry: &Registry<S>,
        state: &S,
    ) -> Result<()>;

    fn validate(
        &self,
        state: &S,
    ) -> Result<()>;

    fn as_reloadable(&self) -> Option<&dyn Reloadable<S>>;
    fn as_finalizable(&self) -> Option<&dyn Finalizable<S>>;

    /// Expose the runnable without resolving dependencies. The runtime checks
    /// scope and activation policy before binding an admitted generation.
    fn runnable(self: Arc<Self>) -> Option<Arc<dyn AnyRunnable<S>>>;

    #[cfg(feature = "config")]
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration>;
    #[cfg(feature = "config")]
    fn as_environmental(&self) -> Option<&dyn congif::Environmental>;
    #[cfg(feature = "fleet")]
    fn as_scoped(&self) -> Option<&dyn crate::fleet::Scoped>;
    #[cfg(feature = "fleet")]
    fn as_dispatched(&self) -> Option<&dyn crate::fleet::Dispatched>;
}

#[async_trait]
impl<S: Send + Sync + 'static, C: Component<S>> AnyComponent<S> for C {
    fn name(&self) -> &'static str {
        Component::name(self)
    }

    fn boot_priority(&self) -> Option<u8> {
        Component::boot_priority(self)
    }

    fn run_priority(&self) -> Option<u8> {
        Component::run_priority(self)
    }

    fn order(&self) -> ComponentOrder {
        Component::order(self)
    }

    fn dependencies(&self) -> Vec<TypeId> {
        let mut types = Vec::new();
        <C::Deps as Resolve<S>>::dependencies(&mut types);
        types
    }

    fn has_bootable(&self) -> bool {
        Component::as_bootable(self).is_some()
    }

    async fn boot(
        &self,
        registry: &Registry<S>,
        state: &S,
    ) -> Result<()> {
        let Some(bootable) = Component::as_bootable(self) else {
            return Ok(());
        };
        let deps = <C::Deps as Resolve<S>>::resolve(registry)
            .map_err(|error| error.into_dependency(Component::name(self)))?;
        bootable.boot(deps, state).await
    }

    fn validate(
        &self,
        state: &S,
    ) -> Result<()> {
        Component::validate(self, state)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<S>> {
        Component::as_reloadable(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<S>> {
        Component::as_finalizable(self)
    }

    fn runnable(self: Arc<Self>) -> Option<Arc<dyn AnyRunnable<S>>> {
        let name = Component::name(self.as_ref());
        let runnable = Component::as_runnable(self)?;
        Some(Arc::new(Unbound { name, runnable }))
    }

    #[cfg(feature = "config")]
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Component::as_configurable(self)
    }

    #[cfg(feature = "config")]
    fn as_environmental(&self) -> Option<&dyn congif::Environmental> {
        Component::as_environmental(self)
    }

    #[cfg(feature = "fleet")]
    fn as_scoped(&self) -> Option<&dyn crate::fleet::Scoped> {
        Component::as_scoped(self)
    }

    #[cfg(feature = "fleet")]
    fn as_dispatched(&self) -> Option<&dyn crate::fleet::Dispatched> {
        Component::as_dispatched(self)
    }
}

/// A type-erased runnable, with dependency admission separated from execution.
///
/// The runtime retains an unbound view for policy and management. Before each
/// admitted generation it calls `bind`; the resulting execution owns typed
/// dependencies. Fleet dispatch clones that bound execution, not the registry.
#[async_trait]
pub trait AnyRunnable<S>: Send + Sync + 'static {
    /// Resolve required dependencies before a generation is marked running.
    fn bind(
        self: Arc<Self>,
        registry: &Registry<S>,
    ) -> Result<Arc<dyn AnyRunnable<S>>>;

    fn start_policy(
        &self,
        state: &S,
    ) -> crate::ServiceStartPolicy;

    async fn run(
        self: Arc<Self>,
        state: S,
        context: crate::RunContext,
    ) -> Result<()>;
}

struct Unbound<S, D> {
    name: &'static str,
    runnable: Arc<dyn Runnable<S, D>>,
}

#[async_trait]
impl<S: Send + Sync + 'static, D: Resolve<S>> AnyRunnable<S> for Unbound<S, D> {
    fn bind(
        self: Arc<Self>,
        registry: &Registry<S>,
    ) -> Result<Arc<dyn AnyRunnable<S>>> {
        let deps = D::resolve(registry)
            .map_err(|error| error.into_dependency(self.name))?;
        Ok(Arc::new(Bound { runnable: self.runnable.clone(), deps }))
    }

    fn start_policy(
        &self,
        state: &S,
    ) -> crate::ServiceStartPolicy {
        self.runnable.start_policy(state)
    }

    async fn run(
        self: Arc<Self>,
        _: S,
        _: crate::RunContext,
    ) -> Result<()> {
        Err(super::Error::msg(format!(
            "runnable '{}' was not bound before execution",
            self.name
        )))
    }
}

struct Bound<S, D> {
    runnable: Arc<dyn Runnable<S, D>>,
    deps: D,
}

#[async_trait]
impl<S: Send + Sync + 'static, D: Resolve<S>> AnyRunnable<S> for Bound<S, D> {
    fn bind(
        self: Arc<Self>,
        _: &Registry<S>,
    ) -> Result<Arc<dyn AnyRunnable<S>>> {
        Ok(self)
    }

    fn start_policy(
        &self,
        state: &S,
    ) -> crate::ServiceStartPolicy {
        self.runnable.start_policy(state)
    }

    async fn run(
        self: Arc<Self>,
        state: S,
        context: crate::RunContext,
    ) -> Result<()> {
        self.runnable.clone().run(self.deps.clone(), state, context).await
    }
}
