use std::any::Any;
use std::sync::Arc;

use super::{AnyRunnable, Resolve, Runnable};

/// One named operational instance produced by a lifecycle component.
///
/// Instances do not create additional DI registrations or configuration owners.
/// Their shared values are resolved before each admitted generation. A factory
/// should retain the same runnable allocation while its declaration is unchanged.
/// Execution policy comes from that runnable's `execution_spec` hook; the
/// instance name does not select an executor.
/// An instance may expose its own reload through `Runnable::as_reloadable`;
/// otherwise targeted reload uses the producing component's shared hook.
pub struct RunnableInstance<S> {
    pub(crate) name: Arc<str>,
    pub(crate) identity: Arc<dyn Any + Send + Sync>,
    pub(crate) runnable: Arc<dyn AnyRunnable<S>>,
    #[cfg(feature = "fleet")]
    pub(crate) placement: Option<crate::fleet::InstancePlacement>,
}

impl<S> Clone for RunnableInstance<S> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            identity: self.identity.clone(),
            runnable: self.runnable.clone(),
            #[cfg(feature = "fleet")]
            placement: self.placement.clone(),
        }
    }
}

impl<S: Send + Sync + 'static> RunnableInstance<S> {
    /// Declare an instance without starting it. Names are owned and must be
    /// unique across this process's runnable declarations.
    pub fn new<D: Resolve<S>>(
        name: impl Into<Arc<str>>,
        runnable: Arc<dyn Runnable<S, D>>,
    ) -> Self {
        let name = name.into();
        let identity: Arc<dyn Any + Send + Sync> = runnable.clone();
        Self {
            runnable: super::erased::erase_runnable(name.clone(), runnable),
            name,
            identity,
            #[cfg(feature = "fleet")]
            placement: None,
        }
    }

    /// Run this instance under its own placement instead of its component's.
    #[cfg(feature = "fleet")]
    pub fn placed(
        mut self,
        placement: crate::fleet::InstancePlacement,
    ) -> Self {
        self.placement = Some(placement);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
