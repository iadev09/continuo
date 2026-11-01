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
pub struct RunnableInstance<S> {
    pub(crate) name: Arc<str>,
    pub(crate) identity: Arc<dyn Any + Send + Sync>,
    pub(crate) runnable: Arc<dyn AnyRunnable<S>>,
}

impl<S> Clone for RunnableInstance<S> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            identity: self.identity.clone(),
            runnable: self.runnable.clone(),
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
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
