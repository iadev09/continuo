//! Execution selection contracts; coordination belongs to the fleet adapter.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{AnyRunnable, Result, RunContext, SharedState};

use super::{Scope, Scoped};

/// Selection policy among processes admitted by [`super::Scope`].
///
/// This is metadata only: declaring a policy does not acquire a lease,
/// start an execution, or install a service-management transport.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Dispatch {
    /// Execute in each eligible process.
    #[default]
    Each,
    /// Select one owner among eligible processes.
    One,
    /// Share an explicitly configured capacity among eligible processes.
    Pool,
}

impl Dispatch {
    /// Whether selection requires coordination rather than local execution.
    pub const fn is_controller_driven(self) -> bool {
        matches!(self, Self::One | Self::Pool)
    }
}

/// Declares selection within a scope; execution adapters apply the policy.
///
/// This contract has no dependency on `Runnable`, allowing tasks and other
/// execution types to share the vocabulary. Service dispatch applies to
/// runnable executions, not to component construction or configuration.
/// Components expose this capability through `Component::as_dispatched`.
/// The execution adapter owns selection, leases, failover and command transport;
/// Continuo does not implement those mechanisms. Absence means `Dispatch::Each`.
pub trait Dispatched: Scoped {
    fn dispatch(&self) -> Dispatch;
}

/// Identity and placement of one locally managed controller generation.
/// Pool capacity and lease policy belong to the adapter's configuration;
/// neither is inferred from the number of registered components.
#[derive(Clone, Copy, Debug)]
pub struct DispatchRequest {
    pub name: &'static str,
    pub generation: u64,
    pub scope: Scope,
    pub dispatch: Dispatch,
}

/// Local execution capability handed to a fleet controller.
///
/// Cloning this handle does not start work. The controller may run it for each
/// slot it owns, using a child of its generation cancellation token. It must
/// await every execution through graceful drain before returning or releasing
/// ownership. A runnable used with `Pool` must support concurrent executions
/// of the same shared component instance.
pub struct ServiceExecution<S> {
    runnable: Arc<dyn AnyRunnable<S>>,
    state: S,
}

impl<S: Clone> Clone for ServiceExecution<S> {
    fn clone(&self) -> Self {
        Self { runnable: self.runnable.clone(), state: self.state.clone() }
    }
}

impl<S: SharedState> ServiceExecution<S> {
    /// Application state used by this generation's execution adapter.
    pub fn state(&self) -> &S {
        &self.state
    }

    pub(crate) fn new(
        runnable: Arc<dyn AnyRunnable<S>>,
        state: S,
    ) -> Self {
        Self { runnable, state }
    }

    /// Run an owned local execution until its runnable has fully returned.
    pub async fn run(
        &self,
        context: RunContext,
    ) -> Result<()> {
        self.runnable.clone().run(self.state.clone(), context).await
    }
}

/// Owns fleet selection for the complete life of a service generation.
///
/// Continuo owns the controller future, cancellation and local management.
/// The adapter owns acquisition, renewal, failover and slot ownership. Losing
/// ownership cancels its local execution; stopping a generation stops new
/// acquisitions and drains all owned executions before ownership is released.
/// Returning is the acknowledgement that no child execution remains alive.
///
/// No process-global transport or timing policy is imposed here. A controller
/// is installed explicitly on Runtime, not started as another component whose
/// registration order determines whether dispatch is available.
#[async_trait]
pub trait Dispatcher<S>: Send + Sync + 'static {
    /// Reject unsupported placement or missing adapter policy before startup.
    /// Must not acquire ownership or start work.
    fn validate(
        &self,
        request: &DispatchRequest,
        state: &S,
    ) -> Result<()>;

    /// Remain active while selecting owners, running and draining local slots.
    async fn run(
        &self,
        request: DispatchRequest,
        execution: ServiceExecution<S>,
        context: RunContext,
    ) -> Result<()>;
}
