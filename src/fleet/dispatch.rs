//! Execution selection contracts; coordination belongs to the fleet adapter.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crate::{AnyRunnable, Error, Result, RunContext, SharedState};

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

    /// Validate an explicitly declared lease policy. `Each` uses no lease.
    pub fn validate_policy(
        self,
        policy: Option<DispatchPolicy>,
    ) -> Result<()> {
        if self.is_controller_driven() {
            policy
                .ok_or_else(|| {
                    Error::msg("dispatch one/pool requires dispatch_policy")
                })?
                .validate()?;
        }
        Ok(())
    }
}

/// Service-owned execution capacity and lease timing, independent of transport.
/// No duration is inferred. `One` uses one slot; only `Pool` uses `capacity`.
/// Adapters implement coordination, not configuration ownership.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DispatchPolicy {
    pub capacity: NonZeroU16,
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub renewal_interval: Duration,
}

impl DispatchPolicy {
    /// Check the declared timing before a controller accepts ownership.
    pub fn validate(&self) -> Result<()> {
        if self.lease_ttl.is_zero() {
            return Err(Error::msg("dispatch lease_ttl must be nonzero"));
        }
        if self.renewal_interval.is_zero()
            || self.renewal_interval >= self.lease_ttl
        {
            return Err(Error::msg(
                "dispatch renewal_interval must be nonzero and shorter than lease_ttl",
            ));
        }
        if tokio::time::Instant::now()
            .checked_add(self.renewal_interval)
            .is_none()
        {
            return Err(Error::msg(
                "dispatch renewal_interval exceeds the runtime clock",
            ));
        }
        Ok(())
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

    /// Policy captured for a runnable generation, usually from prepared config.
    /// `Each` needs none. Lease-based adapters require an explicit policy for
    /// `One`/`Pool`; omission is a validation error, not a fallback to `Each`.
    /// This optional method does not impose lease semantics on other adapters,
    /// such as a task manager that owns its own coordination policy.
    fn dispatch_policy(&self) -> Option<DispatchPolicy> {
        None
    }
}

/// Identity and placement of one locally managed controller generation.
/// The service supplies its policy; the adapter must retain this snapshot for
/// the generation, including drain. Config reload cannot mutate active leases.
#[derive(Clone, Copy, Debug)]
pub struct DispatchRequest {
    pub name: &'static str,
    pub generation: u64,
    pub scope: Scope,
    pub dispatch: Dispatch,
    pub policy: Option<DispatchPolicy>,
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
    active_executions: Arc<AtomicU64>,
}

impl<S: Clone> Clone for ServiceExecution<S> {
    fn clone(&self) -> Self {
        Self {
            runnable: self.runnable.clone(),
            state: self.state.clone(),
            active_executions: self.active_executions.clone(),
        }
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
        active_executions: Arc<AtomicU64>,
    ) -> Self {
        Self { runnable, state, active_executions }
    }

    /// Run an owned local execution until its runnable has fully returned.
    pub async fn run(
        &self,
        context: RunContext,
    ) -> Result<()> {
        let _execution =
            crate::service::ExecutionGuard::enter(&self.active_executions);
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
