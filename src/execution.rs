//! Optional executor placement and poll scheduling for runnable instances.
//!
//! The default uses the ambient Tokio runtime without an execution adapter.
//! Custom specifications require an application-installed [`ExecutionDriver`].
//! This module defines the contract; it creates no threads or schedulers.

use std::any::{TypeId, type_name};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use crate::RunContext;
use crate::registry::{Error, Result};

/// A process-local, typed identity for a shared execution domain.
///
/// Applications supply marker types. A domain is not a component dependency,
/// a service name, an OS thread id or a cross-process identity.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ExecutionDomain {
    id: TypeId,
    type_name: &'static str,
    instance: Option<Arc<str>>,
}

impl ExecutionDomain {
    /// Identify a domain by its marker type. Reusing the type selects the same
    /// process-local domain; different types remain independent.
    pub fn of<T: ?Sized + 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            type_name: type_name::<T>(),
            instance: None,
        }
    }

    /// Select a named domain within a marker type. The name is owned; it may
    /// come from configuration and is never promoted to a static reference.
    pub fn named<T: ?Sized + 'static>(name: impl Into<Arc<str>>) -> Self {
        Self { instance: Some(name.into()), ..Self::of::<T>() }
    }
}

/// The runnable instance an execution is prepared for.
///
/// Built from the component's registered type and the instance's management
/// name, so one instance yields an equal owner at every start and restart.
/// It is not an OS thread id or a cross-process identity.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ExecutionOwner {
    component: TypeId,
    instance: Arc<str>,
}

impl ExecutionOwner {
    /// Identify the instance `instance` of component type `C`. The runtime
    /// builds owners itself; this constructor serves harnesses that drive an
    /// [`ExecutionDriver`] directly.
    pub fn of<C: ?Sized + 'static>(instance: impl Into<Arc<str>>) -> Self {
        Self::new(TypeId::of::<C>(), instance.into())
    }

    pub(crate) fn new(
        component: TypeId,
        instance: Arc<str>,
    ) -> Self {
        Self { component, instance }
    }

    /// Runnable instance's management name.
    pub fn instance(&self) -> &str {
        &self.instance
    }
}

impl fmt::Debug for ExecutionOwner {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("ExecutionOwner")
            .field("instance", &self.instance)
            .field("component", &self.component)
            .finish()
    }
}

impl fmt::Debug for ExecutionDomain {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("ExecutionDomain")
            .field("type", &self.type_name)
            .field("instance", &self.instance)
            .finish()
    }
}

/// Thread topology requested from an execution driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExecutorKind {
    /// A single-threaded executor; placement on an OS thread belongs to the driver.
    CurrentThread,
    /// An executor with the requested nonzero number of worker threads.
    MultiThread { threads: NonZeroUsize },
}

/// Placement of one runnable instance, independent of fleet dispatch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExecutorSpec {
    /// Use the executor which starts the runnable.
    #[default]
    Inherit,
    /// Share an application-defined execution domain with other runnables.
    Shared(ExecutionDomain),
    /// Give the runnable an executor of its own with the declared topology.
    /// The driver keeps it for that [`ExecutionOwner`] across generations,
    /// start and restart, until [`ExecutionDriver::finalize`].
    Dedicated(ExecutorKind),
}

/// Poll urgency, unrelated to component boot or run-start order.
///
/// The installed driver must document fairness and starvation behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PollPriority {
    /// Lower urgency, with fairness defined by the installed driver.
    Low,
    /// Explicit normal urgency; still requires driver support.
    Normal,
    /// Higher urgency, without preempting a future already being polled.
    High,
}

/// Selection among ready work, implemented by the installed driver.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SchedulingPolicy {
    /// Keep the selected executor's ordinary poll scheduling.
    #[default]
    Inherit,
    /// Request driver-defined urgency among ready work.
    Priority(PollPriority),
    /// Relative weight; interpretation belongs to the driver's scheduler.
    Weighted(NonZeroUsize),
}

/// Execution requirements sampled once before admitting a generation.
///
/// Defaults preserve ordinary Tokio spawning. Non-default requirements are
/// never silently downgraded. Applications can implement `From<LocalPolicy>`
/// for this type without adding their domain concepts to Continuo.
///
/// ```
/// use continuo::{ExecutionDomain, ExecutionSpec, ExecutorSpec};
///
/// struct Background;
/// let spec = ExecutionSpec::default().with_executor(
///     ExecutorSpec::Shared(ExecutionDomain::of::<Background>()),
/// );
/// assert_ne!(spec, ExecutionSpec::default());
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExecutionSpec {
    /// Where this generation executes.
    pub executor: ExecutorSpec,
    /// How ready work is selected within that executor.
    pub scheduling: SchedulingPolicy,
}

impl ExecutionSpec {
    /// Set placement without changing poll scheduling.
    pub fn with_executor(
        mut self,
        executor: ExecutorSpec,
    ) -> Self {
        self.executor = executor;
        self
    }

    /// Set poll scheduling without changing placement.
    pub fn with_scheduling(
        mut self,
        scheduling: SchedulingPolicy,
    ) -> Self {
        self.scheduling = scheduling;
        self
    }
}

/// An owned runnable execution. Only custom execution crosses this erasure.
pub type ExecutionFuture =
    Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// Prepare custom execution before a runnable is marked running.
///
/// Preparation must reject unsupported placement or scheduling and must not
/// start operational work. Initial automatic generations are all prepared
/// before any is spawned. Continuo supplies no custom driver by default.
pub trait ExecutionDriver: Send + Sync + 'static {
    /// Validate the complete specification and return a generation adapter.
    /// An error rejects admission; it must not fall back to ambient execution.
    /// `owner` is equal for every generation of the same runnable instance;
    /// a `Dedicated` executor is kept for it until [`Self::finalize`].
    fn prepare(
        &self,
        owner: &ExecutionOwner,
        spec: ExecutionSpec,
    ) -> Result<Arc<dyn PreparedExecution>>;

    /// Reap transferred executions after managed runnable futures have drained.
    /// This also covers work whose submitting future was explicitly aborted.
    /// Shared and dedicated executors stay alive here: providers have not
    /// finalized yet.
    /// The returned future must await owned work without inventing a deadline.
    fn drain(&self) -> ExecutionFuture {
        Box::pin(async { Ok(()) })
    }

    /// Release shared and dedicated executors after runnable drain and
    /// provider finalization. Their I/O drivers must remain alive while
    /// providers close resources created by admitted work.
    /// A targeted service stop or restart does not call this hook.
    fn finalize(&self) -> ExecutionFuture {
        Box::pin(async { Ok(()) })
    }
}

/// One admitted generation's executor/scheduler adapter.
///
/// `execute` must honor the complete prepared specification. Its returned
/// future owns the submitted work until completion, including cancellation
/// and drain; it must not report completion while transferred work survives.
/// Errors and panics on a different executor must be returned to this future.
/// An adapter owns runtimes it creates and only borrows externally supplied
/// runtimes. Shared and dedicated executors must outlive every admitted
/// generation using them.
///
/// The context exposes this same adapter to child-work owners. Ordinary
/// `tokio::spawn` does not acquire a custom poll priority automatically;
/// adapters and hosted services must explicitly carry the execution policy
/// into their child submissions. Continuo does not intercept Tokio spawning.
/// `RunContext::spawn_tracked` submits through `execute_child` on this same
/// adapter, so an adapter can separate root teardown from concurrent child
/// submissions.
pub trait PreparedExecution: Send + Sync + 'static {
    /// Execute the owned root generation with its frozen cancellation and execution
    /// context. If this future is dropped, retain ownership of any transferred
    /// work so [`ExecutionDriver::drain`] can reap it.
    fn execute(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture;

    /// Submit a child using the root generation's prepared executor/scheduler.
    /// The default supports adapters whose submissions share one lifetime.
    /// Adapters whose reactor ends with the root override this to retain it
    /// until root completion while allowing concurrent children on it.
    fn execute_child(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture {
        self.execute(task, context)
    }
}

#[derive(Clone, Default)]
pub(crate) struct ExecutionPlan {
    pub(crate) spec: ExecutionSpec,
    pub(crate) executor: Option<Arc<dyn PreparedExecution>>,
}

impl ExecutionPlan {
    pub(crate) fn prepare(
        owner: &ExecutionOwner,
        spec: ExecutionSpec,
        driver: Option<&dyn ExecutionDriver>,
    ) -> Result<Self> {
        if spec == ExecutionSpec::default() {
            return Ok(Self::default());
        }
        let driver = driver.ok_or_else(|| {
            Error::msg(format!("no execution driver is installed for {spec:?}"))
        })?;
        Ok(Self { executor: Some(driver.prepare(owner, spec.clone())?), spec })
    }
}

impl fmt::Debug for ExecutionPlan {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("ExecutionPlan").field("spec", &self.spec).finish()
    }
}

#[cfg(test)]
#[path = "../tests/execution/unit.rs"]
mod tests;
