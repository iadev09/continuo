use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod identity;
mod tracking;
pub use identity::RunIdentity;
pub(crate) use tracking::{TaskScope, run_owned};

const COMMAND_CAPACITY: usize = 64;

/// Activation policy for one registered runnable service.
///
/// The runtime samples this policy after component boot and re-evaluates it for
/// observation, successful reloads and explicit starts. The provider owns the
/// current policy; the runtime retains the last applied policy to detect
/// availability transitions without undoing operator stops. It is independent
/// from [`ServiceStatus`]: a manually started service is both `Manual` and
/// `Running`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ServiceStartPolicy {
    /// Start initially and when a successful reload changes Unavailable to Automatic.
    #[default]
    Automatic,
    /// Keep the service stopped initially, while allowing an explicit start.
    Manual,
    /// Keep the service stopped and reject explicit start or restart requests.
    Unavailable,
}

impl ServiceStartPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::Manual => "manual",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for ServiceStartPolicy {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Cancellation scope for one live generation of a [`crate::Runnable`].
///
/// The process shutdown token is its parent. A service therefore observes the
/// same cancellation path whether the whole process is draining or only that
/// service was stopped through [`ServiceManager`].
#[derive(Clone, Debug)]
pub struct RunContext {
    cancellation: CancellationToken,
    execution: crate::execution::ExecutionPlan,
    tasks: Arc<TaskScope>,
}

impl RunContext {
    /// Build a generation context around an existing cancellation token.
    ///
    /// [`crate::Runtime`] normally constructs this from a child of the
    /// process shutdown token. The public constructor is useful to focused
    /// component harnesses that drive one [`crate::Runnable`] directly while
    /// preserving the same cancellation contract.
    /// Tracked spawning captures the current Tokio runtime as its submission
    /// owner; construct the context within that runtime when using tracking.
    pub fn new(cancellation: CancellationToken) -> Self {
        Self::with_execution(cancellation, Default::default())
    }

    pub(crate) fn with_execution(
        cancellation: CancellationToken,
        execution: crate::execution::ExecutionPlan,
    ) -> Self {
        Self { cancellation, execution, tasks: Arc::new(TaskScope::new(None)) }
    }

    pub(crate) fn for_generation(
        cancellation: CancellationToken,
        execution: crate::execution::ExecutionPlan,
        instance: Arc<str>,
        generation: u64,
    ) -> Self {
        Self {
            cancellation,
            execution,
            tasks: Arc::new(TaskScope::new(Some(RunIdentity::new(
                instance, generation,
            )))),
        }
    }

    /// Identity assigned by the runtime when this generation starts.
    /// Clones, child contexts and tracked tasks retain it across reload and
    /// restart. A context created directly with [`Self::new`] has no identity.
    pub fn identity(&self) -> Option<&RunIdentity> {
        self.tasks.identity()
    }

    /// Frozen requirements of this generation, not the next reload candidate.
    pub fn execution_spec(&self) -> crate::ExecutionSpec {
        self.execution.spec.clone()
    }

    /// Own a child cancellation scope with this generation's execution policy.
    /// Cancelling the child does not cancel the parent or its other children.
    pub fn child(&self) -> Self {
        Self {
            cancellation: self.cancellation.child_token(),
            execution: self.execution.clone(),
            tasks: self.tasks.clone(),
        }
    }

    /// Spawn an owned child task using this generation's prepared execution.
    ///
    /// The callback receives its own cancellation scope. The tracker is created
    /// on first use. Dropping the returned handle leaves the task tracked;
    /// service stop/restart and drain wait until it finishes. Task results and
    /// panics are available through the handle, as with `tokio::spawn`.
    ///
    /// Admission fails after cancellation or execution completion. The runtime
    /// signals cancellation but does not abort child tasks or impose a deadline.
    ///
    /// ```no_run
    /// # async fn example(context: continuo::RunContext) -> continuo::Result<()> {
    /// let task = context.spawn_tracked(|child| async move {
    ///     child.cancelled().await;
    ///     // Flush owned work before returning.
    ///     Ok(())
    /// })?;
    /// # let _ = task;
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn_tracked<F, Fut>(
        &self,
        task: F,
    ) -> crate::Result<JoinHandle<crate::Result<()>>>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = crate::Result<()>> + Send + 'static,
    {
        if self.is_cancelled() {
            return Err(crate::Error::msg("tracked task scope is cancelled"));
        }
        self.tasks.owner()?;
        let registration = self.tasks.register()?;
        let child = self.child();
        let future = task(child.clone());
        let spawned = self.tasks.spawn(future, child);
        drop(registration);
        Ok(spawned)
    }

    pub(crate) fn task_scope(&self) -> Arc<TaskScope> {
        self.tasks.clone()
    }

    #[cfg(feature = "fleet")]
    pub(crate) fn for_execution(&self) -> Self {
        Self {
            cancellation: self.cancellation.clone(),
            execution: self.execution.clone(),
            tasks: Arc::new(TaskScope::execution(&self.tasks)),
        }
    }

    /// Prepared adapter for explicitly routing owned child work.
    ///
    /// `None` is ambient Tokio execution. Child owners must propagate this
    /// capability when custom scheduling must cover their spawned work.
    /// Merely holding the adapter does not intercept `tokio::spawn`.
    pub fn execution_executor(
        &self
    ) -> Option<&Arc<dyn crate::PreparedExecution>> {
        self.execution.executor.as_ref()
    }

    /// Completes when this service generation must stop.
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    /// Return this generation's cancellation token for APIs that need an
    /// owned token, such as a protocol server's graceful-shutdown future.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// Observable state of one registered runnable service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStatus {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl fmt::Display for ServiceStatus {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let value = match self {
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        };
        f.write_str(value)
    }
}

/// Point-in-time view of a runnable service owned by the runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSnapshot {
    pub(crate) name: Arc<str>,
    pub(crate) component: Arc<str>,
    pub(crate) status: ServiceStatus,
    /// Actual runnable futures, excluding the coordinating controller.
    pub(crate) active_executions: u64,
    #[cfg(feature = "fleet")]
    pub(crate) dispatch: crate::fleet::Dispatch,
    pub(crate) start_policy: ServiceStartPolicy,
    pub(crate) reloadable: bool,
    pub(crate) generation: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) reload_revision: u64,
    pub(crate) last_reload_error: Option<String>,
}

impl ServiceSnapshot {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Lifecycle component that produced this runnable instance.
    pub fn component(&self) -> &str {
        &self.component
    }

    pub fn status(&self) -> ServiceStatus {
        self.status
    }

    /// Includes executions draining after cancellation, until their future ends.
    /// This is execution evidence, not protocol-specific readiness.
    pub fn active_executions(&self) -> u64 {
        self.active_executions
    }

    #[cfg(feature = "fleet")]
    pub fn dispatch(&self) -> crate::fleet::Dispatch {
        self.dispatch
    }

    pub fn start_policy(&self) -> ServiceStartPolicy {
        self.start_policy
    }

    pub fn is_reloadable(&self) -> bool {
        self.reloadable
    }

    /// Monotonically increasing local start generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Monotonically increasing revision of completed targeted reload calls.
    /// Both successful and failed attempts advance the revision; inspect
    /// [`Self::last_reload_error`] for the latest outcome.
    pub fn reload_revision(&self) -> u64 {
        self.reload_revision
    }

    pub fn last_reload_error(&self) -> Option<&str> {
        self.last_reload_error.as_deref()
    }
}

/// Generation-owned execution census. Drop accounts for errors, panic and abort
/// without making a coordinating future look like an actual runnable execution.
pub(crate) struct ExecutionGuard(Arc<AtomicU64>);

impl ExecutionGuard {
    pub(crate) fn enter(count: &Arc<AtomicU64>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ServiceManagerError {
    RuntimeUnavailable,
    NotFound(String),
    Unavailable(String),
    NotReloadable(String),
    Busy { name: String, status: ServiceStatus },
    ShuttingDown,
    OperationFailed { name: String, message: String },
    InvalidDeclarations(String),
}

impl fmt::Display for ServiceManagerError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::RuntimeUnavailable => {
                f.write_str("service runtime is unavailable")
            }
            Self::NotFound(name) => {
                write!(f, "runnable service '{name}' is not registered")
            }
            Self::Unavailable(name) => {
                write!(
                    f,
                    "runnable service '{name}' is unavailable on this runtime"
                )
            }
            Self::NotReloadable(name) => {
                write!(f, "runnable service '{name}' is not reloadable")
            }
            Self::Busy { name, status } => {
                write!(f, "service '{name}' is currently {status}")
            }
            Self::ShuttingDown => {
                f.write_str("service runtime is shutting down")
            }
            Self::OperationFailed { name, message } => {
                write!(f, "service '{name}' operation failed: {message}")
            }
            Self::InvalidDeclarations(message) => {
                write!(f, "runnable declarations rejected: {message}")
            }
        }
    }
}

impl std::error::Error for ServiceManagerError {}

type SnapshotReply =
    oneshot::Sender<Result<ServiceSnapshot, ServiceManagerError>>;
type ListReply =
    oneshot::Sender<Result<Vec<ServiceSnapshot>, ServiceManagerError>>;

pub(crate) enum ServiceCommand {
    List { reply: ListReply },
    Start { name: String, reply: SnapshotReply },
    Stop { name: String, reply: SnapshotReply },
    Reload { name: String, reply: SnapshotReply },
    Reconcile { reply: ListReply },
}

/// Cloneable control handle for runnable services owned by [`crate::Runtime`].
///
/// This component contains no duplicate lifecycle state. Commands cross a
/// bounded channel and are applied by the runtime that owns the live futures.
///
/// # Do not call back into this from a lifecycle hook
///
/// Commands are applied by the runtime's own loop, one at a time, and the loop
/// is *inside* the await while a command runs. So a `Reloadable::reload` that
/// calls `reload`, `start`, `stop` or `list` on a manager — or a runnable that
/// stops itself from its own `run` — waits for a reply from a loop that is
/// waiting for it. Neither side moves.
///
/// A slow hook has the milder version of the same property: while it runs, the
/// loop is not observing shutdown or answering anyone else. Lifecycle hooks
/// should do their own work and return.
pub struct ServiceManager {
    commands: mpsc::Sender<ServiceCommand>,
}

impl ServiceManager {
    pub(crate) fn channel() -> (Arc<Self>, mpsc::Receiver<ServiceCommand>) {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        (Arc::new(Self { commands }), receiver)
    }

    pub async fn list(
        &self
    ) -> Result<Vec<ServiceSnapshot>, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::List { reply })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    pub async fn start(
        &self,
        name: impl AsRef<str>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Start {
                name: name.as_ref().to_owned(),
                reply,
            })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    pub async fn stop(
        &self,
        name: impl AsRef<str>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Stop {
                name: name.as_ref().to_owned(),
                reply,
            })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    /// Reload the instance's owning component, then reconcile its declarations.
    /// All instances from that component share its configuration/reload hook.
    pub async fn reload(
        &self,
        name: impl AsRef<str>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Reload {
                name: name.as_ref().to_owned(),
                reply,
            })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    /// Reconcile prepared runnable declarations with the runtime's live instances.
    /// New automatic instances are admitted; removed instances drain before
    /// disappearing. Replacing an active allocation requires stopping it first.
    /// Like other manager commands, this must not be awaited inside a runtime hook.
    pub async fn reconcile(
        &self
    ) -> Result<Vec<ServiceSnapshot>, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Reconcile { reply })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    /// Stop the current generation completely before starting its successor.
    pub async fn restart(
        &self,
        name: impl AsRef<str>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let name = name.as_ref().to_owned();
        self.stop(name.clone()).await?;
        self.start(name).await
    }
}
