use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::Provider;

const COMMAND_CAPACITY: usize = 64;

/// Initial activation policy for one registered runnable service.
///
/// The runtime samples this policy after provider boot and re-evaluates it for
/// observation and explicit starts, so a provider config reload changes the
/// effective policy without duplicating it in the manager. It is independent
/// from [`ServiceStatus`]: a manually started service is both `Manual` and
/// `Running`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ServiceStartPolicy {
    /// Start an initial generation when the runtime is initialized.
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
}

impl RunContext {
    /// Build a generation context around an existing cancellation token.
    ///
    /// [`crate::Runtime`] normally constructs this from a child of the
    /// process shutdown token. The public constructor is useful to focused
    /// provider harnesses that drive one [`crate::Runnable`] directly while
    /// preserving the same cancellation contract.
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
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
    pub(crate) name: &'static str,
    pub(crate) status: ServiceStatus,
    pub(crate) start_policy: ServiceStartPolicy,
    pub(crate) reloadable: bool,
    pub(crate) generation: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) reload_revision: u64,
    pub(crate) last_reload_error: Option<String>,
}

impl ServiceSnapshot {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn status(&self) -> ServiceStatus {
        self.status
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

#[derive(Debug)]
pub enum ServiceManagerError {
    RuntimeUnavailable,
    NotFound(String),
    Unavailable(String),
    NotReloadable(String),
    Busy { name: String, status: ServiceStatus },
    ShuttingDown,
    OperationFailed { name: String, message: String },
}

impl fmt::Display for ServiceManagerError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::RuntimeUnavailable => f.write_str("service runtime is unavailable"),
            Self::NotFound(name) => write!(f, "runnable service '{name}' is not registered"),
            Self::Unavailable(name) => {
                write!(f, "runnable service '{name}' is unavailable on this runtime")
            }
            Self::NotReloadable(name) => {
                write!(f, "runnable service '{name}' is not reloadable")
            }
            Self::Busy { name, status } => {
                write!(f, "service '{name}' is currently {status}")
            }
            Self::ShuttingDown => f.write_str("service runtime is shutting down"),
            Self::OperationFailed { name, message } => {
                write!(f, "service '{name}' operation failed: {message}")
            }
        }
    }
}

impl std::error::Error for ServiceManagerError {}

type SnapshotReply = oneshot::Sender<Result<ServiceSnapshot, ServiceManagerError>>;
type ListReply = oneshot::Sender<Result<Vec<ServiceSnapshot>, ServiceManagerError>>;

pub(crate) enum ServiceCommand {
    List { reply: ListReply },
    Start { name: String, reply: SnapshotReply },
    Stop { name: String, reply: SnapshotReply },
    Reload { name: String, reply: SnapshotReply },
}

/// Cloneable control handle for runnable services owned by [`crate::Runtime`].
///
/// This provider contains no duplicate lifecycle state. Commands cross a
/// bounded channel and are applied by the runtime that owns the live futures.
pub struct ServiceManager {
    commands: mpsc::Sender<ServiceCommand>,
}

impl ServiceManager {
    pub(crate) fn channel() -> (Arc<Self>, mpsc::Receiver<ServiceCommand>) {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        (Arc::new(Self { commands }), receiver)
    }

    pub async fn list(&self) -> Result<Vec<ServiceSnapshot>, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::List { reply })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    pub async fn start(
        &self,
        name: impl Into<String>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Start { name: name.into(), reply })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    pub async fn stop(
        &self,
        name: impl Into<String>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Stop { name: name.into(), reply })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    /// Reload one runnable provider without replacing its live generation.
    pub async fn reload(
        &self,
        name: impl Into<String>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(ServiceCommand::Reload { name: name.into(), reply })
            .await
            .map_err(|_| ServiceManagerError::RuntimeUnavailable)?;
        result.await.map_err(|_| ServiceManagerError::RuntimeUnavailable)?
    }

    /// Stop the current generation completely before starting its successor.
    pub async fn restart(
        &self,
        name: impl Into<String>,
    ) -> Result<ServiceSnapshot, ServiceManagerError> {
        let name = name.into();
        self.stop(name.clone()).await?;
        self.start(name).await
    }
}

#[async_trait]
impl<S> Provider<S> for ServiceManager
where
    S: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        "service-manager"
    }
}
