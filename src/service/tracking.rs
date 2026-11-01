use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_util::task::{TaskTracker, task_tracker::TaskTrackerToken};

use super::{RunContext, RunIdentity};
use crate::{Error, Result};

/// Shared admission and completion state for an execution. A fleet slot also
/// registers its tasks with the controller's generation scope.
#[derive(Debug)]
pub(crate) struct TaskScope {
    identity: Option<RunIdentity>,
    closed: AtomicBool,
    tracker: OnceLock<TaskTracker>,
    owner: Option<Handle>,
    parent: Option<Arc<TaskScope>>,
}

impl TaskScope {
    pub(super) fn new(identity: Option<RunIdentity>) -> Self {
        Self {
            identity,
            closed: AtomicBool::new(false),
            tracker: OnceLock::new(),
            owner: Handle::try_current().ok(),
            parent: None,
        }
    }

    #[cfg(feature = "fleet")]
    pub(super) fn execution(parent: &Arc<Self>) -> Self {
        let parent = parent.parent.as_ref().unwrap_or(parent).clone();
        Self {
            identity: parent.identity.clone(),
            closed: AtomicBool::new(false),
            tracker: OnceLock::new(),
            owner: parent.owner.clone(),
            parent: Some(parent),
        }
    }

    pub(super) fn owner(&self) -> Result<&Handle> {
        self.owner.as_ref().ok_or_else(|| {
            Error::msg("tracked task scope has no Tokio execution owner")
        })
    }

    pub(super) fn identity(&self) -> Option<&RunIdentity> {
        self.identity.as_ref()
    }

    fn token(&self) -> Result<TaskTrackerToken> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::msg("tracked task scope is closed"));
        }
        let token = self.tracker.get_or_init(TaskTracker::new).token();
        // Register before accepting admission. Closing acquires this RMW's
        // publication, so it sees the initialized tracker and its token. A
        // token registered after closure is rejected and never starts work.
        self.closed
            .compare_exchange(false, false, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::msg("tracked task scope is closed"))?;
        Ok(token)
    }

    pub(super) fn register(&self) -> Result<Registration> {
        Ok(Registration {
            _local: self.token()?,
            _parent: self
                .parent
                .as_ref()
                .map(|parent| parent.token())
                .transpose()?,
        })
    }

    pub(super) fn spawn<F>(
        &self,
        future: F,
        context: RunContext,
    ) -> JoinHandle<Result<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        // Admission's reservation remains held until both futures below have
        // been registered. TaskTracker drops each token after its future's
        // destructor, including when the future was never polled.
        let tracked = self.tracker.get().unwrap().track_future(future);
        match &self.parent {
            Some(parent) => self.execute(
                parent.tracker.get().unwrap().track_future(tracked),
                context,
            ),
            None => self.execute(tracked, context),
        }
    }

    fn execute<F>(
        &self,
        tracked: F,
        context: RunContext,
    ) -> JoinHandle<Result<()>>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let owner = self.owner.as_ref().unwrap();
        match context.execution_executor().cloned() {
            Some(executor) => {
                // A transferred task may outlive its aborted submitter.
                // Track actual work and adapter completion independently.
                let execution =
                    executor.execute_child(Box::pin(tracked), context);
                let completion =
                    self.tracker.get().unwrap().track_future(execution);
                match &self.parent {
                    Some(parent) => owner.spawn(
                        parent.tracker.get().unwrap().track_future(completion),
                    ),
                    None => owner.spawn(completion),
                }
            }
            None => owner.spawn(tracked),
        }
    }

    pub(crate) fn close(&self) {
        self.closed.swap(true, Ordering::AcqRel);
        if let Some(tracker) = self.tracker.get() {
            tracker.close();
        }
    }

    pub(crate) async fn wait(&self) {
        self.close();
        if let Some(tracker) = self.tracker.get() {
            tracker.wait().await;
        }
    }
}

pub(super) struct Registration {
    _local: TaskTrackerToken,
    _parent: Option<TaskTrackerToken>,
}

struct Owner(RunContext);

impl Owner {
    fn close(&self) {
        self.0.tasks.close();
        self.0.cancellation.cancel();
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // Also close admission when an executor drops the owning future.
        self.close();
    }
}

/// Keep the owner's future and its tracked descendants within one completion
/// boundary. Panic is resumed only after descendants have drained.
pub(crate) async fn run_owned<F>(
    context: RunContext,
    future: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let owner = Owner(context);
    let outcome = {
        let future = future;
        tokio::pin!(future);
        poll_fn(|cx| {
            match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
                Err(panic) => Poll::Ready(Err(panic)),
            }
        })
        .await
    };
    owner.close();
    owner.0.tasks.wait().await;
    match outcome {
        Ok(result) => result,
        Err(panic) => resume_unwind(panic),
    }
}

#[cfg(test)]
#[path = "../../tests/service/tracking.rs"]
mod tests;
