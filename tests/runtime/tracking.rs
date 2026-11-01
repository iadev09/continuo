use super::*;
use crate::{Component, Runnable};
use async_trait::async_trait;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

#[derive(Clone, Default)]
struct State(CancellationToken);
impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.clone()
    }
}

#[derive(Default)]
struct Work {
    entered: Notify,
    child_entered: Notify,
    child_draining: Notify,
    release: Arc<Notify>,
    child_finished: Arc<AtomicUsize>,
    contexts: Mutex<Vec<RunContext>>,
    child_handles: Mutex<Vec<tokio::task::AbortHandle>>,
    threads: Mutex<Vec<std::thread::ThreadId>>,
    spec: crate::ExecutionSpec,
    trace_nested: bool,
    panic: bool,
}

impl Component<State> for Work {
    type Deps = ();
    fn name(&self) -> &str {
        "tracked"
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<State> for Work {
    fn execution_spec(
        &self,
        _: &State,
    ) -> crate::ExecutionSpec {
        self.spec.clone()
    }
    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        context: RunContext,
    ) -> Result<()> {
        self.contexts.lock().unwrap().push(context.clone());
        self.threads.lock().unwrap().push(std::thread::current().id());
        let identity = context.identity().cloned();
        let work = self.clone();
        let child = context.spawn_tracked(move |child| async move {
            assert_eq!(child.identity(), identity.as_ref());
            assert_eq!(child.child().identity(), identity.as_ref());
            if work.trace_nested {
                let descendant_identity = identity.clone();
                child
                    .spawn_tracked(move |nested| async move {
                        assert_eq!(
                            nested.identity(),
                            descendant_identity.as_ref()
                        );
                        Ok(())
                    })?
                    .await??;
            }
            work.threads.lock().unwrap().push(std::thread::current().id());
            assert_eq!(child.execution_spec(), work.spec);
            work.child_entered.notify_one();
            child.cancelled().await;
            work.child_draining.notify_one();
            work.release.notified().await;
            work.child_finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })?;
        self.child_handles.lock().unwrap().push(child.abort_handle());
        drop(child);
        self.entered.notify_one();
        assert!(!self.panic, "owner panic");
        context.cancelled().await;
        Ok(())
    }
}

fn setup(work: Arc<Work>) -> (State, Runtime<State>) {
    let registry = Registry::new();
    registry.register(work).unwrap();
    let state = State::default();
    let mut runtime = Runtime::default();
    runtime.spawn_all(&registry, state.clone()).unwrap();
    (state, runtime)
}

#[tokio::test]
async fn restart_waits_for_dropped_handles_and_keeps_generations_isolated() {
    let work = Arc::new(Work::default());
    let (state, mut runtime) = setup(work.clone());
    work.entered.notified().await;
    work.child_entered.notified().await;
    let manager = runtime.manager();
    let observing = state.clone();
    let running = tokio::spawn(async move {
        runtime.wait_until_shutdown(&observing).await.unwrap();
        runtime.drain().await.unwrap();
    });
    let restarting = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.restart("tracked").await.unwrap() })
    };
    work.child_draining.notified().await;
    assert!(!restarting.is_finished());
    let stopping = manager.list().await.unwrap().pop().unwrap();
    assert_eq!(stopping.status(), ServiceStatus::Stopping);
    assert_eq!(stopping.active_executions(), 1);
    work.release.notify_one();
    assert_eq!(restarting.await.unwrap().generation(), 2);
    work.entered.notified().await;
    work.child_entered.notified().await;
    assert_eq!(work.child_finished.load(Ordering::SeqCst), 1);
    let contexts = work.contexts.lock().unwrap().clone();
    assert!(contexts[0].spawn_tracked(|_| async { Ok(()) }).is_err());
    assert!(!contexts[1].is_cancelled());
    state.0.cancel();
    work.child_draining.notified().await;
    work.release.notify_one();
    running.await.unwrap();
    assert_eq!(work.child_finished.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn owner_panic_drains_children_before_reporting_the_panic() {
    use futures_util::FutureExt;
    let work = Arc::new(Work { panic: true, ..Default::default() });
    let (_, mut runtime) = setup(work.clone());
    work.child_draining.notified().await;
    assert!(runtime.join_set.join_next_with_id().now_or_never().is_none());
    work.release.notify_one();
    let joined = runtime.join_set.join_next_with_id().await.unwrap();
    assert!(matches!(&joined, Err(error) if error.is_panic()));
    assert!(runtime.complete_join(joined).is_err());
    runtime.drain().await.unwrap();
    assert_eq!(work.child_finished.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn aborted_owner_keeps_child_drain_owned_by_the_runtime() {
    use futures_util::FutureExt;
    let work = Arc::new(Work::default());
    let (_, mut runtime) = setup(work.clone());
    work.entered.notified().await;
    work.child_entered.notified().await;
    runtime.begin_shutdown();
    runtime.join_set.abort_all();
    let joined = runtime.join_set.join_next_with_id().await.unwrap();
    assert!(runtime.complete_join(joined).is_err());
    work.child_draining.notified().await;
    assert!(!runtime.draining_tasks.is_empty());
    let drain = runtime.drain();
    tokio::pin!(drain);
    assert!(drain.as_mut().now_or_never().is_none());
    work.release.notify_one();
    drain.await.unwrap();
    assert_eq!(work.child_finished.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct Driver {
    prepared: AtomicUsize,
    executed: Arc<AtomicUsize>,
    child_executed: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<crate::ExecutionSpec>>>,
    identities: Arc<Mutex<Vec<Option<crate::RunIdentity>>>>,
    target: Option<tokio::runtime::Handle>,
    tasks: tokio_util::task::TaskTracker,
}
impl crate::ExecutionDriver for Driver {
    fn prepare(
        &self,
        _: crate::ExecutionSpec,
    ) -> Result<Arc<dyn crate::PreparedExecution>> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Adapter {
            executed: self.executed.clone(),
            child_executed: self.child_executed.clone(),
            seen: self.seen.clone(),
            identities: self.identities.clone(),
            target: self.target.clone(),
            tasks: self.tasks.clone(),
        }))
    }
    fn drain(&self) -> crate::ExecutionFuture {
        let tasks = self.tasks.clone();
        Box::pin(async move {
            tasks.close();
            tasks.wait().await;
            Ok(())
        })
    }
}
struct Adapter {
    executed: Arc<AtomicUsize>,
    child_executed: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<crate::ExecutionSpec>>>,
    identities: Arc<Mutex<Vec<Option<crate::RunIdentity>>>>,
    target: Option<tokio::runtime::Handle>,
    tasks: tokio_util::task::TaskTracker,
}
impl crate::PreparedExecution for Adapter {
    fn execute_child(
        &self,
        task: crate::ExecutionFuture,
        context: RunContext,
    ) -> crate::ExecutionFuture {
        self.child_executed.fetch_add(1, Ordering::SeqCst);
        self.execute(task, context)
    }
    fn execute(
        &self,
        task: crate::ExecutionFuture,
        context: RunContext,
    ) -> crate::ExecutionFuture {
        self.executed.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().unwrap().push(context.execution_spec());
        self.identities.lock().unwrap().push(context.identity().cloned());
        match &self.target {
            Some(target) => {
                let transferred = target.spawn(self.tasks.track_future(task));
                Box::pin(async move { transferred.await? })
            }
            None => task,
        }
    }
}

struct ExecutorThread {
    stop: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Drop for ExecutorThread {
    fn drop(&mut self) {
        self.stop.cancel();
        self.thread.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn tracked_children_reuse_the_admitted_adapter_without_repreparing() {
    use futures_util::FutureExt;
    struct Domain;
    let spec = crate::ExecutionSpec::default().with_executor(
        crate::ExecutorSpec::Shared(crate::ExecutionDomain::of::<Domain>()),
    );
    let work = Arc::new(Work {
        spec: spec.clone(),
        trace_nested: true,
        ..Default::default()
    });
    let registry = Registry::new();
    registry.register(work.clone()).unwrap();
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let (ready, prepared) = oneshot::channel();
    // Physical executor placement is the behavior under test here.
    let thread = std::thread::spawn(move || {
        let executor =
            tokio::runtime::Builder::new_current_thread().build().unwrap();
        ready
            .send((executor.handle().clone(), std::thread::current().id()))
            .unwrap();
        executor.block_on(stopping.cancelled());
    });
    let _executor = ExecutorThread { stop, thread: Some(thread) };
    let (target, target_thread) = prepared.await.unwrap();
    let driver =
        Arc::new(Driver { target: Some(target), ..Default::default() });
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    work.entered.notified().await;
    work.child_entered.notified().await;
    // Aborting the submission must not erase the transferred child's lifetime.
    work.child_handles.lock().unwrap()[0].abort();
    runtime.begin_shutdown();
    work.child_draining.notified().await;
    assert!(runtime.join_set.join_next_with_id().now_or_never().is_none());
    work.release.notify_one();
    runtime.drain().await.unwrap();
    assert_eq!(driver.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(driver.executed.load(Ordering::SeqCst), 3);
    assert_eq!(driver.child_executed.load(Ordering::SeqCst), 2);
    assert_eq!(*driver.seen.lock().unwrap(), vec![spec; 3]);
    let identity = work.contexts.lock().unwrap()[0].identity().unwrap().clone();
    assert_eq!(identity.instance(), "tracked");
    assert_eq!(identity.generation(), 1);
    assert_eq!(*driver.identities.lock().unwrap(), vec![Some(identity); 3]);
    assert_eq!(
        *work.threads.lock().unwrap(),
        vec![target_thread, target_thread]
    );
}

#[cfg(feature = "fleet")]
#[tokio::test]
async fn fleet_executions_drain_their_own_children_and_leave_siblings_running()
{
    let work = Arc::new(Work::default());
    let registry = Registry::new();
    registry.register(work.clone()).unwrap();
    let entry = registry.runnable_entries().unwrap().pop().unwrap();
    let active = Arc::new(AtomicU64::new(0));
    let execution = crate::fleet::ServiceExecution::new(
        entry.runnable.bind(&registry).unwrap(),
        State::default(),
        active.clone(),
    );
    let generation = RunContext::for_generation(
        Default::default(),
        Default::default(),
        Arc::from("slots"),
        17,
    );
    let first = {
        let execution = execution.clone();
        let context = generation.child();
        tokio::spawn(async move { execution.run(context).await })
    };
    work.entered.notified().await;
    work.child_entered.notified().await;
    let second = {
        let context = generation.child();
        tokio::spawn(async move { execution.run(context).await })
    };
    work.entered.notified().await;
    work.child_entered.notified().await;
    let contexts = work.contexts.lock().unwrap().clone();
    assert!(contexts.iter().all(|context| {
        let identity = context.identity().unwrap();
        identity.instance() == "slots" && identity.generation() == 17
    }));
    contexts[0].cancellation_token().cancel();
    work.child_draining.notified().await;
    assert_eq!(active.load(Ordering::SeqCst), 2);
    work.release.notify_one();
    first.await.unwrap().unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert!(!second.is_finished());
    contexts[1]
        .spawn_tracked(|_| async { Ok(()) })
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    generation.cancellation_token().cancel();
    work.child_draining.notified().await;
    work.release.notify_one();
    second.await.unwrap().unwrap();
    crate::service::run_owned(generation, async { Ok(()) }).await.unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
