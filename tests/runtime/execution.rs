use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Weak};

use async_trait::async_trait;
use tokio::sync::Notify;

use super::*;
use crate::{
    Component, ExecutionDomain, ExecutionDriver, ExecutionFuture,
    ExecutionSpec, ExecutorSpec, PollPriority, PreparedExecution, Runnable,
    SchedulingPolicy,
};

#[derive(Clone, Default)]
struct State(CancellationToken);

impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.clone()
    }
}

struct Probe<const ID: u8> {
    spec: Mutex<ExecutionSpec>,
    samples: AtomicUsize,
    manual: bool,
    wait_for_cancel: bool,
    started: Notify,
    seen: Mutex<Vec<RunContext>>,
}

impl<const ID: u8> Probe<ID> {
    fn new(spec: ExecutionSpec) -> Self {
        Self {
            spec: Mutex::new(spec),
            samples: AtomicUsize::new(0),
            manual: false,
            wait_for_cancel: false,
            started: Notify::new(),
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl<const ID: u8> Component<State> for Probe<ID> {
    type Deps = ();
    fn name(&self) -> &str {
        if ID == 0 { "ambient" } else { "custom" }
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}

#[async_trait]
impl<const ID: u8> Runnable<State> for Probe<ID> {
    fn start_policy(
        &self,
        _: &State,
    ) -> ServiceStartPolicy {
        if self.manual {
            ServiceStartPolicy::Manual
        } else {
            ServiceStartPolicy::Automatic
        }
    }
    fn execution_spec(
        &self,
        _: &State,
    ) -> ExecutionSpec {
        self.samples.fetch_add(1, Ordering::SeqCst);
        self.spec.lock().unwrap().clone()
    }
    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        context: RunContext,
    ) -> Result<()> {
        self.seen.lock().unwrap().push(context.clone());
        self.started.notify_one();
        if self.wait_for_cancel {
            context.cancelled().await;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Driver {
    prepared: Mutex<Vec<ExecutionSpec>>,
    executed: Arc<AtomicUsize>,
    contexts: Arc<Mutex<Vec<ObservedExecution>>>,
    reject: bool,
    release: Option<Arc<Notify>>,
    task_finished: Arc<Notify>,
    drain_started: Arc<Notify>,
    drain_release: Option<Arc<Notify>>,
}

#[derive(Clone)]
struct ObservedExecution {
    spec: ExecutionSpec,
    executor: Weak<dyn PreparedExecution>,
    cancellation: CancellationToken,
}

impl ExecutionDriver for Driver {
    fn prepare(
        &self,
        spec: ExecutionSpec,
    ) -> Result<Arc<dyn PreparedExecution>> {
        self.prepared.lock().unwrap().push(spec.clone());
        if self.reject {
            return Err(Error::msg("unsupported execution policy"));
        }
        Ok(Arc::new(Adapter {
            executed: self.executed.clone(),
            contexts: self.contexts.clone(),
            release: self.release.clone(),
            task_finished: self.task_finished.clone(),
        }))
    }

    fn drain(&self) -> ExecutionFuture {
        let started = self.drain_started.clone();
        let release = self.drain_release.clone();
        Box::pin(async move {
            started.notify_one();
            if let Some(release) = release {
                release.notified().await;
            }
            Ok(())
        })
    }
}

struct Adapter {
    executed: Arc<AtomicUsize>,
    contexts: Arc<Mutex<Vec<ObservedExecution>>>,
    release: Option<Arc<Notify>>,
    task_finished: Arc<Notify>,
}

impl PreparedExecution for Adapter {
    fn execute(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture {
        let executed = self.executed.clone();
        let contexts = self.contexts.clone();
        let release = self.release.clone();
        let task_finished = self.task_finished.clone();
        Box::pin(async move {
            executed.fetch_add(1, Ordering::SeqCst);
            contexts.lock().unwrap().push(ObservedExecution {
                spec: context.execution_spec(),
                executor: Arc::downgrade(context.execution_executor().unwrap()),
                cancellation: context.cancellation_token(),
            });
            task.await?;
            task_finished.notify_one();
            if let Some(release) = release {
                release.notified().await;
            }
            Ok(())
        })
    }
}

struct Foreground;
fn custom_spec() -> ExecutionSpec {
    ExecutionSpec::default()
        .with_executor(
            ExecutorSpec::Shared(ExecutionDomain::of::<Foreground>()),
        )
        .with_scheduling(SchedulingPolicy::Priority(PollPriority::High))
}

#[tokio::test]
async fn ambient_generation_does_not_consult_installed_driver() {
    let registry = Registry::new();
    let probe = Arc::new(Probe::<0>::new(ExecutionSpec::default()));
    registry.register(probe.clone()).unwrap();
    let driver = Arc::new(Driver { reject: true, ..Default::default() });
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    runtime.drain().await.unwrap();
    assert!(driver.prepared.lock().unwrap().is_empty());
    assert_eq!(driver.executed.load(Ordering::SeqCst), 0);
    let seen = probe.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].execution_spec(), ExecutionSpec::default());
    assert!(seen[0].execution_executor().is_none());
}

#[tokio::test]
async fn missing_driver_rejects_entire_initial_admission_before_spawning() {
    let registry = Registry::new();
    let ambient = Arc::new(Probe::<0>::new(ExecutionSpec::default()));
    let custom = Arc::new(Probe::<1>::new(custom_spec()));
    registry.register(ambient.clone()).unwrap();
    registry.register(custom.clone()).unwrap();
    let mut runtime = Runtime::default();
    let error = runtime.spawn_all(&registry, State::default()).unwrap_err();
    assert!(error.to_string().contains("no execution driver"));
    assert!(runtime.join_set.is_empty());
    assert!(runtime.state.is_none());
    assert!(ambient.seen.lock().unwrap().is_empty());
    assert!(custom.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unsupported_custom_policy_does_not_fall_back_to_ambient() {
    let registry = Registry::new();
    let probe = Arc::new(Probe::<1>::new(custom_spec()));
    registry.register(probe.clone()).unwrap();
    let driver = Arc::new(Driver { reject: true, ..Default::default() });
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    assert!(runtime.spawn_all(&registry, State::default()).is_err());
    assert!(runtime.join_set.is_empty());
    assert!(runtime.state.is_none());
    assert_eq!(driver.executed.load(Ordering::SeqCst), 0);
    assert!(probe.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dormant_custom_generation_requires_driver_only_when_started() {
    let registry = Registry::new();
    let probe =
        Arc::new(Probe::<1> { manual: true, ..Probe::new(custom_spec()) });
    registry.register(probe.clone()).unwrap();
    let mut runtime = Runtime::default();
    runtime.spawn_all(&registry, State::default()).unwrap();
    assert_eq!(probe.samples.load(Ordering::SeqCst), 0);
    assert!(runtime.start_generation("custom", false).is_err());
    let snapshot = runtime.snapshots().pop().unwrap();
    assert_eq!(snapshot.generation(), 0);
    assert_eq!(snapshot.status(), ServiceStatus::Stopped);
    assert!(runtime.join_set.is_empty());
}

#[tokio::test]
async fn prepared_generation_keeps_frozen_spec_adapter_and_cancellation() {
    let registry = Registry::new();
    let selected = custom_spec();
    let probe = Arc::new(Probe::<1> {
        wait_for_cancel: true,
        ..Probe::new(selected.clone())
    });
    registry.register(probe.clone()).unwrap();
    let driver = Arc::new(Driver::default());
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    *probe.spec.lock().unwrap() = ExecutionSpec::default();
    probe.started.notified().await;
    assert_eq!(probe.samples.load(Ordering::SeqCst), 1);
    let seen = probe.seen.lock().unwrap()[0].clone();
    let delivered = driver.contexts.lock().unwrap()[0].clone();
    assert_eq!(seen.execution_spec(), selected);
    assert_eq!(delivered.spec, selected);
    assert!(Arc::ptr_eq(
        seen.execution_executor().unwrap(),
        &delivered.executor.upgrade().unwrap()
    ));
    let child = seen.child();
    assert_eq!(child.execution_spec(), selected);
    assert!(Arc::ptr_eq(
        seen.execution_executor().unwrap(),
        child.execution_executor().unwrap()
    ));
    child.cancellation_token().cancel();
    assert!(!seen.is_cancelled());
    let sibling = seen.child();
    runtime.begin_shutdown();
    runtime.drain().await.unwrap();
    assert!(seen.is_cancelled());
    assert!(sibling.is_cancelled());
    assert!(delivered.cancellation.is_cancelled());
    assert_eq!(driver.executed.load(Ordering::SeqCst), 1);
    drop(child);
    drop(sibling);
    drop(seen);
    probe.seen.lock().unwrap().clear();
    assert!(delivered.executor.upgrade().is_none());
}

#[tokio::test]
async fn restart_reprepares_policy_without_rewriting_previous_context() {
    let registry = Registry::new();
    let first = custom_spec();
    let second = first
        .clone()
        .with_scheduling(SchedulingPolicy::Priority(PollPriority::Low));
    let probe = Arc::new(Probe::<1>::new(first.clone()));
    registry.register(probe.clone()).unwrap();
    let driver = Arc::new(Driver::default());
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    let joined = runtime.join_set.join_next_with_id().await.unwrap();
    runtime.complete_join(joined).unwrap();
    *probe.spec.lock().unwrap() = second.clone();
    runtime.start_generation("custom", false).unwrap();
    runtime.drain().await.unwrap();
    assert_eq!(
        *driver.prepared.lock().unwrap(),
        vec![first.clone(), second.clone()]
    );
    let seen = probe.seen.lock().unwrap();
    assert_eq!(seen[0].execution_spec(), first);
    assert_eq!(seen[1].execution_spec(), second);
}

#[tokio::test]
async fn drain_waits_for_adapter_owned_work_after_runnable_returns() {
    use futures_util::FutureExt;
    let registry = Registry::new();
    let probe = Arc::new(Probe::<1>::new(custom_spec()));
    registry.register(probe).unwrap();
    let release = Arc::new(Notify::new());
    let driver = Arc::new(Driver {
        release: Some(release.clone()),
        ..Default::default()
    });
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    driver.task_finished.notified().await;
    runtime.begin_shutdown();
    let drain = runtime.drain();
    tokio::pin!(drain);
    assert!(drain.as_mut().now_or_never().is_none());
    release.notify_one();
    drain.await.unwrap();
}

#[test]
fn execution_spec_survives_typed_dependency_erasure() {
    let registry = Registry::new();
    registry.register(Arc::new(Probe::<1>::new(custom_spec()))).unwrap();
    let entry = registry.runnable_entries().unwrap().pop().unwrap();
    assert_eq!(entry.runnable.execution_spec(&State::default()), custom_spec());
    let bound = entry.runnable.bind(&registry).unwrap();
    assert_eq!(bound.execution_spec(&State::default()), custom_spec());
}

#[tokio::test]
async fn forced_drain_reaps_driver_work_after_aborting_submitters() {
    let registry = Registry::new();
    let probe = Arc::new(Probe::<1> {
        wait_for_cancel: true,
        ..Probe::new(custom_spec())
    });
    registry.register(probe.clone()).unwrap();
    let release = Arc::new(Notify::new());
    let driver = Arc::new(Driver {
        drain_release: Some(release.clone()),
        ..Default::default()
    });
    let mut runtime =
        Runtime::default().with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    probe.started.notified().await;
    tokio::join!(runtime.abort_and_drain(), async {
        driver.drain_started.notified().await;
        assert!(probe.seen.lock().unwrap()[0].is_cancelled());
        release.notify_one();
    });
    assert!(runtime.join_set.is_empty());
}

#[tokio::test]
async fn execution_finalization_rejects_undrained_generations() {
    let registry = Registry::new();
    let probe = Arc::new(Probe::<0> {
        wait_for_cancel: true,
        ..Probe::new(ExecutionSpec::default())
    });
    registry.register(probe.clone()).unwrap();
    let mut runtime = Runtime::default();
    runtime.spawn_all(&registry, State::default()).unwrap();
    probe.started.notified().await;
    assert!(runtime.finalize_execution().await.is_err());
    runtime.begin_shutdown();
    runtime.drain().await.unwrap();
    runtime.finalize_execution().await.unwrap();
    assert!(runtime.start_generation("ambient", false).is_err());
}

#[cfg(feature = "registry")]
#[tokio::test]
async fn execution_finalization_follows_failed_provider_cleanup() {
    #[derive(Clone)]
    struct RegisteredState(Arc<Registry<RegisteredState>>, CancellationToken);
    impl SharedState for RegisteredState {
        fn shutdown_token(&self) -> CancellationToken {
            self.1.clone()
        }
    }
    impl crate::HasRegistry for RegisteredState {
        fn registry_ref(&self) -> &Registry<Self> {
            &self.0
        }
    }
    struct Provider(Arc<Mutex<Vec<&'static str>>>);
    impl Component<RegisteredState> for Provider {
        type Deps = ();
        fn name(&self) -> &str {
            "resource-owner"
        }
        fn as_finalizable(
            &self
        ) -> Option<&dyn crate::Finalizable<RegisteredState>> {
            Some(self)
        }
    }
    #[async_trait]
    impl crate::Finalizable<RegisteredState> for Provider {
        async fn finalize(
            &self,
            _: &RegisteredState,
        ) -> Result<()> {
            self.0.lock().unwrap().push("provider");
            Err(Error::msg("expected cleanup failure"))
        }
    }
    struct OrderedDriver(Arc<Mutex<Vec<&'static str>>>);
    impl ExecutionDriver for OrderedDriver {
        fn prepare(
            &self,
            _: ExecutionSpec,
        ) -> Result<Arc<dyn PreparedExecution>> {
            unreachable!("there is no custom runnable")
        }
        fn drain(&self) -> ExecutionFuture {
            let steps = self.0.clone();
            Box::pin(async move {
                steps.lock().unwrap().push("drain");
                Ok(())
            })
        }
        fn finalize(&self) -> ExecutionFuture {
            let steps = self.0.clone();
            Box::pin(async move {
                steps.lock().unwrap().push("executor");
                Ok(())
            })
        }
    }
    let state =
        RegisteredState(Arc::new(Registry::new()), CancellationToken::new());
    let steps = Arc::new(Mutex::new(Vec::new()));
    state.0.register(Arc::new(Provider(steps.clone()))).unwrap();
    state.1.cancel();
    let finalized = Runtime::default()
        .with_execution_driver(Arc::new(OrderedDriver(steps.clone())))
        .unwrap()
        .run_prepared(state)
        .await
        .unwrap();
    assert_eq!(finalized.failures().len(), 1);
    assert_eq!(*steps.lock().unwrap(), ["drain", "provider", "executor"]);
}
