use super::*;
use crate::{Component, Runnable, RunnableInstance};
use crate::{
    ExecutionDomain, ExecutionDriver, ExecutionFuture, ExecutionSpec,
    ExecutorSpec, PreparedExecution,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Mutex, RwLock, Weak};
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
    draining: Notify,
    release: Option<Arc<Notify>>,
    contexts: Mutex<Vec<RunContext>>,
    execution: ExecutionSpec,
    own_reload: bool,
    reloads: AtomicUsize,
    reject_reload: AtomicBool,
}

#[async_trait]
impl Runnable<State> for Work {
    fn as_reloadable(&self) -> Option<&dyn crate::Reloadable<State>> {
        self.own_reload.then_some(self)
    }

    fn execution_spec(
        &self,
        _: &State,
    ) -> ExecutionSpec {
        self.execution.clone()
    }

    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        context: RunContext,
    ) -> Result<()> {
        self.contexts.lock().unwrap().push(context.clone());
        self.entered.notify_one();
        context.cancelled().await;
        self.draining.notify_one();
        if let Some(release) = &self.release {
            release.notified().await;
        }
        Ok(())
    }
}

#[async_trait]
impl crate::Reloadable<State> for Work {
    async fn reload(
        &self,
        _: &State,
    ) -> Result<()> {
        if self.reject_reload.load(Ordering::Relaxed) {
            return Err(Error::msg("instance preparation rejected"));
        }
        self.reloads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Default)]
struct Driver(Mutex<Vec<ExecutionSpec>>);

impl ExecutionDriver for Driver {
    fn prepare(
        &self,
        spec: ExecutionSpec,
    ) -> Result<Arc<dyn PreparedExecution>> {
        self.0.lock().unwrap().push(spec);
        Ok(Arc::new(Adapter))
    }
}

struct Adapter;

impl PreparedExecution for Adapter {
    fn execute(
        &self,
        task: ExecutionFuture,
        _: RunContext,
    ) -> ExecutionFuture {
        task
    }
}

#[tokio::test]
async fn each_instance_selects_its_own_execution_spec() {
    struct Domain;
    let (registry, factory, state, runtime) = setup();
    let first_spec =
        ExecutionSpec::default().with_executor(ExecutorSpec::Shared(
            ExecutionDomain::named::<Domain>(String::from("first")),
        ));
    let second_spec =
        ExecutionSpec::default().with_executor(ExecutorSpec::Shared(
            ExecutionDomain::named::<Domain>(String::from("second")),
        ));
    let first =
        Arc::new(Work { execution: first_spec.clone(), ..Default::default() });
    let second =
        Arc::new(Work { execution: second_spec.clone(), ..Default::default() });
    factory.set(vec![
        RunnableInstance::new("first", first.clone()),
        RunnableInstance::new("second", second.clone()),
    ]);
    let driver = Arc::new(Driver::default());
    let mut runtime = runtime.with_execution_driver(driver.clone()).unwrap();
    runtime.spawn_all(&registry, state).unwrap();
    first.entered.notified().await;
    second.entered.notified().await;
    assert_eq!(
        *driver.0.lock().unwrap(),
        vec![first_spec.clone(), second_spec.clone()]
    );
    assert_eq!(first.contexts.lock().unwrap()[0].execution_spec(), first_spec);
    assert_eq!(
        second.contexts.lock().unwrap()[0].execution_spec(),
        second_spec
    );
    runtime.begin_shutdown();
    runtime.drain().await.unwrap();
}

#[derive(Default)]
struct Factory {
    instances: RwLock<Vec<RunnableInstance<State>>>,
    reloads: AtomicUsize,
}
impl Factory {
    fn set(
        &self,
        instances: Vec<RunnableInstance<State>>,
    ) {
        *self.instances.write().unwrap() = instances;
    }
}
impl Component<State> for Factory {
    type Deps = ();
    fn name(&self) -> &str {
        "factory"
    }
    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<State>> {
        self.instances.read().unwrap().clone()
    }
    fn as_reloadable(&self) -> Option<&dyn crate::Reloadable<State>> {
        Some(self)
    }
}

#[async_trait]
impl crate::Reloadable<State> for Factory {
    async fn reload(
        &self,
        _: &State,
    ) -> Result<()> {
        self.reloads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn setup() -> (Registry<State>, Arc<Factory>, State, Runtime<State>) {
    let registry = Registry::new();
    let factory = Arc::new(Factory::default());
    registry.register(factory.clone()).unwrap();
    (registry, factory, State::default(), Runtime::default())
}

#[tokio::test]
async fn targeted_instance_reload_retains_factory_fallback_and_isolates_failure()
 {
    let (registry, factory, state, mut runtime) = setup();
    let first = Arc::new(Work { own_reload: true, ..Default::default() });
    let second = Arc::new(Work::default());
    factory.set(vec![
        RunnableInstance::new("first", first.clone()),
        RunnableInstance::new("second", second.clone()),
    ]);
    runtime.spawn_all(&registry, state.clone()).unwrap();
    first.entered.notified().await;
    second.entered.notified().await;

    // A source notification remains a component-wide operation, even when
    // its selected management entry also has an instance reload capability.
    let (reply, result) = oneshot::channel();
    runtime
        .handle_prepared_command(
            ServiceCommand::Reload { name: "first".into(), reply },
            true,
        )
        .await;
    result.await.unwrap().unwrap();
    assert_eq!(factory.reloads.load(Ordering::Relaxed), 1);
    assert_eq!(first.reloads.load(Ordering::Relaxed), 0);

    let manager = runtime.manager();
    let observing = state.clone();
    let running = tokio::spawn(async move {
        runtime.wait_until_shutdown(&observing).await.unwrap();
        runtime.drain().await.unwrap();
    });
    let reloaded = manager.reload("first").await.unwrap();
    assert!(reloaded.is_reloadable());
    assert_eq!(reloaded.generation(), 1);
    assert_eq!(first.reloads.load(Ordering::Relaxed), 1);
    assert_eq!(factory.reloads.load(Ordering::Relaxed), 1);
    manager.reload("second").await.unwrap();
    assert_eq!(factory.reloads.load(Ordering::Relaxed), 2);
    assert_eq!(second.reloads.load(Ordering::Relaxed), 0);

    first.reject_reload.store(true, Ordering::Relaxed);
    assert!(matches!(
        manager.reload("first").await,
        Err(ServiceManagerError::OperationFailed { .. })
    ));
    assert_eq!(factory.reloads.load(Ordering::Relaxed), 2);
    assert_eq!(first.reloads.load(Ordering::Relaxed), 1);
    assert!(!first.contexts.lock().unwrap()[0].is_cancelled());
    assert!(!second.contexts.lock().unwrap()[0].is_cancelled());
    state.0.cancel();
    running.await.unwrap();
}

#[tokio::test]
async fn named_instances_share_a_type_but_own_independent_generations() {
    let (registry, factory, state, mut runtime) = setup();
    let first = Arc::new(Work::default());
    let second = Arc::new(Work::default());
    factory.set(vec![
        RunnableInstance::new(String::from("listener:first"), first.clone()),
        RunnableInstance::new(String::from("listener:second"), second.clone()),
    ]);
    runtime.spawn_all(&registry, state.clone()).unwrap();
    first.entered.notified().await;
    second.entered.notified().await;
    let first_identity =
        first.contexts.lock().unwrap()[0].identity().unwrap().clone();
    let second_identity =
        second.contexts.lock().unwrap()[0].identity().unwrap().clone();
    assert_eq!(first_identity.instance(), "listener:first");
    assert_eq!(first_identity.generation(), 1);
    assert_eq!(second_identity.instance(), "listener:second");
    assert_eq!(second_identity.generation(), 1);
    assert!(
        runtime
            .snapshots()
            .iter()
            .all(|snapshot| snapshot.component() == "factory")
    );
    let manager = runtime.manager();
    let observing = state.clone();
    let running = tokio::spawn(async move {
        runtime.wait_until_shutdown(&observing).await.unwrap();
        runtime.drain().await.unwrap();
        runtime
    });
    let stopped =
        manager.stop(Arc::<str>::from("listener:first")).await.unwrap();
    assert_eq!(stopped.status(), ServiceStatus::Stopped);
    assert!(!second.contexts.lock().unwrap()[0].is_cancelled());
    let restarted = manager.start("listener:first").await.unwrap();
    first.entered.notified().await;
    assert_eq!(restarted.generation(), 2);
    let next_identity =
        first.contexts.lock().unwrap()[1].identity().unwrap().clone();
    assert_eq!(next_identity.instance(), restarted.name());
    assert_eq!(next_identity.generation(), restarted.generation());
    assert_ne!(first_identity, next_identity);
    assert_eq!(first_identity.generation(), 1);
    assert_eq!(
        second.contexts.lock().unwrap()[0].identity(),
        Some(&second_identity)
    );
    let third = Arc::new(Work::default());
    factory.set(vec![
        RunnableInstance::new("listener:first", first.clone()),
        RunnableInstance::new("listener:second", second.clone()),
        RunnableInstance::new(String::from("listener:third"), third.clone()),
    ]);
    let refreshed = manager.reconcile().await.unwrap();
    third.entered.notified().await;
    assert_eq!(refreshed.len(), 3);
    assert_eq!(
        refreshed
            .iter()
            .find(|item| item.name() == "listener:second")
            .unwrap()
            .generation(),
        1
    );
    state.0.cancel();
    running.await.unwrap();
}

#[tokio::test]
async fn removed_instance_stays_owned_until_its_drain_finishes() {
    let (registry, factory, state, mut runtime) = setup();
    let release = Arc::new(Notify::new());
    let work =
        Arc::new(Work { release: Some(release.clone()), ..Default::default() });
    let weak: Weak<Work> = Arc::downgrade(&work);
    factory.set(vec![RunnableInstance::new(
        String::from("temporary"),
        work.clone(),
    )]);
    runtime.spawn_all(&registry, state).unwrap();
    work.entered.notified().await;
    factory.set(Vec::new());
    let snapshots = runtime.reconcile_instances().unwrap();
    work.draining.notified().await;
    assert_eq!(snapshots[0].status(), ServiceStatus::Stopping);
    assert!(runtime.start_generation("temporary", false).is_err());
    drop(work);
    assert!(weak.upgrade().is_some());
    release.notify_one();
    let exit = runtime.join_set.join_next_with_id().await.unwrap();
    runtime.complete_join(exit).unwrap();
    assert!(runtime.snapshots().is_empty());
    assert!(weak.upgrade().is_none());
    // Snapshots retain an owned label after the factory and task are gone.
    assert_eq!(snapshots[0].name(), "temporary");
}

#[tokio::test]
async fn restored_instance_restarts_only_after_the_retiring_generation_drains()
{
    let (registry, factory, state, mut runtime) = setup();
    let release = Arc::new(Notify::new());
    let work =
        Arc::new(Work { release: Some(release.clone()), ..Default::default() });
    factory.set(vec![RunnableInstance::new("restored", work.clone())]);
    runtime.spawn_all(&registry, state).unwrap();
    work.entered.notified().await;
    factory.set(Vec::new());
    runtime.reconcile_instances().unwrap();
    work.draining.notified().await;
    factory.set(vec![RunnableInstance::new("restored", work.clone())]);
    runtime.reconcile_instances().unwrap();
    assert_eq!(work.contexts.lock().unwrap().len(), 1);
    release.notify_one();
    let exit = runtime.join_set.join_next_with_id().await.unwrap();
    runtime.complete_join(exit).unwrap();
    work.entered.notified().await;
    assert_eq!(runtime.services["restored"].generation, 2);
    assert!(!work.contexts.lock().unwrap()[1].is_cancelled());
    runtime.begin_shutdown();
    release.notify_one();
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn retired_instance_is_released_after_an_aborted_future_is_joined() {
    let (registry, factory, state, mut runtime) = setup();
    let work = Arc::new(Work {
        release: Some(Arc::new(Notify::new())),
        ..Default::default()
    });
    let weak = Arc::downgrade(&work);
    factory.set(vec![RunnableInstance::new("retired", work.clone())]);
    runtime.spawn_all(&registry, state).unwrap();
    work.entered.notified().await;
    factory.set(Vec::new());
    runtime.reconcile_instances().unwrap();
    work.draining.notified().await;
    drop(work);
    runtime.join_set.abort_all();
    let exit = runtime.join_set.join_next_with_id().await.unwrap();
    assert!(runtime.complete_join(exit).is_err());
    assert!(runtime.snapshots().is_empty());
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn active_replacement_is_rejected_without_mutating_live_work() {
    let (registry, factory, state, mut runtime) = setup();
    let first = Arc::new(Work::default());
    let next = Arc::new(Work::default());
    factory.set(vec![RunnableInstance::new("same", first.clone())]);
    runtime.spawn_all(&registry, state).unwrap();
    first.entered.notified().await;
    factory.set(vec![RunnableInstance::new("same", next.clone())]);
    assert!(
        runtime
            .reconcile_instances()
            .unwrap_err()
            .to_string()
            .contains("stop runnable")
    );
    assert!(!first.contexts.lock().unwrap()[0].is_cancelled());
    assert!(next.contexts.lock().unwrap().is_empty());
    first.contexts.lock().unwrap()[0].cancellation_token().cancel();
    let exit = runtime.join_set.join_next_with_id().await.unwrap();
    runtime.complete_join(exit).unwrap();
    runtime.reconcile_instances().unwrap();
    assert_eq!(runtime.services["same"].status, ServiceStatus::Stopped);
    runtime.start_generation("same", false).unwrap();
    next.entered.notified().await;
    assert_eq!(runtime.services["same"].generation, 2);
    runtime.begin_shutdown();
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn duplicate_instance_names_reject_initial_admission() {
    let (registry, factory, state, mut runtime) = setup();
    let work = Arc::new(Work::default());
    factory.set(vec![
        RunnableInstance::new("duplicate", work.clone()),
        RunnableInstance::new("duplicate", work.clone()),
    ]);
    assert!(runtime.spawn_all(&registry, state).is_err());
    assert!(runtime.state.is_none());
    assert!(work.contexts.lock().unwrap().is_empty());
}

struct Missing;
struct NeedsMissing;
#[async_trait]
impl Runnable<State, (Arc<Missing>,)> for NeedsMissing {
    async fn run(
        self: Arc<Self>,
        _: (Arc<Missing>,),
        _: State,
        _: RunContext,
    ) -> Result<()> {
        panic!("a missing dependency must prevent execution")
    }
}

#[tokio::test]
async fn added_instances_are_all_admitted_before_any_existing_one_is_retired() {
    let (registry, factory, state, mut runtime) = setup();
    let current = Arc::new(Work::default());
    let added = Arc::new(Work::default());
    factory.set(vec![RunnableInstance::new("current", current.clone())]);
    runtime.spawn_all(&registry, state).unwrap();
    current.entered.notified().await;
    factory.set(vec![
        RunnableInstance::new("added", added.clone()),
        RunnableInstance::new("missing", Arc::new(NeedsMissing)),
    ]);
    assert!(runtime.reconcile_instances().is_err());
    assert_eq!(runtime.services.len(), 1);
    assert!(!current.contexts.lock().unwrap()[0].is_cancelled());
    assert!(added.contexts.lock().unwrap().is_empty());
    runtime.begin_shutdown();
    runtime.drain().await.unwrap();
}
