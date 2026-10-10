use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;

use super::*;
use crate::{
    Component, HasRegistry, Reloadable, RunContext, Runnable,
    ServiceStartPolicy,
};

#[derive(Clone)]
struct TestState(Arc<TestStateInner>);

struct TestStateInner {
    shutdown: CancellationToken,
    registry: Registry<TestState>,
    #[cfg(feature = "events")]
    events: crate::ProcessEventBus,
}

impl TestState {
    fn new() -> Self {
        Self(Arc::new(TestStateInner {
            shutdown: CancellationToken::new(),
            registry: Registry::new(),
            #[cfg(feature = "events")]
            events: crate::ProcessEventBus::new(),
        }))
    }
}

impl SharedState for TestState {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }
}

impl HasRegistry for TestState {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }
}

#[cfg(feature = "events")]
impl crate::HasEvents for TestState {
    fn events(&self) -> &crate::ProcessEventBus {
        &self.0.events
    }
}

struct ManagedCounter {
    active: AtomicBool,
    starts: AtomicU64,
    stops: AtomicU64,
    reloads: AtomicU64,
    fail_reload: AtomicBool,
}

impl ManagedCounter {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            starts: AtomicU64::new(0),
            stops: AtomicU64::new(0),
            reloads: AtomicU64::new(0),
            fail_reload: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl Component<TestState> for ManagedCounter {
    type Deps = ();
    fn name(&self) -> &'static str {
        "counter"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<TestState>> {
        Some(self)
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<TestState>>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<TestState> for ManagedCounter {
    async fn reload(
        &self,
        _state: &TestState,
    ) -> Result<()> {
        self.reloads.fetch_add(1, Ordering::SeqCst);
        if self.fail_reload.swap(false, Ordering::SeqCst) {
            return Err(Error::msg("configured reload failure"));
        }
        Ok(())
    }
}

#[async_trait]
impl Runnable<TestState> for ManagedCounter {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        _state: TestState,
        context: RunContext,
    ) -> Result<()> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.active.store(true, Ordering::SeqCst);
        context.cancelled().await;
        self.active.store(false, Ordering::SeqCst);
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Lifecycle {
    steps: Mutex<Vec<&'static str>>,
}

impl Lifecycle {
    fn new() -> Self {
        Self { steps: Mutex::new(Vec::new()) }
    }

    fn record(
        &self,
        step: &'static str,
    ) {
        self.steps.lock().expect("steps poisoned").push(step);
    }
}

#[async_trait]
impl Component<TestState> for Lifecycle {
    type Deps = ();
    fn name(&self) -> &'static str {
        "lifecycle"
    }

    fn as_bootable(&self) -> Option<&dyn crate::Bootable<TestState>> {
        Some(self)
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<TestState>>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn crate::Finalizable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl crate::Bootable<TestState> for Lifecycle {
    async fn boot(
        &self,
        _deps: (),
        _state: &TestState,
    ) -> Result<()> {
        self.record("boot");
        Ok(())
    }
}

#[async_trait]
impl Runnable<TestState> for Lifecycle {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: TestState,
        context: RunContext,
    ) -> Result<()> {
        self.record("run");
        // The service decides the process is done, as a signal handler would.
        state.initiate_shutdown();
        context.cancelled().await;
        self.record("stop");
        Ok(())
    }
}

#[async_trait]
impl crate::Finalizable<TestState> for Lifecycle {
    async fn finalize(
        &self,
        _state: &TestState,
    ) -> Result<()> {
        self.record("finalize");
        Ok(())
    }
}

#[tokio::test]
async fn run_drives_boot_spawn_drain_and_finalize_in_order() {
    let state = TestState::new();
    let lifecycle = Arc::new(Lifecycle::new());
    state.registry_ref().register(lifecycle.clone()).unwrap();

    let finalized = Runtime::<TestState>::default().run(state).await.unwrap();

    assert_eq!(finalized.finalized_count(), 1);
    assert_eq!(
        *lifecycle.steps.lock().unwrap(),
        ["boot", "run", "stop", "finalize"]
    );
}

struct Invalid;

impl Component<TestState> for Invalid {
    type Deps = ();
    fn name(&self) -> &'static str {
        "invalid"
    }

    fn validate(
        &self,
        _: &TestState,
    ) -> Result<()> {
        Err(Error::msg("not ready"))
    }
}

#[tokio::test]
async fn run_refuses_to_start_when_validation_fails() {
    let state = TestState::new();
    state.registry_ref().register(Arc::new(Invalid)).unwrap();
    let lifecycle = Arc::new(Lifecycle::new());
    state.registry_ref().register(lifecycle.clone()).unwrap();

    let error = Runtime::<TestState>::default()
        .run(state.clone())
        .await
        .expect_err("invalid component must stop the start");

    let Error::Invalid(outcome) = error else {
        panic!("expected Error::Invalid, got {error}");
    };
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.failures()[0].component(), "invalid");
    assert!(!state.is_shutting_down(), "nothing ran, nothing shut down");
    assert_eq!(*lifecycle.steps.lock().unwrap(), ["boot", "finalize"]);
}

async fn wait_for(
    counter: &ManagedCounter,
    starts: u64,
    stops: u64,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while counter.starts.load(Ordering::SeqCst) != starts
            || counter.stops.load(Ordering::SeqCst) != stops
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed counter did not reach expected generation");
}

#[tokio::test]
async fn manager_owns_stop_start_and_restart_generations() {
    let state = TestState::new();
    let counter = Arc::new(ManagedCounter::new());
    let mut runtime = Runtime::<TestState>::default();
    let manager = runtime.manager();

    state
        .registry_ref()
        .insert(manager.clone())
        .expect("manager registration")
        .register(counter.clone())
        .expect("counter registration");
    runtime
        .spawn_all(state.registry_ref(), state.clone())
        .expect("runtime initialization");

    let runtime_state = state.clone();
    let runtime_task = tokio::spawn(async move {
        let result = runtime.wait_until_shutdown(&runtime_state).await;
        (runtime, result)
    });

    wait_for(&counter, 1, 0).await;
    let stopped = manager.stop("counter").await.expect("stop counter");
    assert_eq!(stopped.status(), ServiceStatus::Stopped);
    assert!(!counter.active.load(Ordering::SeqCst));

    let started = manager.start("counter").await.expect("start counter");
    assert_eq!(started.generation(), 2);
    wait_for(&counter, 2, 1).await;

    let restarted = manager.restart("counter").await.expect("restart counter");
    assert_eq!(restarted.generation(), 3);
    wait_for(&counter, 3, 2).await;

    let reloaded = manager.reload("counter").await.expect("reload counter");
    assert_eq!(reloaded.status(), ServiceStatus::Running);
    assert_eq!(reloaded.generation(), 3);
    assert_eq!(reloaded.reload_revision(), 1);
    assert_eq!(reloaded.last_reload_error(), None);
    assert_eq!(counter.reloads.load(Ordering::SeqCst), 1);

    counter.fail_reload.store(true, Ordering::SeqCst);
    assert!(manager.reload("counter").await.is_err());
    let snapshots =
        manager.list().await.expect("list services after failed reload");
    let failed_reload = snapshots
        .iter()
        .find(|snapshot| snapshot.name() == "counter")
        .expect("counter snapshot");
    assert_eq!(failed_reload.status(), ServiceStatus::Running);
    assert_eq!(failed_reload.reload_revision(), 2);
    assert_eq!(
        failed_reload.last_reload_error(),
        Some("reload of 'counter' failed: configured reload failure")
    );

    state.initiate_shutdown();
    let (mut runtime, result) = runtime_task.await.expect("runtime task join");
    result.expect("runtime shutdown");
    runtime.drain().await.expect("runtime drain");
    wait_for(&counter, 3, 3).await;
}

#[tokio::test]
async fn interrupted_boot_submits_one_cancelled_initial_generation() {
    let state = TestState::new();
    let counter = Arc::new(ManagedCounter::new());
    state
        .registry_ref()
        .register(counter.clone())
        .expect("counter registration");
    state.0.shutdown.cancel();

    let mut runtime = Runtime::<TestState>::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        1
    );
    runtime.drain().await.unwrap();

    assert_eq!(counter.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counter.stops.load(Ordering::SeqCst), 1);
    assert!(!counter.active.load(Ordering::SeqCst));
}

struct PolicyService {
    name: &'static str,
    policy: RwLock<ServiceStartPolicy>,
    starts: AtomicU64,
}

impl PolicyService {
    fn new(
        name: &'static str,
        policy: ServiceStartPolicy,
    ) -> Self {
        Self { name, policy: RwLock::new(policy), starts: AtomicU64::new(0) }
    }

    fn set_policy(
        &self,
        policy: ServiceStartPolicy,
    ) {
        *self.policy.write().expect("policy lock") = policy;
    }
}

#[async_trait]
impl Component<TestState> for PolicyService {
    type Deps = ();
    fn name(&self) -> &'static str {
        self.name
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<TestState>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<TestState> for PolicyService {
    fn start_policy(
        &self,
        _state: &TestState,
    ) -> ServiceStartPolicy {
        *self.policy.read().expect("policy lock")
    }

    async fn run(
        self: Arc<Self>,
        _deps: (),
        _state: TestState,
        context: RunContext,
    ) -> Result<()> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        context.cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn start_policy_controls_initial_and_explicit_generations() {
    {
        let state = TestState::new();
        let manual =
            Arc::new(PolicyService::new("manual", ServiceStartPolicy::Manual));
        let mut runtime = Runtime::<TestState>::default();
        let manager = runtime.manager();

        state
            .registry_ref()
            .insert(manager.clone())
            .expect("manager registration")
            .register(manual.clone())
            .expect("manual registration");
        assert_eq!(
            runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
            1
        );
        assert!(runtime.join_set.is_empty());
        assert_eq!(manual.starts.load(Ordering::SeqCst), 0);

        let runtime_state = state.clone();
        let runtime_task = tokio::spawn(async move {
            let result = runtime.wait_until_shutdown(&runtime_state).await;
            (runtime, result)
        });

        let snapshots = manager.list().await.expect("list manual service");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].status(), ServiceStatus::Stopped);
        assert_eq!(snapshots[0].start_policy(), ServiceStartPolicy::Manual);

        manual.set_policy(ServiceStartPolicy::Unavailable);
        assert!(matches!(
            manager.start("manual").await,
            Err(ServiceManagerError::Unavailable(name)) if name == "manual"
        ));
        manual.set_policy(ServiceStartPolicy::Manual);
        manager
            .start("manual")
            .await
            .expect("manual service starts explicitly");
        tokio::time::timeout(Duration::from_secs(1), async {
            while manual.starts.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("manual service did not start");

        state.initiate_shutdown();
        let (mut runtime, result) =
            runtime_task.await.expect("runtime task join");
        result.expect("runtime shutdown");
        runtime.drain().await.expect("runtime drain");
    }

    {
        let state = TestState::new();
        let unavailable = Arc::new(PolicyService::new(
            "unavailable",
            ServiceStartPolicy::Unavailable,
        ));
        let mut runtime = Runtime::<TestState>::default();
        let manager = runtime.manager();

        state
            .registry_ref()
            .insert(manager.clone())
            .expect("manager registration")
            .register(unavailable.clone())
            .expect("unavailable registration");
        assert_eq!(
            runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
            1
        );
        assert!(runtime.join_set.is_empty());
        assert_eq!(unavailable.starts.load(Ordering::SeqCst), 0);

        let runtime_state = state.clone();
        let runtime_task = tokio::spawn(async move {
            let result = runtime.wait_until_shutdown(&runtime_state).await;
            (runtime, result)
        });

        let snapshots = manager.list().await.expect("list unavailable service");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].status(), ServiceStatus::Stopped);
        assert_eq!(
            snapshots[0].start_policy(),
            ServiceStartPolicy::Unavailable
        );
        assert!(matches!(
            manager.start("unavailable").await,
            Err(ServiceManagerError::Unavailable(name)) if name == "unavailable"
        ));
        assert_eq!(unavailable.starts.load(Ordering::SeqCst), 0);

        state.initiate_shutdown();
        let (mut runtime, result) =
            runtime_task.await.expect("runtime task join");
        result.expect("runtime shutdown");
        runtime.drain().await.expect("runtime drain");
    }
}
