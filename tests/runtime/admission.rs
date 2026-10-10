use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use async_trait::async_trait;

use super::*;
use crate::{Component, Finalizable, Runnable};

// Deliberately no HasRegistry: low-level runtime management and DI remain
// independent, even when a later generation needs a newly published value.
#[derive(Clone, Default)]
struct State {
    shutdown: CancellationToken,
    #[cfg(feature = "fleet")]
    scope: crate::fleet::Scope,
}

impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    #[cfg(feature = "fleet")]
    fn process_scope(&self) -> crate::fleet::Scope {
        self.scope
    }
}

struct Dependency;

struct Consumer {
    policy: AtomicU8,
    seen: Mutex<Vec<Arc<Dependency>>>,
    finalized: AtomicUsize,
}

impl Consumer {
    fn new(policy: u8) -> Self {
        Self {
            policy: AtomicU8::new(policy),
            seen: Mutex::new(Vec::new()),
            finalized: AtomicUsize::new(0),
        }
    }
}

impl Component<State> for Consumer {
    type Deps = (Arc<Dependency>,);

    fn name(&self) -> &'static str {
        "consumer"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<State, Self::Deps>>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<State>> {
        Some(self)
    }

    #[cfg(feature = "fleet")]
    fn as_scoped(&self) -> Option<&dyn crate::fleet::Scoped> {
        Some(self)
    }
}

#[cfg(feature = "fleet")]
impl crate::fleet::Scoped for Consumer {
    fn scope(&self) -> crate::fleet::Scope {
        crate::fleet::Scope::Worker
    }
}

#[async_trait]
impl Runnable<State, (Arc<Dependency>,)> for Consumer {
    fn start_policy(
        &self,
        _: &State,
    ) -> ServiceStartPolicy {
        match self.policy.load(Ordering::SeqCst) {
            0 => ServiceStartPolicy::Unavailable,
            1 => ServiceStartPolicy::Manual,
            3 => {
                // A concurrent policy change after initial admission must not
                // change which initial generations were already admitted.
                self.policy.store(0, Ordering::SeqCst);
                ServiceStartPolicy::Automatic
            }
            _ => ServiceStartPolicy::Automatic,
        }
    }

    async fn run(
        self: Arc<Self>,
        (dependency,): (Arc<Dependency>,),
        _: State,
        _: RunContext,
    ) -> Result<()> {
        self.seen.lock().unwrap().push(dependency);
        Ok(())
    }
}

#[async_trait]
impl Finalizable<State> for Consumer {
    async fn finalize(
        &self,
        _: &State,
    ) -> Result<()> {
        self.finalized.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn dormant_dependency_is_required_only_at_activation() {
    let registry = Registry::new();
    let consumer = Arc::new(Consumer::new(0));
    registry.register(consumer.clone()).unwrap();
    let mut runtime = Runtime::default();
    assert_eq!(runtime.spawn_all(&registry, State::default()).unwrap(), 1);
    assert!(runtime.start_generation("consumer", false).is_err());

    consumer.policy.store(1, Ordering::SeqCst);
    let error = runtime.start_generation("consumer", false).unwrap_err();
    assert!(matches!(error, Error::Dependency { name: "consumer", .. }));
    let snapshot = runtime.snapshots().pop().unwrap();
    assert_eq!(snapshot.generation(), 0);
    assert_eq!(snapshot.status(), ServiceStatus::Stopped);

    let dependency = Arc::new(Dependency);
    registry.insert(dependency.clone()).unwrap();
    for generation in 1..=2 {
        let snapshot = runtime.start_generation("consumer", false).unwrap();
        assert_eq!(snapshot.generation(), generation);
        let joined = runtime.join_set.join_next_with_id().await.unwrap();
        runtime.complete_join(joined).unwrap();
    }
    let seen = consumer.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen.iter().all(|value| Arc::ptr_eq(value, &dependency)));
}

#[cfg(feature = "fleet")]
#[tokio::test]
async fn excluded_worker_does_not_require_worker_dependencies() {
    let registry = Registry::new();
    registry.register(Arc::new(Consumer::new(2))).unwrap();
    let mut runtime = Runtime::default();
    let state =
        State { scope: crate::fleet::Scope::Master, ..State::default() };
    assert_eq!(runtime.spawn_all(&registry, state).unwrap(), 0);
    assert!(runtime.join_set.is_empty());
}

struct First(AtomicUsize);

impl Component<State> for First {
    type Deps = ();
    fn name(&self) -> &'static str {
        "first"
    }
    fn run_priority(&self) -> Option<u8> {
        Some(crate::registry::priority::EARLY)
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<State> for First {
    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        _: RunContext,
    ) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn initial_dependency_failure_starts_no_partial_generation() {
    let registry = Registry::new();
    let first = Arc::new(First(AtomicUsize::new(0)));
    registry.register(first.clone()).unwrap();
    registry.register(Arc::new(Consumer::new(2))).unwrap();
    let mut runtime = Runtime::default();
    assert!(matches!(
        runtime.spawn_all(&registry, State::default()),
        Err(Error::Dependency { .. })
    ));
    assert!(runtime.join_set.is_empty());
    assert!(runtime.services.is_empty());
    assert_eq!(first.0.load(Ordering::SeqCst), 0);
    registry.insert(Arc::new(Dependency)).unwrap();
    assert_eq!(runtime.spawn_all(&registry, State::default()).unwrap(), 2);
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn initial_activation_uses_the_admission_policy_snapshot() {
    let registry = Registry::new();
    let consumer = Arc::new(Consumer::new(3));
    registry.register(consumer.clone()).unwrap();
    registry.insert(Arc::new(Dependency)).unwrap();
    let mut runtime = Runtime::default();
    assert_eq!(runtime.spawn_all(&registry, State::default()).unwrap(), 1);
    runtime.drain().await.unwrap();
    assert_eq!(consumer.seen.lock().unwrap().len(), 1);
    // Observation and subsequent explicit activation still see live policy.
    assert!(runtime.start_generation("consumer", false).is_err());
}

#[cfg(feature = "fleet")]
#[tokio::test]
async fn initial_fleet_admission_validates_once_and_restart_revalidates() {
    use crate::fleet::{
        Dispatch, DispatchRequest, Dispatched, Dispatcher, Scope, Scoped,
        ServiceExecution,
    };

    struct FleetWork(AtomicUsize);
    impl Component<State> for FleetWork {
        type Deps = ();
        fn name(&self) -> &'static str {
            "fleet-work"
        }
        fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
            Some(self)
        }
        fn as_dispatched(&self) -> Option<&dyn Dispatched> {
            Some(self)
        }
    }
    impl Scoped for FleetWork {
        fn scope(&self) -> Scope {
            Scope::Worker
        }
    }
    impl Dispatched for FleetWork {
        fn dispatch(&self) -> Dispatch {
            Dispatch::One
        }
    }
    #[async_trait]
    impl Runnable<State> for FleetWork {
        async fn run(
            self: Arc<Self>,
            _: (),
            _: State,
            _: RunContext,
        ) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct Adapter(AtomicUsize);
    #[async_trait]
    impl Dispatcher<State> for Adapter {
        fn validate(
            &self,
            _: &DispatchRequest,
            _: &State,
        ) -> Result<()> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(Error::msg("adapter policy changed"))
            }
        }
        async fn run(
            &self,
            _: DispatchRequest,
            execution: ServiceExecution<State>,
            context: RunContext,
        ) -> Result<()> {
            execution.run(context).await
        }
    }
    let registry = Registry::new();
    let work = Arc::new(FleetWork(AtomicUsize::new(0)));
    registry.register(work.clone()).unwrap();
    let adapter = Arc::new(Adapter(AtomicUsize::new(0)));
    let mut runtime =
        Runtime::default().with_dispatcher(adapter.clone()).unwrap();
    runtime.spawn_all(&registry, State::default()).unwrap();
    let joined = runtime.join_set.join_next_with_id().await.unwrap();
    runtime.complete_join(joined).unwrap();
    assert_eq!(adapter.0.load(Ordering::SeqCst), 1);
    assert_eq!(work.0.load(Ordering::SeqCst), 1);
    assert!(runtime.start_generation("fleet-work", false).is_err());
    assert_eq!(adapter.0.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.snapshots()[0].generation(), 1);
}

#[cfg(feature = "registry")]
#[tokio::test]
async fn prepared_resources_finalize_after_admission_failure() {
    use crate::HasRegistry;
    #[derive(Clone)]
    struct RegisteredState(Arc<Registry<RegisteredState>>, State);
    impl SharedState for RegisteredState {
        fn shutdown_token(&self) -> CancellationToken {
            self.1.shutdown_token()
        }
    }
    impl HasRegistry for RegisteredState {
        fn registry_ref(&self) -> &Registry<Self> {
            &self.0
        }
    }
    let state = RegisteredState(Arc::new(Registry::new()), State::default());
    // This entry uses the high-level state only to exercise run_prepared's
    // finalization boundary. No process, listener or filesystem is started.
    struct Prepared(AtomicUsize);
    impl Component<RegisteredState> for Prepared {
        type Deps = (Arc<Dependency>,);
        fn name(&self) -> &'static str {
            "prepared"
        }
        fn as_runnable(
            self: Arc<Self>
        ) -> Option<Arc<dyn Runnable<RegisteredState, Self::Deps>>> {
            Some(self)
        }
        fn as_finalizable(&self) -> Option<&dyn Finalizable<RegisteredState>> {
            Some(self)
        }
    }
    #[async_trait]
    impl Runnable<RegisteredState, (Arc<Dependency>,)> for Prepared {
        async fn run(
            self: Arc<Self>,
            _: (Arc<Dependency>,),
            _: RegisteredState,
            _: RunContext,
        ) -> Result<()> {
            unreachable!("missing dependencies reject admission")
        }
    }
    #[async_trait]
    impl Finalizable<RegisteredState> for Prepared {
        async fn finalize(
            &self,
            _: &RegisteredState,
        ) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let prepared = Arc::new(Prepared(AtomicUsize::new(0)));
    state.registry_ref().register(prepared.clone()).unwrap();
    let error = Runtime::default().run_prepared(state).await.unwrap_err();
    assert!(matches!(error, Error::Dependency { .. }));
    assert_eq!(prepared.0.load(Ordering::SeqCst), 1);
}
