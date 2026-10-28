use super::*;
use crate::{Component, ReloadState, Reloadable, Runnable};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU8};

#[derive(Clone, Default)]
struct State(CancellationToken);

impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.clone()
    }
}

#[async_trait]
impl ReloadState for State {
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct OptionalService {
    desired: AtomicU8,
    applied: AtomicU8,
    reject: AtomicBool,
    #[cfg(feature = "fleet")]
    singleton: bool,
}

impl Component<State> for OptionalService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "optional"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<State>> {
        Some(self)
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }

    #[cfg(feature = "fleet")]
    fn as_dispatched(&self) -> Option<&dyn crate::fleet::Dispatched> {
        self.singleton.then_some(self)
    }
}

#[cfg(feature = "fleet")]
impl crate::fleet::Scoped for OptionalService {
    fn scope(&self) -> crate::fleet::Scope {
        crate::fleet::Scope::Master
    }
}

#[cfg(feature = "fleet")]
impl crate::fleet::Dispatched for OptionalService {
    fn dispatch(&self) -> crate::fleet::Dispatch {
        crate::fleet::Dispatch::One
    }
}

#[async_trait]
impl Reloadable<State> for OptionalService {
    async fn reload(
        &self,
        _: &State,
    ) -> Result<()> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(Error::msg("invalid candidate"));
        }
        self.applied
            .store(self.desired.load(Ordering::SeqCst), Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Runnable<State> for OptionalService {
    fn start_policy(
        &self,
        _: &State,
    ) -> ServiceStartPolicy {
        match self.applied.load(Ordering::SeqCst) {
            0 => ServiceStartPolicy::Unavailable,
            1 => ServiceStartPolicy::Automatic,
            _ => ServiceStartPolicy::Manual,
        }
    }

    async fn run(
        self: Arc<Self>,
        _: (),
        _: State,
        context: RunContext,
    ) -> Result<()> {
        context.cancelled().await;
        Ok(())
    }
}

fn setup() -> (Runtime<State>, Registry<State>, State, Arc<OptionalService>) {
    let state = State::default();
    let registry = Registry::new();
    let service = Arc::new(OptionalService::default());
    registry.register(service.clone()).unwrap();
    let mut runtime = Runtime::default();
    registry.insert(runtime.manager()).unwrap();
    runtime.spawn_all(&registry, state.clone()).unwrap();
    (runtime, registry, state, service)
}

async fn reload(
    runtime: &mut Runtime<State>
) -> Result<ServiceSnapshot, ServiceManagerError> {
    let (reply, result) = oneshot::channel();
    runtime
        .handle_command(ServiceCommand::Reload {
            name: "optional".into(),
            reply,
        })
        .await;
    result.await.unwrap()
}

async fn finish_generation(runtime: &mut Runtime<State>) {
    let joined = runtime.join_set.join_next_with_id().await.unwrap();
    runtime.complete_join(joined).unwrap();
}

#[tokio::test]
async fn reload_activation_starts_once_and_preserves_operator_stop() {
    let (mut runtime, _, state, service) = setup();
    assert!(runtime.join_set.is_empty());
    service.desired.store(1, Ordering::SeqCst);
    assert_eq!(reload(&mut runtime).await.unwrap().generation(), 1);
    assert_eq!(reload(&mut runtime).await.unwrap().generation(), 1);

    let (reply, result) = oneshot::channel();
    runtime
        .handle_command(ServiceCommand::Stop { name: "optional".into(), reply })
        .await;
    finish_generation(&mut runtime).await;
    assert_eq!(result.await.unwrap().unwrap().status(), ServiceStatus::Stopped);
    let snapshot = reload(&mut runtime).await.unwrap();
    assert_eq!(snapshot.status(), ServiceStatus::Stopped);
    assert_eq!(snapshot.generation(), 1);
    state.0.cancel();
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn reload_activation_waits_for_drain_before_reenable() {
    let (mut runtime, _, state, service) = setup();
    service.desired.store(1, Ordering::SeqCst);
    reload(&mut runtime).await.unwrap();
    service.desired.store(0, Ordering::SeqCst);
    assert_eq!(
        reload(&mut runtime).await.unwrap().status(),
        ServiceStatus::Stopping
    );
    // Registry reconciliation can arrive while the previous generation drains.
    service.desired.store(1, Ordering::SeqCst);
    Reloadable::reload(service.as_ref(), &state).await.unwrap();
    let snapshot = runtime.reconcile_start_policy("optional").unwrap();
    assert_eq!(snapshot.status(), ServiceStatus::Stopping);
    assert_eq!(snapshot.generation(), 1);
    finish_generation(&mut runtime).await;
    assert_eq!(runtime.services["optional"].generation, 2);
    assert_eq!(runtime.join_set.len(), 1);
    state.0.cancel();
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn reload_activation_rejects_invalid_candidates_and_keeps_manual_stopped()
{
    let (mut runtime, _, state, service) = setup();
    service.desired.store(1, Ordering::SeqCst);
    service.reject.store(true, Ordering::SeqCst);
    assert!(reload(&mut runtime).await.is_err());
    assert!(runtime.join_set.is_empty());
    service.reject.store(false, Ordering::SeqCst);
    service.desired.store(2, Ordering::SeqCst);
    assert_eq!(
        reload(&mut runtime).await.unwrap().status(),
        ServiceStatus::Stopped
    );
    assert!(runtime.join_set.is_empty());
    service.desired.store(0, Ordering::SeqCst);
    reload(&mut runtime).await.unwrap();
    service.desired.store(1, Ordering::SeqCst);
    reload(&mut runtime).await.unwrap();
    service.desired.store(0, Ordering::SeqCst);
    service.reject.store(true, Ordering::SeqCst);
    assert!(reload(&mut runtime).await.is_err());
    assert_eq!(runtime.services["optional"].status, ServiceStatus::Running);
    state.0.cancel();
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn reload_activation_registry_broadcast_and_targeted_paths_reconcile() {
    let (mut runtime, registry, state, service) = setup();
    let manager = runtime.manager();
    let drive = async {
        runtime.wait_until_shutdown(&state).await.unwrap();
        runtime.drain().await.unwrap();
    };
    let control = async {
        service.desired.store(1, Ordering::SeqCst);
        assert!(
            registry.reload_all(&state).await.unwrap().failures().is_empty()
        );
        assert_eq!(manager.list().await.unwrap()[0].generation(), 1);
        registry.reload_one("optional", &state).await.unwrap();
        assert_eq!(manager.list().await.unwrap()[0].generation(), 1);
        service.desired.store(0, Ordering::SeqCst);
        registry.reload_one("optional", &state).await.unwrap();
        manager.stop("optional").await.unwrap();
        service.desired.store(1, Ordering::SeqCst);
        registry.reload_one("optional", &state).await.unwrap();
        assert_eq!(manager.list().await.unwrap()[0].generation(), 2);
        state.0.cancel();
    };
    tokio::join!(drive, control);
}

#[cfg(feature = "fleet")]
#[tokio::test]
async fn reload_activation_preserves_dispatch_admission_and_retries_rejection()
{
    use crate::fleet::{
        Dispatch, DispatchRequest, Dispatcher, Scope, ServiceExecution,
    };

    #[derive(Default)]
    struct Adapter {
        reject: AtomicBool,
        admitted: AtomicU64,
    }

    #[async_trait]
    impl Dispatcher<State> for Adapter {
        fn validate(
            &self,
            request: &DispatchRequest,
            _: &State,
        ) -> Result<()> {
            assert_eq!(request.scope, Scope::Master);
            assert_eq!(request.dispatch, Dispatch::One);
            if self.reject.load(Ordering::SeqCst) {
                return Err(Error::msg("ownership policy rejected"));
            }
            self.admitted.fetch_add(1, Ordering::SeqCst);
            Ok(())
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

    let state = State::default();
    let registry = Registry::new();
    let service =
        Arc::new(OptionalService { singleton: true, ..Default::default() });
    registry.register(service.clone()).unwrap();
    let adapter = Arc::new(Adapter::default());
    let mut runtime =
        Runtime::default().with_dispatcher(adapter.clone()).unwrap();
    runtime.spawn_all(&registry, state.clone()).unwrap();
    assert_eq!(adapter.admitted.load(Ordering::SeqCst), 0);
    service.desired.store(1, Ordering::SeqCst);
    adapter.reject.store(true, Ordering::SeqCst);
    assert!(reload(&mut runtime).await.is_err());
    assert!(runtime.join_set.is_empty());
    assert_eq!(runtime.services["optional"].generation, 0);
    adapter.reject.store(false, Ordering::SeqCst);
    reload(&mut runtime).await.unwrap();
    reload(&mut runtime).await.unwrap();
    assert_eq!(adapter.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.join_set.len(), 1);
    service.desired.store(0, Ordering::SeqCst);
    reload(&mut runtime).await.unwrap();
    service.desired.store(1, Ordering::SeqCst);
    Reloadable::reload(service.as_ref(), &state).await.unwrap();
    runtime.reconcile_start_policy("optional").unwrap();
    adapter.reject.store(true, Ordering::SeqCst);
    finish_generation(&mut runtime).await;
    assert_eq!(runtime.services["optional"].status, ServiceStatus::Failed);
    assert!(runtime.services["optional"].last_reload_error.is_some());
    assert!(!state.is_shutting_down());
    adapter.reject.store(false, Ordering::SeqCst);
    assert_eq!(reload(&mut runtime).await.unwrap().generation(), 2);
    state.0.cancel();
    runtime.drain().await.unwrap();
}

#[tokio::test]
async fn reload_activation_explicit_stop_and_shutdown_cancel_pending_start() {
    for shutdown in [false, true] {
        let (mut runtime, _, state, service) = setup();
        service.desired.store(1, Ordering::SeqCst);
        reload(&mut runtime).await.unwrap();
        service.desired.store(0, Ordering::SeqCst);
        reload(&mut runtime).await.unwrap();
        service.desired.store(1, Ordering::SeqCst);
        Reloadable::reload(service.as_ref(), &state).await.unwrap();
        runtime.reconcile_start_policy("optional").unwrap();
        if shutdown {
            state.0.cancel();
        } else {
            let (reply, _) = oneshot::channel();
            runtime
                .handle_command(ServiceCommand::Stop {
                    name: "optional".into(),
                    reply,
                })
                .await;
        }
        finish_generation(&mut runtime).await;
        assert!(runtime.join_set.is_empty());
        assert_eq!(runtime.services["optional"].generation, 1);
        state.0.cancel();
        runtime.drain().await.unwrap();
    }
}
