use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error};

use crate::registry::{Error, Provider, Registry, Result, Runnable};
use crate::service::{
    RunContext, ServiceCommand, ServiceManager, ServiceManagerError, ServiceSnapshot,
    ServiceStartPolicy, ServiceStatus,
};
use crate::state::SharedState;

struct ServiceExit {
    name: &'static str,
    generation: u64,
    result: Result<()>,
}

#[derive(Clone, Copy)]
struct TaskIdentity {
    name: &'static str,
    generation: u64,
}

struct ServiceEntry<S> {
    provider: Arc<dyn Provider<S>>,
    runnable: Arc<dyn Runnable<S>>,
    status: ServiceStatus,
    generation: u64,
    cancellation: Option<CancellationToken>,
    last_error: Option<String>,
    reload_revision: u64,
    last_reload_error: Option<String>,
    stop_waiters: Vec<oneshot::Sender<Result<ServiceSnapshot, ServiceManagerError>>>,
}

impl<S: 'static> ServiceEntry<S> {
    fn snapshot(
        &self,
        name: &'static str,
        state: &S,
    ) -> ServiceSnapshot {
        ServiceSnapshot {
            name,
            status: self.status,
            start_policy: self.runnable.start_policy(state),
            reloadable: self.provider.as_reloadable().is_some(),
            generation: self.generation,
            last_error: self.last_error.clone(),
            reload_revision: self.reload_revision,
            last_reload_error: self.last_reload_error.clone(),
        }
    }
}

/// Owns the live generations of every registered runnable service.
///
/// Tokio schedules the futures; `Runtime<S>` decides which service generation
/// exists. Its [`ServiceManager`] handle is a registered application capability
/// that sends commands back to this sole owner.
pub struct Runtime<S> {
    join_set: JoinSet<ServiceExit>,
    task_identities: HashMap<Id, TaskIdentity>,
    services: HashMap<&'static str, ServiceEntry<S>>,
    state: Option<S>,
    manager: Arc<ServiceManager>,
    commands: mpsc::Receiver<ServiceCommand>,
    shutting_down: bool,
}

impl<S> Default for Runtime<S> {
    fn default() -> Self {
        let (manager, commands) = ServiceManager::channel();
        Self {
            join_set: JoinSet::new(),
            task_identities: HashMap::new(),
            services: HashMap::new(),
            state: None,
            manager,
            commands,
            shutting_down: false,
        }
    }
}

impl<S> Runtime<S> {
    /// Return the control capability backed by this runtime.
    ///
    /// Register this same `Arc` as a provider before boot so transports and
    /// other services can resolve it without owning runtime state.
    pub fn manager(&self) -> Arc<ServiceManager> {
        self.manager.clone()
    }
}

impl<S> Runtime<S>
where
    S: SharedState,
{
    /// Register and start all runnable providers.
    ///
    /// Runnable names must be unique because they are the stable management
    /// identity. The runtime may only be initialized once.
    pub fn spawn_all(
        &mut self,
        registry: &Registry<S>,
        state: S,
    ) -> Result<usize> {
        if self.state.is_some() {
            return Err(Error::msg("runtime runnable set is already initialized"));
        }

        let runnables = registry.runnable_entries();
        let mut names = HashSet::with_capacity(runnables.len());
        for entry in &runnables {
            if !names.insert(entry.name) {
                let name = entry.name;
                return Err(Error::msg(format!("duplicate runnable service name '{name}'")));
            }
        }

        self.state = Some(state);
        for entry in runnables {
            let name = entry.name;
            let start_policy = entry
                .runnable
                .start_policy(self.state.as_ref().expect("runtime state was initialized above"));
            self.services.insert(
                name,
                ServiceEntry {
                    provider: entry.provider,
                    runnable: entry.runnable,
                    status: ServiceStatus::Stopped,
                    generation: 0,
                    cancellation: None,
                    last_error: None,
                    reload_revision: 0,
                    last_reload_error: None,
                    stop_waiters: Vec::new(),
                },
            );
            if start_policy == ServiceStartPolicy::Automatic {
                // Boot may have been interrupted after providers registered work
                // but before runnable submission. Submit the initial generation
                // even with an already-cancelled parent so each runnable can
                // perform its own no-new-work/final-drain path.
                self.start_generation(name, true)?;
            }
        }

        Ok(self.services.len())
    }

    fn start_generation(
        &mut self,
        name: &'static str,
        allow_cancelled_parent: bool,
    ) -> Result<ServiceSnapshot> {
        let state = self
            .state
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::msg("service runtime is not initialized"))?;
        if self.shutting_down || (!allow_cancelled_parent && state.is_shutting_down()) {
            return Err(Error::msg("service runtime is shutting down"));
        }

        let start_policy = self
            .services
            .get(name)
            .ok_or_else(|| Error::msg(format!("runnable service '{name}' is not registered")))?
            .runnable
            .start_policy(&state);
        if start_policy == ServiceStartPolicy::Unavailable {
            return Err(Error::msg(format!(
                "runnable service '{name}' is unavailable on this runtime"
            )));
        }

        let service = self
            .services
            .get_mut(name)
            .ok_or_else(|| Error::msg(format!("runnable service '{name}' is not registered")))?;
        service.generation = service
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::msg(format!("service '{name}' generation overflow")))?;
        service.status = ServiceStatus::Running;
        service.last_error = None;

        let generation = service.generation;
        let runnable = service.runnable.clone();
        let cancellation = state.shutdown_token().child_token();
        service.cancellation = Some(cancellation.clone());
        let snapshot = service.snapshot(name, &state);

        let abort = self.join_set.spawn(
            async move {
                let context = RunContext::new(cancellation);
                let result = runnable.run(state, context).await.map_err(|e| e.into_run(name));
                ServiceExit { name, generation, result }
            }
            .instrument(tracing::debug_span!("provider", provider = %name, generation)),
        );
        self.task_identities.insert(abort.id(), TaskIdentity { name, generation });

        Ok(snapshot)
    }

    fn snapshots(&self) -> Vec<ServiceSnapshot> {
        let state = self.state.as_ref().expect("runtime state is initialized before observation");
        let mut snapshots = self
            .services
            .iter()
            .map(|(name, service)| service.snapshot(name, state))
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| snapshot.name());
        snapshots
    }

    async fn handle_command(
        &mut self,
        command: ServiceCommand,
    ) {
        if self.shutting_down {
            Self::reject_command(command, ServiceManagerError::ShuttingDown);
            return;
        }

        match command {
            ServiceCommand::List { reply } => {
                let _ = reply.send(Ok(self.snapshots()));
            }
            ServiceCommand::Start { name, reply } => {
                let Some(static_name) = self.services.get_key_value(name.as_str()).map(|(n, _)| *n)
                else {
                    let _ = reply.send(Err(ServiceManagerError::NotFound(name)));
                    return;
                };

                let status = self.services[static_name].status;
                let state = self.state.as_ref().expect("runtime state is initialized");
                if self.services[static_name].runnable.start_policy(state)
                    == ServiceStartPolicy::Unavailable
                {
                    let _ = reply.send(Err(ServiceManagerError::Unavailable(name)));
                    return;
                }
                match status {
                    ServiceStatus::Running => {
                        let _ =
                            reply.send(Ok(self.services[static_name].snapshot(static_name, state)));
                    }
                    ServiceStatus::Stopping => {
                        let _ = reply.send(Err(ServiceManagerError::Busy { name, status }));
                    }
                    ServiceStatus::Stopped | ServiceStatus::Failed => {
                        let result = self.start_generation(static_name, false).map_err(|error| {
                            ServiceManagerError::OperationFailed {
                                name,
                                message: error.to_string(),
                            }
                        });
                        let _ = reply.send(result);
                    }
                }
            }
            ServiceCommand::Stop { name, reply } => {
                let Some(static_name) = self.services.get_key_value(name.as_str()).map(|(n, _)| *n)
                else {
                    let _ = reply.send(Err(ServiceManagerError::NotFound(name)));
                    return;
                };

                let service =
                    self.services.get_mut(static_name).expect("service key just resolved");
                match service.status {
                    ServiceStatus::Running => {
                        service.status = ServiceStatus::Stopping;
                        service.stop_waiters.push(reply);
                        if let Some(cancellation) = &service.cancellation {
                            cancellation.cancel();
                        }
                    }
                    ServiceStatus::Stopping => service.stop_waiters.push(reply),
                    ServiceStatus::Stopped | ServiceStatus::Failed => {
                        let state = self.state.as_ref().expect("runtime state is initialized");
                        let _ = reply.send(Ok(service.snapshot(static_name, state)));
                    }
                }
            }
            ServiceCommand::Reload { name, reply } => {
                let Some(static_name) = self.services.get_key_value(name.as_str()).map(|(n, _)| *n)
                else {
                    let _ = reply.send(Err(ServiceManagerError::NotFound(name)));
                    return;
                };

                let status = self.services[static_name].status;
                if status == ServiceStatus::Stopping {
                    let _ = reply.send(Err(ServiceManagerError::Busy { name, status }));
                    return;
                }

                let Some(state) = self.state.as_ref().cloned() else {
                    let _ = reply.send(Err(ServiceManagerError::RuntimeUnavailable));
                    return;
                };
                let provider = self.services[static_name].provider.clone();
                let Some(reloadable) = provider.as_reloadable() else {
                    let _ = reply.send(Err(ServiceManagerError::NotReloadable(name)));
                    return;
                };
                let next_revision = match self.services[static_name].reload_revision.checked_add(1)
                {
                    Some(revision) => revision,
                    None => {
                        let _ = reply.send(Err(ServiceManagerError::OperationFailed {
                            name,
                            message: "reload revision overflow".to_owned(),
                        }));
                        return;
                    }
                };
                let reload = reloadable
                    .reload(&state)
                    .await
                    .map_err(|error| error.into_reload(static_name).to_string());
                let service = self
                    .services
                    .get_mut(static_name)
                    .expect("service key remains registered during reload");
                service.reload_revision = next_revision;
                let result = match reload {
                    Ok(()) => {
                        service.last_reload_error = None;
                        Ok(service.snapshot(static_name, &state))
                    }
                    Err(message) => {
                        service.last_reload_error = Some(message.clone());
                        Err(ServiceManagerError::OperationFailed { name, message })
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    fn reject_command(
        command: ServiceCommand,
        error: ServiceManagerError,
    ) {
        match command {
            ServiceCommand::List { reply } => {
                let _ = reply.send(Err(error));
            }
            ServiceCommand::Start { reply, .. }
            | ServiceCommand::Stop { reply, .. }
            | ServiceCommand::Reload { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn begin_shutdown(&mut self) {
        self.shutting_down = true;
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            Self::reject_command(command, ServiceManagerError::ShuttingDown);
        }
        for service in self.services.values_mut() {
            if service.status == ServiceStatus::Running {
                service.status = ServiceStatus::Stopping;
            }
            if let Some(cancellation) = &service.cancellation {
                cancellation.cancel();
            }
        }
    }

    fn complete_service(
        &mut self,
        exit: ServiceExit,
    ) -> Result<()> {
        let state = self.state.as_ref().expect("runtime state is initialized");
        let Some(service) = self.services.get_mut(exit.name) else {
            return Err(Error::msg(format!(
                "completed runnable service '{}' is not registered",
                exit.name
            )));
        };
        if service.generation != exit.generation {
            return Err(Error::msg(format!(
                "service '{}' completed stale generation {} while generation {} is current",
                exit.name, exit.generation, service.generation
            )));
        }

        service.cancellation = None;
        match exit.result {
            Ok(()) => {
                service.status = ServiceStatus::Stopped;
                service.last_error = None;
                let snapshot = service.snapshot(exit.name, state);
                for waiter in service.stop_waiters.drain(..) {
                    let _ = waiter.send(Ok(snapshot.clone()));
                }
                debug!(provider = exit.name, generation = exit.generation, "runnable stopped");
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                service.status = ServiceStatus::Failed;
                service.last_error = Some(message.clone());
                for waiter in service.stop_waiters.drain(..) {
                    let _ = waiter.send(Err(ServiceManagerError::OperationFailed {
                        name: exit.name.to_owned(),
                        message: message.clone(),
                    }));
                }

                match error {
                    Error::Recoverable { name, source } => {
                        error!(provider = %name, "runnable failed (runtime continuing): {}", source);
                        Ok(())
                    }
                    fatal => Err(fatal),
                }
            }
        }
    }

    fn complete_join(
        &mut self,
        joined: std::result::Result<(Id, ServiceExit), JoinError>,
    ) -> Result<()> {
        match joined {
            Ok((id, exit)) => {
                self.task_identities.remove(&id);
                self.complete_service(exit)
            }
            Err(join_error) => {
                if let Some(identity) = self.task_identities.remove(&join_error.id())
                    && let Some(service) = self.services.get_mut(identity.name)
                    && service.generation == identity.generation
                {
                    let message = join_error.to_string();
                    service.status = ServiceStatus::Failed;
                    service.last_error = Some(message.clone());
                    for waiter in service.stop_waiters.drain(..) {
                        let _ = waiter.send(Err(ServiceManagerError::OperationFailed {
                            name: identity.name.to_owned(),
                            message: message.clone(),
                        }));
                    }
                }
                Err(join_error.into())
            }
        }
    }

    /// Run until process shutdown is initiated or a critical runnable failure
    /// occurs. An empty runnable set remains alive so stopped services can be
    /// started again through the manager.
    pub async fn wait_until_shutdown(
        &mut self,
        state: &S,
    ) -> Result<()> {
        let shutdown = state.shutdown_token();

        loop {
            if self.join_set.is_empty() {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        self.begin_shutdown();
                        debug!("runtime observed shutdown signal");
                        return Ok(());
                    }
                    command = self.commands.recv() => {
                        match command {
                            Some(command) => self.handle_command(command).await,
                            None => return Err(Error::msg("service manager command channel closed")),
                        }
                    }
                }
            } else {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        self.begin_shutdown();
                        debug!("runtime observed shutdown signal");
                        return Ok(());
                    }
                    command = self.commands.recv() => {
                        match command {
                            Some(command) => self.handle_command(command).await,
                            None => return Err(Error::msg("service manager command channel closed")),
                        }
                    }
                    joined = self.join_set.join_next_with_id() => {
                        let Some(joined) = joined else { continue };
                        self.complete_join(joined)?;
                    }
                }
            }
        }
    }

    /// Abort and drain all remaining runnable tasks.
    pub async fn abort_and_drain(&mut self) {
        self.begin_shutdown();
        self.join_set.abort_all();
        while let Some(joined) = self.join_set.join_next_with_id().await {
            match joined {
                Ok((id, _)) => {
                    self.task_identities.remove(&id);
                }
                Err(error) => {
                    self.task_identities.remove(&error.id());
                }
            }
        }
        debug!("runtime aborted and drained remaining runnable tasks");
    }

    /// Wait for all remaining runnable tasks to finish on their own.
    ///
    /// The global shutdown token has already cancelled every generation.
    /// Each runnable owns its protocol-level graceful drain inside its future;
    /// this layer imposes no implicit deadline.
    pub async fn drain(&mut self) -> Result<()> {
        while let Some(joined) = self.join_set.join_next_with_id().await {
            self.complete_join(joined)?;
        }
        debug!("runtime drained remaining runnable tasks");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::{Reloadable, RunContext, ServiceStartPolicy};

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

        fn registry_ref(&self) -> &Registry<Self> {
            &self.0.registry
        }

        #[cfg(feature = "events")]
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
    impl Provider<TestState> for ManagedCounter {
        fn name(&self) -> &'static str {
            "counter"
        }

        fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<TestState>>> {
            Some(self)
        }

        fn as_reloadable(&self) -> Option<&dyn Reloadable<TestState>> {
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
            .insert(counter.clone())
            .expect("counter registration");
        runtime.spawn_all(state.registry_ref(), state.clone()).expect("runtime initialization");

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
        let snapshots = manager.list().await.expect("list services after failed reload");
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
        state.registry_ref().insert(counter.clone()).expect("counter registration");
        state.0.shutdown.cancel();

        let mut runtime = Runtime::<TestState>::default();
        assert_eq!(runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(), 1);
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
    impl Provider<TestState> for PolicyService {
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
            let manual = Arc::new(PolicyService::new("manual", ServiceStartPolicy::Manual));
            let mut runtime = Runtime::<TestState>::default();
            let manager = runtime.manager();

            state
                .registry_ref()
                .insert(manager.clone())
                .expect("manager registration")
                .insert(manual.clone())
                .expect("manual registration");
            assert_eq!(runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(), 1);
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
            manager.start("manual").await.expect("manual service starts explicitly");
            tokio::time::timeout(Duration::from_secs(1), async {
                while manual.starts.load(Ordering::SeqCst) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("manual service did not start");

            state.initiate_shutdown();
            let (mut runtime, result) = runtime_task.await.expect("runtime task join");
            result.expect("runtime shutdown");
            runtime.drain().await.expect("runtime drain");
        }

        {
            let state = TestState::new();
            let unavailable =
                Arc::new(PolicyService::new("unavailable", ServiceStartPolicy::Unavailable));
            let mut runtime = Runtime::<TestState>::default();
            let manager = runtime.manager();

            state
                .registry_ref()
                .insert(manager.clone())
                .expect("manager registration")
                .insert(unavailable.clone())
                .expect("unavailable registration");
            assert_eq!(runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(), 1);
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
            assert_eq!(snapshots[0].start_policy(), ServiceStartPolicy::Unavailable);
            assert!(matches!(
                manager.start("unavailable").await,
                Err(ServiceManagerError::Unavailable(name)) if name == "unavailable"
            ));
            assert_eq!(unavailable.starts.load(Ordering::SeqCst), 0);

            state.initiate_shutdown();
            let (mut runtime, result) = runtime_task.await.expect("runtime task join");
            result.expect("runtime shutdown");
            runtime.drain().await.expect("runtime drain");
        }
    }
}
