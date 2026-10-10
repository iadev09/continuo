use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error};

#[cfg(feature = "registry")]
use crate::registry::FinalizeOutcome;
use crate::registry::{AnyComponent, AnyRunnable, Error, Registry, Result};
use crate::service::{
    RunContext, ServiceCommand, ServiceManager, ServiceManagerError,
    ServiceSnapshot, ServiceStartPolicy, ServiceStatus,
};
use crate::state::SharedState;
#[cfg(feature = "registry")]
use crate::{HasRegistry, Register};

/// Apply one reload with the catalog bound during runtime configuration.
/// Native file watching is only an invalidation source, not a prerequisite.
#[cfg(feature = "config")]
async fn apply_configured_reload<S: 'static>(
    reader: Option<&congif::ConfigReader>,
    component: &dyn AnyComponent<S>,
    reloadable: &dyn crate::Reloadable<S>,
    state: &S,
    config_prepared: bool,
) -> Result<()> {
    let candidate =
        if let Some(owner) = component.as_configurable() {
            let reader = reader.ok_or_else(|| Error::msg(
            "configure the runtime before reloading a configurable service",
        ))?;
            if !config_prepared {
                reader.prepare_owners(&[owner]).await?;
            }
            reader.prepared_config(owner)
        } else {
            None
        };
    if config_prepared
        && let (Some(reader), Some(owner)) =
            (reader, component.as_configurable())
    {
        reader.notify_changed(owner).await?;
        return Ok(());
    }
    reloadable.reload(state).await?;
    if let (Some(reader), Some(candidate)) = (reader, candidate) {
        reader.confirm_applied(candidate).await?;
    }
    Ok(())
}

#[cfg(all(test, feature = "config"))]
#[path = "../tests/runtime/config_reload.rs"]
mod config_reload_tests;

#[cfg(all(test, feature = "registry", feature = "config-watch"))]
#[path = "../tests/runtime/preparation.rs"]
mod preparation_tests;

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
    component: Arc<dyn AnyComponent<S>>,
    runnable: Arc<dyn AnyRunnable<S>>,
    status: ServiceStatus,
    generation: u64,
    cancellation: Option<CancellationToken>,
    last_error: Option<String>,
    reload_revision: u64,
    last_reload_error: Option<String>,
    stop_waiters:
        Vec<oneshot::Sender<Result<ServiceSnapshot, ServiceManagerError>>>,
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
            reloadable: self.component.as_reloadable().is_some(),
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
    registry: Option<Registry<S>>,
    manager: Arc<ServiceManager>,
    commands: mpsc::Receiver<ServiceCommand>,
    shutting_down: bool,
    // Registrations recorded by `with_services`, applied by `register_services`.
    registrations: Vec<fn(&S) -> Result<()>>,
    // Configuration is a runtime capability, independent of native watching.
    #[cfg(feature = "config")]
    config_reader: Option<fn(&S) -> &congif::ConfigReader>,
    #[cfg(feature = "fleet")]
    dispatcher: Option<Arc<dyn crate::fleet::Dispatcher<S>>>,
    #[cfg(feature = "config-watch")]
    configuration: Option<crate::watcher::ConfigurationWatch<S>>,
}

impl<S> Default for Runtime<S> {
    fn default() -> Self {
        let (manager, commands) = ServiceManager::channel();
        Self {
            join_set: JoinSet::new(),
            task_identities: HashMap::new(),
            services: HashMap::new(),
            state: None,
            registry: None,
            manager,
            commands,
            shutting_down: false,
            registrations: Vec::new(),
            #[cfg(feature = "config")]
            config_reader: None,
            #[cfg(feature = "fleet")]
            dispatcher: None,
            #[cfg(feature = "config-watch")]
            configuration: None,
        }
    }
}

impl<S> Runtime<S> {
    /// Return the control capability backed by this runtime.
    ///
    /// `with_services` registers this capability automatically. For manual
    /// composition, register this same `Arc` before component boot.
    pub fn manager(&self) -> Arc<ServiceManager> {
        self.manager.clone()
    }
}

#[cfg(feature = "registry")]
impl<S: HasRegistry + 'static> Runtime<S> {
    /// Boot, then [`run_prepared`](Self::run_prepared).
    ///
    /// The whole lifecycle of an application without typed configuration:
    /// `boot_all`, `validate_all`, `spawn_all`, wait for shutdown, drain,
    /// `finalize_all`.
    pub async fn run(
        mut self,
        state: S,
    ) -> Result<FinalizeOutcome> {
        self.register_services(&state)?;
        state.registry_ref().boot_all(&state).await?;
        self.run_prepared(state).await
    }

    /// Run an already booted registry to completion.
    ///
    /// Validates every component and refuses to start if any fails
    /// (`Error::Invalid` carries the whole outcome). Spawns the runnables,
    /// waits until shutdown is initiated or a runnable fails fatally, drains,
    /// then finalizes. Finalization runs even after a failure, so named
    /// resources are released; the failure is then returned and finalize
    /// failures are reported through `tracing`. On success the finalize
    /// outcome is returned for the caller to inspect.
    pub async fn run_prepared(
        mut self,
        state: S,
    ) -> Result<FinalizeOutcome> {
        let admission =
            state.registry_ref().validate_all(&state).and_then(|validated| {
                if !validated.is_valid() {
                    return Err(Error::Invalid(validated));
                }
                self.spawn_all(state.registry_ref(), state.clone()).map(|_| ())
            });
        let outcome = match admission {
            Ok(()) => self.wait_until_shutdown(&state).await,
            Err(error) => {
                self.begin_shutdown();
                Err(error)
            }
        };
        let drained = self.drain().await;
        let finalized = state.registry_ref().finalize_all(&state).await;
        match outcome.and(drained) {
            Ok(()) => finalized,
            Err(failure) => {
                match finalized {
                    Ok(finalized) => {
                        for failure in finalized.failures() {
                            error!(component = failure.component(), error = %failure.error(), "finalize failed after runtime failure");
                        }
                    }
                    Err(error) => {
                        error!(%error, "finalization did not run after runtime failure");
                    }
                }
                Err(failure)
            }
        }
    }

    /// Record the application's service types. Nothing is registered yet:
    /// `register_services` applies the recorded registrations, and `run`,
    /// `serve`, `configure` and `prepare` call it first.
    pub fn with_services<R: Register<S>>(mut self) -> Self {
        self.registrations.push(R::register);
        self
    }

    /// Apply the registrations recorded by `with_services`, in order, together
    /// with this runtime's own `ServiceManager`. A manager belonging to
    /// another runtime is rejected, never replaced. As with `Register`, a
    /// failure retains the registrations already completed. Failed and pending
    /// callbacks stay queued; retrying requires the failed callback to tolerate
    /// any work it already completed, since registration is not transactional.
    pub fn register_services(
        &mut self,
        state: &S,
    ) -> Result<()> {
        let registry = state.registry_ref();
        if let Some(manager) = registry.resolve::<ServiceManager>() {
            if !Arc::ptr_eq(&manager, &self.manager) {
                return Err(Error::msg(
                    "service manager belongs to another runtime",
                ));
            }
        } else {
            registry.insert(self.manager())?;
        }
        while let Some(register) = self.registrations.first().copied() {
            register(state)?;
            self.registrations.remove(0);
        }
        Ok(())
    }
}

#[cfg(all(feature = "registry", feature = "config"))]
impl<S: HasRegistry + congif::HasConfigReader> Runtime<S> {
    /// Prepare and validate all registered config owners before resources run.
    /// Components do not need a boot hook merely to initialize configuration.
    pub async fn configure(
        &mut self,
        state: &S,
    ) -> Result<()> {
        if self.state.is_some() {
            return Err(Error::msg("configure services before spawning them"));
        }
        self.register_services(state)?;
        let components = state.registry_ref().configuration_entries()?;
        let owners: Vec<_> = components
            .iter()
            .filter_map(|(_, component)| component.as_configurable())
            .collect();
        state.config_reader().initialize_owners(&owners).await?;
        for registration in owners {
            state.config_reader().configure_registered(registration).await?;
        }
        // Programs are declared from the applied configuration, so their
        // environments are prepared once every owner has been configured.
        prepare_programs(
            state.config_reader(),
            &state.registry_ref().lifecycle_entries()?,
        )?;
        #[cfg(feature = "config-watch")]
        if let Some(watching) = self.configuration.as_mut() {
            watching.refresh_sources(state)?;
        }
        self.config_reader = Some(S::config_reader);
        Ok(())
    }

    /// Prepare configuration, then boot in dependency order before admitting
    /// runnable work. Preparation components may publish complete services.
    /// Their config owner remains the same; it is not parsed or configured a
    /// second time merely because a ready service exposes it.
    pub async fn prepare(
        &mut self,
        state: &S,
    ) -> Result<()> {
        self.register_services(state)?;
        let declared: std::collections::HashSet<_> = state
            .registry_ref()
            .configuration_entries()?
            .into_iter()
            .map(|(owner, _)| owner)
            .collect();
        self.configure(state).await?;
        state.registry_ref().boot_all(state).await?;
        for (owner, component) in
            state.registry_ref().configuration_entries()?
        {
            if !declared.contains(&owner) {
                return Err(Error::msg(format!(
                    "component '{}' published an undeclared config owner during boot; declare configuration in its preparation component",
                    component.name(),
                )));
            }
        }
        prepare_programs(
            state.config_reader(),
            &state.registry_ref().lifecycle_entries()?,
        )?;
        #[cfg(feature = "config-watch")]
        if let Some(watching) = self.configuration.as_ref() {
            self.configuration = Some(watching.rebuild(state)?);
        }
        Ok(())
    }
}

/// Register or replace the program environments of every `Environmental`
/// component among `components`, from their applied configurations.
#[cfg(all(feature = "registry", feature = "config"))]
fn prepare_programs<S: 'static>(
    reader: &congif::ConfigReader,
    components: &[(std::any::TypeId, Arc<dyn AnyComponent<S>>)],
) -> Result<()> {
    for (owner, component) in components {
        if let Some(environmental) = component.as_environmental() {
            reader.prepare_programs(
                *owner,
                environmental.program_environments(),
            )?;
        }
    }
    Ok(())
}

#[cfg(feature = "config-watch")]
impl<S: HasRegistry + crate::HasEvents + crate::HasConfigReader> Runtime<S> {
    /// The whole lifecycle of a configured, watched application:
    /// [`watch_configurations`](Self::watch_configurations),
    /// [`prepare`](Self::prepare), then [`run_prepared`](Self::run_prepared).
    pub async fn serve(
        mut self,
        state: S,
    ) -> Result<FinalizeOutcome> {
        self.watch_configurations(&state)?;
        self.prepare(&state).await?;
        self.run_prepared(state).await
    }

    /// Prepare common file watches and event subscription after registration,
    /// before boot reads config. The existing runtime control loop consumes
    /// notifications; no separate task, future or Application join is needed.
    pub fn watch_configurations(
        &mut self,
        state: &S,
    ) -> Result<()> {
        if self.state.is_some() {
            return Err(Error::msg(
                "configure watching before spawning services",
            ));
        }
        if self.configuration.is_some() {
            return Err(Error::msg("configuration watching already prepared"));
        }
        self.register_services(state)?;
        self.configuration =
            Some(crate::watcher::ConfigurationWatch::new(state)?);
        self.config_reader = Some(S::config_reader);
        Ok(())
    }
}

impl<S> Runtime<S>
where
    S: SharedState,
{
    /// Install fleet coordination before submitting any runnable services.
    /// Local management remains owned by this runtime, not by the adapter.
    #[cfg(feature = "fleet")]
    pub fn with_dispatcher(
        mut self,
        dispatcher: Arc<dyn crate::fleet::Dispatcher<S>>,
    ) -> Result<Self> {
        if self.state.is_some() {
            return Err(Error::msg(
                "install dispatcher before spawning services",
            ));
        }
        self.dispatcher = Some(dispatcher);
        Ok(self)
    }

    /// Register and start all runnable components.
    ///
    /// Runnable names must be unique because they are the stable management
    /// identity. The runtime may only be initialized once.
    pub fn spawn_all(
        &mut self,
        registry: &Registry<S>,
        state: S,
    ) -> Result<usize> {
        if self.state.is_some() {
            return Err(Error::msg(
                "runtime runnable set is already initialized",
            ));
        }
        if !self.registrations.is_empty() {
            return Err(Error::msg(
                "services were recorded after boot; register them before prepare or run",
            ));
        }

        let runnables = registry.runnable_entries()?;
        #[cfg(feature = "fleet")]
        let runnables: Vec<_> = runnables
            .into_iter()
            .filter(|entry| {
                let scope = entry
                    .component
                    .as_scoped()
                    .map(crate::fleet::Scoped::scope)
                    .or_else(|| {
                        entry
                            .component
                            .as_dispatched()
                            .map(|placement| placement.scope())
                    })
                    .unwrap_or_default();
                scope.includes(state.process_scope())
            })
            .collect();
        let mut names = HashSet::with_capacity(runnables.len());
        for entry in &runnables {
            if !names.insert(entry.name) {
                let name = entry.name;
                return Err(Error::msg(format!(
                    "duplicate runnable service name '{name}'"
                )));
            }
        }

        // Admit all automatic generations before starting any task. A missing
        // dependency must not leave earlier services partially running.
        let runnables = runnables
            .into_iter()
            .map(|entry| {
                let bound = if entry.runnable.start_policy(&state)
                    == ServiceStartPolicy::Automatic
                {
                    #[cfg(feature = "fleet")]
                    if let Some(placement) = entry.component.as_dispatched()
                        && placement.dispatch().is_controller_driven()
                    {
                        let dispatcher =
                            self.dispatcher.as_ref().ok_or_else(|| {
                                Error::msg(format!(
                                    "service '{}' requires a fleet dispatcher",
                                    entry.name
                                ))
                            })?;
                        dispatcher.validate(
                            &crate::fleet::DispatchRequest {
                                name: entry.name,
                                generation: 1,
                                scope: placement.scope(),
                                dispatch: placement.dispatch(),
                            },
                            &state,
                        )?;
                    }
                    Some(entry.runnable.clone().bind(registry)?)
                } else {
                    None
                };
                Ok((entry, bound))
            })
            .collect::<Result<Vec<_>>>()?;

        self.registry = Some(registry.shared());
        self.state = Some(state);
        for (entry, bound) in runnables {
            let name = entry.name;
            let automatic = bound.is_some();
            self.services.insert(
                name,
                ServiceEntry {
                    component: entry.component,
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
            if automatic {
                // Boot may have been interrupted after components registered work
                // but before runnable submission. Submit the initial generation
                // even with an already-cancelled parent so each runnable can
                // perform its own no-new-work/final-drain path.
                self.start_admitted_generation(name, true, bound)?;
            }
        }

        Ok(self.services.len())
    }

    fn start_generation(
        &mut self,
        name: &'static str,
        allow_cancelled_parent: bool,
    ) -> Result<ServiceSnapshot> {
        self.start_admitted_generation(name, allow_cancelled_parent, None)
    }

    fn start_admitted_generation(
        &mut self,
        name: &'static str,
        allow_cancelled_parent: bool,
        bound: Option<Arc<dyn AnyRunnable<S>>>,
    ) -> Result<ServiceSnapshot> {
        let state =
            self.state.as_ref().cloned().ok_or_else(|| {
                Error::msg("service runtime is not initialized")
            })?;
        if self.shutting_down
            || (!allow_cancelled_parent && state.is_shutting_down())
        {
            return Err(Error::msg("service runtime is shutting down"));
        }

        let service = self.services.get_mut(name).ok_or_else(|| {
            Error::msg(format!("runnable service '{name}' is not registered"))
        })?;
        // Initial automatic activation was already decided during preflight.
        // Explicit starts and restarts consult the current policy instead.
        if bound.is_none()
            && service.runnable.start_policy(&state)
                == ServiceStartPolicy::Unavailable
        {
            return Err(Error::msg(format!(
                "runnable service '{name}' is unavailable on this runtime"
            )));
        }
        let generation =
            service.generation.checked_add(1).ok_or_else(|| {
                Error::msg(format!("service '{name}' generation overflow"))
            })?;
        #[cfg(feature = "fleet")]
        let dispatch = match service.component.as_dispatched() {
            Some(placement) if placement.dispatch().is_controller_driven() => {
                let request = crate::fleet::DispatchRequest {
                    name,
                    generation,
                    scope: placement.scope(),
                    dispatch: placement.dispatch(),
                };
                let dispatcher = self.dispatcher.clone().ok_or_else(|| {
                    Error::msg(format!(
                        "service '{name}' requires a fleet dispatcher"
                    ))
                })?;
                if bound.is_none() {
                    dispatcher.validate(&request, &state)?;
                }
                Some((dispatcher, request))
            }
            _ => None,
        };
        let runnable = match bound {
            Some(runnable) => runnable,
            None => service.runnable.clone().bind(
                self.registry.as_ref().ok_or_else(|| {
                    Error::msg("service registry is not initialized")
                })?,
            )?,
        };
        service.generation = generation;
        service.status = ServiceStatus::Running;
        service.last_error = None;

        let generation = service.generation;
        let cancellation = state.shutdown_token().child_token();
        service.cancellation = Some(cancellation.clone());
        let snapshot = service.snapshot(name, &state);

        let abort = self.join_set.spawn(
            async move {
                let context = RunContext::new(cancellation);
                #[cfg(feature = "fleet")]
                let result = if let Some((dispatcher, request)) = dispatch {
                    dispatcher
                        .run(
                            request,
                            crate::fleet::ServiceExecution::new(
                                runnable, state,
                            ),
                            context,
                        )
                        .await
                } else {
                    runnable.run(state, context).await
                };
                #[cfg(not(feature = "fleet"))]
                let result = runnable.run(state, context).await;
                let result = result.map_err(|e| e.into_run(name));
                ServiceExit { name, generation, result }
            }
            .instrument(
                tracing::debug_span!("component", component = %name, generation),
            ),
        );
        self.task_identities
            .insert(abort.id(), TaskIdentity { name, generation });

        Ok(snapshot)
    }

    fn snapshots(&self) -> Vec<ServiceSnapshot> {
        let state = self
            .state
            .as_ref()
            .expect("runtime state is initialized before observation");
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
        self.handle_prepared_command(command, false).await;
    }

    async fn handle_prepared_command(
        &mut self,
        command: ServiceCommand,
        config_prepared: bool,
    ) {
        if self.shutting_down {
            Self::reject_command(command, ServiceManagerError::ShuttingDown);
            return;
        }

        // Every arm below reaches for the state, which only exists after
        // `spawn_all`. A command that arrives before it used to panic the
        // runtime task on an `expect` — an operator command racing startup
        // taking the process down. `RuntimeUnavailable` is the answer the enum
        // already has for exactly this.
        if self.state.is_none() {
            Self::reject_command(
                command,
                ServiceManagerError::RuntimeUnavailable,
            );
            return;
        }

        match command {
            ServiceCommand::List { reply } => {
                let _ = reply.send(Ok(self.snapshots()));
            }
            ServiceCommand::Start { name, reply } => {
                let Some(static_name) =
                    self.services.get_key_value(name.as_str()).map(|(n, _)| *n)
                else {
                    let _ =
                        reply.send(Err(ServiceManagerError::NotFound(name)));
                    return;
                };

                let status = self.services[static_name].status;
                let state =
                    self.state.as_ref().expect("runtime state is initialized");
                if self.services[static_name].runnable.start_policy(state)
                    == ServiceStartPolicy::Unavailable
                {
                    let _ =
                        reply.send(Err(ServiceManagerError::Unavailable(name)));
                    return;
                }
                match status {
                    ServiceStatus::Running => {
                        let _ = reply.send(Ok(self.services[static_name]
                            .snapshot(static_name, state)));
                    }
                    ServiceStatus::Stopping => {
                        let _ = reply.send(Err(ServiceManagerError::Busy {
                            name,
                            status,
                        }));
                    }
                    ServiceStatus::Stopped | ServiceStatus::Failed => {
                        let result =
                            self.start_generation(static_name, false).map_err(
                                |error| ServiceManagerError::OperationFailed {
                                    name,
                                    message: error.to_string(),
                                },
                            );
                        let _ = reply.send(result);
                    }
                }
            }
            ServiceCommand::Stop { name, reply } => {
                let Some(static_name) =
                    self.services.get_key_value(name.as_str()).map(|(n, _)| *n)
                else {
                    let _ =
                        reply.send(Err(ServiceManagerError::NotFound(name)));
                    return;
                };

                let service = self
                    .services
                    .get_mut(static_name)
                    .expect("service key just resolved");
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
                        let state = self
                            .state
                            .as_ref()
                            .expect("runtime state is initialized");
                        let _ = reply
                            .send(Ok(service.snapshot(static_name, state)));
                    }
                }
            }
            ServiceCommand::Reload { name, reply } => {
                let Some(static_name) =
                    self.services.get_key_value(name.as_str()).map(|(n, _)| *n)
                else {
                    let _ =
                        reply.send(Err(ServiceManagerError::NotFound(name)));
                    return;
                };

                let status = self.services[static_name].status;
                if status == ServiceStatus::Stopping {
                    let _ = reply
                        .send(Err(ServiceManagerError::Busy { name, status }));
                    return;
                }

                let Some(state) = self.state.as_ref().cloned() else {
                    let _ = reply
                        .send(Err(ServiceManagerError::RuntimeUnavailable));
                    return;
                };
                let component = self.services[static_name].component.clone();
                let Some(reloadable) = component.as_reloadable() else {
                    let _ = reply
                        .send(Err(ServiceManagerError::NotReloadable(name)));
                    return;
                };
                let next_revision = match self.services[static_name]
                    .reload_revision
                    .checked_add(1)
                {
                    Some(revision) => revision,
                    None => {
                        let _ = reply.send(Err(
                            ServiceManagerError::OperationFailed {
                                name,
                                message: "reload revision overflow".to_owned(),
                            },
                        ));
                        return;
                    }
                };
                #[cfg(feature = "config")]
                let reload = apply_configured_reload(
                    self.config_reader.map(|reader| reader(&state)),
                    component.as_ref(),
                    reloadable,
                    &state,
                    config_prepared,
                )
                .await;
                #[cfg(not(feature = "config"))]
                let reload = {
                    let _ = config_prepared;
                    reloadable.reload(&state).await
                };
                let reload = reload.map_err(|error: Error| {
                    error.into_reload(static_name).to_string()
                });
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
                        Err(ServiceManagerError::OperationFailed {
                            name,
                            message,
                        })
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
        #[cfg(feature = "config-watch")]
        {
            self.configuration = None;
        }
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
                debug!(
                    component = exit.name,
                    generation = exit.generation,
                    "runnable stopped"
                );
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                service.status = ServiceStatus::Failed;
                service.last_error = Some(message.clone());
                for waiter in service.stop_waiters.drain(..) {
                    let _ = waiter.send(Err(
                        ServiceManagerError::OperationFailed {
                            name: exit.name.to_owned(),
                            message: message.clone(),
                        },
                    ));
                }

                match error {
                    Error::Recoverable { name, source } => {
                        error!(component = %name, "runnable failed (runtime continuing): {}", source);
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
                if let Some(identity) =
                    self.task_identities.remove(&join_error.id())
                    && let Some(service) = self.services.get_mut(identity.name)
                    && service.generation == identity.generation
                {
                    let message = join_error.to_string();
                    service.status = ServiceStatus::Failed;
                    service.last_error = Some(message.clone());
                    for waiter in service.stop_waiters.drain(..) {
                        let _ = waiter.send(Err(
                            ServiceManagerError::OperationFailed {
                                name: identity.name.to_owned(),
                                message: message.clone(),
                            },
                        ));
                    }
                }
                Err(join_error.into())
            }
        }
    }

    /// Run until process shutdown is initiated or a critical runnable failure
    /// occurs. An empty runnable set remains alive so stopped services can be
    /// started again through the manager.
    ///
    /// A fatal failure begins this runtime's own shutdown before the error is
    /// returned: the remaining runnables are cancelled and the manager stops
    /// accepting commands. It does *not* cancel the process shutdown token —
    /// what the process does about a failed runtime is the embedder's decision,
    /// and it is holding the error.
    pub async fn wait_until_shutdown(
        &mut self,
        state: &S,
    ) -> Result<()> {
        let outcome = self.run_until_shutdown(state).await;
        if outcome.is_err() {
            // Returning without this left the siblings running and the manager
            // open, which is the opposite of what `Error::Run` documents — "the
            // runtime tears the worker down". Claviron's bootstrap happened to
            // shut down on the error; nothing made that the contract.
            self.begin_shutdown();
        }

        outcome
    }

    async fn run_until_shutdown(
        &mut self,
        state: &S,
    ) -> Result<()> {
        let shutdown = state.shutdown_token();

        loop {
            #[cfg(feature = "config-watch")]
            let changes = crate::watcher::ConfigurationWatch::next(
                &mut self.configuration,
            );
            #[cfg(not(feature = "config-watch"))]
            let changes = std::future::pending::<Vec<std::any::TypeId>>();
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
                    joined = self.join_set.join_next_with_id(), if !self.join_set.is_empty() => {
                        let Some(joined) = joined else { continue };
                        self.complete_join(joined)?;
                    }
                    owners = changes => {
                        #[cfg(feature = "config-watch")]
                        self.reload_configurations(owners).await;
                        #[cfg(not(feature = "config-watch"))]
                        let _ = owners;
                    }
            }
        }
    }

    #[cfg(feature = "config-watch")]
    async fn reload_configurations(
        &mut self,
        changes: Vec<crate::watcher::ConfigurationChanged>,
    ) {
        let Some(state) = self.state.as_ref() else { return };
        let Some(watching) = self.configuration.as_mut() else { return };
        let components = match watching.prepare(state, changes).await {
            Ok(components) => components,
            Err(error) => {
                error!(%error, "configuration preparation rejected; previous snapshots retained");
                return;
            }
        };
        for component in components {
            let name = component.name();
            let result = if component.as_reloadable().is_some()
                && self.services.get(name).is_some_and(|service| {
                    Arc::ptr_eq(&service.component, &component)
                }) {
                let (reply, received) = oneshot::channel();
                // Already inside the runtime loop: call the command handler,
                // never enqueue a manager command and await ourselves.
                self.handle_prepared_command(
                    ServiceCommand::Reload { name: name.into(), reply },
                    true,
                )
                .await;
                received
                    .await
                    .map_err(|error| Error::msg(error.to_string()))
                    .and_then(|result| {
                        result
                            .map(|_| ())
                            .map_err(|error| Error::msg(error.to_string()))
                    })
            } else {
                let Some(state) = self.state.as_ref() else { continue };
                match component.as_configurable() {
                    Some(owner) => {
                        (self.config_reader.expect("watch binds reader"))(state)
                            .notify_changed(owner)
                            .await
                            .map_err(Error::from)
                    }
                    None => continue,
                }
            };
            if let Err(error) = result {
                error!(component = name, %error, "configuration reload rejected");
                continue;
            }
            // A reloaded configuration may declare programs differently.
            if let (Some(state), Some(environmental)) =
                (self.state.as_ref(), component.as_environmental())
            {
                let reader =
                    (self.config_reader.expect("watch binds reader"))(state);
                if let Err(error) = reader.prepare_programs(
                    (*component).type_id(),
                    environmental.program_environments(),
                ) {
                    error!(component = name, %error, "program environments rejected");
                }
            }
        }
        if let (Some(state), Some(watching)) =
            (self.state.as_ref(), self.configuration.as_mut())
        {
            watching.deliver_program_changes(state);
            if let Err(error) = watching.refresh_sources(state) {
                error!(%error, "configuration dependency watch update failed");
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
#[path = "../tests/runtime/management.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/runtime/admission.rs"]
mod admission_tests;
