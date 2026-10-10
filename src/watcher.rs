//! Optional config-watch integration: Congif prepares snapshots, then Runtime
//! applies them through its existing registry.
use std::any::TypeId;
use std::path::PathBuf;

use crate::{
    AnyComponent, Event, HasConfigReader, HasEvents, HasRegistry, Registry,
    Result,
};
use congif::watch::ConfigWatcher;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
/// Type-addressed config invalidation, consumed by Runtime's control loop.
pub struct ConfigurationChanged {
    owner: Option<TypeId>,
    paths: Vec<PathBuf>,
}

impl Event for ConfigurationChanged {}

impl ConfigurationChanged {
    /// Request reload through the Runtime's existing event/control loop.
    pub fn of<P: 'static>() -> Self {
        Self { owner: Some(TypeId::of::<P>()), paths: Vec::new() }
    }
}

pub(crate) struct ConfigurationWatch<S> {
    watcher: Option<ConfigWatcher>,
    changes: broadcast::Receiver<ConfigurationChanged>,
    // Capability accessors: no captured state or second component registry.
    registry: fn(&S) -> &Registry<S>,
    reader: fn(&S) -> &congif::ConfigReader,
    rebuild: fn(&S) -> Result<Self>,
}

impl<S: HasEvents + HasRegistry + HasConfigReader> ConfigurationWatch<S> {
    pub fn new(state: &S) -> Result<Self> {
        let changes = state.events().subscribe::<ConfigurationChanged>();
        let mut watcher = None;
        for (owner, component) in
            state.registry_ref().configuration_entries()?
        {
            let Some(configurable) = component.as_configurable() else {
                continue;
            };
            if !configurable.observes_config() {
                continue;
            }
            let mut sources =
                state.config_reader().sources_for(configurable)?;
            if component.as_environmental().is_some() {
                sources.extend(
                    state
                        .config_reader()
                        .program_sources((*component).type_id())?,
                );
            }
            if sources.is_empty() {
                continue;
            }
            if watcher.is_none() {
                let events = state.events().clone();
                watcher = Some(ConfigWatcher::new(move |error| {
                    tracing::error!(%error, "configuration watch failed");
                    events.emit(ConfigurationChanged {
                        owner: None,
                        paths: Vec::new(),
                    });
                })?);
            }
            let events = state.events().clone();
            watcher.as_mut().expect("watcher prepared above").register_owner(
                owner,
                sources,
                move |event| {
                    events.emit(ConfigurationChanged {
                        owner: Some(event.owner),
                        paths: event.paths,
                    })
                },
            )?;
        }
        Ok(Self {
            watcher,
            changes,
            registry: S::registry_ref,
            reader: S::config_reader,
            rebuild: Self::new,
        })
    }
}

impl<S: 'static> ConfigurationWatch<S> {
    pub fn rebuild(
        &self,
        state: &S,
    ) -> Result<Self> {
        (self.rebuild)(state)
    }

    /// Reading can add or remove dependencies even if schema validation fails.
    /// Keep the single watcher in sync so a correction triggers another try.
    pub fn refresh_sources(
        &mut self,
        state: &S,
    ) -> Result<()> {
        let Some(watcher) = self.watcher.as_mut() else { return Ok(()) };
        for (owner, component) in
            (self.registry)(state).configuration_entries()?
        {
            let Some(configurable) = component.as_configurable() else {
                continue;
            };
            if configurable.observes_config() {
                let reader = (self.reader)(state);
                let mut sources = reader.sources_for(configurable)?;
                if component.as_environmental().is_some() {
                    sources.extend(
                        reader.program_sources((*component).type_id())?,
                    );
                }
                watcher.update_owner_paths(owner, sources)?;
            }
        }
        Ok(())
    }

    /// Tell each owner which of its programs had a file re-read by the batch
    /// just prepared; the owner decides what follows.
    pub fn deliver_program_changes(
        &self,
        state: &S,
    ) {
        let changes = match (self.reader)(state).take_program_changes() {
            Ok(changes) => changes,
            Err(error) => {
                tracing::error!(%error, "program environment changes unavailable");
                return;
            }
        };
        for (owner, program, snapshot) in changes {
            let Some(component) =
                (self.registry)(state).resolve_component(owner)
            else {
                continue;
            };
            if let Some(environmental) = component.as_environmental() {
                tracing::debug!(
                    component = component.name(),
                    program,
                    "program environment re-read"
                );
                environmental.environment_changed(&program, &snapshot);
            }
        }
    }

    /// Prepare all owners affected by these files before publishing any config.
    pub async fn prepare(
        &mut self,
        state: &S,
        changes: Vec<ConfigurationChanged>,
    ) -> Result<Vec<Arc<dyn AnyComponent<S>>>> {
        let all = changes.is_empty()
            || changes.iter().any(|event| event.owner.is_none());
        let paths: Vec<_> = changes
            .iter()
            .flat_map(|event| event.paths.iter().cloned())
            .collect();
        let mut owners: Vec<_> = changes
            .iter()
            .filter(|event| event.paths.is_empty())
            .filter_map(|event| event.owner)
            .collect();
        if let Some(watcher) = &self.watcher
            && (all || !paths.is_empty())
        {
            owners.extend(watcher.owners_for_paths(if all {
                &[]
            } else {
                &paths
            }));
        }
        // An explicit invalidation may address a ready service or its wrapper.
        // Both select the same actual configuration owner.
        for owner in &mut owners {
            if let Some(component) =
                (self.registry)(state).resolve_component(*owner)
                && let Some(config) = component.as_configurable()
            {
                *owner = config.type_id();
            }
        }
        let entries: Vec<_> = (self.registry)(state)
            .configuration_entries()?
            .into_iter()
            .filter(|(owner, component)| {
                owners.contains(owner)
                    && component
                        .as_configurable()
                        .is_some_and(|owner| owner.observes_config())
            })
            .collect();
        let configs: Vec<_> = entries
            .iter()
            .map(|(_, component)| {
                component.as_configurable().expect("filtered configurable")
            })
            .collect();
        let reader = (self.reader)(state);
        let changed =
            if all || changes.iter().any(|event| event.paths.is_empty()) {
                reader.prepare_changed_owners(&configs).await
            } else {
                reader.prepare_changed_sources(&configs, &paths).await
            };
        let refreshed = self.refresh_sources(state);
        // Report the preparation error without dropping watch routing for
        // paths discovered before it. No runtime resources were applied.
        let changed = match changed {
            Ok(changed) => {
                refreshed?;
                changed
            }
            Err(error) => {
                if let Err(watch_error) = refreshed {
                    tracing::error!(%watch_error, "configuration dependency watch update failed");
                }
                return Err(error.into());
            }
        };
        tracing::debug!(
            prepared = configs.len(),
            changed = changed.len(),
            "configuration batch prepared"
        );
        Ok(entries
            .into_iter()
            .filter(|(owner, _)| changed.contains(owner))
            .map(|(_, component)| component)
            .collect())
    }

    pub async fn next(
        watching: &mut Option<Self>
    ) -> Vec<ConfigurationChanged> {
        let Some(watching) = watching else {
            return std::future::pending().await;
        };
        let first = match watching.changes.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(_)) => return Vec::new(),
            Err(broadcast::error::RecvError::Closed) => {
                return std::future::pending().await;
            }
        };
        let mut events = vec![first];
        // Consume only the queue already present. Continuous writes must not
        // prevent preparation or starve the runtime's other control commands.
        for _ in 0..watching.changes.len() {
            match watching.changes.try_recv() {
                Ok(event) => events.push(event),
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    return Vec::new();
                }
                Err(_) => return events,
            }
        }
        events
    }
}
