//! Optional config-watch integration: Congif prepares snapshots, then Runtime
//! applies them through its existing registry.
use std::any::TypeId;
use std::path::PathBuf;

use crate::{
    Event, HasConfigReader, HasEvents, HasRegistry, Provider, Registry, Result,
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
    // Capability accessors: no captured state or second provider registry.
    registry: fn(&S) -> &Registry<S>,
    reader: fn(&S) -> &congif::ConfigReader,
}

impl<S: HasEvents + HasRegistry + HasConfigReader> ConfigurationWatch<S> {
    pub fn new(state: &S) -> Result<Self> {
        let changes = state.events().subscribe::<ConfigurationChanged>();
        let mut watcher = None;
        for (owner, provider) in state.registry_ref().provider_entries() {
            let Some(configurable) = provider.as_configurable() else {
                continue;
            };
            let sources = configurable.sources();
            if sources.is_empty() {
                continue;
            }
            if watcher.is_none() {
                let events = state.events().clone();
                watcher = Some(ConfigWatcher::new(move |error| {
                    eprintln!("configuration watch failed: {error}");
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
        })
    }
}

impl<S: 'static> ConfigurationWatch<S> {
    /// Prepare all owners affected by these files before publishing any config.
    pub async fn prepare(
        &self,
        state: &S,
        changes: Vec<ConfigurationChanged>,
    ) -> Result<Vec<Arc<dyn Provider<S>>>> {
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
        let entries: Vec<_> = (self.registry)(state)
            .lifecycle_entries()?
            .into_iter()
            .filter(|(owner, provider)| {
                owners.contains(owner) && provider.as_configurable().is_some()
            })
            .collect();
        let configs: Vec<_> = entries
            .iter()
            .map(|(_, provider)| {
                provider.as_configurable().expect("filtered configurable")
            })
            .collect();
        (self.reader)(state).prepare_owners(&configs).await?;
        Ok(entries.into_iter().map(|(_, provider)| provider).collect())
    }

    pub async fn prepare_provider(
        &self,
        state: &S,
        provider: &dyn Provider<S>,
    ) -> Result<()> {
        let Some(config) = provider.as_configurable() else { return Ok(()) };
        (self.reader)(state).prepare_owners(&[config]).await?;
        Ok(())
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
