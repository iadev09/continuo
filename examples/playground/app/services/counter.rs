//! Playground lifecycle integration; the counter itself is an ordinary DI value.
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use congif::{ConfigObserver, Configurable};
use continuo::{
    Bootable, Component, HasRegistry, Register, Reloadable, Result, RunContext,
    Runnable,
};
use tokio::sync::watch;

use super::super::{
    config::{CounterConfig, CounterState, atomic_write},
    counter::Counter,
    state::{AppState, CounterView},
};

pub struct CounterService {
    config_path: PathBuf,
    state_path: PathBuf,
    counter: Arc<Counter>,
    updates: watch::Sender<CounterView>,
}

impl CounterService {
    fn new(
        config_path: PathBuf,
        state_path: PathBuf,
        counter: Arc<Counter>,
        updates: watch::Sender<CounterView>,
    ) -> Self {
        Self { config_path, state_path, counter, updates }
    }

    fn apply_config(
        &self,
        config: &CounterConfig,
    ) {
        self.updates.send_if_modified(|view| {
            if view.increment == config.increment {
                return false;
            }
            view.increment = config.increment;
            true
        });
    }

    async fn restore_state(&self) -> Result<()> {
        let value = match tokio::fs::read(&self.state_path).await {
            Ok(bytes) => serde_json::from_slice::<CounterState>(&bytes)?.value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        self.counter.set(value);
        self.updates.send_modify(|view| view.value = value);
        Ok(())
    }

    // Generation cleanup, not process finalization: stop and restart save too.
    async fn save_state(&self) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&CounterState {
            value: self.counter.value(),
        })?;
        let path = self.state_path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let parent = path
                .parent()
                .ok_or_else(|| std::io::Error::other("state has no parent"))?;
            std::fs::create_dir_all(parent)?;
            atomic_write(&path, &bytes)
        })
        .await??;
        Ok(())
    }
}

impl Register<AppState> for CounterService {
    fn register(state: &AppState) -> Result<()> {
        state.reader().bind_environment::<Self>(
            state.environment(&state.application_config().counter.env_file)?,
        )?;
        state.registry_ref().insert(state.counter().clone())?;
        state.registry_ref().register(Arc::new(Self::new(
            state.application_config().counter.config_path.clone(),
            super::super::config::root().join("storage/counter.json"),
            state.counter().clone(),
            state.counter_updates().clone(),
        )))?;
        Ok(())
    }
}

impl Component<AppState> for CounterService {
    fn name(&self) -> &'static str {
        "counter"
    }
    fn as_bootable(&self) -> Option<&dyn Bootable<AppState>> {
        Some(self)
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState> for CounterService {
    async fn boot(
        &self,
        _state: &AppState,
    ) -> Result<()> {
        self.restore_state().await
    }
}

#[async_trait]
impl Configurable for CounterService {
    type Config = CounterConfig;
    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: self.config_path.clone(),
            format: congif::Format::Json,
        }
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = CounterConfig>> {
        Some(self)
    }
    fn namespace(&self) -> String {
        "counter".to_owned()
    }
    async fn configure(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<()> {
        self.apply_config(config);
        Ok(())
    }
    fn export_config(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<Option<congif::ConfigValue>> {
        congif::export(config)
    }
    fn validate_config(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<()> {
        config.validate()
    }
}

#[async_trait]
impl ConfigObserver for CounterService {
    async fn config_changed(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<()> {
        self.apply_config(config);
        println!(
            "counter configuration loaded: increment={}",
            config.increment
        );
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for CounterService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        self.config_changed(state.reader().snapshot::<Self>()?.as_ref())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState> for CounterService {
    async fn run(
        self: Arc<Self>,
        _state: AppState,
        context: RunContext,
    ) -> Result<()> {
        self.updates.send_modify(|view| view.active = true);
        println!("counter started");
        context.cancelled().await;
        // The same view lock serializes admission and stop; no hit can pass this boundary.
        self.updates.send_modify(|view| view.active = false);
        self.save_state().await?;
        println!(
            "counter stopped; saved count to {}",
            self.state_path.display()
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../tests/counter/config.rs"]
mod tests;
