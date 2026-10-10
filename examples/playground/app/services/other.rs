//! A second config owner observing the same source, with no artificial task.
use super::super::{config::CounterConfig, state::AppState};
use async_trait::async_trait;
use congif::{ConfigObserver, Configurable};
use continuo::{Component, HasRegistry, Register, Reloadable, Result};
use std::path::PathBuf;
use std::sync::Arc;

pub struct OtherService {
    config_path: PathBuf,
}

impl OtherService {
    fn report(config: &CounterConfig) {
        println!("other service reader: increment={}", config.increment);
    }
}

impl Register<AppState> for OtherService {
    fn register(state: &AppState) -> Result<()> {
        state.reader().bind_environment::<Self>(
            state.environment(&state.application_config().counter.env_file)?,
        )?;
        state.registry_ref().register(Arc::new(Self {
            config_path: state.application_config().counter.config_path.clone(),
        }))?;
        Ok(())
    }
}

impl Component<AppState> for OtherService {
    fn name(&self) -> &'static str {
        "DuplicateReaderService"
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Configurable for OtherService {
    type Config = CounterConfig;
    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: self.config_path.clone(),
            format: congif::Format::Json,
        }
    }
    fn namespace(&self) -> String {
        "other".to_owned()
    }
    fn validate_config(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<()> {
        config.validate()
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = CounterConfig>> {
        Some(self)
    }
    async fn configure(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<()> {
        Self::report(config);
        Ok(())
    }
}

#[async_trait]
impl ConfigObserver for OtherService {
    async fn config_changed(
        &self,
        config: &CounterConfig,
    ) -> congif::Result<()> {
        Self::report(config);
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for OtherService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        Self::report(state.reader().snapshot::<Self>()?.as_ref());
        Ok(())
    }
}
