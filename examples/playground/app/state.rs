//! Shared application state and its runtime capabilities.
use async_trait::async_trait;
use continuo::{
    Error, HasRegistry, Registry, ReloadState, Result, ServiceManager,
    ServiceSnapshot, SharedState,
};
use continuo::{HasEvents, ProcessEventBus};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    reader: Arc<congif::ConfigReader>,
    variables: congif::Variables,
    shutdown: CancellationToken,
    registry: Registry<AppState>,
    events: ProcessEventBus,
}

impl AppState {
    pub fn new() -> Result<Self> {
        let env_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/playground/.env");
        let variables = super::config::variables(&env_path)?;
        let mut reader = congif::ConfigReader::new();
        reader.register("json", congif::parsers::JsonParser)?;
        Ok(Self(Arc::new(Inner {
            reader: Arc::new(reader),
            variables,
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            events: ProcessEventBus::new(),
        })))
    }

    pub fn reader(&self) -> &congif::ConfigReader {
        &self.0.reader
    }

    pub fn variables(&self) -> &congif::Variables {
        &self.0.variables
    }

    /// All demo reload triggers use the runtime owner. Config preparation
    /// failures are logged before application; service revisions describe
    /// actual attempts to apply prepared settings.
    pub async fn reload_counter(&self) -> Result<ServiceSnapshot> {
        let manager = self
            .registry_ref()
            .resolve::<ServiceManager>()
            .ok_or_else(|| Error::msg("service manager not registered"))?;
        manager
            .reload("counter")
            .await
            .map_err(|error| Error::msg(error.to_string()))
    }
}

impl SharedState for AppState {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }
}

impl HasRegistry for AppState {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }
}

impl HasEvents for AppState {
    fn events(&self) -> &ProcessEventBus {
        &self.0.events
    }
}

impl continuo::HasConfigReader for AppState {
    fn config_reader(&self) -> &congif::ConfigReader {
        self.reader()
    }
}

#[async_trait]
impl ReloadState for AppState {
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}
