use continuo::{
    HasConfigReader, HasEvents, HasRegistry, ProcessEventBus, Registry, Result,
    SharedState,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct State(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<State>,
    reader: congif::ConfigReader,
    events: ProcessEventBus,
}

impl State {
    pub fn new() -> Result<Self> {
        Ok(Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::new(),
            reader: congif::ConfigReader::new().with_builtin_parsers()?,
            events: ProcessEventBus::new(),
        })))
    }
}

impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }
}

impl HasRegistry for State {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }
}

impl HasConfigReader for State {
    fn config_reader(&self) -> &congif::ConfigReader {
        &self.0.reader
    }
}

impl HasEvents for State {
    fn events(&self) -> &ProcessEventBus {
        &self.0.events
    }
}
