//! Shared application state and its runtime capabilities.
use super::counter::Counter;
use continuo::{HasEvents, ProcessEventBus};
use continuo::{HasRegistry, Registry, Result, SharedState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    reader: Arc<congif::ConfigReader>,
    environment: dotenw::EnvHandle,
    shutdown: CancellationToken,
    registry: Arc<Registry<AppState>>,
    events: ProcessEventBus,
    counter: Arc<Counter>,
    counter_updates: watch::Sender<CounterView>,
    by_client: Mutex<HashMap<SocketAddr, u64>>,
}

/// Web presentation state, not part of the counter's number.
#[derive(Clone, serde::Serialize)]
pub struct CounterView {
    pub value: u64,
    pub increment: u64,
    pub active: bool,
    pub page_revision: u64,
}

impl AppState {
    pub fn new(environments: dotenw::EnvStore) -> Result<Self> {
        let reader = congif::ConfigReader::new()
            .with_environment_store(environments)
            .with_builtin_parsers()?;
        let environment = super::config::environment(&reader, ".env", None)?;
        Ok(Self(Arc::new(Inner {
            reader: Arc::new(reader),
            environment,
            shutdown: CancellationToken::new(),
            registry: Arc::new(Registry::default()),
            events: ProcessEventBus::new(),
            counter: Arc::new(Counter::default()),
            counter_updates: watch::channel(CounterView {
                value: 0,
                increment: 0,
                active: false,
                page_revision: 0,
            })
            .0,
            by_client: Mutex::new(HashMap::new()),
        })))
    }

    pub fn reader(&self) -> &congif::ConfigReader {
        &self.0.reader
    }

    pub fn counter(&self) -> &Arc<Counter> {
        &self.0.counter
    }

    pub fn counter_updates(&self) -> &watch::Sender<CounterView> {
        &self.0.counter_updates
    }

    pub fn hit(
        &self,
        counter: &Counter,
        client: Option<SocketAddr>,
        reload_page: bool,
    ) -> Option<(u64, u64)> {
        let mut result = None;
        let mut clients = self.0.by_client.lock().expect("client map poisoned");
        self.0.counter_updates.send_if_modified(|view| {
            if !view.active {
                return false;
            }
            view.value = counter.add(view.increment);
            let yours = client.map_or(0, |client| {
                let yours = clients.entry(client).or_insert(0);
                *yours = yours.saturating_add(view.increment);
                *yours
            });
            if reload_page {
                view.page_revision = view.page_revision.wrapping_add(1);
            }
            result = Some((view.value, yours));
            true
        });
        result
    }

    pub fn request_page_reload(&self) {
        self.0.counter_updates.send_modify(|view| {
            view.page_revision = view.page_revision.wrapping_add(1);
        });
    }

    pub fn environment(
        &self,
        source: impl AsRef<std::path::Path>,
    ) -> Result<dotenw::EnvHandle> {
        super::config::environment(
            self.reader(),
            source,
            Some(&self.0.environment),
        )
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
