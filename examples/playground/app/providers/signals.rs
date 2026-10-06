use super::super::state::AppState;
use super::CounterService;
use async_trait::async_trait;
use congif::ConfigStore;
use continuo::{
    Configurable, Error, HasRegistry, Provider, ProviderOrder, Register,
    Reloadable, Result, RunContext, Runnable, SharedState,
};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tokio::signal::unix::Signal;

mod config;
use config::SignalConfig;

#[cfg(test)]
#[path = "../../../../tests/signals/config.rs"]
mod tests;

pub struct SignalService {
    config_path: PathBuf,
    config: OnceLock<ConfigStore<SignalConfig>>,
    reload_lock: tokio::sync::Mutex<()>,
    live: super::live::Live<(SignalConfig, Listeners)>,
}

struct Listeners {
    reload: Signal,
    hit: Signal,
}

impl Listeners {
    fn prepare(config: SignalConfig) -> std::io::Result<Self> {
        Ok(Self {
            reload: tokio::signal::unix::signal(config.reload.kind())?,
            hit: tokio::signal::unix::signal(config.hit.kind())?,
        })
    }
}

impl SignalService {
    fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            config: OnceLock::new(),
            reload_lock: tokio::sync::Mutex::new(()),
            live: super::live::Live::new(),
        }
    }

    pub fn commands(&self) -> Result<(String, String)> {
        let store = self
            .config
            .get()
            .ok_or_else(|| Error::msg("signal configuration not prepared"))?;
        Ok(store.snapshot().commands(std::process::id()))
    }
}

#[async_trait]
impl Configurable for SignalService {
    type Config = SignalConfig;
    fn sources(&self) -> Vec<PathBuf> {
        vec![self.config_path.clone()]
    }
    async fn configure(
        &self,
        reader: &congif::ConfigReader,
    ) -> congif::Result<SignalConfig> {
        reader.read(&self.config_path).await
    }
    fn validate_config(
        &self,
        config: &SignalConfig,
    ) -> congif::Result<()> {
        config.validate()
    }
}

#[async_trait]
impl Provider<AppState> for SignalService {
    fn name(&self) -> &'static str {
        "signals"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<CounterService>()
    }

    async fn boot(
        &self,
        state: &AppState,
    ) -> Result<()> {
        state.reader().prepare_owners(&[self]).await?;
        let config = *state.reader().snapshot::<Self>()?;
        self.config.set(ConfigStore::new(config)).map_err(|_| {
            Error::msg("signal configuration already initialized")
        })?;
        Ok(())
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
    fn as_configurable(&self) -> Option<&dyn congif::ConfigOwner> {
        Some(self)
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<AppState> for SignalService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let _guard = self.reload_lock.lock().await;
        let candidate = *state.reader().snapshot::<Self>()?;
        let store = self
            .config
            .get()
            .ok_or_else(|| Error::msg("signal configuration not prepared"))?;
        if candidate == *store.snapshot() {
            return Ok(());
        }
        if !self.live.is_running() {
            store.replace(candidate, |_| Ok(()))?;
            return Ok(());
        }
        let replacement = Listeners::prepare(candidate)?;
        self.live.replace((candidate, replacement)).await
    }
}

#[async_trait]
impl Runnable<AppState> for SignalService {
    async fn run(
        self: Arc<Self>,
        state: AppState,
        context: RunContext,
    ) -> Result<()> {
        let pid = std::process::id();
        let counter = state
            .registry_ref()
            .resolve::<CounterService>()
            .ok_or_else(|| Error::msg("counter service not registered"))?;
        let store = self
            .config
            .get()
            .ok_or_else(|| Error::msg("signal configuration not prepared"))?;
        let startup = self.reload_lock.lock().await;
        let config = *store.snapshot();
        let mut listeners = Listeners::prepare(config)?;
        let _generation = self.live.start();
        drop(startup);
        let (reload, hit) = config.commands(pid);
        println!(
            "signals ready: Ctrl+C to stop, `{reload}` to reload, `{hit}` to hit"
        );
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("signals: initiating process shutdown");
                    state.initiate_shutdown();
                    return Ok(());
                }
                _ = listeners.reload.recv() => {
                    continuo::HasEvents::events(&state).emit(continuo::ConfigurationChanged::of::<CounterService>());
                    println!("signals: counter reload requested");
                }
                _ = listeners.hit.recv() => {
                    match counter.hit_from_signal() {
                        Some(value) => println!("signals: hit added (count {value})"),
                        None => println!("signals: hit ignored; counter stopped"),
                    }
                }
                replacement = self.live.replacement() => {
                    let super::live::Replacement { value: (candidate, replacement), completed } = replacement;
                    listeners = replacement;
                    store.replace(candidate, |_| Ok(()))?;
                    let (reload, hit) = candidate.commands(pid);
                    println!("signals updated: `{reload}` to reload, `{hit}` to hit");
                    counter.request_page_reload();
                    let _ = completed.send(Ok(()));
                }
                _ = context.cancelled() => return Ok(()),
            }
        }
    }
}

impl Register<AppState> for SignalService {
    fn register(state: &AppState) -> Result<()> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples/playground/config/signals.json");
        state.registry_ref().insert(Arc::new(Self::new(path)))?;
        Ok(())
    }
}
