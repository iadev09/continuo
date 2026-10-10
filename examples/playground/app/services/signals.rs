use super::super::counter::Counter;
use super::super::state::AppState;
use super::CounterService;
use async_trait::async_trait;
use congif::{ConfigObserver, ConfigReader, Configurable};
use continuo::{
    Component, ComponentOrder, HasRegistry, Register, Reloadable, Result,
    RunContext, Runnable, SharedState,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal::unix::Signal;

mod config;
use config::SignalConfig;

#[cfg(test)]
#[path = "../../../../tests/signals/config.rs"]
mod tests;

pub struct SignalService {
    config_path: PathBuf,
    reload_lock: tokio::sync::Mutex<()>,
    live: super::live::Live<SignalConfig>,
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
            reload_lock: tokio::sync::Mutex::new(()),
            live: super::live::Live::new(),
        }
    }

    pub fn commands(
        &self,
        reader: &ConfigReader,
    ) -> congif::Result<(String, String)> {
        Ok(reader.snapshot::<Self>()?.commands(std::process::id()))
    }

    async fn apply_config(
        &self,
        candidate: SignalConfig,
    ) -> Result<()> {
        let _guard = self.reload_lock.lock().await;
        if self.live.is_running() {
            self.live.replace(candidate).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for SignalService {
    fn name(&self) -> &'static str {
        "signals"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<CounterService>()
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
impl Reloadable<AppState> for SignalService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let candidate = *state.reader().snapshot::<Self>()?;
        self.apply_config(candidate).await
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
        let counter =
            state.registry_ref().resolve::<Counter>().ok_or_else(|| {
                continuo::Error::msg("signals: counter not registered")
            })?;
        let startup = self.reload_lock.lock().await;
        let mut config = *state.reader().snapshot::<Self>()?;
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
                    continuo::HasEvents::events(&state).emit(continuo::ReloadRequired);
                    println!("signals: application reload requested");
                }
                _ = listeners.hit.recv() => {
                    match state.hit(&counter, None, true).map(|(value, _)| value) {
                        Some(value) => println!("signals: hit added (count {value})"),
                        None => println!("signals: hit ignored; counter stopped"),
                    }
                }
                replacement = self.live.replacement() => {
                    let super::live::Replacement { value: candidate, completed } = replacement;
                    if config == candidate {
                        let _ = completed.send(Ok(()));
                        continue;
                    }
                    match Listeners::prepare(candidate) {
                        Ok(replacement) => listeners = replacement,
                        Err(error) => {
                            let _ = completed.send(Err(error.into()));
                            continue;
                        }
                    }
                    config = candidate;
                    let (reload, hit) = candidate.commands(pid);
                    println!("signals updated: `{reload}` to reload, `{hit}` to hit");
                    state.request_page_reload();
                    let _ = completed.send(Ok(()));
                }
                _ = context.cancelled() => return Ok(()),
            }
        }
    }
}

impl Register<AppState> for SignalService {
    fn register(state: &AppState) -> Result<()> {
        state.registry_ref().register(Arc::new(Self::new(
            super::super::config::root().join("config/signals.json"),
        )))?;
        Ok(())
    }
}

#[async_trait]
impl Configurable for SignalService {
    type Config = SignalConfig;
    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: self.config_path.clone(),
            format: congif::Format::Json,
        }
    }
    fn namespace(&self) -> String {
        "signals".to_owned()
    }
    fn validate_config(
        &self,
        config: &SignalConfig,
    ) -> congif::Result<()> {
        config.validate()
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = SignalConfig>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for SignalService {
    async fn config_changed(
        &self,
        config: &SignalConfig,
    ) -> congif::Result<()> {
        self.apply_config(*config)
            .await
            .map_err(|error| congif::Error::validation(error.into_std()))
    }
}
