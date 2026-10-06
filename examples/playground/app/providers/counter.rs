use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use congif::{ConfigReader, ConfigSource, ConfigStore};
use continuo::{
    Configurable, Error, HasRegistry, Provider, Register, Reloadable, Result,
    RunContext, Runnable,
};
use serde::{Deserialize, Serialize};

use super::super::state::AppState;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CounterConfig {
    pub(crate) increment: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CounterState {
    value: u64,
}

// Initial value declared by this demo; saved state takes precedence.
const INITIAL_VALUE: u64 = 0;

#[derive(Clone, Serialize)]
pub struct CounterView {
    pub value: u64,
    pub increment: u64,
    pub active: bool,
    pub page_revision: u64,
}

pub struct CounterService {
    config_path: PathBuf,
    state_path: PathBuf,
    config: OnceLock<ConfigStore<CounterConfig>>,
    reload_lock: tokio::sync::Mutex<()>,
    active: AtomicBool,
    hits: AtomicU64,
    page_revision: AtomicU64,
    by_client: Mutex<HashMap<SocketAddr, u64>>,
    updates: tokio::sync::watch::Sender<CounterView>,
}

impl CounterService {
    pub(crate) fn new(
        config_path: PathBuf,
        state_path: PathBuf,
    ) -> Self {
        Self {
            config_path,
            state_path,
            config: OnceLock::new(),
            reload_lock: tokio::sync::Mutex::new(()),
            active: AtomicBool::new(false),
            hits: AtomicU64::new(INITIAL_VALUE),
            page_revision: AtomicU64::new(0),
            by_client: Mutex::new(HashMap::new()),
            updates: tokio::sync::watch::channel(CounterView {
                value: INITIAL_VALUE,
                increment: 0,
                active: false,
                page_revision: 0,
            })
            .0,
        }
    }

    async fn apply_config(
        &self,
        reader: &ConfigReader,
    ) -> Result<()> {
        let _guard = self.reload_lock.lock().await;
        let candidate = reader.snapshot::<Self>()?.as_ref().clone();
        let _clients = self.by_client.lock().expect("client map poisoned");
        if let Some(store) = self.config.get() {
            if store.snapshot().as_ref() == &candidate {
                return Ok(());
            }
            store.replace(candidate, |_| Ok(()))?;
        } else {
            self.config.set(ConfigStore::new(candidate)).map_err(|_| {
                Error::msg("counter configuration already initialized")
            })?;
        }
        self.publish();
        Ok(())
    }

    async fn restore_state(&self) -> Result<()> {
        let value = match tokio::fs::read(&self.state_path).await {
            Ok(bytes) => serde_json::from_slice::<CounterState>(&bytes)?.value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                INITIAL_VALUE
            }
            Err(error) => return Err(error.into()),
        };
        let mut clients = self.by_client.lock().expect("client map poisoned");
        self.hits.store(value, Ordering::Relaxed);
        clients.clear();
        Ok(())
    }

    async fn save_state(&self) -> Result<()> {
        let value = self.hits.load(Ordering::Relaxed);
        let bytes = serde_json::to_vec_pretty(&CounterState { value })?;
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

    fn publish(&self) {
        self.updates.send_replace(CounterView {
            value: self.hits.load(Ordering::Relaxed),
            increment: self
                .config
                .get()
                .map(|store| store.snapshot().increment)
                .unwrap_or(0),
            active: self.active.load(Ordering::Acquire),
            page_revision: self.page_revision.load(Ordering::Relaxed),
        });
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<CounterView> {
        self.updates.subscribe()
    }

    pub fn config_json(&self) -> Result<String> {
        let store = self
            .config
            .get()
            .ok_or_else(|| Error::msg("counter not configured"))?;
        Ok(serde_json::to_string_pretty(store.snapshot().as_ref())?)
    }

    /// Validate first, then atomically replace the file. Only the watcher
    /// applies the new settings; this request does not call reload.
    pub async fn update_config(
        &self,
        reader: &ConfigReader,
        json: &str,
    ) -> Result<()> {
        let candidate: CounterConfig = reader
            .parse(ConfigSource {
                path: &self.config_path,
                bytes: json.as_bytes(),
            })
            .await?;
        self.validate_config(&candidate)?;
        let bytes = serde_json::to_vec_pretty(&candidate)?;
        let path = self.config_path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            atomic_write(&path, &bytes)
        })
        .await??;
        Ok(())
    }

    pub fn hit(
        &self,
        client: SocketAddr,
    ) -> Option<(u64, u64)> {
        self.add_hit(Some(client), false)
    }

    pub fn hit_from_signal(&self) -> Option<u64> {
        self.add_hit(None, true).map(|(total, _)| total)
    }

    pub fn request_page_reload(&self) {
        let _clients = self.by_client.lock().expect("client map poisoned");
        self.page_revision.fetch_add(1, Ordering::Relaxed);
        self.publish();
    }

    fn add_hit(
        &self,
        client: Option<SocketAddr>,
        reload_page: bool,
    ) -> Option<(u64, u64)> {
        if !self.active.load(Ordering::Acquire) {
            return None;
        }
        let mut clients = self.by_client.lock().expect("client map poisoned");
        if !self.active.load(Ordering::Acquire) {
            return None;
        }
        let config = self.config.get()?.snapshot();
        let total =
            self.hits.load(Ordering::Relaxed).saturating_add(config.increment);
        self.hits.store(total, Ordering::Relaxed);
        let yours = client.map_or(0, |client| {
            let yours = clients.entry(client).or_insert(0);
            *yours = yours.saturating_add(config.increment);
            *yours
        });
        if reload_page {
            self.page_revision.fetch_add(1, Ordering::Relaxed);
        }
        self.publish();
        Some((total, yours))
    }
}

impl Register<AppState> for CounterService {
    fn register(state: &AppState) -> Result<()> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples/playground/config/counter.json");
        let state_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples/playground/storage/counter.json");
        state.registry_ref().insert(Arc::new(Self::new(path, state_path)))?;
        Ok(())
    }
}

#[async_trait]
impl Configurable for CounterService {
    type Config = CounterConfig;

    fn sources(&self) -> Vec<PathBuf> {
        vec![self.config_path.clone()]
    }

    async fn configure(
        &self,
        reader: &ConfigReader,
    ) -> congif::Result<Self::Config> {
        reader.read(&self.config_path).await
    }

    fn validate_config(
        &self,
        config: &Self::Config,
    ) -> congif::Result<()> {
        if config.increment == 0 {
            return Err(congif::Error::validation(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "counter increment must be greater than zero",
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Provider<AppState> for CounterService {
    fn name(&self) -> &'static str {
        "counter"
    }

    async fn boot(
        &self,
        state: &AppState,
    ) -> Result<()> {
        state.reader().prepare_owners(&[self]).await?;
        self.apply_config(state.reader()).await
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigOwner> {
        Some(self)
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<AppState> for CounterService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        self.apply_config(state.reader()).await?;
        println!("counter configuration loaded: {}", self.config_json()?);
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
        if self.config.get().is_none() {
            return Err(Error::msg(
                "counter configuration has not been prepared",
            ));
        }
        self.restore_state().await?;
        self.active.store(true, Ordering::Release);
        self.publish();
        println!("counter started");
        context.cancelled().await;
        // Stop admitting hits before taking the final value for persistence.
        {
            let _clients = self.by_client.lock().expect("client map poisoned");
            self.active.store(false, Ordering::Release);
        }
        self.publish();
        self.save_state().await?;
        println!(
            "counter stopped; saved count to {}",
            self.state_path.display()
        );
        Ok(())
    }
}

fn atomic_write(
    path: &std::path::Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("file has no parent"))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
#[path = "../../../../tests/counter/config.rs"]
mod tests;
