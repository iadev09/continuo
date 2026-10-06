use super::super::{
    handlers::{
        counter_events, hit, index, reload, script, service_control_routes,
        styles, update_config,
    },
    state::AppState,
};
use super::{CounterService, SignalService};
use async_trait::async_trait;
use axum::routing::{get, post};
use axum::{Extension, Router};
use congif::ConfigStore;
use continuo::{
    Configurable, Error, HasRegistry, Provider, ProviderOrder, Register,
    Reloadable, Result, RunContext, Runnable, ServiceManager,
};
use serde::Deserialize;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    listen: SocketAddr,
}

pub struct HttpService {
    config_path: PathBuf,
    config: OnceLock<ConfigStore<HttpConfig>>,
    variables: congif::Variables,
    reload_lock: tokio::sync::Mutex<()>,
    live: super::live::Live<(HttpConfig, tokio::net::TcpListener)>,
}

impl HttpService {
    fn new(
        config_path: PathBuf,
        variables: congif::Variables,
    ) -> Self {
        Self {
            config_path,
            config: OnceLock::new(),
            variables,
            reload_lock: tokio::sync::Mutex::new(()),
            live: super::live::Live::new(),
        }
    }
}

#[async_trait]
impl Configurable for HttpService {
    type Config = HttpConfig;

    fn sources(&self) -> Vec<PathBuf> {
        vec![self.config_path.clone()]
    }

    async fn configure(
        &self,
        reader: &congif::ConfigReader,
    ) -> congif::Result<HttpConfig> {
        reader.read_with_variables(&self.config_path, &self.variables).await
    }
}

#[async_trait]
impl Provider<AppState> for HttpService {
    fn name(&self) -> &'static str {
        "http"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new()
            .after::<CounterService>()
            .after::<ServiceManager>()
            .after::<SignalService>()
    }

    async fn boot(
        &self,
        state: &AppState,
    ) -> Result<()> {
        state.reader().prepare_owners(&[self]).await?;
        let config = *state.reader().snapshot::<Self>()?;
        self.config.set(ConfigStore::new(config)).map_err(|_| {
            Error::msg("HTTP configuration already initialized")
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
impl Reloadable<AppState> for HttpService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let _guard = self.reload_lock.lock().await;
        let candidate = *state.reader().snapshot::<Self>()?;
        let store = self
            .config
            .get()
            .ok_or_else(|| Error::msg("HTTP configuration not prepared"))?;
        if candidate.listen == store.snapshot().listen {
            return Ok(());
        }
        if !self.live.is_running() {
            store.replace(candidate, |_| Ok(()))?;
            return Ok(());
        }
        // Binding failure leaves both active config and listener untouched.
        let listener = tokio::net::TcpListener::bind(candidate.listen).await?;
        self.live.replace((candidate, listener)).await
    }
}

#[async_trait]
impl Runnable<AppState> for HttpService {
    async fn run(
        self: Arc<Self>,
        state: AppState,
        context: RunContext,
    ) -> Result<()> {
        let counter = state
            .registry_ref()
            .resolve::<CounterService>()
            .ok_or_else(|| Error::msg("http: CounterService not registered"))?;
        let manager = state
            .registry_ref()
            .resolve::<ServiceManager>()
            .ok_or_else(|| Error::msg("http: ServiceManager not registered"))?;

        let signals = state
            .registry_ref()
            .resolve::<SignalService>()
            .ok_or_else(|| Error::msg("http: SignalService not registered"))?;
        let app = Router::new()
            .route("/", get(index))
            .route("/hit", post(hit))
            .route("/counter/config", post(update_config))
            .route("/counter/events", get(counter_events))
            .route("/assets/app.css", get(styles))
            .route("/assets/app.js", get(script))
            .route("/reload", get(reload))
            .merge(service_control_routes())
            .layer(Extension(counter))
            .layer(Extension(manager))
            .layer(Extension(signals))
            .with_state(state.clone());

        let store = self
            .config
            .get()
            .ok_or_else(|| Error::msg("HTTP configuration not prepared"))?;
        let startup = self.reload_lock.lock().await;
        let mut listener =
            tokio::net::TcpListener::bind(store.snapshot().listen).await?;
        let _generation = self.live.start();
        drop(startup);
        println!("http listening on http://{}/", listener.local_addr()?);

        loop {
            if context.is_cancelled() {
                return Ok(());
            }
            let drain = context.cancellation_token().child_token();
            let shutdown = drain.clone();
            let server = axum::serve(
                listener,
                app.clone()
                    .layer(Extension(drain.clone()))
                    .into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .into_future();
            tokio::pin!(server);
            tokio::select! {
                    result = &mut server => {
                        result?;
                        println!("http drained and stopped");
                        return Ok(());
                    }
                    _ = context.cancelled() => {
                        drain.cancel();
                        server.await?;
                        println!("http drained and stopped");
                        return Ok(());
                    }
                    replacement = self.live.replacement() => {
                        let super::live::Replacement { value: (candidate, replacement), completed } = replacement;
                        // End SSE streams through their cancellation token;
                        // ordinary accepted requests finish under graceful drain.
                        drain.cancel();
                        server.await?;
                        if context.is_cancelled() { return Ok(()); }
                        store.replace(candidate, |_| Ok(()))?;
                        println!("http: our new address is http://{}/", replacement.local_addr()?);
                        listener = replacement;
                        let _ = completed.send(Ok(()));
                    }
            }
        }
    }
}

impl Register<AppState> for HttpService {
    fn register(state: &AppState) -> Result<()> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples/playground/config/http.json");
        state
            .registry_ref()
            .insert(Arc::new(Self::new(path, state.variables().clone())))?;
        Ok(())
    }
}
