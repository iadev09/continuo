use super::super::state::AppState;
use async_trait::async_trait;
use axum::Router;
use axum::{
    Extension,
    routing::{get, post},
};
use congif::{ConfigObserver, Configurable};
use continuo::{
    Component, ComponentOrder, Error, HasRegistry, Register, Reloadable,
    Result, RunContext, Runnable,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

mod listener;
pub use listener::PeerAddress;

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    listen: SocketAddr,
    #[serde(default)]
    max_connections: Option<NonZeroUsize>,
    #[serde(
        with = "humantime_serde",
        default = "continuo::gate::default_acquire_timeout"
    )]
    acquire_timeout: Duration,
    #[serde(default, with = "humantime_serde")]
    graceful_timeout: Option<Duration>,
}

pub struct HttpService {
    config_path: PathBuf,
    reload_lock: tokio::sync::Mutex<()>,
    live: super::live::Live<HttpConfig>,
}

impl HttpService {
    fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            reload_lock: tokio::sync::Mutex::new(()),
            live: super::live::Live::new(),
        }
    }

    async fn apply_config(
        &self,
        candidate: HttpConfig,
    ) -> Result<()> {
        let _guard = self.reload_lock.lock().await;
        if self.live.is_running() {
            self.live.replace(candidate).await?;
        }
        // Stopped generations have no active resources. The reader owns the
        // accepted config; the next run consumes that snapshot.
        Ok(())
    }

    fn routes(state: &AppState) -> Result<Router<AppState>> {
        use super::super::handlers::*;
        let counter =
            state
                .registry_ref()
                .resolve::<super::CounterService>()
                .ok_or_else(|| Error::msg("http: counter not registered"))?;
        let value = state
            .registry_ref()
            .resolve::<super::super::counter::Counter>()
            .ok_or_else(|| Error::msg("http: counter value not registered"))?;
        let manager = state
            .registry_ref()
            .resolve::<continuo::ServiceManager>()
            .ok_or_else(|| {
                Error::msg("http: service manager not registered")
            })?;
        let signals = state
            .registry_ref()
            .resolve::<super::SignalService>()
            .ok_or_else(|| Error::msg("http: signals not registered"))?;
        Ok(Router::new()
            .route("/", get(index))
            .route("/config", get(read_config))
            .route("/hit", post(hit))
            .route("/counter/config", post(update_config))
            .route("/counter/events", get(counter_events))
            .route("/assets/app.css", get(styles))
            .route("/assets/app.js", get(script))
            .route("/reload", get(reload))
            .merge(service_control_routes())
            .layer(Extension(counter))
            .layer(Extension(value))
            .layer(Extension(manager))
            .layer(Extension(signals)))
    }
}

#[async_trait]
impl Component<AppState> for HttpService {
    fn name(&self) -> &'static str {
        "HttpService"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<super::CounterService>()
            .after::<super::SignalService>()
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
impl Reloadable<AppState> for HttpService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> Result<()> {
        let candidate = *state.reader().snapshot::<Self>()?;
        self.apply_config(candidate).await
    }
}

#[async_trait]
impl Runnable<AppState> for HttpService {
    async fn run(
        self: Arc<Self>,
        state: AppState,
        context: RunContext,
    ) -> Result<()> {
        let app = Self::routes(&state)?.with_state(state.clone());
        let startup = self.reload_lock.lock().await;
        let mut config = *state.reader().snapshot::<Self>()?;
        let (mut listener, gate, grace) = {
            let listener =
                Arc::new(tokio::net::TcpListener::bind(config.listen).await?);
            (
                listener,
                listener::gate(config.max_connections, config.acquire_timeout),
                config.graceful_timeout,
            )
        };
        let _generation = self.live.start();
        drop(startup);
        println!("http listening on http://{}/", listener.local_addr()?);
        let mut generations = tokio::task::JoinSet::new();
        let mut active = listener::start(
            &mut generations,
            listener.clone(),
            gate,
            grace,
            app.clone(),
            &context,
        );
        loop {
            tokio::select! {
                    result = generations.join_next(), if !generations.is_empty() => {
                        if let Some(result) = result {
                            let result = result.map_err(|error| Error::msg(error.to_string())).and_then(|result| result);
                            if let Err(error) = result {
                                active.stop();
                                while let Some(result) = generations.join_next().await {
                                    if let Err(drain_error) = result.map_err(|error| Error::msg(error.to_string())).and_then(|result| result) {
                                        tracing::error!(%drain_error, "HTTP generation drain failed");
                                    }
                                }
                                return Err(error);
                            }
                        }
                    }
                    _ = context.cancelled() => {
                        active.stop();
                        let mut failure = None;
                        while let Some(result) = generations.join_next().await {
                            if let Err(error) = result.map_err(|error| Error::msg(error.to_string())).and_then(|result| result) {
                                tracing::error!(%error, "HTTP generation drain failed");
                                failure.get_or_insert(error);
                            }
                        }
                        println!("http drained and stopped");
                        return failure.map_or(Ok(()), Err);
                    }
                    replacement = self.live.replacement() => {
                        let super::live::Replacement { value: candidate, completed } = replacement;
                        if config == candidate {
                            let _ = completed.send(Ok(()));
                            continue;
                        }
                        let replacement = if candidate.listen != config.listen {
                            match tokio::net::TcpListener::bind(candidate.listen).await {
                                Ok(listener) => Some(listener),
                                Err(error) => {
                                    let _ = completed.send(Err(error.into()));
                                    continue;
                                }
                            }
                        } else { None };
                        if let Some(replacement) = &replacement {
                            // Queue the move before cancelling old SSE streams.
                            // The replacement socket has already bound successfully.
                            continuo::HasEvents::events(&state).emit(super::super::events::ServerAddressChanged {
                                from: listener.local_addr()?,
                                to: replacement.local_addr()?,
                                graceful_timeout: active.graceful_timeout(),
                            });
                        }
                        // Stop old admission before starting the new generation.
                        // The bound socket is reused when only HTTP policy changed.
                        active.stop();
                        if let Some(replacement) = replacement {
                            listener = Arc::new(replacement);
                            println!("http: our new address is http://{}/", listener.local_addr()?);
                        }
                        active = listener::start(
                            &mut generations, listener.clone(),
                            listener::gate(candidate.max_connections, candidate.acquire_timeout),
                            candidate.graceful_timeout, app.clone(), &context,
                        );
                        config = candidate;
                        println!("http policy applied; old connections draining");
                        let _ = completed.send(Ok(()));
                    }
            }
        }
    }
}

impl Register<AppState> for HttpService {
    fn register(state: &AppState) -> Result<()> {
        state.reader().bind_environment::<Self>(
            state.environment(&state.application_config().http.env_file)?,
        )?;
        state.registry_ref().register(Arc::new(Self::new(
            state.application_config().http.config_path.clone(),
        )))?;
        Ok(())
    }
}

#[async_trait]
impl Configurable for HttpService {
    type Config = HttpConfig;
    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: self.config_path.clone(),
            format: congif::Format::Json,
        }
    }
    fn namespace(&self) -> String {
        "http".to_owned()
    }
    fn export_config(
        &self,
        config: &HttpConfig,
    ) -> congif::Result<Option<congif::ConfigValue>> {
        congif::export(config)
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = HttpConfig>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for HttpService {
    async fn config_changed(
        &self,
        config: &HttpConfig,
    ) -> congif::Result<()> {
        self.apply_config(*config)
            .await
            .map_err(|error| congif::Error::validation(error.into_std()))
    }
}

#[cfg(test)]
#[path = "../../../../tests/http/config.rs"]
mod tests;
