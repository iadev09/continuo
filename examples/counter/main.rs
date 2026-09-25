//! Local service-management harness.
//!
//! `CounterService` is both a typed application capability and a managed
//! `Runnable`. Every new run generation resets its application-scoped state.
//! The HTTP service resolves the runtime's registered `ServiceManager` once
//! and exposes a small control router for listing, stopping, starting, and
//! restarting the counter.
//!
//! Run with:  cargo run --example counter
//! Open:      http://127.0.0.1:3000/hit

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{Extension, Router};
#[cfg(feature = "events")]
use continuo::{ProcessEventBus,HasEvents};
use continuo::{Error, HasRegistry, Provider, ProviderOrder, Registry, ReloadState, Reloadable, Result, RunContext, Runnable, Runtime, ServiceManager, ServiceSnapshot, SharedState};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct AppState(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<AppState>,
    #[cfg(feature = "events")]
    events: ProcessEventBus,
}

impl AppState {
    fn new() -> Self {
        Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            #[cfg(feature = "events")]
            events: ProcessEventBus::new(),
        }))
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

#[cfg(feature = "events")]
impl HasEvents for AppState {
    fn events(&self) -> &ProcessEventBus {
        &self.0.events
    }
}

#[async_trait]
impl ReloadState for AppState {
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------
// CounterService — one typed instance, one runtime-owned live generation.
// ---------------------------------------------------------------------

struct CounterService {
    active: AtomicBool,
    hits: AtomicU64,
    by_client: Mutex<HashMap<SocketAddr, u64>>,
}

impl CounterService {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            hits: AtomicU64::new(0),
            by_client: Mutex::new(HashMap::new()),
        }
    }

    fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.by_client.lock().expect("client map poisoned").clear();
    }

    /// Returns `None` while this provider has no running service generation.
    fn hit(
        &self,
        client: SocketAddr,
    ) -> Option<(u64, u64)> {
        if !self.active.load(Ordering::Acquire) {
            return None;
        }

        let total = self.hits.fetch_add(1, Ordering::Relaxed) + 1;
        let mut clients = self.by_client.lock().expect("client map poisoned");
        let yours = clients.entry(client).or_insert(0);
        *yours += 1;
        Some((total, *yours))
    }
}

#[async_trait]
impl Provider<AppState> for CounterService {
    fn name(&self) -> &'static str {
        "counter"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
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
        _state: &AppState,
    ) -> Result<()> {
        self.reset();
        println!("counter reloaded: reset to 0");
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
        // A new runtime generation is a new counter lifetime.
        self.reset();
        self.active.store(true, Ordering::Release);
        println!("counter started: reset to 0");

        context.cancelled().await;

        self.active.store(false, Ordering::Release);
        println!("counter stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------
// HTTP handlers and the separate local management router.
// ---------------------------------------------------------------------

async fn hit(
    ConnectInfo(client): ConnectInfo<SocketAddr>,
    Extension(counter): Extension<Arc<CounterService>>,
    Extension(manager): Extension<Arc<ServiceManager>>,
) -> Html<String> {
    let hit = match counter.hit(client) {
        Some((total, yours)) => {
            println!("http: hit #{total} (client {} -> its #{yours})", client.port());
            format!("<h1>hit #{total}</h1><p>Your connection is on hit #{yours}.</p>")
        }
        None => "<h1>counter stopped</h1><p>Start it to accept hits again.</p>".to_owned(),
    };
    let status = manager
        .list()
        .await
        .ok()
        .and_then(|services| services.into_iter().find(|service| service.name() == "counter"))
        .map(|service| format_snapshot(&service))
        .unwrap_or_else(|| "counter status unavailable".to_owned());

    Html(format!(
        "{hit}<p><code>{status}</code></p>{}<p><a href=\"/services\">all services</a> | \
         <a href=\"/reload\">reload providers</a></p>",
        counter_controls()
    ))
}

async fn list_services(Extension(manager): Extension<Arc<ServiceManager>>) -> impl IntoResponse {
    match manager.list().await {
        Ok(services) => {
            let rows = services
                .iter()
                .map(|service| format!("<li><code>{}</code></li>", format_snapshot(service)))
                .collect::<String>();
            (
                StatusCode::OK,
                Html(format!(
                    "<h1>Runnable services</h1><ul>{rows}</ul>{}<p><a href=\"/hit\">counter</a></p>",
                    counter_controls()
                )),
            )
        }
        Err(error) => control_error(error.to_string()),
    }
}

async fn start_counter(Extension(manager): Extension<Arc<ServiceManager>>) -> impl IntoResponse {
    control_result("start", manager.start("counter").await)
}

async fn stop_counter(Extension(manager): Extension<Arc<ServiceManager>>) -> impl IntoResponse {
    control_result("stop", manager.stop("counter").await)
}

async fn restart_counter(Extension(manager): Extension<Arc<ServiceManager>>) -> impl IntoResponse {
    control_result("restart", manager.restart("counter").await)
}

async fn reload_counter(Extension(manager): Extension<Arc<ServiceManager>>) -> impl IntoResponse {
    control_result("reload", manager.reload("counter").await)
}

fn service_control_routes() -> Router<AppState> {
    Router::new()
        .route("/services", get(list_services))
        .route("/services/counter/start", post(start_counter))
        .route("/services/counter/stop", post(stop_counter))
        .route("/services/counter/restart", post(restart_counter))
        .route("/services/counter/reload", post(reload_counter))
}

fn counter_controls() -> &'static str {
    "<div style=\"display:flex;gap:.5rem\">\
       <form method=\"post\" action=\"/services/counter/start\"><button>start</button></form>\
       <form method=\"post\" action=\"/services/counter/stop\"><button>stop</button></form>\
       <form method=\"post\" action=\"/services/counter/restart\"><button>restart</button></form>\
       <form method=\"post\" action=\"/services/counter/reload\"><button>reload</button></form>\
     </div>"
}

fn format_snapshot(service: &ServiceSnapshot) -> String {
    let error =
        service.last_error().map(|message| format!(" | last error={message}")).unwrap_or_default();
    let reload_error = service
        .last_reload_error()
        .map(|message| format!(" | last reload error={message}"))
        .unwrap_or_default();
    format!(
        "{} | {} | reloadable={} | generation={} | reload-revision={}{}{}",
        service.name(),
        service.status(),
        service.is_reloadable(),
        service.generation(),
        service.reload_revision(),
        error,
        reload_error
    )
}

fn control_result(
    action: &str,
    result: std::result::Result<ServiceSnapshot, continuo::ServiceManagerError>,
) -> (StatusCode, Html<String>) {
    match result {
        Ok(service) => (
            StatusCode::OK,
            Html(format!(
                "<h1>{action} completed</h1><p><code>{}</code></p><p><a href=\"/hit\">back</a></p>",
                format_snapshot(&service)
            )),
        ),
        Err(error) => control_error(error.to_string()),
    }
}

fn control_error(message: String) -> (StatusCode, Html<String>) {
    (
        StatusCode::CONFLICT,
        Html(format!(
            "<h1>service command failed</h1><p>{message}</p><p><a href=\"/hit\">back</a></p>"
        )),
    )
}

async fn reload(State(app): State<AppState>) -> Redirect {
    match app.registry_ref().reload_all(&app).await {
        Ok(outcome) if outcome.is_complete() => {
            println!("http: /reload - {} provider(s) reloaded", outcome.reloaded_count())
        }
        Ok(outcome) => println!(
            "http: /reload - {} provider(s) reloaded, {} failed",
            outcome.reloaded_count(),
            outcome.failed_count()
        ),
        Err(error) => println!("http: /reload failed: {error}"),
    }
    Redirect::to("/hit")
}

// ---------------------------------------------------------------------
// HTTP and signal services are ordinary managed runnables too.
// ---------------------------------------------------------------------

struct HttpService {
    addr: SocketAddr,
}

impl HttpService {
    fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

#[async_trait]
impl Provider<AppState> for HttpService {
    fn name(&self) -> &'static str {
        "http"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<CounterService>().after::<ServiceManager>()
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
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

        let app = Router::new()
            .route("/hit", get(hit))
            .route("/reload", get(reload))
            .merge(service_control_routes())
            .layer(Extension(counter))
            .layer(Extension(manager))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        println!("http listening on http://{}/hit", self.addr);

        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move { context.cancelled().await })
            .await?;

        println!("http drained and stopped");
        Ok(())
    }
}

struct SignalService;

#[async_trait]
impl Provider<AppState> for SignalService {
    fn name(&self) -> &'static str {
        "signals"
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
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
        println!("signals ready: Ctrl+C to stop, `kill -HUP {pid}` to reload");
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("signals: initiating process shutdown");
                    state.initiate_shutdown();
                    return Ok(());
                }
                _ = hangup.recv() => {
                    let outcome = state.registry_ref().reload_all(&state).await?;
                    println!(
                        "signals: reload completed ({} reloaded, {} failed)",
                        outcome.reloaded_count(),
                        outcome.failed_count()
                    );
                }
                _ = context.cancelled() => return Ok(()),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let state = AppState::new();
    let mut runtime = Runtime::<AppState>::default();
    let registry = state.registry_ref();

    registry
        .insert(runtime.manager())?
        .insert(Arc::new(CounterService::new()))?
        .insert(Arc::new(HttpService::new(SocketAddr::from(([127, 0, 0, 1], 3000)))))?
        .insert(Arc::new(SignalService))?;

    registry.boot_all(&state).await?;
    let validation = registry.validate_all(&state)?;
    for failure in validation.failures() {
        eprintln!("validation failed for {}: {}", failure.provider(), failure.error());
    }
    if !validation.is_valid() {
        return Err(Error::msg(format!(
            "{} provider validation(s) failed",
            validation.failed_count()
        )));
    }

    runtime.spawn_all(registry, state.clone())?;
    runtime.wait_until_shutdown(&state).await?;
    runtime.drain().await?;

    let finalized = registry.finalize_all(&state).await?;
    for failure in finalized.failures() {
        eprintln!("finalize failed for {}: {}", failure.provider(), failure.error());
    }

    println!("all services finished gracefully");
    Ok(())
}
