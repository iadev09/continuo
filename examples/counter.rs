//! An HTTP service using an application-scoped service — no external
//! dependencies, just what this crate solves.
//!
//! `CounterService` is application-scoped: one instance for the whole
//! application lifetime, shared by every request — a plain `Arc`'d
//! service, no external store needed.
//!
//! The DI cost model is the point: the service is resolved from the
//! registry ONCE, while the router is built — not per request. The
//! service travels to handlers as an axum `Extension`; the router state
//! is the `AppState` itself, so handlers can also reach the registry.
//!
//! The HTTP server itself is an ordinary lifecycle citizen: it boots
//! after its dependency, serves inside `Runnable::run()`, drains
//! gracefully, and `CounterService` persists its final value to
//! `counter.txt` in `Finalizable::finalize()` after the runnables are
//! done — and restores it in `boot()` on the next start (`tokio::fs`;
//! boot is async for exactly this). The count survives restarts.
//!
//! Even signal handling is a service (`SignalService`) — `main`
//! contains no naked `tokio::spawn`; it only composes providers and
//! walks the lifecycle. Ctrl+C initiates shutdown; SIGHUP reloads the
//! reloadable providers (the counter resets to 0).
//!
//! Run with:  cargo run --example counter
//! Then:      open http://127.0.0.1:3000/hit      (refresh — the count
//!            grows; the "reset" link reloads the providers)
//! Reload:    kill -HUP <pid>                     (the count starts over)
//!    or:     curl -L http://127.0.0.1:3000/reload (same reload, over HTTP)
//! Stop with Ctrl+C and watch the graceful drain + finalize; run again
//! and the count picks up where it left off.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{ConnectInfo, State};
use axum::response::{Html, Redirect};
use axum::routing::get;
use axum::{Extension, Router};
#[cfg(feature = "events")]
use continuo::LifecycleBus;
use continuo::{
    Error, Finalizable, Provider, ProviderOrder, Registry, ReloadState, Reloadable, Result,
    Runnable, Runtime, SharedState,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct AppState(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<AppState>,
    #[cfg(feature = "events")]
    events: LifecycleBus,
}

impl AppState {
    fn new() -> Self {
        Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            #[cfg(feature = "events")]
            events: LifecycleBus::new(),
        }))
    }
}

impl SharedState for AppState {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }

    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }

    #[cfg(feature = "events")]
    fn events(&self) -> &LifecycleBus {
        &self.0.events
    }
}

#[async_trait]
impl ReloadState for AppState {
    /// State-level reload hook; providers reload right after this.
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------
// CounterService — application-scoped state with a real lifecycle:
//
// * `boot` restores the last value from `counter.txt` (async I/O with
//   `tokio::fs` — boot is async for exactly this).
// * every request shares the same instance and bumps it,
// * `reload` (SIGHUP or /reload) resets it to 0,
// * `finalize` persists the final value back to `counter.txt` after the
//   runnables drained — the HTTP server is gone, the value cannot move.
//
// Kill it, start it again: the count survives the process.
// ---------------------------------------------------------------------

/// Written at finalize, read at boot. Anchored to the project root via
/// the cargo manifest, so it does not depend on the working directory.
const COUNTER_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/counter.txt");

struct CounterService {
    /// Application-scoped total — shared by every connection.
    hits: AtomicU64,
    /// Per-connection counts, keyed by peer ip:port. Runtime-only —
    /// not persisted; it exists to make the contrast visible: open two
    /// browsers and each keeps its own count while the total is shared.
    by_client: Mutex<HashMap<SocketAddr, u64>>,
}

impl CounterService {
    fn new() -> Self {
        Self { hits: AtomicU64::new(0), by_client: Mutex::new(HashMap::new()) }
    }

    /// Returns (application total, this client's count).
    fn hit(
        &self,
        client: SocketAddr,
    ) -> (u64, u64) {
        let total = self.hits.fetch_add(1, Ordering::Relaxed) + 1;
        let mut clients = self.by_client.lock().expect("client map poisoned");
        let yours = clients.entry(client).or_insert(0);
        *yours += 1;
        (total, *yours)
    }

    fn total(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Provider<AppState> for CounterService {
    fn name(&self) -> &'static str {
        "counter"
    }

    async fn boot(
        &self,
        _state: &AppState,
    ) -> Result<()> {
        // Restore the last persisted value; a missing or unreadable
        // file just means a fresh start.
        let restored = match tokio::fs::read_to_string(COUNTER_FILE).await {
            Ok(text) => {
                let n = text.trim().parse::<u64>().unwrap_or(0);
                println!("counter booted: starting from {n} (restored from {COUNTER_FILE})");
                n
            }
            Err(_) => {
                println!("counter booted: starting fresh from 0 (no {COUNTER_FILE} yet)");
                0
            }
        };
        self.hits.store(restored, Ordering::Relaxed);
        Ok(())
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<AppState> for CounterService {
    /// SIGHUP semantics: rebuild state from scratch — here, reset to 0.
    async fn reload(
        &self,
        _state: &AppState,
    ) -> Result<()> {
        self.hits.store(0, Ordering::Relaxed);
        self.by_client.lock().expect("client map poisoned").clear();
        println!("counter reloaded: reset to 0");
        Ok(())
    }
}

#[async_trait]
impl Finalizable<AppState> for CounterService {
    /// Runs after the runnables drained — the HTTP server is done, no
    /// request can bump the counter anymore, so this value is final.
    async fn finalize(
        &self,
        _state: &AppState,
    ) -> Result<()> {
        let total = self.total();
        tokio::fs::write(COUNTER_FILE, total.to_string()).await?;
        println!("counter finalize: persisted final value = {total} -> {COUNTER_FILE}");
        Ok(())
    }
}

// ---------------------------------------------------------------------
// HttpService — the transport is an implementation detail (axum here);
// the pattern is: resolve dependencies while building the router, hand
// them to handlers as extensions, serve inside run(), drain on shutdown.
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

    /// Boot after the services the handlers consume.
    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<CounterService>()
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

async fn hit(
    ConnectInfo(client): ConnectInfo<SocketAddr>,
    Extension(counter): Extension<Arc<CounterService>>,
) -> Html<String> {
    let (total, yours) = counter.hit(client);
    let port = client.port();
    println!("http: hit #{total} (client port {port} → its #{yours})");
    Html(format!(
        "<h1>hit #{total}</h1>\
         <p>application scope: the total is shared by every connection.<br>\
         your connection (port <code>{port}</code>) is on its hit #{yours}.</p>\
         <p><a href=\"/reload\">reset</a> — or from a terminal: \
         <code>kill -HUP {}</code></p>",
        std::process::id()
    ))
}

/// Reload over HTTP: signals are only one transport for lifecycle
/// commands — any handler can invoke the same `reload_all` directly.
async fn reload(State(app): State<AppState>) -> Redirect {
    match app.registry_ref().reload_all(&app).await {
        Ok(outcome) if outcome.is_complete() => {
            println!("http: /reload — {} provider(s) reloaded", outcome.reloaded_count())
        }
        Ok(outcome) => println!(
            "http: /reload — {} provider(s) reloaded, {} failed",
            outcome.reloaded_count(),
            outcome.failed_count()
        ),
        Err(e) => println!("http: /reload failed: {e}"),
    }
    Redirect::to("/hit")
}

// ---------------------------------------------------------------------
// SignalService — even signal handling is an ordinary runnable, not a
// naked `tokio::spawn` in main. Ctrl+C flips the shared shutdown token;
// SIGHUP triggers `reload_all` (classic daemon semantics). If shutdown
// starts for any other reason it exits quietly, so the drain never
// waits on a listener that will not fire.
// ---------------------------------------------------------------------

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
    ) -> Result<()> {
        let pid = std::process::id();
        println!("signals: ready (pid {pid})");
        println!("signals: reload with `kill -HUP {pid}` or GET /reload — resets the counter");
        println!("signals: stop with Ctrl+C or `kill -INT {pid}` — drains, then finalizes");
        let token = state.shutdown_token();
        let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("signals: Ctrl+C — initiating shutdown");
                    state.initiate_shutdown();
                    return Ok(());
                }
                _ = hangup.recv() => {
                    println!("signals: SIGHUP — reloading providers");
                    let outcome = state.registry_ref().reload_all(&state).await?;
                    if !outcome.is_complete() {
                        println!("signals: reload completed with {} provider failure(s)", outcome.failed_count());
                    }
                }
                _ = token.cancelled() => return Ok(()),
            }
        }
    }
}

#[async_trait]
impl Runnable<AppState> for HttpService {
    /// The server owns its whole life inside this future: resolve its
    /// dependencies, build the router, bind, serve, drain, return.
    async fn run(
        self: Arc<Self>,
        state: AppState,
    ) -> Result<()> {
        // DI resolved once, at router build time: the service travels as
        // an `Extension`, and the router state is the AppState itself —
        // handlers like `/reload` reach the registry through it.
        let counter = state
            .registry_ref()
            .resolve::<CounterService>()
            .ok_or_else(|| Error::msg("http: CounterService not registered"))?;

        let app = Router::new()
            .route("/hit", get(hit))
            .route("/reload", get(reload))
            .layer(Extension(counter))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        println!("http listening on http://{}/hit (Ctrl+C to stop)", self.addr);

        let token = state.shutdown_token();
        // Still plain `axum::serve` — `with_connect_info` is what makes
        // the peer `SocketAddr` extractable in handlers (`ConnectInfo`).
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;

        println!("http drained and stopped");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let state = AppState::new();

    let registry = state.registry_ref();

    registry
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

    let mut runtime = Runtime::<AppState>::default();
    runtime.spawn_all(state.registry_ref(), state.clone());
    runtime.wait_until_shutdown(&state).await?;
    runtime.drain().await?;

    // Runnables are done — release what must not leak into the next boot.
    let finalized = registry.finalize_all(&state).await?;
    for failure in finalized.failures() {
        eprintln!("finalize failed for {}: {}", failure.provider(), failure.error());
    }

    println!("🏁 all services finished gracefully");
    Ok(())
}
