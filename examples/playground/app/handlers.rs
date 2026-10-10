use super::{
    services::{CounterService, SignalService},
    state::AppState,
};
use axum::extract::{ConnectInfo, Form, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use continuo::{HasEvents, ReloadRequired, ServiceManager, ServiceSnapshot};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn read_config(
    State(state): State<AppState>
) -> axum::response::Response {
    let settings = (|| -> congif::Result<_> {
        let listen = state.config::<SocketAddr>("http.listen")?;
        let increment = state.config::<u64>("counter.increment")?;
        let missing = state.config::<String>("counter.missing")?;
        Ok(
            serde_json::json!({ "http": { "listen": listen }, "counter": { "increment": increment }, "missing": missing }),
        )
    })();
    match settings {
        Ok(settings) => Json(settings).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"message": error.to_string()})),
        )
            .into_response(),
    }
}

pub async fn counter_events(
    State(state): State<AppState>,
    Extension(cancel): Extension<tokio_util::sync::CancellationToken>,
) -> impl IntoResponse {
    let receiver = state.counter_updates().subscribe();
    let addresses =
        state.events().subscribe::<super::events::ServerAddressChanged>();
    let stream = futures_util::stream::unfold(
        (receiver, addresses, true, cancel),
        |(mut receiver, mut addresses, first, cancel)| async move {
            // Address moves must win over the old generation's drain signal.
            if let Ok(change) = addresses.try_recv() {
                let event = Event::default()
                    .event("server-address-changed")
                    .json_data(change);
                return Some((event, (receiver, addresses, false, cancel)));
            }
            if cancel.is_cancelled() {
                return None;
            }
            if !first {
                tokio::select! {
                    biased;
                    change = addresses.recv() => {
                        let Ok(change) = change else { return None };
                        let event = Event::default().event("server-address-changed").json_data(change);
                        return Some((event, (receiver, addresses, false, cancel)));
                    }
                    _ = cancel.cancelled() => return None,
                    result = receiver.changed() => if result.is_err() { return None; },
                }
            }
            let snapshot = receiver.borrow_and_update().clone();
            let event = Event::default().json_data(snapshot);
            Some((event, (receiver, addresses, false, cancel)))
        },
    );
    Sse::new(stream)
}

pub async fn styles() -> impl IntoResponse {
    asset("app.css", "text/css; charset=utf-8").await
}

pub async fn script() -> impl IntoResponse {
    asset("app.js", "text/javascript; charset=utf-8").await
}

async fn asset(
    name: &str,
    content_type: &'static str,
) -> axum::response::Response {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/playground/web")
        .join(name);
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            ([(axum::http::header::CONTENT_TYPE, content_type)], bytes)
                .into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
            .into_response(),
    }
}

pub async fn index(
    State(state): State<AppState>,
    Extension(manager): Extension<Arc<ServiceManager>>,
    Extension(signals): Extension<Arc<SignalService>>,
) -> std::result::Result<Html<String>, (StatusCode, String)> {
    let status = manager
        .list()
        .await
        .ok()
        .and_then(|services| {
            services.into_iter().find(|service| service.name() == "counter")
        })
        .map(|service| format_snapshot(&service))
        .unwrap_or_else(|| "counter status unavailable".to_owned());

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/playground/web/index.html");
    let page = tokio::fs::read_to_string(path).await.map_err(|error| {
        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    })?;
    let json =
        state.reader().snapshot::<CounterService>().map_err(|error| {
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        })?;
    let json =
        serde_json::to_string_pretty(json.as_ref()).map_err(|error| {
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        })?;
    let (reload_command, hit_command) =
        signals.commands(state.reader()).map_err(|error| {
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        })?;
    Ok(Html(
        page.replace("{{status}}", &escape_html(&status))
            .replace("{{config}}", &escape_html(&json))
            .replace("{{reload_command}}", &escape_html(&reload_command))
            .replace("{{hit_command}}", &escape_html(&hit_command)),
    ))
}

pub async fn hit(
    ConnectInfo(super::services::PeerAddress(client)): ConnectInfo<
        super::services::PeerAddress,
    >,
    State(state): State<AppState>,
    Extension(counter): Extension<Arc<super::counter::Counter>>,
    headers: HeaderMap,
) -> axum::response::Response {
    match state.hit(&counter, Some(client), false) {
        Some((value, yours)) if wants_json(&headers) => Json(serde_json::json!({
            "message": "Hit added", "value": value, "yours": yours,
        })).into_response(),
        Some(_) => Redirect::to("/").into_response(),
        None => (StatusCode::CONFLICT, Json(serde_json::json!({"message":"Counter is stopped; start it first."}))).into_response(),
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[derive(Deserialize)]
pub struct ConfigForm {
    json: String,
}

pub async fn update_config(
    State(state): State<AppState>,
    Extension(counter): Extension<Arc<CounterService>>,
    headers: HeaderMap,
    Form(form): Form<ConfigForm>,
) -> axum::response::Response {
    match save_counter_config(state.reader(), &counter, &form.json).await {
        Ok(()) if wants_json(&headers) => Json(serde_json::json!({"message":"Configuration saved; watcher will apply it."})).into_response(),
        Ok(()) => Redirect::to("/").into_response(),
        Err(error) if wants_json(&headers) => (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"message":error.to_string()}))).into_response(),
        Err(error) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("Configuration not saved: {error}"),
        )
            .into_response(),
    }
}

async fn list_services(
    Extension(manager): Extension<Arc<ServiceManager>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    match manager.list().await {
        Ok(services) if wants_json(&headers) => Json(serde_json::json!({
            "services": services.iter().map(|service| serde_json::json!({
                "name": service.name(),
                "status": service.status().to_string(),
                "generation": service.generation(),
                "reload_revision": service.reload_revision(),
                "reloadable": service.is_reloadable(),
                "snapshot": format_snapshot(service),
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Ok(services) => {
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("examples/playground/web");
            let (page, row) = match tokio::try_join!(
                tokio::fs::read_to_string(root.join("services.html")),
                tokio::fs::read_to_string(root.join("service.html")),
            ) {
                Ok(templates) => templates,
                Err(error) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        error.to_string(),
                    )
                        .into_response();
                }
            };
            let rows = services
                .iter()
                .map(|service| {
                    row.replace("{{name}}", &escape_html(service.name()))
                        .replace("{{status}}", &service.status().to_string())
                        .replace(
                            "{{generation}}",
                            &service.generation().to_string(),
                        )
                        .replace(
                            "{{revision}}",
                            &service.reload_revision().to_string(),
                        )
                        .replace(
                            "{{reloadable}}",
                            if service.is_reloadable() { "Yes" } else { "No" },
                        )
                        .replace(
                            "{{snapshot}}",
                            &escape_html(&format_snapshot(service)),
                        )
                })
                .collect::<String>();
            Html(
                page.replace("{{services}}", &rows)
                    .replace("{{count}}", &services.len().to_string()),
            )
            .into_response()
        }
        Err(error) if wants_json(&headers) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"message":error.to_string()})),
        )
            .into_response(),
        Err(error) => control_error(error.to_string()).into_response(),
    }
}

async fn start_counter(
    Extension(manager): Extension<Arc<ServiceManager>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    control_result("start", manager.start("counter").await, &headers)
}

async fn stop_counter(
    Extension(manager): Extension<Arc<ServiceManager>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    control_result("stop", manager.stop("counter").await, &headers)
}

async fn restart_counter(
    Extension(manager): Extension<Arc<ServiceManager>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    control_result("restart", manager.restart("counter").await, &headers)
}

async fn reload_counter(
    Extension(manager): Extension<Arc<ServiceManager>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    control_result("reload", manager.reload("counter").await, &headers)
}

pub fn service_control_routes() -> Router<AppState> {
    Router::new()
        .route("/services", get(list_services))
        .route("/services/counter/start", post(start_counter))
        .route("/services/counter/stop", post(stop_counter))
        .route("/services/counter/restart", post(restart_counter))
        .route("/services/counter/reload", post(reload_counter))
}

fn format_snapshot(service: &ServiceSnapshot) -> String {
    let error = service
        .last_error()
        .map(|message| format!(" | last error={message}"))
        .unwrap_or_default();
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
    headers: &HeaderMap,
) -> axum::response::Response {
    match result {
        Ok(service) if wants_json(headers) => Json(serde_json::json!({
            "message":format!("{action} completed"),
            "snapshot":format_snapshot(&service),
            "status":service.status().to_string(),
        }))
        .into_response(),
        Ok(_) => Redirect::to("/").into_response(),
        Err(error) if wants_json(headers) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"message":error.to_string()})),
        )
            .into_response(),
        Err(error) => control_error(error.to_string()).into_response(),
    }
}

fn wants_json(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').any(|part| {
                part.trim().split(';').next() == Some("application/json")
            })
        })
}

fn control_error(message: String) -> (StatusCode, Html<String>) {
    (
        StatusCode::CONFLICT,
        Html(format!(
            "<h1>service command failed</h1><p>{message}</p><p><a href=\"/\">back</a></p>"
        )),
    )
}

pub async fn reload(State(app): State<AppState>) -> Redirect {
    app.events().emit(ReloadRequired);
    tracing::info!("http: application reload requested");
    Redirect::to("/")
}

pub(super) async fn save_counter_config(
    reader: &congif::ConfigReader,
    service: &CounterService,
    json: &str,
) -> continuo::Result<()> {
    let path = congif::Configurable::source(service).path;
    let candidate = reader
        .parse_for::<CounterService>(congif::ConfigSource {
            path: &path,
            bytes: json.as_bytes(),
        })
        .await?;
    congif::Configurable::validate_config(service, &candidate)?;
    let bytes = serde_json::to_vec_pretty(&candidate)?;
    tokio::task::spawn_blocking(move || {
        super::config::atomic_write(&path, &bytes)
    })
    .await??;
    Ok(())
}
