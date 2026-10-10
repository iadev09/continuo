use super::{counter::Counter, services::SignalService, state::AppState};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use continuo::{
    HasEvents, HasRegistry, ReloadRequired, ServiceManager, ServiceSnapshot,
};
use std::any::Any;
use std::sync::Arc;

/// Resolve a service the handler uses at request time. The HTTP service does
/// not depend on it: while it is absent the handler answers 503, and once it
/// is registered the same handler answers normally.
fn available<T: Any + Send + Sync>(
    state: &AppState,
    what: &str,
    headers: &HeaderMap,
) -> std::result::Result<Arc<T>, Box<Response>> {
    state.registry_ref().resolve::<T>().ok_or_else(|| {
        let message = format!("{what} is not available");
        let response = if wants_json(headers) {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"message": message})),
            )
                .into_response()
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, message).into_response()
        };
        Box::new(response)
    })
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
    headers: HeaderMap,
) -> Response {
    let manager = match available::<ServiceManager>(
        &state,
        "service manager",
        &headers,
    ) {
        Ok(manager) => manager,
        Err(response) => return *response,
    };
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
    let page = match tokio::fs::read_to_string(path).await {
        Ok(page) => page,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
                .into_response();
        }
    };
    // Signals are optional on this page: without them the commands are
    // simply not shown.
    let (reload_command, hit_command) = state
        .registry_ref()
        .resolve::<SignalService>()
        .and_then(|signals| signals.commands(state.reader()).ok())
        .unwrap_or_else(|| {
            let unavailable = "signals are not available".to_owned();
            (unavailable.clone(), unavailable)
        });
    Html(
        page.replace("{{status}}", &escape_html(&status))
            .replace("{{reload_command}}", &escape_html(&reload_command))
            .replace("{{hit_command}}", &escape_html(&hit_command)),
    )
    .into_response()
}

pub async fn hit(
    ConnectInfo(super::services::PeerAddress(client)): ConnectInfo<
        super::services::PeerAddress,
    >,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let counter = match available::<Counter>(&state, "counter", &headers) {
        Ok(counter) => counter,
        Err(response) => return *response,
    };
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

async fn list_services(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let manager = match available::<ServiceManager>(
        &state,
        "service manager",
        &headers,
    ) {
        Ok(manager) => manager,
        Err(response) => return *response,
    };
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
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    match available::<ServiceManager>(&state, "service manager", &headers) {
        Ok(manager) => {
            control_result("start", manager.start("counter").await, &headers)
        }
        Err(response) => *response,
    }
}

async fn stop_counter(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    match available::<ServiceManager>(&state, "service manager", &headers) {
        Ok(manager) => {
            control_result("stop", manager.stop("counter").await, &headers)
        }
        Err(response) => *response,
    }
}

async fn restart_counter(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    match available::<ServiceManager>(&state, "service manager", &headers) {
        Ok(manager) => control_result(
            "restart",
            manager.restart("counter").await,
            &headers,
        ),
        Err(response) => *response,
    }
}

async fn reload_counter(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    match available::<ServiceManager>(&state, "service manager", &headers) {
        Ok(manager) => {
            control_result("reload", manager.reload("counter").await, &headers)
        }
        Err(response) => *response,
    }
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
) -> Response {
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
