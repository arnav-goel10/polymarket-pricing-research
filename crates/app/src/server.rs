use crate::state::DashboardState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use execution::FillRecord;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Dry-run, stop, and pause flags shared by the control endpoint and evaluation loop.
#[derive(Debug, Default, Clone, Copy)]
pub struct ControlFlags {
    pub dry_run_enabled: bool,
    pub killed: bool,
    /// When true the observation loop skips all entries/exits (no P&L impact).
    /// The process keeps running and all feeds/state update normally — resume
    /// is instant with `curl … -d '{"action":"resume"}'`.
    pub paused: bool,
}

#[derive(Clone)]
pub struct Shared {
    pub rx: watch::Receiver<DashboardState>,
    pub flags: Arc<Mutex<ControlFlags>>,
    pub history: Arc<Mutex<Vec<crate::state::PnlPoint>>>,
    pub trades: Arc<Mutex<Vec<crate::state::TradeRecord>>>,
    pub fills_path: String,
    /// Bearer token for state-changing dashboard controls. When absent the
    /// control route is not registered, making public shadow demos read-only.
    pub control_token: Option<Arc<str>>,
    /// Feed-health counters exposed at `/metrics`.
    pub metrics: Arc<crate::metrics::Metrics>,
}

const DASHBOARD_HTML: &str = include_str!("../dashboard/index.html");

pub fn router(shared: Shared) -> Router {
    let controls_enabled = shared.control_token.is_some();
    let router = Router::new()
        .route("/", get(|| async { Html(DASHBOARD_HTML) }))
        .route("/ws", get(ws_handler))
        .route("/state", get(state_handler))
        .route("/history", get(history_handler))
        .route("/trades", get(trades_handler))
        .route("/fills", get(fills_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler));
    let router = if controls_enabled {
        router.route("/control", post(control_handler))
    } else {
        router
    };
    router.with_state(shared)
}

/// Plain-JSON snapshot of the latest state (health check / debugging).
async fn state_handler(State(s): State<Shared>) -> impl IntoResponse {
    Json(s.rx.borrow().clone())
}

/// The sampled realized-P&L time series for the dashboard graphs.
async fn history_handler(State(s): State<Shared>) -> impl IntoResponse {
    let hist = s.history.lock().map(|h| h.clone()).unwrap_or_default();
    Json(hist)
}

/// The full closed-trade ledger for the History tab.
async fn trades_handler(State(s): State<Shared>) -> impl IntoResponse {
    let trades = s.trades.lock().map(|t| t.clone()).unwrap_or_default();
    Json(trades)
}

/// Shadow and dry-run fill ledger, read from the JSONL audit file.
async fn fills_handler(State(s): State<Shared>) -> impl IntoResponse {
    Json(read_fills_jsonl(&s.fills_path))
}

fn read_fills_jsonl(path: &str) -> Vec<FillRecord> {
    if path.is_empty() {
        return Vec::new();
    }
    std::fs::read_to_string(path)
        .ok()
        .map(|body| {
            body.lines()
                .filter_map(|line| serde_json::from_str::<FillRecord>(line).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Prometheus text-format feed health (lag, rate, reconnects, stale flags).
async fn metrics_handler(State(s): State<Shared>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        s.metrics.render(),
    )
}

async fn health_handler(State(s): State<Shared>) -> impl IntoResponse {
    let snapshot = s.rx.borrow().clone();
    let ok = !snapshot.killed
        && snapshot
            .chainlink_age_millis
            .is_some_and(|age| age <= 5_000);
    Json(serde_json::json!({
        "ok": ok,
        "mode": snapshot.mode,
        "risk": snapshot.risk,
        "market": snapshot.market_slug,
        "chainlink_age_millis": snapshot.chainlink_age_millis,
        "book_age_millis": snapshot.book_age_millis,
        "last_simulation_error": snapshot.last_simulation_error,
        "controls_enabled": s.control_token.is_some(),
    }))
}

fn websocket_origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        // Non-browser clients do not necessarily send Origin.
        return true;
    };
    let Some(host) = headers.get(header::HOST) else {
        return false;
    };
    let (Ok(origin), Ok(host)) = (origin.to_str(), host.to_str()) else {
        return false;
    };
    origin == format!("http://{host}") || origin == format!("https://{host}")
}

async fn ws_handler(ws: WebSocketUpgrade, State(s): State<Shared>, headers: HeaderMap) -> Response {
    if !websocket_origin_allowed(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, s))
        .into_response()
}

async fn handle_socket(mut socket: WebSocket, s: Shared) {
    let mut rx = s.rx.clone();
    loop {
        let snapshot = rx.borrow().clone();
        let txt = match serde_json::to_string(&snapshot) {
            Ok(t) => t,
            Err(_) => break,
        };
        if socket.send(Message::Text(txt.into())).await.is_err() {
            break;
        }
        if rx.changed().await.is_err() {
            break;
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ControlAction {
    EnableDryRun,
    DisableDryRun,
    Kill,
    Reset,
    Pause,
    Resume,
}

#[derive(Debug, serde::Deserialize)]
struct ControlRequest {
    action: ControlAction,
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(presented) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(presented.as_bytes(), expected.as_bytes())
}

fn apply_control(flags: &mut ControlFlags, action: ControlAction) {
    match action {
        ControlAction::EnableDryRun => flags.dry_run_enabled = true,
        ControlAction::DisableDryRun => flags.dry_run_enabled = false,
        ControlAction::Kill => {
            flags.killed = true;
            flags.dry_run_enabled = false;
        }
        ControlAction::Reset => flags.killed = false,
        ControlAction::Pause => flags.paused = true,
        ControlAction::Resume => flags.paused = false,
    }
}

async fn control_handler(
    State(s): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<ControlRequest>,
) -> impl IntoResponse {
    let Some(expected) = s.control_token.as_deref() else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"ok": false, "error": "controls disabled"})),
        );
    };
    if !authorized(&headers, expected) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"ok": false, "error": "unauthorized"})),
        );
    }
    if let Ok(mut f) = s.flags.lock() {
        apply_control(&mut f, body.action);
    }
    (StatusCode::OK, Json(serde_json::json!({"ok": true})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds_with_shared_state() {
        let (_tx, rx) = watch::channel(DashboardState::default());
        let shared = Shared {
            rx,
            flags: Arc::new(Mutex::new(ControlFlags::default())),
            history: Arc::new(Mutex::new(Vec::new())),
            trades: Arc::new(Mutex::new(Vec::new())),
            fills_path: String::new(),
            control_token: None,
            metrics: Arc::new(crate::metrics::Metrics::default()),
        };
        let _r = router(shared); // builds without panicking
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_prometheus_text() {
        use tower::ServiceExt;
        let (_tx, rx) = watch::channel(DashboardState::default());
        let feed = Arc::new(poly_core::FeedMetrics::new("rtds", 5_000));
        feed.record_message(0);
        let shared = Shared {
            rx,
            flags: Arc::new(Mutex::new(ControlFlags::default())),
            history: Arc::new(Mutex::new(Vec::new())),
            trades: Arc::new(Mutex::new(Vec::new())),
            fills_path: String::new(),
            control_token: None,
            metrics: Arc::new(crate::metrics::Metrics::new(vec![feed])),
        };
        let resp = router(shared)
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/plain; version=0.0.4"));
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("poly_feed_messages_total{feed=\"rtds\"} 1"));
        assert!(text.contains("poly_feed_stale{feed=\"rtds\"} 0"));
    }

    #[test]
    fn control_flags_default_disabled() {
        let f = ControlFlags::default();
        assert!(!f.dry_run_enabled && !f.killed);
    }

    #[test]
    fn bearer_token_comparison_is_exact() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer correct-token".parse().unwrap(),
        );
        assert!(authorized(&headers, "correct-token"));
        assert!(!authorized(&headers, "correct-token-extra"));
        assert!(!authorized(&HeaderMap::new(), "correct-token"));
    }

    #[test]
    fn kill_is_fail_closed() {
        let mut flags = ControlFlags {
            dry_run_enabled: true,
            killed: false,
            paused: false,
        };
        apply_control(&mut flags, ControlAction::Kill);
        assert!(flags.killed);
        assert!(!flags.dry_run_enabled);
    }

    #[test]
    fn websocket_origin_must_match_host() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "127.0.0.1:8080".parse().unwrap());
        headers.insert(header::ORIGIN, "http://127.0.0.1:8080".parse().unwrap());
        assert!(websocket_origin_allowed(&headers));

        headers.insert(header::ORIGIN, "https://attacker.example".parse().unwrap());
        assert!(!websocket_origin_allowed(&headers));
    }
}
