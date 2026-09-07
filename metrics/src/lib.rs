pub use std::sync::LazyLock as Lazy;

pub use prometheus;
pub use prometheus::{CounterVec, GaugeVec, HistogramVec};

pub mod health;
pub use health::{HealthConfig, HealthOutcome, HealthState};

#[macro_export]
macro_rules! counter_vec {
    ($name:ident, $metric_name:expr, $help:expr, $labels:expr) => {
        pub static $name: $crate::Lazy<$crate::CounterVec> = $crate::Lazy::new(|| {
            $crate::prometheus::register_counter_vec!($metric_name, $help, $labels).unwrap()
        });
    };
}

#[macro_export]
macro_rules! gauge_vec {
    ($name:ident, $metric_name:expr, $help:expr, $labels:expr) => {
        pub static $name: $crate::Lazy<$crate::GaugeVec> = $crate::Lazy::new(|| {
            $crate::prometheus::register_gauge_vec!($metric_name, $help, $labels).unwrap()
        });
    };
}

#[macro_export]
macro_rules! histogram_vec {
    ($name:ident, $metric_name:expr, $help:expr, $labels:expr) => {
        pub static $name: $crate::Lazy<$crate::HistogramVec> = $crate::Lazy::new(|| {
            $crate::prometheus::register_histogram_vec!($metric_name, $help, $labels).unwrap()
        });
    };
}

#[macro_export]
macro_rules! init_metrics {
    ($($metric:expr),* $(,)?) => {
        $($crate::Lazy::force(&$metric);)*
    };
}

pub trait MetricLabel {
    fn as_label(&self) -> &'static str;
}

async fn metrics_handler() -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let body = prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .unwrap();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

pub fn start_metrics_server(port: u16) {
    let app = axum::Router::new().route("/metrics", axum::routing::get(metrics_handler));
    spawn_server(port, app);
}

/// Same as `start_metrics_server` but also exposes `/health` backed by the
/// supplied state. Use this from services that want compose to gate on
/// `/health` instead of `/metrics`.
pub fn start_metrics_server_with_health(port: u16, health: std::sync::Arc<HealthState>) {
    spawn_server(port, build_health_app(health));
}

/// Test-only entry point that takes a pre-bound listener so callers can avoid
/// the bind/drop/rebind port-reclaim race when they need to know the port up front.
pub fn start_metrics_server_with_health_from_listener(
    listener: std::net::TcpListener,
    health: std::sync::Arc<HealthState>,
) {
    let app = build_health_app(health);
    listener.set_nonblocking(true).expect("set_nonblocking");
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).expect("from_std");
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("Metrics server error: {}", e);
        }
    });
}

fn build_health_app(health: std::sync::Arc<HealthState>) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/health", axum::routing::get(health_handler))
        .with_state(health)
}

async fn health_handler(
    axum::extract::State(health): axum::extract::State<std::sync::Arc<HealthState>>,
) -> (axum::http::StatusCode, String) {
    match health.check() {
        HealthOutcome::Healthy => (axum::http::StatusCode::OK, r#"{"status":"ok"}"#.to_string()),
        HealthOutcome::ForcedUnhealthy { reason } => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            // Build via serde_json so the reason is escaped correctly, including any
            // control characters that hand-rolled escaping would leave invalid.
            serde_json::json!({"status": "degraded", "reason": "forced", "detail": reason})
                .to_string(),
        ),
        HealthOutcome::BacklogExceeded { pending, ceiling } => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!(
                r#"{{"status":"degraded","reason":"backlog","pending":{},"ceiling":{}}}"#,
                pending, ceiling
            ),
        ),
        HealthOutcome::Stalled { pending, age_secs } => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!(
                r#"{{"status":"degraded","reason":"stalled","pending":{},"age_secs":{}}}"#,
                pending, age_secs
            ),
        ),
    }
}

fn spawn_server(port: u16, app: axum::Router) {
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));

    tracing::info!("Metrics server listening on {}", addr);

    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!("Metrics server error: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("Failed to bind metrics server on {}: {}", addr, e);
            }
        }
    });
}
