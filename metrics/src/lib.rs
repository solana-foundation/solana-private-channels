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
        serve_bounded(listener, app, LIMITS).await;
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
            Ok(listener) => serve_bounded(listener, app, LIMITS).await,
            Err(e) => {
                tracing::error!("Failed to bind metrics server on {}: {}", addr, e);
            }
        }
    });
}

/// Bounds on the metrics listener. Prometheus is the only scraper, so 64 is ample.
#[derive(Clone, Copy)]
struct Limits {
    max_connections: usize,
    header_read_timeout: std::time::Duration,
}

const LIMITS: Limits = Limits {
    max_connections: 64,
    header_read_timeout: std::time::Duration::from_secs(10),
};

/// Replaces `axum::serve`, which accepts without bound and never times out a header, so
/// trickled requests could hold sockets until the process ran out of file descriptors.
async fn serve_bounded(listener: tokio::net::TcpListener, app: axum::Router, limits: Limits) {
    use tower::ServiceExt;

    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(limits.max_connections));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // Usually fd exhaustion; pause instead of spinning on the error.
                tracing::debug!("Metrics accept failed: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        // At the cap the socket is dropped at once rather than queued.
        let Ok(permit) = std::sync::Arc::clone(&slots).try_acquire_owned() else {
            continue;
        };
        let app = app.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let service =
                hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    app.clone().oneshot(req.map(axum::body::Body::new))
                });
            // The timer is what makes header_read_timeout take effect.
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(limits.header_read_timeout)
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            if let Err(e) = conn.await {
                tracing::debug!("Metrics connection closed: {}", e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn boot(limits: Limits) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_health_app(HealthState::new(HealthConfig::operator()));
        tokio::spawn(serve_bounded(listener, app, limits));
        addr
    }

    /// True if the server closes the socket (EOF or reset) within `within`.
    async fn closed_within(stream: &mut TcpStream, within: Duration) -> bool {
        let mut buf = [0u8; 64];
        matches!(
            tokio::time::timeout(within, stream.read(&mut buf)).await,
            Ok(Ok(0)) | Ok(Err(_))
        )
    }

    async fn health_ok(addr: std::net::SocketAddr) -> bool {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        let read = tokio::time::timeout(Duration::from_secs(5), s.read_to_string(&mut out)).await;
        matches!(read, Ok(Ok(_))) && out.starts_with("HTTP/1.1 200")
    }

    /// A client that trickles its headers is closed after the header timeout, and a
    /// complete request is still served meanwhile.
    #[tokio::test]
    async fn header_trickle_is_closed_after_the_timeout() {
        let addr = boot(Limits {
            max_connections: 8,
            header_read_timeout: Duration::from_millis(500),
        })
        .await;
        let mut slow = TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /hea").await.unwrap();
        assert!(health_ok(addr).await);
        assert!(
            closed_within(&mut slow, Duration::from_secs(3)).await,
            "a trickled header must be closed after the timeout"
        );
    }

    /// Connections past the cap are dropped at once; a freed slot is reused.
    #[tokio::test]
    async fn connections_past_the_cap_are_dropped_and_slots_recycle() {
        let addr = boot(Limits {
            max_connections: 2,
            header_read_timeout: Duration::from_secs(30),
        })
        .await;
        let first = TcpStream::connect(addr).await.unwrap();
        let _second = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut over = TcpStream::connect(addr).await.unwrap();
        assert!(
            closed_within(&mut over, Duration::from_secs(2)).await,
            "a connection past the cap must be dropped"
        );
        drop(first);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(health_ok(addr).await, "a freed slot must serve again");
    }
}
