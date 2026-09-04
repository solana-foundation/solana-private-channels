pub mod config;
pub mod db;
pub mod error;
pub mod jwt;
pub mod models;
pub mod password;
pub mod pool_status;
pub mod routes;
pub mod serve;
pub mod throttle;
pub mod validation;

use axum::{
    extract::{DefaultBodyLimit, FromRequestParts, Request, State},
    http::{request::Parts, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, post},
    Json, Router,
};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

use error::AppError;
use jwt::{Claims, JwtConfig};
use password::PasswordWorker;
use pool_status::PoolStatus;
use throttle::AuthThrottle;

/// Body cap for the credential routes, sized to the username and password
/// limits plus JSON overhead. The rest of the router keeps axum's default.
const CREDENTIAL_BODY_LIMIT: usize = 4096;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub jwt: Arc<JwtConfig>,
    pub pool_status: Arc<PoolStatus>,
    pub passwords: PasswordWorker,
    pub throttle: Arc<AuthThrottle>,
}

// Extract and validate JWT from the Authorization header for any route that declares `claims: Claims`.
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync + AsRef<Arc<JwtConfig>>,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({ "error": "missing token" })),
                )
            })?;

        state.as_ref().verify(token).map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "invalid token" })),
            )
        })
    }
}

impl AsRef<Arc<JwtConfig>> for AppState {
    fn as_ref(&self) -> &Arc<JwtConfig> {
        &self.jwt
    }
}

/// `request_timeout` bounds a single request once its headers are in, covering
/// the body read and the handler. It is layered here rather than around the
/// finished router so the CORS layer stays outermost and the response still
/// carries its headers; a browser would otherwise see an opaque network error.
///
/// Keep it above every timeout it encloses. Cancelling a request before a
/// downstream call returns discards that call's error, and the pool error is
/// the only thing that marks `/health` unhealthy: see `POOL_ACQUIRE_TIMEOUT`.
pub fn build_app(state: AppState, cors_allowed_origin: &str, request_timeout: Duration) -> Router {
    // Restrict CORS to only what this service actually needs.
    // CorsLayer::permissive() would allow any origin, method, and header — too broad
    // for a service that issues JWTs and handles credentials.
    let origin = if cors_allowed_origin == "*" {
        AllowOrigin::any()
    } else {
        // Parse into a HeaderValue so tower-http can match it exactly.
        // Panic at startup rather than silently falling back to a permissive default.
        let value = HeaderValue::from_str(cors_allowed_origin)
            .expect("CORS_ALLOWED_ORIGIN is not a valid HTTP header value");
        AllowOrigin::exact(value)
    };

    let cors = CorsLayer::new()
        .allow_origin(origin)
        // Only the methods actually used by auth routes.
        .allow_methods(AllowMethods::list([
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::OPTIONS,
        ]))
        // Only the headers clients need to send.
        .allow_headers(AllowHeaders::list([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ]));

    // The credential routes are the expensive unauthenticated surface: each one
    // runs Argon2. They get the throttle and a tight body limit; the throttle
    // is layered last so it runs before the body is read.
    let credentials = Router::new()
        .route("/auth/register", post(routes::register::register))
        .route("/auth/login", post(routes::login::login))
        .layer(DefaultBodyLimit::max(CREDENTIAL_BODY_LIMIT))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            throttle::per_ip,
        ));

    Router::new()
        .merge(credentials)
        .route("/auth/challenge-wallet", post(routes::challenge::challenge))
        .route(
            "/auth/verify-wallet",
            post(routes::verify_wallet::verify_wallet),
        )
        .route("/auth/wallets", get(routes::wallets::wallets))
        .route(
            "/auth/wallets/{pubkey}",
            delete(routes::wallets::delete_wallet),
        )
        .route("/health", get(health))
        .layer(middleware::from_fn_with_state(
            request_timeout,
            enforce_request_timeout,
        ))
        .layer(cors)
        .with_state(state)
}

/// Sheds a request that outran `limit`, whatever it was waiting on.
///
/// Reported as 503, not 408. The usual cause is server-side contention (the
/// pool or the Argon2 queue), and 408 tells the client it may repeat the request
/// unchanged, which turns a struggling database into a retry storm. 503 is
/// already what this service returns when it sheds for the Argon2 cap.
///
/// Going through `AppError` also gives the response the `{"error": ...}` body
/// every other failure here has; a bare status from a timeout layer would be
/// the one response a browser could neither read nor parse.
async fn enforce_request_timeout(
    State(limit): State<Duration>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    tokio::time::timeout(limit, next.run(request))
        .await
        .map_err(|_| AppError::Unavailable)
}

/// Reads the cached pool-status flag updated by handlers; doesn't itself touch the pool.
async fn health(State(state): State<AppState>) -> StatusCode {
    if state.pool_status.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::routing::get;
    use std::net::SocketAddr;
    use std::num::{NonZeroU32, NonZeroUsize};
    use tower::ServiceExt;

    /// A router over a pool that is never connected. The body cap rejects before
    /// any handler runs, so these tests need no database.
    fn app_without_a_database() -> Router {
        let state = AppState {
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .unwrap(),
            jwt: Arc::new(JwtConfig::new("test-secret")),
            pool_status: PoolStatus::new_healthy(),
            passwords: PasswordWorker::new(NonZeroUsize::new(1).unwrap()),
            throttle: Arc::new(AuthThrottle::new(
                NonZeroU32::new(10_000).unwrap(),
                NonZeroU32::new(10_000).unwrap(),
                NonZeroU32::new(10_000).unwrap(),
            )),
        };
        build_app(state, "*", Duration::from_secs(15))
    }

    /// The credential routes cap bodies well below anything a valid request
    /// needs, so an oversized one is refused before it reaches Argon2 or the
    /// database. The limit rides on a request extension, which this service now
    /// installs by hand in `serve`.
    #[tokio::test]
    async fn an_oversized_credential_body_is_refused() {
        let oversized = "x".repeat(CREDENTIAL_BODY_LIMIT + 1);
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/auth/register")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(oversized))
            .unwrap();
        // The throttle ahead of the body limit reads this, as `serve` supplies it.
        request
            .extensions_mut()
            .insert(ConnectInfo("127.0.0.1:9000".parse::<SocketAddr>().unwrap()));

        let response = app_without_a_database().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// The shed has to look like every other failure here: 503 with an `error`
    /// body, not a bare status a browser can't parse.
    #[tokio::test]
    async fn outrunning_the_request_timeout_sheds_as_503_json() {
        let app = Router::new()
            .route(
                "/stall",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    StatusCode::OK
                }),
            )
            .layer(middleware::from_fn_with_state(
                Duration::from_millis(50),
                enforce_request_timeout,
            ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stall")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("\"error\""),
            "expected an error body, got: {:?}",
            String::from_utf8_lossy(&body)
        );
    }
}
