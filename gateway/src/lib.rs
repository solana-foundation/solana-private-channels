pub mod auth;
pub mod db;
pub mod metrics;

use crate::auth::{
    check_account_data_ownership, check_request_auth, decode_account_data, forbidden_body,
    AuthDecision,
};
use clap::Parser;
use http_body_util::{BodyExt, Empty, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use jsonrpsee::types::error::INVALID_REQUEST_CODE;
use jsonwebtoken::DecodingKey;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info, warn};

/// Maximum allowed request body size (64 KB).
const MAX_BODY_SIZE: usize = 64 * 1024;

const KNOWN_RPC_METHODS: &[&str] = &[
    "sendTransaction",
    "getAccountInfo",
    "getSlot",
    "getBlock",
    "getTransaction",
    "getRecentBlockhash",
    "getTokenAccountBalance",
    "getLatestBlockhash",
    "getSignatureStatuses",
    "getTransactionCount",
    "getFirstAvailableBlock",
    "getBlocks",
    "getEpochInfo",
    "getEpochSchedule",
    "getRecentPerformanceSamples",
    "getBlockTime",
    "getVoteAccounts",
    "getSupply",
    "getSlotLeaders",
    "isBlockhashValid",
    "getSignaturesForAddress",
    "simulateTransaction",
];

#[derive(Parser, Debug, Clone)]
#[command(name = "private-channel-gateway")]
#[command(about = "JSON RPC gateway that routes requests to write or read nodes")]
pub struct Args {
    /// Port to run the gateway on
    #[arg(short, long, env = "GATEWAY_PORT", default_value = "8898")]
    pub port: u16,

    /// Write node URL (for send_transaction requests)
    #[arg(short, long, env = "GATEWAY_WRITE_URL")]
    pub write_url: String,

    /// Read node URL (for all other requests)
    #[arg(short, long, env = "GATEWAY_READ_URL")]
    pub read_url: String,

    /// CORS Access-Control-Allow-Origin header value
    #[arg(long, default_value = "*", env = "GATEWAY_CORS_ALLOWED_ORIGIN")]
    pub cors_allowed_origin: String,

    /// Shared JWT secret used to verify tokens issued by the auth service.
    /// If absent, auth enforcement is disabled (useful for local dev).
    /// Must match the JWT_SECRET configured in the auth service.
    #[arg(long, env = "JWT_SECRET")]
    pub jwt_secret: Option<String>,

    /// Connection URL for the auth service's Postgres database.
    /// Required when JWT_SECRET is set (used for wallet ownership checks).
    #[arg(long, env = "AUTH_DATABASE_URL")]
    pub auth_database_url: Option<String>,

    /// Maximum number of connections in the auth database pool.
    /// Only relevant when AUTH_DATABASE_URL is set. Each concurrent request
    /// that hits a gated method occupies one connection for the ownership
    /// check, so this should be sized to match expected peak concurrency.
    #[arg(long, env = "AUTH_DATABASE_MAX_CONNECTIONS", default_value = "10")]
    pub auth_database_max_connections: u32,
}

pub struct Gateway {
    write_url: String,
    read_url: String,
    cors_allowed_origin: String,
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Full<Bytes>,
    >,
    /// Pre-built decoding key derived from JWT_SECRET at startup.
    /// `None` means auth enforcement is disabled.
    jwt_secret: Option<DecodingKey>,
    /// Connection pool to the auth service's Postgres database.
    /// Used for wallet ownership checks on gated methods.
    /// `None` when auth enforcement is disabled.
    auth_db: Option<PgPool>,
    /// Cached result of the last upstream readiness probe, refreshed on demand.
    ready_cache: Arc<AsyncMutex<Option<ReadyCache>>>,
}

#[derive(Clone, Copy)]
struct ReadyCache {
    checked_at: Instant,
    healthy: bool,
}

const READY_CACHE_TTL: Duration = Duration::from_secs(2);
const READY_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// A `JWT_SECRET` counts as "configured" only if non-empty after trimming, mirroring the
/// auth service so a whitespace-only secret doesn't enable gateway RBAC while auth refuses
/// to start.
fn configured_secret(secret: Option<&str>) -> Option<&str> {
    secret.filter(|s| !s.trim().is_empty())
}

impl Gateway {
    pub fn new(
        write_url: String,
        read_url: String,
        cors_allowed_origin: String,
        jwt_secret: Option<String>,
        auth_db: Option<PgPool>,
    ) -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(https);
        // Treat an empty or whitespace-only JWT_SECRET as "not set"; key is built from the
        // untrimmed bytes so it stays identical to the auth service's signing key.
        let jwt_secret = configured_secret(jwt_secret.as_deref())
            .map(|s| DecodingKey::from_secret(s.as_bytes()));
        Self {
            write_url,
            read_url,
            cors_allowed_origin,
            client,
            jwt_secret,
            auth_db,
            ready_cache: Arc::new(AsyncMutex::new(None)),
        }
    }

    /// Probes a single upstream's /health with a short timeout.
    async fn probe_upstream(&self, url: &str) -> bool {
        let probe_url = format!("{}/health", url.trim_end_matches('/'));
        let Ok(uri) = probe_url.parse::<hyper::Uri>() else {
            return false;
        };
        let Ok(req) = Request::builder()
            .method(hyper::Method::GET)
            .uri(uri)
            .body(Full::new(Bytes::new()))
        else {
            return false;
        };
        match tokio::time::timeout(READY_PROBE_TIMEOUT, self.client.request(req)).await {
            Ok(Ok(resp)) => resp.status().is_success(),
            _ => false,
        }
    }

    /// Returns true if both upstreams pass /health within the cache TTL. Probes are
    /// cached for 2s so probe storms don't cascade into upstream load.
    async fn check_ready(&self) -> bool {
        // Lock held across the probe so concurrent /ready callers single-flight: the first
        // refreshes the cache, the rest wait and read the just-cached result.
        let mut cache = self.ready_cache.lock().await;
        if let Some(c) = *cache {
            if c.checked_at.elapsed() < READY_CACHE_TTL {
                return c.healthy;
            }
        }
        let (write_ok, read_ok) = tokio::join!(
            self.probe_upstream(&self.write_url),
            self.probe_upstream(&self.read_url)
        );
        let healthy = write_ok && read_ok;
        *cache = Some(ReadyCache {
            checked_at: Instant::now(),
            healthy,
        });
        healthy
    }

    fn record_metrics(
        error_type: Option<&str>,
        method: &str,
        target: &str,
        status: &str,
        elapsed: f64,
    ) {
        if let Some(et) = error_type {
            metrics::GATEWAY_ERRORS_TOTAL.with_label_values(&[et]).inc();
        }
        metrics::GATEWAY_REQUESTS_TOTAL
            .with_label_values(&[method, target, status])
            .inc();
        metrics::GATEWAY_REQUEST_DURATION
            .with_label_values(&[method, target])
            .observe(elapsed);
    }

    /// Fetch raw account data from the read node for Phase 2 ownership checks.
    ///
    /// Sends a `getAccountInfo` request with `encoding: "base64"` to the read
    /// node and returns the decoded account bytes alongside the program owner
    /// string (e.g. the SPL Token program ID).
    ///
    /// Returns `None` if the account does not exist or cannot be fetched.
    async fn fetch_account_for_auth(&self, pubkey: &str) -> Option<(Vec<u8>, String)> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            // Request base64 encoding so we get the raw bytes back as a string.
            "params": [pubkey, { "encoding": "base64" }]
        })
        .to_string();

        let uri = self.read_url.parse::<hyper::Uri>().ok()?;
        let req = Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .ok()?;

        let response = self.client.request(req).await.ok()?;
        let body_bytes = response.into_body().collect().await.ok()?.to_bytes();

        let json: Value = serde_json::from_slice(&body_bytes).ok()?;

        // getAccountInfo returns null for result.value when the account doesn't exist.
        let value = json.get("result")?.get("value")?;
        if value.is_null() {
            return None;
        }

        // The program that owns this account — used to confirm it is a token account.
        let program_owner = value.get("owner")?.as_str()?.to_owned();

        // data is [base64_string, encoding_name] — we want index 0.
        let encoded = value.get("data")?.get(0)?.as_str()?;
        let data = decode_account_data(encoded)?;

        Some((data, program_owner))
    }

    fn error_response(
        &self,
        status: StatusCode,
        body: Option<Bytes>,
    ) -> Response<http_body_util::combinators::UnsyncBoxBody<Bytes, hyper::Error>> {
        let mut builder = Response::builder().status(status).header(
            "Access-Control-Allow-Origin",
            self.cors_allowed_origin.as_str(),
        );
        match body {
            Some(bytes) => {
                builder = builder.header("Content-Type", "application/json");
                builder
                    .body(
                        Full::new(bytes)
                            .map_err(|never| match never {})
                            .boxed_unsync(),
                    )
                    .unwrap()
            }
            None => builder
                .body(Empty::new().map_err(|never| match never {}).boxed_unsync())
                .unwrap(),
        }
    }

    /// Enforces RBAC on gated methods.
    ///
    /// Returns `Some(response)` if the request must be rejected, `None` if it
    /// may proceed. No-ops immediately when auth is not configured.
    async fn enforce_auth(
        &self,
        auth_header: Option<&str>,
        method: &str,
        method_label: &str,
        params: &Value,
        start: Instant,
    ) -> Option<Response<http_body_util::combinators::UnsyncBoxBody<Bytes, hyper::Error>>> {
        let (decoding_key, auth_db) = match (&self.jwt_secret, &self.auth_db) {
            (Some(k), Some(db)) => (k, db),
            _ => return None,
        };

        let decision = check_request_auth(auth_header, decoding_key, method, params);

        let (status, body) = match decision {
            AuthDecision::Proceed => return None,
            AuthDecision::Reject(status, body) => (status, body),
            AuthDecision::NeedsAccountFetch { user_id, pubkey } => {
                let result = match self.fetch_account_for_auth(&pubkey).await {
                    Some((data, program_owner)) => {
                        check_account_data_ownership(
                            &data,
                            &program_owner,
                            &pubkey,
                            user_id,
                            auth_db,
                        )
                        .await
                    }
                    None => AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body()),
                };
                match result {
                    AuthDecision::Proceed => return None,
                    AuthDecision::Reject(status, body) => (status, body),
                    AuthDecision::NeedsAccountFetch { .. } => unreachable!(),
                }
            }
        };

        Self::record_metrics(
            Some("auth_rejected"),
            method_label,
            "none",
            &status.as_u16().to_string(),
            start.elapsed().as_secs_f64(),
        );
        Some(self.error_response(status, Some(body)))
    }

    /// Build a JSON-RPC–style error body for 413 responses.
    fn payload_too_large_body() -> Bytes {
        Bytes::from(
            serde_json::json!({
                "error": {
                    "code": INVALID_REQUEST_CODE,
                    "message": format!("Request body exceeds maximum size of {} bytes", MAX_BODY_SIZE)
                }
            })
            .to_string(),
        )
    }

    async fn handle_request(
        self: Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<
        Response<http_body_util::combinators::UnsyncBoxBody<Bytes, hyper::Error>>,
        hyper::Error,
    > {
        let start = Instant::now();

        if req.method() == hyper::Method::OPTIONS {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(
                    "Access-Control-Allow-Origin",
                    self.cors_allowed_origin.as_str(),
                )
                .header("Access-Control-Allow-Methods", "POST, OPTIONS")
                .header(
                    "Access-Control-Allow-Headers",
                    "Content-Type, Authorization, solana-client",
                )
                .header("Access-Control-Max-Age", "86400")
                .body(Empty::new().map_err(|never| match never {}).boxed_unsync())
                .unwrap());
        }

        // Shallow liveness check — verifies the gateway process is running.
        // Does not probe backend read/write nodes.
        if req.method() == hyper::Method::GET && req.uri().path() == "/health" {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header(
                    "Access-Control-Allow-Origin",
                    self.cors_allowed_origin.as_str(),
                )
                .body(
                    Full::new(Bytes::from(r#"{"status":"ok"}"#))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .unwrap());
        }

        // Readiness check — probes both upstreams. For external monitoring only;
        // compose's healthcheck stays on /health so a backend outage doesn't
        // cause the gateway to be restarted (which wouldn't help).
        if req.method() == hyper::Method::GET && req.uri().path() == "/ready" {
            let healthy = self.check_ready().await;
            let (status, body) = if healthy {
                (StatusCode::OK, r#"{"status":"ready"}"#)
            } else {
                (StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"degraded"}"#)
            };
            return Ok(Response::builder()
                .status(status)
                .header("Content-Type", "application/json")
                .header(
                    "Access-Control-Allow-Origin",
                    self.cors_allowed_origin.as_str(),
                )
                .body(
                    Full::new(Bytes::from(body))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .unwrap());
        }

        if req.method() != hyper::Method::POST {
            Self::record_metrics(
                Some("method_not_allowed"),
                "unknown",
                "none",
                "405",
                start.elapsed().as_secs_f64(),
            );
            return Ok(self.error_response(StatusCode::METHOD_NOT_ALLOWED, None));
        }

        if let Some(content_length) = req.headers().get(hyper::header::CONTENT_LENGTH) {
            match content_length
                .to_str()
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(len) if len > MAX_BODY_SIZE => {
                    warn!(
                        "Request body too large: Content-Length {} exceeds limit of {} bytes",
                        len, MAX_BODY_SIZE
                    );
                    Self::record_metrics(
                        Some("payload_too_large"),
                        "unknown",
                        "none",
                        "413",
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(self.error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Some(Self::payload_too_large_body()),
                    ));
                }
                None => {
                    warn!("Unparseable Content-Length header: {:?}", content_length);
                }
                _ => {}
            }
        }

        // Extract the Authorization header as an owned String before req is
        // consumed by into_body(). Needed for the auth check after JSON parsing.
        let auth_header = req
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let limited_body = Limited::new(req.into_body(), MAX_BODY_SIZE);
        let body_bytes = match limited_body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                if e.downcast_ref::<LengthLimitError>().is_some() {
                    warn!(
                        "Request body exceeded size limit of {} bytes",
                        MAX_BODY_SIZE
                    );
                    Self::record_metrics(
                        Some("payload_too_large"),
                        "unknown",
                        "none",
                        "413",
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(self.error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Some(Self::payload_too_large_body()),
                    ));
                }
                warn!("Failed to read request body: {}", e);
                Self::record_metrics(
                    Some("bad_json"),
                    "unknown",
                    "none",
                    "400",
                    start.elapsed().as_secs_f64(),
                );
                return Ok(self.error_response(StatusCode::BAD_REQUEST, None));
            }
        };

        let json: Value = match serde_json::from_slice(&body_bytes) {
            Ok(json) => json,
            Err(e) => {
                warn!("Invalid JSON: {}", e);
                Self::record_metrics(
                    Some("bad_json"),
                    "unknown",
                    "none",
                    "400",
                    start.elapsed().as_secs_f64(),
                );
                return Ok(self.error_response(StatusCode::BAD_REQUEST, None));
            }
        };

        let method = match json.get("method").and_then(|m| m.as_str()) {
            Some(method) => method,
            None => {
                warn!("Missing or invalid 'method' field in JSON-RPC request");
                Self::record_metrics(
                    Some("invalid_method"),
                    "unknown",
                    "none",
                    "400",
                    start.elapsed().as_secs_f64(),
                );
                return Ok(self.error_response(StatusCode::BAD_REQUEST, None));
            }
        };

        let method_label = if KNOWN_RPC_METHODS.contains(&method) {
            method
        } else {
            "unknown"
        };

        // --- RBAC enforcement ---
        let params = json.get("params").cloned().unwrap_or(Value::Null);
        if let Some(rejection) = self
            .enforce_auth(auth_header.as_deref(), method, method_label, &params, start)
            .await
        {
            return Ok(rejection);
        }

        let (target_url, target_label) = if method == "sendTransaction" {
            info!("Routing sendTransaction to write node");
            (&self.write_url, "write")
        } else {
            info!("Routing {} to read node", method);
            (&self.read_url, "read")
        };

        let uri = match target_url.parse::<hyper::Uri>() {
            Ok(uri) => uri,
            Err(e) => {
                error!("Invalid target URL {}: {}", target_url, e);
                Self::record_metrics(
                    Some("url_parse"),
                    method_label,
                    target_label,
                    "500",
                    start.elapsed().as_secs_f64(),
                );
                return Ok(self.error_response(StatusCode::INTERNAL_SERVER_ERROR, None));
            }
        };

        let forwarded_req = match Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(Full::new(body_bytes))
        {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to build forwarded request: {}", e);
                Self::record_metrics(
                    Some("request_build"),
                    method_label,
                    target_label,
                    "500",
                    start.elapsed().as_secs_f64(),
                );
                return Ok(self.error_response(StatusCode::INTERNAL_SERVER_ERROR, None));
            }
        };

        match self.client.request(forwarded_req).await {
            Ok(response) => {
                let status = response.status().as_u16().to_string();
                info!(
                    "Forwarded to {} - Status: {}",
                    target_url,
                    response.status()
                );
                Self::record_metrics(
                    None,
                    method_label,
                    target_label,
                    &status,
                    start.elapsed().as_secs_f64(),
                );

                let (mut parts, body) = response.into_parts();
                parts.headers.insert(
                    "Access-Control-Allow-Origin",
                    hyper::header::HeaderValue::from_str(&self.cors_allowed_origin).unwrap(),
                );
                parts.headers.insert(
                    "Access-Control-Allow-Methods",
                    hyper::header::HeaderValue::from_static("POST, OPTIONS"),
                );
                parts.headers.insert(
                    "Access-Control-Allow-Headers",
                    hyper::header::HeaderValue::from_static(
                        "Content-Type, Authorization, solana-client",
                    ),
                );
                Ok(Response::from_parts(parts, body.boxed_unsync()))
            }
            Err(e) => {
                error!("Failed to forward request to {}: {}", target_url, e);
                Self::record_metrics(
                    Some("backend_error"),
                    method_label,
                    target_label,
                    "502",
                    start.elapsed().as_secs_f64(),
                );
                Ok(self.error_response(StatusCode::BAD_GATEWAY, None))
            }
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    gateway: Arc<Gateway>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Gateway listening on http://{}", listener.local_addr()?);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let gateway = Arc::clone(&gateway);

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let gateway = Arc::clone(&gateway);
                async move { gateway.handle_request(req).await }
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                error!("Error serving connection: {:?}", err);
            }
        });
    }
}

pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting PrivateChannel Gateway");
    info!("  Port: {}", args.port);
    info!("  Write URL: {}", args.write_url);
    info!("  Read URL: {}", args.read_url);
    info!("  CORS Allowed Origin: {}", args.cors_allowed_origin);
    info!(
        "  Auth enforcement: {}",
        if configured_secret(args.jwt_secret.as_deref()).is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    // Refuse to start if JWT_SECRET is set without AUTH_DATABASE_URL.
    //
    // Auth is intentionally optional: both absent means "run without auth" and
    // is valid for local dev. But JWT_SECRET present without AUTH_DATABASE_URL
    // is a misconfiguration — enforce_auth silently falls through to its wildcard
    // arm and returns None, disabling all enforcement with no indication. An
    // operator who sets JWT_SECRET believes auth is active; failing here at boot
    // ensures that belief is correct rather than every request passing through
    // unguarded at runtime.
    if configured_secret(args.jwt_secret.as_deref()).is_some() && args.auth_database_url.is_none() {
        return Err(
            "JWT_SECRET is set but AUTH_DATABASE_URL is not configured. \
             Auth enforcement requires both. Either provide AUTH_DATABASE_URL \
             or unset JWT_SECRET to run without auth."
                .into(),
        );
    }

    // Connect to the auth DB if a URL was provided.
    // This pool is used for per-request wallet ownership checks.
    let auth_db = match args.auth_database_url {
        Some(ref url) => {
            let pool = PgPoolOptions::new()
                .max_connections(args.auth_database_max_connections)
                .connect(url)
                .await?;
            info!(
                "  Auth DB: connected (max_connections={})",
                args.auth_database_max_connections
            );
            Some(pool)
        }
        None => {
            info!("  Auth DB: not configured");
            None
        }
    };

    let gateway = Arc::new(Gateway::new(
        args.write_url,
        args.read_url,
        args.cors_allowed_origin,
        args.jwt_secret,
        auth_db,
    ));

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    let listener = TcpListener::bind(addr).await?;

    serve(listener, gateway).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// Spawn a test gateway with configurable backend URLs.
    /// Each invocation binds to a unique port via port 0 (OS-assigned).
    async fn start_gateway_with_urls(write_url: &str, read_url: &str) -> SocketAddr {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();

        let gateway = Arc::new(Gateway::new(
            write_url.to_string(),
            read_url.to_string(),
            "*".to_string(),
            None, // no auth enforcement in these tests
            None,
        ));

        // Port 0 lets the OS assign a unique free port; avoids collisions between concurrent tests.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let _ = serve(listener, gateway).await;
        });

        addr
    }

    async fn start_test_gateway() -> SocketAddr {
        start_gateway_with_urls("http://127.0.0.1:1", "http://127.0.0.1:1").await
    }

    /// Spawn a minimal HTTP/1.1 backend that replies with a static 200 response body.
    /// Accepts multiple requests in a loop to handle tests that may send more than one request.
    async fn start_mock_http_backend(response_body: &'static str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            // Accept with timeout to prevent indefinite blocking when test exits
            while let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_secs(5), listener.accept()).await
            {
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }
        });

        addr
    }

    /// Send raw bytes to the test gateway and return the response as a string.
    async fn send_raw(addr: SocketAddr, data: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(data).await.unwrap();

        // Buffer for reading response from gateway (8KB safely handles all test cases).
        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    /// Assert the response status line contains the expected HTTP status code.
    fn assert_status(response: &str, expected: u16) {
        let status_line = response.split("\r\n").next().unwrap_or("");
        let code = expected.to_string();
        assert!(
            status_line.contains(&code),
            "Expected {expected} in status line, got: {status_line}"
        );
    }

    #[tokio::test]
    async fn rejects_content_length_over_64kb() {
        let addr = start_test_gateway().await;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            65 * 1024
        );
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 413);
    }

    #[tokio::test]
    async fn rejects_oversized_body_without_content_length() {
        let addr = start_test_gateway().await;

        // Build a chunked request with >64KB of data (no Content-Length header)
        let chunk_size = 65 * 1024;
        let mut raw = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
            chunk_size
        )
        .into_bytes();
        raw.extend(vec![b'A'; chunk_size]);
        raw.extend_from_slice(b"\r\n0\r\n\r\n");

        let response = send_raw(addr, &raw).await;
        assert_status(&response, 413);
    }

    #[tokio::test]
    async fn accepts_body_at_exactly_64kb() {
        let addr = start_test_gateway().await;

        // Send exactly MAX_BODY_SIZE bytes — must NOT be rejected as 413
        let body = vec![b'A'; MAX_BODY_SIZE];
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len(),
        );
        let mut raw = req.into_bytes();
        raw.extend_from_slice(&body);

        let response = send_raw(addr, &raw).await;
        let status_line = response.split("\r\n").next().unwrap_or("");
        assert!(
            !status_line.contains("413"),
            "Body at exactly 64KB must not be rejected as too large, got: {}",
            status_line
        );
    }

    #[tokio::test]
    async fn rejects_oversized_body_despite_small_content_length() {
        let addr = start_test_gateway().await;

        // Lie: claim Content-Length: 100 but send 65KB of data
        let oversized = vec![b'A'; 65 * 1024];
        let header = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n";
        let mut raw = header.as_bytes().to_vec();
        raw.extend_from_slice(&oversized);

        let response = send_raw(addr, &raw).await;
        let status_line = response.split("\r\n").next().unwrap_or("");
        assert!(
            status_line.contains("413") || status_line.contains("400"),
            "Lying Content-Length with oversized body should be rejected, got: {}",
            status_line
        );
    }

    #[tokio::test]
    async fn accepts_normal_sized_request() {
        let addr = start_test_gateway().await;
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );

        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 502);
    }

    #[tokio::test]
    async fn options_request_returns_200_with_cors_headers() {
        let addr = start_test_gateway().await;
        let req = "OPTIONS / HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 200);
        let lower = response.to_lowercase();
        assert!(
            lower.contains("access-control-allow-origin"),
            "CORS origin header missing from OPTIONS response: {response}"
        );
        assert!(
            lower.contains("access-control-allow-methods"),
            "CORS methods header missing from OPTIONS response: {response}"
        );
    }

    #[tokio::test]
    async fn get_health_returns_200_with_status_ok() {
        let addr = start_test_gateway().await;
        let req = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 200);
        assert!(
            response.contains(r#""status":"ok""#),
            "Health response must contain status:ok body, got: {response}"
        );
    }

    #[tokio::test]
    async fn non_post_non_options_returns_405() {
        let addr = start_test_gateway().await;
        let req = "PUT / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 405);
    }

    #[tokio::test]
    async fn invalid_json_body_returns_400() {
        let addr = start_test_gateway().await;
        let body = b"not valid json";
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut raw = req.into_bytes();
        raw.extend_from_slice(body);
        let response = send_raw(addr, &raw).await;
        assert_status(&response, 400);
    }

    #[tokio::test]
    async fn missing_method_field_returns_400() {
        let addr = start_test_gateway().await;
        let body = r#"{"jsonrpc":"2.0","id":1}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 400);
    }

    #[tokio::test]
    async fn send_transaction_attempts_write_node() {
        let addr = start_test_gateway().await;
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["AAAA"]}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        // Both URLs point to a closed port — gateway must attempt forwarding and return 502
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 502);
    }

    #[tokio::test]
    async fn unknown_rpc_method_attempts_read_node() {
        let addr = start_test_gateway().await;
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"customUnknownMethod"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        // Unknown method uses "unknown" label; routing attempt to unreachable read node → 502
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 502);
    }

    #[tokio::test]
    async fn invalid_backend_url_returns_500() {
        // "http://[" is an invalid URI (unclosed IPv6 bracket) — triggers URL parse error path
        let addr = start_gateway_with_urls("http://[", "http://[").await;
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 500);
    }

    /// The `serve()` function should bind, accept connections, and route requests.
    /// Uses a pre-bound listener (port 0) to avoid TOCTOU race.
    #[tokio::test]
    async fn run_binds_and_serves_requests() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gateway = Arc::new(Gateway::new(
            "http://127.0.0.1:1".to_string(),
            "http://127.0.0.1:1".to_string(),
            "*".to_string(),
            None, // no auth enforcement in this test
            None,
        ));
        let handle = tokio::spawn(async move {
            let _ = serve(listener, gateway).await;
        });

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = send_raw(addr, req.as_bytes()).await;
        // Backend is unreachable (port 1) → gateway returns 502.
        assert_status(&response, 502);
        handle.abort();
    }

    /// Invalid Content-Length headers are rejected by Hyper at the HTTP layer.
    /// This test verifies the gateway doesn't crash and returns a proper error response.
    #[tokio::test]
    async fn invalid_content_length_returns_400() {
        let addr = start_test_gateway().await;

        // Hyper's HTTP/1.1 parser validates headers and rejects malformed Content-Length.
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: invalid_integer\r\n\r\n{}",
            body
        );
        let response = send_raw(addr, req.as_bytes()).await;
        // Hyper rejects invalid headers at the HTTP layer → 400 Bad Request.
        assert_status(&response, 400);
    }

    #[tokio::test]
    async fn successful_backend_response_includes_cors_headers() {
        let backend_addr = start_mock_http_backend(r#"{"result":42}"#).await;
        let read_url = format!("http://{backend_addr}");
        let addr = start_gateway_with_urls("http://127.0.0.1:1", &read_url).await;

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 200);
        assert!(
            response
                .to_lowercase()
                .contains("access-control-allow-origin"),
            "CORS header must be present in forwarded response: {response}"
        );
    }

    #[tokio::test]
    async fn send_transaction_routes_to_write_node_mock() {
        let backend_addr = start_mock_http_backend(r#"{"result":"sig123"}"#).await;
        let write_url = format!("http://{backend_addr}");
        let addr = start_gateway_with_urls(&write_url, "http://127.0.0.1:1").await;

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["AAAA"]}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 200);
        assert!(
            response.contains("sig123"),
            "response should contain backend body"
        );
    }

    #[tokio::test]
    async fn payload_too_large_body_contains_error_json() {
        let addr = start_test_gateway().await;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            65 * 1024
        );
        let response = send_raw(addr, req.as_bytes()).await;
        assert_status(&response, 413);
        assert!(
            response.contains("exceeds maximum size"),
            "413 body should explain the limit: {response}"
        );
    }

    #[tokio::test]
    async fn known_read_methods_route_to_read_node() {
        let backend_addr = start_mock_http_backend(r#"{"result":"ok"}"#).await;
        let read_url = format!("http://{backend_addr}");
        let addr = start_gateway_with_urls("http://127.0.0.1:1", &read_url).await;

        for method in &[
            "getAccountInfo",
            "getTransaction",
            "getLatestBlockhash",
            "getEpochInfo",
        ] {
            let body = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{}"}}"#, method);
            let req = format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let response = send_raw(addr, req.as_bytes()).await;
            assert_status(&response, 200);
        }
    }
}
