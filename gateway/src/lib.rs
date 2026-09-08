pub mod auth;
pub mod db;
pub mod metrics;

use crate::auth::{
    auth_unavailable_body, check_account_data_ownership, check_request_auth, decode_account_data,
    forbidden_body, is_gated, redacts_transaction_errors, redacts_transaction_errors_for,
    role_check_error_body, verify_bearer, AuthDecision, Role,
};
use crate::db::get_user_role;
use clap::Parser;
use governor::clock::DefaultClock;
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter};
use http_body_util::{BodyExt, Empty, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use jsonrpsee::types::error::INVALID_REQUEST_CODE;
use jsonwebtoken::DecodingKey;
use serde_json::Value;
use socket2::{SockRef, TcpKeepalive};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Maximum allowed request body size (64 KB).
const MAX_BODY_SIZE: usize = 64 * 1024;

const KNOWN_RPC_METHODS: &[&str] = &[
    "sendTransaction",
    "getAccountInfo",
    "getSlot",
    "getBlockHeight",
    "getBlock",
    "getTransaction",
    "getRecentBlockhash",
    "getTokenAccountBalance",
    "getLatestBlockhash",
    "getSignatureStatuses",
    "getTransactionCount",
    "getFirstAvailableBlock",
    "getBlocks",
    "getBlocksWithLimit",
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

    /// Port for the internal listener, which serves the operator's own services:
    /// no RBAC, no error redaction. Unset or blank means no internal listener.
    ///
    /// It must never be reachable from outside the mesh. Inside a container it
    /// binds all interfaces like the main port; what keeps it internal is that
    /// nothing publishes it to the host.
    ///
    /// Held as a string so a blank value is not a parse error: see
    /// `configured_internal_port`.
    #[arg(long, env = "GATEWAY_INTERNAL_PORT")]
    pub internal_port: Option<String>,

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

    /// Maximum number of concurrent client connections per listener. Connections
    /// beyond this are dropped so a flood cannot exhaust file descriptors or memory.
    #[arg(long, env = "GATEWAY_MAX_CONNECTIONS", default_value = "1024")]
    pub max_connections: NonZeroUsize,

    /// Maximum concurrent connections from a single client IP.
    #[arg(long, env = "GATEWAY_MAX_CONNECTIONS_PER_IP", default_value = "64")]
    pub max_connections_per_ip: NonZeroUsize,

    /// Seconds a client may take to send the full request header block before
    /// the connection is closed (slowloris protection). Must be non-zero; a
    /// zero timeout would fail every request instantly.
    #[arg(
        long,
        env = "GATEWAY_HEADER_READ_TIMEOUT_SECS",
        default_value = "10",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub header_read_timeout_secs: u64,

    /// Seconds to read the full request body once headers are in. Must be
    /// non-zero; a zero timeout would fail every request instantly.
    #[arg(
        long,
        env = "GATEWAY_BODY_READ_TIMEOUT_SECS",
        default_value = "15",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub body_read_timeout_secs: u64,

    /// Seconds the ownership account fetch may take against the read node before
    /// a gated request is failed with 503. Must be non-zero.
    #[arg(
        long,
        env = "GATEWAY_AUTH_FETCH_TIMEOUT_SECS",
        default_value = "3",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub auth_fetch_timeout_secs: u64,

    /// Idle seconds before the OS starts sending TCP keepalive probes. Must be
    /// non-zero.
    #[arg(
        long,
        env = "GATEWAY_TCP_KEEPALIVE_IDLE_SECS",
        default_value = "60",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub tcp_keepalive_idle_secs: u64,

    /// Seconds between TCP keepalive probes. Must be non-zero.
    #[arg(
        long,
        env = "GATEWAY_TCP_KEEPALIVE_INTERVAL_SECS",
        default_value = "15",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub tcp_keepalive_interval_secs: u64,

    /// Sustained request rate allowed per client IP (requests per second).
    #[arg(long, env = "GATEWAY_RATE_LIMIT_PER_SECOND", default_value = "50")]
    pub rate_limit_per_second: NonZeroU32,

    /// Burst capacity per client IP (token bucket size).
    #[arg(long, env = "GATEWAY_RATE_LIMIT_BURST", default_value = "100")]
    pub rate_limit_burst: NonZeroU32,
}

/// Which trust tier a listener serves. The gateway's RBAC is an application
/// control on the public port; the internal port is a network boundary instead,
/// and the two must not be confused.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Access {
    /// Publicly reachable. RBAC is enforced and transaction errors are collapsed
    /// for everyone but an Operator.
    Public,
    /// Reachable only from the internal network. No RBAC and raw errors: the
    /// operator services carry no JWT, and their confirmation handling routes on
    /// the exact `TransactionError` (an `AccountAlreadyInitialized` means a mint
    /// is already there, an `UninitializedAccount` triggers the JIT mint init).
    Internal,
}

/// Tunable resource limits for the serve loop and request handling.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Max concurrent client connections per listener. Connections past this are
    /// dropped so a flood cannot exhaust file descriptors or memory. Per
    /// listener, not per process: with the internal listener enabled the process
    /// accepts up to twice this, though only the public one is floodable.
    pub max_connections: NonZeroUsize,
    /// Max concurrent connections from a single client IP, so one host cannot
    /// consume the whole global connection budget.
    pub max_connections_per_ip: NonZeroUsize,
    /// Max time a client may take to send the full request header block.
    /// Slowloris header-trickle connections are closed after this.
    pub header_read_timeout: Duration,
    /// Max time to read the full request body once headers are in. Bounds
    /// slow-body (trickle) clients that the header timeout doesn't cover.
    pub body_read_timeout: Duration,
    /// Max time the ownership account fetch may take against the read node,
    /// covering the request and the response body together. A hung read node
    /// fails the gated request with 503 instead of parking it.
    pub auth_fetch_timeout: Duration,
    /// Idle time before the OS starts sending TCP keepalive probes.
    pub tcp_keepalive_idle: Duration,
    /// Interval between TCP keepalive probes.
    pub tcp_keepalive_interval: Duration,
    /// Sustained request rate allowed per client IP (requests per second).
    pub rate_limit_per_second: NonZeroU32,
    /// Burst capacity per client IP, i.e. the token bucket size.
    pub rate_limit_burst: NonZeroU32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(1024).unwrap(),
            max_connections_per_ip: NonZeroUsize::new(64).unwrap(),
            header_read_timeout: Duration::from_secs(10),
            body_read_timeout: Duration::from_secs(15),
            auth_fetch_timeout: Duration::from_secs(3),
            tcp_keepalive_idle: Duration::from_secs(60),
            tcp_keepalive_interval: Duration::from_secs(15),
            rate_limit_per_second: NonZeroU32::new(50).unwrap(),
            rate_limit_burst: NonZeroU32::new(100).unwrap(),
        }
    }
}

pub struct Gateway {
    write_url: String,
    read_url: String,
    cors_allowed_origin: String,
    limits: Limits,
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
    /// Cached auth-DB role confirmations, keyed by user id. Lets a token's
    /// Operator claim be re-checked without a DB lookup on every request.
    role_cache: Arc<Mutex<HashMap<Uuid, CachedRole>>>,
}

#[derive(Clone, Copy)]
struct ReadyCache {
    checked_at: Instant,
    healthy: bool,
}

#[derive(Clone, Copy)]
struct CachedRole {
    is_operator: bool,
    checked_at: Instant,
}

const READY_CACHE_TTL: Duration = Duration::from_secs(2);
const READY_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// How long a DB role confirmation is trusted before re-checking. Bounds the
/// window a demoted operator keeps access to at most this duration.
const ROLE_CACHE_TTL: Duration = Duration::from_secs(30);

/// Outcome of the ownership account fetch. `NotFound` means the read node
/// answered and the account does not exist, which is a real 403. `Unavailable`
/// means we never got a usable answer, which must not be reported to the caller
/// as an ownership failure.
enum AccountFetch {
    Found {
        data: Vec<u8>,
        program_owner: String,
    },
    NotFound,
    Unavailable,
}

/// Tracks how many connections each client IP currently holds. Entries are
/// removed when an IP's count reaches zero, so the map only holds IPs with a
/// live connection and stays bounded by the global connection cap.
type IpConnCounts = Arc<Mutex<HashMap<IpAddr, usize>>>;

/// Per-IP request rate limiter (token bucket). Idle IPs are pruned periodically
/// by `retain_recent` so the keyed store stays bounded.
type IpRateLimiter = RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>;

/// True for accept errors that reflect one failed connection (the peer went
/// away before we accepted it) rather than a problem with the listener. These
/// are safe to skip without any backoff.
fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Collapses a peer address to the key the rate limiter buckets by. IPv4 is
/// used as-is; IPv6 is masked to its /64 prefix. A single client is routinely
/// handed a whole /64, so without masking it could mint a fresh key per request
/// and bloat the limiter's keyed store faster than pruning reclaims it.
fn rate_limit_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => {
            let mut octets = v6.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

/// Increments the connection count for `ip` when it is below `max`, returning a
/// guard that decrements it on drop. Returns None when the IP is at the cap.
fn try_acquire_ip(counts: &IpConnCounts, ip: IpAddr, max: usize) -> Option<IpConnGuard> {
    let mut map = counts.lock().unwrap();
    let count = map.entry(ip).or_insert(0);
    if *count >= max {
        return None;
    }
    *count += 1;
    Some(IpConnGuard {
        counts: Arc::clone(counts),
        ip,
    })
}

/// Releases one per-IP connection slot on drop.
struct IpConnGuard {
    counts: IpConnCounts,
    ip: IpAddr,
}

impl Drop for IpConnGuard {
    fn drop(&mut self) {
        let mut map = self.counts.lock().unwrap();
        if let Some(count) = map.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.ip);
            }
        }
    }
}

/// A `JWT_SECRET` counts as "configured" only if non-empty after trimming, mirroring the
/// auth service so a whitespace-only secret doesn't enable gateway RBAC while auth refuses
/// to start.
fn configured_secret(secret: Option<&str>) -> Option<&str> {
    secret.filter(|s| !s.trim().is_empty())
}

/// A blank `GATEWAY_INTERNAL_PORT` means "no internal listener", the same way a
/// blank `JWT_SECRET` means "no auth". Compose substitutes an unset variable as
/// the empty string, so an env file that predates the internal listener must
/// leave the gateway running without one rather than fail to start.
fn configured_internal_port(port: Option<&str>) -> Result<Option<u16>, Box<dyn std::error::Error>> {
    let Some(port) = port.map(str::trim).filter(|port| !port.is_empty()) else {
        return Ok(None);
    };
    let parsed = port
        .parse::<u16>()
        .map_err(|e| format!("GATEWAY_INTERNAL_PORT is not a valid port number: {e}"))?;
    Ok(Some(parsed))
}

/// Replaces every non-null transaction error with one constant marker.
/// Transactions are indexed under every account they touched and their status is
/// readable by anyone holding the signature, so distinct error codes let a
/// caller who owns only the destination of a failed transfer binary-search the
/// source's balance.
///
/// Covers both response shapes: `getSignaturesForAddress` returns a bare array,
/// `getSignatureStatuses` wraps its entries in `result.value` and repeats the
/// same error in a legacy `status` field. The marker is a valid
/// `TransactionError` so clients still parse the page. A body with neither shape
/// carries no error and passes through.
fn redact_transaction_errors(body: Bytes) -> Bytes {
    let Ok(mut json) = serde_json::from_slice::<Value>(&body) else {
        warn!("History response was not JSON, forwarding it unchanged");
        return body;
    };
    let entries = match json.get_mut("result") {
        Some(Value::Array(entries)) => entries,
        Some(result) => match result
            .get_mut("value")
            .and_then(|value| value.as_array_mut())
        {
            Some(entries) => entries,
            None => return body,
        },
        None => return body,
    };
    for entry in entries {
        if let Some(err) = entry.get_mut("err").filter(|err| !err.is_null()) {
            *err = redacted_error();
        }
        // Leaving the legacy `status` untouched would hand back the same value
        // under a different key.
        if let Some(err) = entry
            .get_mut("status")
            .and_then(|status| status.get_mut("Err"))
        {
            *err = redacted_error();
        }
    }
    Bytes::from(json.to_string())
}

/// The single error every collapsed transaction reports.
fn redacted_error() -> Value {
    serde_json::json!({ "InstructionError": [0, "GenericError"] })
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
            limits: Limits::default(),
            client,
            jwt_secret,
            auth_db,
            ready_cache: Arc::new(AsyncMutex::new(None)),
            role_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Overrides the default resource limits.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
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
    /// Only a `result.value` of `null` counts as `NotFound`; every other way of
    /// not getting an answer is `Unavailable`, so an upstream outage is never
    /// reported to the caller as "you don't own this account".
    async fn fetch_account_for_auth(&self, pubkey: &str) -> AccountFetch {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            // Request base64 encoding so we get the raw bytes back as a string.
            "params": [pubkey, { "encoding": "base64" }]
        })
        .to_string();

        let Ok(uri) = self.read_url.parse::<hyper::Uri>() else {
            error!("Read node URL is not a valid URI: {}", self.read_url);
            return AccountFetch::Unavailable;
        };
        let Ok(req) = Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body)))
        else {
            return AccountFetch::Unavailable;
        };

        // One deadline for the request and the body read together, so a hung read
        // node cannot park a gated request. probe_upstream bounds the same call.
        //
        // Each stage names itself in its error, and the causes are formatted with
        // Debug: hyper's client error displays as a bare "client error (Connect)"
        // and keeps the real cause (refused, DNS, TLS) in its source chain.
        let fetched = tokio::time::timeout(self.limits.auth_fetch_timeout, async {
            let response = self
                .client
                .request(req)
                .await
                .map_err(|e| format!("request failed: {e:?}"))?;
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|e| format!("response body read failed: {e:?}"))?;
            Ok::<Bytes, String>(body.to_bytes())
        })
        .await;

        let body_bytes = match fetched {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(cause)) => {
                warn!(
                    "Ownership account fetch failed against {}: {}",
                    self.read_url, cause
                );
                return AccountFetch::Unavailable;
            }
            Err(_) => {
                warn!(
                    "Ownership account fetch timed out after {:?}",
                    self.limits.auth_fetch_timeout
                );
                return AccountFetch::Unavailable;
            }
        };

        let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) else {
            warn!("Ownership account fetch returned a non-JSON response");
            return AccountFetch::Unavailable;
        };

        // getAccountInfo returns null for result.value when the account doesn't
        // exist. A missing result means the node answered with a JSON-RPC error,
        // which says nothing about ownership.
        let Some(value) = json.get("result").and_then(|result| result.get("value")) else {
            warn!("Ownership account fetch returned no result: {}", json);
            return AccountFetch::Unavailable;
        };
        if value.is_null() {
            return AccountFetch::NotFound;
        }

        // The program that owns this account — used to confirm it is a token account.
        let Some(program_owner) = value.get("owner").and_then(|owner| owner.as_str()) else {
            warn!("Ownership account fetch returned an account with no owner");
            return AccountFetch::Unavailable;
        };

        // data is [base64_string, encoding_name] — we want index 0.
        let Some(encoded) = value
            .get("data")
            .and_then(|data| data.get(0))
            .and_then(|data| data.as_str())
        else {
            warn!("Ownership account fetch returned an account with no base64 data");
            return AccountFetch::Unavailable;
        };
        let Some(data) = decode_account_data(encoded) else {
            warn!("Ownership account fetch returned undecodable account data");
            return AccountFetch::Unavailable;
        };

        AccountFetch::Found {
            data,
            program_owner: program_owner.to_owned(),
        }
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

    /// Confirms whether `user_id` currently holds the Operator role in the auth
    /// DB, caching the result for `ROLE_CACHE_TTL`. A JWT can outlive a demotion
    /// by up to 24h, so an Operator claim is only honored when this returns
    /// `true`.
    ///
    /// Returns `Err` if the DB is unreachable; the caller fails closed (denies
    /// the operator elevation) rather than trusting a stale claim.
    async fn confirm_operator(&self, auth_db: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
        // Fast path: a fresh cached confirmation avoids the DB round-trip.
        // The lock is never held across the await below.
        if let Some(entry) = self.role_cache.lock().unwrap().get(&user_id) {
            if entry.checked_at.elapsed() < ROLE_CACHE_TTL {
                return Ok(entry.is_operator);
            }
        }

        // Stamp the check time before the query so a slow lookup can't cache a
        // stale role under a fresh timestamp; staleness stays bounded by the TTL
        // even when concurrent requests race on the same user.
        let checked_at = Instant::now();
        let is_operator = matches!(get_user_role(auth_db, user_id).await?, Some(Role::Operator));

        let mut cache = self.role_cache.lock().unwrap();
        // Evict expired entries so the map stays bounded to recently-active operators.
        cache.retain(|_, entry| entry.checked_at.elapsed() < ROLE_CACHE_TTL);
        cache.insert(
            user_id,
            CachedRole {
                is_operator,
                checked_at,
            },
        );
        Ok(is_operator)
    }

    /// Records an auth-rejection metric and builds the error response.
    fn reject_with_metrics(
        &self,
        method_label: &str,
        status: StatusCode,
        body: Bytes,
        start: Instant,
    ) -> Response<http_body_util::combinators::UnsyncBoxBody<Bytes, hyper::Error>> {
        Self::record_metrics(
            Some("auth_rejected"),
            method_label,
            "none",
            &status.as_u16().to_string(),
            start.elapsed().as_secs_f64(),
        );
        self.error_response(status, Some(body))
    }

    /// Enforces RBAC on gated methods.
    ///
    /// Returns `Some(response)` if the request must be rejected, `None` if it may
    /// proceed. No-ops immediately when auth is not configured. Operator claims
    /// are re-checked against the auth DB (cached), so a demoted operator loses
    /// access within `ROLE_CACHE_TTL` rather than when their 24h token expires.
    ///
    /// Returns `Err(response)` if the request must be rejected. `Ok(redact)` if
    /// it may proceed, where `redact` marks a response whose transaction errors
    /// must be collapsed. No-ops immediately when auth is not configured.
    async fn enforce_auth(
        &self,
        auth_header: Option<&str>,
        method: &str,
        method_label: &str,
        params: &Value,
        start: Instant,
    ) -> Result<bool, Response<http_body_util::combinators::UnsyncBoxBody<Bytes, hyper::Error>>>
    {
        // Auth off, so nothing is redacted either. Every method is ungated in this
        // mode, so a caller reads the balance straight from getTokenAccountBalance
        // instead of inferring it from error codes. Redacting would also hit the
        // operator services, since with no key we cannot tell who is an Operator.
        let (decoding_key, auth_db) = match (&self.jwt_secret, &self.auth_db) {
            (Some(k), Some(db)) => (k, db),
            _ => return Ok(false),
        };

        // Public methods need neither a token nor a role check, but redaction is a
        // separate axis: getSignatureStatuses is ungated and still error-bearing.
        if !is_gated(method) {
            return Ok(redacts_transaction_errors(
                auth_header,
                decoding_key,
                method,
            ));
        }

        let mut claims = verify_bearer(auth_header, decoding_key);

        // Re-check an Operator claim against the DB and downgrade to User when
        // the role was revoked. Fail closed if the DB can't be reached.
        if let Some(caller) = claims.as_mut() {
            if caller.role == Role::Operator {
                let confirmed = match Uuid::parse_str(&caller.sub) {
                    Ok(user_id) => self.confirm_operator(auth_db, user_id).await,
                    // A malformed sub can't map to an operator row.
                    Err(_) => Ok(false),
                };
                match confirmed {
                    Ok(true) => {}                         // still an operator
                    Ok(false) => caller.role = Role::User, // demoted -> effective User
                    Err(_) => {
                        return Err(self.reject_with_metrics(
                            method_label,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            role_check_error_body(),
                            start,
                        ));
                    }
                }
            }
        }

        let decision = check_request_auth(claims.as_ref(), method, params);
        // Independent of the decision: authorizing the request says nothing
        // about whether the caller may see why execution failed.
        let redact = redacts_transaction_errors_for(claims.as_ref(), method);

        let (status, body) = match decision {
            AuthDecision::Proceed => return Ok(redact),
            AuthDecision::Reject(status, body) => (status, body),
            AuthDecision::NeedsAccountFetch { user_id, pubkey } => {
                let result = match self.fetch_account_for_auth(&pubkey).await {
                    AccountFetch::Found {
                        data,
                        program_owner,
                    } => {
                        check_account_data_ownership(
                            &data,
                            &program_owner,
                            &pubkey,
                            method,
                            user_id,
                            auth_db,
                        )
                        .await
                    }
                    AccountFetch::NotFound => {
                        AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body())
                    }
                    AccountFetch::Unavailable => AuthDecision::Reject(
                        StatusCode::SERVICE_UNAVAILABLE,
                        auth_unavailable_body(),
                    ),
                };
                match result {
                    AuthDecision::Proceed => return Ok(redact),
                    AuthDecision::Reject(status, body) => (status, body),
                    AuthDecision::NeedsAccountFetch { .. } => unreachable!(),
                }
            }
        };

        // A 503 here is an upstream failure, not a rejected caller, so it stays out
        // of the auth-rejection panel.
        let error_type = if status == StatusCode::SERVICE_UNAVAILABLE {
            "auth_fetch_unavailable"
        } else {
            "auth_rejected"
        };
        Self::record_metrics(
            Some(error_type),
            method_label,
            "none",
            &status.as_u16().to_string(),
            start.elapsed().as_secs_f64(),
        );
        Err(self.error_response(status, Some(body)))
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

    /// Build a JSON-RPC–style error body for 429 responses.
    fn too_many_requests_body() -> Bytes {
        Bytes::from(
            serde_json::json!({
                "error": { "code": -32005, "message": "Too many requests" }
            })
            .to_string(),
        )
    }

    async fn handle_request(
        self: Arc<Self>,
        req: Request<Incoming>,
        rate_key: IpAddr,
        rate_limiter: Arc<IpRateLimiter>,
        access: Access,
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

        // Rate-limit only the JSON-RPC POST path. OPTIONS preflights, /health,
        // and /ready returned above, so a 429 here never blocks a CORS preflight
        // or trips a health probe. The internal listener is exempt: it isn't
        // reachable from outside the mesh, and one backfill batch alone issues
        // more requests than the public per-IP burst allows.
        if access == Access::Public && rate_limiter.check_key(&rate_key).is_err() {
            metrics::GATEWAY_REJECTED_TOTAL
                .with_label_values(&["rate_limit"])
                .inc();
            Self::record_metrics(
                Some("rate_limited"),
                "unknown",
                "none",
                "429",
                start.elapsed().as_secs_f64(),
            );
            return Ok(self.error_response(
                StatusCode::TOO_MANY_REQUESTS,
                Some(Self::too_many_requests_body()),
            ));
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
        // Bound how long we wait for the body so a slow-body client can't pin
        // the connection open after its headers are in.
        let collected =
            match tokio::time::timeout(self.limits.body_read_timeout, limited_body.collect()).await
            {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        "Request body read timed out after {:?}",
                        self.limits.body_read_timeout
                    );
                    Self::record_metrics(
                        Some("body_timeout"),
                        "unknown",
                        "none",
                        "408",
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(self.error_response(StatusCode::REQUEST_TIMEOUT, None));
                }
            };
        let body_bytes = match collected {
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
        // Skipped on the internal listener: the operator services carry no JWT,
        // and they need the raw errors their confirmation handling routes on.
        let params = json.get("params").cloned().unwrap_or(Value::Null);
        let redact_tx_errors = match access {
            Access::Internal => false,
            Access::Public => match self
                .enforce_auth(auth_header.as_deref(), method, method_label, &params, start)
                .await
            {
                Ok(redact) => redact,
                Err(rejection) => return Ok(rejection),
            },
        };

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
                if !redact_tx_errors {
                    return Ok(Response::from_parts(parts, body.boxed_unsync()));
                }

                // Rewriting the page means buffering it instead of streaming.
                let collected = match body.collect().await {
                    Ok(collected) => collected.to_bytes(),
                    Err(e) => {
                        error!("Failed to read response from {}: {}", target_url, e);
                        return Ok(self.error_response(StatusCode::BAD_GATEWAY, None));
                    }
                };
                // Redaction changes the length, so let hyper re-frame the body.
                parts.headers.remove(hyper::header::CONTENT_LENGTH);
                parts.headers.remove(hyper::header::TRANSFER_ENCODING);
                Ok(Response::from_parts(
                    parts,
                    Full::new(redact_transaction_errors(collected))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                ))
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
    access: Access,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Gateway listening on http://{} ({:?})",
        listener.local_addr()?,
        access
    );

    // Cap total concurrent connections. A connection past the cap is dropped
    // at once rather than queued, so a flood can't pile up resources.
    let connection_slots = Arc::new(Semaphore::new(gateway.limits.max_connections.get()));
    // Per-IP connection counts, so one host can't consume the global budget.
    let ip_counts: IpConnCounts = Arc::new(Mutex::new(HashMap::new()));

    // Per-IP request rate limiter (token bucket): refills at rate_limit_per_second
    // and holds up to rate_limit_burst tokens.
    let quota = Quota::per_second(gateway.limits.rate_limit_per_second)
        .allow_burst(gateway.limits.rate_limit_burst);
    let rate_limiter: Arc<IpRateLimiter> = Arc::new(RateLimiter::keyed(quota));

    // Prune IPs that have fully replenished so the keyed store stays bounded.
    // retain_recent only reclaims replenished keys, so run it often to shed
    // entries soon after churning IPs go quiet.
    let pruner = Arc::clone(&rate_limiter);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            pruner.retain_recent();
        }
    });

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) if is_connection_error(&e) => continue,
            Err(e) => {
                // Anything else, notably EMFILE/ENFILE under an fd flood, must
                // not kill the listener and restart the process. Back off
                // briefly so we don't spin while the fd table drains.
                warn!("accept() failed: {e}; backing off before retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let ip = peer_addr.ip();

        // Take a global slot. None free means we are at the cap, so drop the
        // socket immediately.
        let permit = match Arc::clone(&connection_slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!("Connection limit reached, dropping new connection");
                metrics::GATEWAY_REJECTED_TOTAL
                    .with_label_values(&["global_limit"])
                    .inc();
                continue;
            }
        };

        // Take a per-IP slot. None means this IP is at its cap; continue to drop
        // the socket and release the global permit. Key by the same /64-masked
        // value the rate limiter uses so a client owning a whole IPv6 /64 cannot
        // spray connections across it to bypass the per-IP cap.
        let rate_key = rate_limit_key(ip);
        let ip_guard = match try_acquire_ip(
            &ip_counts,
            rate_key,
            gateway.limits.max_connections_per_ip.get(),
        ) {
            Some(guard) => guard,
            None => {
                warn!("Per-IP connection limit reached for {ip}, dropping connection");
                metrics::GATEWAY_REJECTED_TOTAL
                    .with_label_values(&["per_ip_limit"])
                    .inc();
                continue;
            }
        };

        // Enable OS TCP keepalive so a peer that vanishes without a close (its
        // network dropped) is detected and the socket reclaimed. Best-effort:
        // log and keep serving if it fails.
        let keepalive = TcpKeepalive::new()
            .with_time(gateway.limits.tcp_keepalive_idle)
            .with_interval(gateway.limits.tcp_keepalive_interval);
        if let Err(e) = SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
            warn!("Failed to set TCP keepalive for {ip}: {e}");
        }

        let io = TokioIo::new(stream);
        let gateway = Arc::clone(&gateway);
        let rate_limiter = Arc::clone(&rate_limiter);

        tokio::spawn(async move {
            // Hold both slots for the connection's lifetime; released on drop.
            let _permit = permit;
            let _ip_guard = ip_guard;
            let header_timeout = gateway.limits.header_read_timeout;
            let service = service_fn(move |req| {
                let gateway = Arc::clone(&gateway);
                let rate_limiter = Arc::clone(&rate_limiter);
                async move {
                    gateway
                        .handle_request(req, rate_key, rate_limiter, access)
                        .await
                }
            });

            // The timer is required for header_read_timeout to take effect; it
            // closes clients that trickle their headers (slowloris).
            let conn = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout)
                .serve_connection(io, service);
            if let Err(err) = conn.await {
                error!("Error serving connection: {:?}", err);
            }
        });
    }
}

pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting PrivateChannel Gateway");
    info!("  Port: {}", args.port);
    let internal_port = configured_internal_port(args.internal_port.as_deref())?;
    info!(
        "  Internal port: {}",
        match internal_port {
            Some(port) => port.to_string(),
            None => "disabled".to_string(),
        }
    );
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

    let limits = Limits {
        max_connections: args.max_connections,
        max_connections_per_ip: args.max_connections_per_ip,
        header_read_timeout: Duration::from_secs(args.header_read_timeout_secs),
        body_read_timeout: Duration::from_secs(args.body_read_timeout_secs),
        auth_fetch_timeout: Duration::from_secs(args.auth_fetch_timeout_secs),
        tcp_keepalive_idle: Duration::from_secs(args.tcp_keepalive_idle_secs),
        tcp_keepalive_interval: Duration::from_secs(args.tcp_keepalive_interval_secs),
        rate_limit_per_second: args.rate_limit_per_second,
        rate_limit_burst: args.rate_limit_burst,
    };

    let gateway = Arc::new(
        Gateway::new(
            args.write_url,
            args.read_url,
            args.cors_allowed_origin,
            args.jwt_secret,
            auth_db,
        )
        .with_limits(limits),
    );

    let public = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], args.port))).await?;

    let Some(internal_port) = internal_port else {
        return serve(public, gateway, Access::Public).await;
    };

    let internal = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], internal_port))).await?;
    tokio::try_join!(
        serve(public, Arc::clone(&gateway), Access::Public),
        serve(internal, gateway, Access::Internal),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// Spawn a test gateway with configurable backend URLs.
    /// Each invocation binds to a unique port via port 0 (OS-assigned).
    async fn start_gateway_with_urls(write_url: &str, read_url: &str) -> SocketAddr {
        start_gateway_with_limits(write_url, read_url, Limits::default()).await
    }

    /// Like `start_gateway_with_urls` but with custom resource limits, for the
    /// guard tests.
    async fn start_gateway_with_limits(
        write_url: &str,
        read_url: &str,
        limits: Limits,
    ) -> SocketAddr {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();

        let gateway = Arc::new(
            Gateway::new(
                write_url.to_string(),
                read_url.to_string(),
                "*".to_string(),
                None, // no auth enforcement in these tests
                None,
            )
            .with_limits(limits),
        );

        // Port 0 lets the OS assign a unique free port; avoids collisions between concurrent tests.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let _ = serve(listener, gateway, Access::Public).await;
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

    /// A method missing from KNOWN_RPC_METHODS still routes, but its RPC metrics
    /// land in the "unknown" bucket and disappear from the per-method panels.
    #[test]
    fn slot_enumeration_methods_have_their_own_metric_label() {
        for method in ["getBlocks", "getBlocksWithLimit"] {
            assert!(
                KNOWN_RPC_METHODS.contains(&method),
                "{method} would be recorded under the \"unknown\" metric label"
            );
        }
    }

    /// Same metric-label check for the height read clients use to detect an expired blockhash.
    #[test]
    fn block_height_method_has_its_own_metric_label() {
        assert!(
            KNOWN_RPC_METHODS.contains(&"getBlockHeight"),
            "getBlockHeight would be recorded under the \"unknown\" metric label"
        );
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
            let _ = serve(listener, gateway, Access::Public).await;
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

    /// Holds one slot with a keep-alive connection, then asserts a second
    /// connection is refused rather than served. Shared by the connection-cap
    /// guards, which both cap the second connection opened from this IP.
    async fn assert_second_connection_refused(addr: SocketAddr) {
        // Hold a slot with a keep-alive connection. Reading its 200 back proves
        // the connection was accepted and its permit taken; the socket then
        // stays open, holding the slot for the rest of the check.
        let mut held = TcpStream::connect(addr).await.unwrap();
        held.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 1024];
        let n = held.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 OK"));

        // A second connection is over the cap. Send it a real request: a broken
        // cap would serve it and return a response, failing the test fast. A
        // working cap drops the socket, so the read ends in EOF or a reset.
        let mut over = TcpStream::connect(addr).await.unwrap();
        // Best-effort: the socket may already be closed by the time we write.
        let _ = over
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(2), over.read(&mut buf))
            .await
            .expect("over-cap connection should resolve promptly, not hang");
        match read {
            Ok(0) | Err(_) => {} // EOF or reset: the connection was refused.
            Ok(n) => panic!(
                "connection over the cap was served, got {n} bytes: {:?}",
                String::from_utf8_lossy(&buf[..n])
            ),
        }
    }

    #[tokio::test]
    async fn connection_cap_refuses_over_limit() {
        let addr = start_gateway_with_limits(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            Limits {
                max_connections: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
        )
        .await;
        assert_second_connection_refused(addr).await;
    }

    #[tokio::test]
    async fn rate_limit_returns_429_when_exceeded() {
        let addr = start_gateway_with_limits(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            Limits {
                rate_limit_per_second: NonZeroU32::new(1).unwrap(),
                rate_limit_burst: NonZeroU32::new(1).unwrap(),
                ..Default::default()
            },
        )
        .await;

        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let req = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );

        // Burst of 1 from this IP: the first request passes the limiter (then
        // 502s on the unreachable backend), the immediate second is over budget
        // and is rejected with 429 before it reaches the backend.
        let first = send_raw(addr, req.as_bytes()).await;
        assert_status(&first, 502);
        let second = send_raw(addr, req.as_bytes()).await;
        assert_status(&second, 429);
    }

    #[tokio::test]
    async fn health_and_preflight_are_exempt_from_rate_limit() {
        let addr = start_gateway_with_limits(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            Limits {
                rate_limit_per_second: NonZeroU32::new(1).unwrap(),
                rate_limit_burst: NonZeroU32::new(1).unwrap(),
                ..Default::default()
            },
        )
        .await;

        // Drain the one-token bucket for this IP with a POST, so anything the
        // limiter guards is now over budget.
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#;
        let post = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = send_raw(addr, post.as_bytes()).await;

        // The limiter sits behind routing, so /health probes stay 200 with the
        // bucket drained. Under the old in-front placement they would 429 and
        // the orchestrator would read the gateway as down.
        let health = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
        for _ in 0..5 {
            let response = send_raw(addr, health.as_bytes()).await;
            assert_status(&response, 200);
        }

        // A CORS preflight is likewise exempt, so a drained bucket cannot block
        // the real POST it precedes.
        let preflight = "OPTIONS / HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let response = send_raw(addr, preflight.as_bytes()).await;
        assert_status(&response, 200);
    }

    #[tokio::test]
    async fn body_read_timeout_returns_408_for_slow_body() {
        let addr = start_gateway_with_limits(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            Limits {
                body_read_timeout: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .await;

        // Finish the headers and promise a body via Content-Length, then send
        // none of it.
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n",
        )
        .await
        .unwrap();

        // With the timeout the server replies 408 ~200ms in. Without it the
        // server waits for the body forever and this read hangs; the 2s bound
        // turns that regression into a failure, not a stuck test.
        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf))
            .await
            .expect("server should reply to the slow-body client, not hang")
            .unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("408"), "expected 408, got: {response}");
    }

    #[tokio::test]
    async fn header_read_timeout_closes_slow_client() {
        let addr = start_gateway_with_limits(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            Limits {
                header_read_timeout: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .await;

        // Slowloris: send a partial request and never finish the header block
        // (no terminating blank line).
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();

        // With the timeout the server closes it ~200ms in, so the read resolves.
        // Without it the server waits for headers forever and this read hangs;
        // the 2s bound turns that regression into a failure, not a stuck test.
        let mut buf = [0u8; 64];
        let closed = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf)).await;
        assert!(
            closed.is_ok(),
            "server should close the slow client, not hang"
        );
    }

    #[tokio::test]
    async fn per_ip_connection_cap_refuses_over_limit() {
        // Global cap stays high; only the per-IP cap of 1 is under test, and all
        // test connections come from 127.0.0.1.
        let addr = start_gateway_with_limits(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            Limits {
                max_connections_per_ip: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
        )
        .await;
        assert_second_connection_refused(addr).await;
    }

    #[test]
    fn rate_limit_key_masks_ipv6_to_64() {
        use std::net::Ipv4Addr;

        // Two addresses sharing a /64 collapse to one key, so a client cannot
        // spray its /64 to bloat the keyed store or dodge the per-IP budget.
        let a: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff:ffff:ffff:ffff".parse().unwrap();
        assert_eq!(rate_limit_key(a), rate_limit_key(b));

        // A different /64 stays a distinct key.
        let other: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_ne!(rate_limit_key(a), rate_limit_key(other));

        // IPv4 is keyed by the full address.
        let v4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
        assert_eq!(rate_limit_key(v4), v4);
    }

    /// Both legs of the balance probe (InsufficientFunds above the source
    /// balance, MintMismatch at or below it) must come back indistinguishable.
    #[test]
    fn history_errors_collapse_to_one_marker() {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {"signature": "sig1", "slot": 1, "err": {"InstructionError": [0, {"Custom": 1}]}},
                {"signature": "sig2", "slot": 2, "err": {"InstructionError": [0, {"Custom": 3}]}},
                {"signature": "sig3", "slot": 3, "err": null}
            ]
        })
        .to_string();

        let redacted: Value =
            serde_json::from_slice(&redact_transaction_errors(Bytes::from(body))).unwrap();

        let entries = redacted["result"].as_array().unwrap();
        assert_eq!(
            entries[0]["err"], entries[1]["err"],
            "two different program errors must not stay distinguishable"
        );
        assert_eq!(
            entries[0]["err"],
            json!({"InstructionError": [0, "GenericError"]})
        );
        assert!(
            entries[2]["err"].is_null(),
            "a landed transaction keeps err: null"
        );
    }

    /// Compose substitutes an unset variable as the empty string, so a blank
    /// value must disable the internal listener rather than fail to start.
    #[test]
    fn blank_internal_port_disables_the_listener() {
        assert_eq!(configured_internal_port(None).unwrap(), None);
        assert_eq!(configured_internal_port(Some("")).unwrap(), None);
        assert_eq!(configured_internal_port(Some("  ")).unwrap(), None);
        assert_eq!(configured_internal_port(Some("8904")).unwrap(), Some(8904));
        assert!(configured_internal_port(Some("not-a-port")).is_err());
    }

    /// `getSignatureStatuses` nests its entries and reports the error twice, so
    /// both copies have to collapse or the probe just reads the other one.
    #[test]
    fn signature_status_errors_collapse_in_both_fields() {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "context": {"slot": 3},
                "value": [
                    {
                        "slot": 1,
                        "confirmations": null,
                        "err": {"InstructionError": [0, {"Custom": 1}]},
                        "status": {"Err": {"InstructionError": [0, {"Custom": 1}]}},
                        "confirmationStatus": "finalized"
                    },
                    {
                        "slot": 2,
                        "confirmations": null,
                        "err": {"InstructionError": [0, {"Custom": 3}]},
                        "status": {"Err": {"InstructionError": [0, {"Custom": 3}]}},
                        "confirmationStatus": "finalized"
                    },
                    {
                        "slot": 3,
                        "confirmations": null,
                        "err": null,
                        "status": {"Ok": null},
                        "confirmationStatus": "finalized"
                    },
                    null
                ]
            }
        })
        .to_string();

        let redacted: Value =
            serde_json::from_slice(&redact_transaction_errors(Bytes::from(body))).unwrap();

        let entries = redacted["result"]["value"].as_array().unwrap();
        let marker = json!({"InstructionError": [0, "GenericError"]});
        assert_eq!(entries[0]["err"], marker);
        assert_eq!(entries[1]["err"], marker);
        assert_eq!(
            entries[0]["status"]["Err"], marker,
            "the legacy status field must not keep the real error"
        );
        assert_eq!(entries[1]["status"]["Err"], marker);
        assert!(entries[2]["err"].is_null());
        assert_eq!(
            entries[2]["status"],
            json!({"Ok": null}),
            "a landed transaction keeps its Ok status"
        );
        assert!(entries[3].is_null(), "an unknown signature stays null");
    }

    /// Shared secret for the ownership-fetch tests below, used by both the token
    /// minter and the gateway under test.
    const TEST_JWT_SECRET: &str = "test-gateway-secret";

    /// Start a gateway with auth enforcement on. The auth pool is lazy and never
    /// connects: every path these tests exercise rejects before any DB query.
    async fn start_auth_gateway(read_url: &str, limits: Limits) -> SocketAddr {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();

        let auth_db = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:password@127.0.0.1:1/unused")
            .unwrap();
        let gateway = Arc::new(
            Gateway::new(
                "http://127.0.0.1:1".to_string(),
                read_url.to_string(),
                "*".to_string(),
                Some(TEST_JWT_SECRET.to_string()),
                Some(auth_db),
            )
            .with_limits(limits),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve(listener, gateway, Access::Public).await;
        });

        addr
    }

    /// Mint a User-role JWT the gateway accepts, so a gated request gets as far
    /// as the ownership account fetch.
    fn user_token() -> String {
        let claims = json!({
            "sub": "11111111-1111-4111-8111-111111111111",
            "role": "user",
            "exp": (Utc::now().timestamp() + 3600) as usize,
            "iss": "private-channel-auth",
            "aud": "private-channel-gateway",
        });
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
        )
        .unwrap()
    }

    /// A raw getAccountInfo POST carrying `token`, i.e. a gated request that
    /// triggers the ownership account fetch.
    fn gated_request(token: &str) -> String {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["So11111111111111111111111111111111111111112"]}"#;
        format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\n\r\n{}",
            token,
            body.len(),
            body
        )
    }

    /// Spawn a backend that reads the request and never answers. Accepted streams
    /// are held open so the gateway sees a hang rather than an EOF, which leaves
    /// the fetch deadline as the only way out.
    async fn start_black_hole_backend() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;
                held.push(stream);
            }
        });

        addr
    }

    #[tokio::test]
    async fn hung_read_node_returns_503_for_gated_method() {
        let backend = start_black_hole_backend().await;
        let addr = start_auth_gateway(
            &format!("http://{backend}"),
            Limits {
                auth_fetch_timeout: Duration::from_millis(300),
                ..Default::default()
            },
        )
        .await;

        let mut conn = TcpStream::connect(addr).await.unwrap();
        let start = Instant::now();
        conn.write_all(gated_request(&user_token()).as_bytes())
            .await
            .unwrap();

        // With the deadline the gateway answers ~300ms in. Without it the ownership
        // fetch waits on the hung read node forever and this read hangs; the 3s
        // bound turns that regression into a failure, not a stuck test.
        let mut buf = vec![0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(3), conn.read(&mut buf))
            .await
            .expect("gateway should give up on the hung read node, not hang")
            .unwrap();
        let elapsed = start.elapsed();
        let response = String::from_utf8_lossy(&buf[..n]);

        assert_status(&response, 503);
        // The backend holds the connection open, so the deadline is the only way
        // out. An immediate reply would mean the socket broke and the timeout was
        // never exercised.
        assert!(
            elapsed >= Duration::from_millis(250),
            "expected the fetch deadline to fire, got a reply after {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn read_node_rpc_error_is_503_not_403() {
        // A node answering with a JSON-RPC error says nothing about ownership, so
        // the caller must not be told the account isn't theirs.
        let backend = start_mock_http_backend(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"node is behind"}}"#,
        )
        .await;
        let addr = start_auth_gateway(&format!("http://{backend}"), Limits::default()).await;

        let response = send_raw(addr, gated_request(&user_token()).as_bytes()).await;
        assert_status(&response, 503);
        assert!(
            response.contains("-32004"),
            "503 body should carry the unavailable code: {response}"
        );
    }

    #[tokio::test]
    async fn nonexistent_account_stays_403() {
        // The read node answered: the account really doesn't exist, so this stays a
        // 403 and the 503 split doesn't swallow it.
        let backend =
            start_mock_http_backend(r#"{"jsonrpc":"2.0","id":1,"result":{"value":null}}"#).await;
        let addr = start_auth_gateway(&format!("http://{backend}"), Limits::default()).await;

        let response = send_raw(addr, gated_request(&user_token()).as_bytes()).await;
        assert_status(&response, 403);
    }
}
