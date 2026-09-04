use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use private_channel_auth::config::{Config, POOL_ACQUIRE_TIMEOUT};
use private_channel_auth::{
    build_app, db,
    jwt::JwtConfig,
    password::PasswordWorker,
    pool_status::PoolStatus,
    serve::{serve, Limits},
    throttle::{self, AuthThrottle},
    AppState,
};

/// How often the background task purges expired and used challenge rows.
/// Challenge TTL is 10 minutes, so hourly is more than sufficient.
const CHALLENGE_CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::parse();
    if let Err(e) = config.validate() {
        panic!("invalid configuration: {e}");
    }

    info!("Starting private-channel-auth on port {}", config.port);

    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
        .connect(&config.database_url)
        .await
        .expect("failed to connect to database");

    info!("Connected to database");

    // Create tables and indexes if they don't exist yet.
    db::init_schema(&pool)
        .await
        .expect("failed to initialize schema");

    info!("Schema initialized");

    let pool_status = PoolStatus::new_healthy();
    let auth_throttle = Arc::new(AuthThrottle::new(
        config.auth_rate_limit_per_second,
        config.auth_rate_limit_burst,
        config.auth_username_attempts_per_minute,
    ));
    throttle::spawn_pruner(auth_throttle.clone());

    let state = AppState {
        pool,
        jwt: Arc::new(JwtConfig::new(&config.jwt_secret)),
        pool_status: pool_status.clone(),
        passwords: PasswordWorker::new(config.argon2_max_concurrency),
        throttle: auth_throttle,
    };

    // Periodically remove expired and used challenges so the table doesn't grow unboundedly.
    let cleanup_pool = state.pool.clone();
    let cleanup_status = pool_status.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(CHALLENGE_CLEANUP_INTERVAL).await;
            let r = db::cleanup_stale_challenges(&cleanup_pool).await;
            cleanup_status.observe_sqlx(&r);
            match r {
                Ok(n) => info!(deleted = n, "cleaned up stale challenges"),
                Err(e) => error!("challenge cleanup failed: {e}"),
            }
        }
    });

    let app = build_app(
        state,
        &config.cors_allowed_origin,
        Duration::from_secs(config.request_timeout_secs),
    );

    let limits = Limits {
        max_connections: config.max_connections,
        max_connections_per_ip: config.max_connections_per_ip,
        header_read_timeout: Duration::from_secs(config.header_read_timeout_secs),
        tcp_keepalive_idle: Duration::from_secs(config.tcp_keepalive_idle_secs),
        tcp_keepalive_interval: Duration::from_secs(config.tcp_keepalive_interval_secs),
    };

    // All interfaces, like the gateway: inside a container that is what makes
    // the port reachable at all, and `limits` is what keeps the listener bounded.
    let addr = format!("0.0.0.0:{}", config.port);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");

    serve(listener, app, limits).await.expect("server error");
}
