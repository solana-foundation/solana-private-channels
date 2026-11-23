use clap::Parser;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

/// How long a handler waits for a pool connection.
///
/// Must stay below the request timeout. sqlx defaults this to 30s, so a request
/// stalled on an exhausted pool was cancelled by the request timeout before
/// `acquire` returned `PoolTimedOut`. `/health` only ever flips unhealthy from a
/// handler observing that error, so the probe stayed green while every request
/// shed. `Config::validate` enforces the ordering rather than trusting it.
pub const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser, Debug, Clone)]
#[command(name = "private-channel-auth")]
#[command(about = "PrivateChannel authentication service")]
pub struct Config {
    #[arg(long, env = "AUTH_PORT", default_value = "8903")]
    pub port: u16,

    #[arg(long, env = "AUTH_DATABASE_URL")]
    pub database_url: String,

    /// Base58 64-byte Ed25519 keypair that signs tokens. Only auth holds it.
    #[arg(long, env = "JWT_SIGNING_KEY")]
    pub jwt_signing_key: String,

    /// Base58 public key of JWT_SIGNING_KEY, the same value the gateway verifies with.
    #[arg(long, env = "JWT_PUBLIC_KEY")]
    pub jwt_public_key: String,

    /// The public key JWT_PUBLIC_KEY replaced, accepted for verification only during
    /// a rotation. Blank means off.
    #[arg(long, env = "JWT_PUBLIC_KEY_PREVIOUS")]
    pub jwt_public_key_previous: Option<String>,

    /// Value for the Access-Control-Allow-Origin header.
    /// Set to the frontend origin in production (e.g. "https://app.private_channel.xyz").
    /// Defaults to "*" so local dev works without extra config, but should be
    /// restricted in any environment that handles real credentials.
    #[arg(long, env = "CORS_ALLOWED_ORIGIN", default_value = "*")]
    pub cors_allowed_origin: String,

    /// Maximum number of connections in the database pool.
    #[arg(long, env = "AUTH_DATABASE_MAX_CONNECTIONS", default_value = "10")]
    pub database_max_connections: NonZeroU32,

    /// Maximum number of Argon2 hashes running at once. Hashing is CPU-bound,
    /// so raising this past the core count costs memory without adding throughput.
    #[arg(long, env = "AUTH_ARGON2_MAX_CONCURRENCY", default_value = "4")]
    pub argon2_max_concurrency: NonZeroUsize,

    /// Sustained per-IP request rate for /auth/register, /auth/login and
    /// /auth/challenge-wallet.
    #[arg(long, env = "AUTH_RATE_LIMIT_PER_SECOND", default_value = "5")]
    pub auth_rate_limit_per_second: NonZeroU32,

    /// Burst allowance above the sustained per-IP rate.
    #[arg(long, env = "AUTH_RATE_LIMIT_BURST", default_value = "10")]
    pub auth_rate_limit_burst: NonZeroU32,

    /// Failed logins (and registrations of a taken name) per minute on one username,
    /// across all IPs, before further failures return 429. Not a guess limit: a
    /// correct password always succeeds, so this cannot lock an account out.
    #[arg(
        long,
        env = "AUTH_USERNAME_FAILED_LOGINS_PER_MINUTE",
        default_value = "5"
    )]
    pub auth_username_failed_logins_per_minute: NonZeroU32,

    /// Maximum concurrent client connections. Connections beyond this are
    /// dropped so a flood cannot exhaust sockets, tasks or memory before the
    /// per-request throttle is ever reached.
    #[arg(long, env = "AUTH_MAX_CONNECTIONS", default_value = "1024")]
    pub max_connections: NonZeroUsize,

    /// Maximum concurrent connections from a single client IP.
    #[arg(long, env = "AUTH_MAX_CONNECTIONS_PER_IP", default_value = "64")]
    pub max_connections_per_ip: NonZeroUsize,

    /// Seconds a client may take to send the full request header block before
    /// the connection is closed (slowloris protection). Must be non-zero; a
    /// zero timeout would fail every request instantly.
    #[arg(
        long,
        env = "AUTH_HEADER_READ_TIMEOUT_SECS",
        default_value = "10",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub header_read_timeout_secs: u64,

    /// Seconds a request may take once its headers are in, covering the body
    /// read and the handler. Must be non-zero.
    #[arg(
        long,
        env = "AUTH_REQUEST_TIMEOUT_SECS",
        default_value = "15",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub request_timeout_secs: u64,

    /// Idle seconds before the OS starts sending TCP keepalive probes. Must be
    /// non-zero.
    #[arg(
        long,
        env = "AUTH_TCP_KEEPALIVE_IDLE_SECS",
        default_value = "60",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub tcp_keepalive_idle_secs: u64,

    /// Seconds between TCP keepalive probes. Must be non-zero.
    #[arg(
        long,
        env = "AUTH_TCP_KEEPALIVE_INTERVAL_SECS",
        default_value = "15",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub tcp_keepalive_interval_secs: u64,
}

impl Config {
    /// Rejects values that parse fine but leave a control inert. These fail silently at
    /// runtime, which is worse than not starting. `JwtConfig::new` checks the JWT keys.
    pub fn validate(&self) -> Result<(), String> {
        if Duration::from_secs(self.request_timeout_secs) <= POOL_ACQUIRE_TIMEOUT {
            return Err(format!(
                "AUTH_REQUEST_TIMEOUT_SECS ({}) must exceed the {}s pool acquire timeout, \
                 or a request is cancelled before an exhausted pool can report itself \
                 and /health keeps returning 200 while every request sheds",
                self.request_timeout_secs,
                POOL_ACQUIRE_TIMEOUT.as_secs(),
            ));
        }

        if self.max_connections_per_ip >= self.max_connections {
            return Err(format!(
                "AUTH_MAX_CONNECTIONS_PER_IP ({}) must be below AUTH_MAX_CONNECTIONS ({}), \
                 or one client can take the whole budget and the per-IP cap never applies",
                self.max_connections_per_ip, self.max_connections,
            ));
        }

        Ok(())
    }

    /// The previous JWT public key, or `None` when unset or blank.
    pub fn jwt_public_key_previous(&self) -> Option<&str> {
        self.jwt_public_key_previous
            .as_deref()
            .filter(|s| !s.trim().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a config that passes, so each test can break one thing.
    fn valid_config() -> Config {
        Config::parse_from([
            "auth",
            "--database-url",
            "postgres://localhost/auth",
            "--jwt-signing-key",
            "signing-key",
            "--jwt-public-key",
            "public-key",
        ])
    }

    #[test]
    fn a_request_timeout_under_the_pool_acquire_timeout_is_rejected() {
        let mut config = valid_config();
        config.validate().expect("defaults must be valid");

        config.request_timeout_secs = POOL_ACQUIRE_TIMEOUT.as_secs();
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_per_ip_cap_above_the_global_cap_is_rejected() {
        let mut config = valid_config();
        config.max_connections = NonZeroUsize::new(8).unwrap();
        config.max_connections_per_ip = NonZeroUsize::new(9).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_per_ip_cap_equal_to_the_global_cap_is_rejected() {
        let mut config = valid_config();
        config.max_connections = NonZeroUsize::new(8).unwrap();
        config.max_connections_per_ip = NonZeroUsize::new(8).unwrap();
        let err = config.validate().expect_err("equal caps leave no reserve");
        assert!(err.contains("must be below"), "got: {err}");

        config.max_connections_per_ip = NonZeroUsize::new(7).unwrap();
        config.validate().expect("strictly less is valid");
    }

    #[test]
    fn a_zero_database_pool_size_is_rejected_by_the_parser() {
        let parsed = Config::try_parse_from([
            "auth",
            "--database-url",
            "postgres://localhost/auth",
            "--jwt-signing-key",
            "signing-key",
            "--jwt-public-key",
            "public-key",
            "--database-max-connections",
            "0",
        ]);
        assert!(parsed.is_err(), "a zero pool panics in sqlx pool creation");
    }
}
