use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "private-channel-auth")]
#[command(about = "PrivateChannel authentication service")]
pub struct Config {
    #[arg(long, env = "AUTH_PORT", default_value = "8903")]
    pub port: u16,

    #[arg(long, env = "AUTH_DATABASE_URL")]
    pub database_url: String,

    #[arg(long, env = "JWT_SECRET")]
    pub jwt_secret: String,

    /// Value for the Access-Control-Allow-Origin header.
    /// Set to the frontend origin in production (e.g. "https://app.private_channel.xyz").
    /// Defaults to "*" so local dev works without extra config, but should be
    /// restricted in any environment that handles real credentials.
    #[arg(long, env = "CORS_ALLOWED_ORIGIN", default_value = "*")]
    pub cors_allowed_origin: String,

    /// Maximum number of connections in the database pool.
    #[arg(long, env = "AUTH_DATABASE_MAX_CONNECTIONS", default_value = "10")]
    pub database_max_connections: u32,
}

impl Config {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.jwt_secret.trim().is_empty() {
            return Err("JWT_SECRET must be non-empty when running the auth service");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    fn config_with_secret(secret: &str) -> Config {
        Config {
            port: 8903,
            database_url: "postgres://localhost/private_channel".to_string(),
            jwt_secret: secret.to_string(),
            cors_allowed_origin: "*".to_string(),
            database_max_connections: 10,
        }
    }

    #[test]
    fn validate_accepts_non_empty_jwt_secret() {
        assert!(config_with_secret("secret").validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_jwt_secret() {
        assert_eq!(
            config_with_secret("").validate(),
            Err("JWT_SECRET must be non-empty when running the auth service")
        );
    }

    #[test]
    fn validate_rejects_whitespace_only_jwt_secret() {
        assert_eq!(
            config_with_secret("   ").validate(),
            Err("JWT_SECRET must be non-empty when running the auth service")
        );
    }
}
