use {
    anyhow::{anyhow, Result},
    clap::{Parser, Subcommand},
    private_channel_auth::{db, error::AppError},
    solana_sdk::pubkey::Pubkey,
    sqlx::postgres::PgPoolOptions,
    std::{env, str::FromStr},
    tracing::{error, info},
};

#[derive(Parser, Debug)]
#[command(
    name = "private-channel-auth-admin",
    about = "Manual administrative commands for the private-channel-auth database"
)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "PRIVATE_CHANNEL_LOG_LEVEL")]
    log_level: String,

    /// Enable JSON logging format
    #[arg(long, env = "PRIVATE_CHANNEL_JSON_LOGS")]
    json_logs: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Attach a wallet to a user without verification — operator asserts trust
    AttachWallet(AttachWalletArgs),
}

#[derive(Parser, Debug)]
struct AttachWalletArgs {
    /// Username of the user to attach the wallet to
    #[arg(long)]
    username: String,

    /// Base58-encoded Solana pubkey to attach to the user
    #[arg(long)]
    pubkey: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    init_logging(&args.log_level, args.json_logs);

    if let Err(e) = run(args).await {
        error!("Command failed: {:?}", e);
        std::process::exit(1);
    }
}

fn init_logging(log_level: &str, json_logs: bool) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

    if json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }
}

async fn run(args: Args) -> Result<()> {
    // Read AUTH_DATABASE_URL from the environment rather than a CLI flag so the
    // password never lands in argv (visible via `ps` and shell history).
    let database_url =
        env::var("AUTH_DATABASE_URL").map_err(|_| anyhow!("AUTH_DATABASE_URL is not set"))?;

    if !database_url.starts_with("postgres://") && !database_url.starts_with("postgresql://") {
        return Err(anyhow!("AUTH_DATABASE_URL must be a PostgreSQL URL"));
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .map_err(|e| anyhow!("Failed to connect to auth DB: {}", e))?;

    match args.command {
        Command::AttachWallet(args) => attach_wallet(&pool, args).await?,
    }

    Ok(())
}

async fn attach_wallet(pool: &sqlx::PgPool, args: AttachWalletArgs) -> Result<()> {
    let pubkey = Pubkey::from_str(&args.pubkey)
        .map_err(|_| anyhow!("invalid pubkey: {}", args.pubkey))?
        .to_string();

    let user = db::find_user_by_username(pool, &args.username)
        .await?
        .ok_or_else(|| anyhow!("user not found: {}", args.username))?;

    let wallet = db::insert_verified_wallet(pool, user.id, &pubkey)
        .await
        .map_err(|e| match e {
            // Unique constraint on (user_id, pubkey) — wallet already attached.
            AppError::Db(sqlx::Error::Database(ref db_err))
                if db_err.constraint() == Some("verified_wallets_user_id_pubkey_key") =>
            {
                anyhow!(
                    "wallet {} is already attached to user {}",
                    pubkey,
                    args.username
                )
            }
            other => anyhow::Error::new(other),
        })?;

    info!(
        user_id = %user.id,
        username = %user.username,
        pubkey = %wallet.pubkey,
        "attached wallet"
    );

    println!(
        "attached wallet {} to user {} ({})",
        wallet.pubkey, user.username, user.id
    );

    Ok(())
}

#[cfg(test)]
// `ENV_LOCK` is a synchronous Mutex held across `.await` on purpose: it
// serializes process-global env-var mutation across async tests. An async
// Mutex would defeat the point (the body of each test isn't itself async-
// contended; it just must not interleave with another test). Clippy can't
// distinguish the two cases here, so silence the lint module-wide.
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use sqlx::PgPool;
    use std::sync::Mutex;
    use testcontainers::{runners::AsyncRunner, ContainerAsync};
    use testcontainers_modules::postgres::Postgres;

    // env::set_var / remove_var mutate process-global state. The two `run` tests
    // touch AUTH_DATABASE_URL — serialize them so they don't race when the
    // surrounding test runner schedules them in parallel.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    async fn start_pool() -> (PgPool, ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_db_name("auth_test")
            .with_user("postgres")
            .with_password("password")
            .start()
            .await
            .expect("failed to start postgres container");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:password@{}:{}/auth_test", host, port);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("failed to connect");
        db::init_schema(&pool).await.expect("schema init");
        (pool, container)
    }

    fn attach_args(username: &str, pubkey: &str) -> AttachWalletArgs {
        AttachWalletArgs {
            username: username.into(),
            pubkey: pubkey.into(),
        }
    }

    fn run_args(command: Command) -> Args {
        Args {
            log_level: "info".into(),
            json_logs: false,
            command,
        }
    }

    #[tokio::test]
    async fn run_rejects_missing_database_url() {
        let _g = ENV_LOCK.lock().unwrap();
        let prev = env::var("AUTH_DATABASE_URL").ok();
        env::remove_var("AUTH_DATABASE_URL");

        let result = run(run_args(Command::AttachWallet(attach_args("u", "p")))).await;

        match prev {
            Some(p) => env::set_var("AUTH_DATABASE_URL", p),
            None => env::remove_var("AUTH_DATABASE_URL"),
        }

        let err = result.expect_err("expected error");
        assert!(err.to_string().contains("is not set"), "got: {err}");
    }

    #[tokio::test]
    async fn run_rejects_non_postgres_url() {
        let _g = ENV_LOCK.lock().unwrap();
        let prev = env::var("AUTH_DATABASE_URL").ok();
        env::set_var("AUTH_DATABASE_URL", "mysql://localhost/test");

        let result = run(run_args(Command::AttachWallet(attach_args("u", "p")))).await;

        match prev {
            Some(p) => env::set_var("AUTH_DATABASE_URL", p),
            None => env::remove_var("AUTH_DATABASE_URL"),
        }

        let err = result.expect_err("expected error");
        assert!(
            err.to_string().contains("must be a PostgreSQL URL"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn attach_wallet_rejects_invalid_pubkey() {
        let (pool, _c) = start_pool().await;
        let err = attach_wallet(&pool, attach_args("alice", "not-base58"))
            .await
            .expect_err("expected error");
        assert!(err.to_string().contains("invalid pubkey"), "got: {err}");
    }

    #[tokio::test]
    async fn attach_wallet_rejects_unknown_user() {
        let (pool, _c) = start_pool().await;
        let pubkey = Pubkey::new_unique().to_string();
        let err = attach_wallet(&pool, attach_args("ghost", &pubkey))
            .await
            .expect_err("expected error");
        assert!(err.to_string().contains("user not found"), "got: {err}");
    }

    #[tokio::test]
    async fn attach_wallet_succeeds_for_known_user() {
        let (pool, _c) = start_pool().await;
        let user = db::insert_user(&pool, "bob", "$argon2id$placeholder")
            .await
            .expect("insert user");
        let pubkey = Pubkey::new_unique().to_string();

        attach_wallet(&pool, attach_args("bob", &pubkey))
            .await
            .expect("attach should succeed");

        let wallets = db::list_verified_wallets(&pool, user.id)
            .await
            .expect("list wallets");
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].pubkey, pubkey);
    }

    #[tokio::test]
    async fn attach_wallet_reports_already_attached() {
        let (pool, _c) = start_pool().await;
        let user = db::insert_user(&pool, "carol", "$argon2id$placeholder")
            .await
            .expect("insert user");
        let pubkey = Pubkey::new_unique().to_string();

        attach_wallet(&pool, attach_args("carol", &pubkey))
            .await
            .expect("first attach should succeed");

        let err = attach_wallet(&pool, attach_args("carol", &pubkey))
            .await
            .expect_err("second attach should fail");
        assert!(
            err.to_string().contains("is already attached"),
            "got: {err}"
        );

        // The failed second attach must not duplicate the row.
        let wallets = db::list_verified_wallets(&pool, user.id)
            .await
            .expect("list wallets");
        assert_eq!(wallets.len(), 1, "failed attach should not duplicate row");
        assert_eq!(wallets[0].pubkey, pubkey);
    }
}
