use {
    anyhow::{anyhow, Result},
    clap::{Parser, Subcommand},
    private_channel_auth::{
        db,
        error::AppError,
        models::{Role, User},
    },
    solana_sdk::pubkey::Pubkey,
    sqlx::postgres::PgPoolOptions,
    std::{
        env,
        io::{self, BufRead, Write},
        str::FromStr,
    },
    tracing::{error, info},
    uuid::Uuid,
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
    /// Look up a user's immutable id, role and creation time
    ShowUser(ShowUserArgs),
    /// Attach a wallet to a user without verification — operator asserts trust
    AttachWallet(AttachWalletArgs),
    /// Set a user's RBAC role (e.g. promote to operator so the gateway grants it
    /// full account access + getTransaction)
    SetRole(SetRoleArgs),
}

#[derive(Parser, Debug)]
#[group(required = true, multiple = false)]
struct ShowUserArgs {
    /// Id to look up, to confirm an id reported out of band against the account
    #[arg(long)]
    user_id: Option<Uuid>,

    /// Username to resolve to an id
    #[arg(long)]
    username: Option<String>,
}

#[derive(Parser, Debug)]
struct AttachWalletArgs {
    /// Immutable id of the user to attach the wallet to
    #[arg(long)]
    user_id: Uuid,

    /// Base58-encoded Solana pubkey to attach to the user
    #[arg(long)]
    pubkey: String,

    /// Skip the confirmation prompt
    #[arg(long)]
    yes: bool,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum RoleArg {
    Operator,
    User,
}

impl RoleArg {
    fn to_model(&self) -> Role {
        match self {
            RoleArg::Operator => Role::Operator,
            RoleArg::User => Role::User,
        }
    }
}

#[derive(Parser, Debug)]
struct SetRoleArgs {
    /// Immutable id of the user whose role to change
    #[arg(long)]
    user_id: Uuid,

    /// Role to assign
    #[arg(long)]
    role: RoleArg,

    /// Skip the confirmation prompt
    #[arg(long)]
    yes: bool,
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

    // The actor is resolved before connecting so a missing one fails without the
    // tool having touched the database.
    match args.command {
        Command::ShowUser(args) => show_user(&connect(&database_url).await?, args).await?,
        Command::AttachWallet(args) => {
            let actor = admin_actor()?;
            attach_wallet(
                &connect(&database_url).await?,
                &actor,
                args,
                &mut io::stdin().lock(),
            )
            .await?
        }
        Command::SetRole(args) => {
            let actor = admin_actor()?;
            set_role(
                &connect(&database_url).await?,
                &actor,
                args,
                &mut io::stdin().lock(),
            )
            .await?
        }
    }

    Ok(())
}

async fn connect(database_url: &str) -> Result<sqlx::PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .map_err(|e| anyhow!("Failed to connect to auth DB: {}", e))?;

    ensure_schema(&pool).await?;

    Ok(pool)
}

/// Create the schema only when it is actually missing, which keeps the tool
/// usable against a database whose auth service hasn't yet restarted onto a
/// schema carrying `admin_audit`. Running the DDL unconditionally would mean a
/// least-privilege admin role couldn't so much as look a user up.
async fn ensure_schema(pool: &sqlx::PgPool) -> Result<()> {
    let (initialized,): (bool,) =
        sqlx::query_as(r#"SELECT to_regclass('private_channel_auth.admin_audit') IS NOT NULL"#)
            .fetch_one(pool)
            .await?;

    if !initialized {
        db::init_schema(pool).await?;
    }

    Ok(())
}

/// Names the person running the command in the audit trail. Required rather than
/// defaulted: a row recording "unknown" is not worth writing.
fn admin_actor() -> Result<String> {
    let actor = env::var("AUTH_ADMIN_ACTOR").map_err(|_| anyhow!("AUTH_ADMIN_ACTOR is not set"))?;
    if actor.trim().is_empty() {
        return Err(anyhow!("AUTH_ADMIN_ACTOR must not be empty"));
    }
    Ok(actor)
}

fn print_user(user: &User) {
    println!("  id:         {}", user.id);
    println!("  username:   {}", user.username);
    println!("  role:       {}", user.role.as_str());
    println!("  created_at: {}", user.created_at);
}

/// Show who is about to be changed and require an explicit `yes`. These are the
/// fields an administrator matches against what the account owner reported out of
/// band, so a grant lands on the person it was meant for and not on whoever
/// registered the name first.
fn confirm(user: &User, action: &str, skip: bool, input: &mut impl BufRead) -> Result<bool> {
    if skip {
        return Ok(true);
    }

    println!("about to {action} for:");
    print_user(user);
    print!("type 'yes' to continue: ");
    io::stdout().flush()?;

    // A closed or empty stdin reads as end-of-input, leaving the answer empty,
    // which aborts.
    let mut answer = String::new();
    input.read_line(&mut answer)?;

    Ok(answer.trim() == "yes")
}

async fn show_user(pool: &sqlx::PgPool, args: ShowUserArgs) -> Result<()> {
    let user = match (args.user_id, &args.username) {
        (Some(user_id), _) => db::find_user_by_id(pool, user_id)
            .await?
            .ok_or_else(|| anyhow!("user not found: {user_id}"))?,
        (None, Some(username)) => db::find_user_by_username(pool, username)
            .await?
            .ok_or_else(|| anyhow!("user not found: {username}"))?,
        // clap's argument group requires exactly one of the two.
        (None, None) => return Err(anyhow!("one of --user-id or --username is required")),
    };

    print_user(&user);

    Ok(())
}

async fn set_role(
    pool: &sqlx::PgPool,
    actor: &str,
    args: SetRoleArgs,
    input: &mut impl BufRead,
) -> Result<()> {
    let user = db::find_user_by_id(pool, args.user_id)
        .await?
        .ok_or_else(|| anyhow!("user not found: {}", args.user_id))?;

    let role = args.role.to_model();
    let role_str = role.as_str();
    if !confirm(&user, &format!("set role to {role_str}"), args.yes, input)? {
        println!("aborted");
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    // Both ends of the transition, taken from the statement that performed it, so
    // a real promotion is distinguishable from a command that changed nothing.
    let previous = db::set_user_role(&mut *tx, user.id, role)
        .await?
        .ok_or_else(|| anyhow!("user not found: {}", args.user_id))?;
    let detail = format!("{} -> {role_str}", previous.as_str());
    db::insert_admin_audit(&mut *tx, actor, "set_role", user.id, &detail).await?;
    tx.commit().await?;

    info!(user_id = %user.id, username = %user.username, actor, detail = %detail, "set role");
    println!("set role of {} ({}) to {role_str}", user.username, user.id);

    Ok(())
}

async fn attach_wallet(
    pool: &sqlx::PgPool,
    actor: &str,
    args: AttachWalletArgs,
    input: &mut impl BufRead,
) -> Result<()> {
    let pubkey = Pubkey::from_str(&args.pubkey)
        .map_err(|_| anyhow!("invalid pubkey: {}", args.pubkey))?
        .to_string();

    let user = db::find_user_by_id(pool, args.user_id)
        .await?
        .ok_or_else(|| anyhow!("user not found: {}", args.user_id))?;

    if !confirm(&user, &format!("attach wallet {pubkey}"), args.yes, input)? {
        println!("aborted");
        return Ok(());
    }

    let mut tx = pool.begin().await?;
    let wallet = db::insert_verified_wallet(&mut *tx, user.id, &pubkey)
        .await
        .map_err(|e| match e {
            // Unique constraint on (user_id, pubkey) — wallet already attached.
            AppError::Db(sqlx::Error::Database(ref db_err))
                if db_err.constraint() == Some("verified_wallets_user_id_pubkey_key") =>
            {
                anyhow!("wallet {} is already attached to user {}", pubkey, user.id)
            }
            other => anyhow::Error::new(other),
        })?;
    db::insert_admin_audit(&mut *tx, actor, "attach_wallet", user.id, &wallet.pubkey).await?;
    tx.commit().await?;

    info!(
        user_id = %user.id,
        username = %user.username,
        pubkey = %wallet.pubkey,
        actor,
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
// serializes process-global env-var mutation, so the lock has to stay held for
// the whole `run` call that reads the vars, not just the setup. The lint guards
// against a std guard blocking an executor thread that other tasks need; each
// `#[tokio::test]` here gets its own runtime with a single task, so there is no
// other task to starve. Clippy can't see that, so silence it module-wide.
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use sqlx::PgPool;
    use std::{io::Cursor, sync::Mutex};
    use testcontainers::{runners::AsyncRunner, ContainerAsync};
    use testcontainers_modules::postgres::Postgres;

    // env::set_var / remove_var mutate process-global state. The `run` tests
    // touch AUTH_DATABASE_URL and AUTH_ADMIN_ACTOR — serialize them so they don't
    // race when the surrounding test runner schedules them in parallel.
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

    fn restore(key: &str, prev: Option<String>) {
        match prev {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    // Most tests drive the commands non-interactively. The ones covering the
    // prompt build their args inline with `yes: false` and feed a reader.
    fn attach_args(user_id: Uuid, pubkey: &str) -> AttachWalletArgs {
        AttachWalletArgs {
            user_id,
            pubkey: pubkey.into(),
            yes: true,
        }
    }

    fn set_role_args(user_id: Uuid, role: RoleArg) -> SetRoleArgs {
        SetRoleArgs {
            user_id,
            role,
            yes: true,
        }
    }

    async fn audit_rows(pool: &PgPool) -> Vec<(String, String, Uuid, String)> {
        sqlx::query_as(
            r#"SELECT actor, action, target_user_id, detail FROM private_channel_auth.admin_audit ORDER BY created_at"#,
        )
        .fetch_all(pool)
        .await
        .expect("read audit trail")
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

        let result = run(run_args(Command::AttachWallet(attach_args(
            Uuid::new_v4(),
            "p",
        ))))
        .await;

        restore("AUTH_DATABASE_URL", prev);

        let err = result.expect_err("expected error");
        assert!(err.to_string().contains("is not set"), "got: {err}");
    }

    #[tokio::test]
    async fn run_rejects_non_postgres_url() {
        let _g = ENV_LOCK.lock().unwrap();
        let prev = env::var("AUTH_DATABASE_URL").ok();
        env::set_var("AUTH_DATABASE_URL", "mysql://localhost/test");

        let result = run(run_args(Command::AttachWallet(attach_args(
            Uuid::new_v4(),
            "p",
        ))))
        .await;

        restore("AUTH_DATABASE_URL", prev);

        let err = result.expect_err("expected error");
        assert!(
            err.to_string().contains("must be a PostgreSQL URL"),
            "got: {err}"
        );
    }

    /// A missing actor has to fail before the tool connects, so it can't run DDL
    /// against the auth database and only then refuse the command.
    #[tokio::test]
    async fn run_rejects_missing_admin_actor() {
        let _g = ENV_LOCK.lock().unwrap();
        let prev_url = env::var("AUTH_DATABASE_URL").ok();
        let prev_actor = env::var("AUTH_ADMIN_ACTOR").ok();
        // Unroutable on purpose: reaching the connect step at all is a failure.
        env::set_var(
            "AUTH_DATABASE_URL",
            "postgres://auth:pw@240.0.0.1:5432/auth",
        );
        env::remove_var("AUTH_ADMIN_ACTOR");

        let result = run(run_args(Command::SetRole(set_role_args(
            Uuid::new_v4(),
            RoleArg::Operator,
        ))))
        .await;

        restore("AUTH_DATABASE_URL", prev_url);
        restore("AUTH_ADMIN_ACTOR", prev_actor);

        let err = result.expect_err("expected error");
        assert!(
            err.to_string().contains("AUTH_ADMIN_ACTOR is not set"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn run_rejects_blank_admin_actor() {
        let _g = ENV_LOCK.lock().unwrap();
        let prev_url = env::var("AUTH_DATABASE_URL").ok();
        let prev_actor = env::var("AUTH_ADMIN_ACTOR").ok();
        env::set_var(
            "AUTH_DATABASE_URL",
            "postgres://auth:pw@240.0.0.1:5432/auth",
        );
        env::set_var("AUTH_ADMIN_ACTOR", "   ");

        let result = run(run_args(Command::SetRole(set_role_args(
            Uuid::new_v4(),
            RoleArg::Operator,
        ))))
        .await;

        restore("AUTH_DATABASE_URL", prev_url);
        restore("AUTH_ADMIN_ACTOR", prev_actor);

        let err = result.expect_err("expected error");
        assert!(
            err.to_string()
                .contains("AUTH_ADMIN_ACTOR must not be empty"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn attach_wallet_rejects_invalid_pubkey() {
        let (pool, _c) = start_pool().await;
        let err = attach_wallet(
            &pool,
            "admin",
            attach_args(Uuid::new_v4(), "not-base58"),
            &mut io::empty(),
        )
        .await
        .expect_err("expected error");
        assert!(err.to_string().contains("invalid pubkey"), "got: {err}");
    }

    #[tokio::test]
    async fn attach_wallet_rejects_unknown_user() {
        let (pool, _c) = start_pool().await;
        let pubkey = Pubkey::new_unique().to_string();
        let err = attach_wallet(
            &pool,
            "admin",
            attach_args(Uuid::new_v4(), &pubkey),
            &mut io::empty(),
        )
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

        attach_wallet(
            &pool,
            "admin",
            attach_args(user.id, &pubkey),
            &mut io::empty(),
        )
        .await
        .expect("attach should succeed");

        let wallets = db::list_verified_wallets(&pool, user.id)
            .await
            .expect("list wallets");
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].pubkey, pubkey);

        assert_eq!(
            audit_rows(&pool).await,
            vec![(
                "admin".to_string(),
                "attach_wallet".to_string(),
                user.id,
                pubkey
            )]
        );
    }

    #[tokio::test]
    async fn attach_wallet_reports_already_attached() {
        let (pool, _c) = start_pool().await;
        let user = db::insert_user(&pool, "carol", "$argon2id$placeholder")
            .await
            .expect("insert user");
        let pubkey = Pubkey::new_unique().to_string();

        attach_wallet(
            &pool,
            "admin",
            attach_args(user.id, &pubkey),
            &mut io::empty(),
        )
        .await
        .expect("first attach should succeed");

        let err = attach_wallet(
            &pool,
            "admin",
            attach_args(user.id, &pubkey),
            &mut io::empty(),
        )
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

        // The rolled-back attach must not leave a record of a change that never happened.
        assert_eq!(audit_rows(&pool).await.len(), 1);
    }

    /// Both directions, because the audit detail claims to record the role the
    /// change replaced — not just the one it assigned.
    #[tokio::test]
    async fn set_role_records_each_transition() {
        let (pool, _c) = start_pool().await;
        let user = db::insert_user(&pool, "dave", "$argon2id$placeholder")
            .await
            .expect("insert user");

        set_role(
            &pool,
            "admin",
            set_role_args(user.id, RoleArg::Operator),
            &mut io::empty(),
        )
        .await
        .expect("promotion should succeed");

        let promoted = db::find_user_by_id(&pool, user.id)
            .await
            .expect("find user")
            .expect("user exists");
        assert_eq!(promoted.role, Role::Operator);

        set_role(
            &pool,
            "admin",
            set_role_args(user.id, RoleArg::User),
            &mut io::empty(),
        )
        .await
        .expect("demotion should succeed");

        let demoted = db::find_user_by_id(&pool, user.id)
            .await
            .expect("find user")
            .expect("user exists");
        assert_eq!(demoted.role, Role::User);

        assert_eq!(
            audit_rows(&pool).await,
            vec![
                (
                    "admin".to_string(),
                    "set_role".to_string(),
                    user.id,
                    "user -> operator".to_string()
                ),
                (
                    "admin".to_string(),
                    "set_role".to_string(),
                    user.id,
                    "operator -> user".to_string()
                ),
            ]
        );
    }

    /// The prompt is the control that keeps a grant from landing on the wrong
    /// account, so anything other than `yes` — including a closed stdin — has to
    /// leave both the role and the trail alone.
    #[tokio::test]
    async fn set_role_grants_nothing_without_confirmation() {
        let (pool, _c) = start_pool().await;
        let user = db::insert_user(&pool, "erin", "$argon2id$placeholder")
            .await
            .expect("insert user");

        for answer in ["no\n", "y\n", ""] {
            set_role(
                &pool,
                "admin",
                SetRoleArgs {
                    user_id: user.id,
                    role: RoleArg::Operator,
                    yes: false,
                },
                &mut Cursor::new(answer),
            )
            .await
            .expect("declining is not an error");

            let unchanged = db::find_user_by_id(&pool, user.id)
                .await
                .expect("find user")
                .expect("user exists");
            assert_eq!(
                unchanged.role,
                Role::User,
                "answer {answer:?} must not promote"
            );
            assert!(
                audit_rows(&pool).await.is_empty(),
                "answer {answer:?} must not be recorded"
            );
        }
    }

    /// Runs on every command, so a role without CREATE rights — the
    /// least-privilege deployment — has to get through an initialized database.
    #[tokio::test]
    async fn ensure_schema_skips_ddl_when_already_initialized() {
        let (pool, container) = start_pool().await;
        sqlx::query("CREATE ROLE restricted LOGIN PASSWORD 'password'")
            .execute(&pool)
            .await
            .expect("create role");
        sqlx::query("GRANT USAGE ON SCHEMA private_channel_auth TO restricted")
            .execute(&pool)
            .await
            .expect("grant usage");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let restricted = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!(
                "postgres://restricted:password@{host}:{port}/auth_test"
            ))
            .await
            .expect("connect as restricted");

        // Precondition: the role really can't run the DDL that was previously
        // unconditional, so the assertion below isn't vacuous.
        db::init_schema(&restricted)
            .await
            .expect_err("restricted must lack DDL rights");

        ensure_schema(&restricted).await.expect("must not run DDL");
    }

    #[tokio::test]
    async fn attach_wallet_grants_nothing_without_confirmation() {
        let (pool, _c) = start_pool().await;
        let user = db::insert_user(&pool, "frank", "$argon2id$placeholder")
            .await
            .expect("insert user");
        let pubkey = Pubkey::new_unique().to_string();

        attach_wallet(
            &pool,
            "admin",
            AttachWalletArgs {
                user_id: user.id,
                pubkey,
                yes: false,
            },
            &mut Cursor::new("no\n"),
        )
        .await
        .expect("declining is not an error");

        assert!(db::list_verified_wallets(&pool, user.id)
            .await
            .expect("list wallets")
            .is_empty());
        assert!(audit_rows(&pool).await.is_empty());
    }

    #[tokio::test]
    async fn set_role_rejects_unknown_user() {
        let (pool, _c) = start_pool().await;
        let err = set_role(
            &pool,
            "admin",
            set_role_args(Uuid::new_v4(), RoleArg::Operator),
            &mut io::empty(),
        )
        .await
        .expect_err("expected error");
        assert!(err.to_string().contains("user not found"), "got: {err}");
        assert!(audit_rows(&pool).await.is_empty());
    }
}
