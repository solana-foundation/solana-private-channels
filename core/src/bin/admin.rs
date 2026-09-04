use {
    anyhow::{anyhow, Result},
    clap::{Parser, Subcommand},
    private_channel_core::{
        accounts::{
            postgres::PostgresAccountsDB,
            truncate::{retention_floor_slots, truncate_slots, TruncateOptions, TruncateReport},
        },
        nodes::node::{DEFAULT_BLOCKTIME_MS, DEFAULT_MAX_BLOCKHASHES},
    },
    std::{path::PathBuf, time::Duration},
    tracing::{error, info, warn},
};

#[derive(Parser, Debug)]
#[command(
    name = "private-channel-admin",
    about = "Manual administrative commands for PrivateChannel core databases"
)]
struct Args {
    /// Accounts database connection URL (PostgreSQL only)
    #[arg(long, env = "PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL")]
    accountsdb_connection_url: String,

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
    /// Truncate old slot/transaction data from the core ledger DB
    Truncate(TruncateArgs),
}

#[derive(Parser, Debug)]
struct TruncateArgs {
    /// Keep this many most recent slots; older slots are eligible for deletion
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    keep_slots: u64,

    /// Maximum allowed backup age before truncation is blocked
    #[arg(
        long,
        default_value_t = 24,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    max_backup_age_hours: u64,

    /// Optional pg_dump artifact path (file or directory)
    #[arg(long)]
    pg_dump_path: Option<PathBuf>,

    /// Number of block rows to process per truncation batch
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,

    /// Show what would be deleted without applying changes
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// The node's max_blockhashes, to size the retention floor warning
    #[arg(long, default_value_t = DEFAULT_MAX_BLOCKHASHES as u64, value_parser = clap::value_parser!(u64).range(1..))]
    max_blockhashes: u64,

    /// The node's blocktime_ms, to size the retention floor warning
    #[arg(long, default_value_t = DEFAULT_BLOCKTIME_MS, value_parser = clap::value_parser!(u64).range(1..))]
    blocktime_ms: u64,
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
    if !args.accountsdb_connection_url.starts_with("postgres://")
        && !args.accountsdb_connection_url.starts_with("postgresql://")
    {
        return Err(anyhow!(
            "Truncation CLI only supports PostgreSQL accounts DB URLs"
        ));
    }

    let db = PostgresAccountsDB::new(&args.accountsdb_connection_url, false)
        .await
        .map_err(|e| anyhow!("Failed to connect to PostgreSQL accounts DB: {}", e))?;

    match args.command {
        Command::Truncate(truncate_args) => {
            // A warning, not a refusal: the floor is the idle worst case, and a
            // node under steady load keeps its whole window in far fewer slots.
            let floor = retention_floor_slots(
                truncate_args.max_blockhashes as usize,
                truncate_args.blocktime_ms,
            );
            if truncate_args.keep_slots < floor {
                warn!(
                    "keep_slots {} is below the retention floor of {} ({} blockhashes at {}ms blocktime); \
                     a restart will rebuild the dedup window short and reject transactions still inside \
                     their published deadline",
                    truncate_args.keep_slots,
                    floor,
                    truncate_args.max_blockhashes,
                    truncate_args.blocktime_ms
                );
            }
            let options = TruncateOptions {
                keep_slots: truncate_args.keep_slots,
                max_backup_age: Duration::from_secs(
                    truncate_args
                        .max_backup_age_hours
                        .saturating_mul(60)
                        .saturating_mul(60),
                ),
                pg_dump_path: truncate_args.pg_dump_path,
                batch_size: truncate_args.batch_size,
                dry_run: truncate_args.dry_run,
            };
            let report = truncate_slots(&db, &options).await?;
            print_report(&report, truncate_args.dry_run);
        }
    }

    Ok(())
}

fn print_report(report: &TruncateReport, dry_run: bool) {
    info!(
        mode = if dry_run { "dry_run" } else { "apply" },
        latest_slot = ?report.latest_slot,
        truncate_before_slot = ?report.truncate_before_slot,
        blocks_deleted = report.blocks_deleted,
        transactions_deleted = report.transactions_deleted,
        account_history_rows_deleted = report.account_history_rows_deleted,
        first_available_block = ?report.first_available_block,
        wal_archive_ok = report.backup_check.wal_archive_ok,
        pg_dump_ok = report.backup_check.pg_dump_ok,
        "Truncation summary"
    );

    println!("mode: {}", if dry_run { "dry_run" } else { "apply" });
    println!("latest_slot: {:?}", report.latest_slot);
    println!("truncate_before_slot: {:?}", report.truncate_before_slot);
    println!("wal_archive_ok: {}", report.backup_check.wal_archive_ok);
    println!(
        "wal_archive_reason: {}",
        report.backup_check.wal_archive_reason
    );
    println!("pg_dump_ok: {}", report.backup_check.pg_dump_ok);
    println!("pg_dump_reason: {}", report.backup_check.pg_dump_reason);
    println!("blocks_deleted: {}", report.blocks_deleted);
    println!("transactions_deleted: {}", report.transactions_deleted);
    println!(
        "account_history_rows_deleted: {}",
        report.account_history_rows_deleted
    );
    println!("first_available_block: {:?}", report.first_available_block);
}
