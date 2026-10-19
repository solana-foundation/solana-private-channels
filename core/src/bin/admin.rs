use {
    anyhow::{anyhow, Result},
    clap::{Parser, Subcommand},
    private_channel_core::accounts::{
        postgres::PostgresAccountsDB,
        truncate::{truncate_slots, ConsumerCheck, TruncateOptions, TruncateReport},
    },
    std::path::PathBuf,
    tracing::{error, info, warn},
};

#[derive(Parser, Debug)]
#[command(
    name = "private-channel-admin",
    about = "Manual administrative commands for PrivateChannel core databases"
)]
struct Args {
    /// Accounts database connection URL (PostgreSQL only)
    #[arg(
        long,
        env = "PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL",
        hide_env_values = true
    )]
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

    /// Deprecated and ignored: the dump is now proven to cover every deleted row,
    /// which makes its age irrelevant. Still accepted so existing scripts parse.
    #[arg(long, hide = true)]
    max_backup_age_hours: Option<u64>,

    /// A `pg_dump -Fc` file of this database, proven to hold every row to be deleted
    #[arg(long)]
    pg_dump_path: PathBuf,

    /// The pg_restore used to read the dump; at least the server's major version
    #[arg(long, default_value = "pg_restore")]
    pg_restore_bin: PathBuf,

    /// Number of block rows to process per truncation batch
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,

    /// Show what would be deleted without applying changes
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Deprecated and ignored: the window is read from the ledger, where the write node
    /// publishes it. Still accepted so existing scripts parse.
    #[arg(long, hide = true)]
    max_blockhashes: Option<u64>,

    /// Deprecated and ignored, for the same reason as `--max-blockhashes`.
    #[arg(long, hide = true)]
    blocktime_ms: Option<u64>,

    /// The withdraw indexer's database; truncation refuses to cut past its checkpoint
    #[arg(
        long,
        env = "PRIVATE_CHANNEL_WITHDRAW_INDEXER_DB_URL",
        hide_env_values = true,
        required_unless_present = "skip_withdraw_indexer_check",
        conflicts_with = "skip_withdraw_indexer_check"
    )]
    withdraw_indexer_db_url: Option<String>,

    /// Truncate without checking a withdraw indexer; only for deployments that run none
    #[arg(long, default_value_t = false)]
    skip_withdraw_indexer_check: bool,
}

/// Clap refuses neither and both; if it ever let one through, the check still runs.
fn consumer_check_from_args(url: Option<String>, skip: bool) -> Result<ConsumerCheck> {
    match (url, skip) {
        (Some(database_url), _) => Ok(ConsumerCheck::WithdrawIndexer { database_url }),
        (None, true) => Ok(ConsumerCheck::Skip),
        (None, false) => Err(anyhow!(
            "pass --withdraw-indexer-db-url or --skip-withdraw-indexer-check"
        )),
    }
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
            if truncate_args.max_blockhashes.is_some() || truncate_args.blocktime_ms.is_some() {
                warn!(
                    "--max-blockhashes and --blocktime-ms are deprecated and ignored; the window \
                     is read from the ledger"
                );
            }
            if truncate_args.max_backup_age_hours.is_some() {
                warn!(
                    "--max-backup-age-hours is deprecated and ignored; the dump itself is verified"
                );
            }
            let options = TruncateOptions {
                keep_slots: truncate_args.keep_slots,
                pg_dump_path: Some(truncate_args.pg_dump_path),
                pg_restore_bin: truncate_args.pg_restore_bin,
                batch_size: truncate_args.batch_size,
                dry_run: truncate_args.dry_run,
                consumer_check: consumer_check_from_args(
                    truncate_args.withdraw_indexer_db_url,
                    truncate_args.skip_withdraw_indexer_check,
                )?,
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
        pg_dump_ok = report.backup_check.pg_dump_ok,
        pg_dump_sha256 = ?report.backup_check.sha256,
        "Truncation summary"
    );

    println!("mode: {}", if dry_run { "dry_run" } else { "apply" });
    println!("latest_slot: {:?}", report.latest_slot);
    println!("truncate_before_slot: {:?}", report.truncate_before_slot);
    println!("pg_dump_ok: {}", report.backup_check.pg_dump_ok);
    println!("pg_dump_reason: {}", report.backup_check.pg_dump_reason);
    println!("pg_dump_sha256: {:?}", report.backup_check.sha256);
    println!("blocks_deleted: {}", report.blocks_deleted);
    println!("transactions_deleted: {}", report.transactions_deleted);
    println!(
        "account_history_rows_deleted: {}",
        report.account_history_rows_deleted
    );
    println!("first_available_block: {:?}", report.first_available_block);
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "--accountsdb-connection-url=postgres://x";

    const SKIP: &str = "--skip-withdraw-indexer-check";
    const INDEXER_URL: &str = "--withdraw-indexer-db-url=postgres://indexer";

    fn parse_truncate(extra: &[&str]) -> Result<TruncateArgs, clap::Error> {
        let base = [
            "admin",
            URL,
            "truncate",
            "--keep-slots",
            "1",
            "--pg-dump-path",
            "x",
        ];
        let args = Args::try_parse_from(base.iter().chain(extra))?;
        let Command::Truncate(truncate) = args.command;
        Ok(truncate)
    }

    /// Scripts that still pass the old flags keep parsing; a dump is now required, and so
    /// is a withdraw indexer URL or the explicit skip. The env var would count as a URL.
    #[test]
    fn admin_accepts_deprecated_max_backup_age() {
        std::env::remove_var("PRIVATE_CHANNEL_WITHDRAW_INDEXER_DB_URL");

        let truncate = parse_truncate(&[
            "--max-backup-age-hours",
            "24",
            "--max-blockhashes",
            "150",
            "--blocktime-ms",
            "100",
            SKIP,
        ])
        .expect("the deprecated flags must still parse");
        assert_eq!(truncate.max_backup_age_hours, Some(24));
        assert_eq!(truncate.max_blockhashes, Some(150));
        assert_eq!(truncate.blocktime_ms, Some(100));
        assert_eq!(truncate.pg_restore_bin, PathBuf::from("pg_restore"));

        let missing_dump =
            Args::try_parse_from(["admin", URL, "truncate", "--keep-slots", "1", SKIP]);
        assert!(
            missing_dump.is_err(),
            "a truncation without a dump must not parse"
        );

        assert!(
            parse_truncate(&[]).is_err(),
            "a run with neither the indexer URL nor the skip must not parse"
        );
        assert!(
            parse_truncate(&[INDEXER_URL, SKIP]).is_err(),
            "the URL and the skip are exclusive"
        );

        let gated = parse_truncate(&[INDEXER_URL]).unwrap();
        assert_eq!(
            consumer_check_from_args(
                gated.withdraw_indexer_db_url,
                gated.skip_withdraw_indexer_check
            )
            .unwrap(),
            ConsumerCheck::WithdrawIndexer {
                database_url: "postgres://indexer".to_string()
            }
        );
        let skipped = parse_truncate(&[SKIP]).unwrap();
        assert_eq!(
            consumer_check_from_args(
                skipped.withdraw_indexer_db_url,
                skipped.skip_withdraw_indexer_check
            )
            .unwrap(),
            ConsumerCheck::Skip
        );
        // Off the clap path the mapping fails closed: a URL wins and neither is an error.
        assert!(matches!(
            consumer_check_from_args(Some("postgres://indexer".into()), true),
            Ok(ConsumerCheck::WithdrawIndexer { .. })
        ));
        assert!(consumer_check_from_args(None, false).is_err());
    }
}
