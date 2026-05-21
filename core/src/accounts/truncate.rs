use {
    super::{postgres::PostgresAccountsDB, traits::BlockInfo},
    crate::accounts::address_index_watermark::ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY,
    anyhow::{anyhow, Context, Result},
    sqlx::{Executor, PgPool, Postgres, QueryBuilder, Row},
    std::{
        collections::HashSet,
        fs,
        path::{Path, PathBuf},
        time::{Duration, SystemTime},
    },
};

const FIRST_AVAILABLE_BLOCK_KEY: &str = "first_available_block";
const ACCOUNT_HISTORY_TABLE: &str = "account_history";
const TRUNCATE_ADVISORY_LOCK_ID: i64 = 0x434F4E_54525543; // "CONTRUC" as hex
const MAX_BIND_PARAMS: usize = 60_000;

#[derive(Debug, Clone)]
pub struct TruncateOptions {
    pub keep_slots: u64,
    pub max_backup_age: Duration,
    pub pg_dump_path: Option<PathBuf>,
    pub batch_size: usize,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TruncateReport {
    pub latest_slot: Option<u64>,
    pub truncate_before_slot: Option<u64>,
    pub blocks_deleted: u64,
    pub transactions_deleted: u64,
    pub account_history_rows_deleted: u64,
    pub backup_check: BackupCheckResult,
    pub first_available_block: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct BackupCheckResult {
    pub wal_archive_ok: bool,
    pub wal_archive_reason: String,
    pub pg_dump_ok: bool,
    pub pg_dump_reason: String,
}

impl BackupCheckResult {
    pub fn has_valid_backup(&self) -> bool {
        self.wal_archive_ok || self.pg_dump_ok
    }

    fn skipped() -> Self {
        Self {
            wal_archive_ok: false,
            wal_archive_reason: "Skipped: no rows eligible for truncation".to_string(),
            pg_dump_ok: false,
            pg_dump_reason: "Skipped: no rows eligible for truncation".to_string(),
        }
    }
}

pub async fn truncate_slots(
    db: &PostgresAccountsDB,
    options: &TruncateOptions,
) -> Result<TruncateReport> {
    if options.keep_slots == 0 {
        return Err(anyhow!("keep_slots must be greater than 0"));
    }
    if options.batch_size == 0 {
        return Err(anyhow!("batch_size must be greater than 0"));
    }

    let pool = db.pool.clone();

    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(TRUNCATE_ADVISORY_LOCK_ID)
        .fetch_one(pool.as_ref())
        .await
        .context("Failed to acquire advisory lock")?;
    if !acquired {
        return Err(anyhow!(
            "Another truncation process is already running (advisory lock held)"
        ));
    }

    let result = truncate_slots_inner(pool.as_ref(), options).await;

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(TRUNCATE_ADVISORY_LOCK_ID)
        .execute(pool.as_ref())
        .await
        .context("Failed to release advisory lock")?;

    result
}

async fn truncate_slots_inner(pool: &PgPool, options: &TruncateOptions) -> Result<TruncateReport> {
    let latest_slot = query_latest_slot(pool).await?;

    let Some(latest_slot) = latest_slot else {
        return Ok(TruncateReport {
            latest_slot: None,
            truncate_before_slot: None,
            backup_check: BackupCheckResult::skipped(),
            first_available_block: None,
            ..TruncateReport::default()
        });
    };

    let truncate_before_slot = compute_truncate_before_slot(latest_slot, options.keep_slots);
    let has_account_history = table_exists(pool, ACCOUNT_HISTORY_TABLE).await?;
    let account_history_rows_to_delete = if has_account_history {
        count_account_history_rows_before(pool, truncate_before_slot).await?
    } else {
        0
    };
    let blocks_to_delete = count_blocks_before(pool, truncate_before_slot).await?;

    let should_truncate = blocks_to_delete > 0 || account_history_rows_to_delete > 0;

    let mut report = TruncateReport {
        latest_slot: Some(latest_slot),
        truncate_before_slot: Some(truncate_before_slot),
        first_available_block: query_first_available_slot(pool).await?,
        ..TruncateReport::default()
    };

    if !should_truncate {
        report.backup_check = BackupCheckResult::skipped();
        return Ok(report);
    }

    let backup_check = verify_backup_readiness(
        pool,
        options.pg_dump_path.as_deref(),
        options.max_backup_age,
    )
    .await;
    report.backup_check = backup_check;

    if !report.backup_check.has_valid_backup() {
        return Err(anyhow!(
            "Backup verification failed. WAL: {}. pg_dump: {}",
            report.backup_check.wal_archive_reason,
            report.backup_check.pg_dump_reason
        ));
    }

    if options.dry_run {
        let (_, tx_count) =
            process_block_batches(pool, truncate_before_slot, options.batch_size, true).await?;
        report.blocks_deleted = blocks_to_delete;
        report.transactions_deleted = tx_count;
        report.account_history_rows_deleted = account_history_rows_to_delete;
        return Ok(report);
    }

    let (blocks_deleted, transactions_deleted) =
        process_block_batches(pool, truncate_before_slot, options.batch_size, false).await?;
    report.blocks_deleted = blocks_deleted;
    report.transactions_deleted = transactions_deleted;

    let account_history_rows_deleted = if has_account_history {
        truncate_account_history_rows(pool, truncate_before_slot).await?
    } else {
        0
    };
    report.account_history_rows_deleted = account_history_rows_deleted;

    report.first_available_block =
        set_first_available_block_metadata(pool, query_first_available_slot(pool).await?).await?;

    if blocks_deleted > 0 || transactions_deleted > 0 {
        run_vacuum(pool, &["blocks", "transactions"]).await?;
    }
    if account_history_rows_deleted > 0 {
        run_vacuum(pool, &[ACCOUNT_HISTORY_TABLE]).await?;
    }

    Ok(report)
}

async fn process_block_batches(
    pool: &PgPool,
    truncate_before_slot: u64,
    batch_size: usize,
    dry_run: bool,
) -> Result<(u64, u64)> {
    let mut total_blocks = 0_u64;
    let mut total_transactions = 0_u64;
    let mut last_processed_slot: Option<i64> = None;

    loop {
        let rows = match last_processed_slot {
            Some(last_slot) => {
                sqlx::query(
                    "SELECT slot, data
                     FROM blocks
                     WHERE slot < $1
                       AND slot > $2
                     ORDER BY slot ASC
                     LIMIT $3",
                )
                .bind(truncate_before_slot as i64)
                .bind(last_slot)
                .bind(batch_size as i64)
                .fetch_all(pool)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT slot, data
                     FROM blocks
                     WHERE slot < $1
                     ORDER BY slot ASC
                     LIMIT $2",
                )
                .bind(truncate_before_slot as i64)
                .bind(batch_size as i64)
                .fetch_all(pool)
                .await
            }
        }
        .context("Failed to fetch blocks for truncation")?;

        if rows.is_empty() {
            break;
        }

        let mut slots = Vec::with_capacity(rows.len());
        let mut signatures = HashSet::new();

        for row in rows {
            let slot: i64 = row.get("slot");
            let data: Vec<u8> = row.get("data");
            let block: BlockInfo = bincode::deserialize(&data)
                .with_context(|| format!("Failed to deserialize block at slot {}", slot))?;

            slots.push(slot);
            for signature in block.transaction_signatures {
                signatures.insert(signature.as_ref().to_vec());
            }
        }

        if let Some(slot) = slots.last().copied() {
            last_processed_slot = Some(slot);
        }

        total_blocks += slots.len() as u64;
        total_transactions += signatures.len() as u64;

        if dry_run {
            continue;
        }

        let mut tx = pool
            .begin()
            .await
            .context("Failed to begin truncation transaction")?;

        if !signatures.is_empty() {
            let sig_vec: Vec<Vec<u8>> = signatures.into_iter().collect();
            for chunk in sig_vec.chunks(MAX_BIND_PARAMS) {
                let mut builder: QueryBuilder<'_, Postgres> =
                    QueryBuilder::new("DELETE FROM transactions WHERE signature IN (");
                let mut separated = builder.separated(", ");
                for signature in chunk {
                    separated.push_bind(signature.clone());
                }
                separated.push_unseparated(")");
                builder
                    .build()
                    .execute(&mut *tx)
                    .await
                    .context("Failed to delete old transactions")?;
            }
        }

        let mut builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DELETE FROM blocks WHERE slot IN (");
        let mut separated = builder.separated(", ");
        for slot in slots {
            separated.push_bind(slot);
        }
        separated.push_unseparated(")");
        builder
            .build()
            .execute(&mut *tx)
            .await
            .context("Failed to delete old blocks")?;

        tx.commit()
            .await
            .context("Failed to commit truncation batch transaction")?;
    }

    Ok((total_blocks, total_transactions))
}

async fn table_exists(pool: &PgPool, table_name: &str) -> Result<bool> {
    let oid = sqlx::query_scalar::<_, Option<i64>>("SELECT to_regclass($1)::oid::bigint")
        .bind(table_name)
        .fetch_one(pool)
        .await
        .with_context(|| format!("Failed to check existence of table {}", table_name))?;
    Ok(oid.is_some())
}

async fn count_account_history_rows_before(
    pool: &PgPool,
    truncate_before_slot: u64,
) -> Result<u64> {
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_history WHERE slot < $1")
            .bind(truncate_before_slot as i64)
            .fetch_one(pool)
            .await
            .context("Failed counting account_history rows")?;
    Ok(count as u64)
}

async fn truncate_account_history_rows(pool: &PgPool, truncate_before_slot: u64) -> Result<u64> {
    let result = sqlx::query("DELETE FROM account_history WHERE slot < $1")
        .bind(truncate_before_slot as i64)
        .execute(pool)
        .await
        .context("Failed deleting old account_history rows")?;
    Ok(result.rows_affected())
}

async fn set_first_available_block_metadata(
    pool: &PgPool,
    slot: Option<u64>,
) -> Result<Option<u64>> {
    match slot {
        Some(slot) => {
            sqlx::query(
                "INSERT INTO metadata (key, value) VALUES ($1, $2)
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            )
            .bind(FIRST_AVAILABLE_BLOCK_KEY)
            .bind(slot.to_le_bytes().to_vec())
            .execute(pool)
            .await
            .context("Failed to update first_available_block metadata")?;
            Ok(Some(slot))
        }
        None => {
            sqlx::query("DELETE FROM metadata WHERE key = $1")
                .bind(FIRST_AVAILABLE_BLOCK_KEY)
                .execute(pool)
                .await
                .context("Failed to clear first_available_block metadata")?;
            // Wiped DB must not look consistent to repair.
            sqlx::query("DELETE FROM metadata WHERE key = $1")
                .bind(ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY)
                .execute(pool)
                .await
                .context("Failed to clear address_signatures_flushed_slot metadata")?;
            Ok(None)
        }
    }
}

async fn run_vacuum(pool: &PgPool, table_names: &[&str]) -> Result<()> {
    for table_name in table_names {
        let sql = format!("VACUUM (ANALYZE) {}", table_name);
        pool.execute(sql.as_str())
            .await
            .with_context(|| format!("Failed to VACUUM table {}", table_name))?;
    }
    Ok(())
}

async fn verify_backup_readiness(
    pool: &PgPool,
    pg_dump_path: Option<&Path>,
    max_backup_age: Duration,
) -> BackupCheckResult {
    let (wal_archive_ok, wal_archive_reason) =
        match check_wal_archive_recency(pool, max_backup_age).await {
            Ok(message) => (true, message),
            Err(e) => (false, e.to_string()),
        };

    let (pg_dump_ok, pg_dump_reason) = check_pg_dump_recency(pg_dump_path, max_backup_age);

    BackupCheckResult {
        wal_archive_ok,
        wal_archive_reason,
        pg_dump_ok,
        pg_dump_reason,
    }
}

async fn check_wal_archive_recency(pool: &PgPool, max_backup_age: Duration) -> Result<String> {
    let archive_mode = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'archive_mode'",
    )
    .fetch_one(pool)
    .await
    .context("Unable to read archive_mode from pg_settings")?;

    if archive_mode != "on" && archive_mode != "always" {
        return Err(anyhow!(
            "archive_mode is '{}' (expected 'on' or 'always')",
            archive_mode
        ));
    }

    let archive_command = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'archive_command'",
    )
    .fetch_one(pool)
    .await
    .context("Unable to read archive_command from pg_settings")?;

    if is_noop_archive_command(&archive_command) {
        return Err(anyhow!(
            "archive_command '{}' is a no-op and does not provide recoverable WAL archives",
            archive_command
        ));
    }

    let age_seconds = sqlx::query_scalar::<_, Option<f64>>(
        "SELECT EXTRACT(EPOCH FROM (NOW() - last_archived_time)) FROM pg_stat_archiver",
    )
    .fetch_one(pool)
    .await
    .context("Unable to read last_archived_time from pg_stat_archiver")?;

    let age_seconds = age_seconds.context("No archived WAL segment found in pg_stat_archiver")?;
    if age_seconds > max_backup_age.as_secs_f64() {
        return Err(anyhow!(
            "Latest archived WAL segment is {:.0} seconds old (max allowed: {:.0})",
            age_seconds,
            max_backup_age.as_secs_f64()
        ));
    }

    Ok(format!(
        "WAL archiving healthy; latest archived segment age {:.0} seconds",
        age_seconds
    ))
}

fn check_pg_dump_recency(pg_dump_path: Option<&Path>, max_backup_age: Duration) -> (bool, String) {
    let Some(path) = pg_dump_path else {
        return (false, "No pg_dump path supplied".to_string());
    };

    if !path.is_file() {
        return (
            false,
            format!("pg_dump path '{}' is not a file", path.display()),
        );
    }

    let modified = match fs::metadata(path).and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(e) => {
            return (
                false,
                format!(
                    "Unable to read modified time for '{}': {}",
                    path.display(),
                    e
                ),
            )
        }
    };

    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or(Duration::from_secs(0));

    if age > max_backup_age {
        return (
            false,
            format!(
                "pg_dump artifact '{}' is {} seconds old (max allowed: {})",
                path.display(),
                age.as_secs(),
                max_backup_age.as_secs()
            ),
        );
    }

    (
        true,
        format!(
            "Recent pg_dump artifact '{}' found (age {} seconds)",
            path.display(),
            age.as_secs()
        ),
    )
}

async fn count_blocks_before(pool: &PgPool, truncate_before_slot: u64) -> Result<u64> {
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blocks WHERE slot < $1")
        .bind(truncate_before_slot as i64)
        .fetch_one(pool)
        .await
        .context("Failed to count old blocks")?;
    Ok(count as u64)
}

fn compute_truncate_before_slot(latest_slot: u64, keep_slots: u64) -> u64 {
    latest_slot.saturating_sub(keep_slots.saturating_sub(1))
}

async fn query_latest_slot(pool: &PgPool) -> Result<Option<u64>> {
    let latest_slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(pool)
        .await
        .context("Failed to query latest slot")?;
    Ok(latest_slot.map(|slot| slot as u64))
}

async fn query_first_available_slot(pool: &PgPool) -> Result<Option<u64>> {
    let first_available_slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
        .fetch_one(pool)
        .await
        .context("Failed to query first available slot")?;
    Ok(first_available_slot.map(|slot| slot as u64))
}

fn is_noop_archive_command(command: &str) -> bool {
    let normalized = command.trim().trim_matches('\'').trim_matches('"');
    normalized.is_empty() || normalized == "/bin/true" || normalized == "true" || normalized == ":"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_cutoff_slot_keeps_recent_window() {
        assert_eq!(compute_truncate_before_slot(100, 10), 91);
        assert_eq!(compute_truncate_before_slot(10, 1), 10);
        assert_eq!(compute_truncate_before_slot(8, 16), 0);
    }

    #[test]
    fn compute_cutoff_slot_saturating() {
        assert_eq!(compute_truncate_before_slot(0, 1), 0);
        assert_eq!(compute_truncate_before_slot(0, 100), 0);
        assert_eq!(compute_truncate_before_slot(5, 5), 1);
    }

    #[test]
    fn noop_archive_command_detection_is_strict() {
        assert!(is_noop_archive_command("/bin/true"));
        assert!(is_noop_archive_command(" true "));
        assert!(is_noop_archive_command("':'"));
        assert!(is_noop_archive_command(""));
        assert!(is_noop_archive_command("  "));
        assert!(is_noop_archive_command(":"));
        assert!(is_noop_archive_command("\"true\""));
        assert!(is_noop_archive_command("'/bin/true'"));
        assert!(!is_noop_archive_command("cp %p /backups/%f"));
        assert!(!is_noop_archive_command("wal-g wal-push %p"));
    }

    #[test]
    fn backup_check_result_has_valid_backup() {
        // Neither ok
        let check = BackupCheckResult::default();
        assert!(!check.has_valid_backup());

        // WAL ok only
        let check = BackupCheckResult {
            wal_archive_ok: true,
            ..Default::default()
        };
        assert!(check.has_valid_backup());

        // pg_dump ok only
        let check = BackupCheckResult {
            pg_dump_ok: true,
            ..Default::default()
        };
        assert!(check.has_valid_backup());

        // Both ok
        let check = BackupCheckResult {
            wal_archive_ok: true,
            pg_dump_ok: true,
            ..Default::default()
        };
        assert!(check.has_valid_backup());
    }

    #[test]
    fn backup_check_result_skipped() {
        let check = BackupCheckResult::skipped();
        assert!(!check.has_valid_backup());
        assert!(check.wal_archive_reason.contains("Skipped"));
        assert!(check.pg_dump_reason.contains("Skipped"));
    }

    #[test]
    fn check_pg_dump_no_path() {
        let (ok, reason) = check_pg_dump_recency(None, Duration::from_secs(60));
        assert!(!ok);
        assert!(reason.contains("No pg_dump path"));
    }

    #[test]
    fn check_pg_dump_nonexistent_file() {
        let path = PathBuf::from("/nonexistent/backup.sql");
        let (ok, reason) = check_pg_dump_recency(Some(&path), Duration::from_secs(60));
        assert!(!ok);
        assert!(reason.contains("is not a file"));
    }

    #[test]
    fn check_pg_dump_recent_file() {
        // Create a temp file — its mtime is "now", so it's recent
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let (ok, reason) = check_pg_dump_recency(Some(tmp.path()), Duration::from_secs(3600));
        assert!(ok, "Expected recent file to pass, got: {}", reason);
        assert!(reason.contains("Recent pg_dump"));
    }

    #[test]
    fn truncate_options_fields() {
        let opts = TruncateOptions {
            keep_slots: 100,
            max_backup_age: Duration::from_secs(300),
            pg_dump_path: Some(PathBuf::from("/tmp/backup.sql")),
            batch_size: 500,
            dry_run: true,
        };
        assert_eq!(opts.keep_slots, 100);
        assert!(opts.dry_run);
    }

    #[test]
    fn truncate_report_default() {
        let report = TruncateReport::default();
        assert_eq!(report.latest_slot, None);
        assert_eq!(report.truncate_before_slot, None);
        assert_eq!(report.blocks_deleted, 0);
        assert_eq!(report.transactions_deleted, 0);
        assert_eq!(report.account_history_rows_deleted, 0);
        assert_eq!(report.first_available_block, None);
    }

    #[test]
    fn check_pg_dump_stale_file() {
        // A fresh temp file with max_backup_age=0 will always be "too old"
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let (ok, reason) = check_pg_dump_recency(Some(tmp.path()), Duration::from_secs(0));
        assert!(!ok);
        assert!(reason.contains("seconds old"));
    }

    // --- Integration tests requiring Postgres ---

    use crate::test_helpers::start_test_postgres_raw;

    async fn store_test_blocks(db: &PostgresAccountsDB, slots: &[u64]) {
        let pool = db.pool.clone();
        for &slot in slots {
            let block = BlockInfo {
                slot,
                blockhash: solana_sdk::hash::Hash::new_unique(),
                previous_blockhash: solana_sdk::hash::Hash::default(),
                parent_slot: slot.saturating_sub(1),
                block_height: Some(slot),
                block_time: Some(1_700_000_000 + slot as i64),
                transaction_signatures: vec![solana_sdk::signature::Signature::new_unique()],
                transaction_recent_blockhashes: vec![solana_sdk::hash::Hash::new_unique()],
            };
            let data = bincode::serialize(&block).unwrap();
            sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2) ON CONFLICT (slot) DO UPDATE SET data = $2")
                .bind(slot as i64)
                .bind(&data)
                .execute(pool.as_ref())
                .await
                .unwrap();
            // Also store each transaction signature
            for sig in &block.transaction_signatures {
                sqlx::query(
                    "INSERT INTO transactions (signature, data) VALUES ($1, $2) ON CONFLICT DO NOTHING"
                )
                .bind(sig.as_ref())
                .bind(b"test" as &[u8])
                .execute(pool.as_ref())
                .await
                .unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_rejects_zero_keep_slots() {
        let (db, _pg) = start_test_postgres_raw().await;
        let opts = TruncateOptions {
            keep_slots: 0,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: None,
            batch_size: 100,
            dry_run: false,
        };
        let result = truncate_slots(&db, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("keep_slots"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_rejects_zero_batch_size() {
        let (db, _pg) = start_test_postgres_raw().await;
        let opts = TruncateOptions {
            keep_slots: 10,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: None,
            batch_size: 0,
            dry_run: false,
        };
        let result = truncate_slots(&db, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("batch_size"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_empty_db_returns_early() {
        let (db, _pg) = start_test_postgres_raw().await;
        let opts = TruncateOptions {
            keep_slots: 10,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: None,
            batch_size: 100,
            dry_run: false,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.latest_slot, None);
        assert_eq!(report.blocks_deleted, 0);
        assert!(!report.backup_check.has_valid_backup());
        assert!(report.backup_check.wal_archive_reason.contains("Skipped"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_nothing_to_delete_when_within_keep_window() {
        let (db, _pg) = start_test_postgres_raw().await;
        // Store 5 blocks, keep_slots=10 → nothing to delete
        store_test_blocks(&db, &[0, 1, 2, 3, 4]).await;
        let opts = TruncateOptions {
            keep_slots: 10,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: None,
            batch_size: 100,
            dry_run: false,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.latest_slot, Some(4));
        assert_eq!(report.blocks_deleted, 0);
        assert!(report.backup_check.wal_archive_reason.contains("Skipped"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_fails_without_valid_backup() {
        let (db, _pg) = start_test_postgres_raw().await;
        // Store 20 blocks, keep_slots=5 → 16 blocks eligible for deletion
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;
        let opts = TruncateOptions {
            keep_slots: 5,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: None, // no pg_dump
            batch_size: 100,
            dry_run: false,
        };
        let result = truncate_slots(&db, &opts).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Backup verification failed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_dry_run_does_not_delete() {
        let (db, _pg) = start_test_postgres_raw().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let opts = TruncateOptions {
            keep_slots: 5,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: Some(tmp.path().to_path_buf()),
            batch_size: 100,
            dry_run: true,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.latest_slot, Some(19));
        assert!(
            report.blocks_deleted > 0,
            "dry run should report blocks that would be deleted"
        );
        // Verify blocks are still there
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blocks")
            .fetch_one(db.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(count, 20, "dry run should not actually delete blocks");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_deletes_old_blocks_and_transactions() {
        let (db, _pg) = start_test_postgres_raw().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let opts = TruncateOptions {
            keep_slots: 5,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: Some(tmp.path().to_path_buf()),
            batch_size: 100,
            dry_run: false,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.latest_slot, Some(19));
        // truncate_before_slot = 19 - (5-1) = 15, so slots 0..15 deleted = 15 blocks
        assert_eq!(report.truncate_before_slot, Some(15));
        assert_eq!(report.blocks_deleted, 15);
        assert_eq!(report.transactions_deleted, 15); // 1 tx per block

        // Verify remaining blocks
        let remaining = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blocks")
            .fetch_one(db.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(remaining, 5);

        // first_available_block should be updated
        assert!(report.first_available_block.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_batched_deletion() {
        let (db, _pg) = start_test_postgres_raw().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let opts = TruncateOptions {
            keep_slots: 5,
            max_backup_age: Duration::from_secs(3600),
            pg_dump_path: Some(tmp.path().to_path_buf()),
            batch_size: 3, // small batch to exercise the batching loop
            dry_run: false,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.blocks_deleted, 15);
        assert_eq!(report.transactions_deleted, 15);

        let remaining = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blocks")
            .fetch_one(db.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(remaining, 5);
    }
}
