use {
    super::{
        pg_dump_proof::{check_transaction_cap, prove_dump, LiveLedger},
        postgres::PostgresAccountsDB,
        redis_coherence::read_deployment_id,
        traits::BlockInfo,
    },
    crate::accounts::address_index_watermark::ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY,
    anyhow::{anyhow, Context, Result},
    sha2::{Digest, Sha256},
    sqlx::{Connection, Executor, PgConnection, PgPool, Postgres, QueryBuilder, Row},
    std::{
        collections::{HashMap, HashSet},
        path::{Path, PathBuf},
    },
    tracing::error,
};

const FIRST_AVAILABLE_BLOCK_KEY: &str = "first_available_block";

/// The fewest slots a retention may keep and still hold the whole blockhash
/// window after a restart: the window in blocks, times the slots an idle node
/// ticks per block it produces. One slot per block is the loaded floor.
pub fn retention_floor_slots(max_blockhashes: usize, blocktime_ms: u64) -> u64 {
    let heartbeat_ms = crate::stages::settle::HEARTBEAT_INTERVAL.as_millis() as u64;
    let idle_slots_per_block = (heartbeat_ms / blocktime_ms.max(1)).max(1);
    (max_blockhashes as u64).saturating_mul(idle_slots_per_block)
}
const ACCOUNT_HISTORY_TABLE: &str = "account_history";
const TRUNCATE_ADVISORY_LOCK_ID: i64 = 0x434F4E_54525543; // "CONTRUC" as hex
const MAX_BIND_PARAMS: usize = 60_000;

#[derive(Debug, Clone)]
pub struct TruncateOptions {
    pub keep_slots: u64,
    /// A `pg_dump -Fc` of this database. Without one nothing is deleted.
    pub pg_dump_path: Option<PathBuf>,
    /// The `pg_restore` that reads the dump; at least the server's major version.
    pub pg_restore_bin: PathBuf,
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
    pub pg_dump_ok: bool,
    pub pg_dump_reason: String,
    /// SHA-256 of the dump bytes that were proven, to match against the kept backup.
    pub sha256: Option<String>,
}

impl BackupCheckResult {
    /// Only a verified dump counts. WAL recency and file age prove nothing restorable.
    pub fn has_valid_backup(&self) -> bool {
        self.pg_dump_ok
    }

    fn skipped() -> Self {
        Self {
            pg_dump_ok: false,
            pg_dump_reason: "Skipped: no rows eligible for truncation".to_string(),
            sha256: None,
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

    // The lock belongs to one session, so acquire and release have to run on the
    // same connection. Unlocking from a different one is a silent no-op that
    // strands the lock until that connection is recycled.
    //
    // The session is opened outside the pool, not reserved from it: the work below
    // needs the pool, and a one-connection pool is a legal setting. sqlx also does
    // not reset a returned connection, so a stranded lock would never free.
    let mut lock_conn = PgConnection::connect_with(&pool.connect_options())
        .await
        .context("Failed to open the truncation lock session")?;

    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(TRUNCATE_ADVISORY_LOCK_ID)
        .fetch_one(&mut lock_conn)
        .await
        .context("Failed to acquire advisory lock")?;
    if !acquired {
        return Err(anyhow!(
            "Another truncation process is already running (advisory lock held)"
        ));
    }

    let result = truncate_slots_inner(db, &mut lock_conn, options).await;

    // Read the answer rather than discarding it: false means this session did not
    // hold the lock, which is the bug above and must not pass unnoticed.
    let released = match sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(TRUNCATE_ADVISORY_LOCK_ID)
        .fetch_one(&mut lock_conn)
        .await
    {
        Ok(released) => released,
        Err(e) => {
            // Returning drops the session, and closing its socket frees the lock.
            if let Err(run_err) = result {
                error!("Truncation failed before its lock could be released: {run_err:#}");
            }
            return Err(anyhow!(e).context("Failed to release advisory lock"));
        }
    };
    if !released {
        if let Err(run_err) = result {
            error!("Truncation failed, and its lock was not held by this session: {run_err:#}");
        }
        return Err(anyhow!(
            "Truncation advisory lock was not held by the releasing session"
        ));
    }

    result
}

async fn truncate_slots_inner(
    db: &PostgresAccountsDB,
    lock_conn: &mut PgConnection,
    options: &TruncateOptions,
) -> Result<TruncateReport> {
    let pool = db.pool.as_ref();
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

    // Taken after T is fixed: rows written during the proof land at or above T and are
    // never deleted. Blocks land in slot order, so nothing new can appear below T.
    let transactions = live_doomed_transactions(pool, truncate_before_slot).await?;
    let live = LiveLedger {
        deployment_id: read_deployment_id(db).await?,
        truncate_before_slot,
        live_min_slot: report.first_available_block.unwrap_or(0),
        live_block_count: blocks_to_delete,
        account_history_count: has_account_history.then_some(account_history_rows_to_delete),
        account_history_min_slot: if has_account_history {
            min_account_history_slot(pool).await?
        } else {
            0
        },
        transactions,
    };
    let dump_path = options.pg_dump_path.clone();
    let pg_restore_bin = options.pg_restore_bin.clone();
    report.backup_check = tokio::task::spawn_blocking(move || {
        verify_backup(dump_path.as_deref(), &pg_restore_bin, &live)
    })
    .await
    .context("Backup verification task failed")?;

    if !report.backup_check.has_valid_backup() {
        return Err(anyhow!(
            "Backup verification failed. pg_dump: {}",
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

    // The proof can take long on an idle lock session; a dropped session frees the lock.
    ensure_lock_held(lock_conn).await?;

    let (blocks_deleted, transactions_deleted) =
        process_block_batches(pool, truncate_before_slot, options.batch_size, false).await?;
    report.blocks_deleted = blocks_deleted;
    report.transactions_deleted = transactions_deleted;

    let account_history_rows_deleted = if has_account_history {
        truncate_account_history_rows(pool, truncate_before_slot, account_history_rows_to_delete)
            .await?
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

        // The loop already broke on an empty fetch, so this batch has set the cursor.
        let batch_max_slot = last_processed_slot.expect("a non-empty batch sets the cursor");
        // Query on the batch transaction so it sees deletions this batch has not committed yet.
        // Bound it by the cursor so the scan skips the index entries this run already deleted.
        let remaining_floor = query_first_available_slot_above(&mut *tx, batch_max_slot)
            .await?
            .ok_or_else(|| {
                // Unreachable: keep_slots is at least 1, so the newest block is never
                // deleted and always survives above the cursor.
                anyhow!("No blocks remain above slot {batch_max_slot} after a truncation batch")
            })?;
        // Advance the advertised floor in the same transaction as the deletions.
        upsert_first_available_block(&mut *tx, remaining_floor).await?;

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

async fn min_account_history_slot(pool: &PgPool) -> Result<u64> {
    let min = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM account_history")
        .fetch_one(pool)
        .await
        .context("Failed to query the oldest account_history slot")?;
    Ok(min.unwrap_or(0).max(0) as u64)
}

/// Delete exactly the rows the dump was proven to hold. No writer of this table is
/// visible here, so a different count rolls back instead of deleting unproven rows.
async fn truncate_account_history_rows(
    pool: &PgPool,
    truncate_before_slot: u64,
    proven: u64,
) -> Result<u64> {
    let mut tx = pool
        .begin()
        .await
        .context("Failed to begin account_history deletion")?;
    let deleted = sqlx::query("DELETE FROM account_history WHERE slot < $1")
        .bind(truncate_before_slot as i64)
        .execute(&mut *tx)
        .await
        .context("Failed deleting old account_history rows")?
        .rows_affected();
    if deleted != proven {
        return Err(anyhow!(
            "account_history deletion would remove {deleted} rows but the dump proved {proven}; \
             rolled back"
        ));
    }
    tx.commit()
        .await
        .context("Failed to commit account_history deletion")?;
    Ok(deleted)
}

/// Abort unless this session still holds the truncation lock.
async fn ensure_lock_held(lock_conn: &mut PgConnection) -> Result<()> {
    let held = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
         AND pid = pg_backend_pid() AND granted AND classid::bigint = $1 \
         AND objid::bigint = $2 AND objsubid = 1)",
    )
    .bind(TRUNCATE_ADVISORY_LOCK_ID >> 32)
    .bind(TRUNCATE_ADVISORY_LOCK_ID & 0xFFFF_FFFF)
    .fetch_one(lock_conn)
    .await
    .context("Failed to re-check the truncation lock before deleting")?;
    if !held {
        return Err(anyhow!(
            "The truncation lock was lost before deleting; nothing was deleted"
        ));
    }
    Ok(())
}

/// Write the advertised ledger floor, the oldest slot this node still retains.
/// Takes any executor so a batch can write it on its own open transaction.
async fn upsert_first_available_block<'e, E>(executor: E, slot: u64) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        "INSERT INTO metadata (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(FIRST_AVAILABLE_BLOCK_KEY)
    .bind(slot.to_le_bytes().to_vec())
    .execute(executor)
    .await
    .context("Failed to update first_available_block metadata")?;
    Ok(())
}

async fn set_first_available_block_metadata(
    pool: &PgPool,
    slot: Option<u64>,
) -> Result<Option<u64>> {
    match slot {
        Some(slot) => {
            upsert_first_available_block(pool, slot).await?;
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

/// Build the backup verdict. Any failure, including no dump at all, refuses deletion.
fn verify_backup(
    pg_dump_path: Option<&Path>,
    pg_restore_bin: &Path,
    live: &LiveLedger,
) -> BackupCheckResult {
    let Some(path) = pg_dump_path else {
        return BackupCheckResult {
            pg_dump_ok: false,
            pg_dump_reason: "No pg_dump path supplied".to_string(),
            sha256: None,
        };
    };
    match prove_dump(pg_restore_bin, path, live) {
        Ok(sha256) => BackupCheckResult {
            pg_dump_ok: true,
            pg_dump_reason: format!(
                "pg_dump '{}' restores every row this run deletes",
                path.display()
            ),
            sha256: Some(sha256),
        },
        Err(e) => BackupCheckResult {
            pg_dump_ok: false,
            pg_dump_reason: format!("{e:#}"),
            sha256: None,
        },
    }
}

/// Blocks, then transaction rows, read per query when collecting what truncation deletes.
const LIVE_SCAN_BLOCKS: i64 = 1_000;
const LIVE_SCAN_TRANSACTIONS: usize = 1_000;

/// The transaction rows truncation deletes (named by a block below the cut, and present),
/// as signature to SHA-256 of data. Rows are immutable once written, so a hash is stable.
async fn live_doomed_transactions(
    pool: &PgPool,
    truncate_before_slot: u64,
) -> Result<HashMap<[u8; 64], [u8; 32]>> {
    let mut transactions = HashMap::new();
    let mut after_slot = -1_i64;
    loop {
        let blocks = sqlx::query(
            "SELECT slot, data FROM blocks WHERE slot < $1 AND slot > $2 ORDER BY slot LIMIT $3",
        )
        .bind(truncate_before_slot as i64)
        .bind(after_slot)
        .bind(LIVE_SCAN_BLOCKS)
        .fetch_all(pool)
        .await
        .context("Failed to fetch blocks for the dump proof")?;
        let Some(last) = blocks.last() else {
            break;
        };
        after_slot = last.get("slot");

        let mut named = Vec::new();
        for row in blocks {
            let slot: i64 = row.get("slot");
            let block: BlockInfo = bincode::deserialize(row.get::<&[u8], _>("data"))
                .with_context(|| format!("Failed to deserialize block at slot {slot}"))?;
            named.extend(
                block
                    .transaction_signatures
                    .iter()
                    .map(|s| s.as_ref().to_vec()),
            );
        }
        for chunk in named.chunks(LIVE_SCAN_TRANSACTIONS) {
            let rows =
                sqlx::query("SELECT signature, data FROM transactions WHERE signature = ANY($1)")
                    .bind(chunk)
                    .fetch_all(pool)
                    .await
                    .context("Failed to fetch transactions for the dump proof")?;
            for row in rows {
                let key: [u8; 64] = row
                    .get::<&[u8], _>("signature")
                    .try_into()
                    .map_err(|_| anyhow!("a live transaction signature is not 64 bytes"))?;
                transactions
                    .entry(key)
                    .or_insert_with(|| Sha256::digest(row.get::<&[u8], _>("data")).into());
            }
            check_transaction_cap(transactions.len())?;
        }
    }
    Ok(transactions)
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

/// Oldest retained slot strictly above `slot`.
///
/// Deletion walks slots in ascending order, so once a batch has removed
/// everything up to its highest slot, nothing live remains at or below it and
/// this equals `MIN(slot)` over the whole table.
async fn query_first_available_slot_above<'e, E>(executor: E, slot: i64) -> Result<Option<u64>>
where
    E: Executor<'e, Database = Postgres>,
{
    let first_available_slot =
        sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks WHERE slot > $1")
            .bind(slot)
            .fetch_one(executor)
            .await
            .context("Failed to query remaining first available slot")?;
    Ok(first_available_slot.map(|slot| slot as u64))
}

async fn query_first_available_slot(pool: &PgPool) -> Result<Option<u64>> {
    let first_available_slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
        .fetch_one(pool)
        .await
        .context("Failed to query first available slot")?;
    Ok(first_available_slot.map(|slot| slot as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
    fn backup_check_result_skipped() {
        let check = BackupCheckResult::skipped();
        assert!(!check.has_valid_backup());
        assert!(check.pg_dump_reason.contains("Skipped"));
        assert_eq!(check.sha256, None);
    }

    /// No dump means no proof, whatever else the database looks like.
    #[test]
    fn gate_requires_pg_dump() {
        let live = LiveLedger {
            deployment_id: vec![1],
            truncate_before_slot: 10,
            live_min_slot: 0,
            live_block_count: 10,
            account_history_count: None,
            account_history_min_slot: 0,
            transactions: Default::default(),
        };
        let check = verify_backup(None, Path::new("pg_restore"), &live);
        assert!(!check.has_valid_backup());
        assert!(check.pg_dump_reason.contains("No pg_dump path"));
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

    /// The floor is the dedup window at the widest block spacing the node has,
    /// which moves with the blocktime and never drops below one slot per block.
    #[test]
    fn retention_floor_tracks_the_idle_gap() {
        for (max_blockhashes, blocktime_ms, floor) in [
            (150, 100, 1_500),
            (150, 10, 15_000),
            (150, 1_000, 150),
            (150, 5_000, 150),
            (1, 100, 10),
        ] {
            assert_eq!(
                retention_floor_slots(max_blockhashes, blocktime_ms),
                floor,
                "{max_blockhashes} blockhashes at {blocktime_ms}ms"
            );
        }
    }

    // --- Integration tests requiring Postgres ---

    use crate::test_helpers::{
        container_pg_dump, container_pg_restore_bin, executable_script, start_test_postgres_raw,
        start_test_postgres_with_url,
    };
    use std::time::Duration;

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
                transaction_message_hashes: vec![solana_sdk::hash::Hash::new_unique()],
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

    /// Read the advertised ledger floor exactly as the RPC read path decodes it.
    async fn read_floor_metadata(pool: &PgPool) -> Option<u64> {
        sqlx::query_scalar::<_, Option<Vec<u8>>>(
            "SELECT value FROM metadata WHERE key = 'first_available_block'",
        )
        .fetch_optional(pool)
        .await
        .unwrap()
        .flatten()
        .map(|value| {
            let bytes: [u8; 8] = value.as_slice().try_into().expect("floor is 8 bytes");
            u64::from_le_bytes(bytes)
        })
    }

    /// Ground truth for the oldest retained block, independent of the metadata key.
    async fn min_block_slot(pool: &PgPool) -> Option<u64> {
        sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
            .fetch_one(pool)
            .await
            .unwrap()
            .map(|slot| slot as u64)
    }

    /// Make deleting one block fail so the batch loop aborts at a known slot. A corrupt
    /// block would not do: the proof decodes every doomed block before anything is deleted.
    async fn pin_block(pool: &PgPool, slot: u64) {
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION refuse_pinned_delete() RETURNS trigger AS $$
             BEGIN
                 IF OLD.slot = {slot} THEN RAISE EXCEPTION 'slot {slot} is pinned'; END IF;
                 RETURN OLD;
             END $$ LANGUAGE plpgsql;
             CREATE TRIGGER pin_block BEFORE DELETE ON blocks
                 FOR EACH ROW EXECUTE FUNCTION refuse_pinned_delete();"
        ))
        .execute(pool)
        .await
        .unwrap();
    }

    /// Run one truncation against a pool of its own and prove the lock is gone
    /// before the pool is torn down.
    ///
    /// The check runs while the pool is still open on purpose. `truncate_slots`
    /// holds the lock on one reserved connection and releases it there, so the
    /// lock must be gone the moment it returns. Closing the pool first would free
    /// the lock as a side effect and hide a release that never happened.
    async fn truncate_on_fresh_pool(
        url: &str,
        options: &TruncateOptions,
        observer: &PgPool,
    ) -> Result<TruncateReport> {
        let db = PostgresAccountsDB::new(url, false)
            .await
            .expect("test pool connects");
        let report = truncate_slots(&db, options).await;
        assert_advisory_locks_released(observer).await;
        db.pool.close().await;
        report
    }

    /// No advisory lock may be held once a truncation has returned. Asserted once
    /// rather than polled: the release is synchronous, so a retry loop here would
    /// only turn a stranded lock into a slow test that passes.
    async fn assert_advisory_locks_released(pool: &PgPool) {
        let held = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_locks is readable");
        assert_eq!(
            held, 0,
            "truncate_slots must release its advisory lock before returning"
        );
    }

    /// A one-connection pool is a legal setting, so the lock session must not come
    /// out of the pool: taking it from there leaves nothing for the truncation
    /// itself, which then waits out the acquire timeout and fails.
    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_runs_on_a_single_connection_pool() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;

        // A short timeout so a regression fails in seconds rather than the 30s default.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await
            .expect("single-connection pool connects");
        let starved = PostgresAccountsDB {
            pool: Arc::new(pool),
            read_only: false,
            writer_epoch: None,
        };

        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);
        let report = truncate_slots(&starved, &apply_opts(5, 100, dump.path(), &restore))
            .await
            .expect("truncation must not starve itself of connections");
        assert_eq!(report.blocks_deleted, 15);
    }

    /// A pool whose sessions resolve `pg_advisory_unlock` to a function that raises.
    ///
    /// The acquire still takes the real lock, so this reproduces the one case that
    /// actually strands a key: the release fails while the session stays alive and goes on
    /// holding it. Terminating the backend would not do, since that frees the lock itself
    /// and the bug would vanish with it.
    async fn pool_whose_release_fails(url: &str) -> PgPool {
        let mut setup = sqlx::postgres::PgConnection::connect(url)
            .await
            .expect("setup connects");
        for sql in [
            "CREATE SCHEMA IF NOT EXISTS shadow",
            "CREATE OR REPLACE FUNCTION shadow.pg_advisory_unlock(bigint) RETURNS boolean
             AS $$ BEGIN RAISE EXCEPTION 'release refused'; END; $$ LANGUAGE plpgsql",
        ] {
            sqlx::query(sql)
                .execute(&mut setup)
                .await
                .expect("shadow function is created");
        }
        setup.close().await.expect("setup closes");

        // Set at connect time rather than in a hook, so the lock session opened from the
        // pool's options is shadowed too. pg_catalog is named second so the shadow wins.
        let options = url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .expect("test url parses")
            .options([("search_path", "shadow,pg_catalog,public")]);
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .expect("shadowed pool connects")
    }

    /// Advisory locks still held anywhere on the database.
    async fn advisory_locks_held(pool: &PgPool) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'")
            .fetch_one(pool)
            .await
            .expect("pg_locks is readable")
    }

    /// A release that fails leaves this session still holding the key, so the connection
    /// must not go back to the pool. Pooled, it would strand the lock and refuse every
    /// later truncation until that connection happened to be recycled.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_release_does_not_return_a_locked_connection_to_the_pool() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;
        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);

        let shadowed = PostgresAccountsDB {
            pool: Arc::new(pool_whose_release_fails(&url).await),
            read_only: false,
            writer_epoch: None,
        };
        let outcome = truncate_slots(&shadowed, &apply_opts(10, 3, dump.path(), &restore)).await;
        assert!(
            outcome.is_err(),
            "a release that raises must fail the run, got {outcome:?}"
        );

        // Bounded poll, not tolerance: a detached socket frees the key a moment after the
        // close, while a pooled one holds it for as long as the pool lives, so this cannot
        // pass by waiting.
        for _ in 0..50 {
            if advisory_locks_held(db.pool.as_ref()).await == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("a connection that could not release its lock must not be pooled still holding it");
    }

    fn apply_opts(
        keep_slots: u64,
        batch_size: usize,
        backup: &Path,
        pg_restore_bin: &Path,
    ) -> TruncateOptions {
        TruncateOptions {
            keep_slots,
            pg_dump_path: Some(backup.to_path_buf()),
            pg_restore_bin: pg_restore_bin.to_path_buf(),
            batch_size,
            dry_run: false,
        }
    }

    /// Drop every block, transaction and floor entry so one container can serve
    /// several independent truncation scenarios.
    async fn reset_ledger(pool: &PgPool) {
        for sql in [
            "DELETE FROM blocks",
            "DELETE FROM transactions",
            "DELETE FROM metadata WHERE key = 'first_available_block'",
        ] {
            sqlx::query(sql).execute(pool).await.unwrap();
        }
    }

    /// U1: the advertised floor must equal the retained minimum at every batching
    /// granularity, including single-row batches and runs that fit in one batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_floor_matches_min_slot_across_batch_sizes() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let restore = container_pg_restore_bin(&_pg);

        for batch_size in [1_usize, 3, 1000] {
            reset_ledger(&db.pool).await;
            store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;
            let dump = container_pg_dump(&_pg, "pg_test");

            let report = truncate_on_fresh_pool(
                &url,
                &apply_opts(5, batch_size, dump.path(), &restore),
                &db.pool,
            )
            .await
            .unwrap();

            let floor = read_floor_metadata(&db.pool).await;
            let min_slot = min_block_slot(&db.pool).await;
            assert_eq!(
                floor, min_slot,
                "batch_size {batch_size}: advertised floor must equal the retained minimum"
            );
            assert_eq!(
                report.first_available_block, min_slot,
                "batch_size {batch_size}: report must carry the same floor"
            );
            assert_eq!(min_slot, Some(15), "batch_size {batch_size}: keep window");
        }
    }

    /// U2: regression test. A run that aborts after committing batches must leave
    /// the advertised floor at the retained minimum, never at the pre-run value,
    /// because an absence-based finality verdict treats that floor as proof the
    /// endpoint still holds the slot range it is being asked about.
    ///
    /// Slots 16 to 19 are deliberately absent. The batch that ends at slot 15 is
    /// therefore followed by slot 20, so a floor derived from the batch cursor
    /// instead of the surviving rows would claim slots that were never stored.
    #[tokio::test(flavor = "multi_thread")]
    async fn aborted_run_leaves_floor_at_retained_minimum() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let seeded: Vec<u64> = (0..=15).chain(20..=30).collect();
        store_test_blocks(&db, &seeded).await;
        // One dump serves both runs: blocks the first run deletes are below the live minimum.
        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);

        // First run establishes the metadata key; without it the reader falls back
        // to MIN(slot) and the stale-floor window cannot be observed at all.
        let first =
            truncate_on_fresh_pool(&url, &apply_opts(21, 100, dump.path(), &restore), &db.pool)
                .await
                .unwrap();
        assert_eq!(first.truncate_before_slot, Some(10));
        assert_eq!(read_floor_metadata(&db.pool).await, Some(10));

        pin_block(&db.pool, 20).await;

        // Batches of 3 delete slots 10-12 and 13-15, then abort on slot 20.
        let aborted =
            truncate_on_fresh_pool(&url, &apply_opts(5, 3, dump.path(), &restore), &db.pool).await;
        assert!(aborted.is_err(), "run must abort on the pinned block");
        assert!(aborted
            .unwrap_err()
            .to_string()
            .contains("Failed to delete old blocks"));

        assert_eq!(min_block_slot(&db.pool).await, Some(20));
        assert_eq!(
            read_floor_metadata(&db.pool).await,
            Some(20),
            "floor must not advertise slots the aborted run already deleted"
        );

        let accounts_db = crate::accounts::AccountsDB::Postgres(db.clone());
        assert_eq!(accounts_db.get_first_available_block().await.unwrap(), 20);
    }

    /// U3: the floor write sits inside the batch loop, one `continue` away from the
    /// dry-run path, so a dry run must not overwrite an existing floor either.
    #[tokio::test(flavor = "multi_thread")]
    async fn dry_run_never_writes_floor_even_when_key_exists() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;
        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);

        truncate_on_fresh_pool(&url, &apply_opts(10, 3, dump.path(), &restore), &db.pool)
            .await
            .unwrap();
        let floor_before = read_floor_metadata(&db.pool).await;
        let blocks_before = min_block_slot(&db.pool).await;
        assert_eq!(floor_before, Some(10));

        let mut dry = apply_opts(2, 3, dump.path(), &restore);
        dry.dry_run = true;
        let report = truncate_on_fresh_pool(&url, &dry, &db.pool).await.unwrap();
        assert!(
            report.blocks_deleted > 0,
            "dry run should report work to do"
        );

        assert_eq!(
            read_floor_metadata(&db.pool).await,
            floor_before,
            "dry run must not move the advertised floor"
        );
        assert_eq!(min_block_slot(&db.pool).await, blocks_before);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_rejects_zero_keep_slots() {
        let (db, _pg) = start_test_postgres_raw().await;
        let opts = TruncateOptions {
            keep_slots: 0,
            pg_dump_path: None,
            pg_restore_bin: PathBuf::from("pg_restore"),
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
            pg_dump_path: None,
            pg_restore_bin: PathBuf::from("pg_restore"),
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
            pg_dump_path: None,
            pg_restore_bin: PathBuf::from("pg_restore"),
            batch_size: 100,
            dry_run: false,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.latest_slot, None);
        assert_eq!(report.blocks_deleted, 0);
        assert!(!report.backup_check.has_valid_backup());
        assert!(report.backup_check.pg_dump_reason.contains("Skipped"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_nothing_to_delete_when_within_keep_window() {
        let (db, _pg) = start_test_postgres_raw().await;
        // Store 5 blocks, keep_slots=10 → nothing to delete
        store_test_blocks(&db, &[0, 1, 2, 3, 4]).await;
        let opts = TruncateOptions {
            keep_slots: 10,
            pg_dump_path: None,
            pg_restore_bin: PathBuf::from("pg_restore"),
            batch_size: 100,
            dry_run: false,
        };
        let report = truncate_slots(&db, &opts).await.unwrap();
        assert_eq!(report.latest_slot, Some(4));
        assert_eq!(report.blocks_deleted, 0);
        assert!(report.backup_check.pg_dump_reason.contains("Skipped"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn truncate_fails_without_valid_backup() {
        let (db, _pg) = start_test_postgres_raw().await;
        // Store 20 blocks, keep_slots=5 → 16 blocks eligible for deletion
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;
        let opts = TruncateOptions {
            keep_slots: 5,
            pg_dump_path: None,
            pg_restore_bin: PathBuf::from("pg_restore"), // no pg_dump
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

        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);
        let opts = TruncateOptions {
            keep_slots: 5,
            pg_dump_path: Some(dump.path().to_path_buf()),
            pg_restore_bin: restore.to_path_buf(),
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

        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);
        let opts = TruncateOptions {
            keep_slots: 5,
            pg_dump_path: Some(dump.path().to_path_buf()),
            pg_restore_bin: restore.to_path_buf(),
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

        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);
        let opts = TruncateOptions {
            keep_slots: 5,
            pg_dump_path: Some(dump.path().to_path_buf()),
            pg_restore_bin: restore.to_path_buf(),
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

    /// The account_history delete must match the proven count exactly, or nothing goes.
    #[tokio::test(flavor = "multi_thread")]
    async fn account_history_delete_rolls_back_on_count_mismatch() {
        let (db, _pg) = start_test_postgres_raw().await;
        let pool = db.pool.as_ref();
        sqlx::query("CREATE TABLE account_history (slot BIGINT NOT NULL, data BYTEA NOT NULL)")
            .execute(pool)
            .await
            .unwrap();
        for slot in 0..5_i64 {
            sqlx::query("INSERT INTO account_history (slot, data) VALUES ($1, '\\x00')")
                .bind(slot)
                .execute(pool)
                .await
                .unwrap();
        }

        let err = truncate_account_history_rows(pool, 3, 2).await.unwrap_err();
        assert!(err.to_string().contains("rolled back"), "{err}");
        let left = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM account_history")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(left, 5);
        assert_eq!(truncate_account_history_rows(pool, 3, 3).await.unwrap(), 3);
    }

    /// A lock session that dies during the proof frees the lock for a second truncation,
    /// so the run must stop before its first delete.
    #[tokio::test(flavor = "multi_thread")]
    async fn lock_lost_before_delete_aborts() {
        let (db, _pg) = start_test_postgres_raw().await;
        store_test_blocks(&db, &(0..20).collect::<Vec<_>>()).await;
        let dump = container_pg_dump(&_pg, "pg_test");
        // Kills the lock session while the proof runs, then restores as usual.
        let restore = executable_script(&format!(
            "#!/bin/sh\n\
             docker exec {id} psql -U postgres -d pg_test -qAtc \
             \"SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory'\" \
             >/dev/null\n\
             exec docker exec -i {id} pg_restore \"$@\"\n",
            id = _pg.id()
        ));

        let result = truncate_slots(&db, &apply_opts(5, 100, dump.path(), &restore)).await;

        assert!(result.is_err(), "a lost lock must stop the run");
        assert_eq!(
            min_block_slot(&db.pool).await,
            Some(0),
            "nothing may be deleted"
        );
    }

    /// One block per slot, each with one indexed transfer to `address`; signatures in slot order.
    async fn write_address_history(
        db: &PostgresAccountsDB,
        address: &solana_sdk::pubkey::Pubkey,
        slots: std::ops::Range<u64>,
    ) -> Vec<solana_sdk::signature::Signature> {
        use crate::accounts::AccountsDB;
        use crate::test_helpers::{
            create_test_block_info, create_test_sanitized_transaction,
            flush_address_signatures_sync, no_accounts_deltas,
        };
        use solana_svm::{
            account_loader::LoadedTransaction,
            transaction_execution_result::{ExecutedTransaction, TransactionExecutionDetails},
            transaction_processing_result::ProcessedTransaction,
        };
        let mut accounts_db = AccountsDB::Postgres(db.clone());
        let mut signatures = Vec::new();
        for slot in slots {
            let tx = create_test_sanitized_transaction(
                &solana_sdk::signature::Keypair::new(),
                address,
                1,
            );
            let sig = *tx.signature();
            let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
                loaded_transaction: LoadedTransaction::default(),
                execution_details: TransactionExecutionDetails {
                    status: Ok(()),
                    log_messages: None,
                    inner_instructions: None,
                    return_data: None,
                    executed_units: 0,
                    accounts_deltas: Some(no_accounts_deltas()),
                },
                programs_modified_by_tx: HashMap::new(),
            }));
            let block = BlockInfo {
                transaction_signatures: vec![sig],
                ..create_test_block_info(slot, solana_sdk::hash::Hash::new_unique())
            };
            let rows = accounts_db
                .write_batch(
                    &[],
                    vec![(sig, &tx, slot, 1_700_000_000, &processed)],
                    Some(block),
                )
                .await
                .unwrap();
            flush_address_signatures_sync(&accounts_db, &rows).await;
            signatures.push(sig);
        }
        signatures
    }

    /// History reads after keeping slots 7 to 9 of 0..10: pruned rows end history, never error.
    async fn assert_history_ends_at_the_floor(
        db: &PostgresAccountsDB,
        address: &solana_sdk::pubkey::Pubkey,
        sigs: &[solana_sdk::signature::Signature],
    ) {
        use crate::accounts::AccountsDB;
        let db = AccountsDB::Postgres(db.clone());
        let read = |limit, before, until, ranges: Option<Vec<(i64, i64)>>| {
            let db = db.clone();
            async move {
                db.get_signatures_for_address(address, limit, before, until, ranges.as_deref())
                    .await
                    .expect("history read must not fail on pruned rows")
                    .into_iter()
                    .map(|r| r.signature)
                    .collect::<Vec<_>>()
            }
        };
        let names = |idx: &[usize]| idx.iter().map(|&i| sigs[i].to_string()).collect::<Vec<_>>();
        assert_eq!(
            read(10, None, None, None).await,
            names(&[9, 8, 7]),
            "no cursor"
        );
        assert_eq!(
            read(2, None, None, None).await,
            names(&[9, 8]),
            "first page"
        );
        assert_eq!(
            read(2, Some(&sigs[8]), None, None).await,
            names(&[7]),
            "next page"
        );
        assert!(
            read(10, Some(&sigs[3]), None, None).await.is_empty(),
            "before a pruned row"
        );
        assert_eq!(
            read(10, None, Some(&sigs[3]), None).await,
            names(&[9, 8, 7]),
            "until a pruned row"
        );
        assert_eq!(
            read(10, None, None, Some(vec![(5, 8)])).await,
            names(&[8, 7]),
            "scope spanning the floor"
        );
    }

    /// Regression: truncation leaves index rows behind, and a read reaching them failed.
    #[tokio::test(flavor = "multi_thread")]
    async fn history_survives_truncation() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let address = solana_sdk::pubkey::Pubkey::new_unique();
        let sigs = write_address_history(&db, &address, 0..10).await;
        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);
        truncate_on_fresh_pool(&url, &apply_opts(3, 4, dump.path(), &restore), &db.pool)
            .await
            .unwrap();
        assert_eq!(read_floor_metadata(&db.pool).await, Some(7));

        assert_history_ends_at_the_floor(&db, &address, &sigs).await;
    }

    /// Without the metadata key the floor falls back to the oldest retained block.
    #[tokio::test(flavor = "multi_thread")]
    async fn history_floor_without_metadata_is_min_slot() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let address = solana_sdk::pubkey::Pubkey::new_unique();
        let sigs = write_address_history(&db, &address, 0..10).await;
        let dump = container_pg_dump(&_pg, "pg_test");
        let restore = container_pg_restore_bin(&_pg);
        truncate_on_fresh_pool(&url, &apply_opts(3, 4, dump.path(), &restore), &db.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM metadata WHERE key = 'first_available_block'")
            .execute(db.pool.as_ref())
            .await
            .unwrap();

        assert_history_ends_at_the_floor(&db, &address, &sigs).await;
    }
}
