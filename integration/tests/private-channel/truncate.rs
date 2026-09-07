//! Integration tests for the slot-truncation utility (`truncate_slots`).
//!
//! Each test spins up an isolated Postgres container, seeds five blocks
//! (slots 1–5) with matching transactions and account_history rows, then
//! invokes `truncate_slots` with `keep_slots = 3`.  The expected outcome is
//! that slots 1 and 2 are pruned while slots 3–5 are retained.
//!
//! Scenarios covered:
//! 1. Apply mode  – rows are deleted, `first_available_block` metadata is
//!    written, and `AccountsDB::get_first_available_block()` returns 3.
//! 2. Dry-run mode – row counts and reported deletions match apply-mode values
//!    but no rows are actually removed and metadata is not mutated.

// Keep these in their own files for readability; `#[path]` wires them into
// this same test binary so they share compile state.
#[path = "test_truncate_backup_failure.rs"]
mod backup_failure;
#[path = "test_truncate_floor_atomicity.rs"]
mod floor_atomicity;
#[path = "test_truncate_lock_contention.rs"]
mod lock_contention;

use {
    anyhow::{anyhow, Context, Result},
    private_channel_core::accounts::{
        traits::BlockInfo,
        truncate::{truncate_slots, TruncateOptions},
        AccountsDB, PostgresAccountsDB,
    },
    solana_sdk::{hash::Hash, signature::Signature},
    sqlx::PgPool,
    std::{
        fs,
        path::{Path, PathBuf},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
};

async fn start_postgres(
    db_name: &str,
) -> Result<(PostgresAccountsDB, testcontainers::ContainerAsync<Postgres>)> {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .context("Failed to start PostgreSQL test container")?;

    let host = container
        .get_host()
        .await
        .context("Failed to resolve PostgreSQL container host")?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .context("Failed to resolve PostgreSQL container port")?;
    let db_url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);
    let db = PostgresAccountsDB::new(&db_url, false)
        .await
        .map_err(|e| anyhow!("Failed to initialize PostgresAccountsDB: {}", e))?;

    Ok((db, container))
}

fn create_backup_artifact(test_name: &str) -> Result<PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before UNIX_EPOCH")?
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "private_channel_truncate_{}_{}_{}",
        test_name,
        std::process::id(),
        unique
    ));

    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "Failed to create backup fixture directory {}",
            dir.display()
        )
    })?;

    let backup_file = dir.join("backup.dump");
    fs::write(&backup_file, b"fixture-backup")
        .with_context(|| format!("Failed to write backup fixture {}", backup_file.display()))?;

    Ok(backup_file)
}

fn cleanup_backup_artifact(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::remove_dir_all(parent);
    }
}

fn build_block(slot: u64, previous_blockhash: Hash, signature: Signature) -> BlockInfo {
    let blockhash = Hash::new_unique();
    BlockInfo {
        slot,
        blockhash,
        previous_blockhash,
        parent_slot: slot.saturating_sub(1),
        block_height: Some(slot),
        block_time: Some(slot as i64),
        transaction_signatures: vec![signature],
        transaction_recent_blockhashes: vec![blockhash],
        transaction_message_hashes: vec![Hash::new_unique()],
    }
}

async fn seed_fixture(pool: &PgPool) -> Result<Vec<Signature>> {
    sqlx::query(
        "CREATE TABLE account_history (
            id BIGSERIAL PRIMARY KEY,
            slot BIGINT NOT NULL,
            data BYTEA NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("Failed to create account_history fixture table")?;

    let signatures: Vec<Signature> = (0..5).map(|_| Signature::new_unique()).collect();
    let mut previous_blockhash = Hash::default();

    for (idx, signature) in signatures.iter().enumerate() {
        let slot = (idx + 1) as u64;
        let block = build_block(slot, previous_blockhash, *signature);
        previous_blockhash = block.blockhash;
        let block_data = bincode::serialize(&block).context("Failed to serialize fixture block")?;

        sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(block_data)
            .execute(pool)
            .await
            .with_context(|| format!("Failed to insert fixture block at slot {}", slot))?;

        sqlx::query("INSERT INTO transactions (signature, data) VALUES ($1, $2)")
            .bind(signature.as_ref().to_vec())
            .bind(vec![slot as u8])
            .execute(pool)
            .await
            .with_context(|| format!("Failed to insert fixture transaction at slot {}", slot))?;

        sqlx::query("INSERT INTO account_history (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(vec![slot as u8])
            .execute(pool)
            .await
            .with_context(|| format!("Failed to insert account_history row at slot {}", slot))?;
    }

    Ok(signatures)
}

async fn count_rows(pool: &PgPool, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    let count = sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .with_context(|| format!("Failed to count rows in {}", table))?;
    Ok(count)
}

/// Verifies that `truncate_slots` with `dry_run = false` physically deletes
/// blocks/transactions/account_history rows below the keep-slots threshold,
/// updates the `first_available_block` metadata row, and that
/// `AccountsDB::get_first_available_block()` reflects the new minimum slot.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_apply_mode_e2e() -> Result<()> {
    let (db, _container) = start_postgres("truncate_apply").await?;
    let pool = db.pool.clone();
    let signatures = seed_fixture(pool.as_ref()).await?;
    let backup_path = create_backup_artifact("apply")?;

    let options = TruncateOptions {
        keep_slots: 3,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: Some(backup_path.clone()),
        batch_size: 2,
        dry_run: false,
    };

    let report = truncate_slots(&db, &options).await?;

    assert_eq!(report.latest_slot, Some(5));
    assert_eq!(report.truncate_before_slot, Some(3));
    assert_eq!(report.blocks_deleted, 2);
    assert_eq!(report.transactions_deleted, 2);
    assert_eq!(report.account_history_rows_deleted, 2);
    assert_eq!(report.first_available_block, Some(3));
    assert!(report.backup_check.pg_dump_ok);
    assert!(report.backup_check.has_valid_backup());

    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, 3);
    assert_eq!(count_rows(pool.as_ref(), "transactions").await?, 3);
    assert_eq!(count_rows(pool.as_ref(), "account_history").await?, 3);

    let remaining_min_slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
        .fetch_one(pool.as_ref())
        .await?
        .expect("Expected remaining blocks after truncation");
    assert_eq!(remaining_min_slot, 3);

    for signature in signatures.iter().take(2) {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM transactions WHERE signature = $1")
                .bind(signature.as_ref().to_vec())
                .fetch_one(pool.as_ref())
                .await?;
        assert_eq!(exists, 0, "Old transaction should be deleted");
    }

    let metadata = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT value FROM metadata WHERE key = 'first_available_block'",
    )
    .fetch_one(pool.as_ref())
    .await?
    .expect("Expected first_available_block metadata to be set");
    assert_eq!(metadata.len(), 8);
    assert_eq!(
        u64::from_le_bytes(
            metadata
                .as_slice()
                .try_into()
                .expect("metadata len checked")
        ),
        3
    );

    let accounts_db = AccountsDB::Postgres(db.clone());
    assert_eq!(accounts_db.get_first_available_block().await?, 3);

    cleanup_backup_artifact(&backup_path);
    Ok(())
}

/// Verifies that `truncate_slots` with `dry_run = true` reports the correct
/// deletion counts (matching what apply-mode would remove) but leaves all rows
/// intact and does not write the `first_available_block` metadata entry.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_dry_run_e2e() -> Result<()> {
    let (db, _container) = start_postgres("truncate_dry_run").await?;
    let pool = db.pool.clone();
    seed_fixture(pool.as_ref()).await?;
    let backup_path = create_backup_artifact("dry_run")?;

    let options = TruncateOptions {
        keep_slots: 3,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: Some(backup_path.clone()),
        batch_size: 2,
        dry_run: true,
    };

    let report = tokio::time::timeout(Duration::from_secs(10), truncate_slots(&db, &options))
        .await
        .expect("dry-run truncation timed out")
        .context("dry-run truncation returned an error")?;

    assert_eq!(report.blocks_deleted, 2);
    assert_eq!(report.transactions_deleted, 2);
    assert_eq!(report.account_history_rows_deleted, 2);
    assert!(report.backup_check.pg_dump_ok);

    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, 5);
    assert_eq!(count_rows(pool.as_ref(), "transactions").await?, 5);
    assert_eq!(count_rows(pool.as_ref(), "account_history").await?, 5);

    let metadata = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT value FROM metadata WHERE key = 'first_available_block'",
    )
    .fetch_optional(pool.as_ref())
    .await?
    .flatten();
    assert!(metadata.is_none(), "dry-run must not mutate metadata");

    cleanup_backup_artifact(&backup_path);
    Ok(())
}
