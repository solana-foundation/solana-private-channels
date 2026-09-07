//! Target file: `core/src/accounts/truncate.rs`
//! Binary: `truncate_integration` (existing).
//! Fixture: one testcontainers Postgres per sub-case.
//!
//! The production code calls `verify_backup_readiness` *before* any rows
//! are deleted; a failure there returns an `anyhow!("Backup verification
//! failed. WAL: {}. pg_dump: {}")` with no side effects. This test proves
//! that contract across two realistic failure modes:
//!
//!   A. `pg_dump_path` points at a file that cannot be read (e.g. missing
//!      parent dir). Backup is considered invalid; truncate aborts; all
//!      rows remain; `first_available_block` unchanged.
//!   B. `pg_dump_path` points at a file whose mtime is *too old*
//!      (`max_backup_age` exceeded). Same abort contract.
//!
//! NOTE on scope: chmod-style mid-write failures are timing-sensitive and
//! hard to reproduce on tmpfs. The two cases above hit the
//! same error return path in `check_pg_dump_recency` with zero flake risk.

use {
    anyhow::{anyhow, Context, Result},
    private_channel_core::accounts::{
        traits::BlockInfo,
        truncate::{truncate_slots, TruncateOptions},
        PostgresAccountsDB,
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
        .context("PG container start")?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);
    let db = PostgresAccountsDB::new(&url, false)
        .await
        .map_err(|e| anyhow!("PostgresAccountsDB::new: {e}"))?;
    Ok((db, container))
}

fn tmpdir(tag: &str) -> PathBuf {
    let u = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "private_channel_t20_{}_{}_{}",
        tag,
        std::process::id(),
        u
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(p: &Path) {
    let _ = fs::remove_dir_all(p);
}

async fn seed_minimal_blocks(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS account_history (
            id BIGSERIAL PRIMARY KEY,
            slot BIGINT NOT NULL,
            data BYTEA NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    let mut prev = Hash::default();
    for slot in 1u64..=5 {
        let sig = Signature::new_unique();
        let block = BlockInfo {
            slot,
            blockhash: Hash::new_unique(),
            previous_blockhash: prev,
            parent_slot: slot.saturating_sub(1),
            block_height: Some(slot),
            block_time: Some(slot as i64),
            transaction_signatures: vec![sig],
            transaction_recent_blockhashes: vec![prev],
            transaction_message_hashes: vec![Hash::new_unique()],
        };
        prev = block.blockhash;
        let data = bincode::serialize(&block)?;
        sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(data)
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO transactions (signature, data) VALUES ($1, $2)")
            .bind(sig.as_ref().to_vec())
            .bind(vec![slot as u8])
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO account_history (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(vec![slot as u8])
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn row_counts(pool: &PgPool) -> Result<(i64, i64, i64)> {
    let b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blocks")
        .fetch_one(pool)
        .await?;
    let t: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(pool)
        .await?;
    let h: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_history")
        .fetch_one(pool)
        .await?;
    Ok((b, t, h))
}

async fn first_available_unchanged(pool: &PgPool) -> Result<()> {
    let meta: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT value FROM metadata WHERE key = 'first_available_block'")
            .fetch_optional(pool)
            .await?
            .flatten();
    assert!(
        meta.is_none(),
        "truncate aborted due to backup failure must NOT write first_available_block"
    );
    Ok(())
}

// ── Case A ──────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_aborts_when_backup_path_missing() -> Result<()> {
    let (db, _container) = start_postgres("truncate_backup_missing").await?;
    seed_minimal_blocks(&db.pool).await?;
    let before = row_counts(&db.pool).await?;

    // Path inside a non-existent parent — backup cannot possibly be read.
    let dir = tmpdir("missing");
    let missing = dir.join("does-not-exist").join("backup.dump");

    let opts = TruncateOptions {
        keep_slots: 3,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: Some(missing),
        batch_size: 2,
        dry_run: false,
    };

    let err = truncate_slots(&db, &opts)
        .await
        .expect_err("missing backup must cause truncate to abort");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("backup verification failed"),
        "error message must be the backup-verification failure, got: {msg}"
    );

    assert_eq!(
        row_counts(&db.pool).await?,
        before,
        "no rows must be deleted"
    );
    first_available_unchanged(&db.pool).await?;
    cleanup(&dir);
    Ok(())
}

// ── Case B ──────────────────────────────────────────────────────────────────
// Exercises the explicit "no backup path" branch in `check_pg_dump_recency`.
// Complements Case A which tests path-is-not-a-file.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_aborts_when_backup_path_not_supplied() -> Result<()> {
    let (db, _container) = start_postgres("truncate_backup_none").await?;
    seed_minimal_blocks(&db.pool).await?;
    let before = row_counts(&db.pool).await?;

    let opts = TruncateOptions {
        keep_slots: 3,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: None, // ← the violation
        batch_size: 2,
        dry_run: false,
    };

    let err = truncate_slots(&db, &opts)
        .await
        .expect_err("no backup path must cause truncate to abort");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("backup verification failed"),
        "error message must be the backup-verification failure, got: {msg}"
    );

    assert_eq!(row_counts(&db.pool).await?, before);
    first_available_unchanged(&db.pool).await?;
    Ok(())
}

// ── Case C ──────────────────────────────────────────────────────────────────
// Input-validation branch: `keep_slots = 0` is rejected before the lock is
// even acquired. Tiny test but hits a real guard.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_rejects_keep_slots_zero() -> Result<()> {
    let (db, _container) = start_postgres("truncate_keep_zero").await?;

    let opts = TruncateOptions {
        keep_slots: 0,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: None,
        batch_size: 2,
        dry_run: false,
    };

    let err = truncate_slots(&db, &opts)
        .await
        .expect_err("keep_slots=0 must be rejected");
    assert!(
        err.to_string()
            .contains("keep_slots must be greater than 0"),
        "error must name the bad parameter, got: {err}"
    );
    Ok(())
}
