//! Target file: `core/src/accounts/truncate.rs`
//! Binary: `truncate_integration` (existing).
//! Fixture: one testcontainers Postgres; shared by both contending tasks.
//!
//! Exercises the advisory-lock contention branch. The production code uses
//! `pg_try_advisory_lock`: one caller acquires, the other returns
//! `false` and surfaces the "another truncation process is already running"
//! error. This test proves:
//!   * exactly ONE task succeeds (success_count == 1)
//!   * the loser's error is the lock-contention variant, not some other
//!     error (e.g. backup-verification)
//!   * the winner's DB side effects are durable (count of deleted blocks
//!     matches the expected keep_slots contract)
//!
//! NOTE on error shape: `truncate_slots` returns `anyhow::Result<_>`, not
//! a typed `TruncateError`. The contention branch's message is matched
//! by substring — the exact wording lives in `truncate_slots` (in
//! `core/src/accounts/truncate.rs`) and changes to it should be reflected
//! here.

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

fn make_backup_dir(tag: &str) -> Result<PathBuf> {
    let u = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "private_channel_t19_{}_{}_{}",
        tag,
        std::process::id(),
        u
    ));
    fs::create_dir_all(&dir)?;
    let file = dir.join("backup.dump");
    fs::write(&file, b"fixture-backup")?;
    Ok(file)
}

fn cleanup(p: &Path) {
    if let Some(parent) = p.parent() {
        let _ = fs::remove_dir_all(parent);
    }
}

async fn seed_blocks(pool: &PgPool, n: u64) -> Result<()> {
    // Mirrors the shape used by the existing `seed_fixture` in truncate.rs;
    // keeps us compatible with whatever schema PostgresAccountsDB::new
    // already created.
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
    for slot in 1..=n {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_concurrent_lock_contention() -> Result<()> {
    let (db, _container) = start_postgres("truncate_lock_contention").await?;
    seed_blocks(&db.pool, 10).await?;

    let b1 = make_backup_dir("t1")?;
    let b2 = make_backup_dir("t2")?;

    let opts_for = |bp: PathBuf| TruncateOptions {
        keep_slots: 3,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: Some(bp),
        batch_size: 2,
        dry_run: false,
    };

    // PostgresAccountsDB derives Clone; both tasks get their own handle
    // onto the same pool (Arc internally), so they contend on the real
    // pg_try_advisory_lock rather than on a Rust-level mutex.
    let db1 = db.clone();
    let db2 = db.clone();
    let opts1 = opts_for(b1.clone());
    let opts2 = opts_for(b2.clone());

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { truncate_slots(&db1, &opts1).await }),
        tokio::spawn(async move { truncate_slots(&db2, &opts2).await }),
    );
    let r1 = r1.expect("t1 panicked");
    let r2 = r2.expect("t2 panicked");

    // Contract: exactly one winner, exactly one LockHeld loser.
    let wins = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(wins, 1, "expected exactly one winner; r1={r1:?} r2={r2:?}");

    let loser = if r1.is_err() { &r1 } else { &r2 };
    let loser_msg = loser.as_ref().unwrap_err().to_string().to_lowercase();
    assert!(
        loser_msg.contains("already running") && loser_msg.contains("advisory lock"),
        "loser must report lock contention, got: {loser_msg}"
    );

    // Winner produced the expected deletions (10 slots, keep 3 → delete 7 per table).
    let winner = if r1.is_ok() { &r1 } else { &r2 };
    let report = winner.as_ref().unwrap();
    assert_eq!(report.blocks_deleted, 7);
    assert_eq!(report.transactions_deleted, 7);
    assert_eq!(report.account_history_rows_deleted, 7);
    assert_eq!(report.latest_slot, Some(10));

    cleanup(&b1);
    cleanup(&b2);
    Ok(())
}
