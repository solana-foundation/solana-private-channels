//! Target file: `core/src/accounts/truncate.rs`
//! Binary: `truncate_integration` (existing).
//! Fixture: one testcontainers Postgres, plus a second small pool used only to
//! hold a row lock.
//!
//! The advertised ledger floor (`metadata['first_available_block']`, served as
//! `getFirstAvailableBlock`) is the retention proof an absence-based finality
//! verdict relies on: a floor at or below the attempt's slot range is read as
//! "this node still holds that range and the signature is not in it". If the
//! floor can ever name slots whose rows are already deleted, that proof is
//! false and a landed-but-pruned transaction reads as never landed.
//!
//! The sibling unit test proves the floor is written once per deletion batch.
//! This test proves the stronger property that the write is in the SAME
//! transaction as the deletions, which is what closes the mid-run window. It
//! does that without any production test hook: a separate connection holds the
//! `FOR UPDATE` row lock the batch upsert needs, so the batch parks with its
//! deletions issued but uncommitted, and a third connection then checks that
//! the deletions are invisible for exactly as long as the floor is unchanged.
//!
//! Helpers are reused from the parent module rather than duplicated, so a
//! change to the shared fixture shape is picked up here too.

use {
    anyhow::{Context, Result},
    private_channel_core::accounts::{
        truncate::{truncate_slots, TruncateOptions, TruncateReport},
        PostgresAccountsDB,
    },
    solana_sdk::{hash::Hash, signature::Signature},
    sqlx::{postgres::PgPoolOptions, PgPool},
    std::time::Duration,
};

/// Slots seeded by this test. The first run prunes below `FIRST_FLOOR`, the
/// second run is the one whose batches are observed mid-flight.
const SEEDED_SLOTS: u64 = 60;
const FIRST_KEEP_SLOTS: u64 = 41;
const FIRST_FLOOR: u64 = 20;
const SECOND_KEEP_SLOTS: u64 = 5;
const SECOND_FLOOR: u64 = 56;

fn opts(keep_slots: u64, batch_size: usize, backup: &std::path::Path) -> TruncateOptions {
    TruncateOptions {
        keep_slots,
        max_backup_age: Duration::from_secs(60 * 60),
        pg_dump_path: Some(backup.to_path_buf()),
        batch_size,
        dry_run: false,
    }
}

/// Run one truncation against a pool of its own, close it, and wait until the
/// server has actually dropped the truncation advisory lock.
///
/// `truncate_slots` takes that lock on whichever pooled connection serves the
/// acquire statement and releases it on whichever connection serves the release
/// statement. Those are not guaranteed to be the same connection, so a second
/// run against a still-open pool can find the lock held by an idle one. Closing
/// the pool ends those sessions, but the backends exit asynchronously, so the
/// wait below is what makes a two-run test deterministic. This is test
/// scaffolding only; the lock's connection affinity is not part of this change.
async fn truncate_on_fresh_pool(
    url: &str,
    options: &TruncateOptions,
    observer: &PgPool,
) -> Result<TruncateReport> {
    let db = PostgresAccountsDB::new(url, false)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open truncation pool: {e}"))?;
    let report = truncate_slots(&db, options).await;
    db.pool.close().await;
    await_advisory_locks_released(observer).await?;
    report
}

/// Block until no advisory lock is held anywhere on the test database. This test
/// owns its container, so the truncation lock is the only one possible.
async fn await_advisory_locks_released(pool: &PgPool) -> Result<()> {
    for _ in 0..600 {
        let held = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'",
        )
        .fetch_one(pool)
        .await
        .context("Failed to poll advisory locks")?;
        if held == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("truncation advisory lock still held after its pool was closed")
}

async fn seed_blocks(pool: &PgPool, count: u64) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS account_history (
            id BIGSERIAL PRIMARY KEY,
            slot BIGINT NOT NULL,
            data BYTEA NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("Failed to create account_history fixture table")?;

    let mut previous_blockhash = Hash::default();
    for slot in 1..=count {
        let signature = Signature::new_unique();
        let block = super::build_block(slot, previous_blockhash, signature);
        previous_blockhash = block.blockhash;
        let data = bincode::serialize(&block).context("Failed to serialize fixture block")?;

        sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(data)
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
    Ok(())
}

/// Read the floor and the true retained minimum inside one snapshot, so the two
/// values can never come from different points in time.
async fn snapshot_floor_and_min(pool: &PgPool) -> Result<(Option<u64>, Option<u64>)> {
    let mut tx = pool
        .begin()
        .await
        .context("Failed to open observer transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .context("Failed to set observer isolation level")?;

    let floor = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT value FROM metadata WHERE key = 'first_available_block'",
    )
    .fetch_optional(&mut *tx)
    .await
    .context("Failed to read first_available_block")?
    .flatten()
    .map(|value| {
        let bytes: [u8; 8] = value.as_slice().try_into().expect("floor is 8 bytes");
        u64::from_le_bytes(bytes)
    });

    let min_slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
        .fetch_one(&mut *tx)
        .await
        .context("Failed to read MIN(slot)")?
        .map(|slot| slot as u64);

    tx.rollback().await.ok();
    Ok((floor, min_slot))
}

/// Wait until a backend is blocked specifically by `blocker_pid`, the connection
/// holding the floor row lock.
///
/// Matching any ungranted lock in the cluster would let an unrelated waiter
/// satisfy the poll before the truncation batch had issued its deletions, and
/// the assertions that follow would then pass for the wrong reason. Naming the
/// blocker means only the metadata row lock can end this wait.
async fn await_blocked_by(pool: &PgPool, blocker_pid: i32) -> Result<()> {
    for _ in 0..600 {
        let waiting = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
        )
        .bind(blocker_pid)
        .fetch_one(pool)
        .await
        .context("Failed to poll for blocked backends")?;
        if waiting > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("timed out waiting for the truncation batch to block on the metadata row lock")
}

/// The deletions and the new floor must become visible together. While a batch
/// is parked on the floor row lock, an outside reader must still see both the
/// old floor and the old set of blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_floor_and_deletions_commit_together() -> Result<()> {
    let (db, container) = super::start_postgres("truncate_floor_atomicity").await?;
    seed_blocks(&db.pool, SEEDED_SLOTS).await?;
    let backup_path = super::create_backup_artifact("floor_atomicity")?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!(
        "postgres://postgres:password@{}:{}/truncate_floor_atomicity",
        host, port
    );

    // First run creates the metadata key. Without it the read path falls back to
    // MIN(slot), which is always truthful, and the window cannot be observed.
    let first =
        truncate_on_fresh_pool(&url, &opts(FIRST_KEEP_SLOTS, 100, &backup_path), &db.pool).await?;
    assert_eq!(first.first_available_block, Some(FIRST_FLOOR));
    assert_eq!(
        snapshot_floor_and_min(&db.pool).await?,
        (Some(FIRST_FLOOR), Some(FIRST_FLOOR))
    );

    // A dedicated pool so holding the lock cannot consume the truncation's
    // connections, and so the lock is released exactly when this test says.
    let holder_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .context("Failed to open the lock-holder pool")?;

    let mut holder = holder_pool
        .begin()
        .await
        .context("Failed to begin the lock-holder transaction")?;
    let locked = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM metadata WHERE key = 'first_available_block' FOR UPDATE",
    )
    .fetch_optional(&mut *holder)
    .await
    .context("Failed to take the floor row lock")?;
    assert_eq!(
        locked,
        Some(1),
        "the floor row must exist before locking it"
    );
    let holder_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .context("Failed to read the lock holder's backend pid")?;

    let second_url = url.clone();
    let second_opts = opts(SECOND_KEEP_SLOTS, 3, &backup_path);
    let observer = db.pool.clone();
    let task =
        tokio::spawn(
            async move { truncate_on_fresh_pool(&second_url, &second_opts, &observer).await },
        );

    await_blocked_by(&db.pool, holder_pid).await?;

    // The parked batch has issued its deletions but not committed them, so an
    // outside reader must see the pre-run floor and the pre-run blocks.
    let (floor, min_slot) = snapshot_floor_and_min(&db.pool).await?;
    assert_eq!(
        floor,
        Some(FIRST_FLOOR),
        "floor moved while a batch was still uncommitted"
    );
    assert_eq!(
        min_slot,
        Some(FIRST_FLOOR),
        "deletions became visible while the floor still advertised slot {FIRST_FLOOR}"
    );

    holder
        .commit()
        .await
        .context("Failed to release the floor row lock")?;

    let report = tokio::time::timeout(Duration::from_secs(120), task)
        .await
        .context("truncation task timed out")?
        .context("truncation task panicked")??;

    assert_eq!(report.first_available_block, Some(SECOND_FLOOR));
    assert_eq!(
        snapshot_floor_and_min(&db.pool).await?,
        (Some(SECOND_FLOOR), Some(SECOND_FLOOR)),
        "after the run the floor must equal the retained minimum"
    );

    holder_pool.close().await;
    super::cleanup_backup_artifact(&backup_path);
    Ok(())
}
