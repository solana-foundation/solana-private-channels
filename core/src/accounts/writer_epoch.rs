//! Fencing token for the write pipeline. Each write node claims a higher epoch at
//! startup, and every commit checks it still holds the current one, so a node
//! that was replaced stops at the database even before it notices.

use {
    super::{counter, postgres::PostgresAccountsDB},
    anyhow::{anyhow, Context, Result},
    rand::Rng,
    sqlx::PgConnection,
    std::time::Duration,
};

/// Metadata key holding the current writer epoch.
pub const WRITER_EPOCH_KEY: &str = "writer_epoch";

/// Cap on a bump. It waits for at most one in-flight batch, which is bounded by
/// the settler's attempt timeout, so this only fires on a stuck database.
pub const BUMP_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest random step a bump takes. A restore rolls the row back, and a random
/// step makes a later bump reissue an old writer's epoch only by a 1 in 2^40 chance.
/// Epochs are only compared for equality, so the jump is invisible to readers.
pub const MAX_EPOCH_STEP: u64 = 1 << 40;

/// `current + step`, or `None` on overflow, which fails the bump closed.
fn next_epoch(current: u64, step: u64) -> Option<u64> {
    current.checked_add(step)
}

/// Claim the next writer epoch and return it. Waits for any batch holding the
/// current epoch, so every later batch of the old writer sees the new value.
pub async fn bump(db: &PostgresAccountsDB) -> Result<u64> {
    tokio::time::timeout(BUMP_TIMEOUT, bump_unbounded(db))
        .await
        .map_err(|_| anyhow!("claiming the writer epoch exceeded {BUMP_TIMEOUT:?}"))?
}

async fn bump_unbounded(db: &PostgresAccountsDB) -> Result<u64> {
    let mut tx = db
        .pool
        .begin()
        .await
        .context("Failed to begin the epoch bump")?;

    // Row locks only apply to rows that exist, so seed it before locking it.
    sqlx::query("INSERT INTO metadata (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING")
        .bind(WRITER_EPOCH_KEY)
        .bind(&counter::encode(0)[..])
        .execute(&mut *tx)
        .await
        .context("Failed to seed the writer epoch")?;

    // A value that does not decode fails the bump: restarting from 0 could hand
    // an old writer's epoch to the new one.
    let current = read_locked(&mut tx)
        .await
        .context("Failed to lock the writer epoch")?
        .ok_or_else(|| anyhow!("the stored writer epoch is not an 8-byte counter"))?;
    let step = rand::thread_rng().gen_range(1..=MAX_EPOCH_STEP);
    let next = next_epoch(current, step)
        .ok_or_else(|| anyhow!("the writer epoch {current} cannot advance without overflow"))?;

    sqlx::query("UPDATE metadata SET value = $2 WHERE key = $1")
        .bind(WRITER_EPOCH_KEY)
        .bind(&counter::encode(next)[..])
        .execute(&mut *tx)
        .await
        .context("Failed to store the writer epoch")?;
    tx.commit()
        .await
        .context("Failed to commit the writer epoch")?;
    Ok(next)
}

/// Lock the epoch row until the caller's transaction ends and return its value.
/// `None` means the row is missing or unreadable, which no writer may commit on.
pub async fn read_locked(conn: &mut PgConnection) -> Result<Option<u64>, sqlx::Error> {
    let raw: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT value FROM metadata WHERE key = $1 FOR UPDATE")
            .bind(WRITER_EPOCH_KEY)
            .fetch_optional(conn)
            .await?;
    Ok(raw.as_deref().and_then(counter::decode))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::start_test_postgres_with_url;
    use sqlx::Connection;

    #[test]
    fn next_epoch_matrix() {
        assert_eq!(next_epoch(0, 1), Some(1));
        assert_eq!(next_epoch(5, MAX_EPOCH_STEP), Some(5 + MAX_EPOCH_STEP));
        assert_eq!(next_epoch(u64::MAX - 1, 2), None);
    }

    /// A database with no epoch row is at epoch 0; each bump moves up by a bounded,
    /// nonzero step and stores what it returns.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bump_moves_by_a_bounded_nonzero_step() {
        let (db, _pg, _url) = start_test_postgres_with_url().await;

        let first = bump(&db).await.unwrap();
        let second = bump(&db).await.unwrap();
        assert!(0 < first && first <= MAX_EPOCH_STEP, "{first}");
        assert!(
            first < second && second <= first + MAX_EPOCH_STEP,
            "{second}"
        );

        let raw: Vec<u8> = sqlx::query_scalar("SELECT value FROM metadata WHERE key = $1")
            .bind(WRITER_EPOCH_KEY)
            .fetch_one(db.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(crate::accounts::counter::decode(&raw), Some(second));
    }

    /// A batch holds the epoch row until it commits, so a new writer's bump must
    /// wait for it. That wait is what stops an old batch landing after the bump.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bump_waits_for_a_batch_holding_the_epoch() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let held = bump(&db).await.unwrap();

        let mut batch = sqlx::PgConnection::connect(&url).await.unwrap();
        sqlx::query("BEGIN").execute(&mut batch).await.unwrap();
        assert_eq!(read_locked(&mut batch).await.unwrap(), Some(held));

        let bumper = db.clone();
        let bumping = tokio::spawn(async move { bump(&bumper).await });

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(db.pool.as_ref())
            .await
            .unwrap();
            if waiting > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the bump never waited on the held epoch row"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(!bumping.is_finished(), "the bump must wait for the batch");

        sqlx::query("COMMIT").execute(&mut batch).await.unwrap();
        assert!(bumping.await.unwrap().unwrap() > held);
    }
}
