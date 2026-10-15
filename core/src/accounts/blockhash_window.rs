//! The write node's blockhash window, kept in the ledger. Readers derive the deadline
//! they advertise from it and truncation sizes what it keeps from it, so neither can
//! disagree with the window the writer actually admits on.

use {
    super::{counter, writer_epoch},
    anyhow::{anyhow, Context, Result},
    sqlx::{Executor, PgPool, Postgres},
};

/// Metadata key holding the writer's `max_blockhashes`, a block count.
pub const MAX_BLOCKHASHES_KEY: &str = "max_blockhashes";

/// The write node's publish: fenced by its epoch, so a superseded writer cannot overwrite
/// the window. A larger window keeps every live hash admissible past the deadline readers
/// already handed out, so it is refused unless the operator allows it explicitly.
pub async fn publish_as_writer(
    pool: &PgPool,
    max_blockhashes: u64,
    epoch: u64,
    allow_increase: bool,
) -> Result<Option<u64>> {
    let mut tx = pool
        .begin()
        .await
        .context("Failed to begin the window publish")?;
    let current = writer_epoch::read_locked(&mut tx)
        .await
        .context("Failed to lock the writer epoch")?;
    if current != Some(epoch) {
        return Err(anyhow!(
            "writer epoch {epoch} was superseded by {current:?}; not publishing its window"
        ));
    }
    let previous = read(&mut *tx)
        .await
        .context("Failed to read the published window")?;
    if let Some(previous) = previous.filter(|&p| p < max_blockhashes && !allow_increase) {
        return Err(anyhow!(
            "max_blockhashes would grow from {previous} to {max_blockhashes}, which extends the \
             deadline of every hash already handed out; pass --allow-max-blockhashes-increase \
             to do it anyway"
        ));
    }
    publish(&mut *tx, max_blockhashes)
        .await
        .context("Failed to publish the window")?;
    tx.commit()
        .await
        .context("Failed to commit the window publish")?;
    Ok(previous)
}

/// Store the window and return the value it replaced, if one was readable.
pub async fn publish<'e, E>(executor: E, max_blockhashes: u64) -> sqlx::Result<Option<u64>>
where
    E: Executor<'e, Database = Postgres>,
{
    let previous: Option<Vec<u8>> = sqlx::query_scalar(
        "WITH previous AS (SELECT value FROM metadata WHERE key = $1),
              written AS (
                  INSERT INTO metadata (key, value) VALUES ($1, $2)
                  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
              )
         SELECT (SELECT value FROM previous)",
    )
    .bind(MAX_BLOCKHASHES_KEY)
    .bind(&counter::encode(max_blockhashes)[..])
    .fetch_one(executor)
    .await?;
    Ok(decode(previous.as_deref()))
}

/// The published window, or `None` when it is absent, unreadable or zero.
pub async fn read<'e, E>(executor: E) -> sqlx::Result<Option<u64>>
where
    E: Executor<'e, Database = Postgres>,
{
    let result =
        sqlx::query_scalar::<_, Option<Vec<u8>>>("SELECT value FROM metadata WHERE key = $1")
            .bind(MAX_BLOCKHASHES_KEY)
            .fetch_optional(executor)
            .await;
    match result {
        Ok(value) => Ok(decode(value.flatten().as_deref())),
        // 42P01 = undefined_table; schema not yet created.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => Ok(None),
        Err(e) => Err(e),
    }
}

/// A writer refuses a zero window, so a stored zero is a hand edit and counts as absent.
pub(crate) fn decode(value: Option<&[u8]>) -> Option<u64> {
    value.and_then(counter::decode).filter(|&w| w > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::start_test_postgres_raw;

    /// The writer's publish is fenced by its epoch and refuses a larger window unless allowed.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_publish_refuses_an_increase_and_a_stale_epoch() {
        let (db, _pg) = start_test_postgres_raw().await;
        let epoch = crate::accounts::writer_epoch::bump(&db).await.unwrap();
        let pool = db.pool.as_ref();

        assert_eq!(
            publish_as_writer(pool, 150, epoch, false).await.unwrap(),
            None
        );
        assert_eq!(
            publish_as_writer(pool, 100, epoch, false).await.unwrap(),
            Some(150)
        );
        let err = publish_as_writer(pool, 120, epoch, false)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("100 to 120"), "{err:#}");
        assert_eq!(
            read(pool).await.unwrap(),
            Some(100),
            "a refused increase writes nothing"
        );
        assert_eq!(
            publish_as_writer(pool, 120, epoch, true).await.unwrap(),
            Some(100)
        );

        let newer = crate::accounts::writer_epoch::bump(&db).await.unwrap();
        let err = publish_as_writer(pool, 50, epoch, false).await.unwrap_err();
        assert!(format!("{err:#}").contains("epoch"), "{err:#}");
        assert_eq!(
            read(pool).await.unwrap(),
            Some(120),
            "a superseded writer writes nothing"
        );
        assert_eq!(
            publish_as_writer(pool, 50, newer, false).await.unwrap(),
            Some(120)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_then_read() {
        let (db, _pg) = start_test_postgres_raw().await;
        let pool = db.pool.as_ref();

        assert_eq!(read(pool).await.unwrap(), None, "missing key");
        assert_eq!(publish(pool, 150).await.unwrap(), None, "first publish");
        assert_eq!(read(pool).await.unwrap(), Some(150));
        assert_eq!(
            publish(pool, 7).await.unwrap(),
            Some(150),
            "returns the old value"
        );
        assert_eq!(read(pool).await.unwrap(), Some(7));

        for (label, bytes) in [
            ("wrong length", vec![1u8, 2, 3]),
            ("stored zero", counter::encode(0).to_vec()),
        ] {
            sqlx::query("UPDATE metadata SET value = $2 WHERE key = $1")
                .bind(MAX_BLOCKHASHES_KEY)
                .bind(&bytes)
                .execute(pool)
                .await
                .unwrap();
            assert_eq!(read(pool).await.unwrap(), None, "{label}");
        }
    }
}
