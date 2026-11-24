//! The permanent delegate the last writer booted with. A changed delegate means mints may
//! still name the old key, so the writer refuses to boot unless that key stays policed.

use {
    super::writer_epoch,
    anyhow::{anyhow, Context, Result},
    solana_sdk::pubkey::Pubkey,
    sqlx::{Executor, PgPool, Postgres},
};

/// Metadata key holding the delegate the last writer booted with.
pub const SEIZE_DELEGATE_KEY: &str = "seize_delegate";

/// Checks the stored delegate against the configured keys, then stores the current one.
/// Fenced by the writer epoch like the window publish. Returns the key it replaced.
pub async fn publish_as_writer(
    pool: &PgPool,
    delegate: &Pubkey,
    previous_delegates: &[Pubkey],
    epoch: u64,
) -> Result<Option<Pubkey>> {
    let mut tx = pool
        .begin()
        .await
        .context("Failed to begin the delegate publish")?;
    let current = writer_epoch::read_locked(&mut tx)
        .await
        .context("Failed to lock the writer epoch")?;
    if current != Some(epoch) {
        return Err(anyhow!(
            "writer epoch {epoch} was superseded by {current:?}; not publishing its delegate"
        ));
    }
    let stored = read(&mut *tx)
        .await
        .context("Failed to read the stored delegate")?;
    // Mints keep naming the old delegate until each is rotated, which only works after
    // this restart. Booting without the old key listed would leave it unpoliced.
    if let Some(old) = stored.filter(|old| old != delegate && !previous_delegates.contains(old)) {
        return Err(anyhow!(
            "the permanent delegate changed from {old} to {delegate}; mints may still name \
             {old}, so list it in PRIVATE_CHANNEL_PREVIOUS_DELEGATES (--previous-delegates) \
             until every mint is rotated"
        ));
    }
    sqlx::query(
        "INSERT INTO metadata (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(SEIZE_DELEGATE_KEY)
    .bind(delegate.as_ref())
    .execute(&mut *tx)
    .await
    .context("Failed to store the delegate")?;
    tx.commit()
        .await
        .context("Failed to commit the delegate publish")?;
    Ok(stored)
}

/// The stored delegate, or `None` when it is absent or unreadable.
pub async fn read<'e, E>(executor: E) -> sqlx::Result<Option<Pubkey>>
where
    E: Executor<'e, Database = Postgres>,
{
    let value =
        sqlx::query_scalar::<_, Option<Vec<u8>>>("SELECT value FROM metadata WHERE key = $1")
            .bind(SEIZE_DELEGATE_KEY)
            .fetch_optional(executor)
            .await?;
    Ok(value
        .flatten()
        .and_then(|bytes| Pubkey::try_from(bytes.as_slice()).ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::start_test_postgres_raw;

    /// A changed delegate boots only with the old key listed; once stored, the list may go.
    #[tokio::test(flavor = "multi_thread")]
    async fn delegate_change_without_the_old_key_listed_refuses_boot() {
        let (db, _pg) = start_test_postgres_raw().await;
        let epoch = writer_epoch::bump(&db).await.unwrap();
        let pool = db.pool.as_ref();
        let (a, b) = (Pubkey::new_unique(), Pubkey::new_unique());

        assert_eq!(publish_as_writer(pool, &a, &[], epoch).await.unwrap(), None);
        assert_eq!(read(pool).await.unwrap(), Some(a));

        let err = publish_as_writer(pool, &b, &[], epoch).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains(&a.to_string()), "{msg}");
        assert!(msg.contains("PRIVATE_CHANNEL_PREVIOUS_DELEGATES"), "{msg}");
        assert_eq!(
            read(pool).await.unwrap(),
            Some(a),
            "a refused boot writes nothing"
        );

        assert_eq!(
            publish_as_writer(pool, &b, &[a], epoch).await.unwrap(),
            Some(a)
        );
        assert_eq!(read(pool).await.unwrap(), Some(b));
        assert_eq!(
            publish_as_writer(pool, &b, &[], epoch).await.unwrap(),
            Some(b)
        );

        writer_epoch::bump(&db).await.unwrap();
        let err = publish_as_writer(pool, &a, &[b], epoch).await.unwrap_err();
        assert!(format!("{err:#}").contains("epoch"), "{err:#}");
        assert_eq!(
            read(pool).await.unwrap(),
            Some(b),
            "a superseded writer writes nothing"
        );
    }
}
