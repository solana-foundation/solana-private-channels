use sqlx::{PgPool, Postgres, Transaction};

pub const ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY: &str = "address_signatures_flushed_slot";

pub async fn get_address_signatures_flushed_slot(pool: &PgPool) -> sqlx::Result<Option<i64>> {
    let bytes: Option<Vec<u8>> = sqlx::query_scalar("SELECT value FROM metadata WHERE key = $1")
        .bind(ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY)
        .fetch_optional(pool)
        .await?;

    Ok(bytes.and_then(|b| {
        let arr: [u8; 8] = b.as_slice().try_into().ok()?;
        Some(i64::from_le_bytes(arr))
    }))
}

/// Monotonic UPSERT; never rewinds.
pub async fn upsert_address_signatures_flushed_slot_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    slot: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO metadata (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE
         SET value = EXCLUDED.value
         WHERE metadata.value < EXCLUDED.value",
    )
    .bind(ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY)
    .bind(slot.to_le_bytes().to_vec())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::start_test_postgres_raw;

    #[tokio::test(flavor = "multi_thread")]
    async fn get_watermark_returns_none_when_unset() {
        let (db, _pg) = start_test_postgres_raw().await;
        let v = get_address_signatures_flushed_slot(&db.pool).await.unwrap();
        assert!(v.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_then_get_roundtrip() {
        let (db, _pg) = start_test_postgres_raw().await;
        let mut tx = db.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 42)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let v = get_address_signatures_flushed_slot(&db.pool).await.unwrap();
        assert_eq!(v, Some(42));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_is_monotonic() {
        let (db, _pg) = start_test_postgres_raw().await;

        let mut tx = db.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 100)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let mut tx = db.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 50)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let v = get_address_signatures_flushed_slot(&db.pool).await.unwrap();
        assert_eq!(v, Some(100), "watermark must not rewind");

        let mut tx = db.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 200)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let v = get_address_signatures_flushed_slot(&db.pool).await.unwrap();
        assert_eq!(v, Some(200));
    }
}
