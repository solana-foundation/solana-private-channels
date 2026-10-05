use super::get_latest_slot::LATEST_SLOT_KEY;
use sqlx::{PgPool, Postgres, Transaction};

pub const ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY: &str = "address_signatures_flushed_slot";

pub async fn get_address_signatures_flushed_slot(pool: &PgPool) -> sqlx::Result<Option<i64>> {
    let result =
        sqlx::query_scalar::<_, Option<Vec<u8>>>("SELECT value FROM metadata WHERE key = $1")
            .bind(ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY)
            .fetch_optional(pool)
            .await;

    let bytes = match result {
        Ok(Some(Some(b))) => Some(b),
        Ok(Some(None)) | Ok(None) => None,
        // 42P01 = undefined_table; schema not yet created.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => return Ok(None),
        Err(e) => return Err(e),
    };

    Ok(bytes.and_then(|b| {
        let arr: [u8; 8] = b.as_slice().try_into().ok()?;
        Some(i64::from_be_bytes(arr))
    }))
}

/// The watermark and the newest committed block slot, read in one statement so
/// both come from the same snapshot. `None` if either is missing: a caller that
/// gates on this must refuse rather than guess.
pub async fn get_address_index_progress(pool: &PgPool) -> sqlx::Result<Option<(i64, u64)>> {
    let rows = sqlx::query_as::<_, (String, Vec<u8>)>(
        "SELECT key, value FROM metadata WHERE key = ANY($1)",
    )
    .bind(vec![
        ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY.to_string(),
        LATEST_SLOT_KEY.to_string(),
    ])
    .fetch_all(pool)
    .await?;

    let mut watermark = None;
    let mut latest_block = None;
    for (key, value) in rows {
        if key == ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY {
            watermark = <[u8; 8]>::try_from(value.as_slice())
                .ok()
                .map(i64::from_be_bytes);
        } else {
            latest_block = super::counter::decode(&value);
        }
    }
    Ok(watermark.zip(latest_block))
}

/// Monotonic UPSERT; never rewinds.
pub async fn upsert_address_signatures_flushed_slot_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    slot: i64,
) -> sqlx::Result<()> {
    debug_assert!(
        slot >= 0,
        "watermark slot must be non-negative; a negative slot breaks the big-endian bytea monotonic compare"
    );
    sqlx::query(
        "INSERT INTO metadata (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE
         SET value = EXCLUDED.value
         WHERE metadata.value < EXCLUDED.value",
    )
    .bind(ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY)
    .bind(slot.to_be_bytes().to_vec())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::start_test_postgres_raw;
    use sqlx::postgres::PgPoolOptions;

    /// The two keys use different byte orders, so a swapped decoder would read a huge
    /// slot and pass every gate. A block committed through the real write path pins both.
    #[tokio::test(flavor = "multi_thread")]
    async fn index_progress_reads_the_watermark_and_the_committed_block() {
        use crate::{accounts::traits::AccountsDB, test_helpers::create_test_block_info};
        use solana_sdk::hash::Hash;

        let (mut db, _pg) = crate::test_helpers::start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = db else {
            panic!("expected Postgres variant")
        };
        let pool = postgres_db.pool.clone();
        assert_eq!(db.get_address_index_progress().await.unwrap(), None);

        db.write_batch(
            &[],
            vec![],
            Some(create_test_block_info(300, Hash::new_unique())),
        )
        .await
        .unwrap();
        assert_eq!(
            db.get_address_index_progress().await.unwrap(),
            None,
            "a missing watermark must read as None, not 0"
        );

        let mut tx = pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 200)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            db.get_address_index_progress().await.unwrap(),
            Some((200, 300))
        );

        sqlx::query("DELETE FROM metadata WHERE key = $1")
            .bind(LATEST_SLOT_KEY)
            .execute(&*pool)
            .await
            .unwrap();
        assert_eq!(
            db.get_address_index_progress().await.unwrap(),
            None,
            "a missing newest block must read as None"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_watermark_returns_none_when_unset() {
        let (db, _pg) = start_test_postgres_raw().await;
        let v = get_address_signatures_flushed_slot(&db.pool).await.unwrap();
        assert!(v.is_none());
    }

    /// Fresh DB with no schema must yield None, not an error.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_watermark_tolerates_missing_metadata_table() {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::postgres::Postgres;
        let container = Postgres::default()
            .with_db_name("pg_no_schema")
            .with_user("postgres")
            .with_password("password")
            .start()
            .await
            .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!(
            "postgres://postgres:password@{}:{}/pg_no_schema",
            host, port
        );
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let v = get_address_signatures_flushed_slot(&pool).await.unwrap();
        assert!(v.is_none(), "fresh DB must not error on missing schema");
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

    /// Bytea lexicographic compare must match integer compare across 256.
    #[tokio::test(flavor = "multi_thread")]
    async fn upsert_advances_across_256_byte_boundary() {
        let (db, _pg) = start_test_postgres_raw().await;

        let mut tx = db.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 255)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let mut tx = db.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, 256)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let v = get_address_signatures_flushed_slot(&db.pool).await.unwrap();
        assert_eq!(
            v,
            Some(256),
            "watermark must advance from 255 to 256 under bytea < comparison"
        );
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
