use {
    super::{postgres::PostgresAccountsDB, traits::BlockInfo},
    sqlx::Row,
};

#[derive(Debug, thiserror::Error)]
pub enum TipError {
    #[error("failed to read the chain tip from the blocks table")]
    Query(#[from] sqlx::Error),
    #[error("the tip block row at slot {slot} could not be decoded (likely pre-upgrade block data; wipe the DB or add a migration shim)")]
    Corrupt {
        slot: u64,
        #[source]
        source: bincode::Error,
    },
    #[error("the blocks table is empty but metadata still names a chain tip, so this ledger is not fresh")]
    OrphanedTip,
}

/// Read the chain tip, the block row with the highest slot. `Ok(None)` is the only
/// genesis signal and means the ledger provably has no blocks; every read or decode
/// failure is an `Err`. One row, so slot and hash cannot drift. Never from a cache.
pub async fn get_tip(db: &PostgresAccountsDB) -> Result<Option<BlockInfo>, TipError> {
    // Backward scan of the slot primary key, so this reads one row, not the table.
    let row = sqlx::query("SELECT slot, data FROM blocks ORDER BY slot DESC LIMIT 1")
        .fetch_optional(db.pool.as_ref())
        .await?;

    let Some(row) = row else {
        // No block rows is a fresh ledger only when nothing else claims a chain.
        // Blocks emptied under live accounts would otherwise resume from genesis.
        return if metadata_claims_a_tip(db).await? {
            Err(TipError::OrphanedTip)
        } else {
            Ok(None)
        };
    };

    let slot: i64 = row.try_get("slot")?;
    let data: Vec<u8> = row.try_get("data")?;

    let block = bincode::deserialize::<BlockInfo>(&data).map_err(|source| TipError::Corrupt {
        slot: slot as u64,
        source,
    })?;

    Ok(Some(block))
}

/// Whether the metadata table still names a chain tip.
async fn metadata_claims_a_tip(db: &PostgresAccountsDB) -> Result<bool, TipError> {
    let claimed: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM metadata WHERE key = 'latest_blockhash'")
            .fetch_optional(db.pool.as_ref())
            .await?;

    Ok(claimed.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::AccountsDB,
        test_helpers::{create_test_block_info, start_test_postgres},
    };
    use solana_sdk::hash::Hash;

    /// Hand back the Postgres handle plus a live `AccountsDB` for seeding.
    async fn tip_test_db() -> (
        AccountsDB,
        PostgresAccountsDB,
        testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    ) {
        let (db, container) = start_test_postgres().await;
        let AccountsDB::Postgres(ref pg) = db else {
            panic!("Expected Postgres variant")
        };
        let pg = pg.clone();
        (db, pg, container)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tip_on_an_empty_ledger_is_none() {
        let (_db, pg, _c) = tip_test_db().await;

        let tip = get_tip(&pg).await.expect("read succeeded");

        assert!(tip.is_none(), "a ledger with no blocks has no tip");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tip_returns_the_highest_slot_not_the_last_written() {
        let (mut db, pg, _c) = tip_test_db().await;
        let expected_hash = Hash::new_unique();

        for (slot, hash) in [
            (5, Hash::new_unique()),
            (9, expected_hash),
            (3, Hash::new_unique()),
        ] {
            db.store_block(create_test_block_info(slot, hash))
                .await
                .unwrap();
        }

        let tip = get_tip(&pg).await.expect("read succeeded").expect("a tip");

        assert_eq!(
            tip.slot, 9,
            "the tip is the highest slot, not the newest row"
        );
        assert_eq!(
            tip.blockhash, expected_hash,
            "the blockhash must come from the tip row itself"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tip_propagates_a_query_failure() {
        let (mut db, pg, _c) = tip_test_db().await;
        db.store_block(create_test_block_info(1, Hash::new_unique()))
            .await
            .unwrap();

        pg.pool.close().await;

        let err = get_tip(&pg)
            .await
            .expect_err("a closed pool cannot be read");

        assert!(
            matches!(err, TipError::Query(_)),
            "a database failure must never read as an empty ledger, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tip_propagates_a_missing_schema() {
        let (mut db, pg, _c) = tip_test_db().await;
        db.store_block(create_test_block_info(1, Hash::new_unique()))
            .await
            .unwrap();

        sqlx::query("DROP TABLE blocks")
            .execute(pg.pool.as_ref())
            .await
            .unwrap();

        let err = get_tip(&pg)
            .await
            .expect_err("a missing table cannot be read");

        assert!(
            matches!(err, TipError::Query(_)),
            "a missing blocks table is a failure, not a fresh ledger, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tip_propagates_an_undecodable_tip_row() {
        let (mut db, pg, _c) = tip_test_db().await;
        db.store_block(create_test_block_info(7, Hash::new_unique()))
            .await
            .unwrap();

        sqlx::query("UPDATE blocks SET data = $1 WHERE slot = $2")
            .bind(vec![0xFFu8; 16])
            .bind(7i64)
            .execute(pg.pool.as_ref())
            .await
            .unwrap();

        let err = get_tip(&pg).await.expect_err("garbage cannot decode");

        assert!(
            matches!(err, TipError::Corrupt { slot: 7, .. }),
            "an undecodable tip must name its slot, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_tip_rejects_an_empty_blocks_table_that_metadata_still_claims() {
        let (mut db, pg, _c) = tip_test_db().await;
        db.store_block(create_test_block_info(4, Hash::new_unique()))
            .await
            .unwrap();

        // Blocks emptied under a ledger whose accounts and metadata survive.
        sqlx::query("DELETE FROM blocks")
            .execute(pg.pool.as_ref())
            .await
            .unwrap();

        let err = get_tip(&pg)
            .await
            .expect_err("a claimed tip with no block rows is not a fresh ledger");

        assert!(
            matches!(err, TipError::OrphanedTip),
            "an emptied blocks table must not be read as a fresh ledger, got {err:?}"
        );
    }
}
