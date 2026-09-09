use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::AccountsDB},
    anyhow::{Context, Result},
    redis::AsyncCommands,
    tracing::warn,
};

/// Metadata key holding the slot the chain is on right now. Slots tick whether
/// or not a block is produced, so this is the only tip counter that keeps moving
/// while the node is idle.
pub const CURRENT_SLOT_KEY: &str = "current_slot";

/// The live slot, which is what `getSlot` answers. Never behind
/// [`super::get_latest_slot`]: a produced block writes both in one transaction
/// and an idle tick only raises this one.
pub async fn get_current_slot(db: &AccountsDB) -> Result<Option<u64>> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_current_slot_postgres(postgres_db).await,
        AccountsDB::Redis(redis_db) => get_current_slot_redis(redis_db).await,
    }
}

async fn get_current_slot_postgres(db: &PostgresAccountsDB) -> Result<Option<u64>> {
    let result = sqlx::query_scalar::<_, Vec<u8>>("SELECT value FROM metadata WHERE key = $1")
        .bind(CURRENT_SLOT_KEY)
        .fetch_optional(db.pool.as_ref())
        .await;

    let stored = match result {
        Ok(value) => value,
        // "undefined_table": schema not yet created, treat as a fresh node.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => return Ok(None),
        Err(e) => return Err(anyhow::Error::from(e).context("Failed to query the current slot")),
    };

    match stored.as_deref().and_then(super::counter::decode) {
        Some(slot) => Ok(Some(slot)),
        // A node upgrading in place has no counter yet, and every slot carried a
        // block before this change, so the block tip names the same slot.
        None => super::get_latest_slot::get_latest_slot_postgres(db).await,
    }
}

async fn get_current_slot_redis(db: &RedisAccountsDB) -> Result<Option<u64>> {
    let cached = match db.get_trusted::<u64>(CURRENT_SLOT_KEY).await {
        Ok(slot) => slot,
        Err(e) => {
            warn!("Failed to get the current slot from Redis: {}", e);
            None
        }
    };

    match cached {
        Some(slot) => Ok(Some(slot)),
        // No cached slot is a miss, not a stopped chain.
        None => get_current_slot_postgres(&db.fallback).await,
    }
}

/// Publish a tick that produced no block. One metadata row and no block row: the
/// growth this change exists to stop is block rows, and an eight-byte counter is
/// a different shape entirely.
pub async fn set_current_slot(db: &PostgresAccountsDB, slot: u64) -> Result<()> {
    if db.read_only {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO metadata (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(CURRENT_SLOT_KEY)
    .bind(&super::counter::encode(slot)[..])
    .execute(db.pool.as_ref())
    .await
    .context("Failed to publish the current slot")
    .map(|_| ())
}

/// Mirror the live slot so a replica reading through the cache sees it move.
/// Not the cached tip and not read by the coherence protocol, so it can neither
/// condemn a cache nor erase the evidence of a missed batch.
pub(crate) async fn mirror_current_slot(db: &RedisAccountsDB, slot: u64) {
    let mut conn = db.connection.clone();
    if let Err(e) = conn.set::<_, _, ()>(CURRENT_SLOT_KEY, slot).await {
        warn!("Failed to mirror the current slot to Redis: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::traits::AccountsDB,
        test_helpers::{create_test_block_info, start_test_postgres, start_test_redis},
    };
    use solana_sdk::hash::Hash;

    fn postgres_of(db: &AccountsDB) -> crate::accounts::PostgresAccountsDB {
        let AccountsDB::Postgres(ref postgres_db) = db else {
            panic!("expected Postgres variant")
        };
        postgres_db.clone()
    }

    /// The whole point of the counter: a tick that produced no block still moves
    /// the slot, and it does so without adding a block row.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_idle_tick_moves_the_slot_without_a_block() {
        let (mut db, _pg) = start_test_postgres().await;
        let mut block = create_test_block_info(5, Hash::new_unique());
        block.block_height = Some(3);
        db.write_batch(&[], vec![], Some(block)).await.unwrap();

        let postgres_db = postgres_of(&db);
        for slot in 6..=9u64 {
            set_current_slot(&postgres_db, slot).await.unwrap();
        }

        assert_eq!(db.get_current_slot().await.unwrap(), Some(9));
        assert_eq!(
            db.get_latest_slot().await.unwrap(),
            Some(5),
            "the block tip must not move without a block"
        );
        assert_eq!(
            db.get_block_height().await.unwrap(),
            Some(3),
            "an idle tick produces no block, so it counts none"
        );
        assert!(
            db.get_block(9).await.unwrap().is_none(),
            "an idle slot must carry no block row"
        );
        assert_eq!(
            db.get_blocks(0, Some(9)).await.unwrap(),
            vec![5],
            "an idle tick must not add to the stored blocks"
        );
    }

    /// A block republishes the slot in its own transaction, so the counter can
    /// never be left behind a block a client can fetch.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_block_republishes_the_current_slot() {
        let (mut db, _pg) = start_test_postgres().await;
        let postgres_db = postgres_of(&db);
        set_current_slot(&postgres_db, 9).await.unwrap();

        db.write_batch(
            &[],
            vec![],
            Some(create_test_block_info(10, Hash::new_unique())),
        )
        .await
        .unwrap();

        assert_eq!(db.get_current_slot().await.unwrap(), Some(10));
    }

    /// A node upgrading in place has no counter yet, and every slot carried a
    /// block before this change, so the block tip is the same answer.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_current_slot_falls_back_to_the_block_tip() {
        let (mut db, _pg) = start_test_postgres().await;
        db.store_block(create_test_block_info(12, Hash::new_unique()))
            .await
            .unwrap();

        let postgres_db = postgres_of(&db);
        sqlx::query("DELETE FROM metadata WHERE key = $1")
            .bind(CURRENT_SLOT_KEY)
            .execute(postgres_db.pool.as_ref())
            .await
            .unwrap();

        assert_eq!(db.get_current_slot().await.unwrap(), Some(12));
    }

    /// A replica reads the slot through the cache, so the mirror has to carry it
    /// or the replica would report a slot frozen between blocks.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replica_reads_the_mirrored_slot() {
        let (db, _pg) = start_test_postgres().await;
        let postgres_db = postgres_of(&db);
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;
        let deployment_id = super::super::redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        super::super::redis_coherence::stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        mirror_current_slot(&redis_db, 41).await;

        let cache = AccountsDB::Redis(redis_db);
        assert_eq!(cache.get_current_slot().await.unwrap(), Some(41));
    }
}
