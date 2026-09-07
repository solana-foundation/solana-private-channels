use {
    super::{
        postgres::PostgresAccountsDB,
        redis::RedisAccountsDB,
        traits::{AccountsDB, BlockInfo},
    },
    std::sync::Arc,
    tracing::{debug, warn},
};

pub async fn store_block(db: &mut AccountsDB, block_info: BlockInfo) -> Result<(), String> {
    match db {
        AccountsDB::Postgres(postgres_db) => store_block_postgres(postgres_db, block_info).await,
        AccountsDB::Redis(redis_db) => store_block_redis(redis_db, block_info).await,
    }
}

async fn store_block_postgres(
    db: &mut PostgresAccountsDB,
    block_info: BlockInfo,
) -> Result<(), String> {
    if db.read_only {
        warn!("Attempted to store block in read-only mode");
        return Ok(());
    }

    let pool = Arc::clone(&db.pool);
    let slot = block_info.slot;
    let blockhash = block_info.blockhash;
    let tx_count = block_info.transaction_signatures.len();

    let block_data = bincode::serialize(&block_info)
        .map_err(|e| format!("Failed to serialize block info: {}", e))?;

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("Failed to begin transaction: {}", e))?;

    // Store block
    sqlx::query(
        "INSERT INTO blocks (slot, data) VALUES ($1, $2)
         ON CONFLICT (slot) DO UPDATE SET data = $2",
    )
    .bind(slot as i64)
    .bind(&block_data)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("Failed to store block: {}", e))?;

    // The whole tip, not just the hash: the slot and the height are durable
    // counters now, and leaving them behind would publish a blockhash newer than
    // the height it is paired with.
    let keys: Vec<&str> = vec![
        super::get_latest_blockhash::LATEST_BLOCKHASH_KEY,
        super::get_latest_slot::LATEST_SLOT_KEY,
        super::current_slot::CURRENT_SLOT_KEY,
        super::get_block_height::BLOCK_HEIGHT_KEY,
    ];
    let values: Vec<Vec<u8>> = vec![
        blockhash.as_ref().to_vec(),
        super::counter::encode(slot).to_vec(),
        super::counter::encode(slot).to_vec(),
        super::counter::encode(block_info.block_height.unwrap_or(slot)).to_vec(),
    ];
    sqlx::query(
        "INSERT INTO metadata (key, value)
         SELECT * FROM UNNEST($1::varchar[], $2::bytea[])
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&keys)
    .bind(&values)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("Failed to store the chain tip: {}", e))?;

    tx.commit()
        .await
        .map_err(|e| format!("Failed to commit transaction: {}", e))?;

    debug!(
        "Stored block at slot {} with {} transactions",
        slot, tx_count
    );
    Ok(())
}

async fn store_block_redis(db: &mut RedisAccountsDB, block_info: BlockInfo) -> Result<(), String> {
    let slot = block_info.slot;
    let height = block_info.block_height.unwrap_or(slot);
    let serialized = bincode::serialize(&block_info)
        .map_err(|e| format!("Failed to serialize block info: {}", e))?;

    // The whole tip in one pipeline, matching the mirror: serving a blockhash
    // newer than the height it is paired with would publish a deadline computed
    // against a stale counter.
    let mut pipe = redis::pipe();
    pipe.atomic();
    pipe.set(
        super::get_latest_blockhash::LATEST_BLOCKHASH_KEY,
        block_info.blockhash.to_string(),
    );
    pipe.set(super::get_latest_slot::LATEST_SLOT_KEY, slot);
    pipe.set(super::current_slot::CURRENT_SLOT_KEY, slot);
    pipe.set(super::get_block_height::BLOCK_HEIGHT_KEY, height);
    let key = format!("block:{}", slot);
    match db.block_ttl_secs() {
        0 => pipe.set(key, serialized),
        ttl => pipe.set_ex(key, serialized, ttl),
    };

    pipe.query_async::<()>(&mut db.connection)
        .await
        .map_err(|e| format!("Failed to store block in Redis: {}", e))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_helpers::{create_test_block_info, start_test_postgres, start_test_redis},
        redis::AsyncCommands,
        solana_sdk::hash::Hash,
    };

    /// The Redis arm publishes the whole tip, not just the hash. Leaving the slot
    /// and the height behind would serve a blockhash newer than the counters
    /// paired with it, which is the incoherent tip the Postgres arm avoids.
    #[tokio::test(flavor = "multi_thread")]
    async fn store_block_redis_writes_the_whole_tip_with_the_ttl() {
        let (db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = db else {
            panic!("expected Postgres variant")
        };
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        redis_raw.set_block_ttl_secs(120);
        let mut conn = redis_raw.connection.clone();
        let mut cache = AccountsDB::Redis(redis_raw);

        let blockhash = Hash::new_unique();
        let mut block = create_test_block_info(42, blockhash);
        block.block_height = Some(7);
        cache.store_block(block).await.unwrap();

        assert_eq!(
            conn.get::<_, Option<String>>(super::super::get_latest_blockhash::LATEST_BLOCKHASH_KEY)
                .await
                .unwrap(),
            Some(blockhash.to_string())
        );
        assert_eq!(
            conn.get::<_, Option<u64>>(super::super::get_latest_slot::LATEST_SLOT_KEY)
                .await
                .unwrap(),
            Some(42)
        );
        assert_eq!(
            conn.get::<_, Option<u64>>(super::super::current_slot::CURRENT_SLOT_KEY)
                .await
                .unwrap(),
            Some(42)
        );
        assert_eq!(
            conn.get::<_, Option<u64>>(super::super::get_block_height::BLOCK_HEIGHT_KEY)
                .await
                .unwrap(),
            Some(7)
        );

        let ttl: i64 = conn.ttl("block:42").await.unwrap();
        assert!(
            ttl > 0 && ttl <= 120,
            "the cached block must carry the configured expiry, got {ttl}"
        );
    }

    /// Zero disables the expiry, so a cached block outlives any TTL cycle.
    #[tokio::test(flavor = "multi_thread")]
    async fn store_block_redis_keeps_a_block_forever_when_the_ttl_is_off() {
        let (db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = db else {
            panic!("expected Postgres variant")
        };
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        let mut conn = redis_raw.connection.clone();
        let mut cache = AccountsDB::Redis(redis_raw);

        cache
            .store_block(create_test_block_info(9, Hash::new_unique()))
            .await
            .unwrap();

        // -1 is Redis for "key exists, no expiry".
        assert_eq!(conn.ttl::<_, i64>("block:9").await.unwrap(), -1);
    }
}
