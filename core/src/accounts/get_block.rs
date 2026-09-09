use {
    super::{
        postgres::PostgresAccountsDB,
        redis::RedisAccountsDB,
        traits::{AccountsDB, BlockInfo},
    },
    anyhow::{Context, Result},
    sqlx::Row,
    std::sync::Arc,
    tracing::{debug, warn},
};

/// `Ok(None)` means the slot genuinely holds no block on this node, which is
/// routine because truncation prunes. Every storage or decode failure is an
/// `Err`, so a caller can never read an internal error as a skipped slot.
pub async fn get_block(db: &AccountsDB, slot: u64) -> Result<Option<BlockInfo>> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_block_postgres(postgres_db, slot).await,
        AccountsDB::Redis(redis_db) => get_block_redis(redis_db, slot).await,
    }
}

async fn get_block_postgres(db: &PostgresAccountsDB, slot: u64) -> Result<Option<BlockInfo>> {
    let pool = Arc::clone(&db.pool);

    let row = sqlx::query("SELECT data FROM blocks WHERE slot = $1")
        .bind(slot as i64)
        .fetch_optional(pool.as_ref())
        .await
        .with_context(|| format!("Failed to read block at slot {}", slot))?;

    let Some(row) = row else {
        debug!("Block not found at slot {}", slot);
        return Ok(None);
    };

    let data: Vec<u8> = row.get("data");
    let block_info = bincode::deserialize(&data)
        .with_context(|| format!("Failed to deserialize block at slot {}", slot))?;
    debug!("Retrieved block at slot {}", slot);
    Ok(Some(block_info))
}

async fn get_block_redis(db: &RedisAccountsDB, slot: u64) -> Result<Option<BlockInfo>> {
    let key = format!("block:{}", slot);
    let cached = match db.get_trusted::<Vec<u8>>(&key).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to read block at slot {} from Redis: {}", slot, e);
            None
        }
    };

    if let Some(bytes) = cached {
        match bincode::deserialize(&bytes) {
            Ok(block_info) => return Ok(Some(block_info)),
            // Written by an older build whose BlockInfo had fewer fields. Falling
            // through keeps the slot readable; failing here would make it error
            // for as long as the entry sat in the cache.
            Err(e) => warn!("Failed to deserialize cached block at slot {}: {}", slot, e),
        }
    }

    // A slot missing or unreadable in the cache is a miss, not a pruned or
    // skipped slot.
    get_block_postgres(&db.fallback, slot).await
}
