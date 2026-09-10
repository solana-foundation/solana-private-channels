use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::AccountsDB},
    anyhow::Result,
    tracing::warn,
};

/// Metadata key holding the count of blocks produced so far.
pub const BLOCK_HEIGHT_KEY: &str = "block_height";

/// Read the durable block height: how many blocks the chain has produced, which
/// is independent of the slot once idle ticks stop producing one each.
pub async fn get_block_height(db: &AccountsDB) -> Result<Option<u64>> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_block_height_postgres(postgres_db).await,
        AccountsDB::Redis(redis_db) => get_block_height_redis(redis_db).await,
    }
}

async fn get_block_height_postgres(db: &PostgresAccountsDB) -> Result<Option<u64>> {
    let pool = db.pool.clone();

    let result = sqlx::query_scalar::<_, Vec<u8>>("SELECT value FROM metadata WHERE key = $1")
        .bind(BLOCK_HEIGHT_KEY)
        .fetch_optional(pool.as_ref())
        .await;

    let stored = match result {
        Ok(value) => value,
        // "undefined_table": schema not yet created, treat as a fresh node.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => return Ok(None),
        Err(e) => return Err(anyhow::Error::from(e).context("Failed to query block height")),
    };

    match stored.as_deref().and_then(super::counter::decode) {
        Some(height) => Ok(Some(height)),
        // A node upgrading in place has no counter yet. The last stored block
        // carries the height the old build assigned it, so continuing from there
        // keeps every lastValidBlockHeight a client holds valid.
        None => last_block_height_postgres(db).await,
    }
}

/// The upgrade fallback: the highest stored slot. Before the counters became
/// independent every slot carried a block and the height was the slot, so this is
/// the same answer without decoding a block payload on a hot read path.
async fn last_block_height_postgres(db: &PostgresAccountsDB) -> Result<Option<u64>> {
    let pool = db.pool.clone();

    let result = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(pool.as_ref())
        .await;

    match result {
        Ok(slot) => Ok(slot.map(|s| s as u64)),
        // "undefined_table": schema not yet created, treat as a fresh node.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => Ok(None),
        Err(e) => Err(anyhow::Error::from(e).context("Failed to query the latest block slot")),
    }
}

async fn get_block_height_redis(db: &RedisAccountsDB) -> Result<Option<u64>> {
    let cached = match db.get_trusted::<u64>(BLOCK_HEIGHT_KEY).await {
        Ok(height) => height,
        Err(e) => {
            warn!("Failed to get block height from Redis: {}", e);
            None
        }
    };

    match cached {
        Some(height) => Ok(Some(height)),
        // No cached height is a miss, not an empty ledger.
        None => get_block_height_postgres(&db.fallback).await,
    }
}
