use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::AccountsDB},
    anyhow::Result,
    tracing::warn,
};

/// Metadata key holding the slot of the latest produced block. Slots count ticks,
/// so it steps by however many ticks passed since the last block rather than
/// always by one, and it is only durable when a block makes it so.
pub const LATEST_SLOT_KEY: &str = "latest_slot";

pub async fn get_latest_slot(db: &AccountsDB) -> Result<Option<u64>> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_latest_slot_postgres(postgres_db).await,
        AccountsDB::Redis(redis_db) => get_latest_slot_redis(redis_db).await,
    }
}

pub(super) async fn get_latest_slot_postgres(db: &PostgresAccountsDB) -> Result<Option<u64>> {
    let pool = db.pool.clone();

    let result = sqlx::query_scalar::<_, Vec<u8>>("SELECT value FROM metadata WHERE key = $1")
        .bind(LATEST_SLOT_KEY)
        .fetch_optional(pool.as_ref())
        .await;

    let stored = match result {
        Ok(value) => value,
        // "undefined_table": schema not yet created, treat as fresh node
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => return Ok(None),
        Err(e) => return Err(anyhow::Error::from(e).context("Failed to query latest slot")),
    };

    match stored.as_deref().and_then(super::counter::decode) {
        Some(slot) => Ok(Some(slot)),
        // Ticks now outnumber blocks, so MAX(slot) can no longer advance the
        // counter. It stays the upgrade fallback: before this change every slot
        // carried a block, so it names the same tip the counter would.
        None => max_block_slot_postgres(db).await,
    }
}

async fn max_block_slot_postgres(db: &PostgresAccountsDB) -> Result<Option<u64>> {
    let pool = db.pool.clone();

    let result = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(pool.as_ref())
        .await;

    match result {
        Ok(slot) => Ok(slot.map(|s| s as u64)),
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => Ok(None),
        Err(e) => Err(anyhow::Error::from(e).context("Failed to query latest slot")),
    }
}

async fn get_latest_slot_redis(db: &RedisAccountsDB) -> Result<Option<u64>> {
    let cached = match db.get_trusted::<u64>(LATEST_SLOT_KEY).await {
        Ok(slot) => slot,
        Err(e) => {
            warn!("Failed to get latest slot from Redis: {}", e);
            None
        }
    };

    match cached {
        Some(slot) => Ok(Some(slot)),
        // No cached tip is a miss, not an empty ledger.
        None => get_latest_slot_postgres(&db.fallback).await,
    }
}
