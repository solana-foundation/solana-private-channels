use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::AccountsDB},
    anyhow::{anyhow, Context, Result},
    solana_sdk::hash::Hash,
    std::str::FromStr,
    tracing::warn,
};

/// Metadata key holding the tip blockhash.
pub const LATEST_BLOCKHASH_KEY: &str = "latest_blockhash";

pub async fn get_latest_blockhash(db: &AccountsDB) -> Result<Hash> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_latest_blockhash_postgres(postgres_db).await,
        AccountsDB::Redis(redis_db) => get_latest_blockhash_redis(redis_db).await,
    }
}

async fn get_latest_blockhash_postgres(db: &PostgresAccountsDB) -> Result<Hash> {
    let pool = db.pool.clone();
    // Get the latest blockhash from metadata table
    let blockhash_bytes: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT value FROM metadata WHERE key = 'latest_blockhash'")
            .fetch_optional(pool.as_ref())
            .await
            .context("Failed to query latest blockhash")?;

    if let Some(bytes) = blockhash_bytes {
        // The blockhash is stored as raw bytes (32 bytes)
        let hash_array: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("Invalid blockhash bytes length: {}", bytes.len()))?;
        Ok(Hash::new_from_array(hash_array))
    } else {
        Err(anyhow!("No blockhash found in metadata table"))
    }
}

async fn get_latest_blockhash_redis(db: &RedisAccountsDB) -> Result<Hash> {
    let cached = match db.get_trusted::<String>(LATEST_BLOCKHASH_KEY).await {
        Ok(hash_str) => hash_str,
        Err(e) => {
            warn!("Failed to get latest blockhash from Redis: {}", e);
            None
        }
    };

    if let Some(hash_str) = cached {
        return Hash::from_str(&hash_str).map_err(|e| anyhow!("Invalid blockhash format: {}", e));
    }

    // No cached blockhash is a miss, not a ledger without a tip.
    get_latest_blockhash_postgres(&db.fallback).await
}
