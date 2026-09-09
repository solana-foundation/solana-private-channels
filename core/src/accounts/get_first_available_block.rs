use {
    super::{postgres::PostgresAccountsDB, traits::AccountsDB},
    anyhow::{Context, Result},
};

pub async fn get_first_available_block(db: &AccountsDB) -> Result<u64> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_first_available_block_postgres(postgres_db).await,
        // Served from the source of truth, and no slot index is cached. One
        // would only cover blocks written since the cache attached, so its
        // minimum would be a cache artefact rather than the ledger's first
        // available block.
        AccountsDB::Redis(redis_db) => get_first_available_block_postgres(&redis_db.fallback).await,
    }
}

async fn get_first_available_block_postgres(db: &PostgresAccountsDB) -> Result<u64> {
    let pool = db.pool.clone();

    let metadata_slot = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT value FROM metadata WHERE key = 'first_available_block'",
    )
    .fetch_optional(pool.as_ref())
    .await
    .context("Failed to query first_available_block metadata")?
    .flatten()
    .and_then(|value| decode_first_available_block(&value));

    if let Some(slot) = metadata_slot {
        return Ok(slot);
    }

    let slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
        .fetch_one(pool.as_ref())
        .await
        .context("Failed to query first available block")?;

    slot.map(|s| s as u64)
        .context("No blocks found in database")
}

fn decode_first_available_block(value: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = value.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::decode_first_available_block;

    #[test]
    fn decode_first_available_block_supports_u64_le_bytes() {
        let encoded = 42_u64.to_le_bytes();
        assert_eq!(decode_first_available_block(&encoded), Some(42));
    }

    #[test]
    fn decode_first_available_block_rejects_wrong_length() {
        assert_eq!(decode_first_available_block(b"short"), None);
    }
}
