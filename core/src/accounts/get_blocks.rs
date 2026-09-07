use {
    super::{postgres::PostgresAccountsDB, traits::AccountsDB},
    anyhow::{anyhow, Context, Result},
};

/// Maximum number of blocks that can be returned (per Solana spec)
const MAX_BLOCKS_RANGE: u64 = 500_000;

pub async fn get_blocks(
    db: &AccountsDB,
    start_slot: u64,
    end_slot: Option<u64>,
) -> Result<Vec<u64>> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            get_blocks_postgres(postgres_db, start_slot, end_slot).await
        }
        // Served from the source of truth, never the cache. A range answered
        // from a partial mirror is indistinguishable from a complete one, so a
        // cached miss would silently drop finalized blocks instead of surfacing
        // as a miss.
        AccountsDB::Redis(redis_db) => {
            get_blocks_postgres(&redis_db.fallback, start_slot, end_slot).await
        }
    }
}

async fn get_blocks_postgres(
    db: &PostgresAccountsDB,
    start_slot: u64,
    end_slot: Option<u64>,
) -> Result<Vec<u64>> {
    let pool = db.pool.clone();

    let end_slot = match end_slot {
        Some(end) => end,
        None => sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
            .fetch_one(pool.as_ref())
            .await
            .context("Failed to query latest slot")?
            .context("No blocks found in database")? as u64,
    };

    // Enforce maximum range constraint
    if end_slot > start_slot && (end_slot - start_slot) > MAX_BLOCKS_RANGE {
        return Err(anyhow!(
            "Range too large: {} slots (max: {})",
            end_slot - start_slot,
            MAX_BLOCKS_RANGE
        ));
    }

    // Query blocks within the range
    let slots = sqlx::query_scalar::<_, i64>(
        "SELECT slot FROM blocks WHERE slot >= $1 AND slot <= $2 ORDER BY slot ASC",
    )
    .bind(start_slot as i64)
    .bind(end_slot as i64)
    .fetch_all(pool.as_ref())
    .await
    .context("Failed to query blocks")?;

    // Convert i64 slots to u64
    Ok(slots.into_iter().map(|s| s as u64).collect())
}
