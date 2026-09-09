use {
    super::{postgres::PostgresAccountsDB, traits::AccountsDB},
    anyhow::{anyhow, Context, Result},
};

/// Maximum number of blocks that can be returned (per Solana spec)
const MAX_BLOCKS_LIMIT: u64 = 500_000;

/// The first `limit` slots at or after `start_slot` that produced a block,
/// ascending. Unlike `get_blocks` the upper bound is a count, not a slot, so a
/// caller looking for the next producing slot does not have to guess how far
/// ahead to scan.
pub async fn get_blocks_with_limit(
    db: &AccountsDB,
    start_slot: u64,
    limit: u64,
) -> Result<Vec<u64>> {
    if limit > MAX_BLOCKS_LIMIT {
        return Err(anyhow!(
            "Limit too large: {} (max: {})",
            limit,
            MAX_BLOCKS_LIMIT
        ));
    }
    if limit == 0 {
        return Ok(vec![]);
    }

    match db {
        AccountsDB::Postgres(postgres_db) => {
            get_blocks_with_limit_postgres(postgres_db, start_slot, limit).await
        }
        // Served from the source of truth, and no slot index is cached. One
        // would hold only the blocks written since the cache attached, so the
        // first `limit` members it could offer would not be the ledger's.
        AccountsDB::Redis(redis_db) => {
            get_blocks_with_limit_postgres(&redis_db.fallback, start_slot, limit).await
        }
    }
}

async fn get_blocks_with_limit_postgres(
    db: &PostgresAccountsDB,
    start_slot: u64,
    limit: u64,
) -> Result<Vec<u64>> {
    let pool = db.pool.clone();

    let slots = sqlx::query_scalar::<_, i64>(
        "SELECT slot FROM blocks WHERE slot >= $1 ORDER BY slot ASC LIMIT $2",
    )
    .bind(start_slot as i64)
    .bind(limit as i64)
    .fetch_all(pool.as_ref())
    .await
    .context("Failed to query blocks with limit")?;

    Ok(slots.into_iter().map(|s| s as u64).collect())
}
