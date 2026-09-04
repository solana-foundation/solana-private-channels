use {
    super::{
        postgres::PostgresAccountsDB,
        traits::{AccountsDB, BlockInfo},
    },
    anyhow::{Context, Result},
    sqlx::Row,
};

/// The newest `limit` blocks, oldest first. A count of blocks, not a span of
/// slots: the blockhash window is `max_blockhashes` blocks, and a slot range that
/// wide holds far fewer of them once idle ticks stop producing one each.
pub async fn get_last_blocks(db: &AccountsDB, limit: usize) -> Result<Vec<BlockInfo>> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_last_blocks_postgres(postgres_db, limit).await,
        // Served from the source of truth: the cache cannot express which blocks
        // it is missing, and this path feeds the dedup rebuild, where a dropped
        // block means a replay slips through.
        AccountsDB::Redis(redis_db) => get_last_blocks_postgres(&redis_db.fallback, limit).await,
    }
}

async fn get_last_blocks_postgres(db: &PostgresAccountsDB, limit: usize) -> Result<Vec<BlockInfo>> {
    let pool = db.pool.clone();

    let rows = sqlx::query("SELECT data FROM blocks ORDER BY slot DESC LIMIT $1")
        .bind(limit as i64)
        .fetch_all(pool.as_ref())
        .await
        .context("Failed to query the most recent blocks")?;

    let mut blocks = Vec::with_capacity(rows.len());
    for row in rows.into_iter().rev() {
        let data: Vec<u8> = row.get("data");
        // This path feeds the dedup rebuild, so a decode failure fails closed
        // rather than silently seeding a short cache.
        let block = bincode::deserialize::<BlockInfo>(&data)
            .context("Failed to deserialize a recent block (likely pre-upgrade block data; wipe the DB or add a migration shim)")?;
        blocks.push(block);
    }

    Ok(blocks)
}
