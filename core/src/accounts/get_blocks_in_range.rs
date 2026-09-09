use {
    super::{
        postgres::PostgresAccountsDB,
        traits::{AccountsDB, BlockInfo},
    },
    anyhow::{Context, Result},
    sqlx::Row,
};

pub async fn get_blocks_in_range(
    db: &AccountsDB,
    start_slot: u64,
    end_slot: u64,
) -> Result<Vec<BlockInfo>> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            get_blocks_in_range_postgres(postgres_db, start_slot, end_slot).await
        }
        // Served from the source of truth: the cache cannot express which slots
        // in the range it has no entry for, so reading through it would silently
        // shorten the range instead of missing.
        AccountsDB::Redis(redis_db) => {
            get_blocks_in_range_postgres(&redis_db.fallback, start_slot, end_slot).await
        }
    }
}

async fn get_blocks_in_range_postgres(
    db: &PostgresAccountsDB,
    start_slot: u64,
    end_slot: u64,
) -> Result<Vec<BlockInfo>> {
    let pool = db.pool.clone();

    let rows =
        sqlx::query("SELECT data FROM blocks WHERE slot >= $1 AND slot <= $2 ORDER BY slot ASC")
            .bind(start_slot as i64)
            .bind(end_slot as i64)
            .fetch_all(pool.as_ref())
            .await
            .context("Failed to query blocks in range")?;

    let mut blocks = Vec::with_capacity(rows.len());
    for row in rows {
        let data: Vec<u8> = row.get("data");
        // A trailing field was added to BlockInfo, so a decode failure here most
        // likely means pre-upgrade block data. This path feeds the dedup rebuild,
        // so we fail closed (propagate) rather than silently seed an empty cache;
        // wipe the DB or add a migration shim to recover.
        let block = bincode::deserialize::<BlockInfo>(&data)
            .context("Failed to deserialize block in range query (likely pre-upgrade block data; wipe the DB or add a migration shim)")?;
        blocks.push(block);
    }

    Ok(blocks)
}
