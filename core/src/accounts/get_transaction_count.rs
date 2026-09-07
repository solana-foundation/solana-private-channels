use {
    super::{
        postgres::PostgresAccountsDB, traits::AccountsDB, transaction_count::TransactionCount,
    },
    anyhow::{Context, Result},
};

pub async fn get_transaction_count(db: &AccountsDB) -> Result<u64> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_transaction_count_postgres(postgres_db).await,
        // Served from the source of truth, and no counter is cached at all. One
        // could only be built by INCR, which is not idempotent: a single dropped
        // cache write would leave it permanently short, with nothing to detect
        // the gap against.
        AccountsDB::Redis(redis_db) => get_transaction_count_postgres(&redis_db.fallback).await,
    }
}

async fn get_transaction_count_postgres(db: &PostgresAccountsDB) -> Result<u64> {
    let pool = db.pool.clone();

    let count_bytes = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT value FROM metadata WHERE key = 'transaction_count'",
    )
    .fetch_optional(pool.as_ref())
    .await
    .context("Failed to query transaction count")?;

    let count = count_bytes
        .and_then(|bytes| TransactionCount::from_bytes(&bytes))
        .unwrap_or_default();

    Ok(count.count())
}
