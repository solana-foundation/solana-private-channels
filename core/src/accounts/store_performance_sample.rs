use {
    super::{postgres::PostgresAccountsDB, traits::AccountsDB},
    anyhow::{Context, Result},
    solana_rpc_client_types::response::RpcPerfSample,
};

pub async fn store_performance_sample(db: &mut AccountsDB, sample: RpcPerfSample) -> Result<()> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            store_performance_sample_postgres(postgres_db, sample).await
        }
        // Written to the source of truth, not the cache: the read path serves
        // samples from Postgres, because a trimmed list cannot express a cache
        // miss, so caching them would only produce keys nothing reads.
        AccountsDB::Redis(redis_db) => {
            let mut postgres_db = redis_db.fallback.clone();
            store_performance_sample_postgres(&mut postgres_db, sample).await
        }
    }
}

async fn store_performance_sample_postgres(
    db: &mut PostgresAccountsDB,
    sample: RpcPerfSample,
) -> Result<()> {
    let pool = db.pool.clone();

    sqlx::query(
        r#"
        INSERT INTO performance_samples (slot, num_transactions, num_slots, sample_period_secs, num_non_vote_transactions)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(sample.slot as i64)
    .bind(sample.num_transactions as i64)
    .bind(sample.num_slots as i64)
    .bind(sample.sample_period_secs as i16)
    .bind(sample.num_non_vote_transactions.unwrap_or(0) as i64)
    .execute(pool.as_ref())
    .await
    .context("Failed to store performance sample")?;

    Ok(())
}
