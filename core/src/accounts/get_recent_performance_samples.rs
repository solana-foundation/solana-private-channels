use {
    super::{postgres::PostgresAccountsDB, traits::AccountsDB},
    anyhow::{Context, Result},
    solana_rpc_client_types::response::RpcPerfSample,
};

pub async fn get_recent_performance_samples(
    db: &AccountsDB,
    limit: usize,
) -> Result<Vec<RpcPerfSample>> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            get_recent_performance_samples_postgres(postgres_db, limit).await
        }
        // Served from the source of truth, and no samples are cached. A cached
        // list would be trimmed to a fixed length and hold only samples written
        // since the cache attached, so a short answer would read as a complete
        // one.
        AccountsDB::Redis(redis_db) => {
            get_recent_performance_samples_postgres(&redis_db.fallback, limit).await
        }
    }
}

async fn get_recent_performance_samples_postgres(
    db: &PostgresAccountsDB,
    limit: usize,
) -> Result<Vec<RpcPerfSample>> {
    let pool = db.pool.clone();

    let samples = sqlx::query_as::<_, (i64, i64, i64, i16, i64)>(
        r#"
        SELECT slot, num_transactions, num_slots, sample_period_secs, num_non_vote_transactions
        FROM performance_samples
        ORDER BY slot DESC
        LIMIT $1
        "#,
    )
    .bind(limit as i64)
    .fetch_all(pool.as_ref())
    .await
    .context("Failed to fetch performance samples")?;

    let performance_samples = samples
        .into_iter()
        .map(
            |(slot, num_transactions, num_slots, sample_period_secs, num_non_vote_transactions)| {
                RpcPerfSample {
                    slot: slot as u64,
                    num_transactions: num_transactions as u64,
                    num_slots: num_slots as u64,
                    sample_period_secs: sample_period_secs as u16,
                    num_non_vote_transactions: Some(num_non_vote_transactions as u64),
                }
            },
        )
        .collect();

    Ok(performance_samples)
}
