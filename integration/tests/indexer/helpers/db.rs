#![allow(dead_code)]

use private_channel_indexer::storage::common::amount::TokenAmount;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct DbTransaction {
    pub signature: String,
    pub slot: i64,
    pub initiator: String,
    pub recipient: String,
    pub mint: String,
    pub amount: TokenAmount,
    pub transaction_type: String,
    pub status: String,
    pub counterpart_signature: Option<String>,
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub withdrawal_nonce: Option<i64>,
}

type TransactionRow = (
    String,
    i64,
    String,
    String,
    String,
    TokenAmount,
    String,
    String,
    Option<String>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<i64>,
);

fn row_to_db_transaction(row: TransactionRow) -> DbTransaction {
    DbTransaction {
        signature: row.0,
        slot: row.1,
        initiator: row.2,
        recipient: row.3,
        mint: row.4,
        amount: row.5,
        transaction_type: row.6,
        status: row.7,
        counterpart_signature: row.8,
        processed_at: row.9,
        withdrawal_nonce: row.10,
    }
}

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPool::connect(database_url).await
}

async fn count_transactions_with_filter(
    pool: &PgPool,
    filter_clause: Option<&str>,
    bind_value: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let query = if let Some(clause) = filter_clause {
        format!("SELECT COUNT(*) FROM transactions WHERE {}", clause)
    } else {
        "SELECT COUNT(*) FROM transactions".to_string()
    };

    let mut query_builder = sqlx::query_as::<_, (i64,)>(&query);
    if let Some(value) = bind_value {
        query_builder = query_builder.bind(value);
    }

    let row = query_builder.fetch_one(pool).await?;
    Ok(row.0)
}

pub async fn count_transactions_by_type(pool: &PgPool, tx_type: &str) -> Result<i64, sqlx::Error> {
    count_transactions_with_filter(
        pool,
        Some("transaction_type = $1::transaction_type"),
        Some(tx_type),
    )
    .await
}

pub async fn count_transactions_by_status(pool: &PgPool, status: &str) -> Result<i64, sqlx::Error> {
    count_transactions_with_filter(pool, Some("status = $1::transaction_status"), Some(status))
        .await
}

pub async fn count_transactions(pool: &PgPool) -> Result<i64, sqlx::Error> {
    count_transactions_with_filter(pool, None, None).await
}

async fn wait_for_count_with_filter(
    pool: &PgPool,
    expected: i64,
    timeout_secs: u64,
    filter_clause: Option<&str>,
    bind_value: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let start = std::time::Instant::now();

    while start.elapsed().as_secs() < timeout_secs {
        let count = count_transactions_with_filter(pool, filter_clause, bind_value).await?;
        if count >= expected {
            return Ok(true);
        }
        // 200 ms granularity to detect completion sooner.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    Ok(false)
}

pub async fn wait_for_processed_count(
    pool: &PgPool,
    expected: i64,
    timeout_secs: u64,
) -> Result<bool, sqlx::Error> {
    wait_for_count_with_filter(
        pool,
        expected,
        timeout_secs,
        Some("status = $1::transaction_status"),
        Some("completed"),
    )
    .await
}

pub async fn wait_for_count(
    pool: &PgPool,
    expected: i64,
    timeout_secs: u64,
) -> Result<bool, sqlx::Error> {
    wait_for_count_with_filter(pool, expected, timeout_secs, None, None).await
}

pub async fn wait_for_checkpoint(
    pool: &PgPool,
    program_type: &str,
    target_slot: u64,
    timeout_secs: u64,
) -> Result<bool, sqlx::Error> {
    let start = std::time::Instant::now();

    while start.elapsed().as_secs() < timeout_secs {
        if let Some(checkpoint_slot) = get_checkpoint_slot(pool, program_type).await? {
            if checkpoint_slot >= target_slot {
                return Ok(true);
            }
        }
        // 200 ms granularity to detect completion sooner.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    Ok(false)
}

pub async fn get_checkpoint_slot(
    pool: &PgPool,
    program_type: &str,
) -> Result<Option<u64>, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
            .bind(program_type)
            .fetch_optional(pool)
            .await?;

    Ok(row.map(|(slot,)| slot as u64))
}

pub async fn get_max_slot_from_transactions(pool: &PgPool) -> Result<Option<u64>, sqlx::Error> {
    let row: Option<(Option<i64>,)> = sqlx::query_as("SELECT MAX(slot) FROM transactions")
        .fetch_optional(pool)
        .await?;

    Ok(row.and_then(|(slot,)| slot.map(|s| s as u64)))
}

pub async fn get_transaction(
    pool: &PgPool,
    signature: &str,
) -> Result<Option<DbTransaction>, sqlx::Error> {
    let row: Option<TransactionRow> = sqlx::query_as(
        "SELECT signature, slot, initiator, recipient, mint, amount, transaction_type::text, status::text, counterpart_signature, processed_at, withdrawal_nonce FROM transactions WHERE signature = $1",
    )
    .bind(signature)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(row_to_db_transaction))
}

pub async fn get_processed_transactions(pool: &PgPool) -> Result<Vec<DbTransaction>, sqlx::Error> {
    let rows: Vec<TransactionRow> = sqlx::query_as(
        "SELECT signature, slot, initiator, recipient, mint, amount, transaction_type::text, status::text, counterpart_signature, processed_at, withdrawal_nonce FROM transactions WHERE status = 'completed'::transaction_status ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(row_to_db_transaction).collect())
}
