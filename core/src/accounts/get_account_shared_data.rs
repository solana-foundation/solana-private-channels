use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::AccountsDB},
    redis::AsyncCommands,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        pubkey::Pubkey,
    },
    sqlx::Row,
    std::sync::Arc,
    tracing::{debug, error},
};

pub async fn get_account_shared_data(
    db: &AccountsDB,
    pubkey: &Pubkey,
) -> Option<AccountSharedData> {
    let account = match db {
        AccountsDB::Postgres(postgres_db) => {
            get_account_shared_data_postgres(postgres_db, pubkey).await
        }
        AccountsDB::Redis(redis_db) => get_account_shared_data_redis(redis_db, pubkey).await,
    };
    // A stored row with no lamports describes an account that no longer exists.
    account.filter(|account| account.lamports() != 0)
}

async fn get_account_shared_data_postgres(
    db: &PostgresAccountsDB,
    pubkey: &Pubkey,
) -> Option<AccountSharedData> {
    // Query from database
    let pool = Arc::clone(&db.pool);
    let pubkey_bytes = pubkey.to_bytes();

    match sqlx::query("SELECT data FROM accounts WHERE pubkey = $1")
        .bind(&pubkey_bytes[..])
        .fetch_optional(pool.as_ref())
        .await
    {
        Ok(Some(row)) => {
            let data: Vec<u8> = row.get("data");
            match bincode::deserialize::<AccountSharedData>(&data) {
                Ok(account) => {
                    debug!(
                        "Retrieved account {} with {} lamports",
                        pubkey,
                        account.lamports()
                    );
                    Some(account)
                }
                Err(e) => {
                    error!("Failed to deserialize account {}: {}", pubkey, e);
                    None
                }
            }
        }
        Ok(None) => {
            debug!("Account {} not found", pubkey);
            None
        }
        Err(e) => {
            error!("Failed to read account {}: {}", pubkey, e);
            None
        }
    }
}

async fn get_account_shared_data_redis(
    db: &RedisAccountsDB,
    pubkey: &Pubkey,
) -> Option<AccountSharedData> {
    let mut conn = db.connection.clone();
    let key = format!("account:{}", pubkey);
    let data: redis::RedisResult<Vec<u8>> = conn.get(key).await;
    match data {
        Ok(bytes) => bincode::deserialize(&bytes).ok(),
        Err(e) => {
            error!("Failed to get account {} from Redis: {}", pubkey, e);
            None
        }
    }
}
