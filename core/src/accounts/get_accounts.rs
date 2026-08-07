use {
    super::traits::AccountsDB,
    crate::accounts::{PostgresAccountsDB, RedisAccountsDB},
    redis::{AsyncCommands, RedisResult},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        pubkey::Pubkey,
    },
    sqlx::Row,
    std::sync::Arc,
};

pub async fn get_accounts(db: &AccountsDB, accounts: &[Pubkey]) -> Vec<Option<AccountSharedData>> {
    let mut results = match db {
        AccountsDB::Postgres(postgres_db) => get_accounts_postgres(postgres_db, accounts).await,
        AccountsDB::Redis(redis_db) => get_accounts_redis(redis_db, accounts).await,
    };
    // A stored row with no lamports describes an account that no longer exists.
    // Cleared in place so the result stays positionally aligned with `accounts`.
    for slot in results.iter_mut() {
        if slot.as_ref().is_some_and(|account| account.lamports() == 0) {
            *slot = None;
        }
    }
    results
}

async fn get_accounts_postgres(
    postgres_db: &PostgresAccountsDB,
    accounts: &[Pubkey],
) -> Vec<Option<AccountSharedData>> {
    let pool = Arc::clone(&postgres_db.pool);
    let pubkey_bytes: Vec<Vec<u8>> = accounts.iter().map(|key| key.to_bytes().to_vec()).collect();

    match sqlx::query("SELECT pubkey, data FROM accounts WHERE pubkey = ANY($1)")
        .bind(&pubkey_bytes)
        .fetch_all(pool.as_ref())
        .await
    {
        Ok(rows) => {
            // Initialize result vector with None for all accounts
            let mut result = vec![None; accounts.len()];

            for row in rows {
                let pubkey_bytes: Vec<u8> = row.get("pubkey");
                let data: Vec<u8> = row.get("data");

                // Find the index of this pubkey in the original input
                if let Some(index) = accounts
                    .iter()
                    .position(|&key| key.to_bytes().as_slice() == pubkey_bytes)
                {
                    match bincode::deserialize::<AccountSharedData>(&data) {
                        Ok(account) => result[index] = Some(account),
                        Err(e) => {
                            tracing::error!("Failed to deserialize account data: {}", e);
                        }
                    }
                }
            }
            result
        }
        Err(e) => {
            tracing::error!("Failed to fetch accounts: {}", e);
            vec![None; accounts.len()]
        }
    }
}

async fn get_accounts_redis(
    redis_db: &RedisAccountsDB,
    accounts: &[Pubkey],
) -> Vec<Option<AccountSharedData>> {
    let mut conn = redis_db.connection.clone();
    let keys = accounts
        .iter()
        .map(|key| format!("account:{}", key))
        .collect::<Vec<_>>();
    let data: RedisResult<Vec<Option<Vec<u8>>>> = conn.mget(keys).await;

    match data {
        Ok(results) => results
            .into_iter()
            .map(|opt| opt.and_then(|bytes| bincode::deserialize(&bytes).ok()))
            .collect(),
        Err(_) => vec![None; accounts.len()],
    }
}
