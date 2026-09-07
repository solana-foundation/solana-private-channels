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

use super::get_accounts::AccountLoadError;

/// `Some` when the account is stored, `None` when it is genuinely absent, and
/// `Err` when the store could not answer, which is not the same as absence.
pub async fn get_account_shared_data(
    db: &AccountsDB,
    pubkey: &Pubkey,
) -> Result<Option<AccountSharedData>, AccountLoadError> {
    let account = match db {
        AccountsDB::Postgres(postgres_db) => {
            get_account_shared_data_postgres(postgres_db, pubkey).await?
        }
        AccountsDB::Redis(redis_db) => get_account_shared_data_redis(redis_db, pubkey).await?,
    };
    // A stored row with no lamports describes an account that no longer exists.
    Ok(account.filter(|account| account.lamports() != 0))
}

async fn get_account_shared_data_postgres(
    db: &PostgresAccountsDB,
    pubkey: &Pubkey,
) -> Result<Option<AccountSharedData>, AccountLoadError> {
    // Query from database
    let pool = Arc::clone(&db.pool);
    let pubkey_bytes = pubkey.to_bytes();

    // Only the query is retried; a corrupt row is fatal on the first read.
    let row = super::get_accounts::with_retry(|| async {
        sqlx::query("SELECT data FROM accounts WHERE pubkey = $1")
            .bind(&pubkey_bytes[..])
            .fetch_optional(pool.as_ref())
            .await
    })
    .await
    .map_err(|e| {
        error!("Failed to read account {}: {}", pubkey, e);
        AccountLoadError::Backend(e.to_string())
    })?;

    match row {
        Some(row) => {
            let data: Vec<u8> = row.get("data");
            match bincode::deserialize::<AccountSharedData>(&data) {
                Ok(account) => {
                    debug!(
                        "Retrieved account {} with {} lamports",
                        pubkey,
                        account.lamports()
                    );
                    Ok(Some(account))
                }
                Err(e) => {
                    error!("Failed to deserialize account {}: {}", pubkey, e);
                    Err(AccountLoadError::Corrupt(*pubkey))
                }
            }
        }
        None => {
            debug!("Account {} not found", pubkey);
            Ok(None)
        }
    }
}

/// The cache never mints an error of its own: a missing, unreadable, stale or
/// undecodable entry is a miss that Postgres resolves, so an entry written by an
/// older build cannot halt the node. Only the fallback can fail.
async fn get_account_shared_data_redis(
    db: &RedisAccountsDB,
    pubkey: &Pubkey,
) -> Result<Option<AccountSharedData>, AccountLoadError> {
    let key = format!("account:{}", pubkey);
    let cached = match db.get_trusted::<Vec<u8>>(&key).await {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Failed to get account {} from Redis: {}", pubkey, e);
            None
        }
    };

    if let Some(bytes) = cached {
        match bincode::deserialize(&bytes) {
            Ok(account) => return Ok(Some(account)),
            Err(e) => {
                error!("Failed to deserialize cached account {}: {}", pubkey, e);
                // Evict it, or every later read pays both hops to reach the same
                // conclusion. Nothing else would ever remove it.
                let mut conn = db.connection.clone();
                if let Err(e) = conn.del::<_, ()>(&key).await {
                    error!("Failed to evict corrupt cached account {}: {}", pubkey, e);
                }
            }
        }
    }

    // Absent, unreadable or corrupt cache entries are misses, not proof the
    // account does not exist. Resolve them against the source of truth.
    get_account_shared_data_postgres(&db.fallback, pubkey).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::get_accounts::AccountLoadError;
    use crate::test_helpers::{start_test_postgres, start_test_redis};

    /// Bytes too short to be an `AccountSharedData`, so the row is present but
    /// undeserializable, which is what corruption looks like to the reader.
    const CORRUPT_BYTES: &[u8] = &[0u8, 1, 2];

    fn pool_of(db: &AccountsDB) -> Arc<sqlx::PgPool> {
        match db {
            AccountsDB::Postgres(pg) => Arc::clone(&pg.pool),
            AccountsDB::Redis(_) => panic!("test harness is Postgres-backed"),
        }
    }

    async fn insert_corrupt_account(pool: &sqlx::PgPool, pubkey: &Pubkey) {
        sqlx::query(
            "INSERT INTO accounts (pubkey, data) VALUES ($1, $2)
             ON CONFLICT (pubkey) DO UPDATE SET data = $2",
        )
        .bind(&pubkey.to_bytes()[..])
        .bind(CORRUPT_BYTES)
        .execute(pool)
        .await
        .expect("seeding a corrupt account row must succeed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn corrupt_row_is_reported_not_swallowed() {
        let (db, _pg) = start_test_postgres().await;
        let corrupt = Pubkey::new_unique();
        insert_corrupt_account(pool_of(&db).as_ref(), &corrupt).await;

        match get_account_shared_data(&db, &corrupt).await {
            Err(AccountLoadError::Corrupt(key)) => assert_eq!(key, corrupt),
            other => panic!("expected Corrupt({corrupt}), got {other:?}"),
        }
    }

    /// A zero-lamport row is how a deleted account is stored, so it is absence,
    /// not a failure, and must not become an error alongside the real ones.
    #[tokio::test(flavor = "multi_thread")]
    async fn absent_and_zero_lamport_rows_read_as_absent() {
        let (mut db, _pg) = start_test_postgres().await;
        let zeroed = Pubkey::new_unique();
        db.set_account(zeroed, AccountSharedData::new(0, 0, &Pubkey::default()))
            .await;

        assert!(get_account_shared_data(&db, &zeroed)
            .await
            .unwrap()
            .is_none());
        assert!(get_account_shared_data(&db, &Pubkey::new_unique())
            .await
            .unwrap()
            .is_none());
    }

    /// "No owner match" must not be how an unreadable account looks, or a caller
    /// would treat an integrity fault as a routine ownership mismatch.
    #[tokio::test(flavor = "multi_thread")]
    async fn account_matches_owners_surfaces_corruption() {
        let (db, _pg) = start_test_postgres().await;
        let corrupt = Pubkey::new_unique();
        insert_corrupt_account(pool_of(&db).as_ref(), &corrupt).await;

        let result = crate::accounts::account_matches_owners::account_matches_owners(
            &db,
            &corrupt,
            &[Pubkey::new_unique()],
        )
        .await;
        assert!(
            matches!(result, Err(AccountLoadError::Corrupt(key)) if key == corrupt),
            "expected Corrupt({corrupt}), got {result:?}"
        );
    }

    /// A cache entry an older build wrote may be undecodable here. That must
    /// resolve against Postgres and evict the entry, never halt the node.
    #[tokio::test(flavor = "multi_thread")]
    async fn corrupt_cache_entry_falls_through_and_is_evicted() {
        let (postgres_db, _pg) = crate::test_helpers::start_test_postgres_raw().await;
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let pubkey = Pubkey::new_unique();
        let key = format!("account:{}", pubkey);
        let mut conn = redis_raw.connection.clone();
        conn.set::<_, _, ()>(&key, CORRUPT_BYTES).await.unwrap();

        let mut source = AccountsDB::Postgres(postgres_db);
        let account = AccountSharedData::new(1_234, 0, &Pubkey::new_unique());
        source.set_account(pubkey, account).await;

        let db = AccountsDB::Redis(redis_raw);
        let loaded = get_account_shared_data(&db, &pubkey).await.unwrap();
        assert_eq!(loaded.unwrap().lamports(), 1_234);

        let still_cached: Option<Vec<u8>> = conn.get(&key).await.unwrap();
        assert!(still_cached.is_none(), "the corrupt entry must be evicted");
    }

    /// Falling through must not blunt the fallback: a corrupt Postgres row is a
    /// real integrity fault and still surfaces through a healthy cache.
    #[tokio::test(flavor = "multi_thread")]
    async fn corrupt_source_row_surfaces_through_the_cache() {
        let (postgres_db, _pg) = crate::test_helpers::start_test_postgres_raw().await;
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let corrupt = Pubkey::new_unique();
        insert_corrupt_account(postgres_db.pool.as_ref(), &corrupt).await;

        let db = AccountsDB::Redis(redis_raw);
        let result = get_account_shared_data(&db, &corrupt).await;
        assert!(
            matches!(result, Err(AccountLoadError::Corrupt(key)) if key == corrupt),
            "expected Corrupt({corrupt}), got {result:?}"
        );
    }
}
