use {
    anyhow::Result,
    solana_sdk::{account::AccountSharedData, pubkey::Pubkey},
    solana_svm_callback::{InvokeContextCallback, TransactionProcessingCallback},
    sqlx::{postgres::PgPoolOptions, PgPool},
    std::sync::Arc,
    tracing::{debug, info},
};

/// Default pool size. Needs headroom so the settler's BEGIN…COMMIT doesn't
/// starve the executor's concurrent account-read callbacks on the same pool.
pub(crate) const DEFAULT_PG_MAX_CONNECTIONS: u32 = 32;

/// Hard ceiling — prevents a fat-fingered env var from exhausting Postgres'
/// default `max_connections = 100` across co-located services.
pub(crate) const MAX_PG_MAX_CONNECTIONS: u32 = 256;

/// Reads `PRIVATE_CHANNEL_PG_MAX_CONNECTIONS`; falls back to the default on unset/empty/
/// unparseable/zero; clamps above the ceiling.
pub(crate) fn resolve_pool_size() -> u32 {
    std::env::var("PRIVATE_CHANNEL_PG_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|&n| n > 0)
        .map(|n| n.min(MAX_PG_MAX_CONNECTIONS))
        .unwrap_or(DEFAULT_PG_MAX_CONNECTIONS)
}

#[derive(Clone)]
pub struct PostgresAccountsDB {
    pub pool: Arc<PgPool>,
    pub read_only: bool,
}

/// Returns true when the database URL carries a non-empty password component.
fn database_url_has_password(database_url: &str) -> bool {
    match url::Url::parse(database_url) {
        // None (no password) and Some("") (blanked secret) are both missing credentials.
        Ok(parsed) => parsed.password().is_some_and(|p| !p.is_empty()),
        // Leave unparseable URLs for sqlx to reject on connect.
        Err(_) => true,
    }
}

impl PostgresAccountsDB {
    pub async fn new(
        database_url: &str,
        read_only: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Fail closed: reject a blank password before connecting (blanked env templates interpolate an empty ${POSTGRES_PASSWORD} into a passwordless URL).
        if !database_url_has_password(database_url) {
            return Err(
                "database_url password component is empty; set a non-empty POSTGRES_PASSWORD"
                    .into(),
            );
        }

        // Parse URL to extract host/port without credentials for logging.
        let sanitized_url = if let Ok(parsed) = url::Url::parse(database_url) {
            let host = parsed.host_str().unwrap_or("unknown");
            let port = parsed.port().unwrap_or(5432);
            let db = parsed.path().trim_start_matches('/');
            format!("{}:{}/{}", host, port, db)
        } else {
            "unknown".to_string()
        };
        info!("Connecting to PostgreSQL: {}", sanitized_url);

        let max_connections = resolve_pool_size();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .idle_timeout(std::time::Duration::from_secs(60))
            .connect(database_url)
            .await?;

        info!(max_connections, "Connected to PostgreSQL");

        if !read_only {
            info!("Creating PostgreSQL tables");
            create_tables(&pool).await?;
        } else {
            info!("Skipping table creation in read-only mode");
        }

        let instance = Self {
            pool: Arc::new(pool),
            read_only,
        };

        info!("PostgreSQL accounts database initialized");
        Ok(instance)
    }
}

impl InvokeContextCallback for PostgresAccountsDB {}

impl TransactionProcessingCallback for PostgresAccountsDB {
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        let db = super::traits::AccountsDB::Postgres(self.clone());
        let pubkey = *pubkey;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                super::get_account_shared_data::get_account_shared_data(&db, &pubkey).await
            })
        })
    }

    fn account_matches_owners(&self, account: &Pubkey, owners: &[Pubkey]) -> Option<usize> {
        let db = super::traits::AccountsDB::Postgres(self.clone());
        let account = *account;
        let owners = owners.to_vec();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                super::account_matches_owners::account_matches_owners(&db, &account, &owners).await
            })
        })
    }
}

impl Drop for PostgresAccountsDB {
    fn drop(&mut self) {
        debug!("Closing PostgreSQL connection pool");
        // Connection pool will be closed automatically when Arc<PgPool> is dropped
    }
}

async fn create_tables(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Create tables
    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS accounts (
                pubkey BYTEA PRIMARY KEY,
                data BYTEA NOT NULL
            )
            "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS transactions (
                signature BYTEA PRIMARY KEY,
                data BYTEA NOT NULL
            )
            "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS blocks (
                slot BIGINT PRIMARY KEY,
                data BYTEA NOT NULL
            )
            "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value BYTEA NOT NULL
            )
            "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS performance_samples (
                slot BIGINT PRIMARY KEY,
                num_transactions BIGINT NOT NULL,
                num_slots BIGINT NOT NULL,
                sample_period_secs SMALLINT NOT NULL,
                num_non_vote_transactions BIGINT NOT NULL
            )
            "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS address_signatures (
                address   BYTEA  NOT NULL,
                slot      BIGINT NOT NULL,
                signature BYTEA  NOT NULL,
                PRIMARY KEY (address, slot, signature)
            )
            "#,
    )
    .execute(pool)
    .await?;

    info!("PostgreSQL tables initialized");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{
        postgres_container_url, start_test_postgres, start_test_postgres_raw,
        start_test_postgres_with_new_instance,
    };
    use serial_test::serial;
    use solana_sdk::account::{AccountSharedData, ReadableAccount};
    use solana_sdk::pubkey::Pubkey;
    use solana_svm_callback::TransactionProcessingCallback;

    const ENV_VAR: &str = "PRIVATE_CHANNEL_PG_MAX_CONNECTIONS";

    /// Snapshot the env var, run `body`, restore. `serial_test` prevents
    /// concurrent tests from racing on the shared process env.
    fn with_env_var<F: FnOnce()>(value: Option<&str>, body: F) {
        let original = std::env::var(ENV_VAR).ok();
        match value {
            Some(v) => std::env::set_var(ENV_VAR, v),
            None => std::env::remove_var(ENV_VAR),
        }
        body();
        match original {
            Some(v) => std::env::set_var(ENV_VAR, v),
            None => std::env::remove_var(ENV_VAR),
        }
    }

    /// Unset → default (pins the documented value against silent drift).
    #[test]
    #[serial]
    fn resolve_pool_size_defaults_when_unset() {
        with_env_var(None, || {
            assert_eq!(resolve_pool_size(), DEFAULT_PG_MAX_CONNECTIONS);
            assert_eq!(DEFAULT_PG_MAX_CONNECTIONS, 32);
        });
    }

    /// Valid u32 is honored verbatim.
    #[test]
    #[serial]
    fn resolve_pool_size_parses_valid_value() {
        with_env_var(Some("64"), || {
            assert_eq!(resolve_pool_size(), 64);
        });
    }

    /// Non-numeric → default (no panic).
    #[test]
    #[serial]
    fn resolve_pool_size_invalid_value_falls_back_to_default() {
        with_env_var(Some("not-a-number"), || {
            assert_eq!(resolve_pool_size(), DEFAULT_PG_MAX_CONNECTIONS);
        });
    }

    /// Empty string → default.
    #[test]
    #[serial]
    fn resolve_pool_size_empty_value_falls_back_to_default() {
        with_env_var(Some(""), || {
            assert_eq!(resolve_pool_size(), DEFAULT_PG_MAX_CONNECTIONS);
        });
    }

    /// Negative → default (fails u32 parse).
    #[test]
    #[serial]
    fn resolve_pool_size_negative_value_falls_back_to_default() {
        with_env_var(Some("-1"), || {
            assert_eq!(resolve_pool_size(), DEFAULT_PG_MAX_CONNECTIONS);
        });
    }

    /// 0 → default. Parses as u32 but would crash sqlx (min=1 > max=0).
    #[test]
    #[serial]
    fn resolve_pool_size_zero_falls_back_to_default() {
        with_env_var(Some("0"), || {
            assert_eq!(resolve_pool_size(), DEFAULT_PG_MAX_CONNECTIONS);
        });
    }

    /// Above ceiling → clamped to `MAX_PG_MAX_CONNECTIONS`.
    #[test]
    #[serial]
    fn resolve_pool_size_above_ceiling_is_clamped() {
        with_env_var(Some("100000"), || {
            assert_eq!(resolve_pool_size(), MAX_PG_MAX_CONNECTIONS);
        });
    }

    /// Exactly at ceiling → preserved (off-by-one guard).
    #[test]
    #[serial]
    fn resolve_pool_size_at_ceiling_is_preserved() {
        let ceiling = MAX_PG_MAX_CONNECTIONS.to_string();
        with_env_var(Some(&ceiling), || {
            assert_eq!(resolve_pool_size(), MAX_PG_MAX_CONNECTIONS);
        });
    }

    /// PostgresAccountsDB::new with read_only=false must create all tables.
    /// Calling it twice must not fail (IF NOT EXISTS idempotency).
    #[tokio::test(flavor = "multi_thread")]
    async fn new_write_mode_creates_tables_idempotently() {
        let (first, _second, _pg) = start_test_postgres_with_new_instance().await;
        // Verify tables actually exist by querying them
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM accounts")
            .fetch_one(first.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(count.0, 0, "accounts table should exist and be empty");
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
            .fetch_one(first.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(count.0, 0, "blocks table should exist and be empty");
    }

    /// The synchronous TransactionProcessingCallback::get_account_shared_data
    /// returns None for an account that was never stored.
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_get_account_shared_data_missing_returns_none() {
        let (db, _pg) = start_test_postgres_raw().await;
        let result = db.get_account_shared_data(&Pubkey::new_unique());
        assert!(result.is_none());
    }

    /// The synchronous TransactionProcessingCallback::account_matches_owners
    /// returns None when the account does not exist.
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_account_matches_owners_missing_returns_none() {
        let (db, _pg) = start_test_postgres_raw().await;
        let result = db.account_matches_owners(&Pubkey::new_unique(), &[Pubkey::new_unique()]);
        assert!(result.is_none());
    }

    /// Store an account via the production set_account path and read back
    /// via the synchronous TransactionProcessingCallback.
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_get_account_shared_data_after_set_returns_some() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let account = AccountSharedData::new(42, 0, &owner);

        // Use the production write path
        db.set_account(pubkey, account).await;

        // Read back via the synchronous TransactionProcessingCallback on the inner Postgres DB.
        let pg_db = match &db {
            crate::accounts::AccountsDB::Postgres(pg) => pg,
            _ => panic!("expected Postgres variant"),
        };
        let result = pg_db.get_account_shared_data(&pubkey);
        assert!(
            result.is_some(),
            "account stored via set_account should be retrievable via sync callback"
        );
    }

    /// PostgresAccountsDB::new with invalid URL formats sanitizes gracefully
    /// and logs "unknown" for unparseable URLs.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_with_invalid_url_format_logs_unknown() {
        let invalid_url = "not-a-valid-url-at-all";
        let result = PostgresAccountsDB::new(invalid_url, false).await;
        assert!(result.is_err(), "Invalid URL should fail to connect");
    }

    /// database_url_has_password rejects blank/missing passwords and accepts real ones.
    #[test]
    fn password_guard_classifies_urls() {
        // Blanked secret interpolates to an empty password and must be rejected.
        assert!(!database_url_has_password(
            "postgres://user:@localhost:5432/db"
        ));
        // No password component at all must be rejected.
        assert!(!database_url_has_password(
            "postgres://user@localhost:5432/db"
        ));
        // No userinfo at all must be rejected.
        assert!(!database_url_has_password("postgres://localhost:5432/db"));
        // A real password is accepted.
        assert!(database_url_has_password(
            "postgres://user:secret@localhost:5432/db"
        ));
        // A percent-encoded password (here '@' as %40) is still a real, non-empty password.
        assert!(database_url_has_password(
            "postgres://user:p%40ss@localhost:5432/db"
        ));
        // Unparseable URLs pass the guard so sqlx surfaces the real connect error.
        assert!(database_url_has_password("not-a-valid-url"));
    }

    /// PostgresAccountsDB::new must fail closed on a blank password before connecting.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_rejects_empty_password_url() {
        let empty_pw = PostgresAccountsDB::new("postgres://user:@localhost:5432/db", false).await;
        assert!(empty_pw.is_err(), "empty password must be rejected");
        assert!(empty_pw
            .err()
            .unwrap()
            .to_string()
            .contains("password component is empty"));
        let no_pw = PostgresAccountsDB::new("postgres://user@localhost:5432/db", false).await;
        assert!(no_pw.is_err(), "missing password must be rejected");
    }

    /// Store an account via the production path and verify account_matches_owners.
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_account_matches_owners_returns_some_when_found() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let account = AccountSharedData::new(100, 0, &owner);

        // Use production write path
        db.set_account(pubkey, account).await;

        let pg_db = match &db {
            crate::accounts::AccountsDB::Postgres(pg) => pg,
            _ => panic!("expected Postgres variant"),
        };

        // Query with the matching owner
        let result = pg_db.account_matches_owners(&pubkey, &[owner]);
        assert_eq!(result, Some(0));

        // Query with owner in second position
        let other_owner = Pubkey::new_unique();
        let result = pg_db.account_matches_owners(&pubkey, &[other_owner, owner]);
        assert_eq!(result, Some(1));
    }

    /// Store an account via the production path and verify deserialized data.
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_get_account_shared_data_deserializes_correctly() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let lamports = 5000;
        let account = AccountSharedData::new(lamports, 0, &owner);

        // Use production write path
        db.set_account(pubkey, account).await;

        let pg_db = match &db {
            crate::accounts::AccountsDB::Postgres(pg) => pg,
            _ => panic!("expected Postgres variant"),
        };
        let result = pg_db.get_account_shared_data(&pubkey);
        assert!(result.is_some());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.lamports(), lamports, "Lamports should match");
        assert_eq!(retrieved.owner(), &owner, "Owner should match");
    }

    /// account_matches_owners with empty owners list returns None (no match possible).
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_account_matches_owners_empty_owners() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let account = AccountSharedData::new(100, 0, &Pubkey::new_unique());
        db.set_account(pubkey, account).await;

        let pg_db = match &db {
            crate::accounts::AccountsDB::Postgres(pg) => pg,
            _ => panic!("expected Postgres variant"),
        };
        let result = pg_db.account_matches_owners(&pubkey, &[]);
        assert!(result.is_none(), "empty owners list should never match");
    }

    /// PostgresAccountsDB::new with read_only=true skips table creation.
    /// Validates that tables already created by the write-mode initialization remain intact.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_read_only_mode_skips_table_creation() {
        let (_db, container) = start_test_postgres_raw().await;
        let url = postgres_container_url(&container, "pg_test").await;

        // Connect in read-only mode — tables already exist from start_test_postgres_raw
        let db = PostgresAccountsDB::new(&url, true).await.unwrap();
        assert!(db.read_only, "read_only flag should be set");

        // Tables should still exist (weren't dropped)
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM accounts")
            .fetch_one(db.pool.as_ref())
            .await
            .unwrap();
        assert_eq!(count.0, 0, "accounts table should exist and be empty");
    }

    /// Account exists in database but no queried owner matches.
    /// Validates the None return path when account is found but owner mismatch occurs.
    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_callback_account_matches_owners_no_match_existing_account() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        db.set_account(pubkey, AccountSharedData::new(100, 0, &owner))
            .await;

        let pg_db = match &db {
            crate::accounts::AccountsDB::Postgres(pg) => pg,
            _ => panic!("expected Postgres variant"),
        };

        // Account exists but wrong owner — should return None
        let result = pg_db.account_matches_owners(&pubkey, &[Pubkey::new_unique()]);
        assert!(result.is_none(), "no owner match should return None");
    }
}
