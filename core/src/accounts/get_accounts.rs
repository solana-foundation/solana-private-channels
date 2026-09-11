use {
    super::traits::AccountsDB,
    crate::accounts::{PostgresAccountsDB, RedisAccountsDB},
    redis::{AsyncCommands, RedisResult},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        pubkey::Pubkey,
    },
    sqlx::Row,
    std::{fmt::Display, future::Future, sync::Arc, time::Duration},
};

/// Why an account read produced no answer, kept apart from a genuinely absent
/// account so a failure is never settled as if the account did not exist.
#[derive(Debug, Clone)]
pub enum AccountLoadError {
    /// The query itself failed and did not recover within the retry schedule.
    Backend(String),
    /// A stored row is present but will not deserialize. Retrying cannot change
    /// the bytes, so this is an integrity fault and is fatal on the first read.
    Corrupt(Pubkey),
}

impl Display for AccountLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountLoadError::Backend(msg) => {
                write!(f, "account store read failed after retries: {}", msg)
            }
            AccountLoadError::Corrupt(pubkey) => {
                write!(f, "stored account {} could not be deserialized", pubkey)
            }
        }
    }
}

impl std::error::Error for AccountLoadError {}

/// Bounded retry for a whole-query failure. A load error stops the node, so the
/// schedule only has to outlast a blip such as a failover or a recycled
/// connection; a longer outage is handled better by a restart than by a stall.
const LOAD_MAX_ATTEMPTS: u32 = 4;
const LOAD_BACKOFF_BASE_MS: u64 = 20;
const LOAD_BACKOFF_CAP_MS: u64 = 500;
/// Ceiling on the whole retry sequence. The attempt count alone does not bound
/// it: acquiring a pooled connection can itself block for the pool's acquire
/// timeout, so without this a dead database would stall a batch for minutes.
const LOAD_TOTAL_BUDGET_MS: u64 = 5_000;

#[cfg(not(test))]
fn retry_params() -> (u32, u64) {
    (LOAD_MAX_ATTEMPTS, LOAD_BACKOFF_BASE_MS)
}

#[cfg(not(test))]
fn load_total_budget_ms() -> u64 {
    LOAD_TOTAL_BUDGET_MS
}

#[cfg(test)]
static TEST_BUDGET_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(LOAD_TOTAL_BUDGET_MS);

#[cfg(test)]
fn load_total_budget_ms() -> u64 {
    TEST_BUDGET_MS.load(std::sync::atomic::Ordering::Relaxed)
}

// Tests shrink the schedule so a dead-backend case returns without a real wait.
#[cfg(test)]
static TEST_MAX_ATTEMPTS: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(LOAD_MAX_ATTEMPTS);
#[cfg(test)]
static TEST_BASE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(LOAD_BACKOFF_BASE_MS);

#[cfg(test)]
fn retry_params() -> (u32, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (TEST_MAX_ATTEMPTS.load(Relaxed), TEST_BASE_MS.load(Relaxed))
}

/// The override is process-global. `#[serial_test::serial]` only orders tests
/// that also carry it, so every test in a module that calls this must be marked,
/// not just the ones that read the schedule back.
#[cfg(test)]
pub(crate) fn set_test_retry(attempts: u32, base_ms: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    TEST_MAX_ATTEMPTS.store(attempts, Relaxed);
    TEST_BASE_MS.store(base_ms, Relaxed);
}

#[cfg(test)]
pub(crate) fn set_test_budget_ms(budget_ms: u64) {
    TEST_BUDGET_MS.store(budget_ms, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn reset_test_retry() {
    set_test_retry(LOAD_MAX_ATTEMPTS, LOAD_BACKOFF_BASE_MS);
    set_test_budget_ms(LOAD_TOTAL_BUDGET_MS);
}

/// Whether a failed query could plausibly succeed if tried again. Transport and
/// pool failures always could; a server-side error only if its SQLSTATE names a
/// condition that clears on its own.
fn is_transient(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(db) => db.code().is_some_and(|code| is_transient_sqlstate(&code)),
        // A missing table or a rejected permission answers the same way every time.
        _ => false,
    }
}

/// SQLSTATE classes that describe a condition outside the query itself: 08 is a
/// broken connection, 53 the server running out of a resource, 57 an operator
/// action such as a shutdown. The two rollbacks come from a concurrent writer.
fn is_transient_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || code.starts_with("57")
        || matches!(code, "40001" | "40P01")
}

/// Exponential backoff for a 1-based attempt number, capped so the worst-case
/// wait stays bounded no matter how large the base is.
fn backoff_delay(attempt: u32, base_ms: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    let ms = base_ms
        .saturating_mul(1u64 << shift)
        .min(LOAD_BACKOFF_CAP_MS);
    Duration::from_millis(ms)
}

/// Retry `op` up to `attempts` times, awaiting `sleep` between tries. The
/// sleeper is a parameter so tests can assert the schedule with no wall clock.
async fn with_retry_using_sleep<T, E, F, Fut, S, SFut, R>(
    attempts: u32,
    base_ms: u64,
    mut op: F,
    mut sleep: S,
    retryable: R,
) -> Result<T, E>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    S: FnMut(Duration) -> SFut,
    SFut: Future<Output = ()>,
    R: Fn(&E) -> bool,
{
    let max = attempts.max(1);
    let mut attempt = 1u32;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if attempt >= max || !retryable(&e) {
                    return Err(e);
                }
                let delay = backoff_delay(attempt, base_ms);
                tracing::warn!(
                    "account load attempt {}/{} failed: {}; retrying in {:?}",
                    attempt,
                    max,
                    e,
                    delay
                );
                sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

/// Production wrapper: the configured schedule, a real tokio sleep, and a hard
/// ceiling on total elapsed time. Nothing is armed or allocated unless an
/// attempt has already failed, so a healthy read pays only the closure call.
pub(super) async fn with_retry<T, F, Fut>(op: F) -> Result<T, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    let (attempts, base_ms) = retry_params();
    let budget = Duration::from_millis(load_total_budget_ms());
    match tokio::time::timeout(
        budget,
        with_retry_using_sleep(attempts, base_ms, op, tokio::time::sleep, is_transient),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(sqlx::Error::PoolTimedOut),
    }
}

/// `Some` per stored account, `None` per genuinely absent one, and `Err` when
/// the store could not answer at all, so a failure is never read as absence.
pub async fn get_accounts(
    db: &AccountsDB,
    accounts: &[Pubkey],
) -> Result<Vec<Option<AccountSharedData>>, AccountLoadError> {
    let mut results = match db {
        AccountsDB::Postgres(postgres_db) => get_accounts_postgres(postgres_db, accounts).await?,
        AccountsDB::Redis(redis_db) => get_accounts_redis(redis_db, accounts).await?,
    };
    // A stored row with no lamports describes an account that no longer exists.
    // Cleared in place so the result stays positionally aligned with `accounts`.
    for slot in results.iter_mut() {
        if slot.as_ref().is_some_and(|account| account.lamports() == 0) {
            *slot = None;
        }
    }
    Ok(results)
}

/// Bytes bincode writes around an account's data: lamports, the data length
/// prefix, owner, executable and rent epoch. Pinned by a test, since the size
/// query reads the stored row length and has to subtract this to get data len.
pub const ACCOUNT_SERIALIZED_HEADER_BYTES: usize = 57;

/// Stored data length per account, aligned with `accounts`, zero when absent.
/// Sizes are asked for before bytes because `octet_length` reads the row's TOAST
/// pointer without fetching the blob, so nothing oversized has to be loaded.
pub async fn get_account_data_sizes(
    db: &AccountsDB,
    accounts: &[Pubkey],
) -> Result<Vec<usize>, AccountLoadError> {
    match db {
        AccountsDB::Postgres(postgres_db) => get_account_data_sizes_postgres(postgres_db, accounts),
        // Redis has no cheap length primitive, so sizes come from the source of truth.
        AccountsDB::Redis(redis_db) => {
            get_account_data_sizes_postgres(&redis_db.fallback, accounts)
        }
    }
    .await
}

async fn get_account_data_sizes_postgres(
    postgres_db: &PostgresAccountsDB,
    accounts: &[Pubkey],
) -> Result<Vec<usize>, AccountLoadError> {
    let mut result = vec![0usize; accounts.len()];
    if accounts.is_empty() {
        return Ok(result);
    }

    let pool = Arc::clone(&postgres_db.pool);
    let pubkey_bytes: Vec<Vec<u8>> = accounts.iter().map(|key| key.to_bytes().to_vec()).collect();

    let rows = with_retry(|| async {
        sqlx::query("SELECT pubkey, octet_length(data) AS len FROM accounts WHERE pubkey = ANY($1)")
            .bind(&pubkey_bytes)
            .fetch_all(pool.as_ref())
            .await
    })
    .await
    .map_err(|e| AccountLoadError::Backend(e.to_string()))?;

    for row in rows {
        let row_pubkey: Vec<u8> = row.get("pubkey");
        let stored_len: i32 = row.get("len");
        // Saturating because a short row is corrupt, and preload reports that.
        let data_len = (stored_len.max(0) as usize).saturating_sub(ACCOUNT_SERIALIZED_HEADER_BYTES);
        // Every position, not just the first: a key asked for twice must size the
        // same both times, or a repeat would read as a free account.
        for (index, key) in accounts.iter().enumerate() {
            if key.to_bytes().as_slice() == row_pubkey {
                result[index] = data_len;
            }
        }
    }
    Ok(result)
}

async fn get_accounts_postgres(
    postgres_db: &PostgresAccountsDB,
    accounts: &[Pubkey],
) -> Result<Vec<Option<AccountSharedData>>, AccountLoadError> {
    let pool = Arc::clone(&postgres_db.pool);
    let pubkey_bytes: Vec<Vec<u8>> = accounts.iter().map(|key| key.to_bytes().to_vec()).collect();

    // Only the whole query is retried; a corrupt row below is fatal on sight.
    let rows = with_retry(|| async {
        sqlx::query("SELECT pubkey, data FROM accounts WHERE pubkey = ANY($1)")
            .bind(&pubkey_bytes)
            .fetch_all(pool.as_ref())
            .await
    })
    .await
    .map_err(|e| AccountLoadError::Backend(e.to_string()))?;

    // Keys with no row stay None; a row that will not deserialize is an error,
    // never a silent skip, which the caller could not tell from absence.
    let mut result = vec![None; accounts.len()];
    for row in rows {
        let row_pubkey: Vec<u8> = row.get("pubkey");
        let data: Vec<u8> = row.get("data");

        if let Some(index) = accounts
            .iter()
            .position(|&key| key.to_bytes().as_slice() == row_pubkey)
        {
            match bincode::deserialize::<AccountSharedData>(&data) {
                Ok(account) => result[index] = Some(account),
                Err(e) => {
                    tracing::error!("Failed to deserialize account {}: {}", accounts[index], e);
                    return Err(AccountLoadError::Corrupt(accounts[index]));
                }
            }
        }
    }
    Ok(result)
}

/// The cache never mints an error of its own: an absent, unreadable, stale or
/// undecodable entry is a miss that Postgres resolves, so an entry written by an
/// older build cannot halt the node. Only the fallback can fail.
async fn get_accounts_redis(
    redis_db: &RedisAccountsDB,
    accounts: &[Pubkey],
) -> Result<Vec<Option<AccountSharedData>>, AccountLoadError> {
    // MGET with no keys is a Redis error, and there is nothing to resolve.
    if accounts.is_empty() {
        return Ok(Vec::new());
    }

    let mut conn = redis_db.connection.clone();
    // The deployment stamp rides along as the first key, so the same round trip
    // that fetches the values also proves the cache may still be read from.
    let mut keys = Vec::with_capacity(accounts.len() + 1);
    keys.push(crate::accounts::redis_coherence::DEPLOYMENT_ID_KEY.to_string());
    keys.extend(accounts.iter().map(|key| format!("account:{}", key)));
    let data: RedisResult<Vec<Option<Vec<u8>>>> = conn.mget(keys).await;

    let mut results: Vec<Option<AccountSharedData>> = match data {
        // MGET answers one entry per key, so the split always succeeds; a short
        // reply is treated as an untrusted cache rather than indexed into, since
        // this runs under an RPC request where a panic is far worse than a miss.
        Ok(cached) if cached.len() == accounts.len() + 1 => {
            let (stamp, values) = cached.split_first().expect("checked non-empty above");
            if redis_db.stamp_is_current(stamp.as_ref()) {
                values
                    .iter()
                    .map(|opt| {
                        opt.as_ref()
                            .and_then(|bytes| bincode::deserialize(bytes).ok())
                    })
                    .collect()
            } else {
                // Condemned or foreign cache: every position is a miss.
                vec![None; accounts.len()]
            }
        }
        Ok(cached) => {
            tracing::error!(
                "Redis returned {} values for {} keys; treating the cache as unusable",
                cached.len(),
                accounts.len() + 1
            );
            vec![None; accounts.len()]
        }
        Err(e) => {
            tracing::error!("Failed to read accounts from Redis: {}", e);
            vec![None; accounts.len()]
        }
    };

    // Every position the cache could not answer is a miss. Resolve just those
    // against the source of truth, keeping the result aligned with `accounts`.
    let missing: Vec<usize> = results
        .iter()
        .enumerate()
        .filter(|(_, account)| account.is_none())
        .map(|(position, _)| position)
        .collect();
    if missing.is_empty() {
        return Ok(results);
    }

    let missing_pubkeys: Vec<Pubkey> = missing.iter().map(|&position| accounts[position]).collect();
    let resolved = get_accounts_postgres(&redis_db.fallback, &missing_pubkeys).await?;
    for (position, account) in missing.into_iter().zip(resolved) {
        results[position] = account;
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{
        dead_postgres_db, start_test_postgres, start_test_postgres_raw, start_test_redis,
    };
    use solana_sdk::account::ReadableAccount;
    use std::{
        cell::RefCell,
        sync::atomic::{AtomicU32, Ordering},
    };

    /// Bytes too short to be an `AccountSharedData`, so the row is present but
    /// undeserializable, which is what corruption looks like to the reader.
    const CORRUPT_BYTES: &[u8] = &[0u8, 1, 2];

    fn pool_of(db: &AccountsDB) -> Arc<sqlx::PgPool> {
        match db {
            AccountsDB::Postgres(pg) => Arc::clone(&pg.pool),
            AccountsDB::Redis(_) => panic!("test harness is Postgres-backed"),
        }
    }

    async fn insert_corrupt_account(db: &AccountsDB, pubkey: &Pubkey) {
        sqlx::query(
            "INSERT INTO accounts (pubkey, data) VALUES ($1, $2)
             ON CONFLICT (pubkey) DO UPDATE SET data = $2",
        )
        .bind(&pubkey.to_bytes()[..])
        .bind(CORRUPT_BYTES)
        .execute(pool_of(db).as_ref())
        .await
        .expect("seeding a corrupt account row must succeed");
    }

    /// A sleeper that records what it was asked to wait, so the backoff schedule
    /// is asserted without any wall-clock delay.
    fn recording_sleep(
        log: &RefCell<Vec<Duration>>,
    ) -> impl FnMut(Duration) -> std::future::Ready<()> + '_ {
        move |delay| {
            log.borrow_mut().push(delay);
            std::future::ready(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_retry_stops_at_first_success() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, String> = with_retry_using_sleep(
            5,
            1,
            || async {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                if n < 3 {
                    Err("transient".to_string())
                } else {
                    Ok(n)
                }
            },
            |_| std::future::ready(()),
            |_| true,
        )
        .await;

        assert_eq!(result.unwrap(), 3);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_retry_is_bounded_by_max_attempts() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, String> = with_retry_using_sleep(
            LOAD_MAX_ATTEMPTS,
            1,
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err("dead".to_string())
            },
            |_| std::future::ready(()),
            |_| true,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), LOAD_MAX_ATTEMPTS);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_retry_backoff_is_capped_and_non_decreasing() {
        let log = RefCell::new(Vec::new());
        let result: Result<u32, String> = with_retry_using_sleep(
            LOAD_MAX_ATTEMPTS,
            // A base far above the cap proves the cap, not the base, bounds the wait.
            LOAD_BACKOFF_CAP_MS * 4,
            || async { Err("dead".to_string()) },
            recording_sleep(&log),
            |_| true,
        )
        .await;
        assert!(result.is_err());

        let delays = log.into_inner();
        assert_eq!(delays.len() as u32, LOAD_MAX_ATTEMPTS - 1);
        let cap = Duration::from_millis(LOAD_BACKOFF_CAP_MS);
        for pair in delays.windows(2) {
            assert!(
                pair[0] <= pair[1],
                "backoff must not decrease: {:?}",
                delays
            );
        }
        for delay in &delays {
            assert!(*delay <= cap, "delay {:?} exceeds cap {:?}", delay, cap);
        }
    }

    /// An error the database already answered with cannot improve on a second
    /// ask, so it must cost one attempt rather than the whole schedule.
    #[tokio::test(flavor = "multi_thread")]
    async fn with_retry_does_not_repeat_a_permanent_error() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, String> = with_retry_using_sleep(
            LOAD_MAX_ATTEMPTS,
            1,
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Err("no such table".to_string())
            },
            |_| std::future::ready(()),
            |_| false,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// A server-side error is not automatically permanent: a failover or an
    /// exhausted connection limit arrives as a SQLSTATE and clears on its own.
    #[test]
    fn transient_sqlstates_are_told_apart_from_permanent_ones() {
        // Connection lost, out of connections, shutting down, write conflict.
        for code in ["08006", "53300", "57P01", "40001", "40P01"] {
            assert!(is_transient_sqlstate(code), "{code} should be retried");
        }
        // Missing table, missing column, denied permission, bad syntax.
        for code in ["42P01", "42703", "42501", "42601"] {
            assert!(!is_transient_sqlstate(code), "{code} should not be retried");
        }
    }

    /// A corrupt row must name itself rather than vanishing from the batch: a
    /// hole-punched vector reads downstream as an account that does not exist.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn get_accounts_reports_the_corrupt_key() {
        let (mut db, _pg) = start_test_postgres().await;
        let stored = Pubkey::new_unique();
        let corrupt = Pubkey::new_unique();
        let absent = Pubkey::new_unique();

        db.set_account(
            stored,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )
        .await;
        insert_corrupt_account(&db, &corrupt).await;

        match get_accounts(&db, &[stored, absent, corrupt]).await {
            Err(AccountLoadError::Corrupt(key)) => assert_eq!(key, corrupt),
            other => panic!("expected Corrupt({corrupt}), got {other:?}"),
        }
    }

    /// Absence must stay absence. Over-correcting a miss into an error would
    /// halt the executor on every first touch of a brand new account.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn get_accounts_keeps_absence_positional() {
        let (mut db, _pg) = start_test_postgres().await;
        let stored = Pubkey::new_unique();
        let absent = Pubkey::new_unique();
        db.set_account(
            stored,
            AccountSharedData::new(700, 0, &Pubkey::new_unique()),
        )
        .await;

        let results = get_accounts(&db, &[absent, stored]).await.unwrap();
        assert!(results[0].is_none());
        assert_eq!(results[1].as_ref().unwrap().lamports(), 700);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn get_accounts_unreadable_store_is_a_backend_error() {
        set_test_retry(2, 1);
        let db = dead_postgres_db();
        let result = get_accounts(&db, &[Pubkey::new_unique()]).await;
        reset_test_retry();
        assert!(
            matches!(result, Err(AccountLoadError::Backend(_))),
            "expected Backend, got {result:?}"
        );
    }

    /// Bad bytes will not change, so retrying a corrupt row only delays the same
    /// verdict. Asserting the variant rather than a stopwatch keeps this exact:
    /// with a backoff pinned at the cap and a budget below it, a single retry
    /// would blow the budget and turn the answer into `Backend`.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn get_accounts_never_retries_a_corrupt_row() {
        let (db, _pg) = start_test_postgres().await;
        let corrupt = Pubkey::new_unique();
        insert_corrupt_account(&db, &corrupt).await;
        // Warm the pool so the budget covers the read, not the first connect.
        let _ = get_accounts(&db, &[Pubkey::new_unique()]).await.unwrap();

        set_test_retry(LOAD_MAX_ATTEMPTS, LOAD_BACKOFF_CAP_MS);
        set_test_budget_ms(LOAD_BACKOFF_CAP_MS / 2);
        let result = get_accounts(&db, &[corrupt]).await;
        reset_test_retry();

        assert!(
            matches!(result, Err(AccountLoadError::Corrupt(key)) if key == corrupt),
            "a corrupt row must be reported directly, not retried: {result:?}"
        );
    }

    /// The header the size query subtracts must match what bincode actually
    /// writes, or every size is wrong by a constant.
    #[test]
    fn serialized_header_constant_matches_bincode() {
        let owner = Pubkey::new_unique();
        for len in [0usize, 1, 1000] {
            let account = AccountSharedData::new(1, len, &owner);
            let encoded = bincode::serialize(&account).expect("account must serialize");
            assert_eq!(
                encoded.len(),
                len + ACCOUNT_SERIALIZED_HEADER_BYTES,
                "bincode layout changed for a {len}-byte account"
            );
        }
        let encoded =
            bincode::serialize(&AccountSharedData::default()).expect("default must serialize");
        assert_eq!(encoded.len(), ACCOUNT_SERIALIZED_HEADER_BYTES);
    }

    /// A key asked for twice must size the same both times. Zero would read as
    /// "this account is free" and under-count the caller's limit.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_account_data_sizes_sizes_a_repeated_key_every_time() {
        let (mut db, _pg) = start_test_postgres().await;
        let pubkey = Pubkey::new_unique();
        db.set_account(
            pubkey,
            AccountSharedData::new(1, 4_000, &Pubkey::new_unique()),
        )
        .await;

        let sizes = get_account_data_sizes(&db, &[pubkey, pubkey])
            .await
            .expect("a healthy store must answer");
        assert_eq!(sizes, vec![4_000, 4_000]);
    }

    /// Sizes come back aligned with the request, exact for stored rows and zero
    /// for keys the store has never seen.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_account_data_sizes_reports_exact_lengths() {
        let (mut db, _pg) = start_test_postgres().await;
        let owner = Pubkey::new_unique();

        let mut keys = Vec::new();
        for len in [0usize, 1_000, 5_000] {
            let pubkey = Pubkey::new_unique();
            db.set_account(pubkey, AccountSharedData::new(1, len, &owner))
                .await;
            keys.push(pubkey);
        }
        let absent = Pubkey::new_unique();
        keys.push(absent);

        let sizes = get_account_data_sizes(&db, &keys)
            .await
            .expect("a healthy store must answer");
        assert_eq!(sizes, vec![0, 1_000, 5_000, 0]);
    }

    /// The Redis-backed handle has no length primitive of its own, so it must
    /// answer from the Postgres it fronts.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_account_data_sizes_reads_through_redis_to_postgres() {
        let (postgres_db, _pg) = start_test_postgres_raw().await;
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;
        let mut fallback = AccountsDB::Postgres(postgres_db);
        let owner = Pubkey::new_unique();

        let mut keys = Vec::new();
        for len in [0usize, 1_000, 5_000] {
            let pubkey = Pubkey::new_unique();
            fallback
                .set_account(pubkey, AccountSharedData::new(1, len, &owner))
                .await;
            keys.push(pubkey);
        }
        keys.push(Pubkey::new_unique());

        let sizes = get_account_data_sizes(&AccountsDB::Redis(redis_db), &keys)
            .await
            .expect("the fallback must answer");
        assert_eq!(sizes, vec![0, 1_000, 5_000, 0]);
    }

    /// A store that cannot answer a size query is an error, never zeroes, which
    /// would read as "every account is empty" and admit an oversized batch.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn get_account_data_sizes_surfaces_an_unreadable_store() {
        set_test_retry(2, 1);
        let db = dead_postgres_db();
        let result = get_account_data_sizes(&db, &[Pubkey::new_unique()]).await;
        reset_test_retry();

        assert!(
            matches!(result, Err(AccountLoadError::Backend(_))),
            "an unreadable store must not read as absent accounts: {result:?}"
        );
    }
}
