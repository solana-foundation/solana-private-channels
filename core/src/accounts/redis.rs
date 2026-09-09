use {
    super::postgres::PostgresAccountsDB,
    anyhow::Result,
    redis::{
        aio::{ConnectionManager, ConnectionManagerConfig},
        AsyncCommands, RedisResult,
    },
    solana_sdk::{account::AccountSharedData, pubkey::Pubkey},
    solana_svm_callback::{InvokeContextCallback, TransactionProcessingCallback},
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::Instant,
    tracing::error,
};

/// How long a command may go unanswered before it fails. Redis can accept a
/// connection and then stop answering it. Covers a reply on a live connection,
/// not the reconnect before one, so the settle path bounds its own work too.
const RESPONSE_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a connection attempt may take. Covers the manager's reconnects too.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);

/// How far the failure tally may climb before the writer stops mirroring. One
/// that keeps failing costs the mirror budget every block, and a mirrored batch
/// pays a failure back, so a cache that keeps up never approaches this.
pub(crate) const CACHE_FAILURE_LIMIT: u32 = 3;

#[derive(Clone)]
pub struct RedisAccountsDB {
    pub connection: ConnectionManager,
    /// The Postgres source of truth this cache sits in front of. A key absent
    /// from Redis is a cache miss to be resolved here, never an authoritative
    /// "does not exist".
    ///
    /// Not optional: a cache with no source of truth behind it can only answer
    /// a miss by claiming the data does not exist, which is the one answer it
    /// must never give.
    pub fallback: PostgresAccountsDB,
    /// The deployment the cache must name for its contents to be usable, read
    /// from Postgres once at construction because it never changes for a given
    /// database. Held here so a read can check the stamp without a second
    /// round trip.
    deployment_id: Vec<u8>,
    /// Counts how many times this cache has been taken out of service. A rebuild
    /// carries the value it was started with and declines to stamp if it has
    /// moved on, so a rebuild overtaken by a later condemnation cannot return a
    /// cache to service whose purge predates the newer gap.
    condemnations: Arc<AtomicU64>,
    /// Whether a rebuild is already working on this cache. A condemned cache is
    /// not mirrored to, so its tip stays frozen and every later batch sees the
    /// same discontinuity; without this, each one would condemn again.
    rebuilding: Arc<AtomicBool>,
    /// Mirror attempts that failed and have not been paid back by one that
    /// landed, shared across clones like the counters above.
    cache_failures: Arc<AtomicU32>,
    /// When the settler may next mirror to this cache, `None` while it is
    /// mirroring. Shared across clones of the settler's handle like the counters
    /// above; a read node builds its own handle and takes no part in this.
    mirror_paused_until: Arc<Mutex<Option<Instant>>>,
    /// Whether a probe of this cache is already running. One at a time, so a
    /// cooldown lapsing under a slow probe cannot start a second one that
    /// resumes on work the first has not finished.
    probing: Arc<AtomicBool>,
    /// Expiry in seconds applied to cached `block:{slot}` entries, bounding the
    /// growth an idle node's heartbeat blocks would otherwise cause. Zero
    /// disables it. Only the writer sets it; a reader never writes blocks.
    block_ttl_secs: u64,
}

impl RedisAccountsDB {
    pub async fn new(redis_url: &str, fallback: PostgresAccountsDB) -> Result<Self, String> {
        // Parse URL to extract host/port without credentials for error messages
        let sanitized_url = if let Ok(parsed) = url::Url::parse(redis_url) {
            let host = parsed.host_str().unwrap_or("unknown");
            let port = parsed.port().unwrap_or(6379);
            format!("{}:{}", host, port)
        } else {
            "unknown".to_string()
        };

        let client = redis::Client::open(redis_url)
            .map_err(|_| format!("Failed to create Redis client for {}", sanitized_url))?;
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(CONNECTION_TIMEOUT)
            .set_response_timeout(RESPONSE_TIMEOUT);
        let connection = ConnectionManager::new_with_config(client, config)
            .await
            .map_err(|_| format!("Failed to connect to Redis at {}", sanitized_url))?;

        let deployment_id = super::redis_coherence::read_deployment_id(&fallback)
            .await
            .map_err(|e| format!("{e:#}"))?;

        let db = Self {
            connection,
            fallback,
            deployment_id,
            condemnations: Arc::new(AtomicU64::new(0)),
            rebuilding: Arc::new(AtomicBool::new(false)),
            cache_failures: Arc::new(AtomicU32::new(0)),
            mirror_paused_until: Arc::new(Mutex::new(None)),
            probing: Arc::new(AtomicBool::new(false)),
            block_ttl_secs: 0,
        };
        Ok(db)
    }

    /// Bound cached block entries with an expiry. Postgres stays the source of
    /// truth, so an expired entry is a miss that falls through and re-reads,
    /// never lost history. Zero disables it.
    pub fn set_block_ttl_secs(&mut self, secs: u64) {
        self.block_ttl_secs = secs;
    }

    /// The expiry applied to cached block entries, zero when disabled.
    pub(crate) fn block_ttl_secs(&self) -> u64 {
        self.block_ttl_secs
    }

    /// The deployment this cache must name to be readable. Renewing the lease
    /// rewrites it, so the mirror path needs it without a Postgres round trip.
    pub(crate) fn deployment_id(&self) -> &[u8] {
        &self.deployment_id
    }

    /// How many times this cache has been taken out of service. Shared across
    /// clones of the handle, so a rebuild running on one clone sees a
    /// condemnation raised on another.
    pub(crate) fn condemnation_generation(&self) -> u64 {
        self.condemnations.load(Ordering::SeqCst)
    }

    /// Records that the cache has been taken out of service, superseding any
    /// rebuild already in flight.
    pub(crate) fn record_condemnation(&self) {
        self.condemnations.fetch_add(1, Ordering::SeqCst);
    }

    /// Claims the right to rebuild this cache, returning false when a rebuild
    /// already holds it. Checking and claiming in one step, so two batches
    /// cannot both decide they are the one to condemn.
    pub(crate) fn try_begin_rebuild(&self) -> bool {
        self.rebuilding
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Releases the claim. Called however the rebuild ended: a failed one that
    /// held the claim would leave the cache unmirrored and unable to condemn
    /// again, so it would never recover.
    pub(crate) fn finish_rebuild(&self) {
        self.rebuilding.store(false, Ordering::SeqCst);
    }

    /// Records a batch that could not be mirrored.
    pub(crate) fn record_cache_failure(&self) {
        self.cache_failures.fetch_add(1, Ordering::SeqCst);
    }

    /// Records a mirrored batch, which pays off one failure. One at a time, not
    /// a reset: a cache failing two batches in three is broken, and a reset
    /// would let it cost the mirror budget forever.
    pub(crate) fn record_cache_success(&self) {
        let _ = self
            .cache_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |failures| {
                Some(failures.saturating_sub(1))
            });
    }

    /// Whether the cache has failed often enough to be given up on.
    pub(crate) fn has_failed_too_often(&self) -> bool {
        self.cache_failures.load(Ordering::SeqCst) >= CACHE_FAILURE_LIMIT
    }

    /// Whether the settler is leaving this cache alone. A lapsed cooldown still
    /// counts: only a probe that found the cache usable lifts a pause, so no
    /// batch can be offered to one that has not passed a probe.
    pub(crate) fn is_mirroring_paused(&self) -> bool {
        self.mirror_paused_until.lock().unwrap().is_some()
    }

    /// Whether a pause has run its course, leaving the cache owed a probe.
    pub(crate) fn pause_has_lapsed(&self) -> bool {
        self.mirror_paused_until
            .lock()
            .unwrap()
            .is_some_and(|until| Instant::now() >= until)
    }

    /// Claims the right to probe this cache, returning false when a probe already
    /// holds it. Checking and claiming in one step, so two probes cannot both
    /// decide the cache is theirs to bring back.
    pub(crate) fn try_begin_probe(&self) -> bool {
        self.probing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Releases the claim. Called however the probe ended: one left held would
    /// stop the cache ever being probed again.
    pub(crate) fn finish_probe(&self) {
        self.probing.store(false, Ordering::SeqCst);
    }

    /// Stops mirroring for `cooldown`. Taken as an argument so tests need not
    /// wait out the production one.
    pub(crate) fn pause_mirroring(&self, cooldown: Duration) {
        let until = Instant::now() + cooldown;
        *self.mirror_paused_until.lock().unwrap() = Some(until);
    }

    /// Puts the cache back on the mirror path. The failure tally goes with it:
    /// it counted batches against a cache the probe has just had purged, and
    /// left standing it would give up on that one a block later.
    pub(crate) fn resume_mirroring(&self) {
        self.cache_failures.store(0, Ordering::SeqCst);
        *self.mirror_paused_until.lock().unwrap() = None;
    }

    /// Reads a cached value and the deployment stamp in one round trip, yielding
    /// the value only while the stamp still names this deployment. Checking the
    /// stamp per read, rather than on a timer, is what makes a condemnation take
    /// effect on the very next read. `MGET` of two keys costs what `GET` of one
    /// costs.
    ///
    /// `None` covers a missing key, a missing stamp and a foreign stamp alike:
    /// all three mean the cache cannot answer, and callers resolve that against
    /// Postgres.
    pub(crate) async fn get_trusted<T: redis::FromRedisValue>(
        &self,
        key: &str,
    ) -> RedisResult<Option<T>> {
        let mut conn = self.connection.clone();
        let (stamp, value): (Option<Vec<u8>>, Option<T>) = redis::cmd("MGET")
            .arg(super::redis_coherence::DEPLOYMENT_ID_KEY)
            .arg(key)
            .query_async(&mut conn)
            .await?;

        match stamp {
            Some(stamp) if stamp == self.deployment_id => Ok(value),
            _ => Ok(None),
        }
    }

    /// Whether a stamp read alongside cached values still names this deployment.
    /// For batch reads that fetch the stamp as part of their own `MGET`.
    pub(crate) fn stamp_is_current(&self, stamp: Option<&Vec<u8>>) -> bool {
        stamp.is_some_and(|stamp| *stamp == self.deployment_id)
    }

    pub async fn set_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        let key = format!("account:{}", pubkey);
        let serialized = bincode::serialize(&account).unwrap();
        let _: RedisResult<()> = self.connection.set(key, serialized).await;
    }
}

impl InvokeContextCallback for RedisAccountsDB {}

impl TransactionProcessingCallback for RedisAccountsDB {
    // The upstream signature cannot carry a failure, so a load error is logged
    // and collapses to `None` here. Nothing in production reaches this impl: the
    // SVM always runs against BOB or a gasless callback, both in-memory.
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        let db = super::traits::AccountsDB::Redis(self.clone());
        let pubkey = *pubkey;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                super::get_account_shared_data::get_account_shared_data(&db, &pubkey)
                    .await
                    .unwrap_or_else(|e| {
                        error!("account load failed at the SVM callback boundary: {}", e);
                        None
                    })
            })
        })
    }

    // Same boundary as above: an unanswerable read is logged and reads as "no
    // match" only because the upstream signature offers nowhere else to go.
    fn account_matches_owners(&self, account: &Pubkey, owners: &[Pubkey]) -> Option<usize> {
        let db = super::traits::AccountsDB::Redis(self.clone());
        let account = *account;
        let owners = owners.to_vec();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                super::account_matches_owners::account_matches_owners(&db, &account, &owners)
                    .await
                    .unwrap_or_else(|e| {
                        error!("account load failed at the SVM callback boundary: {}", e);
                        None
                    })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            accounts::traits::AccountsDB,
            test_helpers::{start_test_postgres, start_test_redis},
        },
        std::time::Duration,
    };

    /// A Redis that accepts the connection and then stops answering must fail
    /// the command, not hang on it. `CLIENT PAUSE` is exactly that failure.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unresponsive_redis_fails_the_command_rather_than_hanging() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        // Long enough that a command with no bound of its own outlives every
        // assertion below.
        let pause = Duration::from_secs(5);
        let mut control = redis_db.connection.clone();
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(pause.as_millis() as u64)
            .arg("ALL")
            .query_async(&mut control)
            .await
            .expect("CLIENT PAUSE must be accepted before the pause takes effect");

        let started = tokio::time::Instant::now();
        let mut conn = redis_db.connection.clone();
        let answer: RedisResult<Option<u64>> = conn.get("latest_slot").await;
        let waited = started.elapsed();

        assert!(
            answer.is_err(),
            "an unanswered command must fail, got {answer:?} after {waited:?}"
        );
        assert!(
            waited < pause,
            "the failure must come from our own timeout, not from the pause lifting, waited {waited:?}"
        );
    }
}
