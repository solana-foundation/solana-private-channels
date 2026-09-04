//! Decides when the Redis cache may be served from, and rebuilds it when not.
//!
//! Redis outlives the process that fills it. Point a write node at a fresh,
//! restored or simply different Postgres database while reusing the same Redis
//! instance and every key from the previous ledger is still there, indexed the
//! same way, indistinguishable from current state. A cache can also fall behind
//! the ledger it belongs to: mirroring is best-effort, so an outage leaves its
//! keys holding values Postgres has already moved past.
//!
//! Both are handled by one mechanism. Each Postgres database is stamped with an
//! identifier at creation and the cache carries a copy, which every cached read
//! checks against the identifier the handle was built with. That copy is a lease
//! rather than a flag: the settler renews it on every mirrored batch, so a cache
//! nobody is maintaining falls out of service without anyone having to say so.
//! This matters because the writer that should revoke may be the thing that
//! failed, and it cannot revoke through a connection it has lost.
//!
//! Clearing the stamp does the same thing immediately, for the cases where the
//! writer is healthy enough to say so:
//!
//! - [`staleness_reason`] compares a cache against Postgres at startup, where a
//!   mismatch means another ledger's data or a tip that does not line up.
//! - [`ensure_cache_continuity`] watches for missed batches between blocks, where
//!   the cached tip failing to advance by one slot is the evidence.
//! - [`rebuild_cache`] empties a cache and stamps it, putting it back in service.

use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB},
    anyhow::{anyhow, Context, Result},
    redis::AsyncCommands,
    std::time::Duration,
    tracing::{info, warn},
};

/// Names the deployment the data belongs to. Lives in the Postgres `metadata`
/// table and is mirrored to the same key in Redis once the cache is aligned. Its
/// absence from Redis means the cache has never been checked against a source of
/// truth.
pub(crate) const DEPLOYMENT_ID_KEY: &str = "deployment_id";

/// How long the cache stays trusted without renewal.
///
/// The writer renews on every mirrored batch, so a writer that stops
/// maintaining the cache stops extending it. Revoking would be faster, but
/// revoking needs a working Redis connection and the cases that matter most are
/// the ones where the writer has lost it or died outright. Expiry needs nothing.
///
/// Sized well above any tolerable settle latency. A settler stalled inside a
/// slow Postgres write is not diverging from Postgres, so expiring there would
/// shed no staleness and would land the whole read fleet on the database that is
/// already the bottleneck.
pub(crate) const CACHE_LEASE_TTL: Duration = Duration::from_secs(30);

/// Cached ledger state, keyed by pubkey, signature, slot or address. `addr_sigs:`
/// is no longer written, but a cache filled by an older build still holds it and
/// a purge has to clear it.
const LEDGER_KEY_PREFIXES: [&str; 4] = ["account:", "tx:", "block:", "addr_sigs:"];

/// Cached ledger state under fixed keys: the chain tip, plus the slot index,
/// transaction counter and performance-sample list that older builds wrote.
const LEDGER_FIXED_KEYS: [&str; 7] = [
    "block_slot_index",
    "transaction_count",
    "performance_samples",
    "latest_slot",
    "current_slot",
    "block_height",
    "latest_blockhash",
];

/// Keys per SCAN round. Bounds how long one round blocks the Redis event loop.
const SCAN_COUNT: usize = 512;

/// Holds the claim on a rebuild until it is handed to one. Releasing on drop
/// covers the caller cancelling this part-way through: a claim left held makes
/// every later batch see a rebuild that is not running, and nothing says so.
struct RebuildClaim<'a> {
    redis_db: &'a RedisAccountsDB,
    handed_over: bool,
}

impl RebuildClaim<'_> {
    /// The rebuild is about to be started, and releases the claim when it ends.
    fn hand_over(mut self) {
        self.handed_over = true;
    }
}

impl Drop for RebuildClaim<'_> {
    fn drop(&mut self) {
        if !self.handed_over {
            self.redis_db.finish_rebuild();
        }
    }
}

/// Whether the cache may still be written to and served.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CacheStatus {
    /// Contiguous with the last batch mirrored into it. Mirror this one and
    /// renew its lease.
    InService,
    /// A batch was missed, so every key in it is suspect. Already taken out of
    /// service; the caller owes it a purge off the block-production path.
    Condemned,
    /// Out of service with a purge already running. Leave it alone: do not
    /// mirror, do not renew its lease, do not start a second purge.
    Rebuilding,
}

/// Reads the identifier Postgres was stamped with at schema creation.
pub(crate) async fn read_deployment_id(db: &PostgresAccountsDB) -> Result<Vec<u8>> {
    sqlx::query_scalar::<_, Vec<u8>>("SELECT value FROM metadata WHERE key = $1")
        .bind(DEPLOYMENT_ID_KEY)
        .fetch_optional(db.pool.as_ref())
        .await
        .context("Failed to read deployment id from Postgres")?
        .ok_or_else(|| anyhow!("Postgres has no deployment id; schema initialization did not run"))
}

/// Why the cache cannot be trusted, or `None` when it is coherent with
/// Postgres.
pub(crate) async fn staleness_reason(
    redis_db: &RedisAccountsDB,
    deployment_id: &[u8],
    postgres_slot: Option<u64>,
) -> Result<Option<String>> {
    let mut conn = redis_db.connection.clone();

    let cached_id: Option<Vec<u8>> = conn
        .get(DEPLOYMENT_ID_KEY)
        .await
        .context("Failed to read deployment id from Redis")?;

    // An empty cache cannot serve anything wrongly, so there is nothing to purge
    // and nothing to check. This is the normal first attach.
    if !holds_ledger_keys(redis_db).await? {
        return Ok(None);
    }

    let Some(cached_id) = cached_id else {
        // Ledger state filled in by something that never checked itself against
        // a source of truth.
        return Ok(Some(
            "cache holds ledger keys but names no deployment".to_string(),
        ));
    };
    if cached_id.as_slice() != deployment_id {
        return Ok(Some(format!(
            "cache belongs to deployment {} but Postgres is {}",
            hex::encode(&cached_id),
            hex::encode(deployment_id)
        )));
    }

    // Postgres with no blocks cannot back any cached ledger state, whatever the
    // tips say. Checked separately because two absent tips compare equal below,
    // so a cache that lost only its tip key would otherwise survive a Postgres
    // that was truncated or replaced.
    if postgres_slot.is_none() {
        return Ok(Some(
            "Postgres holds no blocks but the cache holds ledger keys".to_string(),
        ));
    }

    // The tips must match exactly, not merely not-run-ahead.
    //
    // A cache behind Postgres is not a cache with missing entries: its keys are
    // present, holding the values they had at the cached tip. Reads hit them and
    // never reach the fallback, so a stale balance is served as current. The
    // settler commits to Postgres before mirroring to Redis, so a kill between
    // the two, or a Redis outage, leaves exactly that state.
    //
    // Equality holds after a clean shutdown, so this costs nothing in the normal
    // case and a cold cache otherwise. Cold is slow; behind is wrong.
    let cached_slot: Option<u64> = conn
        .get("latest_slot")
        .await
        .context("Failed to read cached tip from Redis")?;
    if cached_slot != postgres_slot {
        return Ok(Some(format!(
            "cached tip {:?} does not match the Postgres tip {:?}",
            cached_slot, postgres_slot
        )));
    }

    Ok(None)
}

/// Detects that the cache has missed settled batches, and takes it out of
/// service when it has.
///
/// Call before mirroring the batch for `slot`. A cached tip that is not the block
/// this batch extends means batches were missed, so every key is suspect. The
/// parent, not `slot - 1`: the previous slot usually holds no block.
///
/// The response is to clear the deployment stamp, which takes the cache out of
/// service on the next read. The caller runs the purge off the block-production
/// path, because a SCAN over a large keyspace costs far more than a blocktime
/// allows.
pub(crate) async fn ensure_cache_continuity(
    redis_db: &RedisAccountsDB,
    parent_slot: u64,
    slot: u64,
) -> Result<CacheStatus> {
    let mut conn = redis_db.connection.clone();
    let cached_slot: Option<u64> = conn
        .get("latest_slot")
        .await
        .context("Failed to read cached tip from Redis")?;

    // No tip: an empty or freshly purged cache, with nothing to be contiguous
    // with and nothing to lose.
    let Some(cached_slot) = cached_slot else {
        return Ok(CacheStatus::InService);
    };
    if cached_slot == parent_slot {
        return Ok(CacheStatus::InService);
    }

    condemn_cache(
        redis_db,
        &format!("missed at least one batch (cached tip {cached_slot}, now settling {slot})"),
    )
    .await
}

/// Takes the cache out of service and claims the rebuild it owes, naming
/// `reason` in the log. The caller runs that rebuild off the block-production
/// path.
///
/// `Rebuilding` when a purge already holds the claim: a condemned cache is not
/// mirrored to, so its tip stays where it is and every later batch lands here
/// too. The rebuild already running covers them all, since nothing is written to
/// the cache while it runs, so no new gap can open behind it. Condemning again
/// would purge the whole keyspace once per block and, worse, would bump the
/// generation and supersede that rebuild, leaving nothing to stamp the cache back
/// into service.
pub(crate) async fn condemn_cache(redis_db: &RedisAccountsDB, reason: &str) -> Result<CacheStatus> {
    if !redis_db.try_begin_rebuild() {
        return Ok(CacheStatus::Rebuilding);
    }
    // The caller only rebuilds on `Condemned`, so every other way out of here
    // has to release the claim, or nothing would ever purge the cache.
    let claim = RebuildClaim {
        redis_db,
        handed_over: false,
    };

    warn!("Redis cache {}, rebuilding it", reason);
    // Recorded before the stamp is cleared so a rebuild started for this
    // condemnation cannot be handed a generation that already looks stale.
    redis_db.record_condemnation();
    clear_deployment_id(redis_db).await?;

    claim.hand_over();
    Ok(CacheStatus::Condemned)
}

/// Empties a condemned cache and then stamps it, which puts it back into service.
///
/// The order is load-bearing: stamping first would return stale keys to service
/// for as long as the purge took. Runs off the block-production path, so the
/// purge is free to walk the whole keyspace.
///
/// `generation` is the condemnation count this rebuild was started for. A cache
/// condemned again while this one was purging has a newer rebuild behind it whose
/// purge covers the newer gap, and stamping here would put keys back in service
/// that only that later purge removes. So a superseded rebuild finishes its purge,
/// which is never wasted work, and leaves the stamping to its successor.
pub(crate) async fn rebuild_cache(redis_db: &RedisAccountsDB, generation: u64) -> Result<()> {
    let outcome = async {
        let deployment_id = read_deployment_id(&redis_db.fallback).await?;
        purge_ledger_keys(redis_db).await?;

        if redis_db.condemnation_generation() != generation {
            info!(
                "Redis cache was condemned again while rebuilding; a later rebuild will finish it"
            );
            return Ok(());
        }

        stamp_deployment_id(redis_db, &deployment_id).await?;
        info!("Redis cache rebuilt and back in service");
        Ok(())
    }
    .await;

    // Released however the rebuild ended. A failed one that kept the claim would
    // leave the cache unmirrored and unable to condemn again, so nothing would
    // ever retry the purge and it would stay out of service until a restart.
    redis_db.finish_rebuild();
    outcome
}

/// Removes every cached ledger key, leaving the cache empty rather than wrong.
/// Reads then miss and resolve against Postgres.
pub(crate) async fn purge_ledger_keys(redis_db: &RedisAccountsDB) -> Result<()> {
    let mut conn = redis_db.connection.clone();
    let mut purged = 0usize;

    for prefix in LEDGER_KEY_PREFIXES {
        let pattern = format!("{}*", prefix);
        let mut cursor = 0u64;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut conn)
                .await
                .with_context(|| format!("Failed to scan {} keys in Redis", pattern))?;

            if !keys.is_empty() {
                purged += keys.len();
                // UNLINK reclaims memory on a background thread, so purging a
                // large keyspace does not stall the Redis event loop.
                let mut unlink = redis::cmd("UNLINK");
                for key in &keys {
                    unlink.arg(key);
                }
                let _: () = unlink
                    .query_async(&mut conn)
                    .await
                    .with_context(|| format!("Failed to unlink {} keys in Redis", pattern))?;
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
    }

    let mut unlink = redis::cmd("UNLINK");
    for key in LEDGER_FIXED_KEYS {
        unlink.arg(key);
    }
    let _: () = unlink
        .query_async(&mut conn)
        .await
        .context("Failed to unlink cached ledger metadata in Redis")?;

    info!(
        "Purged {} cached ledger keys",
        purged + LEDGER_FIXED_KEYS.len()
    );
    Ok(())
}

/// Leases the cache to this deployment for [`CACHE_LEASE_TTL`]. Written only
/// after the cache is known to be coherent, and cleared before a purge, so an
/// interrupted purge leaves the cache unstamped rather than falsely stamped.
///
/// Also the renewal: the settler re-stamps after every mirrored batch, which is
/// what ties trust to maintenance. Nothing else extends the lease, so a cache
/// nobody is mirroring to falls out of service on its own.
pub(crate) async fn stamp_deployment_id(
    redis_db: &RedisAccountsDB,
    deployment_id: &[u8],
) -> Result<()> {
    let mut conn = redis_db.connection.clone();
    conn.set_ex::<_, _, ()>(DEPLOYMENT_ID_KEY, deployment_id, CACHE_LEASE_TTL.as_secs())
        .await
        .context("Failed to write deployment id to Redis")
}

/// Whether a read node should start against this cache.
///
/// Only a cache naming another deployment is refused, because that is a
/// misconfiguration no amount of waiting fixes: the node has been pointed at the
/// wrong Redis. An unstamped cache is fine to start against, stamped or not,
/// since nothing is served from one and the lease lapsing is the ordinary state
/// during a writer outage.
///
/// A startup gate only. Individual reads check the stamp again for themselves, so
/// this is about refusing to start against a misconfigured cache rather than
/// about keeping stale values out of responses.
pub(crate) async fn verify_cache_stamp(redis_db: &RedisAccountsDB) -> Result<()> {
    let deployment_id = read_deployment_id(&redis_db.fallback).await?;

    let mut conn = redis_db.connection.clone();
    let cached_id: Option<Vec<u8>> = conn
        .get(DEPLOYMENT_ID_KEY)
        .await
        .context("Failed to read deployment id from Redis")?;

    match cached_id {
        Some(cached_id) if cached_id.as_slice() == deployment_id => Ok(()),
        Some(cached_id) => Err(anyhow!(
            "Redis cache belongs to deployment {} but Postgres is {}",
            hex::encode(&cached_id),
            hex::encode(&deployment_id)
        )),
        None => {
            // Worth saying out loud: pointed at an idle deployment's Redis, this
            // is the only symptom, since nothing there renews a lease to compare
            // against.
            if holds_ledger_keys(redis_db).await? {
                warn!("Redis cache holds ledger keys but no live lease; serving from Postgres until a write node stamps it");
            }
            Ok(())
        }
    }
}

pub(crate) async fn clear_deployment_id(redis_db: &RedisAccountsDB) -> Result<()> {
    let mut conn = redis_db.connection.clone();
    conn.del::<_, ()>(DEPLOYMENT_ID_KEY)
        .await
        .context("Failed to clear deployment id in Redis")
}

/// Whether the cache holds any ledger state at all.
async fn holds_ledger_keys(redis_db: &RedisAccountsDB) -> Result<bool> {
    let mut conn = redis_db.connection.clone();

    for key in LEDGER_FIXED_KEYS {
        let exists: bool = conn
            .exists(key)
            .await
            .with_context(|| format!("Failed to check {} in Redis", key))?;
        if exists {
            return Ok(true);
        }
    }

    for prefix in LEDGER_KEY_PREFIXES {
        let pattern = format!("{}*", prefix);
        let mut cursor = 0u64;
        // A single SCAN round may return nothing while keys still exist, so walk
        // until the cursor wraps. Exits on the first hit, and an empty keyspace
        // wraps immediately.
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut conn)
                .await
                .with_context(|| format!("Failed to scan {} keys in Redis", pattern))?;

            if !keys.is_empty() {
                return Ok(true);
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::traits::AccountsDB,
        test_helpers::{start_test_postgres, start_test_redis},
    };
    use solana_sdk::pubkey::Pubkey;

    /// Returns a cache handle plus its Postgres source of truth, both throwaway.
    /// The containers are returned so the caller keeps them alive.
    async fn start_cache() -> (
        RedisAccountsDB,
        PostgresAccountsDB,
        testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
        testcontainers::ContainerAsync<testcontainers_modules::redis::Redis>,
    ) {
        let (pg_db, pg_container) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, redis_container) = start_test_redis(postgres_db.clone()).await;
        (redis_db, postgres_db, pg_container, redis_container)
    }

    /// The normal case: each batch follows the one the cache last recorded, so
    /// nothing was missed and the cache keeps everything it holds.
    #[tokio::test(flavor = "multi_thread")]
    async fn continuity_keeps_a_cache_holding_the_block_this_batch_extends() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        let cached_pubkey = Pubkey::new_unique();
        let cached_tip = 100u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("latest_slot", cached_tip).await.unwrap();
        let _: () = conn
            .set(format!("account:{}", cached_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();

        assert_eq!(
            ensure_cache_continuity(&redis_db, cached_tip, cached_tip + 1)
                .await
                .unwrap(),
            CacheStatus::InService,
            "a contiguous batch leaves the cache in service"
        );

        let still_cached: bool = conn
            .exists(format!("account:{}", cached_pubkey))
            .await
            .unwrap();
        assert!(still_cached, "a contiguous batch must not purge the cache");
    }

    /// The outage case: writes failed for a stretch, so the cached tip stopped
    /// advancing while Postgres kept going. Every key in the cache may now be
    /// stale, and the gap must be caught here: once the tip advances past it,
    /// the startup check can no longer tell.
    ///
    /// The response is to condemn, not to repair: clearing the stamp is one
    /// command, while purging is thousands of round-trips and this runs on the
    /// block-production path.
    #[tokio::test(flavor = "multi_thread")]
    async fn continuity_condemns_a_cache_that_missed_batches() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        let stale_pubkey = Pubkey::new_unique();
        let cached_tip = 100u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("latest_slot", cached_tip).await.unwrap();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();

        // Redis is back after missing slots 101..=200.
        assert_eq!(
            ensure_cache_continuity(&redis_db, 200, 201).await.unwrap(),
            CacheStatus::Condemned,
            "a missed batch must condemn the cache"
        );

        let stamped: Option<Vec<u8>> = conn.get(DEPLOYMENT_ID_KEY).await.unwrap();
        assert_eq!(
            stamped, None,
            "the stamp must be cleared so read nodes stop trusting the cache"
        );
        let still_cached: bool = conn
            .exists(format!("account:{}", stale_pubkey))
            .await
            .unwrap();
        assert!(
            still_cached,
            "purging is left to the next startup, off the block-production path"
        );
    }

    /// A condemned cache is not mirrored to, so its tip stays frozen and every
    /// later batch sees the same discontinuity. Condemning again would spawn a
    /// full-keyspace purge per block, and worse, would bump the generation and
    /// supersede the rebuild already running, leaving nothing to stamp the cache
    /// back into service.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cache_already_rebuilding_is_not_condemned_again() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("latest_slot", 100u64).await.unwrap();

        assert_eq!(
            ensure_cache_continuity(&redis_db, 200, 201).await.unwrap(),
            CacheStatus::Condemned
        );
        let generation = redis_db.condemnation_generation();

        // The tip is still 100, because a condemned cache is not mirrored to, so
        // this batch looks exactly like the one that condemned it.
        assert_eq!(
            ensure_cache_continuity(&redis_db, 201, 202).await.unwrap(),
            CacheStatus::Rebuilding,
            "a rebuild already in flight covers this"
        );
        assert_eq!(
            redis_db.condemnation_generation(),
            generation,
            "re-condemning would supersede the rebuild in flight"
        );
    }

    /// The caller bounds this check, so it can be cancelled part-way through. A
    /// claim left held then stops mirroring silently and forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_continuity_check_releases_the_rebuild_claim() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("latest_slot", 100u64).await.unwrap();

        // Reads are still served, so the check gets its tip and takes the claim.
        // Only the write that clears the stamp hangs, which is where the
        // cancellation has to land.
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(5_000u64)
            .arg("WRITE")
            .query_async(&mut conn)
            .await
            .unwrap();

        let cancelled = tokio::time::timeout(
            Duration::from_millis(250),
            ensure_cache_continuity(&redis_db, 199, 200),
        )
        .await;
        assert!(
            cancelled.is_err(),
            "the check must still be inside the stamp clear for this test to mean anything"
        );
        // Recorded just after the claim is taken, so this separates a
        // cancellation inside the claimed region from one that ran out of time
        // on the tip read before reaching it.
        assert_eq!(
            redis_db.condemnation_generation(),
            1,
            "the check must have got past the claim for this test to mean anything"
        );

        assert!(
            redis_db.try_begin_rebuild(),
            "a cancelled check must leave the rebuild claim free"
        );
    }

    /// An empty or freshly purged cache has no tip to be contiguous with, and
    /// nothing to lose either way.
    #[tokio::test(flavor = "multi_thread")]
    async fn continuity_accepts_a_cache_with_no_tip() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        assert_eq!(
            ensure_cache_continuity(&redis_db, 41, 42).await.unwrap(),
            CacheStatus::InService,
            "a cache with no tip has nothing to be discontiguous with"
        );

        let stamped: Option<Vec<u8>> = conn_get_stamp(&redis_db).await;
        assert_eq!(stamped, Some(deployment_id), "the stamp must be untouched");
    }

    async fn conn_get_stamp(redis_db: &RedisAccountsDB) -> Option<Vec<u8>> {
        let mut conn = redis_db.connection.clone();
        conn.get(DEPLOYMENT_ID_KEY).await.unwrap()
    }

    /// Rebuilding empties the cache and only then stamps it. The order is the
    /// point: a stamp written over a half-purged cache would put stale keys back
    /// into service.
    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_empties_the_cache_then_stamps_it() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;

        let stale_pubkey = Pubkey::new_unique();
        let mut conn = redis_db.connection.clone();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", 100u64).await.unwrap();

        // Nothing has condemned the cache since, so this rebuild is the current
        // one and may stamp.
        rebuild_cache(&redis_db, redis_db.condemnation_generation())
            .await
            .unwrap();

        let stale_survived: bool = conn
            .exists(format!("account:{}", stale_pubkey))
            .await
            .unwrap();
        assert!(!stale_survived, "the rebuild must empty the cache");
        let tip: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(tip, None, "the rebuild must clear the cached tip too");
        let stamped: Option<Vec<u8>> = conn.get(DEPLOYMENT_ID_KEY).await.unwrap();
        assert_eq!(
            stamped,
            Some(read_deployment_id(&postgres_db).await.unwrap()),
            "a rebuilt cache must be stamped so reads can use it again"
        );
    }

    /// A rebuild that has been overtaken by a later condemnation must not stamp.
    ///
    /// With a flapping Redis two rebuilds can overlap, and the earlier one's purge
    /// predates the second gap: stamping on its completion would return keys
    /// written before that gap to service. Only the newest rebuild may stamp, and
    /// the ones it superseded finish quietly.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_superseded_rebuild_does_not_put_the_cache_back_in_service() {
        let (redis_db, _postgres_db, _pg, _redis) = start_cache().await;

        let stale_pubkey = Pubkey::new_unique();
        let mut conn = redis_db.connection.clone();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", 100u64).await.unwrap();

        // Two gaps in a row: this rebuild carries the first generation while a
        // second condemnation has already happened.
        let superseded = redis_db.condemnation_generation();
        assert_eq!(
            ensure_cache_continuity(&redis_db, 199, 200).await.unwrap(),
            CacheStatus::Condemned
        );
        let current = redis_db.condemnation_generation();
        assert_ne!(
            superseded, current,
            "each condemnation must advance the generation"
        );

        rebuild_cache(&redis_db, superseded).await.unwrap();

        let stamped: Option<Vec<u8>> = conn.get(DEPLOYMENT_ID_KEY).await.unwrap();
        assert_eq!(
            stamped, None,
            "a superseded rebuild must leave the cache out of service"
        );

        // The rebuild that carries the current generation is the one that may
        // finish the job.
        rebuild_cache(&redis_db, current).await.unwrap();
        let stamped: Option<Vec<u8>> = conn.get(DEPLOYMENT_ID_KEY).await.unwrap();
        assert!(
            stamped.is_some(),
            "the newest rebuild must put the cache back in service"
        );
    }

    /// Trust must decay on its own rather than wait to be revoked. A writer that
    /// has lost Redis cannot announce it through the connection it just lost, so
    /// the lease is what makes "nobody is maintaining this cache" and "stop
    /// serving it" the same event.
    #[tokio::test(flavor = "multi_thread")]
    async fn stamping_leases_the_cache_rather_than_trusting_it_forever() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();

        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        // TTL is -1 for a key with no expiry and -2 for one that is absent, so
        // this fails on a plain SET. The upper bound catches a seconds/millis
        // mixup, which would grant hours and still read as positive.
        let mut conn = redis_db.connection.clone();
        let ttl: i64 = conn.ttl(DEPLOYMENT_ID_KEY).await.unwrap();
        assert!(
            ttl > 0 && ttl <= CACHE_LEASE_TTL.as_secs() as i64,
            "the stamp must carry a lease, got TTL {ttl}"
        );
    }

    /// A cache stamped for this deployment is the one a read node may serve.
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cache_stamp_accepts_a_matching_stamp() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        verify_cache_stamp(&redis_db)
            .await
            .expect("a cache stamped for this deployment must be accepted");
    }

    /// Another ledger's cache must never be served, which is the whole point of
    /// the stamp.
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cache_stamp_rejects_a_foreign_stamp() {
        let (redis_db, _postgres_db, _pg, _redis) = start_cache().await;
        stamp_deployment_id(&redis_db, &[9u8; 16]).await.unwrap();

        let error = verify_cache_stamp(&redis_db)
            .await
            .expect_err("a cache naming another deployment must be rejected");
        assert!(
            format!("{error}").contains("deployment"),
            "error should name the mismatch, got: {error}"
        );
    }

    /// An empty cache holds nothing to serve wrongly, so a read node may attach
    /// to it before any write node has stamped it.
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cache_stamp_accepts_an_empty_unstamped_cache() {
        let (redis_db, _postgres_db, _pg, _redis) = start_cache().await;

        verify_cache_stamp(&redis_db)
            .await
            .expect("an empty cache must be accepted");
    }

    /// Ledger keys with no stamp is what a lapsed lease leaves behind, which is
    /// the ordinary "no writer has mirrored lately" state rather than evidence
    /// of anything wrong. A read node must still start, because it serves
    /// nothing from an unstamped cache: every read resolves against Postgres, so
    /// the answers are correct and only slower. Refusing to boot here would take
    /// reads down for the length of a writer outage.
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cache_stamp_accepts_unstamped_ledger_state() {
        let (redis_db, _postgres_db, _pg, _redis) = start_cache().await;
        let mut conn = redis_db.connection.clone();
        let _: () = conn
            .set(format!("account:{}", Pubkey::new_unique()), vec![1u8, 2, 3])
            .await
            .unwrap();

        verify_cache_stamp(&redis_db)
            .await
            .expect("a lapsed lease must not stop a read node from starting");
    }

    /// The live slot moves ten times per heartbeat block and is not the cached
    /// tip, so the continuity check must ignore it. A cache condemned once per
    /// tick would purge and rebuild the whole keyspace forever.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_moving_current_slot_does_not_condemn_the_cache() {
        let (redis_db, postgres_db, _pg, _redis) = start_cache().await;
        let deployment_id = read_deployment_id(&postgres_db).await.unwrap();
        stamp_deployment_id(&redis_db, &deployment_id)
            .await
            .unwrap();

        // The cache holds the block this batch extends, which is the only thing
        // continuity is about.
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("latest_slot", 100u64).await.unwrap();

        // Nine idle ticks between that block and the next one.
        for slot in 101..=109u64 {
            super::super::current_slot::mirror_current_slot(&redis_db, slot).await;
        }

        assert_eq!(
            ensure_cache_continuity(&redis_db, 100, 110).await.unwrap(),
            CacheStatus::InService,
            "idle ticks must not look like a missed batch"
        );
        let stamp: Option<Vec<u8>> = conn.get(DEPLOYMENT_ID_KEY).await.unwrap();
        assert_eq!(
            stamp.as_deref(),
            Some(deployment_id.as_slice()),
            "the cache must still be in service"
        );
    }
}
