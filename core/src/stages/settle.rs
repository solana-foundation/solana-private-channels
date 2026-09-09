use {
    crate::{
        accounts::{
            get_tip::get_tip,
            postgres::PostgresAccountsDB,
            redis::{RedisAccountsDB, CACHE_FAILURE_LIMIT},
            redis_coherence,
            traits::BlockInfo,
            write_batch::AddressSignatureRow,
            AccountsDB,
        },
        nodes::node::WorkerHandle,
        stage_metrics::SharedMetrics,
    },
    anyhow::{anyhow, Context, Result},
    redis::AsyncCommands,
    solana_rpc_client_types::response::RpcPerfSample,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        hash::{hashv, Hash},
        pubkey::Pubkey,
        signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_svm::{
        transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
        transaction_processor::LoadAndExecuteSanitizedTransactionsOutput,
    },
    solana_svm_transaction::svm_message::SVMMessage,
    std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{sync::mpsc, time::Instant},
    tokio_util::sync::CancellationToken,
    tracing::{debug, error, info, warn},
};

const SETTLE_START_DELAY_MS: u64 = 1000;

/// How long the Redis mirror may hold the settle path, sized against the default
/// 100ms blocktime. Smaller than the transport timeout, so this is what fires.
const CACHE_MIRROR_BUDGET: Duration = Duration::from_millis(100);

/// How long a cache given up on is left alone before the settler probes it.
/// Longer than the lease, so readers have dropped it by then whether or not the
/// eviction that gave it up landed.
pub const CACHE_MIRROR_COOLDOWN: Duration = Duration::from_secs(60);

/// How long a probe may take. Off the block path, so it can cover the reconnect
/// a cache that has been away needs rather than failing a healthy one for being
/// slow to answer the first time.
const CACHE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Idle block cadence, so block height and therefore expiry keep advancing.
/// One second cuts idle rows tenfold against a block every tick, while a longer
/// gap would save little more and stretch idle expiry well past the loaded case.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// How long the idle-slot write may hold the tick, one default blocktime like
/// the mirror budget. The write is one row on a quiet pool, and a tick that
/// outlives this is one tick late: the next tick republishes a higher slot.
const IDLE_SLOT_PUBLISH_BUDGET: Duration = Duration::from_millis(100);

/// A tick produces a block when it settled work, or when the heartbeat is due.
/// Genesis is always produced so the chain has an anchor.
fn should_produce_block(has_work: bool, is_genesis: bool, since_last_block: Duration) -> bool {
    has_work || is_genesis || since_last_block >= HEARTBEAT_INTERVAL
}

/// Record a tick that produced no block, so `getSlot` keeps moving while the
/// node is idle. The in-memory counter is what production uses, so a failed
/// write costs a stale published slot until the next tick, never a wrong block.
async fn publish_idle_slot(
    postgres_db: &PostgresAccountsDB,
    redis_db: Option<&RedisAccountsDB>,
    slot: u64,
) -> Result<()> {
    // Awaited on the tick with no retry budget behind it, so a Postgres that sits
    // on the statement would stop the chain here. Bounded, the stall reaches the
    // heartbeat block instead, and that path has retries and a failure budget.
    match tokio::time::timeout(
        IDLE_SLOT_PUBLISH_BUDGET,
        crate::accounts::current_slot::set_current_slot(postgres_db, slot),
    )
    .await
    {
        Ok(written) => written?,
        Err(_) => {
            return Err(anyhow!(
                "publishing the idle slot exceeded its {:?} budget",
                IDLE_SLOT_PUBLISH_BUDGET
            ))
        }
    }
    // A cache given up on is left alone here too: ten writes a second into one
    // the batch path is deliberately not touching is most of the cost the
    // give-up exists to avoid.
    let Some(redis) = redis_db.filter(|redis| !redis.is_mirroring_paused()) else {
        return Ok(());
    };
    // Same reason as above: the cache is never allowed to hold the chain, on
    // this path or the batch one.
    if tokio::time::timeout(
        CACHE_MIRROR_BUDGET,
        crate::accounts::current_slot::mirror_current_slot(redis, slot),
    )
    .await
    .is_err()
    {
        warn!(
            "Mirroring the idle slot exceeded its {:?} budget, abandoning it",
            CACHE_MIRROR_BUDGET
        );
    }
    Ok(())
}

/// Bounds one settle attempt, matching the stage health margin so an overrun is
/// already unhealthy. Unbounded, the pool's 30s acquire would outlast the budget.
const SETTLE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Total retry time. Blocks stop while it runs and the blockhash window counts
/// blocks, so pinning it to the transaction expiry caps the overshoot at 2x.
const SETTLE_RETRY_BUDGET: Duration = Duration::from_secs(15);

/// Attempt bound once shutdown has been signalled. Short so two attempts fit
/// the shutdown budget below.
const SETTLE_SHUTDOWN_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(1500);

/// All the settling a cancelled node gets, measured from cancellation so an
/// attempt already in flight counts against it. Sized to fit two attempts and
/// to leave the rest of the drain deadline for the cascade.
const SETTLE_SHUTDOWN_BUDGET: Duration = Duration::from_secs(4);

/// First delay between attempts, doubled after each failure up to the cap.
const SETTLE_RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);
const SETTLE_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// A single account that has been settled
/// We need to track if the account was deleted so we can tombstone it
/// in the accounts database
#[derive(Clone)]
pub struct AccountSettlement {
    pub account: AccountSharedData,
    pub deleted: bool,
}

/// One executed batch on its way from the executor to the settler. The third
/// element is the executor's generation for the account writes in this batch.
pub type ExecutedBatch = (
    LoadAndExecuteSanitizedTransactionsOutput,
    Vec<SanitizedTransaction>,
    u64,
);

/// Settlement acknowledgement sent back to BOB after a commit.
///
/// `generation` is a high-water mark: every executor write with a generation
/// <= this one is now durable in the accounts database. One number describes
/// the whole tick because the settler commits its buffer atomically.
pub struct AccountSettlements {
    pub generation: u64,
    pub accounts: Vec<(Pubkey, AccountSettlement)>,
}

/// Cap on buffered account bytes before the settler stops draining the executor.
/// Far below Postgres' 1 GB limit for one bytea, and small enough that committing
/// a full buffer still finishes inside the stage health margin.
pub(crate) const MAX_BUFFERED_SETTLE_BYTES: usize = 64 * 1024 * 1024;

/// Account bytes a batch keeps in memory once buffered for settlement.
/// Rolled-back and zero-lamport writes count too, because the buffer still holds
/// them, so this can never read below what the upsert ends up binding.
pub(crate) fn retained_account_bytes(
    results: &[TransactionProcessingResult],
    transactions: &[SanitizedTransaction],
) -> usize {
    results
        .iter()
        .zip(transactions.iter())
        .map(|(result, transaction)| retained_bytes_of(result, transaction))
        .sum()
}

/// The same count for a single transaction, so the send-side chunker can size
/// one at a time instead of re-walking the whole batch.
pub(crate) fn retained_bytes_of(
    result: &TransactionProcessingResult,
    transaction: &SanitizedTransaction,
) -> usize {
    let Ok(ProcessedTransaction::Executed(executed)) = result else {
        return 0;
    };
    executed
        .loaded_transaction
        .accounts
        .iter()
        .enumerate()
        .filter(|(index, _)| transaction.is_writable(*index))
        .map(|(_, (_, account))| account.data().len())
        .sum()
}

struct SettleResult {
    slot: u64,
    blockhash: Hash,
    /// An announcement found no consumer. Not an error: the block is
    /// committed, and the caller still owes it the bookkeeping.
    publisher_gone: bool,
    /// Height of the block just produced, so the caller can advance its counter
    /// without re-reading the database.
    block_height: u64,
}

/// The channels a committed block is announced on, ahead of any cache work.
#[derive(Clone, Copy)]
struct BlockPublishers<'a> {
    /// Advances the dedup window. Until dedup holds the hash, transactions
    /// built on it are dropped.
    blockhashes: &'a mpsc::UnboundedSender<Hash>,
    /// Acks the commit to BOB, which unpins the settled accounts.
    accounts: &'a mpsc::UnboundedSender<AccountSettlements>,
    address_signatures: &'a mpsc::Sender<Vec<AddressSignatureRow>>,
}

#[derive(Debug)]
enum SettleError {
    AddressIndexWriterGone,
    Other(String),
}

impl std::fmt::Display for SettleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddressIndexWriterGone => f.write_str("address_signatures writer dropped"),
            Self::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for SettleError {}

impl From<String> for SettleError {
    fn from(s: String) -> Self {
        Self::Other(s)
    }
}

#[derive(Clone)]
struct LastBlock {
    slot: u64,
    blockhash: Hash,
    block_height: u64,
}

/// Align the Redis cache with the Postgres source of truth at startup.
///
/// Redis outlives the process that filled it, so its contents are checked
/// against Postgres before anything reads through them: a cache naming a
/// different deployment, or whose tip does not match Postgres exactly, is
/// emptied. A purged cache is empty rather than wrong, and reads then miss and
/// resolve against Postgres.
///
/// `postgres_slot` and `postgres_blockhash` are the caller's view of the
/// Postgres tip, both taken from the same tip block row. The caller treats a
/// failed read as fatal, so an absent tip here always means a ledger with no
/// blocks rather than a database it could not reach.
///
/// This covers what a restart can see, including a cache left unstamped by an
/// interrupted rebuild. Gaps that open while the settler keeps running are caught
/// per batch by `ensure_cache_continuity` instead.
///
/// Returns whether the cache had to be emptied, which the caller records: a
/// deployment that purges on every boot is two of them sharing one Redis.
pub async fn warm_redis_cache(
    postgres_db: &PostgresAccountsDB,
    redis_db: &RedisAccountsDB,
    postgres_slot: Option<u64>,
    postgres_block_height: Option<u64>,
    postgres_blockhash: Option<Hash>,
) -> Result<bool> {
    info!("Aligning Redis cache with Postgres...");

    let deployment_id = redis_coherence::read_deployment_id(postgres_db).await?;

    let mut purged = false;
    if let Some(reason) =
        redis_coherence::staleness_reason(redis_db, &deployment_id, postgres_slot).await?
    {
        warn!(
            "Purging Redis cache, it cannot serve this ledger: {}",
            reason
        );
        // Cleared first: an interrupted purge leaves the cache unstamped, so the
        // next startup purges again rather than trusting a half-emptied cache.
        redis_coherence::clear_deployment_id(redis_db).await?;
        redis_coherence::purge_ledger_keys(redis_db).await?;
        purged = true;
    }

    let mut conn = redis_db.connection.clone();
    if let Some(slot) = postgres_slot {
        conn.set::<_, _, ()>("latest_slot", slot)
            .await
            .map_err(|e| anyhow!("Failed to write latest_slot to Redis: {}", e))?;
    }
    // Published with the tip, not left to the next mirrored block: a purge clears
    // it, and a replica reading a stale or foreign height would answer
    // getBlockHeight from another ledger's counter.
    if let Some(height) = postgres_block_height {
        conn.set::<_, _, ()>("block_height", height)
            .await
            .map_err(|e| anyhow!("Failed to write block_height to Redis: {}", e))?;
    }
    if let Some(blockhash) = postgres_blockhash {
        conn.set::<_, _, ()>("latest_blockhash", blockhash.to_string())
            .await
            .map_err(|e| anyhow!("Failed to write latest_blockhash to Redis: {}", e))?;
    }

    // Stamped last: the cache only names this deployment once it is coherent
    // with it.
    redis_coherence::stamp_deployment_id(redis_db, &deployment_id).await?;

    info!(
        "Redis cache aligned with Postgres: latest_slot = {:?}",
        postgres_slot
    );
    Ok(purged)
}

pub struct SettleArgs {
    pub execution_results_rx: mpsc::Receiver<ExecutedBatch>,
    pub settled_accounts_tx: mpsc::UnboundedSender<AccountSettlements>,
    pub settled_blockhashes_tx: mpsc::UnboundedSender<Hash>,
    /// Bounded channel to the background `address_index_writer`.
    pub address_signatures_tx: mpsc::Sender<Vec<AddressSignatureRow>>,
    pub accountsdb_connection_url: String,
    /// The cache the read path also attaches to. One knob for both, so a writer
    /// cannot stop mirroring a cache that readers still trust.
    pub redis_cache_url: Option<String>,
    /// Expiry in seconds on cached block entries. Zero disables it.
    pub redis_block_ttl_secs: u64,
    pub blocktime_ms: u64,
    /// How long a cache given up on is left alone before it is probed again. A
    /// field so a test can drive a give-up and its recovery without waiting out
    /// the production one.
    pub cache_mirror_cooldown: Duration,
    pub perf_sample_period_secs: u64,
    pub shutdown_token: CancellationToken,
    pub metrics: SharedMetrics,
    pub heartbeat: Arc<crate::health::StageHeartbeat>,
}

pub async fn start_settle_worker(args: SettleArgs) -> WorkerHandle {
    let SettleArgs {
        execution_results_rx,
        settled_accounts_tx,
        settled_blockhashes_tx,
        address_signatures_tx,
        accountsdb_connection_url,
        redis_cache_url,
        redis_block_ttl_secs,
        blocktime_ms,
        cache_mirror_cooldown,
        perf_sample_period_secs,
        shutdown_token,
        metrics,
        heartbeat,
    } = args;
    let handle = tokio::spawn(async move {
        #[allow(clippy::too_many_arguments)]
        async fn run_settle_worker(
            mut execution_results_rx: mpsc::Receiver<ExecutedBatch>,
            settled_accounts_tx: mpsc::UnboundedSender<AccountSettlements>,
            settled_blockhashes_tx: mpsc::UnboundedSender<Hash>,
            address_signatures_tx: mpsc::Sender<Vec<AddressSignatureRow>>,
            accountsdb_connection_url: String,
            redis_cache_url: Option<String>,
            redis_block_ttl_secs: u64,
            blocktime_ms: u64,
            cache_mirror_cooldown: Duration,
            perf_sample_period_secs: u64,
            shutdown_token: CancellationToken,
            metrics: SharedMetrics,
            heartbeat: Arc<crate::health::StageHeartbeat>,
        ) -> anyhow::Result<()> {
            info!("Settle worker started");

            let mut accounts_db = AccountsDB::new(&accountsdb_connection_url, false)
                .await
                .unwrap();

            // The cache handle carries the Postgres source of truth so reads
            // through it resolve a miss instead of reporting an absence. The
            // settler itself only writes through it.
            let AccountsDB::Postgres(ref postgres_db) = accounts_db else {
                anyhow::bail!("Settle worker requires a Postgres accounts database");
            };
            let postgres_db = postgres_db.clone();

            let mut redis_db: Option<RedisAccountsDB> = match redis_cache_url {
                Some(ref redis_url) => {
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        RedisAccountsDB::new(redis_url, postgres_db.clone()),
                    )
                    .await
                    {
                        Ok(Ok(mut r)) => {
                            info!("Redis cache enabled");
                            r.set_block_ttl_secs(redis_block_ttl_secs);
                            Some(r)
                        }
                        // Connecting is what builds the handle, so there is
                        // nothing here to hold paused and nothing for a probe to
                        // pick up: unlike an alignment failure, this one costs
                        // the cache until the node is restarted.
                        Ok(Err(e)) => {
                            warn!(
                                "Redis unavailable ({}), running Postgres-only until restarted",
                                e
                            );
                            metrics.redis_cache_disabled();
                            None
                        }
                        Err(_) => {
                            warn!(
                                "Redis connection timed out, running Postgres-only until restarted"
                            );
                            metrics.redis_cache_disabled();
                            None
                        }
                    }
                }
                None => {
                    info!("No Redis cache configured, running Postgres-only");
                    None
                }
            };

            // Where this settler resumes, slot and hash from the one tip block row.
            // A failed read is fatal: read as an empty ledger it would restart a
            // live chain at slot 0. `None` means the ledger provably has no blocks.
            let mut last_block = get_tip(&postgres_db)
                .await
                .context("Failed to read the chain tip at startup")?
                .map(|tip| LastBlock {
                    slot: tip.slot,
                    blockhash: tip.blockhash,
                    // The tip row carries the height it was written with, which is
                    // the counter's own seed, so one row still describes one block.
                    block_height: tip.block_height.unwrap_or(tip.slot),
                });
            let last_slot = last_block.as_ref().map(|b| b.slot);
            let last_blockhash = last_block.as_ref().map(|b| b.blockhash);

            // The durable counter wins when present: it continues the last stored
            // block's height across an upgrade. Zero would send the height
            // backwards and invalidate every lastValidBlockHeight clients hold.
            if let Some(height) = accounts_db
                .get_block_height()
                .await
                .context("Failed to read the block height at startup")?
            {
                if let Some(block) = last_block.as_mut() {
                    block.block_height = height;
                }
            }

            // The last tick the node reached, which idle ticks advance past the
            // last block. Resuming below it would replay slots clients have
            // already been told about.
            let last_current_slot = accounts_db
                .get_current_slot()
                .await
                .context("Failed to read the current slot at startup")?;
            let last_block_height = last_block.as_ref().map(|b| b.block_height);

            // Align the cache with Postgres before the first dual-write. Failing
            // to verify it means the cache cannot be trusted, so run Postgres-only
            // rather than write a second ledger into it.
            redis_db = match redis_db {
                Some(redis) => {
                    match warm_redis_cache(
                        &postgres_db,
                        &redis,
                        last_slot,
                        last_block_height,
                        last_blockhash,
                    )
                    .await
                    {
                        Ok(purged) => {
                            if purged {
                                metrics.redis_cache_purged();
                            }
                            Some(redis)
                        }
                        Err(e) => {
                            error!(
                                "Redis cache alignment failed, running Postgres-only until it is probed: {}",
                                e
                            );
                            metrics.redis_cache_disabled();
                            // The failure may have come before the stamp was
                            // cleared, leaving a stamped cache whose tip is about
                            // to stop advancing, and read nodes would serve those
                            // frozen values until its lease ran out.
                            if let Err(e) = redis_coherence::clear_deployment_id(&redis).await {
                                error!("Could not take the Redis cache out of service: {}", e);
                            }
                            // Held paused rather than dropped, so the probe that
                            // recovers a cache lost at runtime also recovers one
                            // that could not be verified at boot.
                            redis.pause_mirroring(cache_mirror_cooldown);
                            Some(redis)
                        }
                    }
                }
                None => None,
            };

            let mut processing_results = Vec::new();
            // Settled bytes since the last block; gates the recv arm below.
            let mut buffered_account_bytes = 0usize;

            // High-water mark of executor generations buffered so far. It is
            // worker-loop state rather than settle state because it describes
            // what the next commit will make durable, not one settle call.
            let mut highest_generation = 0u64;

            // Tick-driven block production: the blocktime tick is the sole
            // trigger for producing blocks.  Between ticks, execution results
            // accumulate in `processing_results`.  On each tick, everything is
            // flushed in a single settle call — could be 0 txs, could be 2000.
            //
            // MissedTickBehavior::Delay ensures that if a settle takes longer
            // than blocktime_ms, the next tick is pushed out rather than
            // bursting to catch up.  This guarantees:
            //   - Exactly one block per tick
            //   - Ticks are never faster than blocktime_ms
            //   - Under slow DB, rate degrades gracefully instead of bursting
            let mut blocktime_interval = tokio::time::interval_at(
                Instant::now() + Duration::from_millis(SETTLE_START_DELAY_MS),
                Duration::from_millis(blocktime_ms),
            );
            blocktime_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            // Performance sample tracking
            let mut perf_sample_interval = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(perf_sample_period_secs),
                Duration::from_secs(perf_sample_period_secs),
            );
            let mut perf_start_slot = last_block.as_ref().map(|b| b.slot).unwrap_or(0);
            let mut perf_num_transactions = 0u64;

            // Advances every tick, so it is worker state rather than derived from
            // the last block. A restart resumes from the durable tick counter,
            // which idle ticks keep moving, so the slot never goes backwards.
            let mut next_slot = match last_block.as_ref() {
                Some(block) => last_current_slot.unwrap_or(block.slot).max(block.slot) + 1,
                None => 0,
            };
            // Tick deadlines, not wall-clock reads: the heartbeat check runs once
            // per tick and must not cost a clock syscall.
            let mut last_block_at = Instant::now();

            loop {
                // `biased` keeps block cadence crisp under sustained load:
                // shutdown is handled promptly, the blocktime tick is polled
                // before the (almost-always-ready) result-buffer arm so a
                // tick is never delayed by an arbitrary number of recvs, and
                // MissedTickBehavior::Delay won't slide the schedule out.
                // Exits when the executor closes its results channel, not on the
                // shutdown signal, so every executed batch reaches a commit. The
                // signal still reaches settle_with_retry, where it shortens a
                // running retry to fit the node's drain deadline.
                tokio::select! {
                    biased;

                    // Blocktime tick: produce a block when this tick carried work
                    // or the heartbeat is due, and advance the slot either way.
                    tick = blocktime_interval.tick() => {
                        // A cooldown that has run out earns a probe, off the block
                        // path. Claimed first, so a cooldown lapsing under a slow
                        // probe cannot start a second one, and re-armed so the
                        // cache stays out of the mirror until a probe lifts it.
                        if let Some(paused) =
                            redis_db.as_ref().filter(|redis| redis.pause_has_lapsed())
                        {
                            if paused.try_begin_probe() {
                                paused.pause_mirroring(cache_mirror_cooldown);
                                tokio::spawn(probe_paused_cache(
                                    paused.clone(),
                                    Arc::clone(&metrics),
                                ));
                            }
                        }

                        let num_results = processing_results.len();
                        if !should_produce_block(
                            num_results > 0,
                            last_block.is_none(),
                            tick.saturating_duration_since(last_block_at),
                        ) {
                            // The chain is on this slot even though no block
                            // backs it. Only reached when the tick carried no
                            // work, so the loaded path never pays for it.
                            if let Err(e) =
                                publish_idle_slot(&postgres_db, redis_db.as_ref(), next_slot).await
                            {
                                warn!("Failed to publish idle slot {next_slot}, the next tick republishes: {e:#}");
                                metrics.settler_idle_slot_publish_failed();
                            }
                            next_slot += 1;
                            continue;
                        }
                        if buffered_account_bytes >= MAX_BUFFERED_SETTLE_BYTES {
                            metrics.settler_backpressure_engaged();
                        }
                        match settle_with_retry(
                            next_slot,
                            last_block.clone(),
                            &mut accounts_db,
                            redis_db.as_mut(),
                            &processing_results,
                            &metrics,
                            Some(BlockPublishers {
                                blockhashes: &settled_blockhashes_tx,
                                accounts: &settled_accounts_tx,
                                address_signatures: &address_signatures_tx,
                            }),
                            highest_generation,
                            &heartbeat,
                            &shutdown_token,
                        )
                        .await
                        {
                            Ok(settle_result) => {
                                heartbeat.record_progress();
                                perf_num_transactions += num_results as u64;
                                if num_results > 0 {
                                    metrics.settler_txs_settled(num_results);
                                }

                                last_block = Some(LastBlock {
                                    slot: settle_result.slot,
                                    blockhash: settle_result.blockhash,
                                    block_height: settle_result.block_height,
                                });
                                last_block_at = tick;
                                next_slot += 1;
                                processing_results.clear();
                                buffered_account_bytes = 0;
                                metrics.settler_buffered_account_bytes(0);
                                debug!(
                                    "Settled {} transactions in slot {}, blockhash {}",
                                    num_results,
                                    settle_result.slot,
                                    settle_result.blockhash
                                );
                                // Both announcements are attempted before the
                                // commit returns, so stopping here cannot leave
                                // this block half-announced.
                                //
                                // A drain retires dedup and the executor ahead of
                                // this stage, so their channels closing is the
                                // expected end state and must not cut the drain
                                // short while the executor is still feeding us.
                                // Outside a drain it means a stage died, which
                                // still stops the node.
                                if settle_result.publisher_gone && !shutdown_token.is_cancelled() {
                                    break;
                                }
                            }
                            Err(SettleError::AddressIndexWriterGone) => {
                                anyhow::bail!(
                                    "address_signatures writer dropped, aborting settler"
                                );
                            }
                            Err(SettleError::Other(msg)) => {
                                error!("Failed to settle transactions: {}", msg);
                                // Cleared here so the final flush below does not
                                // spend a second budget on the same doomed batch.
                                discard_buffer(
                                    &msg,
                                    &mut processing_results,
                                    &mut execution_results_rx,
                                    &metrics,
                                );
                                metrics.settler_buffered_account_bytes(0);
                                break;
                            }
                        }

                        // Cheaper to stop mirroring than to pay the budget every
                        // block, but a failover must not cost the cache for the
                        // life of the node: the probe above brings back one that
                        // recovers.
                        if let Some(failing) = redis_db.as_ref().filter(|redis| {
                            !redis.is_mirroring_paused() && redis.has_failed_too_often()
                        }) {
                            failing.pause_mirroring(cache_mirror_cooldown);
                            warn!(
                                "Redis cache failed {} batches it never made up, leaving it alone for {:?}",
                                CACHE_FAILURE_LIMIT, cache_mirror_cooldown
                            );
                            metrics.redis_cache_disabled();
                            // Readers off now, not when the lease runs out.
                            take_cache_out_of_service(failing.clone());
                        }
                    }

                    // Save performance sample periodically
                    _ = perf_sample_interval.tick() => {
                        if let Some(ref current_block) = last_block {
                            let current_slot = current_block.slot;
                            let num_slots = current_slot.saturating_sub(perf_start_slot);

                            let sample = RpcPerfSample {
                                slot: current_slot,
                                num_transactions: perf_num_transactions,
                                num_slots,
                                sample_period_secs: perf_sample_period_secs as u16,
                                // In PrivateChannel, all transactions are non-vote transactions
                                num_non_vote_transactions: Some(perf_num_transactions),
                            };

                            if let Err(e) = accounts_db.store_performance_sample(sample).await {
                                warn!("Failed to store performance sample: {:?}", e);
                            } else {
                                debug!("Stored performance sample for slot {}: {} txs over {} slots",
                                    current_slot, perf_num_transactions, num_slots);
                            }

                            // Reset counters for next period
                            perf_start_slot = current_slot;
                            perf_num_transactions = 0;
                        }
                    }

                    // Buffer execution results, never settle them here.
                    // Over budget, this arm switches off and the queue backs up,
                    // which parks the executor until the tick below drains us.
                    result = execution_results_rx.recv(),
                        if buffered_account_bytes < MAX_BUFFERED_SETTLE_BYTES => {
                        match result {
                            Some((svm_output, transactions, generation)) => {
                                heartbeat.record_input();
                                debug!("Settle worker received output with {} transactions", transactions.len());
                                if svm_output.processing_results.len() != transactions.len() {
                                    error!("Processing results and transactions length mismatch");
                                    break;
                                }
                                debug!("Extending {} processing results", svm_output.processing_results.len());
                                // Measured before the extend consumes both vectors.
                                buffered_account_bytes += retained_account_bytes(
                                    &svm_output.processing_results,
                                    &transactions,
                                );
                                metrics.settler_buffered_account_bytes(buffered_account_bytes);
                                processing_results.extend(svm_output.processing_results.into_iter().zip(transactions.into_iter()));
                                // Fold only once the batch is buffered, so the watermark can
                                // never cover a batch that was rejected above and therefore
                                // never committed. max() and not assignment so a producer
                                // that ever regresses cannot pull the watermark backwards.
                                highest_generation = highest_generation.max(generation);
                            }
                            None => {
                                info!("Settle worker stopped - channel closed");
                                break;
                            }
                        }
                    }

                }
            }

            // Flush any results buffered between the last tick and the loop
            // exit — without this, the final partial block is silently dropped
            if !processing_results.is_empty() {
                let num_results = processing_results.len();
                match settle_with_retry(
                    next_slot,
                    last_block.clone(),
                    &mut accounts_db,
                    redis_db.as_mut(),
                    &processing_results,
                    &metrics,
                    Some(BlockPublishers {
                        blockhashes: &settled_blockhashes_tx,
                        accounts: &settled_accounts_tx,
                        address_signatures: &address_signatures_tx,
                    }),
                    highest_generation,
                    &heartbeat,
                    &shutdown_token,
                )
                .await
                {
                    Ok(settle_result) => {
                        if num_results > 0 {
                            metrics.settler_txs_settled(num_results);
                        }
                        info!(
                            "Final flush settled {} buffered transactions in slot {}",
                            num_results, settle_result.slot
                        );
                    }
                    // Final flush runs while the node is already shutting down; the
                    // block is durably committed before the addr-index send and the
                    // index is rebuilt from Postgres on restart, so no txs are lost.
                    Err(SettleError::AddressIndexWriterGone) => {
                        warn!(
                            "Final flush: address-index writer gone after committing {} buffered transactions; block durable, index rebuilt on restart",
                            num_results
                        );
                    }
                    Err(e) => {
                        discard_buffer(
                            &e.to_string(),
                            &mut processing_results,
                            &mut execution_results_rx,
                            &metrics,
                        );
                    }
                }
                processing_results.clear();
                buffered_account_bytes = 0;
                metrics.settler_buffered_account_bytes(buffered_account_bytes);
            }

            info!("Settle worker stopped");
            Ok(())
        }

        if let Err(e) = run_settle_worker(
            execution_results_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url,
            redis_cache_url,
            redis_block_ttl_secs,
            blocktime_ms,
            cache_mirror_cooldown,
            perf_sample_period_secs,
            shutdown_token,
            metrics,
            heartbeat,
        )
        .await
        {
            error!("Settle worker failed: {:?}", e);
        }
    });

    WorkerHandle::new("Settle".to_string(), handle)
}

/// Derive a block's hash by SHA-256-hashing the parent hash, the slot, a
/// high-resolution timestamp, and every transaction signature in the block.
/// Folding in the nanosecond timestamp and the signatures makes a future block's hash
/// unpredictable.
fn compute_blockhash(
    previous: &Hash,
    slot: u64,
    time_nanos: u128,
    signatures: &[Signature],
) -> Hash {
    let slot_bytes = slot.to_le_bytes();
    let time_bytes = time_nanos.to_le_bytes();
    let mut parts: Vec<&[u8]> = Vec::with_capacity(3 + signatures.len());
    parts.push(previous.as_ref());
    parts.push(&slot_bytes);
    parts.push(&time_bytes);
    parts.extend(signatures.iter().map(|s| s.as_ref()));
    hashv(&parts)
}

/// Record executed transactions that could not be committed, then drop them.
///
/// The only place a buffer clears without a commit, so the record is written
/// first. Queued results fold in; a DB-backed one would need the dead database.
fn discard_buffer(
    reason: &str,
    processing_results: &mut Vec<(TransactionProcessingResult, SanitizedTransaction)>,
    execution_results_rx: &mut mpsc::Receiver<ExecutedBatch>,
    metrics: &crate::stage_metrics::SharedMetrics,
) {
    // Closed before the drain, not just emptied: every `try_recv` frees a slot
    // and the executor may be parked on a send waiting for exactly that slot,
    // so a snapshot lets a batch land behind the record and die with the
    // receiver. Closing refuses it instead, and the executor records its own.
    execution_results_rx.close();
    while let Ok((svm_output, transactions, _generation)) = execution_results_rx.try_recv() {
        processing_results.extend(
            svm_output
                .processing_results
                .into_iter()
                .zip(transactions.into_iter()),
        );
    }

    let signatures: Vec<Signature> = processing_results
        .iter()
        .map(|(_, transaction)| *transaction.signature())
        .collect();
    processing_results.clear();

    crate::stages::record_discarded("settler", reason, &signatures, metrics);
}

/// Commit a batch, retrying a storage failure until the budget runs out.
///
/// The whole batch is either committed or reported as failed, never split, so
/// every attempt rebuilds the same block from the same pinned timestamp. That
/// matters because a commit can succeed on the server and still fail to
/// acknowledge; the retry then rewrites byte-identical rows instead of minting
/// a second blockhash for a slot a replica may already be serving.
#[allow(clippy::too_many_arguments)]
async fn settle_with_retry(
    next_slot: u64,
    last_block: Option<LastBlock>,
    accounts_db: &mut AccountsDB,
    mut redis_db: Option<&mut RedisAccountsDB>,
    processing_results: &[(TransactionProcessingResult, SanitizedTransaction)],
    metrics: &crate::stage_metrics::SharedMetrics,
    publishers: Option<BlockPublishers<'_>>,
    generation: u64,
    heartbeat: &crate::health::StageHeartbeat,
    shutdown_token: &CancellationToken,
) -> Result<SettleResult, SettleError> {
    // One clock read for the whole call, shared by every attempt.
    let block_time_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let has_work = !processing_results.is_empty();
    let mut backoff = SETTLE_RETRY_BACKOFF_BASE;
    let mut attempts = 0u32;

    // Cancelling swaps the budget for a shorter one measured from the moment it
    // arrives, so an attempt already in flight is spent out of the drain's time
    // rather than before it. Both are hard caps: no attempt starts that cannot
    // finish inside the deadline that applies to it.
    let mut draining = shutdown_token.is_cancelled();
    let mut deadline = Instant::now()
        + if draining {
            SETTLE_SHUTDOWN_BUDGET
        } else {
            SETTLE_RETRY_BUDGET
        };

    loop {
        // Health compares last input against last progress, so without a bump both
        // freeze mid-retry and read healthy. Idle first attempts stay exempt.
        if has_work || attempts > 0 {
            heartbeat.record_input();
        }
        attempts += 1;

        let commit_timeout = attempt_timeout(draining, deadline);
        let attempt = settle_transactions(
            next_slot,
            last_block.clone(),
            accounts_db,
            redis_db.as_deref_mut(),
            processing_results,
            metrics,
            publishers,
            generation,
            block_time_nanos,
            commit_timeout,
        );

        let outcome = if draining {
            attempt.await
        } else {
            tokio::pin!(attempt);
            tokio::select! {
                result = &mut attempt => result,
                _ = shutdown_token.cancelled() => {
                    draining = true;
                    deadline = Instant::now() + SETTLE_SHUTDOWN_BUDGET;
                    match tokio::time::timeout_at(deadline, &mut attempt).await {
                        Ok(result) => result,
                        Err(_) => Err(SettleError::Other(
                            "settle attempt outlived the shutdown budget".to_string(),
                        )),
                    }
                }
            }
        };

        let error = match outcome {
            Ok(settle_result) => return Ok(settle_result),
            // Raised only after the commit landed, so the block is already
            // durable and retrying would write it a second time. The index it
            // failed to reach is rebuilt from Postgres on the next boot.
            Err(SettleError::AddressIndexWriterGone) => {
                return Err(SettleError::AddressIndexWriterGone)
            }
            Err(e) => e,
        };

        // Cancellation may have arrived while a draining attempt was running.
        if !draining && shutdown_token.is_cancelled() {
            draining = true;
            deadline = deadline.min(Instant::now() + SETTLE_SHUTDOWN_BUDGET);
        }

        // Counted in full, so the budget bounds the call rather than the moment
        // the last attempt starts.
        if Instant::now() + backoff + attempt_timeout(draining, deadline) > deadline {
            return Err(error);
        }

        warn!("Settle attempt failed, retrying in {backoff:?}: {error}");
        metrics.settler_settle_retried();
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown_token.cancelled(), if !draining => {
                draining = true;
                deadline = deadline.min(Instant::now() + SETTLE_SHUTDOWN_BUDGET);
            }
        }
        backoff = (backoff * 2).min(SETTLE_RETRY_BACKOFF_MAX);
    }
}

/// The most one attempt may take: its own bound, never past the deadline.
fn attempt_timeout(draining: bool, deadline: Instant) -> Duration {
    let bound = if draining {
        SETTLE_SHUTDOWN_ATTEMPT_TIMEOUT
    } else {
        SETTLE_ATTEMPT_TIMEOUT
    };
    bound.min(deadline.saturating_duration_since(Instant::now()))
}

/// Settle transactions: Update accounts database with changes
#[allow(clippy::too_many_arguments)]
async fn settle_transactions(
    // The tick slot this block occupies. It is not `last_block.slot + 1`: idle
    // ticks advance the slot without producing a block.
    next_slot: u64,
    last_block: Option<LastBlock>,
    accounts_db: &mut AccountsDB,
    redis_db: Option<&mut RedisAccountsDB>,
    processing_results: &[(TransactionProcessingResult, SanitizedTransaction)],
    metrics: &crate::stage_metrics::SharedMetrics,
    publishers: Option<BlockPublishers<'_>>,
    // High-water mark of executor generations covered by this commit. Read at
    // the call site so it can never fold in a batch buffered after the flush.
    generation: u64,
    // Sampled by the caller, never here, so a retry rebuilds the same block.
    block_time_nanos: u128,
    // Bounds the commit only: replaying the post-commit publishes would evict a still-live dedup entry.
    commit_timeout: Duration,
) -> Result<SettleResult, SettleError> {
    let t_total = tokio::time::Instant::now();
    // Preallocate per-tick collections from the known result count so the hot
    // path doesn't pay the geometric-growth realloc tax on every tick. The 4×
    // hint absorbs SPL/ATA-creation flows where a single tx can write to up to
    // four accounts; transfers stay well under the load factor.
    let n = processing_results.len();
    let mut final_accounts_actual: HashMap<Pubkey, AccountSettlement> =
        HashMap::with_capacity(n * 4);

    // Derived rather than passed alongside, so the seconds and the nanoseconds
    // in a block can never disagree about when it was produced.
    let block_time = (block_time_nanos / 1_000_000_000) as i64;

    // The parent is the last block actually produced, which on a sparse chain is
    // not the previous slot. The indexer's continuity walk requires that exact
    // linkage or it stops advancing.
    let (last_blockhash, parent_slot, next_height, is_genesis) = match last_block {
        Some(ref lb) => (lb.blockhash, lb.slot, lb.block_height + 1, false),
        None => (Hash::default(), next_slot, 0, true),
    };

    // Phase 1: build account maps and transaction lists
    let t_processing_start = tokio::time::Instant::now();
    let mut block_transaction_signatures = Vec::with_capacity(n);
    let mut block_transaction_recent_blockhashes = Vec::with_capacity(n);
    let mut block_transaction_message_hashes = Vec::with_capacity(n);
    let mut transactions_for_db = Vec::with_capacity(n);

    for (processing_result, sanitized_transaction) in processing_results.iter() {
        let signature = sanitized_transaction.signature();
        let recent_blockhash = *sanitized_transaction.message().recent_blockhash();
        let message_hash = *sanitized_transaction.message_hash();

        // Only collect successful transactions for batch write
        if let Ok(processed_tx) = processing_result {
            transactions_for_db.push((
                *signature,
                sanitized_transaction,
                next_slot,
                block_time,
                processed_tx,
            ));
        }

        match processing_result {
            Ok(ProcessedTransaction::Executed(executed_tx)) => {
                debug!(
                    "Executed transaction: {:?}",
                    sanitized_transaction.signature()
                );

                // A failed executed tx (status Err) holds rolled-back intermediate state; record its signature but persist no account writes.
                if executed_tx.was_successful() {
                    for (index, (pubkey, account_data)) in
                        executed_tx.loaded_transaction.accounts.iter().enumerate()
                    {
                        if sanitized_transaction.is_writable(index) {
                            // Zero lamports means the account is gone, whatever its data
                            // holds. Must match BOB's rule or its entry never reconciles.
                            let deleted = account_data.lamports() == 0;
                            // A delete carries no bytes to Postgres, Redis or BOB, so
                            // don't pin the buffer in the unbounded feedback channel.
                            let account = if deleted {
                                AccountSharedData::default()
                            } else {
                                account_data.clone()
                            };
                            final_accounts_actual
                                .insert(*pubkey, AccountSettlement { account, deleted });
                        }
                    }
                }

                block_transaction_signatures.push(*signature);
                block_transaction_recent_blockhashes.push(recent_blockhash);
                block_transaction_message_hashes.push(message_hash);
            }
            Ok(ProcessedTransaction::FeesOnly(fees_only_transaction)) => {
                warn!("FeesOnly transaction: {:?}", fees_only_transaction);

                // For fees-only transactions, we just record the transaction
                // The rollback accounts have already been handled by SVM
                // and fees have been deducted

                block_transaction_signatures.push(*signature);
                block_transaction_recent_blockhashes.push(recent_blockhash);
                block_transaction_message_hashes.push(message_hash);
            }
            Err(e) => {
                warn!("Transaction failed: {:?}, error: {:?}", signature, e);
                // Failed transactions still get recorded
                block_transaction_signatures.push(*signature);
                block_transaction_recent_blockhashes.push(recent_blockhash);
                block_transaction_message_hashes.push(message_hash);
            }
        }
    }

    let t_processing_ms = t_processing_start.elapsed().as_secs_f64() * 1000.0;

    let next_blockhash = if is_genesis {
        Hash::default()
    } else {
        compute_blockhash(
            &last_blockhash,
            next_slot,
            block_time_nanos,
            &block_transaction_signatures,
        )
    };

    // Convert final_accounts to Vec for batch write
    let accounts_vec: Vec<(Pubkey, AccountSettlement)> =
        final_accounts_actual.into_iter().collect();

    // Create block info
    let block_info = BlockInfo {
        slot: next_slot,
        blockhash: next_blockhash,
        previous_blockhash: last_blockhash,
        parent_slot,
        block_height: Some(next_height),
        block_time: Some(block_time),
        transaction_signatures: block_transaction_signatures,
        transaction_recent_blockhashes: block_transaction_recent_blockhashes,
        transaction_message_hashes: block_transaction_message_hashes,
    };

    // Phase 2: Postgres write (source of truth, fatal on failure)
    let t_db_start = tokio::time::Instant::now();
    let addr_sig_rows = tokio::time::timeout(
        commit_timeout,
        accounts_db.write_batch(
            &accounts_vec,
            transactions_for_db.clone(),
            Some(block_info.clone()),
        ),
    )
    .await
    .map_err(|_| {
        SettleError::Other(format!(
            "commit exceeded {commit_timeout:?} and was abandoned"
        ))
    })??;
    let t_db_ms = t_db_start.elapsed().as_secs_f64() * 1000.0;

    // Both announcements go out before any optional work: the ack unpins the
    // settled accounts in BOB and the hash is what RPC advertises. A cache stall
    // after this point can no longer hold the acceptance window behind the
    // committed state.
    let mut publisher_gone = false;
    if let Some(ref publishers) = publishers {
        if publishers
            .accounts
            .send(AccountSettlements {
                generation,
                accounts: accounts_vec.clone(),
            })
            .is_err()
        {
            warn!("Failed to publish the settled accounts");
            publisher_gone = true;
        }
        if publishers.blockhashes.send(next_blockhash).is_err() {
            warn!("Failed to publish the settled blockhash to dedup");
            publisher_gone = true;
        }

        // Closed channel = writer gone; escalate to exit.
        if !addr_sig_rows.is_empty() {
            let send_t0 = tokio::time::Instant::now();
            match publishers.address_signatures.send(addr_sig_rows).await {
                Ok(()) => {
                    metrics.address_signatures_send_blocked_ms(
                        send_t0.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                Err(_) => {
                    return Err(SettleError::AddressIndexWriterGone);
                }
            }
        }
    }

    // Phase 3: Redis write best-effort (non-fatal)
    let t_redis_start = tokio::time::Instant::now();
    if let Some(redis) = redis_db {
        let mirrored = tokio::time::timeout(
            CACHE_MIRROR_BUDGET,
            mirror_batch_to_cache(
                redis,
                next_slot,
                block_info.parent_slot,
                &accounts_vec,
                transactions_for_db,
                block_info,
                metrics,
            ),
        )
        .await;

        // One accounting event per batch, here rather than inside the attempt,
        // so a batch cannot be counted twice.
        let failed = match mirrored {
            Ok(MirrorOutcome::Mirrored) => {
                redis.record_cache_success();
                false
            }
            Ok(MirrorOutcome::Skipped) => false,
            Ok(MirrorOutcome::Failed) => true,
            // Dropped mid-command, so the tip may or may not have moved. The
            // next continuity check rebuilds if a gap opened.
            Err(_) => {
                warn!(
                    "Redis cache write exceeded its {:?} budget, abandoning this batch",
                    CACHE_MIRROR_BUDGET
                );
                take_cache_out_of_service(redis.clone());
                true
            }
        };
        if failed {
            metrics.redis_cache_write_failed();
            redis.record_cache_failure();
        }
    }
    let t_redis_ms = t_redis_start.elapsed().as_secs_f64() * 1000.0;
    let t_total_ms = t_total.elapsed().as_secs_f64() * 1000.0;

    let num_txs = processing_results.len();
    debug!(
        "settle_batch complete: txs={} | processing={:.3}ms db_write={:.3}ms redis={:.3}ms total={:.3}ms",
        num_txs, t_processing_ms, t_db_ms, t_redis_ms, t_total_ms
    );
    metrics.settler_settle_duration_ms(t_total_ms);
    metrics.settler_db_write_duration_ms(t_db_ms);
    metrics.settler_processing_duration_ms(t_processing_ms);

    Ok(SettleResult {
        slot: next_slot,
        blockhash: next_blockhash,
        publisher_gone,
        block_height: next_height,
    })
}

/// How long a spawned eviction may run. Off the block path, so it can outlast a
/// mirror budget.
const CACHE_EVICTION_TIMEOUT: Duration = Duration::from_secs(2);

/// Clears the deployment stamp, so read nodes stop serving the cache.
///
/// Spawned because callers get here having just watched Redis fail; awaiting it
/// would put that back on the block path. The lease is the backstop if it never
/// lands, and a late one costs a block of Postgres reads.
fn take_cache_out_of_service(redis: RedisAccountsDB) {
    tokio::spawn(async move {
        match tokio::time::timeout(
            CACHE_EVICTION_TIMEOUT,
            redis_coherence::clear_deployment_id(&redis),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Could not take the Redis cache out of service: {}", e),
            Err(_) => warn!("Timed out taking the Redis cache out of service"),
        }
    });
}

/// Empties a condemned cache and stamps it back into service. Spawned, because
/// purging a large keyspace costs far more than a blocktime.
fn rebuild_in_background(redis: &RedisAccountsDB, metrics: &SharedMetrics) {
    metrics.redis_cache_condemned();
    let rebuilding = redis.clone();
    let rebuild_metrics = Arc::clone(metrics);
    let generation = redis.condemnation_generation();
    tokio::spawn(async move {
        match redis_coherence::rebuild_cache(&rebuilding, generation).await {
            Ok(()) => rebuild_metrics.redis_cache_purged(),
            Err(e) => {
                // The batch that condemned was skipped rather than failed, so
                // this is the only thing that counts a cache which cannot be
                // purged at all. Without it such a cache condemns every block
                // and is never given up on.
                rebuilding.record_cache_failure();
                warn!("Redis cache rebuild failed, it stays out of service: {}", e);
            }
        }
    });
}

/// Finds out whether a cache the settler gave up on can be used again, and puts
/// it back on the mirror path when it can.
///
/// Spawned by a caller that has claimed the probe and re-armed the cooldown.
/// Nothing here is on the block path, and the pause outlives the probe, so a
/// probe that hangs costs a block nothing and no batch is offered to a cache that
/// has not passed one.
///
/// A PING and not a batch, because a cache that answers reads but cannot take a
/// batch inside the budget would otherwise cost a block on every cooldown. What
/// answers is condemned before it is trusted again, since it holds pre-pause
/// values and a tip that eviction could have taken is no longer proof of that.
async fn probe_paused_cache(redis: RedisAccountsDB, metrics: SharedMetrics) {
    let probe = async {
        let mut conn = redis.connection.clone();
        let pong: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
        pong.context("cache did not answer a PING")?;

        // Only a condemnation this probe made is proof that this pause's cache is
        // being purged. A rebuild already holding the claim is purging for an
        // older condemnation, and resuming on it would mirror into whatever that
        // purge has not reached.
        let condemned = redis_coherence::condemn_cache(&redis, "was given up on and answers again")
            .await?
            == redis_coherence::CacheStatus::Condemned;
        if condemned {
            rebuild_in_background(&redis, &metrics);
        }
        anyhow::Ok(condemned)
    };

    // Cancelling part-way through is safe: the rebuild claim is released on
    // drop, and the cache stays paused for the next probe either way.
    match tokio::time::timeout(CACHE_PROBE_TIMEOUT, probe).await {
        Ok(Ok(true)) => {
            redis.resume_mirroring();
            info!("Redis cache answers again, mirroring resumes once it is rebuilt");
        }
        Ok(Ok(false)) => {
            info!("Redis cache is still being rebuilt, leaving it alone until the next probe")
        }
        Ok(Err(e)) => warn!("Redis cache is still unusable, leaving it alone: {:#}", e),
        Err(_) => warn!(
            "Redis cache did not finish a probe in {:?}, leaving it alone",
            CACHE_PROBE_TIMEOUT
        ),
    }
    // Released however the probe ended, or nothing would probe this cache again.
    redis.finish_probe();
}

/// What a mirror attempt did. Returned, not recorded here, so the caller counts
/// the batch once even when it ran out of budget.
enum MirrorOutcome {
    /// In the cache, lease renewed.
    Mirrored,
    /// Not mirrored on purpose: the cache is being rebuilt. Redis is fine.
    Skipped,
    /// The cache could not take the batch.
    Failed,
}

/// Mirrors a committed batch into the cache. Nothing here is fatal; Postgres
/// already holds it. Its own function so the caller can bound the whole phase.
async fn mirror_batch_to_cache(
    redis: &mut RedisAccountsDB,
    slot: u64,
    parent_slot: u64,
    accounts_vec: &[(Pubkey, AccountSettlement)],
    transactions_for_db: Vec<(
        Signature,
        &SanitizedTransaction,
        u64,
        i64,
        &ProcessedTransaction,
    )>,
    block_info: BlockInfo,
    metrics: &SharedMetrics,
) -> MirrorOutcome {
    // A cache given up on is left alone until its cooldown is up: paying the
    // mirror budget every block is what it was given up on for. Not a failure,
    // so the tally stays where the give-up left it.
    if redis.is_mirroring_paused() {
        return MirrorOutcome::Skipped;
    }

    // Checked before the write, because the write advances the cached tip
    // and the tip is what shows a batch was missed.
    match redis_coherence::ensure_cache_continuity(redis, parent_slot, slot).await {
        Ok(redis_coherence::CacheStatus::InService) => {}
        // A purge is already walking this cache. Writing to it would renew
        // the lease and put back into service the stale keys that purge has
        // not reached yet, and starting a second one would supersede it.
        Ok(redis_coherence::CacheStatus::Rebuilding) => return MirrorOutcome::Skipped,
        // The check has already cleared the stamp, so nothing is served from
        // the cache until the rebuild finishes and stamps it again. Spawned
        // because purging a large keyspace costs far more than a blocktime.
        //
        // Mirroring stops until that rebuild is done. The purge would sweep
        // most of what this wrote anyway, and a successful write renews the
        // lease, which would put the stale keys the purge has not reached
        // yet straight back into service.
        Ok(redis_coherence::CacheStatus::Condemned) => {
            rebuild_in_background(redis, metrics);
            return MirrorOutcome::Skipped;
        }
        // Whether a batch was missed is now unknown, so this write must not
        // happen: it would advance the cached tip over any gap that does
        // exist, erasing the only evidence of it. Skipping leaves the tip
        // where it is, so the next check that succeeds still sees the gap.
        //
        // Blocks keep being produced either way, so the cache has to stop
        // being served now rather than whenever its lease runs out. That
        // often fails too, since the check just failed against the same
        // Redis, and the lease is what covers that.
        Err(e) => {
            warn!("Skipping the Redis cache write, continuity unknown: {}", e);
            take_cache_out_of_service(redis.clone());
            return MirrorOutcome::Failed;
        }
    }

    match crate::accounts::write_batch::write_batch_redis(
        redis,
        accounts_vec,
        transactions_for_db,
        Some(block_info),
    )
    .await
    {
        // Renewing only on a mirrored batch is what ties trust to
        // maintenance: a writer that stops mirroring stops extending the
        // lease, whether or not it is still able to reach Redis to say
        // so.
        Ok(()) => match redis_coherence::stamp_deployment_id(redis, redis.deployment_id()).await {
            Ok(()) => MirrorOutcome::Mirrored,
            // A failure even though the write landed: the tip advanced, so no
            // continuity check will notice the lease lapsing.
            Err(e) => {
                warn!("Failed to renew the Redis cache lease: {}", e);
                MirrorOutcome::Failed
            }
        },
        Err(e) => {
            warn!(
                "Best-effort Redis cache write failed (non-fatal, Postgres succeeded): {}",
                e
            );
            // The account keys this batch should have updated still hold
            // pre-batch balances. Drop them now so reads miss and resolve
            // against Postgres, rather than waiting for the next batch to
            // notice the tip did not advance and rebuild the whole cache.
            crate::accounts::write_batch::invalidate_batch_redis(redis, accounts_vec).await;
            MirrorOutcome::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        stage_metrics::{NoopMetrics, SharedMetrics},
        test_helpers::{
            create_test_sanitized_transaction, postgres_container_url, start_test_postgres,
            start_test_redis,
        },
    };
    use solana_sdk::{
        account::AccountSharedData,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
    };
    use solana_svm::account_loader::{FeesOnlyTransaction, LoadedTransaction};
    use solana_svm::rollback_accounts::RollbackAccounts;
    use solana_svm::transaction_execution_result::{
        ExecutedTransaction, TransactionExecutionDetails,
    };
    use solana_svm::transaction_processor::LoadAndExecuteSanitizedTransactionsOutput;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    use crate::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY as RESULTS_CAP;
    use crate::stage_metrics::PrometheusMetrics;

    /// Settle `results` and capture the accounts published on the broadcast
    /// channel (the path the worker consumes), returning them with the result so
    /// tests assert on the real settlement output rather than an internal field.
    async fn settle_capturing_accounts(
        last_block: Option<LastBlock>,
        db: &mut AccountsDB,
        results: &[(TransactionProcessingResult, SanitizedTransaction)],
    ) -> (SettleResult, Vec<(Pubkey, AccountSettlement)>) {
        let (accounts_tx, mut accounts_rx) = mpsc::unbounded_channel();
        // Live but undrained: this helper only asserts on the accounts channel,
        // and a dropped receiver would fail the send and flip `publisher_gone`.
        let (blockhashes_tx, _blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(RESULTS_CAP);
        // The next slot after the parent: a busy node produces a block per tick.
        let next_slot = last_block.as_ref().map(|b| b.slot + 1).unwrap_or(0);
        let result = settle_transactions(
            next_slot,
            last_block,
            db,
            None,
            results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            Some(BlockPublishers {
                blockhashes: &blockhashes_tx,
                accounts: &accounts_tx,
                address_signatures: &address_signatures_tx,
            }),
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .expect("settle_transactions succeeded");
        let mut settled = Vec::new();
        while let Ok(batch) = accounts_rx.try_recv() {
            settled.extend(batch.accounts);
        }
        (result, settled)
    }

    fn make_executed(
        accounts: Vec<(solana_sdk::pubkey::Pubkey, AccountSharedData)>,
    ) -> ProcessedTransaction {
        ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts,
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 100,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: std::collections::HashMap::new(),
        }))
    }

    /// `make_executed` with an `Err` status: an executed tx that failed mid-transaction holding rolled-back accounts.
    fn make_failed_executed(
        accounts: Vec<(solana_sdk::pubkey::Pubkey, AccountSharedData)>,
    ) -> ProcessedTransaction {
        ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts,
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Err(
                    solana_transaction_error::TransactionError::InstructionError(
                        1,
                        solana_sdk::instruction::InstructionError::Custom(0),
                    ),
                ),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 100,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: std::collections::HashMap::new(),
        }))
    }

    fn sig(byte: u8) -> Signature {
        Signature::from([byte; 64])
    }

    /// One batch whose single writable account carries `bytes` of data.
    fn sized_settle_batch(
        bytes: usize,
    ) -> (
        LoadAndExecuteSanitizedTransactionsOutput,
        Vec<SanitizedTransaction>,
    ) {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let executed = make_executed(vec![
            (
                from.pubkey(),
                AccountSharedData::new(1, bytes, &Pubkey::default()),
            ),
            (to, AccountSharedData::new(1, 0, &Pubkey::default())),
        ]);
        (
            LoadAndExecuteSanitizedTransactionsOutput {
                processing_results: vec![Ok(executed)],
                error_metrics: Default::default(),
                execute_timings: Default::default(),
                balance_collector: None,
            },
            vec![tx],
        )
    }

    /// Highest value currently reported by one settler gauge family.
    fn settler_gauge(name: &str) -> f64 {
        private_channel_metrics::prometheus::gather()
            .into_iter()
            .filter(|mf| mf.name() == name)
            .flat_map(|mf| mf.get_metric().to_vec())
            .map(|m| m.get_gauge().value())
            .fold(0.0, f64::max)
    }

    /// Sum of one settler counter family, sampled as a delta by the callers.
    /// The Prometheus registry is process-global, so tests that measure a
    /// counter delta have to hold this while they do it.
    static DISCARD_METRIC_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn settler_metric(name: &str) -> f64 {
        private_channel_metrics::prometheus::gather()
            .into_iter()
            .filter(|mf| mf.name() == name)
            .flat_map(|mf| mf.get_metric().to_vec())
            .map(|m| m.get_counter().value())
            .sum()
    }

    /// A batch whose writable account carries `bytes` but whose tx rolled back.
    fn failed_sized_settle_batch(
        bytes: usize,
    ) -> (
        LoadAndExecuteSanitizedTransactionsOutput,
        Vec<SanitizedTransaction>,
    ) {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let executed = make_failed_executed(vec![
            (
                from.pubkey(),
                AccountSharedData::new(1, bytes, &Pubkey::default()),
            ),
            (to, AccountSharedData::new(1, 0, &Pubkey::default())),
        ]);
        (
            LoadAndExecuteSanitizedTransactionsOutput {
                processing_results: vec![Ok(executed)],
                error_metrics: Default::default(),
                execute_timings: Default::default(),
                balance_collector: None,
            },
            vec![tx],
        )
    }

    /// Unread receivers held alive; dropping the address-index one is fatal.
    struct SettlerSinks {
        _blockhashes_rx: mpsc::UnboundedReceiver<Hash>,
        _address_signatures_rx: mpsc::Receiver<Vec<AddressSignatureRow>>,
    }

    /// Settler on a throwaway Postgres, with the channels the assertions read.
    #[allow(clippy::type_complexity)]
    async fn settler_under_test(
        url: String,
        blocktime_ms: u64,
        capacity: usize,
        metrics: SharedMetrics,
        shutdown: CancellationToken,
    ) -> (
        mpsc::Sender<ExecutedBatch>,
        mpsc::UnboundedReceiver<AccountSettlements>,
        WorkerHandle,
        SettlerSinks,
        Arc<crate::health::StageHeartbeat>,
    ) {
        let (exec_tx, exec_rx) = mpsc::channel(capacity);
        let (settled_accounts_tx, settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, address_signatures_rx) = mpsc::channel(64);
        let heartbeat = crate::health::StageHeartbeat::new();
        let handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown,
            metrics,
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: Arc::clone(&heartbeat),
        })
        .await;
        (
            exec_tx,
            settled_accounts_rx,
            handle,
            SettlerSinks {
                _blockhashes_rx: blockhashes_rx,
                _address_signatures_rx: address_signatures_rx,
            },
            heartbeat,
        )
    }

    /// Current wall clock in nanoseconds, the value the worker pins per settle.
    fn settle_now() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }

    /// Pool onto the same database the settler under test is writing to.
    fn test_pool(db: &AccountsDB) -> Arc<sqlx::PgPool> {
        match db {
            AccountsDB::Postgres(pg) => Arc::clone(&pg.pool),
            _ => panic!("expected a Postgres accounts database"),
        }
    }

    /// Make every slot above the current tip fail to commit, the way a
    /// storage-side error would, and return that tip.
    ///
    /// A constraint aimed at one slot races the settler, which mints an empty
    /// block every tick and may already have passed it. Capping the whole table
    /// cannot race: whatever slot comes next is above the cap and fails.
    async fn block_slots_above_tip(pool: &sqlx::PgPool) -> u64 {
        let tip: Option<i64> = sqlx::query_scalar("SELECT MAX(slot) FROM blocks")
            .fetch_one(pool)
            .await
            .expect("read tip");
        let tip = tip.unwrap_or(0) as u64;
        sqlx::query(&format!(
            "ALTER TABLE blocks ADD CONSTRAINT test_slot_ceiling CHECK (slot <= {tip}) NOT VALID"
        ))
        .execute(pool)
        .await
        .expect("add blocking constraint");
        tip
    }

    /// Lift the ceiling so commits succeed again.
    async fn unblock_slots(pool: &sqlx::PgPool) {
        sqlx::query("ALTER TABLE blocks DROP CONSTRAINT test_slot_ceiling")
            .execute(pool)
            .await
            .expect("drop blocking constraint");
    }

    /// Seconds since the epoch, for comparing against a committed block time.
    fn unix_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Wait until the settler has committed a block at `slot`, up to `within`.
    async fn await_block(pool: &sqlx::PgPool, slot: u64, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            let found: Option<i64> = sqlx::query_scalar("SELECT slot FROM blocks WHERE slot = $1")
                .bind(slot as i64)
                .fetch_optional(pool)
                .await
                .expect("query blocks");
            if found.is_some() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// The finding itself: unguarded, the settler drains forever and one tick can
    /// bind an unbounded array. Guarded, the channel backs up and parks the
    /// executor instead, so a full channel here means the fix is working.
    #[tokio::test(flavor = "multi_thread")]
    async fn settler_stops_draining_when_byte_budget_exceeded() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        // A long blocktime means no tick rescues the buffer during the test.
        let (exec_tx, _rx, _h, _sinks, _hb) =
            settler_under_test(url, 60_000, 1, Arc::new(NoopMetrics), shutdown.clone()).await;

        // An eighth of the budget each, so the guard trips before all of them fit.
        let batch_bytes = MAX_BUFFERED_SETTLE_BYTES / 8;
        let mut blocked = false;
        for _ in 0..64 {
            let (output, txs) = sized_settle_batch(batch_bytes);
            match exec_tx.try_send((output, txs, 1)) {
                Ok(()) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    blocked = true;
                    break;
                }
                Err(e) => panic!("unexpected send error: {:?}", e),
            }
        }
        assert!(
            blocked,
            "settler must stop draining once the byte budget is reached"
        );
        shutdown.cancel();
    }

    /// A guard that never reopens is a wedge; this also proves the counter resets.
    #[tokio::test(flavor = "multi_thread")]
    async fn settler_resumes_draining_after_tick_flush() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        let (exec_tx, mut settled_rx, _h, _sinks, _hb) =
            settler_under_test(url, 50, 1, Arc::new(NoopMetrics), shutdown.clone()).await;

        let batch_bytes = MAX_BUFFERED_SETTLE_BYTES / 4;
        let mut delivered = 0;
        for _ in 0..12 {
            let (output, txs) = sized_settle_batch(batch_bytes);
            if tokio::time::timeout(Duration::from_secs(10), exec_tx.send((output, txs, 1)))
                .await
                .is_ok()
            {
                delivered += 1;
            }
        }
        assert!(
            delivered >= 8,
            "ticks must drain the buffer and reopen the guard, delivered {}",
            delivered
        );

        let settled = tokio::time::timeout(Duration::from_secs(10), settled_rx.recv()).await;
        assert!(
            settled.is_ok(),
            "settlements must still flow under backpressure"
        );
        shutdown.cancel();
    }

    /// Pins the claimed bound: the budget plus the one message already accepted
    /// when the guard shut. Only the upper bound is asserted, because the gauge is
    /// a process-global static another settler may also be writing.
    #[tokio::test(flavor = "multi_thread")]
    async fn buffered_bytes_stay_within_budget_plus_one_batch() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        // No tick while sampling, and a one-slot channel blocks sends until a drain.
        let (exec_tx, _rx, _h, _sinks, _hb) = settler_under_test(
            url,
            600_000,
            1,
            Arc::new(PrometheusMetrics),
            shutdown.clone(),
        )
        .await;

        let batch_bytes = MAX_BUFFERED_SETTLE_BYTES / 4;
        let feeder = tokio::spawn(async move {
            for _ in 0..16 {
                let (output, txs) = sized_settle_batch(batch_bytes);
                if exec_tx.send((output, txs, 1)).await.is_err() {
                    break;
                }
            }
        });

        // Poll while the settler drains, so the peak is observed rather than raced.
        let mut peak = 0f64;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            peak = peak.max(settler_gauge(
                "private_channel_settler_buffered_account_bytes",
            ));
            if peak >= MAX_BUFFERED_SETTLE_BYTES as f64 {
                break;
            }
        }

        feeder.abort();
        assert!(peak > 0.0, "the gauge must be observed moving");
        assert!(
            peak <= (MAX_BUFFERED_SETTLE_BYTES + batch_bytes) as f64,
            "buffer {} exceeded budget {} plus one batch {}",
            peak,
            MAX_BUFFERED_SETTLE_BYTES,
            batch_bytes
        );
        shutdown.cancel();
    }

    /// A rolled-back transaction still pins what it allocated. Metering only
    /// successful writes would let a caller allocate megabytes, fail the
    /// transaction, and fill the heap while the guard read zero.
    #[tokio::test(flavor = "multi_thread")]
    async fn rolled_back_allocations_still_engage_the_guard() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, _h, _sinks, _hb) =
            settler_under_test(url, 600_000, 1, Arc::new(NoopMetrics), shutdown.clone()).await;

        let batch_bytes = MAX_BUFFERED_SETTLE_BYTES / 8;
        let mut blocked = false;
        for _ in 0..64 {
            let (output, txs) = failed_sized_settle_batch(batch_bytes);
            match exec_tx.try_send((output, txs, 1)) {
                Ok(()) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    blocked = true;
                    break;
                }
                Err(e) => panic!("unexpected send error: {:?}", e),
            }
        }
        assert!(
            blocked,
            "rolled-back allocations must count toward the byte budget"
        );
        shutdown.cancel();
    }

    /// Ordinary traffic must never trip the guard, catching any units error.
    #[tokio::test(flavor = "multi_thread")]
    async fn normal_traffic_never_engages_backpressure() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();
        let before = settler_metric("private_channel_settler_backpressure_engaged_total");

        let (exec_tx, mut settled_rx, _h, _sinks, _hb) = settler_under_test(
            url,
            50,
            RESULTS_CAP,
            Arc::new(PrometheusMetrics),
            shutdown.clone(),
        )
        .await;

        for _ in 0..20 {
            let (output, txs) = sized_settle_batch(128);
            exec_tx.send((output, txs, 1)).await.unwrap();
        }
        let settled = tokio::time::timeout(Duration::from_secs(10), settled_rx.recv()).await;
        assert!(settled.is_ok(), "ordinary traffic must settle");

        let after = settler_metric("private_channel_settler_backpressure_engaged_total");
        assert_eq!(
            before, after,
            "small batches must never engage backpressure"
        );
        shutdown.cancel();
    }

    /// If the watermark acknowledged an undrained batch, BOB would mark a
    /// non-durable account clean and lose the write. The generation must only ever
    /// describe what the settler actually committed.
    #[tokio::test(flavor = "multi_thread")]
    async fn generation_watermark_excludes_undrained_batches() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        let (exec_tx, mut settled_rx, _h, _sinks, _hb) =
            settler_under_test(url, 400, 1, Arc::new(NoopMetrics), shutdown.clone()).await;

        // Generation 1 saturates the buffer on its own.
        let (output, txs) = sized_settle_batch(MAX_BUFFERED_SETTLE_BYTES + 4096);
        exec_tx.send((output, txs, 1)).await.unwrap();

        // Generation 2 cannot be drained while the guard is shut.
        let (output2, txs2) = sized_settle_batch(4096);
        let _ = exec_tx.try_send((output2, txs2, 2));

        let settlements = tokio::time::timeout(Duration::from_secs(10), settled_rx.recv())
            .await
            .expect("a tick must fire")
            .expect("feedback channel stays open");
        assert_eq!(
            settlements.generation, 1,
            "the watermark must not acknowledge an undrained batch"
        );
        shutdown.cancel();
    }

    /// Backpressure must not read as a stall, or the health valve would restart
    /// the node under load, which is worse than the bug being fixed. The tick keeps
    /// recording progress while the guard is shut, so health must hold.
    #[tokio::test(flavor = "multi_thread")]
    async fn settler_stays_healthy_while_backpressured() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        let (exec_tx, exec_rx) = mpsc::channel(1);
        let (settled_accounts_tx, mut settled_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _bh_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _as_rx) = mpsc::channel(64);
        let heartbeat = crate::health::StageHeartbeat::new();
        let _handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 100,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: Arc::clone(&heartbeat),
        })
        .await;

        // Half the budget each, fed continuously, so the guard keeps re-engaging.
        let feeder = tokio::spawn(async move {
            loop {
                let (output, txs) = sized_settle_batch(MAX_BUFFERED_SETTLE_BYTES / 2);
                if exec_tx.send((output, txs, 1)).await.is_err() {
                    break;
                }
            }
        });

        // Progress is only recorded by a completed tick, so wait for the first.
        let first = tokio::time::timeout(Duration::from_secs(60), settled_rx.recv()).await;
        assert!(first.is_ok(), "settler must produce a first block");

        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(
                heartbeat.is_healthy(),
                "settler must stay healthy while backpressured"
            );
        }

        feeder.abort();
        shutdown.cancel();
    }

    /// The final flush must commit a saturated buffer and reset the counter.
    #[tokio::test(flavor = "multi_thread")]
    async fn final_flush_commits_saturated_buffer() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 60_000, 1, Arc::new(NoopMetrics), shutdown.clone()).await;

        let (output, txs) = sized_settle_batch(MAX_BUFFERED_SETTLE_BYTES + 4096);
        exec_tx.send((output, txs, 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        drop(exec_tx);
        let exited = tokio::time::timeout(Duration::from_secs(30), handle.handle).await;
        assert!(exited.is_ok(), "final flush must exit promptly");
        shutdown.cancel();
    }

    // --- settle retry, pinned timestamps and stall health ---

    /// A storage error that clears must cost a stall, not the node.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_retries_until_a_transient_failure_clears() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        // Genesis is slot 0, so the first block carrying our batch is slot 1.
        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        let tip = block_slots_above_tip(&pool).await;

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(600)).await;
        unblock_slots(&pool).await;

        assert!(
            await_block(&pool, tip + 1, Duration::from_secs(20)).await,
            "the batch must land once the failure clears"
        );
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(20), handle.handle).await;
    }

    /// A retry must rewrite the same block, not a freshly timestamped one: a
    /// replica can already be serving the first attempt's blockhash from the
    /// metadata row, and re-minting it there would strand every client that
    /// signed against it.
    #[tokio::test(flavor = "multi_thread")]
    async fn retried_settle_keeps_the_timestamp_of_the_first_attempt() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        let tip = block_slots_above_tip(&pool).await;

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();

        // Held long enough that a timestamp sampled at the winning attempt
        // would be several seconds later than one pinned at the first.
        tokio::time::sleep(Duration::from_millis(3500)).await;
        let unblocked_at = unix_secs();
        unblock_slots(&pool).await;

        assert!(await_block(&pool, tip + 1, Duration::from_secs(20)).await);
        let raw: Vec<u8> = sqlx::query_scalar("SELECT data FROM blocks WHERE slot = $1")
            .bind((tip + 1) as i64)
            .fetch_one(pool.as_ref())
            .await
            .expect("read the blocked block");
        let block: BlockInfo = bincode::deserialize(&raw).expect("decode block 1");
        let block_time = block.block_time.expect("block time present");
        assert!(
            block_time < unblocked_at,
            "block time {block_time} must predate the unblock at {unblocked_at}"
        );

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(20), handle.handle).await;
    }

    /// Identical inputs must produce an identical block, which is what makes a
    /// retry after an unacknowledged commit safe to repeat.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_is_deterministic_given_a_pinned_timestamp() {
        let (mut db, _pg) = start_test_postgres().await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let results = single_successful_transfer();
        let nanos = 1_700_000_000_123_456_789u128;

        let r1 = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results,
            &metrics,
            None,
            0,
            nanos,
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        let r2 = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results,
            &metrics,
            None,
            0,
            nanos,
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(r1.slot, r2.slot);
        assert_eq!(r1.blockhash, r2.blockhash);
    }

    /// While the settler is stuck retrying it must stop reporting healthy, or a
    /// load balancer keeps sending it work it cannot commit. Both heartbeat
    /// timestamps would otherwise freeze together and the verdict would freeze
    /// with them, because no new input arrives during a retry.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_settler_reports_unhealthy() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, heartbeat) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        block_slots_above_tip(&pool).await;

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();

        // Past the stage progress margin, so a frozen verdict would still read healthy.
        tokio::time::sleep(Duration::from_secs(8)).await;
        assert!(
            !heartbeat.is_healthy(),
            "a settler that cannot commit must not report healthy"
        );

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(20), handle.handle).await;
    }

    /// Shutdown must cut a running retry short rather than run out the full
    /// budget, so the drain still fits inside the container grace period.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_cuts_a_running_retry_short() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        block_slots_above_tip(&pool).await;

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        let t0 = tokio::time::Instant::now();
        shutdown.cancel();
        let exited = tokio::time::timeout(Duration::from_secs(12), handle.handle).await;
        assert!(
            exited.is_ok(),
            "cancelled retry must not run the full budget"
        );
        assert!(
            t0.elapsed() < SETTLE_RETRY_BUDGET,
            "exit took {:?}, which is the uncancelled budget",
            t0.elapsed()
        );
    }

    /// An attempt that hangs must be abandoned, otherwise the pool's own
    /// acquire timeout outlasts the retry budget and the settler stalls far
    /// longer than the blockhash window allows.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_hung_attempt_is_abandoned_and_the_budget_still_holds() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);

        // Hold the table so the settler's insert blocks instead of failing.
        let mut locker = pool.begin().await.expect("begin locker");
        sqlx::query("LOCK TABLE blocks IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *locker)
            .await
            .expect("lock blocks");

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();

        let t0 = tokio::time::Instant::now();
        let exited = tokio::time::timeout(Duration::from_secs(60), handle.handle).await;
        assert!(
            exited.is_ok(),
            "a hung attempt must be abandoned, not waited on forever"
        );
        assert!(
            t0.elapsed() < Duration::from_secs(45),
            "gave up after {:?}, which exceeds the bounded budget",
            t0.elapsed()
        );
        let _ = locker.rollback().await;
        shutdown.cancel();
    }

    /// A drain has time to spare, so a failure that clears must still settle.
    /// Giving up on the first one throws the whole buffer away instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_settler_retries_within_its_shutdown_budget() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();
        let metrics: SharedMetrics = Arc::new(PrometheusMetrics);
        let _metrics_guard = DISCARD_METRIC_LOCK.lock().await;
        let before = settler_metric("private_channel_discarded_executed_transactions_total");

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, metrics, shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        let tip = block_slots_above_tip(&pool).await;

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Cancel while it is retrying, then let the failure clear well inside
        // the shutdown budget. The sender goes with it, since the settler
        // drains until its input closes.
        shutdown.cancel();
        drop(exec_tx);
        let clearing = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
            unblock_slots(&clearing).await;
        });

        let exited = tokio::time::timeout(Duration::from_secs(20), handle.handle).await;
        assert!(exited.is_ok(), "the settler must finish its drain");
        assert!(
            await_block(&pool, tip + 1, Duration::from_secs(5)).await,
            "a cancelled settler must retry a failure that clears inside its budget"
        );
        let discarded =
            settler_metric("private_channel_discarded_executed_transactions_total") - before;
        assert_eq!(
            discarded, 0.0,
            "nothing may be discarded once the retry succeeded"
        );
    }

    /// The budget is a bound as well as an allowance: the drain deadline is
    /// shared with every other stage, so a doomed settle cannot spend it all.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_settler_stops_at_its_shutdown_budget() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        block_slots_above_tip(&pool).await;

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        let t0 = tokio::time::Instant::now();
        shutdown.cancel();
        let exited = tokio::time::timeout(Duration::from_secs(30), handle.handle).await;
        assert!(exited.is_ok(), "the settler must give up, not hang");
        assert!(
            t0.elapsed() < SETTLE_SHUTDOWN_BUDGET + Duration::from_millis(1500),
            "gave up after {:?}, which overruns the shutdown budget",
            t0.elapsed()
        );
    }

    /// Cancellation must bound an attempt that is already running. Waiting it
    /// out first and only then starting the budget stacks the two, which is
    /// what pushes the settler past the drain deadline.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancellation_during_an_attempt_shortens_it() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, Arc::new(NoopMetrics), shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);

        // Hold the table so the attempt hangs rather than failing.
        let mut locker = pool.begin().await.expect("begin locker");
        sqlx::query("LOCK TABLE blocks IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *locker)
            .await
            .expect("lock blocks");

        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();
        // Long enough that an attempt is in flight, short enough that its own
        // timeout has not fired.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let t0 = tokio::time::Instant::now();
        shutdown.cancel();
        let exited = tokio::time::timeout(Duration::from_secs(40), handle.handle).await;
        assert!(exited.is_ok(), "a hung attempt must still end the drain");
        assert!(
            t0.elapsed() < SETTLE_SHUTDOWN_BUDGET + Duration::from_millis(1500),
            "took {:?} from cancellation, so the in-flight attempt was not re-bounded",
            t0.elapsed()
        );
        let _ = locker.rollback().await;
    }

    /// The discard drain frees queue slots, and a parked executor fills them
    /// immediately. Draining has to close the queue, not just empty it, or a
    /// batch lands behind the record and dies with the receiver.
    #[tokio::test(flavor = "multi_thread")]
    async fn discard_closes_the_queue_so_nothing_arrives_behind_the_record() {
        let _metrics_guard = DISCARD_METRIC_LOCK.lock().await;
        let metrics: SharedMetrics = Arc::new(PrometheusMetrics);
        let before = settler_metric("private_channel_discarded_executed_transactions_total");

        // Capacity one, so the second sender parks exactly as the executor does.
        let (tx, mut rx) = mpsc::channel::<ExecutedBatch>(1);
        let (output, txs) = sized_settle_batch(1024);
        tx.send((output, txs, 1)).await.unwrap();

        let parked_tx = tx.clone();
        let parked = tokio::spawn(async move {
            let (output, txs) = sized_settle_batch(1024);
            parked_tx.send((output, txs, 2)).await.is_ok()
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut buffer = Vec::new();
        discard_buffer("test", &mut buffer, &mut rx, &metrics);
        let recorded =
            settler_metric("private_channel_discarded_executed_transactions_total") - before;

        let handed_over = parked.await.unwrap();
        assert_eq!(
            recorded,
            if handed_over { 2.0 } else { 1.0 },
            "a batch the queue accepted must be in the record"
        );

        let (output, txs) = sized_settle_batch(1024);
        assert!(
            tx.send((output, txs, 3)).await.is_err(),
            "the queue must be closed once the buffer is discarded, not merely emptied"
        );
    }

    /// Nothing may be dropped silently. When the budget runs out the settler
    /// must name every executed transaction it is throwing away, including the
    /// ones still queued behind it, because those are just as executed and just
    /// as uncommitted as the ones it had already buffered.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unsettleable_buffer_is_recorded_before_it_is_dropped() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();
        let metrics: SharedMetrics = Arc::new(PrometheusMetrics);
        let _metrics_guard = DISCARD_METRIC_LOCK.lock().await;
        let before = settler_metric("private_channel_discarded_executed_transactions_total");

        let (exec_tx, _rx, handle, _sinks, _hb) =
            settler_under_test(url, 100, 4, metrics, shutdown.clone()).await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        let tip = block_slots_above_tip(&pool).await;

        // Buffered before the tick that fails.
        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 1)).await.unwrap();

        // Queued while the settler is retrying and therefore not draining.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (output, txs) = sized_settle_batch(1024);
        exec_tx.send((output, txs, 2)).await.unwrap();

        let exited = tokio::time::timeout(Duration::from_secs(40), handle.handle).await;
        assert!(
            exited.is_ok(),
            "settler must give up once the budget is spent"
        );

        let discarded =
            settler_metric("private_channel_discarded_executed_transactions_total") - before;
        assert_eq!(
            discarded, 2.0,
            "both the buffered and the queued transaction must be recorded"
        );
        assert!(
            !await_block(&pool, tip + 1, Duration::from_millis(200)).await,
            "no block may exist for a batch that was discarded"
        );
        shutdown.cancel();
    }

    /// A commit that lands but is abandoned before its publishes must not make
    /// the retry announce the same blockhash twice. Dedup keeps a plain list, so
    /// a duplicate entry means evicting the first copy drops the replay cache
    /// for a blockhash that is still live, and an executed transaction could be
    /// resubmitted. The commit alone is bounded; nothing after it is.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_settled_blockhash_is_published_exactly_once() {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let pool = test_pool(&db);
        let shutdown = CancellationToken::new();

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, mut blockhashes_rx) = mpsc::unbounded_channel();
        // Capacity one and never drained, so the post-commit address-index send
        // parks for far longer than the commit bound.
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(1);

        let handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            blocktime_ms: 100,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            redis_block_ttl_secs: 0,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        assert!(await_block(&pool, 0, Duration::from_secs(10)).await);
        for _ in 0..4 {
            let (output, txs) = sized_settle_batch(1024);
            exec_tx.send((output, txs, 1)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut seen: Vec<Hash> = Vec::new();
        while let Ok(hash) = blockhashes_rx.try_recv() {
            seen.push(hash);
        }
        let mut unique = seen.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            seen.len(),
            "a blockhash was published more than once: {seen:?}"
        );

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(20), handle.handle).await;
    }

    /// Account holding `len` bytes of data, funded so it is not a tombstone.
    fn sized_account(len: usize) -> AccountSharedData {
        AccountSharedData::new(1, len, &Pubkey::default())
    }

    /// A transfer's account list: 0 and 1 writable, 2 the read-only program slot.
    fn transfer_accounts(
        from: Pubkey,
        to: Pubkey,
        first: AccountSharedData,
        second: AccountSharedData,
        readonly: AccountSharedData,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        vec![
            (from, first),
            (to, second),
            (Pubkey::new_unique(), readonly),
        ]
    }

    /// Each row is one way the meter could drift from what the buffer retains.
    #[test]
    fn retained_account_bytes_counts_what_the_buffer_holds() {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let fp = from.pubkey();

        let readonly = sized_account(4096);
        let empty = sized_account(0);

        let fees_only = ProcessedTransaction::FeesOnly(Box::new(FeesOnlyTransaction {
            load_error: solana_transaction_error::TransactionError::InsufficientFundsForFee,
            rollback_accounts: RollbackAccounts::FeePayerOnly {
                fee_payer_account: AccountSharedData::new(900, 0, &Pubkey::default()),
            },
            fee_details: Default::default(),
        }));

        let cases: Vec<(&str, TransactionProcessingResult, usize)> = vec![
            (
                "writable, successful, funded",
                Ok(make_executed(transfer_accounts(
                    fp,
                    to,
                    sized_account(4096),
                    empty.clone(),
                    readonly.clone(),
                ))),
                4096,
            ),
            (
                "read-only slot is excluded",
                Ok(make_executed(transfer_accounts(
                    fp,
                    to,
                    empty.clone(),
                    empty.clone(),
                    readonly.clone(),
                ))),
                0,
            ),
            (
                "a rolled-back tx still holds what it allocated",
                Ok(make_failed_executed(transfer_accounts(
                    fp,
                    to,
                    sized_account(4096),
                    empty.clone(),
                    readonly.clone(),
                ))),
                4096,
            ),
            (
                "a zero-lamport account still occupies its bytes",
                Ok(make_executed(transfer_accounts(
                    fp,
                    to,
                    AccountSharedData::new(0, 4096, &Pubkey::default()),
                    empty.clone(),
                    readonly.clone(),
                ))),
                4096,
            ),
            ("fees-only writes nothing", Ok(fees_only), 0),
            (
                "failed transaction writes nothing",
                Err(solana_transaction_error::TransactionError::AccountNotFound),
                0,
            ),
            (
                "both writable slots are summed",
                Ok(make_executed(transfer_accounts(
                    fp,
                    to,
                    sized_account(4096),
                    sized_account(8192),
                    readonly.clone(),
                ))),
                12288,
            ),
        ];

        for (name, result, expected) in cases {
            let got =
                retained_account_bytes(std::slice::from_ref(&result), std::slice::from_ref(&tx));
            assert_eq!(got, expected, "row: {}", name);
        }
    }

    /// The meter sums per batch while the settler dedupes by pubkey, so it may
    /// over-count but never under-count. Under-counting would let the buffer pass
    /// the budget unnoticed, which is exactly what the guard exists to stop.
    #[test]
    fn meter_never_undercounts_what_the_settler_writes() {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let fp = from.pubkey();

        // The same pubkey written twice collapses to one entry downstream.
        let results = vec![
            Ok(make_executed(transfer_accounts(
                fp,
                to,
                sized_account(4096),
                sized_account(0),
                sized_account(4096),
            ))),
            Ok(make_executed(transfer_accounts(
                fp,
                to,
                sized_account(4096),
                sized_account(0),
                sized_account(4096),
            ))),
        ];
        let txs = vec![tx.clone(), tx.clone()];

        let metered = retained_account_bytes(&results, &txs);
        let settled_unique = 4096;

        assert_eq!(metered, 8192, "the meter sums per transaction");
        assert!(
            metered >= settled_unique,
            "the meter must never report fewer bytes than the settler writes"
        );
    }

    #[test]
    fn compute_blockhash_is_deterministic_given_inputs() {
        let parent = Hash::new_unique();
        let sigs = vec![sig(1), sig(2)];
        let a = compute_blockhash(&parent, 7, 123, &sigs);
        let b = compute_blockhash(&parent, 7, 123, &sigs);
        assert_eq!(a, b);
    }

    #[test]
    fn compute_blockhash_depends_on_parent() {
        let sigs = vec![sig(1)];
        let a = compute_blockhash(&Hash::new_unique(), 7, 123, &sigs);
        let b = compute_blockhash(&Hash::new_unique(), 7, 123, &sigs);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_blockhash_depends_on_slot() {
        let parent = Hash::new_unique();
        let sigs = vec![sig(1)];
        let a = compute_blockhash(&parent, 7, 123, &sigs);
        let b = compute_blockhash(&parent, 8, 123, &sigs);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_blockhash_depends_on_time() {
        let parent = Hash::new_unique();
        let sigs = vec![sig(1)];
        let a = compute_blockhash(&parent, 7, 123, &sigs);
        let b = compute_blockhash(&parent, 7, 124, &sigs);
        assert_ne!(a, b);
    }

    #[test]
    fn compute_blockhash_depends_on_signatures() {
        let parent = Hash::new_unique();
        let empty = compute_blockhash(&parent, 7, 123, &[]);
        let one = compute_blockhash(&parent, 7, 123, &[sig(1)]);
        let other = compute_blockhash(&parent, 7, 123, &[sig(2)]);
        assert_ne!(empty, one);
        assert_ne!(one, other);
    }

    #[test]
    fn compute_blockhash_is_not_predictable_packing() {
        // A real hash spreads entropy across all 32 bytes, unlike the old slot-in-bytes[0..8], zero-tail packing.
        let slot: u64 = 42;
        let h = compute_blockhash(&Hash::new_unique(), slot, 123, &[sig(1)]);
        let bytes = h.to_bytes();
        assert!(bytes[16..32].iter().any(|&b| b != 0));
        assert_ne!(&bytes[0..8], &slot.to_le_bytes());
    }

    #[test]
    fn idle_tick_before_the_heartbeat_produces_nothing() {
        assert!(!should_produce_block(
            false,
            false,
            HEARTBEAT_INTERVAL - Duration::from_millis(1)
        ));
    }

    #[test]
    fn heartbeat_due_produces_a_block() {
        assert!(should_produce_block(false, false, HEARTBEAT_INTERVAL));
    }

    /// A tick that carried work produces a block immediately, which also resets
    /// the heartbeat, so a busy node never emits a redundant empty block.
    #[test]
    fn busy_tick_produces_a_block_and_resets_the_heartbeat() {
        assert!(should_produce_block(true, false, Duration::ZERO));
    }

    /// Genesis is produced whatever the clock says, so the chain has an anchor
    /// and getLatestBlockhash answers from the first tick.
    #[test]
    fn genesis_is_always_produced() {
        assert!(should_produce_block(false, true, Duration::ZERO));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_empty_results() {
        let (mut db, _pg) = start_test_postgres().await;
        let (r, settled_accounts) = settle_capturing_accounts(None, &mut db, &[]).await;
        assert_eq!(r.slot, 0);
        // Genesis (last_block == None) deliberately stays the default hash.
        assert_eq!(r.blockhash, Hash::default());
        assert!(settled_accounts.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_increments_slot() {
        let (mut db, _pg) = start_test_postgres().await;

        let r1 = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(r1.slot, 0);

        let last = LastBlock {
            slot: r1.slot,
            blockhash: r1.blockhash,
            block_height: r1.slot,
        };
        let r2 = settle_transactions(
            last.slot + 1,
            Some(last),
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(r2.slot, 1);
        assert_ne!(r2.blockhash, Hash::default());

        // The persisted block must record r1's hash as parent, proving the chain link is written through, not just in memory.
        let block1 = db.get_block(1).await.unwrap().expect("block 1 persisted");
        assert_eq!(block1.previous_blockhash, r1.blockhash);
    }

    // End-to-end: settle derives the new hash and persists it round-tripped and chained.
    // Signature-binding is proven deterministically by compute_blockhash_depends_on_signatures;
    // it cannot be isolated here because each settle reads its own wall-clock nanos, so this
    // asserts the wiring and persistence instead of re-proving the content dependency.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_persists_computed_blockhash() {
        let (mut db, _pg) = start_test_postgres().await;

        let parent_hash = Hash::new_unique();

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let account_pk = Pubkey::new_unique();
        let processed = make_executed(vec![(
            account_pk,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let with_tx: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed), tx)];

        let r = settle_transactions(
            5 + 1,
            Some(LastBlock {
                slot: 5,
                blockhash: parent_hash,
                block_height: 5,
            }),
            &mut db,
            None,
            &with_tx,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        assert_ne!(r.blockhash, Hash::default());
        assert_ne!(r.blockhash, parent_hash);
        let block = db.get_block(r.slot).await.unwrap().unwrap();
        assert_eq!(block.blockhash, r.blockhash);
        assert_eq!(block.previous_blockhash, parent_hash);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_with_executed_transaction() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);

        // Create an executed result with a writable account
        let account_pk = Pubkey::new_unique();
        let account_data = AccountSharedData::new(500, 0, &Pubkey::new_unique());
        let processed = make_executed(vec![(account_pk, account_data)]);
        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed), tx)];

        let result = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        // Should have stored a block, and the transaction signature
        let block = db.get_block(result.slot).await.unwrap();
        assert!(block.is_some());
        assert_eq!(block.unwrap().transaction_signatures.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_writable_stored_readonly_skipped() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);

        // The system transfer tx has writable accounts at indices 0,1 and readonly at 2
        // Create executed result with 3 accounts
        let owner = Pubkey::new_unique();
        let pk0 = from.pubkey();
        let pk1 = to;
        let pk2 = solana_system_interface::program::id();

        let processed = make_executed(vec![
            (pk0, AccountSharedData::new(900, 0, &owner)),
            (pk1, AccountSharedData::new(100, 0, &owner)),
            (pk2, AccountSharedData::new(1, 0, &owner)),
        ]);
        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed), tx)];

        let (_result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // Writable accounts should be in settlements, readonly (system program) should not
        let settlement_keys: Vec<_> = settled_accounts.iter().map(|(k, _)| *k).collect();
        assert!(settlement_keys.contains(&pk0));
        assert!(settlement_keys.contains(&pk1));
        // system program at index 2 is read-only for a system transfer
        assert!(!settlement_keys.contains(&pk2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_deleted_account() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);

        // Account with 0 lamports and empty data = deleted
        let pk = from.pubkey();
        let processed = make_executed(vec![(pk, AccountSharedData::default())]);
        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed), tx)];

        let (_result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // The deleted account should be flagged
        let settlement = settled_accounts.iter().find(|(k, _)| k == &pk);
        assert!(settlement.is_some());
        assert!(settlement.unwrap().1.deleted);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_failed_tx_signature_recorded() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let sig = *tx.signature();

        // Failed transaction
        let results: Vec<(TransactionProcessingResult, _)> = vec![(
            Err(solana_transaction_error::TransactionError::AccountNotFound),
            tx,
        )];

        let (result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // Failed transactions still get their signature recorded in the block
        let block = db.get_block(result.slot).await.unwrap().unwrap();
        assert!(block.transaction_signatures.contains(&sig));
        // But no account settlements
        assert!(settled_accounts.is_empty());
    }

    /// A failed executed tx must persist no account writes from its rolled-back state, yet still be recorded by signature.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_executed_persists_no_accounts_but_records_sig() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let sig = *tx.signature();

        // A token-like data account at idx 0 (writable fee payer slot).
        let data_pk = from.pubkey();
        let data_acct = AccountSharedData::new(500, 8, &spl_token::id());
        let results: Vec<(TransactionProcessingResult, _)> =
            vec![(Ok(make_failed_executed(vec![(data_pk, data_acct)])), tx)];

        let (result, settled) = settle_capturing_accounts(None, &mut db, &results).await;

        assert!(
            settled.is_empty(),
            "a failed executed tx must persist no account writes"
        );
        let block = db.get_block(result.slot).await.unwrap().unwrap();
        assert!(
            block.transaction_signatures.contains(&sig),
            "a failed executed tx must still be recorded by signature"
        );
    }

    /// A successful executed tx (writes A) and a failed one (would-write B): only A settles, B is absent, both sigs recorded.
    #[tokio::test(flavor = "multi_thread")]
    async fn mixed_success_and_failed_executed_in_batch() {
        let (mut db, _pg) = start_test_postgres().await;

        let from1 = Keypair::new();
        let tx1 = create_test_sanitized_transaction(&from1, &Pubkey::new_unique(), 100);
        let sig1 = *tx1.signature();
        let a = from1.pubkey();
        let ok = make_executed(vec![(a, AccountSharedData::new(500, 8, &spl_token::id()))]);

        let from2 = Keypair::new();
        let tx2 = create_test_sanitized_transaction(&from2, &Pubkey::new_unique(), 200);
        let sig2 = *tx2.signature();
        let b = from2.pubkey();
        let failed =
            make_failed_executed(vec![(b, AccountSharedData::new(700, 8, &spl_token::id()))]);

        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(ok), tx1), (Ok(failed), tx2)];

        let (result, settled) = settle_capturing_accounts(None, &mut db, &results).await;

        assert!(
            settled.iter().any(|(k, _)| *k == a),
            "successful tx's account A must be settled"
        );
        assert!(
            !settled.iter().any(|(k, _)| *k == b),
            "failed tx's account B must not be settled"
        );
        assert_eq!(settled.len(), 1, "only A settles");

        let block = db.get_block(result.slot).await.unwrap().unwrap();
        assert!(block.transaction_signatures.contains(&sig1));
        assert!(
            block.transaction_signatures.contains(&sig2),
            "failed executed tx signature must still be recorded"
        );
    }

    /// The executor erases a fabricated fee payer to an empty, zero-lamport
    /// account. The settler must turn that shape into a `deleted` tombstone
    /// while persisting a live account sitting on its 1-lamport existence floor.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_erased_payer_deleted_data_persisted() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 0);

        // An erased fabricated payer and a live account on its 1-lamport
        // existence floor, both writable.
        let dataless = from.pubkey();
        let data_pk = Pubkey::new_unique();
        let data_acct = AccountSharedData::new(1, 8, &spl_token::id());
        let processed = make_executed(vec![
            (dataless, AccountSharedData::default()),
            (data_pk, data_acct),
        ]);
        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed), tx)];

        let (_result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // Dataless (0 lamports, empty data) → deleted tombstone.
        let dataless_settlement = settled_accounts
            .iter()
            .find(|(k, _)| *k == dataless)
            .expect("dataless account must be emitted as a settlement");
        assert!(
            dataless_settlement.1.deleted,
            "an erased fabricated payer must settle as deleted"
        );

        // Data account at the 1-lamport floor → persists, not deleted.
        let data_settlement = settled_accounts
            .iter()
            .find(|(k, _)| *k == data_pk)
            .expect("data account must be emitted as a settlement");
        assert!(
            !data_settlement.1.deleted,
            "a data account at the 1-lamport floor must persist"
        );
        assert_eq!(data_settlement.1.account.lamports(), 1);
    }

    /// Count rows for this pubkey straight from the table. Reading through
    /// `get_accounts` would pass on the filter alone, even if the row remained.
    async fn raw_account_row_count(db: &PostgresAccountsDB, pubkey: &Pubkey) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM accounts WHERE pubkey = $1")
            .bind(&pubkey.to_bytes()[..])
            .fetch_one(db.pool.as_ref())
            .await
            .expect("count query must succeed")
    }

    /// A closed account keeps its data buffer, so the settler must still delete
    /// its durable row rather than upserting a zero-lamport ghost.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_deletes_zero_lamport_data_row_from_postgres() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 0);

        // Index 0 closed with its buffer intact, index 1 live on the 1-lamport
        // floor. Both writable, so both settle.
        let closed = from.pubkey();
        let floor = Pubkey::new_unique();
        let processed = make_executed(vec![
            (closed, AccountSharedData::new(0, 8, &spl_token::id())),
            (floor, AccountSharedData::new(1, 8, &spl_token::id())),
        ]);

        // Seed the row the close is supposed to remove, so the delete has work
        // to do and the assertion cannot pass vacuously.
        db.set_account(closed, AccountSharedData::new(500, 8, &spl_token::id()))
            .await;
        let AccountsDB::Postgres(ref raw) = db else {
            panic!("start_test_postgres must hand back a Postgres backend");
        };
        assert_eq!(
            raw_account_row_count(raw, &closed).await,
            1,
            "the row must exist before settlement for this test to mean anything"
        );

        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed), tx)];
        let (_result, settled) = settle_capturing_accounts(None, &mut db, &results).await;

        let closed_settlement = settled
            .iter()
            .find(|(k, _)| *k == closed)
            .expect("the closed account must be emitted as a settlement");
        assert!(
            closed_settlement.1.deleted,
            "a zero-lamport account must settle as deleted whatever its data holds"
        );
        assert!(
            closed_settlement.1.account.data().is_empty(),
            "a delete settlement must not carry the closed account's buffer"
        );

        let floor_settlement = settled
            .iter()
            .find(|(k, _)| *k == floor)
            .expect("the floor account must be emitted as a settlement");
        assert!(
            !floor_settlement.1.deleted,
            "a data account on the 1-lamport floor must still persist"
        );

        let AccountsDB::Postgres(ref raw) = db else {
            panic!("backend must not change across settlement");
        };
        assert_eq!(
            raw_account_row_count(raw, &closed).await,
            0,
            "the closed account's row must be deleted, not upserted"
        );
        assert_eq!(
            raw_account_row_count(raw, &floor).await,
            1,
            "the floor account's row must be written"
        );
    }

    /// Both classifications must move together. Change one side only and the
    /// entry is never reconciled and never leaves the cache.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_then_preload_leaves_closed_account_absent() {
        let (mut bob, settled_tx, _pg) = crate::test_helpers::create_test_bob_with_postgres().await;
        let mut db = bob.accounts_db.clone();

        let from = Keypair::new();
        let closed = from.pubkey();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 0);
        let closed_account = AccountSharedData::new(0, 8, &spl_token::id());

        db.set_account(closed, AccountSharedData::new(500, 8, &spl_token::id()))
            .await;

        // Both consumers see the same state so the generation lines up.
        // `ProcessedTransaction` is not `Clone`, hence the rebuild below.
        let output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![Ok(make_executed(vec![(closed, closed_account.clone())]))],
            error_metrics: Default::default(),
            execute_timings: Default::default(),
            balance_collector: None,
        };
        let generation = bob.update_accounts(&output, std::slice::from_ref(&tx));

        let results: Vec<(TransactionProcessingResult, _)> =
            vec![(Ok(make_executed(vec![(closed, closed_account)])), tx)];
        let (_result, settled) = settle_capturing_accounts(None, &mut db, &results).await;

        settled_tx
            .send(AccountSettlements {
                generation,
                accounts: settled,
            })
            .unwrap();

        // Drains the acknowledgement and drops the tombstone, then proves the
        // now-absent key is not refilled from the database.
        let (fetched, cached) = bob.preload_accounts(&[closed]).await.unwrap();
        assert_eq!(
            (fetched, cached),
            (0, 1),
            "the tombstone is still resident when the hit/miss split runs"
        );

        let (fetched, cached) = bob.preload_accounts(&[closed]).await.unwrap();
        assert_eq!(
            (fetched, cached),
            (0, 0),
            "the dropped tombstone leaves a miss that the database must not fill"
        );
        assert!(
            solana_svm_callback::TransactionProcessingCallback::get_account_shared_data(
                &bob, &closed
            )
            .is_none(),
            "the closed account must stay unreadable"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_multiple_sequential_batches() {
        let (mut db, _pg) = start_test_postgres().await;

        // Settle first batch
        let from1 = Keypair::new();
        let to1 = Pubkey::new_unique();
        let tx1 = create_test_sanitized_transaction(&from1, &to1, 100);
        let pk1 = Pubkey::new_unique();
        let processed1 = make_executed(vec![(
            pk1,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let results1: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed1), tx1)];

        let r1 = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results1,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(r1.slot, 0);

        // Settle second batch, chaining from first
        let last = LastBlock {
            slot: r1.slot,
            blockhash: r1.blockhash,
            block_height: r1.slot,
        };
        let from2 = Keypair::new();
        let to2 = Pubkey::new_unique();
        let tx2 = create_test_sanitized_transaction(&from2, &to2, 200);
        let pk2 = Pubkey::new_unique();
        let processed2 = make_executed(vec![(
            pk2,
            AccountSharedData::new(300, 0, &Pubkey::new_unique()),
        )]);
        let results2: Vec<(TransactionProcessingResult, _)> = vec![(Ok(processed2), tx2)];

        let r2 = settle_transactions(
            last.slot + 1,
            Some(last),
            &mut db,
            None,
            &results2,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(r2.slot, 1);
        assert_ne!(r2.blockhash, r1.blockhash);

        // Both blocks should be stored
        assert!(db.get_block(0).await.unwrap().is_some());
        assert!(db.get_block(1).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_fees_only_records_signature_no_accounts() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let sig = *tx.signature();

        // FeesOnly: transaction loaded but failed to execute (e.g., insufficient funds).
        // SVM rolls back accounts and deducts fees, but no account changes are settled.
        let fees_only = ProcessedTransaction::FeesOnly(Box::new(FeesOnlyTransaction {
            load_error: solana_transaction_error::TransactionError::InsufficientFundsForFee,
            rollback_accounts: RollbackAccounts::FeePayerOnly {
                fee_payer_account: AccountSharedData::new(
                    900,
                    0,
                    &solana_sdk_ids::system_program::ID,
                ),
            },
            fee_details: Default::default(),
        }));
        let results: Vec<(TransactionProcessingResult, _)> = vec![(Ok(fees_only), tx)];

        let (result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // Signature should be recorded in the block
        let block = db.get_block(result.slot).await.unwrap().unwrap();
        assert!(block.transaction_signatures.contains(&sig));

        // No account settlements — fees-only transactions don't modify accounts
        assert!(settled_accounts.is_empty());
    }

    /// Test that cache warming reads from Postgres and writes to Redis correctly.
    ///
    /// This test verifies:
    /// 1. Reads latest_slot from Postgres (MAX(slot) from blocks table)
    /// 2. Writes latest_slot to Redis
    /// 3. Reads latest_blockhash from Postgres metadata table
    /// 4. Writes latest_blockhash to Redis
    ///
    /// Note: This is an integration test that requires:
    /// - TEST_POSTGRES_URL environment variable with a test database
    /// - TEST_REDIS_URL environment variable with a test Redis instance
    #[tokio::test]
    #[ignore] // Requires database setup
    async fn test_cache_warming() {
        use std::env;

        // Setup: Get test database URLs from environment
        let postgres_url = env::var("TEST_POSTGRES_URL").unwrap_or_else(|_| {
            "postgresql://private_channel:private_channel@localhost:5432/private_channel_test"
                .to_string()
        });
        let redis_url =
            env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

        // Create Postgres connection
        let postgres_db = match PostgresAccountsDB::new(&postgres_url, false).await {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Skipping test: Cannot connect to test Postgres: {}", e);
                return;
            }
        };

        // Create Redis connection
        let redis_db = match RedisAccountsDB::new(&redis_url, postgres_db.clone()).await {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Skipping test: Cannot connect to test Redis: {}", e);
                return;
            }
        };

        // Setup test data in Postgres
        let test_slot = 12345u64;
        let test_blockhash = Hash::default();
        let test_blockhash_bytes = test_blockhash.to_bytes();

        let pool = postgres_db.pool.clone();

        // Insert test block with slot
        let insert_result = sqlx::query(
            "INSERT INTO blocks (slot, blockhash, previous_blockhash, parent_slot, block_time)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (slot) DO NOTHING",
        )
        .bind(test_slot as i64)
        .bind(test_blockhash_bytes.to_vec())
        .bind(test_blockhash_bytes.to_vec())
        .bind(0i64)
        .bind(0i64)
        .execute(pool.as_ref())
        .await;

        if let Err(e) = insert_result {
            eprintln!(
                "Skipping test: Cannot insert test data into Postgres: {}",
                e
            );
            return;
        }

        // Insert test blockhash into metadata
        let metadata_result = sqlx::query(
            "INSERT INTO metadata (key, value)
             VALUES ('latest_blockhash', $1)
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(test_blockhash_bytes.to_vec())
        .execute(pool.as_ref())
        .await;

        if let Err(e) = metadata_result {
            eprintln!("Skipping test: Cannot insert metadata into Postgres: {}", e);
            return;
        }

        // Execute: Call warm_redis_cache
        let result = warm_redis_cache(
            &postgres_db,
            &redis_db,
            Some(test_slot),
            Some(0),
            Some(test_blockhash),
        )
        .await;

        // Verify: Function should succeed
        assert!(
            result.is_ok(),
            "warm_redis_cache should succeed. Got error: {:?}",
            result.err()
        );

        // Verify: Check that Redis was populated correctly
        let mut conn = redis_db.connection.clone();

        // Check latest_slot in Redis
        let redis_slot: Option<u64> = conn.get("latest_slot").await.ok();
        assert_eq!(
            redis_slot,
            Some(test_slot),
            "Redis should contain the correct latest_slot"
        );

        // Check latest_blockhash in Redis
        let redis_blockhash_str: Option<String> = conn.get("latest_blockhash").await.ok();
        assert_eq!(
            redis_blockhash_str,
            Some(test_blockhash.to_string()),
            "Redis should contain the correct latest_blockhash"
        );

        // Cleanup: Remove test data from Postgres
        let _ = sqlx::query("DELETE FROM blocks WHERE slot = $1")
            .bind(test_slot as i64)
            .execute(pool.as_ref())
            .await;

        let _ = sqlx::query("DELETE FROM metadata WHERE key = 'latest_blockhash'")
            .execute(pool.as_ref())
            .await;

        // Cleanup: Remove test data from Redis
        let _: Result<(), _> = conn.del("latest_slot").await;
        let _: Result<(), _> = conn.del("latest_blockhash").await;
    }

    // --- Settle worker integration tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn settle_worker_final_flushes_when_its_input_closes() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;
        let url_for_check = url.clone();

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 100,
            perf_sample_period_secs: 60,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Buffered work, then the executor goes away: the final flush must
        // commit it rather than the stage exiting on the signal and dropping it.
        let (output, txs) = sized_settle_batch(1024);
        let landed = *txs[0].signature();
        exec_tx.send((output, txs, 1)).await.unwrap();
        drop(exec_tx);

        let result = tokio::time::timeout(Duration::from_secs(15), handle.handle).await;
        assert!(
            result.is_ok(),
            "settle worker should exit once its input closes"
        );

        let db = AccountsDB::new(&url_for_check, false).await.unwrap();
        assert!(
            db.get_transaction(&landed).await.unwrap().is_some(),
            "the buffered batch must have been committed by the final flush"
        );
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_worker_processes_results_and_emits_settlements() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, mut settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, mut settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let _handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 50, // fast for testing
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Send a batch of execution results
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let pk = Pubkey::new_unique();
        let account_data = AccountSharedData::new(500, 0, &Pubkey::new_unique());
        let executed = make_executed(vec![(pk, account_data)]);
        let output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![Ok(executed)],
            error_metrics: Default::default(),
            execute_timings: Default::default(),
            balance_collector: None,
        };
        exec_tx.send((output, vec![tx], 1)).await.unwrap();

        // Wait for the blocktime tick to process and emit settlements
        let settlements =
            tokio::time::timeout(Duration::from_secs(5), settled_accounts_rx.recv()).await;
        assert!(
            settlements.is_ok(),
            "should receive settlements within timeout"
        );

        let blockhash =
            tokio::time::timeout(Duration::from_secs(1), settled_blockhashes_rx.recv()).await;
        assert!(blockhash.is_ok(), "should receive blockhash within timeout");

        shutdown.cancel();
    }

    /// Losing dedup must not leave the batch buffered for the final flush to
    /// commit again under the same slot.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_committed_block_is_not_settled_twice_when_dedup_is_gone() {
        let (db, pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, mut address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let worker = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 100,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Dedup and the settler share one shutdown token and dedup exits first,
        // which leaves the settler publishing into a dropped receiver.
        drop(settled_blockhashes_rx);

        // Buffered well before the first tick, which waits out the start delay,
        // so the tick that finds dedup gone has a batch to commit.
        let (output, transactions) = one_account_batch();
        exec_tx.send((output, transactions, 1)).await.unwrap();

        tokio::time::timeout(Duration::from_secs(10), worker.handle)
            .await
            .expect("the settler must exit once dedup is gone")
            .expect("the settler task must not panic");

        assert_eq!(
            db.get_transaction_count().await.unwrap(),
            1,
            "the committed batch must be counted once, not committed again by the final flush"
        );
        assert!(
            address_signatures_rx.try_recv().is_ok(),
            "a committed block's address rows must still be shipped"
        );
    }

    /// Drain feedback until a message actually carries settled accounts. The
    /// settler emits feedback on every blocktime tick, including ticks with an
    /// empty buffer, so a single `recv()` can legitimately return generation 0.
    async fn recv_nonempty_settlements(
        rx: &mut mpsc::UnboundedReceiver<AccountSettlements>,
    ) -> AccountSettlements {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let settlements = rx.recv().await.expect("settler feedback channel closed");
                if !settlements.accounts.is_empty() {
                    return settlements;
                }
            }
        })
        .await
        .expect("timed out waiting for non-empty settler feedback")
    }

    /// Build a one-transaction execution output writing a fresh account. The
    /// caller supplies the generation, so this returns the output and the
    /// transactions rather than a whole `ExecutedBatch`.
    fn one_account_batch() -> (
        LoadAndExecuteSanitizedTransactionsOutput,
        Vec<SanitizedTransaction>,
    ) {
        let tx = create_test_sanitized_transaction(&Keypair::new(), &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![Ok(executed)],
            error_metrics: Default::default(),
            execute_timings: Default::default(),
            balance_collector: None,
        };
        (output, vec![tx])
    }

    /// The generation the executor sent must come back on the acknowledgement,
    /// alongside the accounts the tick made durable.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_worker_feedback_carries_received_generation() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, mut settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let _handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 50,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        let (output, transactions) = one_account_batch();
        exec_tx.send((output, transactions, 7)).await.unwrap();

        let settlements = recv_nonempty_settlements(&mut settled_accounts_rx).await;
        assert_eq!(
            settlements.generation, 7,
            "feedback must report the generation the executor sent"
        );

        shutdown.cancel();
    }

    /// The high-water mark folds with max(), so a producer that regresses cannot
    /// pull the durable watermark backwards.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_worker_feedback_generation_never_regresses() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, mut settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let _handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 50,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        let (output_a, transactions_a) = one_account_batch();
        exec_tx.send((output_a, transactions_a, 9)).await.unwrap();
        let (output_b, transactions_b) = one_account_batch();
        exec_tx.send((output_b, transactions_b, 7)).await.unwrap();

        let settlements = recv_nonempty_settlements(&mut settled_accounts_rx).await;
        assert_eq!(
            settlements.generation, 9,
            "a stale generation must not lower the high-water mark"
        );

        // Assert a later tick too. Under plain assignment the stale 7 shows up in
        // whichever tick receives it, so checking only the first non-empty
        // acknowledgement would pass even without the max() fold.
        let later = tokio::time::timeout(Duration::from_secs(10), settled_accounts_rx.recv())
            .await
            .expect("timed out waiting for a later acknowledgement")
            .expect("settler feedback channel closed");
        assert_eq!(
            later.generation, 9,
            "the high-water mark must stay at 9 on every later tick"
        );

        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_worker_channel_closed_exits() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 50,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        drop(exec_tx);

        let result = tokio::time::timeout(Duration::from_secs(5), handle.handle).await;
        assert!(
            result.is_ok(),
            "settle worker should exit when input channel closes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_mixed_outcomes_in_batch() {
        // Test batch with Executed, FeesOnly, and Error outcomes mixed
        let (mut db, _pg) = start_test_postgres().await;

        let from1 = Keypair::new();
        let to1 = Pubkey::new_unique();
        let tx1 = create_test_sanitized_transaction(&from1, &to1, 100);
        let pk1 = Pubkey::new_unique();
        let executed = make_executed(vec![(
            pk1,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);

        let from2 = Keypair::new();
        let to2 = Pubkey::new_unique();
        let tx2 = create_test_sanitized_transaction(&from2, &to2, 200);

        let fees_only = ProcessedTransaction::FeesOnly(Box::new(FeesOnlyTransaction {
            load_error: solana_transaction_error::TransactionError::InsufficientFundsForFee,
            rollback_accounts: RollbackAccounts::FeePayerOnly {
                fee_payer_account: AccountSharedData::new(
                    900,
                    0,
                    &solana_sdk_ids::system_program::ID,
                ),
            },
            fee_details: Default::default(),
        }));

        let from3 = Keypair::new();
        let to3 = Pubkey::new_unique();
        let tx3 = create_test_sanitized_transaction(&from3, &to3, 300);
        let err = solana_transaction_error::TransactionError::InstructionError(
            0,
            solana_sdk::instruction::InstructionError::Custom(42),
        );

        let mh1 = *tx1.message_hash();
        let mh2 = *tx2.message_hash();
        let mh3 = *tx3.message_hash();

        let results: Vec<(TransactionProcessingResult, _)> =
            vec![(Ok(executed), tx1), (Ok(fees_only), tx2), (Err(err), tx3)];

        let (result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // All three signatures should be recorded in the block
        assert_eq!(
            settled_accounts.len(),
            1,
            "only executed tx settles accounts"
        );
        assert_eq!(
            result.blockhash,
            Hash::default(),
            "first block has default hash"
        );

        let block = db.get_block(result.slot).await.unwrap().unwrap();
        assert_eq!(
            block.transaction_signatures.len(),
            3,
            "all three signatures recorded"
        );
        // Message hashes are recorded for every outcome (Executed, FeesOnly, Err),
        // in order, so the restart cache can be rebuilt across all paths.
        assert_eq!(
            block.transaction_message_hashes,
            vec![mh1, mh2, mh3],
            "message hashes recorded in order across Executed/FeesOnly/Err"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_block_metadata_correctness() {
        // Test that block metadata (time, height, parent_slot) is set correctly
        let (mut db, _pg) = start_test_postgres().await;

        // First block
        let from1 = Keypair::new();
        let to1 = Pubkey::new_unique();
        let tx1 = create_test_sanitized_transaction(&from1, &to1, 100);
        let pk1 = Pubkey::new_unique();
        let executed1 = make_executed(vec![(
            pk1,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let results1 = vec![(Ok(executed1), tx1)];

        let r1 = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results1,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(r1.slot, 0);

        let block1 = db.get_block(0).await.unwrap().unwrap();
        assert_eq!(block1.parent_slot, 0, "first block parent_slot is 0");
        assert_eq!(block1.block_height, Some(0), "first block height is 0");
        assert!(block1.block_time.is_some(), "block time is set");

        // Second block, chained from first
        let last = LastBlock {
            slot: r1.slot,
            blockhash: r1.blockhash,
            block_height: r1.slot,
        };
        let from2 = Keypair::new();
        let to2 = Pubkey::new_unique();
        let tx2 = create_test_sanitized_transaction(&from2, &to2, 200);
        let pk2 = Pubkey::new_unique();
        let executed2 = make_executed(vec![(
            pk2,
            AccountSharedData::new(300, 0, &Pubkey::new_unique()),
        )]);
        let results2 = vec![(Ok(executed2), tx2)];

        let r2 = settle_transactions(
            last.slot + 1,
            Some(last),
            &mut db,
            None,
            &results2,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(r2.slot, 1);

        let block2 = db.get_block(1).await.unwrap().unwrap();
        assert_eq!(block2.parent_slot, 0, "second block parent_slot is 0");
        assert_eq!(block2.block_height, Some(1), "second block height is 1");
        assert_eq!(
            block2.previous_blockhash, r1.blockhash,
            "second block's previous_blockhash matches first block's blockhash"
        );
        assert!(block2.block_time.is_some(), "block time is set");
        assert_ne!(
            block2.blockhash, r1.blockhash,
            "block hashes differ between blocks"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_transaction_signature_ordering() {
        // Test that transaction signatures and recent_blockhashes are collected in order
        let (mut db, _pg) = start_test_postgres().await;

        // Create three transactions with different recent_blockhashes
        let tx1 = create_test_sanitized_transaction(&Keypair::new(), &Pubkey::new_unique(), 100);
        let tx2 = create_test_sanitized_transaction(&Keypair::new(), &Pubkey::new_unique(), 200);
        let tx3 = create_test_sanitized_transaction(&Keypair::new(), &Pubkey::new_unique(), 300);

        // Note: We can't easily modify recent_blockhash on a SanitizedTransaction,
        // so we test signature order instead by using the signature as a proxy
        let sig1 = *tx1.signature();
        let sig2 = *tx2.signature();
        let sig3 = *tx3.signature();
        let mh1 = *tx1.message_hash();
        let mh2 = *tx2.message_hash();
        let mh3 = *tx3.message_hash();

        let pk1 = Pubkey::new_unique();
        let executed1 = make_executed(vec![(
            pk1,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);

        let pk2 = Pubkey::new_unique();
        let executed2 = make_executed(vec![(
            pk2,
            AccountSharedData::new(600, 0, &Pubkey::new_unique()),
        )]);

        let pk3 = Pubkey::new_unique();
        let executed3 = make_executed(vec![(
            pk3,
            AccountSharedData::new(700, 0, &Pubkey::new_unique()),
        )]);

        let results = vec![
            (Ok(executed1), tx1),
            (Ok(executed2), tx2),
            (Ok(executed3), tx3),
        ];

        let result = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let block = db.get_block(result.slot).await.unwrap().unwrap();
        assert_eq!(
            block.transaction_signatures.len(),
            3,
            "all three signatures recorded"
        );
        // Verify signatures are in the same order as input
        assert_eq!(block.transaction_signatures[0], sig1);
        assert_eq!(block.transaction_signatures[1], sig2);
        assert_eq!(block.transaction_signatures[2], sig3);

        // Message hashes are collected as a parallel array in the same order,
        // one per signature. build_dedup_state relies on this invariant.
        assert_eq!(
            block.transaction_message_hashes.len(),
            block.transaction_signatures.len(),
            "message hashes must be parallel to signatures"
        );
        assert_eq!(block.transaction_message_hashes[0], mh1);
        assert_eq!(block.transaction_message_hashes[1], mh2);
        assert_eq!(block.transaction_message_hashes[2], mh3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_writable_only_uses_transaction_metadata() {
        // Test that only writable accounts (per transaction metadata) are settled,
        // even if they're in the loaded accounts list
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);

        // For a system transfer, account indices 0 and 1 are writable (payer, recipient)
        // and 2 (system program) is read-only
        let owner = Pubkey::new_unique();
        let system_prog = solana_system_interface::program::id();

        let executed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![
                    (from.pubkey(), AccountSharedData::new(900, 0, &owner)),
                    (to, AccountSharedData::new(100, 0, &owner)),
                    (system_prog, AccountSharedData::new(1, 0, &owner)),
                ],
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 100,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: std::collections::HashMap::new(),
        }));

        let results = vec![(Ok(executed), tx)];
        let (_result, settled_accounts) = settle_capturing_accounts(None, &mut db, &results).await;

        // Both writable accounts (payer and recipient) should be settled
        assert_eq!(settled_accounts.len(), 2, "both writable accounts settled");
        let settlement_keys: Vec<_> = settled_accounts.iter().map(|(k, _)| *k).collect();
        assert!(settlement_keys.contains(&from.pubkey()), "payer settled");
        assert!(settlement_keys.contains(&to), "recipient settled");
        assert!(
            !settlement_keys.contains(&system_prog),
            "system program not settled (read-only)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_warm_redis_cache_with_postgres_data() {
        // Test that warm_redis_cache reads latest_slot and latest_blockhash from Postgres
        // and writes them to Redis
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        // Seed Postgres via settle_transactions
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let pk = Pubkey::new_unique();
        let executed = make_executed(vec![(
            pk,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        settle_transactions(
            0,
            None,
            &mut pg_db,
            None,
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let slot = pg_db.get_latest_slot().await.unwrap();
        let blockhash = pg_db.get_latest_blockhash().await.ok();
        warm_redis_cache(&postgres_db, &redis_db, slot, Some(0), blockhash)
            .await
            .unwrap();

        // Verify Redis was populated
        let mut conn = redis_db.connection.clone();
        let slot: Option<u64> = conn.get("latest_slot").await.ok();
        assert_eq!(slot, Some(0), "Redis latest_slot should be 0");
        let bh: Option<String> = conn.get("latest_blockhash").await.ok();
        assert!(bh.is_some(), "Redis latest_blockhash should be set");
    }

    /// A cache filled before this check existed names no deployment. Holding
    /// ledger state against a Postgres with no blocks, it can only be a previous
    /// ledger's, so it must not survive into the new one.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_an_unstamped_cache_when_postgres_is_empty() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let stale_pubkey = Pubkey::new_unique();
        let stale_slot = 4242u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", stale_slot).await.unwrap();

        warm_redis_cache(&postgres_db, &redis_db, None, None, None)
            .await
            .unwrap();

        let account_exists: bool = conn
            .exists(format!("account:{}", stale_pubkey))
            .await
            .unwrap();
        assert!(!account_exists, "stale account key must not survive");
        let slot: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(slot, None, "stale tip must not survive");
    }

    /// A reused Redis instance carrying another ledger's keys is the case the
    /// deployment identifier exists to catch.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_a_cache_from_another_deployment() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let stale_pubkey = Pubkey::new_unique();
        let stale_slot = 9000u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &[9u8; 16][..]).await.unwrap();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn
            .set(format!("block:{}", stale_slot), vec![4u8, 5, 6])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", stale_slot).await.unwrap();

        warm_redis_cache(&postgres_db, &redis_db, None, None, None)
            .await
            .unwrap();

        let account_exists: bool = conn
            .exists(format!("account:{}", stale_pubkey))
            .await
            .unwrap();
        assert!(
            !account_exists,
            "another ledger's accounts must not survive"
        );
        let block_exists: bool = conn.exists(format!("block:{}", stale_slot)).await.unwrap();
        assert!(!block_exists, "another ledger's blocks must not survive");

        let stamped: Option<Vec<u8>> = conn.get("deployment_id").await.unwrap();
        assert_eq!(
            stamped,
            Some(
                redis_coherence::read_deployment_id(&postgres_db)
                    .await
                    .unwrap()
            ),
            "the cache must be re-stamped with this deployment"
        );
    }

    /// Same deployment, but Postgres was restored to an earlier point: the cache
    /// holds slots the source of truth no longer has.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_a_cache_ahead_of_postgres() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let cached_slot = 9000u64;
        let postgres_slot = 10u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();
        let _: () = conn
            .set(format!("block:{}", cached_slot), vec![4u8, 5, 6])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", cached_slot).await.unwrap();

        warm_redis_cache(
            &postgres_db,
            &redis_db,
            Some(postgres_slot),
            Some(0),
            Some(Hash::new_unique()),
        )
        .await
        .unwrap();

        let block_exists: bool = conn.exists(format!("block:{}", cached_slot)).await.unwrap();
        assert!(!block_exists, "slots ahead of Postgres must not survive");
        let slot: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(
            slot,
            Some(postgres_slot),
            "the tip must be rewound to Postgres"
        );
    }

    /// Every cached ledger key family has to go, not just the ones a read path
    /// happens to consult today. A family left behind is exactly the kind of
    /// unvalidated state that made a reused cache dangerous.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_every_ledger_key_family() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let stale_pubkey = Pubkey::new_unique();
        let stale_signature = Signature::new_unique();
        let stale_slot = 9000u64;
        let prefixed_keys = [
            format!("account:{}", stale_pubkey),
            format!("tx:{}", stale_signature),
            format!("block:{}", stale_slot),
        ];
        let fixed_keys = [
            "block_slot_index",
            "transaction_count",
            "performance_samples",
            "latest_slot",
            "block_height",
            "latest_blockhash",
        ];

        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &[9u8; 16][..]).await.unwrap();
        for key in &prefixed_keys {
            let _: () = conn.set(key, vec![1u8, 2, 3]).await.unwrap();
        }
        // Written with the types an older build used, since the purge has to
        // clear what those builds left behind.
        let addr_sigs_key = format!("addr_sigs:{}", stale_pubkey);
        let _: () = conn
            .zadd(&addr_sigs_key, "deadbeef", stale_slot as f64)
            .await
            .unwrap();
        let _: () = conn
            .zadd("block_slot_index", stale_slot, stale_slot as f64)
            .await
            .unwrap();
        let _: () = conn.set("transaction_count", 77u64).await.unwrap();
        let _: () = conn.lpush("performance_samples", "{}").await.unwrap();
        let _: () = conn.set("latest_slot", stale_slot).await.unwrap();
        // A foreign height left behind would be served as this ledger's
        // getBlockHeight until the next mirrored block overwrote it.
        let _: () = conn.set("block_height", 77u64).await.unwrap();
        let _: () = conn
            .set("latest_blockhash", Hash::new_unique().to_string())
            .await
            .unwrap();

        warm_redis_cache(&postgres_db, &redis_db, None, None, None)
            .await
            .unwrap();

        for key in prefixed_keys.iter().chain(std::iter::once(&addr_sigs_key)) {
            let exists: bool = conn.exists(key).await.unwrap();
            assert!(!exists, "{} must not survive the purge", key);
        }
        for key in fixed_keys {
            let exists: bool = conn.exists(key).await.unwrap();
            assert!(!exists, "{} must not survive the purge", key);
        }
    }

    /// One SCAN round returns a bounded slice of the keyspace, so the purge has
    /// to keep walking until the cursor wraps. With more keys than fit in a
    /// round, a single-pass purge would leave most of them behind.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_more_keys_than_one_scan_round_returns() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        // Comfortably more than the purge's per-round COUNT of 512.
        let key_count = 1500;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &[9u8; 16][..]).await.unwrap();
        let mut seed = redis::pipe();
        for _ in 0..key_count {
            seed.set(format!("account:{}", Pubkey::new_unique()), vec![1u8, 2, 3]);
        }
        let _: () = seed.query_async(&mut conn).await.unwrap();

        warm_redis_cache(&postgres_db, &redis_db, None, None, None)
            .await
            .unwrap();

        let mut survivors = 0usize;
        let mut cursor = 0u64;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("account:*")
                .arg("COUNT")
                .arg(512)
                .query_async(&mut conn)
                .await
                .unwrap();
            survivors += keys.len();
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(survivors, 0, "the purge must walk the whole keyspace");
    }

    /// Mirroring a batch is what proves the cache is being maintained, so it has
    /// to extend the lease. A settler that mirrored without renewing would let a
    /// perfectly healthy cache lapse and send every read to Postgres.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_renews_the_cache_lease_after_mirroring() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let stamp_key = redis_coherence::DEPLOYMENT_ID_KEY;
        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        // One slot behind the batch below, so continuity holds and the mirror
        // actually runs.
        let last_slot = 200u64;
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", last_slot).await.unwrap();

        // Stands in for a lease most of the way through its life, so a renewal
        // shows up as the TTL climbing back rather than as a value we have to
        // wait out.
        let shortened_lease_secs = 5i64;
        let _: () = conn
            .pexpire(stamp_key, shortened_lease_secs * 1000)
            .await
            .unwrap();

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        settle_transactions(
            last_slot + 1,
            Some(LastBlock {
                slot: last_slot,
                blockhash: Hash::new_unique(),
                block_height: last_slot,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let ttl: i64 = conn.ttl(stamp_key).await.unwrap();
        assert!(
            ttl > shortened_lease_secs,
            "a mirrored batch must renew the lease, got TTL {ttl}"
        );
    }

    /// A cached tip that will not parse leaves the settler unable to tell whether
    /// batches were missed, and this is the case where Redis is perfectly
    /// reachable while the check still fails. Blocks keep being produced either
    /// way, so the cache has to stop being served immediately rather than when
    /// its lease eventually runs out.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_unstamps_a_cache_whose_continuity_cannot_be_checked() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let stamp_key = redis_coherence::DEPLOYMENT_ID_KEY;
        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        // The tip is read as a u64, so a value that is not one fails the check
        // without Redis being down.
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", "not-a-slot").await.unwrap();

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        settle_transactions(
            200 + 1,
            Some(LastBlock {
                slot: 200,
                blockhash: Hash::new_unique(),
                block_height: 200,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        // The eviction is spawned, so it lands within a moment rather than
        // before the settle returns.
        for _ in 0..30 {
            let stamped: Option<Vec<u8>> = conn.get(stamp_key).await.unwrap();
            if stamped.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("a cache whose continuity cannot be checked must stop being served");
    }

    /// Heartbeat blocks grow the cache without bound unless entries expire.
    /// Postgres is the source of truth, so an expired entry is a miss that
    /// re-reads, and the untouched tip keys keep the cache in service.
    #[tokio::test(flavor = "multi_thread")]
    async fn expired_cached_block_is_served_from_postgres() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        redis_raw.set_block_ttl_secs(1);

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();

        settle_transactions(
            0,
            None,
            &mut pg_db,
            Some(&mut redis_raw),
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let ttl: i64 = conn.ttl("block:0").await.unwrap();
        assert!(
            ttl > 0 && ttl <= 1,
            "the cached block must carry the configured expiry, got {ttl}"
        );

        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert!(
            !conn.exists::<_, bool>("block:0").await.unwrap(),
            "the entry must have expired"
        );

        // The tip keys are untouched by the expiry, so the cache is still served.
        let cache_db = AccountsDB::Redis(redis_raw.clone());
        assert!(
            cache_db.get_block(0).await.unwrap().is_some(),
            "an expired entry must fall through to Postgres"
        );
        assert_eq!(
            conn.get::<_, Option<Vec<u8>>>("deployment_id")
                .await
                .unwrap(),
            Some(deployment_id),
            "expiring block entries must not condemn the cache"
        );
        assert_eq!(
            conn.get::<_, Option<u64>>("latest_slot").await.unwrap(),
            Some(0),
            "the cached tip must survive the expiry"
        );
    }

    /// A heartbeat block sits many slots after the block it extends, so judging
    /// continuity against `slot - 1` would condemn the cache once per heartbeat
    /// forever. The cache must stay in service and keep mirroring.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sparse_block_does_not_condemn_the_cache() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();
        let _: () = conn.set("latest_slot", 200u64).await.unwrap();

        // Ten idle ticks later: the heartbeat block extends slot 200 at slot 210.
        settle_transactions(
            210,
            Some(LastBlock {
                slot: 200,
                blockhash: Hash::new_unique(),
                block_height: 20,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let stamped: Option<Vec<u8>> = conn.get("deployment_id").await.unwrap();
        assert_eq!(
            stamped,
            Some(deployment_id),
            "a block that extends the cached tip must leave the cache in service"
        );
        let mirrored: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(mirrored, Some(210), "the block must still be mirrored");
    }

    /// Settling a batch far ahead of the cached tip is what an outage looks like
    /// from the settler's side. The cache must be condemned and left untouched:
    /// if this write advanced the tip, it would close the only gap the startup
    /// check can see, and the accounts missed during the outage would be served
    /// as current from then on.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_unstamps_a_cache_that_missed_batches() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let cached_tip = 100u64;
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();
        let _: () = conn.set("latest_slot", cached_tip).await.unwrap();

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        settle_transactions(
            200 + 1,
            Some(LastBlock {
                slot: 200,
                blockhash: Hash::new_unique(),
                block_height: 200,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        // The stamp is the durable evidence, and clearing it takes the cache out
        // of service on the very next read. Writing carries on, so the tip
        // advances; that is safe precisely because reads are gated on the stamp
        // rather than on the tip.
        let stamped: Option<Vec<u8>> = conn.get("deployment_id").await.unwrap();
        assert_eq!(
            stamped, None,
            "a cache that missed batches must stop being served"
        );
    }

    /// RPC advertises the hash the moment Postgres commits, so a dedup window
    /// that trails the cache write rejects transactions built on it.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_publishes_the_blockhash_before_the_cache_write() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        // Stamped and one slot back, so the mirror is reached rather than skipped.
        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let last_slot = 200u64;
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", last_slot).await.unwrap();

        // Redis accepts the connection and then stops answering it.
        let pause = Duration::from_secs(5);
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(pause.as_millis() as u64)
            .arg("ALL")
            .query_async(&mut conn)
            .await
            .unwrap();

        let (blockhashes_tx, mut blockhashes_rx) = mpsc::unbounded_channel();
        let (accounts_tx, _accounts_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(16);

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let settling = tokio::spawn(async move {
            settle_transactions(
                last_slot + 1,
                Some(LastBlock {
                    slot: last_slot,
                    blockhash: Hash::new_unique(),
                    block_height: last_slot,
                }),
                &mut pg_db,
                Some(&mut redis_raw),
                &[(Ok(executed), tx)],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                Some(BlockPublishers {
                    blockhashes: &blockhashes_tx,
                    accounts: &accounts_tx,
                    address_signatures: &address_signatures_tx,
                }),
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .expect("the commit must succeed with the cache unreachable")
        });

        // Ordering, not speed: the paused cache always burns its whole budget,
        // so how much is left after the publish says which side it was on. The
        // timeout only stops a lost publish hanging the test.
        let published = tokio::time::timeout(Duration::from_secs(5), blockhashes_rx.recv())
            .await
            .expect("the blockhash must be published");
        let published_at = tokio::time::Instant::now();
        let settled = settling.await.unwrap();
        let cache_work_left = published_at.elapsed();

        assert!(
            cache_work_left >= CACHE_MIRROR_BUDGET / 2,
            "the publish must come before the cache work, but only {cache_work_left:?} of it was left afterwards"
        );
        assert_eq!(
            published,
            Some(settled.blockhash),
            "the published hash must be the one that was committed"
        );
    }

    /// BOB unpins the settled accounts on this ack, so an ack that trails the
    /// cache write holds the acceptance window behind committed state for the
    /// whole budget even though Postgres is healthy.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_publishes_the_accounts_ack_before_the_cache_write() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        // Stamped and one slot back, so the mirror is reached rather than skipped.
        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let last_slot = 200u64;
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", last_slot).await.unwrap();

        // Redis accepts the connection and then stops answering it.
        let pause = Duration::from_secs(5);
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(pause.as_millis() as u64)
            .arg("ALL")
            .query_async(&mut conn)
            .await
            .unwrap();

        let (blockhashes_tx, _blockhashes_rx) = mpsc::unbounded_channel();
        let (accounts_tx, mut accounts_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(16);

        let generation = 7u64;
        let written = Pubkey::new_unique();
        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            written,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let settling = tokio::spawn(async move {
            settle_transactions(
                last_slot + 1,
                Some(LastBlock {
                    slot: last_slot,
                    blockhash: Hash::new_unique(),
                    block_height: last_slot,
                }),
                &mut pg_db,
                Some(&mut redis_raw),
                &[(Ok(executed), tx)],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                Some(BlockPublishers {
                    blockhashes: &blockhashes_tx,
                    accounts: &accounts_tx,
                    address_signatures: &address_signatures_tx,
                }),
                generation,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .expect("the commit must succeed with the cache unreachable")
        });

        // Ordering, not speed: the paused cache always burns its whole budget,
        // so how much is left after the ack says which side it was on. The
        // timeout only stops a lost ack hanging the test.
        let acked = tokio::time::timeout(Duration::from_secs(5), accounts_rx.recv())
            .await
            .expect("the accounts ack must be published")
            .expect("the accounts channel must stay open");
        let acked_at = tokio::time::Instant::now();
        settling.await.unwrap();
        let cache_work_left = acked_at.elapsed();

        assert!(
            cache_work_left >= CACHE_MIRROR_BUDGET / 2,
            "the ack must come before the cache work, but only {cache_work_left:?} of it was left afterwards"
        );
        assert_eq!(
            acked.generation, generation,
            "the ack must carry the generation the call site read"
        );
        assert!(
            acked.accounts.iter().any(|(key, _)| *key == written),
            "the ack must carry the accounts this batch committed"
        );
    }

    /// Optional work must never hold the settle path past its budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn settle_gives_up_on_a_cache_that_stops_answering() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        // Stamped and one slot back, so the mirror is reached rather than skipped.
        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let last_slot = 200u64;
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", last_slot).await.unwrap();

        // What a settle costs on this machine, so the assertion below measures
        // the cache and not the Postgres commit under it.
        let healthy_started = tokio::time::Instant::now();
        let healthy = settle_transactions(
            last_slot + 1,
            Some(LastBlock {
                slot: last_slot,
                blockhash: Hash::new_unique(),
                block_height: last_slot,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        let healthy_took = healthy_started.elapsed();

        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(30_000u64)
            .arg("ALL")
            .query_async(&mut conn)
            .await
            .unwrap();

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let started = tokio::time::Instant::now();
        let settled = settle_transactions(
            healthy.slot + 1,
            Some(LastBlock {
                slot: healthy.slot,
                blockhash: healthy.blockhash,
                block_height: healthy.block_height,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();
        let waited = started.elapsed();

        assert!(
            waited < healthy_took + CACHE_MIRROR_BUDGET * 2,
            "the cache must not hold the settle path past its budget, waited {waited:?} against a healthy {healthy_took:?}"
        );
        assert!(
            pg_db.get_block(settled.slot).await.unwrap().is_some(),
            "the block must be committed whatever the cache did"
        );
    }

    /// A cache left holding pre-batch values while blocks keep coming must stop
    /// being served, budget overrun included.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_timed_out_mirror_takes_the_cache_out_of_service() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let stamp_key = redis_coherence::DEPLOYMENT_ID_KEY;
        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let last_slot = 200u64;
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", last_slot).await.unwrap();

        // Reads still served, so the check passes and only the mirror's write
        // hangs. Short enough that the pause lifts while the eviction retries.
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(500u64)
            .arg("WRITE")
            .query_async(&mut conn)
            .await
            .unwrap();

        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, &Pubkey::new_unique(), 100);
        let executed = make_executed(vec![(
            Pubkey::new_unique(),
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        settle_transactions(
            last_slot + 1,
            Some(LastBlock {
                slot: last_slot,
                blockhash: Hash::new_unique(),
                block_height: last_slot,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        // The eviction is spawned, so it lands once the pause lifts rather than
        // before the settle returns.
        for _ in 0..30 {
            let stamped: Option<Vec<u8>> = conn.get(stamp_key).await.unwrap();
            if stamped.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("a cache that could not be mirrored must stop being served");
    }

    /// Paying the budget once is the cost of finding out Redis is down. Paying
    /// it every block is not.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cache_that_keeps_timing_out_is_given_up_on() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", 200u64).await.unwrap();

        // Longer than every batch below can spend on its budget, so the cache
        // stays unresponsive for the whole run.
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(30_000u64)
            .arg("ALL")
            .query_async(&mut conn)
            .await
            .unwrap();

        let mut last = LastBlock {
            slot: 200,
            blockhash: Hash::new_unique(),
            block_height: 200,
        };
        for attempt in 1..=CACHE_FAILURE_LIMIT {
            let settled = settle_transactions(
                last.slot + 1,
                Some(last.clone()),
                &mut pg_db,
                Some(&mut redis_raw),
                &[],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                None,
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .unwrap();
            last = LastBlock {
                slot: settled.slot,
                blockhash: settled.blockhash,
                block_height: settled.block_height,
            };

            assert_eq!(
                redis_raw.has_failed_too_often(),
                attempt == CACHE_FAILURE_LIMIT,
                "the cache must be given up on at exactly {CACHE_FAILURE_LIMIT} failures, not at {attempt}"
            );
        }
    }

    /// A batch counts once however many ways it failed, so the limit means the
    /// number of batches it says. Locks the arithmetic, not any one route to
    /// double counting.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_unmirrored_batch_counts_as_one_failure() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        // A tip that is not a u64 fails the continuity check on every batch,
        // with Redis otherwise perfectly reachable.
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", "not-a-slot").await.unwrap();

        let mut last = LastBlock {
            slot: 200,
            blockhash: Hash::new_unique(),
            block_height: 200,
        };
        for _ in 0..CACHE_FAILURE_LIMIT - 1 {
            let settled = settle_transactions(
                last.slot + 1,
                Some(last.clone()),
                &mut pg_db,
                Some(&mut redis_raw),
                &[],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                None,
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .unwrap();
            last = LastBlock {
                slot: settled.slot,
                blockhash: settled.blockhash,
                block_height: settled.block_height,
            };
        }

        assert!(
            !redis_raw.has_failed_too_often(),
            "{} batches must not reach a limit of {CACHE_FAILURE_LIMIT}",
            CACHE_FAILURE_LIMIT - 1
        );
    }

    /// A cache failing two batches in three is broken without ever failing three
    /// in a row.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_intermittently_failing_cache_is_still_given_up_on() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let mut conn = redis_raw.connection.clone();
        let mut last = LastBlock {
            slot: 200,
            blockhash: Hash::new_unique(),
            block_height: 200,
        };
        // Never two failures in a row after the first pair, and never enough
        // successes to be a healthy cache either.
        for mirrors in [false, false, true, false, false] {
            // A tip one slot back is readable and contiguous, so the batch
            // mirrors. One that is not a u64 fails the continuity check.
            let tip = if mirrors {
                last.slot.to_string()
            } else {
                "not-a-slot".to_string()
            };
            let _: () = conn.set("latest_slot", tip).await.unwrap();

            let settled = settle_transactions(
                last.slot + 1,
                Some(last.clone()),
                &mut pg_db,
                Some(&mut redis_raw),
                &[],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                None,
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .unwrap();
            last = LastBlock {
                slot: settled.slot,
                blockhash: settled.blockhash,
                block_height: settled.block_height,
            };
        }

        assert!(
            redis_raw.has_failed_too_often(),
            "a cache failing more batches than it takes must still be given up on"
        );
    }

    /// A write that landed without renewing the lease is not a mirrored batch:
    /// the tip advanced so nothing condemns, and readers drop the cache anyway.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_that_cannot_renew_the_lease_counts_as_a_failure() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", 200u64).await.unwrap();

        // The write half of a mirror is plain SETs and stays permitted. Only the
        // lease renewal is refused, which is the half a reader depends on.
        let _: () = redis::cmd("ACL")
            .arg("SETUSER")
            .arg("default")
            .arg("-setex")
            .query_async(&mut conn)
            .await
            .unwrap();

        let mut last = LastBlock {
            slot: 200,
            blockhash: Hash::new_unique(),
            block_height: 200,
        };
        for _ in 0..CACHE_FAILURE_LIMIT {
            let settled = settle_transactions(
                last.slot + 1,
                Some(last.clone()),
                &mut pg_db,
                Some(&mut redis_raw),
                &[],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                None,
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .unwrap();
            last = LastBlock {
                slot: settled.slot,
                blockhash: settled.blockhash,
                block_height: settled.block_height,
            };
        }

        let cached_tip: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(
            cached_tip,
            Some(last.slot),
            "the write half must have landed for this test to be about the renewal"
        );
        assert!(
            redis_raw.has_failed_too_often(),
            "a batch that could not renew the lease must count as a failure"
        );
    }

    /// A cache that misses one batch and then keeps up must not be dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_mirrored_batch_pays_back_a_failure() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        // The tip is read as a u64, so a value that is not one fails the
        // continuity check with Redis perfectly reachable.
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", "not-a-slot").await.unwrap();

        let failed = settle_transactions(
            200 + 1,
            Some(LastBlock {
                slot: 200,
                blockhash: Hash::new_unique(),
                block_height: 200,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        // A readable tip one slot back, so the next batch mirrors.
        let _: () = conn.set("latest_slot", failed.slot).await.unwrap();
        settle_transactions(
            failed.slot + 1,
            Some(LastBlock {
                slot: failed.slot,
                blockhash: failed.blockhash,
                block_height: failed.block_height,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let cached_tip: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(
            cached_tip,
            Some(failed.slot + 1),
            "the second batch must actually have been mirrored"
        );
        assert!(
            !redis_raw.has_failed_too_often(),
            "a mirrored batch must pay back the failure before it"
        );
    }

    /// A cache given up on must cost nothing until its cooldown is up: neither
    /// the mirror budget nor another failure counted against it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_paused_mirror_leaves_the_cache_alone() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        // A tip that is not a u64 fails the continuity check, so every batch
        // below would count a failure if the mirror were reached at all.
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set("latest_slot", "not-a-slot").await.unwrap();

        redis_raw.pause_mirroring(Duration::from_secs(60));

        let mut last = LastBlock {
            slot: 200,
            blockhash: Hash::new_unique(),
            block_height: 200,
        };
        for _ in 0..CACHE_FAILURE_LIMIT {
            let settled = settle_transactions(
                last.slot + 1,
                Some(last.clone()),
                &mut pg_db,
                Some(&mut redis_raw),
                &[],
                &(Arc::new(NoopMetrics) as SharedMetrics),
                None,
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await
            .unwrap();
            last = LastBlock {
                slot: settled.slot,
                blockhash: settled.blockhash,
                block_height: settled.block_height,
            };
        }

        let tip: Option<String> = conn.get("latest_slot").await.unwrap();
        assert_eq!(
            tip.as_deref(),
            Some("not-a-slot"),
            "a paused mirror must not write to the cache"
        );
        assert!(
            !redis_raw.has_failed_too_often(),
            "a batch the mirror was never offered must not count as a failure"
        );
    }

    /// A failover must not cost the cache for the life of the node. What comes
    /// back holds pre-pause values, so it is rebuilt before a batch is trusted
    /// to it, and only then mirrored to again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_resumed_cache_is_rebuilt_before_it_is_mirrored_to() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        // A stale key and a tip frozen where mirroring stopped, as a cache the
        // settler gave up on is left.
        let stale_pubkey = Pubkey::new_unique();
        let mut conn = redis_raw.connection.clone();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", 200u64).await.unwrap();

        for _ in 0..CACHE_FAILURE_LIMIT {
            redis_raw.record_cache_failure();
        }
        // A cooldown with no time left, as the settler finds one it is owed a
        // probe for.
        redis_raw.pause_mirroring(Duration::ZERO);
        assert!(
            redis_raw.has_failed_too_often() && redis_raw.pause_has_lapsed(),
            "the cache must be given up on and due a probe for this test to mean anything"
        );

        probe_paused_cache(redis_raw.clone(), Arc::new(NoopMetrics) as SharedMetrics).await;
        assert!(
            !redis_raw.is_mirroring_paused(),
            "a probe that found the cache answering must lift the pause"
        );
        assert!(
            !redis_raw.has_failed_too_often(),
            "a probe that found the cache answering must clear the failures that took it off the mirror path"
        );

        // The rebuild is spawned, so it lands after the probe returns.
        let mut rebuilt = false;
        for _ in 0..30 {
            let stale: bool = conn
                .exists(format!("account:{}", stale_pubkey))
                .await
                .unwrap();
            let stamped: Option<Vec<u8>> =
                conn.get(redis_coherence::DEPLOYMENT_ID_KEY).await.unwrap();
            if !stale && stamped.as_ref() == Some(&deployment_id) {
                rebuilt = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            rebuilt,
            "a resumed cache must be purged and stamped back into service"
        );

        // The purge took the tip with it, so this batch mirrors into an empty
        // cache rather than one it has to be contiguous with.
        let mirrored = settle_transactions(
            500 + 1,
            Some(LastBlock {
                slot: 500,
                blockhash: Hash::new_unique(),
                block_height: 500,
            }),
            &mut pg_db,
            Some(&mut redis_raw),
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let tip: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(
            tip,
            Some(mirrored.slot),
            "the batch after the rebuild must be mirrored"
        );
    }

    /// The claim the cooldown exists for: a node that has given up on its cache
    /// picks it back up on its own, with no restart.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_given_up_cache_is_mirrored_to_again_without_a_restart() {
        let (pg_db, pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let url = crate::test_helpers::postgres_container_url(&pg, "test_db").await;
        // A handle of this test's own, for raw key access. The settler builds
        // its own, which is the one that gives up and probes.
        let (redis_db, redis) = start_test_redis(postgres_db.clone()).await;
        let redis_url = format!(
            "redis://{}:{}",
            redis.get_host().await.unwrap(),
            redis.get_host_port_ipv4(6379).await.unwrap()
        );
        let mut conn = redis_db.connection.clone();

        let (_exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let _worker = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: Some(redis_url),
            redis_block_ttl_secs: 0,
            blocktime_ms: 100,
            // Short enough to watch a give-up and its recovery inside one test.
            cache_mirror_cooldown: Duration::from_millis(300),
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Wait for the settler to be mirroring, so the corruption below lands on
        // a running mirror rather than on the startup alignment.
        let mut mirroring_at = None;
        for _ in 0..100 {
            let tip: Option<u64> = conn.get("latest_slot").await.unwrap_or(None);
            if let Some(tip) = tip {
                mirroring_at = Some(tip);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let mirroring_at =
            mirroring_at.expect("the settler must mirror before the cache is broken");

        // The tip is read as a u64, so this fails every continuity check from
        // here on without Redis being down, and the settler gives up on it.
        let _: () = conn.set("latest_slot", "not-a-slot").await.unwrap();

        // Only the probe gets past this: it has the bad tip purged, and the
        // batch after that writes a real one.
        let mut recovered = None;
        for _ in 0..100 {
            let tip: Option<u64> = conn.get("latest_slot").await.unwrap_or(None);
            if let Some(tip) = tip.filter(|slot| *slot > mirroring_at) {
                recovered = Some(tip);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let recovered =
            recovered.expect("a cache given up on must be mirrored to again without a restart");

        // Still mirroring, not just the one batch that followed the rebuild.
        // Past a heartbeat, because an idle node only produces a block then.
        tokio::time::sleep(HEARTBEAT_INTERVAL + Duration::from_millis(500)).await;
        let tip: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert!(
            tip.is_some_and(|slot| slot > recovered),
            "the cache must keep taking batches after it comes back, tip {tip:?} against {recovered}"
        );

        shutdown.cancel();
    }

    /// A cache that cannot be verified at boot must not cost the node its cache
    /// for the life of the process either. Same cooldown, same probe.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cache_unverifiable_at_boot_is_mirrored_to_once_it_recovers() {
        let (pg_db, pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let url = crate::test_helpers::postgres_container_url(&pg, "test_db").await;
        let (redis_db, redis) = start_test_redis(postgres_db.clone()).await;
        let redis_url = format!(
            "redis://{}:{}",
            redis.get_host().await.unwrap(),
            redis.get_host_port_ipv4(6379).await.unwrap()
        );
        let mut conn = redis_db.connection.clone();

        // A ledger key with no stamp beside it, so alignment has to purge, and
        // writes refused for long enough that it cannot.
        let _: () = conn
            .set(format!("account:{}", Pubkey::new_unique()), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(3_000u64)
            .arg("WRITE")
            .query_async(&mut conn)
            .await
            .unwrap();

        let (_exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let _worker = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: Some(redis_url),
            redis_block_ttl_secs: 0,
            blocktime_ms: 100,
            cache_mirror_cooldown: Duration::from_millis(300),
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // A dropped handle would never write this, however healthy Redis got.
        let mut mirrored = false;
        for _ in 0..150 {
            let tip: Option<u64> = conn.get("latest_slot").await.unwrap_or(None);
            if tip.is_some() {
                mirrored = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            mirrored,
            "a cache that failed its boot alignment must be mirrored to once it recovers"
        );

        shutdown.cancel();
    }

    /// A probe that finds a rebuild already purging the cache must not resume on
    /// it. That purge belongs to an older condemnation, so mirroring into what it
    /// has not reached yet is how pre-pause keys get stamped back into service.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_probe_does_not_resume_on_a_rebuild_it_did_not_start() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        // Redis is healthy, so only the held claim can stop this probe: a purge
        // that started before the pause is still walking the keyspace.
        assert!(
            redis_raw.try_begin_rebuild(),
            "the rebuild claim must be free for this test to hold it"
        );

        redis_raw.pause_mirroring(Duration::ZERO);
        assert!(redis_raw.try_begin_probe());
        probe_paused_cache(redis_raw.clone(), Arc::new(NoopMetrics) as SharedMetrics).await;

        assert!(
            redis_raw.is_mirroring_paused(),
            "a probe must not resume mirroring on a purge it did not start"
        );
        assert!(
            redis_raw.try_begin_probe(),
            "the probe must release its claim so the next cooldown can probe again"
        );
    }

    /// A probe is more than its PING: a cache that answers one and then refuses
    /// the write that takes it out of service must not come back on the strength
    /// of the PING alone, and must leave the next probe able to run.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_probe_that_cannot_take_the_cache_out_of_service_leaves_it_paused() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        // Reads stay served, so the PING passes and only the stamp clear behind
        // it hangs.
        let mut conn = redis_raw.connection.clone();
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(5_000u64)
            .arg("WRITE")
            .query_async(&mut conn)
            .await
            .unwrap();

        redis_raw.pause_mirroring(Duration::ZERO);
        assert!(redis_raw.try_begin_probe());
        probe_paused_cache(redis_raw.clone(), Arc::new(NoopMetrics) as SharedMetrics).await;

        // Lapsed and still paused: only a probe that got all the way through
        // lifts a pause, so neither the mirror path nor the final flush can
        // offer this cache a batch.
        assert!(
            redis_raw.pause_has_lapsed() && redis_raw.is_mirroring_paused(),
            "a probe that could not finish must leave the cache paused"
        );
        assert!(
            redis_raw.try_begin_probe(),
            "the probe must release its claim so the next cooldown can probe again"
        );
    }

    /// Postgres with no blocks cannot back any cached ledger state, whatever the
    /// tips say. Both tips absent compares equal, so without an explicit guard a
    /// correctly-stamped cache that lost only its `latest_slot` key, evicted
    /// under `allkeys-lru` or removed by hand, would survive against a
    /// truncated or replaced Postgres and keep serving its accounts.
    ///
    /// Stamped and tipless on purpose: an unstamped cache exits at the stamp
    /// guard and never reaches this one.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_a_stamped_tipless_cache_when_postgres_is_empty() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let stale_pubkey = Pubkey::new_unique();
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();

        warm_redis_cache(&postgres_db, &redis_db, None, None, None)
            .await
            .unwrap();

        let account_exists: bool = conn
            .exists(format!("account:{}", stale_pubkey))
            .await
            .unwrap();
        assert!(
            !account_exists,
            "cached accounts must not survive a Postgres with no blocks"
        );
    }

    /// A cache left *behind* Postgres is the crash window: the settler commits to
    /// Postgres before mirroring to Redis, so a kill between the two leaves the
    /// cached tip short and every account the batch touched holding its
    /// pre-batch value. Those keys are present, so reads hit them and never
    /// reach the fallback. Being behind is not being incomplete, and the cache
    /// must not survive it.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_purges_a_cache_behind_postgres() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let stale_pubkey = Pubkey::new_unique();
        let cached_slot = 41u64;
        let postgres_slot = 42u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();
        // Right deployment, one batch behind: the pre-batch balance is still here.
        let _: () = conn
            .set(format!("account:{}", stale_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", cached_slot).await.unwrap();

        warm_redis_cache(
            &postgres_db,
            &redis_db,
            Some(postgres_slot),
            Some(0),
            Some(Hash::new_unique()),
        )
        .await
        .unwrap();

        let account_exists: bool = conn
            .exists(format!("account:{}", stale_pubkey))
            .await
            .unwrap();
        assert!(
            !account_exists,
            "a pre-batch balance must not survive a tip that fell behind"
        );
        let slot: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(slot, Some(postgres_slot), "the tip must be Postgres's");
    }

    /// An empty cache is trivially coherent: nothing to purge, and the mismatch
    /// between an absent cached tip and a real Postgres tip is a normal first
    /// attach rather than a stale cache.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_accepts_an_empty_cache() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let slot = 42u64;
        warm_redis_cache(
            &postgres_db,
            &redis_db,
            Some(slot),
            Some(0),
            Some(Hash::new_unique()),
        )
        .await
        .unwrap();

        let mut conn = redis_db.connection.clone();
        let cached_slot: Option<u64> = conn.get("latest_slot").await.unwrap();
        assert_eq!(cached_slot, Some(slot), "the tip must be published");
        let stamped: Option<Vec<u8>> = conn.get("deployment_id").await.unwrap();
        assert_eq!(
            stamped,
            Some(
                redis_coherence::read_deployment_id(&postgres_db)
                    .await
                    .unwrap()
            ),
            "an empty cache must be stamped for this deployment"
        );
    }

    /// The counterweight to the purge tests: a cache that agrees with Postgres
    /// keeps its contents.
    #[tokio::test(flavor = "multi_thread")]
    async fn warm_redis_cache_keeps_a_coherent_cache() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_db, _redis) = start_test_redis(postgres_db.clone()).await;

        let deployment_id = redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        let cached_pubkey = Pubkey::new_unique();
        let slot = 10u64;
        let mut conn = redis_db.connection.clone();
        let _: () = conn.set("deployment_id", &deployment_id[..]).await.unwrap();
        let _: () = conn
            .set(format!("account:{}", cached_pubkey), vec![1u8, 2, 3])
            .await
            .unwrap();
        let _: () = conn.set("latest_slot", slot).await.unwrap();

        warm_redis_cache(
            &postgres_db,
            &redis_db,
            Some(slot),
            Some(0),
            Some(Hash::new_unique()),
        )
        .await
        .unwrap();

        let account_exists: bool = conn
            .exists(format!("account:{}", cached_pubkey))
            .await
            .unwrap();
        assert!(account_exists, "a coherent cache must be left intact");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_worker_perf_sample_tick_fires() {
        // Test that the performance sample tick fires after perf_sample_period_secs
        // and stores a sample in the database
        let (_db, pg_container) = start_test_postgres().await;
        let url = postgres_container_url(&pg_container, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (_settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (_settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _address_signatures_rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();

        let _handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx: _settled_accounts_tx,
            settled_blockhashes_tx: _settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url.clone(),
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 50,
            perf_sample_period_secs: 1, // fires after 1s
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Send a transaction so last_block is set before the perf tick
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let pk = Pubkey::new_unique();
        let executed = make_executed(vec![(
            pk,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![Ok(executed)],
            error_metrics: Default::default(),
            execute_timings: Default::default(),
            balance_collector: None,
        };
        exec_tx.send((output, vec![tx], 1)).await.unwrap();

        // Poll for perf sample with deadline instead of fixed sleep.
        // Perf tick fires after ~1s; poll every 100ms for up to 5s.
        let db_poll = AccountsDB::new(&url, false).await.unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let samples = db_poll.get_recent_performance_samples(10).await.unwrap();
            if !samples.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for perf sample to be stored"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        shutdown.cancel();
    }

    /// Writer-dropped must exit settler.
    #[tokio::test(flavor = "multi_thread")]
    async fn settler_aborts_when_address_index_writer_dropped() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, _settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, address_signatures_rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();

        let handle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms: 50,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Simulate the writer task exiting.
        drop(address_signatures_rx);

        // Tick triggers send-Err.
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let pk = Pubkey::new_unique();
        let executed = make_executed(vec![(
            pk,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        let output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![Ok(executed)],
            error_metrics: Default::default(),
            execute_timings: Default::default(),
            balance_collector: None,
        };
        exec_tx.send((output, vec![tx], 1)).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), handle.handle).await;
        assert!(
            result.is_ok(),
            "settle worker must exit when address-index writer is gone"
        );
    }

    // --- broadcast-before-side-effects ordering tests ---

    /// Build a single successful transfer fixture whose `write_batch` yields a
    /// non-empty address-index row set so the bounded send is actually exercised.
    fn single_successful_transfer() -> Vec<(TransactionProcessingResult, SanitizedTransaction)> {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let pk = Pubkey::new_unique();
        let processed = make_executed(vec![(
            pk,
            AccountSharedData::new(500, 0, &Pubkey::new_unique()),
        )]);
        vec![(Ok(processed), tx)]
    }

    /// The regression test. A blocked-yet-open address-index send must NOT
    /// gate the admission broadcasts: both the settled accounts and the new
    /// blockhash have to reach their consumers while the settler is still parked
    /// on the bounded address-index send, with accounts observed no later than
    /// the blockhash (the preserved relative order).
    #[tokio::test(flavor = "multi_thread")]
    async fn broadcasts_precede_blocked_address_signatures() {
        let (db, _pg) = start_test_postgres().await;
        let mut db = db;

        // capacity-1 channel pre-filled so the in-function send blocks.
        let (addr_sig_tx, mut addr_sig_rx) = mpsc::channel::<Vec<AddressSignatureRow>>(1);
        addr_sig_tx.send(Vec::new()).await.unwrap();

        let (settled_accounts_tx, mut settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, mut settled_blockhashes_rx) = mpsc::unbounded_channel();

        let results = single_successful_transfer();
        let last = LastBlock {
            slot: 5,
            blockhash: Hash::new_unique(),
            block_height: 5,
        };

        // Spawn the settle on its own task; move the DB in and back out.
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let task = tokio::spawn(async move {
            let r = settle_transactions(
                last.slot + 1,
                Some(last),
                &mut db,
                None,
                &results,
                &metrics,
                Some(BlockPublishers {
                    blockhashes: &settled_blockhashes_tx,
                    accounts: &settled_accounts_tx,
                    address_signatures: &addr_sig_tx,
                }),
                0,
                settle_now(),
                SETTLE_ATTEMPT_TIMEOUT,
            )
            .await;
            (r, db)
        });

        // Both broadcasts must arrive while addr-index is still undrained.
        let accounts = tokio::time::timeout(Duration::from_secs(2), settled_accounts_rx.recv())
            .await
            .expect("settled accounts must broadcast before the blocked addr-index send")
            .expect("accounts channel open");
        assert_eq!(
            accounts.accounts.len(),
            1,
            "the one writable account is broadcast"
        );

        let blockhash = tokio::time::timeout(Duration::from_secs(2), settled_blockhashes_rx.recv())
            .await
            .expect("settled blockhash must broadcast before the blocked addr-index send")
            .expect("blockhash channel open");
        assert_ne!(blockhash, Hash::default(), "non-genesis hash broadcast");

        // Drain addr-index (prefill + the new rows) so the parked send completes.
        let _prefill = addr_sig_rx.recv().await.expect("prefill row");
        let _rows = tokio::time::timeout(Duration::from_secs(2), addr_sig_rx.recv())
            .await
            .expect("addr-index rows eventually sent")
            .expect("addr-index channel open");

        let (result, _db) = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("settle task completes")
            .expect("settle task joins");
        assert!(result.is_ok(), "settle returns Ok after addr-index drains");
    }

    /// A block with no address-index rows must still broadcast the blockhash.
    /// Guards against nesting the broadcast under the `!addr_sig_rows.is_empty()`
    /// branch.
    #[tokio::test(flavor = "multi_thread")]
    async fn broadcasts_with_empty_address_signatures() {
        let (mut db, _pg) = start_test_postgres().await;

        let (addr_sig_tx, _addr_sig_rx) = mpsc::channel::<Vec<AddressSignatureRow>>(1);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, mut settled_blockhashes_rx) = mpsc::unbounded_channel();

        // Empty processing results: no addr-index rows produced.
        let result = settle_transactions(
            9 + 1,
            Some(LastBlock {
                slot: 9,
                blockhash: Hash::new_unique(),
                block_height: 9,
            }),
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            Some(BlockPublishers {
                blockhashes: &settled_blockhashes_tx,
                accounts: &settled_accounts_tx,
                address_signatures: &addr_sig_tx,
            }),
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let blockhash = tokio::time::timeout(Duration::from_secs(2), settled_blockhashes_rx.recv())
            .await
            .expect("blockhash broadcast even with empty addr-index rows")
            .expect("blockhash channel open");
        assert_eq!(blockhash, result.blockhash);
    }

    /// Both broadcasts are non-fatal. With both receivers dropped, the
    /// settle must still return Ok and the block must commit to Postgres.
    #[tokio::test(flavor = "multi_thread")]
    async fn broadcast_non_fatal_when_consumer_gone() {
        let (mut db, _pg) = start_test_postgres().await;

        let (settled_accounts_tx, settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (addr_sig_tx, _addr_sig_rx) = mpsc::channel::<Vec<AddressSignatureRow>>(1);
        // Drop both consumers before calling.
        drop(settled_accounts_rx);
        drop(settled_blockhashes_rx);

        let results = single_successful_transfer();
        let result = settle_transactions(
            0,
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            Some(BlockPublishers {
                blockhashes: &settled_blockhashes_tx,
                accounts: &settled_accounts_tx,
                address_signatures: &addr_sig_tx,
            }),
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await;

        assert!(
            result.is_ok(),
            "send failures to gone consumers must be non-fatal"
        );
        let settled = result.unwrap();
        assert!(
            settled.publisher_gone,
            "a gone consumer must be reported so the caller can wind down"
        );
        let slot = settled.slot;
        // The block must be durable despite the broadcast failures.
        let block = db.get_block(slot).await.unwrap();
        assert!(block.is_some(), "block committed to Postgres");
    }

    /// Genesis (last_block = None) still broadcasts the default hash.
    #[tokio::test(flavor = "multi_thread")]
    async fn genesis_blockhash_is_broadcast() {
        let (mut db, _pg) = start_test_postgres().await;

        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, mut settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (addr_sig_tx, _addr_sig_rx) = mpsc::channel::<Vec<AddressSignatureRow>>(1);

        settle_transactions(
            0,
            None,
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            Some(BlockPublishers {
                blockhashes: &settled_blockhashes_tx,
                accounts: &settled_accounts_tx,
                address_signatures: &addr_sig_tx,
            }),
            0,
            settle_now(),
            SETTLE_ATTEMPT_TIMEOUT,
        )
        .await
        .unwrap();

        let blockhash = tokio::time::timeout(Duration::from_secs(2), settled_blockhashes_rx.recv())
            .await
            .expect("genesis blockhash broadcast")
            .expect("blockhash channel open");
        assert_eq!(blockhash, Hash::default());
    }

    /// Keep-alive handles for a wired settle + dedup integration harness. The
    /// worker handles and dedup's input sender must outlive the assertions:
    /// dropping any of them closes a pipeline channel and shuts the workers down
    /// mid-test. The address-index receiver is pre-filled to capacity-1 and never
    /// drained, so any row-producing block parks on the addr-index send while
    /// empty blocks (no rows) pass through untouched.
    struct WiredPipeline {
        exec_tx: mpsc::Sender<ExecutedBatch>,
        addr_sig_rx: mpsc::Receiver<Vec<AddressSignatureRow>>,
        live_blockhashes: Arc<std::sync::RwLock<std::collections::LinkedList<Hash>>>,
        shutdown: CancellationToken,
        _settle: WorkerHandle,
        _dedup: WorkerHandle,
        _dedup_in_tx: mpsc::Sender<SanitizedTransaction>,
        _dedup_out_rx: mpsc::Receiver<SanitizedTransaction>,
        _settled_accounts_rx: mpsc::UnboundedReceiver<AccountSettlements>,
    }

    async fn wire_settle_and_dedup(
        url: String,
        blocktime_ms: u64,
        max_blockhashes: usize,
    ) -> WiredPipeline {
        use crate::stages::dedup::{start_dedup, DedupArgs};
        use std::collections::{HashMap, LinkedList};

        let (exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, addr_sig_rx) = mpsc::channel::<Vec<AddressSignatureRow>>(1);
        address_signatures_tx.send(Vec::new()).await.unwrap();

        let shutdown = CancellationToken::new();
        let _settle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            redis_block_ttl_secs: 0,
            blocktime_ms,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Dedup ingests the shared blockhash channel; we observe its window.
        let (dedup_in_tx, dedup_in_rx) = mpsc::channel::<SanitizedTransaction>(64);
        let (dedup_out_tx, dedup_out_rx) = mpsc::channel::<SanitizedTransaction>(64);
        let (_dedup, live_blockhashes) = start_dedup(DedupArgs {
            max_blockhashes,
            input_rx: dedup_in_rx,
            settled_blockhashes_rx,
            output_tx: dedup_out_tx,
            initial_live_blockhashes: LinkedList::new(),
            initial_dedup_cache: HashMap::new(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        WiredPipeline {
            exec_tx,
            addr_sig_rx,
            live_blockhashes,
            shutdown,
            _settle,
            _dedup,
            _dedup_in_tx: dedup_in_tx,
            _dedup_out_rx: dedup_out_rx,
            _settled_accounts_rx,
        }
    }

    /// Poll dedup's live window until `pred` holds or an 8s deadline elapses.
    async fn wait_for_window<F: Fn(&[Hash]) -> bool>(
        live: &Arc<std::sync::RwLock<std::collections::LinkedList<Hash>>>,
        pred: F,
        what: &str,
    ) -> Vec<Hash> {
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        loop {
            let window: Vec<Hash> = live
                .read()
                .expect("blockhash lock")
                .iter()
                .copied()
                .collect();
            if pred(&window) {
                return window;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{what}; last saw {} entries: {:?}",
                window.len(),
                window
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Everything an idle settler produced over `run_for`: the blocks it stored in
    /// slot order, and the blockhashes it published to dedup.
    struct IdleRun {
        blocks: Vec<BlockInfo>,
        published: Vec<Hash>,
    }

    /// Run a settler against a throwaway Postgres with no traffic at all, so
    /// every block it produces is a heartbeat block.
    async fn run_idle_settler(blocktime_ms: u64, run_for: Duration) -> IdleRun {
        let (db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let (_exec_tx, exec_rx) = mpsc::channel(RESULTS_CAP);
        let (settled_accounts_tx, _settled_accounts_rx) = mpsc::unbounded_channel();
        let (settled_blockhashes_tx, mut settled_blockhashes_rx) = mpsc::unbounded_channel();
        let (address_signatures_tx, _addr_sig_rx) = mpsc::channel::<Vec<AddressSignatureRow>>(64);

        let shutdown = CancellationToken::new();
        let _settle = start_settle_worker(SettleArgs {
            execution_results_rx: exec_rx,
            settled_accounts_tx,
            settled_blockhashes_tx,
            address_signatures_tx,
            accountsdb_connection_url: url,
            redis_cache_url: None,
            redis_block_ttl_secs: 0,
            blocktime_ms,
            cache_mirror_cooldown: CACHE_MIRROR_COOLDOWN,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // The settler waits SETTLE_START_DELAY_MS before its first tick.
        tokio::time::sleep(Duration::from_millis(SETTLE_START_DELAY_MS) + run_for).await;
        shutdown.cancel();

        let mut published = Vec::new();
        while let Ok(hash) = settled_blockhashes_rx.try_recv() {
            published.push(hash);
        }
        let tip = db.get_latest_slot().await.unwrap().expect("a tip block");
        let blocks = db.get_blocks_in_range(0, tip).await.unwrap();
        IdleRun { blocks, published }
    }

    /// The tick keeps advancing the slot while an idle node produces roughly one
    /// block a second, so the tip block's slot runs well ahead of its height.
    #[tokio::test(flavor = "multi_thread")]
    async fn idle_tick_advances_slot_without_producing_a_block() {
        let run = run_idle_settler(100, Duration::from_millis(2_600)).await;

        let tip = run.blocks.last().expect("at least one block");
        let height = tip.block_height.expect("a block height");
        assert!(
            tip.slot > height,
            "idle ticks must advance the slot past the height, saw slot {} height {}",
            tip.slot,
            height
        );
        // Ten ticks per heartbeat, so two heartbeats already open a gap of ~18.
        assert!(
            tip.slot - height >= 9,
            "the gap must track the tick-to-block ratio, saw slot {} height {}",
            tip.slot,
            height
        );
    }

    /// An idle node produces one block per heartbeat interval, not one per tick.
    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_produces_a_block_after_the_interval() {
        let run = run_idle_settler(100, Duration::from_millis(2_600)).await;

        // Genesis plus one block per elapsed heartbeat, with a tick of slack.
        assert!(
            (3..=5).contains(&run.blocks.len()),
            "expected genesis plus ~2 heartbeat blocks, got {}: {:?}",
            run.blocks.len(),
            run.blocks.iter().map(|b| b.slot).collect::<Vec<_>>()
        );
    }

    /// Block height counts blocks, so it is contiguous across a sparse chain
    /// whose slots are not.
    #[tokio::test(flavor = "multi_thread")]
    async fn block_height_counts_blocks_not_slots() {
        let run = run_idle_settler(100, Duration::from_millis(2_600)).await;

        let slots: Vec<u64> = run.blocks.iter().map(|b| b.slot).collect();
        assert!(
            slots.windows(2).any(|w| w[1] - w[0] > 1),
            "the chain must actually be sparse before height is worth checking: {slots:?}"
        );
        let heights: Vec<u64> = run
            .blocks
            .iter()
            .map(|b| b.block_height.expect("a block height"))
            .collect();
        let expected: Vec<u64> = (0..heights.len() as u64).collect();
        assert_eq!(
            heights, expected,
            "height must count blocks, slots {slots:?}"
        );
    }

    /// A block extends the last block actually produced, which the indexer's
    /// continuity walk requires: `slot - 1` names a slot with no block.
    #[tokio::test(flavor = "multi_thread")]
    async fn parent_names_the_previous_produced_block() {
        let run = run_idle_settler(100, Duration::from_millis(2_600)).await;

        let slots: Vec<u64> = run.blocks.iter().map(|b| b.slot).collect();
        assert!(
            slots.windows(2).any(|w| w[1] - w[0] > 1),
            "the chain must actually be sparse before linkage is worth checking: {slots:?}"
        );
        for pair in run.blocks.windows(2) {
            assert_eq!(
                pair[1].parent_slot, pair[0].slot,
                "a block must name the previous stored block, slots {slots:?}"
            );
            assert_eq!(
                pair[1].previous_blockhash, pair[0].blockhash,
                "a block must carry the previous stored block's hash"
            );
        }
    }

    /// One blockhash per block, never one per tick: an idle tick that produces
    /// nothing must not mint or publish a hash.
    #[tokio::test(flavor = "multi_thread")]
    async fn blockhash_is_minted_once_per_block() {
        let run = run_idle_settler(100, Duration::from_millis(2_600)).await;

        assert_eq!(
            run.published.len(),
            run.blocks.len(),
            "one published hash per stored block, slots {:?}",
            run.blocks.iter().map(|b| b.slot).collect::<Vec<_>>()
        );
    }

    /// Two heartbeat blocks carry no transactions at all, so only the slot and
    /// the block time separate their hashes. They must still be distinct.
    #[tokio::test(flavor = "multi_thread")]
    async fn consecutive_heartbeat_blocks_have_distinct_hashes() {
        use std::collections::HashSet;

        let run = run_idle_settler(100, Duration::from_millis(2_600)).await;

        // Genesis keeps the default hash by design, so judge the rest.
        let hashes: Vec<Hash> = run.blocks.iter().skip(1).map(|b| b.blockhash).collect();
        assert!(hashes.len() >= 2, "need two heartbeat blocks to compare");
        let distinct: HashSet<Hash> = hashes.iter().copied().collect();
        assert_eq!(distinct.len(), hashes.len(), "heartbeat hashes must differ");
    }

    /// Wiring + no-double-send: with no txs fed, empty blocks never touch
    /// the bounded addr-index send, so dedup's window must fill purely from the
    /// in-settle blockhash broadcast, to max_blockhashes entries that are all
    /// distinct. A forgotten worker-side send would re-broadcast a hash and break
    /// distinctness.
    #[tokio::test(flavor = "multi_thread")]
    async fn empty_tick_broadcasts_fill_live_window() {
        use std::collections::HashSet;

        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let max_blockhashes = 3usize;
        let p = wire_settle_and_dedup(url, 50, max_blockhashes).await;

        let window = wait_for_window(
            &p.live_blockhashes,
            |w| {
                let distinct: HashSet<Hash> = w.iter().copied().collect();
                w.len() == max_blockhashes && distinct.len() == w.len()
            },
            "empty-tick blockhash broadcasts must fill the live window with distinct hashes",
        )
        .await;
        assert_eq!(window.len(), max_blockhashes);

        p.shutdown.cancel();
    }

    /// The core SOLA6-16 property end-to-end: a row-producing block must
    /// broadcast its blockhash to dedup before the bounded address-index send,
    /// even when that send is blocked. The row tx is fed during the settler start
    /// delay so the first produced block carries it; that block commits,
    /// broadcasts its hash, then parks forever on the full, undrained addr-index
    /// send. dedup's window must therefore hold exactly that one hash and stay
    /// there (the park stops all further block production). The first block is
    /// genesis, whose hash is the default by design; that is irrelevant here since
    /// the test asserts a hash was published at all. Under the pre-fix ordering the
    /// block parks on the addr-index send before broadcasting, leaving the window
    /// empty and timing the wait out.
    #[tokio::test(flavor = "multi_thread")]
    async fn row_block_broadcast_precedes_blocked_addr_index() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;
        let p = wire_settle_and_dedup(url, 50, 3).await;

        // Feed the row tx during the start delay so the first produced block
        // carries it and parks on the addr-index send right after broadcasting.
        let (tx_result, tx) = single_successful_transfer().into_iter().next().unwrap();
        let output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![tx_result],
            error_metrics: Default::default(),
            execute_timings: Default::default(),
            balance_collector: None,
        };
        p.exec_tx.send((output, vec![tx], 1)).await.unwrap();

        wait_for_window(
            &p.live_blockhashes,
            |w| w.len() == 1,
            "row block must broadcast its hash before parking on the blocked addr-index send",
        )
        .await;

        // The window must stay at one entry: the row block parked the settler on
        // the addr-index send, so no further block is produced. If the first block
        // had instead been empty (row tx not buffered in time), the settler would
        // not have parked and the window would keep growing.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let window: Vec<Hash> = p
            .live_blockhashes
            .read()
            .expect("blockhash lock")
            .iter()
            .copied()
            .collect();
        assert_eq!(
            window.len(),
            1,
            "settler parked after the one row block; window did not grow"
        );

        // The addr-index receiver was never drained, so the broadcast was not
        // gated by it.
        assert_eq!(
            p.addr_sig_rx.capacity(),
            0,
            "addr-index channel stayed full (undrained) during the test"
        );

        p.shutdown.cancel();
    }

    /// Poll until the settler has committed a tip at or past `at_least`.
    async fn wait_for_tip(pg: &PostgresAccountsDB, at_least: u64, what: &str) -> BlockInfo {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(tip) = crate::accounts::get_tip::get_tip(pg).await.unwrap() {
                if tip.slot >= at_least {
                    return tip;
                }
            }
            assert!(std::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A restart must continue the existing chain, not start a second one.
    #[tokio::test(flavor = "multi_thread")]
    async fn settler_resumes_from_the_tip_after_a_restart() {
        let (db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref pg) = db else {
            panic!("Expected Postgres variant")
        };
        let pool = pg.pool.clone();
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let first_shutdown = CancellationToken::new();
        let (tx, _rx, handle, _sinks, _hb) = settler_under_test(
            url.clone(),
            50,
            RESULTS_CAP,
            Arc::new(NoopMetrics) as SharedMetrics,
            first_shutdown.clone(),
        )
        .await;
        wait_for_tip(pg, 0, "the first settler must produce a block").await;
        // The settler drains until its input closes, so retire the sender the way
        // a real executor would. Cancelling alone leaves it settling.
        first_shutdown.cancel();
        drop(tx);
        let _ = handle.handle.await;

        let first_tip = crate::accounts::get_tip::get_tip(pg)
            .await
            .unwrap()
            .expect("the first settler produced blocks");

        // Point the derived metadata at a hash no block carries; the restart must ignore it.
        sqlx::query("UPDATE metadata SET value = $1 WHERE key = 'latest_blockhash'")
            .bind(Hash::new_unique().as_ref().to_vec())
            .execute(pool.as_ref())
            .await
            .unwrap();

        let second_shutdown = CancellationToken::new();
        let (tx2, _rx2, handle2, _sinks2, _hb2) = settler_under_test(
            url,
            50,
            RESULTS_CAP,
            Arc::new(NoopMetrics) as SharedMetrics,
            second_shutdown.clone(),
        )
        .await;
        let second_tip = wait_for_tip(
            pg,
            first_tip.slot + 1,
            "the restarted settler must advance past the tip it resumed from",
        )
        .await;
        second_shutdown.cancel();
        drop(tx2);
        let _ = handle2.handle.await;
        assert!(
            second_tip.slot > first_tip.slot,
            "the restarted settler must advance past the tip it resumed from, {} then {}",
            first_tip.slot,
            second_tip.slot
        );

        // Idle ticks advance the slot without a block, so the successor is the
        // next block that exists rather than the next slot.
        let successor: Vec<u8> =
            sqlx::query_scalar("SELECT data FROM blocks WHERE slot > $1 ORDER BY slot LIMIT 1")
                .bind(first_tip.slot as i64)
                .fetch_one(pool.as_ref())
                .await
                .expect("the block after the old tip exists");
        let successor: BlockInfo = bincode::deserialize(&successor).unwrap();
        assert_eq!(
            successor.previous_blockhash, first_tip.blockhash,
            "the first block after a restart must chain off the tip block's own hash"
        );
        assert_eq!(
            successor.parent_slot, first_tip.slot,
            "the first block after a restart must name the tip as its parent"
        );
    }

    /// A tip the settler cannot read must stop it, never pass as an empty ledger.
    /// Resuming from genesis would overwrite slot zero, so the assertions below
    /// check the canonical row is still byte-for-byte intact.
    #[tokio::test(flavor = "multi_thread")]
    async fn settler_refuses_to_restart_from_genesis_when_the_tip_is_unreadable() {
        let (mut db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref pg) = db else {
            panic!("Expected Postgres variant")
        };
        let pool = pg.pool.clone();
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        db.store_block(crate::test_helpers::create_test_block_info(
            0,
            Hash::new_unique(),
        ))
        .await
        .unwrap();
        db.store_block(crate::test_helpers::create_test_block_info(
            1,
            Hash::new_unique(),
        ))
        .await
        .unwrap();

        let genesis_before: Vec<u8> = sqlx::query_scalar("SELECT data FROM blocks WHERE slot = 0")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();

        // Only the tip row is damaged, so the settler fails on the read it now depends on.
        sqlx::query("UPDATE blocks SET data = $1 WHERE slot = 1")
            .bind(vec![0xFFu8; 16])
            .execute(pool.as_ref())
            .await
            .unwrap();

        let shutdown = CancellationToken::new();
        let (_tx, _rx, handle, _sinks, _hb) = settler_under_test(
            url,
            50,
            RESULTS_CAP,
            Arc::new(NoopMetrics) as SharedMetrics,
            shutdown.clone(),
        )
        .await;

        let quit = tokio::time::timeout(
            Duration::from_millis(SETTLE_START_DELAY_MS + 3_000),
            handle.handle,
        )
        .await;
        assert!(
            quit.is_ok(),
            "the settler must quit when it cannot prove the tip"
        );
        assert!(
            !shutdown.is_cancelled(),
            "the settler must quit on its own, not because the test shut it down"
        );

        let max_slot: Option<i64> = sqlx::query_scalar("SELECT MAX(slot) FROM blocks")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
        assert_eq!(
            max_slot,
            Some(1),
            "a settler that cannot read the tip must not produce blocks"
        );

        let genesis_after: Vec<u8> = sqlx::query_scalar("SELECT data FROM blocks WHERE slot = 0")
            .fetch_one(pool.as_ref())
            .await
            .unwrap();
        assert_eq!(
            genesis_before, genesis_after,
            "the canonical slot zero must not be overwritten by a genesis restart"
        );
    }

    /// A cache the settler gave up on is left alone by idle ticks too. Ten writes
    /// a second into a paused cache is most of what the pause exists to stop.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_idle_tick_leaves_a_paused_cache_alone() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        publish_idle_slot(&postgres_db, Some(&redis_raw), 10)
            .await
            .unwrap();
        redis_raw.pause_mirroring(Duration::from_secs(60));
        publish_idle_slot(&postgres_db, Some(&redis_raw), 11)
            .await
            .unwrap();

        let mut conn = redis_raw.connection.clone();
        let mirrored: Option<u64> = conn
            .get(crate::accounts::current_slot::CURRENT_SLOT_KEY)
            .await
            .unwrap();
        assert_eq!(
            mirrored,
            Some(10),
            "a paused cache must not be written by an idle tick"
        );
        assert_eq!(
            pg_db.get_current_slot().await.unwrap(),
            Some(11),
            "the durable counter must advance whatever the cache is doing"
        );
    }

    /// The idle publish is awaited on the blocktime tick, so a cache that accepts
    /// the connection and stops answering would stop the chain: no slot advance,
    /// no heartbeat block, no expiry. The budget is what keeps that impossible.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_idle_tick_does_not_wait_on_the_cache_past_its_budget() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let mut conn = redis_raw.connection.clone();
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(30_000u64)
            .arg("ALL")
            .query_async(&mut conn)
            .await
            .unwrap();

        let started = tokio::time::Instant::now();
        // Its own ceiling, because the failure this pins is a hang.
        tokio::time::timeout(
            CACHE_MIRROR_BUDGET * 10,
            publish_idle_slot(&postgres_db, Some(&redis_raw), 42),
        )
        .await
        .expect("a stalled cache must not hold the settler tick")
        .expect("the durable half publishes whatever the cache does");
        let waited = started.elapsed();

        assert!(
            waited < CACHE_MIRROR_BUDGET * 3,
            "the idle publish must abandon the cache at its budget, waited {waited:?}"
        );
        assert_eq!(
            pg_db.get_current_slot().await.unwrap(),
            Some(42),
            "the slot must keep advancing while the cache is stalled"
        );
    }

    /// Same contract for the durable half: a Postgres that accepts the statement
    /// and sits on it would stop the tick just as a stalled cache would, and this
    /// is the one path in the loop with no retry budget to surface that through.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_idle_tick_does_not_wait_on_postgres_past_its_budget() {
        let (pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("Expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        publish_idle_slot(&postgres_db, None, 10)
            .await
            .expect("a quiet pool publishes");

        // A row lock held open is the smallest server-side stall there is: the
        // next upsert queues behind it for as long as this transaction lives.
        let mut lock = postgres_db.pool.begin().await.unwrap();
        sqlx::query("SELECT value FROM metadata WHERE key = $1 FOR UPDATE")
            .bind(crate::accounts::current_slot::CURRENT_SLOT_KEY)
            .fetch_one(&mut *lock)
            .await
            .unwrap();

        let started = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            IDLE_SLOT_PUBLISH_BUDGET * 10,
            publish_idle_slot(&postgres_db, None, 11),
        )
        .await
        .expect("a stalled Postgres must not hold the settler tick");
        let waited = started.elapsed();

        let error = outcome.expect_err("a write past its budget is a failure the tick reports");
        assert!(
            format!("{error:#}").contains("budget"),
            "the failure must name the budget, got: {error:#}"
        );
        assert!(
            waited < IDLE_SLOT_PUBLISH_BUDGET * 3,
            "the idle publish must abandon Postgres at its budget, waited {waited:?}"
        );

        // Releasing the lock is all it takes for the next tick to land, so the
        // stall was the lock and not a pool the timeout left unusable.
        lock.rollback().await.unwrap();
        publish_idle_slot(&postgres_db, None, 12)
            .await
            .expect("the tick after the stall publishes");
        assert_eq!(pg_db.get_current_slot().await.unwrap(), Some(12));
    }
}
