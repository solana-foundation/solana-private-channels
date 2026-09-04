use {
    crate::{
        accounts::{
            address_index_repair::repair_address_signatures, postgres::PostgresAccountsDB,
            redis::RedisAccountsDB, AccountsDB,
        },
        rpc::{
            server::{start_rpc_service, RpcServiceConfig},
            ReadDeps, WriteDeps,
        },
        scheduler::ConflictFreeBatch,
        stage_metrics::{NoopMetrics, SharedMetrics},
        stages::{
            address_index_writer::{start_address_index_writer, AddressIndexWriterArgs},
            dedup::load_dedup_state,
            execution::start_execution_worker,
            sequencer::start_sequence_worker,
            settle::start_settle_worker,
            sigverify::start_sigverify_workerpool,
            AccountSettlements, ExecutedBatch,
        },
    },
    futures::future::FutureExt,
    solana_hash::Hash,
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
    std::{sync::Arc, time::Duration},
    tokio::{sync::mpsc, task::JoinHandle},
    tokio_util::sync::CancellationToken,
    tracing::{error, info, warn},
};

/// Total time the whole pipeline gets to drain on shutdown. A saturated drain
/// measures well under a second, and the settler bounds its own shutdown work
/// below this, so the remainder covers the cascade.
pub const DRAIN_DEADLINE: Duration = Duration::from_secs(6);

/// Shared across every worker that had to be aborted, not spent per worker: a
/// per-worker reserve would scale with the pipeline. `DRAIN_DEADLINE` plus this
/// is the whole shutdown, and it has to stay under the container stop grace.
const ABORT_RESERVE: Duration = Duration::from_secs(2);

/// RPC→dedup ingress queue capacity. Sized so steady state never sheds.
pub const DEFAULT_INGRESS_QUEUE_CAPACITY: usize = 10_000;
/// sigverify→sequencer queue capacity (mirrors the sigverify queue size).
pub const DEFAULT_SEQUENCER_QUEUE_CAPACITY: usize = 1000;
/// executor→settler results queue capacity.
pub const DEFAULT_EXECUTION_RESULTS_CAPACITY: usize = 1000;
/// The blockhash window in blocks, matching Solana.
pub const DEFAULT_MAX_BLOCKHASHES: usize = 150;
pub const DEFAULT_BLOCKTIME_MS: u64 = 100;

#[derive(Debug, Clone, PartialEq, clap::ValueEnum)]
pub enum NodeMode {
    /// Read-only node - serves read RPCs only
    Read,
    /// Write-only node - processes transactions only
    Write,
    /// All-in-one - both read and write
    Aio,
}

#[derive(Clone)]
pub struct NodeConfig {
    pub mode: NodeMode,
    pub port: u16,
    pub sigverify_queue_size: usize,
    pub sigverify_workers: usize,
    pub max_connections: usize,
    pub max_tx_per_batch: usize,
    pub batch_deadline_ms: u64,
    pub batch_channel_capacity: usize,
    /// RPC→dedup ingress queue capacity; a full queue sheds load at admission.
    pub ingress_queue_capacity: usize,
    /// sigverify→sequencer queue capacity; full applies upstream backpressure.
    pub sequencer_queue_capacity: usize,
    /// executor→settler results queue capacity; full applies upstream backpressure.
    pub execution_results_capacity: usize,
    /// Max parallel SVM worker threads per batch (including the calling thread).
    /// Set to 1 to disable intra-batch parallelism entirely. Effective only for
    /// batches ≥ `MIN_PARALLEL_BATCH_SIZE`; smaller batches always run sequentially.
    pub max_svm_workers: usize,
    pub accountsdb_connection_url: String,
    /// Optional Redis cache in front of the read path. Reads consult Redis
    /// first and fall through to `accountsdb_connection_url` on a miss.
    pub redis_cache_url: Option<String>,
    /// Expiry in seconds on cached block entries, bounding the growth an idle
    /// node's heartbeat blocks cause. Zero disables it.
    pub redis_block_ttl_secs: u64,
    pub admin_keys: Vec<Pubkey>, // Admin keys that can bypass SPL token program execution
    /// How many blocks a blockhash stays valid for. A block count, not a
    /// duration: blocks come from traffic and from the idle heartbeat, so the
    /// wall-clock window moves with load exactly as it does on Solana.
    pub max_blockhashes: usize,
    pub blocktime_ms: u64,
    pub perf_sample_period_secs: u64, // Performance sample collection period (default 60 seconds)
    pub metrics: SharedMetrics,
}

/// Resolve the blockhash window, honouring the deprecated millisecond field for
/// one release so an existing deployment keeps its effective window.
pub fn resolve_max_blockhashes(
    max_blockhashes: usize,
    transaction_expiration_ms: Option<u64>,
    blocktime_ms: u64,
) -> usize {
    match transaction_expiration_ms {
        Some(expiration_ms) => {
            let blocks = (expiration_ms / blocktime_ms.max(1)) as usize;
            warn!(
                "transaction_expiration_ms is deprecated and will be removed; use \
                 max_blockhashes, a block count. It overrides max_blockhashes, mapping \
                 {expiration_ms}ms at a {blocktime_ms}ms blocktime to {blocks} blocks."
            );
            blocks
        }
        None => max_blockhashes,
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            mode: NodeMode::Aio, // Default to all-in-one mode
            port: 8899,
            sigverify_queue_size: 1000,
            sigverify_workers: 4,
            max_connections: 100,
            max_tx_per_batch: 64,
            batch_deadline_ms: 10,
            batch_channel_capacity: 16,
            ingress_queue_capacity: DEFAULT_INGRESS_QUEUE_CAPACITY,
            sequencer_queue_capacity: DEFAULT_SEQUENCER_QUEUE_CAPACITY,
            execution_results_capacity: DEFAULT_EXECUTION_RESULTS_CAPACITY,
            max_svm_workers: 8,
            accountsdb_connection_url: "postgresql://user:password@localhost:5432/private_channel"
                .to_string(),
            redis_cache_url: None,
            redis_block_ttl_secs: 3600, // one hour, well past the blockhash window
            admin_keys: vec![],         // No admin keys by default
            max_blockhashes: DEFAULT_MAX_BLOCKHASHES,
            blocktime_ms: DEFAULT_BLOCKTIME_MS,
            perf_sample_period_secs: 60, // 60 seconds default
            metrics: Arc::new(NoopMetrics),
        }
    }
}

pub struct WorkerHandle {
    name: String,
    pub(crate) handle: JoinHandle<()>,
}

impl WorkerHandle {
    pub fn new(name: String, handle: JoinHandle<()>) -> Self {
        Self { name, handle }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct NodeHandles {
    workers: Vec<WorkerHandle>,
    shutdown_token: CancellationToken,
    /// Closed first on shutdown, which is what refuses admission. `None` on a
    /// read node, which has no write pipeline to close.
    ingress_tx: Option<async_channel::Sender<SanitizedTransaction>>,
}

/// How long a read node waits out a cache stamped for another deployment. Worth
/// waiting for in Aio, where the settler alongside this node purges and re-stamps
/// it moments later; a genuinely wrong Redis costs only this window and then
/// fails closed. An unstamped cache is not waited for at all, since the node
/// serves correctly from Postgres until a write node stamps one.
///
/// This covers the cache, not a Postgres the write node has not created the
/// schema in. That case fails earlier, when the cache handle reads the deployment
/// id, and is not retried.
///
/// Serving is not gated on this: every cached read rechecks the stamp for itself,
/// so a cache condemned later is dropped without waiting for a restart. Failing
/// here is about not starting a node whose cache is misconfigured, rather than
/// letting it come up and quietly serve every read from Postgres.
const CACHE_VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
const CACHE_VERIFY_INTERVAL: Duration = Duration::from_secs(1);

async fn wait_for_verified_cache(
    redis: &crate::accounts::redis::RedisAccountsDB,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CACHE_VERIFY_TIMEOUT;
    loop {
        match crate::accounts::redis_coherence::verify_cache_stamp(redis).await {
            Ok(()) => return Ok(()),
            Err(e) if tokio::time::Instant::now() < deadline => {
                warn!(
                    "Redis cache not usable yet ({}), waiting for a write node",
                    e
                );
                tokio::time::sleep(CACHE_VERIFY_INTERVAL).await;
            }
            Err(e) => return Err(format!("Redis cache never became usable: {e:#}").into()),
        }
    }
}

pub async fn run_node(config: NodeConfig) -> Result<NodeHandles, Box<dyn std::error::Error>> {
    // Validate configuration
    if config.blocktime_ms == 0 {
        return Err("blocktime_ms cannot be 0".into());
    }
    // All modes need a non-zero window: Read advertises it as last_valid_block_height, write modes size the dedup cache with it.
    if config.max_blockhashes == 0 {
        return Err(
            "max_blockhashes must be greater than 0 (if you set the deprecated \
                    transaction_expiration_ms, it must be at least blocktime_ms)"
                .into(),
        );
    }
    // Zero capacity would panic the bounded-channel constructors below; fail closed instead.
    if matches!(config.mode, NodeMode::Write | NodeMode::Aio) {
        for (name, cap) in [
            ("ingress_queue_capacity", config.ingress_queue_capacity),
            ("sequencer_queue_capacity", config.sequencer_queue_capacity),
            (
                "execution_results_capacity",
                config.execution_results_capacity,
            ),
        ] {
            if cap == 0 {
                return Err(format!("{name} must be greater than 0").into());
            }
        }
    }

    // Create a single shutdown token for all services
    let shutdown_token = CancellationToken::new();

    // Heartbeat registry — populated for stages that actually run, consumed by /health.
    let mut heartbeats = crate::health::HeartbeatRegistry::new();

    // Only create write pipeline for Write and Aio modes
    let mut write_workers: Vec<WorkerHandle> = Vec::new();
    let (write_deps, live_blockhashes_arc) =
        if matches!(config.mode, NodeMode::Write | NodeMode::Aio) {
            // RPC ingress channel (receives from RPC, feeds the sigverify worker
            // pool). MPMC so many sigverify workers can pull; a full queue sheds
            // load at RPC ingress.
            let (ingress_tx, ingress_rx) =
                crate::stages::create_ingress_channel(config.ingress_queue_capacity);

            // sigverify to dedup channel: dedup is a single consumer, so mpsc.
            let (dedup_tx, dedup_rx) =
                mpsc::channel::<SanitizedTransaction>(config.sigverify_queue_size);

            // Create sequencer channel (bounded so backpressure chains upstream)
            let (sequencer_tx, sequencer_rx) =
                mpsc::channel::<SanitizedTransaction>(config.sequencer_queue_capacity);

            // Create batch channel between sequencer and executor (bounded for back-pressure)
            let (batch_tx, batch_rx) =
                mpsc::channel::<ConflictFreeBatch>(config.batch_channel_capacity);

            // Create execution results channel between executor and settler (bounded for back-pressure)
            let (execution_results_tx, execution_results_rx) =
                mpsc::channel::<ExecutedBatch>(config.execution_results_capacity);

            // Create settled accounts channel between settler and executor
            let (settled_accounts_tx, settled_accounts_rx) =
                mpsc::unbounded_channel::<AccountSettlements>();

            // Create settled blockhashes channel between settler and dedup
            let (settled_blockhashes_tx, settled_blockhashes_rx) =
                mpsc::unbounded_channel::<Hash>();

            // Load persisted dedup state from DB before starting the stage.
            // Failure here is fatal: starting with an empty cache could allow
            // duplicate transactions to execute after a restart.
            //
            // Opened writable (read_only=false): this is the Write/Aio node, which
            // connects to the primary and owns address_signatures index
            // consistency. repair_address_signatures writes (seeds the watermark
            // and re-derives rows), so it must run against a writable handle.
            // The read-only node opens its own read_only=true handle below, where
            // the repair is skipped.
            let db = AccountsDB::new(&config.accountsdb_connection_url, false).await?;
            repair_address_signatures(&db, Arc::clone(&config.metrics)).await?;
            let (initial_live_blockhashes, initial_dedup_cache) =
                load_dedup_state(&db, config.max_blockhashes).await?;

            let dedup_hb = crate::health::StageHeartbeat::new();
            let sigverify_hb = crate::health::StageHeartbeat::new();
            let sequencer_hb = crate::health::StageHeartbeat::new();
            let executor_hb = crate::health::StageHeartbeat::new();
            let settler_hb = crate::health::StageHeartbeat::new();
            let addr_index_writer_hb = crate::health::StageHeartbeat::new();
            heartbeats.dedup = Some(Arc::clone(&dedup_hb));
            heartbeats.sigverify = Some(Arc::clone(&sigverify_hb));
            heartbeats.sequencer = Some(Arc::clone(&sequencer_hb));
            heartbeats.executor = Some(Arc::clone(&executor_hb));
            heartbeats.settler = Some(Arc::clone(&settler_hb));
            heartbeats.address_index_writer = Some(Arc::clone(&addr_index_writer_hb));

            // Start sigverify worker pool (first stage). Verification runs before
            // dedup so only verified transactions ever reach the dedup cache.
            let sigverify_workers = start_sigverify_workerpool(crate::stages::SigverifyArgs {
                num_workers: config.sigverify_workers,
                admin_keys: config.admin_keys.clone(),
                rx: ingress_rx,
                output_tx: dedup_tx,
                metrics: Arc::clone(&config.metrics),
                heartbeat: sigverify_hb,
            })
            .await;
            write_workers.extend(sigverify_workers);

            // Start dedup stage (drops replays after verification, keyed on the
            // message hash so signature variants of one message collapse to one).
            let (dedup, live_blockhashes) = crate::stages::start_dedup(crate::stages::DedupArgs {
                max_blockhashes: config.max_blockhashes,
                input_rx: dedup_rx,
                settled_blockhashes_rx,
                output_tx: sequencer_tx,
                initial_live_blockhashes,
                initial_dedup_cache,
                metrics: Arc::clone(&config.metrics),
                heartbeat: dedup_hb,
            })
            .await;
            write_workers.push(dedup);

            // Start sequencer (produces conflict-free batches)
            let sequence = start_sequence_worker(crate::stages::SequencerArgs {
                max_tx_per_batch: config.max_tx_per_batch,
                batch_deadline_ms: config.batch_deadline_ms,
                rx: sequencer_rx,
                batch_tx,
                metrics: Arc::clone(&config.metrics),
                heartbeat: sequencer_hb,
            })
            .await;
            write_workers.push(sequence);

            // Start executor (executes and settles batches)
            let execution = start_execution_worker(crate::stages::ExecutionArgs {
                batch_rx,
                settled_accounts_rx,
                execution_results_tx,
                accountsdb_connection_url: config.accountsdb_connection_url.clone(),
                metrics: Arc::clone(&config.metrics),
                max_svm_workers: config.max_svm_workers,
                heartbeat: executor_hb,
                live_blockhashes: Arc::clone(&live_blockhashes),
            })
            .await;
            write_workers.push(execution);

            // Each item is one tick worth of (address, slot, signature) rows.
            const ADDR_SIG_QUEUE_CAPACITY: usize = 1024;
            // Hard cap on rows per writer COMMIT so individual flushes stay
            // sub-second even under sustained load, keeps PG commit latency
            // bounded regardless of how much the writer has backlogged.
            const ADDR_SIG_FLUSH_CHUNK: usize = 5000;
            let (addr_sig_tx, addr_sig_rx) = mpsc::channel(ADDR_SIG_QUEUE_CAPACITY);

            let settle = start_settle_worker(crate::stages::SettleArgs {
                execution_results_rx,
                settled_accounts_tx,
                settled_blockhashes_tx,
                address_signatures_tx: addr_sig_tx,
                accountsdb_connection_url: config.accountsdb_connection_url.clone(),
                redis_cache_url: config.redis_cache_url.clone(),
                redis_block_ttl_secs: config.redis_block_ttl_secs,
                blocktime_ms: config.blocktime_ms,
                cache_mirror_cooldown: crate::stages::settle::CACHE_MIRROR_COOLDOWN,
                perf_sample_period_secs: config.perf_sample_period_secs,
                shutdown_token: shutdown_token.clone(),
                metrics: Arc::clone(&config.metrics),
                heartbeat: settler_hb,
            })
            .await;
            write_workers.push(settle);

            // Push the writer AFTER the settler so shutdown awaits in the
            // right order: settler drains its buffer, drops its sender, the
            // writer's recv_many returns 0, then it flushes any remainder.
            let addr_index_writer = start_address_index_writer(AddressIndexWriterArgs {
                rows_rx: addr_sig_rx,
                accountsdb_connection_url: config.accountsdb_connection_url.clone(),
                flush_chunk_size: ADDR_SIG_FLUSH_CHUNK,
                metrics: Arc::clone(&config.metrics),
                heartbeat: addr_index_writer_hb,
            })
            .await;
            write_workers.push(addr_index_writer);

            (
                Some(WriteDeps {
                    dedup_tx: ingress_tx,
                    metrics: Arc::clone(&config.metrics),
                }),
                live_blockhashes,
            )
        } else {
            // Read-only node: no write pipeline, create empty live_blockhashes Arc
            use std::collections::LinkedList;
            use std::sync::{Arc, RwLock};
            (None, Arc::new(RwLock::new(LinkedList::new())))
        };

    let read_deps = match config.mode {
        NodeMode::Read | NodeMode::Aio => {
            let accounts_db = match config.redis_cache_url {
                // Redis in front of Postgres. Postgres stays reachable so a
                // key missing from the cache resolves against the source of
                // truth instead of reading as an absence.
                Some(ref redis_url) => {
                    let postgres = PostgresAccountsDB::new(&config.accountsdb_connection_url, true)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to create PostgresAccountsDB: {}", e)
                        })?;
                    let redis = RedisAccountsDB::new(redis_url, postgres)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to create RedisAccountsDB: {}", e))?;
                    // Only a write node aligns the cache, so wait for one to
                    // publish a stamp rather than serving whatever is there. An
                    // empty cache verifies immediately. Foreign state verifies
                    // only once a write node purges it, which in Aio is the
                    // settler alongside this one.
                    wait_for_verified_cache(&redis).await?;
                    info!("Read path caching through Redis with Postgres fallback");
                    AccountsDB::Redis(redis)
                }
                None => AccountsDB::new(&config.accountsdb_connection_url, true).await?,
            };
            // Read nodes don't repair: the write node owns the address_signatures
            // index and repairs it on the primary; the read-only replica receives
            // it via replication (repair would write, which fails on a standby).
            let max_blockhashes = config.max_blockhashes as u64;
            Some(ReadDeps {
                admin_keys: config.admin_keys,
                accounts_db,
                live_blockhashes: live_blockhashes_arc,
                max_blockhashes,
            })
        }
        NodeMode::Write => None,
    };

    // The admission handle, kept so shutdown can close it before anything else.
    let ingress_tx = write_deps.as_ref().map(|deps| deps.dedup_tx.clone());

    let rpc_config = RpcServiceConfig {
        port: config.port,
        max_connections: config.max_connections,
        read_deps,
        write_deps,
        heartbeats,
        shutdown_token: shutdown_token.clone(),
    };
    let rpc_handle = start_rpc_service(rpc_config).await?;

    info!("PrivateChannel node started:");
    info!("  Mode: {:?}", config.mode);
    info!("  RPC port: {}", config.port);
    if matches!(config.mode, NodeMode::Write | NodeMode::Aio) {
        info!("  Sigverify workers: {}", config.sigverify_workers);
        info!("  Max transactions per batch: {}", config.max_tx_per_batch);
        info!("  Max SVM workers: {}", config.max_svm_workers);
    }
    info!("  Max connections: {}", config.max_connections);

    // Build vector of all worker handles
    let mut workers = vec![rpc_handle];
    workers.extend(write_workers);

    Ok(NodeHandles {
        workers,
        shutdown_token,
        ingress_tx,
    })
}

impl NodeHandles {
    /// Wait for any worker to quit
    /// Returns the name of the worker that quit
    pub async fn wait_for_any_worker_quit(&mut self) -> String {
        // Use futures::future::select_all to wait for any handle to complete
        let futures: Vec<_> = self
            .workers
            .iter_mut()
            .enumerate()
            .map(|(idx, worker)| {
                let future = (&mut worker.handle).map(move |_| idx);
                Box::pin(future)
            })
            .collect();

        let (completed_idx, _result, remaining) = futures::future::select_all(futures).await;
        // Released before the list is touched, and the finished worker is taken
        // out of it: that poll consumed the task's output, and polling a
        // finished JoinHandle again panics. `remove` rather than `swap_remove`
        // because shutdown drains in pipeline order.
        drop(remaining);
        let worker_name = self.workers.remove(completed_idx).name().to_string();

        error!("{} worker quit unexpectedly", worker_name);
        worker_name
    }

    pub async fn shutdown(self) {
        info!("Shutting down node...");

        // Closes admission. Every stage after the ingress edge exits when its
        // own input closes, so cancelling here starts a drain that walks the
        // pipeline in order rather than stopping all stages at once.
        // Closed before the token so admission stops first. A closed channel
        // still hands its buffered transactions to sigverify, so this refuses
        // new work without discarding anything already accepted. Reversed, the
        // stages would start unwinding while admission was still open.
        if let Some(ref ingress_tx) = self.ingress_tx {
            ingress_tx.close();
        }
        self.shutdown_token.cancel();

        // One deadline for the whole drain, not one per worker: the workers are
        // awaited in pipeline order, so a per-worker budget would multiply by
        // the number of stages and overrun the container's stop grace period,
        // which kills the process mid-drain and loses what the order preserved.
        let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
        // One reserve for all aborts, so the total stays bounded however many
        // workers overrun.
        let abort_deadline = deadline + ABORT_RESERVE;
        let mut overran = false;
        for mut worker in self.workers {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, &mut worker.handle).await {
                Ok(Ok(_)) => info!("{} stopped gracefully", worker.name),
                Ok(Err(e)) => error!("{} error: {:?}", worker.name, e),
                Err(_) => {
                    // Dropping a JoinHandle detaches the task rather than
                    // stopping it, so an in-process restart would leave this
                    // stage holding its pool and channels beside the new one.
                    worker.handle.abort();
                    overran = true;

                    // `abort()` only schedules cancellation, so wait for the task
                    // to actually end. Bounded by the shared reserve, because a
                    // task that never yields cannot be cancelled at all and must
                    // not hold shutdown open past the container's stop grace.
                    let stopped = tokio::time::timeout_at(abort_deadline, &mut worker.handle)
                        .await
                        .is_ok();
                    if stopped {
                        warn!(
                            "{} did not drain within the deadline and was aborted",
                            worker.name
                        );
                    } else {
                        error!(
                            "{} ignored the abort and is still running; an in-process restart would overlap it",
                            worker.name
                        );
                    }
                }
            }
        }

        if overran {
            warn!("Node shutdown finished with at least one stage aborted mid-drain");
        } else {
            info!("Node shutdown complete");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Docker's default SIGTERM-to-SIGKILL window. The whole shutdown has to
    /// finish inside it or the process is killed part-way through.
    const CONTAINER_STOP_GRACE: Duration = Duration::from_secs(10);

    fn handles_from(workers: Vec<WorkerHandle>) -> NodeHandles {
        NodeHandles {
            workers,
            shutdown_token: CancellationToken::new(),
            ingress_tx: None,
        }
    }

    /// A handle whose output was already taken must leave the drain list. Tokio
    /// panics when a finished JoinHandle is polled again, and that panic escapes
    /// shutdown and main, so the process aborts instead of draining.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_quit_worker_is_not_polled_again_by_shutdown() {
        let quitting = tokio::spawn(async {});
        let live = tokio::spawn(async {});
        let mut handles = handles_from(vec![
            WorkerHandle::new("Quitting".to_string(), quitting),
            WorkerHandle::new("Live".to_string(), live),
        ]);

        handles.wait_for_any_worker_quit().await;
        handles.shutdown().await;
    }

    /// The name must come from the worker that actually finished, and every other
    /// worker must still be drained in order.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_any_worker_quit_names_the_worker_that_quit() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let blocked = tokio::spawn(async move {
            let _ = rx.await;
        });
        let quitting = tokio::spawn(async {});
        let mut handles = handles_from(vec![
            WorkerHandle::new("Blocked".to_string(), blocked),
            WorkerHandle::new("Quitting".to_string(), quitting),
        ]);

        let name = handles.wait_for_any_worker_quit().await;
        assert_eq!(name, "Quitting");
        assert_eq!(handles.workers.len(), 1, "the finished worker must be gone");
        assert_eq!(handles.workers[0].name(), "Blocked");

        // Let the survivor exit so shutdown drains it rather than aborting it.
        let _ = tx.send(());
        handles.shutdown().await;
    }

    /// The abort reserve is shared, not per worker. Spent per worker it would
    /// scale with the pipeline and push the whole shutdown past the stop grace.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_abort_reserve_does_not_scale_with_worker_count() {
        let workers: Vec<WorkerHandle> = (0..6)
            .map(|i| {
                let spinning = tokio::task::spawn_blocking(|| {
                    std::thread::sleep(Duration::from_secs(30));
                });
                WorkerHandle::new(format!("Spinning{i}"), spinning)
            })
            .collect();
        let handles = handles_from(workers);

        let started = tokio::time::Instant::now();
        handles.shutdown().await;
        assert!(
            started.elapsed() < CONTAINER_STOP_GRACE,
            "shutdown took {:?}, which does not fit the container stop grace",
            started.elapsed()
        );
    }

    /// A worker that outlives the drain must be stopped, not merely stopped
    /// waiting on. A dropped JoinHandle leaves the task running, so an
    /// in-process restart would put a second pipeline on the same database.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_worker_that_overruns_the_drain_is_aborted() {
        // Set when the task is dropped, which only happens if it was aborted.
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let stopped = Arc::new(AtomicBool::new(false));
        let flag = DropFlag(Arc::clone(&stopped));
        let stuck = tokio::spawn(async move {
            let _flag = flag;
            std::future::pending::<()>().await
        });
        let handles = NodeHandles {
            workers: vec![WorkerHandle::new("Stuck".to_string(), stuck)],
            shutdown_token: CancellationToken::new(),
            ingress_tx: None,
        };

        let started = tokio::time::Instant::now();
        handles.shutdown().await;
        assert!(
            started.elapsed() < DRAIN_DEADLINE + Duration::from_secs(2),
            "shutdown must return once the drain deadline passes"
        );

        // Checked with no grace period: `abort()` only schedules cancellation, so
        // returning before the task is gone lets a replacement pipeline overlap
        // the old one, which is the whole reason for aborting at all.
        assert!(
            stopped.load(Ordering::SeqCst),
            "the overrunning worker was still running when shutdown returned"
        );
    }

    /// A task with no await point cannot be cancelled, so waiting on it must not
    /// hold the drain open past its budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_uncancellable_worker_does_not_extend_the_drain() {
        let spinning = tokio::task::spawn_blocking(|| {
            std::thread::sleep(std::time::Duration::from_secs(30));
        });
        let handles = NodeHandles {
            workers: vec![WorkerHandle::new("Spinning".to_string(), spinning)],
            shutdown_token: CancellationToken::new(),
            ingress_tx: None,
        };

        let started = tokio::time::Instant::now();
        handles.shutdown().await;
        assert!(
            started.elapsed() < DRAIN_DEADLINE + Duration::from_secs(3),
            "shutdown waited on a task that can never be cancelled"
        );
    }

    #[tokio::test]
    async fn test_run_node_rejects_zero_blocktime() {
        let config = NodeConfig {
            blocktime_ms: 0,
            ..Default::default()
        };

        let result = run_node(config).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.to_string(), "blocktime_ms cannot be 0");
    }

    #[tokio::test]
    async fn test_run_node_rejects_zero_max_blockhashes() {
        let config = NodeConfig {
            max_blockhashes: 0,
            ..Default::default()
        };

        let result = run_node(config).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .to_string()
            .contains("max_blockhashes must be greater than 0"));
    }

    /// The deprecated duration maps to the block count it used to imply, so an
    /// existing deployment keeps its effective window across the upgrade.
    #[test]
    fn expiry_config_migrates_from_milliseconds() {
        assert_eq!(resolve_max_blockhashes(150, Some(15_000), 100), 150);
        assert_eq!(resolve_max_blockhashes(150, Some(60_000), 100), 600);
        // Absent, the block count is taken as given.
        assert_eq!(resolve_max_blockhashes(300, None, 100), 300);
        // The deprecated field wins while it is set, so a migration is visible.
        assert_eq!(resolve_max_blockhashes(300, Some(15_000), 100), 150);
    }

    #[tokio::test]
    async fn test_run_node_rejects_zero_queue_capacity() {
        let config = NodeConfig {
            sequencer_queue_capacity: 0,
            ..Default::default()
        };

        let result = run_node(config).await;
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "sequencer_queue_capacity must be greater than 0"
        );
    }
}
