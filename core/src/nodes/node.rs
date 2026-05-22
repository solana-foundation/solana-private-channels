use {
    crate::{
        accounts::AccountsDB,
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
            AccountSettlement,
        },
    },
    futures::future::FutureExt,
    solana_hash::Hash,
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
    solana_svm::transaction_processor::LoadAndExecuteSanitizedTransactionsOutput,
    std::{sync::Arc, time::Duration},
    tokio::{sync::mpsc, task::JoinHandle},
    tokio_util::sync::CancellationToken,
    tracing::{error, info, warn},
};

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
    /// Max parallel SVM worker threads per batch (including the calling thread).
    /// Set to 1 to disable intra-batch parallelism entirely. Effective only for
    /// batches ≥ `MIN_PARALLEL_BATCH_SIZE`; smaller batches always run sequentially.
    pub max_svm_workers: usize,
    pub accountsdb_connection_url: String,
    pub admin_keys: Vec<Pubkey>, // Admin keys that can bypass SPL token program execution
    pub transaction_expiration_ms: u64,
    pub blocktime_ms: u64,
    pub perf_sample_period_secs: u64, // Performance sample collection period (default 60 seconds)
    pub metrics: SharedMetrics,
}

impl NodeConfig {
    /// Calculate max_blockhashes from transaction_expiration_ms and blocktime_ms
    /// This represents how many blockhashes we need to keep in the dedup cache
    pub fn max_blockhashes(&self) -> usize {
        (self.transaction_expiration_ms / self.blocktime_ms) as usize
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
            max_svm_workers: 8,
            accountsdb_connection_url: "postgresql://user:password@localhost:5432/private_channel"
                .to_string(),
            admin_keys: vec![],               // No admin keys by default
            transaction_expiration_ms: 15000, // 15 seconds default
            blocktime_ms: 100,                // 100ms default
            perf_sample_period_secs: 60,      // 60 seconds default
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
}

pub async fn run_node(config: NodeConfig) -> Result<NodeHandles, Box<dyn std::error::Error>> {
    // Validate configuration
    if config.blocktime_ms == 0 && matches!(config.mode, NodeMode::Write | NodeMode::Aio) {
        return Err("blocktime_ms cannot be 0 for write nodes".into());
    }
    if config.max_blockhashes() == 0 && matches!(config.mode, NodeMode::Write | NodeMode::Aio) {
        return Err(
            "transaction_expiration_ms must be >= blocktime_ms (max_blockhashes would be 0)".into(),
        );
    }

    // Create a single shutdown token for all services
    let shutdown_token = CancellationToken::new();

    // Heartbeat registry — populated for stages that actually run, consumed by /health.
    let mut heartbeats = crate::health::HeartbeatRegistry::new();

    // Only create write pipeline for Write and Aio modes
    let mut write_workers: Vec<WorkerHandle> = Vec::new();
    let (write_deps, live_blockhashes_arc) =
        if matches!(config.mode, NodeMode::Write | NodeMode::Aio) {
            // Create the dedup channel (receives from RPC, sends to sigverify) - unbounded
            let (dedup_tx, dedup_rx) = crate::stages::create_dedup_channel();

            // Create the sigverify channel (needed for NodeHandles in all modes)
            let (sigverify_tx, sigverify_rx) =
                async_channel::bounded::<SanitizedTransaction>(config.sigverify_queue_size);

            // Create sequencer channel (unbounded mpsc for single consumer)
            let (sequencer_tx, sequencer_rx) = mpsc::unbounded_channel::<SanitizedTransaction>();

            // Create batch channel between sequencer and executor (bounded for back-pressure)
            let (batch_tx, batch_rx) =
                mpsc::channel::<ConflictFreeBatch>(config.batch_channel_capacity);

            // Create execution results channel between executor and settler (unbounded for pipelining)
            let (execution_results_tx, execution_results_rx) = mpsc::unbounded_channel::<(
                LoadAndExecuteSanitizedTransactionsOutput,
                Vec<SanitizedTransaction>,
            )>();

            // Create settled accounts channel between settler and executor
            let (settled_accounts_tx, settled_accounts_rx) =
                mpsc::unbounded_channel::<Vec<(Pubkey, AccountSettlement)>>();

            // Create settled blockhashes channel between settler and dedup
            let (settled_blockhashes_tx, settled_blockhashes_rx) =
                mpsc::unbounded_channel::<Hash>();

            // Load persisted dedup state from DB before starting the stage.
            // Failure here is fatal: starting with an empty cache could allow
            // duplicate transactions to execute after a restart.
            let db = AccountsDB::new(&config.accountsdb_connection_url, true).await?;
            let (initial_live_blockhashes, initial_dedup_cache) =
                load_dedup_state(&db, config.max_blockhashes()).await?;

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

            // Start dedup stage (filters duplicate transactions before sigverify)
            let (dedup, live_blockhashes) = crate::stages::start_dedup(crate::stages::DedupArgs {
                max_blockhashes: config.max_blockhashes(),
                input_rx: dedup_rx,
                settled_blockhashes_rx,
                output_tx: sigverify_tx.clone(),
                shutdown_token: shutdown_token.clone(),
                initial_live_blockhashes,
                initial_dedup_cache,
                metrics: Arc::clone(&config.metrics),
                heartbeat: dedup_hb,
            })
            .await;
            write_workers.push(dedup);

            // Start sigverify worker pool
            let sigverify_workers = start_sigverify_workerpool(crate::stages::SigverifyArgs {
                num_workers: config.sigverify_workers,
                admin_keys: config.admin_keys.clone(),
                rx: sigverify_rx,
                sequencer_tx,
                shutdown_token: shutdown_token.clone(),
                metrics: Arc::clone(&config.metrics),
                heartbeat: sigverify_hb,
            })
            .await;
            write_workers.extend(sigverify_workers);

            // Start sequencer (produces conflict-free batches)
            let sequence = start_sequence_worker(crate::stages::SequencerArgs {
                max_tx_per_batch: config.max_tx_per_batch,
                batch_deadline_ms: config.batch_deadline_ms,
                rx: sequencer_rx,
                batch_tx,
                shutdown_token: shutdown_token.clone(),
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
                shutdown_token: shutdown_token.clone(),
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
                blocktime_ms: config.blocktime_ms,
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
                shutdown_token: shutdown_token.clone(),
                metrics: Arc::clone(&config.metrics),
                heartbeat: addr_index_writer_hb,
            })
            .await;
            write_workers.push(addr_index_writer);

            (
                Some(WriteDeps {
                    dedup_tx: dedup_tx.clone(),
                }),
                live_blockhashes,
            )
        } else {
            // Read-only node: no write pipeline, create empty live_blockhashes Arc
            use std::collections::LinkedList;
            use std::sync::{Arc, RwLock};
            (None, Arc::new(RwLock::new(LinkedList::new())))
        };

    // Start RPC service based on node mode
    let rpc_config = RpcServiceConfig {
        port: config.port,
        max_connections: config.max_connections,
        read_deps: match config.mode {
            NodeMode::Read | NodeMode::Aio => Some(ReadDeps {
                admin_keys: config.admin_keys,
                accounts_db: AccountsDB::new(&config.accountsdb_connection_url, true)
                    .await
                    .unwrap(),
                live_blockhashes: live_blockhashes_arc,
            }),
            NodeMode::Write => None,
        },
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

        let (completed_idx, _result, _remaining) = futures::future::select_all(futures).await;
        let worker_name = self.workers[completed_idx].name().to_string();

        error!("{} worker quit unexpectedly", worker_name);
        worker_name
    }

    pub async fn shutdown(self) {
        info!("Shutting down node...");

        // Cancel the token - this signals all services to shutdown
        self.shutdown_token.cancel();

        // Wait for all workers to finish
        for worker in self.workers {
            match tokio::time::timeout(Duration::from_secs(5), worker.handle).await {
                Ok(Ok(_)) => info!("{} stopped gracefully", worker.name),
                Ok(Err(e)) => error!("{} error: {:?}", worker.name, e),
                Err(_) => warn!("{} shutdown timeout", worker.name),
            }
        }

        info!("Node shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_run_node_rejects_zero_blocktime() {
        let config = NodeConfig {
            blocktime_ms: 0,
            ..Default::default()
        };

        let result = run_node(config).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.to_string(), "blocktime_ms cannot be 0 for write nodes");
    }

    #[tokio::test]
    async fn test_run_node_rejects_zero_max_blockhashes() {
        // transaction_expiration_ms < blocktime_ms → max_blockhashes() == 0
        let config = NodeConfig {
            transaction_expiration_ms: 50,
            blocktime_ms: 100,
            ..Default::default()
        };

        assert_eq!(config.max_blockhashes(), 0);
        let result = run_node(config).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(
            err.to_string(),
            "transaction_expiration_ms must be >= blocktime_ms (max_blockhashes would be 0)"
        );
    }
}
