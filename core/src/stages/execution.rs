use {
    crate::{
        accounts::{bob::BOB, get_accounts::AccountLoadError, AccountsDB},
        nodes::node::WorkerHandle,
        processor::{
            create_transaction_batch_processor, get_transaction_check_results,
            PrivateChannelForkGraph,
        },
        scheduler::ConflictFreeBatch,
        stage_metrics::SharedMetrics,
        stages::{retained_bytes_of, AccountSettlements, ExecutedBatch},
        transactions::is_admin_instruction,
        vm::{
            admin::AdminVm,
            clock::set_clock_now,
            gasless_callback::{GaslessCallback, SnapshotCallback, DEFAULT_FEE_PAYER_LAMPORTS},
            gasless_rent_collector::GaslessRentCollector,
        },
    },
    solana_compute_budget::compute_budget::SVMTransactionExecutionBudget,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        hash::Hash,
        pubkey::Pubkey,
        transaction::{SanitizedTransaction, TransactionError},
    },
    solana_svm::{
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
        transaction_processor::{
            LoadAndExecuteSanitizedTransactionsOutput, TransactionBatchProcessor,
            TransactionProcessingConfig, TransactionProcessingEnvironment,
        },
    },
    solana_svm_feature_set::SVMFeatureSet,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_timings::ExecuteTimings,
    std::{
        collections::{HashMap, HashSet, LinkedList},
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
    tracing::{debug, error, info, warn},
};

/// Minimum transactions per worker to justify taking the parallel path.
/// The parallel gate is `regular_txs >= max_svm_workers * MIN_PARALLEL_BATCH_FACTOR`,
/// so each worker ends up with at least this many transactions. Below that,
/// thread-spawn + snapshot-build overhead eats the parallel win — keep the
/// sequential GaslessCallback path.
const MIN_PARALLEL_BATCH_FACTOR: usize = 4;

pub struct ExecutionArgs {
    pub batch_rx: mpsc::Receiver<ConflictFreeBatch>,
    pub settled_accounts_rx: mpsc::UnboundedReceiver<AccountSettlements>,
    pub execution_results_tx: mpsc::Sender<ExecutedBatch>,
    pub accountsdb_connection_url: String,
    pub metrics: SharedMetrics,
    /// Max parallel SVM workers per batch (including calling thread).
    /// 1 disables parallelism; >=2 enables it once the batch is large enough
    /// to give each worker ≥ MIN_PARALLEL_BATCH_FACTOR transactions.
    pub max_svm_workers: usize,
    pub heartbeat: Arc<crate::health::StageHeartbeat>,
    /// Shared live-blockhash window (same Arc advanced by dedup). Used at
    /// execute_batch entry to drop txs whose recent_blockhash expired.
    pub live_blockhashes: Arc<RwLock<LinkedList<Hash>>>,
}

pub struct ExecutionDeps {
    pub bob: BOB,
    pub vm: TransactionBatchProcessor<PrivateChannelForkGraph>,
    pub admin_vm: AdminVm,
    /// Effective parallel-worker cap used by `execute_parallel`. Captured at
    /// worker startup so hot-path batch execution never touches shared config.
    pub max_svm_workers: usize,
    /// Shared live-blockhash window
    pub live_blockhashes: Arc<RwLock<LinkedList<Hash>>>,
    /// Account bytes one transaction may reference before it is refused.
    pub max_tx_loaded_accounts_bytes: usize,
    /// Account bytes one preload may pull in before the batch is split.
    pub preload_budget_bytes: usize,

    // Must prevent this from being dropped
    _fork_graph: Arc<RwLock<PrivateChannelForkGraph>>,
}

pub struct ExecutionResult {
    pub admin_transactions: Vec<SanitizedTransaction>,
    pub regular_transactions: Vec<SanitizedTransaction>,
    pub admin_results: Option<LoadAndExecuteSanitizedTransactionsOutput>,
    pub regular_results: Option<LoadAndExecuteSanitizedTransactionsOutput>,
    /// BOB generation stamped on the admin path's account writes, 0 when the
    /// path was skipped. A plain field rather than part of `admin_results` so
    /// that reading the results does not force callers to unwrap a pair.
    pub admin_generation: u64,
    /// BOB generation stamped on the regular path's account writes, 0 when the
    /// path was skipped.
    pub regular_generation: u64,
    /// Transactions this batch could not afford to preload, in their original
    /// order. Nothing has been done to them, so the caller reruns them as their
    /// own batch once these results are off its hands.
    pub deferred: Vec<SanitizedTransaction>,
}

pub async fn start_execution_worker(args: ExecutionArgs) -> WorkerHandle {
    let ExecutionArgs {
        mut batch_rx,
        settled_accounts_rx,
        execution_results_tx,
        accountsdb_connection_url,
        metrics,
        max_svm_workers,
        heartbeat,
        live_blockhashes,
    } = args;
    let handle = tokio::spawn(async move {
        info!(
            "Execution worker started (max_svm_workers={})",
            max_svm_workers
        );

        let accounts_db = AccountsDB::new(&accountsdb_connection_url, true)
            .await
            .unwrap();
        let mut execution_deps = get_execution_deps(
            accounts_db,
            settled_accounts_rx,
            max_svm_workers,
            live_blockhashes,
        )
        .await;

        let mut total_transactions_executed = 0u64;
        let mut total_batches_processed = 0u64;

        loop {
            // Process batches. Closing the input is the only exit: shutdown
            // arrives as an upstream close so every admitted batch is run.
            match batch_rx.recv().await {
                Some(batch) => {
                    heartbeat.record_input();
                    let batch_size = batch.transactions.len();
                    debug!("Executor received batch with {} transactions", batch_size);

                    match process_batch(
                        batch,
                        &mut execution_deps,
                        &execution_results_tx,
                        &metrics,
                        &heartbeat,
                    )
                    .await
                    {
                        BatchOutcome::Continue { executed } => {
                            total_transactions_executed += executed as u64;
                        }
                        BatchOutcome::Stop => break,
                    }

                    total_batches_processed += 1;

                    if total_batches_processed.is_multiple_of(100) {
                        info!(
                            "Executor has processed {} batches, {} total transactions",
                            total_batches_processed, total_transactions_executed
                        );
                    }
                }
                None => {
                    info!("Executor stopped - channel closed, executed {} total transactions in {} batches",
                                  total_transactions_executed, total_batches_processed);
                    return;
                }
            }
        }
    });

    WorkerHandle::new("Execution".to_string(), handle)
}

pub async fn get_execution_deps(
    accounts_db: AccountsDB,
    settled_accounts_rx: mpsc::UnboundedReceiver<AccountSettlements>,
    max_svm_workers: usize,
    live_blockhashes: Arc<RwLock<LinkedList<Hash>>>,
) -> ExecutionDeps {
    let bob = BOB::new(accounts_db, settled_accounts_rx).await;
    let feature_set = SVMFeatureSet::all_enabled();
    let compute_budget = SVMTransactionExecutionBudget::default();
    let (vm, _fork_graph) =
        create_transaction_batch_processor(&bob, &feature_set, &compute_budget).unwrap();
    let admin_vm = AdminVm::default();
    ExecutionDeps {
        bob,
        vm,
        admin_vm,
        max_svm_workers,
        live_blockhashes,
        max_tx_loaded_accounts_bytes: MAX_TX_LOADED_ACCOUNTS_BYTES,
        preload_budget_bytes: MAX_BATCH_PRELOAD_BYTES,
        _fork_graph,
    }
}

/// Execute a chunk of transactions on the shared SVM with a dedicated
/// per-thread processing environment.
///
/// Each thread creates its own `TransactionProcessingEnvironment` because it
/// contains `Option<&dyn SVMRentCollector>` and that trait has no `Sync`
/// supertrait — so the environment can't be shared across threads. The
/// environment is trivially cheap to construct, so per-thread construction has
/// negligible cost compared to the SVM call it frames.
fn execute_chunk(
    vm: &TransactionBatchProcessor<PrivateChannelForkGraph>,
    callback: &SnapshotCallback,
    transactions: &[SanitizedTransaction],
) -> LoadAndExecuteSanitizedTransactionsOutput {
    let gasless_rent_collector = GaslessRentCollector::new();
    let processing_environment = TransactionProcessingEnvironment {
        blockhash: Hash::default(),
        blockhash_lamports_per_signature: 0,
        feature_set: SVMFeatureSet::all_enabled(),
        rent_collector: Some(
            &gasless_rent_collector
                as &dyn solana_svm_rent_collector::svm_rent_collector::SVMRentCollector,
        ),
        ..Default::default()
    };
    let processing_config = TransactionProcessingConfig::default();
    let check_results = get_transaction_check_results(transactions.len());

    vm.load_and_execute_sanitized_transactions(
        callback,
        transactions,
        check_results,
        &processing_environment,
        &processing_config,
    )
}

/// Merge chunk outputs into a single `LoadAndExecuteSanitizedTransactionsOutput`.
///
/// - `processing_results` are concatenated in chunk order, preserving the
///   original transaction ordering (chunks were built via `.chunks()` so
///   iterating them in order gives transactions in their original order).
/// - `error_metrics` and `execute_timings` are accumulated across chunks.
/// - `balance_collector` is always `None` — we don't use balance recording.
///
/// The destination `Vec` is preallocated to the exact total length to avoid
/// reallocations during the extend loop.
fn merge_svm_outputs(
    chunk_outputs: Vec<LoadAndExecuteSanitizedTransactionsOutput>,
) -> LoadAndExecuteSanitizedTransactionsOutput {
    let total_len: usize = chunk_outputs
        .iter()
        .map(|o| o.processing_results.len())
        .sum();

    let mut merged = LoadAndExecuteSanitizedTransactionsOutput {
        processing_results: Vec::with_capacity(total_len),
        error_metrics: TransactionErrorMetrics::default(),
        execute_timings: ExecuteTimings::default(),
        balance_collector: None,
    };

    for output in chunk_outputs {
        merged.processing_results.extend(output.processing_results);
        merged.error_metrics.accumulate(&output.error_metrics);
        merged.execute_timings.accumulate(&output.execute_timings);
    }

    merged
}

/// Account data one transaction may reference. The SVM enforces the same
/// ceiling, but only while loading, which is after the bytes have been read out
/// of the store; checking it here is what keeps them from being read at all.
pub const MAX_TX_LOADED_ACCOUNTS_BYTES: usize = 64 * 1024 * 1024;

/// Account data one preload may pull in. Per-transaction limits alone bound
/// nothing: a full batch of individually legal transactions still adds up to
/// gigabytes, so the batch is executed in pieces that each stay under this.
pub(crate) const MAX_BATCH_PRELOAD_BYTES: usize = 256 * 1024 * 1024;

/// A transaction at the per-transaction cap has to fit an empty batch, or it
/// would be deferred forever.
const _: () = assert!(MAX_BATCH_PRELOAD_BYTES >= MAX_TX_LOADED_ACCOUNTS_BYTES);

/// How a batch splits once its accounts have been sized, as indices into the
/// batch's transactions.
#[derive(Debug, Default)]
pub(crate) struct PreloadPlan {
    /// Runs now.
    pub admitted: Vec<usize>,
    /// References more account data than the SVM would ever load, so it is
    /// failed without fetching anything.
    pub oversized: Vec<usize>,
    /// Would push this preload past its budget. Runs in a later sub-batch.
    pub deferred: Vec<usize>,
}

/// Decide which transactions this preload can afford. The per-transaction sum
/// counts stored data only, while the SVM also charges per account and counts
/// programdata, so it can only reject what the SVM would reject anyway.
pub(crate) fn plan_preload(
    key_sets: &[Vec<Pubkey>],
    sizes: &HashMap<Pubkey, usize>,
    per_tx_limit: usize,
    budget: usize,
) -> PreloadPlan {
    let mut plan = PreloadPlan::default();
    let mut union: HashSet<Pubkey> = HashSet::new();
    let mut union_bytes = 0usize;

    for (index, keys) in key_sets.iter().enumerate() {
        let mut tx_bytes = 0usize;
        let mut marginal_bytes = 0usize;
        let mut counted: HashSet<&Pubkey> = HashSet::new();
        for key in keys {
            let Some(size) = sizes.get(key) else {
                continue;
            };
            // A key repeated within one message is still one account.
            if !counted.insert(key) {
                continue;
            }
            tx_bytes = tx_bytes.saturating_add(*size);
            // An account several transactions name is fetched once, so the batch
            // total charges only what this transaction adds to the union.
            if !union.contains(key) {
                marginal_bytes = marginal_bytes.saturating_add(*size);
            }
        }

        if tx_bytes > per_tx_limit {
            plan.oversized.push(index);
            continue;
        }
        // Never defer the first transaction of a walk: it would come back as an
        // identical batch and never make progress. Its own cap already bounds it.
        if !plan.admitted.is_empty() && union_bytes.saturating_add(marginal_bytes) > budget {
            // Stop rather than skip ahead, so the remainder keeps its order and
            // stays a valid conflict-free batch.
            plan.deferred.extend(index..key_sets.len());
            break;
        }

        union.extend(keys.iter().copied());
        union_bytes = union_bytes.saturating_add(marginal_bytes);
        plan.admitted.push(index);
    }

    plan
}

/// Cap on retained account bytes in one message to the settler.
/// This bounds a message built from several transactions. One transaction is
/// never divided, so bounding that case is an admission problem, tracked apart.
pub(crate) const MAX_SEND_CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// A chunk must never exceed the settler budget it is measured against.
const _: () = assert!(MAX_SEND_CHUNK_BYTES <= crate::stages::MAX_BUFFERED_SETTLE_BYTES);

/// Outcome of a settler send. There is no shutdown variant: abandoning a send
/// would discard a batch that has already executed and already mutated the
/// in-memory accounts, and the settler is still draining when this stage exits.
pub(crate) enum SendOutcome {
    Sent,
    ChannelClosed,
}

/// Whether the executor may take another batch, and how many transactions the
/// batch actually ran.
pub(crate) enum BatchOutcome {
    Continue { executed: usize },
    Stop,
}

/// Run one scheduled batch, in as many sub-batches as its preload budget needs.
/// The loop lives here so each piece reaches the settler before the next is
/// fetched, which keeps one preload's transient cost to the budget.
pub(crate) async fn process_batch(
    batch: ConflictFreeBatch,
    execution_deps: &mut ExecutionDeps,
    execution_results_tx: &mpsc::Sender<ExecutedBatch>,
    metrics: &SharedMetrics,
    heartbeat: &Arc<crate::health::StageHeartbeat>,
) -> BatchOutcome {
    let mut batch = batch;
    let mut executed = 0usize;
    loop {
        let execution_result = match execute_batch(batch, execution_deps, metrics).await {
            Ok(result) => result,
            Err(e) => {
                // Nothing was executed or settled; a restart is
                // preferable to executing against unknown state.
                error!("Executor stopping: {}", e);
                return BatchOutcome::Stop;
            }
        };

        heartbeat.record_progress();
        let deferred = execution_result.deferred;
        executed +=
            execution_result.admin_transactions.len() + execution_result.regular_transactions.len();

        if !execution_result.admin_transactions.is_empty() {
            let Some(admin_results) = execution_result.admin_results else {
                metrics.executor_missing_results("admin");
                error!("Unexpected error: No result found for admin transactions");
                return BatchOutcome::Stop;
            };
            let len = execution_result.admin_transactions.len();
            // Bounded send applies backpressure; race shutdown so a full
            // settler queue never wedges executor exit. Owned values only,
            // no lock guard is held across this await.
            match send_results_chunked(
                execution_results_tx,
                admin_results,
                execution_result.admin_transactions,
                execution_result.admin_generation,
                MAX_SEND_CHUNK_BYTES,
                metrics,
            )
            .await
            {
                SendOutcome::Sent => {}
                SendOutcome::ChannelClosed => {
                    metrics.executor_results_send_failed("admin");
                    error!("Failed to send admin results: channel closed");
                    return BatchOutcome::Stop;
                }
            }
            metrics.executor_results_sent(len);
        }

        if !execution_result.regular_transactions.is_empty() {
            let Some(regular_results) = execution_result.regular_results else {
                metrics.executor_missing_results("regular");
                error!("Unexpected error: No result found for regular transactions");
                return BatchOutcome::Stop;
            };
            let len = execution_result.regular_transactions.len();
            match send_results_chunked(
                execution_results_tx,
                regular_results,
                execution_result.regular_transactions,
                execution_result.regular_generation,
                MAX_SEND_CHUNK_BYTES,
                metrics,
            )
            .await
            {
                SendOutcome::Sent => {}
                SendOutcome::ChannelClosed => {
                    metrics.executor_results_send_failed("regular");
                    error!("Failed to send regular results: channel closed");
                    return BatchOutcome::Stop;
                }
            }
            metrics.executor_results_sent(len);
        }

        if deferred.is_empty() {
            return BatchOutcome::Continue { executed };
        }
        // A subset of a conflict-free batch is still conflict free, and this
        // stage is sequential, so the remainder is a valid batch as it stands.
        batch = ConflictFreeBatch {
            transactions: deferred
                .into_iter()
                .enumerate()
                .map(
                    |(index, transaction)| crate::scheduler::TransactionWithIndex {
                        transaction: Arc::new(transaction),
                        index,
                    },
                )
                .collect(),
        };
    }
}

/// Where to split a batch, as end-exclusive index ranges over its transactions.
/// A chunk closes just before the transaction that would exceed the cap, so an
/// oversized one travels alone and no transaction is ever split across messages.
fn chunk_ranges_by_bytes(
    results: &[TransactionProcessingResult],
    transactions: &[SanitizedTransaction],
    cap: usize,
) -> Vec<std::ops::Range<usize>> {
    // A length mismatch is the settler's error to report, so send the batch whole.
    if results.len() != transactions.len() {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut buffered = 0usize;
    for (index, (result, transaction)) in results.iter().zip(transactions.iter()).enumerate() {
        let bytes = retained_bytes_of(result, transaction);
        if index > start && buffered + bytes > cap {
            ranges.push(start..index);
            start = index;
            buffered = 0;
        }
        buffered += bytes;
    }
    if start < results.len() {
        ranges.push(start..results.len());
    }
    ranges
}

/// Send one batch to the settler, waiting for room rather than giving up.
/// A closed queue hands the batch back so the caller can record it.
async fn send_one(
    results_tx: &mpsc::Sender<ExecutedBatch>,
    batch: ExecutedBatch,
) -> Result<(), ExecutedBatch> {
    match results_tx.send(batch).await {
        Ok(()) => Ok(()),
        Err(mpsc::error::SendError(batch)) => Err(batch),
    }
}

/// Name what the executor could not hand over. The settler closes its queue
/// when it gives up, so these are executed and uncommitted just like the buffer
/// it recorded on its own side, and they end here rather than nowhere.
fn record_unsent(
    unsent: &[SanitizedTransaction],
    never_sent: &[SanitizedTransaction],
    metrics: &SharedMetrics,
) {
    let signatures: Vec<solana_sdk::signature::Signature> = unsent
        .iter()
        .chain(never_sent.iter())
        .map(|transaction| *transaction.signature())
        .collect();
    crate::stages::record_discarded("executor", "settler queue closed", &signatures, metrics);
}

/// Send results to the settler in byte-bounded messages.
/// A batch under the cap is sent untouched, so ordinary traffic only pays the byte
/// sum. When split, the real generation rides the last chunk and earlier ones get zero.
async fn send_results_chunked(
    results_tx: &mpsc::Sender<ExecutedBatch>,
    output: LoadAndExecuteSanitizedTransactionsOutput,
    transactions: Vec<SanitizedTransaction>,
    generation: u64,
    cap: usize,
    metrics: &SharedMetrics,
) -> SendOutcome {
    let ranges = chunk_ranges_by_bytes(&output.processing_results, &transactions, cap);
    if ranges.len() <= 1 {
        return match send_one(results_tx, (output, transactions, generation)).await {
            Ok(()) => SendOutcome::Sent,
            Err((_, transactions, _)) => {
                record_unsent(&transactions, &[], metrics);
                SendOutcome::ChannelClosed
            }
        };
    }

    metrics.executor_results_chunked(ranges.len());

    let LoadAndExecuteSanitizedTransactionsOutput {
        mut processing_results,
        error_metrics,
        execute_timings,
        balance_collector,
    } = output;
    let mut transactions = transactions;
    // Batch-wide telemetry has no per-chunk meaning, so it rides the first message.
    let mut head = Some((error_metrics, execute_timings, balance_collector));

    let last = ranges.len() - 1;
    let mut sent = 0usize;
    for (position, range) in ranges.iter().enumerate() {
        let take = range.end - range.start;
        let (error_metrics, execute_timings, balance_collector) =
            head.take().unwrap_or_else(|| {
                (
                    TransactionErrorMetrics::default(),
                    ExecuteTimings::default(),
                    None,
                )
            });
        let chunk = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: processing_results.drain(..take).collect(),
            error_metrics,
            execute_timings,
            balance_collector,
        };
        let chunk_transactions: Vec<SanitizedTransaction> = transactions.drain(..take).collect();
        // Zero acknowledges nothing, so a partial drain cannot mark writes durable.
        let chunk_generation = if position == last { generation } else { 0 };
        match send_one(results_tx, (chunk, chunk_transactions, chunk_generation)).await {
            Ok(()) => sent += take,
            Err((_, chunk_transactions, _)) => {
                // Report what actually landed; the caller only counts a whole batch.
                if sent > 0 {
                    metrics.executor_results_sent(sent);
                }
                // The chunks still undrained never left either, so they belong
                // in the same record as the one that just failed.
                record_unsent(&chunk_transactions, &transactions, metrics);
                return SendOutcome::ChannelClosed;
            }
        }
    }

    SendOutcome::Sent
}

/// Execute regular transactions across multiple worker threads.
///
/// Correctness:Within a `ConflictFreeBatch`, transactions have disjoint
/// write sets by construction. Nothing mutates shared state
/// during execution, so parallel chunks cannot conflict.
///
/// Threading model: `std::thread::scope` — stdlib-only, no dependency,
/// allows borrowing non-`'static` data (the VM reference, the snapshot).
/// The calling thread processes `chunks[0]` itself, so only `N-1` OS
/// threads are spawned for `N` chunks. On Linux, spawn cost is ~15µs per
/// thread.
///
/// Preallocation: `chunks` Vec capacity set to exactly `num_workers`,
/// `outputs` Vec capacity set to exactly `num_workers`. No reallocations.
///
/// Caller must ensure `max_svm_workers >= 2` — this function assumes the
/// parallel path is wanted and will always split into at least 2 chunks.
fn execute_parallel(
    vm: &TransactionBatchProcessor<PrivateChannelForkGraph>,
    snapshot: &SnapshotCallback,
    transactions: &[SanitizedTransaction],
    max_svm_workers: usize,
) -> LoadAndExecuteSanitizedTransactionsOutput {
    debug_assert!(
        max_svm_workers >= 2,
        "execute_parallel requires max_svm_workers >= 2; gate this at the call site"
    );
    // Pick worker count: at least 2 (caller already gates on max_svm_workers>=2),
    // at most max_svm_workers (config cap), and proportional to the batch so
    // each worker gets ~MIN_PARALLEL_BATCH_FACTOR transactions.
    let num_workers = (transactions.len() / MIN_PARALLEL_BATCH_FACTOR).clamp(2, max_svm_workers);
    // Ceiling division so the last chunk is the smallest (not largest).
    let chunk_size = transactions.len().div_ceil(num_workers);

    // Collect chunk slices first so we can index them by worker id.
    // Preallocate exactly — chunks.len() == num_workers in the common case
    // (could be one less if transactions.len() divides evenly and the last
    // chunk would be empty; .chunks() skips empty chunks).
    let mut chunks: Vec<&[SanitizedTransaction]> = Vec::with_capacity(num_workers);
    chunks.extend(transactions.chunks(chunk_size));

    // Defensive: .chunks(n) on a non-empty slice never yields zero chunks
    // when n >= 1, so this holds. Guard anyway for clarity.
    debug_assert!(!chunks.is_empty(), "non-empty batch must produce ≥1 chunk");

    let chunk_outputs: Vec<LoadAndExecuteSanitizedTransactionsOutput> = std::thread::scope(|s| {
        // Spawn workers for chunks[1..]; chunks[0] runs on the calling thread.
        // This saves one thread spawn and keeps a hot CPU doing real work.
        let mut handles = Vec::with_capacity(chunks.len().saturating_sub(1));
        for chunk in &chunks[1..] {
            let chunk: &[SanitizedTransaction] = chunk;
            handles.push(s.spawn(move || execute_chunk(vm, snapshot, chunk)));
        }

        // Do chunks[0] inline on this thread while workers run.
        let mut outputs: Vec<LoadAndExecuteSanitizedTransactionsOutput> =
            Vec::with_capacity(chunks.len());
        outputs.push(execute_chunk(vm, snapshot, chunks[0]));

        // Join in spawn order to preserve original transaction ordering.
        // A panic in any worker propagates to the executor — we want the
        // process to crash rather than silently drop transactions.
        for handle in handles {
            outputs.push(handle.join().expect("SVM worker thread panicked"));
        }
        outputs
    });

    merge_svm_outputs(chunk_outputs)
}

/// The lamports the gasless callback makes up for an unknown fee payer are the
/// only money here that nobody deposited. Treat them as a loan the transaction
/// has to pay back, less one lamport for each account it creates.
///
/// The SVM already makes every instruction balance, so once this one source is
/// blocked, every other balance is made of money that was already here and does
/// not need checking. A loan that is not paid back fails the transaction rather
/// than editing accounts, because editing accounts is what corrupts bystanders.
///
/// Regular path only: admin execution never makes up fee payers.
fn enforce_lamport_conservation(
    output: &mut LoadAndExecuteSanitizedTransactionsOutput,
    transactions: &[SanitizedTransaction],
    bob: &BOB,
    fee_payers: &HashSet<Pubkey>,
    metrics: &SharedMetrics,
) {
    // Reused across transactions so the whole batch allocates at most once.
    let mut fabricated: Vec<usize> = Vec::new();

    for (result, tx) in output
        .processing_results
        .iter_mut()
        .zip(transactions.iter())
    {
        let Ok(ProcessedTransaction::Executed(executed)) = result else {
            continue;
        };
        if !executed.was_successful() {
            // Failed executed txs commit no account writes downstream.
            continue;
        }

        fabricated.clear();
        // How much of the made-up money is gone, how many new accounts exist to
        // explain it, and whether any older account ended up richer.
        let mut shortfall = 0u64;
        let mut created = 0u64;
        let mut credited = false;
        for (index, (pubkey, acct)) in executed.loaded_transaction.accounts.iter().enumerate() {
            // Read-only accounts are never saved, so never touch or count them.
            if !tx.is_writable(index) {
                continue;
            }
            if let Some(before) = bob.account_lamports(pubkey) {
                // This account already existed, so its balance is real money we
                // must never rewrite. Just note whether it grew.
                credited |= acct.lamports() > before;
            } else if fee_payers.contains(pubkey) {
                // A made-up payer, so anything it no longer holds has escaped.
                // The set covers the whole batch, so moving money from one
                // made-up payer to another still counts as escaped.
                fabricated.push(index);
                shortfall += DEFAULT_FEE_PAYER_LAMPORTS.saturating_sub(acct.lamports());
            } else if acct.lamports() > 0 {
                // BOB never saw it, but it has money, so this tx just made it.
                created += 1;
            }
        }

        // Comparing money to a count works because the rate is one each: the SVM
        // deletes an account holding nothing, and with no rent every way of
        // making an account gives it exactly one lamport.
        //
        // Counting new accounts does not prove the made-up money paid for them,
        // so a new account paid for with real money could excuse a made-up
        // lamport that went elsewhere. Hence no older account may grow while any
        // made-up money is missing.
        if shortfall > created || (shortfall > 0 && credited) {
            // Nothing legitimate trips this, so every hit is worth an alert.
            warn!(
                sig = %tx.signature(),
                shortfall,
                created,
                credited,
                "execution: failing tx that does not account for its fabricated fee-payer lamports"
            );
            metrics.executor_conservation_rejected();
            // Fail the tx instead of taking the money back, which would mean
            // rewriting accounts it owns. Later stages skip failed txs.
            executed.execution_details.status = Err(TransactionError::UnbalancedTransaction);
            continue;
        }
        // Nothing is missing, so wipe the made-up payers. BOB and the settler
        // already read an empty account with no money as deleted, so wiping is
        // how they disappear. Nothing else is touched.
        //
        // Always wipe, which burns any real money sent to the payer. Keeping it
        // would turn an address we made up into a real one, paying the sender
        // back would rewrite an innocent account, and failing the tx would break
        // CancelDvp, which sends the closed escrows' leftover money here.
        for index in &fabricated {
            executed.loaded_transaction.accounts[*index].1 = AccountSharedData::default();
        }
    }
}

/// Returns `Err` when the accounts this batch needs could not be loaded. The
/// abort happens before any SVM run or BOB write, so nothing has changed yet.
pub async fn execute_batch(
    batch: ConflictFreeBatch,
    execution_deps: &mut ExecutionDeps,
    metrics: &SharedMetrics,
) -> Result<ExecutionResult, AccountLoadError> {
    let t_batch = Instant::now();
    let batch_size = batch.transactions.len();
    debug!("Executing batch with {} transactions", batch_size);

    // Extract all transactions from the batch
    let all_transactions: Vec<_> = batch
        .transactions
        .into_iter()
        .map(|tx| tx.transaction.as_ref().clone())
        .collect();

    // Drop txs whose recent_blockhash expired while parked in an upstream
    // bounded queue. Snapshot the window once per batch to keep contains() O(1).
    let live: HashSet<Hash> = execution_deps
        .live_blockhashes
        .read()
        .expect("blockhash lock poisoned")
        .iter()
        .copied()
        .collect();
    let (all_transactions, expired): (Vec<_>, Vec<_>) = all_transactions
        .into_iter()
        .partition(|tx| live.contains(tx.message().recent_blockhash()));
    if !expired.is_empty() {
        for tx in &expired {
            warn!(
                sig = %tx.signature(),
                bh = %tx.message().recent_blockhash(),
                "execution: dropping tx whose recent blockhash expired during pipeline wait"
            );
        }
        metrics.executor_dropped_expired_blockhash(expired.len());
    }

    // Size the accounts before deciding to load them. The SVM applies the same
    // ceiling, but only once the bytes are in memory, so this is the only place
    // a request for too much data can be refused before the store is asked.
    let t_op = Instant::now();
    let sized_keys: Vec<Pubkey> = {
        let mut seen = HashSet::new();
        all_transactions
            .iter()
            .flat_map(|tx| {
                tx.message()
                    .account_keys()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .filter(|key| seen.insert(*key))
            .collect()
    };
    let account_sizes = match execution_deps.bob.account_data_sizes(&sized_keys).await {
        Ok(sizes) => sizes,
        Err(e) => {
            if let AccountLoadError::Corrupt(_) = e {
                metrics.executor_corrupt_account();
            }
            metrics.executor_preload_fatal();
            error!("execution: aborting batch, account sizing failed: {}", e);
            return Err(e);
        }
    };
    let key_sets: Vec<Vec<Pubkey>> = all_transactions
        .iter()
        .map(|tx| tx.message().account_keys().iter().copied().collect())
        .collect();
    let plan = plan_preload(
        &key_sets,
        &account_sizes,
        execution_deps.max_tx_loaded_accounts_bytes,
        execution_deps.preload_budget_bytes,
    );
    let t_sizing = t_op.elapsed();

    // Rebuild the batch as three disjoint lists, consuming the originals so
    // nothing is cloned.
    let mut slots: Vec<Option<SanitizedTransaction>> =
        all_transactions.into_iter().map(Some).collect();
    let take = |slots: &mut Vec<Option<SanitizedTransaction>>, indices: &[usize]| {
        indices
            .iter()
            .map(|i| slots[*i].take().expect("each index is claimed once"))
            .collect::<Vec<_>>()
    };
    let oversized_transactions = take(&mut slots, &plan.oversized);
    let deferred = take(&mut slots, &plan.deferred);
    let all_transactions = take(&mut slots, &plan.admitted);

    if !oversized_transactions.is_empty() {
        for tx in &oversized_transactions {
            warn!(
                sig = %tx.signature(),
                "execution: failing tx that references more account data than the SVM would load"
            );
        }
        metrics.executor_oversized_transactions(oversized_transactions.len());
    }
    if !deferred.is_empty() {
        debug!(
            "execution: deferring {} txs past the {} byte preload budget",
            deferred.len(),
            execution_deps.preload_budget_bytes
        );
        metrics.executor_batch_deferred(deferred.len());
    }

    // TODO: ConflictFree scheduling should do the admin/non-admin/ATA partitioning
    // This would allow better parallelization and cleaner separation of concerns
    // The scheduler could create separate batches for admin vs regular vs ATA transactions

    // Partition transactions into three categories
    let mut admin_transactions = Vec::new();
    let mut regular_transactions = Vec::new();
    let mut fee_payers = HashSet::new();
    let mut accounts_to_preload = HashSet::new();

    let t_op = Instant::now();
    for tx in all_transactions {
        // Collect fee payer BEFORE moving tx
        fee_payers.insert(*tx.fee_payer());
        // Collect all accounts referenced in the transaction
        // This includes program accounts, instruction accounts, and fee payer
        for account in tx.message().account_keys().iter() {
            accounts_to_preload.insert(*account);
        }

        // Router contract: a tx is admin-routed only when EVERY instruction is
        // listed in ADMIN_INSTRUCTIONS_MAP. A mixed tx is routed to
        // the regular SVM where the admin instruction will fail naturally
        let mut has_any_admin = false;
        let mut all_admin = true;
        for (program_id, instruction) in tx.message().program_instructions_iter() {
            let is_admin = instruction
                .data
                .first()
                .is_some_and(|t| is_admin_instruction(program_id, *t));
            has_any_admin |= is_admin;
            all_admin &= is_admin;
        }

        if has_any_admin && all_admin {
            // Pure admin tx, Admin VM.
            admin_transactions.push(tx);
        } else {
            // Pure regular OR mixed, real SVM.
            regular_transactions.push(tx);
        }
    }
    let t_partition = t_op.elapsed();

    let num_admin_transactions = admin_transactions.len();
    let num_regular_transactions = regular_transactions.len();
    debug!(
        "partition: {} admin, {} regular in {:?}",
        num_admin_transactions, num_regular_transactions, t_partition
    );

    // Preload accounts
    let accounts_to_preload = accounts_to_preload.into_iter().collect::<Vec<_>>();
    let t_op = Instant::now();
    // Executing against accounts BOB could not load would settle state derived
    // from accounts the SVM wrongly saw as nonexistent, so the batch stops here.
    let (preload_fetched, preload_cached) = match execution_deps
        .bob
        .preload_accounts(&accounts_to_preload)
        .await
    {
        Ok(counts) => counts,
        Err(e) => {
            if let AccountLoadError::Corrupt(_) = e {
                metrics.executor_corrupt_account();
            }
            metrics.executor_preload_fatal();
            error!("execution: aborting batch, account preload failed: {}", e);
            return Err(e);
        }
    };
    let t_preload = t_op.elapsed();
    debug!(
        "preload: {} accounts ({} fetched, {} cached) in {:?}",
        accounts_to_preload.len(),
        preload_fetched,
        preload_cached,
        t_preload
    );
    metrics.executor_preload_duration_ms(t_preload.as_secs_f64() * 1000.0);

    // Report BOB cache size and drain the eviction delta right after preload,
    // when the cache reflects this batch's working set.
    let cache_stats = execution_deps.bob.cache_stats();
    metrics.bob_cache_entries(cache_stats.entries);
    metrics.bob_cache_dirty_entries(cache_stats.dirty_entries);
    metrics.bob_cache_bytes(cache_stats.bytes);
    metrics.bob_cache_evicted(cache_stats.evicted);
    metrics.bob_settlement_divergences(cache_stats.settlement_divergences);

    // Refresh the SVM's cached Clock sysvar from wall time. Contra has no
    // real Clock source (see `crate::vm::clock`); without this, programs
    // calling `Clock::get()` would read `unix_timestamp = 0`. Must run
    // before any SVM execution in this batch — workers take read locks on
    // the sysvar cache during syscalls, so a mid-batch write would deadlock.
    set_clock_now(&execution_deps.vm);

    // Create processing environment and config
    let feature_set: SVMFeatureSet = SVMFeatureSet::all_enabled();
    // TODO: Use non-default blockhash for TransactionProcessingEnvironment
    // This would add replay attack prevention by ensuring each batch has a unique blockhash
    // Could use a combination of slot number, batch index, or timestamp to generate unique hashes

    // For gasless operation, use our custom gasless rent collector
    let gasless_rent_collector = GaslessRentCollector::new();
    let rent_collector = Some(
        &gasless_rent_collector
            as &dyn solana_svm_rent_collector::svm_rent_collector::SVMRentCollector,
    );

    let processing_environment = TransactionProcessingEnvironment {
        blockhash: Hash::default(), // TODO: Replace with proper blockhash for replay protection
        blockhash_lamports_per_signature: 0, // Gasless - no lamports per signature
        feature_set,
        rent_collector,
        ..Default::default()
    };

    let processing_config = TransactionProcessingConfig {
        ..Default::default()
    };

    // Timing accumulators — stay zero when the corresponding path is skipped.
    let mut t_svm_admin = Duration::ZERO;
    let mut t_bob_admin = Duration::ZERO;
    let mut t_svm_reg = Duration::ZERO;
    let mut t_bob_reg = Duration::ZERO;

    // Generations stamped by each path's BOB update. They stay 0 when that path
    // is skipped, which the settler's max() fold and BOB's high-water comparison
    // both treat as "acknowledges nothing".
    let mut admin_generation = 0u64;
    let mut regular_generation = 0u64;

    // Settle admin transactions immediately so regular transactions see the updates
    let admin_results = if !admin_transactions.is_empty() {
        let t_op = Instant::now();
        let admin_results = execution_deps
            .admin_vm
            .load_and_execute_sanitized_transactions(
                &execution_deps.bob,
                admin_transactions.as_slice(),
                get_transaction_check_results(admin_transactions.len()),
                &processing_environment,
                &processing_config,
            );
        t_svm_admin = t_op.elapsed();
        debug!(
            "svm_admin: {} txs in {:?}",
            num_admin_transactions, t_svm_admin
        );
        metrics.executor_svm_duration_ms("admin", t_svm_admin.as_secs_f64() * 1000.0);

        // Update BOB's in-memory accounts with the execution results
        let t_op = Instant::now();
        admin_generation = execution_deps
            .bob
            .update_accounts(&admin_results, &admin_transactions);
        t_bob_admin = t_op.elapsed();
        debug!("bob_update_admin: {:?}", t_bob_admin);
        metrics.executor_bob_update_duration_ms("admin", t_bob_admin.as_secs_f64() * 1000.0);

        Some(admin_results)
    } else {
        None
    };

    // Parallel path is taken when the batch is large enough to give each of
    // `max_svm_workers` workers at least `MIN_PARALLEL_BATCH_FACTOR` txs, and
    // the operator has configured >=2 workers. Within a `ConflictFreeBatch`
    // write sets are disjoint, so parallel chunks cannot conflict on account
    // state. For smaller batches we keep the single-threaded `GaslessCallback`
    // path, which reads BOB directly and avoids snapshot + thread-spawn overhead.
    let regular_results = if !regular_transactions.is_empty() {
        let t_op = Instant::now();

        // Gate: batch must be large enough to amortise parallel overhead
        // across workers, and operator must have enabled parallelism
        // (max_svm_workers >= 2). Setting max_svm_workers=1 (or 0, treated the
        // same) forces the sequential path regardless of batch size — useful
        // for profiling or single-core deployments.
        let parallel_min = execution_deps
            .max_svm_workers
            .saturating_mul(MIN_PARALLEL_BATCH_FACTOR);
        let use_parallel =
            execution_deps.max_svm_workers >= 2 && regular_transactions.len() >= parallel_min;
        let mut regular_results = if use_parallel {
            // Parallel path: snapshot BOB + spawn workers.
            // `accounts_to_preload` covers admin+regular keys; harmless
            // over-inclusion — admin keys in the snapshot just add a few
            // HashMap entries that regular-tx workers will never look up.
            let snapshot = SnapshotCallback::from_bob(
                &execution_deps.bob,
                &accounts_to_preload,
                fee_payers.clone(),
            );
            // `execute_parallel` uses `std::thread::scope`, which parks this
            // OS thread until the worker threads join. Because we're on a
            // tokio worker, `block_in_place` lets tokio migrate other queued
            // tasks off this thread first so the async pipeline isn't stalled.
            tokio::task::block_in_place(|| {
                execute_parallel(
                    &execution_deps.vm,
                    &snapshot,
                    &regular_transactions,
                    execution_deps.max_svm_workers,
                )
            })
        } else {
            // Sequential path: direct BOB access, no snapshot cost.
            let gasless_callback = GaslessCallback::new(&execution_deps.bob, fee_payers.clone());
            execution_deps.vm.load_and_execute_sanitized_transactions(
                &gasless_callback,
                regular_transactions.as_slice(),
                get_transaction_check_results(regular_transactions.len()),
                &processing_environment,
                &processing_config,
            )
        };

        t_svm_reg = t_op.elapsed();
        debug!(
            "svm_regular: {} txs ({}) in {:?}",
            num_regular_transactions,
            if use_parallel {
                "parallel"
            } else {
                "sequential"
            },
            t_svm_reg
        );
        metrics.executor_svm_duration_ms("regular", t_svm_reg.as_secs_f64() * 1000.0);

        // Run the conservation check before either consumer reads the shared
        // output: the in-memory BOB update below and the durable settler
        // downstream. One pass covers both consumers and both exec paths.
        enforce_lamport_conservation(
            &mut regular_results,
            &regular_transactions,
            &execution_deps.bob,
            &fee_payers,
            metrics,
        );

        // Update BOB's in-memory accounts with the execution results
        let t_op = Instant::now();
        regular_generation = execution_deps
            .bob
            .update_accounts(&regular_results, &regular_transactions);
        t_bob_reg = t_op.elapsed();
        debug!("bob_update_regular: {:?}", t_bob_reg);
        metrics.executor_bob_update_duration_ms("regular", t_bob_reg.as_secs_f64() * 1000.0);

        Some(regular_results)
    } else {
        None
    };

    // Report them with the error the SVM would itself have produced, so every
    // consumer downstream sees an outcome it already handles. Appending after
    // execution keeps results aligned and these out of the path decisions.
    let num_oversized = oversized_transactions.len();
    let mut regular_results = regular_results;
    if !oversized_transactions.is_empty() {
        let results =
            regular_results.get_or_insert_with(|| LoadAndExecuteSanitizedTransactionsOutput {
                processing_results: Vec::new(),
                error_metrics: TransactionErrorMetrics::default(),
                execute_timings: ExecuteTimings::default(),
                balance_collector: None,
            });
        for _ in 0..num_oversized {
            results
                .processing_results
                .push(Err(TransactionError::MaxLoadedAccountsDataSizeExceeded));
        }
        regular_transactions.extend(oversized_transactions);
    }

    let t_total = t_batch.elapsed();
    debug!(
        "execute_batch complete: received={} admin={} regular={} oversized={} deferred={} | \
         sizing={:?} partition={:?} preload={:?} svm_admin={:?} bob_admin={:?} svm_reg={:?} bob_reg={:?} total={:?}",
        batch_size,
        num_admin_transactions,
        num_regular_transactions,
        num_oversized,
        deferred.len(),
        t_sizing,
        t_partition,
        t_preload,
        t_svm_admin,
        t_bob_admin,
        t_svm_reg,
        t_bob_reg,
        t_total,
    );
    metrics.executor_batch_duration_ms(t_total.as_secs_f64() * 1000.0);

    Ok(ExecutionResult {
        admin_transactions,
        regular_transactions,
        admin_results,
        regular_results,
        admin_generation,
        regular_generation,
        deferred,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::bob::BOB, stage_metrics::NoopMetrics, stages::retained_account_bytes,
        test_helpers::start_test_postgres,
    };
    use solana_sdk::account::AccountSharedData;
    use solana_sdk::{
        hash::Hash,
        message::Message,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    };
    use solana_svm::transaction_processor::LoadAndExecuteSanitizedTransactionsOutput;
    use solana_svm_callback::TransactionProcessingCallback;
    use std::collections::{HashMap, HashSet, LinkedList};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    use crate::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY as RESULTS_CAP;

    /// Helper: live-blockhash window containing only `Hash::default()` so the
    /// canned test transactions (built with `Hash::default()` as their recent
    /// blockhash) survive the expiry filter in `execute_batch`.
    fn default_live_blockhashes() -> Arc<RwLock<LinkedList<Hash>>> {
        Arc::new(RwLock::new(LinkedList::from([Hash::default()])))
    }

    fn create_test_transaction() -> SanitizedTransaction {
        sanitize_transfer(&Keypair::new(), Hash::default())
    }

    /// Build a sanitized transfer tx signed by `payer` against `blockhash`.
    fn sanitize_transfer(payer: &Keypair, blockhash: Hash) -> SanitizedTransaction {
        let ix = solana_system_interface::instruction::transfer(
            &payer.pubkey(),
            &Pubkey::new_unique(),
            100,
        );
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer], msg, blockhash);
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to create test transaction")
    }

    /// A transfer carrying `extra` as unused readonly keys, the shape that lets a
    /// small transaction name a lot of account data. They go into the message,
    /// not an instruction, because the SVM charges for every key it finds.
    fn transfer_with_unused_readonly(payer: &Keypair, extra: &[Pubkey]) -> SanitizedTransaction {
        let ix = solana_system_interface::instruction::transfer(
            &payer.pubkey(),
            &Pubkey::new_unique(),
            100,
        );
        let mut msg = Message::new(&[ix], Some(&payer.pubkey()));
        msg.account_keys.extend_from_slice(extra);
        msg.header.num_readonly_unsigned_accounts += extra.len() as u8;
        let tx = Transaction::new(&[payer], msg, Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("a message with unused readonly keys is still well formed")
    }

    /// Store `count` system-owned accounts of `data_len` bytes each.
    async fn seed_sized_accounts(
        db: &mut crate::accounts::AccountsDB,
        count: usize,
        data_len: usize,
    ) -> Vec<Pubkey> {
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            let pubkey = Pubkey::new_unique();
            db.set_account(
                pubkey,
                AccountSharedData::new(1, data_len, &solana_sdk_ids::system_program::ID),
            )
            .await;
            keys.push(pubkey);
        }
        keys
    }

    /// Execution deps whose preload budget is small enough to split a batch of
    /// ordinary test transactions.
    async fn deps_with_budget(
        accounts_db: crate::accounts::AccountsDB,
        budget: usize,
    ) -> ExecutionDeps {
        let (_settled_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        deps.preload_budget_bytes = budget;
        deps
    }

    /// Execution deps with a small per-transaction cap, so the oversized path is
    /// exercised without seeding accounts big enough to clear the real 64 MiB.
    async fn deps_with_tx_cap(
        accounts_db: crate::accounts::AccountsDB,
        per_tx_limit: usize,
    ) -> ExecutionDeps {
        let (_settled_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        deps.max_tx_loaded_accounts_bytes = per_tx_limit;
        deps
    }

    // ── Lamport-cap test helpers ──

    /// Transfer `amount` from `from` to `to`, paid for (and signed) by `from`.
    fn transfer(from: &Keypair, to: &Pubkey, amount: u64) -> SanitizedTransaction {
        let ix = solana_system_interface::instruction::transfer(&from.pubkey(), to, amount);
        let msg = Message::new(&[ix], Some(&from.pubkey()));
        let tx = Transaction::new(&[from], msg, Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to build transfer tx")
    }

    /// Transfer from `from` to `to`, but signed/fee-paid by a separate `payer`.
    fn sponsored_transfer(
        payer: &Keypair,
        from: &Keypair,
        to: &Pubkey,
        amount: u64,
    ) -> SanitizedTransaction {
        let ix = solana_system_interface::instruction::transfer(&from.pubkey(), to, amount);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer, from], msg, Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to build sponsored transfer tx")
    }

    /// Insert a real, funded, system-owned account directly into BOB.
    fn fund(bob: &mut BOB, pubkey: &Pubkey, lamports: u64) {
        bob.insert_account_for_test(
            *pubkey,
            AccountSharedData::new(lamports, 0, &solana_sdk_ids::system_program::ID),
        );
    }

    fn bob_balance(bob: &BOB, pubkey: &Pubkey) -> Option<u64> {
        bob.get_account_shared_data(pubkey).map(|a| a.lamports())
    }

    /// Wrap `txs` into a `ConflictFreeBatch` and run `execute_batch`.
    async fn run_batch(
        deps: &mut ExecutionDeps,
        metrics: &SharedMetrics,
        txs: Vec<SanitizedTransaction>,
    ) -> ExecutionResult {
        let transactions = txs
            .into_iter()
            .enumerate()
            .map(|(i, tx)| crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: i,
            })
            .collect();
        execute_batch(ConflictFreeBatch { transactions }, deps, metrics)
            .await
            .expect("test batch must load its accounts")
    }

    fn regular_result(
        result: &ExecutionResult,
        i: usize,
    ) -> &solana_svm::transaction_processing_result::TransactionProcessingResult {
        &result
            .regular_results
            .as_ref()
            .expect("regular results present")
            .processing_results[i]
    }

    fn is_executed(
        r: &solana_svm::transaction_processing_result::TransactionProcessingResult,
    ) -> bool {
        matches!(r, Ok(ProcessedTransaction::Executed(_)))
    }

    // ── enforce_lamport_conservation unit tests (pure, no SVM) ──
    //
    // The check is pure over (output, transactions, bob, fee_payers). We seed
    // BOB with what existed before the transaction, hand the check a loaded
    // account vector we control paired with a transaction whose writability we
    // choose, and read the status and the accounts back.

    /// Build a single-tx Executed output carrying `accounts` with the given `status`.
    fn executed_with_status(
        status: Result<(), solana_transaction_error::TransactionError>,
        accounts: Vec<(Pubkey, AccountSharedData)>,
    ) -> TransactionProcessingResult {
        use solana_svm::account_loader::LoadedTransaction;
        use solana_svm::transaction_execution_result::{
            ExecutedTransaction, TransactionExecutionDetails,
        };
        Ok(ProcessedTransaction::Executed(Box::new(
            ExecutedTransaction {
                loaded_transaction: LoadedTransaction {
                    accounts,
                    ..Default::default()
                },
                execution_details: TransactionExecutionDetails {
                    status,
                    log_messages: None,
                    inner_instructions: None,
                    return_data: None,
                    executed_units: 0,
                    accounts_data_len_delta: 0,
                },
                programs_modified_by_tx: std::collections::HashMap::new(),
            },
        )))
    }

    /// A successful single-tx Executed output carrying `accounts`.
    fn executed_with(accounts: Vec<(Pubkey, AccountSharedData)>) -> TransactionProcessingResult {
        executed_with_status(Ok(()), accounts)
    }

    /// One transfer per requested size, carrying those bytes on its writable slot.
    fn sized_batch(
        sizes: &[usize],
    ) -> (Vec<TransactionProcessingResult>, Vec<SanitizedTransaction>) {
        let mut results = Vec::new();
        let mut txs = Vec::new();
        for size in sizes {
            let from = Keypair::new();
            let to = Pubkey::new_unique();
            txs.push(transfer(&from, &to, 100));
            results.push(executed_with(vec![
                (
                    from.pubkey(),
                    AccountSharedData::new(1, *size, &Pubkey::default()),
                ),
                (to, AccountSharedData::new(1, 0, &Pubkey::default())),
            ]));
        }
        (results, txs)
    }

    /// Wrap processing results in the SVM output shape the send helper consumes.
    fn output_of(
        processing_results: Vec<TransactionProcessingResult>,
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        LoadAndExecuteSanitizedTransactionsOutput {
            processing_results,
            error_metrics: TransactionErrorMetrics::default(),
            execute_timings: ExecuteTimings::default(),
            balance_collector: None,
        }
    }

    /// A chunk over the cap is no bound at all, and dropping or reordering a
    /// transaction corrupts the block's signature list. Both are asserted on every
    /// row, since an unsplit batch can regress just as easily as a split one.
    #[test]
    fn chunk_ranges_by_bytes_respects_cap_and_preserves_order() {
        let cap = 1000usize;
        let cases: Vec<(&str, Vec<usize>, usize)> = vec![
            ("all small stays unsplit", vec![10, 10, 10, 10], 1),
            ("exact fit stays unsplit", vec![500, 500], 1),
            ("one byte over splits", vec![500, 501], 2),
            ("single oversized tx sits alone", vec![5000], 1),
            ("oversized in the middle isolates", vec![10, 5000, 10], 3),
            ("empty yields no chunks", vec![], 0),
        ];

        for (name, sizes, expected_chunks) in cases {
            let (results, txs) = sized_batch(&sizes);
            let ranges = chunk_ranges_by_bytes(&results, &txs, cap);
            assert_eq!(ranges.len(), expected_chunks, "chunk count for {}", name);

            let mut next = 0usize;
            for r in &ranges {
                assert_eq!(r.start, next, "gap or overlap in {}", name);
                assert!(r.end > r.start, "empty chunk in {}", name);
                next = r.end;
            }
            assert_eq!(next, sizes.len(), "chunks must cover every tx in {}", name);

            for r in &ranges {
                if r.end - r.start > 1 {
                    let bytes = retained_account_bytes(&results[r.clone()], &txs[r.clone()]);
                    assert!(bytes <= cap, "chunk over cap in {}: {}", name, bytes);
                }
            }
        }
    }

    /// The one assertion standing between the split and a data-loss bug. If a
    /// non-final chunk carried the real generation, BOB would treat undrained writes
    /// as durable and drop them, so only the last chunk may advance the watermark.
    #[tokio::test]
    async fn chunked_send_stamps_generation_on_final_chunk_only() {
        let cap = 1000usize;
        let _shutdown = CancellationToken::new();
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        // Each transaction alone exceeds the cap, so this splits into three.
        let (results, txs) = sized_batch(&[5000, 5000, 5000]);
        let (chan_tx, mut rx) = mpsc::channel::<ExecutedBatch>(16);
        let outcome =
            send_results_chunked(&chan_tx, output_of(results), txs, 42, cap, &metrics).await;
        assert!(matches!(outcome, SendOutcome::Sent));

        let mut gens = Vec::new();
        while let Ok((_, _, g)) = rx.try_recv() {
            gens.push(g);
        }
        assert_eq!(
            gens,
            vec![0, 0, 42],
            "only the final chunk may carry the generation"
        );

        // Under the cap the batch goes as one message, still stamped.
        let (results, txs) = sized_batch(&[10, 10]);
        let (chan_tx, mut rx) = mpsc::channel::<ExecutedBatch>(16);
        let outcome =
            send_results_chunked(&chan_tx, output_of(results), txs, 7, cap, &metrics).await;
        assert!(matches!(outcome, SendOutcome::Sent));

        let mut gens = Vec::new();
        while let Ok((_, _, g)) = rx.try_recv() {
            gens.push(g);
        }
        assert_eq!(gens, vec![7], "an unsplit batch stays one message");
    }

    use crate::stage_metrics::PrometheusMetrics;

    /// The Prometheus registry is process-global, so counter deltas are read
    /// one test at a time.
    static DISCARD_METRIC_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn discarded_total() -> f64 {
        private_channel_metrics::prometheus::gather()
            .into_iter()
            .filter(|mf| mf.name() == "private_channel_discarded_executed_transactions_total")
            .flat_map(|mf| mf.get_metric().to_vec())
            .map(|m| m.get_counter().value())
            .sum()
    }

    /// The settler closes its queue when it gives up, so the batch the executor
    /// was holding comes back to it. Dropping that silently moves the audit gap
    /// one stage upstream instead of closing it.
    #[tokio::test]
    async fn a_closed_settler_channel_records_the_unsent_batch() {
        let _guard = DISCARD_METRIC_LOCK.lock().await;
        let metrics: SharedMetrics = Arc::new(PrometheusMetrics);
        let before = discarded_total();

        let (results, txs) = sized_batch(&[10, 10, 10]);
        let (chan_tx, rx) = mpsc::channel::<ExecutedBatch>(4);
        drop(rx);

        let outcome = send_results_chunked(
            &chan_tx,
            output_of(results),
            txs,
            1,
            MAX_SEND_CHUNK_BYTES,
            &metrics,
        )
        .await;
        assert!(matches!(outcome, SendOutcome::ChannelClosed));
        assert_eq!(
            discarded_total() - before,
            3.0,
            "every executed transaction the executor still held must be recorded"
        );
    }

    /// A split batch fails partway. The chunks that never went are just as
    /// executed and just as uncommitted as the one whose send failed.
    #[tokio::test]
    async fn a_closed_channel_mid_chunk_records_the_remainder() {
        let _guard = DISCARD_METRIC_LOCK.lock().await;
        let metrics: SharedMetrics = Arc::new(PrometheusMetrics);
        let before = discarded_total();

        // One transaction per chunk, and room for only the first.
        let (results, txs) = sized_batch(&[5000, 5000, 5000]);
        let (chan_tx, rx) = mpsc::channel::<ExecutedBatch>(1);
        // The receiver never drains, so the second chunk parks. Closing it then
        // is what the settler does when it gives up mid-handover.
        let closer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(rx);
        });
        let outcome =
            send_results_chunked(&chan_tx, output_of(results), txs, 9, 1000, &metrics).await;

        assert!(matches!(outcome, SendOutcome::ChannelClosed));
        assert_eq!(
            discarded_total() - before,
            2.0,
            "the failed chunk and the undrained remainder must both be recorded"
        );
        let _ = closer.await;
    }

    /// A token-like data account (program-owned, non-empty data) with `lamports`.
    fn data_account(lamports: u64) -> AccountSharedData {
        AccountSharedData::new(lamports, 8, &spl_token::id())
    }

    /// A dataless system-owned account with `lamports`.
    fn dataless_account(lamports: u64) -> AccountSharedData {
        AccountSharedData::new(lamports, 0, &solana_sdk_ids::system_program::ID)
    }

    /// A DvP-nonce-tombstone-shaped account: program-owned, no data, sitting on
    /// the 1-lamport existence floor the SVM requires of a live account.
    fn tombstone_account() -> AccountSharedData {
        AccountSharedData::new(1, 0, &Pubkey::new_unique())
    }

    /// The float the gasless callback fabricates for an unknown fee payer.
    const FLOAT: u64 = DEFAULT_FEE_PAYER_LAMPORTS;

    /// Build a transaction over exactly `keys`, the first `writable` of them
    /// writable, plus a trailing read-only program id. Signatures are dummies:
    /// sanitization counts them, nothing here verifies them.
    fn tx_over(keys: &[Pubkey], writable: usize) -> SanitizedTransaction {
        use solana_sdk::{
            instruction::CompiledInstruction, message::MessageHeader, signature::Signature,
        };
        let mut account_keys = keys.to_vec();
        account_keys.push(solana_sdk_ids::system_program::ID);
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: (keys.len() - writable + 1) as u8,
            },
            account_keys,
            recent_blockhash: Hash::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: keys.len() as u8,
                accounts: vec![],
                data: vec![],
            }],
        };
        let tx = Transaction {
            signatures: vec![Signature::default()],
            message,
        };
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to build controlled-writability tx")
    }

    struct Outcome {
        status: Result<(), solana_transaction_error::TransactionError>,
        accounts: Vec<AccountSharedData>,
    }

    impl Outcome {
        fn rejected(&self) -> bool {
            self.status == Err(solana_transaction_error::TransactionError::UnbalancedTransaction)
        }
    }

    /// Seed `pre` into BOB, run the check over one successful transaction whose
    /// loaded accounts are `accounts` (the first `writable` writable), and
    /// return the resulting status and accounts.
    fn run_conservation_with(
        pre: &[(Pubkey, AccountSharedData)],
        accounts: Vec<(Pubkey, AccountSharedData)>,
        writable: usize,
        fee_payers: &[Pubkey],
    ) -> Outcome {
        let (mut bob, _settled_tx) = crate::test_helpers::create_test_bob();
        for (pubkey, account) in pre {
            bob.insert_account_for_test(*pubkey, account.clone());
        }
        let keys: Vec<Pubkey> = accounts.iter().map(|(pubkey, _)| *pubkey).collect();
        let tx = tx_over(&keys, writable);
        let mut output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![executed_with(accounts)],
            error_metrics: TransactionErrorMetrics::default(),
            execute_timings: ExecuteTimings::default(),
            balance_collector: None,
        };
        enforce_lamport_conservation(
            &mut output,
            std::slice::from_ref(&tx),
            &bob,
            &fee_payers.iter().copied().collect(),
            &(Arc::new(NoopMetrics) as SharedMetrics),
        );
        let Ok(ProcessedTransaction::Executed(executed)) = &output.processing_results[0] else {
            panic!("expected executed");
        };
        Outcome {
            status: executed.execution_details.status.clone(),
            accounts: executed
                .loaded_transaction
                .accounts
                .iter()
                .map(|(_, account)| account.clone())
                .collect(),
        }
    }

    /// `run_conservation_with` with every loaded account writable.
    fn run_conservation(
        pre: &[(Pubkey, AccountSharedData)],
        accounts: Vec<(Pubkey, AccountSharedData)>,
        fee_payers: &[Pubkey],
    ) -> Outcome {
        let writable = accounts.len();
        run_conservation_with(pre, accounts, writable, fee_payers)
    }

    /// A repaid loan erases the fabricated payer and leaves everything else
    /// exactly as the SVM produced it.
    #[tokio::test]
    async fn loan_repaid_erases_payer_and_touches_nothing_else() {
        let payer = Pubkey::new_unique();
        let existing = Pubkey::new_unique();
        let out = run_conservation(
            &[(existing, data_account(5000))],
            vec![
                (payer, dataless_account(FLOAT)),
                (existing, data_account(5000)),
            ],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(
            out.accounts[0],
            AccountSharedData::default(),
            "fabricated payer must be erased"
        );
        assert_eq!(
            out.accounts[1],
            data_account(5000),
            "a pre-existing account must be byte-identical after the check"
        );
    }

    /// A payer that spent its float with nothing to show for it never repaid
    /// the loan, so the transaction is rejected and no account is rewritten.
    #[tokio::test]
    async fn unrepaid_loan_rejects_transaction() {
        let payer = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let out = run_conservation(
            &[],
            vec![
                (payer, dataless_account(0)),
                (recipient, dataless_account(FLOAT)),
            ],
            &[payer],
        );
        assert!(out.rejected(), "status was {:?}", out.status);
        assert_eq!(out.accounts[0], dataless_account(0), "payer untouched");
        assert_eq!(
            out.accounts[1],
            dataless_account(FLOAT),
            "a rejected transaction must not trim the account that gained"
        );
    }

    /// Each account the transaction creates consumes one lamport of the float,
    /// because the SVM requires a live account to hold at least one.
    #[tokio::test]
    async fn creation_allowance_permits_one_lamport_per_new_account() {
        let payer = Pubkey::new_unique();
        let first = Pubkey::new_unique();
        let second = Pubkey::new_unique();
        let out = run_conservation(
            &[],
            vec![
                (payer, dataless_account(FLOAT - 2)),
                (first, data_account(1)),
                (second, data_account(1)),
            ],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(out.accounts[0], AccountSharedData::default());
        assert_eq!(out.accounts[1], data_account(1), "created account persists");
        assert_eq!(out.accounts[2], data_account(1), "created account persists");
    }

    /// The allowance is exact: one created account does not cover a shortfall of two.
    #[tokio::test]
    async fn creation_allowance_is_exact() {
        let payer = Pubkey::new_unique();
        let created = Pubkey::new_unique();
        let out = run_conservation(
            &[],
            vec![
                (payer, dataless_account(FLOAT - 2)),
                (created, data_account(1)),
            ],
            &[payer],
        );
        assert!(out.rejected(), "status was {:?}", out.status);
    }

    /// A pre-existing account credited by another pre-existing account keeps
    /// the credit. This is the property whose absence drains a wSOL escrow.
    #[tokio::test]
    async fn pre_existing_account_that_gains_is_untouched() {
        let payer = Pubkey::new_unique();
        let source = Pubkey::new_unique();
        let target = Pubkey::new_unique();
        let out = run_conservation(
            &[(source, data_account(5000)), (target, data_account(5000))],
            vec![
                (payer, dataless_account(FLOAT)),
                (source, data_account(4000)),
                (target, data_account(6000)),
            ],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(
            out.accounts[1],
            data_account(4000),
            "the debited side keeps its balance"
        );
        assert_eq!(
            out.accounts[2],
            data_account(6000),
            "the credited side keeps the credit"
        );
    }

    /// Counting creations does not prove the float paid for them. Here a new
    /// account is funded by real money while one float lamport lands in an
    /// account that already existed, so the count would licence a fabricated
    /// lamport becoming durable. The credit clause rejects it instead.
    #[tokio::test]
    async fn credited_pre_existing_account_rejects_when_float_is_missing() {
        let payer = Pubkey::new_unique();
        let funder = Pubkey::new_unique();
        let escrow = Pubkey::new_unique();
        let fresh = Pubkey::new_unique();
        let out = run_conservation(
            &[(funder, dataless_account(2)), (escrow, data_account(5000))],
            vec![
                // One lamport of the float is gone.
                (payer, dataless_account(FLOAT - 1)),
                // A real account paid for the new one, not the payer.
                (funder, dataless_account(1)),
                (fresh, data_account(1)),
                // The float lamport ended up here, in durable state.
                (escrow, data_account(5001)),
            ],
            &[payer],
        );
        assert!(
            out.rejected(),
            "a credited pre-existing account must not be licenced by an unrelated creation, status was {:?}",
            out.status
        );
        assert_eq!(
            out.accounts[3],
            data_account(5001),
            "a rejected transaction must still not rewrite the account that gained"
        );
    }

    /// The credit clause is conditional, not blanket: with the float intact,
    /// pre-existing accounts may move real lamports between themselves. A
    /// creation funded entirely by real money is still allowed to persist.
    #[tokio::test]
    async fn credited_pre_existing_account_is_fine_when_float_is_intact() {
        let payer = Pubkey::new_unique();
        let funder = Pubkey::new_unique();
        let escrow = Pubkey::new_unique();
        let out = run_conservation(
            &[(funder, dataless_account(5000)), (escrow, data_account(10))],
            vec![
                (payer, dataless_account(FLOAT)),
                (funder, dataless_account(4000)),
                (escrow, data_account(1010)),
            ],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(out.accounts[2], data_account(1010), "the credit stands");
    }

    /// Routing the float through an account that ends where it started, so it
    /// pays a new account's floor for it, is accepted. This is the allowance
    /// doing its job, not a way around it: the relay ends no richer, and total
    /// lamports still rise by exactly one per account created. Rejecting it
    /// would also reject an ordinary creation, which spends the float the same
    /// way in one hop instead of two.
    #[tokio::test]
    async fn float_relayed_through_a_flat_account_stays_within_the_allowance() {
        let payer = Pubkey::new_unique();
        let relay = Pubkey::new_unique();
        let fresh = Pubkey::new_unique();
        let out = run_conservation(
            &[(relay, data_account(5000))],
            vec![
                // One lamport of the float is gone.
                (payer, dataless_account(FLOAT - 1)),
                // It passed through here and left again, so this ends flat.
                (relay, data_account(5000)),
                // And it came to rest as the new account's existence floor.
                (fresh, data_account(1)),
            ],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(
            out.accounts[1],
            data_account(5000),
            "the relay must end exactly where it started, so it gained nothing"
        );
        assert_eq!(
            out.accounts[2],
            data_account(1),
            "the new account keeps the single lamport the allowance covers"
        );
    }

    /// An ordinary wallet (lamports, no data) is neither zeroed nor deleted.
    /// Force-deleting it was the documented divergence from Agave.
    #[tokio::test]
    async fn pre_existing_dataless_account_keeps_lamports() {
        let payer = Pubkey::new_unique();
        let wallet = Pubkey::new_unique();
        let out = run_conservation(
            &[(wallet, dataless_account(7))],
            vec![
                (payer, dataless_account(FLOAT)),
                (wallet, dataless_account(7)),
            ],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(out.accounts[1], dataless_account(7));
    }

    /// A program-owned dataless account the transaction created (the DvP nonce
    /// tombstone) survives, and its existence floor is covered by the allowance.
    #[tokio::test]
    async fn created_program_owned_dataless_account_survives() {
        let payer = Pubkey::new_unique();
        let tombstone = Pubkey::new_unique();
        let created = tombstone_account();
        let out = run_conservation(
            &[],
            vec![
                (payer, dataless_account(FLOAT - 1)),
                (tombstone, created.clone()),
            ],
            &[payer],
        );
        assert!(
            out.status.is_ok(),
            "the tombstone's lamport is covered by the creation allowance, got {:?}",
            out.status
        );
        assert_eq!(
            out.accounts[1], created,
            "the nonce tombstone must survive the batch that creates it"
        );
    }

    /// Read-only accounts are never inspected: they are not persisted, and
    /// erasing one would clobber a shared input. The read-only slot here is
    /// also a batch fee payer, so an inspection would visibly erase it.
    #[tokio::test]
    async fn readonly_accounts_are_never_inspected() {
        let payer = Pubkey::new_unique();
        let writable = Pubkey::new_unique();
        let readonly_payer = Pubkey::new_unique();
        let out = run_conservation_with(
            &[],
            vec![
                (payer, dataless_account(FLOAT)),
                (writable, dataless_account(0)),
                (readonly_payer, dataless_account(99)),
            ],
            2,
            &[payer, readonly_payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(
            out.accounts[2],
            dataless_account(99),
            "a read-only account must not be inspected or rewritten"
        );
    }

    /// A failed executed tx commits no account writes downstream, so the check
    /// leaves both its accounts and its status alone.
    #[tokio::test]
    async fn failed_executed_transaction_is_skipped() {
        let payer = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let original = Err(
            solana_transaction_error::TransactionError::InstructionError(
                1,
                solana_sdk::instruction::InstructionError::Custom(0),
            ),
        );
        let tx = tx_over(&[payer, other], 2);
        let mut output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![executed_with_status(
                original.clone(),
                vec![(payer, dataless_account(0)), (other, data_account(11))],
            )],
            error_metrics: TransactionErrorMetrics::default(),
            execute_timings: ExecuteTimings::default(),
            balance_collector: None,
        };
        let (bob, _settled_tx) = crate::test_helpers::create_test_bob();
        enforce_lamport_conservation(
            &mut output,
            std::slice::from_ref(&tx),
            &bob,
            &HashSet::from([payer]),
            &(Arc::new(NoopMetrics) as SharedMetrics),
        );
        let Ok(ProcessedTransaction::Executed(executed)) = &output.processing_results[0] else {
            panic!("expected executed");
        };
        assert_eq!(
            executed.execution_details.status, original,
            "a failed executed tx must not be re-judged"
        );
        let accounts = &executed.loaded_transaction.accounts;
        assert_eq!(accounts[0].1, dataless_account(0), "payer untouched");
        assert_eq!(accounts[1].1, data_account(11), "account untouched");
    }

    /// A fee payer BOB already knows is real money, not a fabrication: no loan
    /// is charged against it and it is not erased.
    #[tokio::test]
    async fn real_fee_payer_is_not_treated_as_fabricated() {
        let payer = Pubkey::new_unique();
        let out = run_conservation(
            &[(payer, dataless_account(5000))],
            vec![(payer, dataless_account(5000))],
            &[payer],
        );
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(out.accounts[0], dataless_account(5000));
    }

    /// A fabricated payer that allocated data still does not persist: the
    /// address never existed, so no part of it may become durable state.
    #[tokio::test]
    async fn fabricated_payer_with_data_still_does_not_persist() {
        let payer = Pubkey::new_unique();
        let out = run_conservation(&[], vec![(payer, data_account(FLOAT))], &[payer]);
        assert!(out.status.is_ok(), "status was {:?}", out.status);
        assert_eq!(out.accounts[0], AccountSharedData::default());
    }

    /// A fabricated payer that gained lamports is still erased. Real lamports
    /// sent to an address that never existed are burned, not persisted.
    #[tokio::test]
    async fn fabricated_payer_that_gains_is_erased() {
        let payer = Pubkey::new_unique();
        let source = Pubkey::new_unique();
        let out = run_conservation(
            &[(source, dataless_account(5000))],
            vec![
                (payer, dataless_account(FLOAT + 5)),
                (source, dataless_account(4995)),
            ],
            &[payer],
        );
        assert!(
            out.status.is_ok(),
            "an over-repaid loan is not a shortfall, got {:?}",
            out.status
        );
        assert_eq!(out.accounts[0], AccountSharedData::default());
        assert_eq!(
            out.accounts[1],
            dataless_account(4995),
            "the sender's debit stands"
        );
    }

    /// Fabrication follows the batch-wide fee-payer set, not this transaction's
    /// own payer, so draining one fabricated account into another is still a
    /// shortfall and cannot smuggle the float out.
    #[tokio::test]
    async fn second_fabricated_account_cannot_absorb_the_loan() {
        let payer = Pubkey::new_unique();
        let other_payer = Pubkey::new_unique();
        let out = run_conservation(
            &[],
            vec![
                (payer, dataless_account(0)),
                (other_payer, dataless_account(FLOAT * 2)),
            ],
            &[payer, other_payer],
        );
        assert!(out.rejected(), "status was {:?}", out.status);
    }

    // ── execute_batch behavioral tests (through the real SVM) ──

    /// The status a regular result carries after the conservation check.
    fn regular_status(
        result: &ExecutionResult,
        i: usize,
    ) -> Result<(), solana_transaction_error::TransactionError> {
        let Ok(ProcessedTransaction::Executed(executed)) = regular_result(result, i) else {
            panic!("expected an executed result at index {i}");
        };
        executed.execution_details.status.clone()
    }

    fn unbalanced() -> Result<(), solana_transaction_error::TransactionError> {
        Err(solana_transaction_error::TransactionError::UnbalancedTransaction)
    }

    /// Direct exploit: a fabricated payer transfers its whole float to R. The
    /// loan is unrepaid beyond the single lamport R's creation allows, so the
    /// transaction is rejected and nothing persists.
    #[tokio::test(flavor = "multi_thread")]
    async fn direct_exploit_is_rejected() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let r = Pubkey::new_unique();
        let result = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 10)]).await;

        assert_eq!(regular_status(&result, 0), unbalanced());
        assert!(
            bob_balance(&deps.bob, &r).is_none(),
            "R must gain nothing durable"
        );
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none(),
            "synthetic payer must not persist"
        );
    }

    /// Partial spend (5 of the float lands on R): still short by more than the
    /// one lamport R's creation allows, so it is rejected too.
    #[tokio::test(flavor = "multi_thread")]
    async fn partial_spend_persists_nothing() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let r = Pubkey::new_unique();
        let result = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 5)]).await;
        assert_eq!(regular_status(&result, 0), unbalanced());
        assert!(bob_balance(&deps.bob, &r).is_none());
    }

    /// 2-step re-use: a value-neutral setup tx (self-transfer of 0) cannot
    /// graduate the synthetic payer: it is erased and never persisted, so a
    /// later batch still treats `A` as synthetic and its spend is rejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_step_setup_does_not_graduate_payer() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let setup = run_batch(&mut deps, &metrics, vec![transfer(&a, &a.pubkey(), 0)]).await;
        assert_eq!(regular_status(&setup, 0), Ok(()), "the setup tx conserves");
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
            "synthetic payer must not graduate"
        );

        let r = Pubkey::new_unique();
        let spend = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 10)]).await;
        assert_eq!(regular_status(&spend, 0), unbalanced());
        assert!(bob_balance(&deps.bob, &r).is_none());

        // Monotonicity survives the trip through ExecutionResult: a later batch
        // always reports a strictly higher generation than an earlier one. The
        // generation is assigned per BOB update, so a rejected transfer still
        // consumes one.
        assert!(
            spend.regular_generation > setup.regular_generation,
            "generation must strictly increase across batches ({} then {})",
            setup.regular_generation,
            spend.regular_generation
        );
    }

    /// Synthetic fee payer is dropped: any synthetic-payer transaction erases
    /// the payer, so it is never persisted in BOB.
    #[tokio::test(flavor = "multi_thread")]
    async fn synthetic_fee_payer_dropped() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let r = Pubkey::new_unique();
        let _ = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 1)]).await;
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
            "synthetic fee payer must not be persisted"
        );
    }

    // ── Legitimate flows still work ──

    /// Legit gasless sponsorship: a fresh `A` pays for a real `B`'s transfer
    /// without sending or receiving value. The transfer lands on both sides and
    /// the fabricated sponsor is erased.
    #[tokio::test(flavor = "multi_thread")]
    async fn gasless_sponsor_succeeds_and_is_dropped() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let b = Keypair::new();
        fund(&mut deps.bob, &b.pubkey(), 5000);
        let a = Keypair::new(); // synthetic sponsor: neither sends nor receives
        let r = Pubkey::new_unique();
        let result = run_batch(
            &mut deps,
            &metrics,
            vec![sponsored_transfer(&a, &b, &r, 1000)],
        )
        .await;

        assert_eq!(regular_status(&result, 0), Ok(()));
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
            "sponsor must not be persisted"
        );
        assert_eq!(bob_balance(&deps.bob, &b.pubkey()), Some(4000));
        assert_eq!(bob_balance(&deps.bob, &r), Some(1000));
    }

    /// A transfer between accounts made of pre-existing lamports persists on
    /// both sides. This is the clearest semantic change from the old cap, which
    /// zeroed both.
    #[tokio::test(flavor = "multi_thread")]
    async fn real_transfer_persists_both_sides() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let b = Keypair::new();
        fund(&mut deps.bob, &b.pubkey(), 5000);
        let r = Pubkey::new_unique();
        let result = run_batch(&mut deps, &metrics, vec![transfer(&b, &r, 1000)]).await;

        assert_eq!(regular_status(&result, 0), Ok(()));
        assert_eq!(bob_balance(&deps.bob, &b.pubkey()), Some(4000));
        assert_eq!(bob_balance(&deps.bob, &r), Some(1000));
    }

    /// A gasless user creating an ATA: the payer's float covers the ATA's
    /// 1-lamport existence floor, so the transaction is accepted and the ATA
    /// persists. The admin InitializeMint shares the batch, and admin results land
    /// in BOB before regular execution, so the mint is visible here.
    #[tokio::test(flavor = "multi_thread")]
    async fn ata_creation_under_fabricated_payer_succeeds() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let (admin_tx, mint) = create_admin_initialize_mint_tx();
        let payer = Keypair::new();
        let wallet = Pubkey::new_unique();
        let ata = spl_associated_token_account::get_associated_token_address(&wallet, &mint);
        let ix = spl_associated_token_account::instruction::create_associated_token_account(
            &payer.pubkey(),
            &wallet,
            &mint,
            &spl_token::id(),
        );
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let raw = Transaction::new(&[&payer], msg, Hash::default());
        let ata_tx = SanitizedTransaction::try_from_legacy_transaction(raw, &HashSet::new())
            .expect("failed to build ATA-create tx");

        let result = run_batch(&mut deps, &metrics, vec![admin_tx, ata_tx]).await;

        assert_eq!(
            regular_status(&result, 0),
            Ok(()),
            "gasless ATA creation must be accepted"
        );
        assert_eq!(
            bob_balance(&deps.bob, &ata),
            Some(1),
            "the ATA must persist at its existence floor"
        );
        assert!(
            bob_balance(&deps.bob, &payer.pubkey()).is_none_or(|l| l == 0),
            "the fabricated payer must not persist"
        );
    }

    /// A transaction may list accounts no instruction touches. The unrelated
    /// writable account must come out byte-identical: rewriting it is what
    /// would drain a third party's escrow.
    #[tokio::test(flavor = "multi_thread")]
    async fn unrelated_writable_account_is_untouched() {
        use solana_sdk::{
            account::WritableAccount, instruction::CompiledInstruction, message::MessageHeader,
        };

        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let payer = Keypair::new();
        let unrelated = Pubkey::new_unique();
        // rent_epoch is the SVM loader's rent-exempt marker, stamped on every
        // account it loads; seed it so byte-identity is testable at all.
        let mut seeded = data_account(5000);
        seeded.set_rent_epoch(u64::MAX);
        deps.bob.insert_account_for_test(unrelated, seeded.clone());

        // A value-neutral self-transfer, with `unrelated` carried along as a
        // writable key no instruction references.
        let data =
            solana_system_interface::instruction::transfer(&payer.pubkey(), &payer.pubkey(), 0)
                .data;
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                payer.pubkey(),
                unrelated,
                solana_sdk_ids::system_program::ID,
            ],
            recent_blockhash: Hash::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 2,
                accounts: vec![0, 0],
                data,
            }],
        };
        let mut raw = Transaction::new_unsigned(message);
        raw.sign(&[&payer], Hash::default());
        let tx = SanitizedTransaction::try_from_legacy_transaction(raw, &HashSet::new())
            .expect("failed to build carrier tx");

        let result = run_batch(&mut deps, &metrics, vec![tx]).await;

        assert_eq!(regular_status(&result, 0), Ok(()));
        assert_eq!(
            deps.bob.get_account_shared_data(&unrelated),
            Some(seeded),
            "an account no instruction touched must be byte-identical"
        );
    }

    // Real-SVM premise: a mid-tx failure is Executed{Err} and persists nothing.

    /// A two-instruction system tx where ix0 succeeds and ix1 fails on
    /// insufficient funds. The SVM returns `Executed` with `status.is_err()`,
    /// proving the premise that a partial failure is not a top-level `Err`. The
    /// pre-funded payer must keep its pre-execution balance in BOB: its
    /// rolled-back intermediate state must not be committed.
    #[tokio::test(flavor = "multi_thread")]
    async fn partial_failure_through_svm_is_executed_err_and_persists_nothing() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let payer = Keypair::new();
        fund(&mut deps.bob, &payer.pubkey(), 100);
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();

        // ix0: payer pays A (60) and succeeds; ix1: payer pays B (60) and fails (only 40 left).
        let ix0 = solana_system_interface::instruction::transfer(&payer.pubkey(), &a, 60);
        let ix1 = solana_system_interface::instruction::transfer(&payer.pubkey(), &b, 60);
        let msg = Message::new(&[ix0, ix1], Some(&payer.pubkey()));
        let raw = Transaction::new(&[&payer], msg, Hash::default());
        let tx = SanitizedTransaction::try_from_legacy_transaction(raw, &HashSet::new())
            .expect("failed to build two-instruction tx");

        let result = run_batch(&mut deps, &metrics, vec![tx]).await;

        let r = regular_result(&result, 0);
        assert!(
            is_executed(r),
            "the SVM returns Executed for a mid-tx failure"
        );
        let Ok(ProcessedTransaction::Executed(executed)) = r else {
            panic!("expected executed");
        };
        assert!(
            !executed.was_successful(),
            "the partial failure surfaces as Executed with an Err status"
        );
        assert_eq!(
            bob_balance(&deps.bob, &payer.pubkey()),
            Some(100),
            "failed tx must not clobber the pre-funded payer with its rolled-back state"
        );
        assert!(
            bob_balance(&deps.bob, &a).is_none_or(|l| l == 0),
            "intermediate credit to A must not persist"
        );
        assert!(
            bob_balance(&deps.bob, &b).is_none_or(|l| l == 0),
            "B was never credited and must be absent"
        );
    }

    // ── Path parity & invariants ──

    /// Parallel path (SnapshotCallback) must reach the same verdicts as the
    /// sequential path for a batch of synthetic-payer spends.
    #[tokio::test(flavor = "multi_thread")]
    async fn conservation_parallel_path_parity() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let workers = 4;
        let mut deps =
            get_execution_deps(accounts_db, rx, workers, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        // Above the parallel threshold so SnapshotCallback fabricates the payers.
        let n = workers * MIN_PARALLEL_BATCH_FACTOR * 2;
        let mut txs = Vec::with_capacity(n);
        let mut payers = Vec::with_capacity(n);
        let mut recipients = Vec::with_capacity(n);
        for _ in 0..n {
            let a = Keypair::new();
            let r = Pubkey::new_unique();
            txs.push(transfer(&a, &r, 10)); // 1-step spend, unrepaid loan
            payers.push(a);
            recipients.push(r);
        }
        let result = run_batch(&mut deps, &metrics, txs).await;

        for i in 0..n {
            assert_eq!(
                regular_status(&result, i),
                unbalanced(),
                "tx {i} must be rejected on the parallel path"
            );
        }
        for a in &payers {
            assert!(
                bob_balance(&deps.bob, &a.pubkey()).is_none(),
                "synthetic payer must not persist on the parallel path"
            );
        }
        for r in &recipients {
            assert!(
                bob_balance(&deps.bob, r).is_none(),
                "a rejected tx must persist nothing on the parallel path"
            );
        }
    }

    /// Trigger the parallel path: enough txs to give every configured worker
    /// a non-trivial chunk. Verifies result count + ordering match the input.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_parallel_path() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let workers = 4;
        let mut deps =
            get_execution_deps(accounts_db, rx, workers, default_live_blockhashes()).await;

        // 2× the parallel threshold so each worker gets 2× MIN_PARALLEL_BATCH_FACTOR
        // transactions — comfortably inside the parallel regime.
        let n = workers * MIN_PARALLEL_BATCH_FACTOR * 2;
        let transactions: Vec<_> = (0..n)
            .map(|i| crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(create_test_transaction()),
                index: i,
            })
            .collect();
        let batch = ConflictFreeBatch { transactions };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();

        assert_eq!(result.regular_transactions.len(), n);
        assert!(result.admin_transactions.is_empty());
        let results = result
            .regular_results
            .expect("parallel path must produce regular results");
        // Merged output must have exactly one processing result per input tx.
        assert_eq!(results.processing_results.len(), n);
    }

    /// Exercise the exact parallel threshold (lowest batch size that takes
    /// the parallel path): `max_svm_workers * MIN_PARALLEL_BATCH_FACTOR` txs.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_parallel_threshold_boundary() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let workers = 4;
        let mut deps =
            get_execution_deps(accounts_db, rx, workers, default_live_blockhashes()).await;

        let n = workers * MIN_PARALLEL_BATCH_FACTOR;
        let transactions: Vec<_> = (0..n)
            .map(|i| crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(create_test_transaction()),
                index: i,
            })
            .collect();
        let batch = ConflictFreeBatch { transactions };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();

        let results = result.regular_results.unwrap();
        assert_eq!(results.processing_results.len(), n);
    }

    /// Build a well-formed admin InitializeMint tx (single SPL Token ix,
    /// type=0), returning it alongside the mint address it initializes.
    fn create_admin_initialize_mint_tx() -> (SanitizedTransaction, Pubkey) {
        use solana_sdk::instruction::{AccountMeta, Instruction};

        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let mut data = vec![0u8; 35];
        data[1] = 6; // decimals
        data[2..34].copy_from_slice(&authority.to_bytes());
        data[34] = 0; // no freeze authority
        let ix = Instruction {
            program_id: spl_token::id(),
            accounts: vec![
                AccountMeta::new(mint, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data,
        };
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, Hash::default());
        let sanitized = SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to create admin init-mint tx");
        (sanitized, mint)
    }

    /// Build a mixed tx: one admin instruction (InitializeMint) + one
    /// non-admin instruction (system transfer). Router must NOT send this to
    /// the Admin VM.
    fn create_mixed_admin_and_regular_tx() -> SanitizedTransaction {
        use solana_sdk::instruction::{AccountMeta, Instruction};

        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let mut data = vec![0u8; 35];
        data[1] = 6;
        data[2..34].copy_from_slice(&authority.to_bytes());
        let init_mint_ix = Instruction {
            program_id: spl_token::id(),
            accounts: vec![
                AccountMeta::new(mint, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data,
        };
        let transfer_ix =
            solana_system_interface::instruction::transfer(&payer.pubkey(), &recipient, 100);
        let msg = Message::new(&[init_mint_ix, transfer_ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to create mixed tx")
    }

    // An empty batch yields empty partitions and no VM invocations.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_empty_batch() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 4, default_live_blockhashes()).await;

        let empty_batch = ConflictFreeBatch {
            transactions: vec![],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(empty_batch, &mut deps, &noop).await.unwrap();
        assert!(result.admin_transactions.is_empty());
        assert!(result.regular_transactions.is_empty());
        assert!(result.admin_results.is_none());
        assert!(result.regular_results.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_single_normal_transaction() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 4, default_live_blockhashes()).await;

        let tx = create_test_transaction();
        let batch = ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: 0,
            }],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();
        assert!(!result.regular_transactions.is_empty());
        assert!(result.admin_transactions.is_empty());
        assert!(
            result.regular_results.is_some(),
            "regular results should be present"
        );
        assert!(
            result.admin_results.is_none(),
            "no admin results for normal tx"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_multiple_normal_transactions() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 4, default_live_blockhashes()).await;

        let tx1 = create_test_transaction();
        let tx2 = create_test_transaction();
        let batch = ConflictFreeBatch {
            transactions: vec![
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(tx1),
                    index: 0,
                },
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(tx2),
                    index: 1,
                },
            ],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();
        assert_eq!(result.regular_transactions.len(), 2);
        assert!(result.admin_transactions.is_empty());
        let results = result.regular_results.unwrap();
        assert_eq!(results.processing_results.len(), 2);
    }

    /// Txs whose recent_blockhash is not in the live window must be dropped
    /// before SVM dispatch. Settler invariant `processing_results.len() ==
    /// transactions.len()` must still hold over the filtered vec.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_drops_expired_transactions() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();

        let known = Hash::new_unique();
        let live = Arc::new(RwLock::new(LinkedList::from([known])));
        let mut deps = get_execution_deps(accounts_db, rx, 4, Arc::clone(&live)).await;

        // Two txs using the known (live) hash + one tx using an expired hash.
        let payer = Keypair::new();
        let live_tx_1 = sanitize_transfer(&payer, known);
        let live_tx_2 = sanitize_transfer(&payer, known);
        let expired_tx = sanitize_transfer(&payer, Hash::new_unique());
        let expired_sig = *expired_tx.signature();

        let batch = ConflictFreeBatch {
            transactions: vec![
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(live_tx_1),
                    index: 0,
                },
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(expired_tx),
                    index: 1,
                },
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(live_tx_2),
                    index: 2,
                },
            ],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();

        assert_eq!(
            result.regular_transactions.len(),
            2,
            "expired tx must be dropped"
        );
        assert!(
            !result
                .regular_transactions
                .iter()
                .any(|tx| *tx.signature() == expired_sig),
            "expired tx must not appear in regular_transactions"
        );
        let results = result.regular_results.unwrap();
        assert_eq!(
            results.processing_results.len(),
            2,
            "settler invariant: processing_results.len() == transactions.len()"
        );
    }

    /// Plumbing check: the live_blockhashes Arc is read each call, not snapshotted
    /// at deps construction. Mutating the Arc (what dedup does when the window
    /// advances) must flip the filter's verdict on subsequent execute_batch calls.
    /// Guards against a refactor that copies the LinkedList instead of cloning the Arc.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_reads_live_window_each_call() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();

        let bh = Hash::new_unique();
        let live = Arc::new(RwLock::new(LinkedList::from([bh])));
        let mut deps = get_execution_deps(accounts_db, rx, 4, Arc::clone(&live)).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let batch_with = |payer: &Keypair| ConflictFreeBatch {
            transactions: (0..3)
                .map(|i| crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(sanitize_transfer(payer, bh)),
                    index: i,
                })
                .collect(),
        };

        // Pass 1: bh is in the live window — all 3 must execute.
        let r1 = execute_batch(batch_with(&Keypair::new()), &mut deps, &noop)
            .await
            .unwrap();
        assert_eq!(
            r1.regular_transactions.len(),
            3,
            "all live txs must execute"
        );

        // Evict bh from the shared Arc (the operation dedup performs on eviction).
        live.write().unwrap().clear();

        // Pass 2: same blockhash, now expired — all 3 must be filtered.
        let r2 = execute_batch(batch_with(&Keypair::new()), &mut deps, &noop)
            .await
            .unwrap();
        assert_eq!(
            r2.regular_transactions.len(),
            0,
            "evicted-bh txs must be filtered"
        );
        assert!(
            r2.regular_results.is_none(),
            "no SVM run when batch is fully filtered"
        );
    }

    /// The stage exits when its input closes, which is how shutdown reaches it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execution_worker_exits_when_input_closes() {
        let (_accounts_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (batch_tx, batch_rx) = mpsc::channel::<ConflictFreeBatch>(16);
        let (_settled_tx, settled_rx) = mpsc::unbounded_channel();
        let (execution_results_tx, _execution_results_rx) =
            mpsc::channel::<ExecutedBatch>(RESULTS_CAP);

        let handle = start_execution_worker(ExecutionArgs {
            batch_rx,
            settled_accounts_rx: settled_rx,
            execution_results_tx,
            accountsdb_connection_url: url,
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
            max_svm_workers: 4,
            live_blockhashes: default_live_blockhashes(),
        })
        .await;

        drop(batch_tx);

        let result = tokio::time::timeout(Duration::from_secs(5), handle.handle).await;
        assert!(result.is_ok(), "worker should exit once its input closes");
    }

    // --- Corner-case coverage for the parallel SVM execution path.
    //
    // The tests above establish that the parallel path produces the right
    // number of results for "typical" batch sizes. The tests below target
    // invariants that a count-only assertion would miss: ordering across
    // worker-thread joins, uneven-chunk handling, the gate that forces the
    // sequential path, and the accumulation contract of merge_svm_outputs.

    /// Order preservation end-to-end through the parallel path.
    ///
    /// `execute_batch` must return `regular_transactions` and the merged
    /// `processing_results` in input order, even when execute_parallel
    /// splits them across worker threads. This test would fail if a future
    /// refactor joined workers in completion order instead of spawn order
    /// (e.g. switching to a FuturesUnordered-style collector).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_parallel_path_preserves_transaction_order() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let workers = 4;
        let mut deps =
            get_execution_deps(accounts_db, rx, workers, default_live_blockhashes()).await;

        // 2× the parallel threshold so the batch is comfortably in the
        // parallel regime and splits into multiple chunks.
        let n = workers * MIN_PARALLEL_BATCH_FACTOR * 2;
        let inputs: Vec<SanitizedTransaction> = (0..n).map(|_| create_test_transaction()).collect();
        let input_signatures: Vec<_> = inputs.iter().map(|tx| *tx.signature()).collect();

        let transactions: Vec<_> = inputs
            .into_iter()
            .enumerate()
            .map(|(i, tx)| crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: i,
            })
            .collect();
        let batch = ConflictFreeBatch { transactions };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();

        let output_signatures: Vec<_> = result
            .regular_transactions
            .iter()
            .map(|tx| *tx.signature())
            .collect();
        assert_eq!(
            output_signatures, input_signatures,
            "regular_transactions must be in input order after parallel execution"
        );

        let results = result
            .regular_results
            .expect("parallel path must produce regular results");
        assert_eq!(
            results.processing_results.len(),
            n,
            "merge_svm_outputs must produce exactly one processing_result per input"
        );
    }

    /// Uneven chunking: a batch size that does not divide evenly across
    /// workers. For `max_svm_workers=4` and `n=17`, chunks are sized
    /// `[5, 5, 5, 2]` — exercises the small tail-chunk path and ensures
    /// all 17 transactions appear in the merged output in input order.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_parallel_path_uneven_chunking() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let workers = 4;
        let mut deps =
            get_execution_deps(accounts_db, rx, workers, default_live_blockhashes()).await;

        // 17 is intentional: > threshold (16), not divisible by 4, last
        // chunk is much smaller than the others.
        let n = 17;
        let inputs: Vec<SanitizedTransaction> = (0..n).map(|_| create_test_transaction()).collect();
        let input_signatures: Vec<_> = inputs.iter().map(|tx| *tx.signature()).collect();

        let transactions: Vec<_> = inputs
            .into_iter()
            .enumerate()
            .map(|(i, tx)| crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: i,
            })
            .collect();
        let batch = ConflictFreeBatch { transactions };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();

        let output_signatures: Vec<_> = result
            .regular_transactions
            .iter()
            .map(|tx| *tx.signature())
            .collect();
        assert_eq!(
            output_signatures, input_signatures,
            "uneven chunks must not reorder transactions"
        );
        let results = result
            .regular_results
            .expect("parallel path must produce regular results");
        assert_eq!(
            results.processing_results.len(),
            n,
            "all {n} transactions (including the small tail chunk) must appear in the merged output"
        );
    }

    /// `max_svm_workers = 1` forces the sequential path regardless of batch
    /// size. The gate is `max_svm_workers >= 2 && len >= parallel_min`;
    /// with workers=1 the gate is false by construction.
    ///
    /// This test doubles as a structural guard on the gate itself: if
    /// someone removed the `max_svm_workers >= 2` check,
    /// `execute_parallel`'s `num_workers.clamp(2, 1)` would panic at
    /// runtime (clamp requires min <= max), so the test would surface a
    /// regression even without a dedicated "which path was taken" probe.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_max_svm_workers_one_forces_sequential() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;

        // Deliberately well above any reasonable parallel threshold — with
        // workers=2 this size would split; with workers=1 the gate keeps
        // it sequential.
        let n = 64;
        let inputs: Vec<SanitizedTransaction> = (0..n).map(|_| create_test_transaction()).collect();
        let input_signatures: Vec<_> = inputs.iter().map(|tx| *tx.signature()).collect();

        let transactions: Vec<_> = inputs
            .into_iter()
            .enumerate()
            .map(|(i, tx)| crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: i,
            })
            .collect();
        let batch = ConflictFreeBatch { transactions };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();

        let output_signatures: Vec<_> = result
            .regular_transactions
            .iter()
            .map(|tx| *tx.signature())
            .collect();
        assert_eq!(
            output_signatures, input_signatures,
            "sequential path must preserve input order"
        );
        let results = result
            .regular_results
            .expect("sequential path must produce regular results");
        assert_eq!(results.processing_results.len(), n);
    }

    // --- merge_svm_outputs unit tests ---
    //
    // merge_svm_outputs is pure, so we can test it directly with fabricated
    // outputs instead of going through the SVM. These cover the contract
    // execute_parallel relies on: concatenation in chunk-vec order,
    // accumulation of error_metrics and execute_timings, and the constant
    // `balance_collector = None`.

    fn fabricate_output(
        results: Vec<solana_svm::transaction_processing_result::TransactionProcessingResult>,
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: results,
            error_metrics: TransactionErrorMetrics::default(),
            execute_timings: ExecuteTimings::default(),
            balance_collector: None,
        }
    }

    #[test]
    fn test_merge_svm_outputs_empty_input() {
        let merged = merge_svm_outputs(vec![]);
        assert!(merged.processing_results.is_empty());
        assert!(merged.balance_collector.is_none());
        // Default metrics and timings are all zero; spot-check one counter.
        assert_eq!(merged.error_metrics.account_not_found.0, 0);
    }

    #[test]
    fn test_merge_svm_outputs_single_chunk_passthrough() {
        use solana_transaction_error::TransactionError;
        let chunk = fabricate_output(vec![
            Err(TransactionError::AccountNotFound),
            Err(TransactionError::AccountNotFound),
            Err(TransactionError::AccountNotFound),
        ]);
        let merged = merge_svm_outputs(vec![chunk]);
        assert_eq!(merged.processing_results.len(), 3);
        assert!(merged
            .processing_results
            .iter()
            .all(|r| matches!(r, Err(TransactionError::AccountNotFound))));
    }

    /// Multiple uneven chunks: each chunk uses a distinct `TransactionError`
    /// variant, so after merge we can positionally verify the concatenation
    /// order. If merge interleaved or reordered chunks, the variant
    /// sequence would not match.
    #[test]
    fn test_merge_svm_outputs_preserves_chunk_order() {
        use solana_transaction_error::TransactionError;
        let chunk_a = fabricate_output(vec![
            Err(TransactionError::AccountNotFound),
            Err(TransactionError::AccountNotFound),
            Err(TransactionError::AccountNotFound),
        ]);
        let chunk_b = fabricate_output(vec![Err(TransactionError::BlockhashNotFound)]);
        let chunk_c = fabricate_output(vec![
            Err(TransactionError::AccountInUse),
            Err(TransactionError::AccountInUse),
        ]);

        let merged = merge_svm_outputs(vec![chunk_a, chunk_b, chunk_c]);
        assert_eq!(merged.processing_results.len(), 6);

        let tag =
            |r: &solana_svm::transaction_processing_result::TransactionProcessingResult| match r {
                Err(TransactionError::AccountNotFound) => "anf",
                Err(TransactionError::BlockhashNotFound) => "bnf",
                Err(TransactionError::AccountInUse) => "aiu",
                _ => "other",
            };
        let order: Vec<_> = merged.processing_results.iter().map(tag).collect();
        assert_eq!(
            order,
            vec!["anf", "anf", "anf", "bnf", "aiu", "aiu"],
            "chunks must concatenate in input vec order, never interleave"
        );
    }

    #[test]
    fn test_merge_svm_outputs_accumulates_error_metrics() {
        use std::num::Saturating;

        let mut chunk_a = fabricate_output(vec![]);
        chunk_a.error_metrics.account_not_found = Saturating(3);
        chunk_a.error_metrics.insufficient_funds = Saturating(1);

        let mut chunk_b = fabricate_output(vec![]);
        chunk_b.error_metrics.account_not_found = Saturating(5);
        chunk_b.error_metrics.blockhash_not_found = Saturating(2);

        let merged = merge_svm_outputs(vec![chunk_a, chunk_b]);

        // Fields that appear in both chunks sum; fields that appear in only
        // one carry through; untouched fields stay zero.
        assert_eq!(merged.error_metrics.account_not_found.0, 8);
        assert_eq!(merged.error_metrics.insufficient_funds.0, 1);
        assert_eq!(merged.error_metrics.blockhash_not_found.0, 2);
        assert_eq!(merged.error_metrics.already_processed.0, 0);
    }

    #[test]
    fn test_merge_svm_outputs_accumulates_execute_timings() {
        use solana_timings::ExecuteTimingType;
        use std::num::Saturating;

        let mut chunk_a = fabricate_output(vec![]);
        chunk_a.execute_timings.metrics[ExecuteTimingType::LoadUs] = Saturating(100);
        chunk_a.execute_timings.metrics[ExecuteTimingType::ExecuteUs] = Saturating(200);

        let mut chunk_b = fabricate_output(vec![]);
        chunk_b.execute_timings.metrics[ExecuteTimingType::LoadUs] = Saturating(50);
        chunk_b.execute_timings.metrics[ExecuteTimingType::StoreUs] = Saturating(75);

        let merged = merge_svm_outputs(vec![chunk_a, chunk_b]);

        assert_eq!(
            merged.execute_timings.metrics[ExecuteTimingType::LoadUs].0,
            150,
            "overlapping timing fields must sum"
        );
        assert_eq!(
            merged.execute_timings.metrics[ExecuteTimingType::ExecuteUs].0,
            200,
            "fields set in only one chunk must carry through"
        );
        assert_eq!(
            merged.execute_timings.metrics[ExecuteTimingType::StoreUs].0,
            75,
            "fields set in only one chunk must carry through"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execution_worker_channel_closed_exits() {
        let (_accounts_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (batch_tx, batch_rx) = mpsc::channel::<ConflictFreeBatch>(16);
        let (_settled_tx, settled_rx) = mpsc::unbounded_channel();
        let (execution_results_tx, _execution_results_rx) =
            mpsc::channel::<ExecutedBatch>(RESULTS_CAP);
        let _shutdown = CancellationToken::new();

        let handle = start_execution_worker(ExecutionArgs {
            batch_rx,
            settled_accounts_rx: settled_rx,
            execution_results_tx,
            accountsdb_connection_url: url,
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
            max_svm_workers: 4,
            live_blockhashes: default_live_blockhashes(),
        })
        .await;

        drop(batch_tx);

        // Worker should exit when input channel closes
        let result = tokio::time::timeout(Duration::from_secs(2), handle.handle).await;
        assert!(
            result.is_ok(),
            "worker should exit when input channel is closed"
        );
    }

    // ─── Router tests (admin routing must be all-or-nothing) ───

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_routes_pure_admin_tx_to_admin_vm() {
        // A tx whose only instruction is an admin instruction routes to the Admin VM.
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 4, default_live_blockhashes()).await;

        let (tx, _mint) = create_admin_initialize_mint_tx();
        let batch = ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: 0,
            }],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();
        assert_eq!(result.admin_transactions.len(), 1);
        assert!(result.regular_transactions.is_empty());
        assert!(result.admin_results.is_some());
        assert!(result.regular_results.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_routes_mixed_admin_regular_to_real_svm() {
        // A tx that mixes one admin instruction (InitializeMint) with one
        // non-admin instruction (system transfer) must NOT be sent to the
        // Admin VM. The router sends it to the regular SVM path; the admin
        // path stays strictly single-purpose.
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 4, default_live_blockhashes()).await;

        let tx = create_mixed_admin_and_regular_tx();
        let batch = ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: 0,
            }],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();
        assert!(
            result.admin_transactions.is_empty(),
            "mixed tx must not be admin-routed"
        );
        assert_eq!(result.regular_transactions.len(), 1);
        assert!(result.admin_results.is_none());
        assert!(result.regular_results.is_some());
    }

    // In a batch with one pure-admin tx and one pure-regular tx, each routes
    // to the correct VM and both partitions produce results.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_execute_batch_partitions_admin_and_regular_separately() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 4, default_live_blockhashes()).await;

        let (admin_tx, _mint) = create_admin_initialize_mint_tx();
        let regular_tx = create_test_transaction();
        let batch = ConflictFreeBatch {
            transactions: vec![
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(admin_tx),
                    index: 0,
                },
                crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(regular_tx),
                    index: 1,
                },
            ],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await.unwrap();
        assert_eq!(result.admin_transactions.len(), 1);
        assert_eq!(result.regular_transactions.len(), 1);
        assert!(result.admin_results.is_some());
        assert!(result.regular_results.is_some());
        // Each path gets its own BOB write, so each gets its own generation.
        // Exact values, because the admin path must be settled before the
        // regular path so regular transactions observe the admin updates.
        assert_eq!(
            result.admin_generation, 1,
            "the admin BOB update must come first"
        );
        assert_eq!(
            result.regular_generation, 2,
            "the regular BOB update must follow the admin one"
        );
    }

    // A full results channel blocks the executor's send (backpressure) without
    // panic or lock poison — proving no guard is held across the await — and the
    // blocked result is delivered once the receiver drains.
    #[tokio::test(flavor = "multi_thread")]
    async fn executor_blocks_then_completes_on_full_results() {
        let (_accounts_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (batch_tx, batch_rx) = mpsc::channel::<ConflictFreeBatch>(16);
        let (_settled_tx, settled_rx) = mpsc::unbounded_channel();
        let (execution_results_tx, mut execution_results_rx) = mpsc::channel::<ExecutedBatch>(1);
        let shutdown = CancellationToken::new();

        let _handle = start_execution_worker(ExecutionArgs {
            batch_rx,
            settled_accounts_rx: settled_rx,
            execution_results_tx,
            accountsdb_connection_url: url,
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
            max_svm_workers: 1,
            live_blockhashes: default_live_blockhashes(),
        })
        .await;

        let one_batch = || ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(create_test_transaction()),
                index: 0,
            }],
        };
        // Two batches: the first fills the cap-1 channel, the second's send blocks.
        batch_tx.send(one_batch()).await.unwrap();
        batch_tx.send(one_batch()).await.unwrap();

        // First result is available.
        let first = tokio::time::timeout(Duration::from_secs(5), execution_results_rx.recv()).await;
        assert!(first.is_ok(), "first result must arrive");

        // Draining the first unblocks the executor's parked send; the second arrives.
        let second =
            tokio::time::timeout(Duration::from_secs(5), execution_results_rx.recv()).await;
        assert!(
            matches!(second, Ok(Some(_))),
            "second result must arrive once the channel drains (no deadlock, no poison)"
        );

        shutdown.cancel();
    }

    // A full results channel with no receiver must not wedge executor shutdown:
    // the send is raced against the shutdown token.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_results_channel_never_costs_an_executed_batch() {
        let (_accounts_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (batch_tx, batch_rx) = mpsc::channel::<ConflictFreeBatch>(16);
        let (_settled_tx, settled_rx) = mpsc::unbounded_channel();
        let (execution_results_tx, execution_results_rx) = mpsc::channel::<ExecutedBatch>(1);
        let _shutdown = CancellationToken::new();

        let handle = start_execution_worker(ExecutionArgs {
            batch_rx,
            settled_accounts_rx: settled_rx,
            execution_results_tx,
            accountsdb_connection_url: url,
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
            max_svm_workers: 1,
            live_blockhashes: default_live_blockhashes(),
        })
        .await;

        let one_batch = || ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(create_test_transaction()),
                index: 0,
            }],
        };
        // Fill the cap-1 channel so the second send parks, then close the input.
        batch_tx.send(one_batch()).await.unwrap();
        batch_tx.send(one_batch()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(batch_tx);

        // Draining lets the parked send complete. Both batches have already run
        // against the in-memory accounts, so neither may be abandoned.
        let mut received = 0;
        let mut rx = execution_results_rx;
        while received < 2 {
            match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
                Ok(Some(_)) => received += 1,
                _ => break,
            }
        }
        assert_eq!(received, 2, "a full results channel must not drop a batch");

        let result = tokio::time::timeout(Duration::from_secs(10), handle.handle).await;
        assert!(result.is_ok(), "executor must exit once its input closes");
    }

    /// A batch whose accounts could not be loaded must abort before any SVM run,
    /// so nothing is executed against phantom-absent state and settled.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn execute_batch_aborts_when_preload_cannot_read_the_store() {
        use crate::accounts::get_accounts::{reset_test_retry, set_test_retry};

        set_test_retry(2, 1);
        let (_settled_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(
            crate::test_helpers::dead_postgres_db(),
            rx,
            1,
            default_live_blockhashes(),
        )
        .await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let payer = Keypair::new();
        let batch = ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(sanitize_transfer(&payer, Hash::default())),
                index: 0,
            }],
        };
        let result = execute_batch(batch, &mut deps, &noop).await;
        reset_test_retry();

        assert!(result.is_err(), "an unreadable store must abort the batch");
        assert!(
            deps.bob.get_account_shared_data(&payer.pubkey()).is_none(),
            "nothing may be cached from a read that failed"
        );
    }

    async fn insert_corrupt_pg(db: &AccountsDB, pubkey: Pubkey) {
        if let AccountsDB::Postgres(pg) = db {
            sqlx::query(
                "INSERT INTO accounts (pubkey, data) VALUES ($1, $2)
                 ON CONFLICT (pubkey) DO UPDATE SET data = EXCLUDED.data",
            )
            .bind(pubkey.to_bytes().to_vec())
            .bind(vec![0xAAu8; 5])
            .execute(pg.pool.as_ref())
            .await
            .unwrap();
        }
    }

    /// A corrupt account is a fatal integrity fault: execute_batch aborts the
    /// whole batch so nothing is settled, rather than failing single txs.
    #[tokio::test(flavor = "multi_thread")]
    async fn corrupt_account_aborts_batch() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let corrupt = Pubkey::new_unique();
        insert_corrupt_pg(&accounts_db, corrupt).await;

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let batch = ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(transfer(&Keypair::new(), &corrupt, 10)),
                index: 0,
            }],
        };
        let result = execute_batch(batch, &mut deps, &noop).await;

        assert!(
            matches!(result, Err(AccountLoadError::Corrupt(k)) if k == corrupt),
            "a referenced corrupt account must abort the batch fatally"
        );
    }

    /// A corrupt account referenced by no transaction has no effect: it is never
    /// loaded, so the batch executes normally.
    #[tokio::test(flavor = "multi_thread")]
    async fn unreferenced_corrupt_account_has_no_effect() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let unref = Pubkey::new_unique();
        insert_corrupt_pg(&accounts_db, unref).await;

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let result = run_batch(
            &mut deps,
            &metrics,
            vec![transfer(&Keypair::new(), &Pubkey::new_unique(), 10)],
        )
        .await;

        assert_eq!(result.regular_transactions.len(), 1);
        assert!(is_executed(regular_result(&result, 0)));
    }
    // -- preload planner --

    fn sizes(pairs: &[(Pubkey, usize)]) -> HashMap<Pubkey, usize> {
        pairs.iter().copied().collect()
    }

    /// The cap is a ceiling the SVM itself allows, so a set landing exactly on
    /// it must still run; only a set past it is rejected.
    #[test]
    fn plan_preload_admits_up_to_the_limit_and_rejects_past_it() {
        let key = Pubkey::new_unique();
        let cases = [(999usize, true), (1_000, true), (1_001, false)];

        for (size, expected_admitted) in cases {
            let plan = plan_preload(&[vec![key]], &sizes(&[(key, size)]), 1_000, 10_000);
            if expected_admitted {
                assert_eq!(plan.admitted, vec![0], "{size} bytes must be admitted");
                assert!(plan.oversized.is_empty());
            } else {
                assert!(plan.admitted.is_empty(), "{size} bytes must be rejected");
                assert_eq!(plan.oversized, vec![0]);
            }
            assert!(plan.deferred.is_empty());
        }
    }

    /// An account named by several transactions is fetched once, so the budget
    /// must charge it once too.
    #[test]
    fn plan_preload_charges_a_shared_account_once() {
        let shared = Pubkey::new_unique();
        let plan = plan_preload(
            &[vec![shared], vec![shared]],
            &sizes(&[(shared, 600)]),
            10_000,
            1_000,
        );

        assert_eq!(plan.admitted, vec![0, 1], "the union is 600, not 1200");
        assert!(plan.deferred.is_empty());
    }

    /// The walk stops at the first transaction that does not fit and defers the
    /// rest in order, so a deferred sub-batch stays a valid conflict-free batch.
    #[test]
    fn plan_preload_defers_everything_after_the_first_overflow() {
        let keys: Vec<Pubkey> = (0..4).map(|_| Pubkey::new_unique()).collect();
        let sized = sizes(&[
            (keys[0], 400),
            (keys[1], 400),
            (keys[2], 400),
            (keys[3], 10),
        ]);
        let key_sets: Vec<Vec<Pubkey>> = keys.iter().map(|k| vec![*k]).collect();

        let plan = plan_preload(&key_sets, &sized, 10_000, 1_000);

        assert_eq!(plan.admitted, vec![0, 1]);
        assert_eq!(
            plan.deferred,
            vec![2, 3],
            "the small tx after the overflow must wait its turn, not jump it"
        );
        assert!(plan.oversized.is_empty());
    }

    /// The first transaction of a walk always fits an empty union, so a batch
    /// can never stall on a transaction it will never be able to start.
    #[test]
    fn plan_preload_always_admits_a_first_transaction_at_the_cap() {
        let key = Pubkey::new_unique();
        let plan = plan_preload(&[vec![key]], &sizes(&[(key, 1_000)]), 1_000, 1_000);

        assert_eq!(plan.admitted, vec![0]);
        assert!(plan.deferred.is_empty() && plan.oversized.is_empty());
    }

    /// Progress cannot depend on the two limits being ordered. A transaction
    /// under the per-transaction cap but over the budget must still run, or the
    /// caller would rebuild the same batch forever.
    #[test]
    fn plan_preload_admits_a_first_transaction_larger_than_the_whole_budget() {
        let key = Pubkey::new_unique();
        let plan = plan_preload(&[vec![key]], &sizes(&[(key, 5_000)]), 10_000, 1_000);

        assert_eq!(
            plan.admitted,
            vec![0],
            "deferring index 0 would leave the batch unable to make progress"
        );
        assert!(plan.deferred.is_empty() && plan.oversized.is_empty());
    }

    /// Keys with no stored row cost nothing, and an oversized transaction is
    /// never preloaded, so its keys must not enter the union or the running sum.
    #[test]
    fn plan_preload_ignores_absent_keys_and_oversized_keys() {
        let shared = Pubkey::new_unique();
        let huge = Pubkey::new_unique();
        let absent = Pubkey::new_unique();
        let sized = sizes(&[(shared, 100), (huge, 5_000)]);

        let plan = plan_preload(
            &[vec![huge, shared], vec![shared, absent]],
            &sized,
            1_000,
            1_000,
        );

        assert_eq!(plan.oversized, vec![0], "5100 bytes is past the 1000 cap");
        assert_eq!(plan.admitted, vec![1]);
        assert!(
            plan.deferred.is_empty(),
            "the oversized tx must not spend budget it never loads"
        );
    }

    // -- preload size gating in execute_batch --

    /// 10 MiB, the largest an account can be, so a handful of them clears the
    /// per-transaction cap with a transaction that still fits in a packet.
    const BIG_ACCOUNT_BYTES: usize = 10 * 1024 * 1024;

    /// A transaction naming more account data than the SVM would load must fail
    /// without its accounts ever being read out of the store.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_batch_fails_an_oversized_tx_without_loading_its_accounts() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let big = seed_sized_accounts(&mut accounts_db, 7, BIG_ACCOUNT_BYTES).await;

        let (_settled_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let oversized_payer = Keypair::new();
        let control_payer = Keypair::new();
        let oversized = transfer_with_unused_readonly(&oversized_payer, &big);
        let oversized_sig = *oversized.signature();
        let control = sanitize_transfer(&control_payer, Hash::default());
        let control_sig = *control.signature();

        let bytes_before = deps.bob.cache_stats().bytes;
        let result = run_batch(&mut deps, &noop, vec![oversized, control]).await;

        assert_eq!(
            result.regular_transactions.len(),
            result
                .regular_results
                .as_ref()
                .expect("regular path ran")
                .processing_results
                .len(),
            "results must stay aligned with the transactions they describe"
        );

        let position = |sig| {
            result
                .regular_transactions
                .iter()
                .position(|tx| *tx.signature() == sig)
                .expect("every submitted tx must be accounted for")
        };
        let results = &result
            .regular_results
            .as_ref()
            .expect("regular path ran")
            .processing_results;

        assert!(
            matches!(
                results[position(oversized_sig)],
                Err(TransactionError::MaxLoadedAccountsDataSizeExceeded)
            ),
            "an oversized tx must fail exactly as the SVM would have failed it"
        );
        assert!(
            results[position(control_sig)].is_ok(),
            "the control tx must still execute"
        );

        for key in &big {
            assert!(
                deps.bob.get_account_shared_data(key).is_none(),
                "an oversized tx's accounts must never be fetched"
            );
        }
        let growth = deps.bob.cache_stats().bytes - bytes_before;
        assert!(
            growth < BIG_ACCOUNT_BYTES,
            "cache grew by {growth} bytes, so the big accounts were loaded after all"
        );
    }

    /// A batch that would preload more than its budget runs in pieces, and the
    /// overflow comes back untouched rather than being dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_batch_defers_the_transactions_past_its_preload_budget() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let readonly = seed_sized_accounts(&mut accounts_db, 3, 1_500).await;
        let mut deps = deps_with_budget(accounts_db, 3_000).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let txs: Vec<SanitizedTransaction> = readonly
            .iter()
            .map(|key| transfer_with_unused_readonly(&Keypair::new(), &[*key]))
            .collect();
        let deferred_sig = *txs[2].signature();

        let result = run_batch(&mut deps, &noop, txs).await;

        assert_eq!(
            result.regular_transactions.len(),
            2,
            "two txs fit the budget"
        );
        assert_eq!(result.deferred.len(), 1);
        assert_eq!(*result.deferred[0].signature(), deferred_sig);

        assert!(deps.bob.get_account_shared_data(&readonly[0]).is_some());
        assert!(deps.bob.get_account_shared_data(&readonly[1]).is_some());
        assert!(
            deps.bob.get_account_shared_data(&readonly[2]).is_none(),
            "a deferred tx's accounts must not be preloaded either"
        );
    }

    /// Transactions sharing a readonly account are charged for it once, so a
    /// budget that fits the union must not split them.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_batch_charges_a_shared_readonly_account_once() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let shared = seed_sized_accounts(&mut accounts_db, 1, 2_000).await[0];
        let mut deps = deps_with_budget(accounts_db, 2_500).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let txs = vec![
            transfer_with_unused_readonly(&Keypair::new(), &[shared]),
            transfer_with_unused_readonly(&Keypair::new(), &[shared]),
        ];
        let result = run_batch(&mut deps, &noop, txs).await;

        assert_eq!(result.regular_transactions.len(), 2);
        assert!(
            result.deferred.is_empty(),
            "4000 bytes of transactions are 2000 bytes of accounts"
        );
    }

    /// A batch whose accounts are already resident must not need the store, so
    /// sizing cannot have introduced a round-trip on the warm path.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_batch_runs_a_warm_batch_without_the_store() {
        let (_settled_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut deps = get_execution_deps(
            crate::test_helpers::dead_postgres_db(),
            rx,
            1,
            default_live_blockhashes(),
        )
        .await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let payer = Keypair::new();
        let recipient = Pubkey::new_unique();
        fund(&mut deps.bob, &payer.pubkey(), 1_000_000);
        fund(&mut deps.bob, &recipient, 1);
        let ix = solana_system_interface::instruction::transfer(&payer.pubkey(), &recipient, 100);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, Hash::default());
        let tx = SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();

        let result = run_batch(&mut deps, &noop, vec![tx]).await;
        assert!(regular_result(&result, 0).is_ok());
    }

    /// A batch of nothing but oversized transactions still has to produce a
    /// result per transaction, which is the shape simulation reads.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_batch_reports_a_batch_that_is_entirely_oversized() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let big = seed_sized_accounts(&mut accounts_db, 1, 2_000).await;
        let mut deps = deps_with_tx_cap(accounts_db, 1_000).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let tx = transfer_with_unused_readonly(&Keypair::new(), &big);
        let result = run_batch(&mut deps, &noop, vec![tx]).await;

        assert!(result.admin_results.is_none());
        assert!(result.deferred.is_empty());
        let results = &result
            .regular_results
            .as_ref()
            .expect("an oversized tx still needs a result")
            .processing_results;
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0],
            Err(TransactionError::MaxLoadedAccountsDataSizeExceeded)
        ));
    }

    /// The gate runs before admin routing, so an oversized admin transaction is
    /// failed rather than handed to the admin VM.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_batch_fails_an_oversized_admin_tx_before_routing_it() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let big = seed_sized_accounts(&mut accounts_db, 1, 2_000).await;
        let mut deps = deps_with_tx_cap(accounts_db, 1_000).await;
        let noop: SharedMetrics = Arc::new(NoopMetrics);

        let admin_tx = {
            use solana_sdk::instruction::{AccountMeta, Instruction};

            let payer = Keypair::new();
            let mut data = vec![0u8; 35];
            data[1] = 6;
            data[2..34].copy_from_slice(&Pubkey::new_unique().to_bytes());
            let ix = Instruction {
                program_id: spl_token::id(),
                accounts: vec![
                    AccountMeta::new(Pubkey::new_unique(), false),
                    AccountMeta::new(payer.pubkey(), true),
                ],
                data,
            };
            let mut msg = Message::new(&[ix], Some(&payer.pubkey()));
            msg.account_keys.extend_from_slice(&big);
            msg.header.num_readonly_unsigned_accounts += big.len() as u8;
            let tx = Transaction::new(&[&payer], msg, Hash::default());
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
                .expect("an admin tx with unused readonly keys is still well formed")
        };
        let result = run_batch(&mut deps, &noop, vec![admin_tx]).await;

        assert!(
            result.admin_transactions.is_empty(),
            "an oversized tx must never reach the admin VM"
        );
        assert_eq!(result.regular_transactions.len(), 1);
        assert!(matches!(
            result
                .regular_results
                .as_ref()
                .expect("it is reported on the regular path")
                .processing_results[0],
            Err(TransactionError::MaxLoadedAccountsDataSizeExceeded)
        ));
    }

    // -- deferred sub-batch loop --

    /// A batch too big to preload at once is handed to the settler in pieces,
    /// and each piece goes out before the next one is loaded, so no single
    /// preload pulls more than the budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn process_batch_forwards_each_sub_batch_before_running_the_next() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let readonly = seed_sized_accounts(&mut accounts_db, 3, 1_500).await;
        let mut deps = deps_with_budget(accounts_db, 3_000).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let heartbeat = crate::health::StageHeartbeat::new();

        let txs: Vec<SanitizedTransaction> = readonly
            .iter()
            .map(|key| transfer_with_unused_readonly(&Keypair::new(), &[*key]))
            .collect();
        let expected: HashSet<_> = txs.iter().map(|tx| *tx.signature()).collect();
        let batch = ConflictFreeBatch {
            transactions: txs
                .into_iter()
                .enumerate()
                .map(|(index, tx)| crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(tx),
                    index,
                })
                .collect(),
        };

        let (results_tx, mut results_rx) = mpsc::channel::<ExecutedBatch>(8);
        let outcome = process_batch(batch, &mut deps, &results_tx, &metrics, &heartbeat).await;
        assert!(matches!(outcome, BatchOutcome::Continue { executed: 3 }));
        drop(results_tx);

        let mut messages = 0;
        let mut seen = HashSet::new();
        while let Some(executed) = results_rx.recv().await {
            messages += 1;
            for tx in &executed.1 {
                assert!(seen.insert(*tx.signature()), "a tx must be settled once");
            }
        }
        assert_eq!(
            messages, 2,
            "the batch must reach the settler in two pieces"
        );
        assert_eq!(
            seen, expected,
            "every tx must reach the settler exactly once"
        );
    }

    /// A settler that closes mid-way must stop the executor rather than run the
    /// deferred remainder into a channel nothing is reading.
    #[tokio::test(flavor = "multi_thread")]
    async fn process_batch_stops_when_the_settler_closes_between_sub_batches() {
        let (mut accounts_db, _pg) = start_test_postgres().await;
        let readonly = seed_sized_accounts(&mut accounts_db, 3, 1_500).await;
        let mut deps = deps_with_budget(accounts_db, 3_000).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let heartbeat = crate::health::StageHeartbeat::new();

        let batch = ConflictFreeBatch {
            transactions: readonly
                .iter()
                .enumerate()
                .map(|(index, key)| crate::scheduler::TransactionWithIndex {
                    transaction: Arc::new(transfer_with_unused_readonly(&Keypair::new(), &[*key])),
                    index,
                })
                .collect(),
        };

        let (results_tx, results_rx) = mpsc::channel::<ExecutedBatch>(8);
        drop(results_rx);
        let outcome = process_batch(batch, &mut deps, &results_tx, &metrics, &heartbeat).await;

        assert!(
            matches!(outcome, BatchOutcome::Stop),
            "a closed settler must stop the executor, not silently drop the remainder"
        );
    }
}
