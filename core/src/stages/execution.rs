use {
    crate::{
        accounts::{bob::BOB, AccountsDB},
        nodes::node::WorkerHandle,
        processor::{
            create_transaction_batch_processor, get_transaction_check_results,
            PrivateChannelForkGraph,
        },
        scheduler::ConflictFreeBatch,
        stage_metrics::SharedMetrics,
        stages::AccountSettlement,
        transactions::is_admin_instruction,
        vm::{
            admin::AdminVm,
            clock::set_clock_now,
            gasless_callback::{GaslessCallback, SnapshotCallback},
            gasless_rent_collector::GaslessRentCollector,
        },
    },
    solana_compute_budget::compute_budget::SVMTransactionExecutionBudget,
    solana_sdk::{hash::Hash, pubkey::Pubkey, transaction::SanitizedTransaction},
    solana_svm::{
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processor::{
            LoadAndExecuteSanitizedTransactionsOutput, TransactionBatchProcessor,
            TransactionProcessingConfig, TransactionProcessingEnvironment,
        },
    },
    solana_svm_feature_set::SVMFeatureSet,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_timings::ExecuteTimings,
    std::{
        collections::{HashSet, LinkedList},
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
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
    pub settled_accounts_rx: mpsc::UnboundedReceiver<Vec<(Pubkey, AccountSettlement)>>,
    pub execution_results_tx: mpsc::UnboundedSender<(
        LoadAndExecuteSanitizedTransactionsOutput,
        Vec<SanitizedTransaction>,
    )>,
    pub accountsdb_connection_url: String,
    pub shutdown_token: CancellationToken,
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

    // Must prevent this from being dropped
    _fork_graph: Arc<RwLock<PrivateChannelForkGraph>>,
}

pub struct ExecutionResult {
    pub admin_transactions: Vec<SanitizedTransaction>,
    pub regular_transactions: Vec<SanitizedTransaction>,
    pub admin_results: Option<LoadAndExecuteSanitizedTransactionsOutput>,
    pub regular_results: Option<LoadAndExecuteSanitizedTransactionsOutput>,
}

pub async fn start_execution_worker(args: ExecutionArgs) -> WorkerHandle {
    let ExecutionArgs {
        mut batch_rx,
        settled_accounts_rx,
        execution_results_tx,
        accountsdb_connection_url,
        shutdown_token,
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
            tokio::select! {
                // Process batches
                result = batch_rx.recv() => {
                    match result {
                        Some(batch) => {
                            heartbeat.record_input();
                            let batch_size = batch.transactions.len();
                            debug!("Executor received batch with {} transactions", batch_size);

                            let execution_result = execute_batch(
                                batch,
                                &mut execution_deps,
                                &metrics,
                            ).await;

                            let num_transactions_executed = execution_result.admin_transactions.len() + execution_result.regular_transactions.len();
                            heartbeat.record_progress();
                            if !execution_result.admin_transactions.is_empty() {
                                if let Some(admin_results) = execution_result.admin_results {
                                    let len = execution_result.admin_transactions.len();
                                    if let Err(e) = execution_results_tx.send((admin_results, execution_result.admin_transactions)) {
                                        metrics.executor_results_send_failed("admin");
                                        error!("Failed to send admin results: {:?}", e);
                                        break;
                                    }
                                    metrics.executor_results_sent(len);
                                } else {
                                    metrics.executor_missing_results("admin");
                                    error!("Unexpected error: No result found for admin transactions");
                                    break;
                                }
                            }
                            if !execution_result.regular_transactions.is_empty() {
                                if let Some(regular_results) = execution_result.regular_results {
                                    let len = execution_result.regular_transactions.len();
                                    if let Err(e) = execution_results_tx.send((regular_results, execution_result.regular_transactions)) {
                                        metrics.executor_results_send_failed("regular");
                                        error!("Failed to send regular results: {:?}", e);
                                        break;
                                    }
                                    metrics.executor_results_sent(len);
                                } else {
                                    metrics.executor_missing_results("regular");
                                    error!("Unexpected error: No result found for regular transactions");
                                    break;
                                }
                            }

                            total_transactions_executed += num_transactions_executed as u64;
                            total_batches_processed += 1;

                            if total_batches_processed.is_multiple_of(100) {
                                info!("Executor has processed {} batches, {} total transactions",
                                      total_batches_processed, total_transactions_executed);
                            }
                        }
                        None => {
                            info!("Executor stopped - channel closed, executed {} total transactions in {} batches",
                                  total_transactions_executed, total_batches_processed);
                            return;
                        }
                    }
                }

                // Handle shutdown signal
                _ = shutdown_token.cancelled() => {
                    info!("Executor received shutdown signal, executed {} total transactions in {} batches",
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
    settled_accounts_rx: mpsc::UnboundedReceiver<Vec<(Pubkey, AccountSettlement)>>,
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

pub async fn execute_batch(
    batch: ConflictFreeBatch,
    execution_deps: &mut ExecutionDeps,
    metrics: &SharedMetrics,
) -> ExecutionResult {
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
    let (preload_fetched, preload_cached) = execution_deps
        .bob
        .preload_accounts(&accounts_to_preload)
        .await;
    let t_preload = t_op.elapsed();
    debug!(
        "preload: {} accounts ({} fetched, {} cached) in {:?}",
        accounts_to_preload.len(),
        preload_fetched,
        preload_cached,
        t_preload
    );
    metrics.executor_preload_duration_ms(t_preload.as_secs_f64() * 1000.0);

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
        execution_deps
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
        let regular_results = if use_parallel {
            // Parallel path: snapshot BOB + spawn workers.
            // `accounts_to_preload` covers admin+regular keys; harmless
            // over-inclusion — admin keys in the snapshot just add a few
            // HashMap entries that regular-tx workers will never look up.
            let snapshot =
                SnapshotCallback::from_bob(&execution_deps.bob, &accounts_to_preload, fee_payers);
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
            let gasless_callback = GaslessCallback::new(&execution_deps.bob, fee_payers);
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

        // Update BOB's in-memory accounts with the execution results
        let t_op = Instant::now();
        execution_deps
            .bob
            .update_accounts(&regular_results, &regular_transactions);
        t_bob_reg = t_op.elapsed();
        debug!("bob_update_regular: {:?}", t_bob_reg);
        metrics.executor_bob_update_duration_ms("regular", t_bob_reg.as_secs_f64() * 1000.0);

        Some(regular_results)
    } else {
        None
    };

    let t_total = t_batch.elapsed();
    debug!(
        "execute_batch complete: total={} admin={} regular={} | \
         partition={:?} preload={:?} svm_admin={:?} bob_admin={:?} svm_reg={:?} bob_reg={:?} total={:?}",
        batch_size,
        num_admin_transactions,
        num_regular_transactions,
        t_partition,
        t_preload,
        t_svm_admin,
        t_bob_admin,
        t_svm_reg,
        t_bob_reg,
        t_total,
    );
    metrics.executor_batch_duration_ms(t_total.as_secs_f64() * 1000.0);

    ExecutionResult {
        admin_transactions,
        regular_transactions,
        admin_results,
        regular_results,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{stage_metrics::NoopMetrics, test_helpers::start_test_postgres};
    use solana_sdk::{
        hash::Hash,
        message::Message,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    };
    use solana_svm::transaction_processor::LoadAndExecuteSanitizedTransactionsOutput;
    use std::collections::{HashSet, LinkedList};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

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
        let result = execute_batch(batch, &mut deps, &noop).await;

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
        let result = execute_batch(batch, &mut deps, &noop).await;

        let results = result.regular_results.unwrap();
        assert_eq!(results.processing_results.len(), n);
    }

    /// Build a well-formed admin InitializeMint tx (single SPL Token ix, type=0).
    fn create_admin_initialize_mint_tx() -> SanitizedTransaction {
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
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("failed to create admin init-mint tx")
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
        let result = execute_batch(empty_batch, &mut deps, &noop).await;
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
        let result = execute_batch(batch, &mut deps, &noop).await;
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
        let result = execute_batch(batch, &mut deps, &noop).await;
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
        let result = execute_batch(batch, &mut deps, &noop).await;

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
        let r1 = execute_batch(batch_with(&Keypair::new()), &mut deps, &noop).await;
        assert_eq!(
            r1.regular_transactions.len(),
            3,
            "all live txs must execute"
        );

        // Evict bh from the shared Arc (the operation dedup performs on eviction).
        live.write().unwrap().clear();

        // Pass 2: same blockhash, now expired — all 3 must be filtered.
        let r2 = execute_batch(batch_with(&Keypair::new()), &mut deps, &noop).await;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_execution_worker_shutdown_exits_cleanly() {
        let (_accounts_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (_batch_tx, batch_rx) = mpsc::channel::<ConflictFreeBatch>(16);
        let (_settled_tx, settled_rx) = mpsc::unbounded_channel();
        let (execution_results_tx, _execution_results_rx) = mpsc::unbounded_channel::<(
            LoadAndExecuteSanitizedTransactionsOutput,
            Vec<SanitizedTransaction>,
        )>();
        let shutdown = CancellationToken::new();

        let handle = start_execution_worker(ExecutionArgs {
            batch_rx,
            settled_accounts_rx: settled_rx,
            execution_results_tx,
            accountsdb_connection_url: url,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
            max_svm_workers: 4,
            live_blockhashes: default_live_blockhashes(),
        })
        .await;

        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), handle.handle).await;
        assert!(result.is_ok(), "worker should exit promptly after shutdown");
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
        let result = execute_batch(batch, &mut deps, &noop).await;

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
        let result = execute_batch(batch, &mut deps, &noop).await;

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
        let result = execute_batch(batch, &mut deps, &noop).await;

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
        let (execution_results_tx, _execution_results_rx) = mpsc::unbounded_channel::<(
            LoadAndExecuteSanitizedTransactionsOutput,
            Vec<SanitizedTransaction>,
        )>();
        let shutdown = CancellationToken::new();

        let handle = start_execution_worker(ExecutionArgs {
            batch_rx,
            settled_accounts_rx: settled_rx,
            execution_results_tx,
            accountsdb_connection_url: url,
            shutdown_token: shutdown.clone(),
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

        let tx = create_admin_initialize_mint_tx();
        let batch = ConflictFreeBatch {
            transactions: vec![crate::scheduler::TransactionWithIndex {
                transaction: Arc::new(tx),
                index: 0,
            }],
        };

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let result = execute_batch(batch, &mut deps, &noop).await;
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
        let result = execute_batch(batch, &mut deps, &noop).await;
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

        let admin_tx = create_admin_initialize_mint_tx();
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
        let result = execute_batch(batch, &mut deps, &noop).await;
        assert_eq!(result.admin_transactions.len(), 1);
        assert_eq!(result.regular_transactions.len(), 1);
        assert!(result.admin_results.is_some());
        assert!(result.regular_results.is_some());
    }
}
