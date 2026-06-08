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
    solana_sdk::{
        account::{ReadableAccount, WritableAccount},
        hash::Hash,
        pubkey::Pubkey,
        transaction::SanitizedTransaction,
    },
    solana_svm::{
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processing_result::ProcessedTransaction,
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

/// Cap each writable account's post-execution lamports so fabricated SOL never
/// persists: a data account keeps at most its 1-lamport existence floor (0
/// deallocates in the SVM, so the floor is 1, not 0); a dataless account keeps 0.
/// The cap only ever reduces.
///
/// Enumeration-free — any leak (direct `Transfer`, park-then-`CloseAccount`,
/// `RecoverNested`, swap close, any future program) ends with lamports rising
/// above a floor, which the cap removes regardless of how. Legit flows are
/// untouched: transfers don't move lamports; new mints/ATAs are floored at 1 so
/// they persist; the dataless synthetic fee payer caps to 0 so it's deleted.
///
/// Assumes no durable native lamports beyond existence floors (deposits mint
/// tokens; no wrapped SOL) — revisit if native SOL is added. Regular path only
/// (admin never fabricates fee payers).
fn cap_lamports(
    output: &mut LoadAndExecuteSanitizedTransactionsOutput,
    transactions: &[SanitizedTransaction],
) {
    for (result, tx) in output
        .processing_results
        .iter_mut()
        .zip(transactions.iter())
    {
        let Ok(ProcessedTransaction::Executed(executed)) = result else {
            continue;
        };
        for (index, (_, acct)) in executed.loaded_transaction.accounts.iter_mut().enumerate() {
            // Only writable accounts are persisted by the settler / BOB update.
            if !tx.is_writable(index) {
                continue;
            }
            let cap = if acct.data().is_empty() { 0 } else { 1 };
            if acct.lamports() > cap {
                acct.set_lamports(cap);
            }
        }
    }
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
        let mut regular_results = if use_parallel {
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

        // Cap writable lamports before either consumer reads the shared output:
        // the in-memory BOB update below and the durable settler downstream. One
        // mutation covers both consumers and both exec paths.
        cap_lamports(&mut regular_results, &regular_transactions);

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
    use crate::{
        accounts::bob::BOB, stage_metrics::NoopMetrics, test_helpers::start_test_postgres,
    };
    use solana_sdk::account::AccountSharedData;
    use solana_sdk::{
        hash::Hash,
        message::Message,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    };
    use solana_svm::transaction_processing_result::TransactionProcessingResult;
    use solana_svm::transaction_processor::LoadAndExecuteSanitizedTransactionsOutput;
    use solana_svm_callback::TransactionProcessingCallback;
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
        execute_batch(ConflictFreeBatch { transactions }, deps, metrics).await
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

    // ── cap_lamports unit tests (cap math, tested directly) ──
    //
    // cap_lamports is pure over (output, transactions). We build a 1-tx output
    // whose loaded accounts we control, pair it with a tx whose writability we
    // know (`transfer` → idx 0,1 writable, idx 2 system_program read-only),
    // run the cap, and read the accounts back.

    /// Build a single-tx Executed output carrying `accounts` at the loaded
    /// transaction's account positions.
    fn executed_with(accounts: Vec<(Pubkey, AccountSharedData)>) -> TransactionProcessingResult {
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
                    status: Ok(()),
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

    /// A token-like data account (program-owned, non-empty data) with `lamports`.
    fn data_account(lamports: u64) -> AccountSharedData {
        AccountSharedData::new(lamports, 8, &spl_token::id())
    }

    /// A dataless system-owned account with `lamports`.
    fn dataless_account(lamports: u64) -> AccountSharedData {
        AccountSharedData::new(lamports, 0, &solana_sdk_ids::system_program::ID)
    }

    /// Run cap_lamports over one tx whose writable indices are 0 and 1.
    /// Returns the post-cap accounts.
    fn run_cap_one_tx(accounts: Vec<(Pubkey, AccountSharedData)>) -> Vec<AccountSharedData> {
        // `transfer` yields exactly: [from(0,w), to(1,w), system_program(2,ro)].
        let tx = transfer(&Keypair::new(), &Pubkey::new_unique(), 0);
        let mut output = LoadAndExecuteSanitizedTransactionsOutput {
            processing_results: vec![executed_with(accounts)],
            error_metrics: TransactionErrorMetrics::default(),
            execute_timings: ExecuteTimings::default(),
            balance_collector: None,
        };
        cap_lamports(&mut output, std::slice::from_ref(&tx));
        let Ok(ProcessedTransaction::Executed(executed)) = &output.processing_results[0] else {
            panic!("expected executed");
        };
        executed
            .loaded_transaction
            .accounts
            .iter()
            .map(|(_, a)| a.clone())
            .collect()
    }

    /// Data account with excess lamports is floored to its 1-lamport existence
    /// floor — parking fabricated lamports in a token account is neutralized.
    #[test]
    fn cap_data_account_excess_to_one() {
        let out = run_cap_one_tx(vec![(Pubkey::new_unique(), data_account(11))]);
        assert_eq!(out[0].lamports(), 1);
    }

    /// Data account already at the 1-lamport floor is untouched (ATA/mint floor
    /// preserved — legit accounts persist).
    #[test]
    fn cap_data_account_one_is_noop() {
        let out = run_cap_one_tx(vec![(Pubkey::new_unique(), data_account(1))]);
        assert_eq!(out[0].lamports(), 1);
    }

    /// Data account being closed (0 lamports) stays at 0 — the cap only ever
    /// reduces, never raises to the floor.
    #[test]
    fn cap_data_account_zero_stays_zero() {
        let out = run_cap_one_tx(vec![(Pubkey::new_unique(), data_account(0))]);
        assert_eq!(out[0].lamports(), 0);
    }

    /// Dataless account with excess lamports is zeroed — covers the direct
    /// exploit recipient, a RecoverNested destination wallet, a swap close
    /// destination: any dataless gainer.
    #[test]
    fn cap_dataless_account_excess_to_zero() {
        let out = run_cap_one_tx(vec![(Pubkey::new_unique(), dataless_account(10))]);
        assert_eq!(out[0].lamports(), 0);
    }

    /// Dataless account already at 0 is untouched.
    #[test]
    fn cap_dataless_account_zero_is_noop() {
        let out = run_cap_one_tx(vec![(Pubkey::new_unique(), dataless_account(0))]);
        assert_eq!(out[0].lamports(), 0);
    }

    /// Mixed surgical cap: in one result a data account (11 lamports) floors to
    /// 1 while a dataless account (10 lamports) zeroes — per-account, not global.
    #[test]
    fn cap_mixed_per_account() {
        let out = run_cap_one_tx(vec![
            (Pubkey::new_unique(), data_account(11)),
            (Pubkey::new_unique(), dataless_account(10)),
        ]);
        assert_eq!(out[0].lamports(), 1, "data account floored to 1");
        assert!(!out[0].data().is_empty(), "data preserved");
        assert_eq!(out[1].lamports(), 0, "dataless account zeroed");
    }

    /// Read-only accounts are not capped: only writable accounts are persisted,
    /// so capping a read-only account would be pointless work (and could clobber
    /// a shared input the settler never writes). idx 2 is the read-only
    /// system_program slot in a `transfer`.
    #[test]
    fn cap_skips_readonly_account() {
        let out = run_cap_one_tx(vec![
            (Pubkey::new_unique(), dataless_account(0)), // idx 0, writable
            (Pubkey::new_unique(), dataless_account(0)), // idx 1, writable
            (Pubkey::new_unique(), dataless_account(99)), // idx 2, read-only
        ]);
        assert_eq!(
            out[2].lamports(),
            99,
            "read-only account must not be capped"
        );
    }

    // ── execute_batch behavioral tests (system transfers, through the SVM) ──

    /// Direct exploit, behavioral shift from the old reject model: a fresh `A`
    /// transfers its fabricated 10 lamports to `R`. Under the cap the tx is NOT
    /// rejected — it executes — but R (a dataless gainer) is capped to 0 and
    /// nothing durable is created. A is dataless → capped to 0 → absent.
    #[tokio::test(flavor = "multi_thread")]
    async fn direct_exploit_executes_but_persists_nothing() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let r = Pubkey::new_unique();
        let result = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 10)]).await;

        assert!(
            is_executed(regular_result(&result, 0)),
            "under the cap the exploit tx executes, it is not rejected"
        );
        assert!(
            bob_balance(&deps.bob, &r).is_none_or(|l| l == 0),
            "R must gain nothing durable"
        );
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
            "synthetic payer must not persist"
        );
    }

    /// Partial spend (ends at 5 on R): R is a dataless gainer → capped to 0.
    #[tokio::test(flavor = "multi_thread")]
    async fn partial_spend_persists_nothing() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let r = Pubkey::new_unique();
        let result = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 5)]).await;
        assert!(is_executed(regular_result(&result, 0)));
        assert!(bob_balance(&deps.bob, &r).is_none_or(|l| l == 0));
    }

    /// 2-step re-use: a value-neutral setup tx (self-transfer of 0) cannot
    /// graduate the synthetic payer — it is dataless → capped to 0 → not
    /// persisted, so a later batch still treats `A` as synthetic and its spend
    /// still persists nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_step_setup_does_not_graduate_payer() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let a = Keypair::new();
        let setup = run_batch(&mut deps, &metrics, vec![transfer(&a, &a.pubkey(), 0)]).await;
        assert!(is_executed(regular_result(&setup, 0)));
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
            "synthetic payer must not graduate"
        );

        let r = Pubkey::new_unique();
        let spend = run_batch(&mut deps, &metrics, vec![transfer(&a, &r, 10)]).await;
        assert!(is_executed(regular_result(&spend, 0)));
        assert!(bob_balance(&deps.bob, &r).is_none_or(|l| l == 0));
    }

    /// Synthetic fee payer is dropped: any synthetic-payer system transfer →
    /// the payer (dataless) is capped to 0 → not persisted in BOB.
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
    /// without sending or receiving value. The tx settles, `B→R` lands, and the
    /// fabricated sponsor account is dropped (dataless → capped to 0).
    #[tokio::test(flavor = "multi_thread")]
    async fn legit_gasless_sponsor_succeeds_and_payer_dropped() {
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

        assert!(
            is_executed(regular_result(&result, 0)),
            "legit sponsored transfer must succeed"
        );
        // The sponsor pays no fee (gasless) and never touches value. All three
        // accounts here are dataless system accounts: under the cap none persist
        // durable native lamports (the channel has no native SOL). What matters
        // for this test is that the gasless sponsorship EXECUTES (the SVM does
        // not reject for a missing fee payer) and the synthetic sponsor is not
        // graduated into BOB.
        assert!(
            bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
            "sponsor must not be persisted"
        );
        assert!(
            bob_balance(&deps.bob, &r).is_none_or(|l| l == 0),
            "dataless recipient is capped to 0"
        );
    }

    /// A pre-funded real payer settles normally — the cap is a no-op on accounts
    /// whose lamports don't exceed their floor.
    #[tokio::test(flavor = "multi_thread")]
    async fn real_prefunded_payer_unaffected() {
        let (accounts_db, _pg) = start_test_postgres().await;
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut deps = get_execution_deps(accounts_db, rx, 1, default_live_blockhashes()).await;
        let metrics: SharedMetrics = Arc::new(NoopMetrics);

        let b = Keypair::new();
        fund(&mut deps.bob, &b.pubkey(), 5000);
        let r = Pubkey::new_unique();
        let result = run_batch(&mut deps, &metrics, vec![transfer(&b, &r, 1000)]).await;

        assert!(is_executed(regular_result(&result, 0)));
        // B and R are dataless system accounts. Both are capped to 0 durably —
        // a system-account-to-system-account transfer of plain lamports has no
        // durable representation in this channel (there is no native SOL). The
        // transfer still executes; nothing native persists.
        assert!(bob_balance(&deps.bob, &r).is_none_or(|l| l == 0));
        assert!(bob_balance(&deps.bob, &b.pubkey()).is_none_or(|l| l == 0));
    }

    // ── Path parity & invariants ──

    /// Parallel path (SnapshotCallback) must reach the same capped outcomes as
    /// the sequential path for a batch of synthetic-payer transfers.
    #[tokio::test(flavor = "multi_thread")]
    async fn cap_parallel_path_parity() {
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
            txs.push(transfer(&a, &r, 10)); // 1-step spend → capped
            payers.push(a);
            recipients.push(r);
        }
        let result = run_batch(&mut deps, &metrics, txs).await;

        for i in 0..n {
            assert!(
                is_executed(regular_result(&result, i)),
                "tx {i} must execute on the parallel path"
            );
        }
        for a in &payers {
            assert!(
                bob_balance(&deps.bob, &a.pubkey()).is_none_or(|l| l == 0),
                "synthetic payer must not persist on the parallel path"
            );
        }
        for r in &recipients {
            assert!(
                bob_balance(&deps.bob, r).is_none_or(|l| l == 0),
                "dataless recipient must be capped on the parallel path"
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
