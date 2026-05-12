use {
    crate::{
        accounts::{
            postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::BlockInfo,
            write_batch::AddressSignatureRow, AccountsDB,
        },
        nodes::node::WorkerHandle,
        stage_metrics::SharedMetrics,
    },
    anyhow::{anyhow, Context, Result},
    redis::AsyncCommands,
    solana_hash::Hash,
    solana_rpc_client_types::response::RpcPerfSample,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        pubkey::Pubkey,
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

/// A single account that has been settled
/// We need to track if the account was deleted so we can tombstone it
/// in the accounts database
pub struct AccountSettlement {
    pub account: AccountSharedData,
    pub deleted: bool,
}

struct SettleResult {
    slot: u64,
    blockhash: Hash,
    account_settlements: Vec<(Pubkey, AccountSettlement)>,
}

#[derive(Clone)]
struct LastBlock {
    slot: u64,
    blockhash: Hash,
}

/// Warm the Redis cache by reading from Postgres and writing to Redis
/// This is called on startup to ensure Redis has the latest state from Postgres
pub async fn warm_redis_cache(
    postgres_db: &PostgresAccountsDB,
    redis_db: &RedisAccountsDB,
) -> Result<()> {
    info!("Warming Redis cache from Postgres...");

    // Read latest_slot from Postgres
    let pool = postgres_db.pool.clone();
    let slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(pool.as_ref())
        .await
        .context("Failed to query latest slot from Postgres")?;

    if let Some(slot_value) = slot {
        let slot_u64 = slot_value as u64;

        // Write latest_slot to Redis
        let mut conn = redis_db.connection.clone();
        conn.set::<_, _, ()>("latest_slot", slot_u64)
            .await
            .map_err(|e| anyhow!("Failed to write latest_slot to Redis: {}", e))?;

        info!("Warmed Redis cache: latest_slot = {}", slot_u64);
    } else {
        warn!("No blocks found in Postgres - skipping latest_slot cache warming");
    }

    // Read latest_blockhash from Postgres
    let blockhash_bytes: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT value FROM metadata WHERE key = 'latest_blockhash'")
            .fetch_optional(pool.as_ref())
            .await
            .context("Failed to query latest blockhash from Postgres")?;

    if let Some(bytes) = blockhash_bytes {
        // Convert bytes to Hash and then to string for Redis storage
        let hash_array: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("Invalid blockhash bytes length: {}", bytes.len()))?;
        let hash = Hash::new_from_array(hash_array);
        let hash_str = hash.to_string();

        // Write latest_blockhash to Redis
        let mut conn = redis_db.connection.clone();
        conn.set::<_, _, ()>("latest_blockhash", hash_str.clone())
            .await
            .map_err(|e| anyhow!("Failed to write latest_blockhash to Redis: {}", e))?;

        info!("Warmed Redis cache: latest_blockhash = {}", hash_str);
    } else {
        warn!("No blockhash found in Postgres metadata - skipping latest_blockhash cache warming");
    }

    info!("Redis cache warming completed successfully");
    Ok(())
}

pub struct SettleArgs {
    pub execution_results_rx: mpsc::UnboundedReceiver<(
        LoadAndExecuteSanitizedTransactionsOutput,
        Vec<SanitizedTransaction>,
    )>,
    pub settled_accounts_tx: mpsc::UnboundedSender<Vec<(Pubkey, AccountSettlement)>>,
    pub settled_blockhashes_tx: mpsc::UnboundedSender<Hash>,
    /// Bounded channel to the background `address_index_writer`.
    pub address_signatures_tx: mpsc::Sender<Vec<AddressSignatureRow>>,
    pub accountsdb_connection_url: String,
    pub blocktime_ms: u64,
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
        blocktime_ms,
        perf_sample_period_secs,
        shutdown_token,
        metrics,
        heartbeat,
    } = args;
    let handle = tokio::spawn(async move {
        #[allow(clippy::too_many_arguments)]
        async fn run_settle_worker(
            mut execution_results_rx: mpsc::UnboundedReceiver<(
                LoadAndExecuteSanitizedTransactionsOutput,
                Vec<SanitizedTransaction>,
            )>,
            settled_accounts_tx: mpsc::UnboundedSender<Vec<(Pubkey, AccountSettlement)>>,
            settled_blockhashes_tx: mpsc::UnboundedSender<Hash>,
            address_signatures_tx: mpsc::Sender<Vec<AddressSignatureRow>>,
            accountsdb_connection_url: String,
            blocktime_ms: u64,
            perf_sample_period_secs: u64,
            shutdown_token: CancellationToken,
            metrics: SharedMetrics,
            heartbeat: Arc<crate::health::StageHeartbeat>,
        ) -> anyhow::Result<()> {
            info!("Settle worker started");

            let mut accounts_db = AccountsDB::new(&accountsdb_connection_url, false)
                .await
                .unwrap();

            let mut redis_db: Option<RedisAccountsDB> = match std::env::var("REDIS_URL") {
                Ok(redis_url) => {
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        RedisAccountsDB::new(&redis_url),
                    )
                    .await
                    {
                        Ok(Ok(r)) => {
                            info!("Redis cache enabled");
                            Some(r)
                        }
                        Ok(Err(e)) => {
                            warn!("Redis unavailable ({}), running Postgres-only", e);
                            None
                        }
                        Err(_) => {
                            warn!("Redis connection timed out, running Postgres-only");
                            None
                        }
                    }
                }
                Err(_) => {
                    info!("REDIS_URL not set, running Postgres-only");
                    None
                }
            };

            // Warm Redis cache from Postgres on startup
            if let (AccountsDB::Postgres(ref pg), Some(ref redis)) = (&accounts_db, &redis_db) {
                if let Err(e) = warm_redis_cache(pg, redis).await {
                    warn!("Cache warming failed (non-fatal): {}", e);
                }
            }

            let last_slot = accounts_db.get_latest_slot().await.ok().flatten();
            let last_blockhash = accounts_db.get_latest_blockhash().await.ok();

            // Validate that last_slot and last_blockhash are both present or both absent
            match (last_slot, last_blockhash) {
                (Some(_), None) => {
                    anyhow::bail!("Invalid state: last_slot exists but last_blockhash is missing");
                }
                (None, Some(_)) => {
                    anyhow::bail!("Invalid state: last_blockhash exists but last_slot is missing");
                }
                _ => {}
            }

            let mut last_block = match (last_slot, last_blockhash) {
                (Some(last_slot), Some(last_blockhash)) => Some(LastBlock {
                    slot: last_slot,
                    blockhash: last_blockhash,
                }),
                _ => None,
            };
            let mut processing_results = Vec::new();

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

            loop {
                // `biased` keeps block cadence crisp under sustained load:
                // shutdown is handled promptly, the blocktime tick is polled
                // before the (almost-always-ready) result-buffer arm so a
                // tick is never delayed by an arbitrary number of recvs, and
                // MissedTickBehavior::Delay won't slide the schedule out.
                tokio::select! {
                    biased;

                    // Handle shutdown signal
                    _ = shutdown_token.cancelled() => {
                        info!("Settle worker received shutdown signal");
                        break;
                    }

                    // Blocktime tick: unconditionally produce a block with
                    // whatever has accumulated since the last tick.
                    _ = blocktime_interval.tick() => {
                        let num_results = processing_results.len();
                        match settle_transactions(
                            last_block.clone(),
                            &mut accounts_db,
                            redis_db.as_mut(),
                            &processing_results,
                            &metrics,
                            Some(&address_signatures_tx),
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
                                });
                                processing_results.clear();
                                debug!(
                                    "Settled {} transactions in slot {}, blockhash {}",
                                    num_results,
                                    settle_result.slot,
                                    settle_result.blockhash
                                );
                                if let Err(e) =
                                    settled_accounts_tx.send(settle_result.account_settlements)
                                {
                                    warn!("Failed to send settled accounts: {:?}", e);
                                    break;
                                }
                                if let Err(e) =
                                    settled_blockhashes_tx.send(settle_result.blockhash)
                                {
                                    warn!("Failed to send settled blockhashes: {:?}", e);
                                    break;
                                }
                            }
                            Err(_) => {
                                error!("Failed to settle transactions");
                                break;
                            }
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

                    // Accumulate execution results — never flush here, just buffer.
                    result = execution_results_rx.recv() => {
                        match result {
                            Some((svm_output, transactions)) => {
                                heartbeat.record_input();
                                debug!("Settle worker received output with {} transactions", transactions.len());
                                if svm_output.processing_results.len() != transactions.len() {
                                    error!("Processing results and transactions length mismatch");
                                    break;
                                }
                                debug!("Extending {} processing results", svm_output.processing_results.len());
                                processing_results.extend(svm_output.processing_results.into_iter().zip(transactions.into_iter()));
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
                match settle_transactions(
                    last_block.clone(),
                    &mut accounts_db,
                    redis_db.as_mut(),
                    &processing_results,
                    &metrics,
                    Some(&address_signatures_tx),
                )
                .await
                {
                    Ok(settle_result) => {
                        if num_results > 0 {
                            metrics.settler_txs_settled(num_results);
                        }
                        let _ = settled_accounts_tx.send(settle_result.account_settlements);
                        let _ = settled_blockhashes_tx.send(settle_result.blockhash);
                        info!(
                            "Final flush settled {} buffered transactions in slot {}",
                            num_results, settle_result.slot
                        );
                    }
                    Err(e) => {
                        warn!("Final flush failed (buffered txs lost): {:?}", e);
                    }
                }
                processing_results.clear();
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
            blocktime_ms,
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

/// Settle transactions: Update accounts database with changes
async fn settle_transactions(
    last_block: Option<LastBlock>,
    accounts_db: &mut AccountsDB,
    redis_db: Option<&mut RedisAccountsDB>,
    processing_results: &[(TransactionProcessingResult, SanitizedTransaction)],
    metrics: &crate::stage_metrics::SharedMetrics,
    address_signatures_tx: Option<&mpsc::Sender<Vec<AddressSignatureRow>>>,
) -> Result<SettleResult, Box<dyn std::error::Error>> {
    let t_total = tokio::time::Instant::now();
    // Preallocate per-tick collections from the known result count so the hot
    // path doesn't pay the geometric-growth realloc tax on every tick. The 4×
    // hint absorbs SPL/ATA-creation flows where a single tx can write to up to
    // four accounts; transfers stay well under the load factor.
    let n = processing_results.len();
    let mut final_accounts_actual: HashMap<Pubkey, AccountSettlement> =
        HashMap::with_capacity(n * 4);

    // Determine block time
    let block_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    // Generate blockhash and determine next slot
    // TODO: Check the blockhash generation scheme
    let (next_blockhash, next_slot, last_blockhash, last_slot) =
        if let Some(ref last_block) = last_block {
            let mut hash_bytes = [0u8; 32];
            hash_bytes[0..8].copy_from_slice(&last_block.slot.to_le_bytes());
            hash_bytes[8..16].copy_from_slice(&block_time.to_le_bytes());
            let next_blockhash = Hash::new_from_array(hash_bytes);
            let next_slot = last_block.slot + 1;
            (
                next_blockhash,
                next_slot,
                last_block.blockhash,
                last_block.slot,
            )
        } else {
            (Hash::default(), 0, Hash::default(), 0)
        };

    // Phase 1: build account maps and transaction lists
    let t_processing_start = tokio::time::Instant::now();
    let mut block_transaction_signatures = Vec::with_capacity(n);
    let mut block_transaction_recent_blockhashes = Vec::with_capacity(n);
    let mut transactions_for_db = Vec::with_capacity(n);

    for (processing_result, sanitized_transaction) in processing_results.iter() {
        let signature = sanitized_transaction.signature();
        let recent_blockhash = *sanitized_transaction.message().recent_blockhash();

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

                for (index, (pubkey, account_data)) in
                    executed_tx.loaded_transaction.accounts.iter().enumerate()
                {
                    if sanitized_transaction.is_writable(index) {
                        let deleted =
                            account_data.lamports() == 0 && account_data.data().is_empty();
                        final_accounts_actual.insert(
                            *pubkey,
                            AccountSettlement {
                                account: account_data.clone(),
                                deleted,
                            },
                        );
                    }
                }

                block_transaction_signatures.push(*signature);
                block_transaction_recent_blockhashes.push(recent_blockhash);
            }
            Ok(ProcessedTransaction::FeesOnly(fees_only_transaction)) => {
                warn!("FeesOnly transaction: {:?}", fees_only_transaction);

                // For fees-only transactions, we just record the transaction
                // The rollback accounts have already been handled by SVM
                // and fees have been deducted

                block_transaction_signatures.push(*signature);
                block_transaction_recent_blockhashes.push(recent_blockhash);
            }
            Err(e) => {
                warn!("Transaction failed: {:?}, error: {:?}", signature, e);
                // Failed transactions still get recorded
                block_transaction_signatures.push(*signature);
                block_transaction_recent_blockhashes.push(recent_blockhash);
            }
        }
    }

    let t_processing_ms = t_processing_start.elapsed().as_secs_f64() * 1000.0;

    // Convert final_accounts to Vec for batch write
    let accounts_vec: Vec<(Pubkey, AccountSettlement)> =
        final_accounts_actual.into_iter().collect();

    // Create block info
    let block_info = BlockInfo {
        slot: next_slot,
        blockhash: next_blockhash,
        previous_blockhash: last_blockhash,
        parent_slot: last_slot,
        // TODO: Do we need this?
        block_height: Some(next_slot),
        block_time: Some(block_time),
        transaction_signatures: block_transaction_signatures,
        transaction_recent_blockhashes: block_transaction_recent_blockhashes,
    };

    // Phase 2: Postgres write (source of truth, fatal on failure)
    let t_db_start = tokio::time::Instant::now();
    let addr_sig_rows = accounts_db
        .write_batch(
            &accounts_vec,
            transactions_for_db.clone(),
            Some(block_info.clone()),
        )
        .await?;
    let t_db_ms = t_db_start.elapsed().as_secs_f64() * 1000.0;

    // Send-after-commit: address_signatures rows are durable in
    // `transactions` already; the index writer fills in the read view with
    // an eventually-consistent gap of <1 writer-flush interval. .send().await
    // applies backpressure when the bounded channel fills.
    //
    // A closed channel (writer task exited) is logged and tolerated: the
    // atomic commit has already succeeded, so the only consequence is that
    // `address_signatures` is missing this tick's entries. That's the same
    // eventually-consistent contract `getSignaturesForAddress` already
    // tolerates — not a reason to tear down the settler.
    if let Some(tx) = address_signatures_tx {
        if !addr_sig_rows.is_empty() {
            let send_t0 = tokio::time::Instant::now();
            match tx.send(addr_sig_rows).await {
                Ok(()) => {
                    metrics.address_signatures_send_blocked_ms(
                        send_t0.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                Err(_) => {
                    warn!(
                        "address_signatures writer dropped; index entries for this tick will not be written"
                    );
                }
            }
        }
    }

    // Phase 3: Redis write best-effort (non-fatal)
    let t_redis_start = tokio::time::Instant::now();
    if let Some(redis) = redis_db {
        if let Err(e) = crate::accounts::write_batch::write_batch_redis(
            redis,
            &accounts_vec,
            transactions_for_db,
            Some(block_info),
        )
        .await
        {
            warn!(
                "Best-effort Redis cache write failed (non-fatal, Postgres succeeded): {}",
                e
            );
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
        account_settlements: accounts_vec,
    })
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
        signature::{Keypair, Signer},
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_empty_results() {
        let (mut db, _pg) = start_test_postgres().await;
        let result = settle_transactions(
            None,
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await;
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.slot, 0);
        assert_eq!(r.blockhash, Hash::default());
        assert!(r.account_settlements.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_increments_slot() {
        let (mut db, _pg) = start_test_postgres().await;

        let r1 = settle_transactions(
            None,
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r1.slot, 0);

        let last = LastBlock {
            slot: r1.slot,
            blockhash: r1.blockhash,
        };
        let r2 = settle_transactions(
            Some(last),
            &mut db,
            None,
            &[],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r2.slot, 1);
        assert_ne!(r2.blockhash, Hash::default());
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
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // Should have stored a block, and the transaction signature
        let block = db.get_block(result.slot).await;
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

        let result = settle_transactions(
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // Writable accounts should be in settlements, readonly (system program) should not
        let settlement_keys: Vec<_> = result.account_settlements.iter().map(|(k, _)| *k).collect();
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

        let result = settle_transactions(
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // The deleted account should be flagged
        let settlement = result.account_settlements.iter().find(|(k, _)| k == &pk);
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

        let result = settle_transactions(
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // Failed transactions still get their signature recorded in the block
        let block = db.get_block(result.slot).await.unwrap();
        assert!(block.transaction_signatures.contains(&sig));
        // But no account settlements
        assert!(result.account_settlements.is_empty());
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
            None,
            &mut db,
            None,
            &results1,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r1.slot, 0);

        // Settle second batch, chaining from first
        let last = LastBlock {
            slot: r1.slot,
            blockhash: r1.blockhash,
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
            Some(last),
            &mut db,
            None,
            &results2,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r2.slot, 1);
        assert_ne!(r2.blockhash, r1.blockhash);

        // Both blocks should be stored
        assert!(db.get_block(0).await.is_some());
        assert!(db.get_block(1).await.is_some());
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

        let result = settle_transactions(
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // Signature should be recorded in the block
        let block = db.get_block(result.slot).await.unwrap();
        assert!(block.transaction_signatures.contains(&sig));

        // No account settlements — fees-only transactions don't modify accounts
        assert!(result.account_settlements.is_empty());
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
        let redis_db = match RedisAccountsDB::new(&redis_url).await {
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
        let result = warm_redis_cache(&postgres_db, &redis_db).await;

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
    async fn test_settle_worker_shutdown_exits_cleanly() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (_exec_tx, exec_rx) = mpsc::unbounded_channel();
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
            blocktime_ms: 100,
            perf_sample_period_secs: 60,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_secs(5), handle.handle).await;
        assert!(result.is_ok(), "settle worker should exit after shutdown");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_worker_processes_results_and_emits_settlements() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::unbounded_channel();
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
            blocktime_ms: 50, // fast for testing
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
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
        exec_tx.send((output, vec![tx])).unwrap();

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_worker_channel_closed_exits() {
        let (_db, _pg) = start_test_postgres().await;
        let url = crate::test_helpers::postgres_container_url(&_pg, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::unbounded_channel();
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
            blocktime_ms: 50,
            perf_sample_period_secs: 3600,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
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

        let results: Vec<(TransactionProcessingResult, _)> =
            vec![(Ok(executed), tx1), (Ok(fees_only), tx2), (Err(err), tx3)];

        let result = settle_transactions(
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // All three signatures should be recorded in the block
        assert_eq!(
            result.account_settlements.len(),
            1,
            "only executed tx settles accounts"
        );
        assert_eq!(
            result.blockhash,
            Hash::default(),
            "first block has default hash"
        );

        let block = db.get_block(result.slot).await.unwrap();
        assert_eq!(
            block.transaction_signatures.len(),
            3,
            "all three signatures recorded"
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
            None,
            &mut db,
            None,
            &results1,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r1.slot, 0);

        let block1 = db.get_block(0).await.unwrap();
        assert_eq!(block1.parent_slot, 0, "first block parent_slot is 0");
        assert_eq!(block1.block_height, Some(0), "first block height is 0");
        assert!(block1.block_time.is_some(), "block time is set");

        // Second block, chained from first
        let last = LastBlock {
            slot: r1.slot,
            blockhash: r1.blockhash,
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
            Some(last),
            &mut db,
            None,
            &results2,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r2.slot, 1);

        let block2 = db.get_block(1).await.unwrap();
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
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        let block = db.get_block(result.slot).await.unwrap();
        assert_eq!(
            block.transaction_signatures.len(),
            3,
            "all three signatures recorded"
        );
        // Verify signatures are in the same order as input
        assert_eq!(block.transaction_signatures[0], sig1);
        assert_eq!(block.transaction_signatures[1], sig2);
        assert_eq!(block.transaction_signatures[2], sig3);
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
        let result = settle_transactions(
            None,
            &mut db,
            None,
            &results,
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // Both writable accounts (payer and recipient) should be settled
        assert_eq!(
            result.account_settlements.len(),
            2,
            "both writable accounts settled"
        );
        let settlement_keys: Vec<_> = result.account_settlements.iter().map(|(k, _)| *k).collect();
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
        let (redis_db, _redis) = start_test_redis().await;

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
            None,
            &mut pg_db,
            None,
            &[(Ok(executed), tx)],
            &(Arc::new(NoopMetrics) as SharedMetrics),
            None,
        )
        .await
        .unwrap();

        // Get the PostgresAccountsDB variant for warm_redis_cache
        let AccountsDB::Postgres(ref pg) = pg_db else {
            panic!("Expected Postgres variant")
        };
        warm_redis_cache(pg, &redis_db).await.unwrap();

        // Verify Redis was populated
        let mut conn = redis_db.connection.clone();
        let slot: Option<u64> = conn.get("latest_slot").await.ok();
        assert_eq!(slot, Some(0), "Redis latest_slot should be 0");
        let bh: Option<String> = conn.get("latest_blockhash").await.ok();
        assert!(bh.is_some(), "Redis latest_blockhash should be set");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_warm_redis_cache_empty_postgres() {
        // Test that warm_redis_cache handles empty Postgres gracefully
        let (pg_db, _pg) = start_test_postgres().await;
        let (redis_db, _redis) = start_test_redis().await;

        let AccountsDB::Postgres(ref pg) = pg_db else {
            panic!("Expected Postgres variant")
        };
        // Should succeed without panic — empty DB is gracefully handled
        warm_redis_cache(pg, &redis_db).await.unwrap();

        // No keys should be written when Postgres is empty
        let mut conn = redis_db.connection.clone();
        let slot: Option<u64> = conn.get("latest_slot").await.ok();
        assert!(
            slot.is_none(),
            "Redis should have no slot when Postgres is empty"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_settle_worker_perf_sample_tick_fires() {
        // Test that the performance sample tick fires after perf_sample_period_secs
        // and stores a sample in the database
        let (_db, pg_container) = start_test_postgres().await;
        let url = postgres_container_url(&pg_container, "test_db").await;

        let (exec_tx, exec_rx) = mpsc::unbounded_channel();
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
            blocktime_ms: 50,
            perf_sample_period_secs: 1, // fires after 1s
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
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
        exec_tx.send((output, vec![tx])).unwrap();

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
}
