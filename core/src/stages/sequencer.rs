use {
    crate::{
        health::StageHeartbeat,
        nodes::node::WorkerHandle,
        scheduler::{ConflictFreeBatch, Scheduler, SchedulerTrait},
        stage_metrics::SharedMetrics,
    },
    solana_sdk::transaction::SanitizedTransaction,
    std::{sync::Arc, time::Duration},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

pub struct SequencerArgs {
    pub max_tx_per_batch: usize,
    pub batch_deadline_ms: u64,
    pub rx: mpsc::Receiver<SanitizedTransaction>,
    pub batch_tx: mpsc::Sender<ConflictFreeBatch>,
    pub shutdown_token: CancellationToken,
    pub metrics: SharedMetrics,
    pub heartbeat: Arc<StageHeartbeat>,
}

pub async fn start_sequence_worker(args: SequencerArgs) -> WorkerHandle {
    let SequencerArgs {
        max_tx_per_batch,
        batch_deadline_ms,
        mut rx,
        batch_tx,
        shutdown_token,
        metrics,
        heartbeat,
    } = args;
    let handle = tokio::spawn(async move {
        info!(
            "Sequencer started with max_tx_per_batch: {}, batch_deadline_ms: {}",
            max_tx_per_batch, batch_deadline_ms
        );

        let mut scheduler = Scheduler::new_dag();
        let mut pending_transactions = Vec::new();
        let mut total_batches_sent = 0u64;

        loop {
            // Collect transactions up to max_tx_per_batch or until channel is empty
            let mut collected = 0;

            // First, try to get at least one transaction (blocking)
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Some(transaction) => {
                            heartbeat.record_input();
                            debug!("Sequencer received transaction: {}", transaction.signature());
                            pending_transactions.push(transaction);
                            collected += 1;
                        }
                        None => {
                            // Channel closed - flush any remaining with try_send (non-blocking)
                            // to avoid blocking on a full channel when the executor is also exiting.
                            if !pending_transactions.is_empty() {
                                metrics.sequencer_collected(pending_transactions.len());
                                let sent = flush_batches_nonblocking(
                                    &mut scheduler,
                                    &pending_transactions,
                                    &batch_tx,
                                    &metrics,
                                );
                                total_batches_sent += sent;
                            }
                            info!("Sequencer stopped - channel closed, sent {} total batches", total_batches_sent);
                            return;
                        }
                    }
                }

                _ = shutdown_token.cancelled() => {
                    // Flush remaining with try_send (non-blocking) so shutdown completes
                    // promptly even if the output channel is full.
                    if !pending_transactions.is_empty() {
                        metrics.sequencer_collected(pending_transactions.len());
                        let sent = flush_batches_nonblocking(
                            &mut scheduler,
                            &pending_transactions,
                            &batch_tx,
                            &metrics,
                        );
                        total_batches_sent += sent;
                    }
                    info!("Sequencer received shutdown signal, sent {} total batches", total_batches_sent);
                    return;
                }
            }

            // Collect more transactions up to the batch limit.
            // With deadline: wait up to batch_deadline_ms for more txs before dispatching.
            // With no deadline (batch_deadline_ms == 0): drain non-blocking, dispatch immediately.
            if batch_deadline_ms > 0 {
                let deadline = tokio::time::sleep(Duration::from_millis(batch_deadline_ms));
                tokio::pin!(deadline);
                while pending_transactions.len() < max_tx_per_batch {
                    tokio::select! {
                        biased;
                        result = rx.recv() => {
                            match result {
                                Some(tx) => {
                                    debug!("Sequencer received transaction: {}", tx.signature());
                                    pending_transactions.push(tx);
                                    collected += 1;
                                }
                                None => break, // channel closed, flush what we have
                            }
                        }
                        _ = &mut deadline => {
                            debug!("Batch deadline reached after collecting {} transactions", collected);
                            break;
                        }
                    }
                }
            } else {
                // Original non-blocking drain: dispatch immediately when channel is empty
                while collected < max_tx_per_batch {
                    match rx.try_recv() {
                        Ok(transaction) => {
                            debug!(
                                "Sequencer received transaction: {}",
                                transaction.signature()
                            );
                            pending_transactions.push(transaction);
                            collected += 1;
                        }
                        Err(_) => {
                            // Channel is empty (but not closed)
                            debug!("Channel empty after collecting {} transactions", collected);
                            break;
                        }
                    }
                }
            }

            if collected >= max_tx_per_batch {
                debug!("Reached max_tx_per_batch limit: {}", max_tx_per_batch);
            }

            // Process the collected transactions into conflict-free batches
            if !pending_transactions.is_empty() {
                metrics.sequencer_collected(pending_transactions.len());
                let sent = process_and_send_batches(
                    &mut scheduler,
                    &pending_transactions,
                    &batch_tx,
                    &metrics,
                )
                .await;
                if sent > 0 {
                    heartbeat.record_progress();
                }
                total_batches_sent += sent;
                pending_transactions.clear();

                if total_batches_sent.is_multiple_of(100) && total_batches_sent > 0 {
                    info!("Sequencer has sent {} total batches", total_batches_sent);
                }
            }
        }
    });

    WorkerHandle::new("Sequencer".to_string(), handle)
}

/// Non-blocking flush used during shutdown / channel-closed paths.
/// Uses `try_send` so we never block on a full channel when the executor is also exiting.
/// Batches that can't fit are dropped (transactions will be lost), which is acceptable
/// because the node is already stopping and clients will time out and retry.
fn flush_batches_nonblocking(
    scheduler: &mut Scheduler,
    transactions: &[SanitizedTransaction],
    batch_tx: &mpsc::Sender<ConflictFreeBatch>,
    metrics: &SharedMetrics,
) -> u64 {
    let conflict_free_batches = scheduler.schedule(transactions.to_vec());
    let num_transactions = transactions.len();
    if num_transactions > 0 {
        metrics.sequencer_transactions_emitted(num_transactions);
    }
    let mut batches_sent = 0u64;
    let mut dropped_batches = 0u64;
    let mut dropped_txs = 0usize;
    for batch in conflict_free_batches {
        match batch_tx.try_send(batch) {
            Ok(_) => batches_sent += 1,
            Err(e) => {
                let reason = if batch_tx.is_closed() {
                    "channel closed"
                } else {
                    "channel full"
                };
                let n = e.into_inner().transactions.len();
                warn!(
                    "Sequencer flush dropped batch of {} transactions during shutdown ({})",
                    n, reason
                );
                dropped_batches += 1;
                dropped_txs += n;
            }
        }
    }
    if dropped_batches > 0 {
        warn!(
            "Sequencer flush dropped {} batches ({} transactions) during shutdown",
            dropped_batches, dropped_txs
        );
    }
    batches_sent
}

/// Visible to tests in this crate.
async fn process_and_send_batches(
    scheduler: &mut Scheduler,
    transactions: &[SanitizedTransaction],
    batch_tx: &mpsc::Sender<ConflictFreeBatch>,
    metrics: &SharedMetrics,
) -> u64 {
    let num_transactions = transactions.len();
    debug!(
        "Processing {} transactions into conflict-free batches",
        num_transactions
    );

    // Schedule transactions to create conflict-free batches
    let conflict_free_batches = scheduler.schedule(transactions.to_vec());
    let num_batches = conflict_free_batches.len();

    if num_transactions > 0 {
        metrics.sequencer_transactions_emitted(num_transactions);
    }

    debug!(
        "Created {} conflict-free batches from {} transactions",
        num_batches, num_transactions
    );

    let mut batches_sent = 0u64;

    // Send each conflict-free batch to the executor
    for (idx, batch) in conflict_free_batches.into_iter().enumerate() {
        let batch_size = batch.transactions.len();
        debug!(
            "Sending conflict-free batch {} with {} transactions",
            idx, batch_size
        );

        match batch_tx.send(batch).await {
            Ok(_) => {
                debug!("Batch {} sent successfully", idx);
                batches_sent += 1;
            }
            Err(_) => {
                warn!("Failed to send batch {} - channel closed", idx);
                break;
            }
        }
    }

    batches_sent
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{stage_metrics::NoopMetrics, test_helpers::create_test_sanitized_transaction};
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Keypair;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    use crate::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY as SEQ_CAP;

    #[tokio::test]
    async fn test_single_tx_produces_batch() {
        let mut scheduler = Scheduler::new_dag();
        let (batch_tx, mut batch_rx) = mpsc::channel(16);

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let sent = process_and_send_batches(&mut scheduler, &[tx], &batch_tx, &noop).await;
        assert!(sent >= 1);

        // Should have received at least one batch
        let batch = batch_rx.try_recv();
        assert!(batch.is_ok());
        assert!(!batch.unwrap().transactions.is_empty());
    }

    #[tokio::test]
    async fn test_empty_no_batches() {
        let mut scheduler = Scheduler::new_dag();
        let (batch_tx, mut batch_rx) = mpsc::channel(16);

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let sent = process_and_send_batches(&mut scheduler, &[], &batch_tx, &noop).await;
        assert_eq!(sent, 0);
        assert!(batch_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_channel_closed_partial() {
        let mut scheduler = Scheduler::new_dag();
        let (batch_tx, batch_rx) = mpsc::channel(16);

        // Drop the receiver so sends will fail
        drop(batch_rx);

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);

        // Should not panic, just return 0 since channel is closed
        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let sent = process_and_send_batches(&mut scheduler, &[tx], &batch_tx, &noop).await;
        assert_eq!(sent, 0);
    }

    // Conflicting txs (same payer = write conflict) are split across separate conflict-free batches.
    #[tokio::test]
    async fn test_multiple_txs_produce_multiple_batches() {
        // When transactions conflict they are split into separate batches.
        // Use the same payer (write conflict on fee payer account).
        let mut scheduler = Scheduler::new_dag();
        let (batch_tx, mut batch_rx) = mpsc::channel(16);

        let payer = Keypair::new();
        let to1 = Pubkey::new_unique();
        let to2 = Pubkey::new_unique();
        let tx1 = create_test_sanitized_transaction(&payer, &to1, 100);
        let tx2 = create_test_sanitized_transaction(&payer, &to2, 200);

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let sent = process_and_send_batches(&mut scheduler, &[tx1, tx2], &batch_tx, &noop).await;
        // Conflicting transactions should be split into separate batches
        assert_eq!(
            sent, 2,
            "Two conflicting txs should produce two separate batches"
        );

        // Verify first batch received
        let batch1 = batch_rx.try_recv();
        assert!(batch1.is_ok(), "First batch should be received");
        assert_eq!(
            batch1.unwrap().transactions.len(),
            1,
            "First batch should contain one transaction"
        );

        // Verify second batch received
        let batch2 = batch_rx.try_recv();
        assert!(batch2.is_ok(), "Second batch should be received");
        assert_eq!(
            batch2.unwrap().transactions.len(),
            1,
            "Second batch should contain one transaction"
        );
    }

    // Txs with no shared accounts are eligible to be placed in the same batch.
    #[tokio::test]
    async fn test_non_conflicting_txs_may_share_batch() {
        // Transactions with no shared accounts can be in the same batch.
        // Different payers and recipients = no conflicts = can share batch.
        let mut scheduler = Scheduler::new_dag();
        let (batch_tx, mut batch_rx) = mpsc::channel(16);

        let from1 = Keypair::new();
        let from2 = Keypair::new();
        let to1 = Pubkey::new_unique();
        let to2 = Pubkey::new_unique();
        let tx1 = create_test_sanitized_transaction(&from1, &to1, 100);
        let tx2 = create_test_sanitized_transaction(&from2, &to2, 200);

        let noop: SharedMetrics = Arc::new(NoopMetrics);
        let sent = process_and_send_batches(&mut scheduler, &[tx1, tx2], &batch_tx, &noop).await;
        assert_eq!(
            sent, 1,
            "Non-conflicting txs should be grouped into one batch"
        );

        // Verify the batch contains both transactions
        let batch = batch_rx.try_recv();
        assert!(batch.is_ok(), "One batch should be received");
        assert_eq!(
            batch.unwrap().transactions.len(),
            2,
            "Batch should contain both non-conflicting transactions"
        );
    }

    // ---- start_sequence_worker tests ----

    // Closing the input channel with a pending tx causes the worker to flush it then exit.
    #[tokio::test]
    async fn worker_channel_closed_flushes_pending_and_exits() {
        let (input_tx, input_rx) = mpsc::channel(SEQ_CAP);
        let (batch_tx, mut batch_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        input_tx
            .send(create_test_sanitized_transaction(&from, &to, 100))
            .await
            .unwrap();
        drop(input_tx); // close the channel with a pending tx

        let _handle = start_sequence_worker(SequencerArgs {
            max_tx_per_batch: 64,
            batch_deadline_ms: 0,
            rx: input_rx,
            batch_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Worker should receive the pending tx, process it, then exit
        let result = tokio::time::timeout(Duration::from_millis(300), batch_rx.recv()).await;
        assert!(
            result.is_ok(),
            "batch should arrive before channel-close exit"
        );
        shutdown.cancel();
    }

    // Cancelling the shutdown token stops the worker without deadlock or panic.
    #[tokio::test]
    async fn worker_shutdown_signal_exits_cleanly() {
        let (input_tx, input_rx) = mpsc::channel(SEQ_CAP);
        let (batch_tx, mut batch_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let _handle = start_sequence_worker(SequencerArgs {
            max_tx_per_batch: 64,
            batch_deadline_ms: 0,
            rx: input_rx,
            batch_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Send a tx so the worker has something to flush on shutdown
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        input_tx
            .send(create_test_sanitized_transaction(&from, &to, 100))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();

        // The batch emitted before or at shutdown should be receivable
        let _ = tokio::time::timeout(Duration::from_millis(200), batch_rx.recv()).await;
        // No panic or deadlock is the primary assertion here
        drop(input_tx);
    }

    // The worker's non-blocking drain loop stops collecting once max_tx_per_batch is reached.
    #[tokio::test]
    async fn worker_collects_up_to_max_tx_per_batch() {
        let (input_tx, input_rx) = mpsc::channel(SEQ_CAP);
        let (batch_tx, mut batch_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let max = 3usize;
        let num_to_send = max * 2; // 6 items, more than max (3)

        // Pre-fill with more than max transactions so the non-blocking loop
        // hits the limit and breaks.
        for _ in 0..num_to_send {
            let from = Keypair::new();
            let to = Pubkey::new_unique();
            input_tx
                .send(create_test_sanitized_transaction(&from, &to, 100))
                .await
                .unwrap();
        }

        let _handle = start_sequence_worker(SequencerArgs {
            max_tx_per_batch: max,
            batch_deadline_ms: 0,
            rx: input_rx,
            batch_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Use timeout + recv instead of sleep + try_recv for determinism
        let result = tokio::time::timeout(Duration::from_millis(500), batch_rx.recv()).await;
        assert!(result.is_ok(), "expected at least one batch within timeout");
        let batch = result.unwrap().expect("channel should not be closed");
        assert_eq!(
            batch.transactions.len(),
            max,
            "Batch should contain exactly max_tx_per_batch ({}) transactions",
            max
        );
        shutdown.cancel();
    }
}
