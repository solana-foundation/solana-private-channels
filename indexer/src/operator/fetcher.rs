use crate::channel_utils::send_guaranteed;
use crate::config::OperatorConfig;
use crate::error::OperatorError;
use crate::metrics;
use crate::storage::common::models::{DbTransaction, TransactionType};
use crate::storage::Storage;
use crate::ProgramType;
use private_channel_metrics::{HealthState, MetricLabel};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Fetches pending transactions from the database and sends them to the processor
///
/// Uses row-level locking (FOR UPDATE SKIP LOCKED) to ensure only one operator
/// processes a transaction at a time in distributed setups
pub async fn run_fetcher(
    storage: Arc<Storage>,
    processor_tx: mpsc::Sender<DbTransaction>,
    config: OperatorConfig,
    program_type: ProgramType,
    cancellation_token: CancellationToken,
    health: Option<Arc<HealthState>>,
) -> Result<(), OperatorError> {
    info!("Starting fetcher");

    let transaction_type = match program_type {
        ProgramType::Escrow => TransactionType::Deposit,
        ProgramType::Withdraw => TransactionType::Withdrawal,
    };

    loop {
        // Check for cancellation
        if cancellation_token.is_cancelled() {
            info!("Fetcher received cancellation signal, stopping...");
            break;
        }
        match storage.count_pending_transactions(transaction_type).await {
            Ok(count) => {
                metrics::OPERATOR_BACKLOG_DEPTH
                    .with_label_values(&[program_type.as_label()])
                    .set(count as f64);
                if let Some(h) = &health {
                    h.set_pending(count as u64);
                }
            }
            Err(e) => {
                warn!(
                    "Failed to count pending transactions for backlog metric: {}",
                    e
                );
            }
        }

        match storage
            .get_and_lock_pending_transactions(transaction_type, config.batch_size as i64)
            .await
        {
            Ok(transactions) => {
                if !transactions.is_empty() {
                    info!("Fetched {} pending transactions", transactions.len());
                    metrics::OPERATOR_TRANSACTIONS_FETCHED
                        .with_label_values(&[program_type.as_label()])
                        .inc_by(transactions.len() as f64);

                    for transaction in transactions {
                        info!(
                            trace_id = %transaction.trace_id,
                            signature = %transaction.signature,
                            "Sending transaction to processor"
                        );
                        let sig = transaction.signature.clone();
                        if let Err(e) = send_guaranteed(
                            &processor_tx,
                            transaction,
                            &format!("transaction {}", sig),
                        )
                        .await
                        {
                            error!("Failed to send transaction {} to processor: {}", sig, e);
                            return Err(OperatorError::ChannelClosed {
                                component: "fetcher".to_string(),
                            });
                        }
                        if let Some(h) = &health {
                            // Forwarding a tx to the processor counts as progress —
                            // the operator pipeline is moving items along.
                            h.record_progress();
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch pending transactions: {}", e);
            }
        }

        // Sleep between polls
        tokio::time::sleep(config.db_poll_interval).await;
    }

    info!("Fetcher stopped gracefully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::models::{DbTransaction, TransactionStatus};
    use crate::storage::common::storage::mock::MockStorage;
    use chrono::Utc;
    use std::time::Duration;

    fn test_config() -> OperatorConfig {
        OperatorConfig {
            db_poll_interval: Duration::from_millis(50),
            batch_size: 10,
            retry_max_attempts: 3,
            retry_base_delay: Duration::from_millis(100),
            channel_buffer_size: 100,
            rpc_commitment: solana_sdk::commitment_config::CommitmentLevel::Confirmed,
            alert_webhook_url: None,
            reconciliation_interval: Duration::from_secs(300),
            reconciliation_tolerance_bps: 10,
            reconciliation_webhook_url: None,
            feepayer_monitor_interval: Duration::from_secs(60),
            confirmation_poll_interval_ms: 400,
        }
    }

    fn make_test_transaction(sig: &str) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id: 1,
            signature: sig.to_string(),
            trace_id: "trace-1".to_string(),
            slot: 100,
            initiator: "init".to_string(),
            recipient: "recv".to_string(),
            mint: "mint".to_string(),
            amount: 1000,
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Pending,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
        }
    }

    #[tokio::test]
    async fn cancellation_before_first_poll_exits_ok() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let (tx, _rx) = mpsc::channel(10);
        let token = CancellationToken::new();
        token.cancel(); // cancel immediately

        let result =
            run_fetcher(storage, tx, test_config(), ProgramType::Escrow, token, None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pending_transactions_sent_to_channel() {
        let mock = MockStorage::new();
        let txn = make_test_transaction("sig1");
        mock.pending_transactions.lock().unwrap().push(txn);

        let storage = Arc::new(Storage::Mock(mock));
        let (tx, mut rx) = mpsc::channel(10);
        let token = CancellationToken::new();

        let token_clone = token.clone();
        let handle = tokio::spawn(async move {
            run_fetcher(
                storage,
                tx,
                test_config(),
                ProgramType::Escrow,
                token_clone,
                None,
            )
            .await
        });

        // Wait for the transaction to come through
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for transaction")
            .expect("channel closed");
        assert_eq!(received.signature, "sig1");

        token.cancel();
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn channel_closed_returns_error() {
        let mock = MockStorage::new();
        let txn = make_test_transaction("sig2");
        mock.pending_transactions.lock().unwrap().push(txn);

        let storage = Arc::new(Storage::Mock(mock));
        let (tx, rx) = mpsc::channel(10);
        let token = CancellationToken::new();

        drop(rx); // close receiver

        let result =
            run_fetcher(storage, tx, test_config(), ProgramType::Escrow, token, None).await;

        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(err_str.contains("Channel closed"), "got: {}", err_str);
    }
}
