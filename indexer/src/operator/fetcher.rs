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

    let transaction_type = program_type.owned_transaction_type();
    // Start the stall clock at boot, so a backlog that never moves after a restart still shows on /health.
    if let Some(h) = &health {
        h.record_progress();
    }
    // Set while deposits wait for the escrow checkpoint, so the reason is logged once per wait.
    let mut holding = false;

    loop {
        // Check for cancellation
        if cancellation_token.is_cancelled() {
            info!("Fetcher received cancellation signal, stopping...");
            break;
        }

        // Measured before the halt gate so /health can still report a stalled backlog.
        let backlog = match storage.count_pending_transactions(transaction_type).await {
            Ok(count) => {
                metrics::OPERATOR_BACKLOG_DEPTH
                    .with_label_values(&[program_type.as_label()])
                    .set(count as f64);
                if let Some(h) = &health {
                    h.set_pending(count as u64);
                    // An empty backlog is caught up, so idle time never counts against the next row.
                    if count == 0 {
                        h.record_progress();
                    }
                }
                Some(count)
            }
            Err(e) => {
                warn!(
                    "Failed to count pending transactions for backlog metric: {}",
                    e
                );
                None
            }
        };

        // Durable cross-process freeze: a reconciliation halt stops BOTH operators'
        // fetchers here, the single point that moves rows pending -> processing.
        // An unreadable flag cannot prove the halt is clear, so it skips the poll too.
        match storage.is_reconciliation_halted().await {
            Ok(Some(halt)) => {
                warn!(reason = %halt.reason, "Reconciliation halt active; skipping fetch");
                // An outage latch stays replaceable so a later insolvency upgrade shows on /health.
                if let Some(h) = &health {
                    if halt.insolvency {
                        h.force_unhealthy(halt.reason.clone());
                    } else {
                        h.force_unhealthy_provisional(halt.reason.clone());
                    }
                }
                tokio::time::sleep(config.db_poll_interval).await;
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                // Not latched unhealthy: a DB blip must not need a restart to clear.
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[program_type.as_label(), "halt_read_error"])
                    .inc();
                warn!(
                    "Failed to read reconciliation halt flag; skipping fetch: {}",
                    e
                );
                tokio::time::sleep(config.db_poll_interval).await;
                continue;
            }
        }

        match storage
            .get_and_lock_pending_transactions(transaction_type, config.batch_size as i64)
            .await
        {
            Ok(transactions) => {
                let held = transactions.is_empty()
                    && backlog.is_some_and(|count| count > 0)
                    && transaction_type == TransactionType::Deposit;
                if held && !holding {
                    warn!(
                        backlog,
                        "Pending deposits are held until the escrow indexer checkpoint covers their slot"
                    );
                }
                holding = held;
                if !transactions.is_empty() {
                    holding = false;
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
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
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
            rpc_commitment: solana_commitment_config::CommitmentLevel::Confirmed,
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
            amount: TokenAmount(1000),
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
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
            release_refused_on_chain: false,
        }
    }

    /// Mock whose escrow checkpoint covers `make_test_transaction`'s slot, so its deposits are claimable.
    fn covered_mock() -> MockStorage {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 100);
        mock
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
        let mock = covered_mock();
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
    async fn fetcher_skips_fetch_when_halted() {
        use private_channel_metrics::{HealthConfig, HealthState};
        let mock = covered_mock();
        // A pending deposit is present, but the halt flag must stop it being fetched.
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(make_test_transaction("sig_halt"));
        mock.set_reconciliation_halt("test halt").await.unwrap();

        let storage = Arc::new(Storage::Mock(mock));
        let (tx, mut rx) = mpsc::channel(10);
        let token = CancellationToken::new();
        let health_state = HealthState::new(HealthConfig::operator());
        let token_clone = token.clone();
        let health_clone = Some(health_state.clone());
        let handle = tokio::spawn(async move {
            run_fetcher(
                storage,
                tx,
                test_config(),
                ProgramType::Escrow,
                token_clone,
                health_clone,
            )
            .await
        });

        // Nothing should be forwarded while halted.
        let got = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await;
        assert!(got.is_err(), "halted fetcher must not forward transactions");
        assert!(
            !health_state.is_healthy(),
            "halted fetcher must force itself unhealthy"
        );

        token.cancel();
        assert!(handle.await.unwrap().is_ok());
    }

    /// /health follows an outage halt that the DB later upgrades to an insolvency.
    #[tokio::test]
    async fn fetcher_health_follows_an_insolvency_upgrade() {
        use private_channel_metrics::{HealthConfig, HealthOutcome, HealthState};
        let mock = MockStorage::new();
        mock.set_outage_halt("inputs unavailable").await.unwrap();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (tx, _rx) = mpsc::channel(10);
        let token = CancellationToken::new();
        let health = HealthState::new(HealthConfig::operator());
        let token_clone = token.clone();
        let health_clone = Some(health.clone());
        let handle = tokio::spawn(async move {
            run_fetcher(
                storage,
                tx,
                test_config(),
                ProgramType::Escrow,
                token_clone,
                health_clone,
            )
            .await
        });
        let reason = |h: &HealthState| match h.check() {
            HealthOutcome::ForcedUnhealthy { reason } => Some(reason),
            _ => None,
        };

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(reason(&health).as_deref(), Some("inputs unavailable"));
        mock.set_reconciliation_halt("mint X insolvent")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(reason(&health).as_deref(), Some("mint X insolvent"));

        token.cancel();
        assert!(handle.await.unwrap().is_ok());
    }

    /// A halt flag that cannot be read cannot prove the halt is clear, so the fetcher
    /// claims nothing. It still measures the backlog so /health can report the stall.
    #[tokio::test]
    async fn fetcher_halt_read_error_skips_fetch() {
        use private_channel_metrics::HealthOutcome;
        let mock = covered_mock();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(make_test_transaction("sig_err"));
        mock.set_should_fail("is_reconciliation_halted", true);
        let health = fast_stall_health();
        let read_errors = metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[ProgramType::Escrow.as_label(), "halt_read_error"]);
        let errors_before = read_errors.get();

        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (tx, mut rx) = mpsc::channel(10);
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let health_clone = Some(health.clone());
        let handle = tokio::spawn(async move {
            run_fetcher(
                storage,
                tx,
                test_config(),
                ProgramType::Escrow,
                token_clone,
                health_clone,
            )
            .await
        });

        // Polls past the stall window while the read keeps failing.
        let got = tokio::time::timeout(Duration::from_millis(2_500), rx.recv()).await;
        assert!(
            got.is_err(),
            "halt read error must not forward transactions"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Pending,
            "nothing is claimed while the flag is unreadable"
        );
        assert!(
            read_errors.get() > errors_before,
            "each failed read is counted"
        );
        assert!(
            matches!(health.check(), HealthOutcome::Stalled { pending: 1, .. }),
            "backlog is still measured and never latched: {:?}",
            health.check()
        );

        // Once the flag reads clear again the row goes through.
        mock.set_should_fail("is_reconciliation_halted", false);
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for transaction")
            .expect("channel closed");
        assert_eq!(received.signature, "sig_err");

        token.cancel();
        assert!(handle.await.unwrap().is_ok());
    }

    /// Operator health with a one second stall window, so the tests can watch it expire.
    fn fast_stall_health() -> Arc<private_channel_metrics::HealthState> {
        private_channel_metrics::HealthState::new(private_channel_metrics::HealthConfig {
            stale_threshold_secs: 1,
            ..private_channel_metrics::HealthConfig::operator()
        })
    }

    /// A live deposit written while a gap repair still trails its slot is held Pending,
    /// not handed to the mint gate, and goes through once the checkpoint covers it.
    /// The stall clock starts at boot, so a fresh operator holding it still turns /health red.
    #[tokio::test]
    async fn fetcher_holds_deposit_until_checkpoint_covers_it() {
        use private_channel_metrics::HealthOutcome;
        let mock = MockStorage::new();
        let mut txn = make_test_transaction("sig_live");
        txn.slot = 106;
        mock.pending_transactions.lock().unwrap().push(txn);
        mock.set_checkpoint("escrow", 100);
        let health = fast_stall_health();

        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (tx, mut rx) = mpsc::channel(10);
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let health_clone = Some(health.clone());
        let handle = tokio::spawn(async move {
            run_fetcher(
                storage,
                tx,
                test_config(),
                ProgramType::Escrow,
                token_clone,
                health_clone,
            )
            .await
        });

        let got = tokio::time::timeout(Duration::from_millis(2_500), rx.recv()).await;
        assert!(got.is_err(), "an uncovered deposit must not be forwarded");
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Pending,
            "the held deposit stays Pending"
        );
        assert!(
            matches!(health.check(), HealthOutcome::Stalled { pending: 1, .. }),
            "the held deposit still counts as backlog: {:?}",
            health.check()
        );

        mock.set_checkpoint("escrow", 106);
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for transaction")
            .expect("channel closed");
        assert_eq!(received.signature, "sig_live");

        token.cancel();
        assert!(handle.await.unwrap().is_ok());
    }

    /// An idle stretch is not a stall: a deposit that arrives after it starts a fresh window.
    #[tokio::test]
    async fn fetcher_idle_time_does_not_count_against_a_new_deposit() {
        use private_channel_metrics::HealthOutcome;
        let mock = covered_mock();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(make_test_transaction("sig_first"));
        let health = fast_stall_health();

        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (tx, mut rx) = mpsc::channel(10);
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let health_clone = Some(health.clone());
        let handle = tokio::spawn(async move {
            run_fetcher(
                storage,
                tx,
                test_config(),
                ProgramType::Escrow,
                token_clone,
                health_clone,
            )
            .await
        });

        let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for transaction")
            .expect("channel closed");
        assert_eq!(first.signature, "sig_first");

        // Idle well past the stall window, then a deposit arrives that must wait for the checkpoint.
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        let mut txn = make_test_transaction("sig_after_idle");
        txn.slot = 106;
        mock.pending_transactions.lock().unwrap().push(txn);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            health.check(),
            HealthOutcome::Healthy,
            "a deposit held for a moment after idle is not a stall"
        );

        token.cancel();
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn channel_closed_returns_error() {
        let mock = covered_mock();
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
