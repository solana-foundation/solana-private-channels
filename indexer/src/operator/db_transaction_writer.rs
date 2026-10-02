use crate::config::ProgramType;
use crate::error::StorageError;
use crate::metrics;
use crate::operator::sender::TransactionStatusUpdate;
use crate::storage::common::models::TransactionStatus;
use crate::storage::Storage;
use chrono::Utc;
use private_channel_core::webhook::{WebhookClient, WebhookRetryConfig};
use private_channel_metrics::MetricLabel;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};
use tracing::{error, info, warn};

/// Alerts waiting for the webhook; past this an alert is dropped.
const ALERT_QUEUE_CAPACITY: usize = 1024;

/// How long a stopped writer keeps posting what is still queued.
const ALERT_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// DbTransactionWriter that receives transaction status updates from sender
/// and writes them to the database
pub struct DbTransactionWriter {
    storage: Arc<Storage>,
    update_rx: mpsc::Receiver<TransactionStatusUpdate>,
    webhook_client: WebhookClient,
    webhook_url: Option<String>,
    program_type: ProgramType,
    alert_tx: mpsc::Sender<TransactionStatusUpdate>,
    /// Taken by `start`, which drains it beside the writes.
    alert_rx: Option<mpsc::Receiver<TransactionStatusUpdate>>,
}

impl DbTransactionWriter {
    pub fn new(
        storage: Arc<Storage>,
        update_rx: mpsc::Receiver<TransactionStatusUpdate>,
        webhook_url: Option<String>,
        program_type: ProgramType,
    ) -> Self {
        let webhook_client = WebhookClient::new(
            Duration::from_secs(10),
            WebhookRetryConfig::single_attempt(),
        )
        .expect("Failed to build webhook HTTP client");
        let (alert_tx, alert_rx) = mpsc::channel(ALERT_QUEUE_CAPACITY);
        Self {
            storage,
            update_rx,
            webhook_client,
            webhook_url,
            program_type,
            alert_tx,
            alert_rx: Some(alert_rx),
        }
    }

    /// Start processing status updates from the channel
    pub async fn start(mut self) -> Result<(), StorageError> {
        info!("Starting StorageWriter");

        // Delivery runs beside the writes in this task, so a slow endpoint never
        // delays a status write and an abort stops both.
        let deliveries = Self::deliver_alerts(
            self.webhook_client.clone(),
            self.webhook_url.clone(),
            self.alert_rx.take(),
            self.program_type,
        );
        let writes = async move {
            while let Some(update) = self.update_rx.recv().await {
                self.handle_update(update).await;
            }
            // Logged before any drain timeout, so it survives a detached shutdown.
            let queued = self.alert_tx.max_capacity() - self.alert_tx.capacity();
            if queued > 0 {
                warn!(
                    queued,
                    "Writer stopped with alerts still queued; draining them"
                );
            }
            // Dropping the writer here closes the alert queue, which starts the bounded drain.
        };
        tokio::join!(writes, deliveries);

        info!("StorageWriter stopped");
        Ok(())
    }

    /// Handle a single transaction status update
    async fn handle_update(&self, update: TransactionStatusUpdate) {
        let is_alertable = matches!(
            update.status,
            TransactionStatus::Failed
                | TransactionStatus::FailedReminted
                | TransactionStatus::ManualReview
        );
        let trace_id = update.trace_id.as_deref().unwrap_or("none");
        let pt = self.program_type.as_label();
        // Already written under its incarnation's fence. The row may have been
        // re-armed and claimed again since, so this one only alerts.
        if !update.alert_only {
            match self
                .storage
                .update_transaction_status(
                    update.transaction_id,
                    update.status,
                    update.counterpart_signature.clone(),
                    update.processed_at.unwrap_or_else(Utc::now),
                    // The release_signatures column exists and is migrated, but nothing
                    // reads it yet. It is provenance only, and adopting it means a new
                    // field on every TransactionStatusUpdate, so it is deferred, not dropped.
                    None,
                )
                .await
            {
                Ok(true) => {
                    info!(
                        trace_id = trace_id,
                        "Updated transaction {} to status {:?}",
                        update.transaction_id,
                        update.status
                    );
                    metrics::OPERATOR_DB_UPDATES
                        .with_label_values(&[pt, &format!("{:?}", update.status)])
                        .inc();
                }
                // A skipped Completed is unrecoverable: the release landed on chain and
                // validate_bitmap_consistency refuses the next boot once Completed rows
                // and bitmap bits diverge. Every other status is routine recovery churn.
                Ok(false) if update.status == TransactionStatus::Completed => {
                    error!(
                        trace_id = trace_id,
                        "Transaction {} already past Processing; Completed status write LOST",
                        update.transaction_id
                    );
                    metrics::OPERATOR_DB_UPDATE_SKIPPED
                        .with_label_values(&[pt, &format!("{:?}", update.status)])
                        .inc();
                }
                Ok(false) => {
                    // Row off Processing (recovery moved it); webhook still fires.
                    info!(
                        trace_id = trace_id,
                        "Transaction {} already past Processing; status write skipped",
                        update.transaction_id
                    );
                }
                Err(e) => {
                    error!(
                        trace_id = trace_id,
                        "Failed to update transaction {} status: {}", update.transaction_id, e
                    );
                    metrics::OPERATOR_DB_UPDATE_ERRORS
                        .with_label_values(&[pt])
                        .inc();
                    if let Some(err_msg) = &update.error_message {
                        error!(trace_id = trace_id, "Transaction error was: {}", err_msg);
                    }
                }
            }
        }

        if is_alertable {
            // Log failed transaction at ERROR level for paging/alert pipeline visibility.
            error!("Transaction {} {:?}", update.transaction_id, update.status);
            if let Some(err_msg) = &update.error_message {
                error!("Transaction {} error: {}", update.transaction_id, err_msg);
            }

            // Queued off the write path, so a slow endpoint never delays the next update.
            if self.webhook_url.is_some() {
                let transaction_id = update.transaction_id;
                if self.alert_tx.try_send(update).is_err() {
                    warn!(transaction_id, "Alert queue full; alert dropped");
                    metrics::OPERATOR_TRANSACTION_ERRORS
                        .with_label_values(&[pt, "alert_queue_full"])
                        .inc();
                }
            }
        }
    }

    /// Post queued alerts one at a time until the queue closes and drains. Once
    /// the writer has stopped, what is left shares one `ALERT_DRAIN_TIMEOUT`.
    async fn deliver_alerts(
        client: WebhookClient,
        webhook_url: Option<String>,
        alerts: Option<mpsc::Receiver<TransactionStatusUpdate>>,
        program_type: ProgramType,
    ) {
        let (Some(webhook_url), Some(mut alerts)) = (webhook_url, alerts) else {
            return;
        };
        let mut drain_deadline = None;
        while let Some(update) = alerts.recv().await {
            let post = Self::send_webhook_alert(&client, &webhook_url, &update);
            if !alerts.is_closed() {
                post.await;
                continue;
            }
            let deadline = *drain_deadline.get_or_insert(Instant::now() + ALERT_DRAIN_TIMEOUT);
            if timeout_at(deadline, post).await.is_err() {
                // The cut-off post is undelivered too. The ids are the on-call's
                // list of rows to review, since nothing replays these alerts.
                let mut undelivered = vec![update.transaction_id];
                while let Ok(remaining) = alerts.try_recv() {
                    undelivered.push(remaining.transaction_id);
                }
                error!(
                    undelivered = undelivered.len(),
                    transaction_ids = ?undelivered,
                    "Alert drain timed out; these transactions were not paged"
                );
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[program_type.as_label(), "alert_drain_dropped"])
                    .inc_by(undelivered.len() as f64);
                return;
            }
        }
    }

    /// Send webhook alert for failed transaction
    async fn send_webhook_alert(
        client: &WebhookClient,
        webhook_url: &str,
        update: &TransactionStatusUpdate,
    ) {
        let processed_at = update
            .processed_at
            .as_ref()
            .map_or_else(|| Utc::now().to_rfc3339(), |ts| ts.to_rfc3339());
        let timestamp = Utc::now().to_rfc3339();

        let status_str = match update.status {
            TransactionStatus::FailedReminted => "failed_reminted",
            TransactionStatus::Failed => "failed",
            TransactionStatus::ManualReview => "manual_review",
            other => {
                error!("Unexpected alertable status in webhook: {:?}", other);
                "failed"
            }
        };

        let remint_status: Option<&str> = if update.remint_signature.is_some() {
            Some("success")
        } else if update.remint_attempted {
            Some("failed")
        } else {
            None
        };

        let payload = json!({
            "transaction_id": update.transaction_id,
            "trace_id": update.trace_id.clone(),
            "status": status_str,
            "counterpart_signature": update.counterpart_signature.clone(),
            "error_message": update.error_message.clone(),
            "processed_at": processed_at,
            "timestamp": timestamp,
            "remint_signature": update.remint_signature.clone(),
            "remint_status": remint_status,
        });

        let context = format!("transaction {}", update.transaction_id);
        match client.post_json(webhook_url, &payload, &context).await {
            Ok(_) => info!(
                "Webhook alert sent successfully for transaction {}",
                update.transaction_id
            ),
            Err(error) => warn!(
                "Failed to send webhook alert for transaction {}: {}",
                update.transaction_id, error
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProgramType;
    use crate::storage::common::models::TransactionStatus;
    use crate::storage::common::storage::mock::MockStorage;
    use chrono::Utc;
    use mockito::Server;

    // Helper function to create a test TransactionStatusUpdate
    fn create_test_update(status: TransactionStatus) -> TransactionStatusUpdate {
        TransactionStatusUpdate {
            transaction_id: 12345,
            trace_id: Some("trace_test_123".to_string()),
            status,
            counterpart_signature: Some("test_signature_123".to_string()),
            error_message: Some("Test error message".to_string()),
            processed_at: Some(Utc::now()),
            remint_signature: None,
            remint_attempted: false,
            alert_only: false,
        }
    }

    #[tokio::test]
    async fn test_webhook_alert_success() {
        // Create mock webhook server
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(r#"{"success": true}"#)
            .create_async()
            .await;

        // Create DbTransactionWriter with mock webhook URL
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        // Create a failed transaction update
        let update = create_test_update(TransactionStatus::Failed);

        // Send webhook alert
        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;

        // Verify webhook was called
        mock.assert();
    }

    #[tokio::test]
    async fn test_webhook_alert_non_success_status() {
        // Create mock webhook server returning 500 error
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(500)
            .with_body(r#"{"error": "Internal server error"}"#)
            .create_async()
            .await;

        // Create DbTransactionWriter with mock webhook URL
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        // Create a failed transaction update
        let update = create_test_update(TransactionStatus::Failed);

        // Send webhook alert (should handle error gracefully)
        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;

        // Verify webhook was called despite error
        mock.assert();
    }

    #[tokio::test]
    async fn test_webhook_alert_payload_structure() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(json!({
                "transaction_id": 12345_i64,
                "trace_id": "trace_test_123",
                "status": "failed",
                "counterpart_signature": "test_signature_123",
                "error_message": "Test error message",
            })))
            .with_status(200)
            .create_async()
            .await;

        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        let update = create_test_update(TransactionStatus::Failed);

        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;

        mock.assert();
    }

    #[tokio::test]
    async fn test_webhook_alert_network_error() {
        // Use an invalid URL to simulate network error
        let invalid_url = "http://invalid-host-that-does-not-exist.local:9999";

        // Create DbTransactionWriter with invalid webhook URL
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(
            storage,
            rx,
            Some(invalid_url.to_string()),
            ProgramType::Escrow,
        );

        // Create a failed transaction update
        let update = create_test_update(TransactionStatus::Failed);

        // Send webhook alert (should handle error gracefully without panicking)
        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, invalid_url, &update).await;

        // Test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_graceful_degradation_when_webhook_unset() {
        // Create DbTransactionWriter with NO webhook URL (None)
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, None, ProgramType::Escrow);

        // Create a failed transaction update
        let update = create_test_update(TransactionStatus::Failed);

        // Handle the update (should complete gracefully without attempting webhook)
        writer.handle_update(update).await;

        // Test passes if no panic occurs and no webhook is attempted
        // This verifies graceful degradation when ALERT_WEBHOOK is unset
    }

    #[tokio::test]
    async fn test_webhook_failure_does_not_crash_handle_update() {
        // Use an invalid URL to simulate webhook failure
        let invalid_url = "http://invalid-host-that-does-not-exist.local:9999";

        // Create DbTransactionWriter with invalid webhook URL
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(
            storage,
            rx,
            Some(invalid_url.to_string()),
            ProgramType::Escrow,
        );

        // Create a failed transaction update
        let update = create_test_update(TransactionStatus::Failed);

        // Handle the update (webhook POST will fail but should not crash)
        writer.handle_update(update).await;

        // Test passes if no panic occurs
        // This verifies that webhook failures (network errors, 404, timeouts)
        // are logged but don't crash the transaction status update process
    }

    #[tokio::test]
    async fn test_webhook_payload_for_failed_reminted_status() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(json!({
                "transaction_id": 12345_i64,
                "status": "failed_reminted",
                "remint_signature": "remint_sig_abc",
                "remint_status": "success",
            })))
            .with_status(200)
            .create_async()
            .await;

        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        let update = TransactionStatusUpdate {
            transaction_id: 12345,
            trace_id: Some("trace_test_remint".to_string()),
            status: TransactionStatus::FailedReminted,
            counterpart_signature: None,
            error_message: Some("withdrawal failed".to_string()),
            processed_at: Some(Utc::now()),
            remint_signature: Some("remint_sig_abc".to_string()),
            remint_attempted: true,
            alert_only: false,
        };

        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;
        mock.assert();
    }

    #[tokio::test]
    async fn test_webhook_payload_for_manual_review_status() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(json!({
                "transaction_id": 77_i64,
                "status": "manual_review",
            })))
            .with_status(200)
            .create_async()
            .await;

        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        let update = TransactionStatusUpdate {
            transaction_id: 77,
            trace_id: Some("trace_manual_review".to_string()),
            status: TransactionStatus::ManualReview,
            counterpart_signature: None,
            error_message: Some("release failed | remint failed: timeout".to_string()),
            processed_at: Some(Utc::now()),
            remint_signature: None,
            remint_attempted: true,
            alert_only: false,
        };

        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;
        mock.assert();
    }

    #[tokio::test]
    async fn test_webhook_remint_status_is_null_when_remint_not_attempted() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(json!({
                "transaction_id": 78_i64,
                "status": "manual_review",
                "remint_status": null,
            })))
            .with_status(200)
            .create_async()
            .await;

        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        let update = TransactionStatusUpdate {
            transaction_id: 78,
            trace_id: Some("trace_no_remint".to_string()),
            status: TransactionStatus::ManualReview,
            counterpart_signature: None,
            error_message: Some("no signatures to verify — remint unsafe".to_string()),
            processed_at: Some(Utc::now()),
            remint_signature: None,
            remint_attempted: false,
            alert_only: false,
        };

        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;
        mock.assert();
    }

    #[tokio::test]
    async fn test_webhook_remint_status_is_failed_when_remint_attempted_and_failed() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header("content-type", "application/json")
            .match_body(mockito::Matcher::PartialJson(json!({
                "transaction_id": 88_i64,
                "status": "manual_review",
                "remint_status": "failed",
            })))
            .with_status(200)
            .create_async()
            .await;

        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Escrow);

        let update = TransactionStatusUpdate {
            transaction_id: 88,
            trace_id: Some("trace_remint_failed".to_string()),
            status: TransactionStatus::ManualReview,
            counterpart_signature: None,
            error_message: Some("release_funds failed | remint failed: timeout".to_string()),
            processed_at: Some(Utc::now()),
            remint_signature: None,
            remint_attempted: true,
            alert_only: false,
        };

        DbTransactionWriter::send_webhook_alert(&writer.webhook_client, &server.url(), &update)
            .await;
        mock.assert();
    }

    // ── skipped status writes ─────────────────────────────────────────

    /// Seed a withdrawal in `status`. A `ManualReview` row is the fixture that
    /// produces `Ok(false)`: both the mock and the SQL refuse a later status
    /// write against it.
    fn seed_withdrawal(mock: &MockStorage, id: i64, status: TransactionStatus) {
        use crate::storage::common::amount::TokenAmount;
        use crate::storage::common::models::{DbTransaction, TransactionType};
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id,
                signature: format!("sig_{id}"),
                trace_id: format!("trace-{id}"),
                slot: 100,
                initiator: "initiator".to_string(),
                recipient: "recipient".to_string(),
                mint: "mint_addr".to_string(),
                amount: TokenAmount(1000),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(1),
                status,
                created_at: Utc::now(),
                updated_at: Utc::now(),
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
            });
    }

    fn skipped_count(program_type: &str, status: &str) -> f64 {
        metrics::OPERATOR_DB_UPDATE_SKIPPED
            .with_label_values(&[program_type, status])
            .get()
    }

    /// A dropped `Completed` write is the one outcome the row can never
    /// reach again on its own: the release landed on chain but the DB will
    /// never say so, and the next boot rebuilds a local tree that disagrees
    /// with chain. It has to be countable, not an INFO line.
    #[tokio::test]
    async fn skipped_completed_write_escalates_and_counts() {
        let mock = MockStorage::new();
        seed_withdrawal(&mock, 12345, TransactionStatus::ManualReview);
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(mock));
        let writer = DbTransactionWriter::new(storage, rx, None, ProgramType::Withdraw);

        let before = skipped_count("withdraw", "Completed");
        writer
            .handle_update(create_test_update(TransactionStatus::Completed))
            .await;

        assert_eq!(skipped_count("withdraw", "Completed") - before, 1.0);
    }

    /// Recovery legitimately moves rows off `Processing` and re-derives a
    /// non-`Completed` outcome, so those skips stay quiet. Without this the
    /// routine race would page.
    #[tokio::test]
    async fn skipped_non_completed_write_is_not_escalated() {
        let mock = MockStorage::new();
        seed_withdrawal(&mock, 12345, TransactionStatus::ManualReview);
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(mock));
        let writer = DbTransactionWriter::new(storage, rx, None, ProgramType::Withdraw);

        let before = skipped_count("withdraw", "Failed");
        writer
            .handle_update(create_test_update(TransactionStatus::Failed))
            .await;

        assert_eq!(skipped_count("withdraw", "Failed") - before, 0.0);
    }

    /// An alert-only update was already written under its incarnation's fence.
    /// By the time the writer reads it the row may have been re-armed and claimed
    /// again, so writing it would park the new incarnation. It must only alert.
    #[tokio::test]
    async fn an_alert_only_update_never_writes_the_row() {
        let txn_id = 12345;
        let mock = MockStorage::new();
        // The re-armed row, claimed again by the fetcher.
        seed_withdrawal(&mock, txn_id, TransactionStatus::Processing);
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut writer = DbTransactionWriter::new(
            storage,
            rx,
            Some("http://unused.invalid".to_string()),
            ProgramType::Withdraw,
        );
        let mut alerts = writer.alert_rx.take().expect("alert queue present");

        writer
            .handle_update(TransactionStatusUpdate {
                transaction_id: txn_id,
                alert_only: true,
                ..create_test_update(TransactionStatus::ManualReview)
            })
            .await;

        let row_status = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|txn| txn.id == txn_id)
            .map(|txn| txn.status);
        assert_eq!(row_status, Some(TransactionStatus::Processing));
        let alert = alerts.try_recv().expect("the alert must still be queued");
        assert_eq!(alert.transaction_id, txn_id);
    }

    // ── webhook delivery ──────────────────────────────────────────────

    /// A draining writer posts every alert it accepted before `start` returns.
    #[tokio::test]
    async fn queued_alerts_are_delivered_before_start_returns() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .expect(2)
            .create_async()
            .await;

        let (tx, rx) = mpsc::channel(2);
        for transaction_id in [1, 2] {
            tx.send(TransactionStatusUpdate {
                transaction_id,
                ..create_test_update(TransactionStatus::ManualReview)
            })
            .await
            .unwrap();
        }
        drop(tx);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Withdraw)
            .start()
            .await
            .unwrap();

        mock.assert();
    }

    /// A halt-sized burst still reaches a healthy endpoint in full.
    #[tokio::test]
    async fn a_burst_of_alerts_is_delivered_in_full() {
        let burst = 40;
        let mut server = Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .expect(burst as usize)
            .create_async()
            .await;

        let (tx, rx) = mpsc::channel(burst as usize);
        for transaction_id in 1..=burst {
            tx.send(TransactionStatusUpdate {
                transaction_id,
                ..create_test_update(TransactionStatus::ManualReview)
            })
            .await
            .unwrap();
        }
        drop(tx);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        DbTransactionWriter::new(storage, rx, Some(server.url()), ProgramType::Withdraw)
            .start()
            .await
            .unwrap();

        mock.assert();
    }

    /// A stopped writer gives queued alerts one bounded drain, so a hanging
    /// endpoint cannot hold a refused boot for 10s per alert, and counts what
    /// the drain drops.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_writer_bounds_the_drain_of_queued_alerts() {
        // Bound but never accepted, so every post hangs until its 10s timeout.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let webhook_url = format!("http://{}", listener.local_addr().unwrap());

        let queued = 5;
        let (tx, rx) = mpsc::channel(queued as usize);
        for transaction_id in 1..=queued {
            tx.send(TransactionStatusUpdate {
                transaction_id,
                ..create_test_update(TransactionStatus::ManualReview)
            })
            .await
            .unwrap();
        }
        drop(tx);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer =
            DbTransactionWriter::new(storage, rx, Some(webhook_url), ProgramType::Withdraw);
        // Only this test drops on drain under this label, so no parallel test moves it.
        let dropped = metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&["withdraw", "alert_drain_dropped"]);
        let before = dropped.get();

        let started = tokio::time::Instant::now();
        writer.start().await.unwrap();

        // At most one post in flight plus the drain, not one timeout per alert.
        assert!(
            started.elapsed() <= Duration::from_secs(20),
            "drain took {:?}",
            started.elapsed()
        );
        // The first post's own timeout lands on the deadline's tick, so it may
        // count as attempted rather than dropped.
        let dropped_count = dropped.get() - before;
        assert!(
            dropped_count >= (queued - 1) as f64 && dropped_count <= queued as f64,
            "dropped {dropped_count} of {queued}"
        );
    }

    /// With the queue full, the next alert is dropped and counted, never waited on.
    #[tokio::test]
    async fn a_full_alert_queue_drops_the_overflow_and_counts_it() {
        let (_tx, rx) = mpsc::channel(1);
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        // Never started, so nothing drains the queue.
        let writer = DbTransactionWriter::new(
            storage,
            rx,
            Some("http://unused.invalid".to_string()),
            ProgramType::Withdraw,
        );

        let dropped = metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&["withdraw", "alert_queue_full"]);
        let before = dropped.get();
        for transaction_id in 0..=ALERT_QUEUE_CAPACITY as i64 {
            writer
                .handle_update(TransactionStatusUpdate {
                    transaction_id,
                    ..create_test_update(TransactionStatus::ManualReview)
                })
                .await;
        }

        assert_eq!(dropped.get() - before, 1.0);
    }

    /// A webhook that accepts the connection but never answers must not hold
    /// back the next update's DB write.
    #[tokio::test]
    async fn a_hanging_webhook_does_not_hold_back_the_next_status_write() {
        // Bound but never accepted: the kernel completes the handshake, so each
        // post hangs until the client's 10s timeout.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let webhook_url = format!("http://{}", listener.local_addr().unwrap());

        let first_id = 1;
        let second_id = 2;
        let mock = MockStorage::new();
        seed_withdrawal(&mock, first_id, TransactionStatus::Processing);
        seed_withdrawal(&mock, second_id, TransactionStatus::Processing);

        let (tx, rx) = mpsc::channel(2);
        for transaction_id in [first_id, second_id] {
            tx.send(TransactionStatusUpdate {
                transaction_id,
                ..create_test_update(TransactionStatus::ManualReview)
            })
            .await
            .unwrap();
        }
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let writer =
            DbTransactionWriter::new(storage, rx, Some(webhook_url), ProgramType::Withdraw);
        let handle = tokio::spawn(writer.start());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let second_status = mock
                .pending_transactions
                .lock()
                .unwrap()
                .iter()
                .find(|txn| txn.id == second_id)
                .map(|txn| txn.status);
            if second_status == Some(TransactionStatus::ManualReview) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the second write waited on the first webhook"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        handle.abort();
    }
}
