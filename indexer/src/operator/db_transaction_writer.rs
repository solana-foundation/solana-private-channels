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
use tracing::{error, info, warn};

/// DbTransactionWriter that receives transaction status updates from sender
/// and writes them to the database
pub struct DbTransactionWriter {
    storage: Arc<Storage>,
    update_rx: mpsc::Receiver<TransactionStatusUpdate>,
    webhook_client: WebhookClient,
    webhook_url: Option<String>,
    program_type: ProgramType,
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
        Self {
            storage,
            update_rx,
            webhook_client,
            webhook_url,
            program_type,
        }
    }

    /// Start processing status updates from the channel
    pub async fn start(mut self) -> Result<(), StorageError> {
        info!("Starting StorageWriter");

        while let Some(update) = self.update_rx.recv().await {
            self.handle_update(update).await;
        }

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
        match self
            .storage
            .update_transaction_status(
                update.transaction_id,
                update.status,
                update.counterpart_signature.clone(),
                update.processed_at.unwrap_or_else(Utc::now),
            )
            .await
        {
            Ok(true) => {
                info!(
                    trace_id = trace_id,
                    "Updated transaction {} to status {:?}", update.transaction_id, update.status
                );
                metrics::OPERATOR_DB_UPDATES
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

        if is_alertable {
            // Log failed transaction at ERROR level for paging/alert pipeline visibility.
            error!("Transaction {} {:?}", update.transaction_id, update.status);
            if let Some(err_msg) = &update.error_message {
                error!("Transaction {} error: {}", update.transaction_id, err_msg);
            }

            if let Some(webhook_url) = &self.webhook_url {
                self.send_webhook_alert(webhook_url, &update).await;
            }
        }
    }

    /// Send webhook alert for failed transaction
    async fn send_webhook_alert(&self, webhook_url: &str, update: &TransactionStatusUpdate) {
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
        match self
            .webhook_client
            .post_json(webhook_url, &payload, &context)
            .await
        {
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
        writer.send_webhook_alert(&server.url(), &update).await;

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
        writer.send_webhook_alert(&server.url(), &update).await;

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

        writer.send_webhook_alert(&server.url(), &update).await;

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
        writer.send_webhook_alert(invalid_url, &update).await;

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
        };

        writer.send_webhook_alert(&server.url(), &update).await;
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
        };

        writer.send_webhook_alert(&server.url(), &update).await;
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
        };

        writer.send_webhook_alert(&server.url(), &update).await;
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
        };

        writer.send_webhook_alert(&server.url(), &update).await;
        mock.assert();
    }
}
