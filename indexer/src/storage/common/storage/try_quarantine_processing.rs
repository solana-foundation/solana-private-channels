use crate::{error::StorageError, storage::common::storage::Storage};

/// Mark a stuck row as `ManualReview` when recovery can't decide what
/// happened on-chain. Only writes if the row's `updated_at` still
/// matches what the caller read. The `reason` is passed only so the
/// caller can include it in the alert webhook — we don't store it in
/// the DB.
pub async fn try_quarantine_processing(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    reason: String,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_quarantine_processing_internal(transaction_id, expected_updated_at, reason)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_quarantine_processing(transaction_id, expected_updated_at, reason)
                .await
        }
    }
}
