use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Processing` → `ManualReview`; reason rides on the webhook, not DB.
pub async fn try_quarantine_processing(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_quarantine_processing_internal(transaction_id, expected_updated_at)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_quarantine_processing(transaction_id, expected_updated_at)
                .await
        }
    }
}
