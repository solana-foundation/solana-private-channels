use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Processing` → `Completed` on `updated_at`; `Ok(false)` if stale.
pub async fn try_complete_processing(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    counterpart_signature: Option<String>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_complete_processing_internal(
                transaction_id,
                expected_updated_at,
                counterpart_signature,
            )
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_complete_processing(transaction_id, expected_updated_at, counterpart_signature)
                .await
        }
    }
}
