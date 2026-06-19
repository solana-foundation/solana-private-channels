use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Parked` → `Pending` on `updated_at`; `Ok(false)` if stale.
pub async fn try_requeue_parked(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_requeue_parked_internal(transaction_id, expected_updated_at)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_requeue_parked(transaction_id, expected_updated_at)
                .await
        }
    }
}
