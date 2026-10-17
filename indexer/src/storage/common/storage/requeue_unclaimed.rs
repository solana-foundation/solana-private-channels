use crate::{error::StorageError, storage::common::storage::Storage};

/// Return a `Processing` row the sender refused before claiming to `Pending` if it never
/// broadcast, without spending a requeue attempt. `Ok(false)` leaves it for recovery.
pub async fn requeue_unclaimed(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .requeue_unclaimed_internal(transaction_id, expected_updated_at)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .requeue_unclaimed(transaction_id, expected_updated_at)
                .await
        }
    }
}
