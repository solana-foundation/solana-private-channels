use crate::{error::StorageError, storage::common::storage::Storage};

/// Move a stuck row from `Processing` back to `Pending` so the fetcher
/// will retry it. Only writes if the row's `updated_at` still matches
/// what the caller read — i.e., nobody else has touched the row since.
/// Returns `true` if the write happened, `false` if someone else got
/// there first (which is fine, nothing to do).
pub async fn try_requeue_processing(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_requeue_processing_internal(transaction_id, expected_updated_at)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_requeue_processing(transaction_id, expected_updated_at)
                .await
        }
    }
}
