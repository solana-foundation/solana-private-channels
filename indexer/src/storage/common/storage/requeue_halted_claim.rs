use crate::{error::StorageError, storage::common::storage::Storage};

/// Return a halt-refused `Processing` row to `Pending` if it never broadcast, without
/// spending a requeue attempt. `Ok(false)` means the row stays for recovery to classify.
pub async fn requeue_halted_claim(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .requeue_halted_claim_internal(transaction_id, expected_updated_at)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .requeue_halted_claim(transaction_id, expected_updated_at)
                .await
        }
    }
}
