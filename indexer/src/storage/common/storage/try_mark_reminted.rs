use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Processing`/`PendingRemint` to `FailedReminted` on `updated_at`; `Ok(false)` if stale.
pub async fn try_mark_reminted(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    remint_signature: String,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_mark_reminted_internal(transaction_id, expected_updated_at, remint_signature)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_mark_reminted(transaction_id, expected_updated_at, remint_signature)
                .await
        }
    }
}
