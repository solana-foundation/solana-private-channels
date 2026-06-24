use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Parked` → `Processing`; `Ok(false)` if the row is not `Parked`.
pub async fn try_unpark_to_processing(
    storage: &Storage,
    transaction_id: i64,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.try_unpark_to_processing_internal(transaction_id).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.try_unpark_to_processing(transaction_id).await,
    }
}
