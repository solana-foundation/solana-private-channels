use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Processing`/`Parked` → `Parked`; `Ok(false)` if the row is neither.
pub async fn try_park_processing(
    storage: &Storage,
    transaction_id: i64,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.try_park_processing_internal(transaction_id).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.try_park_processing(transaction_id).await,
    }
}
