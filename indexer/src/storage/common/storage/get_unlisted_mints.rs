use crate::{error::StorageError, storage::common::storage::Storage};

/// Mints with status history or deposit rows but no `mints` row.
pub async fn get_unlisted_mints(storage: &Storage) -> Result<Vec<String>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_unlisted_mints_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_unlisted_mints().await,
    }
}
