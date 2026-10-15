use crate::error::StorageError;
use crate::storage::common::models::DbConsumedMint;
use crate::storage::common::storage::Storage;

pub async fn get_consumed_mints(storage: &Storage) -> Result<Vec<DbConsumedMint>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_consumed_mints_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_consumed_mints().await,
    }
}
