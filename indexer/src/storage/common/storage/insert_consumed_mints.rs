use crate::error::StorageError;
use crate::storage::common::models::DbConsumedMint;
use crate::storage::common::storage::Storage;

pub async fn insert_consumed_mints(
    storage: &Storage,
    mints: &[DbConsumedMint],
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.insert_consumed_mints_internal(mints).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.insert_consumed_mints(mints).await,
    }
}
