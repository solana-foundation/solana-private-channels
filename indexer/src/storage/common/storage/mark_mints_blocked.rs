use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn mark_mints_blocked(
    storage: &Storage,
    mint_addresses: &[String],
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => db.mark_mints_blocked_internal(mint_addresses).await,
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.mark_mints_blocked(mint_addresses).await,
    }
}
