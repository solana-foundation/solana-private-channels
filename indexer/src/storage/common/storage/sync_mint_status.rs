use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn sync_mint_status(
    storage: &Storage,
    mint_addresses: &[String],
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => db.sync_mint_status_internal(mint_addresses).await,
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.sync_mint_status(mint_addresses).await,
    }
}
