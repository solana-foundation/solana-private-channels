use crate::{error::StorageError, storage::common::storage::Storage};

/// Every mint as `(mint_address, token_program)`.
pub async fn get_mint_addresses(storage: &Storage) -> Result<Vec<(String, String)>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_mint_addresses_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_mint_addresses().await,
    }
}
