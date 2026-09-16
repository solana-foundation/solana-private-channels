use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn get_mint_addresses(storage: &Storage) -> Result<Vec<String>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_mint_addresses_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_mint_addresses().await,
    }
}
