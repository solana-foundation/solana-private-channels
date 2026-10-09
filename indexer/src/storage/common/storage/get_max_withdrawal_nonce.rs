use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn get_max_withdrawal_nonce(storage: &Storage) -> Result<Option<u64>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_max_withdrawal_nonce_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_max_withdrawal_nonce().await,
    }
}
