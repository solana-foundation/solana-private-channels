use crate::error::StorageError;
use crate::storage::common::storage::Storage;

pub async fn highest_withdrawal_nonce(storage: &Storage) -> Result<Option<u64>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .highest_withdrawal_nonce_internal()
            .await?
            .map(|nonce| nonce as u64)),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.highest_withdrawal_nonce(),
    }
}
