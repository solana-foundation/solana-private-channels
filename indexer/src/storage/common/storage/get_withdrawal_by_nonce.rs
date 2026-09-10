use crate::error::StorageError;
use crate::storage::common::models::DbTransaction;
use crate::storage::common::storage::Storage;

/// Look up the withdrawal row that owns a nonce.
///
/// The boot bitmap diff learns which nonces diverge but nothing else about them,
/// and it needs the row to read that withdrawal's broadcast signatures and to
/// address a status update at it.
pub async fn get_withdrawal_by_nonce(
    storage: &Storage,
    nonce: u64,
) -> Result<Option<DbTransaction>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_withdrawal_by_nonce_internal(nonce as i64).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_withdrawal_by_nonce(nonce).await,
    }
}
