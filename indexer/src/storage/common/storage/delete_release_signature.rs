use crate::{error::StorageError, storage::common::storage::Storage};

/// Forget one broadcast signature, leaving the transaction's others in place.
pub async fn delete_release_signature(
    storage: &Storage,
    transaction_id: i64,
    signature: &str,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.delete_release_signature_internal(transaction_id, signature)
                .await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => {
            mock.delete_release_signature(transaction_id, signature)
                .await
        }
    }
}
