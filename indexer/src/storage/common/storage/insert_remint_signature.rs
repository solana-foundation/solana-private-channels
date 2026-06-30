use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn insert_remint_signature(
    storage: &Storage,
    transaction_id: i64,
    signature: String,
    last_valid_block_height: i64,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.insert_remint_signature_internal(transaction_id, signature, last_valid_block_height)
                .await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => {
            mock.insert_remint_signature(transaction_id, signature, last_valid_block_height)
                .await
        }
    }
}
