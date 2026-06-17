use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn delete_release_signatures(
    storage: &Storage,
    transaction_id: i64,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.delete_release_signatures_internal(transaction_id)
                .await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.delete_release_signatures(transaction_id).await,
    }
}
