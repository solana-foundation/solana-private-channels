use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn get_remint_signatures(
    storage: &Storage,
    transaction_id: i64,
) -> Result<Vec<(String, i64)>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_remint_signatures_internal(transaction_id).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_remint_signatures(transaction_id).await,
    }
}
