use crate::{
    error::StorageError,
    storage::common::{models::TransactionStatus, storage::Storage},
};

/// Read a row's current status. `Ok(None)` means no such row.
pub async fn get_transaction_status(
    storage: &Storage,
    transaction_id: i64,
) -> Result<Option<TransactionStatus>, StorageError> {
    match storage {
        Storage::Postgres(db) => {
            let status = db.get_transaction_status_internal(transaction_id).await?;

            Ok(status)
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_transaction_status(transaction_id).await,
    }
}
