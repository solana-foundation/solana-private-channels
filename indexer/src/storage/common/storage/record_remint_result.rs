use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn record_remint_result(
    storage: &Storage,
    transaction_id: i64,
    remint_signature: String,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.record_remint_result_internal(transaction_id, remint_signature)
                .await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .record_remint_result(transaction_id, remint_signature)
                .await
        }
    }
}
