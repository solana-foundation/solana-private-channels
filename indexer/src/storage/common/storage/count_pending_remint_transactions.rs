use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn count_pending_remint_transactions(
    storage: &Storage,
    after_id: i64,
) -> Result<i64, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .count_pending_remint_transactions_internal(after_id)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.count_pending_remint_transactions(after_id).await,
    }
}
