use crate::{
    error::StorageError,
    storage::common::{models::DbTransaction, storage::Storage},
};

pub async fn get_pending_remint_transactions(
    storage: &Storage,
    after_id: i64,
    limit: i64,
) -> Result<Vec<DbTransaction>, StorageError> {
    match storage {
        Storage::Postgres(db) => {
            let pending_remints = db
                .get_pending_remint_transactions_internal(after_id, limit)
                .await?;

            Ok(pending_remints)
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .get_pending_remint_transactions(after_id, limit)
                .await
        }
    }
}
