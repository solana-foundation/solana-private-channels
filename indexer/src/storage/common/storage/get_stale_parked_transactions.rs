use crate::{
    error::StorageError,
    storage::common::{models::DbTransaction, storage::Storage},
};

/// Stale `Parked` rows older than the threshold, oldest-first.
pub async fn get_stale_parked_transactions(
    storage: &Storage,
    threshold: std::time::Duration,
    limit: i64,
) -> Result<Vec<DbTransaction>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_stale_parked_transactions_internal(threshold, limit)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .get_stale_parked_transactions(threshold, limit)
                .await
        }
    }
}
