use crate::{
    error::StorageError,
    storage::common::{models::DbTransaction, storage::Storage},
};
use std::time::Duration;

/// Stale `Processing` rows past the threshold, oldest-first.
pub async fn get_stale_processing_transactions(
    storage: &Storage,
    threshold: Duration,
    limit: i64,
) -> Result<Vec<DbTransaction>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_stale_processing_transactions_internal(threshold, limit)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .get_stale_processing_transactions(threshold, limit)
                .await
        }
    }
}
