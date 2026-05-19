use crate::{
    error::StorageError,
    storage::common::{models::DbTransaction, storage::Storage},
};
use std::time::Duration;

/// Rows that have been sitting in `Processing` longer than `threshold`,
/// oldest first. These are the rows the recovery worker investigates —
/// they're almost certainly orphans from a crashed operator.
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
