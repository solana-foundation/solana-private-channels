use crate::{
    error::StorageError,
    storage::common::{
        models::{DbTransaction, TransactionType},
        storage::Storage,
    },
};

/// Stale `Parked` rows of one type older than the threshold, oldest-first.
pub async fn get_stale_parked_transactions(
    storage: &Storage,
    threshold: std::time::Duration,
    limit: i64,
    transaction_type: TransactionType,
) -> Result<Vec<DbTransaction>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_stale_parked_transactions_internal(threshold, limit, transaction_type)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .get_stale_parked_transactions(threshold, limit, transaction_type)
                .await
        }
    }
}
