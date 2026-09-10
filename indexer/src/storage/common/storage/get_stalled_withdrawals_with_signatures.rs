use crate::{
    error::StorageError,
    storage::common::{
        models::{DbTransaction, TransactionStatus},
        storage::Storage,
    },
};

/// Withdrawals stalled in `status` that still carry stored release signatures,
/// keyed forward from `after_id`. Rows with no usable evidence are filtered out
/// by the query.
pub async fn get_stalled_withdrawals_with_signatures(
    storage: &Storage,
    status: TransactionStatus,
    after_id: i64,
    limit: i64,
) -> Result<Vec<DbTransaction>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_stalled_withdrawals_with_signatures_internal(status, after_id, limit)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .get_stalled_withdrawals_with_signatures(status, after_id, limit)
                .await
        }
    }
}
