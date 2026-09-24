use crate::{
    error::StorageError,
    storage::common::{models::ReleasedWithdrawal, storage::Storage},
};

pub async fn get_released_withdrawals(
    storage: &Storage,
    after_id: i64,
    limit: i64,
) -> Result<Vec<ReleasedWithdrawal>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_released_withdrawals_internal(after_id, limit)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_released_withdrawals(after_id, limit).await,
    }
}
