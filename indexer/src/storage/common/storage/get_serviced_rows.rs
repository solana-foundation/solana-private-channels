use crate::{
    error::StorageError,
    storage::common::{
        models::{ServicedRow, TransactionType},
        storage::Storage,
    },
};

pub async fn get_serviced_rows(
    storage: &Storage,
    own: TransactionType,
    after_id: i64,
    limit: i64,
) -> Result<Vec<ServicedRow>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_serviced_rows_internal(own, after_id, limit).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_serviced_rows(own, after_id, limit),
    }
}
