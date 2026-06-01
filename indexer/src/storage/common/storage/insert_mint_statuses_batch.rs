use crate::{
    error::StorageError,
    storage::common::{models::DbMintStatus, storage::Storage},
};

pub async fn insert_mint_statuses_batch(
    storage: &Storage,
    statuses: &[DbMintStatus],
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.insert_mint_statuses_batch_internal(statuses).await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.insert_mint_statuses_batch(statuses).await,
    }
}
