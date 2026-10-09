use crate::{
    error::StorageError,
    storage::{common::storage::Storage, TransactionType},
};

pub async fn get_newest_known_mint_signatures(
    storage: &Storage,
    mint: &str,
    kind: TransactionType,
) -> Result<Vec<String>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_newest_known_mint_signatures_internal(mint, kind)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_newest_known_mint_signatures(mint, kind).await,
    }
}
