use crate::{
    error::StorageError,
    storage::common::{models::TransactionType, storage::Storage},
};

/// Pending rows of one type: those the fetch can claim now, and undecryptable deposits
/// held until recovery. A release waiting out its retry delay is in neither.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingBacklog {
    pub claimable: i64,
    pub undecrypted: i64,
}

pub async fn count_pending_transactions(
    storage: &Storage,
    transaction_type: TransactionType,
) -> Result<PendingBacklog, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .count_pending_transactions_internal(transaction_type)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.count_pending_transactions(transaction_type).await,
    }
}
