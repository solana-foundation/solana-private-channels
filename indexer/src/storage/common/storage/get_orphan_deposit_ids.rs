use crate::{error::StorageError, storage::common::storage::Storage};

/// `transactions.id` for every `deposit` row whose mint was not in `allowed`
/// status at the deposit's slot (per `mint_status_history`). A non-empty
/// result means the indexer recorded a deposit for a mint that was either
/// never allowlisted or was blocked at the time of the deposit — a
/// trust-boundary leak. Reconciliation queries this to alert on any such
/// rows.
///
/// IDs (not mints) are returned so reconciliation detects
/// *each new* orphan deposit.
pub async fn get_orphan_deposit_ids(storage: &Storage) -> Result<Vec<i64>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_orphan_deposit_ids_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_orphan_deposit_ids().await,
    }
}
