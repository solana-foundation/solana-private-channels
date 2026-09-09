use crate::{
    error::StorageError,
    storage::common::{models::HaltInfo, storage::Storage},
};

/// Set (or refresh) the durable reconciliation halt flag.
pub async fn set_reconciliation_halt(storage: &Storage, reason: &str) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.set_reconciliation_halt_internal(reason).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.set_reconciliation_halt(reason).await,
    }
}

/// Return the halt info when the flag is set, else `None`.
pub async fn is_reconciliation_halted(storage: &Storage) -> Result<Option<HaltInfo>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.is_reconciliation_halted_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.is_reconciliation_halted().await,
    }
}

/// Clear the halt so the pipelines can resume (manual/runbook use).
pub async fn clear_reconciliation_halt(storage: &Storage) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.clear_reconciliation_halt_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.clear_reconciliation_halt().await,
    }
}
