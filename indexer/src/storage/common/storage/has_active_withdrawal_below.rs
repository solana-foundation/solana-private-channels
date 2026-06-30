use crate::{error::StorageError, storage::common::storage::Storage};

/// True if any withdrawal with a lower nonce is still active (non-terminal:
/// Pending/Processing/Parked/PendingRemint/ManualReview). Used to gate the
/// boundary-rotation so a lower nonce is never stranded on a closed tree.
pub async fn has_active_withdrawal_below(
    storage: &Storage,
    nonce: i64,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.has_active_withdrawal_below_internal(nonce).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.has_active_withdrawal_below(nonce).await,
    }
}
