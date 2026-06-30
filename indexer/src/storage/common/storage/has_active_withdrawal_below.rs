use crate::{error::StorageError, storage::common::storage::Storage};

/// True if any withdrawal with a lower nonce is unresolved and not yet handed to
/// the sender (Pending/Parked/PendingRemint/ManualReview). Gates the boundary
/// rotation so a lower nonce is never stranded on a closed tree. Processing is
/// excluded: those are already dispatched ahead of the rotation and held by the
/// sender's in-flight guard.
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
