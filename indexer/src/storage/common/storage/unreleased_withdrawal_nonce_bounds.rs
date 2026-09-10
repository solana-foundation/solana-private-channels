use crate::{error::StorageError, storage::common::storage::Storage};

/// Lowest and highest withdrawal nonce at or above `min_nonce` that still owes a
/// release, or `None` when none do.
///
/// The driver decides to rotate from this answer, then rotates seconds later
/// without asking again. The answer cannot go stale in a way that matters: a
/// withdrawal created later gets a higher nonce from the sequence, and a
/// finished one is never moved back to owing a release. So no withdrawal can
/// appear in a generation the driver already read as clear and then have its
/// window wiped by the rotation, which would leave it unpayable forever.
pub async fn unreleased_withdrawal_nonce_bounds(
    storage: &Storage,
    min_nonce: i64,
) -> Result<Option<(i64, i64)>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .unreleased_withdrawal_nonce_bounds_internal(min_nonce)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.unreleased_withdrawal_nonce_bounds(min_nonce).await,
    }
}
