use crate::{error::StorageError, storage::common::storage::Storage};

/// Lowest and highest withdrawal nonce at or above `min_nonce` that still owes a
/// release, or `None` when none do.
///
/// The rotation driver reads this when it arms and again before it sends, so a
/// row that returns to owing in between is seen. Rows without a nonce do not
/// contribute to the bound.
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
