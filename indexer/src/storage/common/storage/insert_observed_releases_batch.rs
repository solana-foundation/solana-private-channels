use crate::error::StorageError;
use crate::storage::common::models::DbObservedRelease;
use crate::storage::common::storage::Storage;

/// Record the releases a slot was seen to contain. Idempotent on the nonce.
pub async fn insert_observed_releases_batch(
    storage: &Storage,
    releases: &[DbObservedRelease],
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.insert_observed_releases_batch_internal(releases).await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.insert_observed_releases_batch(releases).await,
    }
}
