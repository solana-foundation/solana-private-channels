use crate::error::StorageError;
use crate::storage::common::models::DbObservedRelease;
use crate::storage::common::storage::Storage;

/// The release the indexer recorded for `nonce`, if it saw one. `Ok(None)` is
/// the only answer that means "not released", and it is only as strong as the
/// indexer's coverage of the slots in which a release for this nonce could have
/// landed.
pub async fn get_observed_release(
    storage: &Storage,
    nonce: u64,
) -> Result<Option<DbObservedRelease>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_observed_release_internal(nonce as i64).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_observed_release(nonce as i64).await,
    }
}
