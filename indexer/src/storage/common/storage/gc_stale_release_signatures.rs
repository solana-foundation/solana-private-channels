use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn gc_stale_release_signatures(storage: &Storage) -> Result<u64, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.gc_stale_release_signatures_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.gc_stale_release_signatures().await,
    }
}
