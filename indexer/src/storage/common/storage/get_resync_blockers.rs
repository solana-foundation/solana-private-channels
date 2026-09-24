use crate::{
    error::StorageError,
    storage::common::{
        models::{ResyncBlockers, TransactionType},
        storage::Storage,
    },
};

pub async fn get_resync_blockers(
    storage: &Storage,
    own: TransactionType,
) -> Result<ResyncBlockers, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_resync_blockers_internal(own).await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_resync_blockers(own),
    }
}
