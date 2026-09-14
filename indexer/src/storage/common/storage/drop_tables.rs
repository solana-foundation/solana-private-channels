use crate::{
    error::StorageError,
    storage::common::storage::{live_lock::LiveLockGuard, Storage},
    storage::postgres::db::PostgresDb,
};

pub async fn drop_tables(storage: &Storage) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.drop_tables().await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.drop_tables().await,
    }
}

/// Drop every table on the session holding `lock`, so the drop cannot outlive the lock.
///
/// The pooled variant above issues its statements on whatever connection is free, which
/// keeps running after the lock session dies and the lock is freed. This one stops.
pub async fn drop_tables_fenced(
    storage: &Storage,
    lock: &LiveLockGuard,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(_) => {
            lock.run_fenced(|conn| Box::pin(PostgresDb::drop_tables_on(conn)))
                .await
        }
        // No shared session to fence against, and nothing else can reach the store.
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.drop_tables().await,
    }
}
