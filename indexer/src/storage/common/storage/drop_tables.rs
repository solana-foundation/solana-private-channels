use crate::{
    config::ProgramType,
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

/// Delete `program`'s rows on the session holding `lock`, so the delete cannot outlive the lock.
pub async fn wipe_program_fenced(
    storage: &Storage,
    lock: &LiveLockGuard,
    program: ProgramType,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(_) => {
            lock.run_fenced(move |conn| Box::pin(PostgresDb::wipe_program_on(conn, program)))
                .await
        }
        // No shared session to fence against, and nothing else can reach the store.
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.wipe_program(program),
    }
}
