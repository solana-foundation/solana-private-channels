use crate::{
    config::ProgramType,
    error::StorageError,
    indexer::checkpoint::program_key,
    storage::common::storage::{live_lock::LiveLockGuard, Storage},
    storage::postgres::db::PostgresDb,
};

/// Halt reason a resync writes with its marker, so operators that predate the marker stop too.
pub fn resync_halt_reason(program: ProgramType) -> String {
    let key = program_key(program);
    format!("unfinished {key} resync: rerun resync for {key}; do not clear this halt by hand")
}

/// The program whose resync deleted rows and has not finished rebuilding them, if any.
pub async fn get_unfinished_resync(storage: &Storage) -> Result<Option<String>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_unfinished_resync_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_unfinished_resync(),
    }
}

/// Clear the marker and this resync's halt on the session holding `lock`, so a resync that
/// lost its lock cannot clear them.
pub async fn clear_unfinished_resync_fenced(
    storage: &Storage,
    lock: &LiveLockGuard,
    program: ProgramType,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(_) => {
            lock.run_fenced(move |conn| {
                Box::pin(PostgresDb::clear_unfinished_resync_on(conn, program))
            })
            .await
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.clear_unfinished_resync(program),
    }
}
