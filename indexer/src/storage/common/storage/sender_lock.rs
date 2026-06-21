use crate::{error::StorageError, storage::common::storage::Storage};

/// Held for the sender's whole lifetime. Dropping it (or a crash) releases the
/// Postgres advisory lock. The `Postgres` variant owns the pinned connection
/// carrying the lock, so it must stay alive.
pub enum SenderLockGuard {
    // Boxed: the pooled connection is large and the mock variant is empty.
    Postgres(Box<sqlx::pool::PoolConnection<sqlx::Postgres>>),
    #[cfg(any(test, feature = "test-mock-storage"))]
    Noop,
}

/// Try to become the singleton sender for `key`. `Ok(None)` means another
/// sender holds the lock and the caller must refuse to start.
pub async fn try_acquire_sender_lock(
    storage: &Storage,
    key: i64,
) -> Result<Option<SenderLockGuard>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_acquire_sender_lock(key)
            .await?
            .map(|conn| SenderLockGuard::Postgres(Box::new(conn)))),
        // Mock has no shared backing store, so nothing to contend on.
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(_) => Ok(Some(SenderLockGuard::Noop)),
    }
}
