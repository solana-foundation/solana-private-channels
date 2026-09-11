/// Errors from database storage operations (PostgreSQL via sqlx)
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Query execution failed: {0}")]
    QueryFailed(#[from] sqlx::Error),

    #[error("Database error: {message}")]
    DatabaseError { message: String },

    /// The live-state lock could not be taken in the requested mode, so another
    /// party is using the database in a way that conflicts with this one.
    #[error("{}", requested.refusal_detail())]
    LiveStateLockHeld {
        requested: crate::storage::common::storage::live_lock::LiveLockMode,
    },

    /// Ownership of a live-state lock we already hold stopped being provable.
    #[error("live-state lock ownership could not be proven")]
    LiveStateLockLost,
}
