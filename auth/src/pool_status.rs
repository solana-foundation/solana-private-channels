use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{is_communication_error, AppError};

/// Tracks whether the Postgres pool is reachable, updated by handlers after
/// each DB call. Read by the /health endpoint so the probe doesn't take a
/// connection from the main pool.
#[derive(Debug)]
pub struct PoolStatus {
    healthy: AtomicBool,
}

impl PoolStatus {
    pub fn new_healthy() -> Arc<Self> {
        Arc::new(Self {
            healthy: AtomicBool::new(true),
        })
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Update from a sqlx result. Communication errors flip unhealthy; logical
    /// errors (e.g. UNIQUE violations) prove the DB is reachable, so they flip
    /// healthy too.
    pub fn observe_sqlx<T>(&self, result: &Result<T, sqlx::Error>) {
        match result {
            Ok(_) => self.healthy.store(true, Ordering::Relaxed),
            Err(e) if is_communication_error(e) => self.healthy.store(false, Ordering::Relaxed),
            Err(_) => self.healthy.store(true, Ordering::Relaxed),
        }
    }

    /// Same as observe_sqlx but for AppResult, where the sqlx error is wrapped.
    pub fn observe_app<T>(&self, result: &Result<T, AppError>) {
        match result {
            Ok(_) => self.healthy.store(true, Ordering::Relaxed),
            Err(AppError::Db(e)) if is_communication_error(e) => {
                self.healthy.store(false, Ordering::Relaxed)
            }
            Err(_) => self.healthy.store(true, Ordering::Relaxed),
        }
    }
}
