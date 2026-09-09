use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2, PasswordHash, PasswordVerifier,
};
use tokio::sync::Semaphore;

use crate::error::{AppError, AppResult};

/// Wait this long for a permit before shedding with 503.
const PERMIT_WAIT: Duration = Duration::from_secs(2);

/// Runs Argon2 on the blocking pool under a concurrency cap, so password work
/// can't starve the async workers or exhaust memory.
///
/// Each permit is moved into its blocking task. Blocking tasks are never
/// cancelled, so a permit tied to the caller's future would be released by a
/// client disconnect while the hashing kept running uncounted.
#[derive(Clone)]
pub struct PasswordWorker {
    permits: Arc<Semaphore>,
}

impl PasswordWorker {
    /// Non-zero because a zero cap silently turns every credential request into
    /// a 503 after the permit wait, with the service otherwise reporting healthy.
    pub fn new(max_concurrency: NonZeroUsize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrency.get())),
        }
    }

    /// An unparseable hash counts as a mismatch.
    pub async fn verify(&self, password: String, hash: String) -> AppResult<bool> {
        let permit = tokio::time::timeout(PERMIT_WAIT, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| AppError::Unavailable)?
            .expect("semaphore is never closed");

        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let Ok(parsed) = PasswordHash::new(&hash) else {
                return false;
            };
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok()
        })
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("verify task panicked: {e}")))
    }

    pub async fn hash(&self, password: String) -> AppResult<String> {
        let permit = tokio::time::timeout(PERMIT_WAIT, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| AppError::Unavailable)?
            .expect("semaphore is never closed");

        let hashed = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let salt = SaltString::generate(&mut OsRng);
            Argon2::default()
                .hash_password(password.as_bytes(), &salt)
                .map(|hash| hash.to_string())
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("hash task panicked: {e}")))?;

        hashed.map_err(|e| AppError::Internal(anyhow::anyhow!("argon2 hashing failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn corrupt_hash_never_authenticates() {
        let worker = PasswordWorker::new(NonZeroUsize::new(2).unwrap());

        assert!(!worker
            .verify("anything".to_string(), "not-a-hash".to_string())
            .await
            .unwrap());
    }

    /// A client disconnect drops the handler future. The blocking task keeps
    /// running regardless, so the permit has to stay taken or the cap is free to
    /// be bypassed by hanging up mid-request.
    #[tokio::test]
    async fn abandoned_work_keeps_its_permit() {
        let worker = PasswordWorker::new(NonZeroUsize::new(1).unwrap());
        let hash = worker.hash("password".to_string()).await.unwrap();

        let abandoned = worker.verify("password".to_string(), hash);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), abandoned)
                .await
                .is_err(),
            "hashing should still be in flight"
        );

        assert_eq!(worker.permits.available_permits(), 0);
    }

    #[tokio::test]
    async fn saturated_permits_shed_instead_of_queueing() {
        let worker = PasswordWorker::new(NonZeroUsize::new(1).unwrap());
        let held = worker.permits.clone().acquire_owned().await.unwrap();

        let err = worker
            .verify("password".to_string(), "not-a-hash".to_string())
            .await
            .unwrap_err();

        assert!(matches!(err, AppError::Unavailable));
        drop(held);
    }
}
