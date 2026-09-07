use crate::error::StorageError;
use std::future::Future;
use tracing::warn;

/// Run a storage read or write, retrying a transient error a few times with a
/// short backoff. A brief DB blip should not force the caller's failure path on
/// the first error; an exhausted retry returns the error for the caller to
/// handle. `transaction_id` and `op_name` are for log context only.
pub(crate) async fn with_storage_backoff<T, F, Fut>(
    op_name: &str,
    transaction_id: i64,
    mut op: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    const ATTEMPTS: u32 = 3;
    const BACKOFF_MS: u64 = 100;

    let mut last_err = None;
    for attempt in 0..ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if attempt + 1 < ATTEMPTS {
                    warn!(
                        transaction_id,
                        attempt, "{op_name} failed; retrying after backoff: {e}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(BACKOFF_MS)).await;
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.expect("loop body runs at least once"))
}
