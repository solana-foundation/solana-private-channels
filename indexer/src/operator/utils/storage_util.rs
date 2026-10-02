use crate::error::StorageError;
use crate::storage::common::models::TransactionStatus;
use crate::storage::Storage;
use std::future::Future;
use tracing::{error, warn};

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

/// Run a terminal write fenced to one incarnation of the row, and return
/// whether the caller should send its status update, which is what pages.
/// `reason` is the caller's error message, logged whenever nothing is sent.
///
/// On `Ok(false)` the row is re-read. A row already in `status` is most likely
/// this write landing on an attempt whose reply was lost, so it pages; a
/// duplicate is harmless. Any other row has moved past this incarnation and
/// stays silent. A write or re-read that still fails after its retries sends
/// nothing. If the write never committed, the row is still Processing and
/// recovery redoes it; if it committed with its reply lost, nothing retries it
/// and the ERROR line is its only record.
pub(crate) async fn fenced_terminal_write<F, Fut>(
    storage: &Storage,
    op_name: &str,
    transaction_id: i64,
    status: TransactionStatus,
    reason: &str,
    write: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, StorageError>>,
{
    let verified = match with_storage_backoff(op_name, transaction_id, write).await {
        Ok(true) => return true,
        Ok(false) => with_storage_backoff(&format!("{op_name} re-read"), transaction_id, || {
            storage.get_transaction_status(transaction_id)
        })
        .await
        .map(|current| current == Some(status)),
        Err(e) => Err(e),
    };
    match verified {
        Ok(true) => true,
        Ok(false) => {
            warn!(
                transaction_id,
                reason,
                "{op_name}: the row has moved past this incarnation; not writing or alerting"
            );
            false
        }
        Err(e) => {
            error!(
                transaction_id,
                reason, "{op_name} could not be verified; if it committed, nothing retries it: {e}"
            );
            false
        }
    }
}
