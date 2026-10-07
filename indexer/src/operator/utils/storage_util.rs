use crate::error::StorageError;
use crate::metrics;
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

/// What a fenced terminal write did, which decides whether the caller alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FencedWrite {
    /// The row holds the target status: this write applied, or an earlier attempt did.
    Applied,
    /// The row has moved past this incarnation, so nothing is sent.
    Stale,
    /// Still failing after retries. It may have committed, and nothing retries a
    /// committed write, so the caller alerts anyway, without writing.
    Unverified,
}

impl FencedWrite {
    /// The alert's error message, flagged when the write could not be verified.
    pub(crate) fn alert_message(self, reason: &str) -> String {
        match self {
            FencedWrite::Unverified => {
                format!("{reason} (status write unverified; check the row)")
            }
            FencedWrite::Applied | FencedWrite::Stale => reason.to_string(),
        }
    }
}

/// Run a terminal write fenced to one incarnation of the row, and report what
/// it did. `reason` is the caller's error message, logged unless it applied.
///
/// On `Ok(false)` the row is re-read. A row already in `status` is most likely
/// this write landing on an attempt whose reply was lost, so it counts as
/// applied; a duplicate page is harmless. Any other row has moved past this
/// incarnation. A write or re-read that still fails after its retries is
/// unverified. If that write never committed, the row is still Processing and
/// recovery redoes it, paging again.
///
/// The writer never sees these writes, so they are counted here as it counts its own.
pub(crate) async fn fenced_terminal_write<F, Fut>(
    storage: &Storage,
    pt: &str,
    op_name: &str,
    transaction_id: i64,
    status: TransactionStatus,
    reason: &str,
    write: F,
) -> FencedWrite
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, StorageError>>,
{
    let verified = match with_storage_backoff(op_name, transaction_id, write).await {
        Ok(true) => Ok(true),
        Ok(false) => with_storage_backoff(&format!("{op_name} re-read"), transaction_id, || {
            storage.get_transaction_status(transaction_id)
        })
        .await
        .map(|current| current == Some(status)),
        Err(e) => Err(e),
    };
    match verified {
        Ok(true) => {
            metrics::OPERATOR_DB_UPDATES
                .with_label_values(&[pt, &format!("{status:?}")])
                .inc();
            FencedWrite::Applied
        }
        Ok(false) => {
            warn!(
                transaction_id,
                reason,
                "{op_name}: the row has moved past this incarnation; not writing or alerting"
            );
            FencedWrite::Stale
        }
        Err(e) => {
            error!(
                transaction_id,
                reason, "{op_name} could not be verified; alerting without writing: {e}"
            );
            metrics::OPERATOR_DB_UPDATE_ERRORS
                .with_label_values(&[pt])
                .inc();
            FencedWrite::Unverified
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::storage::mock::MockStorage;

    /// A fenced write never reaches the writer, so it counts its own update or
    /// the DB Updates panel misses every Failed and ManualReview.
    #[tokio::test]
    async fn an_applied_fenced_write_counts_as_a_db_update() {
        // Only this test uses the label, so no parallel test moves the series.
        let label = "test_fenced_write_applied";
        let updates = metrics::OPERATOR_DB_UPDATES.with_label_values(&[label, "ManualReview"]);
        let before = updates.get();

        let outcome = fenced_terminal_write(
            &Storage::Mock(MockStorage::new()),
            label,
            "quarantine",
            1,
            TransactionStatus::ManualReview,
            "withdrawals blocked",
            || async { Ok(true) },
        )
        .await;

        assert_eq!(outcome, FencedWrite::Applied);
        assert_eq!(updates.get() - before, 1.0);
    }

    /// A fenced write that still fails after its retries counts as an update
    /// error, as a failed write in the writer does.
    #[tokio::test]
    async fn an_unverified_fenced_write_counts_as_a_db_update_error() {
        // Only this test uses the label, so no parallel test moves the series.
        let label = "test_fenced_write_unverified";
        let errors = metrics::OPERATOR_DB_UPDATE_ERRORS.with_label_values(&[label]);
        let before = errors.get();

        let outcome = fenced_terminal_write(
            &Storage::Mock(MockStorage::new()),
            label,
            "failure",
            1,
            TransactionStatus::Failed,
            "program error",
            || async {
                Err(StorageError::DatabaseError {
                    message: "connection reset".to_string(),
                })
            },
        )
        .await;

        assert_eq!(outcome, FencedWrite::Unverified);
        assert_eq!(errors.get() - before, 1.0);
    }
}
