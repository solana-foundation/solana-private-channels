use crate::{error::StorageError, storage::common::storage::Storage};
use chrono::{DateTime, Utc};

/// Outcome of a pre-broadcast requeue attempt. The cap is enforced inside the
/// single write so the caller never needs a separate counter read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueOutcome {
    /// Row was Processing and under the cap: flipped to Pending. `attempts` is the
    /// new durable counter after the increment.
    Requeued { attempts: i32 },
    /// Row was Processing but at/over the cap: left Processing, not requeued.
    /// The write still matched the row and bumped `updated_at`; `lease` is the
    /// new value, which a later fenced write on this incarnation must present.
    AtCap { lease: DateTime<Utc> },
    /// The row is no longer the Processing incarnation this lease names, so
    /// nothing was written.
    NotProcessing,
}

/// Cap-gated CAS `Processing` → `Pending` on `expected_updated_at`. Requeues only
/// when the durable counter is below `max_attempts`, incrementing it in the same
/// statement; a Processing row at the cap is left Processing (`AtCap`). Folding
/// the cap into the write means there is no separate counter read that could fail
/// and let the row requeue forever.
pub async fn try_requeue_prebroadcast(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: DateTime<Utc>,
    max_attempts: i32,
) -> Result<RequeueOutcome, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_requeue_prebroadcast_internal(transaction_id, expected_updated_at, max_attempts)
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_requeue_prebroadcast(transaction_id, expected_updated_at, max_attempts)
                .await
        }
    }
}
