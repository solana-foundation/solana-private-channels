use crate::{error::StorageError, storage::common::storage::Storage};

/// Mark active withdrawals at or above `min_nonce` as `ManualReview`.
///
/// Called once per poison-pill detection in the withdrawal pipeline.
/// Halting the pipeline instead of skipping the single bad row is
/// deliberate: the processor rotates the withdrawal bitmap whenever it
/// crosses a boundary nonce, and once the bitmap has rotated past the
/// quarantined row's generation the on-chain program rejects that nonce
/// permanently. Stopping keeps the row re-armable until a human decides.
///
/// `min_nonce` bounds the sweep to the poison row's own nonce and above.
/// A row below it may already be signed or in flight, and terminalizing one
/// would discard its eventual `Completed` write, leaving the next boot's
/// bitmap diff seeing a set bit with no `Completed` row. `None` keeps the
/// sweep unbounded,
/// which is the fallback when the poison row carries no nonce.
///
/// `exclude_id` is the poison row already quarantined via the async storage
/// writer. Excluding it here prevents a duplicate `ManualReview` webhook if
/// the async update has not yet committed.
///
/// Terminal statuses (Completed, Failed, ManualReview, PendingRemint) are
/// left alone so operators don't get re-alerted on already-handled rows.
pub async fn quarantine_active_withdrawals(
    storage: &Storage,
    exclude_id: Option<i64>,
    min_nonce: Option<i64>,
) -> Result<u64, StorageError> {
    match storage {
        Storage::Postgres(db) => db
            .quarantine_active_withdrawals_internal(exclude_id, min_nonce)
            .await
            .map_err(StorageError::from),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .quarantine_active_withdrawals(exclude_id, min_nonce)
                .await
        }
    }
}
