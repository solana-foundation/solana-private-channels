use crate::{error::StorageError, storage::common::storage::Storage};

/// CAS `Processing` → `ManualReview`; reason rides on the webhook, not DB.
/// The optional signature arrays are recorded on the row in the same write, so
/// the evidence outlives the release-signature journal's GC. `None` leaves both
/// columns untouched.
pub async fn try_quarantine_processing(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    remint_signatures: Option<Vec<String>>,
    remint_last_valid_block_heights: Option<Vec<i64>>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_quarantine_processing_internal(
                transaction_id,
                expected_updated_at,
                remint_signatures,
                remint_last_valid_block_heights,
            )
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_quarantine_processing(
                    transaction_id,
                    expected_updated_at,
                    remint_signatures,
                    remint_last_valid_block_heights,
                )
                .await
        }
    }
}
