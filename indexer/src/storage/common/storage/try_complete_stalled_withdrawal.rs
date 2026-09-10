use crate::{
    error::StorageError,
    storage::common::{models::TransactionStatus, storage::Storage},
};

/// CAS a stalled withdrawal (`ManualReview` or `PendingRemint`) to `Completed`
/// on `updated_at`; `Ok(false)` if stale or the guard rejects the row.
pub async fn try_complete_stalled_withdrawal(
    storage: &Storage,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    from_status: TransactionStatus,
    counterpart_signature: Option<String>,
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .try_complete_stalled_withdrawal_internal(
                transaction_id,
                expected_updated_at,
                from_status,
                counterpart_signature,
            )
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .try_complete_stalled_withdrawal(
                    transaction_id,
                    expected_updated_at,
                    from_status,
                    counterpart_signature,
                )
                .await
        }
    }
}
