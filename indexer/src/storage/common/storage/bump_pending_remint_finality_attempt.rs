use crate::{error::StorageError, storage::common::storage::Storage};

pub async fn bump_pending_remint_finality_attempt(
    storage: &Storage,
    transaction_id: i64,
    attempts: i32,
    new_deadline: chrono::DateTime<chrono::Utc>,
) -> Result<(), StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.bump_pending_remint_finality_attempt_internal(
                transaction_id,
                attempts,
                new_deadline,
            )
            .await?;
            Ok(())
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .bump_pending_remint_finality_attempt(transaction_id, attempts, new_deadline)
                .await
        }
    }
}
