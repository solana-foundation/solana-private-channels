use crate::{error::StorageError, storage::common::storage::Storage};

/// Claim the exclusive right to broadcast one remint attempt.
/// `Ok(false)` means another sender owns the live attempt: do not broadcast.
pub async fn claim_remint_attempt(
    storage: &Storage,
    transaction_id: i64,
    signature: String,
    last_valid_block_height: i64,
    blockhash_slot: Option<i64>,
    superseded_signatures: &[String],
) -> Result<bool, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .claim_remint_attempt_internal(
                transaction_id,
                signature,
                last_valid_block_height,
                blockhash_slot,
                superseded_signatures,
            )
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => {
            mock.claim_remint_attempt(
                transaction_id,
                signature,
                last_valid_block_height,
                blockhash_slot,
                superseded_signatures,
            )
            .await
        }
    }
}
