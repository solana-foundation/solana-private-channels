use crate::{
    error::StorageError,
    storage::common::{models::MintStatusAtSlot, storage::Storage},
};

/// Resolve a mint's status for a deposit at `slot`: `Allowed` if the status
/// coming into the slot is allowed or any change inside the slot is. Returns
/// `NeverAllowed` when no history row exists at or before that slot.
pub async fn get_mint_status_at_slot(
    storage: &Storage,
    mint_address: &str,
    slot: i64,
) -> Result<MintStatusAtSlot, StorageError> {
    match storage {
        Storage::Postgres(db) => {
            db.get_mint_status_at_slot_internal(mint_address, slot)
                .await
        }
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_mint_status_at_slot(mint_address, slot).await,
    }
}
