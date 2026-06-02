use crate::{
    error::StorageError,
    storage::common::{models::MintStatusAtSlot, storage::Storage},
};

/// Resolve a mint's status as of `slot` by reading the latest history row
/// with `effective_slot <= slot`. Returns `NeverAllowed` when no such row
/// exists (the mint has never been allowlisted at or before that slot).
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
