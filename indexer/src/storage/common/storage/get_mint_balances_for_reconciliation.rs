use crate::{
    error::StorageError,
    storage::common::{models::MintDbBalance, storage::Storage},
};

/// Slots are far below `i64::MAX`, so the clamp is unreachable in practice and only
/// keeps an absurd value from wrapping into a negative bound that matches nothing.
fn slot_bound(as_of_slot: u64) -> i64 {
    i64::try_from(as_of_slot).unwrap_or(i64::MAX)
}

pub async fn get_mint_balances_for_reconciliation(
    storage: &Storage,
    as_of_slot: u64,
) -> Result<Vec<MintDbBalance>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_mint_balances_for_reconciliation_internal(slot_bound(as_of_slot))
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock) => mock.get_mint_balances_for_reconciliation(as_of_slot).await,
    }
}
