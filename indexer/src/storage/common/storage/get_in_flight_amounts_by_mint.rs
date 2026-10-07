use crate::{
    error::StorageError,
    storage::common::{
        models::MintInFlightAmount, storage::get_mint_balances_for_reconciliation::slot_bound,
        storage::Storage,
    },
};

/// Per-mint sum of every unsettled transaction amount (the in-flight envelope), plus settled
/// deposits above `deposits_after` and settled withdrawals above `withdrawals_after`.
/// A `None` bound skips that arm.
pub async fn get_in_flight_amounts_by_mint(
    storage: &Storage,
    deposits_after: Option<u64>,
    withdrawals_after: Option<u64>,
) -> Result<Vec<MintInFlightAmount>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db
            .get_in_flight_amounts_by_mint_internal(
                deposits_after.map(slot_bound),
                withdrawals_after.map(slot_bound),
            )
            .await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => {
            mock_db
                .get_in_flight_amounts_by_mint(deposits_after, withdrawals_after)
                .await
        }
    }
}
