use crate::{
    error::StorageError,
    storage::common::{models::MintInFlightAmount, storage::Storage},
};

/// Per-mint sum of every unsettled transaction amount (the in-flight envelope).
pub async fn get_in_flight_amounts_by_mint(
    storage: &Storage,
) -> Result<Vec<MintInFlightAmount>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_in_flight_amounts_by_mint_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_in_flight_amounts_by_mint().await,
    }
}
