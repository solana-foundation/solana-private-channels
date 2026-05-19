use crate::{error::StorageError, storage::common::storage::Storage};

/// `transactions.id` for every `deposit` row whose `mint` has no entry in
/// `mints` (the allowlist populated from on-chain `AllowMint`). A non-empty
/// result means the indexer recorded a deposit for a mint that was never
/// allowlisted, a trust-boundary leak. Reconciliation queries this to alert
/// on any such rows; they describe the same condition the deposit-side
/// gate (`assert_mint_allowlisted`) refuses at process time.
///
/// IDs (not mints) are returned so reconciliation's per-tick dedup detects
/// *each new* orphan deposit, not just each new orphan mint — a single
/// orphan mint can accumulate many stuck deposits and each one is its own
/// incident.
pub async fn get_orphan_deposit_ids(storage: &Storage) -> Result<Vec<i64>, StorageError> {
    match storage {
        Storage::Postgres(db) => Ok(db.get_orphan_deposit_ids_internal().await?),
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(mock_db) => mock_db.get_orphan_deposit_ids().await,
    }
}
