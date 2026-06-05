use private_channel_indexer::{
    error::StorageError,
    storage::{
        common::models::{DbMint, DbMintStatus},
        Storage,
    },
};

/// Test helper: seed a mint AND a slot-0 `allowed` history entry so the
/// operator gate (`assert_mint_allowed_at_slot`) and the reconciliation
/// orphan query both treat the mint as allowed for any deposit slot.
pub async fn seed_allowed_mint(
    storage: &Storage,
    mint_address: &str,
    decimals: i16,
    token_program: &str,
    effective_slot: i64,
) -> Result<(), StorageError> {
    storage
        .upsert_mints_batch(&[DbMint::new(
            mint_address.to_string(),
            decimals,
            token_program.to_string(),
        )])
        .await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            mint_address: mint_address.to_string(),
            status: "allowed".to_string(),
            effective_slot,
            signature: format!("test-seed-{mint_address}"),
            created_at: chrono::Utc::now(),
        }])
        .await?;
    Ok(())
}
