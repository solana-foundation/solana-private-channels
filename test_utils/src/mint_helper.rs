use private_channel_indexer::{
    error::StorageError,
    storage::{
        common::{
            amount::TokenAmount,
            models::{DbMint, DbMintStatus},
        },
        Storage,
    },
};

/// Withdraw fee the e2e harnesses allow mints with. The operator writes it to
/// the channel on every deposit, so every withdrawal after one pays it.
pub const TEST_WITHDRAW_FEE: u64 = 1_000;

/// Minimum withdrawal the e2e harnesses allow mints with. Kept at 1 so every
/// amount these suites withdraw stays valid.
pub const TEST_MIN_WITHDRAW_AMOUNT: u64 = 1;

/// Minimum deposit the e2e harnesses allow mints with. 0 so every amount these
/// suites deposit stays valid.
pub const TEST_MIN_DEPOSIT_AMOUNT: u64 = 0;

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
        .upsert_mints_batch(&[DbMint {
            allow_mint_slot: effective_slot,
            ..DbMint::new(
                mint_address.to_string(),
                decimals,
                token_program.to_string(),
                TokenAmount(TEST_WITHDRAW_FEE),
            )
        }])
        .await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            mint_address: mint_address.to_string(),
            status: "allowed".to_string(),
            withdrawals_blocked: false,
            effective_slot,
            transaction_index: 0,
            instruction_index: 0,
            inner_index: None,
            signature: format!("test-seed-{mint_address}"),
            created_at: chrono::Utc::now(),
        }])
        .await?;
    Ok(())
}
