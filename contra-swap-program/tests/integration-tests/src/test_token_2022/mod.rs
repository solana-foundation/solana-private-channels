//! Token-2022 integration tests for the Contra Swap Program.
//!
//! Coverage:
//! - **Positive lifecycle** on Token-2022 mints with each *allowed*
//!   amount-preserving extension applied to one leg in isolation
//!   (Pausable, PermanentDelegate, TransferHook, plain T22 with no
//!   extensions). Both single-program-T22 and mixed (legacy ↔ T22)
//!   topologies are exercised.
//! - **CreateDvp negative** for every *blocked* extension on either leg
//!   (TransferFeeConfig, InterestBearingConfig, ScaledUiAmount,
//!   ConfidentialTransferMint).
//! - **Owner mismatch**: legacy SPL mint paired with T22 token program.
//! - **Post-Create extension activation**: a mint is swapped to a
//!   blocked-extension layout *after* CreateDvp, and Settle/Reject must
//!   still drain funds (the unwind paths intentionally skip the
//!   extension check so funds can never get stranded).

use contra_swap_program_client::instructions::CreateDvpBuilder;
use solana_sdk::signature::Signer;

use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_b, assert_reject_dvp, assert_settle_dvp,
        setup_dvp_with_programs, AMOUNT_A, AMOUNT_B, INITIAL_BALANCE,
    },
    utils::{
        assert_program_error, get_token_balance, set_mint_2022_with_confidential_transfer,
        set_mint_2022_with_interest_bearing, set_mint_2022_with_pausable,
        set_mint_2022_with_permanent_delegate, set_mint_2022_with_scaled_ui_amount,
        set_mint_2022_with_transfer_fee, set_mint_2022_with_transfer_hook, TestContext,
        BLOCKED_MINT_EXTENSION, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID,
    },
};

// ---------------------------------------------------------------------
// Positive lifecycle on Token-2022.
// ---------------------------------------------------------------------

/// Both legs are bare Token-2022 mints (no extensions). The full
/// Create → fund → Settle path must work end-to-end.
#[test]
fn test_full_lifecycle_both_legs_token_2022() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);
    assert_settle_dvp(&mut context, &fixture);

    // Each user receives the *other* leg's mint at exactly amount_x.
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    // Originating ATAs are debited by exactly amount_x.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE - AMOUNT_B
    );
    assert!(context.get_account(&fixture.swap_dvp).is_none());
}

/// Cross-program lifecycle: asset leg is legacy SPL Token, cash leg is
/// Token-2022. The per-leg `token_program_a` / `token_program_b`
/// account lets these coexist in a single trade.
#[test]
fn test_full_lifecycle_mixed_legacy_and_token_2022() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);
    assert_settle_dvp(&mut context, &fixture);

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

/// Allowed extension: PermanentDelegate on `mint_b`. Trade settles
/// normally; the extension does not affect amount mechanics.
#[test]
fn test_settle_with_permanent_delegate_on_mint_b() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    // Overwrite mint_b in place with a PermanentDelegate-bearing T22 mint.
    set_mint_2022_with_permanent_delegate(
        &mut context,
        &fixture.mint_b,
        &fixture.settlement_authority.pubkey(),
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);
    assert_settle_dvp(&mut context, &fixture);

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

/// Allowed extension: Pausable on `mint_a` (initialized in the unpaused
/// state). Trade settles normally.
#[test]
fn test_settle_with_pausable_on_mint_a() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    set_mint_2022_with_pausable(
        &mut context,
        &fixture.mint_a,
        &fixture.settlement_authority.pubkey(),
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);
    assert_settle_dvp(&mut context, &fixture);

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

/// Allowed at CreateDvp: TransferHook is *not* in the deny-list. The
/// program forwards transfer-hook extras through every `TransferChecked`
/// CPI (Settle/Cancel/Reject/Reclaim), so a hook-bearing mint can run
/// the full lifecycle when the client passes the resolved extras as
/// trailing accounts. End-to-end Settle/Cancel/Reject coverage requires
/// a deployed hook program in the SVM and is deferred; this test pins
/// only the Create-time acceptance.
#[test]
fn test_create_accepts_transfer_hook_on_mint_a() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    // Hook program ID is arbitrary — Create only checks for hook
    // *presence* on the deny-list (it isn't on it), so any non-zero
    // pubkey suffices. Use the swap program's own ID as a placeholder
    // we already have in scope.
    set_mint_2022_with_transfer_hook(
        &mut context,
        &fixture.mint_a,
        &contra_swap_program_client::CONTRA_SWAP_PROGRAM_ID,
        &fixture.settlement_authority.pubkey(),
    );

    assert_create_dvp(&mut context, &fixture);
}

// ---------------------------------------------------------------------
// CreateDvp negative tests — one per blocked extension on either leg.
// ---------------------------------------------------------------------

/// Helper: derive the standard CreateDvp instruction from a fixture.
/// Used by the blocked-extension tests so they stay focused on the
/// rejection rather than rebuilding the full builder chain.
fn build_create_dvp_ix(
    context: &TestContext,
    fixture: &crate::state_utils::DvpFixture,
) -> solana_sdk::instruction::Instruction {
    CreateDvpBuilder::new()
        .payer(context.payer.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .user_a(fixture.user_a.pubkey())
        .user_b(fixture.user_b.pubkey())
        .settlement_authority(fixture.settlement_authority.pubkey())
        .amount_a(AMOUNT_A)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(fixture.expiry)
        .nonce(fixture.nonce)
        .instruction()
}

#[test]
fn test_create_rejects_transfer_fee_on_mint_a() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    set_mint_2022_with_transfer_fee(
        &mut context,
        &fixture.mint_a,
        &fixture.settlement_authority.pubkey(),
        100,    // 1% fee
        10_000, // max
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

#[test]
fn test_create_rejects_transfer_fee_on_mint_b() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    set_mint_2022_with_transfer_fee(
        &mut context,
        &fixture.mint_b,
        &fixture.settlement_authority.pubkey(),
        100,
        10_000,
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

#[test]
fn test_create_rejects_interest_bearing_on_mint_a() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    set_mint_2022_with_interest_bearing(
        &mut context,
        &fixture.mint_a,
        &fixture.settlement_authority.pubkey(),
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

#[test]
fn test_create_rejects_scaled_ui_amount_on_mint_b() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    set_mint_2022_with_scaled_ui_amount(
        &mut context,
        &fixture.mint_b,
        &fixture.settlement_authority.pubkey(),
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

#[test]
fn test_create_rejects_confidential_transfer_on_mint_a() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    set_mint_2022_with_confidential_transfer(
        &mut context,
        &fixture.mint_a,
        &fixture.settlement_authority.pubkey(),
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

// ---------------------------------------------------------------------
// Mint / token-program ownership mismatch.
// ---------------------------------------------------------------------

/// Caller passes `token_program_a = TOKEN_2022_PROGRAM_ID` for a mint
/// that's actually owned by legacy SPL Token. `verify_account_owner`
/// rejects it with InvalidAccountOwner (program error code 23).
#[test]
fn test_create_rejects_mint_program_owner_mismatch() {
    let mut context = TestContext::new();
    // Setup with legacy SPL on both legs (so the mints exist and are
    // legacy-owned), then override `token_program_a` on the fixture.
    let mut fixture = setup_dvp_with_programs(&mut context, 0, TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID);
    fixture.token_program_a = TOKEN_2022_PROGRAM_ID;
    // Re-derive `dvp_ata_a` for the new (mismatching) program — otherwise
    // we'd fail an earlier check (canonical-ATA) instead of the owner one.
    // But the bound here is on the mint's owner being the program we
    // pass, which check fires before the ATA check on the same program.
    // Recompute the ATA so the ATA check passes and the owner check is
    // the actual failure surfaced.
    fixture.dvp_ata_a = crate::utils::dvp_ata(
        &fixture.swap_dvp,
        &fixture.mint_a,
        &fixture.token_program_a,
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    let err = context.send(ix, &[]).expect_err("owner mismatch must fail");
    assert!(
        err.contains("InvalidAccountOwner"),
        "expected InvalidAccountOwner, got: {err}"
    );
}

// ---------------------------------------------------------------------
// Defense-in-depth removed by design: extension validation runs only at
// Create. The two tests below pin that behaviour by activating a
// blocked extension *after* Create and confirming the unwind paths
// (Settle, Reject) still drain funds — the property the design hinges
// on so that funds can never get stranded.
// ---------------------------------------------------------------------

/// Create with bare T22 mints, then mutate `mint_a` to bear an
/// `InterestBearingConfig` extension *after* Create. InterestBearing is
/// on the deny-list (it would be rejected if Create re-ran) but it does
/// not affect the raw `TransferChecked` amount path — UI scaling is
/// applied client-side, the on-chain transfer amount is the raw value.
///
/// Settle must still succeed: the unwind paths intentionally skip the
/// extension check so a post-Create extension activation can never
/// strand the trade.
#[test]
fn test_settle_succeeds_after_post_create_interest_bearing_activation() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    // Swap mint_a in place to an InterestBearing-bearing T22 mint —
    // would fail Create's deny-list now, but Settle does not re-check.
    set_mint_2022_with_interest_bearing(
        &mut context,
        &fixture.mint_a,
        &fixture.settlement_authority.pubkey(),
    );

    assert_settle_dvp(&mut context, &fixture);

    // Cross delivery occurred at exactly the agreed amounts.
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    assert!(context.get_account(&fixture.swap_dvp).is_none());
}

/// Same property on the Reject unwind path: post-Create, `mint_a` is
/// swapped to a Pausable T22 mint (allowed, but the point is that
/// Reject does no extension check regardless). Reject must still drain
/// both legs back to their depositors.
#[test]
fn test_reject_succeeds_after_post_create_extension_change() {
    let mut context = TestContext::new();
    let fixture =
        setup_dvp_with_programs(&mut context, 0, TOKEN_2022_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    set_mint_2022_with_pausable(
        &mut context,
        &fixture.mint_a,
        &fixture.settlement_authority.pubkey(),
    );

    assert_reject_dvp(&mut context, &fixture, &fixture.user_a);

    // Both depositors recover their full balances.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE
    );
    assert!(context.get_account(&fixture.swap_dvp).is_none());
}
