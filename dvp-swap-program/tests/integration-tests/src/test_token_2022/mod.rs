//! Token-2022 integration tests for the DvP Swap Program.
//!
//! Coverage:
//! - **Positive lifecycle** on Token-2022 mints with each *allowed*
//!   amount-preserving extension applied to one leg in isolation
//!   (Pausable, PermanentDelegate, TransferHook, plain T22 with no
//!   extensions). Both single-program-T22 and mixed (legacy ↔ T22)
//!   topologies are exercised.
//! - **CreateDvp negative** for every *blocked* extension on either leg
//!   (TransferFeeConfig, InterestBearingConfig, ScaledUiAmount,
//!   ConfidentialTransferMint, NonTransferable). `ConfidentialTransferFeeConfig`
//!   is also blocked but can't exist without `ConfidentialTransferMint`, so the
//!   ConfidentialTransferMint test covers that path.
//! - **Owner mismatch**: legacy SPL mint paired with T22 token program.
//! - **Post-Create extension activation**: a mint is swapped to a
//!   blocked-extension layout *after* CreateDvp, and Settle/Reject must
//!   still drain funds (the unwind paths intentionally skip the
//!   extension check so funds can never get stranded).

use dvp_swap_program_client::instructions::{
    CancelDvpBuilder, CreateDvpBuilder, ReclaimDvpBuilder, RejectDvpBuilder, SettleDvpBuilder,
};
use solana_sdk::{account::Account, signature::Signer};

use crate::{
    state_utils::{
        assert_cancel_dvp, assert_create_dvp, assert_fund_a, assert_fund_a_amount, assert_fund_b,
        assert_fund_b_amount, assert_reclaim_a, assert_reject_dvp, assert_settle_dvp,
        setup_dvp_with_programs, AMOUNT_A, AMOUNT_B, INITIAL_BALANCE,
    },
    utils::{
        assert_instruction_error, assert_program_error, get_token_balance, hook_extras_for_mint,
        set_mint_2022_with_confidential_transfer, set_mint_2022_with_interest_bearing,
        set_mint_2022_with_non_transferable, set_mint_2022_with_pausable,
        set_mint_2022_with_permanent_delegate, set_mint_2022_with_scaled_ui_amount,
        set_mint_2022_with_transfer_fee, set_mint_2022_with_transfer_hook,
        set_token_2022_with_hook_account, set_token_2022_with_memo_required, setup_hook_mint,
        TestContext, BLOCKED_MINT_EXTENSION, MEMO_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
        TOKEN_PROGRAM_ID,
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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(&mut context, 0, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID);

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    // Hook program ID is arbitrary — Create only checks for hook
    // *presence* on the deny-list (it isn't on it), so any non-zero
    // pubkey suffices. Use the swap program's own ID as a placeholder
    // we already have in scope.
    set_mint_2022_with_transfer_hook(
        &mut context,
        &fixture.mint_a,
        &dvp_swap_program_client::DVP_SWAP_PROGRAM_ID,
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
        .nonce_tombstone(fixture.nonce_tombstone)
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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    set_mint_2022_with_scaled_ui_amount(
        &mut context,
        &fixture.mint_b,
        &fixture.settlement_authority.pubkey(),
    );

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

/// `NonTransferable` permanently blocks transfers out of any escrow, so
/// a balance reaching it could never be settled, refunded, or reclaimed.
/// Create must reject the mint up front.
#[test]
fn test_create_rejects_non_transferable_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    set_mint_2022_with_non_transferable(&mut context, &fixture.mint_a);

    let ix = build_create_dvp_ix(&context, &fixture);
    assert_program_error(context.send(ix, &[]), BLOCKED_MINT_EXTENSION);
}

#[test]
fn test_create_rejects_confidential_transfer_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
/// rejects it with `InvalidAccountOwner`.
#[test]
fn test_create_rejects_mint_program_owner_mismatch_a() {
    let mut context = TestContext::new();
    let mut fixture = setup_dvp_with_programs(&mut context, 0, TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID);
    fixture.token_program_a = TOKEN_2022_PROGRAM_ID;
    // Re-derive `dvp_ata_a` for the new program so the ATA-canonicality
    // check passes and the owner check is the actual failure surfaced.
    fixture.dvp_ata_a =
        crate::utils::dvp_ata(&fixture.swap_dvp, &fixture.mint_a, &fixture.token_program_a);

    let result = context.send(build_create_dvp_ix(&context, &fixture), &[]);
    assert_instruction_error(result, "InvalidAccountOwner");
}

/// Symmetric leg-B mismatch.
#[test]
fn test_create_rejects_mint_program_owner_mismatch_b() {
    let mut context = TestContext::new();
    let mut fixture = setup_dvp_with_programs(&mut context, 0, TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID);
    fixture.token_program_b = TOKEN_2022_PROGRAM_ID;
    fixture.dvp_ata_b =
        crate::utils::dvp_ata(&fixture.swap_dvp, &fixture.mint_b, &fixture.token_program_b);

    let result = context.send(build_create_dvp_ix(&context, &fixture), &[]);
    assert_instruction_error(result, "InvalidAccountOwner");
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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

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

// ---------------------------------------------------------------------
// TransferHook end-to-end: the test hook fixture (`HOOK_FIXTURE_PROGRAM_ID`)
// logs `hook accounts: N` on every Execute. With one declared extra in
// the `ExtraAccountMetaList` (the system program), Token-2022 invokes
// the hook with 6 accounts: source, mint, destination, authority,
// validation PDA, system program. Each test asserts the log appears the
// expected number of times for that lifecycle path.
// ---------------------------------------------------------------------

const HOOK_LOG: &str = "hook accounts: 6";

/// Installs the hook fixture on `mint_a` and rewrites the user ATAs that
/// hold it so they carry the `TransferHookAccount` extension. The escrow
/// `dvp_ata_a` is created by the swap program's CPI to the ATA program
/// *after* the mint already bears `TransferHook`, so it gets the
/// extension automatically — only the pre-existing user ATAs need
/// fix-up here.
fn setup_hook_on_mint_a(context: &mut TestContext, fixture: &crate::state_utils::DvpFixture) {
    setup_hook_mint(context, &fixture.mint_a);
    set_token_2022_with_hook_account(
        context,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.user_a.pubkey(),
        INITIAL_BALANCE,
    );
    set_token_2022_with_hook_account(
        context,
        &fixture.user_b_ata_a,
        &fixture.mint_a,
        &fixture.user_b.pubkey(),
        0,
    );
}

/// Funds leg A by issuing a `transfer_checked` with the hook trailing
/// accounts appended — the raw SPL transfer used as the canonical funding
/// path needs the same extras the swap program forwards on settle.
fn fund_a_with_hook(context: &mut TestContext, fixture: &crate::state_utils::DvpFixture) {
    let extras = hook_extras_for_mint(&fixture.mint_a);
    let mut ix = spl_token_2022::instruction::transfer_checked(
        &TOKEN_2022_PROGRAM_ID,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.dvp_ata_a,
        &fixture.user_a.pubkey(),
        &[],
        AMOUNT_A,
        6,
    )
    .expect("build transfer_checked");
    ix.accounts.extend_from_slice(&extras);
    context
        .send(ix, &[&fixture.user_a])
        .expect("fund A with hook");
}

#[test]
fn test_settle_with_hook_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );
    setup_hook_on_mint_a(&mut context, &fixture);

    assert_create_dvp(&mut context, &fixture);
    fund_a_with_hook(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let leg_a_extras = hook_extras_for_mint(&fixture.mint_a);
    let ix = SettleDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(leg_a_extras.len() as u8)
        .add_remaining_accounts(&leg_a_extras)
        .instruction();
    let meta = context
        .send(ix, &[&fixture.settlement_authority])
        .expect("settle with hook");

    let hits = meta.logs.iter().filter(|l| l.contains(HOOK_LOG)).count();
    assert_eq!(hits, 1, "hook should run exactly once for the asset leg");

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

#[test]
fn test_cancel_with_hook_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );
    setup_hook_on_mint_a(&mut context, &fixture);

    assert_create_dvp(&mut context, &fixture);
    fund_a_with_hook(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let leg_a_extras = hook_extras_for_mint(&fixture.mint_a);
    let ix = CancelDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(leg_a_extras.len() as u8)
        .add_remaining_accounts(&leg_a_extras)
        .instruction();
    let meta = context
        .send(ix, &[&fixture.settlement_authority])
        .expect("cancel with hook");

    let hits = meta.logs.iter().filter(|l| l.contains(HOOK_LOG)).count();
    assert_eq!(hits, 1, "hook should run once for the leg A refund");

    // Both depositors recover their deposit; DvP is closed.
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

#[test]
fn test_reject_with_hook_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );
    setup_hook_on_mint_a(&mut context, &fixture);

    assert_create_dvp(&mut context, &fixture);
    fund_a_with_hook(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let leg_a_extras = hook_extras_for_mint(&fixture.mint_a);
    let ix = RejectDvpBuilder::new()
        .signer(fixture.user_a.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(leg_a_extras.len() as u8)
        .add_remaining_accounts(&leg_a_extras)
        .instruction();
    let meta = context
        .send(ix, &[&fixture.user_a])
        .expect("reject with hook");

    let hits = meta.logs.iter().filter(|l| l.contains(HOOK_LOG)).count();
    assert_eq!(hits, 1, "hook should run once for the leg A refund");
    assert!(context.get_account(&fixture.swap_dvp).is_none());
}

#[test]
fn test_reclaim_with_hook_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );
    setup_hook_on_mint_a(&mut context, &fixture);

    assert_create_dvp(&mut context, &fixture);
    fund_a_with_hook(&mut context, &fixture);

    let leg_a_extras = hook_extras_for_mint(&fixture.mint_a);
    let ix = ReclaimDvpBuilder::new()
        .signer(fixture.user_a.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint(fixture.mint_a)
        .dvp_source_ata(fixture.dvp_ata_a)
        .signer_dest_ata(fixture.user_a_ata_a)
        .token_program(fixture.token_program_a)
        .memo_program(MEMO_PROGRAM_ID)
        .add_remaining_accounts(&leg_a_extras)
        .instruction();
    let meta = context
        .send(ix, &[&fixture.user_a])
        .expect("reclaim with hook");

    let hits = meta.logs.iter().filter(|l| l.contains(HOOK_LOG)).count();
    assert_eq!(hits, 1, "hook should run once for the reclaim transfer");
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
}

/// Settle invokes `transfer_checked_cpi` a *second* time on leg A when
/// the escrow holds more than `amount_a` (over-funding), to refund the
/// surplus to user_a. That second CPI must also forward the hook extras
/// — if `leg_a_extras` were silently dropped from the surplus CPI, the
/// `set_transferring` step on user_a_ata_a would still succeed (the
/// account carries the extension), but the hook would not fire. Hence
/// asserting `hits == 2` pins that the surplus path forwards the extras.
#[test]
fn test_settle_with_hook_and_surplus_on_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );
    setup_hook_on_mint_a(&mut context, &fixture);

    assert_create_dvp(&mut context, &fixture);

    // Over-fund leg A by `surplus`; leg B funded exactly.
    let surplus = 1_234u64;
    let extras = hook_extras_for_mint(&fixture.mint_a);
    let mut fund_ix = spl_token_2022::instruction::transfer_checked(
        &TOKEN_2022_PROGRAM_ID,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.dvp_ata_a,
        &fixture.user_a.pubkey(),
        &[],
        AMOUNT_A + surplus,
        6,
    )
    .expect("build transfer_checked");
    fund_ix.accounts.extend_from_slice(&extras);
    context
        .send(fund_ix, &[&fixture.user_a])
        .expect("fund A with surplus + hook");
    assert_fund_b(&mut context, &fixture);

    let leg_a_extras = hook_extras_for_mint(&fixture.mint_a);
    let ix = SettleDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(leg_a_extras.len() as u8)
        .add_remaining_accounts(&leg_a_extras)
        .instruction();
    let meta = context
        .send(ix, &[&fixture.settlement_authority])
        .expect("settle with hook + surplus");

    let hits = meta.logs.iter().filter(|l| l.contains(HOOK_LOG)).count();
    assert_eq!(
        hits, 2,
        "hook should run twice: once for the leg transfer, once for the surplus refund"
    );

    // Surplus landed back on user_a; counterparty got exactly AMOUNT_A.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

/// Same hook fixture on both mints: the test asserts the log fires
/// exactly twice (once per leg). If `split_leg_remaining_accounts`
/// flipped the slices, each leg would receive the *other* mint's
/// validation PDA. `invoke_execute` searches the trailing accounts for
/// the validation PDA derived from the *transferring* mint; if not
/// found, it CPIs the hook with only 4 accounts ("hook accounts: 4")
/// instead of 6. So a swapped split flips the count and breaks this
/// assertion.
#[test]
fn test_settle_with_hooks_on_both_legs() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    // Mint A hook + ATAs that hold mint_a.
    setup_hook_on_mint_a(&mut context, &fixture);
    // Mint B hook + ATAs that hold mint_b.
    setup_hook_mint(&mut context, &fixture.mint_b);
    set_token_2022_with_hook_account(
        &mut context,
        &fixture.user_b_ata_b,
        &fixture.mint_b,
        &fixture.user_b.pubkey(),
        INITIAL_BALANCE,
    );
    set_token_2022_with_hook_account(
        &mut context,
        &fixture.user_a_ata_b,
        &fixture.mint_b,
        &fixture.user_a.pubkey(),
        0,
    );

    assert_create_dvp(&mut context, &fixture);
    fund_a_with_hook(&mut context, &fixture);

    // Fund B by raw transfer_checked + hook extras for mint_b.
    let extras_b = hook_extras_for_mint(&fixture.mint_b);
    let mut fund_b_ix = spl_token_2022::instruction::transfer_checked(
        &TOKEN_2022_PROGRAM_ID,
        &fixture.user_b_ata_b,
        &fixture.mint_b,
        &fixture.dvp_ata_b,
        &fixture.user_b.pubkey(),
        &[],
        AMOUNT_B,
        6,
    )
    .expect("build transfer_checked");
    fund_b_ix.accounts.extend_from_slice(&extras_b);
    context
        .send(fund_b_ix, &[&fixture.user_b])
        .expect("fund B with hook");

    let leg_a_extras = hook_extras_for_mint(&fixture.mint_a);
    let leg_b_extras = hook_extras_for_mint(&fixture.mint_b);
    let mut remaining = leg_a_extras.clone();
    remaining.extend_from_slice(&leg_b_extras);
    let ix = SettleDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(leg_a_extras.len() as u8)
        .add_remaining_accounts(&remaining)
        .instruction();
    let meta = context
        .send(ix, &[&fixture.settlement_authority])
        .expect("settle with hooks on both legs");

    let hits = meta.logs.iter().filter(|l| l.contains(HOOK_LOG)).count();
    assert_eq!(hits, 2, "hook should run exactly once per leg");

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

/// `split_leg_remaining_accounts` rejects an extras count greater than
/// the number of trailing accounts actually passed. Direct boundary test
/// for the bound at `processor/shared/utils.rs:split_leg_remaining_accounts`.
#[test]
fn test_settle_rejects_extras_count_overrun() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    // leg_a_extras_count = 5 but no trailing accounts passed.
    let ix = SettleDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(5)
        .instruction();
    let result = context.send(ix, &[&fixture.settlement_authority]);
    assert_instruction_error(result, "InvalidInstructionData");
}

// Closed-mint recovery: a closed leg mint must not block refunding the
// other leg.

/// Overwrite `mint` as an empty System-owned account (a closed mint).
fn close_mint(context: &mut TestContext, mint: &solana_sdk::pubkey::Pubkey) {
    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1,
                data: vec![],
                owner: solana_sdk::system_program::ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .unwrap();
}

/// leg A unfunded with its mint closed, leg B funded; Cancel refunds B.
#[test]
fn test_cancel_recovers_funded_leg_when_other_legs_mint_closed() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    close_mint(&mut context, &fixture.mint_a);

    // Past expiry, so Reclaim is no longer an option.
    let advance = fixture.expiry - context.now() + 1;
    context.advance_clock(advance);

    assert_cancel_dvp(&mut context, &fixture);

    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE
    );
    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}

/// A memo-required refund destination must not block Cancel. Both legs
/// funded; user_a's refund ATA requires an incoming memo. Cancel must
/// still refund both legs.
#[test]
fn test_cancel_with_memo_required_destination() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.user_a.pubkey(),
        INITIAL_BALANCE - AMOUNT_A,
    );

    assert_cancel_dvp(&mut context, &fixture);

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

/// A memo-required refund destination must not block Reject.
#[test]
fn test_reject_with_memo_required_destination() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.user_a.pubkey(),
        INITIAL_BALANCE - AMOUNT_A,
    );

    assert_reject_dvp(&mut context, &fixture, &fixture.user_a);

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

/// A memo-required destination must not block Reclaim.
#[test]
fn test_reclaim_with_memo_required_destination() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.user_a.pubkey(),
        INITIAL_BALANCE - AMOUNT_A,
    );

    assert_reclaim_a(&mut context, &fixture);

    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
}

/// Every Settle destination (both deliveries and both surplus refunds)
/// requires a memo; Settle must emit one before each transfer.
#[test]
fn test_settle_with_memo_required_destinations() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    let surplus_a: u64 = 5_000;
    let surplus_b: u64 = 3_000;

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a_amount(&mut context, &fixture, AMOUNT_A + surplus_a);
    assert_fund_b_amount(&mut context, &fixture, AMOUNT_B + surplus_b);

    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_a_ata_b,
        &fixture.mint_b,
        &fixture.user_a.pubkey(),
        0,
    );
    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_b_ata_a,
        &fixture.mint_a,
        &fixture.user_b.pubkey(),
        0,
    );
    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_a_ata_a,
        &fixture.mint_a,
        &fixture.user_a.pubkey(),
        INITIAL_BALANCE - AMOUNT_A - surplus_a,
    );
    set_token_2022_with_memo_required(
        &mut context,
        &fixture.user_b_ata_b,
        &fixture.mint_b,
        &fixture.user_b.pubkey(),
        INITIAL_BALANCE - AMOUNT_B - surplus_b,
    );

    assert_settle_dvp(&mut context, &fixture);

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
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

/// Symmetric: leg B unfunded with its mint closed, leg A funded; Reject refunds A.
#[test]
fn test_reject_recovers_funded_leg_when_other_legs_mint_closed() {
    let mut context = TestContext::new();
    let fixture = setup_dvp_with_programs(
        &mut context,
        0,
        TOKEN_2022_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID,
    );

    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    close_mint(&mut context, &fixture.mint_b);

    let advance = fixture.expiry - context.now() + 1;
    context.advance_clock(advance);

    assert_reject_dvp(&mut context, &fixture, &fixture.user_a);

    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}
