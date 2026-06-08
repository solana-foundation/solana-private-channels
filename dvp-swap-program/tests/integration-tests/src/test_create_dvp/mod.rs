use dvp_swap_program_client::instructions::CreateDvpBuilder;
use solana_sdk::signature::{Keypair, Signer};
use spl_associated_token_account::instruction::create_associated_token_account;

use crate::{
    state_utils::{assert_cancel_dvp, assert_create_dvp, setup_dvp, AMOUNT_A, AMOUNT_B},
    utils::{
        assert_program_error, get_token_balance, TestContext, EARLIEST_AFTER_EXPIRY,
        EXPIRY_NOT_IN_FUTURE, EXPIRY_TOO_FAR_IN_FUTURE, NONCE_ALREADY_USED, SAME_MINT, SELF_DVP,
        SETTLEMENT_AUTHORITY_EXECUTABLE, SETTLEMENT_AUTHORITY_IS_PARTY, SWAP_PROGRAM_ID,
        ZERO_AMOUNT,
    },
};

#[test]
fn test_create_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    assert_create_dvp(&mut context, &fixture);

    assert!(
        context.get_account(&fixture.swap_dvp).is_some(),
        "SwapDvp PDA must exist"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_some(),
        "dvp_ata_a must exist"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_b).is_some(),
        "dvp_ata_b must exist"
    );
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), 0);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_b), 0);
}

// `validate_args` runs before PDA derivation in the processor, so the
// error tests below can reuse the fixture's PDA + escrow ATAs even when
// the args they pass would derive a different PDA. Only the offending
// arg differs from `assert_create_dvp`.

#[test]
fn test_create_dvp_rejects_expiry_at_now() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    let now = context.now();

    let ix = CreateDvpBuilder::new()
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
        .expiry_timestamp(now)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), EXPIRY_NOT_IN_FUTURE);
}

#[test]
fn test_create_dvp_rejects_expiry_too_far_in_future() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    // One year + 1s past now exceeds the MAX_DVP_DURATION_SECS cap.
    let one_year_plus_one = context.now() + 365 * 24 * 60 * 60 + 1;
    let ix = CreateDvpBuilder::new()
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
        .expiry_timestamp(one_year_plus_one)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), EXPIRY_TOO_FAR_IN_FUTURE);
}

#[test]
fn test_create_dvp_rejects_earliest_after_expiry() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let ix = CreateDvpBuilder::new()
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
        .earliest_settlement_timestamp(fixture.expiry + 1)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), EARLIEST_AFTER_EXPIRY);
}

#[test]
fn test_create_dvp_rejects_self_dvp() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let ix = CreateDvpBuilder::new()
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
        .user_b(fixture.user_a.pubkey())
        .settlement_authority(fixture.settlement_authority.pubkey())
        .amount_a(AMOUNT_A)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(fixture.expiry)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), SELF_DVP);
}

#[test]
fn test_create_dvp_rejects_same_mint() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let ix = CreateDvpBuilder::new()
        .payer(context.payer.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .nonce_tombstone(fixture.nonce_tombstone)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_a)
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
        .instruction();

    assert_program_error(context.send(ix, &[]), SAME_MINT);
}

#[test]
fn test_create_dvp_rejects_zero_amount_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let ix = CreateDvpBuilder::new()
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
        .amount_a(0)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(fixture.expiry)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), ZERO_AMOUNT);
}

#[test]
fn test_create_dvp_rejects_zero_amount_b() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let ix = CreateDvpBuilder::new()
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
        .amount_b(0)
        .expiry_timestamp(fixture.expiry)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), ZERO_AMOUNT);
}

#[test]
fn test_create_dvp_rejects_executable_settlement_authority() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    // Point settlement_authority at an executable account (the program
    // under test). An executable can't be credited the closed-account rent
    // at Settle/Cancel, so CreateDvp must reject it up front.
    let ix = CreateDvpBuilder::new()
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
        .settlement_authority(SWAP_PROGRAM_ID)
        .amount_a(AMOUNT_A)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(fixture.expiry)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), SETTLEMENT_AUTHORITY_EXECUTABLE);
}

#[test]
fn test_create_dvp_rejects_settlement_authority_as_party() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    // settlement_authority must be a neutral third party. Pointing it at
    // either swap counterparty would let a party settle its own trade, so
    // CreateDvp must reject both user_a and user_b up front.
    for party in [fixture.user_a.pubkey(), fixture.user_b.pubkey()] {
        let ix = CreateDvpBuilder::new()
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
            .settlement_authority(party)
            .amount_a(AMOUNT_A)
            .amount_b(AMOUNT_B)
            .expiry_timestamp(fixture.expiry)
            .nonce(fixture.nonce)
            .instruction();

        assert_program_error(context.send(ix, &[]), SETTLEMENT_AUTHORITY_IS_PARTY);
    }
}

/// Once a DvP is closed, its `(seeds, nonce)` PDA address can never be
/// re-instantiated: the nonce tombstone outlives the trade. This blocks
/// the stale-deposit capture attack — an attacker can't recreate the
/// same address with predatory terms to drain a deposit the victim
/// queued against the old escrow.
#[test]
fn test_create_dvp_rejects_reused_nonce_after_close() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    // Close the trade: the SwapDvp PDA and escrows go away, but the
    // nonce tombstone remains.
    assert_cancel_dvp(&mut context, &fixture);
    assert!(context.get_account(&fixture.swap_dvp).is_none());

    // Re-creating the same nonce — even with predatory terms — must fail.
    let ix = CreateDvpBuilder::new()
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
        .amount_a(1)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(fixture.expiry)
        .nonce(fixture.nonce)
        .instruction();

    assert_program_error(context.send(ix, &[]), NONCE_ALREADY_USED);
}

/// A front-runner pre-creates the canonical asset escrow ATA before
/// CreateDvp lands. CreateDvp must accept the existing account
/// (idempotent path) instead of bricking the trade.
#[test]
fn test_create_dvp_succeeds_when_escrow_ata_was_pre_created() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let frontrunner = Keypair::new();
    context.airdrop_if_required(&frontrunner.pubkey(), 1_000_000_000);
    let pre_create_ix = create_associated_token_account(
        &frontrunner.pubkey(),
        &fixture.swap_dvp,
        &fixture.mint_a,
        &fixture.token_program_a,
    );
    context
        .send(pre_create_ix, &[&frontrunner])
        .expect("front-runner pre-creates dvp_ata_a");
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_some(),
        "dvp_ata_a must exist after front-run"
    );

    assert_create_dvp(&mut context, &fixture);

    assert!(context.get_account(&fixture.swap_dvp).is_some());
    assert!(context.get_account(&fixture.dvp_ata_b).is_some());
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), 0);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_b), 0);
}
