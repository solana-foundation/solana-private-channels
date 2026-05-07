use contra_swap_program_client::instructions::ReclaimDvpBuilder;
use solana_sdk::signature::{Keypair, Signer};
use spl_associated_token_account::get_associated_token_address;

use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_b, assert_reclaim_a, assert_settle_dvp,
        setup_dvp, AMOUNT_A, AMOUNT_B, INITIAL_BALANCE,
    },
    utils::{assert_program_error, get_token_balance, TestContext, DVP_EXPIRED, SIGNER_NOT_PARTY},
};

#[test]
fn test_reclaim_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), AMOUNT_A);

    assert_reclaim_a(&mut context, &fixture);

    // Funds restored to user_a; DvP itself stays open.
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), 0);
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert!(
        context.get_account(&fixture.swap_dvp).is_some(),
        "SwapDvp stays open after reclaim"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_some(),
        "escrow stays open after reclaim"
    );
}

/// Mirror of `test_reclaim_dvp_success` but signed by user_b — locks
/// down the leg-selection-by-signer logic for the cash leg.
#[test]
fn test_reclaim_dvp_b_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);
    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_b), AMOUNT_B);

    let ix = ReclaimDvpBuilder::new()
        .signer(fixture.user_b.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_source_ata(fixture.dvp_ata_b)
        .signer_dest_ata(fixture.user_b_ata_b)
        .instruction();
    context
        .send(ix, &[&fixture.user_b])
        .expect("ReclaimDvp B should succeed");

    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_b), 0);
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE
    );
}

#[test]
fn test_reclaim_dvp_rejects_settlement_authority() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    // settlement_authority is not a depositor, so the leg-selection
    // arm in process_reclaim_dvp returns SignerNotParty.
    let auth_ata_a =
        get_associated_token_address(&fixture.settlement_authority.pubkey(), &fixture.mint_a);
    let ix = ReclaimDvpBuilder::new()
        .signer(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_source_ata(fixture.dvp_ata_a)
        .signer_dest_ata(auth_ata_a)
        .instruction();
    let result = context.send(ix, &[&fixture.settlement_authority]);
    assert_program_error(result, SIGNER_NOT_PARTY);
}

#[test]
fn test_reclaim_dvp_rejects_third_party() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    let outsider = Keypair::new();
    context.airdrop_if_required(&outsider.pubkey(), 1_000_000_000);
    let outsider_ata_a = get_associated_token_address(&outsider.pubkey(), &fixture.mint_a);

    let ix = ReclaimDvpBuilder::new()
        .signer(outsider.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_source_ata(fixture.dvp_ata_a)
        .signer_dest_ata(outsider_ata_a)
        .instruction();
    let result = context.send(ix, &[&outsider]);
    assert_program_error(result, SIGNER_NOT_PARTY);
}

#[test]
fn test_reclaim_dvp_rejects_post_expiry() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    // Step the clock past expiry. After this, Reclaim must reject so
    // that an expired-but-funded DvP can only be drained via Cancel /
    // Reject — the load-bearing reason those instructions exist.
    let advance = fixture.expiry - context.now() + 1;
    context.advance_clock(advance);

    let ix = ReclaimDvpBuilder::new()
        .signer(fixture.user_a.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_source_ata(fixture.dvp_ata_a)
        .signer_dest_ata(fixture.user_a_ata_a)
        .instruction();
    let result = context.send(ix, &[&fixture.user_a]);
    assert_program_error(result, DVP_EXPIRED);
}

/// Reclaiming a never-funded leg is a documented no-op (skips the
/// Transfer CPI and returns Ok). Pin that behaviour: nothing moves and
/// the DvP stays open and re-fundable.
#[test]
fn test_reclaim_dvp_empty_escrow_is_noop() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    assert_reclaim_a(&mut context, &fixture);

    assert_eq!(get_token_balance(&context, &fixture.dvp_ata_a), 0);
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert!(context.get_account(&fixture.swap_dvp).is_some());
    assert!(context.get_account(&fixture.dvp_ata_a).is_some());
}

/// Reclaim leaves the DvP open and re-fundable. After re-funding both
/// legs the trade can settle normally.
#[test]
fn test_reclaim_then_refund_then_settle() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    assert_fund_a(&mut context, &fixture);
    assert_reclaim_a(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    assert_settle_dvp(&mut context, &fixture);

    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    assert!(context.get_account(&fixture.swap_dvp).is_none());
}

