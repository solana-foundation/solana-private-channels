use contra_swap_program_client::instructions::RejectDvpBuilder;
use solana_sdk::signature::{Keypair, Signer};

use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_b, assert_reject_dvp, setup_dvp,
        INITIAL_BALANCE,
    },
    utils::{assert_program_error, get_token_balance, TestContext, SIGNER_NOT_PARTY},
};

#[test]
fn test_reject_dvp_success_signed_by_user_b() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    assert_reject_dvp(&mut context, &fixture, &fixture.user_b);

    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE
    );
    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}

/// user_a should equally be able to pull the plug. Mirrors the
/// signed-by-user_b test to lock down both branches of the
/// `signer ∈ {user_a, user_b}` check.
#[test]
fn test_reject_dvp_success_signed_by_user_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

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

/// Reject must close the trade even when neither leg has been funded.
#[test]
fn test_reject_dvp_neither_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    assert_reject_dvp(&mut context, &fixture, &fixture.user_a);

    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}

/// Reject has no expiry check by design — without it, an
/// expired-but-funded DvP would strand funds.
#[test]
fn test_reject_dvp_works_post_expiry() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let advance = fixture.expiry - context.now() + 1;
    context.advance_clock(advance);

    assert_reject_dvp(&mut context, &fixture, &fixture.user_b);

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

/// Reject's authorization is the inverse of Cancel's: only the
/// counterparties may sign. The settlement authority is the rent
/// recipient, never a valid signer.
#[test]
fn test_reject_dvp_rejects_settlement_authority_as_signer() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    let ix = RejectDvpBuilder::new()
        .signer(fixture.settlement_authority.pubkey())
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .instruction();
    let result = context.send(ix, &[&fixture.settlement_authority]);
    assert_program_error(result, SIGNER_NOT_PARTY);
}

#[test]
fn test_reject_dvp_rejects_third_party() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    let outsider = Keypair::new();
    context.airdrop_if_required(&outsider.pubkey(), 1_000_000_000);

    let ix = RejectDvpBuilder::new()
        .signer(outsider.pubkey())
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .instruction();
    let result = context.send(ix, &[&outsider]);
    assert_program_error(result, SIGNER_NOT_PARTY);
}
