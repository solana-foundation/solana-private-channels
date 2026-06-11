use dvp_swap_program_client::instructions::{CancelDvpBuilder, CreateDvpBuilder};
use solana_sdk::signature::{Keypair, Signer};

use crate::{
    state_utils::{
        assert_cancel_dvp, assert_create_dvp, assert_fund_a, assert_fund_b, setup_dvp, AMOUNT_A,
        AMOUNT_B, INITIAL_BALANCE, REF_STRING,
    },
    utils::{
        assert_instruction_error, assert_program_error, get_token_balance, TestContext,
        MEMO_PROGRAM_ID, SETTLEMENT_AUTHORITY_MISMATCH,
    },
};

#[test]
fn test_cancel_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    assert_cancel_dvp(&mut context, &fixture);

    // Each user got their own leg back. No cross transfer happened.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE
    );
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE
    );
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), 0);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), 0);
    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}

#[test]
fn test_cancel_dvp_only_leg_a_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    assert_cancel_dvp(&mut context, &fixture);

    // user_a's leg refunded; user_b never funded so balance unchanged.
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

#[test]
fn test_cancel_dvp_only_leg_b_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

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

/// Cancel must close the trade even when no funding ever happened —
/// otherwise an abandoned-create would strand its rent.
#[test]
fn test_cancel_dvp_neither_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    assert_cancel_dvp(&mut context, &fixture);

    assert!(context.get_account(&fixture.swap_dvp).is_none());
    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}

/// Settlement destinations apply to executed settlement only: Cancel
/// refunds the depositors' own ATAs even when both destinations are
/// set, and never touches a destination account (none is even passed).
#[test]
fn test_cancel_dvp_ignores_settlement_destinations() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let create_ix = CreateDvpBuilder::new()
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
        .ref_string(REF_STRING.to_string())
        .settlement_destination_a(Keypair::new().pubkey())
        .settlement_destination_b(Keypair::new().pubkey())
        .instruction();
    context
        .send(create_ix, &[])
        .expect("CreateDvp with destinations");

    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    assert_cancel_dvp(&mut context, &fixture);

    // Refunds went to the depositors, exactly as without destinations.
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

/// Cancel intentionally has no expiry check — without it, an
/// expired-but-funded DvP would strand the deposited funds because
/// Settle would also be locked out by the expiry check.
#[test]
fn test_cancel_dvp_works_post_expiry() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let advance = fixture.expiry - context.now() + 1;
    context.advance_clock(advance);

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

#[test]
fn test_cancel_dvp_rejects_user_signer() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let ix = CancelDvpBuilder::new()
        .settlement_authority(fixture.user_a.pubkey())
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
        .leg_a_extras_count(0)
        .instruction();
    let result = context.send(ix, &[&fixture.user_a]);
    assert_program_error(result, SETTLEMENT_AUTHORITY_MISMATCH);
}

/// Mid-trade mint substitution: passing a mint pubkey that differs
/// from the one stored at Create must fail. Pins the
/// `mint_a_info.address() != dvp.mint_a` guard in process_cancel_dvp.
#[test]
fn test_cancel_dvp_rejects_substituted_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let ix = CancelDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_b)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .token_program_a(fixture.token_program_a)
        .token_program_b(fixture.token_program_b)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(0)
        .instruction();
    let result = context.send(ix, &[&fixture.settlement_authority]);
    assert_instruction_error(result, "InvalidAccountData");
}

#[test]
fn test_cancel_dvp_rejects_third_party() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let outsider = Keypair::new();
    context.airdrop_if_required(&outsider.pubkey(), 1_000_000_000);

    let ix = CancelDvpBuilder::new()
        .settlement_authority(outsider.pubkey())
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
        .leg_a_extras_count(0)
        .instruction();
    let result = context.send(ix, &[&outsider]);
    assert_program_error(result, SETTLEMENT_AUTHORITY_MISMATCH);
}
