use contra_swap_program_client::instructions::CreateDvpBuilder;
use solana_sdk::signature::Signer;

use crate::{
    state_utils::{assert_create_dvp, setup_dvp, AMOUNT_A, AMOUNT_B},
    utils::{
        assert_program_error, get_token_balance, TestContext, EARLIEST_AFTER_EXPIRY,
        EXPIRY_NOT_IN_FUTURE, SAME_MINT, SELF_DVP, ZERO_AMOUNT,
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
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
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
fn test_create_dvp_rejects_earliest_after_expiry() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);

    let ix = CreateDvpBuilder::new()
        .payer(context.payer.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
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
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
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
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_a)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
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
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
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
        .mint_a(fixture.mint_a)
        .mint_b(fixture.mint_b)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
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
