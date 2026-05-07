use contra_swap_program_client::instructions::{
    CancelDvpBuilder, CreateDvpBuilder, SettleDvpBuilder,
};
use solana_sdk::signature::{Keypair, Signer};
use spl_token::{instruction::transfer as spl_transfer, ID as TOKEN_PROGRAM_ID};

use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_a_amount, assert_fund_b,
        assert_fund_b_amount, assert_settle_dvp, setup_dvp, AMOUNT_A, AMOUNT_B, INITIAL_BALANCE,
    },
    utils::{
        assert_program_error, dvp_ata, fund_wallet_ata, get_token_balance, swap_dvp_pda,
        TestContext, LEG_NOT_FUNDED, SETTLEMENT_AUTHORITY_MISMATCH,
    },
};

#[test]
fn test_settle_dvp_success() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    assert_settle_dvp(&mut context, &fixture);

    // user_a paid asset, received cash; user_b paid cash, received asset.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE - AMOUNT_B
    );

    // SwapDvp + both escrow ATAs are closed.
    assert!(
        context.get_account(&fixture.swap_dvp).is_none(),
        "SwapDvp must be closed"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_a).is_none(),
        "dvp_ata_a must be closed"
    );
    assert!(
        context.get_account(&fixture.dvp_ata_b).is_none(),
        "dvp_ata_b must be closed"
    );
}

/// With raw SPL Transfer as the funding path, an over-deposit is
/// possible. Settle must transfer exactly `amount_x` to the counterparty
/// and refund the surplus to the depositor — otherwise the surplus would
/// leak to the counterparty.
#[test]
fn test_settle_dvp_refunds_overfunding_to_depositor() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    let asset_surplus = 1_234u64;
    let cash_surplus = 567u64;
    assert_fund_a_amount(&mut context, &fixture, AMOUNT_A + asset_surplus);
    assert_fund_b_amount(&mut context, &fixture, AMOUNT_B + cash_surplus);

    assert_settle_dvp(&mut context, &fixture);

    // Each user paid only `amount_x`; the surplus is back on their own
    // mint, so depositors are made whole modulo the agreed swap.
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    assert_eq!(
        get_token_balance(&context, &fixture.user_b_ata_b),
        INITIAL_BALANCE - AMOUNT_B
    );

    assert!(context.get_account(&fixture.dvp_ata_a).is_none());
    assert!(context.get_account(&fixture.dvp_ata_b).is_none());
}

#[test]
fn test_settle_dvp_rejects_user_as_authority() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let ix = SettleDvpBuilder::new()
        .settlement_authority(fixture.user_a.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .instruction();
    let result = context.send(ix, &[&fixture.user_a]);
    assert_program_error(result, SETTLEMENT_AUTHORITY_MISMATCH);
}

#[test]
fn test_settle_dvp_rejects_third_party_as_authority() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let outsider = Keypair::new();
    context.airdrop_if_required(&outsider.pubkey(), 1_000_000_000);

    let ix = SettleDvpBuilder::new()
        .settlement_authority(outsider.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .instruction();
    let result = context.send(ix, &[&outsider]);
    assert_program_error(result, SETTLEMENT_AUTHORITY_MISMATCH);
}

#[test]
fn test_settle_dvp_rejects_when_neither_leg_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    let result = context.send(
        SettleDvpBuilder::new()
            .settlement_authority(fixture.settlement_authority.pubkey())
            .swap_dvp(fixture.swap_dvp)
            .dvp_ata_a(fixture.dvp_ata_a)
            .dvp_ata_b(fixture.dvp_ata_b)
            .user_a_ata_b(fixture.user_a_ata_b)
            .user_b_ata_a(fixture.user_b_ata_a)
            .user_a_ata_a(fixture.user_a_ata_a)
            .user_b_ata_b(fixture.user_b_ata_b)
            .instruction(),
        &[&fixture.settlement_authority],
    );
    assert_program_error(result, LEG_NOT_FUNDED);
}

/// Asset leg unfunded: process_settle_dvp checks escrow A first, so
/// this exercises the early-leg branch.
#[test]
fn test_settle_dvp_rejects_when_only_leg_b_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_b(&mut context, &fixture);

    let result = context.send(
        SettleDvpBuilder::new()
            .settlement_authority(fixture.settlement_authority.pubkey())
            .swap_dvp(fixture.swap_dvp)
            .dvp_ata_a(fixture.dvp_ata_a)
            .dvp_ata_b(fixture.dvp_ata_b)
            .user_a_ata_b(fixture.user_a_ata_b)
            .user_b_ata_a(fixture.user_b_ata_a)
            .user_a_ata_a(fixture.user_a_ata_a)
            .user_b_ata_b(fixture.user_b_ata_b)
            .instruction(),
        &[&fixture.settlement_authority],
    );
    assert_program_error(result, LEG_NOT_FUNDED);
}

/// Cash leg unfunded: exercises the second-leg branch of the
/// LegNotFunded check.
#[test]
fn test_settle_dvp_rejects_when_only_leg_a_funded() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    let result = context.send(
        SettleDvpBuilder::new()
            .settlement_authority(fixture.settlement_authority.pubkey())
            .swap_dvp(fixture.swap_dvp)
            .dvp_ata_a(fixture.dvp_ata_a)
            .dvp_ata_b(fixture.dvp_ata_b)
            .user_a_ata_b(fixture.user_a_ata_b)
            .user_b_ata_a(fixture.user_b_ata_a)
            .user_a_ata_a(fixture.user_a_ata_a)
            .user_b_ata_b(fixture.user_b_ata_b)
            .instruction(),
        &[&fixture.settlement_authority],
    );
    assert_program_error(result, LEG_NOT_FUNDED);
}

/// The asset leg can be funded across several raw SPL transfers — the
/// program never tracks deposits, just the resulting escrow balance.
/// Two half-amount sends must add up and settle normally.
#[test]
fn test_settle_dvp_with_funding_split_across_two_transfers() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    let half = AMOUNT_A / 2;
    assert_fund_a_amount(&mut context, &fixture, half);
    assert_fund_a_amount(&mut context, &fixture, AMOUNT_A - half);
    assert_fund_b(&mut context, &fixture);

    assert_settle_dvp(&mut context, &fixture);

    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE - AMOUNT_A
    );
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
}

/// Anyone can fund a leg's escrow because escrow ATAs are derivable
/// from public state. A donor's contribution effectively gifts the
/// trade — `amount_a` of the donation flows to the counterparty as the
/// asset leg, and any surplus is refunded to user_a (the leg's
/// "depositor of record" by program convention). This test locks down
/// that documented behaviour.
#[test]
fn test_settle_dvp_treats_third_party_donation_as_gift() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);

    let donor = Keypair::new();
    let donation = AMOUNT_A + 500;
    let donor_ata_a = fund_wallet_ata(&mut context, &donor, &fixture.mint_a, donation);

    let donate_ix = spl_transfer(
        &TOKEN_PROGRAM_ID,
        &donor_ata_a,
        &fixture.dvp_ata_a,
        &donor.pubkey(),
        &[],
        donation,
    )
    .expect("build donor SPL transfer");
    context.send(donate_ix, &[&donor]).expect("donor funds A");

    // user_a never deposits A; user_b funds B normally.
    assert_fund_b(&mut context, &fixture);
    assert_settle_dvp(&mut context, &fixture);

    // user_b receives exactly amount_a — the trade settled at the
    // agreed terms.
    assert_eq!(get_token_balance(&context, &fixture.user_b_ata_a), AMOUNT_A);
    // user_a receives the cash leg AND the surplus refund of (donation
    // - amount_a) on mint_a, even though they never deposited.
    assert_eq!(get_token_balance(&context, &fixture.user_a_ata_b), AMOUNT_B);
    assert_eq!(
        get_token_balance(&context, &fixture.user_a_ata_a),
        INITIAL_BALANCE + (donation - AMOUNT_A)
    );
    // Donor walks away empty-handed — donations are unrecoverable.
    assert_eq!(get_token_balance(&context, &donor_ata_a), 0);
}

/// Two DvPs between the same parties + mints, disambiguated only by
/// `nonce`, must be fully independent: settling one leaves the other's
/// PDA and escrows untouched, and the second can subsequently be
/// cancelled cleanly.
#[test]
fn test_two_dvps_same_parties_different_nonces_are_isolated() {
    let mut context = TestContext::new();

    let first_dvp = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &first_dvp);

    // Build the second DvP by hand from the first DvP's parties + mints;
    // only the nonce differs, which yields a fresh PDA + escrow ATAs.
    let second_nonce = 1u64;
    let (second_swap_dvp, _) = swap_dvp_pda(
        &first_dvp.settlement_authority.pubkey(),
        &first_dvp.user_a.pubkey(),
        &first_dvp.user_b.pubkey(),
        &first_dvp.mint_a,
        &first_dvp.mint_b,
        second_nonce,
    );
    let second_dvp_ata_a = dvp_ata(&second_swap_dvp, &first_dvp.mint_a);
    let second_dvp_ata_b = dvp_ata(&second_swap_dvp, &first_dvp.mint_b);

    let create_second = CreateDvpBuilder::new()
        .payer(context.payer.pubkey())
        .swap_dvp(second_swap_dvp)
        .mint_a(first_dvp.mint_a)
        .mint_b(first_dvp.mint_b)
        .dvp_ata_a(second_dvp_ata_a)
        .dvp_ata_b(second_dvp_ata_b)
        .user_a(first_dvp.user_a.pubkey())
        .user_b(first_dvp.user_b.pubkey())
        .settlement_authority(first_dvp.settlement_authority.pubkey())
        .amount_a(AMOUNT_A)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(first_dvp.expiry)
        .nonce(second_nonce)
        .instruction();
    context
        .send(create_second, &[])
        .expect("CreateDvp second nonce");

    // Fund + settle the first DvP; assert the second is untouched.
    assert_fund_a(&mut context, &first_dvp);
    assert_fund_b(&mut context, &first_dvp);
    assert_settle_dvp(&mut context, &first_dvp);

    assert!(context.get_account(&first_dvp.swap_dvp).is_none());
    assert!(
        context.get_account(&second_swap_dvp).is_some(),
        "second DvP's PDA must survive the first's settlement"
    );
    assert_eq!(get_token_balance(&context, &second_dvp_ata_a), 0);
    assert_eq!(get_token_balance(&context, &second_dvp_ata_b), 0);

    // Cancel the second DvP to confirm it's still operable end-to-end.
    let cancel_second = CancelDvpBuilder::new()
        .settlement_authority(first_dvp.settlement_authority.pubkey())
        .swap_dvp(second_swap_dvp)
        .dvp_ata_a(second_dvp_ata_a)
        .dvp_ata_b(second_dvp_ata_b)
        .user_a_ata_a(first_dvp.user_a_ata_a)
        .user_b_ata_b(first_dvp.user_b_ata_b)
        .instruction();
    context
        .send(cancel_second, &[&first_dvp.settlement_authority])
        .expect("CancelDvp second nonce");

    assert!(context.get_account(&second_swap_dvp).is_none());
    assert!(context.get_account(&second_dvp_ata_a).is_none());
    assert!(context.get_account(&second_dvp_ata_b).is_none());
}
