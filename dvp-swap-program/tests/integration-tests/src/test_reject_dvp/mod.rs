use dvp_swap_program_client::instructions::{CreateDvpBuilder, RejectDvpBuilder};
use solana_program::pubkey;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_token_2022::instruction::transfer_checked;

use crate::{
    state_utils::{
        assert_create_dvp, assert_fund_a, assert_fund_b, assert_reject_dvp, setup_dvp, AMOUNT_A,
        AMOUNT_B, INITIAL_BALANCE,
    },
    utils::{
        assert_instruction_error, assert_program_error, create_ata, dvp_ata, fund_wallet_ata,
        get_token_balance, nonce_tombstone_pda, set_mint, set_native_mint, swap_dvp_pda,
        TestContext, MEMO_PROGRAM_ID, NATIVE_MINT, SIGNER_NOT_PARTY, TOKEN_PROGRAM_ID,
    },
};
use spl_associated_token_account::get_associated_token_address_with_program_id;

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
/// counterparties may sign. The settlement authority is not a Reject
/// account at all post-finding-1, so the only thing left to lock down
/// is that submitting it as the signer fails on the party check.
#[test]
fn test_reject_dvp_rejects_settlement_authority_as_signer() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    let ix = RejectDvpBuilder::new()
        .signer(fixture.settlement_authority.pubkey())
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
    let result = context.send(ix, &[&fixture.settlement_authority]);
    assert_program_error(result, SIGNER_NOT_PARTY);
}

/// Mid-trade mint substitution: passing a mint pubkey that differs
/// from the one stored at Create must fail. Pins the
/// `mint_a_info.address() != dvp.mint_a` guard in process_reject_dvp.
#[test]
fn test_reject_dvp_rejects_substituted_mint_a() {
    let mut context = TestContext::new();
    let fixture = setup_dvp(&mut context, 0);
    assert_create_dvp(&mut context, &fixture);
    assert_fund_a(&mut context, &fixture);

    let ix = RejectDvpBuilder::new()
        .signer(fixture.user_a.pubkey())
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
    let result = context.send(ix, &[&fixture.user_a]);
    assert_instruction_error(result, "InvalidAccountData");
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
    assert_program_error(result, SIGNER_NOT_PARTY);
}

/// Reject must work even when `settlement_authority` is a pubkey
/// nobody can sign as (here, the Clock sysvar). Settle and Cancel are
/// unreachable in that case; Reject is the safety valve.
#[test]
fn test_reject_dvp_works_when_settlement_authority_is_unreachable() {
    let mut context = TestContext::new();

    // Clock sysvar — no private key, so nobody can sign Settle/Cancel
    // as this authority.
    let unreachable_authority: Pubkey = pubkey!("SysvarC1ock11111111111111111111111111111111");

    let user_a = Keypair::new();
    let user_b = Keypair::new();
    let mint_a = Keypair::new().pubkey();
    let mint_b = Keypair::new().pubkey();
    set_mint(&mut context, &mint_a, &TOKEN_PROGRAM_ID);
    set_mint(&mut context, &mint_b, &TOKEN_PROGRAM_ID);

    let user_a_ata_a = fund_wallet_ata(
        &mut context,
        &user_a,
        &mint_a,
        INITIAL_BALANCE,
        &TOKEN_PROGRAM_ID,
    );
    let user_b_ata_b = fund_wallet_ata(
        &mut context,
        &user_b,
        &mint_b,
        INITIAL_BALANCE,
        &TOKEN_PROGRAM_ID,
    );

    let nonce: u64 = 0;
    let (swap_dvp, _) = swap_dvp_pda(
        &unreachable_authority,
        &user_a.pubkey(),
        &user_b.pubkey(),
        &mint_a,
        &mint_b,
        nonce,
    );
    let dvp_ata_a = dvp_ata(&swap_dvp, &mint_a, &TOKEN_PROGRAM_ID);
    let dvp_ata_b = dvp_ata(&swap_dvp, &mint_b, &TOKEN_PROGRAM_ID);

    let expiry = context.now() + 3600;
    let create_ix = CreateDvpBuilder::new()
        .payer(context.payer.pubkey())
        .swap_dvp(swap_dvp)
        .nonce_tombstone(nonce_tombstone_pda(&swap_dvp).0)
        .mint_a(mint_a)
        .mint_b(mint_b)
        .dvp_ata_a(dvp_ata_a)
        .dvp_ata_b(dvp_ata_b)
        .token_program_a(TOKEN_PROGRAM_ID)
        .token_program_b(TOKEN_PROGRAM_ID)
        .user_a(user_a.pubkey())
        .user_b(user_b.pubkey())
        .settlement_authority(unreachable_authority)
        .amount_a(AMOUNT_A)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(expiry)
        .nonce(nonce)
        .instruction();
    context.send(create_ix, &[]).expect("CreateDvp");

    let fund_ix = transfer_checked(
        &TOKEN_PROGRAM_ID,
        &user_a_ata_a,
        &mint_a,
        &dvp_ata_a,
        &user_a.pubkey(),
        &[],
        AMOUNT_A,
        6,
    )
    .expect("build TransferChecked");
    context.send(fund_ix, &[&user_a]).expect("fund leg A");

    let reject_ix = RejectDvpBuilder::new()
        .signer(user_a.pubkey())
        .swap_dvp(swap_dvp)
        .mint_a(mint_a)
        .mint_b(mint_b)
        .dvp_ata_a(dvp_ata_a)
        .dvp_ata_b(dvp_ata_b)
        .user_a_ata_a(user_a_ata_a)
        .user_b_ata_b(user_b_ata_b)
        .token_program_a(TOKEN_PROGRAM_ID)
        .token_program_b(TOKEN_PROGRAM_ID)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(0)
        .instruction();
    context
        .send(reject_ix, &[&user_a])
        .expect("Reject must succeed despite unreachable settlement_authority");

    assert_eq!(get_token_balance(&context, &user_a_ata_a), INITIAL_BALANCE);
    assert!(context.get_account(&swap_dvp).is_none());
    assert!(context.get_account(&dvp_ata_a).is_none());
    assert!(context.get_account(&dvp_ata_b).is_none());
}

/// A wrapped-SOL escrow funded with raw, unsynced lamports must still be
/// refunded in full. The program `SyncNative`s the escrow before reading
/// its balance, so the deposit is recovered to the depositor instead of
/// leaking to the close recipient. Reject is signed by the counterparty
/// (user_b) here, so a leaked balance would land with them, not user_a,
/// making the refund the only path by which user_a ends up with it.
#[test]
fn test_reject_dvp_syncs_native_escrow_before_refund() {
    let mut context = TestContext::new();
    set_native_mint(&mut context);

    let user_a = Keypair::new();
    let user_b = Keypair::new();
    let settlement_authority = Keypair::new();
    context.airdrop_if_required(&user_b.pubkey(), 1_000_000_000);

    let mint_a = NATIVE_MINT; // WSOL leg
    let mint_b = Keypair::new().pubkey();
    set_mint(&mut context, &mint_b, &TOKEN_PROGRAM_ID);

    // user_a's WSOL refund ATA must exist for the refund transfer; leg B
    // stays unfunded so its ATA is only address-validated.
    let user_a_ata_a = create_ata(&mut context, &user_a.pubkey(), &mint_a, &TOKEN_PROGRAM_ID);
    let user_b_ata_b =
        get_associated_token_address_with_program_id(&user_b.pubkey(), &mint_b, &TOKEN_PROGRAM_ID);

    let nonce: u64 = 0;
    let (swap_dvp, _) = swap_dvp_pda(
        &settlement_authority.pubkey(),
        &user_a.pubkey(),
        &user_b.pubkey(),
        &mint_a,
        &mint_b,
        nonce,
    );
    let dvp_ata_a = dvp_ata(&swap_dvp, &mint_a, &TOKEN_PROGRAM_ID);
    let dvp_ata_b = dvp_ata(&swap_dvp, &mint_b, &TOKEN_PROGRAM_ID);

    let deposit: u64 = 5_000_000; // WSOL base units (9 decimals)
    let create_ix = CreateDvpBuilder::new()
        .payer(context.payer.pubkey())
        .swap_dvp(swap_dvp)
        .nonce_tombstone(nonce_tombstone_pda(&swap_dvp).0)
        .mint_a(mint_a)
        .mint_b(mint_b)
        .dvp_ata_a(dvp_ata_a)
        .dvp_ata_b(dvp_ata_b)
        .token_program_a(TOKEN_PROGRAM_ID)
        .token_program_b(TOKEN_PROGRAM_ID)
        .user_a(user_a.pubkey())
        .user_b(user_b.pubkey())
        .settlement_authority(settlement_authority.pubkey())
        .amount_a(deposit)
        .amount_b(AMOUNT_B)
        .expiry_timestamp(context.now() + 3600)
        .nonce(nonce)
        .instruction();
    context.send(create_ix, &[]).expect("CreateDvp");

    // Simulate a raw-lamport deposit into the WSOL escrow: bump its
    // lamports without calling SyncNative, so `amount` stays 0 (the exact
    // condition the finding describes).
    assert_eq!(get_token_balance(&context, &dvp_ata_a), 0);
    let mut escrow = context.get_account(&dvp_ata_a).expect("escrow exists");
    escrow.lamports += deposit;
    context.svm.set_account(dvp_ata_a, escrow).unwrap();

    let reject_ix = RejectDvpBuilder::new()
        .signer(user_b.pubkey())
        .swap_dvp(swap_dvp)
        .mint_a(mint_a)
        .mint_b(mint_b)
        .dvp_ata_a(dvp_ata_a)
        .dvp_ata_b(dvp_ata_b)
        .user_a_ata_a(user_a_ata_a)
        .user_b_ata_b(user_b_ata_b)
        .token_program_a(TOKEN_PROGRAM_ID)
        .token_program_b(TOKEN_PROGRAM_ID)
        .memo_program(MEMO_PROGRAM_ID)
        .leg_a_extras_count(0)
        .instruction();
    context.send(reject_ix, &[&user_b]).expect("RejectDvp");

    // The unsynced deposit was synced and refunded to user_a, not leaked
    // to the close recipient (user_b).
    assert_eq!(get_token_balance(&context, &user_a_ata_a), deposit);
    assert!(context.get_account(&dvp_ata_a).is_none());
}
