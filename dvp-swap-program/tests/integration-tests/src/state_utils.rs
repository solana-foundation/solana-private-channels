use dvp_swap_program_client::instructions::{
    CancelDvpBuilder, CreateDvpBuilder, ReclaimDvpBuilder, RejectDvpBuilder, SettleDvpBuilder,
};
use litesvm::types::TransactionMetadata;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_token_2022::instruction::transfer_checked;

use crate::utils::{
    create_ata, dvp_ata, fund_wallet_ata, nonce_tombstone_pda, set_mint, swap_dvp_pda, TestContext,
    MEMO_PROGRAM_ID, TOKEN_PROGRAM_ID,
};

pub const AMOUNT_A: u64 = 75_000;
pub const AMOUNT_B: u64 = 50_000;
pub const INITIAL_BALANCE: u64 = 200_000;

/// Pubkeys and ATAs for one DvP, built by `setup_dvp`. `token_program_a`
/// and `token_program_b` default to legacy SPL Token for backwards
/// compatibility with the original tests; tests that exercise Token-2022
/// (or mixed-program) lifecycles construct the fixture directly with
/// `setup_dvp_with_programs`.
pub struct DvpFixture {
    pub user_a: Keypair,
    pub user_b: Keypair,
    pub settlement_authority: Keypair,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub token_program_a: Pubkey,
    pub token_program_b: Pubkey,
    pub swap_dvp: Pubkey,
    pub nonce_tombstone: Pubkey,
    pub user_a_ata_a: Pubkey,
    pub user_a_ata_b: Pubkey,
    pub user_b_ata_a: Pubkey,
    pub user_b_ata_b: Pubkey,
    pub dvp_ata_a: Pubkey,
    pub dvp_ata_b: Pubkey,
    pub expiry: i64,
    pub nonce: u64,
}

/// Mints, two users with their own leg pre-funded, the settle-recipient
/// cross ATAs pre-created (Settle's TransferChecked requires them
/// initialized), and the SwapDvp PDA address derived for `nonce`. Does
/// not call CreateDvp; use `assert_create_dvp` for that.
///
/// Each leg's mint is created as a *bare* mint owned by the supplied
/// `token_program_*`. Tests that need extensions on a leg's mint should
/// overwrite the mint with one of the `set_mint_2022_with_*` builders
/// after this returns but before calling `assert_create_dvp`.
pub fn setup_dvp_with_programs(
    context: &mut TestContext,
    nonce: u64,
    token_program_a: Pubkey,
    token_program_b: Pubkey,
) -> DvpFixture {
    let user_a = Keypair::new();
    let user_b = Keypair::new();
    let settlement_authority = Keypair::new();
    let mint_a = Keypair::new().pubkey();
    let mint_b = Keypair::new().pubkey();

    set_mint(context, &mint_a, &token_program_a);
    set_mint(context, &mint_b, &token_program_b);

    let user_a_ata_a =
        fund_wallet_ata(context, &user_a, &mint_a, INITIAL_BALANCE, &token_program_a);
    let user_b_ata_b =
        fund_wallet_ata(context, &user_b, &mint_b, INITIAL_BALANCE, &token_program_b);
    let user_a_ata_b = create_ata(context, &user_a.pubkey(), &mint_b, &token_program_b);
    let user_b_ata_a = create_ata(context, &user_b.pubkey(), &mint_a, &token_program_a);

    context.airdrop_if_required(&settlement_authority.pubkey(), 1_000_000_000);

    let (swap_dvp, _) = swap_dvp_pda(
        &settlement_authority.pubkey(),
        &user_a.pubkey(),
        &user_b.pubkey(),
        &mint_a,
        &mint_b,
        nonce,
    );

    DvpFixture {
        dvp_ata_a: dvp_ata(&swap_dvp, &mint_a, &token_program_a),
        dvp_ata_b: dvp_ata(&swap_dvp, &mint_b, &token_program_b),
        nonce_tombstone: nonce_tombstone_pda(&swap_dvp).0,
        user_a,
        user_b,
        settlement_authority,
        mint_a,
        mint_b,
        token_program_a,
        token_program_b,
        swap_dvp,
        user_a_ata_a,
        user_a_ata_b,
        user_b_ata_a,
        user_b_ata_b,
        expiry: context.now() + 3600,
        nonce,
    }
}

/// Backwards-compatible default: both legs on legacy SPL Token.
pub fn setup_dvp(context: &mut TestContext, nonce: u64) -> DvpFixture {
    setup_dvp_with_programs(context, nonce, TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID)
}

pub fn assert_create_dvp(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
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
        .nonce(fixture.nonce)
        .instruction();
    context.send(ix, &[]).expect("CreateDvp")
}

/// Fund a leg by issuing a raw `TransferChecked` (the canonical funding
/// path — the program has no FundDvp instruction). `TransferChecked`
/// works against both legacy SPL Token and Token-2022 programs.
fn fund_leg(
    context: &mut TestContext,
    user: &Keypair,
    source_ata: &Pubkey,
    dest_ata: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    amount: u64,
) -> TransactionMetadata {
    let ix = transfer_checked(
        token_program,
        source_ata,
        mint,
        dest_ata,
        &user.pubkey(),
        &[],
        amount,
        6, // matches the `decimals` field in set_mint / set_mint_2022_*
    )
    .expect("build TransferChecked");
    context.send(ix, &[user]).expect("fund leg")
}

pub fn assert_fund_a_amount(
    context: &mut TestContext,
    fixture: &DvpFixture,
    amount: u64,
) -> TransactionMetadata {
    fund_leg(
        context,
        &fixture.user_a,
        &fixture.user_a_ata_a,
        &fixture.dvp_ata_a,
        &fixture.mint_a,
        &fixture.token_program_a,
        amount,
    )
}

pub fn assert_fund_b_amount(
    context: &mut TestContext,
    fixture: &DvpFixture,
    amount: u64,
) -> TransactionMetadata {
    fund_leg(
        context,
        &fixture.user_b,
        &fixture.user_b_ata_b,
        &fixture.dvp_ata_b,
        &fixture.mint_b,
        &fixture.token_program_b,
        amount,
    )
}

pub fn assert_fund_a(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
    assert_fund_a_amount(context, fixture, AMOUNT_A)
}

pub fn assert_fund_b(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
    assert_fund_b_amount(context, fixture, AMOUNT_B)
}

pub fn assert_reclaim_a(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
    let ix = ReclaimDvpBuilder::new()
        .signer(fixture.user_a.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .mint(fixture.mint_a)
        .dvp_source_ata(fixture.dvp_ata_a)
        .signer_dest_ata(fixture.user_a_ata_a)
        .token_program(fixture.token_program_a)
        .memo_program(MEMO_PROGRAM_ID)
        .instruction();
    context.send(ix, &[&fixture.user_a]).expect("ReclaimDvp A")
}

pub fn assert_settle_dvp(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
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
        .leg_a_extras_count(0)
        .instruction();
    context
        .send(ix, &[&fixture.settlement_authority])
        .expect("SettleDvp")
}

pub fn assert_cancel_dvp(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
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
        .leg_a_extras_count(0)
        .instruction();
    context
        .send(ix, &[&fixture.settlement_authority])
        .expect("CancelDvp")
}

pub fn assert_reject_dvp(
    context: &mut TestContext,
    fixture: &DvpFixture,
    signer: &Keypair,
) -> TransactionMetadata {
    let ix = RejectDvpBuilder::new()
        .signer(signer.pubkey())
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
    context.send(ix, &[signer]).expect("RejectDvp")
}
