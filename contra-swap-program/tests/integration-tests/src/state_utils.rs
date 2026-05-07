use contra_swap_program_client::instructions::{
    CancelDvpBuilder, CreateDvpBuilder, ReclaimDvpBuilder, RejectDvpBuilder, SettleDvpBuilder,
};
use litesvm::types::TransactionMetadata;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_token::{instruction::transfer as spl_transfer, ID as TOKEN_PROGRAM_ID};

use crate::utils::{create_ata, dvp_ata, fund_wallet_ata, set_mint, swap_dvp_pda, TestContext};

pub const AMOUNT_A: u64 = 75_000;
pub const AMOUNT_B: u64 = 50_000;
pub const INITIAL_BALANCE: u64 = 200_000;

/// Pubkeys and ATAs for one DvP, built by `setup_dvp`.
pub struct DvpFixture {
    pub user_a: Keypair,
    pub user_b: Keypair,
    pub settlement_authority: Keypair,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub swap_dvp: Pubkey,
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
/// cross ATAs pre-created (Settle's Transfer requires them initialized),
/// and the SwapDvp PDA address derived for `nonce`. Does not call
/// CreateDvp; use `assert_create_dvp` for that.
pub fn setup_dvp(context: &mut TestContext, nonce: u64) -> DvpFixture {
    let user_a = Keypair::new();
    let user_b = Keypair::new();
    let settlement_authority = Keypair::new();
    let mint_a = Keypair::new().pubkey();
    let mint_b = Keypair::new().pubkey();

    set_mint(context, &mint_a);
    set_mint(context, &mint_b);

    let user_a_ata_a = fund_wallet_ata(context, &user_a, &mint_a, INITIAL_BALANCE);
    let user_b_ata_b = fund_wallet_ata(context, &user_b, &mint_b, INITIAL_BALANCE);
    let user_a_ata_b = create_ata(context, &user_a.pubkey(), &mint_b);
    let user_b_ata_a = create_ata(context, &user_b.pubkey(), &mint_a);

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
        dvp_ata_a: dvp_ata(&swap_dvp, &mint_a),
        dvp_ata_b: dvp_ata(&swap_dvp, &mint_b),
        user_a,
        user_b,
        settlement_authority,
        mint_a,
        mint_b,
        swap_dvp,
        user_a_ata_a,
        user_a_ata_b,
        user_b_ata_a,
        user_b_ata_b,
        expiry: context.now() + 3600,
        nonce,
    }
}

pub fn assert_create_dvp(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
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
        .nonce(fixture.nonce)
        .instruction();
    context.send(ix, &[]).expect("CreateDvp")
}

/// Fund the asset leg by transferring `amount` of `mint_a` from
/// `user_a_ata_a` to the escrow `dvp_ata_a` via a raw SPL Transfer.
/// This is the canonical funding path — the program has no FundDvp
/// instruction; legs are funded by ordinary token transfers so that
/// custodian integrations need no custom program call.
pub fn assert_fund_a_amount(
    context: &mut TestContext,
    fixture: &DvpFixture,
    amount: u64,
) -> TransactionMetadata {
    let ix = spl_transfer(
        &TOKEN_PROGRAM_ID,
        &fixture.user_a_ata_a,
        &fixture.dvp_ata_a,
        &fixture.user_a.pubkey(),
        &[],
        amount,
    )
    .expect("build SPL transfer A");
    context.send(ix, &[&fixture.user_a]).expect("fund leg A")
}

pub fn assert_fund_b_amount(
    context: &mut TestContext,
    fixture: &DvpFixture,
    amount: u64,
) -> TransactionMetadata {
    let ix = spl_transfer(
        &TOKEN_PROGRAM_ID,
        &fixture.user_b_ata_b,
        &fixture.dvp_ata_b,
        &fixture.user_b.pubkey(),
        &[],
        amount,
    )
    .expect("build SPL transfer B");
    context.send(ix, &[&fixture.user_b]).expect("fund leg B")
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
        .dvp_source_ata(fixture.dvp_ata_a)
        .signer_dest_ata(fixture.user_a_ata_a)
        .instruction();
    context.send(ix, &[&fixture.user_a]).expect("ReclaimDvp A")
}

pub fn assert_settle_dvp(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
    let ix = SettleDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_b(fixture.user_a_ata_b)
        .user_b_ata_a(fixture.user_b_ata_a)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .instruction();
    context
        .send(ix, &[&fixture.settlement_authority])
        .expect("SettleDvp")
}

pub fn assert_cancel_dvp(context: &mut TestContext, fixture: &DvpFixture) -> TransactionMetadata {
    let ix = CancelDvpBuilder::new()
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
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
        .settlement_authority(fixture.settlement_authority.pubkey())
        .swap_dvp(fixture.swap_dvp)
        .dvp_ata_a(fixture.dvp_ata_a)
        .dvp_ata_b(fixture.dvp_ata_b)
        .user_a_ata_a(fixture.user_a_ata_a)
        .user_b_ata_b(fixture.user_b_ata_b)
        .instruction();
    context.send(ix, &[signer]).expect("RejectDvp")
}
