//! Shared setup, types, and helpers used by all fuzz harnesses.

use private_channel_escrow_program_client::instructions::{
    AddOperatorBuilder, AllowMintBuilder, CreateInstanceBuilder,
};
use solana_sdk::{pubkey, pubkey::Pubkey, system_program::ID as SYSTEM_PROGRAM_ID};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use trident_fuzz::fuzzing::*;

pub const PRIVATE_CHANNEL_ESCROW_PROGRAM_ID: Pubkey =
    pubkey!("9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU");
pub const SPL_TOKEN_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

/// Clamp raw fuzz amounts to [1, 999_999].
pub fn clamp_amount(raw: u64) -> u64 {
    (raw % 1_000_000).max(1)
}

// ── Account addresses ─────────────────────────────────────────────────────────

#[derive(Default)]
pub struct AccountAddresses {
    pub admin: AddressStorage,
    /// Random seed used to derive the instance PDA — fresh each iteration.
    pub instance_seed: AddressStorage,
    /// Escrow instance PDA: `["instance", instance_seed]`.
    pub instance: AddressStorage,
    /// Self-CPI event authority PDA: `["event_authority"]`.
    pub event_authority: AddressStorage,
    pub mint: AddressStorage,
    /// Allowlist PDA: `["allowed_mint", instance, mint]`.
    pub allowed_mint: AddressStorage,
    /// Escrow ATA (ATA of the instance PDA).
    pub instance_ata: AddressStorage,
    /// Withdrawal nonce bitmap PDA: `["withdrawal_bitmap", instance]`.
    pub withdrawal_bitmap: AddressStorage,
    /// Operator keypair authorised to call `ReleaseFunds` and `RotateBitmap`.
    pub operator: AddressStorage,
    /// Operator permission PDA: `["operator", instance, operator]`.
    pub operator_pda: AddressStorage,
    pub user: AddressStorage,
    /// User's token account (ATA of the user keypair).
    pub user_ata: AddressStorage,
}

// ── Setup ─────────────────────────────────────────────────────────────────────

/// Bootstrap a complete escrow environment for one fuzz iteration:
/// instance → mint → allowlist → operator → user ATA (funded with 10T tokens).
///
/// Returns the user's actual token balance after minting, used by harnesses to
/// track the user-side balance invariant.
///
/// Called from `#[init]` in every harness so setup logic is never duplicated.
pub fn setup_escrow(trident: &mut Trident, accounts: &mut AccountAddresses) -> u64 {
    let payer = trident.payer().pubkey();

    // ── CreateInstance ────────────────────────────────────────────────────────
    let admin = accounts.admin.insert(trident, None);
    let instance_seed = accounts.instance_seed.insert(trident, None);
    let (instance_pda, instance_bump) = Pubkey::find_program_address(
        &[b"instance", instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    accounts.instance.insert_with_address(instance_pda);
    let (event_authority_pda, _) =
        Pubkey::find_program_address(&[b"event_authority"], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);
    accounts
        .event_authority
        .insert_with_address(event_authority_pda);
    let (withdrawal_bitmap_pda, bitmap_bump) = Pubkey::find_program_address(
        &[b"withdrawal_bitmap", instance_pda.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    accounts
        .withdrawal_bitmap
        .insert_with_address(withdrawal_bitmap_pda);

    let res = trident.process_transaction(
        &[CreateInstanceBuilder::new()
            .payer(payer)
            .admin(admin)
            .instance_seed(instance_seed)
            .instance(instance_pda)
            .withdrawal_bitmap(withdrawal_bitmap_pda)
            .system_program(SYSTEM_PROGRAM_ID)
            .event_authority(event_authority_pda)
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .bump(instance_bump)
            .bitmap_bump(bitmap_bump)
            .instruction()],
        Some("create_instance"),
    );
    assert!(res.is_success(), "create_instance failed: {}", res.logs());

    // ── Mint ──────────────────────────────────────────────────────────────────
    let mint = accounts.mint.insert(trident, None);
    let init_mint_ixs = trident.initialize_mint(&payer, &mint, 6, &payer, None);
    assert!(trident
        .process_transaction(&init_mint_ixs, Some("init_mint"))
        .is_success());

    // ── AllowMint ─────────────────────────────────────────────────────────────
    let (allowed_mint_pda, allowed_mint_bump) = Pubkey::find_program_address(
        &[b"allowed_mint", instance_pda.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    accounts.allowed_mint.insert_with_address(allowed_mint_pda);
    let instance_ata =
        get_associated_token_address_with_program_id(&instance_pda, &mint, &SPL_TOKEN_ID);
    accounts.instance_ata.insert_with_address(instance_ata);

    let res = trident.process_transaction(
        &[AllowMintBuilder::new()
            .payer(payer)
            .admin(admin)
            .instance(instance_pda)
            .mint(mint)
            .allowed_mint(allowed_mint_pda)
            .instance_ata(instance_ata)
            .bump(allowed_mint_bump)
            .instruction()],
        Some("allow_mint"),
    );
    assert!(res.is_success(), "allow_mint failed: {}", res.logs());

    // ── AddOperator ───────────────────────────────────────────────────────────
    let operator = accounts.operator.insert(trident, None);
    let (operator_pda, operator_bump) = Pubkey::find_program_address(
        &[b"operator", instance_pda.as_ref(), operator.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    accounts.operator_pda.insert_with_address(operator_pda);

    let res = trident.process_transaction(
        &[AddOperatorBuilder::new()
            .payer(payer)
            .admin(admin)
            .instance(instance_pda)
            .operator(operator)
            .operator_pda(operator_pda)
            .bump(operator_bump)
            .instruction()],
        Some("add_operator"),
    );
    assert!(res.is_success(), "add_operator failed: {}", res.logs());

    // ── User + ATA + fund ─────────────────────────────────────────────────────
    let user = accounts.user.insert(trident, None);
    let user_ata = get_associated_token_address_with_program_id(&user, &mint, &SPL_TOKEN_ID);
    accounts.user_ata.insert_with_address(user_ata);

    let create_ata_ix = trident.initialize_associated_token_account(&payer, &mint, &user);
    assert!(trident
        .process_transaction(&[create_ata_ix], Some("create_user_ata"))
        .is_success());

    let mint_to_ix = trident.mint_to(&user_ata, &mint, &payer, 10_000_000_000_000);
    assert!(trident
        .process_transaction(&[mint_to_ix], Some("mint_to_user"))
        .is_success());

    token_amount(trident, &user_ata)
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/// Read the token balance of an SPL token account. Returns 0 if not found.
pub fn token_amount(trident: &mut Trident, account: &Pubkey) -> u64 {
    trident
        .get_token_account(*account)
        .map(|t| t.account.amount)
        .unwrap_or(0)
}
