extern crate alloc;

use crate::{
    error::DvpSwapProgramError,
    processor::shared::account_check::{
        verify_account_owner, verify_ata_program, verify_signer, verify_system_account,
        verify_system_program, verify_token_program,
    },
    processor::shared::pda_utils::create_pda_account,
    processor::shared::token_utils::{validate_mint_extensions, verify_canonical_ata},
    require, require_len,
    state::swap_dvp::{SwapDvp, NONCE_TOMBSTONE_SEED, SWAP_DVP_SEED},
};
use pinocchio::{
    account::AccountView,
    address::Address,
    cpi::Seed,
    error::ProgramError,
    sysvars::{clock::Clock, rent::Rent, Sysvar},
    ProgramResult,
};
use pinocchio_associated_token_account::instructions::CreateIdempotent as CreateAtaIdempotent;

/// Max DvP lifetime (one year) as a duration from creation. Caps escrow rent lock-up.
const MAX_DVP_DURATION_SECS: i64 = 365 * 24 * 60 * 60;

/// Processes the CreateDvp instruction.
///
/// Permissionless: any signer can pay the rent. The DvP starts empty;
/// each leg is deposited by sending tokens to the leg's escrow ATA via
/// a raw SPL Transfer (the canonical funding path so that custodian
/// integrations need no custom program call).
///
/// The parties do not sign, and terms aren't bound to the PDA address, so
/// a record here is not proof the named parties agreed to its terms.
/// Clients must use a random `nonce` (anti-squat) and verify the stored
/// terms before funding or settling. See README "CreateDvp is
/// permissionless".
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Funds account/ATA creation rent
/// 1. `[writable]` swap_dvp - SwapDvp PDA to be created
/// 2. `[writable]` nonce_tombstone - Per-DvP nonce tombstone PDA, created here and never closed; rejects nonce reuse
/// 3. `[]` settlement_authority - Third party allowed to settle/cancel; must not be executable
/// 4. `[]` mint_a - Mint of the asset leg (seller delivers)
/// 5. `[]` mint_b - Mint of the cash leg (buyer delivers)
/// 6. `[writable]` dvp_ata_a - swap_dvp's ATA for mint_a (created here)
/// 7. `[writable]` dvp_ata_b - swap_dvp's ATA for mint_b (created here)
/// 8. `[]` system_program
/// 9. `[]` token_program_a - SPL Token or Token-2022; must own mint_a
/// 10. `[]` token_program_b - SPL Token or Token-2022; must own mint_b
/// 11. `[]` associated_token_program
///
/// # Instruction Data
/// * `user_a` (Pubkey) - Seller
/// * `user_b` (Pubkey) - Buyer
/// * `amount_a` (u64) - Asset leg size
/// * `amount_b` (u64) - Cash leg size
/// * `expiry_timestamp` (i64) - After this, settlement is rejected
/// * `nonce` (u64) - Disambiguates DvPs sharing all other seeds
/// * `earliest_settlement_timestamp` (Option<i64>) - If set, settlement
///   is also rejected before this timestamp
pub fn process_create_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = parse_instruction_data(instruction_data)?;

    let [payer_info, swap_dvp_info, nonce_tombstone_info, settlement_authority_info, mint_a_info, mint_b_info, dvp_ata_a_info, dvp_ata_b_info, system_program_info, token_program_a_info, token_program_b_info, associated_token_program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_system_account(swap_dvp_info, true)?;
    verify_system_program(system_program_info)?;
    verify_token_program(token_program_a_info)?;
    verify_token_program(token_program_b_info)?;
    verify_ata_program(associated_token_program_info)?;
    // settlement_authority receives the closed-account rent at Settle/Cancel.
    // An executable account can't be credited lamports (ExecutableLamportChange),
    // so reject it at creation rather than stranding funds until Reject/Reclaim.
    require!(
        !settlement_authority_info.executable(),
        DvpSwapProgramError::SettlementAuthorityExecutable
    );
    verify_account_owner(mint_a_info, token_program_a_info.address())?;
    verify_account_owner(mint_b_info, token_program_b_info.address())?;
    validate_mint_extensions(mint_a_info)?;
    validate_mint_extensions(mint_b_info)?;

    let now = Clock::get()?.unix_timestamp;
    validate_args(
        &args,
        settlement_authority_info.address(),
        mint_a_info.address(),
        mint_b_info.address(),
        now,
    )?;

    let nonce_bytes = args.nonce.to_le_bytes();
    let (expected_swap_dvp, bump) = Address::find_program_address(
        &[
            SWAP_DVP_SEED,
            settlement_authority_info.address().as_ref(),
            args.user_a.as_ref(),
            args.user_b.as_ref(),
            mint_a_info.address().as_ref(),
            mint_b_info.address().as_ref(),
            &nonce_bytes,
        ],
        program_id,
    );
    require!(
        swap_dvp_info.address() == &expected_swap_dvp,
        ProgramError::InvalidSeeds
    );

    // Nonce tombstone, derived from the SwapDvp address so it's 1:1 with
    // this trade's seeds. It's created below and never closed, so a
    // non-system owner here means the nonce was already used — reject
    // before re-creating the (closed) SwapDvp at the same address.
    let (expected_tombstone, tombstone_bump) = Address::find_program_address(
        &[NONCE_TOMBSTONE_SEED, expected_swap_dvp.as_ref()],
        program_id,
    );
    require!(
        nonce_tombstone_info.address() == &expected_tombstone,
        ProgramError::InvalidAccountData
    );
    require!(
        nonce_tombstone_info.owned_by(&pinocchio_system::ID),
        DvpSwapProgramError::NonceAlreadyUsed
    );

    // dvp_ata_a is the DvP PDA's ATA for mint_a (asset escrow).
    verify_canonical_ata(
        dvp_ata_a_info,
        swap_dvp_info.address(),
        mint_a_info.address(),
        token_program_a_info,
    )?;
    // dvp_ata_b is the DvP PDA's ATA for mint_b (cash escrow).
    verify_canonical_ata(
        dvp_ata_b_info,
        swap_dvp_info.address(),
        mint_b_info.address(),
        token_program_b_info,
    )?;

    let dvp = SwapDvp {
        bump,
        user_a: args.user_a,
        user_b: args.user_b,
        mint_a: *mint_a_info.address(),
        mint_b: *mint_b_info.address(),
        settlement_authority: *settlement_authority_info.address(),
        token_program_a: *token_program_a_info.address(),
        token_program_b: *token_program_b_info.address(),
        amount_a: args.amount_a,
        amount_b: args.amount_b,
        expiry_timestamp: args.expiry_timestamp,
        nonce: args.nonce,
        earliest_settlement_timestamp: args.earliest_settlement_timestamp,
    };
    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);

    let rent = Rent::get()?;
    create_pda_account(
        payer_info,
        &rent,
        SwapDvp::LEN,
        program_id,
        swap_dvp_info,
        swap_dvp_seeds,
    )?;

    // Mark this nonce used. The tombstone holds no data — its mere
    // existence (program-owned) is the signal — and is never closed.
    let tombstone_bump_bytes = [tombstone_bump];
    let tombstone_seeds = [
        Seed::from(NONCE_TOMBSTONE_SEED),
        Seed::from(expected_swap_dvp.as_ref()),
        Seed::from(&tombstone_bump_bytes),
    ];
    create_pda_account(
        payer_info,
        &rent,
        0,
        program_id,
        nonce_tombstone_info,
        tombstone_seeds,
    )?;

    CreateAtaIdempotent {
        funding_account: payer_info,
        account: dvp_ata_a_info,
        wallet: swap_dvp_info,
        mint: mint_a_info,
        system_program: system_program_info,
        token_program: token_program_a_info,
    }
    .invoke()?;

    CreateAtaIdempotent {
        funding_account: payer_info,
        account: dvp_ata_b_info,
        wallet: swap_dvp_info,
        mint: mint_b_info,
        system_program: system_program_info,
        token_program: token_program_b_info,
    }
    .invoke()?;

    let dvp_data = dvp.to_bytes();
    let mut data_slice = swap_dvp_info.try_borrow_mut()?;
    data_slice[..dvp_data.len()].copy_from_slice(&dvp_data);

    Ok(())
}

#[derive(Debug)]
struct CreateDvpArgs {
    user_a: Address,
    user_b: Address,
    amount_a: u64,
    amount_b: u64,
    expiry_timestamp: i64,
    nonce: u64,
    earliest_settlement_timestamp: Option<i64>,
}

/// Wire layout (variable, 97–105 bytes):
///   user_a(32) | user_b(32) |
///   amount_a(8) | amount_b(8) | expiry_timestamp(8) | nonce(8) |
///   earliest_tag(1) [ | earliest_payload(8) if tag == 1 ]
///
/// Codama-style Option encoding: payload is omitted when tag == 0.
fn parse_instruction_data(data: &[u8]) -> Result<CreateDvpArgs, ProgramError> {
    // Required prefix: two pubkeys + four u64/i64 fields + option tag.
    require_len!(data, 32 * 2 + 8 * 4 + 1);

    let mut offset = 0;

    let mut user_a = [0u8; 32];
    user_a.copy_from_slice(&data[offset..offset + 32]);
    offset += 32;

    let mut user_b = [0u8; 32];
    user_b.copy_from_slice(&data[offset..offset + 32]);
    offset += 32;

    let amount_a = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let amount_b = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let expiry_timestamp = i64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let nonce = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let earliest_settlement_timestamp = match data[offset] {
        0 => None,
        1 => {
            require_len!(data, offset + 1 + 8);
            Some(i64::from_le_bytes(
                data[offset + 1..offset + 9]
                    .try_into()
                    .map_err(|_| ProgramError::InvalidInstructionData)?,
            ))
        }
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    Ok(CreateDvpArgs {
        user_a: Address::new_from_array(user_a),
        user_b: Address::new_from_array(user_b),
        amount_a,
        amount_b,
        expiry_timestamp,
        nonce,
        earliest_settlement_timestamp,
    })
}

/// Reject DvPs that can never settle, are degenerate, or have leg
/// configurations the rest of the processor would mishandle later.
fn validate_args(
    args: &CreateDvpArgs,
    settlement_authority: &Address,
    mint_a: &Address,
    mint_b: &Address,
    now: i64,
) -> Result<(), ProgramError> {
    require!(
        args.expiry_timestamp > now,
        DvpSwapProgramError::ExpiryNotInFuture
    );
    require!(
        args.expiry_timestamp <= now.saturating_add(MAX_DVP_DURATION_SECS),
        DvpSwapProgramError::ExpiryTooFarInFuture
    );
    if let Some(earliest) = args.earliest_settlement_timestamp {
        require!(
            earliest <= args.expiry_timestamp,
            DvpSwapProgramError::EarliestAfterExpiry
        );
    }
    require!(args.user_a != args.user_b, DvpSwapProgramError::SelfDvp);
    require!(
        settlement_authority != &args.user_a && settlement_authority != &args.user_b,
        DvpSwapProgramError::SettlementAuthorityIsParty
    );
    require!(mint_a != mint_b, DvpSwapProgramError::SameMint);
    require!(
        args.amount_a != 0 && args.amount_b != 0,
        DvpSwapProgramError::ZeroAmount
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_780_000_000;

    fn args() -> CreateDvpArgs {
        CreateDvpArgs {
            user_a: Address::new_from_array([1u8; 32]),
            user_b: Address::new_from_array([2u8; 32]),
            amount_a: 1_000,
            amount_b: 2_000,
            expiry_timestamp: NOW + 3_600,
            nonce: 0,
            earliest_settlement_timestamp: None,
        }
    }

    fn settlement_authority() -> Address {
        Address::new_from_array([3u8; 32])
    }

    fn mint_a() -> Address {
        Address::new_from_array([10u8; 32])
    }
    fn mint_b() -> Address {
        Address::new_from_array([20u8; 32])
    }

    fn assert_custom(err: ProgramError, expected: DvpSwapProgramError) {
        assert_eq!(err, ProgramError::Custom(expected as u32));
    }

    #[test]
    fn validate_args_accepts_well_formed_input() {
        validate_args(&args(), &settlement_authority(), &mint_a(), &mint_b(), NOW)
            .expect("baseline must pass");
    }

    #[test]
    fn validate_args_accepts_earliest_equal_to_expiry() {
        let mut a = args();
        a.earliest_settlement_timestamp = Some(a.expiry_timestamp);
        validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW)
            .expect("earliest == expiry is allowed");
    }

    #[test]
    fn validate_args_accepts_earliest_in_the_past() {
        let mut a = args();
        a.earliest_settlement_timestamp = Some(NOW - 100);
        validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW)
            .expect("past earliest means 'any time'");
    }

    #[test]
    fn validate_args_rejects_expiry_at_now() {
        let mut a = args();
        a.expiry_timestamp = NOW;
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::ExpiryNotInFuture);
    }

    #[test]
    fn validate_args_rejects_expiry_in_past() {
        let mut a = args();
        a.expiry_timestamp = NOW - 1;
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::ExpiryNotInFuture);
    }

    #[test]
    fn validate_args_accepts_expiry_at_max_horizon() {
        let mut a = args();
        a.expiry_timestamp = NOW + MAX_DVP_DURATION_SECS;
        validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW)
            .expect("expiry exactly at the cap is allowed");
    }

    #[test]
    fn validate_args_rejects_expiry_beyond_max_horizon() {
        let mut a = args();
        a.expiry_timestamp = NOW + MAX_DVP_DURATION_SECS + 1;
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::ExpiryTooFarInFuture);
    }

    #[test]
    fn validate_args_rejects_earliest_after_expiry() {
        let mut a = args();
        a.earliest_settlement_timestamp = Some(a.expiry_timestamp + 1);
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::EarliestAfterExpiry);
    }

    #[test]
    fn validate_args_rejects_self_dvp() {
        let mut a = args();
        a.user_b = a.user_a;
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::SelfDvp);
    }

    #[test]
    fn validate_args_rejects_settlement_authority_equal_to_user_a() {
        let a = args();
        let err = validate_args(&a, &a.user_a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::SettlementAuthorityIsParty);
    }

    #[test]
    fn validate_args_rejects_settlement_authority_equal_to_user_b() {
        let a = args();
        let err = validate_args(&a, &a.user_b, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::SettlementAuthorityIsParty);
    }

    #[test]
    fn validate_args_rejects_same_mint() {
        let err =
            validate_args(&args(), &settlement_authority(), &mint_a(), &mint_a(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::SameMint);
    }

    #[test]
    fn validate_args_rejects_zero_amount_a() {
        let mut a = args();
        a.amount_a = 0;
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::ZeroAmount);
    }

    #[test]
    fn validate_args_rejects_zero_amount_b() {
        let mut a = args();
        a.amount_b = 0;
        let err =
            validate_args(&a, &settlement_authority(), &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, DvpSwapProgramError::ZeroAmount);
    }
}
