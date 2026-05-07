extern crate alloc;

use crate::{
    error::ContraSwapProgramError,
    processor::shared::account_check::{
        verify_account_owner, verify_ata_program, verify_signer, verify_system_account,
        verify_system_program, verify_token_program,
    },
    processor::shared::pda_utils::create_pda_account,
    processor::shared::token_utils::verify_canonical_ata,
    require_len,
    state::swap_dvp::{SwapDvp, SWAP_DVP_SEED},
};
use pinocchio::{
    account::AccountView,
    address::Address,
    error::ProgramError,
    sysvars::{clock::Clock, rent::Rent, Sysvar},
    ProgramResult,
};
use pinocchio_associated_token_account::instructions::Create as CreateAta;

/// Processes the CreateDvp instruction.
///
/// Permissionless: any signer can pay the rent. The DvP starts empty;
/// each leg is deposited by sending tokens to the leg's escrow ATA via
/// a raw SPL Transfer (the canonical funding path so that custodian
/// integrations need no custom program call).
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Funds account/ATA creation rent
/// 1. `[writable]` swap_dvp - SwapDvp PDA to be created
/// 2. `[]` mint_a - Mint of the asset leg (seller delivers)
/// 3. `[]` mint_b - Mint of the cash leg (buyer delivers)
/// 4. `[writable]` dvp_ata_a - swap_dvp's ATA for mint_a (created here)
/// 5. `[writable]` dvp_ata_b - swap_dvp's ATA for mint_b (created here)
/// 6. `[]` system_program
/// 7. `[]` token_program
/// 8. `[]` associated_token_program
///
/// # Instruction Data
/// * `user_a` (Pubkey) - Seller
/// * `user_b` (Pubkey) - Buyer
/// * `settlement_authority` (Pubkey) - Only party allowed to settle
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

    let [payer_info, swap_dvp_info, mint_a_info, mint_b_info, dvp_ata_a_info, dvp_ata_b_info, system_program_info, token_program_info, associated_token_program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_system_account(swap_dvp_info, true)?;
    verify_system_program(system_program_info)?;
    verify_token_program(token_program_info)?;
    verify_ata_program(associated_token_program_info)?;
    verify_account_owner(mint_a_info, token_program_info.address())?;
    verify_account_owner(mint_b_info, token_program_info.address())?;

    let now = Clock::get()?.unix_timestamp;
    validate_args(&args, mint_a_info.address(), mint_b_info.address(), now)?;

    let nonce_bytes = args.nonce.to_le_bytes();
    let (expected_swap_dvp, bump) = Address::find_program_address(
        &[
            SWAP_DVP_SEED,
            args.settlement_authority.as_ref(),
            args.user_a.as_ref(),
            args.user_b.as_ref(),
            mint_a_info.address().as_ref(),
            mint_b_info.address().as_ref(),
            &nonce_bytes,
        ],
        program_id,
    );
    if swap_dvp_info.address() != &expected_swap_dvp {
        return Err(ProgramError::InvalidSeeds);
    }

    let dvp = SwapDvp {
        bump,
        user_a: args.user_a,
        user_b: args.user_b,
        mint_a: *mint_a_info.address(),
        mint_b: *mint_b_info.address(),
        settlement_authority: args.settlement_authority,
        amount_a: args.amount_a,
        amount_b: args.amount_b,
        expiry_timestamp: args.expiry_timestamp,
        nonce: args.nonce,
        earliest_settlement_timestamp: args.earliest_settlement_timestamp,
    };
    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);

    // dvp_ata_a is the DvP PDA's ATA for mint_a (asset escrow).
    verify_canonical_ata(
        dvp_ata_a_info,
        swap_dvp_info.address(),
        mint_a_info.address(),
        token_program_info,
    )?;
    // dvp_ata_b is the DvP PDA's ATA for mint_b (cash escrow).
    verify_canonical_ata(
        dvp_ata_b_info,
        swap_dvp_info.address(),
        mint_b_info.address(),
        token_program_info,
    )?;

    let rent = Rent::get()?;
    create_pda_account(
        payer_info,
        &rent,
        SwapDvp::LEN,
        program_id,
        swap_dvp_info,
        swap_dvp_seeds,
        None,
    )?;

    CreateAta {
        funding_account: payer_info,
        account: dvp_ata_a_info,
        wallet: swap_dvp_info,
        mint: mint_a_info,
        system_program: system_program_info,
        token_program: token_program_info,
    }
    .invoke()?;

    CreateAta {
        funding_account: payer_info,
        account: dvp_ata_b_info,
        wallet: swap_dvp_info,
        mint: mint_b_info,
        system_program: system_program_info,
        token_program: token_program_info,
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
    settlement_authority: Address,
    amount_a: u64,
    amount_b: u64,
    expiry_timestamp: i64,
    nonce: u64,
    earliest_settlement_timestamp: Option<i64>,
}

/// Wire layout (variable, 129–137 bytes):
///   user_a(32) | user_b(32) | settlement_authority(32) |
///   amount_a(8) | amount_b(8) | expiry_timestamp(8) | nonce(8) |
///   earliest_tag(1) [ | earliest_payload(8) if tag == 1 ]
///
/// Codama-style Option encoding: payload is omitted when tag == 0.
fn parse_instruction_data(data: &[u8]) -> Result<CreateDvpArgs, ProgramError> {
    // Required prefix: three pubkeys + four u64/i64 fields + option tag.
    require_len!(data, 32 * 3 + 8 * 4 + 1);

    let mut offset = 0;

    let mut user_a = [0u8; 32];
    user_a.copy_from_slice(&data[offset..offset + 32]);
    offset += 32;

    let mut user_b = [0u8; 32];
    user_b.copy_from_slice(&data[offset..offset + 32]);
    offset += 32;

    let mut settlement_authority = [0u8; 32];
    settlement_authority.copy_from_slice(&data[offset..offset + 32]);
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
        settlement_authority: Address::new_from_array(settlement_authority),
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
    mint_a: &Address,
    mint_b: &Address,
    now: i64,
) -> Result<(), ProgramError> {
    if args.expiry_timestamp <= now {
        return Err(ContraSwapProgramError::ExpiryNotInFuture.into());
    }
    if let Some(earliest) = args.earliest_settlement_timestamp {
        if earliest > args.expiry_timestamp {
            return Err(ContraSwapProgramError::EarliestAfterExpiry.into());
        }
    }
    if args.user_a == args.user_b {
        return Err(ContraSwapProgramError::SelfDvp.into());
    }
    if mint_a == mint_b {
        return Err(ContraSwapProgramError::SameMint.into());
    }
    if args.amount_a == 0 || args.amount_b == 0 {
        return Err(ContraSwapProgramError::ZeroAmount.into());
    }
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
            settlement_authority: Address::new_from_array([3u8; 32]),
            amount_a: 1_000,
            amount_b: 2_000,
            expiry_timestamp: NOW + 3_600,
            nonce: 0,
            earliest_settlement_timestamp: None,
        }
    }

    fn mint_a() -> Address {
        Address::new_from_array([10u8; 32])
    }
    fn mint_b() -> Address {
        Address::new_from_array([20u8; 32])
    }

    fn assert_custom(err: ProgramError, expected: ContraSwapProgramError) {
        assert_eq!(err, ProgramError::Custom(expected as u32));
    }

    #[test]
    fn validate_args_accepts_well_formed_input() {
        validate_args(&args(), &mint_a(), &mint_b(), NOW).expect("baseline must pass");
    }

    #[test]
    fn validate_args_accepts_earliest_equal_to_expiry() {
        let mut a = args();
        a.earliest_settlement_timestamp = Some(a.expiry_timestamp);
        validate_args(&a, &mint_a(), &mint_b(), NOW).expect("earliest == expiry is allowed");
    }

    #[test]
    fn validate_args_accepts_earliest_in_the_past() {
        let mut a = args();
        a.earliest_settlement_timestamp = Some(NOW - 100);
        validate_args(&a, &mint_a(), &mint_b(), NOW).expect("past earliest means 'any time'");
    }

    #[test]
    fn validate_args_rejects_expiry_at_now() {
        let mut a = args();
        a.expiry_timestamp = NOW;
        let err = validate_args(&a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::ExpiryNotInFuture);
    }

    #[test]
    fn validate_args_rejects_expiry_in_past() {
        let mut a = args();
        a.expiry_timestamp = NOW - 1;
        let err = validate_args(&a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::ExpiryNotInFuture);
    }

    #[test]
    fn validate_args_rejects_earliest_after_expiry() {
        let mut a = args();
        a.earliest_settlement_timestamp = Some(a.expiry_timestamp + 1);
        let err = validate_args(&a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::EarliestAfterExpiry);
    }

    #[test]
    fn validate_args_rejects_self_dvp() {
        let mut a = args();
        a.user_b = a.user_a;
        let err = validate_args(&a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::SelfDvp);
    }

    #[test]
    fn validate_args_rejects_same_mint() {
        let err = validate_args(&args(), &mint_a(), &mint_a(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::SameMint);
    }

    #[test]
    fn validate_args_rejects_zero_amount_a() {
        let mut a = args();
        a.amount_a = 0;
        let err = validate_args(&a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::ZeroAmount);
    }

    #[test]
    fn validate_args_rejects_zero_amount_b() {
        let mut a = args();
        a.amount_b = 0;
        let err = validate_args(&a, &mint_a(), &mint_b(), NOW).unwrap_err();
        assert_custom(err, ContraSwapProgramError::ZeroAmount);
    }
}
