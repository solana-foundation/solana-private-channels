use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use pinocchio::{
    account::AccountView, address::Address, error::ProgramError,
    sysvars::instructions::INSTRUCTIONS_ID,
};
use pinocchio_associated_token_account::ID as ATA_PROGRAM_ID;
use pinocchio_token::ID as TOKEN_PROGRAM_ID;
use pinocchio_token_2022::ID as TOKEN_2022_PROGRAM_ID;

#[inline(always)]
pub fn verify_signer(info: &AccountView, expect_writable: bool) -> Result<(), ProgramError> {
    if !info.is_signer() {
        return Err(ProgramError::MissingRequiredSignature);
    }
    if expect_writable && !info.is_writable() {
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_account_owner(
    info: &AccountView,
    expected_owner: &Address,
) -> Result<(), ProgramError> {
    if !info.owned_by(expected_owner) {
        return Err(ProgramError::InvalidAccountOwner);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_mutability(info: &AccountView, expect_writable: bool) -> Result<(), ProgramError> {
    if expect_writable && !info.is_writable() {
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_system_account(info: &AccountView, is_writable: bool) -> Result<(), ProgramError> {
    if !info.owned_by(&pinocchio_system::ID) {
        return Err(ProgramError::InvalidAccountOwner);
    }

    if !info.is_data_empty() {
        return Err(ProgramError::AccountAlreadyInitialized);
    }

    if is_writable && !info.is_writable() {
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_system_program(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&pinocchio_system::ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_ata_program(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&ATA_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_current_program(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&PRIVATE_CHANNEL_ESCROW_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_token_programs(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&TOKEN_PROGRAM_ID) && info.address().ne(&TOKEN_2022_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    Ok(())
}

/// Confidential transfers exist only in Token-2022.
#[inline(always)]
pub fn verify_token_2022_program(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&TOKEN_2022_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    Ok(())
}

#[inline(always)]
pub fn verify_instructions_sysvar(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&INSTRUCTIONS_ID) {
        return Err(ProgramError::UnsupportedSysvar);
    }

    Ok(())
}

#[inline(always)]
pub fn validate_pda_account(
    seeds: &[&[u8]],
    program_id: &Address,
    expected_bump: u8,
    account_info: &AccountView,
) -> Result<Address, ProgramError> {
    let (calculated_pda, calculated_bump) = Address::find_program_address(seeds, program_id);

    if calculated_bump != expected_bump {
        return Err(ProgramError::InvalidInstructionData);
    }
    if account_info.address() != &calculated_pda {
        return Err(ProgramError::InvalidSeeds);
    }
    if !account_info.is_data_empty() && !account_info.owned_by(program_id) {
        return Err(ProgramError::InvalidAccountOwner);
    }

    Ok(calculated_pda)
}
