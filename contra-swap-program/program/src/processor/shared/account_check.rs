use pinocchio::{account::AccountView, address::Address, error::ProgramError};
use pinocchio_associated_token_account::ID as ATA_PROGRAM_ID;
use pinocchio_token::ID as TOKEN_PROGRAM_ID;

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
pub fn verify_token_program(info: &AccountView) -> Result<(), ProgramError> {
    if info.address().ne(&TOKEN_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }
    Ok(())
}

#[inline(always)]
pub fn verify_token_program_account(info: &AccountView) -> Result<(), ProgramError> {
    if !info.owned_by(&TOKEN_PROGRAM_ID) {
        return Err(ProgramError::InvalidAccountOwner);
    }
    Ok(())
}
