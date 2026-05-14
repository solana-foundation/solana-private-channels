use pinocchio::{account::AccountView, address::Address, error::ProgramError};
use pinocchio_associated_token_account::ID as ATA_PROGRAM_ID;
use pinocchio_token::ID as TOKEN_PROGRAM_ID;
use pinocchio_token_2022::ID as TOKEN_2022_PROGRAM_ID;

use crate::require;

#[inline(always)]
pub fn verify_signer(info: &AccountView, expect_writable: bool) -> Result<(), ProgramError> {
    require!(info.is_signer(), ProgramError::MissingRequiredSignature);
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
    require!(
        info.owned_by(expected_owner),
        ProgramError::InvalidAccountOwner
    );
    Ok(())
}

#[inline(always)]
pub fn verify_system_account(info: &AccountView, is_writable: bool) -> Result<(), ProgramError> {
    require!(
        info.owned_by(&pinocchio_system::ID),
        ProgramError::InvalidAccountOwner
    );
    require!(
        info.is_data_empty(),
        ProgramError::AccountAlreadyInitialized
    );
    if is_writable && !info.is_writable() {
        return Err(ProgramError::InvalidAccountData);
    }
    Ok(())
}

#[inline(always)]
pub fn verify_system_program(info: &AccountView) -> Result<(), ProgramError> {
    require!(
        info.address().eq(&pinocchio_system::ID),
        ProgramError::IncorrectProgramId
    );
    Ok(())
}

#[inline(always)]
pub fn verify_ata_program(info: &AccountView) -> Result<(), ProgramError> {
    require!(
        info.address().eq(&ATA_PROGRAM_ID),
        ProgramError::IncorrectProgramId
    );
    Ok(())
}

/// Accepts either legacy SPL Token or Token-2022 as the program info
/// argument. The caller is responsible for binding the mint to this
/// program via `verify_account_owner(mint, token_program.address())`.
#[inline(always)]
pub fn verify_token_program(info: &AccountView) -> Result<(), ProgramError> {
    require!(
        info.address().eq(&TOKEN_PROGRAM_ID) || info.address().eq(&TOKEN_2022_PROGRAM_ID),
        ProgramError::IncorrectProgramId
    );
    Ok(())
}
