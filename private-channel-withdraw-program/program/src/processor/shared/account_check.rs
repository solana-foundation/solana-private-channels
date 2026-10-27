use crate::error::PrivateChannelWithdrawProgramError;
use pinocchio::{account::AccountView, error::ProgramError, Address};
use pinocchio_associated_token_account::ID as ATA_PROGRAM_ID;
use pinocchio_token_2022::{state::Mint, ID as TOKEN_PROGRAM_ID};

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
pub fn verify_owner_mutability(
    info: &AccountView,
    owner: &Address,
    expect_writable: bool,
) -> Result<(), ProgramError> {
    if !info.owned_by(owner) {
        return Err(ProgramError::InvalidAccountOwner);
    }
    if expect_writable && !info.is_writable() {
        return Err(ProgramError::InvalidAccountData);
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

#[inline(always)]
pub fn verify_mint_account(info: &AccountView) -> Result<(), ProgramError> {
    if !info.owned_by(&TOKEN_PROGRAM_ID) {
        return Err(PrivateChannelWithdrawProgramError::InvalidMint.into());
    }

    let data = info.try_borrow()?;
    // Token-2022 mints carry extensions after the base layout, so only a floor is checked.
    if data.len() < Mint::BASE_LEN {
        return Err(PrivateChannelWithdrawProgramError::InvalidMint.into());
    }

    let _ = unsafe { Mint::from_bytes_unchecked(&data) };

    Ok(())
}
