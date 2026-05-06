use pinocchio::{account::AccountView, address::Address, error::ProgramError};

/// Verify that `ata_info` is the canonical Associated Token Account for
/// the given wallet/mint/token-program tuple. Address-only — does not
/// check initialization. Callers that need the ATA initialized rely on
/// the downstream Transfer/CloseAccount CPI to fail naturally on an
/// uninitialized account.
#[inline(always)]
pub fn verify_canonical_ata(
    ata_info: &AccountView,
    wallet: &Address,
    mint: &Address,
    token_program_info: &AccountView,
) -> Result<(), ProgramError> {
    let expected = Address::find_program_address(
        &[
            wallet.as_ref(),
            token_program_info.address().as_ref(),
            mint.as_ref(),
        ],
        &pinocchio_associated_token_account::ID,
    )
    .0;
    if ata_info.address() != &expected {
        return Err(ProgramError::InvalidSeeds);
    }
    Ok(())
}
