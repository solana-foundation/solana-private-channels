extern crate alloc;
use super::discriminator::{
    AccountSerialize, Discriminator, PrivateChannelEscrowAccountDiscriminators,
};
use crate::constants::DEPOSITOR_SEED;
use crate::error::PrivateChannelEscrowProgramError;
use crate::processor::validate_pda_account;
use crate::require_len;
use crate::validate_discriminator;
use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use alloc::vec;
use alloc::vec::Vec;
use codama::CodamaAccount;
use pinocchio::account::AccountView;
use pinocchio::{error::ProgramError, Address};

/// A wallet on the instance whitelist. The account existing is the entry.
///
/// Seeds: [b"depositor", instance_pda, wallet_pubkey]
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct Depositor {
    pub bump: u8,
}

impl Discriminator for Depositor {
    const DISCRIMINATOR: u8 =
        PrivateChannelEscrowAccountDiscriminators::DepositorDiscriminator as u8;
}

impl AccountSerialize for Depositor {
    fn to_bytes_inner(&self) -> Vec<u8> {
        vec![self.bump]
    }
}

impl Depositor {
    pub const LEN: usize = 1 + // discriminator
        1; // bump

    pub fn new(bump: u8) -> Self {
        Self { bump }
    }

    pub fn try_from_bytes(data: &[u8]) -> Result<Self, ProgramError> {
        validate_discriminator!(data, Self::DISCRIMINATOR);

        require_len!(data, Self::LEN);

        let offset: usize = 1;

        let bump = data[offset];

        Ok(Self { bump })
    }

    pub fn validate_pda(
        &self,
        instance_pda: &Address,
        wallet: &Address,
        account_info: &AccountView,
    ) -> Result<(), ProgramError> {
        validate_pda_account(
            &[DEPOSITOR_SEED, instance_pda.as_ref(), wallet.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            self.bump,
            account_info,
        )
        .map(|_| ())
    }

    /// Fails unless `account` is `wallet`'s entry on this instance. An empty account
    /// means the wallet is not listed; anything else in its place is the wrong PDA.
    pub fn verify_listed(
        account: &AccountView,
        instance_pda: &Address,
        wallet: &Address,
    ) -> Result<(), ProgramError> {
        if account.is_data_empty() {
            return Err(PrivateChannelEscrowProgramError::WalletNotAllowed.into());
        }

        let data = account.try_borrow()?;
        let depositor = Self::try_from_bytes(&data)
            .map_err(|_| PrivateChannelEscrowProgramError::InvalidDepositorPda)?;

        depositor
            .validate_pda(instance_pda, wallet, account)
            .map_err(|_| PrivateChannelEscrowProgramError::InvalidDepositorPda.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Another escrow account type must not pass as a whitelist entry.
    #[test]
    fn test_depositor_try_from_bytes_invalid_discriminator() {
        // Operator discriminator (1) placed where Depositor discriminator (4) is expected.
        let data = [1u8, 42u8];
        let result = Depositor::try_from_bytes(&data);
        assert_eq!(result.err(), Some(ProgramError::InvalidAccountData));
    }
}
