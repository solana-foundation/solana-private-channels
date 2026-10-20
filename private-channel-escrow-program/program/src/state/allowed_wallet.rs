extern crate alloc;
use super::discriminator::{
    AccountSerialize, Discriminator, PrivateChannelEscrowAccountDiscriminators,
};
use crate::constants::ALLOWED_WALLET_SEED;
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

/// A wallet on the instance whitelist. Each flag is read only while the instance
/// whitelists that direction.
///
/// Seeds: [b"allowed_wallet", instance_pda, wallet_pubkey]
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct AllowedWallet {
    pub bump: u8,
    pub deposits_blocked: bool,
    pub withdrawals_blocked: bool,
}

/// Which flag `AllowedWallet::verify_allowed` reads.
pub enum Direction {
    Deposit,
    Withdraw,
}

impl Discriminator for AllowedWallet {
    const DISCRIMINATOR: u8 =
        PrivateChannelEscrowAccountDiscriminators::AllowedWalletDiscriminator as u8;
}

impl AccountSerialize for AllowedWallet {
    fn to_bytes_inner(&self) -> Vec<u8> {
        vec![
            self.bump,
            self.deposits_blocked as u8,
            self.withdrawals_blocked as u8,
        ]
    }
}

impl AllowedWallet {
    pub const LEN: usize = 1 + // discriminator
        1 + // bump
        1 + // deposits_blocked
        1; // withdrawals_blocked

    pub fn new(bump: u8, deposits_blocked: bool, withdrawals_blocked: bool) -> Self {
        Self {
            bump,
            deposits_blocked,
            withdrawals_blocked,
        }
    }

    pub fn try_from_bytes(data: &[u8]) -> Result<Self, ProgramError> {
        validate_discriminator!(data, Self::DISCRIMINATOR);

        require_len!(data, Self::LEN);

        let mut offset: usize = 1;

        let bump = data[offset];
        offset += 1;

        // The Borsh client refuses any bool byte but 0 or 1, so we do too.
        let deposits_blocked = match data[offset] {
            0 => false,
            1 => true,
            _ => return Err(ProgramError::InvalidAccountData),
        };
        offset += 1;

        let withdrawals_blocked = match data[offset] {
            0 => false,
            1 => true,
            _ => return Err(ProgramError::InvalidAccountData),
        };

        Ok(Self {
            bump,
            deposits_blocked,
            withdrawals_blocked,
        })
    }

    pub fn validate_pda(
        &self,
        instance_pda: &Address,
        wallet: &Address,
        account_info: &AccountView,
    ) -> Result<(), ProgramError> {
        validate_pda_account(
            &[ALLOWED_WALLET_SEED, instance_pda.as_ref(), wallet.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            self.bump,
            account_info,
        )
        .map(|_| ())
    }

    /// Fails unless `account` is `wallet`'s entry on this instance and does not block
    /// `direction`. The address is checked first, so any other account is
    /// InvalidAllowedWalletPda and WalletNotAllowed always means the wallet itself.
    pub fn verify_allowed(
        account: &AccountView,
        instance_pda: &Address,
        wallet: &Address,
        direction: Direction,
    ) -> Result<(), ProgramError> {
        let (expected_address, _) = Address::find_program_address(
            &[ALLOWED_WALLET_SEED, instance_pda.as_ref(), wallet.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        );
        if account.address() != &expected_address {
            return Err(PrivateChannelEscrowProgramError::InvalidAllowedWalletPda.into());
        }

        if account.is_data_empty() {
            return Err(PrivateChannelEscrowProgramError::WalletNotAllowed.into());
        }

        // Mirrors validate_pda_account's owner check for a non-empty PDA.
        if !account.owned_by(&PRIVATE_CHANNEL_ESCROW_PROGRAM_ID) {
            return Err(PrivateChannelEscrowProgramError::InvalidAllowedWalletPda.into());
        }

        let data = account.try_borrow()?;
        let allowed_wallet = Self::try_from_bytes(&data)
            .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedWalletPda)?;

        let blocked = match direction {
            Direction::Deposit => allowed_wallet.deposits_blocked,
            Direction::Withdraw => allowed_wallet.withdrawals_blocked,
        };
        if blocked {
            return Err(PrivateChannelEscrowProgramError::WalletNotAllowed.into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Another escrow account type must not pass as a whitelist entry.
    #[test]
    fn test_allowed_wallet_try_from_bytes_invalid_discriminator() {
        // Operator discriminator (1) placed where AllowedWallet discriminator (4) is expected.
        let data = [1u8, 42u8, 0u8, 0u8];
        let result = AllowedWallet::try_from_bytes(&data);
        assert_eq!(result.err(), Some(ProgramError::InvalidAccountData));
    }
}
