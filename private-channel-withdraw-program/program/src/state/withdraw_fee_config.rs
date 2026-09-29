extern crate alloc;

use crate::error::PrivateChannelWithdrawProgramError;
use alloc::vec::Vec;
use codama::CodamaAccount;
use pinocchio::{error::ProgramError, Address};

pub const WITHDRAW_FEE_CONFIG_SEED: &[u8] = b"withdraw_fee_config";

/// Seeds: [b"withdraw_fee_config", mint]
///
/// The fee `WithdrawFunds` charges on top of the burned amount. The operator
/// rewrites it on every deposit from the fee set at the escrow's AllowMint.
///
/// No discriminator: this is the program's only account type, and every read
/// already checks the owner, the PDA address and the length.
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct WithdrawFeeConfig {
    pub bump: u8,
    pub fee: u64,
    /// Slot of the AllowMint `fee` came from. A write from an older slot is
    /// ignored, so a deposit that lands late cannot restore an older fee.
    pub fee_slot: u64,
    /// Withdraws without paying the fee, so it can move collected fees out.
    pub treasury: Address,
    /// The treasury's ATA for this mint. Fees are credited here.
    pub treasury_token_account: Address,
}

impl WithdrawFeeConfig {
    pub const LEN: usize = 1 + // bump
        8 + // fee
        8 + // fee_slot
        32 + // treasury
        32; // treasury_token_account

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(Self::LEN);
        data.push(self.bump);
        data.extend_from_slice(&self.fee.to_le_bytes());
        data.extend_from_slice(&self.fee_slot.to_le_bytes());
        data.extend_from_slice(self.treasury.as_ref());
        data.extend_from_slice(self.treasury_token_account.as_ref());
        data
    }

    /// Fails with `InvalidFeeConfig`, never a builtin error: the deposit
    /// transaction runs this, and the operator retries a mint forever on
    /// `InvalidAccountData`.
    pub fn try_from_bytes(data: &[u8]) -> Result<Self, ProgramError> {
        if data.len() != Self::LEN {
            return Err(PrivateChannelWithdrawProgramError::InvalidFeeConfig.into());
        }

        let mut offset: usize = 0;

        let bump = data[offset];
        offset += 1;

        let fee = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| PrivateChannelWithdrawProgramError::InvalidFeeConfig)?,
        );
        offset += 8;

        let fee_slot = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| PrivateChannelWithdrawProgramError::InvalidFeeConfig)?,
        );
        offset += 8;

        let treasury = Address::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| PrivateChannelWithdrawProgramError::InvalidFeeConfig)?,
        );
        offset += 32;

        let treasury_token_account = Address::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| PrivateChannelWithdrawProgramError::InvalidFeeConfig)?,
        );

        Ok(Self {
            bump,
            fee,
            fee_slot,
            treasury,
            treasury_token_account,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every field gets a distinct value, and `fee` one whose bytes differ, so a
    // swapped field or a wrong-endian read fails here.
    #[test]
    fn test_withdraw_fee_config_serialization_roundtrip() {
        let config = WithdrawFeeConfig {
            bump: 200,
            fee: 1_234_567,
            fee_slot: 7_654_321,
            treasury: Address::new_from_array([7u8; 32]),
            treasury_token_account: Address::new_from_array([9u8; 32]),
        };

        let bytes = config.to_bytes();

        assert_eq!(bytes.len(), WithdrawFeeConfig::LEN);
        assert_eq!(WithdrawFeeConfig::try_from_bytes(&bytes).unwrap(), config);
    }

    #[test]
    fn test_withdraw_fee_config_try_from_bytes_wrong_length() {
        let data = [0u8; WithdrawFeeConfig::LEN - 1];

        let result = WithdrawFeeConfig::try_from_bytes(&data);

        assert_eq!(
            result.err(),
            Some(PrivateChannelWithdrawProgramError::InvalidFeeConfig.into())
        );
    }
}
