extern crate alloc;
use super::discriminator::{
    AccountSerialize, Discriminator, PrivateChannelEscrowAccountDiscriminators,
};
use crate::constants::{INSTANCE_SEED, INSTANCE_VERSION};
use crate::error::PrivateChannelEscrowProgramError;
use crate::processor::validate_pda_account;
use crate::require_len;
use crate::validate_discriminator;
use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use alloc::vec::Vec;
use codama::CodamaAccount;
use pinocchio::account::AccountView;
use pinocchio::{error::ProgramError, Address as Pubkey};

/// Zeroed space for future fields, so adding one needs no new layout.
const RESERVED_LEN: usize = 64;

/// Seeds: [b"instance", instance_seed.as_ref()]
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct Instance {
    pub bump: u8,
    pub version: u8,
    pub instance_seed: Pubkey,
    pub admin: Pubkey,
    /// When set, Deposit requires the depositor and recipient to be allowed to deposit.
    pub deposits_whitelisted: bool,
    /// When set, ReleaseFunds requires the withdrawer and destination to be allowed to withdraw.
    pub withdrawals_whitelisted: bool,
    /// RESERVED_LEN, written out because codama drops a field whose size is a constant.
    pub reserved: [u8; 64],
}

impl Discriminator for Instance {
    const DISCRIMINATOR: u8 =
        PrivateChannelEscrowAccountDiscriminators::InstanceDiscriminator as u8;
}

impl AccountSerialize for Instance {
    fn to_bytes_inner(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(self.bump);
        data.push(self.version);
        data.extend_from_slice(self.instance_seed.as_ref());
        data.extend_from_slice(self.admin.as_ref());
        data.push(self.deposits_whitelisted as u8);
        data.push(self.withdrawals_whitelisted as u8);
        data.extend_from_slice(&self.reserved);
        data
    }
}

impl Instance {
    /// A 0.1.0 instance, which ends at the admin.
    pub const V1_LEN: usize = 1 + // discriminator
        1 + // bump
        1 + // version
        32 + // instance_seed
        32; // admin

    pub const LEN: usize = Self::V1_LEN +
        1 + // deposits_whitelisted
        1 + // withdrawals_whitelisted
        RESERVED_LEN; // reserved

    pub fn new(bump: u8, instance_seed: Pubkey, admin: Pubkey) -> Self {
        Self {
            bump,
            version: INSTANCE_VERSION,
            instance_seed,
            admin,
            deposits_whitelisted: false,
            withdrawals_whitelisted: false,
            reserved: [0; RESERVED_LEN],
        }
    }

    pub fn validate_pda(&self, account_info: &AccountView) -> Result<(), ProgramError> {
        validate_pda_account(
            &[INSTANCE_SEED, self.instance_seed.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            self.bump,
            account_info,
        )
        .map(|_| ())
    }

    pub fn validate_admin(&self, provided_admin: &Pubkey) -> Result<(), ProgramError> {
        if self.admin != *provided_admin {
            return Err(PrivateChannelEscrowProgramError::InvalidAdmin.into());
        }
        Ok(())
    }

    pub fn try_from_bytes(data: &[u8]) -> Result<Self, ProgramError> {
        validate_discriminator!(data, Self::DISCRIMINATOR);

        if data.len() != Self::V1_LEN {
            require_len!(data, Self::LEN);
        }

        let mut offset: usize = 1;

        let bump = data[offset];
        offset += 1;

        let version = data[offset];
        offset += 1;

        let instance_seed = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let admin = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let mut instance = Self {
            bump,
            version,
            instance_seed,
            admin,
            deposits_whitelisted: false,
            withdrawals_whitelisted: false,
            reserved: [0; RESERVED_LEN],
        };

        // 0.1.0 had no whitelist, so its instances read with it off.
        if data.len() == Self::V1_LEN {
            return Ok(instance);
        }

        // The Borsh client refuses any bool byte but 0 or 1, so we do too.
        instance.deposits_whitelisted = match data[offset] {
            0 => false,
            1 => true,
            _ => return Err(ProgramError::InvalidAccountData),
        };
        offset += 1;

        instance.withdrawals_whitelisted = match data[offset] {
            0 => false,
            1 => true,
            _ => return Err(ProgramError::InvalidAccountData),
        };
        offset += 1;

        instance.reserved = data[offset..offset + RESERVED_LEN]
            .try_into()
            .map_err(|_| ProgramError::InvalidAccountData)?;

        Ok(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_constructor() {
        let admin = Pubkey::new_from_array([0u8; 32]);
        let instance_seed = Pubkey::new_from_array([0u8; 32]);
        let instance = Instance::new(1, instance_seed, admin);

        assert_eq!(instance.version, INSTANCE_VERSION);
        assert_eq!(instance.admin, admin);
        assert_eq!(instance.instance_seed, instance_seed);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let admin = Pubkey::new_from_array([42u8; 32]);
        let instance_seed = Pubkey::new_from_array([7u8; 32]);
        let mut instance = Instance::new(255, instance_seed, admin);
        // Opposite values confirm the two flags are not read from the same byte.
        instance.deposits_whitelisted = true;

        let bytes = instance.to_bytes();
        assert_eq!(bytes.len(), Instance::LEN);

        let deserialized = Instance::try_from_bytes(&bytes).expect("Should deserialize");

        assert_eq!(deserialized.bump, instance.bump);
        assert_eq!(deserialized.version, instance.version);
        assert_eq!(deserialized.instance_seed, instance.instance_seed);
        assert_eq!(deserialized.admin, instance.admin);
        assert!(deserialized.deposits_whitelisted);
        assert!(!deserialized.withdrawals_whitelisted);
    }

    // validate_admin should return Ok when the provided key matches the stored admin.
    #[test]
    fn test_validate_admin_correct() {
        let admin = Pubkey::new_from_array([5u8; 32]);
        let instance = Instance::new(1, Pubkey::new_from_array([1u8; 32]), admin);

        assert!(instance.validate_admin(&admin).is_ok());
    }

    // validate_admin should return InvalidAdmin when a different key is provided.
    // This is the guard that prevents a non-admin from performing admin-only operations
    // (allow_mint, block_mint, add_operator, remove_operator, set_new_admin).
    #[test]
    fn test_validate_admin_wrong() {
        let admin = Pubkey::new_from_array([5u8; 32]);
        let wrong_admin = Pubkey::new_from_array([6u8; 32]);
        let instance = Instance::new(1, Pubkey::new_from_array([1u8; 32]), admin);

        let result = instance.validate_admin(&wrong_admin);

        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::InvalidAdmin.into())
        );
    }

    // try_from_bytes should reject data whose first byte doesn't match the Instance
    // discriminator, preventing a different account type from being parsed as an Instance.
    #[test]
    fn test_try_from_bytes_invalid_discriminator() {
        let admin = Pubkey::new_from_array([42u8; 32]);
        let instance_seed = Pubkey::new_from_array([7u8; 32]);
        let instance = Instance::new(1, instance_seed, admin);
        let mut bytes = instance.to_bytes();
        bytes[0] = 99; // corrupt the discriminator byte

        let result = Instance::try_from_bytes(&bytes);

        assert_eq!(result.err(), Some(ProgramError::InvalidAccountData));
    }

    // A 0.1.0 instance ends at the admin. It reads with the whitelist off, so every
    // instruction keeps working on it after the upgrade.
    #[test]
    fn test_try_from_bytes_reads_a_0_1_0_instance_with_the_whitelist_off() {
        let admin = Pubkey::new_from_array([42u8; 32]);
        let instance_seed = Pubkey::new_from_array([7u8; 32]);
        let bytes = Instance::new(1, instance_seed, admin).to_bytes();

        let instance =
            Instance::try_from_bytes(&bytes[..Instance::V1_LEN]).expect("Should deserialize");

        assert_eq!(instance.instance_seed, instance_seed);
        assert_eq!(instance.admin, admin);
        assert!(!instance.deposits_whitelisted);
        assert!(!instance.withdrawals_whitelisted);
    }

    // Longer than a 0.1.0 instance but short of the current layout is a cut-short
    // account, refused rather than read.
    #[test]
    fn test_try_from_bytes_rejects_a_cut_short_instance() {
        let instance = Instance::new(
            1,
            Pubkey::new_from_array([7u8; 32]),
            Pubkey::new_from_array([42u8; 32]),
        );
        let bytes = instance.to_bytes();

        let result = Instance::try_from_bytes(&bytes[..Instance::V1_LEN + 2]);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
