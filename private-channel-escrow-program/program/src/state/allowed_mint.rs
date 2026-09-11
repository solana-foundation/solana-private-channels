extern crate alloc;
use super::discriminator::{
    AccountSerialize, Discriminator, PrivateChannelEscrowAccountDiscriminators,
};
use crate::constants::ALLOWED_MINT_SEED;
use crate::processor::validate_pda_account;
use crate::require_len;
use crate::validate_discriminator;
use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use alloc::vec;
use alloc::vec::Vec;
use codama::CodamaAccount;
use pinocchio::account::AccountView;
use pinocchio::{error::ProgramError, Address};

/// Seeds: [b"allowed_mint", instance_pda, mint_pubkey]
///
/// Gates are independent so blocking deposits cannot strand existing balances.
///
/// `decimals`, `token_program`, `extensions` and `has_freeze_authority` pin what
/// the admin reviewed. A mint closed at zero supply and recreated at the same
/// address can change any of them, so Deposit compares them and stops until an
/// admin blocks and re-allows the mint.
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct AllowedMint {
    pub bump: u8,
    pub deposits_blocked: bool,
    pub withdrawals_blocked: bool,
    pub decimals: u8,
    pub token_program: Address,
    /// Bit N is set when the mint carries the `ExtensionType` whose
    /// discriminant is N. Always 0 for a legacy SPL mint. Pins which
    /// extensions exist, not their contents: a fee raised or a hook program
    /// swapped leaves this unchanged.
    pub extensions: u64,
    /// A freeze authority can be revoked but never re-added, so only gaining
    /// one implies a recreate. Deposit compares this in one direction.
    pub has_freeze_authority: bool,
}

impl Discriminator for AllowedMint {
    const DISCRIMINATOR: u8 =
        PrivateChannelEscrowAccountDiscriminators::AllowedMintDiscriminator as u8;
}

impl AccountSerialize for AllowedMint {
    fn to_bytes_inner(&self) -> Vec<u8> {
        let mut data = vec![
            self.bump,
            self.deposits_blocked as u8,
            self.withdrawals_blocked as u8,
            self.decimals,
        ];
        data.extend_from_slice(self.token_program.as_ref());
        data.extend_from_slice(&self.extensions.to_le_bytes());
        data.push(self.has_freeze_authority as u8);
        data
    }
}

impl AllowedMint {
    pub const LEN: usize = 1 + // discriminator
        1 + // bump
        1 + // deposits_blocked
        1 + // withdrawals_blocked
        1 + // decimals
        32 + // token_program
        8 + // extensions
        1; // has_freeze_authority

    pub fn new(
        bump: u8,
        decimals: u8,
        token_program: Address,
        extensions: u64,
        has_freeze_authority: bool,
    ) -> Self {
        Self {
            bump,
            deposits_blocked: false,
            withdrawals_blocked: false,
            decimals,
            token_program,
            extensions,
            has_freeze_authority,
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
        offset += 1;

        let decimals = data[offset];
        offset += 1;

        let token_program = Address::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let extensions = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 8;

        let has_freeze_authority = match data[offset] {
            0 => false,
            1 => true,
            _ => return Err(ProgramError::InvalidAccountData),
        };

        Ok(Self {
            bump,
            deposits_blocked,
            withdrawals_blocked,
            decimals,
            token_program,
            extensions,
            has_freeze_authority,
        })
    }

    pub fn validate_pda(
        &self,
        instance_pda: &Address,
        mint: &Address,
        account_info: &AccountView,
    ) -> Result<(), ProgramError> {
        validate_pda_account(
            &[ALLOWED_MINT_SEED, instance_pda.as_ref(), mint.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            self.bump,
            account_info,
        )
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A newly allowed mint must have both gates open, or AllowMint would
    // create an account that blocks the deposit it was created for.
    #[test]
    fn test_allowed_mint_new() {
        let token_program = Address::new_from_array([7u8; 32]);
        let allowed_mint = AllowedMint::new(99, 6, token_program, 0b1010, true);

        assert_eq!(allowed_mint.bump, 99);
        assert!(!allowed_mint.deposits_blocked);
        assert!(!allowed_mint.withdrawals_blocked);
        assert_eq!(allowed_mint.decimals, 6);
        assert_eq!(allowed_mint.token_program, token_program);
        assert_eq!(allowed_mint.extensions, 0b1010);
        assert!(allowed_mint.has_freeze_authority);
    }

    // The gates are adjacent bytes of the same type and decimals follows them, so
    // this gives every field a distinct value to pin the order on the wire.
    // `extensions` gets a byte-asymmetric value so a wrong-endian read fails here.
    #[test]
    fn test_allowed_mint_serialization_roundtrip() {
        let mut allowed_mint = AllowedMint::new(
            200,
            9,
            Address::new_from_array([7u8; 32]),
            0x0000_0000_0400_000A,
            true,
        );
        allowed_mint.deposits_blocked = true;

        let bytes = allowed_mint.to_bytes();

        assert_eq!(bytes.len(), AllowedMint::LEN);
        assert_eq!(bytes[0], AllowedMint::DISCRIMINATOR);

        let deserialized = AllowedMint::try_from_bytes(&bytes).expect("Should deserialize");
        assert_eq!(deserialized, allowed_mint);
    }

    // Only 0 and 1 are valid for a gate. Anything else would be state the
    // Borsh-based client cannot decode, so it is rejected on read.
    #[test]
    fn test_allowed_mint_try_from_bytes_non_canonical_gate() {
        let mut data = [0u8; AllowedMint::LEN];
        data[0] = AllowedMint::DISCRIMINATOR;
        data[1] = 200; // bump
        data[2] = 2; // deposits_blocked, neither 0 nor 1

        let result = AllowedMint::try_from_bytes(&data);

        assert_eq!(result.err(), Some(ProgramError::InvalidAccountData));
    }

    // Wrong discriminator byte should be rejected. Prevents a different account type
    // (e.g. Operator) from being accepted as an AllowedMint PDA, which would bypass
    // the mint allowlist entirely.
    #[test]
    fn test_allowed_mint_try_from_bytes_invalid_discriminator() {
        // Operator discriminator (1) placed where AllowedMint discriminator (2) is expected.
        let data = [1u8, 99u8];
        let result = AllowedMint::try_from_bytes(&data);
        assert_eq!(result.err(), Some(ProgramError::InvalidAccountData));
    }

    // Empty slice must be rejected — validate_discriminator! checks is_empty() first.
    #[test]
    fn test_allowed_mint_try_from_bytes_empty_data() {
        let result = AllowedMint::try_from_bytes(&[]);
        assert_eq!(result.err(), Some(ProgramError::InvalidAccountData));
    }

    // Correct discriminator but truncated. require_len! fires before the field
    // reads, returning InvalidInstructionData instead of panicking. This is also
    // what a pre-gates 2-byte account hits after the layout change.
    #[test]
    fn test_allowed_mint_try_from_bytes_too_short() {
        let data = [AllowedMint::DISCRIMINATOR, 200]; // bump only, LEN=46
        let result = AllowedMint::try_from_bytes(&data);
        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
