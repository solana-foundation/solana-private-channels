use super::discriminator::{Discriminator, PrivateChannelEscrowAccountDiscriminators};
use crate::constants::bitmap_constants::{BITMAP_BYTES, NONCES_PER_GENERATION};
use crate::constants::WITHDRAWAL_BITMAP_SEED;
use crate::error::PrivateChannelEscrowProgramError;
use crate::processor::validate_pda_account;
use crate::validate_discriminator;
use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use codama::CodamaAccount;
use pinocchio::account::AccountView;
use pinocchio::{error::ProgramError, Address};

/// Withdrawal nonce replay protection: one bit per nonce in the current generation.
///
/// Seeds: [b"withdrawal_bitmap", instance_pda]
///
/// Only the fixed header is declared, for IDL and client generation. The bits
/// follow at BITS_OFFSET and are read by slicing: they do not fit on the BPF
/// stack, and their length varies with the test-tree feature.
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct WithdrawalBitmap {
    pub bump: u8,
    /// Nonce window this bitmap covers: nonce / NONCES_PER_GENERATION.
    pub generation: u64,
}

impl Discriminator for WithdrawalBitmap {
    const DISCRIMINATOR: u8 =
        PrivateChannelEscrowAccountDiscriminators::WithdrawalBitmapDiscriminator as u8;
}

/// `init` and `validate` check the account length. The accessors below index
/// within it and must only run on an account one of them has accepted.
impl WithdrawalBitmap {
    const BUMP_OFFSET: usize = 1;
    const GENERATION_OFFSET: usize = 2;
    const BITS_OFFSET: usize = 10;

    pub const LEN: usize = Self::BITS_OFFSET + BITMAP_BYTES;

    /// Writes the header of a freshly created account. Bits are already zero
    /// because CreateAccount zeroes the allocation.
    ///
    /// Refuses an account that has been written before. Re-running init on a live
    /// bitmap would clear every consumed nonce and make the generation replayable.
    /// Only this program can write to an account it owns, so a non-zero first byte
    /// means the account is already initialized.
    pub fn init(data: &mut [u8], bump: u8) -> Result<(), ProgramError> {
        Self::verify_len(data)?;

        if data[0] != 0 {
            return Err(ProgramError::AccountAlreadyInitialized);
        }

        data[0] = Self::DISCRIMINATOR;
        data[Self::BUMP_OFFSET] = bump;
        data[Self::GENERATION_OFFSET..Self::BITS_OFFSET].copy_from_slice(&0u64.to_le_bytes());

        Ok(())
    }

    pub fn validate(
        data: &[u8],
        instance: &Address,
        account_info: &AccountView,
    ) -> Result<(), ProgramError> {
        validate_discriminator!(data, Self::DISCRIMINATOR);
        Self::verify_len(data)?;

        // The bump is read from the passed account, so a foreign bitmap fails on
        // either the bump or the address depending on whether the two instances'
        // bumps collide. Map both to one error so the rejection is deterministic.
        validate_pda_account(
            &[WITHDRAWAL_BITMAP_SEED, instance.as_ref()],
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            data[Self::BUMP_OFFSET],
            account_info,
        )
        .map(|_| ())
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap.into())
    }

    pub fn generation(data: &[u8]) -> Result<u64, ProgramError> {
        Ok(u64::from_le_bytes(
            data[Self::GENERATION_OFFSET..Self::BITS_OFFSET]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        ))
    }

    /// Rejects a nonce belonging to a generation this bitmap no longer covers.
    pub fn validate_generation(data: &[u8], nonce: u64) -> Result<(), ProgramError> {
        if nonce / NONCES_PER_GENERATION != Self::generation(data)? {
            return Err(PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration.into());
        }

        Ok(())
    }

    /// Marks a nonce as released. Fails if it was already released.
    ///
    /// The bits are one flat row of NONCES_PER_GENERATION switches, 8 to a byte.
    /// Only whole bytes are addressable, so the nonce splits into a byte index
    /// and a position inside that byte.
    pub fn consume_nonce(data: &mut [u8], nonce: u64) -> Result<(), ProgramError> {
        // Position within the generation, 0..NONCES_PER_GENERATION.
        let bit = (nonce % NONCES_PER_GENERATION) as usize;
        // 8 bits to a byte, past the header.
        let byte = Self::BITS_OFFSET + bit / 8;
        // A byte holding a single 1 at this nonce's position: bit 4 -> 0b0001_0000.
        let mask = 1u8 << (bit % 8);

        // Non-zero only where both sides have a 1, so only if this exact bit is set.
        if data[byte] & mask != 0 {
            return Err(PrivateChannelEscrowProgramError::NonceAlreadyUsed.into());
        }
        // Sets our bit and leaves the seven neighbouring nonces in this byte alone.
        data[byte] |= mask;

        Ok(())
    }

    /// Clears every bit and advances to the next generation. `expected_generation`
    /// makes this non-idempotent so a replayed rotation cannot skip a generation.
    pub fn rotate(data: &mut [u8], expected_generation: u64) -> Result<(), ProgramError> {
        let generation = Self::generation(data)?;
        if generation != expected_generation {
            return Err(PrivateChannelEscrowProgramError::UnexpectedGeneration.into());
        }

        // Lowers to the sol_memset_ syscall, ~64 CU for the full bitmap.
        data[Self::BITS_OFFSET..Self::LEN].fill(0);

        let next_generation = generation
            .checked_add(1)
            .ok_or(ProgramError::ArithmeticOverflow)?;
        data[Self::GENERATION_OFFSET..Self::BITS_OFFSET]
            .copy_from_slice(&next_generation.to_le_bytes());

        Ok(())
    }

    fn verify_len(data: &[u8]) -> Result<(), ProgramError> {
        if data.len() < Self::LEN {
            return Err(PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap.into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_bitmap(bump: u8) -> [u8; WithdrawalBitmap::LEN] {
        let mut data = [0u8; WithdrawalBitmap::LEN];
        WithdrawalBitmap::init(&mut data, bump).expect("init should accept a full-length account");
        data
    }

    // A second release of the same nonce is the attack this account exists to stop.
    #[test]
    fn test_consume_nonce_rejects_replay() {
        let mut data = new_bitmap(255);

        assert!(WithdrawalBitmap::consume_nonce(&mut data, 7).is_ok());

        let result = WithdrawalBitmap::consume_nonce(&mut data, 7);
        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::NonceAlreadyUsed.into())
        );
    }

    // Nonces 0..7 share one byte, so a wrong mask would make one nonce consume another.
    #[test]
    fn test_consume_nonce_neighbours_do_not_collide() {
        let mut data = new_bitmap(255);

        for nonce in 0..8 {
            assert!(
                WithdrawalBitmap::consume_nonce(&mut data, nonce).is_ok(),
                "nonce {nonce} should still be free"
            );
        }

        for nonce in 0..8 {
            assert_eq!(
                WithdrawalBitmap::consume_nonce(&mut data, nonce).err(),
                Some(PrivateChannelEscrowProgramError::NonceAlreadyUsed.into()),
                "nonce {nonce} should now be consumed"
            );
        }
    }

    // Generation 0 covers 0..NONCES_PER_GENERATION. The first nonce of the next
    // generation must be refused until a rotation lands.
    #[test]
    fn test_validate_generation_boundary() {
        let data = new_bitmap(255);

        assert!(WithdrawalBitmap::validate_generation(&data, 0).is_ok());
        assert!(WithdrawalBitmap::validate_generation(&data, NONCES_PER_GENERATION - 1).is_ok());

        let result = WithdrawalBitmap::validate_generation(&data, NONCES_PER_GENERATION);
        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration.into())
        );
    }

    // After rotation the same bit positions are reusable by the next generation's nonces.
    #[test]
    fn test_rotate_clears_bits_and_advances_generation() {
        let mut data = new_bitmap(255);
        WithdrawalBitmap::consume_nonce(&mut data, 0).unwrap();

        WithdrawalBitmap::rotate(&mut data, 0).unwrap();

        assert_eq!(WithdrawalBitmap::generation(&data).unwrap(), 1);
        assert!(WithdrawalBitmap::validate_generation(&data, NONCES_PER_GENERATION).is_ok());
        assert!(WithdrawalBitmap::consume_nonce(&mut data, NONCES_PER_GENERATION).is_ok());
    }

    // Rotation clears the bits, so only the generation check keeps the previous
    // window closed. Without it every rotation would reopen the whole range.
    #[test]
    fn test_validate_generation_rejects_previous_generation() {
        let mut data = new_bitmap(255);
        WithdrawalBitmap::consume_nonce(&mut data, 42).unwrap();
        WithdrawalBitmap::rotate(&mut data, 0).unwrap();

        let result = WithdrawalBitmap::validate_generation(&data, 42);
        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration.into())
        );
    }

    // The generation counter must saturate rather than wrap: wrapping to 0 would
    // reopen generation 0 and make every nonce ever released replayable.
    #[test]
    fn test_rotate_rejects_generation_overflow() {
        let mut data = new_bitmap(255);
        data[WithdrawalBitmap::GENERATION_OFFSET..WithdrawalBitmap::BITS_OFFSET]
            .copy_from_slice(&u64::MAX.to_le_bytes());

        let result = WithdrawalBitmap::rotate(&mut data, u64::MAX);
        assert_eq!(result.err(), Some(ProgramError::ArithmeticOverflow));
    }

    // A replayed rotation would skip a whole generation of nonces.
    #[test]
    fn test_rotate_rejects_stale_expected_generation() {
        let mut data = new_bitmap(255);
        WithdrawalBitmap::rotate(&mut data, 0).unwrap();

        let result = WithdrawalBitmap::rotate(&mut data, 0);
        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::UnexpectedGeneration.into())
        );
        assert_eq!(WithdrawalBitmap::generation(&data).unwrap(), 1);
    }

    // Re-initializing a live bitmap would zero the generation and free every
    // consumed nonce, so init must refuse and leave the account untouched.
    #[test]
    fn test_init_rejects_already_initialized_account() {
        let mut data = new_bitmap(255);
        WithdrawalBitmap::rotate(&mut data, 0).unwrap();
        WithdrawalBitmap::consume_nonce(&mut data, NONCES_PER_GENERATION).unwrap();

        let result = WithdrawalBitmap::init(&mut data, 254);
        assert_eq!(result.err(), Some(ProgramError::AccountAlreadyInitialized));

        assert_eq!(WithdrawalBitmap::generation(&data).unwrap(), 1);
        assert_eq!(
            WithdrawalBitmap::consume_nonce(&mut data, NONCES_PER_GENERATION).err(),
            Some(PrivateChannelEscrowProgramError::NonceAlreadyUsed.into()),
            "consumed nonce must stay consumed"
        );
    }

    #[test]
    fn test_init_rejects_undersized_account() {
        let mut data = [0u8; WithdrawalBitmap::LEN - 1];

        let result = WithdrawalBitmap::init(&mut data, 255);
        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap.into())
        );
    }
}
