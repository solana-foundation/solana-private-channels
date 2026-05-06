extern crate alloc;

use alloc::vec::Vec;
use codama::CodamaAccount;
use pinocchio::{cpi::Seed, error::ProgramError, Address as Pubkey};

pub const SWAP_DVP_SEED: &[u8] = b"dvp";

/// Atomic DvP escrow for a P2P token swap inside a Contra channel.
///
/// `user_a` (seller) delivers `amount_a` of `mint_a` (the asset);
/// `user_b` (buyer) delivers `amount_b` of `mint_b` (the cash). Only the
/// `settlement_authority` can settle; either party (or the authority)
/// can abort before settlement.
///
/// Seeds: `[b"dvp", settlement_authority, user_a, user_b, mint_a, mint_b,
/// nonce.to_le_bytes(), bump]`. `bump` is derived by `find_program_address`
/// at create time and stored on the account so post-create instructions
/// can re-sign as this PDA without re-running the derivation.
#[derive(Clone, Debug, PartialEq, CodamaAccount)]
#[repr(C)]
pub struct SwapDvp {
    pub bump: u8,
    pub user_a: Pubkey,
    pub user_b: Pubkey,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    pub settlement_authority: Pubkey,
    pub amount_a: u64,
    pub amount_b: u64,
    pub expiry_timestamp: i64,
    pub nonce: u64,
    /// `None` = settlement allowed any time before `expiry_timestamp`.
    /// `Some(t)` = additionally requires `now >= t`.
    pub earliest_settlement_timestamp: Option<i64>,
}

impl SwapDvp {
    pub const LEN: usize = 1   // bump
        + 32 * 5               // user_a, user_b, mint_a, mint_b, settlement_authority
        + 8 * 4                // amount_a, amount_b, expiry_timestamp, nonce
        + 1 + 8; // earliest_settlement_timestamp (opt)

    /// Owned `(nonce, bump)` byte buffers. Bind to a local so
    /// `signing_seeds` can borrow from them across the CPI.
    pub fn seed_buffers(&self) -> ([u8; 8], [u8; 1]) {
        (self.nonce.to_le_bytes(), [self.bump])
    }

    /// PDA seed array (with bump) for signing CPIs as the SwapDvp authority.
    pub fn signing_seeds<'a>(
        &'a self,
        nonce_bytes: &'a [u8; 8],
        bump_bytes: &'a [u8; 1],
    ) -> [Seed<'a>; 8] {
        [
            Seed::from(SWAP_DVP_SEED),
            Seed::from(self.settlement_authority.as_ref()),
            Seed::from(self.user_a.as_ref()),
            Seed::from(self.user_b.as_ref()),
            Seed::from(self.mint_a.as_ref()),
            Seed::from(self.mint_b.as_ref()),
            Seed::from(nonce_bytes),
            Seed::from(bump_bytes),
        ]
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(Self::LEN);
        data.push(self.bump);
        data.extend_from_slice(self.user_a.as_ref());
        data.extend_from_slice(self.user_b.as_ref());
        data.extend_from_slice(self.mint_a.as_ref());
        data.extend_from_slice(self.mint_b.as_ref());
        data.extend_from_slice(self.settlement_authority.as_ref());
        data.extend_from_slice(&self.amount_a.to_le_bytes());
        data.extend_from_slice(&self.amount_b.to_le_bytes());
        data.extend_from_slice(&self.expiry_timestamp.to_le_bytes());
        data.extend_from_slice(&self.nonce.to_le_bytes());

        match self.earliest_settlement_timestamp {
            Some(timestamp) => {
                data.push(1);
                data.extend_from_slice(&timestamp.to_le_bytes());
            }
            None => {
                data.push(0);
                data.extend_from_slice(&i64::MAX.to_le_bytes());
            }
        }

        data
    }

    pub fn try_from_bytes(data: &[u8]) -> Result<Self, ProgramError> {
        if data.len() < Self::LEN {
            return Err(ProgramError::InvalidAccountData);
        }

        let mut offset: usize = 0;

        let bump = data[offset];
        offset += 1;

        let user_a = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let user_b = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let mint_a = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let mint_b = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let settlement_authority = Pubkey::new_from_array(
            data[offset..offset + 32]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 32;

        let amount_a = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 8;

        let amount_b = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 8;

        let expiry_timestamp = i64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 8;

        let nonce = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| ProgramError::InvalidAccountData)?,
        );
        offset += 8;

        // Tag is the source of truth; the payload after a `0` tag is a
        // sentinel (see `to_bytes`) and is intentionally not validated.
        let earliest_settlement_timestamp = match data[offset] {
            0 => None,
            1 => Some(i64::from_le_bytes(
                data[offset + 1..offset + 9]
                    .try_into()
                    .map_err(|_| ProgramError::InvalidAccountData)?,
            )),
            _ => return Err(ProgramError::InvalidAccountData),
        };

        Ok(Self {
            bump,
            user_a,
            user_b,
            mint_a,
            mint_b,
            settlement_authority,
            amount_a,
            amount_b,
            expiry_timestamp,
            nonce,
            earliest_settlement_timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_dvp(earliest: Option<i64>) -> SwapDvp {
        SwapDvp {
            bump: 254,
            user_a: Pubkey::new_from_array([1u8; 32]),
            user_b: Pubkey::new_from_array([2u8; 32]),
            mint_a: Pubkey::new_from_array([3u8; 32]),
            mint_b: Pubkey::new_from_array([4u8; 32]),
            settlement_authority: Pubkey::new_from_array([5u8; 32]),
            amount_a: 1_000,
            amount_b: 2_500,
            expiry_timestamp: 1_780_000_000,
            nonce: 42,
            earliest_settlement_timestamp: earliest,
        }
    }

    #[test]
    fn test_serialization_roundtrip_with_earliest() {
        let dvp = sample_dvp(Some(1_775_000_000));

        let bytes = dvp.to_bytes();
        assert_eq!(bytes.len(), SwapDvp::LEN);

        let parsed = SwapDvp::try_from_bytes(&bytes).expect("roundtrip");
        assert_eq!(parsed, dvp);
    }

    #[test]
    fn test_serialization_roundtrip_without_earliest() {
        let dvp = sample_dvp(None);

        let bytes = dvp.to_bytes();
        assert_eq!(bytes.len(), SwapDvp::LEN);

        let parsed = SwapDvp::try_from_bytes(&bytes).expect("roundtrip");
        assert_eq!(parsed, dvp);
    }

    /// Three-arm match on the option tag: 0 → None, 1 → Some, anything
    /// else → Err. The `_` arm is easy to drop in a refactor, and the
    /// failure mode (silently reading garbage past the tag as a valid
    /// `Some(_)`) is hard to spot in review.
    #[test]
    fn test_try_from_bytes_rejects_invalid_option_tag() {
        let mut bytes = sample_dvp(None).to_bytes();
        // Tag offset = bump(1) + 5*pubkey(160) + 4*u64-or-i64(32) = 193.
        let option_tag_offset = 1 + 32 * 5 + 8 * 4;
        bytes[option_tag_offset] = 2;
        let err = SwapDvp::try_from_bytes(&bytes).expect_err("must reject invalid tag");
        assert!(matches!(err, ProgramError::InvalidAccountData));
    }
}
