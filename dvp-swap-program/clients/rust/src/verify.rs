//! Checked, verify-before-fund helpers for the DvP swap client.
//!
//! The generated readers Borsh-decode any bytes at an address with no owner
//! or size check. Escrows are funded by raw token transfers to an ATA derived
//! from the `swap_dvp` pubkey, so an attacker-owned account that decodes as a
//! `SwapDvp` lets a funder deposit into an ATA the attacker can drain.
//!
//! These helpers require, before treating an account as a canonical `SwapDvp`:
//! owner is the DvP program, size is exactly [`SWAP_DVP_ACCOUNT_LEN`], and the
//! address is the canonical PDA for the decoded terms. [`find_swap_dvp_address`]
//! and [`find_swap_dvp_escrow_ata`] derive those addresses from agreed terms.

use crate::accounts::SwapDvp;
use crate::generated::programs::DVP_SWAP_PROGRAM_ID;
use solana_pubkey::{pubkey, Pubkey};

/// Fixed on-chain size of a `SwapDvp` account (`SwapDvp::LEN`). The final
/// `earliest_settlement_timestamp` is always 1 tag byte + 8 payload bytes,
/// even for `None` (whose payload is an ignored sentinel).
pub const SWAP_DVP_ACCOUNT_LEN: usize = 1  // bump
    + 32 * 7   // user_a, user_b, mint_a, mint_b, settlement_authority, token_program_a, token_program_b
    + 8 * 4    // amount_a, amount_b, expiry_timestamp, nonce
    + 64       // ref_string
    + 32 * 2   // user_a_settlement_destination, user_b_settlement_destination
    + 32 * 2   // mint_a_authority, mint_b_authority
    + 1 + 8; // earliest_settlement_timestamp (tag + payload)

/// Seed prefix for the `SwapDvp` PDA (matches `SWAP_DVP_SEED` on-chain).
pub const SWAP_DVP_SEED: &[u8] = b"dvp";

/// Canonical SPL Associated Token Account program.
pub const ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey =
    pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

/// Why a fetched account was rejected as a canonical `SwapDvp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwapDvpVerifyError {
    /// Account is not owned by the DvP program.
    WrongOwner { expected: Pubkey, actual: Pubkey },
    /// Account data is not exactly [`SWAP_DVP_ACCOUNT_LEN`] bytes.
    WrongSize { expected: usize, actual: usize },
    /// Account data could not be parsed as a `SwapDvp`.
    Malformed(String),
    /// Account is program-owned but not at its canonical PDA for the
    /// decoded terms.
    WrongAddress { expected: Pubkey, actual: Pubkey },
}

impl core::fmt::Display for SwapDvpVerifyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SwapDvpVerifyError::WrongOwner { expected, actual } => write!(
                f,
                "account is not owned by the DvP program (expected owner {expected}, got {actual})"
            ),
            SwapDvpVerifyError::WrongSize { expected, actual } => write!(
                f,
                "account has {actual} bytes of data; a canonical SwapDvp is exactly {expected} bytes"
            ),
            SwapDvpVerifyError::Malformed(msg) => {
                write!(f, "account data is not a valid SwapDvp: {msg}")
            }
            SwapDvpVerifyError::WrongAddress { expected, actual } => write!(
                f,
                "account {actual} is program-owned but not at its canonical PDA for the stored terms (expected address {expected})"
            ),
        }
    }
}

impl std::error::Error for SwapDvpVerifyError {}

impl SwapDvp {
    /// Parse an on-chain `SwapDvp`, rejecting any buffer that is not exactly
    /// [`SWAP_DVP_ACCOUNT_LEN`] bytes.
    ///
    /// The size gate is what makes reusing the generated Borsh decoder safe.
    /// The on-chain layout is fixed-width, but Borsh's `Option` is not (`None`
    /// is 1 byte, `Some` is 9), and `from_bytes` ignores trailing bytes, so on
    /// its own it can't tell the real 458-byte account from the 450-byte
    /// forgery. Pinned to 458 bytes, Borsh decodes `None` and `Some`
    /// unambiguously and still rejects an invalid option tag.
    pub fn try_from_bytes(data: &[u8]) -> Result<Self, SwapDvpVerifyError> {
        if data.len() != SWAP_DVP_ACCOUNT_LEN {
            return Err(SwapDvpVerifyError::WrongSize {
                expected: SWAP_DVP_ACCOUNT_LEN,
                actual: data.len(),
            });
        }

        SwapDvp::from_bytes(data).map_err(|e| SwapDvpVerifyError::Malformed(e.to_string()))
    }
}

/// Derives the canonical `SwapDvp` PDA from agreed terms (on-chain seeds
/// `[b"dvp", settlement_authority, user_a, user_b, mint_a, mint_b, nonce_le]`).
/// Compare this against any address a counterparty supplies.
pub fn find_swap_dvp_address(
    settlement_authority: &Pubkey,
    user_a: &Pubkey,
    user_b: &Pubkey,
    mint_a: &Pubkey,
    mint_b: &Pubkey,
    nonce: u64,
) -> (Pubkey, u8) {
    let nonce_bytes = nonce.to_le_bytes();
    Pubkey::find_program_address(
        &[
            SWAP_DVP_SEED,
            settlement_authority.as_ref(),
            user_a.as_ref(),
            user_b.as_ref(),
            mint_a.as_ref(),
            mint_b.as_ref(),
            &nonce_bytes,
        ],
        &DVP_SWAP_PROGRAM_ID,
    )
}

/// Derives a leg's escrow ATA (the `SwapDvp` PDA's ATA for a mint/token
/// program). Funders send raw transfers here, so derive it from a verified
/// `swap_dvp` PDA, not from an unverified supplied address.
pub fn find_swap_dvp_escrow_ata(
    swap_dvp: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Pubkey {
    Pubkey::find_program_address(
        &[swap_dvp.as_ref(), token_program.as_ref(), mint.as_ref()],
        &ASSOCIATED_TOKEN_PROGRAM_ID,
    )
    .0
}

/// Verifies raw account fields (owner, size, layout, canonical PDA) and
/// returns the decoded `SwapDvp`. Core used by [`decode_swap_dvp_account`];
/// available without the `fetch` feature.
pub fn verify_swap_dvp_bytes(
    expected_address: &Pubkey,
    owner: &Pubkey,
    data: &[u8],
) -> Result<SwapDvp, SwapDvpVerifyError> {
    if *owner != DVP_SWAP_PROGRAM_ID {
        return Err(SwapDvpVerifyError::WrongOwner {
            expected: DVP_SWAP_PROGRAM_ID,
            actual: *owner,
        });
    }

    let dvp = SwapDvp::try_from_bytes(data)?;

    let (expected_pda, _bump) = find_swap_dvp_address(
        &dvp.settlement_authority,
        &dvp.user_a,
        &dvp.user_b,
        &dvp.mint_a,
        &dvp.mint_b,
        dvp.nonce,
    );
    if expected_pda != *expected_address {
        return Err(SwapDvpVerifyError::WrongAddress {
            expected: expected_pda,
            actual: *expected_address,
        });
    }

    Ok(dvp)
}

/// Fetched-account wrapper over [`verify_swap_dvp_bytes`]. Use instead of the
/// generated `SwapDvp::from_bytes` before acting on terms.
#[cfg(feature = "fetch")]
pub fn decode_swap_dvp_account(
    expected_address: &Pubkey,
    account: &solana_account::Account,
) -> Result<SwapDvp, SwapDvpVerifyError> {
    verify_swap_dvp_bytes(expected_address, &account.owner, &account.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::BorshSerialize;

    fn sample() -> SwapDvp {
        SwapDvp {
            bump: 254,
            user_a: Pubkey::new_from_array([1u8; 32]),
            user_b: Pubkey::new_from_array([2u8; 32]),
            mint_a: Pubkey::new_from_array([3u8; 32]),
            mint_b: Pubkey::new_from_array([4u8; 32]),
            settlement_authority: Pubkey::new_from_array([5u8; 32]),
            token_program_a: Pubkey::new_from_array([6u8; 32]),
            token_program_b: Pubkey::new_from_array([7u8; 32]),
            amount_a: 1_000,
            amount_b: 2_500,
            expiry_timestamp: 1_780_000_000,
            nonce: 42,
            ref_string: [8u8; 64],
            user_a_settlement_destination: Pubkey::new_from_array([9u8; 32]),
            user_b_settlement_destination: Pubkey::new_from_array([10u8; 32]),
            mint_a_authority: Pubkey::new_from_array([11u8; 32]),
            mint_b_authority: Pubkey::new_from_array([12u8; 32]),
            earliest_settlement_timestamp: None,
        }
    }

    /// On-chain fixed layout: 1 tag byte + 8 payload bytes for the option,
    /// with `i64::MAX` as the None sentinel.
    fn on_chain_bytes(dvp: &SwapDvp) -> Vec<u8> {
        let mut data = Vec::with_capacity(SWAP_DVP_ACCOUNT_LEN);
        data.push(dvp.bump);
        for pk in [
            &dvp.user_a,
            &dvp.user_b,
            &dvp.mint_a,
            &dvp.mint_b,
            &dvp.settlement_authority,
            &dvp.token_program_a,
            &dvp.token_program_b,
        ] {
            data.extend_from_slice(pk.as_ref());
        }
        data.extend_from_slice(&dvp.amount_a.to_le_bytes());
        data.extend_from_slice(&dvp.amount_b.to_le_bytes());
        data.extend_from_slice(&dvp.expiry_timestamp.to_le_bytes());
        data.extend_from_slice(&dvp.nonce.to_le_bytes());
        data.extend_from_slice(&dvp.ref_string);
        data.extend_from_slice(dvp.user_a_settlement_destination.as_ref());
        data.extend_from_slice(dvp.user_b_settlement_destination.as_ref());
        data.extend_from_slice(dvp.mint_a_authority.as_ref());
        data.extend_from_slice(dvp.mint_b_authority.as_ref());
        match dvp.earliest_settlement_timestamp {
            Some(t) => {
                data.push(1);
                data.extend_from_slice(&t.to_le_bytes());
            }
            None => {
                data.push(0);
                data.extend_from_slice(&i64::MAX.to_le_bytes());
            }
        }
        data
    }

    #[test]
    fn strict_try_from_bytes_accepts_on_chain_none_layout() {
        let dvp = sample();
        let bytes = on_chain_bytes(&dvp);
        assert_eq!(bytes.len(), SWAP_DVP_ACCOUNT_LEN);
        let parsed = SwapDvp::try_from_bytes(&bytes).unwrap();
        assert_eq!(parsed, dvp);
    }

    #[test]
    fn strict_try_from_bytes_accepts_some() {
        let mut dvp = sample();
        dvp.earliest_settlement_timestamp = Some(1_770_000_000);
        let parsed = SwapDvp::try_from_bytes(&on_chain_bytes(&dvp)).unwrap();
        assert_eq!(parsed.earliest_settlement_timestamp, Some(1_770_000_000));
    }

    #[test]
    fn strict_try_from_bytes_rejects_borsh_none_forgery() {
        // The forgery: Borsh None is a lone `0` tag, 8 bytes short.
        let mut short = Vec::new();
        sample().serialize(&mut short).unwrap();
        assert_eq!(short.len(), 450);
        assert!(matches!(
            SwapDvp::try_from_bytes(&short),
            Err(SwapDvpVerifyError::WrongSize { .. })
        ));
    }

    #[test]
    fn strict_try_from_bytes_rejects_oversize() {
        let mut over = on_chain_bytes(&sample());
        over.push(0);
        assert!(matches!(
            SwapDvp::try_from_bytes(&over),
            Err(SwapDvpVerifyError::WrongSize { .. })
        ));
    }

    #[test]
    fn strict_try_from_bytes_rejects_invalid_option_tag() {
        let mut bytes = on_chain_bytes(&sample());
        // Offset of the earliest_settlement_timestamp Option tag. Mirrors
        // SWAP_DVP_ACCOUNT_LEN: 4 pubkeys follow ref_string (user_a/user_b
        // settlement destinations + mint_a/mint_b authorities), not 2.
        let tag_offset = 1 + 32 * 7 + 8 * 4 + 64 + 32 * 2 + 32 * 2;
        bytes[tag_offset] = 2;
        assert!(matches!(
            SwapDvp::try_from_bytes(&bytes),
            Err(SwapDvpVerifyError::Malformed(_))
        ));
    }

    #[test]
    fn verify_rejects_wrong_owner() {
        let dvp = sample();
        let (addr, _) = find_swap_dvp_address(
            &dvp.settlement_authority,
            &dvp.user_a,
            &dvp.user_b,
            &dvp.mint_a,
            &dvp.mint_b,
            dvp.nonce,
        );
        let not_program = Pubkey::new_from_array([99u8; 32]);
        let err = verify_swap_dvp_bytes(&addr, &not_program, &on_chain_bytes(&dvp)).unwrap_err();
        assert!(matches!(err, SwapDvpVerifyError::WrongOwner { .. }));
    }

    #[test]
    fn verify_rejects_wrong_address() {
        let dvp = sample();
        let wrong = Pubkey::new_from_array([77u8; 32]);
        let err =
            verify_swap_dvp_bytes(&wrong, &DVP_SWAP_PROGRAM_ID, &on_chain_bytes(&dvp)).unwrap_err();
        assert!(matches!(err, SwapDvpVerifyError::WrongAddress { .. }));
    }

    #[test]
    fn verify_accepts_canonical_program_owned_account() {
        let dvp = sample();
        let (addr, _) = find_swap_dvp_address(
            &dvp.settlement_authority,
            &dvp.user_a,
            &dvp.user_b,
            &dvp.mint_a,
            &dvp.mint_b,
            dvp.nonce,
        );
        let parsed =
            verify_swap_dvp_bytes(&addr, &DVP_SWAP_PROGRAM_ID, &on_chain_bytes(&dvp)).unwrap();
        assert_eq!(parsed, dvp);
    }
}
