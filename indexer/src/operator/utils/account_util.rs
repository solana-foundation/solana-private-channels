use crate::error::{AccountError, OperatorError, ProgramError};
use crate::operator::bitmap_constants::{BITMAP_BYTES, NONCES_PER_GENERATION};
use crate::operator::bitmap_layout::{ACCOUNT_LEN, BITS_OFFSET};
use crate::operator::RpcClientWithRetry;
use private_channel_escrow_program_client::{
    programs::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, Instance, WithdrawalBitmap,
};
use solana_sdk::pubkey::Pubkey;

const INSTANCE_SEED: &[u8] = b"instance";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";
const ALLOWED_MINT_SEED: &[u8] = b"allowed_mint";
const OPERATOR_SEED: &[u8] = b"operator";
const WITHDRAWAL_BITMAP_SEED: &[u8] = b"withdrawal_bitmap";

/// Account-type tag the escrow program writes in the first byte of a bitmap.
const WITHDRAWAL_BITMAP_DISCRIMINATOR: u8 = 3;

pub fn find_instance_pda(instance_seed: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[INSTANCE_SEED, instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

pub fn find_event_authority_pda() -> Pubkey {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).0
}

pub fn find_allowed_mint_pda(instance_pda: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[ALLOWED_MINT_SEED, instance_pda.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

pub fn find_operator_pda(instance_pda: &Pubkey, wallet: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[OPERATOR_SEED, instance_pda.as_ref(), wallet.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

/// One bitmap per instance, so the instance PDA is the only seed input.
pub fn find_withdrawal_bitmap_pda(instance_pda: &Pubkey) -> Pubkey {
    find_withdrawal_bitmap_pda_with_bump(instance_pda).0
}

/// Same derivation, keeping the bump for `CreateInstance`, which takes it as an argument.
pub fn find_withdrawal_bitmap_pda_with_bump(instance_pda: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[WITHDRAWAL_BITMAP_SEED, instance_pda.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

pub fn parse_instance(instance_data: &[u8]) -> Result<Instance, std::io::Error> {
    Instance::from_bytes(instance_data)
}

/// The consumed-nonce window the chain is currently enforcing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapState {
    pub generation: u64,
    /// Absolute nonces whose bit is set, ascending. Only meaningful for this generation.
    pub consumed: Vec<u64>,
}

impl BitmapState {
    /// True when this nonce falls in the window the bitmap currently covers.
    /// Outside it the bits say nothing, because rotation cleared them.
    pub fn covers(&self, nonce: u64) -> bool {
        nonce / NONCES_PER_GENERATION == self.generation
    }

    pub fn is_consumed(&self, nonce: u64) -> bool {
        self.covers(nonce) && self.consumed.binary_search(&nonce).is_ok()
    }
}

/// Decode a withdrawal bitmap account into its generation and the nonces it
/// records as released.
pub fn parse_withdrawal_bitmap(data: &[u8]) -> Result<BitmapState, AccountError> {
    if data.len() < ACCOUNT_LEN {
        return Err(AccountError::AccountDeserializationFailed {
            pubkey: Pubkey::default(),
            reason: format!(
                "withdrawal bitmap too short: {} bytes, expected at least {}",
                data.len(),
                ACCOUNT_LEN
            ),
        });
    }

    let header = WithdrawalBitmap::from_bytes(data).map_err(|e| {
        AccountError::AccountDeserializationFailed {
            pubkey: Pubkey::default(),
            reason: e.to_string(),
        }
    })?;

    // Borsh decodes any 10 bytes, so a wrong account would read as garbage bits.
    if header.discriminator != WITHDRAWAL_BITMAP_DISCRIMINATOR {
        return Err(AccountError::AccountDeserializationFailed {
            pubkey: Pubkey::default(),
            reason: format!(
                "not a withdrawal bitmap: discriminator {}",
                header.discriminator
            ),
        });
    }

    let base = header.generation.saturating_mul(NONCES_PER_GENERATION);
    let bits = &data[BITS_OFFSET..BITS_OFFSET + BITMAP_BYTES];

    let mut consumed = Vec::new();
    for (byte_index, byte) in bits.iter().enumerate() {
        if *byte == 0 {
            continue;
        }
        for bit in 0..8u32 {
            if byte & (1u8 << bit) != 0 {
                consumed.push(base + (byte_index as u64) * 8 + bit as u64);
            }
        }
    }

    Ok(BitmapState {
        generation: header.generation,
        consumed,
    })
}

/// Read only the generation. Used at the rotation boundary and when routing a
/// generation rejection, where the bits are irrelevant.
pub async fn fetch_bitmap_generation(
    rpc_client: &RpcClientWithRetry,
    bitmap_pda: &Pubkey,
) -> Result<u64, OperatorError> {
    Ok(fetch_consumed_nonces(rpc_client, bitmap_pda, None)
        .await?
        .generation)
}

/// Read the authoritative consumed-nonce set for the current generation.
///
/// Generation and bits come from one `getAccountInfo`, so the returned view is
/// internally consistent even if a release lands while it is in flight.
///
/// `min_context_slot` binds the answer to a slot the caller has already proven
/// fresh. A backend that cannot serve there errors instead of returning an older
/// snapshot, which is what stops a clear bit on a lagging node from reading as
/// proof that a release never happened.
pub async fn fetch_consumed_nonces(
    rpc_client: &RpcClientWithRetry,
    bitmap_pda: &Pubkey,
    min_context_slot: Option<u64>,
) -> Result<BitmapState, OperatorError> {
    // Named as a bitmap failure rather than a generic transport one because
    // callers branch on it. An unreadable bitmap leaves a withdrawal row alone
    // for the recovery worker, where an error they do not recognise marks the
    // row permanently failed for what was only a read that did not answer.
    let response = rpc_client
        .get_account_with_context_min_slot(
            bitmap_pda,
            rpc_client.rpc_client.commitment(),
            min_context_slot,
        )
        .await
        .map_err(|e| ProgramError::BitmapUnavailable {
            reason: format!("get_account_info({bitmap_pda}): {e}"),
        })?;

    let account = response
        .value
        .ok_or_else(|| ProgramError::BitmapUnavailable {
            reason: format!("bitmap {bitmap_pda} not found"),
        })?;

    parse_withdrawal_bitmap(&account.data)
        .map_err(|e| match e {
            AccountError::AccountDeserializationFailed { reason, .. } => {
                AccountError::AccountDeserializationFailed {
                    pubkey: *bitmap_pda,
                    reason,
                }
            }
            other => other,
        })
        .map_err(Into::into)
}

/// Serialize a withdrawal bitmap account exactly as the program lays it out, so
/// tests and mocked RPC responses agree with the chain byte for byte.
#[cfg(any(test, feature = "test-mock-storage"))]
pub fn bitmap_account_bytes(generation: u64, consumed: &[u64], bump: u8) -> Vec<u8> {
    use crate::operator::bitmap_layout::GENERATION_OFFSET;

    let mut data = vec![0u8; ACCOUNT_LEN];
    data[0] = WITHDRAWAL_BITMAP_DISCRIMINATOR;
    data[1] = bump;
    data[GENERATION_OFFSET..BITS_OFFSET].copy_from_slice(&generation.to_le_bytes());

    for nonce in consumed {
        let bit = (nonce % NONCES_PER_GENERATION) as usize;
        data[BITS_OFFSET + bit / 8] |= 1u8 << (bit % 8);
    }

    data
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(i: u8) -> Pubkey {
        let mut b = [0u8; 32];
        b[0] = i;
        Pubkey::new_from_array(b)
    }

    #[test]
    fn find_instance_pda_deterministic() {
        let seed = pk(1);
        let pda1 = find_instance_pda(&seed);
        let pda2 = find_instance_pda(&seed);
        assert_eq!(pda1, pda2);
    }

    #[test]
    fn find_event_authority_pda_non_default() {
        let pda = find_event_authority_pda();
        assert_ne!(pda, Pubkey::default());
    }

    #[test]
    fn find_allowed_mint_pda_different_mints_different_pdas() {
        let instance = pk(1);
        let pda_a = find_allowed_mint_pda(&instance, &pk(2));
        let pda_b = find_allowed_mint_pda(&instance, &pk(3));
        assert_ne!(pda_a, pda_b);
    }

    #[test]
    fn find_operator_pda_different_wallets_different_pdas() {
        let instance = pk(1);
        let pda_a = find_operator_pda(&instance, &pk(10));
        let pda_b = find_operator_pda(&instance, &pk(11));
        assert_ne!(pda_a, pda_b);
    }

    #[test]
    fn parse_instance_empty_bytes_errors() {
        let result = parse_instance(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_instance_short_bytes_errors() {
        let result = parse_instance(&[1, 2, 3]);
        assert!(result.is_err());
    }

    // ── find_withdrawal_bitmap_pda ────────────────────────────────────

    /// The derivation must be stable across calls, or a release would address a
    /// different account each time it is built.
    #[test]
    fn find_withdrawal_bitmap_pda_is_deterministic() {
        let instance = pk(1);
        assert_eq!(
            find_withdrawal_bitmap_pda(&instance),
            find_withdrawal_bitmap_pda(&instance)
        );
    }

    /// Two instances must never share a bitmap: a shared one would let a nonce
    /// released on one instance block the same nonce on the other.
    #[test]
    fn find_withdrawal_bitmap_pda_is_per_instance() {
        assert_ne!(
            find_withdrawal_bitmap_pda(&pk(1)),
            find_withdrawal_bitmap_pda(&pk(2))
        );
    }

    /// The bump must be the one the derivation found, since CreateInstance
    /// passes it to the program which re-derives with it.
    #[test]
    fn find_withdrawal_bitmap_pda_with_bump_agrees_with_address() {
        let instance = pk(3);
        let (pda, bump) = find_withdrawal_bitmap_pda_with_bump(&instance);
        assert_eq!(pda, find_withdrawal_bitmap_pda(&instance));
        assert_eq!(
            pda,
            Pubkey::create_program_address(
                &[WITHDRAWAL_BITMAP_SEED, instance.as_ref(), &[bump]],
                &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            )
            .expect("returned bump must produce the returned address")
        );
    }

    // ── parse_withdrawal_bitmap bit extraction ────────────────────────

    #[test]
    fn parse_withdrawal_bitmap_reads_generation_and_bump_independently() {
        let bytes = bitmap_account_bytes(7, &[], 254);
        let state = parse_withdrawal_bitmap(&bytes).expect("well-formed bitmap must parse");
        assert_eq!(state.generation, 7);
        assert!(state.consumed.is_empty());
    }

    #[test]
    fn parse_withdrawal_bitmap_rejects_short_account() {
        let bytes = vec![0u8; ACCOUNT_LEN - 1];
        assert!(parse_withdrawal_bitmap(&bytes).is_err());
    }

    /// A fresh bitmap reports nothing consumed, so a boot diff on a new
    /// instance must not invent divergence.
    #[test]
    fn parse_withdrawal_bitmap_empty_set() {
        let state = parse_withdrawal_bitmap(&bitmap_account_bytes(0, &[], 255)).unwrap();
        assert_eq!(state.consumed, Vec::<u64>::new());
    }

    /// Every nonce in the first byte, set individually, must decode back to
    /// exactly itself. A wrong shift direction would mirror the byte.
    #[test]
    fn parse_withdrawal_bitmap_first_byte_positions_do_not_bleed() {
        for nonce in 0..8u64 {
            let state = parse_withdrawal_bitmap(&bitmap_account_bytes(0, &[nonce], 255)).unwrap();
            assert_eq!(state.consumed, vec![nonce], "nonce {nonce} decoded wrong");
        }
    }

    /// The generation offsets the whole window, so bit 0 of generation 3 is
    /// nonce 3 * NONCES_PER_GENERATION, not nonce 0.
    #[test]
    fn parse_withdrawal_bitmap_offsets_nonces_by_generation() {
        let base = 3 * NONCES_PER_GENERATION;
        let state = parse_withdrawal_bitmap(&bitmap_account_bytes(3, &[base + 1], 255)).unwrap();
        assert_eq!(state.consumed, vec![base + 1]);
    }

    #[test]
    fn bitmap_state_covers_only_its_own_generation() {
        let state = parse_withdrawal_bitmap(&bitmap_account_bytes(1, &[], 255)).unwrap();
        assert!(state.covers(NONCES_PER_GENERATION));
        assert!(!state.covers(0));
        assert!(!state.covers(2 * NONCES_PER_GENERATION));
    }

    /// A set bit from a previous generation must not read as consumed: rotation
    /// clears the bits, so only the current window can be answered.
    #[test]
    fn bitmap_state_is_consumed_ignores_other_generations() {
        let base = NONCES_PER_GENERATION;
        let state = parse_withdrawal_bitmap(&bitmap_account_bytes(1, &[base], 255)).unwrap();
        assert!(state.is_consumed(base));
        assert!(!state.is_consumed(0));
    }

    /// Byte boundaries are where an off-by-one in the byte/bit split shows up.
    /// Only reachable when the window is wider than one byte.
    #[cfg(not(feature = "test-tree"))]
    #[test]
    fn parse_withdrawal_bitmap_byte_boundary_table() {
        let cases: Vec<(&str, Vec<u64>)> = vec![
            ("byte boundary 7/8", vec![7, 8]),
            ("word boundary 63/64", vec![63, 64]),
            ("sparse across the window", vec![0, 1, 500, 65_535]),
            ("dense run", (100..164).collect()),
            ("last nonce only", vec![NONCES_PER_GENERATION - 1]),
        ];

        for (label, consumed) in cases {
            let state = parse_withdrawal_bitmap(&bitmap_account_bytes(0, &consumed, 255)).unwrap();
            assert_eq!(state.consumed, consumed, "{label}");
        }
    }

    /// Neighbouring nonces share a byte, so setting one must leave the other
    /// clear. This is the failure a naive mask would produce.
    #[cfg(not(feature = "test-tree"))]
    #[test]
    fn parse_withdrawal_bitmap_neighbour_isolation() {
        let state = parse_withdrawal_bitmap(&bitmap_account_bytes(0, &[8], 255)).unwrap();
        assert_eq!(state.consumed, vec![8]);
        assert!(!state.is_consumed(7));
        assert!(!state.is_consumed(9));
    }

    /// Callers branch on "the bitmap could not be read" to leave a row alone
    /// instead of failing it, so the read site has to say that in a way they can
    /// recognise. A generic transport error falls through to the arm that marks
    /// the row permanently failed.
    #[tokio::test]
    async fn bitmap_read_failure_is_reported_as_unavailable() {
        use crate::error::ProgramError;
        use crate::operator::utils::rpc_util::RetryConfig;
        use solana_commitment_config::CommitmentConfig;

        let mut server = mockito::Server::new_async().await;
        let _down = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("node down")
            .create_async()
            .await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        );

        let err = fetch_consumed_nonces(&rpc, &pk(9), None).await.unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Program(ProgramError::BitmapUnavailable { .. })
            ),
            "an unreadable bitmap must be distinguishable from any other failure: {err:?}"
        );
    }
}
