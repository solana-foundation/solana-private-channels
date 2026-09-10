use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use const_crypto::ed25519;
use pinocchio::Address;

// Seeds
pub const INSTANCE_SEED: &[u8] = b"instance";
pub const OPERATOR_SEED: &[u8] = b"operator";
pub const ALLOWED_MINT_SEED: &[u8] = b"allowed_mint";
pub const WITHDRAWAL_BITMAP_SEED: &[u8] = b"withdrawal_bitmap";

// Instance
pub const INSTANCE_VERSION: u8 = 1;

#[cfg(not(feature = "test-tree"))]
pub mod bitmap_constants {
    // Nonces covered by one bitmap generation, one bit each.
    pub const NONCES_PER_GENERATION: u64 = 65_536;
    pub const BITMAP_BYTES: usize = (NONCES_PER_GENERATION / 8) as usize;
}

// 8 nonces for testing
#[cfg(feature = "test-tree")]
pub mod bitmap_constants {
    pub const NONCES_PER_GENERATION: u64 = 8;
    pub const BITMAP_BYTES: usize = (NONCES_PER_GENERATION / 8) as usize;
}

// Seeds and PDAs
pub const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

// Anchor Compatitable Discriminator: Sha256(anchor:event)[..8]
pub const EVENT_IX_TAG: u64 = 0x1d9acb512ea545e4;
pub const EVENT_IX_TAG_LE: &[u8] = EVENT_IX_TAG.to_le_bytes().as_slice();

// Event Authority PDA
pub mod event_authority_pda {

    use super::*;

    const EVENT_AUTHORITY_AND_BUMP: ([u8; 32], u8) = ed25519::derive_program_address(
        &[EVENT_AUTHORITY_SEED],
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.as_array(),
    );

    pub const ID: Address = Address::new_from_array(EVENT_AUTHORITY_AND_BUMP.0);
    pub const BUMP: u8 = EVENT_AUTHORITY_AND_BUMP.1;
}
