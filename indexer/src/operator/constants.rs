pub const MINT_IDEMPOTENCY_SIGNATURE_LOOKBACK_LIMIT: usize = 1000;
pub const DEFAULT_CU_RELEASE_FUNDS: Option<u32> = Some(600_000);
pub const DEFAULT_CU_MINT: Option<u32> = None;
pub const MINT_IDEMPOTENCY_MEMO_PREFIX: &str = "private_channel:mint-idempotency:";
/// Per-page signature limit (getSignaturesForAddress max) used when the resync
/// consumed-set enumeration pages the channel authority's mint history.
pub const CONSUMED_SET_PAGE_SIZE: usize = 1000;

// Withdrawal bitmap geometry, which must match the on-chain program exactly.
// The operator derives a nonce's generation from these numbers, and the program
// enforces the same derivation from its own copy. If the two disagree, every
// release the operator considers valid is rejected on-chain as belonging to
// another generation, and nothing in the pipeline can recover from it.
// The test-tree feature shrinks both sides together for integration tests.
#[cfg(not(feature = "test-tree"))]
pub mod bitmap_constants {
    /// Nonces covered by one bitmap generation, one bit each.
    pub const NONCES_PER_GENERATION: u64 = 65_536;
    pub const BITMAP_BYTES: usize = (NONCES_PER_GENERATION / 8) as usize;
}

// 8 nonces for testing, so a rotation boundary is reachable in a few withdrawals.
#[cfg(feature = "test-tree")]
pub mod bitmap_constants {
    pub const NONCES_PER_GENERATION: u64 = 8;
    pub const BITMAP_BYTES: usize = (NONCES_PER_GENERATION / 8) as usize;
}

/// Byte offsets inside the withdrawal bitmap account, mirroring the on-chain
/// layout: discriminator, bump, generation as a little-endian u64, then the bits.
pub mod bitmap_layout {
    use super::bitmap_constants::BITMAP_BYTES;

    pub const GENERATION_OFFSET: usize = 2;
    pub const BITS_OFFSET: usize = 10;
    pub const ACCOUNT_LEN: usize = BITS_OFFSET + BITMAP_BYTES;
}
