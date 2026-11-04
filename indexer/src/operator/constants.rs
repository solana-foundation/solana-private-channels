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

/// Tunables for confidential releases, applies, AES repairs and rebalances.
pub mod confidential_constants {
    use std::time::Duration;

    /// Rebuilds after Token-2022 refused a release's proofs (26, 27, 32, 33) before it parks.
    pub const STALE_PROOF_BUDGET: u32 = 5;
    /// Back-to-back repairs a racing deposit can force before the repair backs off.
    pub const AES_REPAIR_BUDGET: u32 = 5;
    /// How often the sender reads each configured escrow account. A read only.
    pub const BACKGROUND_CHECK_INTERVAL: Duration = Duration::from_secs(60);
    /// Apply once the pending credit counter reaches this share of its cap.
    pub const CREDIT_COUNTER_APPLY_PERCENT: u64 = 50;
    /// Apply once unapplied pending reaches this, keeping pending hi inside the 32-bit search.
    pub const PENDING_APPLY_THRESHOLD: u64 = 1 << 47;
    /// First wait for a release with no verified balance, and for a deferred repair.
    pub const WAIT_BACKOFF_BASE: Duration = Duration::from_secs(5);
    /// Waits and repair backoff double up to this.
    pub const WAIT_BACKOFF_CAP: Duration = Duration::from_secs(300);
    /// A confidential park older than this alerts (mirrored in the alert rules).
    pub const PARK_AGE_ALERT: Duration = Duration::from_secs(3600);
    /// Compute for a release with inline proofs. A fee and hook mint release, the costliest
    /// path, measured about 496,000 on Token-2022 11.1.0; this leaves a 20% margin.
    pub const CU_CONFIDENTIAL_RELEASE: u32 = 600_000;
    /// Compute for an apply, a repair or a move between balances.
    pub const CU_CONFIDENTIAL_APPLY: u32 = 200_000;
    /// v1 reads a missing limit as 0; Token-2022 alone loads about 1.3 MB.
    pub const LOADED_ACCOUNTS_DATA_SIZE_LIMIT: u32 = 4_000_000;
}
