pub const DEFAULT_CU_RELEASE_FUNDS: Option<u32> = Some(600_000);
pub const DEFAULT_CU_MINT: Option<u32> = None;
pub const MINT_IDEMPOTENCY_MEMO_PREFIX: &str = "private_channel:mint-idempotency:";

// SMT tree constants (must match on-chain program)
#[cfg(not(feature = "test-tree"))]
pub mod tree_constants {
    use crate::operator::{const_hash_combine, EMPTY_LEAF};

    pub const TREE_HEIGHT: usize = 16;
    pub const MAX_TREE_LEAVES: usize = 2_usize.pow(16); // 65,536 leaves in production

    // Empty subtree hashes at each level
    // Computed at compile time: Level 0 is EMPTY_LEAF, each subsequent level is hash(prev_level || prev_level)
    // These are used as default sibling values when a position hasn't been inserted yet
    pub const EMPTY_SUBTREE_HASHES: [[u8; 32]; TREE_HEIGHT] = {
        let mut hashes = [[0u8; 32]; TREE_HEIGHT];
        hashes[0] = EMPTY_LEAF;
        hashes[1] = const_hash_combine(&hashes[0], &hashes[0]);
        hashes[2] = const_hash_combine(&hashes[1], &hashes[1]);
        hashes[3] = const_hash_combine(&hashes[2], &hashes[2]);
        hashes[4] = const_hash_combine(&hashes[3], &hashes[3]);
        hashes[5] = const_hash_combine(&hashes[4], &hashes[4]);
        hashes[6] = const_hash_combine(&hashes[5], &hashes[5]);
        hashes[7] = const_hash_combine(&hashes[6], &hashes[6]);
        hashes[8] = const_hash_combine(&hashes[7], &hashes[7]);
        hashes[9] = const_hash_combine(&hashes[8], &hashes[8]);
        hashes[10] = const_hash_combine(&hashes[9], &hashes[9]);
        hashes[11] = const_hash_combine(&hashes[10], &hashes[10]);
        hashes[12] = const_hash_combine(&hashes[11], &hashes[11]);
        hashes[13] = const_hash_combine(&hashes[12], &hashes[12]);
        hashes[14] = const_hash_combine(&hashes[13], &hashes[13]);
        hashes[15] = const_hash_combine(&hashes[14], &hashes[14]);
        hashes
    };

    // This is the SMT root if all leaves are 0 (empty tree)
    // Computed at compile time as hash(EMPTY_SUBTREE_HASHES[TREE_HEIGHT-1] || EMPTY_SUBTREE_HASHES[TREE_HEIGHT-1])
    pub const EMPTY_TREE_ROOT: [u8; 32] =
        const_hash_combine(&EMPTY_SUBTREE_HASHES[15], &EMPTY_SUBTREE_HASHES[15]);
}

#[cfg(feature = "test-tree")]
pub mod tree_constants {
    use crate::operator::{const_hash_combine, EMPTY_LEAF};

    pub const TREE_HEIGHT: usize = 3;
    pub const MAX_TREE_LEAVES: usize = 2_usize.pow(3); // 8 leaves for testing

    pub const EMPTY_SUBTREE_HASHES: [[u8; 32]; TREE_HEIGHT] = {
        let mut hashes = [[0u8; 32]; TREE_HEIGHT];
        hashes[0] = EMPTY_LEAF;
        hashes[1] = const_hash_combine(&hashes[0], &hashes[0]);
        hashes[2] = const_hash_combine(&hashes[1], &hashes[1]);
        hashes
    };

    pub const EMPTY_TREE_ROOT: [u8; 32] =
        const_hash_combine(&EMPTY_SUBTREE_HASHES[2], &EMPTY_SUBTREE_HASHES[2]);
}

// Leaf values (must match on-chain program)
pub const EMPTY_LEAF: [u8; 32] = [0u8; 32];

// This is the leaf value of a present nonce (non-empty leaf)
// Computed at compile time: SHA256([1u8; 32])
pub const NON_EMPTY_LEAF_HASH: [u8; 32] = const_crypto::sha2::Sha256::new()
    .update(&[1u8; 32])
    .finalize();

pub const SIBLING_PROOF_SIZE: usize = 512;

// Helper function to hash two 32-byte arrays (for const context)
pub const fn const_hash_combine(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut combined = [0u8; 64];
    let mut i = 0;
    while i < 32 {
        combined[i] = left[i];
        combined[i + 32] = right[i];
        i += 1;
    }
    const_crypto::sha2::Sha256::new()
        .update(&combined)
        .finalize()
}
