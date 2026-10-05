/// Maximum allowed request body size (64 KB).
pub const MAX_BODY_SIZE: usize = 64 * 1024;

/// Maximum slot range for `getBlocks` queries (matches Solana mainnet).
pub const MAX_SLOT_RANGE: u64 = 500_000;

/// Maximum number of signatures per `getSignatureStatuses` request (matches Solana mainnet).
pub const MAX_SIGNATURES: usize = 256;

/// Maximum JSON-RPC response size (10 MB).
pub const MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024;

/// Encoded-byte budget for the `accounts` array of a `simulateTransaction` reply.
/// Half the response ceiling, leaving room for logs and inner instructions beside it.
pub const MAX_SIMULATION_ACCOUNTS_BYTES: usize = MAX_RESPONSE_SIZE / 2;

/// `simulateTransaction` calls that may run at once. Each loads at most the
/// per-transaction account data cap, so this bounds their total memory.
pub const MAX_CONCURRENT_SIMULATIONS: usize = 8;

/// Widest `getBlocks` span or `getBlocksWithLimit` limit served without a block listing
/// permit. The indexer's lookahead search must fit under it, so a flood of large
/// listings holding every permit can never stall the indexer.
pub const MAX_UNCAPPED_BLOCK_SPAN: u64 = 10_000;

/// Block listings above the uncapped span that may run at once: an eighth of the
/// Postgres pool, so large scans from every caller together leave the rest to other reads.
pub fn block_list_slots(pool_size: u32) -> usize {
    (pool_size as usize / 8).max(1)
}

/// Encoded-byte budget for the account in a `getAccountInfo` reply.
/// Sized off what the endpoint actually serves: the largest account is the
/// 134 KB SPL Token precompile, encoding to ~175 KB. The reply holds nothing
/// else, so there is no logs allowance to leave room for.
pub const MAX_ACCOUNT_RESPONSE_BYTES: usize = 1024 * 1024;

/// Allowance per account for its metadata fields and the JSON punctuation.
pub const PER_ACCOUNT_JSON_OVERHEAD: usize = 256;

/// Estimated JSON bytes one encoded account contributes.
pub fn estimated_encoded_bytes(data_len: usize) -> usize {
    data_len.div_ceil(3) * 4 + PER_ACCOUNT_JSON_OVERHEAD
}

/// Maximum serialized transaction size (matches Solana's PACKET_DATA_SIZE).
pub const PACKET_DATA_SIZE: usize = 1232;

/// Maximum serialized size of a v1 transaction.
///
/// The v1 format raised the ceiling for itself only; legacy and v0 keep the
/// packet size above. Both numbers match what the network enforces, so a
/// transaction accepted here is one the network would also accept.
pub const MAX_TRANSACTION_V1_SIZE: usize = solana_sdk::message::v1::MAX_TRANSACTION_SIZE;
