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
