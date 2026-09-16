// Constants for RocksDB column families and reserved keys
// ===== Column Family Names =====

/// Column family for storing account state data
/// Key: Pubkey (32 bytes)
/// Value: Serialized AccountSharedData
pub const CF_ACCOUNTS: &str = "accounts";

/// Column family for storing system metadata
/// Used for storing configuration and state that isn't account-specific
pub const CF_METADATA: &str = "metadata";

/// Column family for storing transaction history
/// Key: Signature (64 bytes)
/// Value: `StoredTransaction::to_bytes`, a versioned row holding the signed wire bytes and metadata
pub const CF_TRANSACTIONS: &str = "transactions";

/// Column family for storing block metadata
/// Key: Slot number (u64, 8 bytes, little-endian)
/// Value: Serialized BlockInfo (includes blockhash, parent slot, timestamps, transaction signatures)
pub const CF_BLOCKS: &str = "blocks";

// ===== Reserved Keys =====

/// Key for storing the latest processed slot number
/// Column Family: CF_METADATA
/// Value: u64 (8 bytes, little-endian)
/// Used to track the current slot for the sequencer and read nodes
pub const METADATA_KEY_LATEST_SLOT: &[u8] = b"latest_slot";

/// Key for storing the latest blockhash
/// Column Family: CF_METADATA
/// Value: Hash (32 bytes)
/// Used to track the most recent blockhash
pub const METADATA_KEY_LATEST_BLOCKHASH: &[u8] = b"latest_blockhash";
