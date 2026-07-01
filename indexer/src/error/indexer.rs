use crate::error::account::AccountError;

use super::DataSourceRpcError;
use super::StorageError;

/// Top-level errors from the indexer component
///
/// The indexer monitors blockchain events and stores them in the database.
/// This error type aggregates all possible failures during indexing operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    // Channel errors
    #[error("Channel send failed during shutdown")]
    ShutdownChannelSend,

    #[error("Datasource error: {0}")]
    DataSource(#[from] DataSourceError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Parser error: {0}")]
    Parser(#[from] ParserError),

    #[error("Backfill error: {0}")]
    Backfill(#[from] BackfillError),

    #[error("Checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),

    #[error("Reconciliation failed: {0}")]
    Reconciliation(#[from] ReconciliationError),
}

/// Errors from startup reconciliation against on-chain state
#[derive(Debug, thiserror::Error)]
pub enum ReconciliationError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("RPC error for mint {mint}: {reason}")]
    Rpc { mint: String, reason: String },

    #[error("{count} mint(s) exceed mismatch threshold of {threshold} raw units; see logs for per-mint details")]
    MismatchExceedsThreshold { count: usize, threshold: u64 },

    #[error("Invalid pubkey '{pubkey}': {reason}")]
    InvalidPubkey { pubkey: String, reason: String },

    #[error("DB net balance for mint {mint} exceeds u64::MAX ({net}); the escrow ATA cannot hold this, so the DB is corrupt")]
    DbBalanceOverflow { mint: String, net: String },
}

/// Errors from data sources (RPC polling, Yellowstone, backfill operations)
#[derive(Debug, thiserror::Error)]
pub enum DataSourceError {
    #[error("RPC error: {0}")]
    Rpc(#[from] DataSourceRpcError),

    #[error("Backfill error: {0}")]
    Backfill(#[from] BackfillError),

    #[error("Invalid configuration: {reason}")]
    InvalidConfig { reason: String },

    #[error("Commitment level parse error: {value}")]
    InvalidCommitment { value: String },

    #[error("Gap fill failed: {reason}")]
    GapFillFailed { reason: String },
}

/// Errors specific to backfill operations
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("Gap too large: {gap} slots (max: {max_gap})")]
    GapTooLarge { gap: u64, max_gap: u64 },

    #[error("Failed to fetch slot {slot}: {source}")]
    SlotFetchFailed {
        slot: u64,
        #[source]
        source: DataSourceRpcError,
    },

    #[error("Slot {slot} transaction {signature} is missing metadata; block is incomplete")]
    MissingMeta { slot: u64, signature: String },

    // Channel errors
    #[error("Channel send failed: {0}")]
    ChannelSend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Errors from parsing blockchain data (instructions, accounts, etc.)
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("Invalid pubkey: {reason}")]
    InvalidPubkey { reason: String },

    #[error("Failed to parse instruction data: {reason}")]
    InstructionParseFailed { reason: String },

    #[error("Account error: {0}")]
    Account(#[from] AccountError),

    #[error("Missing field: {field}")]
    MissingField { field: String },

    #[error("Invalid base64 encoding: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("Base58 error: {0}")]
    Base58Error(#[from] bs58::decode::Error),

    #[error("Borsh deserialization failed: {0}")]
    BorshError(#[from] std::io::Error),
}

/// Errors related to checkpoint management
///
/// Checkpoints track indexing progress to enable resumption after restarts
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Invalid checkpoint: slot {slot} is before last checkpoint {last}")]
    InvalidCheckpoint { slot: u64, last: u64 },
}
