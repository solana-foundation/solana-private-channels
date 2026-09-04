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

    #[error("Checkpoint channel closed; cannot persist slot progress")]
    CheckpointChannelClosed,

    #[error("Transaction processor task panicked")]
    ProcessorPanicked,

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

    /// A backfill-only run finished without durably recording its whole range.
    /// `committed` is `None` when no checkpoint was ever written, which is not
    /// the same as a checkpoint that stalled part way and must stay
    /// distinguishable in the logs an operator reads after a failed repair.
    #[error(
        "backfill for {program_type} left the committed checkpoint at {committed:?}, short of \
         target slot {target}; the range was not fully recorded"
    )]
    BackfillIncomplete {
        program_type: String,
        committed: Option<u64>,
        target: u64,
    },

    /// The checkpoint writer had to be cancelled before it confirmed its final flush, so the
    /// run cannot prove it finished even though its slots may all be stored. Kept apart from
    /// `BackfillIncomplete` because the operator action differs: this one points at a slow or
    /// wedged database, not at a slot the pipeline failed to record.
    #[error(
        "backfill for {program_type} could not confirm its checkpoint: the writer was still \
         running after {waited_secs}s and was cancelled, leaving the durable checkpoint at \
         {committed:?} against target {target}. Re-run the repair once the database is healthy"
    )]
    BackfillCheckpointUnconfirmed {
        program_type: String,
        committed: Option<u64>,
        target: u64,
        waited_secs: u64,
    },
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

    /// Minted channel supply is above the custody backing it. Kept separate from a
    /// custody-versus-ledger mismatch because no amount of indexing changes either side
    /// of this comparison: it reads the chain twice and never touches the database.
    #[error("{count} mint(s) have channel supply above escrow custody by more than {threshold} raw units; see logs for per-mint details")]
    SupplyExceedsCustody { count: usize, threshold: u64 },

    /// Channel supply could not be read at all, so the invariant never ran. Startup stops
    /// rather than proceed unchecked: an unreadable gateway hides an existing breach just
    /// as well as a healthy channel does, and the two are indistinguishable from here.
    #[error("channel supply for {count} mint(s) was unreadable across every attempt; the supply invariant did not run, so custody cannot be vouched for")]
    SupplyInvariantUnverified { count: usize },

    /// The two token-program sweeps never answered at the same slot, so the custody
    /// numbers describe no single point and cannot be compared against a ledger bounded at
    /// one. Usually a load-balanced endpoint answering from nodes at different heights,
    /// which is why another sweep is worth trying before giving up.
    #[error("custody sweeps never settled on one slot after {attempts} attempts (last spread {low}..{high}); custody cannot be pinned to a single point")]
    CustodySlotUnsettled { attempts: u32, low: u64, high: u64 },

    /// The custody reading came from a slot the ledger has already passed, so the two
    /// cannot be compared at a common point and any verdict would be guesswork. Usually a
    /// lagging RPC node, which is why a re-read is worth trying before giving up.
    #[error("custody was read at slot {snapshot_slot}, behind the committed checkpoint {committed}; the node is answering from behind the ledger")]
    CustodyBehindLedger { snapshot_slot: u64, committed: u64 },

    #[error("Invalid pubkey '{pubkey}': {reason}")]
    InvalidPubkey { pubkey: String, reason: String },

    #[error(
        "source_rpc_url (channel RPC) required for the escrow indexer: the startup \
         supply invariant reads channel-token supply from it and must always run"
    )]
    MissingChannelRpc,

    #[error("DB net balance for mint {mint} exceeds u64::MAX ({net}); the escrow ATA cannot hold this, so the DB is corrupt")]
    DbBalanceOverflow { mint: String, net: String },

    /// The pre-drop consumed-set could not be built completely (channel unreachable,
    /// pagination failed, or a legacy-scheme memo could not be reconciled). Resync
    /// aborts before any destruction so the live DB is left intact.
    #[error("consumed-set unavailable, resync aborted before drop: {reason}")]
    ConsumedSetUnavailable { reason: String },
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

    #[error("Slot {slot} is unavailable: a block exists here that this endpoint will not serve, so its contents are unknown")]
    SlotUnavailable { slot: u64 },

    #[error("Could not list the blocks produced in slots {from}..={to}, so the backfill boundary cannot be anchored: {source}")]
    ProducerLookupFailed {
        from: u64,
        to: u64,
        #[source]
        source: DataSourceRpcError,
    },

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

    /// `last` stays an Option so "stalled at slot N" and "no row was ever written" read
    /// differently in the log: the first points at an unprocessed slot in the range, the
    /// second at a checkpoint writer that never flushed. They need different responses.
    #[error(
        "Checkpoint for {program_type} reached {last:?}, never {target}, after {waited_secs}s"
    )]
    CommitTimeout {
        program_type: String,
        last: Option<u64>,
        target: u64,
        waited_secs: u64,
    },

    /// `setting` names the offending config key so the message points at the knob to
    /// change, since the two keys that can trigger this need different remedies.
    #[error(
        "Configured {setting} {start_slot} is ahead of the durable checkpoint {checkpoint} \
         for {program_type}: the slots after {checkpoint} and below {start_slot} have never \
         been indexed and would be skipped. Lower {setting} to {checkpoint} or below, unset \
         it, or run a destructive resync if the skip is intended."
    )]
    StartSlotAheadOfCheckpoint {
        setting: &'static str,
        program_type: &'static str,
        start_slot: u64,
        checkpoint: u64,
    },
}
