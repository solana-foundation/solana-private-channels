use super::{AccountError, ProgramError, StorageError, TransactionError};

/// Top-level errors from the operator component
///
/// The operator fetches pending transactions from storage, processes them,
/// and sends them to the blockchain.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Transaction error: {0}")]
    Transaction(#[from] Box<TransactionError>),

    #[error("Account error: {0}")]
    Account(#[from] AccountError),

    #[error("Program error: {0}")]
    Program(#[from] ProgramError),

    #[error("Invalid pubkey '{pubkey}': {reason}")]
    InvalidPubkey { pubkey: String, reason: String },

    #[error("Missing transaction builder")]
    MissingBuilder,

    #[error("Channel closed: {component}")]
    ChannelClosed { component: String },

    #[error("Channel send failed: {0}")]
    ChannelSend(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("Channel send failed during shutdown")]
    ShutdownChannelSend,

    #[error("RPC error: {0}")]
    RpcError(String),

    /// The channel no longer holds the block the withdraw indexer's checkpoint was built
    /// on, so it was restored behind the indexer DB. See docs/PITR.md.
    #[error(
        "channel fence check failed: {reason}. The channel database was restored behind the \
         indexer database; restore the indexer to a point before the channel's restore target \
         (docs/PITR.md)"
    )]
    ChannelFence { reason: String },

    #[error("channel fence could not be checked: {reason}")]
    ChannelFenceUnchecked { reason: String },

    /// The channel mint history could not be read completely, so a row it may already
    /// have paid cannot be told apart from a new one.
    #[error("consumed set unavailable: {reason}")]
    ConsumedSet { reason: String },

    #[error("Invalid config: {0}")]
    InvalidConfig(String),

    #[error("Webhook error: {0}")]
    WebhookError(String),

    #[error(
        "Mint {mint} has no allowed status in mint_status_history coming into or inside the deposit's slot (transaction {transaction_id}); refusing to mint"
    )]
    MintNotAllowed { transaction_id: i64, mint: String },

    #[error("Another {program_type:?} sender already holds the singleton lock; refusing to start")]
    SenderAlreadyRunning { program_type: crate::ProgramType },

    #[error(
        "The {program_type:?} sender lock was lost during the boot pre-flight; refusing to start"
    )]
    SenderLockLostAtBoot { program_type: crate::ProgramType },
}
