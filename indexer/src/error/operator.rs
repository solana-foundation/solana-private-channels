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

    #[error("Webhook error: {0}")]
    WebhookError(String),
}
