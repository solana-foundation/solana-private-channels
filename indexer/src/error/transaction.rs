/// Errors from Solana transaction operations
///
/// Covers transaction submission, simulation, confirmation, and program errors
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] Box<solana_rpc_client_api::client_error::Error>),

    #[error("Signer error: {0}")]
    Signer(#[from] solana_keychain::SignerError),

    #[error("Program execution failed: {0}")]
    Program(#[from] ProgramError),

    #[error("Failed to persist release signature before broadcast: {reason}")]
    PreSendPersistFailed { reason: String },
}

/// Errors from Solana program execution
///
/// Program-specific errors including system programs, token programs, and custom programs
#[derive(Debug, thiserror::Error)]
pub enum ProgramError {
    #[error("Invalid proof: {reason}")]
    InvalidProof { reason: String },

    #[error("SMT proof generation failed: {reason}")]
    SmtProofFailed { reason: String },

    #[error("SMT state not initialized")]
    SmtNotInitialized,

    #[error("Invalid instruction builder: {reason}")]
    InvalidBuilder { reason: String },

    #[error("Tree rotation pending: {in_flight_count} in-flight transactions must settle before rotating")]
    RotationPending { in_flight_count: usize },

    #[error("Transaction nonce {nonce} expects tree_index {expected_tree_index} but current local tree_index is {current_tree_index}")]
    TreeIndexMismatch {
        nonce: u64,
        expected_tree_index: u64,
        current_tree_index: u64,
    },

    #[error("SMT root mismatch: local root {local_root:?} does not match on-chain root {onchain_root:?}. Database may be out of sync with on-chain state.")]
    SmtRootMismatch {
        local_root: [u8; 32],
        onchain_root: [u8; 32],
    },
}
