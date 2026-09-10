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

    #[error("Invalid instruction builder: {reason}")]
    InvalidBuilder { reason: String },

    #[error("Bitmap rotation pending: {in_flight_count} in-flight transactions must settle before rotating")]
    RotationPending { in_flight_count: usize },

    #[error("Withdrawal bitmap unavailable: {reason}")]
    BitmapUnavailable { reason: String },

    #[error("Transaction nonce {nonce} belongs to generation {nonce_generation} but the bitmap is on generation {chain_generation}")]
    GenerationMismatch {
        nonce: u64,
        nonce_generation: u64,
        chain_generation: u64,
    },

    #[error("Withdrawal bitmap diverges from the database: nonces {db_only:?} are Completed in the database but unconsumed on-chain, nonces {chain_only:?} are consumed on-chain but not Completed in the database")]
    BitmapDivergence {
        db_only: Vec<u64>,
        chain_only: Vec<u64>,
    },
}
