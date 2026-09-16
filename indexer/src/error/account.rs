use solana_sdk::pubkey::Pubkey;

/// Errors related to account operations and data
#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("Account {pubkey} not found")]
    AccountNotFound { pubkey: Pubkey },

    /// The node answered and the mint is genuinely absent from the target chain.
    /// Kept apart from `AccountNotFound`, which a read that may heal also produces.
    #[error("Mint {pubkey} not found on target chain")]
    TargetMintMissing { pubkey: Pubkey },

    #[error("Instance {instance} not found")]
    InstanceNotFound { instance: Pubkey },

    #[error("Invalid mint {pubkey}: {reason}")]
    InvalidMint { pubkey: Pubkey, reason: String },

    /// The live mint no longer carries something the reviewed profile pinned,
    /// which is what a close and recreate looks like. Permanent for the row:
    /// only a fresh AllowMint re-pins the profile.
    #[error("Mint {pubkey} no longer matches its reviewed profile: {reason}")]
    MintProfileMismatch { pubkey: Pubkey, reason: String },

    #[error("Failed to deserialize account data for {pubkey}: {reason}")]
    AccountDeserializationFailed { pubkey: Pubkey, reason: String },

    #[error("Insufficient accounts: required {required}, actual {actual}")]
    InsufficientAccounts { required: usize, actual: usize },

    #[error("Account index out of bounds: {index}")]
    AccountIndexOutOfBounds { index: usize },
}
