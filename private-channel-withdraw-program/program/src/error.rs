use codama::CodamaErrors;
use pinocchio::error::ProgramError;
use thiserror::Error;

/// Errors that may be returned by the PrivateChannel Withdraw Program.
#[derive(Clone, Debug, Eq, PartialEq, Error, CodamaErrors)]
pub enum PrivateChannelWithdrawProgramError {
    /// (0) Invalid mint provided
    #[error("Invalid mint provided")]
    InvalidMint,

    /// (1) Withdrawal amount must be greater than zero
    #[error("Withdrawal amount must be greater than zero")]
    ZeroAmount,

    /// (2) Withdraw config is not the mint's PDA or its data is malformed
    #[error("Invalid withdraw config")]
    InvalidWithdrawConfig,

    /// (3) No withdraw config exists for this mint yet
    #[error("Withdraw config not initialized")]
    WithdrawConfigNotInitialized,

    /// (4) Signer is not the mint authority
    #[error("Signer is not the mint authority")]
    InvalidMintAuthority,

    /// (5) Fee destination is not the configured treasury token account
    #[error("Invalid treasury token account")]
    InvalidTreasuryAccount,

    /// (6) System program account is not the system program
    #[error("Invalid system program")]
    InvalidSystemProgram,

    /// (7) Withdrawal amount is below the mint's minimum
    #[error("Withdrawal amount is below the mint's minimum")]
    AmountBelowMinimum,
}

impl From<PrivateChannelWithdrawProgramError> for ProgramError {
    fn from(e: PrivateChannelWithdrawProgramError) -> Self {
        ProgramError::Custom(e as u32)
    }
}
