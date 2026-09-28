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

    /// (2) Fee config is not the mint's PDA or its data is malformed
    #[error("Invalid withdraw fee config")]
    InvalidFeeConfig,

    /// (3) No fee config exists for this mint yet
    #[error("Withdraw fee config not initialized")]
    FeeConfigNotInitialized,

    /// (4) Signer is not the mint authority
    #[error("Signer is not the mint authority")]
    InvalidMintAuthority,

    /// (5) Fee destination is not the configured treasury token account
    #[error("Invalid treasury token account")]
    InvalidTreasuryAccount,

    /// (6) System program account is not the system program
    #[error("Invalid system program")]
    InvalidSystemProgram,

    /// (7) Withdraw fee must be greater than zero
    #[error("Withdraw fee must be greater than zero")]
    ZeroFee,
}

impl From<PrivateChannelWithdrawProgramError> for ProgramError {
    fn from(e: PrivateChannelWithdrawProgramError) -> Self {
        ProgramError::Custom(e as u32)
    }
}
