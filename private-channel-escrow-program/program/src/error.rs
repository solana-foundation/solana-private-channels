use codama::CodamaErrors;
use pinocchio::error::ProgramError;
use thiserror::Error;

/// Errors that may be returned by the PrivateChannel Escrow Program.
#[derive(Clone, Debug, Eq, PartialEq, Error, CodamaErrors)]
pub enum PrivateChannelEscrowProgramError {
    /// (0) Invalid event authority provided
    #[error("Invalid event authority provided")]
    InvalidEventAuthority,

    /// (1) Invalid ATA provided
    #[error("Invalid ATA provided")]
    InvalidAta,

    /// (2) Invalid mint provided
    #[error("Invalid mint provided")]
    InvalidMint,

    /// (3) Instance ID invalid or does not respect rules
    #[error("Instance ID invalid or does not respect rules")]
    InvalidInstanceId,

    /// (4) Invalid instance provided
    #[error("Invalid instance provided")]
    InvalidInstance,

    /// (5) Invalid admin provided
    #[error("Invalid admin provided")]
    InvalidAdmin,

    /// (6) Transfer hook extension not allowed
    #[error("Transfer hook extension not allowed")]
    TransferHookNotAllowed,

    /// (7) Invalid operator PDA provided
    #[error("Invalid operator PDA provided")]
    InvalidOperatorPda,

    /// (8) Invalid token account provided
    #[error("Invalid token account provided")]
    InvalidTokenAccount,

    /// (9) Invalid escrow balance
    #[error("Invalid escrow balance")]
    InvalidEscrowBalance,

    /// (10) Invalid allowed mint
    #[error("Invalid allowed mint")]
    InvalidAllowedMint,

    /// (11) Invalid SMT proof provided
    #[error("Invalid SMT proof provided")]
    InvalidSmtProof,

    /// (12) Invalid transaction nonce for current tree index
    #[error("Invalid transaction nonce for current tree index")]
    InvalidTransactionNonceForCurrentTreeIndex,

    /// (13) ResetSmtRoot pre-state mismatch. Blocks replaying a landed reset.
    #[error("Unexpected current tree index for SMT root reset")]
    UnexpectedTreeIndex,
}

impl From<PrivateChannelEscrowProgramError> for ProgramError {
    fn from(e: PrivateChannelEscrowProgramError) -> Self {
        ProgramError::Custom(e as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each error variant must map to a fixed Custom(N) code. Client SDKs and
    // indexers decode errors by number, so silently reordering or inserting a
    // variant in the middle would break them without any compile error.
    // This test acts as an explicit lock on the ABI.
    #[test]
    fn test_error_codes_are_stable() {
        use PrivateChannelEscrowProgramError::*;

        let cases: &[(PrivateChannelEscrowProgramError, u32)] = &[
            (InvalidEventAuthority, 0),
            (InvalidAta, 1),
            (InvalidMint, 2),
            (InvalidInstanceId, 3),
            (InvalidInstance, 4),
            (InvalidAdmin, 5),
            (TransferHookNotAllowed, 6),
            (InvalidOperatorPda, 7),
            (InvalidTokenAccount, 8),
            (InvalidEscrowBalance, 9),
            (InvalidAllowedMint, 10),
            (InvalidSmtProof, 11),
            (InvalidTransactionNonceForCurrentTreeIndex, 12),
            (UnexpectedTreeIndex, 13),
        ];

        for (error, expected_code) in cases {
            assert_eq!(
                ProgramError::from(error.clone()),
                ProgramError::Custom(*expected_code),
                "{error:?} should map to Custom({expected_code})"
            );
        }
    }
}
