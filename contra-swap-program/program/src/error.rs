use codama::CodamaErrors;
use pinocchio::error::ProgramError;
use thiserror::Error;

/// Errors that may be returned by the Contra Swap Program.
#[derive(Clone, Debug, Eq, PartialEq, Error, CodamaErrors)]
pub enum ContraSwapProgramError {
    /// (0) Signer is not a party to the DvP (must be user_a or user_b)
    #[error("Signer is not a party to the DvP")]
    SignerNotParty,

    /// (1) DvP has passed its expiry timestamp
    #[error("DvP has expired")]
    DvpExpired,

    /// (2) Signer is not the configured settlement authority
    #[error("Signer is not the settlement authority")]
    SettlementAuthorityMismatch,

    /// (3) Current time is before earliest_settlement_timestamp
    #[error("Settlement is not yet allowed")]
    SettlementTooEarly,

    /// (4) Leg balance does not match the expected amount
    #[error("DvP leg is not funded with the expected amount")]
    LegNotFunded,

    /// (5) `expiry_timestamp` is at or before `now()` at creation time
    #[error("DvP expiry must be in the future at creation")]
    ExpiryNotInFuture,

    /// (6) `earliest_settlement_timestamp` is after `expiry_timestamp`
    /// (DvP would never be settleable)
    #[error("Earliest settlement must not be after expiry")]
    EarliestAfterExpiry,

    /// (7) `user_a == user_b`: self-DvP — leg B is unfundable
    #[error("user_a and user_b must differ")]
    SelfDvp,

    /// (8) `mint_a == mint_b`: degenerate same-asset trade
    #[error("mint_a and mint_b must differ")]
    SameMint,

    /// (9) `amount_a == 0` or `amount_b == 0`
    #[error("DvP leg amounts must be non-zero")]
    ZeroAmount,
}

impl From<ContraSwapProgramError> for ProgramError {
    fn from(e: ContraSwapProgramError) -> Self {
        ProgramError::Custom(e as u32)
    }
}
