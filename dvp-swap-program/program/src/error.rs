use codama::CodamaErrors;
use pinocchio::error::ProgramError;
use thiserror::Error;

/// Errors that may be returned by the DvP Swap Program.
#[derive(Clone, Debug, Eq, PartialEq, Error, CodamaErrors)]
pub enum DvpSwapProgramError {
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

    /// (10) Mint carries a Token-2022 extension the swap program refuses
    /// to support (confidential transfer, confidential transfer fee config,
    /// transfer fee, interest bearing, scaled UI amount, non-transferable).
    /// Checked only at CreateDvp.
    #[error("Mint carries an unsupported Token-2022 extension")]
    BlockedMintExtension,

    /// (11) `settlement_authority` equals `user_a` or `user_b`: it must be
    /// a third party, per the documented role.
    #[error("settlement_authority must not be user_a or user_b")]
    SettlementAuthorityIsParty,

    /// (12) `settlement_authority` is an executable account, which can't be
    /// credited the closed-account rent at Settle/Cancel.
    #[error("settlement_authority must not be executable")]
    SettlementAuthorityExecutable,

    /// (13) A DvP with these seeds was already created; the nonce
    /// tombstone outlives the closed trade so the PDA can't be reused.
    #[error("nonce already used for these DvP seeds")]
    NonceAlreadyUsed,

    /// (14) `expiry_timestamp` is more than `MAX_DVP_DURATION_SECS` past
    /// creation time, which would lock escrow rent for an unbounded term.
    #[error("DvP expiry is too far in the future")]
    ExpiryTooFarInFuture,
}

impl From<DvpSwapProgramError> for ProgramError {
    fn from(e: DvpSwapProgramError) -> Self {
        ProgramError::Custom(e as u32)
    }
}
