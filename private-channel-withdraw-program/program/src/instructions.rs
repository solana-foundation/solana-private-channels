extern crate alloc;

use codama::CodamaInstructions;
use pinocchio::Address as Pubkey;

/// Instructions for the Solana PrivateChannel Withdraw Program.
#[repr(C, u8)]
#[derive(Clone, Debug, PartialEq, CodamaInstructions)]
pub enum PrivateChannelWithdrawProgramInstruction {
    /// Withdraw funds from a token account to itself or to a destination (if provided)
    #[codama(account(name = "user", docs = "User initiating the withdrawal", signer))]
    #[codama(account(name = "mint", docs = "Token mint", writable))]
    #[codama(account(name = "token_account", docs = "Source token account", writable))]
    #[codama(account(name = "token_program", docs = "Token program"))]
    #[codama(account(name = "associated_token_program", docs = "Associated token program"))]
    #[codama(account(name = "withdraw_fee_config", docs = "Fee config PDA for the mint"))]
    #[codama(account(
        name = "treasury_token_account",
        docs = "Treasury token account the fee is paid to",
        writable
    ))]
    WithdrawFunds {
        /// Amount of tokens to withdraw
        amount: u64,
        /// Destination public key
        destination: Option<Pubkey>,
    } = 0,

    /// Create or overwrite the mint's withdraw fee config (mint authority only)
    #[codama(account(
        name = "authority",
        docs = "Mint authority, also pays for the config",
        signer,
        writable
    ))]
    #[codama(account(name = "mint", docs = "Token mint"))]
    #[codama(account(
        name = "withdraw_fee_config",
        docs = "Fee config PDA for the mint",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    SetWithdrawFeeConfig {
        /// Fee in base units charged on top of each withdrawal
        fee: u64,
        /// Owner of the token account fees are paid to
        treasury: Pubkey,
    } = 1,
}
