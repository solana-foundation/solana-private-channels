extern crate alloc;

use codama::CodamaInstructions;
use pinocchio::Address as Pubkey;

/// Instructions for the Solana PrivateChannel Escrow Program. This
/// is currently not used in the program business logic, but
/// we include it for IDL generation.
#[allow(clippy::large_enum_variant)]
#[repr(C, u8)]
#[derive(Clone, Debug, PartialEq, CodamaInstructions)]
pub enum PrivateChannelEscrowProgramInstruction {
    /// Create a new escrow instance with the specified admin.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(
        name = "instance_seed",
        docs = "Instance seed signer for PDA derivation",
        signer
    ))]
    #[codama(account(name = "instance", docs = "Instance PDA to be created", writable))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    CreateInstance {
        /// Bump for the instance PDA
        bump: u8,
    } = 0,

    /// Allow new token mints for the instance (admin-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "mint", docs = "Token mint to be allowed"))]
    #[codama(account(name = "allowed_mint", docs = "PDA of the Allowed Mint", writable))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance Escrow account for specified mint",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(name = "token_program", docs = "Token program"))]
    #[codama(account(name = "associated_token_program", docs = "Associated Token program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    AllowMint {
        /// Bump for the allowed mint PDA
        bump: u8,
    } = 1,

    /// Block previously allowed mints for the instance (admin-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "mint", docs = "Token mint to be blocked"))]
    #[codama(account(name = "allowed_mint", docs = "Existing Allowed Mint PDA", writable))]
    #[codama(account(name = "system_program", docs = "System program for account creation"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    BlockMint {} = 2,

    /// Add an operator to the instance (admin-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "operator", docs = "Operator public key to be added"))]
    #[codama(account(name = "operator_pda", docs = "Operator PDA to be created", writable))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    AddOperator {
        /// Bump for the operator PDA
        bump: u8,
    } = 3,

    /// Remove an operator from the instance (admin-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "operator", docs = "Operator public key to be removed"))]
    #[codama(account(name = "operator_pda", docs = "Existing Operator PDA", writable))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    RemoveOperator {} = 4,

    /// Set a new admin for the instance (current admin only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "current_admin", docs = "Current admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to update admin", writable))]
    #[codama(account(name = "new_admin", docs = "New admin public key", signer))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    SetNewAdmin {} = 5,

    /// Deposit tokens from user ATA to instance escrow ATA (permissionless).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "user", docs = "User depositing tokens", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate"))]
    #[codama(account(name = "mint", docs = "Token mint being deposited"))]
    #[codama(account(
        name = "allowed_mint",
        docs = "AllowedMint PDA to validate mint is allowed"
    ))]
    #[codama(account(
        name = "user_ata",
        docs = "User's Associated Token Account for this mint",
        writable
    ))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance's Associated Token Account (escrow) for this mint",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(name = "token_program", docs = "Token program for the mint"))]
    #[codama(account(name = "associated_token_program", docs = "Associated Token program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    Deposit {
        /// Amount of tokens to deposit
        amount: u64,
        /// Optional recipient for PrivateChannel tracking, is the wallet address, not the ATA (if None, defaults to user)
        recipient: Option<Pubkey>,
    } = 6,

    /// Release funds from escrow to user (operator-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator releasing the funds", signer))]
    #[codama(account(
        name = "instance",
        docs = "Instance PDA to validate and update",
        writable
    ))]
    #[codama(account(
        name = "operator_pda",
        docs = "Operator PDA to validate operator permissions"
    ))]
    #[codama(account(name = "mint", docs = "Token mint being released"))]
    #[codama(account(
        name = "allowed_mint",
        docs = "AllowedMint PDA to validate mint is allowed"
    ))]
    #[codama(account(
        name = "user_ata",
        docs = "User's Associated Token Account for this mint",
        writable
    ))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance's Associated Token Account (escrow) for this mint",
        writable
    ))]
    #[codama(account(name = "token_program", docs = "Token program for the mint"))]
    #[codama(account(name = "associated_token_program", docs = "Associated Token program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    ReleaseFunds {
        /// Amount of tokens to release
        amount: u64,
        /// User receiving the funds (wallet address, not the ATA)
        user: Pubkey,
        /// New withdrawal transactions root
        new_withdrawal_root: [u8; 32],
        /// Transaction nonce
        transaction_nonce: u64,
        /// Sibling proofs (flattened as 512 bytes: 16 proofs × 32 bytes each)
        sibling_proofs: [u8; 512],
    } = 7,

    /// Reset the SMT root for the instance (operator-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator resetting the SMT root", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to reset", writable))]
    #[codama(account(
        name = "operator_pda",
        docs = "Operator PDA to validate operator permissions"
    ))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    ResetSmtRoot {
        /// Tree index the caller expects the instance to be at. Rejected if it
        /// no longer matches, so a replayed reset cannot advance the tree twice.
        expected_current_tree_index: u64,
    } = 8,

    /// Invoked via CPI from another program to log event via instruction data.
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events",
        signer
    ))]
    EmitEvent {} = 228,
}
