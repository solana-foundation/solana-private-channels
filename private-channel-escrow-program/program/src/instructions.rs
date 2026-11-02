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
    #[codama(account(
        name = "withdrawal_bitmap",
        docs = "Withdrawal bitmap PDA to be created",
        writable
    ))]
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
        /// Bump for the withdrawal bitmap PDA
        bitmap_bump: u8,
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
        /// Fee in base units charged on each channel withdrawal of this mint.
        /// Re-allowing with a new value reprices from the next deposit on.
        withdraw_fee: u64,
        /// Smallest amount a channel withdrawal of this mint may move, 0 for
        /// none. Updated with the fee, from the next deposit on.
        min_withdraw_amount: u64,
        /// Smallest amount a deposit of this mint must land in the escrow, 0
        /// for none. Enforced by Deposit; a re-allow reprices it.
        min_deposit_amount: u64,
    } = 1,

    /// Set the deposit and withdrawal gates on an allowed mint (admin-only).
    /// Both flags are absolute, so passing false for one re-opens that gate.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "mint", docs = "Token mint whose gates are being set"))]
    #[codama(account(name = "allowed_mint", docs = "Existing Allowed Mint PDA", writable))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    BlockMint {
        /// Reject new deposits for this mint
        block_deposits: bool,
        /// Reject fund releases for this mint
        block_withdrawals: bool,
    } = 2,

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
    ///
    /// Append the mint's transfer-hook extras after the fixed accounts: hook
    /// program, validation PDA, and whatever its `ExtraAccountMetaList`
    /// resolves to. Omit for mints without a hook. Codama drops trailing
    /// accounts, so they are not in the generated account list.
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
    #[codama(account(
        name = "user_allowed_wallet_pda",
        docs = "AllowedWallet PDA of user; read only while deposits are whitelisted"
    ))]
    #[codama(account(
        name = "recipient_allowed_wallet_pda",
        docs = "AllowedWallet PDA of the recipient, or of user when none is given; read only while deposits are whitelisted"
    ))]
    Deposit {
        /// Amount of tokens to deposit
        amount: u64,
        /// Optional recipient for PrivateChannel tracking, is the wallet address, not the ATA (if None, defaults to user)
        recipient: Option<Pubkey>,
    } = 6,

    /// Release funds from escrow to user (operator-only).
    ///
    /// Append the mint's transfer-hook extras after the fixed accounts: hook
    /// program, validation PDA, and whatever its `ExtraAccountMetaList`
    /// resolves to. Omit for mints without a hook. Codama drops trailing
    /// accounts, so they are not in the generated account list.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator releasing the funds", signer))]
    #[codama(account(
        name = "instance",
        docs = "Instance PDA to validate and sign the transfer"
    ))]
    #[codama(account(
        name = "withdrawal_bitmap",
        docs = "Withdrawal bitmap PDA to consume the nonce",
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
    #[codama(account(
        name = "memo_program",
        docs = "SPL Memo program; only used when user_ata requires a memo"
    ))]
    #[codama(account(
        name = "user_allowed_wallet_pda",
        docs = "AllowedWallet PDA of user; read only while withdrawals are whitelisted"
    ))]
    #[codama(account(
        name = "withdrawer_allowed_wallet_pda",
        docs = "AllowedWallet PDA of withdrawer; read only while withdrawals are whitelisted"
    ))]
    ReleaseFunds {
        /// Amount of tokens to release
        amount: u64,
        /// User receiving the funds (wallet address, not the ATA)
        user: Pubkey,
        /// Transaction nonce to consume from the withdrawal bitmap
        transaction_nonce: u64,
        /// Wallet whose channel burn this release settles; may differ from user
        withdrawer: Pubkey,
    } = 7,

    /// Rotate the withdrawal bitmap to the next generation (operator-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator rotating the bitmap", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA the bitmap belongs to"))]
    #[codama(account(
        name = "withdrawal_bitmap",
        docs = "Withdrawal bitmap PDA to rotate",
        writable
    ))]
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
    RotateBitmap {
        /// Generation the caller expects the bitmap to be at. Rejected if it no
        /// longer matches, so a replayed rotation cannot skip a generation.
        expected_generation: u64,
    } = 8,

    /// Add a wallet to the instance whitelist, or rewrite its flags (admin-only).
    /// Blocking both directions is rejected; use RemoveAllowedWallet instead.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "wallet", docs = "Wallet to be added or updated"))]
    #[codama(account(
        name = "allowed_wallet_pda",
        docs = "AllowedWallet PDA to be created or updated",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    AddOrUpdateAllowedWallet {
        /// Bump for the allowed wallet PDA
        bump: u8,
        /// Refuse this wallet on Deposit while deposits are whitelisted
        block_deposits: bool,
        /// Refuse this wallet on ReleaseFunds while withdrawals are whitelisted
        block_withdrawals: bool,
    } = 9,

    /// Remove a wallet from the instance whitelist (admin-only).
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to validate admin authority"))]
    #[codama(account(name = "wallet", docs = "Wallet to be removed"))]
    #[codama(account(
        name = "allowed_wallet_pda",
        docs = "Existing AllowedWallet PDA",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    RemoveAllowedWallet {} = 10,

    /// Set which directions the instance whitelist gates (admin-only). While
    /// deposits are whitelisted, Deposit requires the depositor and recipient to
    /// be allowed to deposit; while withdrawals are, ReleaseFunds requires the
    /// withdrawer and destination to be allowed to withdraw. Grows a 0.1.0 instance
    /// to the current layout, with the payer topping up its rent.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA to update", writable))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    SetWhitelistEnabled {
        /// Require allowed wallets on Deposit
        whitelist_deposits: bool,
        /// Require allowed wallets on ReleaseFunds
        whitelist_withdrawals: bool,
    } = 11,

    /// Turn on confidential transfers for the escrow account of an allowed mint
    /// (admin-only). Confidential credits stay off outside the escrow's own
    /// instructions. Put the `VerifyPubkeyValidity` proof of the instance ElGamal
    /// key in the same transaction.
    #[codama(account(
        name = "payer",
        docs = "Pays for the larger escrow account",
        signer,
        writable
    ))]
    #[codama(account(name = "admin", docs = "Admin of Instance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA, owner of the escrow account"))]
    #[codama(account(name = "mint", docs = "Token mint of the escrow account"))]
    #[codama(account(name = "allowed_mint", docs = "AllowedMint PDA of the mint"))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance's Associated Token Account (escrow) for this mint",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(name = "token_program", docs = "Token-2022 program"))]
    #[codama(account(
        name = "instructions_sysvar",
        docs = "Instructions sysvar, holding the key's proof"
    ))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    ConfigureConfidentialAccount {
        /// Zero, encrypted under the instance AES key
        decryptable_zero_balance: [u8; 36],
        /// Position of the `VerifyPubkeyValidity` instruction relative to this one, never 0
        proof_instruction_offset: i8,
    } = 12,

    /// Deposit a hidden amount from the user's confidential balance into the escrow
    /// (permissionless). The indexer decrypts the amount from the event.
    ///
    /// Each proof offset is the proof instruction's position relative to this
    /// one, or 0 for a context account. Append one context account per zero
    /// offset, in Token-2022's proof order, then the mint's transfer-hook extras.
    /// Codama drops trailing accounts, so they are not in the generated list.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "user", docs = "User depositing tokens", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA, owner of the escrow account"))]
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
    #[codama(account(name = "token_program", docs = "Token-2022 program"))]
    #[codama(account(
        name = "instructions_sysvar",
        docs = "Instructions sysvar, read when a proof is an instruction"
    ))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    #[codama(account(
        name = "user_allowed_wallet_pda",
        docs = "AllowedWallet PDA of user; read only while deposits are whitelisted"
    ))]
    #[codama(account(
        name = "recipient_allowed_wallet_pda",
        docs = "AllowedWallet PDA of the recipient, or of user when none is given; read only while deposits are whitelisted"
    ))]
    ConfidentialDeposit {
        /// User's available balance after the deposit, encrypted under their AES key
        new_source_decryptable_available_balance: [u8; 36],
        /// Low bits of the amount, encrypted under the mint's auditor key
        auditor_ciphertext_lo: [u8; 64],
        /// High bits of the amount, encrypted under the mint's auditor key
        auditor_ciphertext_hi: [u8; 64],
        equality_proof_instruction_offset: i8,
        ciphertext_validity_proof_instruction_offset: i8,
        range_proof_instruction_offset: i8,
        /// Read only for mints with a transfer fee
        fee_sigma_proof_instruction_offset: i8,
        /// Read only for mints with a transfer fee
        fee_ciphertext_validity_proof_instruction_offset: i8,
        /// Optional recipient for PrivateChannel tracking, is the wallet address, not the ATA (if None, defaults to user)
        recipient: Option<Pubkey>,
    } = 13,

    /// Release a hidden amount from the escrow to the user's confidential balance
    /// (operator-only).
    ///
    /// Proof offsets and trailing accounts as in ConfidentialDeposit.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator releasing the funds", signer))]
    #[codama(account(
        name = "instance",
        docs = "Instance PDA to validate and sign the transfer"
    ))]
    #[codama(account(
        name = "withdrawal_bitmap",
        docs = "Withdrawal bitmap PDA to consume the nonce",
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
    #[codama(account(name = "token_program", docs = "Token-2022 program"))]
    #[codama(account(
        name = "instructions_sysvar",
        docs = "Instructions sysvar, read when a proof is an instruction"
    ))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    #[codama(account(
        name = "memo_program",
        docs = "SPL Memo program; only used when user_ata requires a memo"
    ))]
    #[codama(account(
        name = "user_allowed_wallet_pda",
        docs = "AllowedWallet PDA of user; read only while withdrawals are whitelisted"
    ))]
    #[codama(account(
        name = "withdrawer_allowed_wallet_pda",
        docs = "AllowedWallet PDA of withdrawer; read only while withdrawals are whitelisted"
    ))]
    ConfidentialReleaseFunds {
        /// User receiving the funds (wallet address, not the ATA)
        user: Pubkey,
        /// Transaction nonce to consume from the withdrawal bitmap
        transaction_nonce: u64,
        /// Wallet whose channel burn this release settles; may differ from user
        withdrawer: Pubkey,
        /// Escrow's available balance after the release, encrypted under the instance AES key
        new_source_decryptable_available_balance: [u8; 36],
        /// Low bits of the amount, encrypted under the mint's auditor key
        auditor_ciphertext_lo: [u8; 64],
        /// High bits of the amount, encrypted under the mint's auditor key
        auditor_ciphertext_hi: [u8; 64],
        equality_proof_instruction_offset: i8,
        ciphertext_validity_proof_instruction_offset: i8,
        range_proof_instruction_offset: i8,
        /// Read only for mints with a transfer fee
        fee_sigma_proof_instruction_offset: i8,
        /// Read only for mints with a transfer fee
        fee_ciphertext_validity_proof_instruction_offset: i8,
    } = 14,

    /// Move the escrow's pending balance into its available balance
    /// (operator-only). A credit that arrived after the operator read the account
    /// is applied too but missing from the AES balance; the event carries the
    /// pending balance actually applied, so the operator counts available itself.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator applying the balance", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA, owner of the escrow account"))]
    #[codama(account(
        name = "operator_pda",
        docs = "Operator PDA to validate operator permissions"
    ))]
    #[codama(account(name = "mint", docs = "Token mint of the escrow account"))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance's Associated Token Account (escrow) for this mint",
        writable
    ))]
    #[codama(account(name = "token_program", docs = "Token-2022 program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    ApplyPendingBalance {
        /// Pending credits the operator saw
        expected_credit_counter: u64,
        /// Available balance after the apply, encrypted under the instance AES key
        new_decryptable_available_balance: [u8; 36],
    } = 15,

    /// Move escrow funds from the public to the confidential balance
    /// (operator-only). The amount is public.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator moving the funds", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA, owner of the escrow account"))]
    #[codama(account(
        name = "operator_pda",
        docs = "Operator PDA to validate operator permissions"
    ))]
    #[codama(account(name = "mint", docs = "Token mint of the escrow account"))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance's Associated Token Account (escrow) for this mint",
        writable
    ))]
    #[codama(account(name = "token_program", docs = "Token-2022 program"))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    MoveToConfidential {
        /// Amount to move
        amount: u64,
    } = 16,

    /// Move escrow funds from the confidential to the public balance
    /// (operator-only). The amount is public.
    ///
    /// Append one context account per zero proof offset, equality first.
    #[codama(account(name = "payer", docs = "Transaction fee payer", signer, writable))]
    #[codama(account(name = "operator", docs = "Operator moving the funds", signer))]
    #[codama(account(name = "instance", docs = "Instance PDA, owner of the escrow account"))]
    #[codama(account(
        name = "operator_pda",
        docs = "Operator PDA to validate operator permissions"
    ))]
    #[codama(account(name = "mint", docs = "Token mint of the escrow account"))]
    #[codama(account(
        name = "instance_ata",
        docs = "Instance's Associated Token Account (escrow) for this mint",
        writable
    ))]
    #[codama(account(name = "token_program", docs = "Token-2022 program"))]
    #[codama(account(
        name = "instructions_sysvar",
        docs = "Instructions sysvar, read when a proof is an instruction"
    ))]
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events"
    ))]
    #[codama(account(
        name = "private_channel_escrow_program",
        docs = "Current program for CPI"
    ))]
    MoveToPublic {
        /// Amount to move
        amount: u64,
        /// Available balance after the move, encrypted under the instance AES key
        new_decryptable_available_balance: [u8; 36],
        equality_proof_instruction_offset: i8,
        range_proof_instruction_offset: i8,
    } = 17,

    /// Invoked via CPI from another program to log event via instruction data.
    #[codama(account(
        name = "event_authority",
        docs = "Event authority PDA for emitting events",
        signer
    ))]
    EmitEvent {} = 228,
}
