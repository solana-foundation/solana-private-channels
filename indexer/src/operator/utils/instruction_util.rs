use crate::error::ProgramError;
use crate::operator::{
    is_mint_already_initialized_error, is_mint_not_initialized_error, ConfirmationResult,
    SignerUtil, DEFAULT_CU_MINT, DEFAULT_CU_RELEASE_FUNDS, MINT_IDEMPOTENCY_MEMO_PREFIX,
};
use crate::storage::common::models::DbTransaction;
use private_channel_escrow_program_client::instructions::{
    ReleaseFundsBuilder, RotateBitmapBuilder,
};
use solana_keychain::Signer;
use solana_sdk::hash::hashv;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_token::instruction::mint_to;
use std::fmt::Display;

pub const REMINT_IDEMPOTENCY_MEMO_PREFIX: &str = "private_channel:remint:";

/// Inner-instruction sentinel used when a source event is a top-level instruction.
/// Matches the DB natural key's `COALESCE(inner_index, -1)` so the id derived from a
/// rebuilt row equals the one derived when the mint was first sent.
const NO_INNER_INDEX: i32 = -1;

/// Length in bytes of the SHA256 digest a current-scheme source-event-id encodes.
pub const SOURCE_EVENT_ID_DIGEST_LEN: usize = 32;

/// Durable, chain-reproducible identity for a single source economic event.
///
/// Derived as `base58(SHA256(signature || instruction_index || inner_index))` from the
/// event's on-chain coordinates. Because it depends only on chain-visible identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceEventId(String);

impl SourceEventId {
    pub fn new(signature: &str, instruction_index: i32, inner_index: Option<i32>) -> Self {
        let digest = hashv(&[
            signature.as_bytes(),
            &instruction_index.to_le_bytes(),
            &inner_index.unwrap_or(NO_INNER_INDEX).to_le_bytes(),
        ]);
        SourceEventId(bs58::encode(digest.as_ref()).into_string())
    }

    /// Derive the id from a persisted transaction row's on-chain coordinates.
    pub fn from_row(transaction: &DbTransaction) -> Self {
        Self::new(
            &transaction.signature,
            transaction.instruction_index,
            transaction.inner_index,
        )
    }

    /// Wrap a memo value already present on-chain, accepting it only if it parses as a
    /// current-scheme id (base58 of a 32-byte digest). `None` flags a legacy/foreign
    /// scheme the reconcile cannot match, so callers can fail closed.
    pub fn from_encoded(value: &str) -> Option<Self> {
        match bs58::decode(value).into_vec() {
            Ok(bytes) if bytes.len() == SOURCE_EVENT_ID_DIGEST_LEN => {
                Some(SourceEventId(value.to_string()))
            }
            _ => None,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for SourceEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/*
Mint initialization is going to be done outside of the operator. There's a command that will add to the allowed mints on Solana mainnet
and will also initialize that mint on PrivateChannel. This simplifies our operator's code and reduces the checks it needs to do if we'd want to
validate mint existence on PrivateChannel.
*/

pub fn mint_idempotency_memo(transaction_id: impl Display) -> String {
    format!("{MINT_IDEMPOTENCY_MEMO_PREFIX}{transaction_id}")
}

pub fn remint_idempotency_memo(transaction_id: impl Display) -> String {
    format!("{REMINT_IDEMPOTENCY_MEMO_PREFIX}{transaction_id}")
}

/// Info needed to remint PrivateChannel tokens back to user on permanent withdrawal failure.
/// Captured in the processor where locals are available, since ReleaseFundsBuilder
/// has private fields (codama-generated).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WithdrawalRemintInfo {
    pub transaction_id: i64,
    pub trace_id: String,
    pub mint: Pubkey,
    pub user: Pubkey,
    pub user_ata: Pubkey,
    pub token_program: Pubkey,
    pub amount: u64,
}

/// Retry policy for transaction submission
/// Controls whether failed transaction sends should be retried
#[derive(Clone, Debug, Copy)]
pub enum RetryPolicy {
    /// No retry - for non-idempotent operations where duplicate sends would cause issues
    None,
    /// Retry with exponential backoff - safe for idempotent operations
    Idempotent,
}

pub type ExtraErrorCheckFn = Box<
    dyn Fn(&solana_sdk::transaction::TransactionError) -> Option<ConfirmationResult>
        + Send
        + Sync
        + 'static,
>;

// Extra error check policy for transaction submission
pub enum ExtraErrorCheckPolicy {
    /// No extra error checks
    None,
    /// Extra error checks
    Extra(Vec<ExtraErrorCheckFn>),
}

/// Which builder a transaction came from, so consumers dispatch on the kind rather than on absent ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionKind {
    ReleaseFunds,
    InitializeMint,
    Mint,
    RotateBitmap,
}

/// Wrapper enum for different transaction builder types
/// Allows processor to send multiple builder types through a single channel to sender
#[derive(Clone, Debug)]
pub enum TransactionBuilder {
    /// Release funds transaction (PrivateChannel to Solana) - consumes a nonce bit
    ReleaseFunds(Box<ReleaseFundsBuilderWithNonce>),
    /// Initialize mint transaction (Solana → PrivateChannel) - simple initialize_mint instruction
    InitializeMint(Box<InitializeMintBuilder>),
    /// Mint transaction (Solana → PrivateChannel) - simple SPL mint, no proof needed
    Mint(Box<MintToBuilderWithTxnId>),
    /// Rotate the withdrawal bitmap - clears the bits and opens the next generation
    RotateBitmap(Box<RotateBitmapBuilder>),
}

impl TransactionBuilder {
    /// The one place a builder variant is turned into the kind carried downstream.
    pub fn kind(&self) -> TransactionKind {
        match self {
            Self::ReleaseFunds(_) => TransactionKind::ReleaseFunds,
            Self::InitializeMint(_) => TransactionKind::InitializeMint,
            Self::Mint(_) => TransactionKind::Mint,
            Self::RotateBitmap(_) => TransactionKind::RotateBitmap,
        }
    }

    pub fn instructions(&self) -> Result<Vec<Instruction>, crate::error::ProgramError> {
        match self {
            Self::ReleaseFunds(builder_with_nonce) => {
                Ok(vec![builder_with_nonce.builder.instruction()])
            }
            Self::InitializeMint(builder) => Ok(vec![builder.instruction()?]),
            Self::Mint(builder_with_txn_id) => builder_with_txn_id.builder.instructions(),
            Self::RotateBitmap(builder) => Ok(vec![builder.instruction()]),
        }
    }

    pub fn compute_unit_price(&self) -> Option<u64> {
        match self {
            Self::ReleaseFunds(_) | Self::RotateBitmap(_) => Some(1),
            Self::InitializeMint(_) | Self::Mint(_) => None,
        }
    }

    /// Get optional compute budget for this transaction type (in compute units)
    /// Returns None if default compute budget (200k CU) is sufficient
    pub fn compute_budget(&self) -> Option<u32> {
        match self {
            Self::ReleaseFunds(_) => DEFAULT_CU_RELEASE_FUNDS,
            Self::InitializeMint(_) | Self::Mint(_) | Self::RotateBitmap(_) => DEFAULT_CU_MINT,
        }
    }

    pub fn signers(&self) -> Vec<&'static Signer> {
        match self {
            Self::ReleaseFunds(_) | Self::RotateBitmap(_) => {
                vec![SignerUtil::admin_signer(), SignerUtil::operator_signer()]
            }
            Self::InitializeMint(_) | Self::Mint(_) => vec![SignerUtil::admin_signer()],
        }
    }

    /// Get the database transaction ID for storage/logging operations
    /// Returns the DB id for all transaction types with a DB record
    pub fn transaction_id(&self) -> Option<i64> {
        match self {
            Self::ReleaseFunds(builder) => Some(builder.transaction_id),
            Self::Mint(builder) => Some(builder.txn_id),
            Self::InitializeMint(_) | Self::RotateBitmap(_) => None,
        }
    }

    pub fn trace_id(&self) -> Option<String> {
        match self {
            Self::ReleaseFunds(b) => Some(b.trace_id.clone()),
            Self::Mint(b) => Some(b.trace_id.clone()),
            Self::InitializeMint(_) | Self::RotateBitmap(_) => None,
        }
    }

    /// The fetch-time ownership token, present only for a deposit `Mint`. A
    /// release carries its own token on the builder and arms it as a lease at
    /// submission, so it is not reported here.
    pub fn fetched_updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Self::Mint(b) => Some(b.fetched_updated_at),
            Self::ReleaseFunds(_) | Self::InitializeMint(_) | Self::RotateBitmap(_) => None,
        }
    }

    pub fn withdrawal_nonce(&self) -> Option<u64> {
        match self {
            Self::ReleaseFunds(builder) => Some(builder.nonce),
            Self::InitializeMint(_) | Self::Mint(_) | Self::RotateBitmap(_) => None,
        }
    }

    /// Get retry policy for this transaction type
    ///
    /// # Retry Policies by Transaction Type
    /// - **InitializeMint**: Idempotent retry - Safe to retry if mint already initialized.
    /// - **Mint**: No sender-level retry - retries happen only after memo-based idempotency
    ///   verification to prevent duplicate issuance.
    /// - **ReleaseFunds**: Idempotent retry - Uses transaction nonce to prevent duplicates.
    ///   Safe to retry on transient network failures.
    /// - **RotateBitmap**: Idempotent retry - carries expected_generation, so a replay
    ///   after the first rotation lands is rejected on-chain (UnexpectedGeneration)
    ///   rather than skipping a whole generation of nonces.
    pub fn retry_policy(&self) -> RetryPolicy {
        match self {
            Self::Mint(_) => RetryPolicy::None,
            Self::ReleaseFunds(_) | Self::InitializeMint(_) | Self::RotateBitmap(_) => {
                RetryPolicy::Idempotent
            }
        }
    }

    pub fn extra_error_checks_policy(&self) -> ExtraErrorCheckPolicy {
        match self {
            Self::Mint(_) => mint_extra_error_checks_policy(),
            Self::InitializeMint(_) => {
                ExtraErrorCheckPolicy::Extra(vec![Box::new(is_mint_already_initialized_error)])
            }
            Self::ReleaseFunds(_) | Self::RotateBitmap(_) => ExtraErrorCheckPolicy::None,
        }
    }
}

// One source for the Mint error-check policy so every caller stays in sync.
// Rebuilt on demand because the policy holds boxed closures and is not Clone.
pub(crate) fn mint_extra_error_checks_policy() -> ExtraErrorCheckPolicy {
    ExtraErrorCheckPolicy::Extra(vec![Box::new(is_mint_not_initialized_error)])
}

#[derive(Clone, Debug)]
pub struct ReleaseFundsBuilderWithNonce {
    pub builder: ReleaseFundsBuilder,
    pub nonce: u64,
    pub transaction_id: i64,
    pub trace_id: String,
    pub remint_info: Option<WithdrawalRemintInfo>,
    /// The row's `updated_at` at fetch time. The sender CASes on it when it
    /// persists the write-ahead signature, proving the release still owns the
    /// same `Processing` incarnation it was handed.
    pub fetched_updated_at: chrono::DateTime<chrono::Utc>,
}

/// Builder for simple SPL token mint instructions (deposit flow)
/// Creates ATA idempotently, then mints tokens
#[derive(Clone, Debug, Default)]
pub struct MintToBuilder {
    mint: Option<Pubkey>,
    recipient: Option<Pubkey>,
    recipient_ata: Option<Pubkey>,
    payer: Option<Pubkey>,
    mint_authority: Option<Pubkey>,
    token_program: Option<Pubkey>,
    amount: Option<u64>,
    idempotency_memo: Option<String>,
}

impl MintToBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mint(&mut self, mint: Pubkey) -> &mut Self {
        self.mint = Some(mint);
        self
    }

    pub fn recipient(&mut self, recipient: Pubkey) -> &mut Self {
        self.recipient = Some(recipient);
        self
    }

    pub fn recipient_ata(&mut self, recipient_ata: Pubkey) -> &mut Self {
        self.recipient_ata = Some(recipient_ata);
        self
    }

    pub fn payer(&mut self, payer: Pubkey) -> &mut Self {
        self.payer = Some(payer);
        self
    }

    pub fn mint_authority(&mut self, mint_authority: Pubkey) -> &mut Self {
        self.mint_authority = Some(mint_authority);
        self
    }

    pub fn token_program(&mut self, token_program: Pubkey) -> &mut Self {
        self.token_program = Some(token_program);
        self
    }

    pub fn amount(&mut self, amount: u64) -> &mut Self {
        self.amount = Some(amount);
        self
    }

    pub fn idempotency_memo(&mut self, memo: String) -> &mut Self {
        self.idempotency_memo = Some(memo);
        self
    }

    pub fn get_mint(&self) -> Option<Pubkey> {
        self.mint
    }

    pub fn get_token_program(&self) -> Option<Pubkey> {
        self.token_program
    }

    pub fn get_payer(&self) -> Option<Pubkey> {
        self.payer
    }

    pub fn get_mint_authority(&self) -> Option<Pubkey> {
        self.mint_authority
    }

    pub fn get_amount(&self) -> Option<u64> {
        self.amount
    }

    pub fn get_recipient_ata(&self) -> Option<Pubkey> {
        self.recipient_ata
    }

    pub fn try_as_expected_mint(&self) -> Option<(Pubkey, Pubkey, Pubkey, Pubkey, u64)> {
        Some((
            self.mint?,
            self.recipient_ata?,
            self.mint_authority?,
            self.token_program?,
            self.amount?,
        ))
    }

    /// Returns instructions: [create_ata_idempotent, optional_memo, mint_to]
    pub fn instructions(&self) -> Result<Vec<Instruction>, crate::error::ProgramError> {
        let mint = self.mint.ok_or_else(|| ProgramError::InvalidBuilder {
            reason: "mint not set".to_string(),
        })?;
        let recipient = self.recipient.ok_or_else(|| ProgramError::InvalidBuilder {
            reason: "recipient not set".to_string(),
        })?;
        let payer = self.payer.ok_or_else(|| ProgramError::InvalidBuilder {
            reason: "payer not set".to_string(),
        })?;
        let token_program = self
            .token_program
            .ok_or_else(|| ProgramError::InvalidBuilder {
                reason: "token_program not set".to_string(),
            })?;

        let mut instructions = vec![
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &payer,
                &recipient,
                &mint,
                &token_program,
            ),
        ];

        if let Some(memo) = self.idempotency_memo.as_deref() {
            instructions.push(Instruction {
                program_id: spl_memo::id(),
                accounts: vec![AccountMeta::new_readonly(payer, true)],
                data: memo.as_bytes().to_vec(),
            });
        }

        instructions.push(self.instruction()?);

        Ok(instructions)
    }

    pub fn instruction(&self) -> Result<Instruction, crate::error::ProgramError> {
        mint_to(
            &self
                .token_program
                .ok_or_else(|| ProgramError::InvalidBuilder {
                    reason: "token_program not set".to_string(),
                })?,
            &self.mint.ok_or_else(|| ProgramError::InvalidBuilder {
                reason: "mint not set".to_string(),
            })?,
            &self
                .recipient_ata
                .ok_or_else(|| ProgramError::InvalidBuilder {
                    reason: "recipient_ata not set".to_string(),
                })?,
            &self
                .mint_authority
                .ok_or_else(|| ProgramError::InvalidBuilder {
                    reason: "mint_authority not set".to_string(),
                })?,
            &[],
            self.amount.ok_or_else(|| ProgramError::InvalidBuilder {
                reason: "amount not set".to_string(),
            })?,
        )
        .map_err(|e| ProgramError::InvalidBuilder {
            reason: format!("failed to build mint_to instruction: {}", e),
        })
    }
}

#[derive(Clone, Debug)]
pub struct MintToBuilderWithTxnId {
    pub builder: MintToBuilder,
    pub txn_id: i64,
    pub trace_id: String,
    /// The row's `updated_at` at fetch time, the ownership token the sender
    /// CASes on when it persists the write-ahead signature.
    pub fetched_updated_at: chrono::DateTime<chrono::Utc>,
}

/// Builder for initialize_mint instruction (sent before first mint)
#[derive(Clone, Debug)]
pub struct InitializeMintBuilder {
    pub mint: Pubkey,
    pub decimals: u8,
    pub mint_authority: Pubkey,
    pub token_program: Pubkey,
    pub payer: Pubkey,
}

impl InitializeMintBuilder {
    pub fn new(
        mint: Pubkey,
        decimals: u8,
        mint_authority: Pubkey,
        token_program: Pubkey,
        payer: Pubkey,
    ) -> Self {
        Self {
            mint,
            decimals,
            mint_authority,
            token_program,
            payer,
        }
    }

    pub fn instruction(&self) -> Result<Instruction, crate::error::ProgramError> {
        spl_token::instruction::initialize_mint(
            &self.token_program,
            &self.mint,
            &self.mint_authority,
            Some(&self.mint_authority),
            self.decimals,
        )
        .map_err(|e| ProgramError::InvalidBuilder {
            reason: format!("failed to build initialize_mint: {}", e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use private_channel_escrow_program_client::instructions::RotateBitmapBuilder;
    use solana_sdk::pubkey::Pubkey;

    fn pk(i: u8) -> Pubkey {
        let mut b = [0u8; 32];
        b[0] = i;
        Pubkey::new_from_array(b)
    }

    // ========================================================================
    // MintToBuilder
    // ========================================================================

    #[test]
    fn try_as_expected_mint_all_set() {
        let mut b = MintToBuilder::new();
        b.mint(pk(1))
            .recipient_ata(pk(3))
            .mint_authority(pk(5))
            .token_program(pk(6))
            .amount(100);
        let result = b.try_as_expected_mint();
        assert!(result.is_some());
        let (mint, ata, auth, tp, amt) = result.unwrap();
        assert_eq!(mint, pk(1));
        assert_eq!(ata, pk(3));
        assert_eq!(auth, pk(5));
        assert_eq!(tp, pk(6));
        assert_eq!(amt, 100);
    }

    #[test]
    fn try_as_expected_mint_missing_field() {
        let mut b = MintToBuilder::new();
        b.mint(pk(1)).recipient_ata(pk(3));
        // missing mint_authority, token_program, amount
        assert!(b.try_as_expected_mint().is_none());
    }

    fn fully_configured_builder() -> MintToBuilder {
        let mut b = MintToBuilder::new();
        b.mint(pk(1))
            .recipient(pk(2))
            .recipient_ata(pk(3))
            .payer(pk(4))
            .mint_authority(pk(5))
            .token_program(spl_token::id())
            .amount(500);
        b
    }

    #[test]
    fn instructions_with_memo_returns_3() {
        let mut b = fully_configured_builder();
        b.idempotency_memo("test:memo".to_string());
        let ixs = b.instructions().unwrap();
        assert_eq!(ixs.len(), 3);
        // first = create_ata, second = memo, third = mint_to
        assert_eq!(ixs[1].program_id, spl_memo::id());
    }

    #[test]
    fn instructions_without_memo_returns_2() {
        let b = fully_configured_builder();
        let ixs = b.instructions().unwrap();
        assert_eq!(ixs.len(), 2);
    }

    #[test]
    fn instructions_missing_required_field_errors() {
        let b = MintToBuilder::new(); // nothing set
        let result = b.instructions();
        assert!(result.is_err());
    }

    #[test]
    fn instruction_success() {
        let b = fully_configured_builder();
        let ix = b.instruction();
        assert!(ix.is_ok());
    }

    #[test]
    fn instruction_missing_mint_errors() {
        let mut b = MintToBuilder::new();
        b.recipient_ata(pk(3))
            .mint_authority(pk(5))
            .token_program(spl_token::id())
            .amount(500);
        let result = b.instruction();
        assert!(result.is_err());
    }

    // ========================================================================
    // InitializeMintBuilder
    // ========================================================================

    #[test]
    fn initialize_mint_builder_instruction_ok() {
        let builder = InitializeMintBuilder::new(pk(1), 6, pk(2), spl_token::id(), pk(3));
        let ix = builder.instruction();
        assert!(ix.is_ok());
    }

    // ========================================================================
    // TransactionBuilder enum methods
    // ========================================================================

    fn make_release_funds_builder() -> TransactionBuilder {
        let mut inner = ReleaseFundsBuilder::new();
        inner
            .payer(pk(1))
            .operator(pk(2))
            .instance(pk(3))
            .operator_pda(pk(4))
            .mint(pk(5))
            .allowed_mint(pk(6))
            .user_ata(pk(7))
            .instance_ata(pk(8))
            .token_program(spl_token::id())
            .associated_token_program(spl_associated_token_account::id())
            .event_authority(pk(10))
            .private_channel_escrow_program(pk(11))
            .amount(100)
            .user(pk(12))
            .withdrawal_bitmap(pk(13))
            .transaction_nonce(42);
        TransactionBuilder::ReleaseFunds(Box::new(ReleaseFundsBuilderWithNonce {
            builder: inner.clone(),
            nonce: 42,
            transaction_id: 7,
            trace_id: "trace-rf".to_string(),
            remint_info: None,
            fetched_updated_at: chrono::Utc::now(),
        }))
    }

    fn make_mint_builder() -> TransactionBuilder {
        TransactionBuilder::Mint(Box::new(MintToBuilderWithTxnId {
            builder: fully_configured_builder(),
            txn_id: 10,
            trace_id: "trace-mint".to_string(),
            fetched_updated_at: chrono::Utc::now(),
        }))
    }

    fn make_init_mint_builder() -> TransactionBuilder {
        TransactionBuilder::InitializeMint(Box::new(InitializeMintBuilder::new(
            pk(1),
            6,
            pk(2),
            spl_token::id(),
            pk(3),
        )))
    }

    fn make_rotate_bitmap_builder() -> TransactionBuilder {
        let mut inner = RotateBitmapBuilder::new();
        inner
            .payer(pk(1))
            .operator(pk(2))
            .instance(pk(3))
            .withdrawal_bitmap(pk(4))
            .operator_pda(pk(5))
            .event_authority(pk(6))
            .private_channel_escrow_program(pk(7))
            .expected_generation(0);
        TransactionBuilder::RotateBitmap(Box::new(inner.clone()))
    }

    #[test]
    fn compute_unit_price_per_variant() {
        assert_eq!(make_release_funds_builder().compute_unit_price(), Some(1));
        assert_eq!(make_init_mint_builder().compute_unit_price(), None);
        assert_eq!(make_mint_builder().compute_unit_price(), None);
        assert_eq!(make_rotate_bitmap_builder().compute_unit_price(), Some(1));
    }

    #[test]
    fn compute_budget_per_variant() {
        assert_eq!(
            make_release_funds_builder().compute_budget(),
            DEFAULT_CU_RELEASE_FUNDS
        );
        assert_eq!(make_init_mint_builder().compute_budget(), DEFAULT_CU_MINT);
        assert_eq!(make_mint_builder().compute_budget(), DEFAULT_CU_MINT);
        assert_eq!(
            make_rotate_bitmap_builder().compute_budget(),
            DEFAULT_CU_MINT
        );
    }

    #[test]
    fn transaction_id_per_variant() {
        assert_eq!(make_release_funds_builder().transaction_id(), Some(7));
        assert_eq!(make_init_mint_builder().transaction_id(), None);
        assert_eq!(make_mint_builder().transaction_id(), Some(10));
        assert_eq!(make_rotate_bitmap_builder().transaction_id(), None);
    }

    #[test]
    fn trace_id_per_variant() {
        assert_eq!(
            make_release_funds_builder().trace_id(),
            Some("trace-rf".to_string())
        );
        assert_eq!(make_init_mint_builder().trace_id(), None);
        assert_eq!(
            make_mint_builder().trace_id(),
            Some("trace-mint".to_string())
        );
        assert_eq!(make_rotate_bitmap_builder().trace_id(), None);
    }

    #[test]
    fn withdrawal_nonce_per_variant() {
        assert_eq!(make_release_funds_builder().withdrawal_nonce(), Some(42));
        assert_eq!(make_init_mint_builder().withdrawal_nonce(), None);
        assert_eq!(make_mint_builder().withdrawal_nonce(), None);
        assert_eq!(make_rotate_bitmap_builder().withdrawal_nonce(), None);
    }

    #[test]
    fn retry_policy_per_variant() {
        assert!(matches!(
            make_release_funds_builder().retry_policy(),
            RetryPolicy::Idempotent
        ));
        assert!(matches!(
            make_init_mint_builder().retry_policy(),
            RetryPolicy::Idempotent
        ));
        assert!(matches!(
            make_mint_builder().retry_policy(),
            RetryPolicy::None
        ));
        assert!(matches!(
            make_rotate_bitmap_builder().retry_policy(),
            RetryPolicy::Idempotent
        ));
    }

    #[test]
    fn extra_error_checks_policy_per_variant() {
        assert!(matches!(
            make_release_funds_builder().extra_error_checks_policy(),
            ExtraErrorCheckPolicy::None
        ));
        assert!(matches!(
            make_init_mint_builder().extra_error_checks_policy(),
            ExtraErrorCheckPolicy::Extra(_)
        ));
        assert!(matches!(
            make_mint_builder().extra_error_checks_policy(),
            ExtraErrorCheckPolicy::Extra(_)
        ));
        assert!(matches!(
            make_rotate_bitmap_builder().extra_error_checks_policy(),
            ExtraErrorCheckPolicy::None
        ));
    }

    #[test]
    fn remint_idempotency_memo_format() {
        assert_eq!(
            remint_idempotency_memo(99_i64),
            "private_channel:remint:99".to_string()
        );
    }
}
