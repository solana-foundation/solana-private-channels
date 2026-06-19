use crate::storage::common::amount::TokenAmount;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;
use uuid::Uuid;

/// Type of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "transaction_type", rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
}

/// Indexer state for checkpoint tracking
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct IndexerState {
    pub program_type: String,
    pub last_seen_slot: i64,
    pub updated_at: DateTime<Utc>,
}

/// Status of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "transaction_status", rename_all = "lowercase")]
pub enum TransactionStatus {
    // Hasn't been picked up for processing by an Operator yet
    Pending,
    // Currently processing by an Operator
    Processing,
    // Withdrawal whose proof generation is blocked by an unresolved ambiguous
    // nonce in the same tree. Never broadcast on-chain. Lives only while the
    // sender owns it in memory; recovery requeues it if a restart orphans it.
    Parked,
    Completed,
    Failed,
    // Withdrawal failed permanently but burned PrivateChannel tokens were reminted back to user
    #[sqlx(rename = "failed_reminted")]
    FailedReminted,
    // Remint attempted but could not confirm — requires manual investigation
    #[sqlx(rename = "manual_review")]
    ManualReview,
    // Withdrawal failed permanently; remint is queued and will be attempted after
    // the finality window. Transitions to FailedReminted, ManualReview, or
    // Completed (if original withdrawal actually landed).
    #[sqlx(rename = "pending_remint")]
    PendingRemint,
}

/// DbTransaction domain model
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[sqlx(type_name = "transaction", rename_all = "lowercase")]
pub struct DbTransaction {
    pub id: i64,
    pub signature: String,
    pub trace_id: String,
    pub slot: i64,
    pub initiator: String,
    pub recipient: String,
    pub mint: String,
    pub amount: TokenAmount,
    pub memo: Option<String>,
    pub transaction_type: TransactionType,
    pub withdrawal_nonce: Option<i64>,
    pub status: TransactionStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    // If this is a deposit from Solana to PrivateChannel, this will represent the PrivateChannel signature and
    // if this is a withdrawal from PrivateChannel to Solana, this will represent the Solana signature
    pub counterpart_signature: Option<String>,
    /// Withdrawal signatures sent to Solana, stored when status transitions to
    /// PendingRemint. Used on restart to verify whether the original withdrawal
    /// finalized before attempting to remint.
    pub remint_signatures: Option<Vec<String>>,
    /// `last_valid_block_height` per stored signature, index-paired with `remint_signatures`.
    pub remint_last_valid_block_heights: Option<Vec<i64>>,
    /// UTC timestamp of when the finality check deadline expires for PendingRemint transactions.
    /// Set when transitioning to PendingRemint, used to restore the exact wait time on restart.
    pub pending_remint_deadline_at: Option<DateTime<Utc>>,
    /// Persisted defer counter for PendingRemint rows. Each finality-check
    /// failure or liveness deferral bumps this; restoring it on restart is
    /// what keeps the `MAX_FINALITY_CHECK_ATTEMPTS` budget honest across
    /// crashes. Non-PendingRemint rows always carry 0 (column DEFAULT).
    pub finality_check_attempts: i32,
    /// Durable recovery requeue counter. Each `Demote` (stale
    /// `processing` -> `pending`) bumps this; the cap quarantines a row that
    /// can never progress instead of looping forever.
    pub recovery_requeue_attempts: i32,
    /// Absolute position of the source instruction within its transaction.
    /// Paired with `signature` it is the row's durable identity, so multiple
    /// economic events in one transaction persist as distinct rows.
    pub instruction_index: i32,
    /// Position in the top-level ancestor's *flattened* inner-instruction list
    /// for a CPI; `None` (NULL) for a top-level instruction. The validator
    /// flattens every CPI depth into one list, so this is unique at any depth (not
    /// a nesting level). Completes the `(signature, instruction_index, inner_index)`
    /// identity so a CPI deposit/withdraw never collides with its parent's row.
    pub inner_index: Option<i32>,
    /// Confirmed remint signature, written in the same UPDATE that flips status
    /// to FailedReminted. A crash before the async writer runs can no longer
    /// leave a landed remint recorded only as PendingRemint (which would replay).
    pub landed_remint_signature: Option<String>,
}

/// Per-mint balance aggregate used during startup reconciliation.
/// Returned by the reconciliation storage query.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MintDbBalance {
    pub mint_address: String,
    pub token_program: String,
    /// Sum of amounts for all indexed deposits (any status).
    /// Deposits increase the on-chain ATA balance the moment they are observed,
    /// regardless of whether the operator has completed the corresponding private_channel mint.
    /// Held as `BigDecimal` because the gross sum of many near-`u64::MAX` amounts can
    /// exceed `u64::MAX` even though the net (deposits - withdrawals) cannot.
    pub total_deposits: BigDecimal,
    /// Sum of amounts for completed withdrawals only.
    /// Only a completed `release_funds` call actually reduces the on-chain ATA balance.
    pub total_withdrawals: BigDecimal,
}

/// Mint metadata stored
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DbMint {
    pub mint_address: String,
    pub decimals: i16,
    pub token_program: String,
    /// Current allow/block state (`"allowed"` | `"blocked"`)
    pub status: String,
    pub created_at: DateTime<Utc>,
    /// `None` = the on-chain PausableConfig extension state is unknown to us yet.
    /// Resolved lazily by the operator's MintCache on first RPC fetch.
    pub is_pausable: Option<bool>,
    /// `None` = the on-chain PermanentDelegate extension state is unknown to us yet.
    /// Resolved lazily alongside `is_pausable` in a single RPC fetch.
    pub has_permanent_delegate: Option<bool>,
}

impl DbMint {
    pub fn new(mint_address: String, decimals: i16, token_program: String) -> Self {
        Self {
            mint_address,
            decimals,
            token_program,
            // A DbMint is only ever constructed on the allow path.
            status: "allowed".to_string(),
            created_at: Utc::now(),
            is_pausable: None,
            has_permanent_delegate: None,
        }
    }
}

/// Status transition row recording an Allow/Block decision for a mint at a slot.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbMintStatus {
    pub mint_address: String,
    pub status: String,
    pub effective_slot: i64,
    pub signature: String,
    pub created_at: DateTime<Utc>,
}

/// Resolved mint status at a particular slot, derived from `mint_status_history`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MintStatusAtSlot {
    Allowed,
    Blocked,
    NeverAllowed,
}

/// Builder for DbTransaction
pub struct DbTransactionBuilder {
    signature: String,
    slot: i64,
    mint: String,
    amount: TokenAmount,
    initiator: Option<String>,
    recipient: Option<String>,
    memo: Option<String>,
    transaction_type: Option<TransactionType>,
    trace_id: Option<String>,
    instruction_index: i32,
    inner_index: Option<i32>,
}

impl DbTransactionBuilder {
    pub fn new(signature: String, slot: u64, mint: String, amount: u64) -> Self {
        Self {
            signature,
            slot: slot as i64,
            mint,
            amount: TokenAmount(amount),
            initiator: None,
            recipient: None,
            memo: None,
            transaction_type: None,
            trace_id: None,
            instruction_index: 0,
            inner_index: None,
        }
    }

    pub fn initiator(mut self, initiator: String) -> Self {
        self.initiator = Some(initiator);
        self
    }

    pub fn recipient(mut self, recipient: String) -> Self {
        self.recipient = Some(recipient);
        self
    }

    pub fn memo(mut self, memo: Option<String>) -> Self {
        self.memo = memo;
        self
    }

    pub fn transaction_type(mut self, transaction_type: TransactionType) -> Self {
        self.transaction_type = Some(transaction_type);
        self
    }

    pub fn trace_id(mut self, trace_id: String) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    pub fn instruction_index(mut self, instruction_index: i32) -> Self {
        self.instruction_index = instruction_index;
        self
    }

    pub fn inner_index(mut self, inner_index: Option<i32>) -> Self {
        self.inner_index = inner_index;
        self
    }

    pub fn build(self) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id: 0,
            signature: self.signature,
            trace_id: self.trace_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            slot: self.slot,
            initiator: self.initiator.expect("initiator is required"),
            recipient: self.recipient.expect("recipient is required"),
            mint: self.mint,
            amount: self.amount,
            memo: self.memo,
            transaction_type: self.transaction_type.expect("transaction_type is required"),
            withdrawal_nonce: None,
            status: TransactionStatus::Pending,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: self.instruction_index,
            inner_index: self.inner_index,
            landed_remint_signature: None,
        }
    }
}
