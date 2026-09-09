use crate::config::ProgramType;
use crate::operator::sender::remint::FinalityRpc;
use crate::operator::utils::instruction_util::{
    ExtraErrorCheckPolicy, MintToBuilder, RetryPolicy, TransactionKind, WithdrawalRemintInfo,
};
use crate::operator::MintCache;
use crate::operator::RpcClientWithRetry;
use crate::storage::common::models::TransactionStatus;
use crate::storage::common::storage::Storage;
use chrono::{DateTime, Utc};
use private_channel_escrow_program_client::instructions::RotateBitmapBuilder;
use solana_keychain::Signer;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum number of fire-and-forget transactions allowed in-flight simultaneously.
/// New sends are rejected with a permanent failure once this cap is reached,
/// preventing unbounded memory growth under sustained load.
pub const MAX_IN_FLIGHT: usize = 1000;

/// Shared queue of in-flight fire-and-forget transactions.
///
/// Owned jointly by the sender loop (pushes new entries, reads routing results)
/// and the poll task (drains entries for `getSignatureStatuses`, puts unconfirmed
/// back).  The `notify` wakes the poll task whenever a new entry is pushed so it
/// never busy-loops when the queue is empty.
pub struct InFlightQueue {
    pub entries: std::sync::Mutex<Vec<InFlightTx>>,
    pub notify: tokio::sync::Notify,
}

impl InFlightQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: std::sync::Mutex::new(Vec::with_capacity(MAX_IN_FLIGHT)),
            notify: tokio::sync::Notify::new(),
        })
    }

    /// Push an entry and wake the poll task.
    pub fn push(&self, tx: InFlightTx) {
        self.entries.lock().unwrap().push(tx);
        self.notify.notify_one();
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }

    /// Take all entries out atomically, leaving a pre-allocated buffer in place
    /// so the next cycle does not need to reallocate.
    pub fn drain_all(&self) -> Vec<InFlightTx> {
        let mut guard = self.entries.lock().unwrap();
        let mut out = Vec::with_capacity(guard.capacity());
        std::mem::swap(&mut *guard, &mut out);
        out
    }

    /// Re-insert a batch of entries with a single mutex lock and one notify.
    pub fn push_all(&self, txs: Vec<InFlightTx>) {
        if txs.is_empty() {
            return;
        }
        self.entries.lock().unwrap().extend(txs);
        self.notify.notify_one();
    }
}

#[derive(Clone, Debug)]
pub struct TransactionContext {
    pub transaction_id: Option<i64>,
    pub withdrawal_nonce: Option<u64>,
    pub trace_id: Option<String>,
    /// What this transaction is; an InitializeMint and a rotation carry identical empty ids.
    pub kind: TransactionKind,
    /// Ownership lease from this deposit's most recent successful claim (the
    /// row's `updated_at`). A re-fire of the same deposit presents it again.
    pub deposit_claim_lease: Option<DateTime<Utc>>,
}

/// How a fire-and-store send is handled, decided by whether it carries user value.
///
/// `Recoverable` (a user `Mint`) claims the row and persists the signature before
/// broadcasting, and every pre-broadcast failure leaves the row Processing so
/// recovery re-mints it. `Terminal` (`InitializeMint`) mints no balance and is
/// on-chain idempotent, so it journals nothing and fails fast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendDurability {
    Recoverable {
        deposit_expected_updated_at: DateTime<Utc>,
    },
    Terminal,
}

/// Transaction status update to send to storage
#[derive(Debug, Clone)]
pub struct TransactionStatusUpdate {
    pub transaction_id: i64,
    pub trace_id: Option<String>,
    pub status: TransactionStatus,
    pub counterpart_signature: Option<String>,
    pub processed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    /// Signature of the remint transaction (only set for FailedReminted status)
    pub remint_signature: Option<String>,
    /// True when a remint was attempted but failed (ManualReview). Lets consumers
    /// distinguish "remint tried and failed" from "remint never attempted".
    pub remint_attempted: bool,
}

/// A Mint or InitializeMint transaction that has been sent but not yet confirmed.
///
/// Stored in `SenderState::in_flight` and checked in batch on each confirmation timer
/// tick via a single `getSignatureStatuses` RPC call.  Decoupling send from confirm
/// allows the sender to process new transactions while waiting for on-chain confirmation.
///
/// Only Mint and InitializeMint are eligible. ReleaseFunds and RotateBitmap still use
/// the blocking `send_and_confirm` path so that at most one withdrawal is in flight
/// when a rotation is waiting to clear the bits.
pub struct InFlightTx {
    /// Signature returned by `sendTransaction`. Used as the polling key.
    pub signature: Signature,
    /// Routing context for storage updates on success or failure.
    pub ctx: TransactionContext,
    /// Original instruction kept for re-sign + re-send on confirmation timeout.
    pub instruction: InstructionWithSigners,
    /// Compute unit price forwarded to the re-send path unchanged.
    pub compute_unit_price: Option<u64>,
    /// Whether this tx can be safely re-sent (Idempotent) or must fail (None).
    pub retry_policy: RetryPolicy,
    /// On-chain error checks applied when the tx is confirmed with an error
    /// (e.g. MintNotInitialized detection for Mint transactions).
    pub extra_error_checks_policy: ExtraErrorCheckPolicy,
    /// Number of timer ticks elapsed since the send without a confirmed status.
    /// When this reaches `MAX_POLL_ATTEMPTS_CONFIRMATION` the tx is either re-sent
    /// (Idempotent) or declared a permanent failure (None).
    pub poll_attempts: u32,
    /// Number of times this transaction has been re-signed and re-sent after a
    /// confirmation timeout.  Compared against `SenderState::retry_max_attempts`
    /// before each re-send; once the cap is reached even an Idempotent tx is
    /// declared a permanent failure rather than looping forever.
    pub resend_count: u32,
    /// Whether this transaction's signature was persisted write-ahead before broadcast.
    /// When true, an uncertain terminal outcome (send error or confirmation timeout)
    /// leaves the row Processing for recovery instead of writing Failed.
    pub persisted: bool,
    /// Semaphore permit held for the lifetime of this entry.
    ///
    /// Acquired before spawning the send task; dropped when the entry is removed
    /// from the queue (on confirmation, permanent failure, or transfer to a retry).
    /// This is the sole mechanism that enforces `MAX_IN_FLIGHT` across both
    /// in-flight entries and send tasks that have not yet pushed to the queue.
    pub permit: OwnedSemaphorePermit,
}

/// Sender state tracking in-flight work and deferred remints
pub struct SenderState {
    pub rpc_client: Arc<RpcClientWithRetry>,
    /// Source chain RPC: PrivateChannel for the withdraw operator, where the
    /// burn happened. Remints broadcast here to restore the burned balance.
    /// rpc_client is the destination chain (Solana) for ReleaseFunds.
    pub source_rpc_client: Arc<RpcClientWithRetry>,
    /// Independent destination-chain endpoint that re-checks a Dead verdict.
    /// One node's missing status can be a prune rather than proof of absence.
    pub fallback_rpc_client: Option<Arc<RpcClientWithRetry>>,
    pub storage: Arc<Storage>,
    pub instance_pda: Option<Pubkey>,
    /// Withdrawal nonces broadcast but not yet settled. The rotation barrier reads
    /// this: clearing the bits while a release is in flight would let its nonce be
    /// replayed in the next generation.
    pub in_flight_withdrawals: HashSet<u64>,
    /// The generation the bitmap was last known to be on, or `None` for unknown.
    ///
    /// Only ever written from a value the chain itself reported, so it can lag
    /// the chain but never lead it. It is trusted only when it permits a send;
    /// every refusal is taken against a fresh read instead, which is what keeps
    /// a stale entry from stranding a releasable withdrawal.
    pub cached_generation: Option<u64>,
    pub retry_counts: HashMap<u64, u32>,
    /// Attempts spent on the rotation in hand; one counter suffices because at most one is ever in flight.
    pub rotation_retry_attempts: u32,
    /// The rotation last dispatched, kept because nothing else can re-dispatch one that failed.
    pub rotation_in_flight: Option<Box<RotateBitmapBuilder>>,
    /// The generation that rotation was bound to. It survives a re-arm on
    /// purpose: rebinding a rotation that already landed would make the replay
    /// look legitimate and close a generation nobody ever opened.
    pub rotation_bound_generation: Option<u64>,
    /// Times that rotation has been put back on the tick, so a hopeless one stops being retried.
    pub rotation_rearm_attempts: u32,
    /// Consecutive gate passes that withheld a rotation. Crossing a boundary
    /// looks blocked for a moment even when nothing is wrong, so the report
    /// waits for the count rather than firing on the first pass.
    pub rotation_blocked_passes: u32,
    pub mint_builders: HashMap<i64, MintToBuilder>,
    pub mint_cache: MintCache,
    pub retry_max_attempts: u32,
    /// Milliseconds between `getSignatureStatuses` polls. Populated from `OperatorConfig`.
    pub confirmation_poll_interval_ms: u64,
    /// Withdrawals the program refused because their generation had not opened
    /// yet. The instruction is kept whole rather than the builder: nothing in it
    /// depends on the generation, so a rotation makes the same bytes valid.
    pub rotation_retry_queue: Vec<(TransactionContext, InstructionWithSigners)>,
    /// Pending RotateBitmap transaction waiting for in-flight txs to settle
    pub pending_rotation: Option<Box<RotateBitmapBuilder>>,
    pub program_type: ProgramType,
    /// Cached remint info for withdrawal transactions, keyed by nonce.
    /// Extracted before cleanup_failed_transaction drops the nonce's caches.
    pub remint_cache: HashMap<u64, WithdrawalRemintInfo>,
    /// Signatures sent per withdrawal nonce (with lvbh), used for finality checks before reminting.
    pub pending_signatures: HashMap<u64, Vec<PendingSig>>,
    /// Ownership lease per withdrawal nonce: the row's `updated_at` as of this
    /// sender's most recent successful claim. Every release attempt presents the
    /// lease and adopts the one the claim returns.
    pub release_leases: HashMap<u64, DateTime<Utc>>,
    /// Deferred remint queue — entries are processed after their deadline matures.
    pub pending_remints: Vec<PendingRemint>,
    /// Mint/InitializeMint transactions sent but awaiting on-chain confirmation.
    /// Shared with the dedicated poll task via `Arc`; capped at `MAX_IN_FLIGHT`.
    pub in_flight: Arc<InFlightQueue>,
    /// Enforces the `MAX_IN_FLIGHT` cap across both entries in `in_flight` and
    /// in-progress spawned send tasks.  A permit is acquired before spawning a
    /// send task and released only when the entry reaches a terminal state
    /// (confirmed, permanent failure, or transfer to a retry), so
    /// `available_permits()` accurately reflects remaining capacity at all times.
    pub semaphore: Arc<Semaphore>,
}

impl SenderState {
    /// Finality oracle for the destination `rpc_client`, carrying the optional
    /// fallback used to re-check a `Dead` verdict (the prunable Solana path).
    ///
    /// Which chain `rpc_client` points at follows the role, not the field name:
    /// a withdraw operator releases on Solana, an escrow operator mints on the
    /// channel. A wrong tag reads an lvbh against the wrong height scale.
    pub(crate) fn dest_finality(&self) -> FinalityRpc<'_> {
        let fallback = self.fallback_rpc_client.as_deref();
        match self.program_type {
            ProgramType::Withdraw => FinalityRpc::solana(&self.rpc_client, fallback),
            ProgramType::Escrow => FinalityRpc::channel(&self.rpc_client, fallback),
        }
    }

    /// Finality oracle for `source_rpc_client`, single-endpoint: neither chain
    /// has a second node configured for this role's source. The ledger-floor
    /// check is therefore the whole protection on this path.
    pub(crate) fn source_finality(&self) -> FinalityRpc<'_> {
        match self.program_type {
            ProgramType::Withdraw => FinalityRpc::channel(&self.source_rpc_client, None),
            ProgramType::Escrow => FinalityRpc::solana(&self.source_rpc_client, None),
        }
    }
}

/// Withdrawal signature, its blockhash's `last_valid_block_height`, and the slot
/// that blockhash was read at, so the remint gate can prove both that the
/// signature can no longer land and that the endpoint still retains its window.
#[derive(Debug, Clone, Copy)]
pub struct PendingSig {
    pub signature: Signature,
    pub last_valid_block_height: u64,
    /// `None` for an attempt journaled before the column existed.
    pub blockhash_slot: Option<u64>,
}

/// A remint deferred until Solana finality window passes, allowing us to verify
/// that the original withdrawal definitively did not land before reminting.
pub struct PendingRemint {
    pub ctx: TransactionContext,
    pub remint_info: WithdrawalRemintInfo,
    pub signatures: Vec<PendingSig>,
    pub original_error: String,
    /// UTC timestamp after which the finality check runs. Using DateTime<Utc> instead of
    /// Instant allows the deadline to be persisted to the database and restored on restart.
    /// The minor risk of clock skew affecting a 32-second window is acceptable — the       
    /// finality check runs regardless, so a slightly early or late execution is safe.
    pub deadline: DateTime<Utc>,
    /// Number of times the finality check has been retried (e.g. due to RPC errors).
    pub finality_check_attempts: u32,
    /// Set when the program itself refused this nonce's release.
    ///
    /// That refusal is direct proof no payout occurred, and the only evidence
    /// that outlives a rotation. It is persisted with the entry and restored
    /// with it, so a restart inside the finality window still lets the refund
    /// go through instead of falling back to manual review.
    pub release_refused_on_chain: bool,
    /// The last slot a release for this nonce could have landed in, read once.
    ///
    /// The indexer's checkpoint has to reach this before an absent release
    /// record proves anything. Re-reading it each tick would move the target
    /// the checkpoint is chasing, so it is captured on the first check and
    /// kept. A restart re-captures a later slot, which only asks for more
    /// coverage than before, never less.
    pub coverage_slot: Option<u64>,
}

/// Result item sent from the dedicated poll task back to the sender loop.
///
/// The poll task handles confirmed-success entries directly — it fires the storage
/// update and increments the metric — so the common path never wakes the main
/// select loop.  Only rare outcomes (on-chain errors, confirmation timeouts) are
/// sent back for routing through `SenderState`.
pub enum PollTaskResult {
    /// Mint/InitializeMint confirmed with no error.  The poll task already sent the
    /// `Completed` status update and incremented `OPERATOR_MINTS_SENT`.
    /// The sender loop removes the builder from `SenderState::mint_builders`.
    /// `None` for `InitializeMint` which has no `transaction_id`.
    ConfirmedSuccess(Option<i64>),
    /// Needs routing via `SenderState` — either an on-chain error was returned
    /// or the entry timed out.  `None` status means timeout (not an RPC null).
    NeedsRouting(
        Box<InFlightTx>,
        Option<solana_transaction_status::TransactionStatus>,
    ),
}

#[derive(Clone)]
pub struct InstructionWithSigners {
    pub instructions: Vec<solana_sdk::instruction::Instruction>,
    pub fee_payer: Pubkey,
    pub signers: Vec<&'static Signer>,
    pub compute_unit_price: Option<u64>,
    pub compute_budget: Option<u32>,
}
