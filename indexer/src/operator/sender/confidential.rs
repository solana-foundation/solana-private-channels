//! Confidential releases, applies, AES repairs and rebalances, all run by the one sender
//! so they never race each other. Every balance the sender spends is first confirmed by
//! the opening check against an account read at confirmed commitment, no older than its
//! own last transaction.

use super::transaction::{
    claim_and_persist_or_abort, defer_remint_after_failure, handle_nonce_already_used,
    handle_nonce_outside_generation, handle_success, refund_failure_journaled_first,
    send_fenced_outcome, SignatureClaim,
};
use super::types::{InstructionWithSigners, PendingSig, SenderState, TransactionContext};
use crate::metrics;
use crate::operator::confidential_constants::*;
use crate::operator::recovery::{load_pending_sigs, RECOVERY_INTERVAL};
use crate::operator::utils::instruction_util::{
    apply_pending_balance_instruction, move_to_confidential_instruction,
    move_to_public_instructions, ConfidentialReleaseRequest, EscrowConfidentialAccounts,
    TransactionBuilder, TransactionKind,
};
use crate::operator::utils::mint_util::{
    decode_confidential_account, destination_readiness, read_confidential_mint_params,
    ConfidentialAccountRead, ConfidentialAccountView, ConfidentialMintParams, DestinationReadiness,
};
use crate::operator::utils::transaction_util::{
    build_and_sign, build_and_sign_v1, escrow_error_for_code, failing_program, resign_v1,
    send_signed, V1BuildError, V1Error,
};
use crate::operator::TransactionStatusUpdate;
use crate::operator::{
    find_event_authority_pda, find_operator_pda, RetryPolicy, RpcClientWithRetry, SignerUtil,
    MAX_POLL_ATTEMPTS_CONFIRMATION,
};
use crate::storage::common::models::TransactionStatus;
use crate::storage::common::storage::confidential_sender::{
    attempt_status, rebalance_status, OperatorAttempt, PendingInputs, RebalanceDirection,
    RebalanceRequest,
};
use crate::storage::Storage;
use chrono::{DateTime, Utc};
use private_channel_confidential::{Ciphertext, Keys, PendingError, ProofError};
use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
use private_channel_escrow_program_client::programs::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_metrics::MetricLabel;
use solana_commitment_config::CommitmentConfig;
use solana_keychain::SolanaSigner;
use solana_sdk::instruction::InstructionError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------------------

/// What the sender remembers per escrow mint. In memory only: every value is re-verified
/// by the opening check before use, so losing it on restart costs a candidate, not safety.
#[derive(Debug, Default)]
pub struct MintBook {
    /// Candidate (a): the last verified available minus the releases confirmed since.
    pub local: Option<u64>,
    /// Candidate (d): the verified available before our last apply, and that apply.
    pub last_apply: Option<LastApply>,
    pub repair: RepairState,
    /// Set once a frozen escrow account was reported, so the hold is logged once.
    pub frozen_warned: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LastApply {
    pub prior_available: u64,
    pub signature: Signature,
    /// The pending the apply was built with, verified against the account it read.
    pub applied_pending: Option<u64>,
}

/// The confidential half of the withdraw sender.
pub struct ConfidentialSender {
    pub keys: Arc<Keys>,
    pub payer: Pubkey,
    pub operator: Pubkey,
    pub instance: Pubkey,
    pub books: HashMap<Pubkey, MintBook>,
    /// Floor for every account read: the landing slot of our last own transaction.
    pub min_context_slot: u64,
    /// Proof rebuilds left per nonce after Token-2022 refused stale proofs.
    pub stale_budget: HashMap<u64, u32>,
    /// Rows parked through the journal gate: once no attempt can land, park instead of remint.
    /// Mirrored on the row, so a restart during the gate's wait keeps the park.
    pub parks: HashMap<i64, String>,
    /// Requests waiting on a rotation, rebuilt with fresh proofs when the drain reaches them.
    pub requests: HashMap<u64, ConfidentialReleaseRequest>,
    /// Mint of each confidential release by nonce; only those still in flight count.
    pub release_mints: HashMap<u64, Pubkey>,
    /// Releases confirmed by this process whose row update may not be written yet.
    pub settled_releases: std::collections::HashSet<i64>,
    pub last_background_check: Option<Instant>,
    /// Releases with a journaled attempt held in the sender after a transient pre-send error.
    pub held: HashMap<u64, TransientHold>,
}

/// A release waiting in the sender to retry; `request` is taken while it runs.
pub struct TransientHold {
    pub since: DateTime<Utc>,
    pub next_at: DateTime<Utc>,
    /// Last time the row's lease was refreshed, so recovery never sees it as abandoned.
    pub refreshed_at: DateTime<Utc>,
    pub request: Option<ConfidentialReleaseRequest>,
}

impl ConfidentialSender {
    pub fn new(keys: Arc<Keys>, payer: Pubkey, operator: Pubkey, instance: Pubkey) -> Self {
        Self {
            keys,
            payer,
            operator,
            instance,
            books: HashMap::new(),
            min_context_slot: 0,
            stale_budget: HashMap::new(),
            parks: HashMap::new(),
            requests: HashMap::new(),
            release_mints: HashMap::new(),
            settled_releases: std::collections::HashSet::new(),
            last_background_check: None,
            held: HashMap::new(),
        }
    }

    /// From the environment; `None` when no keys are loaded, which leaves confidential
    /// releases parking at the pre-flight.
    pub fn from_env(
        instance: Pubkey,
    ) -> Result<Option<Self>, private_channel_confidential::KeysError> {
        Ok(Keys::from_env()?.map(|keys| {
            Self::new(
                Arc::new(keys),
                SignerUtil::get_admin_pubkey(),
                SignerUtil::get_operator_pubkey(),
                instance,
            )
        }))
    }

    pub fn accounts(&self, mint: &Pubkey) -> EscrowConfidentialAccounts {
        EscrowConfidentialAccounts {
            payer: self.payer,
            operator: self.operator,
            instance: self.instance,
            operator_pda: find_operator_pda(&self.instance, &self.operator),
            mint: *mint,
            instance_ata: escrow_ata(&self.instance, mint),
            event_authority: find_event_authority_pda(),
        }
    }

    fn observe_slot(&mut self, slot: u64) {
        self.min_context_slot = self.min_context_slot.max(slot);
    }
}

pub fn escrow_ata(instance: &Pubkey, mint: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(instance, mint, &spl_token_2022::ID)
}

// ---------------------------------------------------------------------------------------
// Available-balance candidates
// ---------------------------------------------------------------------------------------

/// Where a verified available balance came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateSource {
    /// (a) our own count.
    Local,
    /// (b) the AES copy.
    Aes,
    /// (d) our last apply plus the pending it applied.
    LastApply,
    /// The AES copy plus the decrypted gap to the ciphertext, used only when the copy is
    /// known stale and the apply that made it stale is not in memory (after a restart).
    StaleAesGap,
}

/// The first candidate the `available` ciphertext opens to, in order (a), (b), (d).
pub fn verify_available(
    keys: &Keys,
    view: &ConfidentialAccountView,
    local: Option<u64>,
    last_apply: Option<u64>,
) -> Option<(u64, CandidateSource)> {
    let aes = keys.decrypt_aes(&view.decryptable_available);
    [
        (local, CandidateSource::Local),
        (aes, CandidateSource::Aes),
        (last_apply, CandidateSource::LastApply),
    ]
    .into_iter()
    .find_map(|(value, source)| {
        value
            .filter(|value| keys.opens_to(&view.available, *value))
            .map(|value| (value, source))
    })
    .or_else(|| stale_aes_gap(keys, view).map(|value| (value, CandidateSource::StaleAesGap)))
}

/// Our last apply, as candidate (d) needs it: the available before it, what its event says
/// it applied, and the pending we verified before sending it.
pub type ApplyEvent = (u64, Ciphertext, Ciphertext, Option<u64>);

/// `verify_available` with the discrete logs on a blocking thread. (a) and (b) need none, so
/// (d) and the gap are only searched when neither opens.
pub async fn verify_available_off_executor(
    keys: Arc<Keys>,
    view: &ConfidentialAccountView,
    local: Option<u64>,
    apply: Option<ApplyEvent>,
) -> Option<(u64, CandidateSource)> {
    if let Some(found) = cheap_candidate(&keys, view, local) {
        return Some(found);
    }
    let view = view.clone();
    tokio::task::spawn_blocking(move || {
        let d = apply.and_then(|(prior, lo, hi, verified)| {
            candidate_from_apply(&keys, prior, &lo, &hi, verified)
        });
        verify_available(&keys, &view, None, d)
    })
    .await
    .ok()
    .flatten()
}

/// Candidates (a) then (b): an AES decryption and opening checks, no discrete log.
fn cheap_candidate(
    keys: &Keys,
    view: &ConfidentialAccountView,
    local: Option<u64>,
) -> Option<(u64, CandidateSource)> {
    let aes = keys.decrypt_aes(&view.decryptable_available);
    [(local, CandidateSource::Local), (aes, CandidateSource::Aes)]
        .into_iter()
        .find_map(|(value, source)| {
            value
                .filter(|value| keys.opens_to(&view.available, *value))
                .map(|value| (value, source))
        })
}

/// The pending balance, searched on a blocking thread.
pub async fn pending_off_executor(
    keys: Arc<Keys>,
    view: &ConfidentialAccountView,
) -> Result<u64, PendingError> {
    let (lo, hi) = (view.pending_lo, view.pending_hi);
    tokio::task::spawn_blocking(move || keys.decrypt_pending(&lo, &hi))
        .await
        .unwrap_or(Err(PendingError::Invalid))
}

/// `background_apply_due` on a blocking thread, since it may search pending.
pub async fn background_apply_due_off_executor(
    keys: Arc<Keys>,
    view: &ConfidentialAccountView,
    halted: bool,
    blocked: bool,
    release_in_flight: bool,
) -> Option<BackgroundApply> {
    let view = view.clone();
    tokio::task::spawn_blocking(move || {
        background_apply_due(&keys, &view, halted, blocked, release_in_flight)
    })
    .await
    .ok()
    .flatten()
}

/// Candidate (d): the available before our apply plus the pending it actually applied. When
/// the event's pending is past the search, the pending we verified before sending stands in;
/// the opening check on available still decides.
pub fn candidate_from_apply(
    keys: &Keys,
    prior_available: u64,
    applied_lo: &Ciphertext,
    applied_hi: &Ciphertext,
    verified_pending: Option<u64>,
) -> Option<u64> {
    let applied = match keys.decrypt_pending(applied_lo, applied_hi) {
        Ok(applied) => applied,
        Err(PendingError::HiTooLarge) => verified_pending?,
        Err(_) => return None,
    };
    prior_available.checked_add(applied)
}

/// Deposits in the last apply's own slot whose order against it is tried both ways.
const MAX_SAME_SLOT_DEPOSITS: usize = 10;

/// The pending `lo + 2^16 * hi` opens to, out of the indexed deposits and moves since the
/// last apply that landed at `boundary`. Deposits in that same slot may have landed before
/// or after it, so each split of them is tried. `None` when nothing opens.
pub fn indexed_pending(
    keys: &Keys,
    lo: &Ciphertext,
    hi: &Ciphertext,
    inputs: &PendingInputs,
    boundary: Option<u64>,
) -> Option<u64> {
    let mut certain = inputs
        .moves
        .iter()
        .try_fold(0u64, |sum, amount| sum.checked_add(*amount))?;
    let mut same_slot = Vec::new();
    for (slot, amount) in &inputs.deposits {
        let amount = (*amount)?;
        if Some(*slot) == boundary {
            same_slot.push(amount);
        } else {
            certain = certain.checked_add(amount)?;
        }
    }
    if same_slot.len() > MAX_SAME_SLOT_DEPOSITS {
        return None;
    }
    (0u32..1 << same_slot.len()).find_map(|split| {
        let candidate = same_slot
            .iter()
            .enumerate()
            .filter(|(i, _)| split & (1 << i) != 0)
            .try_fold(certain, |sum, (_, amount)| sum.checked_add(*amount))?;
        keys.pending_opens_to(lo, hi, candidate)
            .then_some(candidate)
    })
}

// A stale copy misses exactly the deposits that raced the apply, so the gap between the
// copy and the ciphertext is their sum and decrypts in the same bounded search as pending.
fn stale_aes_gap(keys: &Keys, view: &ConfidentialAccountView) -> Option<u64> {
    if !view.aes_known_stale() {
        return None;
    }
    let copy = keys.decrypt_aes(&view.decryptable_available)?;
    let gap = keys.decrypt_gap(&view.available, copy)?;
    let value = copy.checked_add(gap)?;
    keys.opens_to(&view.available, value).then_some(value)
}

/// The AES copy does not open to the ciphertext (a raced apply, or never written).
pub fn aes_is_stale(keys: &Keys, view: &ConfidentialAccountView) -> bool {
    keys.decrypt_aes(&view.decryptable_available)
        .is_none_or(|copy| !keys.opens_to(&view.available, copy))
}

// ---------------------------------------------------------------------------------------
// Waiting
// ---------------------------------------------------------------------------------------

/// When a release with no verified balance may be fetched again, and the wait's start.
/// The delay is the time already waited, clamped, so successive waits double to the cap.
pub fn next_wait(
    now: DateTime<Utc>,
    wait_started_at: Option<DateTime<Utc>>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let started = wait_started_at.unwrap_or(now);
    let waited = (now - started).to_std().unwrap_or_default();
    let delay = waited.clamp(WAIT_BACKOFF_BASE, WAIT_BACKOFF_CAP);
    (
        now + chrono::Duration::from_std(delay).expect("cap fits"),
        started,
    )
}

// ---------------------------------------------------------------------------------------
// Apply triggers
// ---------------------------------------------------------------------------------------

/// Release flow: apply first when the verified balance cannot cover the release and
/// pending holds something that could.
pub fn release_needs_apply(
    verified: Option<u64>,
    amount: u64,
    view: &ConfidentialAccountView,
) -> bool {
    view.has_pending() && verified.is_none_or(|available| available < amount)
}

/// Why the background check would apply, if it should.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackgroundApply {
    CounterHalfFull,
    PendingLarge,
}

/// Background check: apply only at half the credit cap or 2^47 unapplied pending, and
/// never while halted, blocked or with a release in flight.
pub fn background_apply_due(
    keys: &Keys,
    view: &ConfidentialAccountView,
    halted: bool,
    blocked: bool,
    release_in_flight: bool,
) -> Option<BackgroundApply> {
    if halted || blocked || release_in_flight || !view.has_pending() {
        return None;
    }
    if view.counter_at_percent(CREDIT_COUNTER_APPLY_PERCENT) {
        return Some(BackgroundApply::CounterHalfFull);
    }
    match keys.decrypt_pending(&view.pending_lo, &view.pending_hi) {
        Ok(pending) if pending >= PENDING_APPLY_THRESHOLD => Some(BackgroundApply::PendingLarge),
        // Past the wide search is far above the threshold.
        Err(PendingError::HiTooLarge) => Some(BackgroundApply::PendingLarge),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------
// AES repair
// ---------------------------------------------------------------------------------------

/// Repair budget and backoff for one mint's AES copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairState {
    /// Back-to-back repairs left before backing off.
    pub budget: u32,
    /// Set once the budget ran out; cleared when a repair or any write fixes the copy.
    pub deferred_until: Option<Instant>,
    pub backoff: Duration,
    pub alerted: bool,
}

impl Default for RepairState {
    fn default() -> Self {
        Self {
            budget: AES_REPAIR_BUDGET,
            deferred_until: None,
            backoff: WAIT_BACKOFF_BASE,
            alerted: false,
        }
    }
}

/// What to do after an apply when the copy is still stale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairNext {
    RepairNow,
    /// Out of budget: alert once (`alert` is true the first time) and wait for the backoff.
    Defer {
        alert: bool,
    },
}

impl RepairState {
    /// Any apply other than a repair starts a fresh budget.
    pub fn fresh_budget(&mut self) {
        self.budget = AES_REPAIR_BUDGET;
    }

    pub fn on_still_stale(&mut self, now: Instant) -> RepairNext {
        if self.budget > 0 {
            self.budget -= 1;
            return RepairNext::RepairNow;
        }
        self.deferred_until = Some(now + self.backoff);
        self.backoff = (self.backoff * 2).min(WAIT_BACKOFF_CAP);
        let alert = !self.alerted;
        self.alerted = true;
        RepairNext::Defer { alert }
    }

    /// The copy opens again; the episode is over.
    pub fn on_fixed(&mut self) {
        *self = Self::default();
    }
}

/// End `mint`'s stale episode: fresh repair state and the backoff alert cleared with it.
fn end_stale_episode(book: &mut MintBook, mint: &Pubkey) {
    book.repair.on_fixed();
    metrics::OPERATOR_CONFIDENTIAL_AES_REPAIR_DEFERRED
        .with_label_values(&[&mint.to_string()])
        .set(0.0);
}

impl RepairState {
    /// A deferred mint gets one repair per background check once its backoff passed.
    pub fn deferred_repair_due(&self, now: Instant) -> bool {
        self.deferred_until.is_some_and(|until| now >= until)
    }
}

// ---------------------------------------------------------------------------------------
// Failure classification
// ---------------------------------------------------------------------------------------

/// Token-2022 error codes a confidential release can fail with.
pub mod token_2022_codes {
    use spl_token_2022::error::TokenError;
    pub const ACCOUNT_FROZEN: u32 = TokenError::AccountFrozen as u32;
    pub const NOT_APPROVED: u32 = TokenError::ConfidentialTransferAccountNotApproved as u32;
    pub const CREDITS_DISABLED: u32 =
        TokenError::ConfidentialTransferDepositsAndTransfersDisabled as u32;
    pub const PUBKEY_MISMATCH: u32 = TokenError::ConfidentialTransferElGamalPubkeyMismatch as u32;
    pub const BALANCE_MISMATCH: u32 = TokenError::ConfidentialTransferBalanceMismatch as u32;
    pub const FEE_MISMATCH: u32 = TokenError::FeeMismatch as u32;
    pub const FEE_PARAMETERS_MISMATCH: u32 = TokenError::FeeParametersMismatch as u32;
    pub const COUNTER_FULL: u32 = TokenError::MaximumPendingBalanceCreditCounterExceeded as u32;
}

const ESCROW_CONFIDENTIAL_NOT_CONFIGURED: u32 = 22;

/// How a failed confidential release is routed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfidentialFailure {
    /// The proofs no longer match the account or the fee: rebuild on the stale budget.
    RebuildProofs,
    /// Nothing a resend changes; parks through the journal gate.
    Park(&'static str),
    /// An escrow error the public path already handles (nonce used, generation).
    Escrow(PrivateChannelEscrowProgramError),
    /// A custom error whose raising program is unknown (its log was unreadable). It proves
    /// nothing about the release, which moved nothing, so it is retried like the public path.
    OriginUnproven,
    /// Everything else takes today's path.
    Other,
}

/// Classify by the program that raised the code: escrow 17, 22 and 24 collide with
/// Token-2022's, so the code alone proves nothing.
pub fn classify_confidential(origin: Option<Pubkey>, code: Option<u32>) -> ConfidentialFailure {
    use token_2022_codes::*;
    let (Some(origin), Some(code)) = (origin, code) else {
        return match code {
            Some(_) => ConfidentialFailure::OriginUnproven,
            None => ConfidentialFailure::Other,
        };
    };
    if origin == spl_token_2022::ID {
        return match code {
            PUBKEY_MISMATCH | BALANCE_MISMATCH | FEE_MISMATCH | FEE_PARAMETERS_MISMATCH => {
                ConfidentialFailure::RebuildProofs
            }
            // The escrow refuses a frozen or unapproved destination itself before the CPI.
            ACCOUNT_FROZEN | NOT_APPROVED => ConfidentialFailure::Park(
                metrics::BAIL_REASON_CONFIDENTIAL_ESCROW_ACCOUNT_NOT_READY,
            ),
            CREDITS_DISABLED | COUNTER_FULL => {
                ConfidentialFailure::Park(metrics::BAIL_REASON_DESTINATION_NOT_READY)
            }
            _ => ConfidentialFailure::Other,
        };
    }
    if origin == PRIVATE_CHANNEL_ESCROW_PROGRAM_ID {
        if code == ESCROW_CONFIDENTIAL_NOT_CONFIGURED {
            return ConfidentialFailure::Park(
                metrics::BAIL_REASON_CONFIDENTIAL_MINT_NOT_CONFIGURED,
            );
        }
        // The escrow names the states Token-2022 would refuse; route them the same way.
        use PrivateChannelEscrowProgramError as E;
        if [
            E::RecipientKeyChanged,
            E::StaleAuditorKey,
            E::StaleConfidentialBalance,
        ]
        .map(|e| e as u32)
        .contains(&code)
        {
            return ConfidentialFailure::RebuildProofs;
        }
        if [
            E::RecipientNotConfidential,
            E::RecipientNotApproved,
            E::RecipientCreditsDisabled,
            E::RecipientPendingFull,
            E::RecipientFrozen,
        ]
        .map(|e| e as u32)
        .contains(&code)
        {
            return ConfidentialFailure::Park(metrics::BAIL_REASON_DESTINATION_NOT_READY);
        }
        if let Some(error) = escrow_error_for_code(code) {
            return ConfidentialFailure::Escrow(error);
        }
    }
    ConfidentialFailure::Other
}

/// Classify a release failure. A refusal raised by the mint's transfer hook (a program in
/// the resolved hook accounts) parks: Token-2022 always passes the hook `u64::MAX`, so no
/// resend can pass it.
pub fn classify_release(
    origin: Option<Pubkey>,
    code: Option<u32>,
    hook_extras: &[solana_sdk::instruction::AccountMeta],
) -> ConfidentialFailure {
    if origin.is_some_and(|origin| {
        origin != spl_token_2022::ID
            && origin != PRIVATE_CHANNEL_ESCROW_PROGRAM_ID
            && hook_extras.iter().any(|meta| meta.pubkey == origin)
    }) {
        return ConfidentialFailure::Park(metrics::BAIL_REASON_HOOK_UNRESOLVABLE);
    }
    classify_confidential(origin, code)
}

/// Classify a failed release attempt. Running out of compute comes first: the log then
/// names whichever program ran out, the hook included, and no resend passes it.
pub fn classify_attempt(
    err: Option<&solana_sdk::transaction::TransactionError>,
    origin: Option<Pubkey>,
    hook_extras: &[solana_sdk::instruction::AccountMeta],
) -> ConfidentialFailure {
    if matches!(
        err,
        Some(solana_sdk::transaction::TransactionError::InstructionError(
            _,
            InstructionError::ComputationalBudgetExceeded
        ))
    ) {
        return ConfidentialFailure::Park(metrics::BAIL_REASON_CONFIDENTIAL_COMPUTE_EXCEEDED);
    }
    classify_release(origin, err.and_then(custom_code), hook_extras)
}

/// The custom code of a failed transaction, if it has one.
pub fn custom_code(err: &solana_sdk::transaction::TransactionError) -> Option<u32> {
    match err {
        solana_sdk::transaction::TransactionError::InstructionError(
            _,
            InstructionError::Custom(code),
        ) => Some(*code),
        _ => None,
    }
}

/// The error and logs of a refused preflight simulation, so a send error classifies the
/// same way a landed failure does.
pub fn preflight_failure(
    err: &solana_rpc_client_api::client_error::Error,
) -> Option<(
    Option<solana_sdk::transaction::TransactionError>,
    Option<Pubkey>,
)> {
    use solana_rpc_client_api::{
        client_error::ErrorKind, request::RpcError, request::RpcResponseErrorData,
    };
    let ErrorKind::RpcError(RpcError::RpcResponseError {
        data: RpcResponseErrorData::SendTransactionPreflightFailure(result),
        ..
    }) = err.kind()
    else {
        return None;
    };
    let err = result
        .err
        .as_ref()
        .map(|err| solana_sdk::transaction::TransactionError::from(err.clone()));
    let origin = result.logs.as_deref().and_then(failing_program);
    Some((err, origin))
}

/// A send error worth resending: anything but a preflight refusal, or a preflight that only
/// missed the blockhash. Neither says the release itself would fail.
pub fn resendable(err: &solana_rpc_client_api::client_error::Error) -> bool {
    use solana_rpc_client_api::{
        client_error::ErrorKind, request::RpcError, request::RpcResponseErrorData,
    };
    match err.kind() {
        ErrorKind::RpcError(RpcError::RpcResponseError {
            data: RpcResponseErrorData::SendTransactionPreflightFailure(result),
            ..
        }) => result.err.as_ref().is_some_and(|err| {
            solana_sdk::transaction::TransactionError::from(err.clone())
                == solana_sdk::transaction::TransactionError::BlockhashNotFound
        }),
        _ => true,
    }
}

// ---------------------------------------------------------------------------------------
// Proof reuse
// ---------------------------------------------------------------------------------------

/// Why an attempt is being made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptCause {
    First,
    /// The previous attempt timed out unconfirmed.
    TimeoutResend,
    /// Token-2022 refused the proofs (26, 27, 32, 33).
    StaleProofs,
    /// The rotation drain re-sends it.
    RotationDrain,
}

/// Only a timeout resend may keep its proofs: they bind the balance ciphertext, not the
/// blockhash. Anything else may follow a change in available, so the proofs are rebuilt.
pub fn reuse_proofs(cause: AttemptCause) -> bool {
    cause == AttemptCause::TimeoutResend
}

// ---------------------------------------------------------------------------------------
// Rebalance
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebalanceDecision {
    Execute,
    Refuse(String),
    /// No candidate verified; try on a later check.
    Wait,
}

/// A move to public runs only against the available it was made for; a move to confidential
/// only needs enough public. The request id keeps a rerun from moving twice.
pub fn rebalance_decision(
    request: &RebalanceRequest,
    verified: Option<u64>,
    public: u64,
) -> RebalanceDecision {
    let Some(verified) = verified else {
        return RebalanceDecision::Wait;
    };
    match request.direction {
        RebalanceDirection::ToPublic if verified != request.expected_available => {
            RebalanceDecision::Refuse(format!(
                "verified available {verified} differs from expected {}",
                request.expected_available
            ))
        }
        RebalanceDirection::ToPublic if request.amount > verified => RebalanceDecision::Refuse(
            format!("amount {} above available {verified}", request.amount),
        ),
        RebalanceDirection::ToConfidential if request.amount > public => RebalanceDecision::Refuse(
            format!("amount {} above public balance {public}", request.amount),
        ),
        _ => RebalanceDecision::Execute,
    }
}

// ---------------------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------------------

/// Read an escrow account at confirmed commitment, no older than `min_context_slot`.
pub async fn read_escrow(
    rpc: &RpcClientWithRetry,
    ata: &Pubkey,
    min_context_slot: u64,
) -> Result<(ConfidentialAccountRead, u64), String> {
    let response = rpc
        .get_account_with_context_min_slot(
            ata,
            CommitmentConfig::confirmed(),
            Some(min_context_slot),
        )
        .await
        .map_err(|e| format!("escrow account read {ata}: {e}"))?;
    Ok((
        decode_confidential_account(response.value.as_ref()),
        response.context.slot,
    ))
}

/// Seed the read floor with the landing slot of our last own transaction, so the first read
/// after a restart cannot come from before it. The journal's blockhash slot, a lower bound,
/// is the fallback until a landing slot is stored.
pub async fn seed_min_context_slot(conf: &mut ConfidentialSender, storage: &Storage) {
    let seeded = match storage.confidential_landing_slot().await {
        Ok(Some(slot)) => Ok(Some(slot)),
        Ok(None) => storage.max_release_journal_slot().await,
        Err(e) => Err(e),
    };
    match seeded {
        Ok(Some(slot)) => conf.observe_slot(slot),
        Ok(None) => {}
        Err(e) => warn!("Could not seed the confidential read floor: {e}"),
    }
}

/// A landed transaction of ours left `available` on `mint`: keep it as candidate (a), also
/// across a restart. Every use is still opening-checked.
async fn remember_available(state: &mut SenderState, mint: &Pubkey, available: u64, slot: u64) {
    let Some(conf) = state.confidential.as_mut() else {
        return;
    };
    conf.books.entry(*mint).or_default().local = Some(available);
    if let Err(e) = state
        .storage
        .record_confidential_mint_state(&mint.to_string(), available, slot)
        .await
    {
        warn!(%mint, "Could not keep the verified available: {e}");
    }
}

/// One of our transactions landed at `slot`: raise the read floor and persist it for boot.
pub async fn record_landing(state: &mut SenderState, slot: u64) {
    let Some(conf) = state.confidential.as_mut() else {
        return;
    };
    conf.observe_slot(slot);
    if let Err(e) = state.storage.record_confidential_landing_slot(slot).await {
        warn!(slot, "Could not persist the confidential read floor: {e}");
    }
}

/// Our configured escrow account for `mint`, read fresh; `None` when not ours to use.
async fn read_ours(
    rpc: &RpcClientWithRetry,
    conf: &ConfidentialSender,
    mint: &Pubkey,
) -> Result<Option<(Box<ConfidentialAccountView>, u64)>, String> {
    let (read, slot) = read_escrow(
        rpc,
        &escrow_ata(&conf.instance, mint),
        conf.min_context_slot,
    )
    .await?;
    Ok(match read {
        ConfidentialAccountRead::Configured(view) if view.elgamal_pubkey == conf.keys.pubkey() => {
            Some((view, slot))
        }
        _ => None,
    })
}

/// The applied pending lo and hi from our apply's own event.
async fn applied_pending_of(
    rpc: &RpcClientWithRetry,
    signature: &Signature,
) -> Result<(Ciphertext, Ciphertext), String> {
    use crate::indexer::datasource::common::parser::escrow::decode_confidential_event;
    use solana_transaction_status::{
        option_serializer::OptionSerializer, UiInstruction, UiParsedInstruction,
    };
    const APPLY: u8 = 15;
    // Event layout: tag (8), discriminator (1), three keys, then applied lo and hi.
    const LO: usize = 9 + 3 * 32;

    let transaction = rpc
        .get_transaction(signature)
        .await
        .map_err(|e| format!("apply transaction {signature}: {e}"))?;
    let OptionSerializer::Some(inner) = transaction
        .transaction
        .meta
        .ok_or("apply transaction has no meta")?
        .inner_instructions
    else {
        return Err("apply transaction has no inner instructions".into());
    };
    for set in inner {
        for ix in set.instructions {
            let (program, data) = match ix {
                UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(ix)) => {
                    (ix.program_id, ix.data)
                }
                _ => continue,
            };
            if program != PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string() {
                continue;
            }
            let Ok(bytes) = bs58::decode(&data).into_vec() else {
                continue;
            };
            if let Some(event) = decode_confidential_event(APPLY, &bytes) {
                let lo = event.raw[LO..LO + 64].try_into().expect("length checked");
                let hi = event.raw[LO + 64..LO + 128]
                    .try_into()
                    .expect("length checked");
                return Ok((lo, hi));
            }
        }
    }
    Err(format!("no apply event in {signature}"))
}

/// Verified available for `mint`, trying (a), (b), then (d) from our last apply.
async fn verified_available(
    rpc: &RpcClientWithRetry,
    conf: &ConfidentialSender,
    mint: &Pubkey,
    view: &ConfidentialAccountView,
) -> Option<(u64, CandidateSource)> {
    let book = conf.books.get(mint);
    let local = book.and_then(|book| book.local);
    // (a) and (b) cost no RPC and no search; (d) is fetched only when neither opens.
    if let Some(found) = cheap_candidate(&conf.keys, view, local) {
        return Some(found);
    }
    let last_apply = match book.and_then(|book| book.last_apply) {
        Some(apply) => match applied_pending_of(rpc, &apply.signature).await {
            Ok((lo, hi)) => Some((apply.prior_available, lo, hi, apply.applied_pending)),
            Err(e) => {
                warn!(%mint, "Candidate (d) unavailable: {e}");
                None
            }
        },
        None => None,
    };
    verify_available_off_executor(conf.keys.clone(), view, local, last_apply).await
}

// ---------------------------------------------------------------------------------------
// Sending operator transactions (applies, repairs, moves)
// ---------------------------------------------------------------------------------------

/// Confirmation of one of our transactions, with the code and its origin on failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Confirmation {
    Confirmed {
        slot: u64,
    },
    Failed {
        slot: u64,
        err: solana_sdk::transaction::TransactionError,
        origin: Option<Pubkey>,
    },
    Timeout,
}

async fn confirm(
    rpc: &RpcClientWithRetry,
    signature: &Signature,
    poll_ms: u64,
) -> Result<Confirmation, String> {
    for attempt in 0..MAX_POLL_ATTEMPTS_CONFIRMATION {
        let response = rpc
            .get_signature_statuses(&[*signature])
            .await
            .map_err(|e| format!("status of {signature}: {e}"))?;
        if let Some(status) = response.value.first().and_then(|s| s.as_ref()) {
            if status.satisfies_commitment(CommitmentConfig::confirmed()) {
                let Some(err) = &status.err else {
                    return Ok(Confirmation::Confirmed { slot: status.slot });
                };
                let origin = match rpc.get_transaction(signature).await {
                    Ok(tx) => tx
                        .transaction
                        .meta
                        .and_then(|meta| Option::<Vec<String>>::from(meta.log_messages))
                        .as_deref()
                        .and_then(failing_program),
                    Err(_) => None,
                };
                return Ok(Confirmation::Failed {
                    slot: status.slot,
                    err: err.clone(),
                    origin,
                });
            }
        }
        if attempt + 1 < MAX_POLL_ATTEMPTS_CONFIRMATION {
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
        }
    }
    Ok(Confirmation::Timeout)
}

/// Journal an apply or repair (legacy; no proofs), then send it and wait. One whose
/// outcome is unknown stays journaled and blocks its mint until it settles.
async fn send_journaled_apply(
    state: &SenderState,
    mint: &Pubkey,
    trigger: ApplyTrigger,
    prior_available: u64,
    applied_pending: u64,
    instruction: solana_sdk::instruction::Instruction,
) -> Result<(Signature, u64), String> {
    let ix = InstructionWithSigners {
        instructions: vec![instruction],
        fee_payer: TransactionBuilder::release_signers()[0].pubkey(),
        signers: TransactionBuilder::release_signers(),
        compute_unit_price: Some(1),
        compute_budget: Some(CU_CONFIDENTIAL_APPLY),
    };
    let (transaction, signature, last_valid_block_height, blockhash_slot) =
        build_and_sign(&state.rpc_client, ix)
            .await
            .map_err(|e| e.to_string())?;
    let id = state
        .storage
        .journal_operator_attempt(&OperatorAttempt {
            id: 0,
            mint: mint.to_string(),
            kind: trigger.label().to_string(),
            signature: signature.to_string(),
            last_valid_block_height,
            blockhash_slot,
            prior_available: Some(prior_available),
            applied_pending: Some(applied_pending),
            status: attempt_status::SENT.to_string(),
            landed_slot: None,
            created_at: Utc::now(),
        })
        .await
        .map_err(|e| format!("apply journal: {e}"))?;
    send_signed(&state.rpc_client, &transaction, RetryPolicy::Idempotent)
        .await
        .map_err(|e| format!("{signature}: send: {e}; journaled, settles before the next write"))?;
    let landed = match confirm(
        &state.rpc_client,
        &signature,
        state.confirmation_poll_interval_ms,
    )
    .await?
    {
        Confirmation::Confirmed { slot } => Some(slot),
        // Failed on-chain: it changed nothing, so it no longer blocks the mint.
        Confirmation::Failed { .. } => None,
        Confirmation::Timeout => {
            return Err(format!(
                "{signature}: not confirmed; journaled, settles before the next write"
            ))
        }
    };
    // Best effort: left `sent`, the next write settles it from history instead.
    if let Err(e) = state.storage.settle_operator_attempt(id, landed).await {
        warn!(%signature, "Could not settle the journaled apply: {e}");
    }
    match landed {
        Some(slot) => Ok((signature, slot)),
        None => Err(format!("{signature}: failed on-chain")),
    }
}

/// Which kind of confidential write is about to go out on a mint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Write {
    Release,
    Rebalance,
    /// An apply or repair; `release` is the release that asked for it, if any.
    Apply {
        release: Option<i64>,
    },
}

/// Why a write on `mint` must wait, if it must. An apply or repair that could still land
/// would otherwise overwrite the AES copy after the write; an apply also waits for any
/// rebalance or other release of the mint, since one landing after it leaves its AES stale.
pub async fn write_blocked(
    state: &mut SenderState,
    mint: &Pubkey,
    write: Write,
) -> Result<Option<String>, String> {
    let open = settle_operator_attempts(state, Some(mint)).await?;
    if open > 0 {
        return Ok(Some(format!(
            "an apply on {mint} may still land; waiting until it lands or expires"
        )));
    }
    let Write::Apply { release } = write else {
        return Ok(None);
    };
    let mint_str = mint.to_string();
    let sent = |requests: Vec<RebalanceRequest>| {
        requests
            .into_iter()
            .filter(|request| request.mint == mint_str)
            .collect::<Vec<_>>()
    };
    let rebalances = state
        .storage
        .get_rebalance_requests(rebalance_status::SENT)
        .await
        .map_err(|e| format!("rebalance requests: {e}"))?;
    for request in sent(rebalances) {
        settle_sent_rebalance(state, &request).await;
    }
    let rebalances = state
        .storage
        .get_rebalance_requests(rebalance_status::SENT)
        .await
        .map_err(|e| format!("rebalance requests: {e}"))?;
    if !sent(rebalances).is_empty() {
        return Ok(Some(format!("a rebalance on {mint} may still land")));
    }
    let settled = state
        .confidential
        .as_ref()
        .map(|conf| conf.settled_releases.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    let except: Vec<i64> = settled.into_iter().chain(release).collect();
    if state
        .storage
        .confidential_release_unsettled(&mint_str, &except)
        .await
        .map_err(|e| format!("release journal: {e}"))?
    {
        return Ok(Some(format!(
            "another confidential release of {mint} may still land"
        )));
    }
    Ok(None)
}

/// Settle journaled applies and repairs, of one mint or all (at boot): landed ones raise the
/// read floor and become candidate (d), expired ones are dropped. Returns how many are
/// still undecided.
pub async fn settle_operator_attempts(
    state: &mut SenderState,
    mint: Option<&Pubkey>,
) -> Result<usize, String> {
    let mint = mint.map(Pubkey::to_string);
    let attempts = state
        .storage
        .outstanding_operator_attempts(mint.as_deref())
        .await
        .map_err(|e| format!("apply journal: {e}"))?;
    let mut open = 0;
    for attempt in attempts {
        if !settle_attempt(state, &attempt).await {
            open += 1;
        }
    }
    Ok(open)
}

/// Settle one attempt by the rule releases use; false while it could still land.
async fn settle_attempt(state: &mut SenderState, attempt: &OperatorAttempt) -> bool {
    use super::remint::{classify_signatures, FinalityRpc, SigFinality};
    let Ok(signature) = Signature::from_str(&attempt.signature) else {
        warn!(
            id = attempt.id,
            "Journaled apply has no valid signature; resolve it by hand"
        );
        return false;
    };
    let pending = PendingSig {
        signature,
        last_valid_block_height: attempt.last_valid_block_height,
        blockhash_slot: Some(attempt.blockhash_slot),
    };
    let finality = FinalityRpc::solana(&state.rpc_client, state.fallback_rpc_client.as_deref());
    let landed_slot = match classify_signatures(&finality, &[pending]).await {
        SigFinality::Landed(_) => match landing_slot(&state.rpc_client, &signature).await {
            Ok(slot) => Some(slot),
            Err(e) => {
                warn!(%signature, "Landed apply has no readable slot yet: {e}");
                return false;
            }
        },
        SigFinality::Dead => None,
        SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
            info!(%signature, mint = attempt.mint, "Apply still undecided: {reason}");
            return false;
        }
    };
    if let Err(e) = state
        .storage
        .settle_operator_attempt(attempt.id, landed_slot)
        .await
    {
        warn!(%signature, "Could not settle the journaled apply: {e}");
        return false;
    }
    let (Some(slot), Ok(mint)) = (landed_slot, Pubkey::from_str(&attempt.mint)) else {
        info!(%signature, mint = attempt.mint, "Journaled apply expired without landing");
        return true;
    };
    warn!(%signature, %mint, slot, "Journaled apply landed late");
    record_landing(state, slot).await;
    if let Some(conf) = state.confidential.as_mut() {
        let book = conf.books.entry(mint).or_default();
        book.local = None;
        book.last_apply = attempt.prior_available.map(|prior_available| LastApply {
            prior_available,
            signature,
            applied_pending: attempt.applied_pending,
        });
    }
    true
}

async fn landing_slot(rpc: &RpcClientWithRetry, signature: &Signature) -> Result<u64, String> {
    rpc.get_signature_statuses_with_history(&[*signature])
        .await
        .map_err(|e| e.to_string())?
        .value
        .first()
        .and_then(|status| status.as_ref())
        .map(|status| status.slot)
        .ok_or_else(|| "no status".to_string())
}

/// Pending past the bounded search: take what the indexer recorded since the mint's last
/// landed apply, and use it only if the combined pending ciphertext opens to it. Otherwise
/// wait (the indexer may be behind) and alert; never apply a guessed value.
async fn pending_from_indexed(
    state: &SenderState,
    mint: &Pubkey,
    view: &ConfidentialAccountView,
) -> Result<u64, String> {
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let mint_label = mint.to_string();
    let last = state
        .storage
        .last_landed_operator_attempt(&mint_label)
        .await
        .map_err(|e| format!("apply journal: {e}"))?;
    let inputs = state
        .storage
        .confidential_pending_inputs(&mint_label, last.as_ref())
        .await
        .map_err(|e| format!("indexed deposits: {e}"))?;
    let boundary = last.and_then(|attempt| attempt.landed_slot);
    let (keys, lo, hi) = (conf.keys.clone(), view.pending_lo, view.pending_hi);
    let found =
        tokio::task::spawn_blocking(move || indexed_pending(&keys, &lo, &hi, &inputs, boundary))
            .await
            .ok()
            .flatten();
    let unverified =
        metrics::OPERATOR_CONFIDENTIAL_PENDING_UNVERIFIED.with_label_values(&[&mint_label]);
    match found {
        Some(pending) => {
            unverified.set(0.0);
            info!(%mint, pending, "Pending past the search verified from the indexed deposits");
            Ok(pending)
        }
        None => {
            unverified.set(1.0);
            error!(%mint, "Pending is past the search and the indexed deposits do not open it; waiting");
            Err(format!(
                "pending of {mint} is past the search and the indexed deposits do not open it"
            ))
        }
    }
}

/// Which path asked for an apply, for the metric label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyTrigger {
    Release,
    Background,
    Repair,
}

impl ApplyTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Background => "background",
            Self::Repair => "repair",
        }
    }
}

/// One apply built from one read: expected counter and pending from that read, AES =
/// verified available + that pending. Any racing deposit shows up as a counter mismatch.
async fn apply_once(
    state: &mut SenderState,
    mint: &Pubkey,
    trigger: ApplyTrigger,
    release: Option<i64>,
) -> Result<(), String> {
    if let Some(reason) = write_blocked(state, mint, Write::Apply { release }).await? {
        return Err(reason);
    }
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let (view, _) = read_ours(&state.rpc_client, conf, mint)
        .await?
        .ok_or("escrow account not configured with our key")?;
    let (verified, _) = verified_available(&state.rpc_client, conf, mint, &view)
        .await
        .ok_or("no candidate opens the available balance")?;
    let pending = match pending_off_executor(conf.keys.clone(), &view).await {
        Ok(pending) => pending,
        Err(PendingError::HiTooLarge) => pending_from_indexed(state, mint, &view).await?,
        Err(e) => return Err(format!("pending: {e}")),
    };
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let aes = conf.keys.encrypt_aes(
        verified
            .checked_add(pending)
            .ok_or("available plus pending overflows")?,
    );
    let ix =
        apply_pending_balance_instruction(&conf.accounts(mint), view.pending_credit_counter, aes);

    let (signature, slot) =
        send_journaled_apply(state, mint, trigger, verified, pending, ix).await?;
    metrics::OPERATOR_CONFIDENTIAL_APPLIES
        .with_label_values(&[&mint.to_string(), trigger.label()])
        .inc();
    record_landing(state, slot).await;
    let conf = state.confidential.as_mut().ok_or("no confidential keys")?;
    let book = conf.books.entry(*mint).or_default();
    // Available moved by an amount only the event knows, so (d) replaces (a).
    book.local = None;
    book.last_apply = Some(LastApply {
        prior_available: verified,
        signature,
        applied_pending: Some(pending),
    });
    info!(%mint, %signature, trigger = trigger.label(), "Applied pending balance");
    Ok(())
}

/// Apply, then repair the AES copy while deposits keep racing, within the budget.
pub async fn apply_and_repair(
    state: &mut SenderState,
    mint: &Pubkey,
    trigger: ApplyTrigger,
    release: Option<i64>,
) -> Result<(), String> {
    if trigger != ApplyTrigger::Repair {
        if let Some(conf) = state.confidential.as_mut() {
            conf.books.entry(*mint).or_default().repair.fresh_budget();
        }
    }
    let mut trigger = trigger;
    loop {
        apply_once(state, mint, trigger, release).await?;
        let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
        let Some((view, slot)) = read_ours(&state.rpc_client, conf, mint).await? else {
            return Ok(());
        };
        let stale = aes_is_stale(&conf.keys, &view);
        // Kept now, since a restart before the repair lands loses the apply from memory.
        if let Some((available, _)) = verified_available(&state.rpc_client, conf, mint, &view).await
        {
            remember_available(state, mint, available, slot).await;
        }
        let conf = state.confidential.as_mut().ok_or("no confidential keys")?;
        let book = conf.books.entry(*mint).or_default();
        if !stale {
            end_stale_episode(book, mint);
            return Ok(());
        }
        let repair = &mut book.repair;
        match repair.on_still_stale(Instant::now()) {
            RepairNext::RepairNow => trigger = ApplyTrigger::Repair,
            RepairNext::Defer { alert } => {
                if alert {
                    error!(%mint, "AES copy still stale after {AES_REPAIR_BUDGET} repairs; backing off");
                }
                metrics::OPERATOR_CONFIDENTIAL_AES_REPAIR_DEFERRED
                    .with_label_values(&[&mint.to_string()])
                    .set(1.0);
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Confidential release
// ---------------------------------------------------------------------------------------

/// Outcome of preparing a release from a fresh read.
enum Prepared {
    Ready {
        instructions: Vec<solana_sdk::instruction::Instruction>,
        verified: u64,
    },
    Wait(String),
    /// An earlier write on the mint could still land.
    Blocked(String),
    Park(&'static str, String),
}

async fn prepare_release(
    state: &mut SenderState,
    request: &ConfidentialReleaseRequest,
) -> Result<Prepared, String> {
    let mint = request.mint;
    if let Some(reason) = write_blocked(state, &mint, Write::Release).await? {
        return Ok(Prepared::Blocked(reason));
    }
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let Some((view, _)) = read_ours(&state.rpc_client, conf, &mint).await? else {
        return Ok(Prepared::Park(
            metrics::BAIL_REASON_CONFIDENTIAL_MINT_NOT_CONFIGURED,
            format!("escrow account for {mint} is not configured with our key"),
        ));
    };
    let mut verified = verified_available(&state.rpc_client, conf, &mint, &view).await;

    let mut view = view;
    if release_needs_apply(verified.map(|(v, _)| v), request.amount, &view) {
        let release = Some(request.transaction_id);
        if let Err(e) = apply_and_repair(state, &mint, ApplyTrigger::Release, release).await {
            // An apply that may still land, or a write it must wait for, is a wait.
            return match write_blocked(state, &mint, Write::Apply { release }).await? {
                Some(reason) => Ok(Prepared::Blocked(format!("{e}; {reason}"))),
                None => Err(e),
            };
        }
        let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
        let Some((fresh, _)) = read_ours(&state.rpc_client, conf, &mint).await? else {
            return Err("escrow account unreadable after the apply".into());
        };
        view = fresh;
        verified = verified_available(&state.rpc_client, conf, &mint, &view).await;
    }

    let Some((available, source)) = verified else {
        return Ok(Prepared::Wait(format!(
            "no candidate opens the available balance of {mint}"
        )));
    };
    if available < request.amount {
        return Ok(Prepared::Park(
            metrics::BAIL_REASON_CONFIDENTIAL_BALANCE_LOW,
            format!(
                "verified confidential available {available} below release {} for {mint}; rebalance to confidential",
                request.amount
            ),
        ));
    }

    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let (destination_elgamal, mint_params) =
        match release_inputs(&state.rpc_client, conf, request).await? {
            Ok(inputs) => inputs,
            Err(park) => return Ok(park),
        };
    let proofs = match conf.keys.prove_release(
        &view.available,
        available,
        request.amount,
        &destination_elgamal,
        mint_params.auditor.as_ref(),
        mint_params.fee.as_ref(),
    ) {
        Ok(proofs) => proofs,
        Err(ProofError::InvalidInput) => {
            return Ok(Prepared::Park(
                metrics::BAIL_REASON_DESTINATION_NOT_READY,
                "destination or auditor key does not decode".into(),
            ))
        }
        Err(e) => return Err(format!("proof generation: {e}")),
    };
    info!(%mint, available, ?source, "Built confidential release proofs");
    Ok(Prepared::Ready {
        instructions: request.instructions(&proofs),
        verified: available,
    })
}

/// The destination's key and the mint's auditor and fee, read for every proof build: a key
/// rotation or a fee epoch change between attempts must not leave a rebuild on old values.
async fn release_inputs(
    rpc: &RpcClientWithRetry,
    conf: &ConfidentialSender,
    request: &ConfidentialReleaseRequest,
) -> Result<Result<([u8; 32], ConfidentialMintParams), Prepared>, String> {
    let (destination, _) = read_escrow(rpc, &request.destination, conf.min_context_slot).await?;
    let destination_elgamal = match destination_readiness(&destination) {
        DestinationReadiness::Ready { elgamal_pubkey } => elgamal_pubkey,
        DestinationReadiness::NotReady(reason) => {
            return Ok(Err(Prepared::Park(
                metrics::BAIL_REASON_DESTINATION_NOT_READY,
                format!("destination {}: {reason}", request.destination),
            )))
        }
    };
    let mint_params = read_confidential_mint_params(rpc, &request.mint, conf.min_context_slot)
        .await
        .map_err(|e| format!("mint read: {e}"))?;
    let Some(mint_params) = mint_params else {
        return Ok(Err(Prepared::Park(
            metrics::BAIL_REASON_CONFIDENTIAL_MINT_NOT_CONFIGURED,
            format!("mint {} does not decode as confidential", request.mint),
        )));
    };
    Ok(Ok((destination_elgamal, mint_params)))
}

fn release_ctx(request: &ConfidentialReleaseRequest) -> TransactionContext {
    TransactionContext {
        transaction_id: Some(request.transaction_id),
        withdrawal_nonce: Some(request.nonce),
        trace_id: Some(request.trace_id.clone()),
        kind: TransactionKind::ReleaseFunds,
        deposit_claim_lease: None,
    }
}

fn release_ix(
    instructions: Vec<solana_sdk::instruction::Instruction>,
    compute_limit: u32,
) -> InstructionWithSigners {
    let signers = TransactionBuilder::release_signers();
    InstructionWithSigners {
        instructions,
        fee_payer: signers[0].pubkey(),
        signers,
        compute_unit_price: Some(1),
        compute_budget: Some(compute_limit),
    }
}

/// Park a row that never broadcast: a fenced write under the release lease.
async fn park_unsent(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    label: &'static str,
    reason: &str,
) {
    let (Some(nonce), Some(transaction_id)) = (ctx.withdrawal_nonce, ctx.transaction_id) else {
        return;
    };
    let lease = state.release_leases.get(&nonce).copied();
    let pt = state.program_type.as_label();
    let outcome = send_fenced_outcome(
        &state.storage,
        storage_tx,
        pt,
        ctx,
        transaction_id,
        lease,
        TransactionStatus::ManualReview,
        &format!("{label}: {reason}"),
        Some(label),
    )
    .await;
    if outcome == crate::operator::utils::storage_util::FencedWrite::Applied {
        metrics::OPERATOR_TRANSACTION_QUARANTINED
            .with_label_values(&[pt, label])
            .inc();
    }
    forget_release(state, nonce);
}

/// Park a row with a journaled attempt only once no attempt can still land: the remint
/// gate runs, and where it would remint the row parks instead.
async fn park_through_journal(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    label: &'static str,
    reason: &str,
) {
    if let (Some(conf), Some(transaction_id)) = (state.confidential.as_mut(), ctx.transaction_id) {
        conf.parks.insert(transaction_id, label.to_string());
        if let Some(nonce) = ctx.withdrawal_nonce {
            conf.stale_budget.remove(&nonce);
            conf.requests.remove(&nonce);
            conf.held.remove(&nonce);
        }
        // Best effort: without it a restart during the wait ends in the remint, still safe.
        if let Err(e) = state
            .storage
            .set_confidential_park_label(transaction_id, label)
            .await
        {
            warn!(
                transaction_id,
                "Could not persist the park label {label}: {e}"
            );
        }
    }
    defer_remint_after_failure(state, ctx, storage_tx, &format!("{label}: {reason}"), false).await;
}

/// Seed candidate (a) of each mint with the available kept before a restart.
pub async fn seed_mint_states(conf: &mut ConfidentialSender, storage: &Storage) {
    let states = match storage.confidential_mint_states().await {
        Ok(states) => states,
        Err(e) => {
            warn!("Could not restore the kept confidential balances: {e}");
            return;
        }
    };
    for (mint, available) in states {
        if let Ok(mint) = Pubkey::from_str(&mint) {
            conf.books
                .entry(mint)
                .or_default()
                .local
                .get_or_insert(available);
        }
    }
}

/// Seed candidate (d) of each mint with its last landed apply, as memory held it before a
/// restart. Applies that land later replace it when the journal is settled.
pub async fn seed_last_applies(conf: &mut ConfidentialSender, storage: &Storage) {
    let attempts = match storage.last_landed_operator_attempts().await {
        Ok(attempts) => attempts,
        Err(e) => {
            warn!("Could not restore the last landed applies: {e}");
            return;
        }
    };
    for attempt in attempts {
        let (Ok(mint), Ok(signature), Some(prior_available)) = (
            Pubkey::from_str(&attempt.mint),
            Signature::from_str(&attempt.signature),
            attempt.prior_available,
        ) else {
            continue;
        };
        conf.books
            .entry(mint)
            .or_default()
            .last_apply
            .get_or_insert(LastApply {
                prior_available,
                signature,
                applied_pending: attempt.applied_pending,
            });
    }
}

/// Restore the parks a previous incarnation left waiting on their journal gate.
pub async fn seed_parks(conf: &mut ConfidentialSender, storage: &Storage) {
    match storage.get_confidential_park_labels().await {
        Ok(labels) => conf.parks.extend(labels),
        Err(e) => warn!("Could not restore confidential park labels; they end as remints: {e}"),
    }
}

/// How a gated park ends when the gate proved no attempt landed.
pub const PROVEN_ABSENT: &str = "no attempt landed; parked";

/// End a gated park: the gate proved no attempt landed, or it gave up at its attempt cap.
/// Returns false when the row is not a confidential park, so the caller's own path goes on.
pub async fn finish_gated_park(
    state: &mut SenderState,
    transaction_id: i64,
    trace_id: Option<String>,
    original_error: &str,
    outcome: &str,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) -> bool {
    let Some(label) = state
        .confidential
        .as_mut()
        .and_then(|conf| conf.parks.remove(&transaction_id))
    else {
        return false;
    };
    crate::channel_utils::send_guaranteed(
        storage_tx,
        TransactionStatusUpdate {
            transaction_id,
            trace_id,
            status: TransactionStatus::ManualReview,
            counterpart_signature: None,
            processed_at: Some(Utc::now()),
            error_message: Some(format!(
                "{} | {outcome}",
                park_message(&label, original_error)
            )),
            remint_signature: None,
            remint_attempted: false,
            alert_only: false,
        },
        "confidential park",
    )
    .await
    .ok();
    metrics::OPERATOR_TRANSACTION_QUARANTINED
        .with_label_values(&[state.program_type.as_label(), &label])
        .inc();
    true
}

// After a restart the stored error is generic, so the label is put back in front.
fn park_message(label: &str, original_error: &str) -> String {
    if original_error.starts_with(label) {
        original_error.to_string()
    } else {
        format!("{label}: {original_error}")
    }
}

fn forget_release(state: &mut SenderState, nonce: u64) {
    state.in_flight_withdrawals.remove(&nonce);
    state.release_leases.remove(&nonce);
    state.remint_cache.remove(&nonce);
    state.pending_signatures.remove(&nonce);
    state.retry_counts.remove(&nonce);
    if let Some(conf) = state.confidential.as_mut() {
        conf.stale_budget.remove(&nonce);
        conf.requests.remove(&nonce);
        conf.held.remove(&nonce);
    }
}

async fn journaled(state: &SenderState, transaction_id: i64) -> Result<bool, String> {
    load_pending_sigs(&state.storage, transaction_id)
        .await
        .map(|sigs| !sigs.is_empty())
        .map_err(|_| "journal unreadable".to_string())
}

/// A transient error before anything new went out: before any attempt the row waits in
/// `Pending`; after one it is held in the sender.
async fn wait_or_park(
    state: &mut SenderState,
    ctx: &TransactionContext,
    request: &ConfidentialReleaseRequest,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    reason: &str,
) {
    let nonce = request.nonce;
    match journaled(state, request.transaction_id).await {
        Ok(false) => {
            let Some(lease) = state.release_leases.get(&nonce).copied() else {
                return;
            };
            let (not_before, started) = next_wait(Utc::now(), request.wait_started_at);
            match state
                .storage
                .requeue_confidential_wait(request.transaction_id, lease, not_before, started)
                .await
            {
                Ok(true) => {
                    metrics::OPERATOR_CONFIDENTIAL_WAITS
                        .with_label_values(&[&request.mint.to_string()])
                        .inc();
                    info!(nonce, %not_before, "Confidential release waits for a verified balance: {reason}");
                }
                Ok(false) => warn!(nonce, "Release no longer ours to requeue; leaving it"),
                Err(e) => warn!(
                    nonce,
                    "Could not requeue the waiting release; recovery will: {e}"
                ),
            }
            forget_release(state, nonce);
        }
        Ok(true) => hold_or_park(state, ctx, request, storage_tx, reason).await,
        // Unknown journal: leave the row Processing for recovery.
        Err(e) => {
            warn!(nonce, "Leaving the release to recovery: {e}");
            leave_to_recovery(state, nonce);
        }
    }
}

/// The mint is blocked by an earlier write: before any attempt the row waits in `Pending`;
/// after one it is left `Processing` for recovery, since the block lifts on its own.
async fn wait_or_leave(
    state: &mut SenderState,
    request: &ConfidentialReleaseRequest,
    reason: &str,
) {
    let nonce = request.nonce;
    match journaled(state, request.transaction_id).await {
        Ok(false) => requeue_unsent(state, request, reason).await,
        Ok(true) | Err(_) => {
            warn!(nonce, "Leaving the release to recovery: {reason}");
            leave_to_recovery(state, nonce);
        }
    }
}

/// Back to `Pending` with the persisted backoff; the retry count goes with the attempt.
async fn requeue_unsent(
    state: &mut SenderState,
    request: &ConfidentialReleaseRequest,
    reason: &str,
) {
    let nonce = request.nonce;
    let Some(lease) = state.release_leases.get(&nonce).copied() else {
        return;
    };
    let (not_before, started) = next_wait(Utc::now(), request.wait_started_at);
    match state
        .storage
        .requeue_confidential_wait(request.transaction_id, lease, not_before, started)
        .await
    {
        Ok(true) => info!(nonce, %not_before, "Confidential release waits: {reason}"),
        Ok(false) => warn!(nonce, "Release no longer ours to requeue; leaving it"),
        Err(e) => warn!(
            nonce,
            "Could not requeue the waiting release; recovery will: {e}"
        ),
    }
    forget_release(state, nonce);
}

// Recovery owns the row now; a kept retry count would end its next run at max retries.
fn leave_to_recovery(state: &mut SenderState, nonce: u64) {
    state.in_flight_withdrawals.remove(&nonce);
    state.retry_counts.remove(&nonce);
    if let Some(conf) = state.confidential.as_mut() {
        conf.held.remove(&nonce);
    }
}

// Nothing went out, so the attempt does not count against the retry budget.
fn unspend_retry(state: &mut SenderState, nonce: u64) {
    if let Some(count) = state.retry_counts.get_mut(&nonce) {
        *count = count.saturating_sub(1);
    }
}

async fn park(
    state: &mut SenderState,
    ctx: &TransactionContext,
    request: &ConfidentialReleaseRequest,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    label: &'static str,
    reason: &str,
) {
    match journaled(state, request.transaction_id).await {
        Ok(false) => park_unsent(state, ctx, storage_tx, label, reason).await,
        Ok(true) => park_through_journal(state, ctx, storage_tx, label, reason).await,
        Err(e) => {
            warn!(
                nonce = request.nonce,
                "Leaving the release to recovery: {e}"
            );
            leave_to_recovery(state, request.nonce);
        }
    }
}

/// Retry the held releases whose backoff is due, one at a time like any other send.
pub async fn retry_held(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let now = Utc::now();
    // The backoff can reach recovery's stale threshold, so a waiting row is refreshed apart from its retry.
    let refresh_after = chrono::Duration::from_std(RECOVERY_INTERVAL).expect("interval fits");
    let stale: Vec<(u64, i64)> = match state.confidential.as_ref() {
        Some(conf) => conf
            .held
            .iter()
            .filter_map(|(nonce, hold)| {
                let request = hold.request.as_ref()?;
                (now - hold.refreshed_at >= refresh_after)
                    .then_some((*nonce, request.transaction_id))
            })
            .collect(),
        None => return,
    };
    for (nonce, transaction_id) in stale {
        refresh_held_lease(state, nonce, transaction_id).await;
    }
    let due: Vec<u64> = match state.confidential.as_ref() {
        Some(conf) => conf
            .held
            .iter()
            .filter(|(_, hold)| hold.request.is_some() && hold.next_at <= now)
            .map(|(nonce, _)| *nonce)
            .collect(),
        None => return,
    };
    for nonce in due {
        let Some(request) = state
            .confidential
            .as_mut()
            .and_then(|conf| conf.held.get_mut(&nonce))
            .and_then(|hold| hold.request.take())
        else {
            continue;
        };
        submit_release(state, request, storage_tx, AttemptCause::StaleProofs, false).await;
        // Held again only if it failed the same way; any other end drops the hold.
        if let Some(conf) = state.confidential.as_mut() {
            if conf
                .held
                .get(&nonce)
                .is_some_and(|hold| hold.request.is_none())
            {
                conf.held.remove(&nonce);
            }
        }
    }
}

/// A transient error before the resend of a journaled release: it stays `Processing` and
/// in flight here and retries with backoff, then parks through the gate at the ceiling.
async fn hold_or_park(
    state: &mut SenderState,
    ctx: &TransactionContext,
    request: &ConfidentialReleaseRequest,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    reason: &str,
) {
    let now = Utc::now();
    let nonce = request.nonce;
    let Some(conf) = state.confidential.as_mut() else {
        return;
    };
    let since = conf.held.get(&nonce).map_or(now, |hold| hold.since);
    if (now - since).to_std().unwrap_or_default() >= TRANSIENT_FAILURE_CEILING {
        conf.held.remove(&nonce);
        park_through_journal(
            state,
            ctx,
            storage_tx,
            metrics::BAIL_REASON_CONFIDENTIAL_TRANSIENT_FAILURE,
            reason,
        )
        .await;
        return;
    }
    let (next_at, _) = next_wait(now, Some(since));
    warn!(nonce, %next_at, "Confidential release held in the sender: {reason}");
    conf.held.insert(
        nonce,
        TransientHold {
            since,
            next_at,
            refreshed_at: now,
            request: Some(request.clone()),
        },
    );
    refresh_held_lease(state, nonce, request.transaction_id).await;
}

/// Bump a held row's lease so recovery does not take it as abandoned. A row that is no
/// longer ours is left to whoever took it; a failed write is tried again next interval.
async fn refresh_held_lease(state: &mut SenderState, nonce: u64, transaction_id: i64) {
    let Some(lease) = state.release_leases.get(&nonce).copied() else {
        return;
    };
    if let Some(hold) = state
        .confidential
        .as_mut()
        .and_then(|conf| conf.held.get_mut(&nonce))
    {
        hold.refreshed_at = Utc::now();
    }
    match state
        .storage
        .refresh_release_lease(transaction_id, lease)
        .await
    {
        Ok(Some(new_lease)) => {
            state.release_leases.insert(nonce, new_lease);
        }
        Ok(None) => {
            warn!(
                nonce,
                "Held release is no longer ours; leaving it to recovery"
            );
            leave_to_recovery(state, nonce);
        }
        Err(e) => warn!(nonce, "Could not refresh the held release's lease: {e}"),
    }
}

/// Submit a confidential release: verify the balance, apply if needed, prove, journal,
/// send as v1, and route the outcome. `arm_lease` is false when the caller (the rotation
/// drain) already holds a newer lease.
pub async fn submit_release(
    state: &mut SenderState,
    request: ConfidentialReleaseRequest,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cause: AttemptCause,
    arm_lease: bool,
) {
    let ctx = release_ctx(&request);
    let nonce = request.nonce;
    if arm_lease {
        state
            .release_leases
            .insert(nonce, request.fetched_updated_at);
        // A fresh pickup holds a newer lease than any hold of this nonce.
        if let Some(conf) = state.confidential.as_mut() {
            conf.held.remove(&nonce);
        }
    }
    if let Some(info) = &request.remint_info {
        state.remint_cache.insert(nonce, info.clone());
    }
    if state.confidential.is_none() {
        park(
            state,
            &ctx,
            &request,
            storage_tx,
            metrics::BAIL_REASON_CONFIDENTIAL_MINT_NOT_CONFIGURED,
            "the sender has no confidential keys",
        )
        .await;
        return;
    }
    state.in_flight_withdrawals.insert(nonce);
    if let Some(conf) = state.confidential.as_mut() {
        conf.stale_budget.entry(nonce).or_insert(STALE_PROOF_BUDGET);
        conf.release_mints.insert(nonce, request.mint);
    }

    let mut cause = cause;
    let mut prepared: Option<(Vec<solana_sdk::instruction::Instruction>, u64)> = None;
    // Set once a send below the maximum ran out of compute: the resend goes at the maximum.
    let mut at_max = false;
    loop {
        if prepared.is_none() || !reuse_proofs(cause) {
            match prepare_release(state, &request).await {
                Ok(Prepared::Ready {
                    instructions,
                    verified,
                }) => prepared = Some((instructions, verified)),
                Ok(Prepared::Wait(reason)) => {
                    wait_or_park(state, &ctx, &request, storage_tx, &reason).await;
                    return;
                }
                Ok(Prepared::Blocked(reason)) => {
                    wait_or_leave(state, &request, &reason).await;
                    return;
                }
                Ok(Prepared::Park(label, reason)) => {
                    park(state, &ctx, &request, storage_tx, label, &reason).await;
                    return;
                }
                // A read or apply failed: like a build or sign error, the row waits before
                // any attempt and is held in the sender after one.
                Err(reason) => {
                    warn!(
                        nonce,
                        "Could not prepare the confidential release: {reason}"
                    );
                    wait_or_park(state, &ctx, &request, storage_tx, &reason).await;
                    return;
                }
            }
        }
        let (instructions, verified) = prepared.clone().expect("prepared above");
        let ix = release_ix(instructions.clone(), CU_CONFIDENTIAL_RELEASE_MAX);

        // Retry bound shared with public releases.
        let attempts = state.retry_counts.get(&nonce).copied().unwrap_or(0);
        if attempts >= state.retry_max_attempts {
            refund_failure_journaled_first(state, &ctx, storage_tx, "Max retries exceeded", false)
                .await;
            return;
        }
        state.retry_counts.insert(nonce, attempts + 1);

        let signed = match build_and_sign_v1(
            &state.rpc_client,
            &ix,
            LOADED_ACCOUNTS_DATA_SIZE_LIMIT,
        )
        .await
        {
            Ok(signed) => signed,
            Err(V1Error::Build(
                e @ (V1BuildError::TooLarge { .. } | V1BuildError::TooManyAccounts { .. }),
            )) => {
                park(
                    state,
                    &ctx,
                    &request,
                    storage_tx,
                    metrics::BAIL_REASON_CONFIDENTIAL_TX_TOO_LARGE,
                    &e.to_string(),
                )
                .await;
                return;
            }
            Err(e) => {
                unspend_retry(state, nonce);
                wait_or_park(
                    state,
                    &ctx,
                    &request,
                    storage_tx,
                    &format!("build/sign: {e}"),
                )
                .await;
                return;
            }
        };

        // Simulated at the maximum no older than our last landing, then signed again on the same
        // blockhash with what it used. A node behind the floor is a transient error.
        let floor = state
            .confidential
            .as_ref()
            .map(|conf| conf.min_context_slot);
        let simulated = match state
            .rpc_client
            .simulate_versioned_transaction(&signed.transaction, floor)
            .await
        {
            Ok(simulated) => simulated,
            // Says nothing about the release, so it is handled like a build error.
            Err(e) => {
                unspend_retry(state, nonce);
                let reason = format!("simulation: {e}");
                wait_or_park(state, &ctx, &request, storage_tx, &reason).await;
                return;
            }
        };
        let (signed, ix, refused) = match simulated.err {
            Some(err) => {
                let err = solana_sdk::transaction::TransactionError::from(err);
                if err == solana_sdk::transaction::TransactionError::BlockhashNotFound {
                    unspend_retry(state, nonce);
                    let reason = format!("simulation: {err}");
                    wait_or_park(state, &ctx, &request, storage_tx, &reason).await;
                    return;
                }
                let origin = simulated.logs.as_deref().and_then(failing_program);
                let failure = classify_attempt(Some(&err), origin, &request.hook_extras);
                (
                    signed,
                    ix,
                    Some((failure, None, format!("simulation: {err}"))),
                )
            }
            None => {
                let limit = if at_max {
                    CU_CONFIDENTIAL_RELEASE_MAX
                } else {
                    release_compute_limit(simulated.units_consumed)
                };
                let ix = release_ix(instructions, limit);
                match resign_v1(&ix, LOADED_ACCOUNTS_DATA_SIZE_LIMIT, &signed.blockhash()).await {
                    Ok(signed) => (signed, ix, None),
                    Err(e) => {
                        unspend_retry(state, nonce);
                        let reason = format!("build/sign: {e}");
                        wait_or_park(state, &ctx, &request, storage_tx, &reason).await;
                        return;
                    }
                }
            }
        };
        // A refusal seen in simulation was never journaled or sent.
        let sent = refused.is_none().then_some(signed.signature);
        let failure = if refused.is_some() {
            refused
        } else {
            let Some(lease) = state.release_leases.get(&nonce).copied() else {
                leave_to_recovery(state, nonce);
                return;
            };
            let pt = state.program_type.as_label();
            match claim_and_persist_or_abort(
                &state.storage,
                &state.solvency_fence,
                pt,
                request.transaction_id,
                lease,
                &signed.signature,
                signed.last_valid_block_height,
                signed.blockhash_slot,
                "release_claim_lost",
            )
            .await
            {
                SignatureClaim::Owned(new_lease) => {
                    state.release_leases.insert(nonce, new_lease);
                }
                SignatureClaim::Lost | SignatureClaim::Failed => {
                    leave_to_recovery(state, nonce);
                    return;
                }
            }

            match state
                .rpc_client
                .send_versioned_transaction(&signed.transaction, RetryPolicy::Idempotent, floor)
                .await
            {
                // Resent with the same proofs, inside the retry budget.
                Err(e) if resendable(&e) => {
                    warn!(nonce, "Confidential release send failed, resending: {e}");
                    cause = AttemptCause::TimeoutResend;
                    continue;
                }
                Err(e) => match preflight_failure(&e) {
                    Some((err, origin)) => {
                        let failure = classify_attempt(err.as_ref(), origin, &request.hook_extras);
                        Some((failure, None, e.to_string()))
                    }
                    None => {
                        refund_failure_journaled_first(
                            state,
                            &ctx,
                            storage_tx,
                            &e.to_string(),
                            false,
                        )
                        .await;
                        return;
                    }
                },
                Ok(_) => {
                    state
                        .pending_signatures
                        .entry(nonce)
                        .or_default()
                        .push(PendingSig {
                            signature: signed.signature,
                            last_valid_block_height: signed.last_valid_block_height,
                            blockhash_slot: Some(signed.blockhash_slot),
                        });
                    match confirm(
                        &state.rpc_client,
                        &signed.signature,
                        state.confirmation_poll_interval_ms,
                    )
                    .await
                    {
                        Ok(Confirmation::Confirmed { slot }) => {
                            mark_settled(state, request.transaction_id);
                            settle_local(state, &request.mint, verified, request.amount, slot)
                                .await;
                            handle_success(state, &ctx, signed.signature, storage_tx).await;
                            forget_release(state, nonce);
                            return;
                        }
                        Ok(Confirmation::Timeout) => {
                            cause = AttemptCause::TimeoutResend;
                            continue;
                        }
                        Ok(Confirmation::Failed { slot, err, origin }) => Some((
                            classify_attempt(Some(&err), origin, &request.hook_extras),
                            Some(slot),
                            format!("{err} from {origin:?}"),
                        )),
                        // The status is unknown, not failed: resend within the retry budget.
                        Err(e) => {
                            warn!(
                                nonce,
                                "Confidential release status poll failed, resending: {e}"
                            );
                            cause = AttemptCause::TimeoutResend;
                            continue;
                        }
                    }
                }
            }
        };

        let Some((failure, slot, detail)) = failure else {
            return;
        };
        match failure {
            ConfidentialFailure::RebuildProofs => {
                let left = state
                    .confidential
                    .as_mut()
                    .and_then(|conf| conf.stale_budget.get_mut(&nonce))
                    .map(|left| {
                        *left = left.saturating_sub(1);
                        *left
                    })
                    .unwrap_or(0);
                if left == 0 {
                    // A simulation refusal leaves nothing journaled, so `park` picks the path.
                    park(
                        state,
                        &ctx,
                        &request,
                        storage_tx,
                        metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF,
                        &format!("proofs refused {STALE_PROOF_BUDGET} times: {detail}"),
                    )
                    .await;
                    return;
                }
                // The stale budget, not the timeout budget, bounds these.
                if let Some(count) = state.retry_counts.get_mut(&nonce) {
                    *count = count.saturating_sub(1);
                }
                cause = AttemptCause::StaleProofs;
            }
            // Rebuilt from a fresh read on the retry budget, as a timeout would be.
            ConfidentialFailure::OriginUnproven => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[
                        state.program_type.as_label(),
                        "escrow_error_origin_unproven",
                    ])
                    .inc();
                warn!(
                    nonce,
                    "Confidential release failed with no provable origin, retrying: {detail}"
                );
                cause = AttemptCause::StaleProofs;
            }
            // Sent below the maximum, so the maximum may still fit: one fresh attempt at it.
            ConfidentialFailure::Park(metrics::BAIL_REASON_CONFIDENTIAL_COMPUTE_EXCEEDED)
                if sent.is_some() && ix.compute_budget < Some(CU_CONFIDENTIAL_RELEASE_MAX) =>
            {
                warn!(
                    nonce,
                    "Confidential release ran out of compute below the maximum, resending at it: {detail}"
                );
                at_max = true;
                cause = AttemptCause::TimeoutResend;
            }
            ConfidentialFailure::Park(label) => {
                park(state, &ctx, &request, storage_tx, label, &detail).await;
                return;
            }
            ConfidentialFailure::Escrow(PrivateChannelEscrowProgramError::NonceAlreadyUsed) => {
                if handle_nonce_already_used(state, &ctx, signed.signature, storage_tx).await {
                    mark_settled(state, request.transaction_id);
                    settle_local(
                        state,
                        &request.mint,
                        verified,
                        request.amount,
                        slot.unwrap_or(0),
                    )
                    .await;
                }
                forget_release(state, nonce);
                return;
            }
            ConfidentialFailure::Escrow(
                PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration,
            ) => {
                if let Some(conf) = state.confidential.as_mut() {
                    conf.requests.insert(nonce, request.clone());
                }
                handle_nonce_outside_generation(
                    state,
                    &ctx,
                    sent,
                    slot.unwrap_or(0),
                    ix,
                    storage_tx,
                )
                .await;
                return;
            }
            ConfidentialFailure::Escrow(_) | ConfidentialFailure::Other => {
                refund_failure_journaled_first(state, &ctx, storage_tx, &detail, false).await;
                return;
            }
        }
    }
}

/// The compute limit a release is sent with: what its simulation used plus a fifth, capped
/// at the maximum it was simulated with.
pub fn release_compute_limit(units_consumed: Option<u64>) -> u32 {
    units_consumed
        .map(|units| units.saturating_mul(6).div_ceil(5))
        .and_then(|limit| u32::try_from(limit).ok())
        .map_or(CU_CONFIDENTIAL_RELEASE_MAX, |limit| {
            limit.min(CU_CONFIDENTIAL_RELEASE_MAX)
        })
}

/// Ids kept for releases settled here; a row update lands within moments, so older ids go.
const SETTLED_RELEASES_KEPT: usize = 4_096;

/// The release's row update is on its way; until it lands the row must not count as unsettled.
fn mark_settled(state: &mut SenderState, transaction_id: i64) {
    if let Some(conf) = state.confidential.as_mut() {
        if conf.settled_releases.len() >= SETTLED_RELEASES_KEPT {
            conf.settled_releases.clear();
        }
        conf.settled_releases.insert(transaction_id);
    }
}

/// A release of ours landed: available is now the verified value minus the amount.
async fn settle_local(
    state: &mut SenderState,
    mint: &Pubkey,
    verified: u64,
    amount: u64,
    slot: u64,
) {
    record_landing(state, slot).await;
    if let Some(conf) = state.confidential.as_mut() {
        let book = conf.books.entry(*mint).or_default();
        book.local = verified.checked_sub(amount);
        // A release rewrites the AES copy correctly, so a stale episode is over.
        end_stale_episode(book, mint);
    }
    if let Some(available) = verified.checked_sub(amount) {
        remember_available(state, mint, available, slot).await;
    }
}

/// The rotation drain reached a confidential release: rebuild it from a fresh read.
pub async fn resubmit_after_rotation(
    state: &mut SenderState,
    nonce: u64,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) -> bool {
    let Some(request) = state
        .confidential
        .as_mut()
        .and_then(|conf| conf.requests.remove(&nonce))
    else {
        return false;
    };
    submit_release(
        state,
        request,
        storage_tx,
        AttemptCause::RotationDrain,
        false,
    )
    .await;
    true
}

// ---------------------------------------------------------------------------------------
// Background check and rebalances
// ---------------------------------------------------------------------------------------

/// The configured mints the background check reads: Token-2022 mints from the `mints` table.
async fn configured_mints(storage: &Storage) -> Vec<Pubkey> {
    match storage.get_mint_addresses().await {
        Ok(rows) => rows
            .into_iter()
            .filter(|(_, program)| *program == spl_token_2022::ID.to_string())
            .filter_map(|(mint, _)| Pubkey::from_str(&mint).ok())
            .collect(),
        Err(e) => {
            warn!("Confidential background check could not list mints: {e}");
            Vec::new()
        }
    }
}

/// Every 60 s: read each configured escrow account, apply at the thresholds, repair a
/// stale copy whose backoff passed, and run pending rebalance requests. Reads only otherwise.
pub async fn background_check(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let Some(conf) = state.confidential.as_mut() else {
        return;
    };
    let now = Instant::now();
    if conf
        .last_background_check
        .is_some_and(|last| now.duration_since(last) < BACKGROUND_CHECK_INTERVAL)
    {
        return;
    }
    conf.last_background_check = Some(now);

    let halted = matches!(
        state.storage.is_reconciliation_halted().await,
        Ok(Some(_)) | Err(_)
    );
    // Public withdrawals never touch the escrow's confidential balance, so only a
    // confidential release of the same mint holds its apply or repair.
    let releasing = match state.confidential.as_mut() {
        Some(conf) => releasing_mints(conf, &state.in_flight_withdrawals),
        None => return,
    };
    for mint in configured_mints(&state.storage).await {
        let release_in_flight = releasing.contains(&mint);
        if let Err(e) = check_mint(state, &mint, halted, release_in_flight).await {
            warn!(%mint, "Confidential background check: {e}");
        }
    }
    run_rebalances(state, storage_tx, halted).await;
    report_parks_and_generation(state).await;
}

/// Mints with a confidential release still in flight; settled entries are dropped.
pub fn releasing_mints(
    conf: &mut ConfidentialSender,
    in_flight: &std::collections::HashSet<u64>,
) -> std::collections::HashSet<Pubkey> {
    conf.release_mints
        .retain(|nonce, _| in_flight.contains(nonce));
    conf.release_mints.values().copied().collect()
}

/// Share of `generation`'s nonces issued, given the highest nonce the DB holds.
pub fn generation_used_ratio(max_nonce: Option<u64>, generation: u64) -> f64 {
    use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
    let start = generation.saturating_mul(NONCES_PER_GENERATION);
    let used = max_nonce.map_or(0, |nonce| nonce.saturating_add(1).saturating_sub(start));
    (used.min(NONCES_PER_GENERATION) as f64) / NONCES_PER_GENERATION as f64
}

async fn report_parks_and_generation(state: &SenderState) {
    let pt = state.program_type.as_label();
    if let Ok(oldest) = state.storage.oldest_confidential_park().await {
        let age = oldest.map_or(0, |at| (Utc::now() - at).num_seconds().max(0));
        metrics::OPERATOR_CONFIDENTIAL_OLDEST_PARK_SECONDS
            .with_label_values(&[pt])
            .set(age as f64);
    }
    if let (Some(generation), Ok(max_nonce)) = (
        state.cached_generation,
        state.storage.get_max_withdrawal_nonce().await,
    ) {
        metrics::OPERATOR_BITMAP_GENERATION_USED_RATIO
            .with_label_values(&[pt])
            .set(generation_used_ratio(max_nonce, generation));
    }
}

async fn check_mint(
    state: &mut SenderState,
    mint: &Pubkey,
    halted: bool,
    release_in_flight: bool,
) -> Result<(), String> {
    let conf = state.confidential.as_ref().ok_or("no keys")?;
    let Some((view, _)) = read_ours(&state.rpc_client, conf, mint).await? else {
        return Ok(());
    };
    if view.maximum_pending_credit_counter > 0 {
        metrics::OPERATOR_CONFIDENTIAL_PENDING_COUNTER
            .with_label_values(&[&mint.to_string()])
            .set(view.pending_credit_counter as f64 / view.maximum_pending_credit_counter as f64);
    }
    // Token-2022 refuses an apply on a frozen account, so each one would only cost a fee.
    let conf = state.confidential.as_mut().ok_or("no keys")?;
    if hold_while_frozen(conf.books.entry(*mint).or_default(), mint, view.frozen) {
        return Ok(());
    }
    let conf = state.confidential.as_ref().ok_or("no keys")?;
    let blocked = mint_withdrawals_blocked(state, mint).await?;
    if let Some(reason) = background_apply_due_off_executor(
        conf.keys.clone(),
        &view,
        halted,
        blocked,
        release_in_flight,
    )
    .await
    {
        info!(%mint, ?reason, "Background check applies pending");
        return apply_and_repair(state, mint, ApplyTrigger::Background, None).await;
    }
    if halted || blocked || release_in_flight || !aes_is_stale(&conf.keys, &view) {
        return Ok(());
    }
    // Stale with nothing scheduled covers a crash between an apply and its repair.
    let repair = &conf
        .books
        .get(mint)
        .map(|book| book.repair.clone())
        .unwrap_or_default();
    if repair.deferred_until.is_none() || repair.deferred_repair_due(Instant::now()) {
        return apply_and_repair(state, mint, ApplyTrigger::Repair, None).await;
    }
    Ok(())
}

/// Whether background applies and repairs hold for a frozen escrow account.
pub fn hold_while_frozen(book: &mut MintBook, mint: &Pubkey, frozen: bool) -> bool {
    if !frozen {
        book.frozen_warned = false;
        return false;
    }
    let first = !std::mem::replace(&mut book.frozen_warned, true);
    if first {
        warn!(%mint, "Escrow account is frozen; background applies and repairs wait until it is thawed");
    }
    true
}

async fn mint_withdrawals_blocked(state: &SenderState, mint: &Pubkey) -> Result<bool, String> {
    let conf = state.confidential.as_ref().ok_or("no keys")?;
    let pda = crate::operator::find_allowed_mint_pda(&conf.instance, mint);
    let response = state
        .rpc_client
        .get_account_with_context_min_slot(
            &pda,
            CommitmentConfig::confirmed(),
            Some(conf.min_context_slot),
        )
        .await
        .map_err(|e| format!("allowed mint read: {e}"))?;
    Ok(response
        .value
        .and_then(|account| {
            private_channel_escrow_program_client::AllowedMint::from_bytes(&account.data).ok()
        })
        .is_none_or(|allowed| allowed.withdrawals_blocked))
}

/// `indexer rebalance`: record a request under the operator's `request_id`, refused if that id
/// was used before. The sender runs it later and refuses it if the balance no longer allows it.
#[allow(clippy::too_many_arguments)]
pub async fn request_rebalance(
    storage: &Storage,
    rpc: &RpcClientWithRetry,
    keys: &Keys,
    instance: &Pubkey,
    mint: &Pubkey,
    direction: RebalanceDirection,
    amount: u64,
    request_id: &str,
) -> Result<i64, String> {
    if request_id.is_empty() {
        return Err("a request id is required".into());
    }
    if amount == 0 {
        return Err("amount must be above zero".into());
    }
    let (read, _) = read_escrow(rpc, &escrow_ata(instance, mint), 0).await?;
    let ConfidentialAccountRead::Configured(view) = read else {
        return Err(format!("escrow account for {mint} is not configured"));
    };
    if view.elgamal_pubkey != keys.pubkey() {
        return Err(format!(
            "escrow account for {mint} is configured with another key"
        ));
    }
    let (available, _) = verify_available(keys, &view, None, None).ok_or_else(|| {
        format!("available balance of {mint} does not verify yet (AES copy awaiting repair); retry shortly")
    })?;
    let request = RebalanceRequest {
        id: 0,
        mint: mint.to_string(),
        direction,
        amount,
        expected_available: available,
        request_id: request_id.to_string(),
        status: rebalance_status::PENDING.into(),
        signature: None,
        error: None,
        last_valid_block_height: None,
        blockhash_slot: None,
    };
    if let RebalanceDecision::Refuse(reason) =
        rebalance_decision(&request, Some(available), view.public_amount)
    {
        return Err(reason);
    }
    storage
        .insert_rebalance_request(&request.mint, direction, amount, available, request_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("rebalance request id {request_id} was already used; pick a new id"))
}

/// Run pending rebalance requests, and settle any left `sent` by a crash.
pub async fn run_rebalances(
    state: &mut SenderState,
    _storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    halted: bool,
) {
    // A request sent before a crash is settled by its signature, never re-run.
    if let Ok(sent) = state
        .storage
        .get_rebalance_requests(rebalance_status::SENT)
        .await
    {
        for request in sent {
            settle_sent_rebalance(state, &request).await;
        }
    }
    if halted {
        return;
    }
    let Ok(pending) = state
        .storage
        .get_rebalance_requests(rebalance_status::PENDING)
        .await
    else {
        return;
    };
    for request in pending {
        if let Err(e) = execute_rebalance(state, &request).await {
            warn!(id = request.id, "Rebalance request: {e}");
        }
    }
}

/// Settle a request left `sent` by the rule releases use: completed once its attempt
/// finalized, failed only once its blockhash expired and history proves it absent,
/// otherwise left `sent` for the next check.
async fn settle_sent_rebalance(state: &SenderState, request: &RebalanceRequest) {
    use super::remint::{classify_signatures, FinalityRpc, SigFinality};
    let Some(Ok(signature)) = request.signature.as_deref().map(Signature::from_str) else {
        warn!(
            id = request.id,
            "Rebalance request is sent without a signature; resolve it by hand"
        );
        return;
    };
    let finality = FinalityRpc::solana(&state.rpc_client, state.fallback_rpc_client.as_deref());
    let verdict = match request.last_valid_block_height {
        Some(last_valid_block_height) => {
            let attempt = PendingSig {
                signature,
                last_valid_block_height,
                blockhash_slot: request.blockhash_slot,
            };
            classify_signatures(&finality, &[attempt]).await
        }
        // Journaled before its height was recorded: only a landing can settle it.
        None => match classify_signatures(
            &finality,
            &[PendingSig {
                signature,
                last_valid_block_height: u64::MAX,
                blockhash_slot: None,
            }],
        )
        .await
        {
            SigFinality::Landed(sig) => SigFinality::Landed(sig),
            _ => SigFinality::Uncertain("no journaled blockhash height".into()),
        },
    };
    let (to, error) = match verdict {
        SigFinality::Landed(_) => (rebalance_status::COMPLETED, None),
        SigFinality::Dead => (
            rebalance_status::FAILED,
            Some(format!(
                "{signature} did not land and can no longer; re-request after checking the balance"
            )),
        ),
        SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
            info!(id = request.id, %signature, "Rebalance request still undecided: {reason}");
            return;
        }
    };
    let _ = state
        .storage
        .transition_rebalance_request(request.id, rebalance_status::SENT, to, None, error)
        .await;
}

/// Execute one request if the balance it spends still equals what it was made against.
pub async fn execute_rebalance(
    state: &mut SenderState,
    request: &RebalanceRequest,
) -> Result<(), String> {
    let mint = Pubkey::from_str(&request.mint).map_err(|e| format!("mint: {e}"))?;
    if let Some(reason) = write_blocked(state, &mint, Write::Rebalance).await? {
        info!(id = request.id, "Rebalance waits: {reason}");
        return Ok(());
    }
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let Some((view, _)) = read_ours(&state.rpc_client, conf, &mint).await? else {
        return refuse(state, request, "escrow account not configured with our key").await;
    };
    let verified = verified_available(&state.rpc_client, conf, &mint, &view).await;
    let available = match rebalance_decision(request, verified.map(|(v, _)| v), view.public_amount)
    {
        RebalanceDecision::Wait => return Ok(()),
        RebalanceDecision::Refuse(reason) => return refuse(state, request, &reason).await,
        RebalanceDecision::Execute => verified.map_or(0, |(v, _)| v),
    };
    let accounts = conf.accounts(&mint);
    let (signature, slot) = match request.direction {
        RebalanceDirection::ToPublic => {
            let proofs = conf
                .keys
                .prove_move_to_public(&view.available, available, request.amount)
                .map_err(|e| format!("proofs: {e}"))?;
            let instructions = move_to_public_instructions(&accounts, request.amount, &proofs);
            journal_and_send(state, request, instructions, true).await?
        }
        RebalanceDirection::ToConfidential => {
            let instructions = vec![move_to_confidential_instruction(&accounts, request.amount)];
            journal_and_send(state, request, instructions, false).await?
        }
    };
    record_landing(state, slot).await;
    if let Some(conf) = state.confidential.as_mut() {
        let book = conf.books.entry(mint).or_default();
        book.local = match request.direction {
            RebalanceDirection::ToPublic => {
                // MoveToPublic rewrites the AES copy, as a release does.
                end_stale_episode(book, &mint);
                available.checked_sub(request.amount)
            }
            // Moves into pending; available is unchanged until the next apply.
            RebalanceDirection::ToConfidential => Some(available),
        };
        if let Some(local) = book.local {
            remember_available(state, &mint, local, slot).await;
        }
    }
    state
        .storage
        .transition_rebalance_request(
            request.id,
            rebalance_status::SENT,
            rebalance_status::COMPLETED,
            Some(signature.to_string()),
            None,
        )
        .await
        .map_err(|e| e.to_string())?;
    info!(id = request.id, %signature, "Rebalance executed");
    Ok(())
}

async fn refuse(
    state: &SenderState,
    request: &RebalanceRequest,
    reason: &str,
) -> Result<(), String> {
    warn!(id = request.id, "Refusing rebalance: {reason}");
    state
        .storage
        .transition_rebalance_request(
            request.id,
            rebalance_status::PENDING,
            rebalance_status::REFUSED,
            None,
            Some(reason.to_string()),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Journal the signature on the request (`pending` to `sent`) before broadcasting.
async fn journal_and_send(
    state: &SenderState,
    request: &RebalanceRequest,
    instructions: Vec<solana_sdk::instruction::Instruction>,
    v1: bool,
) -> Result<(Signature, u64), String> {
    let signers = TransactionBuilder::release_signers();
    let ix = InstructionWithSigners {
        instructions,
        fee_payer: signers[0].pubkey(),
        signers,
        compute_unit_price: Some(1),
        compute_budget: Some(if v1 {
            CU_CONFIDENTIAL_RELEASE
        } else {
            CU_CONFIDENTIAL_APPLY
        }),
    };
    if v1 {
        let signed = build_and_sign_v1(&state.rpc_client, &ix, LOADED_ACCOUNTS_DATA_SIZE_LIMIT)
            .await
            .map_err(|e| e.to_string())?;
        claim_rebalance(
            state,
            request,
            &signed.signature,
            signed.last_valid_block_height,
            signed.blockhash_slot,
        )
        .await?;
        state
            .rpc_client
            .send_versioned_transaction(
                &signed.transaction,
                RetryPolicy::Idempotent,
                state
                    .confidential
                    .as_ref()
                    .map(|conf| conf.min_context_slot),
            )
            .await
            .map_err(|e| e.to_string())?;
        await_rebalance(state, request, signed.signature).await
    } else {
        let (transaction, signature, last_valid_block_height, blockhash_slot) =
            build_and_sign(&state.rpc_client, ix)
                .await
                .map_err(|e| e.to_string())?;
        claim_rebalance(
            state,
            request,
            &signature,
            last_valid_block_height,
            blockhash_slot,
        )
        .await?;
        send_signed(&state.rpc_client, &transaction, RetryPolicy::Idempotent)
            .await
            .map_err(|e| e.to_string())?;
        await_rebalance(state, request, signature).await
    }
}

async fn claim_rebalance(
    state: &SenderState,
    request: &RebalanceRequest,
    signature: &Signature,
    last_valid_block_height: u64,
    blockhash_slot: u64,
) -> Result<(), String> {
    match state
        .storage
        .claim_rebalance_request(
            request.id,
            &signature.to_string(),
            last_valid_block_height,
            blockhash_slot,
        )
        .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err("request no longer pending".into()),
        Err(e) => Err(format!("journal: {e}")),
    }
}

async fn await_rebalance(
    state: &SenderState,
    request: &RebalanceRequest,
    signature: Signature,
) -> Result<(Signature, u64), String> {
    match confirm(
        &state.rpc_client,
        &signature,
        state.confirmation_poll_interval_ms,
    )
    .await
    {
        Ok(Confirmation::Confirmed { slot }) => Ok((signature, slot)),
        // Left `sent`: the next check settles it by this signature.
        Ok(Confirmation::Timeout) | Err(_) => Err(format!("{signature} not confirmed yet")),
        Ok(Confirmation::Failed { err, .. }) => {
            let code = custom_code(&err);
            let _ = state
                .storage
                .transition_rebalance_request(
                    request.id,
                    rebalance_status::SENT,
                    rebalance_status::FAILED,
                    None,
                    Some(format!("failed on-chain with code {code:?}")),
                )
                .await;
            Err(format!("{signature} failed with code {code:?}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProgramType;
    use crate::operator::recovery::STALE_THRESHOLD;
    use crate::operator::sender::test_support::{
        ensure_test_signer, push_processing_row, row_status, row_updated_at,
        sender_state_with_storage_and_role,
    };
    use crate::operator::utils::mint_util::confidential_fixtures::{
        account_with_confidential, confidential_mint, rpc_account, AccountSpec,
    };
    use crate::storage::common::models::StoredSig;
    use crate::storage::common::models::TransactionType;
    use crate::storage::common::storage::mock::MockStorage;
    use private_channel_confidential::test_support::{self as cts, SimAccount};
    use private_channel_escrow_program_client::instructions::ConfidentialReleaseFundsBuilder;

    fn view_of(spec: &AccountSpec) -> ConfidentialAccountView {
        let account = solana_sdk::account::Account {
            lamports: 1,
            data: account_with_confidential(spec),
            owner: spl_token_2022::ID,
            executable: false,
            rent_epoch: 0,
        };
        match decode_confidential_account(Some(&account)) {
            ConfidentialAccountRead::Configured(view) => *view,
            other => panic!("{other:?}"),
        }
    }

    /// Available `available` with the AES copy at `aes_value`, nothing pending.
    fn escrow(available: u64, aes_value: u64) -> (Keys, AccountSpec) {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, available, 0);
        sim.apply();
        let spec = AccountSpec::healthy(&keys, &sim, 0, aes_value);
        (keys, spec)
    }

    // ---- 19: candidates ----------------------------------------------------------------

    #[test]
    fn candidate_a_is_our_count_after_a_release() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 1_000, 0);
        sim.apply();
        sim.debit(&keys, 300);
        // The copy a release writes can be anything; (a) is tried first.
        let spec = AccountSpec::healthy(&keys, &sim, 0, 999);
        assert_eq!(
            verify_available(&keys, &view_of(&spec), Some(700), None),
            Some((700, CandidateSource::Local))
        );
    }

    #[test]
    fn candidate_b_is_the_aes_copy_when_our_count_is_wrong_or_missing() {
        let (keys, spec) = escrow(5_000, 5_000);
        let view = view_of(&spec);
        assert_eq!(
            verify_available(&keys, &view, None, None),
            Some((5_000, CandidateSource::Aes))
        );
        assert_eq!(
            verify_available(&keys, &view, Some(4_000), None),
            Some((5_000, CandidateSource::Aes))
        );
    }

    #[test]
    fn candidate_d_is_our_last_apply_plus_what_it_applied() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 500, 0);
        sim.apply();
        // The apply read 60,000 pending but a 10,000 deposit raced it.
        sim.credit(&keys, 60_000, 0);
        sim.credit(&keys, 10_000, 0);
        let (applied_lo, applied_hi) = (sim.pending_lo, sim.pending_hi);
        sim.apply();
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 60_500);
        spec.expected_pending_credit_counter = 1;
        spec.actual_pending_credit_counter = 2;
        let d = candidate_from_apply(&keys, 500, &applied_lo, &applied_hi, None);
        assert_eq!(d, Some(70_500));
        assert_eq!(
            verify_available(&keys, &view_of(&spec), None, d),
            Some((70_500, CandidateSource::LastApply))
        );
    }

    #[test]
    fn candidate_d_uses_the_wider_search() {
        let keys = cts::keys();
        // hi above 32 bits: 2^33 * 2^16 + 5.
        let lo = cts::encrypt(&keys, 5);
        let hi = cts::encrypt(&keys, 1 << 33);
        assert_eq!(
            candidate_from_apply(&keys, 7, &lo, &hi, None),
            Some(7 + 5 + (1u64 << 49))
        );
    }

    #[tokio::test]
    async fn candidate_a_holds_after_a_nonce_already_used_settle() {
        let (keys, mut spec) = escrow(1_000, 1_000);
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 1_000, 0);
        sim.apply();
        sim.debit(&keys, 400);
        spec.available = sim.available;
        let mut state = sender_state_with_storage_and_role(
            "http://127.0.0.1:1",
            MockStorage::new(),
            ProgramType::Withdraw,
        );
        state.confidential = Some(Box::new(ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        )));
        let mint = Pubkey::new_unique();
        settle_local(&mut state, &mint, 1_000, 400, 55).await;
        let conf = state.confidential.as_ref().unwrap();
        assert_eq!(conf.min_context_slot, 55);
        let local = conf.books[&mint].local;
        assert_eq!(
            verify_available(&keys, &view_of(&spec), local, None),
            Some((600, CandidateSource::Local))
        );
    }

    #[test]
    fn no_candidate_opens_a_balance_we_cannot_explain() {
        let (keys, spec) = escrow(5_000, 4_999);
        assert_eq!(
            verify_available(&keys, &view_of(&spec), Some(1), Some(2)),
            None
        );
    }

    #[test]
    fn a_known_stale_copy_is_recovered_from_the_gap_after_a_restart() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 70_500, 0);
        sim.apply();
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 500);
        // Counters equal: the gap is not trusted.
        assert_eq!(verify_available(&keys, &view_of(&spec), None, None), None);
        spec.expected_pending_credit_counter = 3;
        spec.actual_pending_credit_counter = 4;
        assert_eq!(
            verify_available(&keys, &view_of(&spec), None, None),
            Some((70_500, CandidateSource::StaleAesGap))
        );
    }

    /// Off the executor, every candidate case gives exactly the inline answer.
    #[tokio::test(flavor = "current_thread")]
    async fn candidates_off_the_executor_match_the_inline_search() {
        let keys = cts::keys();
        let shared = Arc::new(cts::keys());
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 500, 0);
        sim.apply();
        sim.credit(&keys, 60_000, 0);
        sim.credit(&keys, 10_000, 0);
        let applied = (500, sim.pending_lo, sim.pending_hi, None);
        sim.apply();
        let mut raced = AccountSpec::healthy(&keys, &sim, 0, 60_500);
        raced.expected_pending_credit_counter = 1;
        raced.actual_pending_credit_counter = 2;
        let cases = [
            (escrow(5_000, 5_000).1, None, None),
            (escrow(5_000, 5_000).1, Some(4_000), None),
            (escrow(5_000, 1).1, Some(5_000), Some(applied)),
            (raced.clone(), None, Some(applied)),
            (raced, None, None),
            (escrow(5_000, 4_999).1, Some(1), None),
        ];
        for (spec, local, apply) in cases {
            let view = view_of(&spec);
            let d = apply.and_then(|(prior, lo, hi, verified)| {
                candidate_from_apply(&keys, prior, &lo, &hi, verified)
            });
            assert_eq!(
                verify_available_off_executor(shared.clone(), &view, local, apply).await,
                verify_available(&keys, &view, local, d)
            );
        }
        let (_, view) = pending_view(&[70_000, 5], 40_000, 65_536);
        assert_eq!(
            pending_off_executor(shared.clone(), &view).await,
            keys.decrypt_pending(&view.pending_lo, &view.pending_hi)
        );
        for (halted, releasing) in [(false, false), (true, false), (false, true)] {
            assert_eq!(
                background_apply_due_off_executor(shared.clone(), &view, halted, false, releasing)
                    .await,
                background_apply_due(&keys, &view, halted, false, releasing)
            );
        }
    }

    // ---- 20: waiting -------------------------------------------------------------------

    #[test]
    fn waits_double_from_the_base_to_the_cap() {
        let start = Utc::now();
        let (first, started) = next_wait(start, None);
        assert_eq!(started, start);
        assert_eq!((first - start).to_std().unwrap(), WAIT_BACKOFF_BASE);
        let later = start + chrono::Duration::seconds(40);
        let (second, kept) = next_wait(later, Some(start));
        assert_eq!(kept, start);
        assert_eq!((second - later).num_seconds(), 40);
        let much_later = start + chrono::Duration::hours(2);
        let (third, _) = next_wait(much_later, Some(start));
        assert_eq!((third - much_later).to_std().unwrap(), WAIT_BACKOFF_CAP);
    }

    fn request_for(
        transaction_id: i64,
        mint: Pubkey,
        lease: DateTime<Utc>,
    ) -> ConfidentialReleaseRequest {
        ConfidentialReleaseRequest {
            builder: ConfidentialReleaseFundsBuilder::new(),
            nonce: 7,
            transaction_id,
            trace_id: "trace".into(),
            remint_info: Some(
                crate::operator::utils::instruction_util::WithdrawalRemintInfo {
                    transaction_id,
                    source_event_id: crate::operator::utils::instruction_util::SourceEventId::new(
                        "sig", 0, None,
                    ),
                    trace_id: "trace".into(),
                    mint,
                    user: Pubkey::new_unique(),
                    user_ata: Pubkey::new_unique(),
                    token_program: spl_token_2022::ID,
                    amount: 100,
                },
            ),
            fetched_updated_at: lease,
            mint,
            instance_ata: Pubkey::new_unique(),
            amount: 100,
            destination: Pubkey::new_unique(),
            hook_extras: Vec::new(),
            wait_started_at: None,
        }
    }

    /// A sender whose escrow account for one mint answers with `spec` at `slot`.
    async fn sender_reading(
        spec: &AccountSpec,
        slot: u64,
        mock: MockStorage,
    ) -> (SenderState, mockito::ServerGuard, Pubkey) {
        let mut server = mockito::Server::new_async().await;
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": rpc_account(&spl_token_2022::ID, &account_with_confidential(spec), slot),
        });
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getAccountInfo"}),
            ))
            .with_body(body.to_string())
            .create_async()
            .await;
        let mut state =
            sender_state_with_storage_and_role(&server.url(), mock, ProgramType::Withdraw);
        let instance = Pubkey::new_unique();
        state.confidential = Some(Box::new(ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            instance,
        )));
        (state, server, instance)
    }

    #[tokio::test]
    async fn before_any_attempt_an_unverifiable_balance_waits_in_pending() {
        let (_, spec) = escrow(5_000, 1);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        push_processing_row(&mock, 2);
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);

        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;

        assert_eq!(row_status(&mock, 1), Some(TransactionStatus::Pending));
        let row = mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == 1)
            .cloned()
            .unwrap();
        assert!(row.not_before.unwrap() > Utc::now());
        assert!(row.wait_started_at.is_some());
        assert_eq!(
            row.recovery_requeue_attempts, 0,
            "a wait spends no recovery attempt"
        );
        assert!(state.retry_counts.is_empty(), "a wait spends no retry");
        assert!(!state.in_flight_withdrawals.contains(&7));
        // The other row is still fetchable; the waiting one is not.
        mock.pending_transactions
            .lock()
            .unwrap()
            .iter_mut()
            .for_each(|t| {
                if t.id == 2 {
                    t.status = TransactionStatus::Pending
                }
            });
        let fetched = state
            .storage
            .get_and_lock_pending_transactions(crate::storage::TransactionType::Withdrawal, 10)
            .await
            .unwrap();
        assert_eq!(fetched.iter().map(|t| t.id).collect::<Vec<_>>(), vec![2]);
    }

    #[tokio::test]
    async fn after_a_journaled_attempt_an_unverifiable_balance_stays_in_the_sender() {
        let (_, spec) = escrow(5_000, 1);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        journal_one(&mock, 1);
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);

        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;

        assert_held(&state, &mock, 0);
    }

    /// A row that parked stale through the journal gate after a journaled attempt.
    async fn gated_park(mock: &MockStorage) -> SenderState {
        let (_, spec) = escrow(5_000, 1);
        push_processing_row(mock, 1);
        mock.pending_transactions.lock().unwrap()[0].confidential = true;
        journal_one(mock, 1);
        let (mut state, server, _) = sender_reading(&spec, 10, mock.clone()).await;
        std::mem::forget(server);
        let lease = row_updated_at(mock, 1).unwrap();
        let request = request_for(1, Pubkey::new_unique(), lease);
        state.release_leases.insert(7, lease);
        state
            .remint_cache
            .insert(7, request.remint_info.clone().unwrap());
        let (tx, _rx) = mpsc::channel(8);
        park_through_journal(
            &mut state,
            &release_ctx(&request),
            &tx,
            metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF,
            "proofs refused",
        )
        .await;
        assert_eq!(row_status(mock, 1), Some(TransactionStatus::PendingRemint));
        state
    }

    fn quarantined(label: &str) -> f64 {
        metrics::OPERATOR_TRANSACTION_QUARANTINED
            .with_label_values(&[ProgramType::Withdraw.as_label(), label])
            .get()
    }

    /// The park label is on the row, so a restart during the gate's wait still parks, and
    /// the park message leads with the label even though the original error was not stored.
    #[tokio::test]
    async fn a_restart_during_the_journal_gate_keeps_the_park() {
        let mock = MockStorage::new();
        drop(gated_park(&mock).await);
        assert_eq!(
            mock.confidential_sender.lock().unwrap().park_labels.get(&1),
            Some(&metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF.to_string())
        );

        // A new incarnation: nothing in memory but what boot restores.
        let (_, spec) = escrow(5_000, 1);
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let (tx, mut rx) = mpsc::channel(8);
        state.recover_pending_remints(&tx).await.unwrap();
        let conf = state.confidential.as_mut().unwrap();
        seed_parks(conf, &state.storage).await;
        assert_eq!(
            conf.parks.get(&1).map(String::as_str),
            Some(metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF)
        );
        let original = state.pending_remints[0].original_error.clone();
        assert!(finish_gated_park(&mut state, 1, None, &original, PROVEN_ABSENT, &tx).await);
        let update = rx.recv().await.unwrap();
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let message = update.error_message.unwrap();
        assert!(
            message.starts_with(metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF),
            "{message}"
        );
        assert!(message.ends_with(PROVEN_ABSENT), "{message}");
        // A parked row is no longer seeded at the next boot.
        mock.pending_transactions
            .lock()
            .unwrap()
            .iter_mut()
            .for_each(|t| t.status = TransactionStatus::ManualReview);
        assert!(state
            .storage
            .get_confidential_park_labels()
            .await
            .unwrap()
            .is_empty());
    }

    /// The remint gate's attempt cap on a gated park ends as the park: counted under its
    /// label, with the park message, and the entry cleared.
    #[tokio::test]
    async fn a_remint_cap_escalation_of_a_gated_park_finishes_as_the_park() {
        let label = metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF;
        let mock = MockStorage::new();
        let mut state = gated_park(&mock).await;
        let before = quarantined(label);
        // Every status lookup fails, so the gate can only defer until its cap.
        state.pending_remints[0].finality_check_attempts = 2;
        state.pending_remints[0].deadline = Utc::now() - chrono::Duration::seconds(1);
        let (tx, mut rx) = mpsc::channel(8);
        crate::operator::sender::remint::process_pending_remints(&mut state, &tx).await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 1);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let message = update.error_message.unwrap();
        assert!(message.starts_with(label), "{message}");
        assert!(
            message.contains("escalated to ManualReview after 3 attempts"),
            "{message}"
        );
        // Other tests count the same label concurrently.
        assert!(quarantined(label) >= before + 1.0);
        assert!(state.confidential.as_ref().unwrap().parks.is_empty());
        assert!(state.pending_remints.is_empty());
    }

    /// A sender with keys whose RPC answers only what `server` was told to.
    fn sender_on(server: &mockito::ServerGuard, mock: &MockStorage) -> SenderState {
        let mut state =
            sender_state_with_storage_and_role(&server.url(), mock.clone(), ProgramType::Withdraw);
        state.confidential = Some(Box::new(ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        )));
        state
    }

    /// A journaled row whose escrow account cannot be read stays in the sender.
    #[tokio::test]
    async fn an_unreadable_account_after_an_attempt_stays_in_the_sender() {
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        journal_one(&mock, 1);
        // Nothing answers getAccountInfo, so every read fails.
        let server = mockito::Server::new_async().await;
        let mut state = sender_on(&server, &mock);
        state.retry_counts.insert(7, 2);
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;
        assert_held(&state, &mock, 2);
    }

    /// A load-balanced node behind our floor answers "minimum context slot not reached".
    #[tokio::test]
    async fn a_node_behind_the_floor_after_an_attempt_stays_in_the_sender() {
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        journal_one(&mock, 1);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "error": {
                        "code": -32016,
                        "message": "Minimum context slot has not been reached",
                        "data": {"contextSlot": 5},
                    },
                })
                .to_string(),
            )
            .create_async()
            .await;
        let mut state = sender_on(&server, &mock);
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;
        assert_held(&state, &mock, 0);
    }

    /// The reviewer's case: attempt 1 timed out, then the resend's blockhash fetch fails.
    async fn held_after_a_blockhash_failure(
        mock: &MockStorage,
    ) -> (SenderState, mockito::ServerGuard) {
        let (_, spec) = escrow(5_000, 5_000);
        push_processing_row(mock, 1);
        journal_one(mock, 1);
        let (mut state, server, _) = sender_reading(&spec, 10, mock.clone()).await;
        state.retry_counts.insert(7, 1);
        let lease = row_updated_at(mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        submit_release(
            &mut state,
            full_request(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::TimeoutResend,
            true,
        )
        .await;
        (state, server)
    }

    #[tokio::test]
    async fn a_blockhash_failure_after_an_attempt_stays_in_the_sender() {
        let mock = MockStorage::new();
        let (state, _server) = held_after_a_blockhash_failure(&mock).await;
        assert_held(&state, &mock, 1);
    }

    /// A held row is retried when its backoff is due, keeps the time of its first failure,
    /// and parks `confidential_transient_failure` through the journal gate past the ceiling.
    #[tokio::test]
    async fn a_held_release_retries_with_backoff_then_parks_transient_after_the_ceiling() {
        let mock = MockStorage::new();
        let (mut state, _server) = held_after_a_blockhash_failure(&mock).await;
        let (tx, mut rx) = mpsc::channel(8);
        let first = held(&state).unwrap().since;

        // Not due yet: nothing is retried.
        retry_held(&mut state, &tx).await;
        assert!(held(&state).unwrap().request.is_some());

        // Due: retried, fails the same way, held again from the same first failure.
        let hold = state
            .confidential
            .as_mut()
            .unwrap()
            .held
            .get_mut(&7)
            .unwrap();
        hold.next_at = Utc::now() - chrono::Duration::seconds(1);
        retry_held(&mut state, &tx).await;
        assert_held(&state, &mock, 1);
        let hold = held(&state).unwrap();
        assert_eq!(hold.since, first);
        assert!(hold.next_at > Utc::now());

        // Past the ceiling: parked through the gate under its own label.
        let hold = state
            .confidential
            .as_mut()
            .unwrap()
            .held
            .get_mut(&7)
            .unwrap();
        hold.since = Utc::now()
            - chrono::Duration::from_std(TRANSIENT_FAILURE_CEILING).unwrap()
            - chrono::Duration::seconds(1);
        hold.next_at = Utc::now() - chrono::Duration::seconds(1);
        retry_held(&mut state, &tx).await;
        let label = metrics::BAIL_REASON_CONFIDENTIAL_TRANSIENT_FAILURE;
        assert_eq!(row_status(&mock, 1), Some(TransactionStatus::PendingRemint));
        let conf = state.confidential.as_ref().unwrap();
        assert_eq!(conf.parks.get(&1).map(String::as_str), Some(label));
        assert!(conf.held.is_empty());
        assert_eq!(state.pending_remints.len(), 1);
        assert!(finish_gated_park(&mut state, 1, None, "transient", PROVEN_ABSENT, &tx).await);
        let update = rx.recv().await.unwrap();
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(update.error_message.unwrap().starts_with(label));
    }

    /// A fresh pickup of a held nonce carries a newer lease, so it replaces the hold.
    #[tokio::test]
    async fn a_fresh_pickup_of_a_held_release_replaces_the_hold() {
        let mock = MockStorage::new();
        let (mut state, _server) = held_after_a_blockhash_failure(&mock).await;
        let earlier = Utc::now() - chrono::Duration::minutes(10);
        state
            .confidential
            .as_mut()
            .unwrap()
            .held
            .get_mut(&7)
            .unwrap()
            .since = earlier;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        submit_release(
            &mut state,
            full_request(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;
        assert!(held(&state).unwrap().since > earlier);
    }

    /// A held row refreshes its lease on every hold and once per recovery interval while it
    /// waits, so recovery never takes it; the hold keeps its start and still parks at the ceiling.
    #[tokio::test]
    async fn a_held_release_keeps_its_lease_past_the_stale_threshold() {
        let mock = MockStorage::new();
        let (mut state, _server) = held_after_a_blockhash_failure(&mock).await;
        let (tx, mut rx) = mpsc::channel(8);
        let first = held(&state).unwrap().since;
        let old = Utc::now()
            - chrono::Duration::from_std(STALE_THRESHOLD).unwrap()
            - chrono::Duration::minutes(1);

        // A retry that fails again is held again, with a fresh lease.
        age_lease(&mut state, &mock, old);
        held_mut(&mut state).next_at = Utc::now() - chrono::Duration::seconds(1);
        retry_held(&mut state, &tx).await;
        assert_held(&state, &mock, 1);
        assert_lease_fresh(&state, &mock).await;

        // Not due, but held for a whole recovery interval: the lease is refreshed anyway.
        age_lease(&mut state, &mock, old);
        let hold = held_mut(&mut state);
        hold.refreshed_at = old;
        hold.next_at = Utc::now() + chrono::Duration::minutes(5);
        retry_held(&mut state, &tx).await;
        assert!(held(&state).unwrap().request.is_some(), "not retried");
        assert_lease_fresh(&state, &mock).await;
        assert_eq!(held(&state).unwrap().since, first);

        // The ceiling still parks it under its own label.
        let hold = held_mut(&mut state);
        hold.since = Utc::now()
            - chrono::Duration::from_std(TRANSIENT_FAILURE_CEILING).unwrap()
            - chrono::Duration::seconds(1);
        hold.next_at = Utc::now() - chrono::Duration::seconds(1);
        retry_held(&mut state, &tx).await;
        let label = metrics::BAIL_REASON_CONFIDENTIAL_TRANSIENT_FAILURE;
        assert_eq!(row_status(&mock, 1), Some(TransactionStatus::PendingRemint));
        assert!(finish_gated_park(&mut state, 1, None, "transient", PROVEN_ABSENT, &tx).await);
        let update = rx.recv().await.unwrap();
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(update.error_message.unwrap().starts_with(label));
    }

    /// Age the row and the sender's lease on it together, as time passing would.
    fn age_lease(state: &mut SenderState, mock: &MockStorage, at: DateTime<Utc>) {
        let mut rows = mock.pending_transactions.lock().unwrap();
        rows.iter_mut().find(|t| t.id == 1).unwrap().updated_at = at;
        state.release_leases.insert(7, at);
    }

    /// Recovery's stale read skips the row, and the sender holds the row's current lease.
    async fn assert_lease_fresh(state: &SenderState, mock: &MockStorage) {
        let stale = mock
            .get_stale_processing_transactions(STALE_THRESHOLD, 100, TransactionType::Withdrawal)
            .await
            .unwrap();
        assert!(stale.is_empty(), "recovery would take the held row");
        assert_eq!(
            state.release_leases.get(&7).copied(),
            row_updated_at(mock, 1)
        );
    }

    fn held_mut(state: &mut SenderState) -> &mut TransientHold {
        state
            .confidential
            .as_mut()
            .unwrap()
            .held
            .get_mut(&7)
            .unwrap()
    }

    fn held(state: &SenderState) -> Option<&TransientHold> {
        state.confidential.as_ref().unwrap().held.get(&7)
    }

    /// Still `Processing` and in flight in the sender, not parked, no budget spent.
    fn assert_held(state: &SenderState, mock: &MockStorage, retries: u32) {
        assert_eq!(row_status(mock, 1), Some(TransactionStatus::Processing));
        let conf = state.confidential.as_ref().unwrap();
        assert!(conf.parks.is_empty(), "not parked");
        assert!(state.pending_remints.is_empty());
        assert!(held(state).is_some_and(|hold| hold.request.is_some()));
        assert_eq!(conf.stale_budget.get(&7), Some(&STALE_PROOF_BUDGET));
        assert_eq!(state.retry_counts.get(&7).copied().unwrap_or(0), retries);
        assert!(
            state.in_flight_withdrawals.contains(&7),
            "an attempt may still land"
        );
        assert!(state.release_leases.contains_key(&7));
    }

    fn journal_one(mock: &MockStorage, transaction_id: i64) {
        mock.release_signatures.lock().unwrap().insert(
            transaction_id,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 100,
                blockhash_slot: Some(9),
            }],
        );
    }

    /// `request_for` with every account the escrow instruction needs, signed by the test signers.
    fn full_request(
        transaction_id: i64,
        mint: Pubkey,
        lease: DateTime<Utc>,
    ) -> ConfidentialReleaseRequest {
        ensure_test_signer();
        let mut request = request_for(transaction_id, mint, lease);
        let any = Pubkey::new_unique;
        request
            .builder
            .payer(SignerUtil::get_admin_pubkey())
            .operator(SignerUtil::get_operator_pubkey())
            .instance(any())
            .withdrawal_bitmap(any())
            .operator_pda(any())
            .mint(mint)
            .allowed_mint(any())
            .user_ata(any())
            .instance_ata(request.instance_ata)
            .token_program(spl_token_2022::ID)
            .event_authority(any())
            .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
            .user_allowed_wallet_pda(any())
            .withdrawer_allowed_wallet_pda(any())
            .user(any())
            .withdrawer(any())
            .transaction_nonce(7);
        request
    }

    /// How the mock answers a release's simulation, send and status poll.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum SendReply {
        /// The node is unreachable: not a preflight refusal.
        Unreachable,
        /// Preflight refuses with `BlockhashNotFound`.
        BlockhashNotFound,
        /// Accepted, then every status poll errors.
        AcceptedThenStatusFails,
        /// Accepted, then found failed with escrow error 33 and an unreadable log.
        FailedWithUnreadableLog,
        /// Accepted, then found failed out of compute.
        LandedOutOfCompute,
        /// The simulation runs out of compute.
        SimulatedOutOfCompute,
        /// The simulating node is behind the read floor.
        SimulationBehind,
        /// The simulation result says the blockhash is unknown.
        SimulationBlockhashNotFound,
        /// The simulation is refused by the escrow with this code.
        SimulationRefused(u32),
        /// Simulated near the maximum, so sent at it, then found failed out of compute.
        LandedOutOfComputeAtMax,
        /// The simulation fails with escrow code 33 and no log naming the program.
        SimulationUnreadable,
    }

    /// What one `release_with` run saw.
    struct ReleaseRun {
        /// Blockhash fetches, one per attempt.
        attempts: usize,
        sends: usize,
        mock: MockStorage,
        simulated: Vec<serde_json::Value>,
        sent: Vec<serde_json::Value>,
        state: SenderState,
    }

    /// Units the mock simulation reports a release consumed.
    const SIMULATED_UNITS: u64 = 500_000;
    /// The read floor the sender starts with in `release_with`.
    const RELEASE_FLOOR: u64 = 10;

    /// Run one release whose simulation and sends go as `reply` says.
    async fn release_with(reply: SendReply) -> ReleaseRun {
        release_run(reply, false).await
    }

    /// `release_with`, on a row with an earlier journaled attempt when `journaled`.
    async fn release_run(reply: SendReply, journaled: bool) -> ReleaseRun {
        let (_, spec) = escrow(5_000, 5_000);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        mock.pending_transactions.lock().unwrap()[0].confidential = true;
        if journaled {
            journal_one(&mock, 1);
        }
        let (mut state, mut server, _) = sender_reading(&spec, RELEASE_FLOOR, mock.clone()).await;
        state.confidential.as_mut().unwrap().min_context_slot = RELEASE_FLOOR;
        let mint = Pubkey::new_unique();
        answer_mint(&mut server, &mint, confidential_mint(None, None), 0).await;
        let rpc =
            |method: &str| mockito::Matcher::PartialJson(serde_json::json!({ "method": method }));
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = attempts.clone();
        server
            .mock("POST", "/")
            .match_body(rpc("getLatestBlockhash"))
            .with_body_from_request(move |_| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": {
                        "context": {"slot": 11},
                        "value": {
                            "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                            "lastValidBlockHeight": 100
                        }
                    }
                })
                .to_string()
                .into()
            })
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(rpc("getVersion"))
            .with_body(
                r#"{"jsonrpc":"2.0","id":1,"result":{"solana-core":"2.2.0","feature-set":1}}"#,
            )
            .create_async()
            .await;
        let body_of = |request: &mockito::Request| -> serde_json::Value {
            serde_json::from_slice(request.body().unwrap()).unwrap()
        };
        let simulated = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = simulated.clone();
        server
            .mock("POST", "/")
            .match_body(rpc("simulateTransaction"))
            .with_body_from_request(move |request| {
                seen.lock().unwrap().push(body_of(request));
                let value = match reply {
                    SendReply::SimulatedOutOfCompute => serde_json::json!({
                        "err": {"InstructionError": [2, "ComputationalBudgetExceeded"]},
                        "logs": [], "accounts": null, "unitsConsumed": 1_400_000,
                        "returnData": null
                    }),
                    SendReply::SimulationBlockhashNotFound => serde_json::json!({
                        "err": "BlockhashNotFound", "logs": [], "accounts": null,
                        "unitsConsumed": 0, "returnData": null
                    }),
                    SendReply::SimulationRefused(code) => serde_json::json!({
                        "err": {"InstructionError": [2, {"Custom": code}]},
                        "logs": [format!(
                            "Program {PRIVATE_CHANNEL_ESCROW_PROGRAM_ID} failed: custom program error: {code:#x}"
                        )],
                        "accounts": null, "unitsConsumed": 40_000, "returnData": null
                    }),
                    SendReply::SimulationUnreadable => serde_json::json!({
                        "err": {"InstructionError": [2, {"Custom": 33}]},
                        "logs": [], "accounts": null, "unitsConsumed": 40_000,
                        "returnData": null
                    }),
                    SendReply::LandedOutOfComputeAtMax => serde_json::json!({
                        "err": null, "logs": [], "accounts": null,
                        "unitsConsumed": 1_300_000, "returnData": null
                    }),
                    _ => serde_json::json!({
                        "err": null, "logs": [], "accounts": null,
                        "unitsConsumed": SIMULATED_UNITS, "returnData": null
                    }),
                };
                let answer = match reply {
                    SendReply::SimulationBehind => serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "error": {"code": -32016, "message": "Minimum context slot has not been reached"}
                    }),
                    _ => serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {"context": {"slot": 11}, "value": value}
                    }),
                };
                answer.to_string().into()
            })
            .create_async()
            .await;
        let sends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sent_count = sends.clone();
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sent_bodies = sent.clone();
        let send = server.mock("POST", "/").match_body(rpc("sendTransaction"));
        match reply {
            SendReply::Unreachable => send.with_status(503).with_body("down"),
            SendReply::BlockhashNotFound => send.with_body(
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "error": {
                        "code": -32002,
                        "message": "Transaction simulation failed: Blockhash not found",
                        "data": {"err": "BlockhashNotFound", "logs": [], "accounts": null,
                                 "unitsConsumed": 0, "returnData": null, "innerInstructions": null}
                    }
                })
                .to_string(),
            ),
            _ => send.with_body_from_request(move |request| {
                sent_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = body_of(request);
                // Echo the transaction's own signature, as a node does.
                let wire = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    body["params"][0].as_str().unwrap(),
                )
                .unwrap();
                sent_bodies.lock().unwrap().push(body);
                // A v1 wire is the message, then one signature per required signer.
                let first = wire.len() - usize::from(wire[1]) * 64;
                let signature = Signature::try_from(&wire[first..first + 64]).unwrap();
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": signature.to_string()})
                    .to_string()
                    .into()
            }),
        }
        .create_async()
        .await;
        let failed_with = |err: serde_json::Value| {
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {
                "context": {"slot": 50},
                "value": [{"slot": 50, "confirmations": null, "status": {"Err": err.clone()},
                           "err": err, "confirmationStatus": "confirmed"}]
            }})
            .to_string()
        };
        let statuses = server
            .mock("POST", "/")
            .match_body(rpc("getSignatureStatuses"));
        match reply {
            SendReply::FailedWithUnreadableLog => statuses.with_body(failed_with(
                serde_json::json!({"InstructionError": [0, {"Custom": 33}]}),
            )),
            SendReply::LandedOutOfCompute | SendReply::LandedOutOfComputeAtMax => statuses
                .with_body(failed_with(
                    serde_json::json!({"InstructionError": [2, "ComputationalBudgetExceeded"]}),
                )),
            _ => statuses.with_status(503).with_body("down"),
        }
        .create_async()
        .await;
        server
            .mock("POST", "/")
            .match_body(rpc("getTransaction"))
            .with_status(503)
            .with_body("down")
            .create_async()
            .await;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(64);
        submit_release(
            &mut state,
            full_request(1, mint, lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;
        let simulated = simulated.lock().unwrap().clone();
        let sent = sent.lock().unwrap().clone();
        ReleaseRun {
            attempts: attempts.load(std::sync::atomic::Ordering::SeqCst),
            sends: sends.load(std::sync::atomic::Ordering::SeqCst),
            mock,
            simulated,
            sent,
            state,
        }
    }

    /// A send error that is not a preflight refusal, a failed status poll and a preflight
    /// `BlockhashNotFound` are retried within the retry budget, then end on the journal gate.
    #[tokio::test]
    async fn transient_send_and_poll_errors_are_retried_within_the_budget() {
        for reply in [
            SendReply::Unreachable,
            SendReply::BlockhashNotFound,
            SendReply::AcceptedThenStatusFails,
        ] {
            let run = release_with(reply).await;
            assert_eq!(run.attempts, 3, "one attempt per retry");
            assert_eq!(
                row_status(&run.mock, 1),
                Some(TransactionStatus::PendingRemint)
            );
            // One blockhash in the mock, so every resend journals the same signature.
            assert!(!run.mock.release_signatures.lock().unwrap()[&1].is_empty());
        }
    }

    /// A landed failure whose log cannot be read has no provable origin, so it is retried on
    /// the retry budget like the public path; once that is spent it is refunded through the gate.
    #[tokio::test]
    async fn a_failure_with_an_unreadable_log_is_retried_then_refunded_through_the_gate() {
        let run = release_with(SendReply::FailedWithUnreadableLog).await;
        assert_eq!(run.sends, 3, "one send per retry");
        assert_eq!(
            row_status(&run.mock, 1),
            Some(TransactionStatus::PendingRemint)
        );
        let entry = &run.state.pending_remints[0];
        assert!(!entry.signatures.is_empty(), "the gate checks the sends");
        assert!(!entry.release_refused_on_chain);
        assert!(run.state.confidential.as_ref().unwrap().parks.is_empty());
    }

    /// The simulation and the send are both pinned at or after our last landing, so a lagging
    /// node cannot judge the release against an older balance.
    #[tokio::test]
    async fn the_release_is_simulated_and_sent_no_older_than_our_last_landing() {
        let run = release_with(SendReply::AcceptedThenStatusFails).await;
        assert!(!run.simulated.is_empty() && !run.sent.is_empty());
        for body in run.simulated.iter().chain(&run.sent) {
            assert_eq!(body["params"][1]["minContextSlot"], RELEASE_FLOOR, "{body}");
        }
    }

    /// A node behind the floor, or one that does not know the blockhash, says nothing about
    /// the release: before any send the row waits in `Pending` and keeps its retry budget.
    #[tokio::test]
    async fn a_transient_simulation_failure_waits_in_pending_without_spending_retries() {
        assert!(resendable(
            &crate::operator::utils::rpc_util::below_min_context_slot(1, 2)
        ));
        for reply in [
            SendReply::SimulationBehind,
            SendReply::SimulationBlockhashNotFound,
        ] {
            let run = release_with(reply).await;
            assert_eq!(run.attempts, 1, "no resend in place");
            assert_eq!(run.sends, 0, "nothing goes out unsimulated");
            assert_eq!(row_status(&run.mock, 1), Some(TransactionStatus::Pending));
            assert!(run.mock.pending_transactions.lock().unwrap()[0]
                .not_before
                .is_some());
            assert!(run.state.retry_counts.is_empty(), "no retry spent");
            assert!(run.state.pending_remints.is_empty());
        }
    }

    /// After a journaled attempt the row cannot go back to `Pending`: it is held in the
    /// sender with no retry spent, like any transient error before the resend.
    #[tokio::test]
    async fn a_transient_simulation_failure_after_a_send_stays_in_the_sender() {
        for reply in [
            SendReply::SimulationBehind,
            SendReply::SimulationBlockhashNotFound,
        ] {
            let run = release_run(reply, true).await;
            assert_eq!(run.attempts, 1, "no resend in place");
            assert_eq!(run.sends, 0);
            assert_eq!(
                row_status(&run.mock, 1),
                Some(TransactionStatus::Processing)
            );
            assert!(run.state.pending_remints.is_empty());
            assert!(held(&run.state).is_some_and(|hold| hold.request.is_some()));
            assert_eq!(run.state.retry_counts.get(&7).copied().unwrap_or(0), 0);
        }
    }

    /// A refusal seen only in simulation on a row that never broadcast is refunded at once,
    /// with no signature to wait on: the wallet left the allow list (20), withdrawals are
    /// blocked (16), the recipient is invalid (24), the mint is paused (32).
    #[tokio::test]
    async fn a_refusal_in_simulation_on_a_never_sent_row_is_refunded() {
        for code in [20, 32, 16, 24] {
            let run = release_with(SendReply::SimulationRefused(code)).await;
            assert_eq!(run.sends, 0, "code {code}");
            assert_eq!(
                row_status(&run.mock, 1),
                Some(TransactionStatus::PendingRemint),
                "code {code}"
            );
            let entry = &run.state.pending_remints[0];
            assert!(entry.signatures.is_empty(), "code {code}");
            assert!(
                entry.deadline <= Utc::now(),
                "code {code}: no finality wait"
            );
            assert!(
                run.mock.pending_transactions.lock().unwrap()[0].release_refused_on_chain,
                "code {code}: kept across a restart"
            );
        }
    }

    /// The same refusal on a row with an earlier journaled attempt still waits on the gate.
    #[tokio::test]
    async fn a_refusal_in_simulation_after_a_send_goes_through_the_gate() {
        let run = release_run(SendReply::SimulationRefused(20), true).await;
        assert_eq!(run.sends, 0);
        assert_eq!(
            row_status(&run.mock, 1),
            Some(TransactionStatus::PendingRemint)
        );
        let entry = &run.state.pending_remints[0];
        assert_eq!(entry.signatures.len(), 1);
        assert!(!entry.release_refused_on_chain);
    }

    /// A refusal with no provable origin seen only in simulation spends the retry budget;
    /// with nothing ever sent it is then refunded directly, not sent to manual review.
    #[tokio::test]
    async fn retries_spent_with_nothing_sent_refund_directly() {
        let run = release_with(SendReply::SimulationUnreadable).await;
        assert_eq!(run.attempts, 3, "one attempt per retry");
        assert_eq!(run.sends, 0);
        assert_eq!(
            row_status(&run.mock, 1),
            Some(TransactionStatus::PendingRemint)
        );
        assert!(run.state.pending_remints[0].signatures.is_empty());
    }

    /// The release is simulated at the maximum, then sent with what it used plus 20%.
    #[tokio::test]
    async fn the_release_is_sent_with_its_simulated_units_plus_a_fifth() {
        assert_eq!(release_compute_limit(Some(SIMULATED_UNITS)), 600_000);
        assert_eq!(
            release_compute_limit(Some(1_300_000)),
            CU_CONFIDENTIAL_RELEASE_MAX
        );
        assert_eq!(release_compute_limit(None), CU_CONFIDENTIAL_RELEASE_MAX);
        let run = release_with(SendReply::AcceptedThenStatusFails).await;
        assert_eq!(
            compute_limit_of(&run.simulated[0]),
            Some(CU_CONFIDENTIAL_RELEASE_MAX)
        );
        assert_eq!(compute_limit_of(&run.sent[0]), Some(600_000));
    }

    /// Running out of compute in the simulation (run at the maximum) or in a send at the
    /// maximum parks with its own label: no resend passes it.
    #[tokio::test]
    async fn running_out_of_compute_at_the_maximum_parks_as_compute_exceeded() {
        for reply in [
            SendReply::SimulatedOutOfCompute,
            SendReply::LandedOutOfComputeAtMax,
        ] {
            let run = release_with(reply).await;
            assert_eq!(run.attempts, 1, "no resend");
            // Refused in simulation nothing was sent, so it parks at once; after a send it
            // waits on the journal gate first.
            let status = match reply {
                SendReply::SimulatedOutOfCompute => TransactionStatus::ManualReview,
                _ => TransactionStatus::PendingRemint,
            };
            assert_eq!(row_status(&run.mock, 1), Some(status));
            if reply == SendReply::LandedOutOfComputeAtMax {
                assert_eq!(
                    compute_limit_of(&run.sent[0]),
                    Some(CU_CONFIDENTIAL_RELEASE_MAX)
                );
            }
            assert_compute_park(&run);
        }
    }

    /// A release sent below the maximum that runs out of compute on chain is resent once at
    /// the maximum, journaled as usual; only that send failing too parks it.
    #[tokio::test]
    async fn running_out_of_compute_below_the_maximum_resends_once_at_the_maximum() {
        let run = release_with(SendReply::LandedOutOfCompute).await;
        assert_eq!(run.sends, 2);
        assert_eq!(compute_limit_of(&run.sent[0]), Some(600_000));
        assert_eq!(
            compute_limit_of(&run.sent[1]),
            Some(CU_CONFIDENTIAL_RELEASE_MAX)
        );
        assert_eq!(
            row_status(&run.mock, 1),
            Some(TransactionStatus::PendingRemint)
        );
        assert_compute_park(&run);
    }

    fn assert_compute_park(run: &ReleaseRun) {
        let labels = run
            .mock
            .confidential_sender
            .lock()
            .unwrap()
            .park_labels
            .clone();
        assert_eq!(
            labels.get(&1).map(String::as_str),
            Some(metrics::BAIL_REASON_CONFIDENTIAL_COMPUTE_EXCEEDED)
        );
    }

    /// The compute limit a sent or simulated v1 release carries.
    fn compute_limit_of(body: &serde_json::Value) -> Option<u32> {
        let wire = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            body["params"][0].as_str().unwrap(),
        )
        .unwrap();
        let first = wire.len() - usize::from(wire[1]) * 64;
        // The message without its version prefix.
        solana_sdk::message::v1::deserialize(&wire[1..first])
            .unwrap()
            .config
            .compute_unit_limit
    }

    #[tokio::test]
    async fn a_balance_below_the_release_parks_before_anything_is_sent() {
        let (_, spec) = escrow(50, 50);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        mock.pending_transactions.lock().unwrap()[0].confidential = true;
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;
        assert_eq!(row_status(&mock, 1), Some(TransactionStatus::ManualReview));
        // The label is kept on the row, as a park through the journal gate keeps it.
        assert_eq!(
            mock.confidential_sender
                .lock()
                .unwrap()
                .park_labels
                .get(&1)
                .map(String::as_str),
            Some(metrics::BAIL_REASON_CONFIDENTIAL_BALANCE_LOW)
        );
    }

    /// A 9-decimal mint: a 5,000-token deposit raced our apply and the operator restarted
    /// before the repair landed. The AES copy is stale, the gap is past the search and the
    /// apply is no longer in memory; the available the sender verified before the restart
    /// still opens the ciphertext, even after a recover that found nothing to recover.
    #[tokio::test]
    async fn after_a_restart_the_last_verified_available_still_proves_the_balance() {
        let keys = cts::keys();
        let raced: u64 = 5_000 * 1_000_000_000;
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 1_000, 0);
        sim.apply();
        sim.credit(&keys, 700, 0);
        sim.credit(&keys, raced, 0);
        sim.apply();
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 1_700);
        spec.expected_pending_credit_counter = 1;
        spec.actual_pending_credit_counter = 2;
        let available = 1_700 + raced;
        assert!(raced > 1 << 40, "the gap is past the search");

        let mock = MockStorage::new();
        let (state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let view = view_of(&spec);
        let fresh = state.confidential.as_ref().unwrap();
        assert_eq!(
            verified_available(&state.rpc_client, fresh, &spec.mint, &view).await,
            None,
            "without the kept value nothing opens"
        );

        // Kept by the sender after the apply landed, before the restart.
        state
            .storage
            .record_confidential_mint_state(&spec.mint.to_string(), available, 60)
            .await
            .unwrap();
        // Ops ran confidential-recover on the mint only to clear poisoning; nothing recovered.
        let lock = state
            .storage
            .try_acquire_live_lock(
                crate::storage::common::storage::live_lock::LiveLockMode::Exclusive,
                "recover",
                tokio_util::sync::CancellationToken::new(),
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();
        let report = crate::indexer::confidential::recover_mint(
            &state.storage,
            &lock,
            &keys,
            &spec.mint.to_string(),
        )
        .await
        .unwrap();
        assert_eq!(report.recovered, 0);
        let (mut state, _server, _) = sender_reading(&spec, 10, mock).await;
        let storage = state.storage.clone();
        seed_mint_states(state.confidential.as_mut().unwrap(), &storage).await;
        let conf = state.confidential.as_ref().unwrap();
        assert_eq!(
            verified_available(&state.rpc_client, conf, &spec.mint, &view).await,
            Some((available, CandidateSource::Local))
        );
    }

    /// At boot each mint's last landed apply is candidate (d) again, as it was in memory.
    #[tokio::test]
    async fn a_restart_restores_our_last_landed_apply_as_candidate_d() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock.clone());
        let mint = Pubkey::new_unique();
        let signature = Signature::new_unique();
        for (sig, slot) in [(Signature::new_unique(), 40), (signature, 50)] {
            let id = storage
                .journal_operator_attempt(&OperatorAttempt {
                    id: 0,
                    mint: mint.to_string(),
                    kind: "background".into(),
                    signature: sig.to_string(),
                    last_valid_block_height: 100,
                    blockhash_slot: 9,
                    prior_available: Some(slot),
                    applied_pending: Some(7),
                    status: attempt_status::SENT.into(),
                    landed_slot: None,
                    created_at: Utc::now(),
                })
                .await
                .unwrap();
            storage
                .settle_operator_attempt(id, Some(slot))
                .await
                .unwrap();
        }
        let mut conf = ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        seed_last_applies(&mut conf, &storage).await;
        assert_eq!(
            conf.books[&mint].last_apply,
            Some(LastApply {
                prior_available: 50,
                signature,
                applied_pending: Some(7),
            })
        );
    }

    // ---- 21: fresh reads ---------------------------------------------------------------

    #[tokio::test]
    async fn reads_are_confirmed_and_no_older_than_our_last_transaction() {
        let (_, spec) = escrow(1, 1);
        let mut server = mockito::Server::new_async().await;
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 0,
            "result": rpc_account(&spl_token_2022::ID, &account_with_confidential(&spec), 4_242),
        });
        let read = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""commitment":"confirmed""#.into()),
                mockito::Matcher::Regex(r#""minContextSlot":4242"#.into()),
            ]))
            .with_body(body.to_string())
            .expect(1)
            .create_async()
            .await;
        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            Default::default(),
            CommitmentConfig::finalized(),
        );
        let (read_back, slot) = read_escrow(&rpc, &Pubkey::new_unique(), 4_242)
            .await
            .unwrap();
        assert!(matches!(read_back, ConfidentialAccountRead::Configured(_)));
        assert_eq!(slot, 4_242);
        read.assert_async().await;
    }

    #[tokio::test]
    async fn the_read_floor_is_seeded_from_the_journal_at_boot() {
        let storage = Storage::Mock(MockStorage::new());
        let Storage::Mock(mock) = &storage else {
            unreachable!()
        };
        for (id, slot) in [(1, 77), (2, 99)] {
            mock.release_signatures.lock().unwrap().insert(
                id,
                vec![StoredSig {
                    signature: Signature::new_unique().to_string(),
                    last_valid_block_height: 1,
                    blockhash_slot: Some(slot),
                }],
            );
        }
        let mut conf = ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        seed_min_context_slot(&mut conf, &storage).await;
        assert_eq!(conf.min_context_slot, 99);
    }

    /// Our own confirmed transactions persist their landing slot, and a restarted sender's
    /// read floor is that slot, not the lower blockhash slot of the journal.
    #[tokio::test]
    async fn after_a_restart_the_read_floor_is_our_last_landing_slot() {
        let mock = MockStorage::new();
        mock.release_signatures.lock().unwrap().insert(
            1,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 1,
                blockhash_slot: Some(99),
            }],
        );
        let (_, spec) = escrow(1, 1);
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        record_landing(&mut state, 420).await;
        record_landing(&mut state, 300).await;
        assert_eq!(state.confidential.as_ref().unwrap().min_context_slot, 420);

        let storage = Storage::Mock(mock);
        let mut conf = ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        seed_min_context_slot(&mut conf, &storage).await;
        assert_eq!(conf.min_context_slot, 420);
    }

    // ---- 22: apply triggers ------------------------------------------------------------

    fn pending_view(credits: &[u64], counter: u64, max: u64) -> (Keys, ConfidentialAccountView) {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        for amount in credits {
            sim.credit(&keys, *amount, 0);
        }
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 0);
        spec.pending_credit_counter = counter;
        spec.maximum_pending_credit_counter = max;
        (keys, view_of(&spec))
    }

    #[test]
    fn the_release_flow_applies_only_when_short_and_pending_could_help() {
        let (_, view) = pending_view(&[500], 1, 100);
        assert!(release_needs_apply(Some(100), 200, &view));
        assert!(release_needs_apply(None, 200, &view));
        assert!(!release_needs_apply(Some(200), 200, &view));
        let (_, empty) = pending_view(&[], 0, 100);
        assert!(!release_needs_apply(Some(100), 200, &empty));
    }

    #[test]
    fn the_background_check_applies_at_half_the_cap_or_large_pending_only() {
        let (keys, half) = pending_view(&[1], 50, 100);
        assert_eq!(
            background_apply_due(&keys, &half, false, false, false),
            Some(BackgroundApply::CounterHalfFull)
        );
        let (keys, large) = pending_view(&[PENDING_APPLY_THRESHOLD], 1, 100);
        assert_eq!(
            background_apply_due(&keys, &large, false, false, false),
            Some(BackgroundApply::PendingLarge)
        );
        let (keys, neither) = pending_view(&[PENDING_APPLY_THRESHOLD - 1], 49, 100);
        assert_eq!(
            background_apply_due(&keys, &neither, false, false, false),
            None
        );
    }

    #[test]
    fn no_background_apply_while_halted_blocked_or_releasing() {
        let (keys, half) = pending_view(&[1], 50, 100);
        assert_eq!(background_apply_due(&keys, &half, true, false, false), None);
        assert_eq!(background_apply_due(&keys, &half, false, true, false), None);
        assert_eq!(background_apply_due(&keys, &half, false, false, true), None);
    }

    // ---- 23: AES repair ----------------------------------------------------------------

    #[test]
    fn repairs_run_back_to_back_then_alert_once_and_back_off() {
        let mut repair = RepairState::default();
        let now = Instant::now();
        for _ in 0..AES_REPAIR_BUDGET {
            assert_eq!(repair.on_still_stale(now), RepairNext::RepairNow);
        }
        assert_eq!(
            repair.on_still_stale(now),
            RepairNext::Defer { alert: true }
        );
        assert!(!repair.deferred_repair_due(now));
        assert!(repair.deferred_repair_due(now + WAIT_BACKOFF_BASE));
        assert_eq!(
            repair.on_still_stale(now),
            RepairNext::Defer { alert: false }
        );
        assert_eq!(repair.backoff, WAIT_BACKOFF_BASE * 4);
        for _ in 0..10 {
            repair.on_still_stale(now);
        }
        assert_eq!(repair.backoff, WAIT_BACKOFF_CAP);
        // Any later apply gets a fresh budget.
        repair.fresh_budget();
        assert_eq!(repair.on_still_stale(now), RepairNext::RepairNow);
    }

    #[tokio::test]
    async fn a_later_release_ends_the_stale_episode() {
        let mut state = sender_state_with_storage_and_role(
            "http://127.0.0.1:1",
            MockStorage::new(),
            ProgramType::Withdraw,
        );
        state.confidential = Some(Box::new(ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        )));
        let mint = Pubkey::new_unique();
        let book = state
            .confidential
            .as_mut()
            .unwrap()
            .books
            .entry(mint)
            .or_default();
        for _ in 0..=AES_REPAIR_BUDGET {
            book.repair.on_still_stale(Instant::now());
        }
        assert!(book.repair.deferred_until.is_some());
        let deferred = metrics::OPERATOR_CONFIDENTIAL_AES_REPAIR_DEFERRED
            .with_label_values(&[&mint.to_string()]);
        deferred.set(1.0);
        settle_local(&mut state, &mint, 10, 4, 1).await;
        assert_eq!(
            state.confidential.as_ref().unwrap().books[&mint].repair,
            RepairState::default()
        );
        // What the landed release left is kept for a restart.
        assert_eq!(
            state.storage.confidential_mint_states().await.unwrap(),
            vec![(mint.to_string(), 6)]
        );
        assert_eq!(
            deferred.get(),
            0.0,
            "the backoff alert clears with the episode"
        );
    }

    #[test]
    fn a_stale_copy_is_detected_by_the_opening_check() {
        let (keys, good) = escrow(900, 900);
        assert!(!aes_is_stale(&keys, &view_of(&good)));
        let (keys, stale) = escrow(900, 800);
        assert!(aes_is_stale(&keys, &view_of(&stale)));
    }

    // ---- 24: classification ------------------------------------------------------------

    #[test]
    fn token_2022_proof_refusals_rebuild_and_readiness_refusals_park() {
        use token_2022_codes::*;
        let t22 = Some(spl_token_2022::ID);
        for code in [
            PUBKEY_MISMATCH,
            BALANCE_MISMATCH,
            FEE_MISMATCH,
            FEE_PARAMETERS_MISMATCH,
        ] {
            assert_eq!(
                classify_confidential(t22, Some(code)),
                ConfidentialFailure::RebuildProofs
            );
        }
        for code in [CREDITS_DISABLED, COUNTER_FULL] {
            assert_eq!(
                classify_confidential(t22, Some(code)),
                ConfidentialFailure::Park(metrics::BAIL_REASON_DESTINATION_NOT_READY)
            );
        }
        // The escrow refuses a frozen or unapproved destination itself first, so Token-2022's
        // own 17 and 24 can only be about the escrow's account.
        for code in [ACCOUNT_FROZEN, NOT_APPROVED] {
            assert_eq!(
                classify_confidential(t22, Some(code)),
                ConfidentialFailure::Park(
                    metrics::BAIL_REASON_CONFIDENTIAL_ESCROW_ACCOUNT_NOT_READY
                )
            );
        }
        assert_eq!(
            (ACCOUNT_FROZEN, NOT_APPROVED, CREDITS_DISABLED),
            (17, 24, 25)
        );
        assert_eq!(
            (
                PUBKEY_MISMATCH,
                BALANCE_MISMATCH,
                FEE_MISMATCH,
                FEE_PARAMETERS_MISMATCH,
                COUNTER_FULL
            ),
            (26, 27, 32, 33, 39)
        );
    }

    #[test]
    fn escrow_refusals_naming_token_2022_states_route_like_token_2022s() {
        use PrivateChannelEscrowProgramError as E;
        let escrow = Some(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);
        for error in [
            E::RecipientKeyChanged,
            E::StaleAuditorKey,
            E::StaleConfidentialBalance,
        ] {
            assert_eq!(
                classify_confidential(escrow, Some(error.clone() as u32)),
                ConfidentialFailure::RebuildProofs,
                "{error:?}"
            );
        }
        for error in [
            E::RecipientNotConfidential,
            E::RecipientNotApproved,
            E::RecipientCreditsDisabled,
            E::RecipientPendingFull,
            E::RecipientFrozen,
        ] {
            assert_eq!(
                classify_confidential(escrow, Some(error.clone() as u32)),
                ConfidentialFailure::Park(metrics::BAIL_REASON_DESTINATION_NOT_READY),
                "{error:?}"
            );
        }
        // A paused mint keeps today's path, as Token-2022's own refusal did.
        assert_eq!(
            classify_confidential(escrow, Some(E::MintPaused as u32)),
            ConfidentialFailure::Other
        );
        // The same codes from Token-2022 keep their Token-2022 meaning.
        assert_eq!(
            classify_confidential(Some(spl_token_2022::ID), Some(29)),
            ConfidentialFailure::Other
        );
    }

    #[test]
    fn a_refusal_raised_by_the_mints_hook_parks_whatever_its_code() {
        use solana_sdk::instruction::AccountMeta;
        let hook = Pubkey::new_unique();
        let extras = vec![
            AccountMeta::new_readonly(Pubkey::new_unique(), false),
            AccountMeta::new_readonly(hook, false),
        ];
        // A hook that refuses u64::MAX fails with a builtin error, so there is no code.
        for code in [None, Some(0), Some(27)] {
            assert_eq!(
                classify_release(Some(hook), code, &extras),
                ConfidentialFailure::Park(metrics::BAIL_REASON_HOOK_UNRESOLVABLE)
            );
        }
        // Token-2022's own refusals keep their routing on a hook mint.
        assert_eq!(
            classify_release(Some(spl_token_2022::ID), Some(27), &extras),
            ConfidentialFailure::RebuildProofs
        );
        assert_eq!(
            classify_release(Some(hook), None, &[]),
            ConfidentialFailure::Other
        );
    }

    #[test]
    fn colliding_codes_are_told_apart_by_origin() {
        let escrow = Some(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);
        let t22 = Some(spl_token_2022::ID);
        assert_eq!(
            classify_confidential(escrow, Some(22)),
            ConfidentialFailure::Park(metrics::BAIL_REASON_CONFIDENTIAL_MINT_NOT_CONFIGURED)
        );
        // Escrow 17 and 24 are its own errors, not readiness.
        assert_eq!(
            classify_confidential(escrow, Some(17)),
            ConfidentialFailure::Other
        );
        assert_eq!(
            classify_confidential(escrow, Some(24)),
            ConfidentialFailure::Other
        );
        assert_eq!(
            classify_confidential(t22, Some(22)),
            ConfidentialFailure::Other
        );
        assert_eq!(
            classify_confidential(escrow, Some(12)),
            ConfidentialFailure::Escrow(PrivateChannelEscrowProgramError::NonceAlreadyUsed)
        );
        // A code with no provable origin is retried; no code at all takes today's path.
        assert_eq!(
            classify_confidential(None, Some(27)),
            ConfidentialFailure::OriginUnproven
        );
        assert_eq!(classify_confidential(t22, None), ConfidentialFailure::Other);
    }

    #[test]
    fn preflight_refusals_classify_like_landed_failures() {
        use solana_rpc_client_api::{
            client_error::{Error, ErrorKind},
            request::{RpcError, RpcResponseErrorData},
            response::RpcSimulateTransactionResult,
        };
        let result = RpcSimulateTransactionResult {
            err: Some(
                solana_sdk::transaction::TransactionError::InstructionError(
                    0,
                    InstructionError::Custom(27),
                )
                .into(),
            ),
            logs: Some(vec![
                format!("Program {} invoke [1]", PRIVATE_CHANNEL_ESCROW_PROGRAM_ID),
                format!("Program {} invoke [2]", spl_token_2022::ID),
                format!(
                    "Program {} failed: custom program error: 0x1b",
                    spl_token_2022::ID
                ),
                format!(
                    "Program {} failed: custom program error: 0x1b",
                    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID
                ),
            ]),
            accounts: None,
            units_consumed: None,
            loaded_accounts_data_size: None,
            return_data: None,
            inner_instructions: None,
            replacement_blockhash: None,
            fee: None,
            pre_balances: None,
            post_balances: None,
            pre_token_balances: None,
            post_token_balances: None,
            loaded_addresses: None,
        };
        let err = Error::from(ErrorKind::RpcError(RpcError::RpcResponseError {
            code: -32002,
            message: "preflight".into(),
            data: RpcResponseErrorData::SendTransactionPreflightFailure(result),
        }));
        let (err, origin) = preflight_failure(&err).unwrap();
        assert_eq!(
            classify_confidential(origin, err.as_ref().and_then(custom_code)),
            ConfidentialFailure::RebuildProofs
        );
        assert!(preflight_failure(&Error::from(ErrorKind::Custom("x".into()))).is_none());
    }

    // ---- 25: proof reuse ---------------------------------------------------------------

    #[test]
    fn only_a_timeout_resend_reuses_proofs() {
        assert!(reuse_proofs(AttemptCause::TimeoutResend));
        for cause in [
            AttemptCause::First,
            AttemptCause::StaleProofs,
            AttemptCause::RotationDrain,
        ] {
            assert!(!reuse_proofs(cause), "{cause:?} must rebuild");
        }
    }

    // ---- 26: rebalance -----------------------------------------------------------------

    fn rebalance(direction: RebalanceDirection, amount: u64, expected: u64) -> RebalanceRequest {
        RebalanceRequest {
            id: 1,
            mint: Pubkey::new_unique().to_string(),
            direction,
            amount,
            expected_available: expected,
            request_id: "r".into(),
            status: rebalance_status::PENDING.into(),
            signature: None,
            error: None,
            last_valid_block_height: None,
            blockhash_slot: None,
        }
    }

    #[test]
    fn a_rebalance_runs_only_against_the_balance_it_was_made_for() {
        let to_public = rebalance(RebalanceDirection::ToPublic, 300, 1_000);
        assert_eq!(
            rebalance_decision(&to_public, Some(1_000), 0),
            RebalanceDecision::Execute
        );
        assert!(matches!(
            rebalance_decision(&to_public, Some(700), 0),
            RebalanceDecision::Refuse(_)
        ));
        assert_eq!(
            rebalance_decision(&to_public, None, 0),
            RebalanceDecision::Wait
        );
        let too_much = rebalance(RebalanceDirection::ToPublic, 2_000, 1_000);
        assert!(matches!(
            rebalance_decision(&too_much, Some(1_000), 0),
            RebalanceDecision::Refuse(_)
        ));
        // A move to confidential needs only enough public; its id keeps it from running twice.
        let to_confidential = rebalance(RebalanceDirection::ToConfidential, 50, 1_000);
        for (available, public) in [(1_000, 50), (900, 50), (1_000, 80)] {
            assert_eq!(
                rebalance_decision(&to_confidential, Some(available), public),
                RebalanceDecision::Execute
            );
        }
        assert!(matches!(
            rebalance_decision(&to_confidential, Some(1_000), 49),
            RebalanceDecision::Refuse(_)
        ));
    }

    #[tokio::test]
    async fn a_rerun_after_the_move_landed_refuses_instead_of_moving_twice() {
        // The move already landed: available is 700, the request expected 1,000.
        let (_, spec) = escrow(700, 700);
        let mock = MockStorage::new();
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let mint = spec.mint;
        let storage = state.storage.clone();
        let id = storage
            .insert_rebalance_request(
                &Pubkey::new_unique().to_string(),
                RebalanceDirection::ToPublic,
                300,
                1_000,
                "r",
            )
            .await
            .unwrap()
            .unwrap();
        let mut request = storage
            .get_rebalance_requests(rebalance_status::PENDING)
            .await
            .unwrap()
            .remove(0);
        request.mint = mint.to_string();
        execute_rebalance(&mut state, &request).await.unwrap();
        let refused = storage
            .get_rebalance_requests(rebalance_status::REFUSED)
            .await
            .unwrap();
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].id, id);
        assert!(refused[0].signature.is_none(), "nothing was sent");
    }

    /// Ops runs `--to-confidential 200`, sees it complete, then reruns it thinking it failed:
    /// the rerun carries the same id and is refused, whatever the first request's state.
    #[tokio::test]
    async fn a_rerun_with_the_same_id_is_refused_whatever_the_first_did() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 1_000, 0);
        sim.apply();
        let spec = AccountSpec::healthy(&keys, &sim, 500, 1_000);
        let mock = MockStorage::new();
        let (state, _server, instance) = sender_reading(&spec, 10, mock.clone()).await;
        let request = |id: &'static str| {
            request_rebalance(
                &state.storage,
                &state.rpc_client,
                &keys,
                &instance,
                &spec.mint,
                RebalanceDirection::ToConfidential,
                200,
                id,
            )
        };
        let first = request("r1").await.unwrap();
        for to in [
            rebalance_status::COMPLETED,
            rebalance_status::REFUSED,
            rebalance_status::FAILED,
        ] {
            mock.confidential_sender.lock().unwrap().rebalance_requests[0].status = to.into();
            let rerun = request("r1").await.unwrap_err();
            assert!(rerun.contains("r1"), "{rerun}");
        }
        let all = state
            .storage
            .get_rebalance_requests(rebalance_status::FAILED)
            .await
            .unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!((all[0].id, all[0].request_id.as_str()), (first, "r1"));
        // A new id is a new request.
        request("r2").await.unwrap();
    }

    /// On a busy mint a public deposit lands between the request and its run: the move still
    /// runs, since the public balance still covers it.
    #[tokio::test]
    async fn a_move_to_confidential_runs_after_the_public_balance_grew() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 1_000, 0);
        sim.apply();
        let spec = AccountSpec::healthy(&keys, &sim, 500, 1_000);
        let mock = MockStorage::new();
        let (state, _server, instance) = sender_reading(&spec, 10, mock.clone()).await;
        request_rebalance(
            &state.storage,
            &state.rpc_client,
            &keys,
            &instance,
            &spec.mint,
            RebalanceDirection::ToConfidential,
            200,
            "busy",
        )
        .await
        .unwrap();
        let request = state
            .storage
            .get_rebalance_requests(rebalance_status::PENDING)
            .await
            .unwrap()
            .remove(0);

        let mut deposited = spec.clone();
        deposited.public = 800;
        let (mut state, _server, _) = sender_reading(&deposited, 10, mock).await;
        // The mock node cannot take the send; only the decision matters here.
        let _ = execute_rebalance(&mut state, &request).await;
        assert!(state
            .storage
            .get_rebalance_requests(rebalance_status::REFUSED)
            .await
            .unwrap()
            .is_empty());
    }

    // ---- 26: a request left `sent` is settled only once its attempt can no longer land ----

    /// A sender over `mock` whose Solana RPC answers the status of every signature with
    /// `status` (history included) at block height `height`, keeping every slot. Fails
    /// the test if anything is sent.
    async fn settling_sender(
        height: u64,
        status: serde_json::Value,
        mock: MockStorage,
    ) -> (SenderState, mockito::ServerGuard) {
        let mut server = mockito::Server::new_async().await;
        for (method, result) in [
            ("getBlockHeight", serde_json::json!(height)),
            ("getFirstAvailableBlock", serde_json::json!(0)),
            (
                "getSignatureStatuses",
                serde_json::json!({"context": {"slot": height}, "value": [status]}),
            ),
        ] {
            server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(
                    serde_json::json!({"method": method}),
                ))
                .with_body(
                    serde_json::json!({"jsonrpc": "2.0", "id": 0, "result": result}).to_string(),
                )
                .create_async()
                .await;
        }
        for method in ["sendTransaction", "getLatestBlockhash"] {
            server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(
                    serde_json::json!({"method": method}),
                ))
                .expect(0)
                .create_async()
                .await;
        }
        let state = sender_state_with_storage_and_role(&server.url(), mock, ProgramType::Withdraw);
        (state, server)
    }

    /// A request journaled as `sent` with a blockhash valid up to height 200.
    async fn sent_rebalance(mock: &MockStorage) -> i64 {
        let storage = Storage::Mock(mock.clone());
        let id = storage
            .insert_rebalance_request(
                "m",
                RebalanceDirection::ToConfidential,
                5,
                10,
                &Signature::new_unique().to_string(),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(storage
            .claim_rebalance_request(id, &Signature::new_unique().to_string(), 200, 50)
            .await
            .unwrap());
        id
    }

    fn status_of(mock: &MockStorage, id: i64) -> String {
        mock.confidential_sender
            .lock()
            .unwrap()
            .rebalance_requests
            .iter()
            .find(|request| request.id == id)
            .unwrap()
            .status
            .clone()
    }

    fn landed(commitment: &str) -> serde_json::Value {
        // `confirmations` is null only once rooted.
        let confirmations = (commitment != "finalized").then_some(1);
        serde_json::json!({
            "slot": 60,
            "confirmations": confirmations,
            "err": null,
            "status": {"Ok": null},
            "confirmationStatus": commitment,
        })
    }

    async fn settle(state: &SenderState) {
        let sent = state
            .storage
            .get_rebalance_requests(rebalance_status::SENT)
            .await
            .unwrap();
        for request in sent {
            settle_sent_rebalance(state, &request).await;
        }
    }

    #[tokio::test]
    async fn a_sent_request_not_found_before_its_blockhash_expires_stays_sent() {
        let mock = MockStorage::new();
        let id = sent_rebalance(&mock).await;
        let (state, server) = settling_sender(150, serde_json::Value::Null, mock.clone()).await;
        settle(&state).await;
        assert_eq!(status_of(&mock, id), rebalance_status::SENT);
        drop(server);
    }

    #[tokio::test]
    async fn a_sent_request_that_lands_after_a_slow_confirm_completes() {
        let mock = MockStorage::new();
        let id = sent_rebalance(&mock).await;
        // Confirmed but not final yet: still undecided.
        let (state, _server) = settling_sender(150, landed("confirmed"), mock.clone()).await;
        settle(&state).await;
        assert_eq!(status_of(&mock, id), rebalance_status::SENT);
        // Found in history after its blockhash expired.
        let (state, _server) = settling_sender(300, landed("finalized"), mock.clone()).await;
        settle(&state).await;
        assert_eq!(status_of(&mock, id), rebalance_status::COMPLETED);
    }

    #[tokio::test]
    async fn a_sent_request_absent_after_its_blockhash_expired_fails() {
        let mock = MockStorage::new();
        let id = sent_rebalance(&mock).await;
        let (state, _server) = settling_sender(300, serde_json::Value::Null, mock.clone()).await;
        settle(&state).await;
        assert_eq!(status_of(&mock, id), rebalance_status::FAILED);
    }

    #[tokio::test]
    async fn a_crash_while_sent_resumes_and_settles_without_resending() {
        let mock = MockStorage::new();
        let id = sent_rebalance(&mock).await;
        // Each step is a fresh sender over the same rows, as after a restart; `run_rebalances`
        // is the boot and background path, and the mocks refuse any send.
        let (tx, _rx) = mpsc::channel(8);
        let (mut state, _server) =
            settling_sender(150, serde_json::Value::Null, mock.clone()).await;
        run_rebalances(&mut state, &tx, false).await;
        assert_eq!(status_of(&mock, id), rebalance_status::SENT);
        let (mut state, _server) = settling_sender(300, landed("finalized"), mock.clone()).await;
        run_rebalances(&mut state, &tx, false).await;
        assert_eq!(status_of(&mock, id), rebalance_status::COMPLETED);

        // The same crash with the attempt lost: failed only once expired, never re-sent.
        let id = sent_rebalance(&mock).await;
        let (mut state, _server) =
            settling_sender(150, serde_json::Value::Null, mock.clone()).await;
        run_rebalances(&mut state, &tx, false).await;
        assert_eq!(status_of(&mock, id), rebalance_status::SENT);
        let (mut state, server) = settling_sender(300, serde_json::Value::Null, mock.clone()).await;
        run_rebalances(&mut state, &tx, false).await;
        assert_eq!(status_of(&mock, id), rebalance_status::FAILED);
        drop(server);
    }

    #[tokio::test]
    async fn a_sent_request_without_a_journaled_height_is_never_failed() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock.clone());
        let id = storage
            .insert_rebalance_request("m", RebalanceDirection::ToConfidential, 5, 10, "r")
            .await
            .unwrap()
            .unwrap();
        storage
            .transition_rebalance_request(
                id,
                rebalance_status::PENDING,
                rebalance_status::SENT,
                Some(Signature::new_unique().to_string()),
                None,
            )
            .await
            .unwrap();
        let (state, _server) = settling_sender(300, serde_json::Value::Null, mock.clone()).await;
        settle(&state).await;
        assert_eq!(status_of(&mock, id), rebalance_status::SENT);
        let (state, _server) = settling_sender(300, landed("finalized"), mock.clone()).await;
        settle(&state).await;
        assert_eq!(status_of(&mock, id), rebalance_status::COMPLETED);
    }

    #[test]
    fn generation_use_is_the_share_of_its_nonces_issued() {
        use crate::operator::bitmap_constants::NONCES_PER_GENERATION as N;
        assert_eq!(generation_used_ratio(None, 0), 0.0);
        assert_eq!(generation_used_ratio(Some(N / 2 - 1), 0), 0.5);
        assert_eq!(generation_used_ratio(Some(N + N - 1), 1), 1.0);
        // Nonces from an older generation count as none of this one.
        assert_eq!(generation_used_ratio(Some(3), 1), 0.0);
    }

    #[tokio::test]
    async fn the_oldest_confidential_park_is_reported() {
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        push_processing_row(&mock, 2);
        let old = Utc::now() - chrono::Duration::hours(2);
        for row in mock.pending_transactions.lock().unwrap().iter_mut() {
            row.status = TransactionStatus::ManualReview;
            row.confidential = row.id == 1;
            row.updated_at = if row.id == 1 {
                old
            } else {
                old - chrono::Duration::hours(1)
            };
        }
        let storage = Storage::Mock(mock);
        assert_eq!(storage.oldest_confidential_park().await.unwrap(), Some(old));
    }

    #[tokio::test]
    async fn a_rebalance_request_records_the_balance_verified_now() {
        let (_, spec) = escrow(1_000, 1_000);
        let (state, _server, instance) = sender_reading(&spec, 10, MockStorage::new()).await;
        let mint = Pubkey::new_unique();
        let id = request_rebalance(
            &state.storage,
            &state.rpc_client,
            &cts::keys(),
            &instance,
            &mint,
            RebalanceDirection::ToPublic,
            400,
            "r",
        )
        .await
        .unwrap();
        let pending = state
            .storage
            .get_rebalance_requests(rebalance_status::PENDING)
            .await
            .unwrap();
        assert_eq!(
            (
                pending[0].id,
                pending[0].expected_available,
                pending[0].amount
            ),
            (id, 1_000, 400)
        );

        // More than available, or nothing verifiable: refused, nothing recorded.
        for (aes, amount) in [(1_000, 2_000), (7, 10)] {
            let (_, spec) = escrow(1_000, aes);
            let (state, _server, instance) = sender_reading(&spec, 10, MockStorage::new()).await;
            assert!(request_rebalance(
                &state.storage,
                &state.rpc_client,
                &cts::keys(),
                &instance,
                &mint,
                RebalanceDirection::ToPublic,
                amount,
                "r",
            )
            .await
            .is_err());
            assert!(state
                .storage
                .get_rebalance_requests(rebalance_status::PENDING)
                .await
                .unwrap()
                .is_empty());
        }
    }

    // ---- 22: only a confidential release of the same mint holds the background check ----

    /// Runs one background check against an escrow account answering with `spec` and
    /// reports whether it started an apply (it asked for a blockhash to sign one).
    async fn background_check_applies(
        spec: &AccountSpec,
        in_flight: &[(u64, Option<Pubkey>)],
    ) -> bool {
        use private_channel_escrow_program_client::AllowedMint;
        let mock = MockStorage::new();
        mock.mints.lock().unwrap().insert(
            spec.mint.to_string(),
            crate::storage::common::models::DbMint::new(
                spec.mint.to_string(),
                6,
                spl_token_2022::ID.to_string(),
                crate::storage::common::amount::TokenAmount(0),
            ),
        );
        let (mut state, mut server, instance) = sender_reading(spec, 10, mock).await;
        let allowed = AllowedMint {
            discriminator: 2,
            bump: 255,
            deposits_blocked: false,
            withdrawals_blocked: false,
            decimals: 6,
            token_program: spl_token_2022::ID,
            extensions: 0,
            has_freeze_authority: false,
            min_deposit_amount: 0,
        };
        let pda = crate::operator::find_allowed_mint_pda(&instance, &spec.mint);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 0,
            "result": rpc_account(
                &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
                &borsh::to_vec(&allowed).unwrap(),
                10,
            ),
        });
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(pda.to_string()))
            .with_body(body.to_string())
            .create_async()
            .await;
        let blockhash = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getLatestBlockhash"}),
            ))
            .with_status(500)
            .expect_at_least(1)
            .create_async()
            .await;
        let conf = state.confidential.as_mut().unwrap();
        for (nonce, mint) in in_flight {
            if let Some(mint) = mint {
                conf.release_mints.insert(*nonce, *mint);
            }
        }
        state
            .in_flight_withdrawals
            .extend(in_flight.iter().map(|(nonce, _)| *nonce));
        let (tx, _rx) = mpsc::channel(8);
        background_check(&mut state, &tx).await;
        blockhash.matched_async().await
    }

    /// A due apply (credit counter at half its cap) on an account under our key.
    fn due_apply() -> AccountSpec {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 1, 0);
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 0);
        spec.pending_credit_counter = 32_768;
        spec
    }

    /// A stale AES copy with a deposit known to have raced the apply.
    fn due_repair() -> AccountSpec {
        let (_, mut spec) = escrow(5_000, 4_000);
        spec.expected_pending_credit_counter = 2;
        spec.actual_pending_credit_counter = 1;
        spec
    }

    /// Token-2022 refuses an apply on a frozen account, so a frozen escrow account gets no
    /// background apply or repair, and they resume once it is thawed.
    #[tokio::test]
    async fn a_frozen_escrow_account_holds_background_applies_and_repairs() {
        ensure_test_signer();
        for spec in [due_apply(), due_repair()] {
            let mut frozen = spec.clone();
            frozen.frozen = true;
            assert!(!background_check_applies(&frozen, &[]).await);
            assert!(background_check_applies(&spec, &[]).await);
        }
        // Warned once per freeze, and again after a thaw and a new freeze.
        let mut book = MintBook::default();
        assert!(hold_while_frozen(&mut book, &Pubkey::default(), true));
        assert!(book.frozen_warned);
        assert!(hold_while_frozen(&mut book, &Pubkey::default(), true));
        assert!(!hold_while_frozen(&mut book, &Pubkey::default(), false));
        assert!(!book.frozen_warned);
    }

    #[tokio::test]
    async fn a_public_withdrawal_in_flight_does_not_hold_a_due_apply_or_repair() {
        ensure_test_signer();
        for spec in [due_apply(), due_repair()] {
            assert!(background_check_applies(&spec, &[]).await);
            assert!(
                background_check_applies(&spec, &[(99, None)]).await,
                "a public withdrawal must not hold the background check"
            );
            assert!(
                background_check_applies(&spec, &[(98, Some(Pubkey::new_unique()))]).await,
                "a release of another mint must not hold it either"
            );
        }
    }

    #[tokio::test]
    async fn a_confidential_release_of_the_same_mint_holds_the_apply_and_repair() {
        ensure_test_signer();
        for spec in [due_apply(), due_repair()] {
            assert!(!background_check_applies(&spec, &[(97, Some(spec.mint))]).await);
        }
    }

    #[test]
    fn a_settled_release_stops_holding_its_mint() {
        let mut conf = ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let (mint, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        conf.release_mints.insert(1, mint);
        conf.release_mints.insert(2, other);
        let in_flight = std::collections::HashSet::from([2, 3]);
        assert_eq!(
            releasing_mints(&mut conf, &in_flight),
            std::collections::HashSet::from([other])
        );
        assert!(
            !conf.release_mints.contains_key(&1),
            "settled entries are dropped"
        );
    }

    // ---- every proof build reads the destination key, auditor and fee fresh ----------

    /// What a release's proofs are bound to besides the balance, as the chain shows it.
    #[derive(Clone, Copy)]
    struct ProofWorld {
        destination: [u8; 32],
        auditor: [u8; 32],
        /// Maximum fee of the older (epoch 0) and newer (epoch 5) fee settings.
        fees: (u64, u64),
        epoch: u64,
    }

    /// An ElGamal pubkey derived from a fixed seed; a top byte of zero keeps it canonical.
    fn seeded_key(seed: u8) -> [u8; 32] {
        let mut secret = [seed; 32];
        secret[31] = 0;
        Keys::from_bytes(&secret, &[seed; 16]).unwrap().pubkey()
    }

    fn fee_with_max(maximum_fee: u64) -> private_channel_confidential::ReleaseFee {
        private_channel_confidential::ReleaseFee {
            basis_points: 100,
            maximum_fee,
            withdraw_withheld_pubkey: seeded_key(9),
        }
    }

    /// A fee mint whose newer fee takes over at epoch 5.
    fn mint_for(world: &ProofWorld) -> Vec<u8> {
        use spl_token_2022::extension::transfer_fee::{TransferFee, TransferFeeConfig};
        use spl_token_2022::extension::{BaseStateWithExtensionsMut, StateWithExtensionsMut};
        let mut data = confidential_mint(Some(world.auditor), Some(fee_with_max(world.fees.0)));
        let mut state =
            StateWithExtensionsMut::<spl_token_2022::state::Mint>::unpack(&mut data).unwrap();
        state
            .get_extension_mut::<TransferFeeConfig>()
            .unwrap()
            .newer_transfer_fee = TransferFee {
            epoch: 5.into(),
            maximum_fee: world.fees.1.into(),
            transfer_fee_basis_points: 100.into(),
        };
        data
    }

    async fn answer(
        server: &mut mockito::ServerGuard,
        address: &Pubkey,
        result: serde_json::Value,
    ) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::PartialJson(serde_json::json!({"method": "getAccountInfo"})),
                mockito::Matcher::Regex(address.to_string()),
            ]))
            .with_body(serde_json::json!({"jsonrpc": "2.0", "id": 0, "result": result}).to_string())
            .create_async()
            .await;
    }

    /// The mint account and the current epoch, as a proof build reads them.
    async fn answer_mint(
        server: &mut mockito::ServerGuard,
        mint: &Pubkey,
        data: Vec<u8>,
        epoch: u64,
    ) {
        answer(server, mint, rpc_account(&spl_token_2022::ID, &data, 10)).await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getEpochInfo"}),
            ))
            .with_body(
                serde_json::json!({"jsonrpc": "2.0", "id": 0, "result": {
                    "absoluteSlot": 10, "blockHeight": 10, "epoch": epoch,
                    "slotIndex": 0, "slotsInEpoch": 432_000, "transactionCount": null
                }})
                .to_string(),
            )
            .create_async()
            .await;
    }

    /// Prepare the same release under `before`, then under `after` as a rebuild would, and
    /// return each build's proof instruction bytes.
    async fn proofs_before_and_after(before: ProofWorld, after: ProofWorld) -> [Vec<u8>; 2] {
        let (_, escrow_spec) = escrow(5_000, 5_000);
        let mint = Pubkey::new_unique();
        let request = full_request(1, mint, Utc::now());
        let mut built = Vec::new();
        for world in [before, after] {
            let mut server = mockito::Server::new_async().await;
            let mut state = sender_state_with_storage_and_role(
                &server.url(),
                MockStorage::new(),
                ProgramType::Withdraw,
            );
            let instance = Pubkey::new_unique();
            state.confidential = Some(Box::new(ConfidentialSender::new(
                Arc::new(cts::keys()),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                instance,
            )));
            let escrow_data = account_with_confidential(&escrow_spec);
            let escrow = rpc_account(&spl_token_2022::ID, &escrow_data, 10);
            answer(&mut server, &escrow_ata(&instance, &mint), escrow).await;
            let mut destination = escrow_spec.clone();
            destination.elgamal_pubkey = world.destination;
            let destination = account_with_confidential(&destination);
            let destination = rpc_account(&spl_token_2022::ID, &destination, 10);
            answer(&mut server, &request.destination, destination).await;
            answer_mint(&mut server, &mint, mint_for(&world), world.epoch).await;
            let Ok(Prepared::Ready { instructions, .. }) =
                prepare_release(&mut state, &request).await
            else {
                panic!("the release must prepare");
            };
            built.push(
                instructions[1..]
                    .iter()
                    .flat_map(|ix| ix.data.clone())
                    .collect::<Vec<u8>>(),
            );
        }
        built.try_into().unwrap()
    }

    fn holds(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn world() -> ProofWorld {
        ProofWorld {
            destination: seeded_key(1),
            auditor: seeded_key(2),
            fees: (111_111_111, 222_222_222),
            epoch: 4,
        }
    }

    #[tokio::test]
    async fn a_rebuild_after_the_fee_epoch_changes_proves_the_new_fee() {
        let before = world();
        let after = ProofWorld { epoch: 5, ..before };
        let [first, second] = proofs_before_and_after(before, after).await;
        assert!(holds(&first, &111_111_111u64.to_le_bytes()));
        assert!(holds(&second, &222_222_222u64.to_le_bytes()));
        assert!(!holds(&second, &111_111_111u64.to_le_bytes()));
    }

    #[tokio::test]
    async fn a_rebuild_after_an_auditor_rotation_proves_for_the_new_auditor() {
        let before = world();
        let after = ProofWorld {
            auditor: seeded_key(3),
            ..before
        };
        let [first, second] = proofs_before_and_after(before, after).await;
        assert!(holds(&first, &seeded_key(2)));
        assert!(holds(&second, &seeded_key(3)));
        assert!(!holds(&second, &seeded_key(2)));
    }

    #[tokio::test]
    async fn a_rebuild_after_the_destination_key_changes_proves_for_the_new_key() {
        let before = world();
        let after = ProofWorld {
            destination: seeded_key(4),
            ..before
        };
        let [first, second] = proofs_before_and_after(before, after).await;
        assert!(holds(&first, &seeded_key(1)));
        assert!(holds(&second, &seeded_key(4)));
        assert!(!holds(&second, &seeded_key(1)));
    }

    // ---- every apply and repair is journaled and blocks its mint until settled ---------

    /// How the node treats an apply's broadcast.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Broadcast {
        SendFails,
        NeverConfirms,
    }

    /// An escrow with 5,000 available (AES copy current) and 700 pending, under a node that
    /// answers sends as `broadcast`, knows no status, and is at block height 50. Returns
    /// the sender, the node, the mint and a count of sends.
    async fn apply_chain(
        mock: MockStorage,
        broadcast: Broadcast,
    ) -> (
        SenderState,
        mockito::ServerGuard,
        Pubkey,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 5_000, 0);
        sim.apply();
        sim.credit(&keys, 700, 0);
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 5_000);
        spec.pending_credit_counter = 1;
        apply_chain_with(&spec, mock, broadcast).await
    }

    /// `apply_chain` with the escrow account `spec`.
    async fn apply_chain_with(
        spec: &AccountSpec,
        mock: MockStorage,
        broadcast: Broadcast,
    ) -> (
        SenderState,
        mockito::ServerGuard,
        Pubkey,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        ensure_test_signer();
        let (state, mut server, _) = sender_reading(spec, 10, mock).await;
        let rpc =
            |method: &str| mockito::Matcher::PartialJson(serde_json::json!({ "method": method }));
        for (method, result) in [
            (
                "getLatestBlockhash",
                serde_json::json!({"context": {"slot": 11}, "value": {
                    "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                    "lastValidBlockHeight": 100
                }}),
            ),
            (
                "getVersion",
                serde_json::json!({"solana-core": "2.2.0", "feature-set": 1}),
            ),
            (
                "getSignatureStatuses",
                serde_json::json!({"context": {"slot": 50}, "value": [null]}),
            ),
            ("getBlockHeight", serde_json::json!(50)),
            ("getFirstAvailableBlock", serde_json::json!(0)),
        ] {
            server
                .mock("POST", "/")
                .match_body(rpc(method))
                .with_body(
                    serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string(),
                )
                .create_async()
                .await;
        }
        let sends = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = sends.clone();
        server
            .mock("POST", "/")
            .match_body(rpc("sendTransaction"))
            .with_status(if broadcast == Broadcast::SendFails {
                503
            } else {
                200
            })
            .with_body_from_request(move |request| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body: serde_json::Value =
                    serde_json::from_slice(request.body().unwrap()).unwrap();
                let wire = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    body["params"][0].as_str().unwrap(),
                )
                .unwrap();
                let signature = Signature::try_from(&wire[1..65]).unwrap();
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": signature.to_string()})
                    .to_string()
                    .into()
            })
            .create_async()
            .await;
        (state, server, Pubkey::new_unique(), sends)
    }

    /// The attempt is now found finalized at slot 60.
    async fn land_late(server: &mut mockito::ServerGuard) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getSignatureStatuses"}),
            ))
            .with_body(
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {
                    "context": {"slot": 300}, "value": [landed("finalized")]
                }})
                .to_string(),
            )
            .create_async()
            .await;
    }

    /// The chain moved past the attempt's last valid block height.
    async fn expire(server: &mut mockito::ServerGuard) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getBlockHeight"}),
            ))
            .with_body(serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": 300}).to_string())
            .create_async()
            .await;
    }

    fn attempts_of(mock: &MockStorage) -> Vec<OperatorAttempt> {
        mock.confidential_sender.lock().unwrap().attempts.clone()
    }

    /// Nothing may be written on the mint while the attempt could still land.
    async fn assert_mint_blocked(state: &mut SenderState, mint: &Pubkey, mock: &MockStorage) {
        let journaled = attempts_of(mock).len();
        assert!(apply_once(state, mint, ApplyTrigger::Background, None)
            .await
            .is_err());
        assert_eq!(attempts_of(mock).len(), journaled, "no second apply");
        let request = full_request(1, *mint, Utc::now());
        assert!(matches!(
            prepare_release(state, &request).await,
            Ok(Prepared::Blocked(_))
        ));
        for write in [
            Write::Release,
            Write::Rebalance,
            Write::Apply { release: None },
        ] {
            assert!(write_blocked(state, mint, write).await.unwrap().is_some());
        }
    }

    fn assert_landed_late(state: &SenderState, mint: &Pubkey, mock: &MockStorage) {
        let attempt = &attempts_of(mock)[0];
        assert_eq!(attempt.status, attempt_status::LANDED);
        assert_eq!(attempt.landed_slot, Some(60));
        let conf = state.confidential.as_ref().unwrap();
        let book = &conf.books[mint];
        assert_eq!(
            book.local, None,
            "available moved by what the apply applied"
        );
        let last = book.last_apply.unwrap();
        assert_eq!(last.prior_available, 5_000);
        assert_eq!(last.signature.to_string(), attempt.signature);
        assert!(conf.min_context_slot >= 60);
    }

    /// A landed apply leaves an available the sender verifies and keeps for a restart.
    #[tokio::test]
    async fn a_landed_apply_keeps_the_verified_available() {
        let mock = MockStorage::new();
        let (mut state, mut server, mint, _) =
            apply_chain(mock.clone(), Broadcast::NeverConfirms).await;
        land_late(&mut server).await;
        // The account as read after the apply landed (slot 60).
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 5_000, 0);
        sim.apply();
        let after = AccountSpec::healthy(&keys, &sim, 0, 5_000);
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getAccountInfo"}),
            ))
            .with_body(
                serde_json::json!({"jsonrpc": "2.0", "id": 0, "result": rpc_account(
                    &spl_token_2022::ID, &account_with_confidential(&after), 300)})
                .to_string(),
            )
            .create_async()
            .await;
        apply_and_repair(&mut state, &mint, ApplyTrigger::Background, None)
            .await
            .unwrap();
        assert_eq!(
            state.storage.confidential_mint_states().await.unwrap(),
            vec![(mint.to_string(), 5_000)]
        );
    }

    #[tokio::test]
    async fn an_apply_whose_send_failed_blocks_its_mint_until_it_lands_late() {
        let mock = MockStorage::new();
        let (mut state, mut server, mint, sends) =
            apply_chain(mock.clone(), Broadcast::SendFails).await;
        assert!(
            apply_once(&mut state, &mint, ApplyTrigger::Background, None)
                .await
                .is_err()
        );
        let attempts = attempts_of(&mock);
        assert_eq!(attempts.len(), 1, "journaled before the broadcast");
        assert_eq!(attempts[0].status, attempt_status::SENT);
        assert_eq!(attempts[0].applied_pending, Some(700));
        assert_mint_blocked(&mut state, &mint, &mock).await;

        land_late(&mut server).await;
        assert!(write_blocked(&mut state, &mint, Write::Release)
            .await
            .unwrap()
            .is_none());
        assert_landed_late(&state, &mint, &mock);
        assert_eq!(sends.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_apply_that_never_confirmed_blocks_its_mint_until_it_lands_late() {
        let mock = MockStorage::new();
        let (mut state, mut server, mint, sends) =
            apply_chain(mock.clone(), Broadcast::NeverConfirms).await;
        assert!(
            apply_once(&mut state, &mint, ApplyTrigger::Background, None)
                .await
                .is_err()
        );
        assert_eq!(attempts_of(&mock)[0].status, attempt_status::SENT);
        assert_mint_blocked(&mut state, &mint, &mock).await;

        land_late(&mut server).await;
        assert!(
            write_blocked(&mut state, &mint, Write::Apply { release: None })
                .await
                .unwrap()
                .is_none()
        );
        assert_landed_late(&state, &mint, &mock);
        assert_eq!(
            sends.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the AES copy is written once"
        );
    }

    #[tokio::test]
    async fn an_expired_apply_absent_from_history_clears_the_block() {
        let mock = MockStorage::new();
        let (mut state, mut server, mint, _) =
            apply_chain(mock.clone(), Broadcast::SendFails).await;
        assert!(
            apply_once(&mut state, &mint, ApplyTrigger::Background, None)
                .await
                .is_err()
        );
        assert!(write_blocked(&mut state, &mint, Write::Release)
            .await
            .unwrap()
            .is_some());
        expire(&mut server).await;
        assert!(write_blocked(&mut state, &mint, Write::Release)
            .await
            .unwrap()
            .is_none());
        assert_eq!(attempts_of(&mock)[0].status, attempt_status::DEAD);
        let book = state.confidential.as_ref().unwrap().books.get(&mint);
        assert!(book.is_none_or(|book| book.last_apply.is_none()));
    }

    #[tokio::test]
    async fn a_restart_settles_an_outstanding_apply_before_any_write() {
        let mock = MockStorage::new();
        let (_, _, mint, _) = apply_chain(mock.clone(), Broadcast::SendFails).await;
        // Left `sent` by the previous incarnation.
        Storage::Mock(mock.clone())
            .journal_operator_attempt(&OperatorAttempt {
                id: 0,
                mint: mint.to_string(),
                kind: ApplyTrigger::Background.label().into(),
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 100,
                blockhash_slot: 11,
                prior_available: Some(5_000),
                applied_pending: Some(700),
                status: attempt_status::SENT.into(),
                landed_slot: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        let (mut state, mut server, _, sends) =
            apply_chain(mock.clone(), Broadcast::SendFails).await;
        assert_eq!(settle_operator_attempts(&mut state, None).await.unwrap(), 1);
        assert_mint_blocked(&mut state, &mint, &mock).await;
        assert_eq!(sends.load(std::sync::atomic::Ordering::SeqCst), 0);

        land_late(&mut server).await;
        assert_eq!(settle_operator_attempts(&mut state, None).await.unwrap(), 0);
        assert_landed_late(&state, &mint, &mock);
    }

    #[tokio::test]
    async fn an_apply_also_waits_for_a_sent_rebalance_and_other_releases_of_its_mint() {
        let mock = MockStorage::new();
        let (mut state, _server, mint, _) = apply_chain(mock.clone(), Broadcast::SendFails).await;
        let storage = Storage::Mock(mock.clone());
        let id = storage
            .insert_rebalance_request(&mint.to_string(), RebalanceDirection::ToPublic, 5, 10, "r")
            .await
            .unwrap()
            .unwrap();
        storage
            .claim_rebalance_request(id, &Signature::new_unique().to_string(), 100, 11)
            .await
            .unwrap();
        let apply = Write::Apply { release: None };
        assert!(write_blocked(&mut state, &mint, apply)
            .await
            .unwrap()
            .is_some());
        // Proofs bind a release to the balance it read, so only the apply must wait.
        assert!(write_blocked(&mut state, &mint, Write::Release)
            .await
            .unwrap()
            .is_none());
        storage
            .transition_rebalance_request(
                id,
                rebalance_status::SENT,
                rebalance_status::FAILED,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(write_blocked(&mut state, &mint, apply)
            .await
            .unwrap()
            .is_none());

        // A release of the mint whose journaled attempt is not settled yet.
        push_processing_row(&mock, 9);
        {
            let mut rows = mock.pending_transactions.lock().unwrap();
            let row = rows.iter_mut().find(|row| row.id == 9).unwrap();
            row.mint = mint.to_string();
            row.confidential = true;
        }
        journal_one(&mock, 9);
        assert!(write_blocked(&mut state, &mint, apply)
            .await
            .unwrap()
            .is_some());
        let own = Write::Apply { release: Some(9) };
        assert!(
            write_blocked(&mut state, &mint, own)
                .await
                .unwrap()
                .is_none(),
            "the release asking for the apply does not block itself"
        );
        // Confirmed here while its row update is still on the way: settled, not in limbo.
        let conf = state.confidential.as_mut().unwrap();
        conf.settled_releases.insert(9);
        assert!(write_blocked(&mut state, &mint, apply)
            .await
            .unwrap()
            .is_none());
    }

    // ---- a pending past the search is applied from the indexed deposits --------------

    const MAX: u64 = private_channel_confidential::MAX_CONFIDENTIAL_AMOUNT;

    /// 257 maximum deposits: the high part of pending is past the 40-bit search.
    fn huge_pending(keys: &Keys) -> SimAccount {
        let mut sim = SimAccount::new(keys);
        for _ in 0..257 {
            sim.credit(keys, MAX, 0);
        }
        sim
    }

    fn inputs(deposits: &[(u64, u64)], moves: &[u64]) -> PendingInputs {
        PendingInputs {
            deposits: deposits.iter().map(|(slot, a)| (*slot, Some(*a))).collect(),
            moves: moves.to_vec(),
        }
    }

    #[test]
    fn the_indexed_pending_is_used_only_when_the_ciphertext_opens_to_it() {
        let keys = cts::keys();
        let sim = huge_pending(&keys);
        let (lo, hi) = (sim.pending_lo, sim.pending_hi);
        let rows: Vec<(u64, u64)> = (0..257).map(|i| (20 + i, MAX)).collect();
        assert_eq!(
            indexed_pending(&keys, &lo, &hi, &inputs(&rows, &[]), Some(10)),
            Some(257 * MAX)
        );
        // A wrong sum is refused.
        let mut wrong = rows.clone();
        wrong[3].1 -= 1;
        assert_eq!(
            indexed_pending(&keys, &lo, &hi, &inputs(&wrong, &[]), Some(10)),
            None
        );
        // The indexer is one deposit behind.
        let behind = &rows[..256];
        assert_eq!(
            indexed_pending(&keys, &lo, &hi, &inputs(behind, &[]), Some(10)),
            None
        );
        // A deposit the key could not decrypt leaves no candidate.
        let mut undecrypted = inputs(&rows, &[]);
        undecrypted.deposits[0].1 = None;
        assert_eq!(
            indexed_pending(&keys, &lo, &hi, &undecrypted, Some(10)),
            None
        );
    }

    #[test]
    fn the_indexed_pending_counts_moves_and_sorts_out_the_applys_own_slot() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        // 111 landed in the apply's slot before it, so it was applied; 222 after it.
        for amount in [222, 333, 50] {
            sim.credit(&keys, amount, 0);
        }
        let (lo, hi) = (sim.pending_lo, sim.pending_hi);
        let rows = [(10, 111), (10, 222), (11, 333)];
        assert_eq!(
            indexed_pending(&keys, &lo, &hi, &inputs(&rows, &[50]), Some(10)),
            Some(605)
        );
        assert_eq!(
            indexed_pending(&keys, &lo, &hi, &inputs(&rows, &[]), Some(10)),
            None
        );
    }

    #[test]
    fn candidate_d_after_an_apply_past_the_search_uses_the_verified_pending() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 5_000, 0);
        sim.apply();
        for _ in 0..257 {
            sim.credit(&keys, MAX, 0);
        }
        let (lo, hi) = (sim.pending_lo, sim.pending_hi);
        sim.apply();
        // The AES copy is wrong, so only (d) can explain the balance.
        let spec = AccountSpec::healthy(&keys, &sim, 0, 1);
        let d = candidate_from_apply(&keys, 5_000, &lo, &hi, Some(257 * MAX));
        assert_eq!(d, Some(5_000 + 257 * MAX));
        assert_eq!(
            verify_available(&keys, &view_of(&spec), None, d),
            Some((5_000 + 257 * MAX, CandidateSource::LastApply))
        );
    }

    fn unverified(mint: &Pubkey) -> f64 {
        metrics::OPERATOR_CONFIDENTIAL_PENDING_UNVERIFIED
            .with_label_values(&[&mint.to_string()])
            .get()
    }

    /// 257 maximum deposits arrive while the operator is down; once the indexer has them
    /// all, the apply goes out with exactly their sum.
    #[tokio::test]
    async fn a_pending_past_the_search_waits_for_the_indexer_then_applies() {
        let keys = cts::keys();
        let mut sim = SimAccount::new(&keys);
        sim.credit(&keys, 5_000, 0);
        sim.apply();
        for _ in 0..257 {
            sim.credit(&keys, MAX, 0);
        }
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 5_000);
        spec.pending_credit_counter = 257;
        let mock = MockStorage::new();
        let (mut state, mut server, mint, sends) =
            apply_chain_with(&spec, mock.clone(), Broadcast::NeverConfirms).await;
        // The last apply before the deposits landed at slot 10.
        mock.confidential_sender
            .lock()
            .unwrap()
            .attempts
            .push(OperatorAttempt {
                id: 1,
                mint: mint.to_string(),
                kind: "background".into(),
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 100,
                blockhash_slot: 9,
                prior_available: Some(0),
                applied_pending: Some(5_000),
                status: attempt_status::LANDED.into(),
                landed_slot: Some(10),
                created_at: Utc::now(),
            });
        let deposit = |slot: u64| {
            crate::storage::common::models::DbTransactionBuilder::new(
                Signature::new_unique().to_string(),
                slot,
                mint.to_string(),
                MAX,
            )
            .confidential(true)
            .initiator("user".into())
            .recipient("user".into())
            .transaction_type(crate::storage::common::models::TransactionType::Deposit)
            .build()
        };
        for slot in 11..267 {
            mock.pending_transactions
                .lock()
                .unwrap()
                .push(deposit(slot));
        }

        // One deposit not indexed yet: nothing is sent and the mint alerts.
        assert!(
            apply_once(&mut state, &mint, ApplyTrigger::Background, None)
                .await
                .is_err()
        );
        assert_eq!(attempts_of(&mock).len(), 1);
        assert_eq!(sends.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(unverified(&mint), 1.0);

        mock.pending_transactions.lock().unwrap().push(deposit(267));
        land_late(&mut server).await;
        apply_once(&mut state, &mint, ApplyTrigger::Background, None)
            .await
            .unwrap();
        let sent = &attempts_of(&mock)[1];
        assert_eq!(sent.applied_pending, Some(257 * MAX));
        assert_eq!(sent.prior_available, Some(5_000));
        assert_eq!(unverified(&mint), 0.0);
        let book = &state.confidential.as_ref().unwrap().books[&mint];
        assert_eq!(book.last_apply.unwrap().applied_pending, Some(257 * MAX));
    }
}
