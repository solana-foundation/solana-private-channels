//! Confidential releases, applies, AES repairs and rebalances, all run by the one sender
//! so they never race each other. Every balance the sender spends is first confirmed by
//! the opening check against an account read at confirmed commitment, no older than its
//! own last transaction.

use super::transaction::{
    claim_and_persist_or_abort, defer_remint_after_failure, handle_nonce_already_used,
    handle_nonce_outside_generation, handle_permanent_failure, handle_success, send_fenced_outcome,
    SignatureClaim,
};
use super::types::{InstructionWithSigners, PendingSig, SenderState, TransactionContext};
use crate::metrics;
use crate::operator::confidential_constants::*;
use crate::operator::recovery::load_pending_sigs;
use crate::operator::utils::instruction_util::{
    apply_pending_balance_instruction, move_to_confidential_instruction,
    move_to_public_instructions, ConfidentialReleaseRequest, EscrowConfidentialAccounts,
    TransactionBuilder, TransactionKind,
};
use crate::operator::utils::mint_util::{
    decode_confidential_account, ConfidentialAccountRead, ConfidentialAccountView,
};
use crate::operator::utils::transaction_util::{
    build_and_sign, build_and_sign_v1, escrow_error_for_code, failing_program, send_signed,
    V1BuildError, V1Error,
};
use crate::operator::TransactionStatusUpdate;
use crate::operator::{
    find_event_authority_pda, find_operator_pda, RetryPolicy, RpcClientWithRetry, SignerUtil,
    MAX_POLL_ATTEMPTS_CONFIRMATION,
};
use crate::storage::common::models::TransactionStatus;
use crate::storage::common::storage::confidential_sender::{
    rebalance_status, RebalanceDirection, RebalanceRequest,
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LastApply {
    pub prior_available: u64,
    pub signature: Signature,
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
    pub last_background_check: Option<Instant>,
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
            last_background_check: None,
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

/// Our last apply, as candidate (d) needs it: the available before it and what it applied.
pub type ApplyEvent = (u64, Ciphertext, Ciphertext);

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
        let d = apply.and_then(|(prior, lo, hi)| candidate_from_apply(&keys, prior, &lo, &hi));
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

/// Candidate (d): the available before our apply plus the pending it actually applied.
pub fn candidate_from_apply(
    keys: &Keys,
    prior_available: u64,
    applied_lo: &Ciphertext,
    applied_hi: &Ciphertext,
) -> Option<u64> {
    let applied = keys.decrypt_pending(applied_lo, applied_hi).ok()?;
    prior_available.checked_add(applied)
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
    /// Everything else takes today's path.
    Other,
}

/// Classify by the program that raised the code: escrow 17, 22 and 24 collide with
/// Token-2022's, so the code alone proves nothing.
pub fn classify_confidential(origin: Option<Pubkey>, code: Option<u32>) -> ConfidentialFailure {
    use token_2022_codes::*;
    let (Some(origin), Some(code)) = (origin, code) else {
        return ConfidentialFailure::Other;
    };
    if origin == spl_token_2022::ID {
        return match code {
            PUBKEY_MISMATCH | BALANCE_MISMATCH | FEE_MISMATCH | FEE_PARAMETERS_MISMATCH => {
                ConfidentialFailure::RebuildProofs
            }
            ACCOUNT_FROZEN | NOT_APPROVED | CREDITS_DISABLED | COUNTER_FULL => {
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
) -> Option<(Option<u32>, Option<Pubkey>)> {
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
    let code = result.err.as_ref().and_then(|err| {
        custom_code(&solana_sdk::transaction::TransactionError::from(
            err.clone(),
        ))
    });
    let origin = result.logs.as_deref().and_then(failing_program);
    Some((code, origin))
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

/// A request runs only if the verified available still equals what it was made against,
/// so a rerun after the move landed refuses instead of moving twice.
pub fn rebalance_decision(
    request: &RebalanceRequest,
    verified: Option<u64>,
    public: u64,
) -> RebalanceDecision {
    let Some(verified) = verified else {
        return RebalanceDecision::Wait;
    };
    if verified != request.expected_available {
        return RebalanceDecision::Refuse(format!(
            "verified available {verified} differs from expected {}",
            request.expected_available
        ));
    }
    match request.direction {
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
            Ok((lo, hi)) => Some((apply.prior_available, lo, hi)),
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
        code: Option<u32>,
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
                    code: custom_code(err),
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

/// Send an operator-only transaction (legacy; no proofs) and wait for it.
async fn send_operator_legacy(
    state: &SenderState,
    instructions: Vec<solana_sdk::instruction::Instruction>,
) -> Result<(Signature, u64), String> {
    let ix = InstructionWithSigners {
        instructions,
        fee_payer: TransactionBuilder::release_signers()[0].pubkey(),
        signers: TransactionBuilder::release_signers(),
        compute_unit_price: Some(1),
        compute_budget: Some(CU_CONFIDENTIAL_APPLY),
    };
    let (transaction, signature, _, _) = build_and_sign(&state.rpc_client, ix)
        .await
        .map_err(|e| e.to_string())?;
    send_signed(&state.rpc_client, &transaction, RetryPolicy::Idempotent)
        .await
        .map_err(|e| e.to_string())?;
    match confirm(
        &state.rpc_client,
        &signature,
        state.confirmation_poll_interval_ms,
    )
    .await?
    {
        Confirmation::Confirmed { slot } => Ok((signature, slot)),
        other => Err(format!("{signature}: {other:?}")),
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
) -> Result<(), String> {
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let (view, _) = read_ours(&state.rpc_client, conf, mint)
        .await?
        .ok_or("escrow account not configured with our key")?;
    let (verified, _) = verified_available(&state.rpc_client, conf, mint, &view)
        .await
        .ok_or("no candidate opens the available balance")?;
    let pending = pending_off_executor(conf.keys.clone(), &view)
        .await
        .map_err(|e| format!("pending: {e}"))?;
    let aes = conf.keys.encrypt_aes(
        verified
            .checked_add(pending)
            .ok_or("available plus pending overflows")?,
    );
    let ix =
        apply_pending_balance_instruction(&conf.accounts(mint), view.pending_credit_counter, aes);

    let (signature, slot) = send_operator_legacy(state, vec![ix]).await?;
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
    });
    info!(%mint, %signature, trigger = trigger.label(), "Applied pending balance");
    Ok(())
}

/// Apply, then repair the AES copy while deposits keep racing, within the budget.
pub async fn apply_and_repair(
    state: &mut SenderState,
    mint: &Pubkey,
    trigger: ApplyTrigger,
) -> Result<(), String> {
    if trigger != ApplyTrigger::Repair {
        if let Some(conf) = state.confidential.as_mut() {
            conf.books.entry(*mint).or_default().repair.fresh_budget();
        }
    }
    let mut trigger = trigger;
    loop {
        apply_once(state, mint, trigger).await?;
        let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
        let Some((view, _)) = read_ours(&state.rpc_client, conf, mint).await? else {
            return Ok(());
        };
        let stale = aes_is_stale(&conf.keys, &view);
        let conf = state.confidential.as_mut().ok_or("no confidential keys")?;
        let repair = &mut conf.books.entry(*mint).or_default().repair;
        if !stale {
            repair.on_fixed();
            metrics::OPERATOR_CONFIDENTIAL_AES_REPAIR_DEFERRED
                .with_label_values(&[&mint.to_string()])
                .set(0.0);
            return Ok(());
        }
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
    Park(&'static str, String),
}

async fn prepare_release(
    state: &mut SenderState,
    request: &ConfidentialReleaseRequest,
) -> Result<Prepared, String> {
    let mint = request.mint;
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
        apply_and_repair(state, &mint, ApplyTrigger::Release).await?;
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
    let proofs = match conf.keys.prove_release(
        &view.available,
        available,
        request.amount,
        &request.destination_elgamal,
        request.mint_params.auditor.as_ref(),
        request.mint_params.fee.as_ref(),
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

fn release_ctx(request: &ConfidentialReleaseRequest) -> TransactionContext {
    TransactionContext {
        transaction_id: Some(request.transaction_id),
        withdrawal_nonce: Some(request.nonce),
        trace_id: Some(request.trace_id.clone()),
        kind: TransactionKind::ReleaseFunds,
        deposit_claim_lease: None,
    }
}

fn release_ix(instructions: Vec<solana_sdk::instruction::Instruction>) -> InstructionWithSigners {
    let signers = TransactionBuilder::release_signers();
    InstructionWithSigners {
        instructions,
        fee_payer: signers[0].pubkey(),
        signers,
        compute_unit_price: Some(1),
        compute_budget: Some(CU_CONFIDENTIAL_RELEASE),
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
    }
}

async fn journaled(state: &SenderState, transaction_id: i64) -> Result<bool, String> {
    load_pending_sigs(&state.storage, transaction_id)
        .await
        .map(|sigs| !sigs.is_empty())
        .map_err(|_| "journal unreadable".to_string())
}

/// No balance could be verified: before any attempt the row waits in `Pending`; after one
/// it parks through the journal gate.
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
        Ok(true) => {
            park_through_journal(
                state,
                ctx,
                storage_tx,
                metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF,
                reason,
            )
            .await
        }
        // Unknown journal: leave the row Processing for recovery.
        Err(e) => {
            warn!(nonce, "Leaving the release to recovery: {e}");
            leave_to_recovery(state, nonce);
        }
    }
}

// Recovery owns the row now; a kept retry count would end its next run at max retries.
fn leave_to_recovery(state: &mut SenderState, nonce: u64) {
    state.in_flight_withdrawals.remove(&nonce);
    state.retry_counts.remove(&nonce);
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
                Ok(Prepared::Park(label, reason)) => {
                    park(state, &ctx, &request, storage_tx, label, &reason).await;
                    return;
                }
                // A read or apply failed: like a build or sign error, the row waits before
                // any attempt and parks stale through the journal gate after one.
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
        let ix = release_ix(instructions);

        // Retry bound shared with public releases.
        let attempts = state.retry_counts.get(&nonce).copied().unwrap_or(0);
        if attempts >= state.retry_max_attempts {
            handle_permanent_failure(state, &ctx, storage_tx, "Max retries exceeded").await;
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

        let failure = match state
            .rpc_client
            .send_versioned_transaction(&signed.transaction, RetryPolicy::Idempotent)
            .await
        {
            // Resent with the same proofs, inside the retry budget.
            Err(e) if resendable(&e) => {
                warn!(nonce, "Confidential release send failed, resending: {e}");
                cause = AttemptCause::TimeoutResend;
                continue;
            }
            Err(e) => match preflight_failure(&e) {
                Some((code, origin)) => {
                    let failure = classify_release(origin, code, &request.hook_extras);
                    Some((failure, None, e.to_string()))
                }
                None => {
                    handle_permanent_failure(state, &ctx, storage_tx, &e.to_string()).await;
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
                        settle_local(state, &request.mint, verified, request.amount, slot).await;
                        handle_success(state, &ctx, signed.signature, storage_tx).await;
                        forget_release(state, nonce);
                        return;
                    }
                    Ok(Confirmation::Timeout) => {
                        cause = AttemptCause::TimeoutResend;
                        continue;
                    }
                    Ok(Confirmation::Failed { slot, code, origin }) => Some((
                        classify_release(origin, code, &request.hook_extras),
                        Some(slot),
                        format!("code {code:?} from {origin:?}"),
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
                    park_through_journal(
                        state,
                        &ctx,
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
            ConfidentialFailure::Park(label) => {
                park_through_journal(state, &ctx, storage_tx, label, &detail).await;
                return;
            }
            ConfidentialFailure::Escrow(PrivateChannelEscrowProgramError::NonceAlreadyUsed) => {
                if handle_nonce_already_used(state, &ctx, signed.signature, storage_tx).await {
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
                    signed.signature,
                    slot.unwrap_or(0),
                    ix,
                    storage_tx,
                )
                .await;
                return;
            }
            ConfidentialFailure::Escrow(_) | ConfidentialFailure::Other => {
                handle_permanent_failure(state, &ctx, storage_tx, &detail).await;
                return;
            }
        }
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
        book.repair.on_fixed();
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
        return apply_and_repair(state, mint, ApplyTrigger::Background).await;
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
        return apply_and_repair(state, mint, ApplyTrigger::Repair).await;
    }
    Ok(())
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

/// `indexer rebalance`: record a request against the available balance verified now.
/// The sender runs it later and refuses it if that balance moved in between.
pub async fn request_rebalance(
    storage: &Storage,
    rpc: &RpcClientWithRetry,
    keys: &Keys,
    instance: &Pubkey,
    mint: &Pubkey,
    direction: RebalanceDirection,
    amount: u64,
) -> Result<i64, String> {
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
        .insert_rebalance_request(&request.mint, direction, amount, available)
        .await
        .map_err(|e| e.to_string())
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

/// Execute one request if the verified available still equals its `expected_available`.
pub async fn execute_rebalance(
    state: &mut SenderState,
    request: &RebalanceRequest,
) -> Result<(), String> {
    let mint = Pubkey::from_str(&request.mint).map_err(|e| format!("mint: {e}"))?;
    let conf = state.confidential.as_ref().ok_or("no confidential keys")?;
    let Some((view, _)) = read_ours(&state.rpc_client, conf, &mint).await? else {
        return refuse(state, request, "escrow account not configured with our key").await;
    };
    let verified = verified_available(&state.rpc_client, conf, &mint, &view).await;
    match rebalance_decision(request, verified.map(|(v, _)| v), view.public_amount) {
        RebalanceDecision::Wait => return Ok(()),
        RebalanceDecision::Refuse(reason) => return refuse(state, request, &reason).await,
        RebalanceDecision::Execute => {}
    }
    let available = request.expected_available;
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
            RebalanceDirection::ToPublic => available.checked_sub(request.amount),
            // Moves into pending; available is unchanged until the next apply.
            RebalanceDirection::ToConfidential => Some(available),
        };
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
            .send_versioned_transaction(&signed.transaction, RetryPolicy::Idempotent)
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
        Ok(Confirmation::Failed { code, .. }) => {
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
    use crate::operator::sender::test_support::{
        ensure_test_signer, push_processing_row, row_status, row_updated_at,
        sender_state_with_storage_and_role,
    };
    use crate::operator::utils::mint_util::confidential_fixtures::{
        account_with_confidential, rpc_account, AccountSpec,
    };
    use crate::operator::utils::mint_util::ConfidentialMintParams;
    use crate::storage::common::models::StoredSig;
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
        let d = candidate_from_apply(&keys, 500, &applied_lo, &applied_hi);
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
            candidate_from_apply(&keys, 7, &lo, &hi),
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
        let applied = (500, sim.pending_lo, sim.pending_hi);
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
            let d = apply.and_then(|(prior, lo, hi)| candidate_from_apply(&keys, prior, &lo, &hi));
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
            destination_elgamal: cts::other_keys().pubkey(),
            mint_params: ConfidentialMintParams {
                auditor: None,
                fee: None,
            },
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
    async fn after_a_journaled_attempt_it_parks_only_through_the_journal_gate() {
        let (_, spec) = escrow(5_000, 1);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        mock.release_signatures.lock().unwrap().insert(
            1,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 100,
                blockhash_slot: Some(9),
            }],
        );
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, mut rx) = mpsc::channel(8);

        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;

        // Not parked yet: handed to the finality gate with the park label remembered.
        assert_eq!(row_status(&mock, 1), Some(TransactionStatus::PendingRemint));
        assert_eq!(
            state
                .confidential
                .as_ref()
                .unwrap()
                .parks
                .get(&1)
                .map(String::as_str),
            Some(metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF)
        );
        assert_eq!(state.pending_remints.len(), 1);

        // The gate proved nothing landed: the row parks instead of reminting.
        assert!(finish_gated_park(&mut state, 1, None, "stale", PROVEN_ABSENT, &tx).await);
        let update = rx.recv().await.unwrap();
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(!update.remint_attempted);
        assert!(
            !finish_gated_park(&mut state, 1, None, "stale", PROVEN_ABSENT, &tx).await,
            "parks once"
        );
    }

    /// A row that parked through the journal gate after a journaled attempt.
    async fn gated_park(mock: &MockStorage) -> SenderState {
        let (_, spec) = escrow(5_000, 1);
        push_processing_row(mock, 1);
        mock.release_signatures.lock().unwrap().insert(
            1,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 100,
                blockhash_slot: Some(9),
            }],
        );
        let (mut state, server, _) = sender_reading(&spec, 10, mock.clone()).await;
        std::mem::forget(server);
        let lease = row_updated_at(mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        submit_release(
            &mut state,
            request_for(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
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

    /// A journaled row whose escrow account cannot be read parks stale through the journal
    /// gate, with its retry count cleared, instead of going to recovery.
    #[tokio::test]
    async fn an_unreadable_account_after_an_attempt_parks_through_the_gate() {
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        journal_one(&mock, 1);
        // Nothing answers getAccountInfo, so every read fails.
        let server = mockito::Server::new_async().await;
        let mut state =
            sender_state_with_storage_and_role(&server.url(), mock.clone(), ProgramType::Withdraw);
        state.confidential = Some(Box::new(ConfidentialSender::new(
            Arc::new(cts::keys()),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        )));
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
        assert_gated_stale_park(&state, &mock);
    }

    /// A build failure (here the blockhash fetch) after a journaled attempt parks the same way.
    #[tokio::test]
    async fn a_build_failure_after_an_attempt_parks_through_the_gate() {
        let (_, spec) = escrow(5_000, 5_000);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        journal_one(&mock, 1);
        let (mut state, _server, _) = sender_reading(&spec, 10, mock.clone()).await;
        state.retry_counts.insert(7, 1);
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
        assert_gated_stale_park(&state, &mock);
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

    fn assert_gated_stale_park(state: &SenderState, mock: &MockStorage) {
        assert_eq!(row_status(mock, 1), Some(TransactionStatus::PendingRemint));
        assert_eq!(
            state
                .confidential
                .as_ref()
                .unwrap()
                .parks
                .get(&1)
                .map(String::as_str),
            Some(metrics::BAIL_REASON_CONFIDENTIAL_STALE_PROOF)
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert!(state.retry_counts.is_empty(), "no retry count survives");
        assert!(!state.in_flight_withdrawals.contains(&7));
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

    /// How the mock answers a release's send and its status poll.
    #[derive(Clone, Copy)]
    enum SendReply {
        /// The node is unreachable: not a preflight refusal.
        Unreachable,
        /// Preflight refuses with `BlockhashNotFound`.
        BlockhashNotFound,
        /// Accepted, then every status poll errors.
        AcceptedThenStatusFails,
    }

    /// Run one release whose sends go wrong as `reply` says; returns how many times it fetched
    /// a blockhash, which is once per attempt.
    async fn release_with(reply: SendReply) -> (usize, MockStorage) {
        let (_, spec) = escrow(5_000, 5_000);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
        let (mut state, mut server, _) = sender_reading(&spec, 10, mock.clone()).await;
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
            SendReply::AcceptedThenStatusFails => send.with_body_from_request(|request| {
                // Echo the transaction's own signature, as a node does.
                let body: serde_json::Value =
                    serde_json::from_slice(request.body().unwrap()).unwrap();
                let wire = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    body["params"][0].as_str().unwrap(),
                )
                .unwrap();
                // A short signature count, then the first signature.
                let signature = Signature::try_from(&wire[1..65]).unwrap();
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": signature.to_string()})
                    .to_string()
                    .into()
            }),
        }
        .create_async()
        .await;
        server
            .mock("POST", "/")
            .match_body(rpc("getSignatureStatuses"))
            .with_status(503)
            .with_body("down")
            .create_async()
            .await;
        let lease = row_updated_at(&mock, 1).unwrap();
        let (tx, _rx) = mpsc::channel(64);
        submit_release(
            &mut state,
            full_request(1, Pubkey::new_unique(), lease),
            &tx,
            AttemptCause::First,
            true,
        )
        .await;
        (attempts.load(std::sync::atomic::Ordering::SeqCst), mock)
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
            let (attempts, mock) = release_with(reply).await;
            assert_eq!(attempts, 3, "one attempt per retry");
            assert_eq!(row_status(&mock, 1), Some(TransactionStatus::PendingRemint));
            // One blockhash in the mock, so every resend journals the same signature.
            assert!(!mock.release_signatures.lock().unwrap()[&1].is_empty());
        }
    }

    #[tokio::test]
    async fn a_balance_below_the_release_parks_before_anything_is_sent() {
        let (_, spec) = escrow(50, 50);
        let mock = MockStorage::new();
        push_processing_row(&mock, 1);
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
        settle_local(&mut state, &mint, 10, 4, 1).await;
        assert_eq!(
            state.confidential.as_ref().unwrap().books[&mint].repair,
            RepairState::default()
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
        for code in [ACCOUNT_FROZEN, NOT_APPROVED, CREDITS_DISABLED, COUNTER_FULL] {
            assert_eq!(
                classify_confidential(t22, Some(code)),
                ConfidentialFailure::Park(metrics::BAIL_REASON_DESTINATION_NOT_READY)
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
        // Unknown origin or code: today's path.
        assert_eq!(
            classify_confidential(None, Some(27)),
            ConfidentialFailure::Other
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
        let (code, origin) = preflight_failure(&err).unwrap();
        assert_eq!(
            classify_confidential(origin, code),
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
        let to_confidential = rebalance(RebalanceDirection::ToConfidential, 50, 1_000);
        assert_eq!(
            rebalance_decision(&to_confidential, Some(1_000), 50),
            RebalanceDecision::Execute
        );
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
            )
            .await
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
            .insert_rebalance_request("m", RebalanceDirection::ToConfidential, 5, 10)
            .await
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
            .insert_rebalance_request("m", RebalanceDirection::ToConfidential, 5, 10)
            .await
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
}
