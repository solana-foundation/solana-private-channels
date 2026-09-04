//! Recovers rows stuck in `Processing` after an operator crash.

use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::OperatorError;
use crate::metrics::{OPERATOR_RELEASE_VERIFY, OPERATOR_STALE_PROCESSING_RECOVERED};
use crate::operator::sender::types::PendingSig;
use crate::operator::sender::{
    classify_signatures, verify_release_landed, FinalityRpc, ReleaseVerdict, SigFinality,
};
use crate::operator::utils::rpc_util::RpcClientWithRetry;
use crate::operator::utils::storage_util::with_storage_backoff;
use crate::operator::TransactionStatusUpdate;
use crate::storage::common::models::{
    DbTransaction, StoredSig, TransactionStatus, TransactionType,
};
use crate::storage::common::storage::Storage;
use chrono::{DateTime, Utc};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// How often the recovery loop runs.
pub(crate) const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

/// Age cutoff for "stuck"; must exceed the sender's 30s drain + retries.
pub(crate) const STALE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// Per-tick batch cap; leftovers are picked up next tick.
pub(crate) const RECOVERY_BATCH_LIMIT: i64 = 100;

/// Max durable requeues before a stuck row is quarantined (paged). Bumped and
/// enforced by both this sweep and the sender's pre-broadcast requeue path.
pub(crate) const MAX_RECOVERY_REQUEUE_ATTEMPTS: i32 = 3;

/// How long a signatureless `Processing` withdrawal may keep failing its
/// on-chain proof before it is escalated to a human.
///
/// An inconclusive proof means the corroborating read was unavailable, not that
/// anything is wrong, and the row is left exactly where it was while we wait, so
/// the 60s sweep normally resolves it once the endpoint returns. Escalating on
/// the first inconclusive read would page for a passing RPC blip; never
/// escalating would wedge the nonce frontier in silence. This bounds the wait.
pub(crate) const RELEASE_PROOF_ESCALATE_AFTER: Duration =
    Duration::from_secs(2 * STALE_THRESHOLD.as_secs());

/// Deposit recovery outcome. Uncertainty must NOT demote (double-mint risk); an
/// in-flight signature leaves the row Processing for the next sweep. Shared with
/// the processor's pre-mint gate, which asks the same question at pickup time.
pub(crate) enum DepositOutcome {
    Landed { signature: String },
    NotLanded,
    Live { reason: String },
    Ambiguous { reason: String },
}

/// Withdrawal recovery outcome. We verify on-chain finality before demoting so
/// a release that already landed is never re-sent.
enum WithdrawalAction {
    /// Release finalized on-chain → mark Completed with that signature.
    /// `release_signatures` carries the full attempt list for durable provenance
    /// when an SMT-confirmed release supplies it; `None` otherwise.
    Complete {
        signature: String,
        release_signatures: Option<Vec<String>>,
    },
    /// Every recorded signature is dead → safe to requeue.
    Demote,
    /// A recorded signature could still land → re-evaluate next sweep.
    LeaveProcessing { reason: String },
    /// Uncertain (no signatures, or RPC could not classify) → page.
    Quarantine { reason: String },
}

/// The endpoint pair a recovery sweep works with, before a chain is chosen. The
/// sweep dispatches by row type and the row type fixes the chain: deposit mints
/// land on the channel, withdrawal releases on Solana.
pub(crate) struct RecoveryFinality<'a> {
    primary: &'a RpcClientWithRetry,
    fallback: Option<&'a RpcClientWithRetry>,
}

impl<'a> RecoveryFinality<'a> {
    pub(crate) fn new(
        primary: &'a RpcClientWithRetry,
        fallback: Option<&'a RpcClientWithRetry>,
    ) -> Self {
        Self { primary, fallback }
    }

    fn channel(&self) -> FinalityRpc<'a> {
        FinalityRpc::channel(self.primary, self.fallback)
    }

    fn solana(&self) -> FinalityRpc<'a> {
        FinalityRpc::solana(self.primary, self.fallback)
    }
}

/// Unified action for the storage router.
enum RecoveryAction {
    Complete {
        signature: String,
        release_signatures: Option<Vec<String>>,
    },
    Demote,
    /// Leave the row in Processing this tick (no CAS write).
    NoAction {
        reason: String,
    },
    Quarantine {
        reason: String,
    },
}

/// Recovery loop. First tick runs on boot (the prime crash-recovery moment).
#[allow(clippy::too_many_arguments)]
pub async fn run_recovery_worker(
    storage: Arc<Storage>,
    rpc_client: Arc<RpcClientWithRetry>,
    fallback_rpc_client: Option<Arc<RpcClientWithRetry>>,
    program_type: ProgramType,
    instance_pda: Option<Pubkey>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: CancellationToken,
) -> Result<(), OperatorError> {
    info!("Starting recovery worker");
    // Endpoints for the sweep. The optional fallback re-checks a Dead verdict;
    // None keeps recovery single-endpoint (legacy behavior).
    let finality = RecoveryFinality::new(&rpc_client, fallback_rpc_client.as_deref());
    let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Recovery worker received cancellation, exiting");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = recover_once(
                    &storage,
                    &finality,
                    program_type,
                    instance_pda,
                    &storage_tx,
                    &cancellation_token,
                    STALE_THRESHOLD,
                )
                .await
                {
                    // Per-row writes are independent; retry next tick.
                    warn!("Recovery tick failed: {}", e);
                }
            }
        }
    }
    Ok(())
}

/// The row type this operator services (same mapping the fetcher uses). Recovery
/// sweeps are scoped to it so an operator never classifies another chain's
/// signatures against its own RPC and wrongly demotes a landed row.
fn expected_transaction_type(program_type: ProgramType) -> TransactionType {
    match program_type {
        ProgramType::Escrow => TransactionType::Deposit,
        ProgramType::Withdraw => TransactionType::Withdrawal,
    }
}

async fn recover_once(
    storage: &Storage,
    finality: &RecoveryFinality<'_>,
    program_type: ProgramType,
    instance_pda: Option<Pubkey>,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
    threshold: Duration,
) -> Result<(), OperatorError> {
    // Best-effort GC of release/remint signatures whose parent left its live
    // status; a failure here must not block recovery.
    match storage.gc_stale_release_signatures().await {
        Ok(removed) => debug!(removed, "Recovery GC'd stale release signatures"),
        Err(e) => warn!("Recovery release-signature GC failed: {}", e),
    }
    match storage.gc_stale_remint_signatures().await {
        Ok(removed) => debug!(removed, "Recovery GC'd stale remint signatures"),
        Err(e) => warn!("Recovery remint-signature GC failed: {}", e),
    }

    let stale = storage
        .get_stale_processing_transactions(
            expected_transaction_type(program_type),
            threshold,
            RECOVERY_BATCH_LIMIT,
        )
        .await?;

    if !stale.is_empty() {
        debug!(
            count = stale.len(),
            "Recovery sweep found stale Processing rows"
        );
    }

    for row in stale {
        // Cooperate with shutdown between rows so long batches exit cleanly.
        if cancellation_token.is_cancelled() {
            info!("Recovery sweep cancelled; remaining rows deferred");
            return Ok(());
        }
        // Capture `updated_at` before the RPC so the write below CAS-checks it.
        let captured = row.updated_at;
        let action = decide_action(&row, storage, finality, instance_pda).await;
        route_outcome(storage, &row, captured, action, program_type, storage_tx).await;
    }

    // Rescue parked withdrawals orphaned by a restart. A live sender unparks
    // these itself, so anything stale here lost its in-memory driver. Parked
    // rows were never sent on-chain, so requeue them without verifying finality.
    let stale_parked = storage
        .get_stale_parked_transactions(
            expected_transaction_type(program_type),
            threshold,
            RECOVERY_BATCH_LIMIT,
        )
        .await?;
    for row in stale_parked {
        if cancellation_token.is_cancelled() {
            info!("Recovery sweep cancelled; remaining parked rows deferred");
            return Ok(());
        }
        requeue_parked(storage, &row, program_type).await;
    }
    Ok(())
}

async fn decide_action(
    row: &DbTransaction,
    storage: &Storage,
    finality: &RecoveryFinality<'_>,
    instance_pda: Option<Pubkey>,
) -> RecoveryAction {
    // Recovery is same-type by construction: the sweep queries filter on the
    // operator's own row type. The row type still selects the chain here, so a
    // deposit is never classified with Solana's height source or window.
    let action = match row.transaction_type {
        TransactionType::Deposit => match check_deposit(row, storage, &finality.channel()).await {
            DepositOutcome::Landed { signature } => RecoveryAction::Complete {
                signature,
                release_signatures: None,
            },
            DepositOutcome::NotLanded => RecoveryAction::Demote,
            DepositOutcome::Live { reason } => RecoveryAction::NoAction { reason },
            DepositOutcome::Ambiguous { reason } => RecoveryAction::Quarantine { reason },
        },
        TransactionType::Withdrawal => {
            match check_withdrawal(row, storage, &finality.solana(), instance_pda).await {
                WithdrawalAction::Complete {
                    signature,
                    release_signatures,
                } => RecoveryAction::Complete {
                    signature,
                    release_signatures,
                },
                WithdrawalAction::Demote => RecoveryAction::Demote,
                WithdrawalAction::LeaveProcessing { reason } => RecoveryAction::NoAction { reason },
                WithdrawalAction::Quarantine { reason } => RecoveryAction::Quarantine { reason },
            }
        }
    };
    // Cap recovery requeue attempts. Rows that fail to make progress after
    // MAX_RECOVERY_REQUEUE_ATTEMPTS are quarantined (and paged) rather than
    // looping between Pending and Processing indefinitely.
    if matches!(action, RecoveryAction::Demote)
        && row.recovery_requeue_attempts >= MAX_RECOVERY_REQUEUE_ATTEMPTS
    {
        return RecoveryAction::Quarantine {
            reason: format!(
                "exceeded {MAX_RECOVERY_REQUEUE_ATTEMPTS} recovery requeues without progress"
            ),
        };
    }
    action
}

/// Decide a stuck Processing deposit's fate from its persisted broadcast signatures.
/// Like `check_withdrawal`, but with no signatures a deposit Demotes (safe re-mint)
/// where a withdrawal Quarantines: the pre-broadcast persist makes "no signature" mean
/// "never broadcast", so re-minting cannot double-mint, and quarantining every such row
/// would flood manual review at deposit volume.
pub(crate) async fn check_deposit(
    row: &DbTransaction,
    storage: &Storage,
    finality: &FinalityRpc<'_>,
) -> DepositOutcome {
    // Retry a transient DB blip before treating the read as uncertainty; an
    // exhausted read still quarantines rather than risk a blind re-mint.
    let stored = match with_storage_backoff("journal read", row.id, || {
        storage.get_release_signatures(row.id)
    })
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return DepositOutcome::Ambiguous {
                reason: format!(
                    "could not verify mint landed (release signature lookup failed: {e})"
                ),
            }
        }
    };
    classify_deposit_signatures(&stored, finality).await
}

/// Classify a deposit's already-read write-ahead signatures. Shared by the
/// recovery sweep and the processor's pre-mint gate so the journal is read
/// exactly once per decision. A malformed stored signature is uncertainty,
/// never Dead, so a re-mint can never be authorized on an unreadable journal.
pub(crate) async fn classify_deposit_signatures(
    stored: &[StoredSig],
    finality: &FinalityRpc<'_>,
) -> DepositOutcome {
    if stored.is_empty() {
        return DepositOutcome::NotLanded;
    }

    let mut pending = Vec::with_capacity(stored.len());
    for entry in stored {
        let sig_str = &entry.signature;
        match Signature::from_str(sig_str) {
            Ok(signature) => pending.push(PendingSig {
                signature,
                last_valid_block_height: entry.last_valid_block_height as u64,
                blockhash_slot: entry.blockhash_slot.and_then(|s| u64::try_from(s).ok()),
            }),
             // Corrupt or tampered stored signature: treat as uncertain, never mint.
            Err(e) => {
                return DepositOutcome::Ambiguous {
                    reason: format!(
                        "could not verify mint landed (malformed stored release signature {sig_str}: {e})"
                    ),
                }
            }
        }
    }

    match classify_signatures(finality, &pending).await {
        SigFinality::Landed(sig) => DepositOutcome::Landed {
            signature: sig.to_string(),
        },
        SigFinality::Dead => DepositOutcome::NotLanded,
        // Still in flight; re-check next sweep rather than demote or complete.
        SigFinality::Live(reason) => DepositOutcome::Live { reason },
        // Never demote on uncertainty; risks a double-mint on re-pickup.
        SigFinality::Uncertain(reason) => DepositOutcome::Ambiguous {
            reason: format!("could not verify mint landed ({reason})"),
        },
    }
}

/// Whether a row has waited out the window allowed for an unresolvable read.
///
/// A negative age (clock skew) fails to convert and reads as inside the window,
/// which is the conservative direction.
fn proof_wait_expired(row: &DbTransaction) -> bool {
    Utc::now()
        .signed_duration_since(row.updated_at)
        .to_std()
        .is_ok_and(|age| age >= RELEASE_PROOF_ESCALATE_AFTER)
}

/// Decide a stuck Processing withdrawal's fate by verifying on-chain finality
/// of the persisted release signatures; never demote one whose release landed.
///
/// A row with no recorded signature provably never broadcast: the sender writes
/// the signature in the very transaction that claims the row, and the signature
/// GC only collects terminal rows. That would justify re-arming it on its own,
/// but a release cannot be undone, so the on-chain root has to corroborate it and
/// only proven non-inclusion re-arms. Re-arming is safe against a live sender
/// because both sides contend on one `updated_at` CAS: whichever writes first
/// invalidates the other's token, so a row demoted here can never also be
/// released by a builder still in flight.
async fn check_withdrawal(
    row: &DbTransaction,
    storage: &Storage,
    finality: &FinalityRpc<'_>,
    instance_pda: Option<Pubkey>,
) -> WithdrawalAction {
    let Some(nonce) = row.withdrawal_nonce else {
        return WithdrawalAction::Quarantine {
            reason: "withdrawal row missing nonce".to_string(),
        };
    };
    let nonce = nonce as u64;

    let pending = match load_pending_sigs(storage, row.id).await {
        Ok(p) => p,
        // Corruption is deterministic and proves a signature was recorded, so the
        // release may have broadcast. Re-reading returns the same bytes; escalate now.
        Err(e @ JournalError::Corrupt(_)) => {
            return WithdrawalAction::Quarantine {
                reason: e.to_string(),
            }
        }
        // This read is the same kind of unavailability the proof gate waits on, and
        // its internal retries span only a moment. Paging here would escalate on the
        // very outage that stranded the row, so wait it out on the same window.
        Err(e @ JournalError::Unavailable(_)) => {
            OPERATOR_RELEASE_VERIFY
                .with_label_values(&["presend", "journal_unavailable"])
                .inc();
            if proof_wait_expired(row) {
                return WithdrawalAction::Quarantine {
                    reason: format!(
                        "release signature journal still unreadable after {}s ({e})",
                        RELEASE_PROOF_ESCALATE_AFTER.as_secs()
                    ),
                };
            }
            // This row holds the dequeue frontier while it waits, so say so at warn:
            // the counter alone leaves a stalled pipeline looking idle.
            warn!(
                transaction_id = row.id,
                nonce,
                "Release signature journal unreadable; withdrawal held in Processing and \
                 blocking later nonces until it escalates: {e}"
            );
            return WithdrawalAction::LeaveProcessing {
                reason: format!("release signature journal unreadable ({e})"),
            };
        }
    };

    // Nothing recorded means nothing broadcast, but corroborate that on-chain
    // before re-arming a row whose release would be irreversible.
    if pending.is_empty() {
        // With no instance there is no root to compare against, so the proof can
        // never resolve and waiting on it would wedge the nonce frontier for
        // nothing. Keep the pre-proof behaviour for that configuration.
        let Some(instance_pda) = instance_pda else {
            return WithdrawalAction::Quarantine {
                reason: "no broadcast signatures recorded and \
                         no escrow instance configured to verify the release against"
                    .to_string(),
            };
        };
        // max_lvbh 0: nothing was broadcast, so there is no validity window to
        // outlast and no lvbh to derive one from. The tree-window and root checks
        // still apply in full.
        return match verify_release_landed(finality.primary, storage, Some(instance_pda), nonce, 0)
            .await
        {
            ReleaseVerdict::NotLanded => {
                OPERATOR_RELEASE_VERIFY
                    .with_label_values(&["presend", "not_landed"])
                    .inc();
                WithdrawalAction::Demote
            }
            ReleaseVerdict::Landed => {
                OPERATOR_RELEASE_VERIFY
                    .with_label_values(&["presend", "landed"])
                    .inc();
                // The write-ahead invariant is broken. Completing would fabricate
                // provenance we do not have, and demoting would re-send a released
                // nonce and credit the user twice, so this needs a human.
                WithdrawalAction::Quarantine {
                    reason: format!(
                        "nonce {nonce} released on-chain with no recorded broadcast signature"
                    ),
                }
            }
            ReleaseVerdict::Uncertain(reason) => {
                OPERATOR_RELEASE_VERIFY
                    .with_label_values(&["presend", "uncertain"])
                    .inc();
                // An unreadable proof is an unavailable corroboration, not evidence
                // of a problem, and leaving the row untouched costs nothing but a sweep.
                if proof_wait_expired(row) {
                    WithdrawalAction::Quarantine {
                        reason: format!(
                            "no broadcast signatures recorded and \
                             release verification still uncertain after {}s ({reason})",
                            RELEASE_PROOF_ESCALATE_AFTER.as_secs()
                        ),
                    }
                } else {
                    // This row holds the dequeue frontier while it waits, so say so
                    // at warn: the counter alone leaves a stalled pipeline looking idle.
                    warn!(
                        transaction_id = row.id,
                        nonce,
                        "Release proof unavailable; withdrawal held in Processing and blocking \
                         later nonces until it escalates: {reason}"
                    );
                    WithdrawalAction::LeaveProcessing {
                        reason: format!(
                            "no broadcast signatures recorded and release verification \
                             unavailable ({reason})"
                        ),
                    }
                }
            }
        };
    }

    match classify_signatures(finality, &pending).await {
        SigFinality::Landed(sig) => WithdrawalAction::Complete {
            signature: sig.to_string(),
            release_signatures: None,
        },
        // The classifier calls the release dead by absence. Confirm it against the
        // on-chain SMT root before demoting, since a pruned/lagging endpoint can
        // report absence for a release that actually landed.
        SigFinality::Dead => {
            // pending is non-empty here, so max() is always Some.
            let max_lvbh = pending
                .iter()
                .map(|p| p.last_valid_block_height)
                .max()
                .unwrap_or(0);
            match verify_release_landed(finality.primary, storage, instance_pda, nonce, max_lvbh)
                .await
            {
                ReleaseVerdict::Landed => {
                    OPERATOR_RELEASE_VERIFY
                        .with_label_values(&["recovery", "landed"])
                        .inc();
                    // The SMT proves the nonce released but not which attempt landed.
                    // On-chain exclusion-proof insertion means only the earliest attempt
                    // can consume the nonce, so the first recorded signature is the
                    // correct single-value provenance; the full list is kept durably.
                    WithdrawalAction::Complete {
                        signature: pending[0].signature.to_string(),
                        release_signatures: Some(
                            pending.iter().map(|p| p.signature.to_string()).collect(),
                        ),
                    }
                }
                ReleaseVerdict::NotLanded => {
                    OPERATOR_RELEASE_VERIFY
                        .with_label_values(&["recovery", "not_landed"])
                        .inc();
                    WithdrawalAction::Demote
                }
                ReleaseVerdict::Uncertain(reason) => {
                    OPERATOR_RELEASE_VERIFY
                        .with_label_values(&["recovery", "uncertain"])
                        .inc();
                    WithdrawalAction::Quarantine {
                        reason: format!("release verification uncertain ({reason})"),
                    }
                }
            }
        }
        SigFinality::Live(reason) => WithdrawalAction::LeaveProcessing { reason },
        SigFinality::Uncertain(reason) => WithdrawalAction::Quarantine {
            reason: format!(
                "could not verify release landed ({reason}); signatures: {}",
                pending
                    .iter()
                    .map(|p| p.signature.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        },
    }
}

/// Why a signature journal read produced no usable list. The two cases pull in
/// opposite directions: an unread journal says nothing about the row and is worth
/// waiting on, while one that reads back corrupt will read back corrupt forever.
pub(crate) enum JournalError {
    /// The read itself failed, so the journal's contents are still unknown.
    Unavailable(String),
    /// The journal was read and holds a signature that will not parse.
    Corrupt(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(reason) | Self::Corrupt(reason) => f.write_str(reason),
        }
    }
}

/// Load and parse a row's persisted broadcast signatures into `PendingSig`s for the
/// finality classifier. Shared by the withdrawal recovery sweep and the sender's
/// permanent-failure path; deposits read the journal through `check_deposit`. Neither
/// error is ever read as "dead", so callers never demote a row whose signatures could
/// not be read or parsed.
pub(crate) async fn load_pending_sigs(
    storage: &Storage,
    id: i64,
) -> Result<Vec<PendingSig>, JournalError> {
    // Absorbs a brief blip only; a longer outage is the caller's to wait out.
    let stored = with_storage_backoff("journal read", id, || storage.get_release_signatures(id))
        .await
        .map_err(|e| JournalError::Unavailable(format!("release signature lookup failed: {e}")))?;

    let mut pending = Vec::with_capacity(stored.len());
    for entry in &stored {
        let sig_str = &entry.signature;
        let signature = Signature::from_str(sig_str).map_err(|e| {
            JournalError::Corrupt(format!("malformed stored release signature {sig_str}: {e}"))
        })?;
        pending.push(PendingSig {
            signature,
            last_valid_block_height: entry.last_valid_block_height as u64,
            blockhash_slot: entry.blockhash_slot.and_then(|s| u64::try_from(s).ok()),
        });
    }
    Ok(pending)
}

fn pt_label(program_type: ProgramType) -> &'static str {
    match program_type {
        ProgramType::Escrow => "escrow",
        ProgramType::Withdraw => "withdraw",
    }
}

/// Requeue an orphaned `Parked` row to `Pending` so the processor rebuilds it.
async fn requeue_parked(storage: &Storage, row: &DbTransaction, program_type: ProgramType) {
    match storage.try_requeue_parked(row.id, row.updated_at).await {
        Ok(true) => {
            info!(
                transaction_id = row.id,
                "Recovery requeued orphaned Parked → Pending"
            );
            OPERATOR_STALE_PROCESSING_RECOVERED
                .with_label_values(&[pt_label(program_type), "requeued_parked", "withdrawal"])
                .inc();
        }
        Ok(false) => debug!(
            id = row.id,
            "parked requeue skipped — another writer touched the row first"
        ),
        Err(e) => warn!(id = row.id, "parked requeue write error: {}", e),
    }
}

async fn route_outcome(
    storage: &Storage,
    row: &DbTransaction,
    captured_updated_at: DateTime<Utc>,
    action: RecoveryAction,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let pt_label = pt_label(program_type);
    let type_label = match row.transaction_type {
        TransactionType::Deposit => "deposit",
        TransactionType::Withdrawal => "withdrawal",
    };

    match action {
        RecoveryAction::Complete {
            signature,
            release_signatures,
        } => {
            match storage
                .try_complete_processing(
                    row.id,
                    captured_updated_at,
                    Some(signature.clone()),
                    release_signatures,
                )
                .await
            {
                Ok(true) => {
                    info!(
                        transaction_id = row.id,
                        signature, "Recovery promoted stale Processing → Completed"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "completed", type_label])
                        .inc();
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
        RecoveryAction::Demote => {
            // Trigger bumps `updated_at`; the next sweep skips it.
            match storage
                .try_requeue_processing(row.id, captured_updated_at)
                .await
            {
                Ok(true) => {
                    info!(
                        transaction_id = row.id,
                        "Recovery demoted stale Processing → Pending"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "requeued", type_label])
                        .inc();
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
        RecoveryAction::NoAction { reason } => {
            // Release could still land; leave Processing untouched (no CAS write).
            debug!(
                transaction_id = row.id,
                reason = %reason,
                "Recovery left stale Processing row untouched — broadcast may still land"
            );
        }
        RecoveryAction::Quarantine { reason } => {
            // Noisy by design — page on uncertainty, never silently demote.
            match storage
                .try_quarantine_processing(row.id, captured_updated_at)
                .await
            {
                Ok(true) => {
                    warn!(
                        transaction_id = row.id,
                        reason = %reason,
                        "Recovery quarantined stale Processing → ManualReview"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "quarantined", type_label])
                        .inc();
                    // Fire the existing webhook + alert log (see sender/state.rs).
                    let update = TransactionStatusUpdate {
                        transaction_id: row.id,
                        trace_id: Some(row.trace_id.clone()),
                        status: TransactionStatus::ManualReview,
                        counterpart_signature: None,
                        processed_at: Some(Utc::now()),
                        error_message: Some(reason),
                        remint_signature: None,
                        remint_attempted: false,
                        release_signatures: None,
                    };
                    // Closed channel = on-call alert lost; surface it loudly.
                    if let Err(e) =
                        send_guaranteed(storage_tx, update, "recovery manual review").await
                    {
                        warn!(
                            transaction_id = row.id,
                            "Recovery quarantined row but failed to deliver alert webhook: {}", e
                        );
                    }
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
    }
}

/// Synchronous boot pre-flight reconcile: repeatedly run `recover_once` with a
/// `Duration::ZERO` threshold (so even a fresh crash row is reconciled) until no
/// `Processing` rows remain, bounded by `max_passes`. A withdraw operator is
/// single-active (SMT nonce ordering forbids a second sender), so at boot there
/// is no live sibling whose not-yet-stale work this could disrupt. Exhausting
/// `max_passes` with rows still `Processing` returns `Ok`: the caller's
/// `validate_smt_root` is the terminal gate that refuses to start on a real mismatch.
#[allow(clippy::too_many_arguments)]
pub async fn boot_reconcile_processing(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    fallback_rpc_client: Option<Arc<RpcClientWithRetry>>,
    program_type: ProgramType,
    instance_pda: Option<Pubkey>,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
    max_passes: u32,
) -> Result<(), OperatorError> {
    // Same endpoint bundle the periodic worker uses; None stays single-endpoint.
    let finality = RecoveryFinality::new(rpc_client, fallback_rpc_client.as_deref());
    for pass in 0..max_passes {
        recover_once(
            storage,
            &finality,
            program_type,
            instance_pda,
            storage_tx,
            cancellation_token,
            Duration::ZERO,
        )
        .await?;

        let remaining = storage
            .get_stale_processing_transactions(
                expected_transaction_type(program_type),
                Duration::ZERO,
                RECOVERY_BATCH_LIMIT,
            )
            .await?;
        if remaining.is_empty() {
            return Ok(());
        }
        debug!(
            pass,
            remaining = remaining.len(),
            "Boot reconcile still has Processing rows; iterating"
        );
    }
    warn!(
        max_passes,
        "Boot reconcile exhausted its pass budget with Processing rows remaining"
    );
    Ok(())
}

/// Promote PendingRemint withdrawals whose release already landed on-chain to
/// Completed, so the boot SMT validation sees the consumed nonce instead of
/// refusing to start on a spurious mismatch. Deadline is ignored: a landed
/// nonce diverges the root the moment it lands. Best-effort like
/// boot_reconcile_processing; on any error validate_smt_root still fails closed.
pub async fn boot_reconcile_landed_pending_remints(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    fallback_rpc_client: Option<&RpcClientWithRetry>,
) -> Result<(), OperatorError> {
    // Same endpoint bundle the sweep uses: a pruned primary must not turn a
    // landed release into an absence that leaves the row PendingRemint and
    // refuses the boot validation.
    let finality = RecoveryFinality::new(rpc_client, fallback_rpc_client);
    for row in storage.get_pending_remint_transactions().await? {
        let Some(nonce) = row.withdrawal_nonce else {
            continue;
        };

        // Signatures live on the row, not the release_signatures table. Without
        // a well-formed set we cannot verify finality, so skip and let the live
        // sender escalate; validate_smt_root still fails closed if it landed.
        let sig_strings = row.remint_signatures.unwrap_or_default();
        let lvbhs = row.remint_last_valid_block_heights.unwrap_or_default();
        if sig_strings.is_empty() || sig_strings.len() != lvbhs.len() {
            continue;
        }
        let parsed: Result<Vec<PendingSig>, ()> = sig_strings
            .iter()
            .zip(&lvbhs)
            .map(|(sig_str, &lvbh)| {
                Ok(PendingSig {
                    signature: Signature::from_str(sig_str).map_err(|_| ())?,
                    last_valid_block_height: u64::try_from(lvbh).map_err(|_| ())?,
                    // The row predates the journal that records it; the classifier
                    // derives the floor from the window instead.
                    blockhash_slot: None,
                })
            })
            .collect();
        let Ok(signatures) = parsed else {
            continue;
        };

        // Only a finalized-success release consumed the nonce. Dead/Live are
        // correctly absent from the on-chain root, so leave them PendingRemint.
        match classify_signatures(&finality.solana(), &signatures).await {
            SigFinality::Landed(sig) => match storage
                .update_transaction_status(
                    row.id,
                    TransactionStatus::Completed,
                    Some(sig.to_string()),
                    Utc::now(),
                    None,
                )
                .await
            {
                Ok(true) => info!(
                    transaction_id = row.id,
                    nonce,
                    signature = %sig,
                    "Boot reconcile promoted landed PendingRemint to Completed"
                ),
                // A rolling-restart sibling already completed it.
                Ok(false) => debug!(
                    transaction_id = row.id,
                    "Boot reconcile skipped PendingRemint already past pending_remint"
                ),
                Err(e) => warn!(
                    transaction_id = row.id,
                    "Boot reconcile failed to complete landed PendingRemint: {}", e
                ),
            },
            // Could not classify (RPC failure/length mismatch). The nonce stays
            // out of the local tree, so if it did land validate_smt_root refuses;
            // log it so that refusal is traceable to a flaky boot RPC.
            SigFinality::Uncertain(reason) => warn!(
                transaction_id = row.id,
                nonce, "Boot reconcile could not classify PendingRemint signatures: {}", reason
            ),
            // Live hasn't finalized yet, so it hasn't consumed the nonce; the
            // next boot/tick promotes it once it does. Dead never will. Either
            // way, leave it PendingRemint.
            SigFinality::Live(_) | SigFinality::Dead => {}
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only entry to drive a single recovery tick deterministically.
    use super::*;

    pub async fn run_recovery_once(
        storage: &Storage,
        rpc_client: &RpcClientWithRetry,
        program_type: ProgramType,
        instance_pda: Option<Pubkey>,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        // Fresh, never-cancelled token; tests run to completion. Uses the periodic
        // worker's STALE_THRESHOLD; the ZERO boot threshold is exercised by calling
        // recover_once directly.
        let token = CancellationToken::new();
        // Single-endpoint bundle: the worker and boot reconcile pass a fallback
        // via their own params; this test hook keeps legacy behavior.
        let finality = RecoveryFinality::new(rpc_client, None);
        recover_once(
            storage,
            &finality,
            program_type,
            instance_pda,
            storage_tx,
            &token,
            STALE_THRESHOLD,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::utils::rpc_util::RetryConfig;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::storage::mock::MockStorage;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;

    fn make_deposit_row(id: i64) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id,
            signature: format!("sig-{id}"),
            instruction_index: 0,
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
            recipient: Pubkey::new_unique().to_string(),
            mint: Pubkey::new_unique().to_string(),
            amount: TokenAmount(1_000),
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Processing,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            inner_index: None,
            landed_remint_signature: None,
        }
    }

    fn make_withdrawal_row(id: i64, nonce: Option<i64>) -> DbTransaction {
        let mut row = make_deposit_row(id);
        row.transaction_type = TransactionType::Withdrawal;
        row.withdrawal_nonce = nonce;
        row
    }

    /// Deposit-path bundle: mints land on the channel, at the default window.
    fn channel_finality(client: &RpcClientWithRetry) -> FinalityRpc<'_> {
        FinalityRpc::channel(client, None)
    }

    /// Sweep-level bundle for tests that drive `recover_once` / `decide_action`.
    fn recovery_finality(client: &RpcClientWithRetry) -> RecoveryFinality<'_> {
        RecoveryFinality::new(client, None)
    }

    fn make_rpc_client(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    // ── check_deposit outcome matrix (signature-driven) ──────────────

    fn mock_null_status(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create()
    }

    fn mock_block_height(server: &mut mockito::ServerGuard, height: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(format!(r#"{{"jsonrpc":"2.0","result":{height},"id":1}}"#))
            .create()
    }

    /// Finalized getLatestBlockhash carrying `context_slot` and `lvbh`, the verifier's
    /// freshness anchor: it derives tip = lvbh - MAX_PROCESSING_AGE and binds the
    /// instance account read to `context_slot`. Pick lvbh so tip is above (fresh) or
    /// at/below (stale) the attempt's lvbh.
    fn mock_latest_blockhash(
        server: &mut mockito::ServerGuard,
        context_slot: u64,
        lvbh: u64,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":{context_slot}}},"value":{{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":{lvbh}}}}},"id":1}}"#
            ))
            .create()
    }

    /// Covered ledger floor so an absence-Dead resolves to Dead, not a prune.
    fn mock_first_available_block(server: &mut mockito::ServerGuard, floor: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getFirstAvailableBlock""#.into(),
            ))
            .with_status(200)
            .with_body(format!(r#"{{"jsonrpc":"2.0","result":{floor},"id":1}}"#))
            .create()
    }

    /// Root of a fresh tree carrying `nonces` (used to craft an on-chain instance).
    fn smt_root(tree_index: u64, nonces: &[u64]) -> [u8; 32] {
        use crate::operator::utils::smt_util::SmtState;
        let mut smt = SmtState::new(tree_index);
        for n in nonces {
            smt.insert_nonce(*n);
        }
        smt.current_root()
    }

    /// getAccountInfo mock returning an escrow Instance with `root`/`tree_index`
    /// at response context `slot`. `expect` bounds how many calls are allowed so a
    /// fast-path test can assert the finalized read never fired (expect 0).
    fn mock_instance_at_slot(
        server: &mut mockito::ServerGuard,
        root: [u8; 32],
        tree_index: u64,
        slot: u64,
        expect: usize,
    ) -> mockito::Mock {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use borsh::BorshSerialize;
        use private_channel_escrow_program_client::Instance;
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: root,
            current_tree_index: tree_index,
        };
        let mut bytes = Vec::new();
        instance.serialize(&mut bytes).unwrap();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": slot},
                        "value": {
                            "owner": Pubkey::new_unique().to_string(),
                            "lamports": 1_000_000u64,
                            "data": [STANDARD.encode(&bytes), "base64"],
                            "executable": false,
                            "rentEpoch": 0
                        }
                    }
                })
                .to_string(),
            )
            .expect(expect)
            .create()
    }

    /// A deposit with no persisted signature is provably never broadcast (the
    /// signature is written in the same transaction that claims the row), so it
    /// Demotes for a safe re-mint rather than Quarantining. No RPC is consulted.
    ///
    /// The withdrawal side reaches the same conclusion now, but pays for an
    /// on-chain non-inclusion proof first because a release cannot be undone.
    #[tokio::test]
    async fn deposit_no_sigs_demotes() {
        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client("http://localhost:1");
        let row = make_deposit_row(1);
        let outcome = check_deposit(&row, &storage, &channel_finality(&client)).await;
        assert!(
            matches!(outcome, DepositOutcome::NotLanded),
            "empty sigs must map to NotLanded (Demote), not Ambiguous/Quarantine"
        );
    }

    /// A transient DB blip on the deposit journal read is absorbed by the
    /// bounded retry: the read recovers and classifies normally instead of
    /// quarantining the row as uncertain.
    #[tokio::test]
    async fn deposit_read_blip_recovers_not_quarantined() {
        let mock = MockStorage::new();
        // First two reads fail, the third succeeds (empty): inside the budget.
        mock.set_fail_times("get_release_signatures", 2);
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let row = make_deposit_row(1);
        let outcome = check_deposit(&row, &storage, &channel_finality(&client)).await;
        assert!(
            matches!(outcome, DepositOutcome::NotLanded),
            "a transient read blip must be retried, not quarantined as Ambiguous"
        );
    }

    /// A finalized-success signature returns Landed and is never re-minted.
    #[tokio::test]
    async fn deposit_landed_sig_completes_without_remint() {
        let landed_sig = Signature::new_unique();
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        match check_deposit(&row, &storage, &channel_finality(&client)).await {
            DepositOutcome::Landed { signature } => assert_eq!(signature, landed_sig.to_string()),
            _ => panic!("expected Landed"),
        }
    }

    /// A null-status sig past blockhash validity is dead: NotLanded, safe to re-mint.
    #[tokio::test]
    async fn deposit_dead_sigs_demote() {
        let mut server = mockito::Server::new_async().await;
        // Block height 200 is past lvbh 100, so the absence is expired.
        let _status = mock_null_status(&mut server);
        let _height = mock_block_height(&mut server, 200);
        // Covered floor (0) so the single-endpoint absence is proven Dead.
        let _floor = mock_first_available_block(&mut server, 0);

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, Some(0))
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        assert!(
            matches!(
                check_deposit(&row, &storage, &channel_finality(&client)).await,
                DepositOutcome::NotLanded
            ),
            "dead sigs map to NotLanded (Demote)"
        );
    }

    /// A sig still within blockhash validity is Live: leave Processing this sweep.
    #[tokio::test]
    async fn deposit_live_sig_leaves_processing() {
        let mut server = mockito::Server::new_async().await;
        // Block height 200 is below lvbh 1000, so the sig can still land.
        let _status = mock_null_status(&mut server);
        let _height = mock_block_height(&mut server, 200);

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        assert!(
            matches!(
                check_deposit(&row, &storage, &channel_finality(&client)).await,
                DepositOutcome::Live { .. }
            ),
            "a still-live sig must leave the row Processing, not demote"
        );
    }

    /// An RPC failure during classification is uncertain: Ambiguous, never demote.
    #[tokio::test]
    async fn deposit_rpc_uncertain_quarantines() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("internal server error")
            .create();

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        match check_deposit(&row, &storage, &channel_finality(&client)).await {
            DepositOutcome::Ambiguous { reason } => {
                assert!(
                    reason.contains("could not verify mint landed"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Ambiguous"),
        }
    }

    /// A malformed stored signature is uncertainty, never read as "dead"; it
    /// must Quarantine rather than demote.
    #[tokio::test]
    async fn deposit_malformed_stored_sig_quarantines() {
        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(
            row.id,
            "not-a-valid-base58-signature".to_string(),
            100,
            None,
        )
        .await
        .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");

        match check_deposit(&row, &storage, &channel_finality(&client)).await {
            DepositOutcome::Ambiguous { reason } => {
                assert!(
                    reason.contains("malformed stored release signature"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Ambiguous on malformed signature"),
        }
    }

    // ── check_withdrawal outcome matrix ───────────────────────────────

    /// Missing nonce → quarantine before any RPC/storage read.
    #[tokio::test]
    async fn check_withdrawal_quarantines_when_nonce_missing() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let row = make_withdrawal_row(1, None);
        let action =
            check_withdrawal(&row, &storage, &FinalityRpc::solana(&client, None), None).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(reason.contains("withdrawal row missing nonce"));
            }
            _ => panic!("expected Quarantine"),
        }
    }

    // ── signatureless withdrawal: proof-gated requeue ─────────────────

    /// Mocks for the signatureless proof: a fresh finalized blockhash to bind the
    /// account read, and an instance holding `root` on `tree_index`. No signature
    /// classification happens on this branch, so nothing else is consulted.
    fn mock_release_proof(
        server: &mut mockito::ServerGuard,
        root: [u8; 32],
        tree_index: u64,
    ) -> mockito::Mock {
        let _bh = mock_latest_blockhash(server, 500, 1000);
        mock_instance_at_slot(server, root, tree_index, 1000, 1)
    }

    /// The fix. A `Processing` withdrawal with no recorded signature never
    /// broadcast, and the on-chain root corroborates it: the nonce is absent, so
    /// the row is re-armed instead of paging a human.
    #[tokio::test]
    async fn no_sigs_not_landed_demotes() {
        let mut server = mockito::Server::new_async().await;
        // Tree 0 with no completed nonces: the root excludes nonce 3.
        let _proof = mock_release_proof(&mut server, smt_root(0, &[]), 0);

        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client(&server.url());
        let row = make_withdrawal_row(1, Some(3));

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(
            matches!(action, WithdrawalAction::Demote),
            "proven non-inclusion must re-arm the row, not quarantine it"
        );
    }

    /// Released on-chain with nothing recorded: the write-ahead invariant is
    /// broken. Completing would fabricate provenance and demoting would pay the
    /// user twice, so the only safe verdict is a page.
    #[tokio::test]
    async fn no_sigs_landed_quarantines_as_invariant_violation() {
        let mut server = mockito::Server::new_async().await;
        // The on-chain root includes nonce 3 while the journal holds nothing.
        let _proof = mock_release_proof(&mut server, smt_root(0, &[3]), 0);

        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client(&server.url());
        let row = make_withdrawal_row(1, Some(3));

        match check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await
        {
            WithdrawalAction::Quarantine { reason } => assert!(
                reason.contains("no recorded broadcast signature"),
                "reason must name the missing signature: {reason}"
            ),
            _ => panic!("a landed release with no recorded signature must Quarantine"),
        }
    }

    /// A Withdraw operator may run without an escrow instance configured, and
    /// then the proof can never resolve. Waiting out the escalation window would
    /// hold the nonce frontier the whole time to reach the same verdict, so that
    /// configuration keeps the immediate quarantine and consults no RPC.
    #[tokio::test]
    async fn no_sigs_without_instance_quarantines_immediately() {
        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client("http://localhost:1");
        let row = make_withdrawal_row(1, Some(3));

        match check_withdrawal(&row, &storage, &FinalityRpc::solana(&client, None), None).await {
            WithdrawalAction::Quarantine { reason } => assert!(
                reason.contains("no escrow instance configured"),
                "reason must name the missing instance: {reason}"
            ),
            _ => panic!("an unprovable configuration must not stall the frontier"),
        }
    }

    /// An unreadable proof is not evidence of a problem. Inside the escalation
    /// window the row is left exactly where it was, so a DB outage that strands a
    /// row and an RPC outage that hides the proof do not together page a human.
    #[tokio::test]
    async fn no_sigs_uncertain_inside_window_leaves_processing() {
        let mut server = mockito::Server::new_async().await;
        let _down = server.mock("POST", "/").with_status(503).create();

        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client(&server.url());
        let row = make_withdrawal_row(1, Some(3));

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(
            matches!(action, WithdrawalAction::LeaveProcessing { .. }),
            "an unavailable proof must neither demote nor quarantine inside the window"
        );
    }

    /// The window is bounded: a row whose proof stays unreadable still pages, or
    /// it would wedge the nonce frontier in silence.
    #[tokio::test]
    async fn no_sigs_uncertain_past_window_quarantines() {
        let mut server = mockito::Server::new_async().await;
        let _down = server.mock("POST", "/").with_status(503).create();

        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client(&server.url());
        let mut row = make_withdrawal_row(1, Some(3));
        row.updated_at = Utc::now()
            - chrono::Duration::from_std(RELEASE_PROOF_ESCALATE_AFTER).unwrap()
            - chrono::Duration::seconds(1);

        assert!(
            matches!(
                check_withdrawal(
                    &row,
                    &storage,
                    &FinalityRpc::solana(&client, None),
                    Some(Pubkey::new_unique()),
                )
                .await,
                WithdrawalAction::Quarantine { .. }
            ),
            "an unresolvable proof must escalate once it ages past the window"
        );
    }

    /// The journal read is itself a DB read, and its internal retries only cover
    /// about 200ms. An outage past that says nothing about the row, so it waits on
    /// the same window an unreadable proof does instead of paging on sight.
    #[tokio::test]
    async fn journal_unreadable_inside_window_leaves_processing() {
        let mock = MockStorage::new();
        mock.set_should_fail("get_release_signatures", true);
        let storage = Storage::Mock(mock);
        // Unreachable: the decision must be reached without consulting an RPC.
        let client = make_rpc_client("http://localhost:1");
        let row = make_withdrawal_row(1, Some(3));

        assert!(
            matches!(
                check_withdrawal(
                    &row,
                    &storage,
                    &FinalityRpc::solana(&client, None),
                    Some(Pubkey::new_unique()),
                )
                .await,
                WithdrawalAction::LeaveProcessing { .. }
            ),
            "an unreadable journal must not quarantine inside the window"
        );
    }

    /// The wait is bounded the same way the proof wait is: a journal that never
    /// becomes readable still pages rather than wedging the frontier in silence.
    #[tokio::test]
    async fn journal_unreadable_past_window_quarantines() {
        let mock = MockStorage::new();
        mock.set_should_fail("get_release_signatures", true);
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let mut row = make_withdrawal_row(1, Some(3));
        row.updated_at = Utc::now()
            - chrono::Duration::from_std(RELEASE_PROOF_ESCALATE_AFTER).unwrap()
            - chrono::Duration::seconds(1);

        match check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await
        {
            WithdrawalAction::Quarantine { reason } => assert!(
                reason.contains("release signature journal"),
                "reason should name the unreadable journal: {reason}"
            ),
            _ => panic!("an indefinitely unreadable journal must escalate"),
        }
    }

    /// A journal that reads back unparseable is deterministic corruption and also
    /// proves a signature was recorded, so it escalates at once: waiting would only
    /// re-read the same bytes.
    #[tokio::test]
    async fn journal_corrupt_quarantines_immediately() {
        let mock = MockStorage::new();
        mock.insert_release_signature(1, "not-a-valid-base58-signature".to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        // Fresh row: corruption must not be held for the window like unavailability.
        let row = make_withdrawal_row(1, Some(3));

        match check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await
        {
            WithdrawalAction::Quarantine { reason } => assert!(
                reason.contains("malformed stored release signature"),
                "reason should name the corrupt signature: {reason}"
            ),
            _ => panic!("a corrupt journal must quarantine without waiting"),
        }
    }

    /// A nonce whose tree has rotated away cannot be proven against the current
    /// root, so it is never re-armed: the verifier returns Uncertain and the aged
    /// row escalates to a human.
    #[tokio::test]
    async fn no_sigs_wrong_tree_window_quarantines() {
        let mut server = mockito::Server::new_async().await;
        // Nonce 3 lives in tree 0, but the chain has already rotated to tree 1.
        let _proof = mock_release_proof(&mut server, smt_root(1, &[]), 1);

        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client(&server.url());
        let mut row = make_withdrawal_row(1, Some(3));
        row.updated_at = Utc::now()
            - chrono::Duration::from_std(RELEASE_PROOF_ESCALATE_AFTER).unwrap()
            - chrono::Duration::seconds(1);

        assert!(
            matches!(
                check_withdrawal(
                    &row,
                    &storage,
                    &FinalityRpc::solana(&client, None),
                    Some(Pubkey::new_unique()),
                )
                .await,
                WithdrawalAction::Quarantine { .. }
            ),
            "a rotated-away nonce must never be re-armed"
        );
    }

    /// The new branch is scoped to the empty journal only: one recorded signature
    /// still routes through the signature classifier, whose still-live verdict
    /// leaves the row Processing rather than consulting the root.
    #[tokio::test]
    async fn with_sigs_still_uses_the_signature_classifier() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_null_status(&mut server);
        // Current height below lvbh: the recorded attempt can still land.
        let _height = mock_block_height(&mut server, 50);
        // The signatureless branch would read the instance; it must not.
        let account = mock_instance_at_slot(&mut server, smt_root(0, &[]), 0, 1000, 0);

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(3));
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(
            matches!(action, WithdrawalAction::LeaveProcessing { .. }),
            "a recorded signature keeps the classifier path"
        );
        account.assert();
    }

    /// The requeue cap is the backstop on a row that keeps coming back: the
    /// proof still says Demote, but `decide_action` escalates it instead of
    /// cycling the row between Pending and Processing forever.
    #[tokio::test]
    async fn no_sigs_at_requeue_cap_quarantines() {
        let mut server = mockito::Server::new_async().await;
        let _proof = mock_release_proof(&mut server, smt_root(0, &[]), 0);

        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client(&server.url());
        let mut row = make_withdrawal_row(1, Some(3));
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;

        match decide_action(
            &row,
            &storage,
            &recovery_finality(&client),
            Some(Pubkey::new_unique()),
        )
        .await
        {
            RecoveryAction::Quarantine { reason } => assert!(
                reason.contains("recovery requeues"),
                "the cap reason must survive: {reason}"
            ),
            _ => panic!("a capped row must Quarantine even when the proof says Demote"),
        }
    }

    /// Register the Dead-by-absence classifier mocks (null status + expired height
    /// + covered floor) on `server`. The withdrawal then routes through the SMT gate.
    fn mock_dead_by_absence(server: &mut mockito::ServerGuard) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":1000,"id":1}"#)
            .create();
        // Covered floor (0) so the single-endpoint absence is proven Dead.
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getFirstAvailableBlock""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":0,"id":1}"#)
            .create();
    }

    /// IR2: a Dead-by-absence release whose nonce is NOT in the fresh on-chain root
    /// resolves NotLanded, so it Demotes (preserves the old behavior under the SMT gate).
    #[tokio::test]
    async fn check_withdrawal_demotes_when_signature_dead() {
        let mut server = mockito::Server::new_async().await;
        mock_dead_by_absence(&mut server);
        // Fresh finalized tip (1000 - 150 = 850 > lvbh 100) binds the account read.
        let _bh = mock_latest_blockhash(&mut server, 500, 1000);
        // On-chain tree 0 has no completed nonces; snapshot slot 1000 >= bound slot.
        let _account = mock_instance_at_slot(&mut server, smt_root(0, &[]), 0, 1000, 1);

        let mock = MockStorage::new();
        // Small nonce so tree_index is 0 under both prod and test-tree sizes.
        let row = make_withdrawal_row(1, Some(3));
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(
            matches!(action, WithdrawalAction::Demote),
            "root excludes nonce + fresh snapshot must Demote"
        );
    }

    /// IR1: a Dead-by-absence release whose nonce IS in the fresh on-chain root is
    /// proven landed, so it Completes (not Demote), never re-sending a released withdrawal.
    #[tokio::test]
    async fn check_withdrawal_completes_when_smt_proves_landed() {
        let mut server = mockito::Server::new_async().await;
        mock_dead_by_absence(&mut server);
        // Fresh finalized tip (1000 - 150 = 850 > lvbh 100) binds the account read.
        let _bh = mock_latest_blockhash(&mut server, 500, 1000);
        // On-chain tree 0 includes nonce 3; snapshot slot 1000 >= bound slot.
        let _account = mock_instance_at_slot(&mut server, smt_root(0, &[3]), 0, 1000, 1);

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(3));
        let recorded = Signature::new_unique().to_string();
        mock.insert_release_signature(row.id, recorded.clone(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        match action {
            WithdrawalAction::Complete {
                signature,
                release_signatures,
            } => {
                // The earliest recorded signature is the provenance pointer.
                assert_eq!(signature, recorded, "earliest recorded sig is provenance");
                assert_eq!(release_signatures, Some(vec![recorded]));
            }
            other => panic!(
                "expected Complete, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    /// IR3: a Dead release whose finalized snapshot is not past the validity window
    /// is Uncertain, so it Quarantines (fails closed, never demotes). The release is
    /// finalized-failed so the classifier returns Dead without a block-height read,
    /// leaving the verifier's finalized getLatestBlockhash the only freshness read,
    /// mocked stale. The stale gate short-circuits before the bound account read.
    #[tokio::test]
    async fn check_withdrawal_quarantines_when_snapshot_stale() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();
        // Finalized tip 250 - 150 = 100 == lvbh 100, so freshness fails closed to Uncertain.
        let _bh = mock_latest_blockhash(&mut server, 500, 250);
        // The stale gate returns before the account read, so this must never fire.
        let account = mock_instance_at_slot(&mut server, smt_root(0, &[]), 0, 999, 0);

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(3));
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(
            matches!(action, WithdrawalAction::Quarantine { .. }),
            "stale snapshot must Quarantine, never Demote"
        );
        account.assert(); // stale gate short-circuits before the bound account read
    }

    /// F1: the classifier's finalized-success fast path completes WITHOUT the
    /// finalized getAccountInfo read; the SMT gate is paid only on the Dead branch.
    #[tokio::test]
    async fn check_withdrawal_landed_fast_path_skips_account_read() {
        let landed_sig = Signature::new_unique();
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();
        // Must never be hit on the fast path.
        let account = mock_instance_at_slot(&mut server, smt_root(0, &[]), 0, 1000, 0);

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(3));
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(matches!(action, WithdrawalAction::Complete { .. }));
        account.assert(); // no getAccountInfo request fired
    }

    /// F2: a live (still within validity) signature leaves the row Processing
    /// without any finalized getAccountInfo read.
    #[tokio::test]
    async fn check_withdrawal_live_fast_path_skips_account_read() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create();
        // current_height (50) <= lvbh (1000) means still live.
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":50,"id":1}"#)
            .create();
        let account = mock_instance_at_slot(&mut server, smt_root(0, &[]), 0, 1000, 0);

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(3));
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(
            &row,
            &storage,
            &FinalityRpc::solana(&client, None),
            Some(Pubkey::new_unique()),
        )
        .await;
        assert!(matches!(action, WithdrawalAction::LeaveProcessing { .. }));
        account.assert(); // no getAccountInfo request fired
    }

    /// Finalized-success signature → Complete with that sig.
    #[tokio::test]
    async fn check_withdrawal_completes_when_signature_landed() {
        let landed_sig = Signature::new_unique();
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action =
            check_withdrawal(&row, &storage, &FinalityRpc::solana(&client, None), None).await;
        match action {
            WithdrawalAction::Complete { signature, .. } => {
                assert_eq!(signature, landed_sig.to_string());
            }
            _ => panic!("expected Complete"),
        }
    }

    /// Signature still within blockhash validity → leave in Processing.
    #[tokio::test]
    async fn check_withdrawal_leaves_processing_when_signature_live() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create();
        let _height = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":50,"id":1}"#)
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        // current_height (50) <= lvbh (1000) means still live.
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action =
            check_withdrawal(&row, &storage, &FinalityRpc::solana(&client, None), None).await;
        assert!(
            matches!(action, WithdrawalAction::LeaveProcessing { .. }),
            "expected LeaveProcessing"
        );
    }

    /// RPC failure during classification is uncertainty → quarantine, never demote.
    #[tokio::test]
    async fn check_withdrawal_quarantines_on_rpc_uncertainty() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("internal server error")
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        let recorded_sig = Signature::new_unique().to_string();
        mock.insert_release_signature(row.id, recorded_sig.clone(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action =
            check_withdrawal(&row, &storage, &FinalityRpc::solana(&client, None), None).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(
                    reason.contains("could not verify release landed"),
                    "reason: {reason}"
                );
                assert!(
                    reason.contains(&recorded_sig),
                    "sig should be in reason: {reason}"
                );
            }
            _ => panic!("expected Quarantine"),
        }
    }

    // ── route_outcome calls the right storage helper per variant ─────

    async fn seed_processing_row(mock: &MockStorage, row: DbTransaction) -> DateTime<Utc> {
        let captured = row.updated_at;
        mock.pending_transactions.lock().unwrap().push(row);
        captured
    }

    #[tokio::test]
    async fn route_outcome_complete_writes_completed() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(1);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Complete {
                signature: "sig-abc".to_string(),
                release_signatures: None,
            },
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Completed);
        assert_eq!(after[0].counterpart_signature.as_deref(), Some("sig-abc"));
    }

    #[tokio::test]
    async fn route_outcome_demote_writes_pending() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(2);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Demote,
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Pending);
    }

    #[tokio::test]
    async fn route_outcome_quarantine_writes_manual_review_and_sends_alert() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(3, None);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Quarantine {
                reason: "withdrawal row missing nonce".to_string(),
            },
            ProgramType::Withdraw,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::ManualReview);
        drop(after);

        let update = storage_rx
            .try_recv()
            .expect("expected manual review update");
        assert_eq!(update.transaction_id, row.id);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(
            update.error_message.as_deref(),
            Some("withdrawal row missing nonce")
        );
    }

    // ── parked sweep ─────────────────────────────────────────────────

    /// A stale Parked row (orphaned by a restart) is requeued to Pending so the
    /// processor rebuilds it. No signature lookup, no alert webhook, and the
    /// requeue cap counter is left untouched.
    #[tokio::test]
    async fn stale_parked_row_requeued_to_pending_without_alert() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(70, Some(3));
        row.status = TransactionStatus::Parked;
        // Backdate past STALE_THRESHOLD so the parked sweep selects it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        // Parked rows need no on-chain check, so the RPC client is never called.
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Pending,
            "stale parked → requeued"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 0,
            "parked requeue must not bump the cap counter"
        );
        drop(after);
        assert!(
            storage_rx.try_recv().is_err(),
            "parked requeue must not send an alert"
        );
    }

    /// A fresh Parked row (a live sender still owns it) is left untouched.
    #[tokio::test]
    async fn fresh_parked_row_left_untouched() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(71, Some(3));
        row.status = TransactionStatus::Parked;
        row.updated_at = Utc::now();
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Parked,
            "fresh parked row must be left alone"
        );
    }

    // ── type-scoped sweeps ───────────────────────────────────────────

    /// A withdraw operator's sweep must leave stale deposit rows alone: their
    /// mint signatures live on the channel chain, which this operator's RPC
    /// cannot see, so classifying them here would wrongly demote a landed mint.
    #[tokio::test]
    async fn withdraw_recovery_ignores_stale_deposit_rows() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(80);
        row.status = TransactionStatus::Processing;
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        // Unreachable RPC doubles as proof the row is never classified.
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "withdraw sweep must not touch a deposit row"
        );
        assert_eq!(after[0].recovery_requeue_attempts, 0);
        drop(after);
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update may be sent for a cross-type row"
        );
    }

    /// Mirrored direction: the escrow operator must not classify withdrawal
    /// release signatures against the channel RPC.
    #[tokio::test]
    async fn escrow_recovery_ignores_stale_withdrawal_rows() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(81, Some(9));
        row.status = TransactionStatus::Processing;
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "escrow sweep must not touch a withdrawal row"
        );
        drop(after);
        assert!(
            storage_rx.try_recv().is_err(),
            "no alert may fire for a cross-type row"
        );
    }

    /// The parked sweep is type-scoped too: only the withdraw operator may
    /// requeue orphaned parked withdrawals.
    #[tokio::test]
    async fn escrow_parked_sweep_ignores_parked_withdrawals() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(82, Some(3));
        row.status = TransactionStatus::Parked;
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Parked,
            "escrow sweep must leave parked withdrawals for the withdraw operator"
        );
    }

    /// A withdraw boot reconcile with only deposit rows in flight must leave
    /// them untouched and converge on the first pass instead of exhausting
    /// max_passes on rows it can never resolve.
    #[tokio::test]
    async fn boot_reconcile_converges_ignoring_other_type_rows() {
        let mock = MockStorage::new();
        // Fresh Processing deposit: the ZERO boot threshold would select it if unscoped.
        let mut row = make_deposit_row(83);
        row.status = TransactionStatus::Processing;
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        boot_reconcile_processing(
            &storage,
            &client,
            None,
            ProgramType::Withdraw,
            None,
            &storage_tx,
            &CancellationToken::new(),
            5,
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "withdraw boot must not demote or quarantine an in-flight deposit"
        );
        drop(after);
        // One sweep read plus one convergence read; more means the pass loop
        // failed to converge and burned extra passes on the deposit row.
        assert_eq!(
            mock.calls("get_stale_processing_transactions"),
            2,
            "boot reconcile must converge after the first pass"
        );
    }

    // ── recovery requeue cap ─────────────────────────────────────────

    /// Under the cap: a NotLanded deposit is requeued AND its durable
    /// counter increments, so the next stale sweep sees the higher count.
    #[tokio::test]
    async fn requeue_under_cap_increments_counter_and_requeues() {
        // No persisted signatures: NotLanded, so Demote, with no RPC consulted.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(50);
        row.status = TransactionStatus::Processing;
        row.recovery_requeue_attempts = 0;
        // Backdate past STALE_THRESHOLD so the sweep actually selects it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Pending,
            "under cap → requeued"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 1,
            "durable requeue counter must increment on demote"
        );
    }

    /// At the cap: a row that would otherwise Demote is quarantined to
    /// ManualReview and the alert webhook is sent.
    #[tokio::test]
    async fn requeue_at_cap_quarantines_and_alerts() {
        // No persisted signatures would Demote, but the cap converts it to Quarantine.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(51);
        row.status = TransactionStatus::Processing;
        // At the cap (MAX requeues already done) → the next demote is blocked.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        // Backdate past STALE_THRESHOLD so the sweep actually selects it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::ManualReview,
            "at cap → quarantined, not requeued"
        );
        drop(after);

        let update = storage_rx
            .try_recv()
            .expect("cap must fire the manual-review alert webhook");
        assert_eq!(update.transaction_id, 51);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let reason = update.error_message.as_deref().unwrap_or("");
        assert!(
            reason.contains("recovery requeues")
                && reason.contains(&MAX_RECOVERY_REQUEUE_ATTEMPTS.to_string()),
            "alert must name the requeue cap and its count: {reason}"
        );
    }

    /// `decide_action` caps the Demote arm uniformly regardless of type. Uses a deposit
    /// row with no persisted signatures (NotLanded, so Demote, no RPC).
    #[tokio::test]
    async fn decide_action_caps_demote_at_threshold() {
        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client("http://localhost:1");

        let mut row = make_deposit_row(52);
        // One below the cap still demotes (requeues) - pins the off-by-one boundary.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS - 1;
        let below = decide_action(&row, &storage, &recovery_finality(&client), None).await;
        assert!(
            matches!(below, RecoveryAction::Demote),
            "one below the cap must still Demote (requeue)"
        );
        // At the cap, the demote is converted to Quarantine.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        let at_cap = decide_action(&row, &storage, &recovery_finality(&client), None).await;
        assert!(
            matches!(at_cap, RecoveryAction::Quarantine { .. }),
            "demote at the cap must become Quarantine"
        );
    }

    #[tokio::test]
    async fn route_outcome_demote_noops_when_captured_updated_at_stale() {
        // The `updated_at` check fails → no metric increment, row unchanged.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(4);
        row.status = TransactionStatus::Processing;
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        // Captured timestamp that does NOT match the seeded row's updated_at.
        let stale = row.updated_at - chrono::Duration::seconds(60);
        route_outcome(
            &storage,
            &row,
            stale,
            RecoveryAction::Demote,
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Processing);
    }

    // ── boot pre-flight (reconcile then validate) ──────────────────

    use crate::operator::sender::validate_smt_root;
    use crate::operator::utils::smt_util::SmtState;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;

    /// Mock a finalized-success `getSignatureStatuses` so the classifier reports the release landed.
    fn mock_finalized_status(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .expect_at_least(1)
            .create()
    }

    /// Mock `getAccountInfo` to return an Instance carrying `root`.
    fn mock_instance_account(server: &mut mockito::ServerGuard, root: [u8; 32]) -> mockito::Mock {
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: root,
            current_tree_index: 0,
        };
        let mut bytes = Vec::new();
        instance.serialize(&mut bytes).unwrap();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "owner": Pubkey::new_unique().to_string(),
                            "lamports": 1_000_000u64,
                            "data": [STANDARD.encode(&bytes), "base64"],
                            "executable": false,
                            "rentEpoch": 0
                        }
                    }
                })
                .to_string(),
            )
            .create()
    }

    fn processing_withdrawal(id: i64, nonce: i64) -> DbTransaction {
        let mut row = make_withdrawal_row(id, Some(nonce));
        row.status = TransactionStatus::Processing;
        row
    }

    /// A fresh `Processing` row with a landed signature is promoted to `Completed` under `Duration::ZERO` (the 5-minute default would skip it).
    #[tokio::test]
    async fn recover_once_zero_threshold_picks_up_fresh_processing_row() {
        let mut server = mockito::Server::new_async().await;
        let landed_sig = Signature::new_unique();
        let _status = mock_finalized_status(&mut server);

        let mock = MockStorage::new();
        let row = processing_withdrawal(1, 42);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);

        recover_once(
            &storage,
            &recovery_finality(&client),
            ProgramType::Withdraw,
            None,
            &storage_tx,
            &CancellationToken::new(),
            Duration::ZERO,
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Completed,
            "fresh landed row must be promoted under ZERO threshold"
        );
        assert_eq!(
            after[0].counterpart_signature.as_deref(),
            Some(landed_sig.to_string().as_str())
        );
    }

    /// Pre-flight happy path: a landed-but-uncompleted nonce is reconciled to Completed, then `validate_smt_root` agrees; zero rows Failed.
    #[tokio::test]
    async fn preflight_reconciles_landed_nonce_then_validates_ok() {
        let landed_nonce: u64 = 3;
        let mut onchain_tree = SmtState::new(0);
        onchain_tree.insert_nonce(landed_nonce);

        let mut server = mockito::Server::new_async().await;
        let _status = mock_finalized_status(&mut server);
        let _account = mock_instance_account(&mut server, onchain_tree.current_root());

        let mock = MockStorage::new();
        let row = processing_withdrawal(1, landed_nonce as i64);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);
        let token = CancellationToken::new();

        boot_reconcile_processing(
            &storage,
            &client,
            None,
            ProgramType::Withdraw,
            None,
            &storage_tx,
            &token,
            5,
        )
        .await
        .unwrap();

        let validated = validate_smt_root(&storage, &client, Some(Pubkey::new_unique())).await;
        assert!(
            validated.is_ok(),
            "validate must pass once the landed nonce is reconciled: {validated:?}"
        );

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Completed);
        assert!(
            after.iter().all(|t| t.status != TransactionStatus::Failed),
            "no row may be Failed by the pre-flight"
        );
    }

    /// Pre-flight refuse-to-start path: a divergence the reconcile cannot resolve
    /// (a no-signature Processing row goes to ManualReview, leaving the DB one nonce
    /// behind an on-chain root) makes `validate_smt_root` return Err. No row is
    /// Failed (the anti-SOLA2-21 assertion).
    #[tokio::test]
    async fn preflight_refuses_start_on_unreconcilable_mismatch() {
        // On-chain root includes nonce 7 that the DB will never record.
        let mut onchain_tree = SmtState::new(0);
        onchain_tree.insert_nonce(7);

        let mut server = mockito::Server::new_async().await;
        let _account = mock_instance_account(&mut server, onchain_tree.current_root());

        let mock = MockStorage::new();
        // A no-signature Processing withdrawal is quarantined to ManualReview, not Failed.
        let row = processing_withdrawal(1, 7);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);
        let token = CancellationToken::new();

        boot_reconcile_processing(
            &storage,
            &client,
            None,
            ProgramType::Withdraw,
            None,
            &storage_tx,
            &token,
            5,
        )
        .await
        .unwrap();

        let validated = validate_smt_root(&storage, &client, Some(Pubkey::new_unique())).await;
        assert!(
            matches!(
                validated,
                Err(OperatorError::Program(
                    crate::error::ProgramError::SmtRootMismatch { .. }
                ))
            ),
            "unreconcilable divergence must refuse to start: {validated:?}"
        );

        let after = mock.pending_transactions.lock().unwrap();
        assert!(
            after.iter().all(|t| t.status != TransactionStatus::Failed),
            "refuse-to-start must never mark a row Failed"
        );
    }

    /// End-to-end recovery. The release looks gone on the primary but landed
    /// on the fallback, so recovery completes the row instead of demoting it.
    #[tokio::test]
    async fn recovery_withdrawal_completes_when_fallback_finds_release_landed() {
        let landed_sig = Signature::new_unique();

        // Primary: pruned endpoint, release sig gone (null + expired).
        let mut primary = mockito::Server::new_async().await;
        let _p_status = primary
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create();
        let _p_height = mock_block_height(&mut primary, 1000);

        // Archival fallback: still has the finalized-success record.
        let mut fb = mockito::Server::new_async().await;
        let _fb_status = fb
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();

        let mock = MockStorage::new();
        let row = processing_withdrawal(1, 42);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock.clone());
        let primary_client = make_rpc_client(&primary.url());
        let fb_client = make_rpc_client(&fb.url());
        let finality = RecoveryFinality::new(&primary_client, Some(&fb_client));
        let (storage_tx, _rx) = mpsc::channel(8);

        recover_once(
            &storage,
            &finality,
            ProgramType::Withdraw,
            None,
            &storage_tx,
            &CancellationToken::new(),
            Duration::ZERO,
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Completed,
            "fallback-corroborated landed release must Complete, not Demote"
        );
        assert_eq!(
            after[0].counterpart_signature.as_deref(),
            Some(landed_sig.to_string().as_str())
        );
    }

    /// The reported bug, end to end across the two components that own it. A DB
    /// outage lasting longer than the processor's rescue budget leaves the row
    /// `Processing` with no signature and no owner; the next sweep proves it never
    /// released and re-arms it, where before it went straight to manual review.
    #[tokio::test]
    async fn transient_strand_then_recovery_rescues() {
        use crate::operator::processor::{
            process_release_funds, ProcessorState, ReleaseFundsState,
        };

        let mock = MockStorage::new();
        // Stale enough for the sweep to pick it up once the processor strands it.
        let mut row = make_withdrawal_row(1, Some(3));
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        // The allowlist read fails for the whole of its retry budget, which is the
        // DB outage, and the rescue write that would requeue the row fails too.
        mock.set_fail_times("get_mint", 3);
        mock.set_fail_times("try_requeue_prebroadcast", 1);
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut processor_state = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(ReleaseFundsState {
                instance_pda: Pubkey::new_unique(),
                operator_pubkey: Pubkey::new_unique(),
                operator_pda: Pubkey::new_unique(),
                event_authority_pda: Pubkey::new_unique(),
                allowed_mints: std::collections::HashMap::new(),
                instance_atas: std::collections::HashMap::new(),
            }),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel(4);
        fetcher_tx.send(row).await.unwrap();
        drop(fetcher_tx);
        let processed = process_release_funds(
            &mut processor_state,
            fetcher_rx,
            mpsc::channel(4).0,
            mpsc::channel(4).0,
            storage.clone(),
            ProgramType::Withdraw,
        )
        .await;
        assert!(processed.is_err(), "the transient must still bubble up");

        {
            let stranded = mock.pending_transactions.lock().unwrap();
            assert_eq!(
                stranded[0].status,
                TransactionStatus::Processing,
                "the failed rescue write leaves the row stranded, which is the bug"
            );
            assert!(mock.release_signatures.lock().unwrap().is_empty());
        }

        // The sweep proves nonce 3 never released and re-arms the row.
        let mut server = mockito::Server::new_async().await;
        let _proof = mock_release_proof(&mut server, smt_root(0, &[]), 0);
        let client = make_rpc_client(&server.url());
        test_hooks::run_recovery_once(
            &storage,
            &client,
            ProgramType::Withdraw,
            Some(Pubkey::new_unique()),
            &mpsc::channel(8).0,
        )
        .await
        .unwrap();

        let rescued = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            rescued[0].status,
            TransactionStatus::Pending,
            "recovery must re-arm the stranded withdrawal"
        );
        assert_eq!(rescued[0].recovery_requeue_attempts, 1);
    }

    /// A PendingRemint whose release finalized during the safety window must be
    /// promoted to Completed at boot even though its deadline has not matured, so
    /// validate_smt_root sees the consumed nonce and agrees instead of refusing
    /// to start.
    #[tokio::test]
    async fn boot_reconcile_completes_landed_pending_remint_then_validates_ok() {
        let landed_nonce: u64 = 3;
        let mut onchain_tree = SmtState::new(0);
        onchain_tree.insert_nonce(landed_nonce);

        let mut server = mockito::Server::new_async().await;
        let landed_sig = Signature::new_unique();
        let _status = mock_finalized_status(&mut server);
        let _account = mock_instance_account(&mut server, onchain_tree.current_root());

        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(1, Some(landed_nonce as i64));
        row.status = TransactionStatus::PendingRemint;
        row.remint_signatures = Some(vec![landed_sig.to_string()]);
        row.remint_last_valid_block_heights = Some(vec![100]);
        // Deadline still in the future: the restart happened inside the window.
        row.pending_remint_deadline_at = Some(Utc::now() + chrono::Duration::seconds(60));
        // The mock splits storage: get_pending_remint_transactions reads one vec,
        // update_transaction_status / get_completed_withdrawal_nonces the other.
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(row.clone());
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());

        boot_reconcile_landed_pending_remints(&storage, &client, None)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Completed,
            "landed PendingRemint must be promoted despite a future deadline"
        );

        let validated = validate_smt_root(&storage, &client, Some(Pubkey::new_unique())).await;
        assert!(
            validated.is_ok(),
            "validate must pass once the landed PendingRemint is reconciled: {validated:?}"
        );
    }

    /// A PendingRemint whose release is on-chain but not yet finalized (still
    /// inside the safety window) must be left PendingRemint, never completed on
    /// an unfinalized signature.
    #[tokio::test]
    async fn boot_reconcile_leaves_unfinalized_pending_remint_untouched() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":10,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}]},"id":1}"#,
            )
            .create();

        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(1, Some(3));
        row.status = TransactionStatus::PendingRemint;
        row.remint_signatures = Some(vec![Signature::new_unique().to_string()]);
        row.remint_last_valid_block_heights = Some(vec![100]);
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(row.clone());
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());

        boot_reconcile_landed_pending_remints(&storage, &client, None)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::PendingRemint,
            "unfinalized release must stay PendingRemint"
        );
    }
}
