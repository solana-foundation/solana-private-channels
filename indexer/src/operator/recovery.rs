//! Recovers rows stuck in `Processing` after an operator crash.

use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::OperatorError;
use crate::metrics::OPERATOR_STALE_PROCESSING_RECOVERED;
use crate::operator::sender::types::PendingSig;
use crate::operator::sender::{classify_release_signatures, SigFinality};
use crate::operator::utils::rpc_util::RpcClientWithRetry;
use crate::operator::TransactionStatusUpdate;
use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
use crate::storage::common::storage::Storage;
use chrono::{DateTime, Utc};
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

/// Stalled withdrawals examined in one sweep before the depth is worth a warning.
/// Purely an observability threshold: the sweep never stops early on it.
const RECONCILE_BACKLOG_WARN_AT: usize = 1_000;

/// Wall-clock ceiling on the periodic reconcile.
///
/// Held below `RECOVERY_INTERVAL` so the worker still idles between ticks. The
/// tick interval bursts on a missed deadline, so a sweep that outran it would
/// leave the worker permanently busy and push the stale-`Processing` recovery
/// at the top of the next tick behind it. The cursor makes stopping early cost
/// nothing: the next sweep resumes rather than restarts.
pub(crate) const RECONCILE_SWEEP_BUDGET: Duration = Duration::from_secs(45);

/// Wall-clock ceiling on the boot pre-flight's reconcile.
///
/// Startup waits on this sweep, and a degraded RPC turns each row into five
/// retries, so an unbounded pass can hold withdrawals down for the better part
/// of an hour with nothing paged. Giving up early costs nothing in that case:
/// an RPC that cannot classify cannot promote either, so the sweep would not
/// have cleared the row anyway. The bitmap diff still runs, and a db-ahead
/// divergence that survives is the designed refuse-to-start, which is loud and
/// has a runbook.
pub(crate) const BOOT_RECONCILE_BUDGET: Duration = Duration::from_secs(120);

/// Max durable Demote requeues before a stuck row is quarantined (paged).
pub(crate) const MAX_RECOVERY_REQUEUE_ATTEMPTS: i32 = 3;

/// Deposit recovery outcome. Uncertainty must NOT demote (double-mint risk); an
/// in-flight signature leaves the row Processing for the next sweep.
enum DepositOutcome {
    Landed { signature: String },
    NotLanded,
    Live { reason: String },
    Ambiguous { reason: String },
}

/// Withdrawal recovery outcome. We verify on-chain finality before demoting so
/// a release that already landed is never re-sent.
enum WithdrawalAction {
    /// Release finalized on-chain → mark Completed with that signature.
    Complete { signature: String },
    /// Every recorded signature is dead → safe to requeue.
    Demote,
    /// A recorded signature could still land → re-evaluate next sweep.
    LeaveProcessing { reason: String },
    /// Uncertain (no signatures, or RPC could not classify) → page.
    Quarantine { reason: String },
}

/// Unified action for the storage router.
enum RecoveryAction {
    Complete {
        signature: String,
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
pub async fn run_recovery_worker(
    storage: Arc<Storage>,
    rpc_client: Arc<RpcClientWithRetry>,
    program_type: ProgramType,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: CancellationToken,
) -> Result<(), OperatorError> {
    info!("Starting recovery worker");
    let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
    // Lives across ticks so a sweep that stops on its budget resumes where it
    // left off, rather than rescanning the same prefix every minute.
    let mut reconcile_cursor = 0i64;
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Recovery worker received cancellation, exiting");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = recover_once(
                    &storage,
                    &rpc_client,
                    program_type,
                    &storage_tx,
                    &cancellation_token,
                    STALE_THRESHOLD,
                    &mut reconcile_cursor,
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

async fn recover_once(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
    threshold: Duration,
    reconcile_cursor: &mut i64,
) -> Result<(), OperatorError> {
    // Best-effort GC of release signatures whose parent is no longer Processing;
    // a failure here must not block recovery.
    match storage.gc_stale_release_signatures().await {
        Ok(removed) => debug!(removed, "Recovery GC'd stale release signatures"),
        Err(e) => warn!("Recovery release-signature GC failed: {}", e),
    }

    let owned_type = program_type.owned_transaction_type();

    let stale = storage
        .get_stale_processing_transactions(threshold, RECOVERY_BATCH_LIMIT, owned_type)
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
        if !role_owns(program_type, &row) {
            continue;
        }
        // Capture `updated_at` before the RPC so the write below CAS-checks it.
        let captured = row.updated_at;
        let action = decide_action(&row, storage, rpc_client).await;
        route_outcome(storage, &row, captured, action, program_type, storage_tx).await;
    }

    // Rescue parked withdrawals orphaned by a restart. A live sender unparks
    // these itself, so anything stale here lost its in-memory driver. Parked
    // rows were never sent on-chain, so requeue them without verifying finality.
    let stale_parked = storage
        .get_stale_parked_transactions(threshold, RECOVERY_BATCH_LIMIT, owned_type)
        .await?;
    for row in stale_parked {
        if cancellation_token.is_cancelled() {
            info!("Recovery sweep cancelled; remaining parked rows deferred");
            return Ok(());
        }
        if !role_owns(program_type, &row) {
            continue;
        }
        requeue_parked(storage, &row, program_type).await;
    }

    // `manual_review` has no owner, so re-checking it every tick is free of
    // contention. `pending_remint` is deliberately absent here: the sender owns
    // those rows and may have a remint in flight, so they are reconciled once at
    // boot, before the sender exists.
    //
    // Best-effort: this runs at the tail of every tick, including inside the
    // bounded boot-reconcile loop, so propagating an error here would cost the
    // remaining passes of Processing reconciliation over a transient DB blip.
    if program_type == ProgramType::Withdraw {
        if let Err(e) = reconcile_landed_withdrawals(
            storage,
            rpc_client,
            TransactionStatus::ManualReview,
            RECONCILE_SWEEP_BUDGET,
            reconcile_cursor,
            cancellation_token,
        )
        .await
        {
            warn!("Stalled-withdrawal reconcile failed: {}", e);
        }
    }
    Ok(())
}

/// Whether this operator role owns the row, and may therefore act on it.
///
/// The sweep queries already filter by type, so in production this is always
/// true. It is kept as a second, in-process gate because the consequence of a
/// miss is severe and silent: the two roles share one database but hold RPC
/// clients pointed at opposite chains, so acting on a foreign row classifies
/// its signatures against a chain they were never broadcast to. Guarding here,
/// ahead of every storage read and RPC, means any future unfiltered query
/// cannot reach the wrong chain.
fn role_owns(program_type: ProgramType, row: &DbTransaction) -> bool {
    let owned = row.transaction_type == program_type.owned_transaction_type();
    if !owned {
        warn!(
            transaction_id = row.id,
            "Recovery skipped a row owned by the other operator role"
        );
    }
    owned
}

/// Clear withdrawals stalled in `from_status` whose stored release signatures
/// prove the release finalized on-chain.
///
/// Only `SigFinality::Landed` writes. Rows quarantined on a structural problem
/// carry no signatures, are filtered out by the query, and stay for a human;
/// rows quarantined on a transient one clear themselves once the chain catches
/// up. That asymmetry is the whole reason this is safe to run unattended.
///
/// `budget` bounds the sweep in wall-clock time. The periodic worker passes
/// `None` and runs to exhaustion: it blocks nothing, and only an exhaustive
/// sweep guarantees a row is never permanently hidden behind rows that can
/// never clear. The boot pre-flight passes `Some`, because it holds up startup
/// and every row costs an RPC round trip that retries five times against a
/// degraded endpoint.
pub(crate) async fn reconcile_landed_withdrawals(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    from_status: TransactionStatus,
    budget: Duration,
    cursor: &mut i64,
    cancellation_token: &CancellationToken,
) -> Result<(), OperatorError> {
    // The ceiling has to wrap the whole sweep, not gate entry to each row. A
    // single classification retries five times, so a deadline consulted only
    // between rows is overrun by whatever is already in flight when it passes.
    // Dropping the future mid-flight is safe: the sole write is one CAS
    // statement, so it either committed or it did not.
    match tokio::time::timeout(
        budget,
        reconcile_sweep(storage, rpc_client, from_status, cursor, cancellation_token),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            // The cursor keeps its position, so the next sweep resumes here
            // instead of rescanning the prefix that just used up the budget.
            warn!(
                resume_after_id = *cursor,
                ?from_status,
                "Reconcile sweep hit its time budget; remainder deferred to the next sweep"
            );
            Ok(())
        }
    }
}

/// The sweep itself. Split out so the caller can impose a hard wall-clock
/// ceiling on it as a whole.
async fn reconcile_sweep(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    from_status: TransactionStatus,
    cursor: &mut i64,
    cancellation_token: &CancellationToken,
) -> Result<(), OperatorError> {
    let outcome_label = match from_status {
        TransactionStatus::ManualReview => "manual_review_cleared",
        TransactionStatus::PendingRemint => "pending_remint_cleared",
        other => {
            warn!(
                ?other,
                "Stalled-withdrawal reconcile called with an unsupported status"
            );
            return Ok(());
        }
    };

    // Page forward by id from wherever the last sweep stopped.
    //
    // There is no row-count cap. Rows that do not classify are left untouched by
    // design, so nothing drains this set on its own; a cap paired with a cursor
    // that restarts at zero would rescan the same prefix forever and hide every
    // row behind it, including a landed release still wedging the boot gate.
    // The time ceiling bounds the work instead, and the cursor is what keeps
    // that bound from turning into a permanent blind spot.
    let mut scanned = 0usize;
    loop {
        let batch = storage
            .get_stalled_withdrawals_with_signatures(from_status, *cursor, RECOVERY_BATCH_LIMIT)
            .await?;
        if batch.is_empty() {
            break;
        }
        debug!(
            count = batch.len(),
            after_id = *cursor,
            ?from_status,
            "Reconcile sweep found stalled withdrawals with stored signatures"
        );
        let exhausted = batch.len() < RECOVERY_BATCH_LIMIT as usize;
        scanned += batch.len();

        for row in batch {
            if cancellation_token.is_cancelled() {
                info!("Reconcile sweep cancelled; remaining stalled rows deferred");
                return Ok(());
            }
            // Advances even when a row is skipped, which is what stops an
            // unclearable row from being re-fetched ahead of everything else.
            *cursor = row.id;
            // The query is withdrawal-only, so this is the same belt-and-braces gate
            // the other sweeps use: never classify a row against the wrong chain.
            if !role_owns(ProgramType::Withdraw, &row) {
                continue;
            }
            let Some(pending) = row_pending_sigs(&row) else {
                continue;
            };
            let SigFinality::Landed(signature) =
                classify_release_signatures(rpc_client, &pending).await
            else {
                continue;
            };
            promote_stalled_row(storage, &row, from_status, outcome_label, signature).await;
        }

        if exhausted {
            break;
        }
    }

    // Reaching the end means every row was seen, so the next sweep starts over
    // and picks up whatever stalled in the meantime.
    *cursor = 0;

    // A backlog this deep is an incident of its own: every row in it is a
    // withdrawal that failed and could not be resolved, and each costs an RPC
    // round trip per sweep. Surface it rather than let the cost stay invisible.
    if scanned >= RECONCILE_BACKLOG_WARN_AT {
        warn!(
            scanned,
            ?from_status,
            "Reconcile sweep examined an unusually deep stalled-withdrawal backlog"
        );
    }
    Ok(())
}

/// Parse a stalled row's stored signatures into `PendingSig`s. Corrupt or
/// misaligned columns yield `None` so the row is skipped rather than promoted.
fn row_pending_sigs(row: &DbTransaction) -> Option<Vec<PendingSig>> {
    let sigs = row.remint_signatures.as_ref()?;
    let heights = row.remint_last_valid_block_heights.as_ref()?;
    if sigs.len() != heights.len() {
        warn!(
            transaction_id = row.id,
            "Stalled withdrawal has mismatched signature and block-height columns; skipping"
        );
        return None;
    }
    let mut pending = Vec::with_capacity(sigs.len());
    for (sig_str, lvbh) in sigs.iter().zip(heights) {
        let signature = match Signature::from_str(sig_str) {
            Ok(signature) => signature,
            Err(e) => {
                warn!(
                    transaction_id = row.id,
                    "Stalled withdrawal has a malformed stored signature {sig_str}: {e}"
                );
                return None;
            }
        };
        // A negative height is corrupt, not a very large one; casting would
        // wrap it into a height no chain ever reaches.
        let Ok(last_valid_block_height) = u64::try_from(*lvbh) else {
            warn!(
                transaction_id = row.id,
                "Stalled withdrawal has a negative stored block height {lvbh}; skipping"
            );
            return None;
        };
        pending.push(PendingSig {
            signature,
            last_valid_block_height,
        });
    }
    Some(pending)
}

/// CAS a proven-landed stalled row to `Completed`, then log and count it.
async fn promote_stalled_row(
    storage: &Storage,
    row: &DbTransaction,
    from_status: TransactionStatus,
    outcome_label: &str,
    signature: Signature,
) {
    let signature = signature.to_string();
    match storage
        .try_complete_stalled_withdrawal(
            row.id,
            row.updated_at,
            from_status,
            Some(signature.clone()),
        )
        .await
    {
        // Loud on purpose: a status that means "a human decided this needs
        // eyes" just cleared itself, and the on-call should see why.
        Ok(true) => {
            warn!(
                transaction_id = row.id,
                ?from_status,
                signature,
                "Reconcile promoted a stalled withdrawal to Completed on finalized on-chain proof"
            );
            // Both call sites gate on the withdraw operator, so the
            // program-type label is fixed rather than threaded through.
            OPERATOR_STALE_PROCESSING_RECOVERED
                .with_label_values(&[pt_label(ProgramType::Withdraw), outcome_label, "withdrawal"])
                .inc();
        }
        Ok(false) => debug!(
            id = row.id,
            "reconcile skipped, another writer touched the row first"
        ),
        Err(e) => warn!(id = row.id, "reconcile write error: {}", e),
    }
}

async fn decide_action(
    row: &DbTransaction,
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
) -> RecoveryAction {
    let action = match row.transaction_type {
        TransactionType::Deposit => match check_deposit(row, storage, rpc_client).await {
            DepositOutcome::Landed { signature } => RecoveryAction::Complete { signature },
            DepositOutcome::NotLanded => RecoveryAction::Demote,
            DepositOutcome::Live { reason } => RecoveryAction::NoAction { reason },
            DepositOutcome::Ambiguous { reason } => RecoveryAction::Quarantine { reason },
        },
        TransactionType::Withdrawal => match check_withdrawal(row, storage, rpc_client).await {
            WithdrawalAction::Complete { signature } => RecoveryAction::Complete { signature },
            WithdrawalAction::Demote => RecoveryAction::Demote,
            WithdrawalAction::LeaveProcessing { reason } => RecoveryAction::NoAction { reason },
            WithdrawalAction::Quarantine { reason } => RecoveryAction::Quarantine { reason },
        },
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
async fn check_deposit(
    row: &DbTransaction,
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
) -> DepositOutcome {
    let pending = match load_pending_sigs(storage, row.id).await {
        Ok(p) => p,
        Err(reason) => {
            return DepositOutcome::Ambiguous {
                reason: format!("could not verify mint landed ({reason})"),
            }
        }
    };

    if pending.is_empty() {
        return DepositOutcome::NotLanded;
    }

    match classify_release_signatures(rpc_client, &pending).await {
        SigFinality::Landed(sig) => DepositOutcome::Landed {
            signature: sig.to_string(),
        },
        SigFinality::Dead => DepositOutcome::NotLanded,
        // Still in flight; re-check next sweep rather than demote or complete.
        SigFinality::Live(reason) => DepositOutcome::Live { reason },
        // Never demote on uncertainty — risks a double-mint on re-pickup.
        SigFinality::Uncertain(reason) => DepositOutcome::Ambiguous {
            reason: format!("could not verify mint landed ({reason})"),
        },
    }
}

/// Decide a stuck Processing withdrawal's fate by verifying on-chain finality
/// of the persisted release signatures; never demote one whose release landed.
async fn check_withdrawal(
    row: &DbTransaction,
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
) -> WithdrawalAction {
    if row.withdrawal_nonce.is_none() {
        return WithdrawalAction::Quarantine {
            reason: "withdrawal row missing nonce".to_string(),
        };
    }

    let pending = match load_pending_sigs(storage, row.id).await {
        Ok(p) => p,
        Err(reason) => return WithdrawalAction::Quarantine { reason },
    };

    // No recorded signatures → can't verify a release landed; demoting risks a
    // double-payout, so page instead.
    if pending.is_empty() {
        return WithdrawalAction::Quarantine {
            reason: "no broadcast signatures recorded; cannot verify release landed".to_string(),
        };
    }

    match classify_release_signatures(rpc_client, &pending).await {
        SigFinality::Landed(sig) => WithdrawalAction::Complete {
            signature: sig.to_string(),
        },
        SigFinality::Dead => WithdrawalAction::Demote,
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

/// Load and parse a row's persisted broadcast signatures into `PendingSig`s for the
/// finality classifier. Shared by deposit and withdrawal recovery. A read error or a
/// malformed stored signature returns a quarantine reason (uncertainty, never "dead"),
/// so callers never demote a row whose signatures could not be read or parsed.
async fn load_pending_sigs(storage: &Storage, id: i64) -> Result<Vec<PendingSig>, String> {
    let stored = storage
        .get_release_signatures(id)
        .await
        .map_err(|e| format!("release signature lookup failed: {e}"))?;

    let mut pending = Vec::with_capacity(stored.len());
    for entry in &stored {
        let sig_str = &entry.signature;
        let signature = Signature::from_str(sig_str)
            .map_err(|e| format!("malformed stored release signature {sig_str}: {e}"))?;
        pending.push(PendingSig {
            signature,
            last_valid_block_height: entry.last_valid_block_height as u64,
        });
    }
    Ok(pending)
}

/// Split a row's journalled release signatures into the two parallel arrays the
/// quarantine CAS stores. The inner `(None, None)` means there was nothing worth
/// recording, which leaves the columns untouched.
///
/// An outer `None` means the read failed and the caller must defer. Quarantining
/// blind is irreversible: the journal is GC'd as soon as the row leaves
/// `Processing`, so a transient read error would otherwise destroy the row's only
/// evidence and make it permanently unreconcilable.
async fn stored_release_signatures(
    storage: &Storage,
    id: i64,
) -> Option<(Option<Vec<String>>, Option<Vec<i64>>)> {
    match storage.get_release_signatures(id).await {
        Ok(stored) if stored.is_empty() => Some((None, None)),
        Ok(stored) => {
            let (sigs, heights): (Vec<String>, Vec<i64>) = stored
                .into_iter()
                .map(|e| (e.signature, e.last_valid_block_height))
                .unzip();
            Some((Some(sigs), Some(heights)))
        }
        Err(e) => {
            warn!(
                id,
                "deferring quarantine, release signature read failed: {}", e
            );
            None
        }
    }
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
        RecoveryAction::Complete { signature } => {
            match storage
                .try_complete_processing(row.id, captured_updated_at, Some(signature.clone()), None)
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
            // The journal these came from is GC'd as soon as the row leaves
            // Processing, so a quarantined withdrawal keeps its own copy or the
            // evidence is gone before anything can re-check it.
            let (sigs, heights) = match row.transaction_type {
                TransactionType::Withdrawal => {
                    match stored_release_signatures(storage, row.id).await {
                        Some(columns) => columns,
                        // Retry next tick rather than spend the row's evidence.
                        None => return,
                    }
                }
                TransactionType::Deposit => (None, None),
            };
            // Noisy by design — page on uncertainty, never silently demote.
            match storage
                .try_quarantine_processing(row.id, captured_updated_at, sigs, heights)
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
/// `Processing` rows of this role's type remain, bounded by `max_passes`. Rows
/// belonging to the other role are never counted or touched. A withdraw operator is
/// single-active (the sender holds an advisory lock and releases block on
/// confirmation), so at boot there is no live sibling whose not-yet-stale work
/// this could disrupt. Exhausting `max_passes` with rows still `Processing`
/// returns `Ok`: the caller's bitmap diff is the terminal gate that refuses to
/// start on a real divergence.
pub async fn boot_reconcile_processing(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
    max_passes: u32,
) -> Result<(), OperatorError> {
    let mut reconcile_cursor = 0i64;
    for pass in 0..max_passes {
        recover_once(
            storage,
            rpc_client,
            program_type,
            storage_tx,
            cancellation_token,
            Duration::ZERO,
            &mut reconcile_cursor,
        )
        .await?;

        // Same type scope as the sweep above, or the loop could never converge
        // while a sibling role has Processing rows of its own.
        let remaining = storage
            .get_stale_processing_transactions(
                Duration::ZERO,
                RECOVERY_BATCH_LIMIT,
                program_type.owned_transaction_type(),
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

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only entry to drive a single recovery tick deterministically.
    use super::*;

    pub async fn run_recovery_once(
        storage: &Storage,
        rpc_client: &RpcClientWithRetry,
        program_type: ProgramType,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        // Fresh, never-cancelled token; tests run to completion. Uses the periodic
        // worker's STALE_THRESHOLD; the ZERO boot threshold is exercised by calling
        // recover_once directly.
        let token = CancellationToken::new();
        recover_once(
            storage,
            rpc_client,
            program_type,
            storage_tx,
            &token,
            STALE_THRESHOLD,
            &mut 0,
        )
        .await
    }

    /// Drive one stalled-withdrawal reconcile pass, exactly as the boot
    /// pre-flight does for `PendingRemint` and the periodic tick does for
    /// `ManualReview`.
    pub async fn reconcile_stalled_withdrawals_once(
        storage: &Storage,
        rpc_client: &RpcClientWithRetry,
        from_status: TransactionStatus,
    ) -> Result<(), OperatorError> {
        let token = CancellationToken::new();
        reconcile_landed_withdrawals(
            storage,
            rpc_client,
            from_status,
            RECONCILE_SWEEP_BUDGET,
            &mut 0,
            &token,
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
            release_refused_on_chain: false,
        }
    }

    fn make_withdrawal_row(id: i64, nonce: Option<i64>) -> DbTransaction {
        let mut row = make_deposit_row(id);
        row.transaction_type = TransactionType::Withdrawal;
        row.withdrawal_nonce = nonce;
        row
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

    /// The keystone divergence from withdrawal: a deposit with no persisted signature is
    /// provably never broadcast (pre-broadcast persist), so it Demotes for a safe re-mint
    /// rather than Quarantining. No RPC is consulted.
    #[tokio::test]
    async fn deposit_no_sigs_demotes() {
        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client("http://localhost:1");
        let row = make_deposit_row(1);
        let outcome = check_deposit(&row, &storage, &client).await;
        assert!(
            matches!(outcome, DepositOutcome::NotLanded),
            "empty sigs must map to NotLanded (Demote), not Ambiguous/Quarantine"
        );
        // Same state on the withdrawal side Quarantines; assert the difference.
        let wrow = make_withdrawal_row(2, Some(42));
        let waction = check_withdrawal(&wrow, &storage, &client).await;
        assert!(
            matches!(waction, WithdrawalAction::Quarantine { .. }),
            "withdrawal with no sigs must Quarantine - the deliberate deposit divergence"
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

        match check_deposit(&row, &storage, &client).await {
            DepositOutcome::Landed { signature } => assert_eq!(signature, landed_sig.to_string()),
            _ => panic!("expected Landed"),
        }
    }

    /// A null-status sig past blockhash validity is dead: NotLanded, safe to re-mint.
    #[tokio::test]
    async fn deposit_dead_sigs_demote() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_null_status(&mut server);
        // current_height (1000) > lvbh (100) means expired/dead.
        let _height = mock_block_height(&mut server, 1000);

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        assert!(
            matches!(
                check_deposit(&row, &storage, &client).await,
                DepositOutcome::NotLanded
            ),
            "dead sigs map to NotLanded (Demote)"
        );
    }

    /// A sig still within blockhash validity is Live: leave Processing this sweep.
    #[tokio::test]
    async fn deposit_live_sig_leaves_processing() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_null_status(&mut server);
        // current_height (50) <= lvbh (1000) means still live.
        let _height = mock_block_height(&mut server, 50);

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        assert!(
            matches!(
                check_deposit(&row, &storage, &client).await,
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

        match check_deposit(&row, &storage, &client).await {
            DepositOutcome::Ambiguous { reason } => {
                assert!(
                    reason.contains("could not verify mint landed"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Ambiguous"),
        }
    }

    /// A malformed stored signature (via the shared `load_pending_sigs`) is uncertainty,
    /// never read as "dead"; it must Quarantine rather than demote.
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

        match check_deposit(&row, &storage, &client).await {
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
        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(reason.contains("withdrawal row missing nonce"));
            }
            _ => panic!("expected Quarantine"),
        }
    }

    /// No recorded signatures → quarantine, not demote (double-payout risk).
    #[tokio::test]
    async fn check_withdrawal_quarantines_when_no_signatures_recorded() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let row = make_withdrawal_row(1, Some(42));
        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(
                    reason.contains("no broadcast signatures recorded"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Quarantine"),
        }
    }

    /// Null-status signature past blockhash validity is dead → demote.
    #[tokio::test]
    async fn check_withdrawal_demotes_when_signature_dead() {
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
            .with_body(r#"{"jsonrpc":"2.0","result":1000,"id":1}"#)
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        // current_height (1000) > lvbh (100) means expired/dead.
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(&row, &storage, &client).await;
        assert!(
            matches!(action, WithdrawalAction::Demote),
            "expected Demote"
        );
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

        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Complete { signature } => {
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

        let action = check_withdrawal(&row, &storage, &client).await;
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

        let action = check_withdrawal(&row, &storage, &client).await;
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

    /// A quarantined withdrawal must keep its release signatures on the row,
    /// because the journal they came from is GC'd on the next sweep. Deposits
    /// have no such reader, so their columns stay untouched.
    #[tokio::test]
    async fn quarantine_persists_release_signatures_for_withdrawals_only() {
        let mock = MockStorage::new();
        let mut withdrawal = make_withdrawal_row(10, Some(5));
        withdrawal.status = TransactionStatus::Processing;
        let mut deposit = make_deposit_row(11);
        deposit.status = TransactionStatus::Processing;
        let w_captured = seed_processing_row(&mock, withdrawal.clone()).await;
        let d_captured = seed_processing_row(&mock, deposit.clone()).await;
        let w_sig = Signature::new_unique().to_string();
        mock.insert_release_signature(withdrawal.id, w_sig.clone(), 4242, None)
            .await
            .unwrap();
        mock.insert_release_signature(deposit.id, Signature::new_unique().to_string(), 99, None)
            .await
            .unwrap();

        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);
        for (row, captured) in [(&withdrawal, w_captured), (&deposit, d_captured)] {
            route_outcome(
                &storage,
                row,
                captured,
                RecoveryAction::Quarantine {
                    reason: "could not verify release landed (rpc down)".to_string(),
                },
                ProgramType::Withdraw,
                &storage_tx,
            )
            .await;
        }

        let after = mock.pending_transactions.lock().unwrap();
        let stored = after.iter().find(|t| t.id == withdrawal.id).unwrap();
        assert_eq!(stored.status, TransactionStatus::ManualReview);
        assert_eq!(stored.remint_signatures.as_deref(), Some(&[w_sig][..]));
        assert_eq!(
            stored.remint_last_valid_block_heights.as_deref(),
            Some(&[4242i64][..])
        );

        let deposit_row = after.iter().find(|t| t.id == deposit.id).unwrap();
        assert_eq!(deposit_row.status, TransactionStatus::ManualReview);
        assert!(
            deposit_row.remint_signatures.is_none()
                && deposit_row.remint_last_valid_block_heights.is_none(),
            "the deposit path must not start writing withdrawal remint columns"
        );
    }

    // ── stalled-withdrawal reconcile ─────────────────────────────────

    /// A withdrawal stalled in `status` with its release signatures on the row.
    fn stalled_withdrawal(id: i64, status: TransactionStatus, sigs: &[String]) -> DbTransaction {
        let mut row = make_withdrawal_row(id, Some(id));
        row.status = status;
        row.remint_last_valid_block_heights = Some(vec![100; sigs.len()]);
        row.remint_signatures = Some(sigs.to_vec());
        row
    }

    /// A backlog of rows that can never classify must not hide the rows behind
    /// it, at any depth. Nothing writes to them, so they stay in the set for
    /// good and the cursor restarts at zero every sweep; a per-sweep cap would
    /// leave the landed row below permanently unreachable.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_pages_past_a_backlog_larger_than_any_batch_budget() {
        let landed_sig = Signature::new_unique().to_string();
        let mut server = mockito::Server::new_async().await;
        let _status = mock_finalized_status(&mut server);

        let mock = MockStorage::new();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            // Well past ten batches of RECOVERY_BATCH_LIMIT. Each parses to
            // nothing, so it is fetched, skipped, and never costs an RPC.
            for id in 1..=1_200 {
                db.push(stalled_withdrawal(
                    id,
                    TransactionStatus::ManualReview,
                    &["not-a-signature".to_string()],
                ));
            }
            db.push(stalled_withdrawal(
                9_999,
                TransactionStatus::ManualReview,
                std::slice::from_ref(&landed_sig),
            ));
        }
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            RECONCILE_SWEEP_BUDGET,
            &mut 0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        let target = after.iter().find(|t| t.id == 9_999).unwrap();
        assert_eq!(
            target.status,
            TransactionStatus::Completed,
            "the landed row must be reached however deep the unclearable backlog is"
        );
        assert!(
            after
                .iter()
                .filter(|t| t.id <= 1_200)
                .all(|t| t.status == TransactionStatus::ManualReview),
            "rows that cannot classify stay exactly where they are"
        );
    }

    /// Startup waits on the boot sweep, so a backlog it cannot get through must
    /// hand control back rather than hold withdrawals down. The bitmap diff
    /// after it is what decides whether the operator may start.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_returns_when_the_boot_budget_is_spent() {
        let mut server = mockito::Server::new_async().await;
        // Every classification stalls, so the budget expires before the backlog does.
        let _slow = server
            .mock("POST", "/")
            .with_status(500)
            .with_chunked_body(|_| {
                std::thread::sleep(std::time::Duration::from_millis(60));
                Ok(())
            })
            .expect_at_least(1)
            .create();

        let mock = MockStorage::new();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            for id in 1..=200 {
                db.push(stalled_withdrawal(
                    id,
                    TransactionStatus::PendingRemint,
                    &[Signature::new_unique().to_string()],
                ));
            }
        }
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());

        let started = std::time::Instant::now();
        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::PendingRemint,
            Duration::from_millis(150),
            &mut 0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        // The ceiling must hold even though a classification is mid-retry when it
        // passes: a deadline consulted only between rows would overshoot by the
        // whole in-flight retry chain, which is what the wrapping timeout stops.
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the boot sweep must return on its budget even mid-retry, took {:?}",
            started.elapsed()
        );
        let after = mock.pending_transactions.lock().unwrap();
        assert!(
            after
                .iter()
                .all(|t| t.status == TransactionStatus::PendingRemint),
            "giving up on time must never promote a row"
        );
    }

    /// A sweep that stops on its budget must resume, not restart. Without a
    /// cursor that survives the call, every sweep would rescan the same prefix
    /// and the rows behind it would never be reached.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_cursor_resumes_after_a_spent_budget() {
        let mut server = mockito::Server::new_async().await;
        let _slow = server
            .mock("POST", "/")
            .with_status(500)
            .with_chunked_body(|_| {
                std::thread::sleep(std::time::Duration::from_millis(60));
                Ok(())
            })
            .expect_at_least(1)
            .create();

        let mock = MockStorage::new();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            for id in 1..=200 {
                db.push(stalled_withdrawal(
                    id,
                    TransactionStatus::ManualReview,
                    &[Signature::new_unique().to_string()],
                ));
            }
        }
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let mut cursor = 0i64;

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            Duration::from_millis(200),
            &mut cursor,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(
            cursor > 0,
            "a sweep stopped by its budget must leave the cursor where it got to"
        );
        let first_stop = cursor;

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            Duration::from_millis(200),
            &mut cursor,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(
            cursor > first_stop,
            "the next sweep must carry on past {first_stop}, not restart at zero"
        );
    }

    fn cleared_metric(outcome: &str) -> f64 {
        OPERATOR_STALE_PROCESSING_RECOVERED
            .with_label_values(&["withdraw", outcome, "withdrawal"])
            .get()
    }

    /// Mock a finalized-but-failed `getSignatureStatuses`, the only shape that
    /// classifies `Dead` without a block-height round trip.
    fn mock_finalized_failure(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create()
    }

    /// Proven-landed evidence promotes the row and records the landed signature.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_landed_promotes_manual_review_on_finalized_success() {
        let landed_sig = Signature::new_unique().to_string();
        let mut server = mockito::Server::new_async().await;
        let _status = mock_finalized_status(&mut server);

        let mock = MockStorage::new();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(stalled_withdrawal(
                1,
                TransactionStatus::ManualReview,
                std::slice::from_ref(&landed_sig),
            ));
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let before = cleared_metric("manual_review_cleared");

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            RECONCILE_SWEEP_BUDGET,
            &mut 0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Completed);
        assert_eq!(
            after[0].counterpart_signature.as_deref(),
            Some(landed_sig.as_str())
        );
        assert_eq!(
            cleared_metric("manual_review_cleared"),
            before + 1.0,
            "a cleared row must be visible as its own metric series"
        );
    }

    /// Seed one stalled row, serve `setup`'s RPC shape, and assert nothing moved.
    async fn assert_not_promoted(
        label: &str,
        setup: impl FnOnce(&mut mockito::ServerGuard) -> Vec<mockito::Mock>,
    ) {
        let mut server = mockito::Server::new_async().await;
        let _mocks = setup(&mut server);

        let mock = MockStorage::new();
        let row = stalled_withdrawal(
            1,
            TransactionStatus::ManualReview,
            &[Signature::new_unique().to_string()],
        );
        let seeded_updated_at = row.updated_at;
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let before = cleared_metric("manual_review_cleared");

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            RECONCILE_SWEEP_BUDGET,
            &mut 0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::ManualReview,
            "{label}: only proven-landed evidence may clear a quarantine"
        );
        assert_eq!(
            after[0].updated_at, seeded_updated_at,
            "{label}: a non-landed verdict must not write to the row at all"
        );
        assert_eq!(
            cleared_metric("manual_review_cleared"),
            before,
            "{label}: no write means no metric"
        );
    }

    /// The load-bearing safety property: everything short of proof is a no-op.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_landed_leaves_row_on_every_non_landed_verdict() {
        // Live: no status entry yet and the blockhash has not expired.
        assert_not_promoted("live", |server| {
            vec![mock_null_status(server), mock_block_height(server, 0)]
        })
        .await;
        assert_not_promoted("dead", |server| vec![mock_finalized_failure(server)]).await;
        assert_not_promoted("uncertain", |server| {
            vec![server
                .mock("POST", "/")
                .with_status(500)
                .with_body("internal server error")
                .create()]
        })
        .await;
    }

    /// Corrupt evidence must be inert: no panic, no RPC, and no promotion.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_landed_skips_unparseable_stored_signature() {
        let mut server = mockito::Server::new_async().await;
        let status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .expect(0)
            .create();

        let mock = MockStorage::new();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(stalled_withdrawal(
                1,
                TransactionStatus::ManualReview,
                &["not-a-valid-base58-signature".to_string()],
            ));
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            RECONCILE_SWEEP_BUDGET,
            &mut 0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::ManualReview
        );
        status.assert();
    }

    /// A negative stored block height cannot be a real one, so the row is
    /// skipped rather than sign-wrapped into a height no chain will reach.
    #[tokio::test]
    #[serial_test::serial(manual_review_cleared_metric)]
    async fn reconcile_landed_skips_negative_block_height() {
        let mut server = mockito::Server::new_async().await;
        let status = mock_finalized_status(&mut server).expect(0);

        let mock = MockStorage::new();
        let mut row = stalled_withdrawal(
            1,
            TransactionStatus::ManualReview,
            &[Signature::new_unique().to_string()],
        );
        row.remint_last_valid_block_heights = Some(vec![-1]);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());

        reconcile_landed_withdrawals(
            &storage,
            &client,
            TransactionStatus::ManualReview,
            RECONCILE_SWEEP_BUDGET,
            &mut 0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::ManualReview
        );
        status.assert();
    }

    /// The sweep runs at the tail of every tick, including inside the bounded
    /// boot-reconcile loop. A failure there must not abort the tick and cost
    /// the remaining passes of Processing reconciliation.
    #[tokio::test]
    async fn recover_once_tolerates_reconcile_query_failure() {
        let mock = MockStorage::new();
        mock.set_should_fail("get_stalled_withdrawals_with_signatures", true);
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        let result = recover_once(
            &storage,
            &client,
            ProgramType::Withdraw,
            &storage_tx,
            &CancellationToken::new(),
            Duration::ZERO,
            &mut 0,
        )
        .await;

        assert!(
            result.is_ok(),
            "a reconcile query failure must not abort the tick: {result:?}"
        );
    }

    /// Quarantining without the evidence is irreversible: the journal is GC'd
    /// on the next tick, so the row could never be auto-reconciled again.
    /// A read failure must defer the quarantine, not spend the row's evidence.
    #[tokio::test]
    async fn quarantine_defers_when_release_signature_read_fails() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(12, Some(6));
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        mock.set_should_fail("get_release_signatures", true);
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Quarantine {
                reason: "could not verify release landed (rpc down)".to_string(),
            },
            ProgramType::Withdraw,
            &storage_tx,
        )
        .await;

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Processing,
            "the row must stay Processing so the next tick can retry with evidence"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a deferred quarantine must not fire the manual-review alert"
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

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, &storage_tx)
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

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, &storage_tx)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Parked,
            "fresh parked row must be left alone"
        );
    }

    // ── role ownership ───────────────────────────────────────────────

    /// A withdraw operator must leave escrow deposits alone. Its RPC client points
    /// at the withdrawal destination chain, so classifying a deposit's mint
    /// signature there reads a chain the signature was never sent to.
    #[tokio::test]
    async fn withdraw_sweep_ignores_processing_deposit() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(80);
        row.status = TransactionStatus::Processing;
        // Backdate past STALE_THRESHOLD so only ownership can keep the sweep off it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        // A persisted signature is what an unowned sweep would classify cross-chain.
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "a deposit is not the withdraw role's row to recover"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 0,
            "a foreign row must not burn the requeue cap"
        );
        drop(after);
        assert!(
            storage_rx.try_recv().is_err(),
            "skipping a foreign row must not alert"
        );
    }

    /// `role_owns` exhaustively, over all four role/row-type pairs. Called directly
    /// on purpose: the SQL and mock reads are both type-scoped, so a cross-role row
    /// can no longer reach the guard through storage.
    #[test]
    fn role_owns_rejects_cross_role_rows() {
        let deposit = make_deposit_row(1);
        let withdrawal = make_withdrawal_row(2, Some(1));
        let cases = [
            (ProgramType::Escrow, &deposit, true),
            (ProgramType::Escrow, &withdrawal, false),
            (ProgramType::Withdraw, &withdrawal, true),
            (ProgramType::Withdraw, &deposit, false),
        ];
        for (program_type, row, expected) in cases {
            assert_eq!(
                role_owns(program_type, row),
                expected,
                "{program_type:?} vs {:?}",
                row.transaction_type
            );
        }
    }

    /// The parked mirror: an escrow operator must not unpark a withdrawal a
    /// withdraw sender still owns. This path issues no RPC, so ownership is the
    /// only thing standing between it and a cross-role write.
    #[tokio::test]
    async fn escrow_sweep_ignores_parked_withdrawal() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(81, Some(9));
        row.status = TransactionStatus::Parked;
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, &storage_tx)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Parked,
            "a withdrawal is not the escrow role's row to unpark"
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

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, &storage_tx)
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

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, &storage_tx)
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
        let below = decide_action(&row, &storage, &client).await;
        assert!(
            matches!(below, RecoveryAction::Demote),
            "one below the cap must still Demote (requeue)"
        );
        // At the cap, the demote is converted to Quarantine.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        let at_cap = decide_action(&row, &storage, &client).await;
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

    use crate::operator::sender::validate_bitmap_consistency;
    use crate::operator::utils::account_util::bitmap_account_bytes;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

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

    /// Mock `getAccountInfo` to return a bitmap recording `consumed` as released.
    fn mock_bitmap_account(server: &mut mockito::ServerGuard, consumed: &[u64]) -> mockito::Mock {
        let bytes = bitmap_account_bytes(0, consumed, 255);
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
            &client,
            ProgramType::Withdraw,
            &storage_tx,
            &CancellationToken::new(),
            Duration::ZERO,
            &mut 0,
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

    /// Pre-flight happy path: a landed-but-uncompleted nonce is reconciled to
    /// Completed, then the bitmap diff agrees; zero rows Failed.
    #[tokio::test]
    async fn preflight_reconciles_landed_nonce_then_validates_ok() {
        let landed_nonce: u64 = 3;

        let mut server = mockito::Server::new_async().await;
        let _status = mock_finalized_status(&mut server);
        let _account = mock_bitmap_account(&mut server, &[landed_nonce]);

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
            ProgramType::Withdraw,
            &storage_tx,
            &token,
            5,
        )
        .await
        .unwrap();

        let validated =
            validate_bitmap_consistency(&storage, &client, Some(Pubkey::new_unique()), &storage_tx)
                .await;
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

    /// Pre-flight halt path: the database records a Completed release the chain
    /// never made, which is the one divergence the reconcile cannot explain away.
    /// No row is Failed (the anti-SOLA2-21 assertion).
    #[tokio::test]
    async fn preflight_refuses_start_on_unreconcilable_mismatch() {
        let mut server = mockito::Server::new_async().await;
        // Nothing consumed on-chain, so the Completed row below has no bit.
        let _account = mock_bitmap_account(&mut server, &[]);

        let mock = MockStorage::new();
        let mut row = processing_withdrawal(1, 7);
        row.status = TransactionStatus::Completed;
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);
        let token = CancellationToken::new();

        boot_reconcile_processing(
            &storage,
            &client,
            ProgramType::Withdraw,
            &storage_tx,
            &token,
            5,
        )
        .await
        .unwrap();

        let validated =
            validate_bitmap_consistency(&storage, &client, Some(Pubkey::new_unique()), &storage_tx)
                .await;
        assert!(
            matches!(
                validated,
                Err(OperatorError::Program(
                    crate::error::ProgramError::BitmapDivergence { .. }
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
}
