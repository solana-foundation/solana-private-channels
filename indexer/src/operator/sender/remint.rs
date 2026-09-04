#[cfg(test)]
use super::types::InFlightQueue;
use super::types::SenderState;
use super::{verify_release_landed, ReleaseVerdict};
use crate::metrics::{
    OPERATOR_ABSENCE_CLASSIFY, OPERATOR_RELEASE_VERIFY, OPERATOR_REMINT_CLAIM_LOST,
};
use crate::operator::tree_constants::MAX_TREE_LEAVES;
use crate::{
    channel_utils::send_guaranteed,
    config::ProgramType,
    operator::{
        check_transaction_status, remint_idempotency_memo,
        sender::{
            transaction::FINALITY_SAFETY_DELAY,
            types::{InstructionWithSigners, PendingRemint, PendingSig},
        },
        utils::instruction_util::WithdrawalRemintInfo,
        utils::transaction_util::{build_and_sign, send_signed},
        ConfirmationResult, ExtraErrorCheckPolicy, MintToBuilder, RetryPolicy, RpcClientWithRetry,
        SignerUtil, TransactionStatusUpdate,
    },
    storage::TransactionStatus,
};
use chrono::Utc;
use private_channel_metrics::MetricLabel;
use solana_keychain::SolanaSigner;
use solana_sdk::{
    clock::MAX_PROCESSING_AGE, commitment_config::CommitmentConfig, signature::Signature,
};
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Cap on total deferrals of a single pending remint. Covers both transient
/// RPC errors during the finality check AND liveness extensions when a stored
/// signature is still within blockhash validity. Past this cap we escalate
/// to ManualReview rather than loop indefinitely.
const MAX_FINALITY_CHECK_ATTEMPTS: u32 = 3;

/// Outcome of a single `attempt_remint` call.
enum RemintAttempt {
    /// A remint landed on-chain (a prior attempt or the one just sent).
    Confirmed(Signature),
    /// Failed before any transaction could be broadcast, with no live signature
    /// in play: nothing can land, so a bounded retry that ends in ManualReview is
    /// safe. Caller re-queues via the capped escalation path.
    DeferPreBroadcast(String),
    /// A signature is persisted, or we cannot prove one isn't, so a transaction
    /// may land. Reconcile it; never terminalize on a counter. Caller re-queues
    /// without a cap so the entry keeps reclassifying until Landed/Dead.
    DeferInFlight(String),
    /// Cannot reconcile and cannot proceed safely; escalate to ManualReview.
    Failed(String),
}

/// Remint burned PrivateChannel tokens back to the user after a permanent withdrawal failure.
///
/// Persists every MintTo signature write-ahead, then classifies stored signatures on entry
/// (before any resend) so a crash between broadcast and the FailedReminted write can't
/// double-mint. Everything runs on the source chain (PrivateChannel). No sender-level retry.
async fn attempt_remint(state: &SenderState, info: &WithdrawalRemintInfo) -> RemintAttempt {
    let stored = match state
        .storage
        .get_remint_signatures(info.transaction_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            // Lookup failed: we can't read whether a prior remint sig is persisted, so
            // capping here could abandon an unread in-flight sig into a double-mint.
            // Retry the lookup instead (no resend). Row stays PendingRemint, so recovery
            // reloads it across restarts; in-process it retries until the DB recovers.
            return RemintAttempt::DeferInFlight(format!(
                "stored remint-signature lookup failed for transaction {}: {}; will retry",
                info.transaction_id, e
            ));
        }
    };

    // Prior attempts the classifier proves dead. Only these may be retired by the
    // claim below; an Uncertain or Live verdict returns before it is populated.
    let mut proven_dead: Vec<String> = Vec::new();

    if !stored.is_empty() {
        let prior_attempts: Vec<PendingSig> = match stored
            .iter()
            .map(|stored| {
                let signature = Signature::from_str(&stored.signature)
                    .map_err(|e| format!("invalid stored remint signature: {e}"))?;
                let lvbh = stored.last_valid_block_height;
                let last_valid_block_height = u64::try_from(lvbh)
                    .map_err(|_| format!("negative last_valid_block_height: {lvbh}"))?;
                Ok(PendingSig {
                    signature,
                    last_valid_block_height,
                    blockhash_slot: stored.blockhash_slot.and_then(|s| u64::try_from(s).ok()),
                })
            })
            .collect::<Result<Vec<_>, String>>()
        {
            Ok(sigs) => sigs,
            Err(e) => {
                return RemintAttempt::Failed(format!(
                    "unparseable stored remint signature for transaction {}: {}; refusing to remint",
                    info.transaction_id, e
                ));
            }
        };

        match classify_signatures(&state.source_finality(), &prior_attempts).await {
            SigFinality::Landed(signature) => {
                info!(
                    "Remint already landed for transaction {}: {}",
                    info.transaction_id, signature
                );
                return RemintAttempt::Confirmed(signature);
            }
            SigFinality::Live(reason) => {
                // A persisted sig could still land. Reconcile until it resolves;
                // blockhash expiry forces it to Landed or Dead, so this never spins.
                return RemintAttempt::DeferInFlight(format!(
                    "prior remint attempt still in flight: {reason}"
                ));
            }
            SigFinality::Uncertain(reason) => {
                return RemintAttempt::Failed(format!(
                    "remint idempotency classification unavailable for transaction {}: {}; refusing to remint",
                    info.transaction_id, reason
                ));
            }
            // All prior attempts finalized-failed or expired: safe to resend.
            SigFinality::Dead => {
                proven_dead = stored.iter().map(|s| s.signature.clone()).collect();
            }
        }
    }

    // Durable, chain-reproducible memo (survives a resync wipe); the stored signature
    // remains the live idempotency control.
    let memo = remint_idempotency_memo(&info.source_event_id);
    let admin_pubkey = SignerUtil::admin_signer().pubkey();

    // Memo is an on-chain marker only; the stored signature is the idempotency control.
    let mut builder = MintToBuilder::new();
    builder
        .mint(info.mint)
        .recipient(info.user)
        .recipient_ata(info.user_ata)
        .payer(admin_pubkey)
        .mint_authority(admin_pubkey)
        .token_program(info.token_program)
        .amount(info.amount)
        .idempotency_memo(memo);

    // No transaction is broadcast until send_signed below, so instruction-build,
    // blockhash and signing failures are pre-broadcast: defer and retry, never ManualReview.
    let instructions = match builder.instructions() {
        Ok(instructions) => instructions,
        Err(e) => {
            return RemintAttempt::DeferPreBroadcast(format!(
                "failed to build remint instructions for transaction {}: {}; will retry",
                info.transaction_id, e
            ));
        }
    };

    let ix = InstructionWithSigners {
        instructions,
        fee_payer: admin_pubkey,
        signers: vec![SignerUtil::admin_signer()],
        compute_unit_price: None,
        compute_budget: None,
    };

    let (transaction, signature, last_valid_block_height, blockhash_slot) =
        match build_and_sign(&state.source_rpc_client, ix).await {
            Ok(signed) => signed,
            Err(e) => {
                return RemintAttempt::DeferPreBroadcast(format!(
                    "failed to build/sign remint for transaction {}: {}; will retry",
                    info.transaction_id, e
                ));
            }
        };

    // Write-ahead persist before broadcast, and the exclusive claim in the same step.
    // Checked cast keeps the round-trip symmetric with the u64::try_from read-back.
    let lvbh_i64 = i64::try_from(last_valid_block_height).unwrap_or(i64::MAX);
    match state
        .storage
        .claim_remint_attempt(
            info.transaction_id,
            signature.to_string(),
            lvbh_i64,
            i64::try_from(blockhash_slot).ok(),
            &proven_dead,
        )
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            // Nothing else can hold the claim, so a second sender is running and
            // the advisory lock is gone. Emit no status of any kind: ManualReview
            // here would move the row off pending_remint and permanently block the
            // winner's remint record, stranding a mint that did land. Re-queue
            // uncapped instead; the next tick classifies the winner's signature.
            OPERATOR_REMINT_CLAIM_LOST
                .with_label_values(&[state.program_type.as_label()])
                .inc();
            error!(
                "Remint claim lost for transaction {} (trace {}): another sender owns the live attempt; refusing to broadcast",
                info.transaction_id, info.trace_id
            );
            return RemintAttempt::DeferInFlight(format!(
                "remint claim for transaction {} is held by another sender",
                info.transaction_id
            ));
        }
        Err(e) => {
            return RemintAttempt::DeferPreBroadcast(format!(
                "pre-send remint claim failed for transaction {}: {}; will retry",
                info.transaction_id, e
            ));
        }
    }

    if let Err(e) = send_signed(&state.source_rpc_client, &transaction, RetryPolicy::None).await {
        // Signature is durable; next attempt reclassifies it.
        return RemintAttempt::DeferInFlight(format!(
            "remint send failed for transaction {}: {}; will reclassify",
            info.transaction_id, e
        ));
    }

    let result = match check_transaction_status(
        state.source_rpc_client.clone(),
        &signature,
        CommitmentConfig::finalized(),
        &ExtraErrorCheckPolicy::None,
        state.confirmation_poll_interval_ms,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            return RemintAttempt::DeferInFlight(format!(
                "remint confirmation failed for transaction {}: {}; will reclassify",
                info.transaction_id, e
            ));
        }
    };

    match result {
        ConfirmationResult::Confirmed => {
            info!("Remint confirmed: {}", signature);
            RemintAttempt::Confirmed(signature)
        }
        other => RemintAttempt::DeferInFlight(format!("remint not yet confirmed: {:?}", other)),
    }
}

/// Result of executing a matured PendingRemint entry.
/// Boxed variants keep the enum small (PendingRemint is ~300 bytes).
pub enum DeferredRemintOutcome {
    /// Terminal: a FailedReminted or ManualReview status was already emitted.
    Resolved,
    /// Failed before broadcast with no live sig: caller re-queues via the capped
    /// escalation path, which ends in ManualReview once the retry budget is spent.
    DeferPreBroadcast(Box<PendingRemint>, String),
    /// A sig is (or might be) persisted: caller re-queues without a cap so the
    /// entry keeps reclassifying until it resolves. Never terminalized on a counter.
    DeferInFlight(Box<PendingRemint>, String),
}

/// Execute the actual remint for a matured PendingRemint entry.
pub async fn execute_deferred_remint(
    state: &SenderState,
    entry: PendingRemint,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) -> DeferredRemintOutcome {
    match attempt_remint(state, &entry.remint_info).await {
        RemintAttempt::Confirmed(signature) => {
            info!(
                "Withdrawal failed but tokens reminted successfully: {}",
                signature
            );

            // Always Some for remint entries (queued only for a failed withdrawal,
            // which has a DB row). No id means no row to record against — log loudly.
            let Some(transaction_id) = entry.ctx.transaction_id else {
                error!(
                    "Remint confirmed (sig: {}) but entry has no transaction_id; cannot record FailedReminted",
                    signature
                );
                return DeferredRemintOutcome::Resolved;
            };

            // Flip to FailedReminted now so a crash before the async writer runs can't
            // leave the row PendingRemint for recovery to remint again.
            match state
                .storage
                .record_remint_result(transaction_id, signature.to_string())
                .await
            {
                // Row is durably terminal: safe to drop the write-ahead rows.
                Ok(()) => {
                    if let Err(e) = state.storage.delete_remint_signatures(transaction_id).await {
                        warn!(
                            "Failed to clear remint signatures for txn {}: {}; GC will sweep",
                            transaction_id, e
                        );
                    }
                }
                // Row stays PendingRemint, so the write-ahead rows MUST be kept: if we
                // crash before the async writer below commits, restart recovery has to
                // classify this landed signature instead of broadcasting a duplicate.
                Err(persist_err) => {
                    error!(
                        "Remint sig {} confirmed but durable persist failed for txn {}: {}; keeping write-ahead rows, falling back to async writer",
                        signature, transaction_id, persist_err
                    );
                }
            }

            // Drives the webhook alert, and is the fallback status write. No-op once
            // the row is already FailedReminted.
            if let Err(e) = send_guaranteed(
                storage_tx,
                TransactionStatusUpdate {
                    transaction_id,
                    trace_id: entry.ctx.trace_id.clone(),
                    status: TransactionStatus::FailedReminted,
                    counterpart_signature: None,
                    processed_at: Some(Utc::now()),
                    error_message: Some(entry.original_error.clone()),
                    remint_signature: Some(signature.to_string()),
                    remint_attempted: true,
                    release_signatures: None,
                },
                "transaction status update",
            )
            .await
            {
                error!(
                    "Failed to send FailedReminted status for txn {}: {}. Remint sig {} confirmed on-chain.",
                    transaction_id, e, signature
                );
            }
            DeferredRemintOutcome::Resolved
        }
        RemintAttempt::DeferPreBroadcast(reason) => {
            DeferredRemintOutcome::DeferPreBroadcast(Box::new(entry), reason)
        }
        RemintAttempt::DeferInFlight(reason) => {
            DeferredRemintOutcome::DeferInFlight(Box::new(entry), reason)
        }
        RemintAttempt::Failed(remint_error) => {
            error!("Remint also failed: {}", remint_error);
            let combined = format!("{} | remint failed: {}", entry.original_error, remint_error);
            if let Some(transaction_id) = entry.ctx.transaction_id {
                send_guaranteed(
                    storage_tx,
                    TransactionStatusUpdate {
                        transaction_id,
                        trace_id: entry.ctx.trace_id.clone(),
                        status: TransactionStatus::ManualReview,
                        counterpart_signature: None,
                        processed_at: Some(Utc::now()),
                        error_message: Some(combined),
                        remint_signature: None,
                        remint_attempted: true,
                        release_signatures: None,
                    },
                    "transaction status update",
                )
                .await
                .ok();
            }
            DeferredRemintOutcome::Resolved
        }
    }
}

/// On-chain finality verdict for a set of broadcast signatures (withdrawal
/// releases or remint MintTos). Shared by the remint gate and recovery.
pub(crate) enum SigFinality {
    /// A signature finalized successfully — the transaction landed.
    Landed(Signature),
    /// A signature could still land; carries a reason for triage logs.
    Live(String),
    /// Every signature is finalized-failed or expired — safe to remint/demote.
    Dead,
    /// Could not classify (RPC/length error); callers must NOT treat as Dead.
    Uncertain(String),
}

/// Which chain an endpoint serves, tagged statically because nothing on the wire
/// distinguishes them. Used only by `coverage_verdict`, to decide whether an
/// attempt with no journaled blockhash slot may reconstruct one, and for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Chain {
    /// PrivateChannel, whose blockhash window is operator-tunable.
    Channel,
    /// Solana, whose 150-block window is protocol-fixed.
    Solana,
}

impl Chain {
    /// Metric label for this chain.
    fn chain_label(self) -> &'static str {
        match self {
            Chain::Channel => "channel",
            Chain::Solana => "solana",
        }
    }
}

/// A primary RPC endpoint plus an optional fallback. One endpoint's missing status
/// can be a prune or lag rather than proof, so only a `Dead` verdict re-checks it.
pub(crate) struct FinalityRpc<'a> {
    pub primary: &'a RpcClientWithRetry,
    pub fallback: Option<&'a RpcClientWithRetry>,
    /// Which chain these endpoints serve.
    pub chain: Chain,
}

impl<'a> FinalityRpc<'a> {
    /// Endpoints on the PrivateChannel chain. Its `max_blockhashes` is operator-tunable,
    /// so no window value is carried here: every attempt journals the slot its own
    /// blockhash was read at, which is what bounds the retention proof.
    pub fn channel(
        primary: &'a RpcClientWithRetry,
        fallback: Option<&'a RpcClientWithRetry>,
    ) -> Self {
        Self {
            primary,
            fallback,
            chain: Chain::Channel,
        }
    }

    /// Endpoints on Solana, whose 150-block blockhash validity is protocol-fixed
    /// and therefore never operator-tunable.
    pub fn solana(
        primary: &'a RpcClientWithRetry,
        fallback: Option<&'a RpcClientWithRetry>,
    ) -> Self {
        Self {
            primary,
            fallback,
            chain: Chain::Solana,
        }
    }
}

/// Per-endpoint classification carrying the extra detail the policy layer needs
/// to decide whether an absence-based `Dead` requires a ledger-coverage proof.
enum EndpointVerdict {
    Landed(Signature),
    Live(String),
    Uncertain(String),
    /// Every signature carries a finalized-failed status: positive on-chain
    /// evidence of non-inclusion, so no coverage proof is needed.
    DeadFinalizedFailure,
    /// At least one signature is null-status past its blockhash validity. Absence
    /// is trustworthy only if the endpoint still retains the attempt's slot range;
    /// `min_lvbh` is the lowest such height, bounding the top of that range.
    DeadByAbsence {
        min_lvbh: u64,
        /// Lowest journaled blockhash slot across those signatures, which is the
        /// exact bottom of the range. `None` if any of them predates the column,
        /// in which case the bound falls back to the window derivation.
        min_blockhash_slot: Option<u64>,
    },
}

/// Resolve an absence-based `Dead`: `Dead` only when the endpoint proves it retains the
/// attempt's slot range, else `Uncertain`. A floor at or below the bottom of that range
/// proves retention. Assumes a single consistent archival endpoint, not a split pool.
///
/// The bottom of the range is the slot the attempt's blockhash was read at, journaled
/// with the broadcast. An attempt journaled before that column existed carries none, and
/// then the bound depends on whether the chain's window can move:
///
/// - Solana's is `MAX_PROCESSING_AGE`, fixed by the protocol, so `lvbh - window` holds no
///   matter when the attempt was broadcast (slot >= height, and that slack only
///   over-reports Uncertain, never a false covered).
/// - The channel's is `max_blockhashes`, which an operator can lower. A reduction before
///   the read would reconstruct a bound narrower than the one the attempt was actually
///   broadcast under, and nothing left on the row can reveal that. So absence is not
///   provable and the verdict is `Uncertain`.
async fn coverage_verdict(
    finality: &FinalityRpc<'_>,
    rpc: &RpcClientWithRetry,
    min_lvbh: u64,
    min_blockhash_slot: Option<u64>,
    endpoint_label: &str,
) -> SigFinality {
    let chain = finality.chain.chain_label();
    let bound = match (min_blockhash_slot, finality.chain) {
        (Some(slot), _) => slot,
        (None, Chain::Solana) => min_lvbh.saturating_sub(MAX_PROCESSING_AGE as u64),
        (None, Chain::Channel) => {
            OPERATOR_ABSENCE_CLASSIFY
                .with_label_values(&[chain, "uncertain"])
                .inc();
            return SigFinality::Uncertain(format!(
                "{endpoint_label}attempt predates the journaled blockhash slot (lvbh {min_lvbh}); \
                 the channel's blockhash window may have been reduced since it was broadcast, so \
                 absence is not proof of non-inclusion"
            ));
        }
    };
    let floor = match rpc.get_first_available_block().await {
        Ok(floor) => floor,
        Err(e) => {
            OPERATOR_ABSENCE_CLASSIFY
                .with_label_values(&[chain, "uncertain"])
                .inc();
            return SigFinality::Uncertain(format!("ledger floor RPC failed: {e}"));
        }
    };
    if floor <= bound {
        OPERATOR_ABSENCE_CLASSIFY
            .with_label_values(&[chain, "dead"])
            .inc();
        SigFinality::Dead
    } else {
        OPERATOR_ABSENCE_CLASSIFY
            .with_label_values(&[chain, "uncertain"])
            .inc();
        SigFinality::Uncertain(format!(
            "{endpoint_label}ledger floor {floor} above attempt window (lvbh {min_lvbh}, retained-slot bound {bound}); pruned or lagging, absence is not proof of non-inclusion"
        ))
    }
}

/// Log the case corroboration exists to catch: the primary called signatures dead but the fallback disagrees.
/// `detail` carries the overriding verdict's payload (landed signature or live reason) so triage needs no re-query.
fn warn_fallback_override(verdict: &str, detail: &str, sigs: &[PendingSig]) {
    warn!(
        "finality fallback overrode a primary Dead verdict ({verdict}: {detail}) for signature(s): {}",
        sigs.iter()
            .map(|p| p.signature.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
}

/// Classify `sigs` into a corroborated, coverage-proven verdict: a `Dead` must survive a ledger-floor
/// check, so a prunable absence degrades to `Uncertain`. Safe verdicts return early, so the floor RPC is rare.
pub(crate) async fn classify_signatures(
    finality: &FinalityRpc<'_>,
    sigs: &[PendingSig],
) -> SigFinality {
    let (primary_lvbh, primary_blockhash_slot) =
        match classify_endpoint(finality.primary, sigs).await {
            EndpointVerdict::Landed(sig) => return SigFinality::Landed(sig),
            EndpointVerdict::Live(reason) => return SigFinality::Live(reason),
            EndpointVerdict::Uncertain(reason) => return SigFinality::Uncertain(reason),
            // A finalized-failed status is immutable on-chain evidence, so trust it directly and
            // skip fallback corroboration; only an absence-based Dead needs a coverage proof.
            EndpointVerdict::DeadFinalizedFailure => return SigFinality::Dead,
            EndpointVerdict::DeadByAbsence {
                min_lvbh,
                min_blockhash_slot,
            } => (min_lvbh, min_blockhash_slot),
        };

    match finality.fallback {
        // Destination path: the primary is allowed to be pruned (that is why the
        // fallback exists), so we trust the fallback's verdict and coverage-check
        // the fallback, never the primary.
        Some(fb) => match classify_endpoint(fb, sigs).await {
            EndpointVerdict::Landed(sig) => {
                warn_fallback_override("Landed", &sig.to_string(), sigs);
                SigFinality::Landed(sig)
            }
            EndpointVerdict::Live(reason) => {
                warn_fallback_override("Live", &reason, sigs);
                SigFinality::Live(reason)
            }
            EndpointVerdict::Uncertain(reason) => SigFinality::Uncertain(reason),
            EndpointVerdict::DeadFinalizedFailure => SigFinality::Dead,
            EndpointVerdict::DeadByAbsence {
                min_lvbh,
                min_blockhash_slot,
            } => coverage_verdict(finality, fb, min_lvbh, min_blockhash_slot, "fallback ").await,
        },
        // Source/escrow single endpoint: no second node can corroborate, so the
        // sole endpoint's coverage is the whole protection.
        None => {
            coverage_verdict(
                finality,
                finality.primary,
                primary_lvbh,
                primary_blockhash_slot,
                "",
            )
            .await
        }
    }
}

/// Thin test-only wrapper over `classify_endpoint` mapping both `Dead` shapes to `SigFinality::Dead`.
/// Lets the per-endpoint unit tests assert status logic without the coverage gate `classify_signatures` adds.
#[cfg(test)]
pub(crate) async fn classify_against(rpc: &RpcClientWithRetry, sigs: &[PendingSig]) -> SigFinality {
    match classify_endpoint(rpc, sigs).await {
        EndpointVerdict::Landed(sig) => SigFinality::Landed(sig),
        EndpointVerdict::Live(reason) => SigFinality::Live(reason),
        EndpointVerdict::Uncertain(reason) => SigFinality::Uncertain(reason),
        EndpointVerdict::DeadFinalizedFailure | EndpointVerdict::DeadByAbsence { .. } => {
            SigFinality::Dead
        }
    }
}

/// Classify `sigs` against one endpoint's `getSignatureStatuses` history, distinguishing a
/// finalized-failed `Dead` from an absence-based one so only the latter needs a coverage proof.
async fn classify_endpoint(rpc: &RpcClientWithRetry, sigs: &[PendingSig]) -> EndpointVerdict {
    let flat: Vec<Signature> = sigs.iter().map(|p| p.signature).collect();

    let response = match rpc.get_signature_statuses_with_history(&flat).await {
        Ok(r) => r,
        Err(e) => {
            return EndpointVerdict::Uncertain(format!("signature status RPC failed: {}", e));
        }
    };

    // RPC returns one status per signature in order; a length mismatch would
    // silently skip checks below, so treat it as uncertain.
    if response.value.len() != flat.len() {
        return EndpointVerdict::Uncertain(format!(
            "RPC returned {} statuses for {} signatures",
            response.value.len(),
            flat.len()
        ));
    }

    // Any sig finalized successfully means the transaction landed.
    let finalized_success_index = response.value.iter().position(|signature_status| {
        signature_status.as_ref().is_some_and(|status| {
            status.satisfies_commitment(CommitmentConfig::finalized()) && status.err.is_none()
        })
    });
    if let Some(index) = finalized_success_index {
        return EndpointVerdict::Landed(flat[index]);
    }

    // Only the lvbh check on null-status sigs needs it, so it costs a call only
    // when one is absent. It must be a height on both chains: a context slot
    // outruns the height, and judging an lvbh against one abandons live work.
    let current_height = if response.value.iter().any(|s| s.is_none()) {
        match rpc.get_block_height().await {
            Ok(h) => h,
            Err(e) => {
                return EndpointVerdict::Uncertain(format!("block height RPC failed: {}", e));
            }
        }
    } else {
        // Unused: the null-status branch below only fires when some status is None.
        0
    };

    // Lowest lvbh across null-status expired sigs bounds the slot range whose
    // retention the coverage proof must cover.
    let mut min_absent_lvbh: Option<u64> = None;
    // Lowest journaled blockhash slot across those same sigs, the exact bottom of
    // that range. Latched off the moment one of them has none.
    let mut min_absent_blockhash_slot: Option<u64> = None;
    let mut absent_slot_unknown = false;

    // Walk the sigs to see if any could still land (index-aligned with response.value).
    for (index, pending_sig) in sigs.iter().enumerate() {
        let signature_status = &response.value[index];

        if let Some(status) = signature_status.as_ref() {
            // Only `finalized` is definitive; success was handled above, so this is failure.
            if status.satisfies_commitment(CommitmentConfig::finalized()) {
                continue;
            }
            // confirmed/processed: in a block, will finalize regardless of blockhash validity.
            return EndpointVerdict::Live(
                "signature is on-chain (confirmed/processed) and awaiting finalization".to_string(),
            );
        }

        // No status entry. lvbh is the only thing keeping it alive.
        if current_height > pending_sig.last_valid_block_height {
            min_absent_lvbh = Some(
                min_absent_lvbh.map_or(pending_sig.last_valid_block_height, |m| {
                    m.min(pending_sig.last_valid_block_height)
                }),
            );
            // One attempt without a journaled slot forfeits the exact bound for
            // the whole set: the proof must cover every absent signature, and
            // that one's earliest possible block is unknown.
            match pending_sig.blockhash_slot {
                Some(slot) if !absent_slot_unknown => {
                    min_absent_blockhash_slot =
                        Some(min_absent_blockhash_slot.map_or(slot, |m: u64| m.min(slot)));
                }
                Some(_) => {}
                None => {
                    absent_slot_unknown = true;
                    min_absent_blockhash_slot = None;
                }
            }
            continue;
        }
        return EndpointVerdict::Live(format!(
            "signatures still within blockhash validity (current_height={})",
            current_height
        ));
    }

    match min_absent_lvbh {
        // At least one sig is an expired absence: its non-inclusion needs a proof.
        Some(min_lvbh) => EndpointVerdict::DeadByAbsence {
            min_lvbh,
            min_blockhash_slot: min_absent_blockhash_slot,
        },
        // No absence: every sig carried a finalized-failed status.
        None => EndpointVerdict::DeadFinalizedFailure,
    }
}

/// Process matured entries in the deferred remint queue. For each matured
/// entry, classify the stored withdrawal signatures and pick one of:
///   1. Any sig finalized + success → report Completed.
///   2. Any sig still live (has a non-finalized status entry, OR has no
///      status entry but still within blockhash validity)
///      → defer with extended deadline.
///   3. Every sig finalized-failed, or null-status with expired blockhash
///      → remint.
///
/// RPC failures during classification fall through the same defer-or-escalate
/// path as case 2.
pub async fn process_pending_remints(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    // Deferred remints are a Withdraw-only responsibility; skip for any other role.
    if state.program_type != ProgramType::Withdraw {
        return;
    }

    let now = Utc::now();

    // Drain the queue and split: due now vs. wait longer.
    let mut remaining = Vec::new();
    let mut matured = Vec::new();
    for entry in state.pending_remints.drain(..) {
        if entry.deadline <= now {
            matured.push(entry);
        } else {
            remaining.push(entry);
        }
    }

    // Each matured entry leaves the queue unless we push it back into `remaining`.
    for entry in matured {
        let nonce_label = entry
            .ctx
            .withdrawal_nonce
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".to_string());

        // Classify the stored signatures against on-chain state. This runs on
        // rpc_client (the destination/Solana chain where ReleaseFunds was sent),
        // not source_rpc_client which only the remint MintTo uses.
        match classify_signatures(&state.dest_finality(), &entry.signatures).await {
            // Case 1: a sig finalized successfully, the withdrawal landed.
            SigFinality::Landed(sig) => {
                handle_release_landed(state, &entry, &nonce_label, sig, storage_tx).await;
            }
            // Case 2: could still land or unclassifiable → defer, don't remint.
            SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
                defer_or_escalate(
                    &mut remaining,
                    entry,
                    &nonce_label,
                    &reason,
                    &state.storage,
                    storage_tx,
                )
                .await;
            }
            // Case 3: the classifier calls every sig dead by absence. Before
            // reminting (which moves value), confirm non-inclusion against the
            // on-chain SMT root; a pruned/lagging endpoint can report absence for
            // a release that actually landed. Only a proven NotLanded remints.
            SigFinality::Dead => {
                let verdict = match entry.ctx.withdrawal_nonce {
                    Some(nonce) => {
                        let max_lvbh = entry
                            .signatures
                            .iter()
                            .map(|p| p.last_valid_block_height)
                            .max()
                            .unwrap_or(0);
                        let v = verify_release_landed(
                            &state.rpc_client,
                            &state.storage,
                            state.instance_pda,
                            nonce,
                            max_lvbh,
                        )
                        .await;
                        // Count only real verifications so the None short-circuit
                        // below is not mislabeled as a verified non-landing.
                        let label = match &v {
                            ReleaseVerdict::Landed => "landed",
                            ReleaseVerdict::NotLanded => "not_landed",
                            ReleaseVerdict::Uncertain(_) => "uncertain",
                        };
                        OPERATOR_RELEASE_VERIFY
                            .with_label_values(&["remint", label])
                            .inc();
                        v
                    }
                    // No nonce to prove membership; fall through to the remint path,
                    // which re-checks remint idempotency before broadcasting. No
                    // verification ran, so no release-verify metric is emitted here.
                    None => ReleaseVerdict::NotLanded,
                };

                match verdict {
                    // Proven landed on-chain: skip the remint, mark Completed.
                    ReleaseVerdict::Landed => {
                        match entry.signatures.first().map(|p| p.signature) {
                            Some(sig) => {
                                handle_release_landed(state, &entry, &nonce_label, sig, storage_tx)
                                    .await;
                            }
                            // Landed but nothing recorded to attribute it to; defer
                            // rather than complete with no provenance.
                            None => {
                                defer_or_escalate(
                                    &mut remaining,
                                    entry,
                                    &nonce_label,
                                    "release verified landed but no signature recorded",
                                    &state.storage,
                                    storage_tx,
                                )
                                .await;
                            }
                        }
                    }
                    // Proven not landed: remint is safe.
                    ReleaseVerdict::NotLanded => {
                        info!(
                            "All withdrawal signatures for nonce {} are finalized-failed or expired; attempting remint",
                            nonce_label
                        );
                        match execute_deferred_remint(state, entry, storage_tx).await {
                            DeferredRemintOutcome::Resolved => {}
                            // Nothing was broadcast: bounded retry, then ManualReview.
                            // Safe because no signature can land.
                            DeferredRemintOutcome::DeferPreBroadcast(entry, reason) => {
                                defer_or_escalate(
                                    &mut remaining,
                                    *entry,
                                    &nonce_label,
                                    &reason,
                                    &state.storage,
                                    storage_tx,
                                )
                                .await;
                            }
                            // A signature is (or might be) persisted: re-queue without
                            // the cap so it keeps reclassifying. Terminalizing here would
                            // abandon a possibly-live sig and invite a double-mint.
                            DeferredRemintOutcome::DeferInFlight(entry, reason) => {
                                requeue_in_flight(&mut remaining, *entry, &nonce_label, &reason);
                            }
                        }
                    }
                    // Uncertain: fail closed. Defer to a later tick or escalate.
                    ReleaseVerdict::Uncertain(reason) => {
                        defer_or_escalate(
                            &mut remaining,
                            entry,
                            &nonce_label,
                            &format!("release verification uncertain ({reason})"),
                            &state.storage,
                            storage_tx,
                        )
                        .await;
                    }
                }
            }
        }
    }

    // `remaining` = entries not yet due + entries re-queued by `defer_or_escalate`
    // or `requeue_in_flight`.
    state.pending_remints = remaining;
}

/// Shared handling for a release confirmed landed (by the classifier fast path
/// or the SMT verify gate): re-insert the nonce into the local SMT, which
/// handle_permanent_failure had removed assuming failure, then mark Completed.
/// Skips the SMT touch if the tree already rotated past this nonce's window.
async fn handle_release_landed(
    state: &mut SenderState,
    entry: &PendingRemint,
    nonce_label: &str,
    sig: Signature,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    if let Some(nonce) = entry.ctx.withdrawal_nonce {
        if let Some(smt) = state.smt_state.as_mut() {
            if smt.smt_state.tree_index() == nonce / MAX_TREE_LEAVES as u64 {
                if smt.smt_state.insert_nonce(nonce) {
                    info!("Re-inserted landed nonce {nonce} into local SMT");
                } else {
                    debug!("Landed nonce {nonce} already present in local SMT, no divergence");
                }
            }
        }
    }
    send_completed(storage_tx, entry, nonce_label, sig).await;
}

/// Report a pending-remint entry as Completed because one of its withdrawal
/// signatures finalized on Solana.
async fn send_completed(
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    entry: &PendingRemint,
    nonce_label: &str,
    sig: Signature,
) {
    info!(
        "Withdrawal nonce {} finalized on-chain (sig: {}); skipping remint",
        nonce_label, sig
    );
    // Always Some in practice: entries are only queued for withdrawals, which
    // carry a DB id. A None here drops a finalized withdrawal with no DB trace,
    // so log it instead of returning silently.
    let Some(transaction_id) = entry.ctx.transaction_id else {
        error!(
            "send_completed for nonce {} has no transaction_id; finalized withdrawal (sig: {}) cannot be marked Completed",
            nonce_label, sig
        );
        return;
    };
    send_guaranteed(
        storage_tx,
        TransactionStatusUpdate {
            transaction_id,
            trace_id: entry.ctx.trace_id.clone(),
            status: TransactionStatus::Completed,
            counterpart_signature: Some(sig.to_string()),
            processed_at: Some(Utc::now()),
            error_message: None,
            remint_signature: None,
            remint_attempted: false,
            // Durable provenance of every broadcast attempt. counterpart_signature
            // stays the single landed sig; this keeps the full list.
            release_signatures: Some(
                entry
                    .signatures
                    .iter()
                    .map(|p| p.signature.to_string())
                    .collect(),
            ),
        },
        "transaction status update",
    )
    .await
    .ok();
}

/// Bump the entry's deferral counter and either re-queue with an extended
/// deadline or escalate to ManualReview when the cap is hit. For the bounded
/// paths only: dest-signature classification (Case 2) and pre-broadcast remint
/// failures, where no signature can land so terminalizing at the cap is safe.
/// In-flight remint defers use `requeue_in_flight` instead and never hit this cap.
async fn defer_or_escalate(
    remaining: &mut Vec<PendingRemint>,
    entry: PendingRemint,
    nonce_label: &str,
    reason: &str,
    storage: &crate::storage::Storage,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let attempt = entry.finality_check_attempts + 1;

    if attempt >= MAX_FINALITY_CHECK_ATTEMPTS {
        error!(
            "Pending remint for nonce {} reached attempt cap ({}); escalating to ManualReview: {}",
            nonce_label, attempt, reason
        );
        if let Some(transaction_id) = entry.ctx.transaction_id {
            send_guaranteed(
                storage_tx,
                TransactionStatusUpdate {
                    transaction_id,
                    trace_id: entry.ctx.trace_id.clone(),
                    status: TransactionStatus::ManualReview,
                    counterpart_signature: None,
                    processed_at: Some(Utc::now()),
                    error_message: Some(format!(
                        "{} | escalated to ManualReview after {} attempts: {}",
                        entry.original_error, attempt, reason
                    )),
                    remint_signature: None,
                    remint_attempted: false,
                    release_signatures: None,
                },
                "transaction status update",
            )
            .await
            .ok();
        }
        return;
    }

    let new_deadline = Utc::now() + chrono::Duration::from_std(FINALITY_SAFETY_DELAY).unwrap();

    // Fail-closed: an inability to persist the bumped counter is itself
    // ambiguity. Escalate to ManualReview rather than continue deferring with
    // a counter we can't trust to survive a restart.
    if let Some(transaction_id) = entry.ctx.transaction_id {
        if let Err(persist_err) = storage
            .bump_pending_remint_finality_attempt(transaction_id, attempt as i32, new_deadline)
            .await
        {
            error!(
                "Pending remint for nonce {} counter persist failed, escalating to ManualReview: {}",
                nonce_label, persist_err
            );
            send_guaranteed(
                storage_tx,
                TransactionStatusUpdate {
                    transaction_id,
                    trace_id: entry.ctx.trace_id.clone(),
                    status: TransactionStatus::ManualReview,
                    counterpart_signature: None,
                    processed_at: Some(Utc::now()),
                    error_message: Some(format!(
                        "{} | counter persist failed at attempt {}: {}",
                        entry.original_error, attempt, persist_err
                    )),
                    remint_signature: None,
                    remint_attempted: false,
                    release_signatures: None,
                },
                "transaction status update",
            )
            .await
            .ok();
            return;
        }
    }

    warn!(
        "Pending remint for nonce {} deferred (attempt {}/{}): {}",
        nonce_label, attempt, MAX_FINALITY_CHECK_ATTEMPTS, reason
    );
    remaining.push(PendingRemint {
        finality_check_attempts: attempt,
        deadline: new_deadline,
        ..entry
    });
}

/// Re-queue an in-flight remint (a sig is, or might be, persisted). Never
/// terminalizes and never bumps the counter: the classify gate resolves the sig
/// on a later tick, and terminalizing a live sig would risk a double-mint.
fn requeue_in_flight(
    remaining: &mut Vec<PendingRemint>,
    entry: PendingRemint,
    nonce_label: &str,
    reason: &str,
) {
    let new_deadline = Utc::now() + chrono::Duration::from_std(FINALITY_SAFETY_DELAY).unwrap();
    warn!(
        "Pending remint for nonce {} still in flight, re-queued (not terminalized): {}",
        nonce_label, reason
    );
    remaining.push(PendingRemint {
        deadline: new_deadline,
        ..entry
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::sender::types::{
        PendingRemint, PendingSig, SenderSMTState, SenderState, TransactionContext, MAX_IN_FLIGHT,
    };
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use crate::operator::utils::smt_util::SmtState;
    use crate::operator::MintCache;
    use crate::operator::RetryConfig;
    use crate::operator::RpcClientWithRetry;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::StoredSig;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Once;
    use tokio::sync::{mpsc, Semaphore};

    static INIT_TEST_SIGNER: Once = Once::new();
    fn ensure_test_signer() {
        INIT_TEST_SIGNER.call_once(|| {
            let kp = solana_sdk::signer::keypair::Keypair::new();
            let b58 = bs58::encode(kp.to_bytes()).into_string();
            std::env::set_var("ADMIN_SIGNER", "memory");
            std::env::set_var("ADMIN_PRIVATE_KEY", &b58);
        });
    }

    fn make_sender_state() -> (SenderState, MockStorage) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            crate::operator::RetryConfig::default(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let state = SenderState {
            rpc_client: rpc.clone(),
            source_rpc_client: rpc,
            fallback_rpc_client: None,
            storage: storage.clone(),
            instance_pda: Some(Pubkey::new_unique()),
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Withdraw,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        };
        (state, mock)
    }

    /// Push a stub PendingRemint row into the mock so a subsequent
    /// `bump_pending_remint_finality_attempt(id, ...)` can find a row to update.
    /// Only the id and attempts fields matter for the bump path.
    fn seed_pending_remint_row(mock: &MockStorage, id: i64, attempts: i32) {
        use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
        let now = Utc::now();
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id,
                signature: Signature::new_unique().to_string(),
                trace_id: format!("trace-{id}"),
                slot: 0,
                initiator: Pubkey::new_unique().to_string(),
                recipient: Pubkey::new_unique().to_string(),
                mint: Pubkey::new_unique().to_string(),
                amount: TokenAmount(0),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(id),
                status: TransactionStatus::PendingRemint,
                created_at: now,
                updated_at: now,
                processed_at: None,
                counterpart_signature: None,
                remint_signatures: None,
                remint_last_valid_block_heights: None,
                pending_remint_deadline_at: Some(now),
                finality_check_attempts: attempts,
                recovery_requeue_attempts: 0,
                instruction_index: 0,
                inner_index: None,
                landed_remint_signature: None,
            });
    }

    fn make_remint_info(txn_id: i64) -> WithdrawalRemintInfo {
        WithdrawalRemintInfo {
            transaction_id: txn_id,
            source_event_id: crate::operator::instruction_util::SourceEventId::new(
                &format!("remint-sig-{txn_id}"),
                0,
                None,
            ),
            trace_id: format!("trace-{txn_id}"),
            mint: solana_sdk::pubkey::Pubkey::new_unique(),
            user: solana_sdk::pubkey::Pubkey::new_unique(),
            user_ata: solana_sdk::pubkey::Pubkey::new_unique(),
            token_program: spl_token::id(),
            amount: 5000,
        }
    }

    fn make_sender_state_with_rpc(rpc_url: &str) -> (SenderState, MockStorage) {
        make_sender_state_with_role(rpc_url, crate::config::ProgramType::Withdraw)
    }

    fn make_sender_state_with_role(
        rpc_url: &str,
        role: crate::config::ProgramType,
    ) -> (SenderState, MockStorage) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            rpc_url.to_string(),
            crate::operator::RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let state = SenderState {
            rpc_client: rpc.clone(),
            source_rpc_client: rpc,
            fallback_rpc_client: None,
            storage: storage.clone(),
            instance_pda: Some(Pubkey::new_unique()),
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: role,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        };
        (state, mock)
    }

    /// Register a mockito response for a specific Solana RPC method.
    async fn mock_rpc(server: &mut mockito::Server, method: &str, body: &str) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(format!(
                r#""method"\s*:\s*"{}""#,
                method
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await
    }

    /// getAccountInfo returning an escrow instance whose root proves `nonce` did
    /// NOT land (empty completed set), fresh enough to pass the freshness gate, so
    /// the SMT verify resolves NotLanded and the Dead branch remints exactly as it
    /// did before the gate existed. Also registers a fresh finalized getLatestBlockhash
    /// (the verifier's freshness anchor). It matches only the finalized commitment so
    /// the remint's own confirmed getLatestBlockhash on the source path is untouched.
    /// Feature-proof: tree_index is derived from the same MAX_TREE_LEAVES the verifier uses.
    fn mock_instance_not_landed(server: &mut mockito::Server, nonce: u64) -> mockito::Mock {
        use crate::operator::utils::smt_util::SmtState;
        use base64::{engine::general_purpose::STANDARD, Engine};
        use borsh::BorshSerialize;
        use private_channel_escrow_program_client::Instance;
        // Finalized tip well above any lvbh in these tests, so the freshness check passes.
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getLatestBlockhash""#.into()),
                mockito::Matcher::Regex(r#""finalized""#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":{"context":{"slot":1000000000},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000000000}},"id":0}"#)
            .create();
        let tree_index = nonce / MAX_TREE_LEAVES as u64;
        let root = SmtState::new(tree_index).current_root();
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
            .with_header("content-type", "application/json")
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        // Context slot is no longer read by the gate; freshness uses height.
                        "context": {"slot": 1_000_000_000u64},
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

    /// Builds a SenderState with distinct rpc_client and source_rpc_client
    /// endpoints, matching the cross-chain withdraw operator (rpc_url=Solana,
    /// source_rpc_url=PrivateChannel).
    fn make_sender_state_split_rpc(
        dest_url: &str,
        source_url: &str,
        dest_fallback_url: Option<&str>,
    ) -> (SenderState, MockStorage) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let fast = crate::operator::RetryConfig {
            max_attempts: 1,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(1),
        };
        let rpc_client = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            dest_url.to_string(),
            fast.clone(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let source_rpc_client = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            source_url.to_string(),
            fast.clone(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let fallback_rpc_client = dest_fallback_url.map(|url| {
            Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
                url.to_string(),
                fast,
                solana_sdk::commitment_config::CommitmentConfig::confirmed(),
            ))
        });
        let state = SenderState {
            rpc_client,
            source_rpc_client,
            fallback_rpc_client,
            storage: storage.clone(),
            instance_pda: Some(Pubkey::new_unique()),
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 1,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Withdraw,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            release_leases: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        };
        (state, mock)
    }

    /// The withdraw operator's compensating remint MintTo must be broadcast on the
    /// source (PrivateChannel) RPC, where the burn occurred, not on the destination
    /// (Solana) RPC used for ReleaseFunds.
    ///
    /// Asserts the sendTransaction broadcast reaches the source server. On the buggy
    /// code the remint runs against rpc_client, so the source server is never called.
    #[tokio::test]
    async fn withdrawal_remint_broadcasts_to_source_rpc_not_destination() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await; // Solana / rpc_client
        let mut source = mockito::Server::new_async().await; // PrivateChannel / source_rpc_client

        // Destination: release sig finalized-failed, so classify returns Dead and
        // the gate proceeds to remint.
        let _dest_status = dest
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                    "slot":100,"confirmations":null,
                    "err":{"InstructionError":[0,{"Custom":1}]},
                    "status":{"Err":{"InstructionError":[0,{"Custom":1}]}},
                    "confirmationStatus":"finalized"}]},"id":0}"#,
            )
            .create_async()
            .await;
        // Dead branch now SMT-verifies before reminting; prove nonce 5 NotLanded.
        let _dest_account = mock_instance_not_landed(&mut dest, 5);

        // Source: backs the remint blockhash and broadcast. With no stored
        // remint signatures the idempotency classification is skipped entirely.
        let _src_bh = mock_rpc(
            &mut source,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        let sent_sig = Signature::new_unique();
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{sent_sig}","id":0}}"#
            ))
            .expect_at_least(1)
            .create_async()
            .await;

        let (mut state, _mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(555),
                withdrawal_nonce: Some(5),
                trace_id: Some("trace-555".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(555),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // The broadcast must reach the source server. The mocked node returns a
        // placeholder signature so the send does not confirm, but the request
        // still proves which chain the remint targeted.
        src_send.assert_async().await;
    }

    #[tokio::test]
    async fn process_pending_remints_requeues_on_rpc_error() {
        let (mut state, mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter; the row must exist in the
        // mock for that write to succeed (otherwise the counter is held).
        seed_pending_remint_row(&mock, 20, 0);

        // Push a matured entry — RPC will fail (no real endpoint)
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "max retries".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // RPC error on first attempt → re-queued, not resolved
        assert!(
            storage_rx.try_recv().is_err(),
            "should NOT send status on first RPC failure"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "should re-queue entry after RPC error"
        );
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);

        // The bumped counter must also be persisted so it survives a restart.
        let persisted = mock
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == 20)
            .map(|t| t.finality_check_attempts);
        assert_eq!(persisted, Some(1));
    }

    /// Fail-closed on persist failure: if the counter bump can't be written,
    /// the safety fuse is no longer trustworthy, so the entry must escalate
    /// to ManualReview rather than continue deferring on shaky state.
    #[tokio::test]
    async fn process_pending_remints_escalates_when_bump_persist_fails() {
        let (mut state, mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 30, 1);
        mock.set_should_fail("bump_pending_remint_finality_attempt", true);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(30),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-30".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(30),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 1,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Entry dropped from in-memory queue, not re-queued.
        assert!(state.pending_remints.is_empty());

        // ManualReview update was sent with the persist error in the message.
        let update = storage_rx
            .try_recv()
            .expect("persist failure must produce a ManualReview update");
        assert_eq!(update.transaction_id, 30);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap();
        assert!(err.contains("counter persist failed"), "got: {err}");
        assert!(err.contains("release_funds failed"), "got: {err}");

        // DB row was not modified by the failed bump.
        let persisted = mock
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == 30)
            .map(|t| t.finality_check_attempts);
        assert_eq!(persisted, Some(1));
    }

    #[tokio::test]
    async fn process_pending_remints_manual_review_after_max_rpc_failures() {
        let (mut state, _mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Push entry already at max attempts — next RPC failure triggers ManualReview
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "max retries".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 2, // MAX_FINALITY_CHECK_ATTEMPTS - 1
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx.try_recv().expect("should receive status update");
        assert_eq!(update.transaction_id, 20);
        assert_eq!(
            update.status,
            TransactionStatus::ManualReview,
            "exhausted finality check retries should produce ManualReview"
        );

        let err = update.error_message.as_deref().unwrap();
        assert!(
            err.contains("escalated to ManualReview"),
            "should mention ManualReview escalation: {err}"
        );
        assert!(
            err.contains("signature status RPC failed"),
            "should mention the underlying failure: {err}"
        );
        assert!(
            err.contains("max retries"),
            "should contain original error: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "should not re-queue after max attempts"
        );
    }

    /// When the pending_remints queue contains both matured entries (deadline
    /// in the past) and immature ones (deadline in the future), only the
    /// matured entries should be processed on a given tick.
    ///
    /// The immature entry must remain in the queue completely unchanged —
    /// same deadline, same attempt count. Processing it early would violate
    /// the finality window guarantee that prevents double-minting.
    #[tokio::test]
    async fn process_pending_remints_handles_mixed_matured_and_immature() {
        let (mut state, mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The matured entry (id 10) defers, which now persists the bump.
        seed_pending_remint_row(&mock, 10, 0);

        let future_deadline = Utc::now() + chrono::Duration::seconds(600);

        // Entry 1: matured — RPC will fail (localhost unreachable), so it
        // gets re-queued with attempt=1. This is the observable side-effect
        // that proves it was processed.
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(10),
                withdrawal_nonce: Some(1),
                trace_id: Some("trace-10".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(10),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        // Entry 2: immature — must not be touched at all.
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(2),
                trace_id: Some("trace-20".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: future_deadline,
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // No status update yet — the matured entry's RPC failed and was re-queued,
        // the immature entry was skipped entirely.
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update expected on first RPC failure"
        );

        // Both entries are still in the queue.
        assert_eq!(state.pending_remints.len(), 2);

        // The matured entry was processed: attempt counter incremented.
        let matured = state
            .pending_remints
            .iter()
            .find(|e| e.ctx.transaction_id == Some(10))
            .expect("matured entry should still be in queue");
        assert_eq!(
            matured.finality_check_attempts, 1,
            "matured entry should have attempt=1 after first RPC failure"
        );

        // The immature entry was not touched: attempt counter and deadline unchanged.
        let immature = state
            .pending_remints
            .iter()
            .find(|e| e.ctx.transaction_id == Some(20))
            .expect("immature entry should still be in queue");
        assert_eq!(
            immature.finality_check_attempts, 0,
            "immature entry must not be processed"
        );
        assert_eq!(
            immature.deadline, future_deadline,
            "immature entry deadline must be unchanged"
        );
    }

    /// The core anti-duplication invariant: if the original withdrawal
    /// transaction reached finality on Solana, the remint must be skipped
    /// and the transaction marked Completed instead.
    ///
    /// Skipping this check would mean reminting tokens that were already
    /// successfully withdrawn — a direct double-credit to the user.
    #[tokio::test]
    async fn process_pending_remints_marks_completed_when_withdrawal_finalized() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _mock = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "jsonrpc": "2.0",
                    "result": {
                        "context": {"slot": 200},
                        "value": [{
                            "slot": 100,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    },
                    "id": 0
                }"#,
            )
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(99),
                withdrawal_nonce: Some(7),
                trace_id: Some("trace-99".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(99),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive Completed status");
        assert_eq!(update.transaction_id, 99);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(sig.to_string().as_str()),
            "counterpart_signature must be the finalized withdrawal sig"
        );
        // The full attempt list rides on the Completed update for durable provenance.
        assert_eq!(
            update.release_signatures,
            Some(vec![sig.to_string()]),
            "release_signatures must carry the broadcast attempt list"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "should send exactly one status update — no remint attempted"
        );
        assert!(
            state.pending_remints.is_empty(),
            "entry should be removed from queue after Completed"
        );
    }

    /// IM1: a Dead-by-absence release whose nonce IS in the fresh on-chain SMT
    /// root is proven landed by the verify gate, so Completed is sent with the
    /// attempt list, and no remint is broadcast.
    #[tokio::test]
    async fn process_pending_remints_smt_verify_landed_marks_completed() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let recorded = Signature::new_unique();
        // Release classified Dead (finalized-failed).
        mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"}]},"id":0}"#,
        )
        .await;
        // Finalized tip 1_000_000 - 150 > max_lvbh 100 so the freshness check passes.
        mock_rpc(
            &mut rpc_server,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1000000},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000000}},"id":0}"#,
        )
        .await;
        // On-chain root includes nonce 3 so the verifier resolves Landed.
        {
            use crate::operator::utils::smt_util::SmtState;
            use base64::{engine::general_purpose::STANDARD, Engine};
            use borsh::BorshSerialize;
            use private_channel_escrow_program_client::Instance;
            let tree_index = 3u64 / MAX_TREE_LEAVES as u64;
            let mut smt = SmtState::new(tree_index);
            smt.insert_nonce(3);
            let instance = Instance {
                discriminator: 0,
                bump: 0,
                version: 0,
                instance_seed: Pubkey::new_unique(),
                admin: Pubkey::new_unique(),
                withdrawal_transactions_root: smt.current_root(),
                current_tree_index: tree_index,
            };
            let mut bytes = Vec::new();
            instance.serialize(&mut bytes).unwrap();
            rpc_server
                .mock("POST", "/")
                .match_body(mockito::Matcher::Regex(
                    r#""method"\s*:\s*"getAccountInfo""#.into(),
                ))
                .with_status(200)
                .with_body(
                    serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {"context": {"slot": 1_000_000u64}, "value": {
                            "owner": Pubkey::new_unique().to_string(),
                            "lamports": 1_000_000u64,
                            "data": [STANDARD.encode(&bytes), "base64"],
                            "executable": false, "rentEpoch": 0
                        }}
                    })
                    .to_string(),
                )
                .create();
        }

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(300),
                withdrawal_nonce: Some(3),
                trace_id: Some("trace-300".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(300),
            signatures: vec![PendingSig {
                signature: recorded,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx.try_recv().expect("Completed expected");
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(recorded.to_string().as_str())
        );
        assert_eq!(update.release_signatures, Some(vec![recorded.to_string()]));
        assert!(
            state.pending_remints.is_empty(),
            "no remint, entry resolved"
        );
    }

    /// IM3: a Dead-by-absence release whose finalized snapshot is too stale to
    /// prove non-inclusion is Uncertain, so the entry defers (no remint broadcast).
    #[tokio::test]
    async fn process_pending_remints_smt_verify_uncertain_defers() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        seed_pending_remint_row(&mock, 301, 0);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"}]},"id":0}"#,
        )
        .await;
        // Finalized tip 250 - 150 = 100 == max_lvbh 100, so freshness fails closed to Uncertain.
        mock_rpc(
            &mut rpc_server,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":500},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":250}},"id":0}"#,
        )
        .await;
        {
            use crate::operator::utils::smt_util::SmtState;
            use base64::{engine::general_purpose::STANDARD, Engine};
            use borsh::BorshSerialize;
            use private_channel_escrow_program_client::Instance;
            let tree_index = 3u64 / MAX_TREE_LEAVES as u64;
            let instance = Instance {
                discriminator: 0,
                bump: 0,
                version: 0,
                instance_seed: Pubkey::new_unique(),
                admin: Pubkey::new_unique(),
                withdrawal_transactions_root: SmtState::new(tree_index).current_root(),
                current_tree_index: tree_index,
            };
            let mut bytes = Vec::new();
            instance.serialize(&mut bytes).unwrap();
            rpc_server
                .mock("POST", "/")
                .match_body(mockito::Matcher::Regex(
                    r#""method"\s*:\s*"getAccountInfo""#.into(),
                ))
                .with_status(200)
                .with_body(
                    serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {"context": {"slot": 100u64}, "value": {
                            "owner": Pubkey::new_unique().to_string(),
                            "lamports": 1_000_000u64,
                            "data": [STANDARD.encode(&bytes), "base64"],
                            "executable": false, "rentEpoch": 0
                        }}
                    })
                    .to_string(),
                )
                .create();
        }
        // A remint broadcast here would be a double-pay bug; forbid it.
        let no_send = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .expect(0)
            .create();

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(301),
                withdrawal_nonce: Some(3),
                trace_id: Some("trace-301".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(301),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Uncertain fails closed: deferred, no terminal status, no remint.
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update on Uncertain"
        );
        assert_eq!(state.pending_remints.len(), 1, "entry re-queued");
        no_send.assert();
    }

    /// The deferred remint queue is a Withdraw-only responsibility. An Escrow
    /// sender must never classify or remint a queued row: doing so would send
    /// RPC on the wrong chain and could escalate the row to ManualReview.
    #[tokio::test]
    async fn process_pending_remints_noop_for_escrow_role() {
        // No endpoints are mounted, so any RPC call would be an observable fault.
        let mut rpc_server = mockito::Server::new_async().await;

        // A catch-all set to expect zero hits: any request fails the assertion.
        let no_calls = rpc_server
            .mock("POST", "/")
            .with_status(200)
            .expect(0)
            .create_async()
            .await;

        let (mut state, _mock) =
            make_sender_state_with_role(&rpc_server.url(), crate::config::ProgramType::Escrow);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(88),
                withdrawal_nonce: Some(4),
                trace_id: Some("trace-88".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(88),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // No classify/send RPC reached the mock server.
        no_calls.assert_async().await;

        // No status update emitted for an Escrow sender.
        assert!(
            storage_rx.try_recv().is_err(),
            "Escrow must not emit any status update from the remint processor"
        );
    }

    /// A withdrawal that ambiguously failed but actually landed must have its
    /// nonce re-inserted into the local SMT. Otherwise later withdrawals in the
    /// same tree fail InvalidSmtProof until restart.
    #[tokio::test]
    async fn process_pending_remints_reinserts_landed_nonce_into_smt() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let landed_nonce: u64 = 7;

        // Withdrawal signature finalized successfully.
        let _status_mock = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                    "slot":100,"confirmations":null,"err":null,
                    "status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":0}"#,
            )
            .create_async()
            .await;

        // Local SMT has forgotten the nonce: the bug's starting state.
        state.smt_state = Some(SenderSMTState {
            smt_state: SmtState::new(0),
            nonce_to_builder: HashMap::new(),
        });
        assert!(!state
            .smt_state
            .as_ref()
            .unwrap()
            .smt_state
            .contains_nonce(landed_nonce));

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(99),
                withdrawal_nonce: Some(landed_nonce),
                trace_id: Some("trace-99".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(99),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            state
                .smt_state
                .as_ref()
                .unwrap()
                .smt_state
                .contains_nonce(landed_nonce),
            "landed nonce must be re-inserted so local tree matches chain"
        );
        assert!(
            state.pending_remints.is_empty(),
            "entry consumed after Completed"
        );
    }

    // ── execute_deferred_remint paths ───────────────────────────────

    /// Fail-closed: when a stored remint attempt cannot be classified (here the
    /// source backend errors on getSignatureStatuses), attempt_remint must refuse
    /// to mint and escalate to ManualReview rather than risk a duplicate remint.
    #[tokio::test]
    async fn execute_deferred_remint_fails_closed_when_idempotency_lookup_unavailable() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // A prior remint attempt is on record, so classification must run before
        // any resend — and the source backend errors, making it unverifiable.
        let unverifiable = Signature::new_unique().to_string();
        mock.remint_signatures.lock().unwrap().insert(
            700,
            vec![StoredSig {
                signature: unverifiable.clone(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
        )
        .await;

        let entry = PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(700),
                withdrawal_nonce: Some(70),
                trace_id: Some("trace-700".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(700),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        };

        let outcome = execute_deferred_remint(&state, entry, &storage_tx).await;
        assert!(matches!(outcome, DeferredRemintOutcome::Resolved));

        let update = storage_rx
            .try_recv()
            .expect("unverifiable classification must emit a status update");
        assert_eq!(update.transaction_id, 700);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        // This string only comes from the fail-closed arm, before any send.
        assert!(
            err.contains("refusing to remint"),
            "must escalate with the fail-closed reason: {err}"
        );
        assert!(
            err.contains("release_funds failed"),
            "must preserve the original withdrawal error: {err}"
        );
        // Uncertain is an RPC failure, not proof of death, so the attempt keeps
        // the live slot and no replacement may be claimed against it.
        assert!(
            !mock
                .superseded_remint_signatures
                .lock()
                .unwrap()
                .contains(&unverifiable),
            "an unclassifiable attempt must never be superseded"
        );
        assert_eq!(
            mock.get_remint_signatures(700).await.unwrap().len(),
            1,
            "no replacement attempt may be claimed on an Uncertain verdict"
        );
    }

    /// The stored-signature lookup is the idempotency gate's first step. A DB
    /// failure there means we can't read whether a prior sig is persisted, so
    /// `attempt_remint` must defer in-flight: retry the lookup, never escalate or
    /// authorize a mint. In-flight defers never bump the finality counter.
    #[tokio::test]
    async fn execute_deferred_remint_defers_when_signature_lookup_fails() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // No resend may be broadcast when the idempotency lookup cannot run.
        let send = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .expect(0)
            .create_async()
            .await;

        mock.set_should_fail("get_remint_signatures", true);

        let entry = PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(730),
                withdrawal_nonce: Some(73),
                trace_id: Some("trace-730".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(730),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        };

        let outcome = execute_deferred_remint(&state, entry, &storage_tx).await;

        // Deferred in-flight, not resolved: the row stays PendingRemint for the
        // next tick and no counter is bumped (a possibly-persisted sig we can't
        // read must never be terminalized on a cap).
        let DeferredRemintOutcome::DeferInFlight(entry, reason) = outcome else {
            panic!("lookup failure must defer in-flight, not resolve");
        };
        assert_eq!(
            entry.finality_check_attempts, 0,
            "in-flight defer must not bump the finality counter"
        );
        assert!(
            reason.contains("stored remint-signature lookup failed"),
            "defer reason must name the lookup failure: {reason}"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a lookup failure must not emit a status update"
        );
        send.assert_async().await;
    }

    /// When the finality check returns null for a withdrawal signature
    /// (transaction was dropped), `execute_deferred_remint` is called. If the
    /// remint cannot even be built (source blockhash RPC unreachable), nothing
    /// was broadcast, so the entry must defer and requeue, never ManualReview.
    #[tokio::test]
    async fn process_pending_remints_not_finalized_remint_build_fails_defers() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter, so the row must exist.
        seed_pending_remint_row(&mock, 77, 0);
        mock_instance_not_landed(&mut rpc_server, 11);

        let sig = Signature::new_unique();

        // Finality check: null means the tx was dropped, proceed to remint.
        let _status_mock = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
            )
            .create_async()
            .await;

        // Block height ahead of the stored lvbh (0) so every sig is treated as
        // expired and the gate falls through to Case 3 (remint).
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        // Covered ledger floor so the absence-Dead is proven, not a prune.
        let _floor_mock = mock_rpc(
            &mut rpc_server,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","result":0,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(77),
                withdrawal_nonce: Some(11),
                trace_id: Some("trace-77".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(77),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "a pre-broadcast build failure must defer, not write ManualReview"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 1,
            "counter must be bumped after the deferral"
        );

        // The row stays PendingRemint so restart recovery can retry the remint.
        let persisted = mock
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == 77)
            .map(|t| t.finality_check_attempts);
        assert_eq!(persisted, Some(1));
    }

    /// A withdrawal that reached finality but failed on-chain (err field is set)
    /// is NOT a successful withdrawal — the user's funds never left the escrow.
    /// The operator must proceed to remint, not mark Completed.
    #[tokio::test]
    async fn process_pending_remints_finalized_with_onchain_error_proceeds_to_remint() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The remint proceeds and defers on the (unmocked) blockhash, which
        // persists the bumped counter, so the row must exist.
        seed_pending_remint_row(&mock, 88, 0);
        mock_instance_not_landed(&mut rpc_server, 12);

        let sig = Signature::new_unique();

        // Finalized-failed: status present with an error. A finalized-failed sig
        // is dead outright, so classification needs no block-height check.
        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{
                "jsonrpc": "2.0",
                "result": {
                    "context": {"slot": 200},
                    "value": [{
                        "slot": 100,
                        "confirmations": null,
                        "err": {"InstructionError": [0, {"Custom": 1}]},
                        "status": {"Err": {"InstructionError": [0, {"Custom": 1}]}},
                        "confirmationStatus": "finalized"
                    }]
                },
                "id": 0
            }"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(88),
                withdrawal_nonce: Some(12),
                trace_id: Some("trace-88".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(88),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "timeout".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Not Completed: the gate reached Case 3 and attempted the remint, which
        // defers on the unmocked blockhash rather than marking the withdrawal done.
        assert!(
            storage_rx.try_recv().is_err(),
            "finalized-with-error must NOT produce Completed — it proceeds to remint"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    /// Regression: when every stored signature already has a status entry, the
    /// liveness decision is already implied (finalized-failed) and no block
    /// height RPC is needed. A transient `getBlockHeight` outage in that
    /// scenario must NOT consume defer attempts.
    #[tokio::test]
    async fn process_pending_remints_skips_block_height_when_all_sigs_classifiable() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter, so the row must exist.
        seed_pending_remint_row(&mock, 89, 0);
        mock_instance_not_landed(&mut rpc_server, 13);

        let sig = Signature::new_unique();

        // Finalized-failed: status present, finalized commitment, error set.
        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{
                "jsonrpc": "2.0",
                "result": {
                    "context": {"slot": 200},
                    "value": [{
                        "slot": 100,
                        "confirmations": null,
                        "err": {"InstructionError": [0, {"Custom": 1}]},
                        "status": {"Err": {"InstructionError": [0, {"Custom": 1}]}},
                        "confirmationStatus": "finalized"
                    }]
                },
                "id": 0
            }"#,
        )
        .await;

        // getBlockHeight must NOT be called: every sig carries a status, so the
        // classification is decided without it. expect(0) enforces the pre-check.
        let block_height_never = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .expect(0)
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(89),
                withdrawal_nonce: Some(13),
                trace_id: Some("trace-89".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(89),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Reached execute_deferred_remint and deferred on the remint build (no
        // blockhash mock), not on a spurious getBlockHeight call.
        assert!(
            storage_rx.try_recv().is_err(),
            "classifiable-Dead then pre-broadcast build failure must defer"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
        block_height_never.assert_async().await;
    }

    /// When a withdrawal was retried and produced multiple signatures, one of the
    /// later retry signatures may reach finality. The operator must identify which
    /// specific signature finalized and record it as the counterpart_signature.
    #[tokio::test]
    async fn process_pending_remints_second_of_two_sigs_finalized_marks_completed() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig1 = Signature::new_unique(); // first attempt — dropped
        let sig2 = Signature::new_unique(); // retry — finalized

        let _mock = rpc_server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "jsonrpc": "2.0",
                    "result": {
                        "context": {"slot": 200},
                        "value": [
                            null,
                            {
                                "slot": 100,
                                "confirmations": null,
                                "err": null,
                                "status": {"Ok": null},
                                "confirmationStatus": "finalized"
                            }
                        ]
                    },
                    "id": 0
                }"#,
            )
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(55),
                withdrawal_nonce: Some(6),
                trace_id: Some("trace-55".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(55),
            signatures: vec![
                PendingSig {
                    signature: sig1,
                    last_valid_block_height: 0,
                    blockhash_slot: None,
                },
                PendingSig {
                    signature: sig2,
                    last_valid_block_height: 0,
                    blockhash_slot: None,
                },
            ],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive Completed status");
        assert_eq!(update.transaction_id, 55);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(sig2.to_string().as_str()),
            "counterpart_signature must be the finalized sig (sig2), not the dropped sig1"
        );
        assert!(
            state.pending_remints.is_empty(),
            "entry consumed after Completed"
        );
    }

    // ── classify_signatures (multi-sig) ─────────────────

    /// Bare RPC client (1 attempt, fast) for direct classifier tests.
    fn make_rpc(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    /// Finalized success after an earlier finalized failure must win (full-list scan, not first-match).
    #[tokio::test]
    async fn classify_signatures_finalized_success_wins_over_earlier_failure() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        let failed = Signature::new_unique();
        let success = Signature::new_unique();

        // value[0] finalized-failed, value[1] finalized-success (positional).
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[
                {"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"},
                {"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}
            ]},"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: failed,
                last_valid_block_height: 0,
                blockhash_slot: None,
            },
            PendingSig {
                signature: success,
                last_valid_block_height: 0,
                blockhash_slot: None,
            },
        ];

        match classify_against(&rpc, &sigs).await {
            SigFinality::Landed(s) => assert_eq!(
                s, success,
                "must return the finalized-success sig, not the failed one"
            ),
            _ => panic!("expected Landed(success sig), got a different verdict"),
        }
    }

    /// Confirmed success behind a finalized failure must stay Live, never Dead.
    #[tokio::test]
    async fn classify_signatures_confirmed_success_after_failure_is_live_not_dead() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        // value[0] finalized-failed, value[1] confirmed-success (in a block,
        // will finalize).
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[
                {"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"},
                {"slot":100,"confirmations":10,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}
            ]},"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            },
        ];

        assert!(
            matches!(classify_against(&rpc, &sigs).await, SigFinality::Live(_)),
            "confirmed success behind a finalized failure must be Live, not Dead"
        );
    }

    /// A still-valid null after an expired null must be Live: nulls are walked fully, not cut at the first.
    #[tokio::test]
    async fn classify_signatures_live_null_after_expired_null_is_live() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null,null]},"id":0}"#,
        )
        .await;
        // current_height 1000: sig[0] lvbh 100 expired, sig[1] lvbh 2000 live.
        let _height = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 100,
                blockhash_slot: None,
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 2000,
                blockhash_slot: None,
            },
        ];

        assert!(
            matches!(classify_against(&rpc, &sigs).await, SigFinality::Live(_)),
            "a still-valid null after an expired null must be Live, not Dead"
        );
    }

    /// A truncated status list (fewer statuses than sigs) must be Uncertain, never read as "missing = dead".
    #[tokio::test]
    async fn classify_signatures_status_length_mismatch_is_uncertain() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        // Two sigs requested, one status returned.
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            },
        ];

        assert!(
            matches!(
                classify_against(&rpc, &sigs).await,
                SigFinality::Uncertain(_)
            ),
            "length mismatch must be Uncertain"
        );
    }

    // ── classify_signatures corroboration wrapper (FinalityRpc) ─────────

    /// The four canonical single-signature getSignatureStatuses response bodies,
    /// so each corroboration test states its endpoint's verdict in one word.
    enum StatusKind {
        FinalizedSuccess,
        ConfirmedLive,
        Null,
    }

    fn status_body(kind: StatusKind) -> &'static str {
        match kind {
            StatusKind::FinalizedSuccess => {
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                    "slot":100,"confirmations":null,"err":null,
                    "status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":0}"#
            }
            StatusKind::ConfirmedLive => {
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                    "slot":100,"confirmations":10,"err":null,
                    "status":{"Ok":null},"confirmationStatus":"confirmed"}]},"id":0}"#
            }
            StatusKind::Null => {
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#
            }
        }
    }

    /// One expired sig: current_height 1000 > lvbh 100, so a null status is Dead.
    fn one_expired_sig() -> Vec<PendingSig> {
        vec![PendingSig {
            signature: Signature::new_unique(),
            last_valid_block_height: 100,
            blockhash_slot: None,
        }]
    }

    /// Register a `getFirstAvailableBlock` reply of `floor` on `server`.
    async fn mock_floor(server: &mut mockito::Server, floor: u64) {
        mock_rpc(
            server,
            "getFirstAvailableBlock",
            &format!(r#"{{"jsonrpc":"2.0","result":{floor},"id":0}}"#),
        )
        .await;
    }

    /// Make the endpoint return null + a block height past validity (absence-Dead)
    /// with a covered ledger floor (0), so the absence resolves to Dead.
    async fn mock_dead(server: &mut mockito::Server) {
        mock_rpc(
            server,
            "getSignatureStatuses",
            status_body(StatusKind::Null),
        )
        .await;
        mock_rpc(
            server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        mock_floor(server, 0).await;
    }

    /// Like `mock_dead` but with a pruned ledger floor above the attempt window,
    /// so the absence is uncovered and must resolve to Uncertain, never Dead.
    async fn mock_dead_pruned(server: &mut mockito::Server, floor: u64) {
        mock_rpc(
            server,
            "getSignatureStatuses",
            status_body(StatusKind::Null),
        )
        .await;
        mock_rpc(
            server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        mock_floor(server, floor).await;
    }

    /// The bug scenario. The primary reports the release gone but the fallback
    /// still has the finalized-success record, so the verdict is Landed, not Dead.
    #[tokio::test]
    async fn primary_dead_fallback_landed_returns_landed() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_dead(&mut primary).await;
        mock_rpc(
            &mut fb,
            "getSignatureStatuses",
            status_body(StatusKind::FinalizedSuccess),
        )
        .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Landed(_)
            ),
            "fallback finalized-success must override the primary's Dead"
        );
    }

    /// Both endpoints agree the signatures are gone and the fallback's floor covers
    /// the attempt window, so Dead stands and the remint is safe.
    #[tokio::test]
    async fn dead_corroborated_by_fallback_covered_stays_dead() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_dead(&mut primary).await;
        mock_dead(&mut fb).await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Dead
            ),
            "both endpoints Dead with a covered fallback floor must stay Dead"
        );
    }

    /// The fallback also sees the signatures gone, but its ledger floor is above the attempt window
    /// (pruned/lagging), so absence on the pruned fallback must be Uncertain, never Dead.
    #[tokio::test]
    async fn dead_by_fallback_uncovered_is_uncertain() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_dead(&mut primary).await;
        mock_dead_pruned(&mut fb, 1000).await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        match classify_signatures(&finality, &one_expired_sig()).await {
            SigFinality::Uncertain(reason) => {
                assert!(
                    reason.contains("fallback ledger floor"),
                    "reason must name the fallback: {reason}"
                );
            }
            _ => panic!("a pruned fallback absence must be Uncertain, not Dead/Live/Landed"),
        }
    }

    /// Coverage is judged on the fallback only: the primary's floor is pruned high and never consulted,
    /// while the fallback's floor covers the window, so the verdict is Dead.
    #[tokio::test]
    async fn primary_pruned_fallback_covered_is_dead() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        // Primary absence with a pruned floor; asserting it is never queried.
        mock_rpc(
            &mut primary,
            "getSignatureStatuses",
            status_body(StatusKind::Null),
        )
        .await;
        mock_rpc(
            &mut primary,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        let primary_floor_untouched = primary
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getFirstAvailableBlock""#.into(),
            ))
            .expect(0)
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":9999,"id":0}"#)
            .create_async()
            .await;
        // Fallback absence with a covered floor.
        mock_dead(&mut fb).await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Dead
            ),
            "coverage on the fallback alone must decide Dead"
        );
        primary_floor_untouched.assert_async().await;
    }

    /// A finalized-failed primary status is definitive non-inclusion: neither a
    /// coverage floor check nor the fallback is consulted.
    #[tokio::test]
    async fn primary_finalized_failure_is_dead_without_coverage_check() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        // Finalized-failed status: positive on-chain failure evidence.
        mock_rpc(
            &mut primary,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":null,
                "err":{"InstructionError":[0,{"Custom":1}]},
                "status":{"Err":{"InstructionError":[0,{"Custom":1}]}},
                "confirmationStatus":"finalized"}]},"id":0}"#,
        )
        .await;
        // No floor mock: reaching getFirstAvailableBlock would 501-fail the test.
        let fb_untouched = fb
            .mock("POST", "/")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Dead
            ),
            "a finalized-failed status must be Dead with no coverage or fallback call"
        );
        fb_untouched.assert_async().await;
    }

    /// The fallback still sees the sig on-chain (confirmed), so defer rather
    /// than remint.
    #[tokio::test]
    async fn primary_dead_fallback_live_defers() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_dead(&mut primary).await;
        mock_rpc(
            &mut fb,
            "getSignatureStatuses",
            status_body(StatusKind::ConfirmedLive),
        )
        .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Live(_)
            ),
            "a live fallback status must defer, not remint"
        );
    }

    /// The fallback is down. Never trust a lone Dead when a fallback was
    /// configured to corroborate it: fail closed to Uncertain.
    #[tokio::test]
    async fn primary_dead_fallback_unavailable_is_uncertain() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_dead(&mut primary).await;
        mock_rpc(
            &mut fb,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
        )
        .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Uncertain(_)
            ),
            "an unavailable fallback must fail closed to Uncertain, never Dead"
        );
    }

    /// Single endpoint, absence-Dead, ledger floor covers the attempt window:
    /// the absence is proven non-inclusion, so Dead stands.
    #[tokio::test]
    async fn dead_by_absence_covered_single_endpoint_is_dead() {
        let mut primary = mockito::Server::new_async().await;
        mock_dead(&mut primary).await;

        let p = make_rpc(&primary.url());
        let finality = FinalityRpc::solana(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Dead
            ),
            "a covered single-endpoint absence must stay Dead"
        );
    }

    /// Single endpoint, absence-Dead, but the ledger floor sits above the attempt window (pruned/lagging).
    /// Absence is no longer proof of non-inclusion, so it must degrade to Uncertain and carry the floor.
    #[tokio::test]
    async fn dead_by_absence_uncovered_single_endpoint_is_uncertain() {
        let mut primary = mockito::Server::new_async().await;
        mock_dead_pruned(&mut primary, 1000).await;

        let p = make_rpc(&primary.url());
        let finality = FinalityRpc::solana(&p, None);
        match classify_signatures(&finality, &one_expired_sig()).await {
            SigFinality::Uncertain(reason) => {
                assert!(reason.contains("ledger floor"), "reason: {reason}");
                assert!(
                    reason.contains("1000"),
                    "reason must carry the floor: {reason}"
                );
            }
            _ => panic!("expected Uncertain on a pruned single endpoint, got Dead/Live/Landed"),
        }
    }

    /// Single endpoint, absence-Dead, but the floor RPC itself fails. We cannot
    /// prove coverage, so fail closed to Uncertain rather than trust the absence.
    #[tokio::test]
    async fn dead_by_absence_floor_rpc_error_single_endpoint_is_uncertain() {
        let mut primary = mockito::Server::new_async().await;
        mock_rpc(
            &mut primary,
            "getSignatureStatuses",
            status_body(StatusKind::Null),
        )
        .await;
        mock_rpc(
            &mut primary,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        mock_rpc(
            &mut primary,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
        )
        .await;

        let p = make_rpc(&primary.url());
        let finality = FinalityRpc::solana(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &one_expired_sig()).await,
                SigFinality::Uncertain(_)
            ),
            "a floor-RPC failure must fail closed to Uncertain, never Dead"
        );
    }

    /// Positive evidence is unforgeable and needs no second opinion, so a
    /// Landed primary must not query the fallback at all.
    #[tokio::test]
    async fn landed_on_primary_skips_fallback() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_rpc(
            &mut primary,
            "getSignatureStatuses",
            status_body(StatusKind::FinalizedSuccess),
        )
        .await;
        let fb_untouched = fb
            .mock("POST", "/")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(matches!(
            classify_signatures(&finality, &one_expired_sig()).await,
            SigFinality::Landed(_)
        ));
        fb_untouched.assert_async().await;
    }

    /// A live primary defers without consulting the fallback.
    #[tokio::test]
    async fn live_on_primary_skips_fallback() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_rpc(
            &mut primary,
            "getSignatureStatuses",
            status_body(StatusKind::ConfirmedLive),
        )
        .await;
        let fb_untouched = fb
            .mock("POST", "/")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(matches!(
            classify_signatures(&finality, &one_expired_sig()).await,
            SigFinality::Live(_)
        ));
        fb_untouched.assert_async().await;
    }

    /// A primary error is already fail-closed; the fallback is not consulted
    /// to rescue an Uncertain (only the unsafe Dead verdict is corroborated).
    #[tokio::test]
    async fn uncertain_on_primary_skips_fallback() {
        let mut primary = mockito::Server::new_async().await;
        let mut fb = mockito::Server::new_async().await;
        mock_rpc(
            &mut primary,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
        )
        .await;
        let fb_untouched = fb
            .mock("POST", "/")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;

        let p = make_rpc(&primary.url());
        let f = make_rpc(&fb.url());
        let finality = FinalityRpc::solana(&p, Some(&f));
        assert!(matches!(
            classify_signatures(&finality, &one_expired_sig()).await,
            SigFinality::Uncertain(_)
        ));
        fb_untouched.assert_async().await;
    }

    // ── liveness gate paths ────────────────────────────────────────────

    /// Sig has no on-chain record AND its blockhash is past validity. Dead.
    /// The gate must proceed to remint.
    #[tokio::test]
    async fn process_pending_remints_all_sigs_expired_proceeds_to_remint() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter, so the row must exist.
        seed_pending_remint_row(&mock, 100, 0);
        mock_instance_not_landed(&mut rpc_server, 20);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // current_height (1000) > lvbh (100): sig is expired.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        // Covered ledger floor so the absence-Dead is proven, not a prune.
        let _floor_mock = mock_rpc(
            &mut rpc_server,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","result":0,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(100),
                withdrawal_nonce: Some(20),
                trace_id: Some("trace-100".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(100),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Reaching Case 3 triggers execute_deferred_remint; the remint build has
        // no blockhash mock, so it fails pre-broadcast and the entry defers.
        assert!(
            storage_rx.try_recv().is_err(),
            "pre-broadcast remint failure must defer, not ManualReview"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    /// Sig has no on-chain record but its blockhash is still within validity.
    /// Could still land. The gate must defer (no remint, no status update)
    /// and bump the counter.
    #[tokio::test]
    async fn process_pending_remints_one_sig_still_live_defers() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 101, 0);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // current_height (50) <= lvbh (1000): sig still within validity.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":50,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(101),
                withdrawal_nonce: Some(21),
                trace_id: Some("trace-101".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(101),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1000,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: row must stay PendingRemint while the broadcast could still land"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 1,
            "counter must be bumped after a liveness deferral"
        );
    }

    /// Entry already at the deferral cap on the liveness branch must escalate
    /// to ManualReview, and the error message must identify the cause as the
    /// liveness check (not an RPC failure).
    #[tokio::test]
    async fn process_pending_remints_live_sig_at_cap_escalates_to_manual_review() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // Sig still live: lvbh (1000) > current_height (50).
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":50,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(102),
                withdrawal_nonce: Some(22),
                trace_id: Some("trace-102".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(102),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1000,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 2, // one more attempt hits the cap
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview at the cap");
        assert_eq!(update.transaction_id, 102);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("signatures still within blockhash validity"),
            "escalation message must identify the liveness cause: {err}"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// getBlockHeight RPC fails. The gate cannot evaluate liveness, so it
    /// must defer (not remint blindly). Same shape as the existing
    /// sig-status RPC failure handling.
    #[tokio::test]
    async fn process_pending_remints_block_height_rpc_failure_defers() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 103, 0);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // getBlockHeight returns an RPC-level error.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"server error"},"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(103),
                withdrawal_nonce: Some(23),
                trace_id: Some("trace-103".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(103),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: RPC failure under cap just defers the entry"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    /// Sig is already on-chain at `confirmed` (in a block, awaiting
    /// finalization) but its blockhash has expired. The tx will finalize
    /// regardless of blockhash validity, so the gate must defer rather than
    /// remint. Reminting here would cause a double-payout once the tx
    /// finalizes a few slots later.
    #[tokio::test]
    async fn process_pending_remints_confirmed_not_finalized_past_lvbh_defers() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 105, 0);

        let sig = Signature::new_unique();

        // Status: confirmed (in a block) but not yet finalized, no error.
        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{
                "jsonrpc": "2.0",
                "result": {
                    "context": {"slot": 200},
                    "value": [{
                        "slot": 100,
                        "confirmations": 1,
                        "err": null,
                        "status": {"Ok": null},
                        "confirmationStatus": "confirmed"
                    }]
                },
                "id": 0
            }"#,
        )
        .await;

        // current_height (1000) > lvbh (100): blockhash validity has passed.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(105),
                withdrawal_nonce: Some(25),
                trace_id: Some("trace-105".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(105),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: a confirmed-but-not-finalized sig must defer the remint"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    // ── write-ahead remint signature classification (classify #2) ────

    /// Finalized-failed release on the dest server so the gate proceeds to remint.
    /// Also registers the SMT-verify NotLanded read the Dead branch now performs
    /// before reminting; `nonce` must match the entry so the tree-window check passes.
    async fn mock_release_dead(server: &mut mockito::Server, nonce: u64) -> mockito::Mock {
        mock_instance_not_landed(server, nonce);
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                    "slot":100,"confirmations":null,
                    "err":{"InstructionError":[0,{"Custom":1}]},
                    "status":{"Err":{"InstructionError":[0,{"Custom":1}]}},
                    "confirmationStatus":"finalized"}]},"id":0}"#,
            )
            .create_async()
            .await
    }

    /// A stored remint attempt still in flight (confirmed, not finalized) must
    /// defer, never broadcast a duplicate while the prior one may still land.
    #[tokio::test]
    async fn remint_defers_when_stored_attempt_still_live() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        // Release is dead, so the gate reaches the remint step.
        let _dest_status = mock_release_dead(&mut dest, 9).await;

        // The stored attempt is on-chain but not yet finalized (still live).
        let _src_status = mock_rpc(
            &mut source,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":10,"err":null,
                "status":{"Ok":null},"confirmationStatus":"confirmed"}]},"id":0}"#,
        )
        .await;
        // Must not broadcast: expect(0).
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect(0)
            .create_async()
            .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        seed_pending_remint_row(&mock, 901, 0);
        // A prior attempt is on record, so classification runs before any resend.
        let live_attempt = Signature::new_unique().to_string();
        mock.remint_signatures.lock().unwrap().insert(
            901,
            vec![StoredSig {
                signature: live_attempt.clone(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(901),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-901".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(901),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Deferred in-flight: re-queued, no terminal status, and the counter is
        // NOT bumped (a live sig must never be terminalized on the cap).
        assert!(
            storage_rx.try_recv().is_err(),
            "a live prior attempt must defer, not resolve"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 0,
            "in-flight defer must not consume the finality budget"
        );
        // The write-ahead sig must survive: reclassification depends on it.
        assert!(
            mock.remint_signatures
                .lock()
                .unwrap()
                .get(&901)
                .is_some_and(|sigs| !sigs.is_empty()),
            "the persisted remint signature must not be deleted"
        );
        // A live attempt still owns the claim; superseding it would let a second
        // MintTo be broadcast alongside one that can still land.
        assert!(
            !mock
                .superseded_remint_signatures
                .lock()
                .unwrap()
                .contains(&live_attempt),
            "a live attempt must never be superseded"
        );
        src_send.assert_async().await;
    }

    /// An in-flight remint whose stored signature is still live must never be
    /// escalated to ManualReview, even at the finality-check cap. It stays queued
    /// with its persisted signature intact so reclassification can resolve it.
    #[tokio::test]
    async fn remint_at_cap_with_live_stored_attempt_requeues_not_manual_review() {
        ensure_test_signer();
        let txn_id = 903;
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        // Release is dead, so the gate reaches the remint step.
        let _dest_status = mock_release_dead(&mut dest, 9).await;

        // The stored attempt is on-chain but not yet finalized (still live).
        let _src_status = mock_rpc(
            &mut source,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":10,"err":null,
                "status":{"Ok":null},"confirmationStatus":"confirmed"}]},"id":0}"#,
        )
        .await;
        // Must not broadcast a duplicate while the prior one may still land.
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .expect(0)
            .create_async()
            .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        let at_cap = MAX_FINALITY_CHECK_ATTEMPTS - 1;
        seed_pending_remint_row(&mock, txn_id, at_cap as i32);
        mock.remint_signatures.lock().unwrap().insert(
            txn_id,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(txn_id),
                withdrawal_nonce: Some(9),
                trace_id: Some(format!("trace-{txn_id}")),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(txn_id),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            // One below the cap: a bounded defer would escalate on this tick.
            finality_check_attempts: at_cap,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // No ManualReview (no status at all): a live sig is never terminalized.
        assert!(
            storage_rx.try_recv().is_err(),
            "a live sig at the cap must not be escalated to ManualReview"
        );
        // Still queued, counter unchanged: it keeps reclassifying.
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, at_cap);
        // The persisted sig survives so reclassification can resolve it.
        assert!(
            mock.remint_signatures
                .lock()
                .unwrap()
                .get(&txn_id)
                .is_some_and(|sigs| !sigs.is_empty()),
            "the persisted remint signature must not be deleted"
        );
        src_send.assert_async().await;
    }

    /// A pre-send persist failure means nothing was broadcast, so the entry must
    /// defer and retry next tick, not escalate.
    #[tokio::test]
    async fn remint_defers_when_pre_send_persist_fails() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        // Release is dead, so the gate reaches the remint step.
        let _dest_status = mock_release_dead(&mut dest, 9).await;

        // Blockhash present so build_and_sign succeeds and we reach the persist step.
        let _src_bh = mock_rpc(
            &mut source,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        // Must not broadcast: expect(0).
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect(0)
            .create_async()
            .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        seed_pending_remint_row(&mock, 902, 0);
        // The write-ahead persist fails.
        mock.set_should_fail("claim_remint_attempt", true);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(902),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-902".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(902),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Deferred: re-queued, no terminal status, and nothing was broadcast.
        assert!(
            storage_rx.try_recv().is_err(),
            "persist failure before send must defer, not emit a status"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
        src_send.assert_async().await;
    }

    /// Losing the pre-send claim proves a second sender is running. The loser must
    /// not broadcast and must emit no status at all: a ManualReview here would move
    /// the row off pending_remint and permanently block the winner's remint record,
    /// stranding a mint that did land. It re-queues uncapped instead.
    #[tokio::test]
    async fn remint_claim_lost_emits_no_status_and_never_broadcasts() {
        ensure_test_signer();
        let txn_id = 904;
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        // Release is dead, so the gate reaches the remint step.
        let _dest_status = mock_release_dead(&mut dest, 9).await;

        // Blockhash present so build_and_sign succeeds and we reach the claim.
        let _src_bh = mock_rpc(
            &mut source,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        // The whole point: the loser must not put a second MintTo on the wire.
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .expect(0)
            .create_async()
            .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        seed_pending_remint_row(&mock, txn_id, 0);
        // The other sender already owns the live attempt for this transaction.
        mock.foreign_remint_claims.lock().unwrap().insert(txn_id);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let metric = crate::metrics::OPERATOR_REMINT_CLAIM_LOST
            .with_label_values(&[state.program_type.as_label()]);
        let before_metric = metric.get();

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(txn_id),
                withdrawal_nonce: Some(9),
                trace_id: Some(format!("trace-{txn_id}")),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(txn_id),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "a lost claim must emit no status update of any kind"
        );
        // Re-queued uncapped: the winner's signature resolves it on a later tick.
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 0,
            "a lost claim must not consume the finality budget"
        );
        assert_eq!(
            metric.get(),
            before_metric + 1.0,
            "a lost claim must increment the claim-lost counter"
        );
        // The loser wrote nothing, so the winner's attempt stays the only one.
        assert!(mock.get_remint_signatures(txn_id).await.unwrap().is_empty());
        src_send.assert_async().await;
    }

    /// Send succeeds but the remint never reaches confirmed within the poll
    /// budget. The signature is durable, so the gate defers (reclassify next
    /// tick) rather than escalating.
    #[tokio::test]
    async fn remint_defers_when_sent_but_not_confirmed() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        // Release is dead, so the gate reaches the remint step.
        let _dest_status = mock_release_dead(&mut dest, 9).await;

        // No stored attempt, so we sign and broadcast.
        let _src_bh = mock_rpc(
            &mut source,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect_at_least(1)
            .create_async()
            .await;
        // Confirmation never sees the sig: unconfirmed across the whole poll budget.
        let _src_status = mock_rpc(
            &mut source,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":[null]},"id":0}"#,
        )
        .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        seed_pending_remint_row(&mock, 904, 0);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(904),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-904".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(904),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Broadcast happened, so the sig is durable and in-flight: defer without
        // bumping the counter (reclassify next tick), don't escalate.
        assert!(
            storage_rx.try_recv().is_err(),
            "an unconfirmed remint must defer, not emit a status"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 0,
            "in-flight defer must not consume the finality budget"
        );
        src_send.assert_async().await;
    }

    /// End-to-end restart: a PendingRemint row is rehydrated from the DB, and
    /// its write-ahead remint signature already landed, so the gate confirms it
    /// without broadcasting a duplicate. This is the core anti-double-mint
    /// guarantee, exercised across the full recover → classify → resolve chain.
    #[tokio::test]
    async fn restart_recovery_skips_resend_when_stored_attempt_landed() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;

        let release_sig = Signature::new_unique();
        let landed_remint = Signature::new_unique();

        // Release classifies dead, so the gate reaches the remint step.
        let _dest_status = mock_release_dead(&mut dest, 905).await;
        // The stored remint attempt finalized on the source chain.
        let _src_status = mock_rpc(
            &mut source,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":null,"err":null,
                "status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":0}"#,
        )
        .await;
        // Must not broadcast a duplicate: expect(0).
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect(0)
            .create_async()
            .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        // A crashed-mid-flight row on disk: PendingRemint carrying its release
        // signature, plus a write-ahead remint signature recorded before the crash.
        seed_pending_remint_row(&mock, 905, 0);
        {
            let mut rows = mock.pending_remint_transactions.lock().unwrap();
            let row = rows.iter_mut().find(|t| t.id == 905).unwrap();
            row.remint_signatures = Some(vec![release_sig.to_string()]);
            row.remint_last_valid_block_heights = Some(vec![0]);
            row.pending_remint_deadline_at = Some(Utc::now() - chrono::Duration::seconds(1));
        }
        mock.remint_signatures.lock().unwrap().insert(
            905,
            vec![StoredSig {
                signature: landed_remint.to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Restart: rehydrate the queue from the DB row.
        state
            .recover_pending_remints(&storage_tx)
            .await
            .expect("recovery must succeed");
        assert_eq!(
            state.pending_remints.len(),
            1,
            "the PendingRemint row must rehydrate into the queue"
        );

        // Tick: classify release (dead) → remint gate → stored attempt already landed.
        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx.try_recv().expect("should report FailedReminted");
        assert_eq!(update.transaction_id, 905);
        assert_eq!(update.status, TransactionStatus::FailedReminted);
        assert_eq!(
            update.remint_signature.as_deref(),
            Some(landed_remint.to_string().as_str()),
            "must report the already-landed signature, not a fresh one"
        );
        // expect(0): recovery did not trigger a duplicate broadcast.
        src_send.assert_async().await;
    }

    /// If recording the confirmed remint fails, the write-ahead rows must be
    /// kept (not cleaned up): the row is still PendingRemint, so a crash before
    /// the async writer commits would otherwise leave recovery with an empty sig
    /// table and re-broadcast a duplicate mint.
    #[tokio::test]
    async fn remint_keeps_write_ahead_rows_when_record_result_fails() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        let _dest_status = mock_release_dead(&mut dest, 9).await;

        // Stored attempt already finalized, so the gate confirms via short-circuit.
        let landed_sig = Signature::new_unique();
        let _src_status = mock_rpc(
            &mut source,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":null,"err":null,
                "status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":0}"#,
        )
        .await;

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        seed_pending_remint_row(&mock, 906, 0);
        mock.remint_signatures.lock().unwrap().insert(
            906,
            vec![StoredSig {
                signature: landed_sig.to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        // The durable terminal write fails.
        mock.set_should_fail("record_remint_result", true);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(906),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-906".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(906),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // The write-ahead rows survive so restart recovery can still classify the
        // landed signature instead of broadcasting a duplicate.
        assert_eq!(
            mock.remint_signatures.lock().unwrap().get(&906),
            Some(&vec![StoredSig {
                signature: landed_sig.to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None
            }]),
            "write-ahead rows must be kept when the terminal record write fails"
        );
    }

    /// End-to-end remint gate. The primary reports the release gone but the
    /// fallback finds it landed, so the withdrawal is Completed with no MintTo.
    #[tokio::test]
    async fn pending_remint_skips_remint_when_fallback_finds_release_landed() {
        ensure_test_signer();
        let mut dest = mockito::Server::new_async().await;
        let mut dest_fb = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;

        let release_sig = Signature::new_unique();

        // Destination primary: pruned, returns null for the release sig.
        mock_rpc(
            &mut dest,
            "getSignatureStatuses",
            status_body(StatusKind::Null),
        )
        .await;
        // Block height past the stored lvbh so the null sig is treated as expired.
        mock_rpc(
            &mut dest,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        // Destination fallback (archival): still has the finalized-success record.
        mock_rpc(
            &mut dest_fb,
            "getSignatureStatuses",
            status_body(StatusKind::FinalizedSuccess),
        )
        .await;

        // The compensating MintTo must never be broadcast on the source chain.
        let src_send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect(0)
            .create_async()
            .await;

        let (mut state, _mock) =
            make_sender_state_split_rpc(&dest.url(), &source.url(), Some(&dest_fb.url()));
        // Dead branch SMT-verifies on the dest primary before reminting.
        let _dest_account = mock_instance_not_landed(&mut dest, 15);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(1500),
                withdrawal_nonce: Some(15),
                trace_id: Some("trace-1500".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(1500),
            signatures: vec![PendingSig {
                signature: release_sig,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("fallback-found landed release must emit a Completed status");
        assert_eq!(update.transaction_id, 1500);
        assert_eq!(
            update.status,
            TransactionStatus::Completed,
            "a release the fallback proves landed must mark Completed, not remint"
        );
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(release_sig.to_string().as_str()),
            "counterpart must be the landed release signature"
        );
        assert!(state.pending_remints.is_empty());
        // No compensating MintTo was broadcast.
        src_send.assert_async().await;
    }

    // ── absence classification against block height ─────────────────

    /// A null status. Its context slot sits far above every height these tests
    /// use, so a verdict that reads the slot instead of the height shows up as a
    /// wrong answer rather than a coincidence.
    fn null_status() -> String {
        r#"{"jsonrpc":"2.0","result":{"context":{"slot":9000},"value":[null]},"id":0}"#.to_string()
    }

    /// Register a `getBlockHeight` reply of `height` on `server`.
    async fn mock_height(server: &mut mockito::Server, height: u64) {
        mock_rpc(
            server,
            "getBlockHeight",
            &format!(r#"{{"jsonrpc":"2.0","result":{height},"id":0}}"#),
        )
        .await;
    }

    fn sig_with_lvbh(lvbh: u64) -> Vec<PendingSig> {
        vec![PendingSig {
            signature: Signature::new_unique(),
            last_valid_block_height: lvbh,
            blockhash_slot: None,
        }]
    }

    /// One absent signature whose broadcast blockhash slot was journaled.
    fn sig_with_slot(lvbh: u64, blockhash_slot: u64) -> Vec<PendingSig> {
        vec![PendingSig {
            signature: Signature::new_unique(),
            last_valid_block_height: lvbh,
            blockhash_slot: Some(blockhash_slot),
        }]
    }

    /// A finalized `getLatestBlockhash` whose `lvbh - context_slot` is `window`,
    /// which is exactly the endpoint's `max_blockhashes`.
    async fn mock_window(server: &mut mockito::Server, window: u64) {
        let slot = 5_000u64;
        mock_rpc(
            server,
            "getLatestBlockhash",
            &format!(
                r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":{slot}}},"value":{{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":{}}}}},"id":0}}"#,
                slot + window
            ),
        )
        .await;
    }

    /// An absence past `getBlockHeight` with a covered floor is Dead.
    #[tokio::test]
    async fn channel_absence_uses_block_height() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 0).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sig_with_slot(1000, 900)).await,
                SigFinality::Dead
            ),
            "a block height past lvbh with a covered floor must resolve the absence"
        );
    }

    /// An idle node ticks slots while producing far fewer blocks, so a status
    /// response's context slot runs past a still-valid `lastValidBlockHeight` within
    /// seconds. Judging an absence against it abandons live withdrawal broadcasts.
    #[tokio::test]
    async fn operator_does_not_declare_a_live_broadcast_dead() {
        let mut server = mockito::Server::new_async().await;
        // The context slot is far past lvbh 1000; the block height 900 is not.
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 900).await;
        mock_floor(&mut server, 0).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sig_with_slot(1000, 900)).await,
                SigFinality::Live(_)
            ),
            "a broadcast still inside its block-height validity must stay Live"
        );
    }

    #[tokio::test]
    async fn channel_absence_calls_block_height() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_floor(&mut server, 0).await;
        let height = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":2000,"id":0}"#)
            .expect_at_least(1)
            .create_async()
            .await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        let _ = classify_signatures(&finality, &sig_with_lvbh(1000)).await;
        height.assert_async().await;
    }

    /// Boundary: `height == lvbh` is still within validity, matching the strict
    /// `>` the expiry check uses.
    #[tokio::test]
    async fn channel_absence_within_validity_is_live() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 1000).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(matches!(
            classify_signatures(&finality, &sig_with_lvbh(1000)).await,
            SigFinality::Live(_)
        ));
    }

    /// The Solana constructor reads the height the same way.
    #[tokio::test]
    async fn solana_absence_calls_block_height() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_floor(&mut server, 0).await;
        let height = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"jsonrpc":"2.0","result":1000,"id":0}"#)
            .expect_at_least(1)
            .create_async()
            .await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::solana(&p, None);
        let _ = classify_signatures(&finality, &one_expired_sig()).await;
        height.assert_async().await;
    }

    /// The core-side seam: a channel node now reports an internal failure as a
    /// JSON-RPC error rather than a null, and that must fail closed.
    #[tokio::test]
    async fn channel_status_rpc_error_is_uncertain() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(
            &mut server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"Failed to get transaction status"},"id":0}"#,
        )
        .await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(matches!(
            classify_signatures(&finality, &sig_with_lvbh(100)).await,
            SigFinality::Uncertain(_)
        ));
    }

    /// The retention proof survives the context-slot rewrite: a pruned floor still
    /// degrades a channel absence to Uncertain.
    #[tokio::test]
    async fn channel_absence_pruned_floor_is_uncertain() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 1000).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        match classify_signatures(&finality, &sig_with_slot(100, 50)).await {
            SigFinality::Uncertain(reason) => {
                assert!(reason.contains("ledger floor"), "reason: {reason}")
            }
            _ => panic!("a pruned channel absence must be Uncertain"),
        }
    }

    // ── legacy attempts with no journaled slot ──────────────────────

    /// The channel's `max_blockhashes` is operator-tunable, so an attempt journaled
    /// before the slot column existed has no reconstructable bound: a reduction since
    /// broadcast would make `lvbh - window` claim more coverage than the node has.
    /// No window value may turn such an attempt into a `Dead`.
    #[tokio::test]
    async fn channel_absence_without_a_journaled_slot_is_uncertain() {
        // A floor of 0 retains everything, so only the missing bound can hold it back.
        for window in [150u64, 600u64] {
            let mut server = mockito::Server::new_async().await;
            mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
            mock_height(&mut server, 2000).await;
            mock_floor(&mut server, 0).await;
            mock_window(&mut server, window).await;

            let p = make_rpc(&server.url());
            let finality = FinalityRpc::channel(&p, None);
            match classify_signatures(&finality, &sig_with_lvbh(1000)).await {
                SigFinality::Uncertain(reason) => assert!(
                    reason.contains("predates the journaled blockhash slot"),
                    "reason: {reason}"
                ),
                _ => panic!("a legacy channel attempt must be Uncertain"),
            }
        }
    }

    /// Solana's window is `MAX_PROCESSING_AGE`, fixed by the protocol and beyond any
    /// operator's reach, so a legacy attempt there keeps a sound `lvbh - window` bound.
    #[tokio::test]
    async fn solana_absence_without_a_journaled_slot_still_resolves() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 0).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::solana(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sig_with_lvbh(1000)).await,
                SigFinality::Dead
            ),
            "a protocol-fixed window keeps the legacy bound sound"
        );
    }

    /// Solana's window is protocol-fixed, so its coverage proof must not spend an
    /// RPC reading one.
    #[tokio::test]
    async fn solana_coverage_reads_no_window() {
        let mut server = mockito::Server::new_async().await;
        mock_floor(&mut server, 500).await;
        let never = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .expect(0)
            .create_async()
            .await;

        let rpc = make_rpc(&server.url());
        assert!(matches!(
            solana_coverage(&rpc, 1000).await,
            SigFinality::Dead
        ));
        never.assert_async().await;
    }

    // ── coverage_verdict boundaries ─────────────────────────────────

    /// Coverage check on the Solana window against a single endpoint.
    async fn solana_coverage(rpc: &RpcClientWithRetry, min_lvbh: u64) -> SigFinality {
        coverage_verdict(&FinalityRpc::solana(rpc, None), rpc, min_lvbh, None, "").await
    }

    /// Register a single `getFirstAvailableBlock` reply and return a fast client.
    async fn floor_client(floor: u64) -> (mockito::ServerGuard, RpcClientWithRetry) {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(
            &mut server,
            "getFirstAvailableBlock",
            &format!(r#"{{"jsonrpc":"2.0","result":{floor},"id":0}}"#),
        )
        .await;
        let client = make_rpc(&server.url());
        (server, client)
    }

    #[tokio::test]
    async fn covered_when_floor_below_bound() {
        // lvbh 1000 -> bound 850; floor 500 <= 850.
        let (_s, rpc) = floor_client(500).await;
        assert!(matches!(
            solana_coverage(&rpc, 1000).await,
            SigFinality::Dead
        ));
    }

    #[tokio::test]
    async fn boundary_floor_equals_bound_is_covered() {
        // lvbh 1000 -> bound 850; floor 850 == 850 is covered (inclusive).
        let (_s, rpc) = floor_client(850).await;
        assert!(matches!(
            solana_coverage(&rpc, 1000).await,
            SigFinality::Dead
        ));
    }

    #[tokio::test]
    async fn boundary_floor_one_above_is_uncovered() {
        // lvbh 1000 -> bound 850; floor 851 > 850 is uncovered.
        let (_s, rpc) = floor_client(851).await;
        assert!(matches!(
            solana_coverage(&rpc, 1000).await,
            SigFinality::Uncertain(_)
        ));
    }

    #[tokio::test]
    async fn underflow_low_lvbh_requires_floor_zero() {
        // lvbh 100 -> saturating bound 0. Only a floor of 0 covers it.
        let (_s1, rpc1) = floor_client(1).await;
        assert!(matches!(
            solana_coverage(&rpc1, 100).await,
            SigFinality::Uncertain(_)
        ));
        let (_s0, rpc0) = floor_client(0).await;
        assert!(matches!(
            solana_coverage(&rpc0, 100).await,
            SigFinality::Dead
        ));
    }

    #[tokio::test]
    async fn floor_rpc_error_is_uncertain() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(
            &mut server,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"node unavailable"},"id":0}"#,
        )
        .await;
        let rpc = make_rpc(&server.url());
        assert!(matches!(
            solana_coverage(&rpc, 1000).await,
            SigFinality::Uncertain(_)
        ));
    }

    // ── coverage-gated absence ──────────────────────────────────────

    /// The release signatures are expired but the destination endpoint's ledger
    /// floor is pruned above the attempt window, so absence is not proof of
    /// non-inclusion. The gate must defer (no remint, no status write) and bump.
    #[tokio::test]
    async fn process_pending_remints_expired_but_pruned_dest_defers() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 110, 0);

        let sig = Signature::new_unique();
        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        // Pruned floor: above the lvbh(100)-150 bound (0), so absence is uncovered.
        let _floor_mock = mock_rpc(
            &mut rpc_server,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(110),
                withdrawal_nonce: Some(30),
                trace_id: Some("trace-110".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(110),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "a pruned dest absence must defer, not remint or write a status"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    /// A prior remint signature exists but the source endpoint's ledger floor is
    /// pruned above the attempt window, so its absence cannot prove non-inclusion.
    /// attempt_remint must refuse to resend and escalate to ManualReview, never
    /// broadcast a duplicate MintTo.
    #[tokio::test]
    async fn attempt_remint_source_pruned_refuses_and_escalates() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // A prior remint attempt is on record, so classification runs before resend.
        mock.remint_signatures.lock().unwrap().insert(
            710,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        // Source is the channel: the context slot (200) is the block height and is
        // already past the stored attempt's lvbh (0), so no getBlockHeight is needed.
        mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;
        // Pruned floor: the source cannot prove the prior attempt did not land.
        mock_rpc(
            &mut rpc_server,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;
        // A resend must never be broadcast.
        let send = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect(0)
            .create_async()
            .await;

        let entry = PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(710),
                withdrawal_nonce: Some(71),
                trace_id: Some("trace-710".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(710),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        };

        let outcome = execute_deferred_remint(&state, entry, &storage_tx).await;
        assert!(matches!(outcome, DeferredRemintOutcome::Resolved));
        let update = storage_rx
            .try_recv()
            .expect("a pruned source must escalate to a status update");
        assert_eq!(update.transaction_id, 710);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("refusing to remint"),
            "must fail closed: {err}"
        );
        send.assert_async().await;
    }

    /// The mirror of the pruned case: a prior remint signature exists and the
    /// source floor covers the attempt window, so the absence is proven Dead and
    /// the idempotency gate allows the resend to proceed.
    #[tokio::test]
    async fn attempt_remint_source_covered_dead_resends() {
        ensure_test_signer();
        let dest = mockito::Server::new_async().await;
        let mut source = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url(), None);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let dead_attempt = Signature::new_unique().to_string();
        mock.remint_signatures.lock().unwrap().insert(
            720,
            vec![StoredSig {
                signature: dead_attempt.clone(),
                last_valid_block_height: 0,
                blockhash_slot: Some(0),
            }],
        );
        // Source is the channel: expired absence (block height 200 past lvbh 0)
        // with a covered floor, so classification is Dead.
        mock_rpc(
            &mut source,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;
        mock_height(&mut source, 200).await;
        mock_rpc(
            &mut source,
            "getFirstAvailableBlock",
            r#"{"jsonrpc":"2.0","result":0,"id":0}"#,
        )
        .await;
        // The resend build needs a blockhash, then broadcasts a fresh MintTo.
        mock_rpc(
            &mut source,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        let send = source
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":"{}","id":0}}"#,
                Signature::new_unique()
            ))
            .expect_at_least(1)
            .create_async()
            .await;

        let entry = PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(720),
                withdrawal_nonce: Some(72),
                trace_id: Some("trace-720".to_string()),
                deposit_claim_lease: None,
            },
            remint_info: make_remint_info(720),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        };

        // Covered Dead classification lets the resend broadcast (confirmation then
        // stays unconfirmed here, so the entry defers; the broadcast is the point).
        let _ = execute_deferred_remint(&state, entry, &storage_tx).await;
        send.assert_async().await;

        // The proven-dead attempt is retired, not deleted: it stays classifiable
        // in case it lands late, and the fresh attempt now owns the claim.
        assert!(mock
            .superseded_remint_signatures
            .lock()
            .unwrap()
            .contains(&dead_attempt));
        let stored = mock.get_remint_signatures(720).await.unwrap();
        assert_eq!(
            stored.len(),
            2,
            "history kept plus the new claim: {stored:?}"
        );
        assert_eq!(stored[0].signature, dead_attempt);
    }

    // ── per-attempt blockhash slot ──────────────────────────────────

    /// The journaled slot is the exact earliest block the signature could be in,
    /// so it decides the proof outright: same floor, opposite verdicts.
    #[tokio::test]
    async fn journaled_slot_decides_the_coverage_verdict() {
        for (slot, expect_dead) in [(900u64, true), (400u64, false)] {
            let mut server = mockito::Server::new_async().await;
            mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
            mock_height(&mut server, 2000).await;
            mock_floor(&mut server, 800).await;

            let p = make_rpc(&server.url());
            let finality = FinalityRpc::channel(&p, None);
            let verdict = classify_signatures(&finality, &sig_with_slot(1000, slot)).await;
            assert_eq!(
                matches!(verdict, SigFinality::Dead),
                expect_dead,
                "journaled slot {slot} must bound the retention proof"
            );
        }
    }

    /// The whole point of journaling the slot: a node narrowed after the attempt
    /// was broadcast cannot shrink that attempt's bound. The window says 150,
    /// which would put the bound at 850 and pass the floor of 800, but the
    /// attempt was actually signed at slot 400 and could be in a pruned block.
    #[tokio::test]
    async fn a_narrowed_window_cannot_shrink_a_journaled_bound() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 800).await;
        mock_window(&mut server, 150).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sig_with_slot(1000, 400)).await,
                SigFinality::Uncertain(_)
            ),
            "the journaled slot, not the current window, must bound the proof"
        );
    }

    /// A journaled slot makes the bound self-contained, so the window is never
    /// read. Registering no `getLatestBlockhash` proves the call does not happen:
    /// an unreadable window would otherwise be Uncertain, and this is Dead.
    #[tokio::test]
    async fn journaled_slot_needs_no_window_rpc() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 800).await;

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sig_with_slot(1000, 900)).await,
                SigFinality::Dead
            ),
            "a journaled slot must not consult the blockhash window"
        );
    }

    /// The proof has to cover every absent signature, so one attempt without a
    /// journaled slot forfeits the exact bound for the whole set even though the
    /// other attempt has one that would have proven coverage on its own.
    #[tokio::test]
    async fn one_absent_sig_without_a_slot_forfeits_the_exact_bound() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 800).await;
        mock_window(&mut server, 600).await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1000,
                blockhash_slot: Some(900),
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1000,
                blockhash_slot: None,
            },
        ];

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sigs).await,
                SigFinality::Uncertain(_)
            ),
            "an unknown bound on any absent signature must not be ignored"
        );
    }

    /// Several journaled slots: the lowest one bounds the range, since the proof
    /// must reach the earliest block any of them could occupy.
    #[tokio::test]
    async fn lowest_journaled_slot_bounds_the_set() {
        let mut server = mockito::Server::new_async().await;
        mock_rpc(&mut server, "getSignatureStatuses", &null_status()).await;
        mock_height(&mut server, 2000).await;
        mock_floor(&mut server, 800).await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1000,
                blockhash_slot: Some(950),
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1000,
                blockhash_slot: Some(700),
            },
        ];

        let p = make_rpc(&server.url());
        let finality = FinalityRpc::channel(&p, None);
        assert!(
            matches!(
                classify_signatures(&finality, &sigs).await,
                SigFinality::Uncertain(_)
            ),
            "the lowest journaled slot must bound the set"
        );
    }
}
