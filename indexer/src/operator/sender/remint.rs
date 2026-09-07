#[cfg(test)]
use super::types::InFlightQueue;
use super::types::SenderState;
use crate::config::ProgramType;
use crate::metrics::OPERATOR_REMINT_CLAIM_LOST;
use crate::{
    channel_utils::send_guaranteed,
    operator::{
        check_transaction_status, fetch_consumed_nonces, find_withdrawal_bitmap_pda,
        remint_idempotency_memo,
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
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
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
/// Journals every MintTo signature write-ahead, then classifies stored signatures on entry
/// (before any resend) so a crash between broadcast and the FailedReminted write cannot
/// double-mint. Everything runs on the source chain (PrivateChannel), not rpc_client
/// (Solana, the ReleaseFunds destination). No sender-level retry.
async fn attempt_remint(state: &SenderState, info: &WithdrawalRemintInfo) -> RemintAttempt {
    let stored = match state
        .storage
        .get_remint_signatures(info.transaction_id)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            // An unread journal could be hiding an in-flight attempt, so capping
            // here risks a double mint. Retry the read; no resend either way.
            return RemintAttempt::DeferInFlight(format!(
                "stored remint-signature lookup failed for transaction {}: {}; will retry",
                info.transaction_id, e
            ));
        }
    };

    // Prior attempts the classifier proves dead. Only these may be retired by the
    // claim below; a Live or Uncertain verdict returns before it is populated.
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

        match classify_release_signatures(&state.source_rpc_client, &prior_attempts).await {
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

    let memo = remint_idempotency_memo(info.transaction_id);
    let admin_pubkey = SignerUtil::admin_signer().pubkey();

    // The memo is an on-chain marker only; the journaled signature is the
    // idempotency control this path decides on.
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

    // Nothing is broadcast until send_signed below, so build, blockhash and
    // signing failures are pre-broadcast: defer and retry, never ManualReview.
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

    // Write-ahead persist before broadcast, and the exclusive claim in the same
    // step. The checked cast keeps the round trip symmetric with the read-back.
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
            // Nothing else can hold the claim, so a second sender is running.
            // Emit no status: ManualReview here would move the row off
            // pending_remint and permanently block the winner's remint record,
            // stranding a mint that did land. Re-queue uncapped instead.
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
        // The signature is durable; the next attempt reclassifies it.
        return RemintAttempt::DeferInFlight(format!(
            "remint send failed for transaction {}: {}; will reclassify",
            info.transaction_id, e
        ));
    }

    let result = match check_transaction_status(
        state.source_rpc_client.clone(),
        &signature,
        CommitmentConfig::confirmed(),
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

            // Always Some for remint entries: they are only queued for a failed
            // withdrawal, which has a DB row. With no id there is no row to record
            // the landed remint against and no status message to key, so the only
            // action is a loud log. The remint already confirmed on-chain.
            let Some(transaction_id) = entry.ctx.transaction_id else {
                error!(
                    "Remint confirmed (sig: {}) but entry has no transaction_id; \
                     cannot record FailedReminted",
                    signature
                );
                return DeferredRemintOutcome::Resolved;
            };

            // Durably record the landed remint before the async channel send.
            // This flips status to FailedReminted now, so a crash in the window
            // before the writer runs can no longer leave the row PendingRemint
            // for restart recovery to pick up and remint a second time.
            match state
                .storage
                .record_remint_result(transaction_id, signature.to_string())
                .await
            {
                // The row is durably terminal, so the journal has nothing left to prove.
                Ok(()) => {
                    if let Err(e) = state.storage.delete_remint_signatures(transaction_id).await {
                        warn!(
                            "Failed to clear remint signatures for txn {}: {}; GC will sweep",
                            transaction_id, e
                        );
                    }
                }
                // The row stays PendingRemint, so the journal MUST be kept: a crash
                // before the async writer commits leaves restart recovery to classify
                // this landed signature rather than broadcast a duplicate.
                Err(persist_err) => {
                    error!(
                        "Remint sig {} confirmed but durable persist failed for txn {}: {}; \
                         keeping the journal, falling back to async status writer",
                        signature, transaction_id, persist_err
                    );
                }
            }

            // Drives the webhook alert, and is the fallback status write when the
            // durable persist above errored. Its UPDATE only touches
            // processing/pending_remint rows, so once the row is FailedReminted
            // this is a no-op.
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
                },
                "transaction status update",
            )
            .await
            {
                error!(
                    "Failed to send FailedReminted status for txn {}: {}. \
                     Remint sig {} confirmed on-chain.",
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
/// releases or remint MintTos). Shared by the remint gate and recovery so both
/// agree before mutating a withdrawal.
pub(crate) enum SigFinality {
    /// A signature finalized successfully — the release landed.
    Landed(Signature),
    /// A signature could still land; carries a reason for triage logs.
    Live(String),
    /// Every signature is finalized-failed or expired — safe to remint/demote.
    Dead,
    /// Could not classify (RPC/length error); callers must NOT treat as Dead.
    Uncertain(String),
}

/// Classify `sigs` against on-chain state (see `SigFinality` variants).
pub(crate) async fn classify_release_signatures(
    rpc: &RpcClientWithRetry,
    sigs: &[PendingSig],
) -> SigFinality {
    let flat: Vec<Signature> = sigs.iter().map(|p| p.signature).collect();

    let response = match rpc.get_signature_statuses_with_history(&flat).await {
        Ok(r) => r,
        Err(e) => {
            return SigFinality::Uncertain(format!("signature status RPC failed: {}", e));
        }
    };

    // RPC returns one status per signature in order; a length mismatch would
    // silently skip checks below, so treat it as uncertain.
    if response.value.len() != flat.len() {
        return SigFinality::Uncertain(format!(
            "RPC returned {} statuses for {} signatures",
            response.value.len(),
            flat.len()
        ));
    }

    // Any sig finalized successfully → the release landed.
    let finalized_success_index = response.value.iter().position(|signature_status| {
        signature_status.as_ref().is_some_and(|status| {
            status.satisfies_commitment(CommitmentConfig::finalized()) && status.err.is_none()
        })
    });
    if let Some(index) = finalized_success_index {
        return SigFinality::Landed(flat[index]);
    }

    // Fetch block height only for the lvbh check on null-status sigs, so a
    // transient getBlockHeight outage isn't treated as uncertainty otherwise.
    let current_height = if response.value.iter().any(|s| s.is_none()) {
        match rpc.get_block_height().await {
            Ok(h) => h,
            Err(e) => {
                return SigFinality::Uncertain(format!("block height RPC failed: {}", e));
            }
        }
    } else {
        // Unused: the null-status branch below only fires when some status is None.
        0
    };

    // Walk the sigs to see if any could still land (index-aligned with response.value).
    for (index, pending_sig) in sigs.iter().enumerate() {
        let signature_status = &response.value[index];

        if let Some(status) = signature_status.as_ref() {
            // Only `finalized` is definitive; success was handled above, so this is failure.
            if status.satisfies_commitment(CommitmentConfig::finalized()) {
                continue;
            }
            // confirmed/processed: in a block, will finalize regardless of blockhash validity.
            return SigFinality::Live(
                "signature is on-chain (confirmed/processed) and awaiting finalization".to_string(),
            );
        }

        // No status entry. lvbh is the only thing keeping it alive.
        if current_height > pending_sig.last_valid_block_height {
            continue;
        }
        return SigFinality::Live(format!(
            "signatures still within blockhash validity (current_height={})",
            current_height
        ));
    }

    // Every sig is finalized-failed or expired.
    SigFinality::Dead
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
    // Deferred remints are Withdraw-only; another role would classify the
    // release signatures against the wrong chain.
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
    for mut entry in matured {
        let nonce_label = entry
            .ctx
            .withdrawal_nonce
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".to_string());

        // Classify the stored signatures against on-chain state. This runs on
        // rpc_client (the destination/Solana chain where ReleaseFunds was sent),
        // not source_rpc_client which only the remint MintTo uses.
        match classify_release_signatures(&state.rpc_client, &entry.signatures).await {
            // Case 1: a sig finalized successfully, the withdrawal landed.
            // Nothing local to repair: the chain is the only record of consumption.
            SigFinality::Landed(sig) => {
                send_completed(storage_tx, &entry, &nonce_label, sig).await;
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
            // Case 3: every sig is finalized-failed or expired, so the bitmap decides.
            SigFinality::Dead => match bitmap_verdict(state, &entry).await {
                BitmapVerdict::Blocked(reason) => {
                    error!("Refusing to remint nonce {}: {}", nonce_label, reason);
                    send_manual_review(storage_tx, &entry, &reason).await;
                }
                // Not knowing whether the release landed is never permission to
                // credit the user again, so an unanswerable bitmap takes the same
                // route as an unclassifiable signature: wait a while, look
                // again, and hand the entry to a human if it stays unanswerable.
                BitmapVerdict::Unknown(reason) if !entry.release_refused_on_chain => {
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
                // The payout record is the last gate, and the only one whose answer survives a rotation.
                BitmapVerdict::Unknown(_) | BitmapVerdict::Clear => {
                    match release_record(state, &mut entry).await {
                        ReleaseRecord::Found(reason) => {
                            error!("Refusing to remint nonce {}: {}", nonce_label, reason);
                            send_manual_review(storage_tx, &entry, &reason).await;
                        }
                        // The indexer normally catches up in seconds, so waiting
                        // costs a tick where escalating costs a person.
                        ReleaseRecord::Unproven(reason) => {
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
                        ReleaseRecord::ProvenAbsent => {
                            info!(
                                "All withdrawal signatures for nonce {} are finalized-failed or expired; attempting remint",
                                nonce_label
                            );
                            match execute_deferred_remint(state, entry, storage_tx).await {
                                DeferredRemintOutcome::Resolved => {}
                                // Nothing was broadcast: bounded retry, then
                                // ManualReview. Safe because no sig can land.
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
                                // A signature is (or might be) journaled: re-queue
                                // uncapped so it keeps reclassifying. Terminalizing
                                // here would abandon a live sig into a double mint.
                                DeferredRemintOutcome::DeferInFlight(entry, reason) => {
                                    requeue_in_flight(
                                        &mut remaining,
                                        *entry,
                                        &nonce_label,
                                        &reason,
                                    );
                                }
                            }
                        }
                    }
                }
            },
        }
    }

    // `remaining` = entries not yet due + entries re-queued by `defer_or_escalate`
    // or `requeue_in_flight`.
    state.pending_remints = remaining;
}

/// What the chain can tell us about a nonce at the instant before a remint.
///
/// The three outcomes exist because "the bit is not set" and "we could not read
/// the bit" are completely different facts. Collapsing them into one is how a
/// gate that exists to stop a double credit ends up waving one through, so the
/// two are kept apart all the way to the decision.
enum BitmapVerdict {
    /// The bit is set: the release landed, so a remint would pay it out twice.
    Blocked(String),
    /// The bit is clear inside the window the bitmap currently covers.
    Clear,
    /// The bitmap could not answer, which is not the same as answering "no".
    Unknown(String),
}

/// Ask the chain whether this nonce was actually released before crediting the
/// user again.
///
/// This read is the safety property the credit rests on. Every other input to the
/// decision is indirect: signatures can look dead while the release landed under
/// one we never recorded. The bit is the release.
///
/// It is deliberately consulted here and nowhere earlier. Anywhere sooner and the
/// answer could go stale before the credit; this is the last instant at which it
/// is still true.
async fn bitmap_verdict(state: &SenderState, entry: &PendingRemint) -> BitmapVerdict {
    // No nonce or no instance means there is no bitmap to consult at all.
    // Neither is reachable from the withdraw sender, which is the only one that
    // queues a remint and is always configured with the instance that owns the
    // bitmap, so this is a shape guard rather than a live outcome.
    let (Some(nonce), Some(instance_pda)) = (entry.ctx.withdrawal_nonce, state.instance_pda) else {
        return BitmapVerdict::Clear;
    };

    let bitmap = match fetch_consumed_nonces(
        &state.rpc_client,
        &find_withdrawal_bitmap_pda(&instance_pda),
    )
    .await
    {
        Ok(bitmap) => bitmap,
        Err(e) => {
            warn!("Could not read the bitmap before reminting nonce {nonce}: {e}");
            return BitmapVerdict::Unknown(format!("bitmap read failed: {e}"));
        }
    };

    // Rotation clears every bit, so outside the current window a clear bit is
    // indistinguishable from a release that happened and was then wiped. Of the
    // two readings available here, treating it as "free" is the only one that
    // can pay a user twice, so the window is reported as unanswerable instead.
    if !bitmap.covers(nonce) {
        debug!(
            "Bitmap is on generation {} and cannot answer for nonce {nonce}",
            bitmap.generation
        );
        return BitmapVerdict::Unknown(format!(
            "the bitmap is on generation {} and its bits say nothing about nonce {nonce}",
            bitmap.generation
        ));
    }

    if bitmap.is_consumed(nonce) {
        return BitmapVerdict::Blocked(format!(
            "nonce {nonce} is consumed on-chain in generation {}, so the release landed \
             despite every signature looking dead; reminting would credit it twice",
            bitmap.generation
        ));
    }

    BitmapVerdict::Clear
}

/// What the indexer's record of releases can say about this nonce.
///
/// The three outcomes exist because an absent row has two very different
/// meanings: the indexer walked the slots and saw no release, or it has not
/// walked them yet. Only the first rules a payout out.
enum ReleaseRecord {
    /// A release is on record, so refunding would credit the nonce twice.
    Found(String),
    /// Every slot a release could sit in is indexed, and none holds one.
    ProvenAbsent,
    /// No row, but the record is not known to cover the window in question.
    Unproven(String),
}

/// Ask the indexer's record whether this nonce already paid out.
///
/// This is the last gate, and past a rotation it is the only one, because the
/// bits that would have answered have been cleared. So it has to distinguish an
/// absence it can stand behind from one it cannot: the checkpoint is the highest
/// slot the indexer has fully processed, and a release is written before the
/// checkpoint moves past its slot, so a checkpoint at or beyond the window makes
/// an empty lookup a real negative rather than a gap.
async fn release_record(state: &SenderState, entry: &mut PendingRemint) -> ReleaseRecord {
    let Some(nonce) = entry.ctx.withdrawal_nonce else {
        return ReleaseRecord::ProvenAbsent;
    };

    match state.storage.get_observed_release(nonce).await {
        Ok(Some(release)) => {
            return ReleaseRecord::Found(format!(
                "a release for nonce {nonce} is on record (slot {}, signature {}), so it already \
                 paid out and reminting would credit it twice",
                release.slot, release.signature
            ))
        }
        Ok(None) => {}
        // An unreadable record rules no payout out, and only a ruled-out payout may be refunded.
        Err(e) => {
            return ReleaseRecord::Found(format!(
                "the release record for nonce {nonce} could not be read ({e}), so a payout cannot \
                 be ruled out"
            ))
        }
    }

    coverage_verdict(state, entry, nonce).await
}

/// Whether the indexer has walked far enough for the empty lookup above to mean
/// anything.
async fn coverage_verdict(
    state: &SenderState,
    entry: &mut PendingRemint,
    nonce: u64,
) -> ReleaseRecord {
    let bound = match entry.coverage_slot {
        Some(slot) => slot,
        None => match state.rpc_client.get_slot().await {
            // A release that happened is in a slot at or below this one, so this
            // is the whole window the record has to cover.
            Ok(slot) => {
                entry.coverage_slot = Some(slot);
                slot
            }
            Err(e) => {
                return ReleaseRecord::Unproven(format!(
                    "could not read the current slot to bound the release window for nonce \
                     {nonce} ({e})"
                ))
            }
        },
    };

    match state
        .storage
        .get_committed_checkpoint(ProgramType::Escrow.as_label())
        .await
    {
        Ok(Some(checkpoint)) if checkpoint >= bound => ReleaseRecord::ProvenAbsent,
        Ok(Some(checkpoint)) => ReleaseRecord::Unproven(format!(
            "no release is on record for nonce {nonce}, but the indexer has only reached slot \
             {checkpoint} of {bound}, so a release could still be unindexed"
        )),
        Ok(None) => ReleaseRecord::Unproven(format!(
            "no release is on record for nonce {nonce} and the indexer has committed no \
             checkpoint, so nothing says the record covers slot {bound}"
        )),
        Err(e) => ReleaseRecord::Unproven(format!(
            "no release is on record for nonce {nonce} and the indexer checkpoint could not be \
             read ({e}), so its coverage is unknown"
        )),
    }
}

/// Escalate a pending-remint entry that must not be reminted and cannot be
/// completed either.
async fn send_manual_review(
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    entry: &PendingRemint,
    reason: &str,
) {
    let Some(transaction_id) = entry.ctx.transaction_id else {
        error!("Cannot escalate a pending remint with no transaction id: {reason}");
        return;
    };
    send_guaranteed(
        storage_tx,
        TransactionStatusUpdate {
            transaction_id,
            trace_id: entry.ctx.trace_id.clone(),
            status: TransactionStatus::ManualReview,
            counterpart_signature: None,
            processed_at: Some(Utc::now()),
            error_message: Some(format!("{} | {}", entry.original_error, reason)),
            remint_signature: None,
            remint_attempted: false,
        },
        "transaction status update",
    )
    .await
    .ok();
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
        },
        "transaction status update",
    )
    .await
    .ok();
}

/// Bump the entry's deferral counter and either re-queue with an extended
/// deadline or escalate to ManualReview when the cap is hit. Used by every
/// "couldn't classify this entry as ready-to-remint" branch.
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

/// Re-queue an in-flight remint (a sig is, or might be, journaled). Never
/// terminalizes and never bumps the counter: the classify gate resolves the sig
/// on a later tick, and terminalizing a live sig would risk a double mint.
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
    use crate::operator::sender::test_support::{mock_bitmap_account, mock_bitmap_read_failure};
    use crate::operator::sender::types::{
        PendingRemint, PendingSig, SenderState, TransactionContext, MAX_IN_FLIGHT,
    };
    use crate::operator::utils::instruction_util::{TransactionKind, WithdrawalRemintInfo};
    use crate::operator::MintCache;
    use crate::operator::RetryConfig;
    use crate::operator::RpcClientWithRetry;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbObservedRelease, StoredSig};
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use std::collections::{HashMap, HashSet};
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
            storage: storage.clone(),
            instance_pda: None,
            in_flight_withdrawals: HashSet::new(),
            retry_counts: HashMap::new(),
            cached_generation: None,
            rotation_retry_attempts: 0,
            rotation_in_flight: None,
            rotation_rearm_attempts: 0,
            rotation_blocked_passes: 0,
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Withdraw,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
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
                release_refused_on_chain: false,
            });
    }

    /// A stored PendingRemint row for a release the program refused: real
    /// signatures so the finality check can classify them, a deadline that has
    /// already matured so the entry is due on the first pass, and the refusal
    /// recorded on the row rather than only in the queue.
    fn seed_refused_pending_remint_row(mock: &MockStorage, id: i64, nonce: i64) {
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
                amount: TokenAmount(5_000),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(nonce),
                status: TransactionStatus::PendingRemint,
                created_at: now,
                updated_at: now,
                processed_at: None,
                counterpart_signature: None,
                remint_signatures: Some(vec![Signature::new_unique().to_string()]),
                remint_last_valid_block_heights: Some(vec![0]),
                pending_remint_deadline_at: Some(now - chrono::Duration::seconds(1)),
                finality_check_attempts: 0,
                recovery_requeue_attempts: 0,
                instruction_index: 0,
                inner_index: None,
                landed_remint_signature: None,
                release_refused_on_chain: true,
            });
    }

    fn make_remint_info(txn_id: i64) -> WithdrawalRemintInfo {
        WithdrawalRemintInfo {
            transaction_id: txn_id,
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
            storage: storage.clone(),
            instance_pda: None,
            in_flight_withdrawals: HashSet::new(),
            retry_counts: HashMap::new(),
            cached_generation: None,
            rotation_retry_attempts: 0,
            rotation_in_flight: None,
            rotation_rearm_attempts: 0,
            rotation_blocked_passes: 0,
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: role,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
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

    /// Builds a SenderState with distinct rpc_client and source_rpc_client
    /// endpoints, matching the cross-chain withdraw operator (rpc_url=Solana,
    /// source_rpc_url=PrivateChannel).
    fn make_sender_state_split_rpc(dest_url: &str, source_url: &str) -> (SenderState, MockStorage) {
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
            fast,
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let state = SenderState {
            rpc_client,
            source_rpc_client,
            storage: storage.clone(),
            instance_pda: None,
            in_flight_withdrawals: HashSet::new(),
            retry_counts: HashMap::new(),
            cached_generation: None,
            rotation_retry_attempts: 0,
            rotation_in_flight: None,
            rotation_rearm_attempts: 0,
            rotation_blocked_passes: 0,
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 1,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Withdraw,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
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

        // Source: backs the remint blockhash and broadcast.
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

        let (mut state, mock) = make_sender_state_split_rpc(&dest.url(), &source.url());
        let _cover = cover_release_window(&mut dest, &mock).await;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(555),
                withdrawal_nonce: Some(5),
                trace_id: Some("trace-555".to_string()),
            },
            remint_info: make_remint_info(555),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // The broadcast must reach the source server. The mocked node returns a
        // placeholder signature so the send does not confirm, but the request
        // still proves which chain the remint targeted.
        src_send.assert_async().await;
    }

    /// The deferred remint queue belongs to the Withdraw role. An Escrow sender
    /// must neither classify nor remint a queued entry: it would send RPC on the
    /// wrong chain and could escalate the row to ManualReview.
    #[tokio::test]
    async fn process_pending_remints_noop_for_escrow_role() {
        let mut server = mockito::Server::new_async().await;

        // A catch-all expecting zero hits, so any RPC the processor makes fails.
        let no_calls = server
            .mock("POST", "/")
            .with_status(200)
            .expect(0)
            .create_async()
            .await;

        let (mut state, _mock) =
            make_sender_state_with_role(&server.url(), crate::config::ProgramType::Escrow);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(88),
                withdrawal_nonce: Some(4),
                trace_id: Some("trace-88".to_string()),
            },
            remint_info: make_remint_info(88),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        no_calls.assert_async().await;
        assert!(
            storage_rx.try_recv().is_err(),
            "Escrow must not emit any status update from the remint processor"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "the entry must be left in place, not consumed"
        );
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "max retries".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(30),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-30".to_string()),
            },
            remint_info: make_remint_info(30),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 1,
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "max retries".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 2, // MAX_FINALITY_CHECK_ATTEMPTS - 1
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(10),
                withdrawal_nonce: Some(1),
                trace_id: Some("trace-10".to_string()),
            },
            remint_info: make_remint_info(10),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        // Entry 2: immature — must not be touched at all.
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(20),
                withdrawal_nonce: Some(2),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: future_deadline,
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(99),
                withdrawal_nonce: Some(7),
                trace_id: Some("trace-99".to_string()),
            },
            remint_info: make_remint_info(99),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
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
        assert!(
            storage_rx.try_recv().is_err(),
            "should send exactly one status update — no remint attempted"
        );
        assert!(
            state.pending_remints.is_empty(),
            "entry should be removed from queue after Completed"
        );
    }

    // ── remint bitmap gate ──────────────────────────────────────────

    /// Arm the getSignatureStatuses route so every stored signature classifies
    /// as finalized-and-failed, which is the verdict that would otherwise remint.
    async fn mock_dead_signature(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                    "slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":12}]},
                    "status":{"Err":{"InstructionError":[0,{"Custom":12}]}},
                    "confirmationStatus":"finalized"}]},"id":0}"#,
            )
            .create_async()
            .await
    }

    fn queue_dead_remint(state: &mut SenderState, nonce: u64) {
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(99),
                withdrawal_nonce: Some(nonce),
                trace_id: Some("trace-99".to_string()),
            },
            remint_info: make_remint_info(99),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });
    }

    /// Let a remint reach its write-ahead journal: the source blockhash resolves
    /// so the attempt is signed and claimed, but nothing is broadcast. A journaled
    /// signature is then the proof that the gate let the remint through.
    async fn mock_remint_blockhash(server: &mut mockito::Server) -> mockito::Mock {
        mock_rpc(
            server,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await
    }

    /// How many write-ahead remint attempts are journaled for a transaction.
    fn journaled_attempts(mock: &MockStorage, transaction_id: i64) -> usize {
        mock.remint_signatures
            .lock()
            .unwrap()
            .get(&transaction_id)
            .map(|sigs| sigs.len())
            .unwrap_or(0)
    }

    /// The money-safety case. Every signature looks dead, but the bit says the
    /// release landed. Reminting here would credit the user twice, so the entry
    /// must escalate instead and no mint may be attempted.
    #[tokio::test]
    async fn remint_blocked_when_bitmap_shows_nonce_consumed() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[3]);

        let (mut state, _mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("a blocked remint must still report the row");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            !update.remint_attempted,
            "no mint may be attempted once the bit proves the release landed"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// A clear bit leaves the signature verdict standing, so the remint proceeds
    /// down its normal path. The gate must not block every remint.
    #[tokio::test]
    async fn remint_proceeds_when_bitmap_shows_nonce_free() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);
        let _blockhash = mock_remint_blockhash(&mut server).await;

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        let _cover = cover_release_window(&mut server, &mock).await;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        process_pending_remints(&mut state, &storage_tx).await;

        assert_eq!(
            journaled_attempts(&mock, 99),
            1,
            "a clear bit must let the remint run as far as its write-ahead journal"
        );
    }

    /// A bitmap we could not read says nothing about the nonce, and "nothing"
    /// is not permission to credit the user a second time. The entry must go
    /// back on the queue for another look rather than reminting on the strength
    /// of a read that never happened.
    #[tokio::test]
    async fn remint_defers_when_the_bitmap_cannot_be_read() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_read_failure(&mut server);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        seed_pending_remint_row(&mock, 99, 0);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "an unreadable bitmap must not resolve the entry either way"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "the entry must be deferred for another attempt, not reminted"
        );
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    /// Deferring on an unreadable bitmap must still terminate.
    #[tokio::test]
    async fn remint_escalates_after_the_bitmap_stays_unreadable() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_read_failure(&mut server);

        let (mut state, _mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].finality_check_attempts = MAX_FINALITY_CHECK_ATTEMPTS - 1;
        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("the attempt cap must produce an outcome");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            !update.remint_attempted,
            "no mint may be attempted while the bitmap is unreadable"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// Once the bitmap has rotated past a nonce its cleared bit is not evidence
    /// the release never happened, so the gate must treat the window as
    /// unanswerable and defer rather than hand the signature verdict a payout
    /// it has no way to check.
    #[tokio::test]
    async fn remint_defers_when_the_bitmap_has_rotated_past_the_nonce() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        // Generation 1 covers a later window than nonce 3.
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        seed_pending_remint_row(&mock, 99, 0);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "a rotated-past window must not resolve the entry"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "a cleared bit in another generation proves nothing, so defer"
        );
    }

    /// The one case a rotated-past window must not block: the program itself
    /// refused this release, so the funds provably never moved and the user
    /// would otherwise be left holding neither the withdrawal nor the tokens
    /// that were burned to pay for it.
    #[tokio::test]
    async fn remint_proceeds_past_a_rotated_bitmap_when_the_chain_refused_the_release() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        let _blockhash = mock_remint_blockhash(&mut server).await;

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        let _cover = cover_release_window(&mut server, &mock).await;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;
        process_pending_remints(&mut state, &storage_tx).await;

        assert_eq!(
            journaled_attempts(&mock, 99),
            1,
            "an on-chain refusal is proof enough to return the tokens"
        );
    }

    /// Record a release for `nonce` the way the indexer does when it indexes one.
    async fn seed_observed_release(mock: &MockStorage, nonce: i64, signature: &str) {
        mock.insert_observed_releases_batch(&[DbObservedRelease {
            withdrawal_nonce: nonce,
            signature: signature.to_string(),
            slot: 4_000,
        }])
        .await
        .unwrap();
    }

    /// Put the record gate in the state a refund needs: a slot to bound the
    /// window a release could sit in, and a checkpoint that has reached it.
    async fn cover_release_window(
        server: &mut mockito::ServerGuard,
        mock: &MockStorage,
    ) -> mockito::Mock {
        let (slot_mock, _) = mock_get_slot(server, 1_000);
        mock.update_committed_checkpoint("escrow", 1_000)
            .await
            .unwrap();
        slot_mock
    }

    /// Answer `getSlot` with `slot`, counting the calls so a test can prove the
    /// bound is read once rather than on every tick.
    fn mock_get_slot(
        server: &mut mockito::ServerGuard,
        slot: u64,
    ) -> (mockito::Mock, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        let mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSlot""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body_from_request(move |_| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                format!(r#"{{"jsonrpc":"2.0","result":{slot},"id":0}}"#).into_bytes()
            })
            .expect_at_least(0)
            .create();
        (mock, calls)
    }

    /// An absent record only means "no release" once the indexer has walked the
    /// slots a release could sit in. Until its checkpoint covers them, absence
    /// is a gap in the record, and a gap is not permission to credit.
    #[tokio::test]
    async fn a_refund_waits_while_the_indexer_has_not_covered_the_release_window() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        let (_slot, _calls) = mock_get_slot(&mut server, 9_000);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        // The indexer is 1000 slots behind the point a release could have landed.
        mock.update_committed_checkpoint("escrow", 8_000)
            .await
            .unwrap();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter, so the row has to exist.
        seed_pending_remint_row(&mock, 99, 0);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;
        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "an uncovered window must defer quietly, not settle the row"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "the entry must wait for the indexer instead of refunding on an unproven absence"
        );
    }

    /// Once the checkpoint reaches the bound, the absence is real evidence and
    /// the refund the refusal authorised goes through unattended.
    #[tokio::test]
    async fn a_refund_proceeds_once_the_checkpoint_covers_the_release_window() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        let (_slot, _calls) = mock_get_slot(&mut server, 9_000);
        let _blockhash = mock_remint_blockhash(&mut server).await;

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        mock.update_committed_checkpoint("escrow", 9_000)
            .await
            .unwrap();
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;
        process_pending_remints(&mut state, &storage_tx).await;

        assert_eq!(
            journaled_attempts(&mock, 99),
            1,
            "a proven absence is what the refund was waiting on"
        );
    }

    /// The bound is the slot a release could last have landed in, so it is read
    /// once and kept. Re-reading it each tick would move the target the
    /// checkpoint is chasing and the refund could never clear.
    #[tokio::test]
    async fn the_coverage_bound_is_read_once_and_then_reused() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        let (_slot, calls) = mock_get_slot(&mut server, 9_000);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        mock.update_committed_checkpoint("escrow", 8_000)
            .await
            .unwrap();
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter, so the row has to exist.
        seed_pending_remint_row(&mock, 99, 0);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;

        for _ in 0..2 {
            // Mature the entry again so the second tick re-evaluates it.
            state.pending_remints[0].deadline = Utc::now() - chrono::Duration::seconds(1);
            process_pending_remints(&mut state, &storage_tx).await;
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the bound must be captured once, not chased"
        );
    }

    /// A checkpoint that cannot be read says nothing about coverage, and nothing
    /// is not the positive evidence a second credit needs.
    #[tokio::test]
    async fn an_unreadable_checkpoint_holds_the_refund() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        let (_slot, _calls) = mock_get_slot(&mut server, 9_000);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        mock.set_should_fail("get_committed_checkpoint", true);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter, so the row has to exist.
        seed_pending_remint_row(&mock, 99, 0);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;
        process_pending_remints(&mut state, &storage_tx).await;

        assert_eq!(
            state.pending_remints.len(),
            1,
            "an unreadable checkpoint must hold the refund, not wave it through"
        );
    }

    /// Past a rotated bitmap the refusal is all that carries the refund, so the payout record is the last refusal left.
    #[tokio::test]
    async fn a_recorded_release_blocks_the_refund_a_refusal_would_have_allowed() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        seed_observed_release(&mock, 3, "sig-already-paid").await;
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;
        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("a blocked refund must still report the row");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            !update.remint_attempted,
            "no mint may be attempted for a nonce that already paid out"
        );
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("sig-already-paid"),
            "the refusal must name the release it found: {err}"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// A clear bit contradicting a recorded release is not the positive evidence a second credit needs.
    #[tokio::test]
    async fn a_recorded_release_blocks_the_refund_a_clear_bit_would_have_allowed() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        seed_observed_release(&mock, 3, "sig-already-paid").await;
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("a blocked refund must still report the row");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            !update.remint_attempted,
            "a bit that disagrees with a recorded payout may not authorise a credit"
        );
    }

    /// An unreadable record rules nothing out, and only a ruled-out payout can be refunded.
    #[tokio::test]
    async fn an_unreadable_release_record_blocks_the_refund() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        mock.set_should_fail("get_observed_release", true);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        queue_dead_remint(&mut state, 3);
        state.pending_remints[0].release_refused_on_chain = true;
        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("an unreadable record must still report the row");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            !update.remint_attempted,
            "a lookup that never answered is not permission to credit"
        );
    }

    /// The same bypass, but for an entry that came back off a row instead of
    /// living in the queue the whole time. A restart inside the finality window
    /// must not cost the user an automatic refund, so the refusal has to survive
    /// the trip through storage and still carry the remint past a rotated bitmap.
    #[tokio::test]
    async fn a_recovered_on_chain_refusal_still_bypasses_a_rotated_bitmap() {
        ensure_test_signer();
        let mut server = mockito::Server::new_async().await;
        let _dead = mock_dead_signature(&mut server).await;
        let _bitmap = mock_bitmap_account(&mut server, 1, &[]);
        let _blockhash = mock_remint_blockhash(&mut server).await;

        let (mut state, mock) = make_sender_state_with_rpc(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        seed_refused_pending_remint_row(&mock, 55, 3);
        let _cover = cover_release_window(&mut server, &mock).await;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();
        assert_eq!(state.pending_remints.len(), 1, "the row must re-hydrate");

        process_pending_remints(&mut state, &storage_tx).await;

        assert_eq!(
            journaled_attempts(&mock, 55),
            1,
            "a refusal recovered from storage is still proof enough to return the tokens"
        );
    }

    // ── execute_deferred_remint paths ───────────────────────────────

    /// A journaled attempt that landed is the whole point of the write-ahead
    /// record: the crash between broadcast and the FailedReminted write must
    /// resolve from the journal, never from a second MintTo.
    #[tokio::test]
    async fn execute_deferred_remint_confirms_a_journaled_attempt_without_resending() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let journaled = Signature::new_unique();
        mock.remint_signatures.lock().unwrap().insert(
            710,
            vec![StoredSig {
                signature: journaled.to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        // The journaled attempt finalized successfully on the source chain.
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
                "slot":100,"confirmations":null,"err":null,
                "status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":0}"#,
        )
        .await;
        // A second broadcast here is the double credit this gate exists to stop.
        let no_send = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .expect(0)
            .create_async()
            .await;

        let entry = make_matured_remint(710, 71);
        let _outcome = execute_deferred_remint(&state, entry, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("a landed journaled attempt must resolve the row");
        assert_eq!(update.status, TransactionStatus::FailedReminted);
        assert_eq!(
            update.remint_signature.as_deref(),
            Some(journaled.to_string().as_str()),
            "the recorded remint must be the journaled attempt, not a fresh one"
        );
        no_send.assert_async().await;
    }

    /// The signature has to reach the journal before it reaches the network, or
    /// a crash in the send window leaves nothing for the next pass to classify.
    #[tokio::test]
    async fn execute_deferred_remint_journals_the_signature_before_broadcasting() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let _blockhash = mock_rpc(
            &mut rpc_server,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        let _send = mock_rpc(
            &mut rpc_server,
            "sendTransaction",
            r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":0}"#,
        )
        .await;

        let entry = make_matured_remint(712, 73);
        let _outcome = execute_deferred_remint(&state, entry, &storage_tx).await;

        assert_eq!(
            mock.remint_signatures
                .lock()
                .unwrap()
                .get(&712)
                .map(|s| s.len())
                .unwrap_or(0),
            1,
            "the attempt must be journaled even though the send failed"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a possibly-broadcast attempt must not be terminalized"
        );
    }

    /// The partial unique index hands one live attempt to one sender. The loser
    /// must not broadcast and must not write a status: the winner's mint may
    /// land, and ManualReview here would block the record of it.
    #[tokio::test]
    async fn execute_deferred_remint_defers_when_another_sender_owns_the_claim() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        mock.foreign_remint_claims.lock().unwrap().insert(711);

        let _blockhash = mock_rpc(
            &mut rpc_server,
            "getLatestBlockhash",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":1000}},"id":0}"#,
        )
        .await;
        let no_send = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .expect(0)
            .create_async()
            .await;

        let entry = make_matured_remint(711, 72);
        let _outcome = execute_deferred_remint(&state, entry, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "a lost claim must leave the row for the winner to resolve"
        );
        no_send.assert_async().await;
    }

    /// A matured entry for `transaction_id`, already past its deadline so the
    /// queue processes it on the first pass.
    fn make_matured_remint(transaction_id: i64, nonce: u64) -> PendingRemint {
        PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(transaction_id),
                withdrawal_nonce: Some(nonce),
                trace_id: Some(format!("trace-{transaction_id}")),
            },
            remint_info: make_remint_info(transaction_id),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        }
    }

    /// Fail-closed: a journaled attempt that cannot be classified (here the
    /// backend rejects getSignatureStatuses) might have landed, so attempt_remint
    /// must refuse to mint and escalate rather than risk a duplicate remint.
    #[tokio::test]
    async fn execute_deferred_remint_fails_closed_when_a_journaled_attempt_is_unclassifiable() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        mock.remint_signatures.lock().unwrap().insert(
            700,
            vec![StoredSig {
                signature: Signature::new_unique().to_string(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );
        // The status lookup that would classify it is unavailable on this backend.
        let _statuses = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":0}"#,
        )
        .await;

        let entry = PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(700),
                withdrawal_nonce: Some(70),
                trace_id: Some("trace-700".to_string()),
            },
            remint_info: make_remint_info(700),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        };

        let _outcome = execute_deferred_remint(&state, entry, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("an unclassifiable attempt must emit a status update");
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
    }

    /// When the finality check returns null for a withdrawal signature
    /// (transaction was dropped), `execute_deferred_remint` is called. The
    /// retry a failing remint gets is bounded: an entry already at the last
    /// attempt trips the cap and goes to ManualReview instead of looping.
    #[tokio::test]
    async fn process_pending_remints_escalates_a_failing_remint_at_the_attempt_cap() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let _cover = cover_release_window(&mut rpc_server, &mock).await;
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

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

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(77),
                withdrawal_nonce: Some(11),
                trace_id: Some("trace-77".to_string()),
            },
            remint_info: make_remint_info(77),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: MAX_FINALITY_CHECK_ATTEMPTS - 1,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx.try_recv().expect("should receive ManualReview");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.status, TransactionStatus::ManualReview);

        let err = update.error_message.as_deref().unwrap();
        assert!(
            err.contains("escalated to ManualReview"),
            "error should name the escalation: {err}"
        );
        assert!(
            err.contains("release_funds failed"),
            "error should include original withdrawal error: {err}"
        );

        assert!(state.pending_remints.is_empty());
    }

    /// A withdrawal that reached finality but failed on-chain (err field is set)
    /// is NOT a successful withdrawal — the user's funds never left the escrow.
    /// The operator must proceed to remint, not mark Completed.
    #[tokio::test]
    async fn process_pending_remints_finalized_with_onchain_error_proceeds_to_remint() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

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

        // Block height ahead of the stored lvbh (0) so the finalized-failed sig
        // counts as dead and the gate falls through to Case 3 (remint).
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(88),
                withdrawal_nonce: Some(12),
                trace_id: Some("trace-88".to_string()),
            },
            remint_info: make_remint_info(88),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
            original_error: "timeout".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive a status update");
        assert_ne!(
            update.status,
            TransactionStatus::Completed,
            "finalized-with-error must NOT produce Completed — funds never left escrow"
        );
        assert_eq!(update.transaction_id, 88);
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
        let _cover = cover_release_window(&mut rpc_server, &mock).await;
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

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

        // Deliberately NOT mocking getBlockHeight: if the code reaches that
        // call mockito returns 501, the call errors, and defer_or_escalate
        // fires with "block height RPC failed" instead of execute_deferred_remint.
        // The blockhash mock lets a remint that does run journal its attempt.
        let _blockhash = mock_remint_blockhash(&mut rpc_server).await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(89),
                withdrawal_nonce: Some(13),
                trace_id: Some("trace-89".to_string()),
            },
            remint_info: make_remint_info(89),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "the remint attempt is unresolved, so nothing may be written yet"
        );
        assert_eq!(
            journaled_attempts(&mock, 89),
            1,
            "must reach execute_deferred_remint; a block-height read here would \
             have deferred before the remint and journaled nothing"
        );
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(55),
                withdrawal_nonce: Some(6),
                trace_id: Some("trace-55".to_string()),
            },
            remint_info: make_remint_info(55),
            signatures: vec![
                PendingSig {
                    signature: sig1,
                    last_valid_block_height: 0,
                },
                PendingSig {
                    signature: sig2,
                    last_valid_block_height: 0,
                },
            ],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
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

    // ── classify_release_signatures (multi-sig) ─────────────────

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
    async fn classify_release_signatures_finalized_success_wins_over_earlier_failure() {
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
            },
            PendingSig {
                signature: success,
                last_valid_block_height: 0,
            },
        ];

        match classify_release_signatures(&rpc, &sigs).await {
            SigFinality::Landed(s) => assert_eq!(
                s, success,
                "must return the finalized-success sig, not the failed one"
            ),
            _ => panic!("expected Landed(success sig), got a different verdict"),
        }
    }

    /// Confirmed success behind a finalized failure must stay Live, never Dead.
    #[tokio::test]
    async fn classify_release_signatures_confirmed_success_after_failure_is_live_not_dead() {
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
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            },
        ];

        assert!(
            matches!(
                classify_release_signatures(&rpc, &sigs).await,
                SigFinality::Live(_)
            ),
            "confirmed success behind a finalized failure must be Live, not Dead"
        );
    }

    /// A still-valid null after an expired null must be Live: nulls are walked fully, not cut at the first.
    #[tokio::test]
    async fn classify_release_signatures_live_null_after_expired_null_is_live() {
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
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 2000,
            },
        ];

        assert!(
            matches!(
                classify_release_signatures(&rpc, &sigs).await,
                SigFinality::Live(_)
            ),
            "a still-valid null after an expired null must be Live, not Dead"
        );
    }

    /// A truncated status list (fewer statuses than sigs) must be Uncertain, never read as "missing = dead".
    #[tokio::test]
    async fn classify_release_signatures_status_length_mismatch_is_uncertain() {
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
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            },
        ];

        assert!(
            matches!(
                classify_release_signatures(&rpc, &sigs).await,
                SigFinality::Uncertain(_)
            ),
            "length mismatch must be Uncertain"
        );
    }

    // ── liveness gate paths ────────────────────────────────────────────

    /// Sig has no on-chain record AND its blockhash is past validity. Dead, so
    /// the gate proceeds to remint. The remint then fails before anything is
    /// signed or sent, which nothing can have landed from: the entry is
    /// re-queued for a later pass instead of being handed to a human.
    #[tokio::test]
    async fn process_pending_remints_all_sigs_expired_requeues_a_pre_broadcast_failure() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let _cover = cover_release_window(&mut rpc_server, &mock).await;
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        seed_pending_remint_row(&mock, 100, 0);

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

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(100),
                withdrawal_nonce: Some(20),
                trace_id: Some("trace-100".to_string()),
            },
            remint_info: make_remint_info(100),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Reaching Case 3 triggers execute_deferred_remint, whose blockhash
        // fetch has no matching mock, so the attempt dies before signing.
        assert!(
            storage_rx.try_recv().is_err(),
            "a pre-broadcast failure must not write a terminal status"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "reaching Case 3 means execute_deferred_remint ran and re-queued"
        );
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 1,
            "the deferral must bump the attempt counter"
        );
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(101),
                withdrawal_nonce: Some(21),
                trace_id: Some("trace-101".to_string()),
            },
            remint_info: make_remint_info(101),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1000,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(102),
                withdrawal_nonce: Some(22),
                trace_id: Some("trace-102".to_string()),
            },
            remint_info: make_remint_info(102),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1000,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 2, // one more attempt hits the cap
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(103),
                withdrawal_nonce: Some(23),
                trace_id: Some("trace-103".to_string()),
            },
            remint_info: make_remint_info(103),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
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
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(105),
                withdrawal_nonce: Some(25),
                trace_id: Some("trace-105".to_string()),
            },
            remint_info: make_remint_info(105),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
            release_refused_on_chain: false,
            coverage_slot: None,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: a confirmed-but-not-finalized sig must defer the remint"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }
}
