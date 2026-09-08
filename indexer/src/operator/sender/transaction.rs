use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::TransactionError;
use crate::error::{OperatorError, ProgramError};
use crate::metrics;
use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
use crate::operator::recovery::{load_pending_sigs, MAX_RECOVERY_REQUEUE_ATTEMPTS};
use crate::operator::utils::instruction_util::{
    mint_extra_error_checks_policy, TransactionBuilder, TransactionKind, WithdrawalRemintInfo,
};
use crate::operator::utils::storage_util::with_storage_backoff;
use crate::operator::utils::transaction_util::parse_program_error;
use crate::operator::utils::transaction_util::{
    build_and_sign, check_transaction_status, send_signed, ConfirmationResult,
    MAX_POLL_ATTEMPTS_CONFIRMATION,
};
use crate::operator::{
    sign_and_send_transaction, ExtraErrorCheckPolicy, RetryPolicy, RpcClientWithRetry,
};
use crate::storage::common::models::TransactionStatus;
use crate::storage::common::storage::{RequeueOutcome, Storage};
use chrono::Utc;
use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
use private_channel_metrics::MetricLabel;
use solana_keychain::SolanaSigner;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::{error, info, info_span, warn, Instrument};

use super::mint::{cleanup_mint_builder, try_jit_mint_initialization, JitOutcome};
use super::proof::cleanup_failed_transaction;
use super::types::{
    InFlightQueue, InFlightTx, InstructionWithSigners, PendingRemint, PendingSig, PollTaskResult,
    SendDurability, SenderState, TransactionContext, TransactionStatusUpdate, MAX_IN_FLIGHT,
};
use super::{classify_signatures, SigFinality};

use std::sync::Arc;

use std::time::Duration;

/// Safety delay before checking finality and reminting.
/// Solana finalized ≈ 32 slots × 400ms = ~12.8s. We use 2.5× safety factor.
pub const FINALITY_SAFETY_DELAY: Duration = Duration::from_secs(32);

const MAX_SIGS_PER_CALL: usize = 256;

impl SenderState {
    /// Turn an incoming builder into a signable instruction.
    pub(super) async fn handle_transaction_builder(
        &mut self,
        tx_builder: TransactionBuilder,
    ) -> Result<InstructionWithSigners, OperatorError> {
        let signers = tx_builder.signers();
        let compute_unit_price = tx_builder.compute_unit_price();
        let compute_budget = tx_builder.compute_budget();

        // For now fee payer is always the first signer
        let fee_payer = match signers.first() {
            Some(s) => s.pubkey(),
            None => {
                return Err(ProgramError::InvalidBuilder {
                    reason: "No signers provided".to_string(),
                }
                .into())
            }
        };

        match tx_builder {
            TransactionBuilder::ReleaseFunds(builder_with_nonce) => {
                // Cache remint info for potential recovery on permanent failure
                if let Some(ref info) = builder_with_nonce.remint_info {
                    self.remint_cache
                        .insert(builder_with_nonce.nonce, info.clone());
                }

                self.handle_release_funds_transaction(
                    builder_with_nonce,
                    fee_payer,
                    signers,
                    compute_unit_price,
                    compute_budget,
                )
                .await
            }
            // InitializeMint transaction: creates mint account via AdminVm
            TransactionBuilder::InitializeMint(_) => Ok(InstructionWithSigners {
                instructions: tx_builder.instructions()?,
                fee_payer,
                signers,
                compute_unit_price,
                compute_budget,
            }),
            TransactionBuilder::Mint(ref builder_with_txn_id) => {
                // Cache the builder for potential JIT retry
                self.mint_builders.insert(
                    builder_with_txn_id.txn_id,
                    builder_with_txn_id.builder.clone(),
                );

                // Mint transaction: creates ATA + mints tokens
                Ok(InstructionWithSigners {
                    instructions: tx_builder.instructions()?,
                    fee_payer,
                    signers,
                    compute_unit_price,
                    compute_budget,
                })
            }
            TransactionBuilder::RotateBitmap(mut builder) => {
                // Rotation frees an in-flight nonce for replay, so wait for the drain.
                let in_flight_count = self.in_flight_withdrawals.len();
                if in_flight_count > 0 {
                    info!(
                        "Rotation transaction received but {} in-flight txs exist - queuing",
                        in_flight_count
                    );

                    self.pending_rotation = Some(builder);

                    return Err(ProgramError::RotationPending { in_flight_count }.into());
                }

                // Bind the rotation to the generation the chain is actually on, so
                // a replayed rotation is rejected rather than skipping a whole
                // generation of nonces that could then never be released.
                //
                // Read fresh every time rather than taking the cached value.
                // This is the one place a wrong generation would be written on
                // chain and left there, instead of being handed straight back
                // by the program as a refusal the sender can act on.
                let expected_generation = match self.refresh_generation().await {
                    Ok(generation) => generation,
                    // Nothing re-dispatches a rotation once the boundary row has
                    // been processed, so dropping it here would leave the next
                    // generation closed and every withdrawal in it refused.
                    // Park it for the tick to retry instead.
                    Err(e) => {
                        self.pending_rotation = Some(builder);
                        return Err(e);
                    }
                };
                builder.expected_generation(expected_generation);

                // Kept because a rotation that fails has nothing else to rebuild it from.
                self.rotation_in_flight = Some(builder.clone());

                Ok(InstructionWithSigners {
                    instructions: vec![builder.instruction()],
                    fee_payer,
                    signers,
                    compute_budget,
                    compute_unit_price,
                })
            }
        }
    }
}

/// Top-level handler for a single transaction submission
pub async fn handle_transaction_submission(
    state: &mut SenderState,
    tx_builder: TransactionBuilder,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let ctx = TransactionContext {
        transaction_id: tx_builder.transaction_id(),
        withdrawal_nonce: tx_builder.withdrawal_nonce(),
        trace_id: tx_builder.trace_id(),
        kind: tx_builder.kind(),
        deposit_claim_lease: None,
    };

    // A submitted builder always carries the token for the incarnation it owns,
    // so arming the lease from it is correct on a first arrival and on a re-entry
    // after a rotation wait alike.
    if let TransactionBuilder::ReleaseFunds(builder_with_nonce) = &tx_builder {
        state.release_leases.insert(
            builder_with_nonce.nonce,
            builder_with_nonce.fetched_updated_at,
        );
    }

    let retry_policy = tx_builder.retry_policy();
    let compute_unit_price = tx_builder.compute_unit_price();
    // Owned so it can be moved into InFlightTx
    let extra_error_checks_policy = tx_builder.extra_error_checks_policy();

    let span = info_span!(
        "tx",
        trace_id = ctx.trace_id.as_deref().unwrap_or("none"),
        nonce = ctx.withdrawal_nonce.map(|n| n as i64),
    );

    async {
        match state.handle_transaction_builder(tx_builder.clone()).await {
            Ok(instruction) => {
                info!("Transaction instruction ready for submission");
                // Mint and InitializeMint use fire-and-forget: send immediately,
                // defer confirmation to the batch timer poll in `poll_in_flight`.
                // ReleaseFunds and RotateBitmap block so a rotation never overtakes a release.
                match &tx_builder {
                    TransactionBuilder::Mint(_) | TransactionBuilder::InitializeMint(_) => {
                        // Only a real user-fund Mint is Recoverable, and it is the
                        // only builder carrying an ownership token; InitializeMint
                        // mints no balance and is on-chain idempotent.
                        let durability = match tx_builder.fetched_updated_at() {
                            Some(deposit_expected_updated_at) => SendDurability::Recoverable {
                                deposit_expected_updated_at,
                            },
                            None => SendDurability::Terminal,
                        };
                        spawn_fire_and_store(
                            state,
                            instruction,
                            compute_unit_price,
                            ctx.clone(),
                            retry_policy,
                            extra_error_checks_policy,
                            storage_tx.clone(),
                            durability,
                        );
                    }
                    _ => {
                        send_and_confirm(
                            state,
                            instruction,
                            compute_unit_price,
                            &ctx,
                            retry_policy,
                            &extra_error_checks_policy,
                            storage_tx,
                        )
                        .await;
                    }
                }
            }
            Err(e) => {
                route_builder_error(state, &ctx, storage_tx, e).await;
            }
        }
    }
    .instrument(span)
    .await;
}

/// Route a `handle_transaction_builder` error to its non-success path; separate from
/// `handle_transaction_submission` so it is testable without real signers.
pub(super) async fn route_builder_error(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    err: OperatorError,
) {
    match err {
        OperatorError::Program(ProgramError::RotationPending { in_flight_count }) => {
            info!(
                "Rotation pending, waiting for {} in-flight txs to settle",
                in_flight_count
            );
        }
        // The pre-send check refused to broadcast a release the bitmap's window
        // cannot accept. A nonce whose window has not opened was parked on the
        // rotation retry queue by the build path, which is the only place that
        // still holds the built instruction; nothing more is owed here.
        OperatorError::Program(ProgramError::GenerationMismatch {
            nonce,
            nonce_generation,
            chain_generation,
        }) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "nonce_outside_generation"])
                .inc();
            if nonce_generation > chain_generation {
                info!(
                    nonce,
                    nonce_generation,
                    chain_generation,
                    "Release queued for the rotation that opens its window"
                );
                return;
            }
            error!(
                nonce,
                nonce_generation,
                chain_generation,
                "Nonce belongs to a generation the bitmap has already rotated past; it can never be released"
            );
            // Never broadcast, so nothing moved funds. That is the same evidence
            // an on-chain refusal carries, so it takes the same compensating
            // route: signatures from any earlier attempt are classified first,
            // and a nonce with none of them ends in manual review.
            remint_after_onchain_refusal(
                state,
                ctx,
                storage_tx,
                &ProgramError::GenerationMismatch {
                    nonce,
                    nonce_generation,
                    chain_generation,
                }
                .to_string(),
            )
            .await;
        }
        e @ OperatorError::Program(ProgramError::BitmapUnavailable { .. })
        | e @ OperatorError::Account(_)
        | e @ OperatorError::Storage(_) => {
            // The bitmap could not be read, or an account or database read failed
            // on the way to building this transaction. Nothing was broadcast, so
            // the row never released and must never be marked Failed on what is
            // only a read failure.
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "bitmap_unavailable"])
                .inc();
            match prebroadcast_requeue_target(state, ctx) {
                // A bounded retry from Pending beats waiting for the recovery
                // sweep, which would only quarantine what a reread can settle.
                Some((nonce, transaction_id)) => {
                    warn!(
                        transaction_id,
                        nonce, "Could not read chain state to build the transaction: {e}"
                    );
                    let reason = format!(
                        "chain state unreadable after {MAX_RECOVERY_REQUEUE_ATTEMPTS} requeues: {e}"
                    );
                    requeue_or_fail_prebroadcast(
                        state,
                        ctx,
                        storage_tx,
                        nonce,
                        transaction_id,
                        &reason,
                    )
                    .await;
                }
                None => error!(
                    transaction_id = ctx.transaction_id,
                    nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                    "Could not read chain state to build the transaction; leaving row Processing for recovery: {}",
                    e
                ),
            }
        }
        // A deposit mint that fails to build never signed and never broadcast, so
        // the source funds are escrowed with nothing minted against them. Failed is
        // a status no worker re-claims, so the row stays Processing for recovery.
        e if ctx.kind == TransactionKind::Mint => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "build_error"])
                .inc();
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[
                    state.program_type.as_label(),
                    "left_processing_for_recovery",
                ])
                .inc();
            error!(
                transaction_id = ctx.transaction_id,
                "Failed to build deposit mint; leaving row Processing for recovery: {}", e
            );
        }
        e => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "build_error"])
                .inc();
            error!("Failed to build transaction: {}", e);
            send_fatal_error(storage_tx, ctx, &e.to_string()).await;
        }
    }
}

/// Verdict of the pre-broadcast ownership claim.
pub(super) enum SignatureClaim {
    /// The row is still the incarnation we were handed; `lease` is the token the
    /// next claim on it must present.
    Owned(chrono::DateTime<Utc>),
    /// Another writer reached the row first. Never broadcast.
    Lost,
    /// The claim write itself failed, so ownership is unknown. Never broadcast.
    Failed,
}

/// Claim the `Processing` incarnation this transaction was handed and persist its
/// broadcast signature write-ahead, both in one storage transaction. Fail-closed:
/// only `Owned` authorizes a send.
///
/// Presenting the lease rather than a bare status check is deliberate: recovery
/// CASes the same `updated_at` column, so a demote and a claim can never both
/// win, whereas a row that was demoted and re-fetched is `Processing` again and
/// would pass a status test.
#[allow(clippy::too_many_arguments)]
pub(super) async fn claim_and_persist_or_abort(
    storage: &Storage,
    pt: &str,
    transaction_id: i64,
    expected_updated_at: chrono::DateTime<Utc>,
    signature: &Signature,
    last_valid_block_height: u64,
    blockhash_slot: u64,
    lost_label: &str,
) -> SignatureClaim {
    match storage
        .claim_and_persist_signature(
            transaction_id,
            expected_updated_at,
            signature.to_string(),
            last_valid_block_height as i64,
            i64::try_from(blockhash_slot).ok(),
        )
        .await
    {
        Ok(Some(lease)) => SignatureClaim::Owned(lease),
        Ok(None) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, lost_label])
                .inc();
            warn!(
                transaction_id,
                signature = %signature,
                "Ownership lost before broadcast; dropping stale builder without sending"
            );
            SignatureClaim::Lost
        }
        Err(e) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "pre_send_persist_error"])
                .inc();
            let abort = TransactionError::PreSendPersistFailed {
                reason: e.to_string(),
            };
            error!(
                transaction_id,
                signature = %signature,
                "Aborting before broadcast, leaving row Processing for recovery: {}",
                abort
            );
            SignatureClaim::Failed
        }
    }
}

/// Sender-level attempts already spent, or `None` for a kind this bound does not cover.
fn sender_retry_attempts(state: &SenderState, ctx: &TransactionContext) -> Option<u32> {
    match ctx.kind {
        TransactionKind::RotateBitmap => Some(state.rotation_retry_attempts),
        TransactionKind::ReleaseFunds => ctx
            .withdrawal_nonce
            .map(|nonce| state.retry_counts.get(&nonce).copied().unwrap_or(0)),
        // A mint is bounded by the poll queue, and a bound here would fail it into a status nothing can write.
        TransactionKind::Mint | TransactionKind::InitializeMint => None,
    }
}

fn record_sender_retry_attempt(state: &mut SenderState, ctx: &TransactionContext, attempts: u32) {
    match ctx.kind {
        TransactionKind::RotateBitmap => state.rotation_retry_attempts = attempts,
        TransactionKind::ReleaseFunds => {
            if let Some(nonce) = ctx.withdrawal_nonce {
                state.retry_counts.insert(nonce, attempts);
            }
        }
        TransactionKind::Mint | TransactionKind::InitializeMint => {}
    }
}

/// Forget a settled rotation, so the next starts on a full budget and nothing is left owed on this one.
fn clear_rotation_retry_state(state: &mut SenderState, ctx: &TransactionContext) {
    if ctx.kind == TransactionKind::RotateBitmap {
        state.rotation_retry_attempts = 0;
        state.rotation_rearm_attempts = 0;
        state.rotation_in_flight = None;
    }
}

/// Re-arms allowed per rotation; each buys a fresh send budget, so the product bounds what one rotation can cost.
const MAX_ROTATION_REARMS: u32 = 3;

/// Put a failed rotation back on the tick; nothing else re-dispatches one, and the generation it owes stays shut.
fn rearm_failed_rotation(state: &mut SenderState, error_msg: &str) {
    // The re-armed rotation is a fresh send, so it gets the full retry budget.
    state.rotation_retry_attempts = 0;

    let give_up = |state: &mut SenderState, reason: &str| {
        metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[state.program_type.as_label(), "rotation_lost"])
            .inc();
        error!(
            "Rotation abandoned after failing: {error_msg}; {reason}. Every nonce in the \
             unopened generation stays unreleasable until a rotation lands."
        );
        state.rotation_in_flight = None;
        // The driver starts a fresh rotation once nothing is in flight, and a
        // count left at the limit would abandon that one on its first failure.
        state.rotation_rearm_attempts = 0;
    };

    let Some(builder) = state.rotation_in_flight.clone() else {
        give_up(state, "nothing was held to re-dispatch it from");
        return;
    };

    if state.rotation_rearm_attempts >= MAX_ROTATION_REARMS {
        give_up(state, "it has already been re-armed to the limit");
        return;
    }

    state.rotation_rearm_attempts += 1;
    metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[state.program_type.as_label(), "rotation_rearmed"])
        .inc();
    error!(
        attempt = state.rotation_rearm_attempts,
        "Rotation failed and was re-armed for the next tick: {error_msg}"
    );
    state.pending_rotation = Some(builder);
}

/// Sign, send, confirm, and handle the result
pub(super) async fn send_and_confirm(
    state: &mut SenderState,
    instruction: InstructionWithSigners,
    compute_unit_price: Option<u64>,
    ctx: &TransactionContext,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: &ExtraErrorCheckPolicy,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    // Check retry limit - only for idempotent operations that can be retried at sender level.
    // An uncounted rotation re-enters here forever, so the bound is keyed on the kind rather than on absent ids.
    match retry_policy {
        RetryPolicy::Idempotent => {
            if let Some(attempts) = sender_retry_attempts(state, ctx) {
                if attempts >= state.retry_max_attempts {
                    metrics::OPERATOR_TRANSACTION_ERRORS
                        .with_label_values(&[state.program_type.as_label(), "max_retries_exceeded"])
                        .inc();
                    error!(
                        nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                        transaction_id = ctx.transaction_id,
                        "Max retries ({}) exceeded",
                        state.retry_max_attempts
                    );
                    handle_permanent_failure(state, ctx, storage_tx, "Max retries exceeded").await;
                    return;
                }
                record_sender_retry_attempt(state, ctx, attempts + 1);
                info!(
                    nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                    "Transaction attempt {}/{}",
                    attempts + 1,
                    state.retry_max_attempts
                );
            }
        }
        RetryPolicy::None => {
            info!("Sending non-idempotent transaction - single sender-level attempt");
        }
    }

    let pt = state.program_type.as_label();
    let send_start = std::time::Instant::now();

    // Build and sign before broadcasting so the signature can be persisted write-ahead.
    let (transaction, signature, last_valid_block_height, blockhash_slot) =
        match build_and_sign(&state.rpc_client, instruction.clone()).await {
            Ok(signed) => signed,
            Err(e) => {
                metrics::OPERATOR_RPC_SEND_DURATION
                    .with_label_values(&[pt, "error"])
                    .observe(send_start.elapsed().as_secs_f64());
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "build_sign_error"])
                    .inc();
                error!("Failed to build/sign transaction: {}", e);
                // The failure came before a signature existed, so a withdrawal
                // with nothing stashed from an earlier attempt provably never
                // broadcast and can simply be retried.
                match prebroadcast_requeue_target(state, ctx) {
                    Some((nonce, transaction_id)) => {
                        let reason = format!(
                            "build/sign failed after {MAX_RECOVERY_REQUEUE_ATTEMPTS} requeues: {e}"
                        );
                        requeue_or_fail_prebroadcast(
                            state,
                            ctx,
                            storage_tx,
                            nonce,
                            transaction_id,
                            &reason,
                        )
                        .await;
                    }
                    None => handle_permanent_failure(state, ctx, storage_tx, &e.to_string()).await,
                }
                return;
            }
        };

    // A withdrawal nonce is consumed on broadcast, so a release that lands must already
    // have a durable signature record for crash recovery to reconcile against, and this
    // sender must still own the row it is about to pay out.
    if let (Some(nonce), Some(txid)) = (ctx.withdrawal_nonce, ctx.transaction_id) {
        // Defensive: submission arms the lease for every release it dispatches, so
        // a missing one means we cannot prove ownership and must not pay out.
        let Some(expected_updated_at) = state.release_leases.get(&nonce).copied() else {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "release_missing_claim_lease"])
                .inc();
            error!(
                transaction_id = txid,
                nonce, "No ownership lease held for release; aborting before broadcast"
            );
            state.in_flight_withdrawals.remove(&nonce);
            return;
        };

        match claim_and_persist_or_abort(
            &state.storage,
            pt,
            txid,
            expected_updated_at,
            &signature,
            last_valid_block_height,
            blockhash_slot,
            "release_claim_lost",
        )
        .await
        {
            // Each attempt presents the previous claim's token, so adopt the new
            // one before any retry re-enters here.
            SignatureClaim::Owned(lease) => {
                state.release_leases.insert(nonce, lease);
            }
            SignatureClaim::Lost | SignatureClaim::Failed => {
                // Nothing was broadcast, so this nonce is not in flight and must not
                // keep holding the rotation barrier. The row stays Processing for the
                // recovery worker either way.
                state.in_flight_withdrawals.remove(&nonce);
                return;
            }
        }
    }

    match send_signed(&state.rpc_client, &transaction, retry_policy).await {
        // send_signed returns the same signature we already persisted; keep using it.
        Ok(_) => {
            info!("Transaction sent with signature: {}", signature);

            // Stash the in-flight signature only after a successful broadcast. A send
            // that never reached the network (e.g. a failed simulation) thus leaves no
            // stashed signature, so a permanent failure routes to ManualReview rather
            // than a deferred remint, preserving the pre-existing failure semantics.
            if let Some(nonce) = ctx.withdrawal_nonce {
                state
                    .pending_signatures
                    .entry(nonce)
                    .or_default()
                    .push(PendingSig {
                        signature,
                        last_valid_block_height,
                        blockhash_slot: Some(blockhash_slot),
                    });
            }

            let commitment_config = CommitmentConfig::confirmed();

            let result = check_transaction_status(
                state.rpc_client.clone(),
                &signature,
                commitment_config,
                extra_error_checks_policy,
                state.confirmation_poll_interval_ms,
            )
            .await;

            let result_label = match &result {
                Ok(ConfirmationResult::Confirmed) => "success",
                _ => "failure",
            };
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, result_label])
                .observe(send_start.elapsed().as_secs_f64());

            handle_confirmation_result(
                state,
                result,
                signature,
                compute_unit_price,
                ctx,
                instruction,
                retry_policy,
                extra_error_checks_policy,
                storage_tx,
            )
            .await;
        }
        Err(e) => {
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "error"])
                .observe(send_start.elapsed().as_secs_f64());
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "rpc_send_error"])
                .inc();
            error!("Failed to send transaction: {}", e);
            handle_permanent_failure(state, ctx, storage_tx, &e.to_string()).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_confirmation_result<'a>(
    state: &'a mut SenderState,
    result: Result<ConfirmationResult, crate::error::TransactionError>,
    signature: Signature,
    compute_unit_price: Option<u64>,
    ctx: &'a TransactionContext,
    instruction: InstructionWithSigners,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: &'a ExtraErrorCheckPolicy,
    storage_tx: &'a mpsc::Sender<TransactionStatusUpdate>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let pt = state.program_type.as_label();
        match result {
            Ok(ConfirmationResult::Confirmed) => {
                handle_success(state, ctx, signature, storage_tx).await;
            }
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceAlreadyUsed,
            ))) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "nonce_already_used"])
                    .inc();
                handle_nonce_already_used(state, ctx, signature, storage_tx).await;
            }
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration,
            ))) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "nonce_outside_generation"])
                    .inc();
                handle_nonce_outside_generation(state, ctx, signature, instruction, storage_tx)
                    .await;
            }
            Ok(ConfirmationResult::MintNotInitialized) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "mint_not_initialized"])
                    .inc();
                let Some(txn_id) = ctx.transaction_id else {
                    error!("MintNotInitialized error without transaction_id");
                    handle_permanent_failure(state, ctx, storage_tx, "Mint initialization failed")
                        .await;
                    return;
                };
                if !state.mint_builders.contains_key(&txn_id) {
                    error!("MintNotInitialized error for non-Mint transaction");
                    handle_permanent_failure(state, ctx, storage_tx, "Unexpected mint error").await;
                    return;
                }
                warn!(
                    "Mint not initialized — running JIT verdict for txn {}",
                    txn_id
                );
                match try_jit_mint_initialization(state, txn_id, instruction.clone()).await {
                    JitOutcome::Retry(new_instruction) => {
                        let Some(lease) = ctx.deposit_claim_lease else {
                            metrics::OPERATOR_TRANSACTION_ERRORS
                                .with_label_values(&[pt, "jit_missing_claim_lease"])
                                .inc();
                            warn!(
                                transaction_id = txn_id,
                                "JIT retry missing deposit claim lease; leaving row Processing for recovery"
                            );
                            return;
                        };
                        // Journal the retry signature through the ownership claim
                        // before broadcast. Awaited inline since this rare retry is
                        // already off the hot path.
                        let Ok(permit) = Arc::clone(&state.semaphore).try_acquire_owned() else {
                            metrics::OPERATOR_TRANSACTION_ERRORS
                                .with_label_values(&[pt, "in_flight_cap_exceeded"])
                                .inc();
                            warn!(
                                transaction_id = txn_id,
                                "In-flight cap reached; deferring JIT retry, row stays Processing"
                            );
                            return;
                        };
                        info!(
                            "JIT verdict: Retry — re-issuing mint via write-ahead fire-and-store"
                        );
                        fire_and_store_task(
                            state.rpc_client.clone(),
                            state.storage.clone(),
                            state.in_flight.clone(),
                            state.program_type,
                            new_instruction,
                            compute_unit_price,
                            ctx.clone(),
                            retry_policy,
                            mint_extra_error_checks_policy(),
                            storage_tx.clone(),
                            SendDurability::Recoverable {
                                deposit_expected_updated_at: lease,
                            },
                            permit,
                        )
                        .await;
                    }
                    JitOutcome::ManualReview(reason) => {
                        metrics::OPERATOR_TRANSACTION_ERRORS
                            .with_label_values(&[pt, "mint_jit_manual_review"])
                            .inc();
                        error!("JIT verdict: ManualReview — {}", reason);
                        send_guaranteed(
                            storage_tx,
                            TransactionStatusUpdate {
                                transaction_id: txn_id,
                                trace_id: ctx.trace_id.clone(),
                                status: TransactionStatus::ManualReview,
                                counterpart_signature: None,
                                processed_at: Some(Utc::now()),
                                error_message: Some(reason),
                                remint_signature: None,
                                remint_attempted: false,
                            },
                            "transaction status update",
                        )
                        .await
                        .ok();
                        // Release the cached MintToBuilder so it doesn't
                        // linger past the terminal transition. For deposits
                        // ctx.withdrawal_nonce is None, so the remint /
                        // pending_signatures cleanup is a no-op; Mirrors
                        // the cleanup pattern in handle_permanent_failure.
                        cleanup_failed_transaction(state, ctx.withdrawal_nonce);
                        state.mint_builders.remove(&txn_id);
                    }
                    // Only deposits reach this block: the guard above proves a
                    // cached mint builder, and those are cached for the deposit
                    // mint alone. So there is no nonce-keyed state to unwind
                    // and no withdrawal remint to compensate here.
                    JitOutcome::Transient(reason) => {
                        requeue_deposit_after_jit(state, txn_id, &signature, &reason).await;
                    }
                }
            }
            Ok(ConfirmationResult::Retry) => match retry_policy {
                RetryPolicy::None => {
                    metrics::OPERATOR_TRANSACTION_ERRORS
                        .with_label_values(&[pt, "confirmation_timeout_non_idempotent"])
                        .inc();
                    error!("Confirmation failed for non-idempotent operation - status unknown, cannot retry");
                    handle_permanent_failure(
                        state,
                        ctx,
                        storage_tx,
                        "Confirmation failed - transaction status unknown, unsafe to retry",
                    )
                    .await;
                }
                RetryPolicy::Idempotent => {
                    metrics::OPERATOR_TRANSACTION_ERRORS
                        .with_label_values(&[pt, "confirmation_timeout"])
                        .inc();
                    warn!("Confirmation failed for idempotent operation - retrying (nonce protects against duplicates)");
                    send_and_confirm(
                        state,
                        instruction,
                        compute_unit_price,
                        ctx,
                        retry_policy,
                        extra_error_checks_policy,
                        storage_tx,
                    )
                    .await;
                }
            },
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::UnexpectedGeneration,
            ))) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "rotation_already_landed"])
                    .inc();
                // A rotation already advanced the generation on-chain, so this one
                // was a duplicate and the window is open either way. There is no
                // local index to resync, so the rejection needs no repair: the next
                // rotation reads the generation fresh.
                warn!("RotateBitmap rejected: the generation already advanced on-chain");
                // The refusal says nothing about the next rotation, so charging it would wedge every one after it.
                clear_rotation_retry_state(state, ctx);
            }
            Ok(ConfirmationResult::Failed(program_error)) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "program_error"])
                    .inc();
                error!("Other program error: {:?}", program_error);
                handle_permanent_failure(state, ctx, storage_tx, &format!("{:?}", program_error))
                    .await;
            }
            Err(e) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "confirmation_error"])
                    .inc();
                error!("Confirmation error: {}", e);
                handle_permanent_failure(state, ctx, storage_tx, &e.to_string()).await;
            }
        }
    })
}

/// Handle successful transaction confirmation
pub(super) async fn handle_success(
    state: &mut SenderState,
    ctx: &TransactionContext,
    signature: Signature,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    info!("Transaction confirmed: {}", signature);
    clear_rotation_retry_state(state, ctx);

    // Handle ReleaseFunds (withdrawal nonce-based) transactions
    if let Some(nonce) = ctx.withdrawal_nonce {
        state.in_flight_withdrawals.remove(&nonce);
        state.retry_counts.remove(&nonce);
        state.remint_cache.remove(&nonce);
        state.pending_signatures.remove(&nonce);
        state.release_leases.remove(&nonce);
        info!("Cleaned up state for withdrawal_nonce {}", nonce);

        metrics::OPERATOR_MINTS_SENT
            .with_label_values(&[state.program_type.as_label()])
            .inc();

        if let Some(txn_id) = ctx.transaction_id {
            send_guaranteed(
                storage_tx,
                TransactionStatusUpdate {
                    transaction_id: txn_id,
                    trace_id: ctx.trace_id.clone(),
                    status: TransactionStatus::Completed,
                    counterpart_signature: Some(signature.to_string()),
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
    }
    // Handle Mint (transaction_id-based) transactions
    else if let Some(transaction_id) = ctx.transaction_id {
        info!("Updating database for transaction_id {}", transaction_id);

        metrics::OPERATOR_MINTS_SENT
            .with_label_values(&[state.program_type.as_label()])
            .inc();

        cleanup_mint_builder(state, Some(transaction_id));

        send_guaranteed(
            storage_tx,
            TransactionStatusUpdate {
                transaction_id,
                trace_id: ctx.trace_id.clone(),
                status: TransactionStatus::Completed,
                counterpart_signature: Some(signature.to_string()),
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
    // Handle RotateBitmap, named by its kind because an InitializeMint arrives here with the same empty ids.
    //
    // The rotation was bound to the generation the cache holds, and the program
    // accepts it only from exactly that generation, so a confirmation moves both
    // the chain and the cache on by one. Deriving the new value from the old one
    // rather than from the rotation cannot outrun the chain: an unknown cache
    // stays unknown and is resolved by the next read.
    else if ctx.kind == TransactionKind::RotateBitmap {
        state.cached_generation = state.cached_generation.map(|generation| generation + 1);
        info!(
            generation = state.cached_generation,
            "Bitmap rotation complete"
        );
    }
}

/// Route a release the program rejected because its nonce bit was already set.
///
/// A set bit is proof the nonce was consumed, so unlike an ordinary failure there
/// is nothing to wait out: the only open question is which of our broadcasts did
/// it. The existing signature classifier answers that, and skipping the finality
/// delay is safe precisely because the bit already settled the outcome.
pub(super) async fn handle_nonce_already_used(
    state: &mut SenderState,
    ctx: &TransactionContext,
    signature: Signature,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let mut signatures = ctx
        .withdrawal_nonce
        .and_then(|nonce| state.pending_signatures.get(&nonce).cloned())
        .unwrap_or_default();

    // The in-memory stash is empty after a restart, but every release persists
    // its signature before broadcast, so the durable record can still say which
    // of our sends consumed the nonce. Without this fall back, a restart between
    // broadcast and confirmation would send a correctly-paid withdrawal to
    // manual review for want of evidence we already wrote down.
    if signatures.is_empty() {
        if let Some(transaction_id) = ctx.transaction_id {
            signatures =
                super::state::load_persisted_release_signatures(&state.storage, transaction_id)
                    .await;
        }
    }

    if signatures.is_empty() {
        error!(
            nonce = ctx.withdrawal_nonce.map(|n| n as i64),
            "Nonce already consumed on-chain but we broadcast nothing that could have done it"
        );
        send_manual_review(
            state,
            ctx,
            storage_tx,
            "nonce already consumed on-chain with no broadcast signature of ours to account for it",
        )
        .await;
        return;
    }

    match classify_signatures(&state.dest_finality(), &signatures).await {
        SigFinality::Landed(landed) => {
            info!(
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                "Nonce already consumed by our own earlier broadcast; recording it as complete"
            );
            handle_success(state, ctx, landed, storage_tx).await;
        }
        // One of ours may still be the one that landed, so re-check after finality.
        SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
            warn!(
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                "Nonce already consumed, deferring resolution: {reason}"
            );
            handle_permanent_failure(
                state,
                ctx,
                storage_tx,
                &format!("nonce already consumed on-chain; awaiting finality: {reason}"),
            )
            .await;
        }
        // Every broadcast of ours failed yet the nonce is spent, so a human decides.
        SigFinality::Dead => {
            error!(
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                last_signature = %signature,
                "Nonce consumed on-chain but none of our signatures landed"
            );
            send_manual_review(
                state,
                ctx,
                storage_tx,
                "nonce consumed on-chain but none of our broadcast signatures landed",
            )
            .await;
        }
    }
}

/// Where a nonce sits relative to the window the bitmap currently covers.
pub(super) enum GenerationWindow {
    /// The bitmap is on this nonce's generation, so it is releasable now.
    Open,
    /// A later generation owns this nonce; the rotation that opens it is pending.
    NotYetOpen,
    /// The bitmap has rotated past this nonce's generation, which never returns.
    Closed,
}

/// The one place the direction of a generation difference is decided.
///
/// Both the pre-send check and the on-chain rejection handler route on this, and
/// they must not drift: they disagree about what to do with an open window, but
/// never about which window a nonce is in.
pub(super) fn classify_generation(nonce: u64, chain_generation: u64) -> GenerationWindow {
    match (nonce / NONCES_PER_GENERATION).cmp(&chain_generation) {
        std::cmp::Ordering::Equal => GenerationWindow::Open,
        std::cmp::Ordering::Greater => GenerationWindow::NotYetOpen,
        std::cmp::Ordering::Less => GenerationWindow::Closed,
    }
}

/// CAS the row to `Parked` so a release waiting on a rotation has a state that
/// outlives this process, and report whether it worked.
///
/// `false` means the row is not ours to hold, or we could not find out. Either
/// way the caller must not queue: an entry whose row is not parked puts the
/// in-memory queue back in the position of being the only copy of the work,
/// which is the exact state parking exists to prevent. Leaving the row as it is
/// keeps it visible to the recovery sweep.
pub(super) async fn park_release_for_rotation(
    storage: &Storage,
    transaction_id: i64,
    nonce: u64,
) -> bool {
    match storage.try_park_processing(transaction_id).await {
        Ok(true) => true,
        Ok(false) => {
            error!(
                nonce,
                transaction_id,
                "Release is no longer this sender's to park; leaving it for recovery"
            );
            false
        }
        Err(e) => {
            error!(
                nonce,
                transaction_id, "Could not park the waiting release: {e}; leaving it for recovery"
            );
            false
        }
    }
}

/// Route a release the program rejected because its nonce is outside the window
/// the bitmap currently covers.
///
/// The pre-send check withholds most of these before they cost a fee, but it
/// answers from a cache that can be behind the chain, so this arm is still the
/// authority rather than a last resort. Which side of the window the nonce falls
/// on decides everything: ahead of the chain is a timing problem that a rotation
/// fixes, behind it is unrecoverable.
pub(super) async fn handle_nonce_outside_generation(
    state: &mut SenderState,
    ctx: &TransactionContext,
    signature: Signature,
    instruction: InstructionWithSigners,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let Some(nonce) = ctx.withdrawal_nonce else {
        error!("Generation rejection on a transaction that carries no nonce");
        handle_permanent_failure(
            state,
            ctx,
            storage_tx,
            "nonce outside the current bitmap generation",
        )
        .await;
        return;
    };

    let chain_generation = match state.refresh_generation().await {
        Ok(generation) => generation,
        // We cannot tell which side of the window this nonce is on, and the two
        // answers are terminal in opposite directions: one requeues the
        // withdrawal, the other declares it permanently unreleasable.
        //
        // Guessing either way on an unread bitmap risks the wrong terminal state,
        // so leave the row Processing and let the recovery worker decide once a
        // read succeeds.
        Err(e) => {
            error!(
                nonce,
                "Generation rejection but the bitmap could not be read: {e}; leaving row Processing"
            );
            state.in_flight_withdrawals.remove(&nonce);
            return;
        }
    };

    let nonce_generation = nonce / NONCES_PER_GENERATION;

    // An open window here is the rotation landing between the program's refusal
    // and this read: the nonce is releasable right now, so it is retried rather
    // than written off. Only a nonce the chain has rotated past has lost a
    // window that can never come back.
    if !matches!(
        classify_generation(nonce, chain_generation),
        GenerationWindow::Closed
    ) {
        state.in_flight_withdrawals.remove(&nonce);

        let Some(transaction_id) = ctx.transaction_id else {
            error!(
                nonce,
                "No row to park this waiting release against; not queueing it"
            );
            return;
        };
        if !park_release_for_rotation(&state.storage, transaction_id, nonce).await {
            return;
        }

        info!(
            nonce,
            nonce_generation, chain_generation, "Rotation has not landed yet; queuing for retry"
        );
        // This refusal was predictable and says nothing about the withdrawal, so
        // give back the attempt it was charged. Spending the budget here would
        // permanently fail a good withdrawal for the sole reason that its
        // rotation took a few ticks longer than the budget allowed.
        state
            .retry_counts
            .entry(nonce)
            .and_modify(|attempts| *attempts = attempts.saturating_sub(1));
        forget_rejected_signature(state, nonce, ctx.transaction_id, &signature).await;
        state.rotation_retry_queue.push((ctx.clone(), instruction));
        return;
    }

    error!(
        nonce,
        nonce_generation,
        chain_generation,
        "Nonce belongs to a generation the bitmap has already rotated past; it can never be released"
    );
    // The release can never happen, so the user gets their burned tokens back
    // rather than being left holding neither side of the trade. The refusal came
    // from the program itself, which is proof this transaction moved no funds;
    // the deferred path still classifies every signature we broadcast, so an
    // earlier attempt that did land is completed instead of paid twice.
    remint_after_onchain_refusal(
        state,
        ctx,
        storage_tx,
        &ProgramError::GenerationMismatch {
            nonce,
            nonce_generation,
            chain_generation,
        }
        .to_string(),
    )
    .await;
}

/// Drop a signature the chain confirmed it rejected: it moved no funds, so it is not payout evidence worth keeping.
async fn forget_rejected_signature(
    state: &mut SenderState,
    nonce: u64,
    transaction_id: Option<i64>,
    signature: &Signature,
) {
    if let Some(stashed) = state.pending_signatures.get_mut(&nonce) {
        stashed.retain(|pending| pending.signature != *signature);
        if stashed.is_empty() {
            state.pending_signatures.remove(&nonce);
        }
    }

    let Some(transaction_id) = transaction_id else {
        return;
    };
    if let Err(e) = state
        .storage
        .delete_release_signature(transaction_id, &signature.to_string())
        .await
    {
        warn!(
            transaction_id,
            %signature,
            "Could not drop a rejected release signature: {e}"
        );
    }
}

/// Queue the compensating remint for a release the program itself refused.
///
/// The refusal is the one piece of evidence that outlives a rotation. Once the
/// bits for the nonce's window are cleared the bitmap can never answer for it
/// again, so a remint held to the usual gate would defer until it timed out into
/// manual review while the user's funds sat in neither place.
async fn remint_after_onchain_refusal(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    reason: &str,
) {
    defer_remint_after_failure(state, ctx, storage_tx, reason, true).await;
}

/// Terminal escalation for a withdrawal whose outcome a human has to settle.
/// Drops the nonce's caches first so a queued rotation is not held by a row that
/// will never resolve on its own.
///
/// The broadcast signatures are deliberately kept: this escalation happens
/// because the outcome is unknown, and they are the only thing that can still
/// classify it.
pub(super) async fn send_manual_review(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    reason: &str,
) {
    cleanup_failed_transaction(state, ctx.withdrawal_nonce);

    let Some(transaction_id) = ctx.transaction_id else {
        error!("Cannot escalate to manual review without a transaction id: {reason}");
        return;
    };

    send_guaranteed(
        storage_tx,
        TransactionStatusUpdate {
            transaction_id,
            trace_id: ctx.trace_id.clone(),
            status: TransactionStatus::ManualReview,
            counterpart_signature: None,
            processed_at: Some(Utc::now()),
            error_message: Some(reason.to_string()),
            remint_signature: None,
            remint_attempted: false,
        },
        "transaction status update",
    )
    .await
    .ok();
}

/// Leave a persisted transaction Processing after an uncertain terminal outcome.
/// The broadcast may have landed, so a terminal Failed would strand a possibly-funded
/// deposit and drop the signature recovery needs; recovery reconciles it next sweep.
fn leave_processing_for_recovery(
    pt: &str,
    transaction_id: Option<i64>,
    signature: &Signature,
    reason: &str,
) {
    metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[pt, "left_processing_for_recovery"])
        .inc();
    warn!(
        transaction_id,
        signature = %signature,
        "{reason}; leaving row Processing for recovery to reconcile",
    );
}

/// Bounded pre-broadcast requeue for a withdrawal the caller has confirmed
/// stashed no signature. One cap-gated write flips Processing to Pending under
/// the cap and escalates at it; folding the cap into the write means no separate
/// counter read can fail and let the row requeue forever. `retry_counts` is kept
/// so `send_and_confirm`'s attempt cap still bounds the loop.
pub(super) async fn requeue_or_fail_prebroadcast(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    nonce: u64,
    transaction_id: i64,
    reason_at_cap: &str,
) {
    let pt = state.program_type.as_label();

    // Nothing was broadcast, so this nonce is not in flight and must stop
    // holding the rotation barrier. The next attempt puts it back.
    state.in_flight_withdrawals.remove(&nonce);

    match state
        .storage
        .try_requeue_prebroadcast(transaction_id, MAX_RECOVERY_REQUEUE_ATTEMPTS)
        .await
    {
        Ok(RequeueOutcome::Requeued { attempts }) => {
            // Re-inserted by handle_transaction_builder on the next attempt.
            state.remint_cache.remove(&nonce);
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "prebroadcast_requeued"])
                .inc();
            info!(
                transaction_id,
                nonce, attempts, "Requeued withdrawal to Pending after pre-broadcast failure"
            );
        }
        Ok(RequeueOutcome::AtCap) => {
            // Keep remint_cache: handle_permanent_failure consumes it to route a
            // no-signature withdrawal to ManualReview rather than a bare Failed.
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "prebroadcast_requeue_cap"])
                .inc();
            handle_permanent_failure(state, ctx, storage_tx, reason_at_cap).await;
        }
        Ok(RequeueOutcome::NotProcessing) => {
            state.remint_cache.remove(&nonce);
            warn!(
                transaction_id,
                nonce, "Pre-broadcast requeue skipped: row no longer Processing"
            );
        }
        Err(e) => {
            // Nothing was requeued, so the row stays Processing for recovery. A
            // loop would need a successful requeue, so there is none.
            state.remint_cache.remove(&nonce);
            warn!(
                transaction_id,
                nonce, "Pre-broadcast requeue write failed, row left Processing for recovery: {e}"
            );
        }
    }
}

/// Re-arm a deposit whose JIT mint initialization could not be completed yet.
/// The `mint_to` was already broadcast and failed on chain, and its signature is
/// still journaled; the helper only adds an `InitializeMint`, which moves no
/// balance and is idempotent, so re-arming sends nothing value-bearing.
///
/// No status is written on any branch: the cap, a raced row and a failed write
/// all leave the row to the recovery sweep, which classifies the deposit
/// on-chain before escalating to a human.
pub(super) async fn requeue_deposit_after_jit(
    state: &mut SenderState,
    txn_id: i64,
    signature: &Signature,
    reason: &str,
) {
    let pt = state.program_type.as_label();
    metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[pt, "mint_jit_transient"])
        .inc();

    // The row leaves this task on every branch below and the next pickup builds
    // its own builder, so the cached one would only go stale here.
    state.mint_builders.remove(&txn_id);

    match state
        .storage
        .try_requeue_prebroadcast(txn_id, MAX_RECOVERY_REQUEUE_ATTEMPTS)
        .await
    {
        Ok(RequeueOutcome::Requeued { attempts }) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "prebroadcast_requeued"])
                .inc();
            warn!(
                transaction_id = txn_id,
                attempts, "JIT verdict: transient - requeued deposit to Pending: {reason}"
            );
        }
        // The capped write still matches the row, so its `updated_at` trigger
        // fires and the staleness clock restarts. That delays the recovery sweep
        // by one window, the cost of keeping the cap inside a single statement.
        Ok(RequeueOutcome::AtCap) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "prebroadcast_requeue_cap"])
                .inc();
            leave_processing_for_recovery(
                pt,
                Some(txn_id),
                signature,
                &format!("JIT mint initialization still failing at the requeue cap ({reason})"),
            );
        }
        // Someone else advanced the row, so it is not Processing and the recovery
        // sweep will not look at it. Reporting it as left-for-recovery would send
        // an on-call after a reconciliation that never runs.
        Ok(RequeueOutcome::NotProcessing) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "mint_jit_requeue_raced"])
                .inc();
            warn!(
                transaction_id = txn_id,
                signature = %signature,
                "JIT requeue skipped, row no longer Processing and now owned elsewhere: {reason}",
            );
        }
        Err(e) => leave_processing_for_recovery(
            pt,
            Some(txn_id),
            signature,
            &format!("JIT requeue write failed ({e}); underlying verdict: {reason}"),
        ),
    }
}

/// The ids a pre-broadcast requeue needs, or `None` when this transaction
/// cannot take one: it is not a withdrawal, or an earlier attempt already
/// broadcast a signature for the nonce and the release may have landed.
fn prebroadcast_requeue_target(
    state: &SenderState,
    ctx: &TransactionContext,
) -> Option<(u64, i64)> {
    let (Some(nonce), Some(transaction_id)) = (ctx.withdrawal_nonce, ctx.transaction_id) else {
        return None;
    };
    state
        .pending_signatures
        .get(&nonce)
        .is_none_or(|sigs| sigs.is_empty())
        .then_some((nonce, transaction_id))
}

/// Handle permanent transaction failure with deferred remint for withdrawals.
///
/// For withdrawal transactions: removes remint info from cache, runs cleanup,
/// then queues a deferred remint that will execute after the Solana finality
/// window passes. This prevents double-spend if the original withdrawal lands
/// on-chain after our polling window.
///
/// For non-withdrawal transactions: delegates to send_fatal_error.
pub(super) async fn handle_permanent_failure(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    error_msg: &str,
) {
    // A rotation has no row to fail and no burn to refund, so it is re-armed instead.
    if ctx.kind == TransactionKind::RotateBitmap {
        rearm_failed_rotation(state, error_msg);
        return;
    }

    defer_remint_after_failure(state, ctx, storage_tx, error_msg, false).await;
}

/// `handle_permanent_failure`, plus whether the program itself refused the
/// release. The refusal is written in the same storage call that queues the
/// refund and restored with it, so it decides the bitmap gate on this run and
/// on every run after a restart.
async fn defer_remint_after_failure(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    error_msg: &str,
    release_refused_on_chain: bool,
) {
    clear_rotation_retry_state(state, ctx);

    // Extract remint info BEFORE cleanup destroys builder cache
    let remint_info = ctx
        .withdrawal_nonce
        .and_then(|nonce| state.remint_cache.remove(&nonce));

    // Clear the per-nonce stash. It is no longer what the finality gate reads,
    // so it is only in-memory bookkeeping here, restored below if this failure
    // cannot be handed off.
    let stashed = ctx
        .withdrawal_nonce
        .and_then(|nonce| state.pending_signatures.remove(&nonce))
        .unwrap_or_default();

    cleanup_failed_transaction(state, ctx.withdrawal_nonce);

    let Some(info) = remint_info else {
        // Not a withdrawal, so use the normal fatal error path
        send_fatal_error(storage_tx, ctx, error_msg).await;
        return;
    };

    // Guard before the journal read below, which is keyed by transaction_id.
    // `transaction_id` is always `Some` for a withdrawal: only `ReleaseFunds`
    // populates `remint_cache`, and it always carries a DB row. This prevents
    // queuing a `PendingRemint` with no record, which a restart would lose.
    let Some(transaction_id) = ctx.transaction_id else {
        error!(
            "Cannot defer remint for nonce {:?}, no transaction_id, entry would be unrecoverable on restart",
            ctx.withdrawal_nonce,
        );
        return;
    };

    // Every attempt is journaled before its send, so the journal is the only
    // complete record of what may still land: a send that errored ambiguously
    // never reaches the stash, and classifying the stash alone can call a live
    // release dead and remint on top of it. Recovery reads the same table.
    let signatures = match load_pending_sigs(&state.storage, transaction_id).await {
        Ok(sigs) => sigs,
        // Without the broadcast set there is nothing to prove the release dead
        // against, so leave the row Processing for recovery, which reads the
        // same journal.
        Err(reason) => {
            error!(
                transaction_id,
                "Cannot read the release-signature journal, leaving the row to recovery: {reason}"
            );
            restore_remint_material(state, ctx, info, stashed);
            return;
        }
    };

    // Zero signatures means there is nothing of our own to classify, and the RPC
    // may still have broadcast before erroring. Nothing available here is
    // positive evidence that no payout occurred: an absent release record only
    // ever refuses a refund, it never permits one, and a bitmap that has rotated
    // cannot answer for the nonce at all. So a human settles it.
    if signatures.is_empty() {
        error!(
            "No signatures to verify for nonce {:?}, cannot safely remint, sending to ManualReview",
            ctx.withdrawal_nonce,
        );
        send_guaranteed(
            storage_tx,
            TransactionStatusUpdate {
                transaction_id,
                trace_id: ctx.trace_id.clone(),
                status: TransactionStatus::ManualReview,
                counterpart_signature: None,
                processed_at: Some(Utc::now()),
                error_message: Some(format!(
                    "{} | no signatures to verify, remint unsafe",
                    error_msg
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

    let deadline = Utc::now() + chrono::Duration::from_std(FINALITY_SAFETY_DELAY).unwrap();

    // Atomically transition to PendingRemint, persisting the withdrawal signatures
    // needed for the finality check. This replaces the previous Failed write —
    // keeping status as Processing until the remint resolves avoids partial state
    // if the operator crashes during the finality window.
    let sig_strings: Vec<String> = signatures
        .iter()
        .map(|pending_sig| pending_sig.signature.to_string())
        .collect();
    let lvbhs: Vec<i64> = signatures
        .iter()
        .map(|pending_sig| pending_sig.last_valid_block_height as i64)
        .collect();

    // Retry the handoff: a statement timeout, deadlock or dropped connection is
    // transient, and the compensation material is still held in the locals
    // above, so nothing is given up while retrying.
    let write_result = with_storage_backoff("pending remint transition", transaction_id, || {
        state.storage.set_pending_remint(
            transaction_id,
            sig_strings.clone(),
            lvbhs.clone(),
            deadline,
            release_refused_on_chain,
        )
    })
    .await;

    if let Err(e) = write_result {
        // The error is ambiguous: the write may or may not have committed.
        // Read the row back; its status says who owns this withdrawal now.
        let observed = with_storage_backoff("pending remint status read", transaction_id, || {
            state.storage.get_transaction_status(transaction_id)
        })
        .await;

        match observed {
            // It committed and only the acknowledgement was lost, so this sender
            // still owns the remint. Fall through and queue it.
            Ok(Some(TransactionStatus::PendingRemint)) => {
                warn!(
                    transaction_id,
                    "set_pending_remint failed but the row is PendingRemint, treating the handoff as committed: {e}"
                );
            }
            // Nothing committed. Leave the row Processing for the recovery
            // worker, which reloads the same journal and completes, requeues or
            // quarantines it. Queuing the remint here as well could pay twice.
            Ok(Some(TransactionStatus::Processing)) => {
                error!(
                    transaction_id,
                    "Failed to persist PendingRemint, leaving the row to recovery: {e}"
                );
                restore_remint_material(state, ctx, info, stashed);
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[
                        state.program_type.as_label(),
                        "pending_remint_persist_failed",
                    ])
                    .inc();
                return;
            }
            // Another writer already moved the row, so it owns the outcome.
            Ok(Some(status)) => {
                warn!(
                    transaction_id,
                    "Failed to persist PendingRemint and the row is already {status:?}, leaving it alone: {e}"
                );
                return;
            }
            // Neither state can be established, so retry the write itself: it is
            // idempotent for this payload, which makes it the only probe that is
            // safe whichever state committed. A terminal status would strand the
            // withdrawal instead, since no sweep selects ManualReview and the
            // material pulled from the caches above is the only live copy.
            unresolved => {
                error!(
                    "Failed to persist PendingRemint for transaction {transaction_id} and could not read it back ({unresolved:?}), retrying the handoff: {e}"
                );
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[
                        state.program_type.as_label(),
                        "pending_remint_state_unknown",
                    ])
                    .inc();
                match state
                    .storage
                    .set_pending_remint(
                        transaction_id,
                        sig_strings,
                        lvbhs,
                        deadline,
                        release_refused_on_chain,
                    )
                    .await
                {
                    Ok(()) => warn!(
                        transaction_id,
                        "The handoff retry committed against an unreadable row; driving the remint from this sender"
                    ),
                    // Either the row is no longer ours to take or the database is
                    // still unreachable. Both leave it where it is, so hold the
                    // material: a row that never left Processing belongs to the
                    // recovery sweep, which classifies the release on-chain first.
                    Err(retry_err) => {
                        error!(
                            transaction_id,
                            "Could not establish the PendingRemint handoff, holding the remint info and signature stash for recovery: {retry_err}"
                        );
                        restore_remint_material(state, ctx, info, stashed);
                        return;
                    }
                }
            }
        }
    }

    info!(
        "Remint deferred for finality check ({}s) — {} signature(s) to verify for nonce {:?}",
        FINALITY_SAFETY_DELAY.as_secs(),
        signatures.len(),
        ctx.withdrawal_nonce,
    );

    state.pending_remints.push(PendingRemint {
        ctx: ctx.clone(),
        remint_info: info,
        signatures,
        original_error: error_msg.to_string(),
        deadline,
        finality_check_attempts: 0,
        release_refused_on_chain,
        coverage_slot: None,
    });
}

/// Put back the compensation material a failed handoff pulled out of the caches,
/// so a later attempt on the same nonce still has it. A stale entry is only ever
/// read by such an attempt, so holding it costs nothing.
fn restore_remint_material(
    state: &mut SenderState,
    ctx: &TransactionContext,
    info: WithdrawalRemintInfo,
    stashed: Vec<PendingSig>,
) {
    if let Some(nonce) = ctx.withdrawal_nonce {
        state.remint_cache.insert(nonce, info);
        state.pending_signatures.insert(nonce, stashed);
    }
}

/// Sign, send, and store a Mint or InitializeMint tx in `state.in_flight`.
///
/// Called from the `route_poll_results` retry path where the caller already holds a
/// semaphore permit (carried inside the timed-out `InFlightTx`).  The permit transfers
/// to the new `InFlightTx` on success, or is dropped (slot released) on send failure.
///
/// New incoming transactions use `spawn_fire_and_store` instead, which acquires the
/// permit and offloads the blocking send to a background task.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fire_and_store(
    state: &mut SenderState,
    instruction: InstructionWithSigners,
    compute_unit_price: Option<u64>,
    ctx: TransactionContext,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: ExtraErrorCheckPolicy,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    resend_count: u32,
    permit: OwnedSemaphorePermit,
) {
    let pt = state.program_type.as_label();
    let send_start = std::time::Instant::now();

    match sign_and_send_transaction(state.rpc_client.clone(), instruction.clone(), retry_policy)
        .await
    {
        Ok((signature, _last_valid_block_height, _blockhash_slot)) => {
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "in_flight"])
                .observe(send_start.elapsed().as_secs_f64());
            info!("Transaction sent: {}", signature);
            // push() also notifies the poll task if it is waiting on an empty queue.
            // Only InitializeMint is resent here, and it mints no balance, so no persist.
            state.in_flight.push(InFlightTx {
                signature,
                ctx,
                instruction,
                compute_unit_price,
                retry_policy,
                extra_error_checks_policy,
                poll_attempts: 0,
                resend_count,
                persisted: false,
                permit,
            });
        }
        Err(e) => {
            drop(permit);
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "error"])
                .observe(send_start.elapsed().as_secs_f64());
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "rpc_send_error"])
                .inc();
            error!("Failed to send transaction (fire-and-forget): {}", e);
            handle_permanent_failure(state, &ctx, storage_tx, &e.to_string()).await;
        }
    }
}

/// Acquire a semaphore permit and spawn a background task that signs and sends
/// the transaction without blocking the sender loop's `recv` arm.
///
/// The permit is held from acquisition until the entry reaches a terminal state:
///  - **Success**: permit moves into `InFlightTx` in `in_flight`; dropped when the
///    poll task (or drain loop) confirms the tx.
///  - **Send error**: permit dropped before reporting the failure to storage.
///
/// Returns `false` if the semaphore is already at `MAX_IN_FLIGHT` capacity.  The DB
/// status is left unchanged so the fetcher re-emits the transaction on the next poll
/// cycle.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_fire_and_store(
    state: &SenderState,
    instruction: InstructionWithSigners,
    compute_unit_price: Option<u64>,
    ctx: TransactionContext,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: ExtraErrorCheckPolicy,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    durability: SendDurability,
) -> bool {
    let permit = match Arc::clone(&state.semaphore).try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "in_flight_cap_exceeded"])
                .inc();
            warn!(
                "In-flight cap ({MAX_IN_FLIGHT}) reached — skipping send for txn {:?}; \
                 DB status unchanged, will be re-fetched",
                ctx.transaction_id,
            );
            return false;
        }
    };

    let rpc_client = state.rpc_client.clone();
    let in_flight = state.in_flight.clone();
    let program_type = state.program_type;
    let storage = state.storage.clone();

    tokio::spawn(fire_and_store_task(
        rpc_client,
        storage,
        in_flight,
        program_type,
        instruction,
        compute_unit_price,
        ctx,
        retry_policy,
        extra_error_checks_policy,
        storage_tx,
        durability,
        permit,
    ));

    true
}

/// Build, sign, claim the row and persist the signature when `durability` is
/// `Recoverable`, then broadcast and stash the in-flight tx. Every pre-broadcast
/// failure on that path leaves the row Processing for recovery. Split from
/// `spawn_fire_and_store` so tests can await it directly without `tokio::spawn`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fire_and_store_task(
    rpc_client: Arc<RpcClientWithRetry>,
    storage: Arc<Storage>,
    in_flight: Arc<InFlightQueue>,
    program_type: ProgramType,
    instruction: InstructionWithSigners,
    compute_unit_price: Option<u64>,
    mut ctx: TransactionContext,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: ExtraErrorCheckPolicy,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    durability: SendDurability,
    permit: OwnedSemaphorePermit,
) {
    let pt = program_type.as_label();
    let send_start = std::time::Instant::now();

    let (transaction, signature, last_valid_block_height, blockhash_slot) = match build_and_sign(
        &rpc_client,
        instruction.clone(),
    )
    .await
    {
        Ok(signed) => signed,
        Err(e) => {
            drop(permit);
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "error"])
                .observe(send_start.elapsed().as_secs_f64());
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "build_sign_error"])
                .inc();
            // Blockhash fetch and signing both run before any signature exists,
            // so a Recoverable mint that fails here minted nothing; Failed is a
            // status no worker re-claims, which would strand the funded deposit.
            match durability {
                SendDurability::Recoverable { .. } => {
                    metrics::OPERATOR_TRANSACTION_ERRORS
                        .with_label_values(&[pt, "left_processing_for_recovery"])
                        .inc();
                    warn!(
                            transaction_id = ctx.transaction_id,
                            "Build/sign failed for a recoverable mint before broadcast; leaving row Processing for recovery: {}",
                            e
                        );
                }
                SendDurability::Terminal => {
                    error!("Failed to build/sign transaction (fire-and-forget): {}", e);
                    send_fatal_error(&storage_tx, &ctx, &e.to_string()).await;
                }
            }
            return;
        }
    };

    let persisted = match durability {
        SendDurability::Recoverable {
            deposit_expected_updated_at,
        } => {
            // Persist required but no transaction_id to key on: abort before broadcasting an unrecoverable mint.
            let Some(txid) = ctx.transaction_id else {
                drop(permit);
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "pre_send_persist_error"])
                    .inc();
                error!("Persist required but transaction has no id; aborting before broadcast");
                return;
            };
            match claim_and_persist_or_abort(
                &storage,
                pt,
                txid,
                deposit_expected_updated_at,
                &signature,
                last_valid_block_height,
                blockhash_slot,
                "deposit_ownership_lost",
            )
            .await
            {
                // A re-fire of this same deposit presents the token this claim won.
                SignatureClaim::Owned(lease) => {
                    ctx.deposit_claim_lease = Some(lease);
                    true
                }
                SignatureClaim::Lost | SignatureClaim::Failed => {
                    drop(permit);
                    return;
                }
            }
        }
        SendDurability::Terminal => false,
    };

    match send_signed(&rpc_client, &transaction, retry_policy).await {
        // send_signed returns the same signature we already hold; keep using it.
        Ok(_) => {
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "in_flight"])
                .observe(send_start.elapsed().as_secs_f64());
            info!("Transaction sent: {}", signature);
            in_flight.push(InFlightTx {
                signature,
                ctx,
                instruction,
                compute_unit_price,
                retry_policy,
                extra_error_checks_policy,
                poll_attempts: 0,
                resend_count: 0,
                persisted,
                permit,
            });
        }
        Err(e) => {
            drop(permit);
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "error"])
                .observe(send_start.elapsed().as_secs_f64());
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "rpc_send_error"])
                .inc();
            error!("Failed to send transaction (fire-and-forget): {}", e);
            // Even a preflight rejection can be a stale-node false negative, so a persisted
            // mint may already have landed. A terminal Failed would strand a funded deposit
            // and drop the signature recovery reconciles against; leave it for recovery.
            if persisted {
                leave_processing_for_recovery(
                    pt,
                    ctx.transaction_id,
                    &signature,
                    "send error after write-ahead persist",
                );
            } else {
                send_fatal_error(&storage_tx, &ctx, &e.to_string()).await;
            }
        }
    }
}

/// Route a batch of `(InFlightTx, Option<TransactionStatus>)` pairs returned by a
/// `getSignatureStatuses` call.
///
/// Called from both `poll_in_flight` (test / shutdown drain path) and the sender
/// loop's `poll_result_rx` arm (normal production path).
///
/// Unconfirmed entries are pushed back into `state.in_flight`, which automatically
/// re-arms the poll task's `Notify` for the next cycle.
pub(super) async fn route_poll_results(
    state: &mut SenderState,
    results: Vec<(
        InFlightTx,
        Option<solana_transaction_status::TransactionStatus>,
    )>,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    for (mut tx, status_opt) in results {
        match status_opt {
            Some(status) if status.satisfies_commitment(CommitmentConfig::finalized()) => {
                // Free this finalized tx's in-flight slot now so a continuation (the JIT
                // mint retry) can reuse it instead of being refused when in-flight is full.
                drop(tx.permit);

                let result = if let Some(err) = &status.err {
                    let mut extra_result = None;
                    if let ExtraErrorCheckPolicy::Extra(ref checks) = tx.extra_error_checks_policy {
                        for check in checks.iter() {
                            if let Some(r) = check(err) {
                                extra_result = Some(Ok(r));
                                break;
                            }
                        }
                    }
                    extra_result
                        .unwrap_or_else(|| Ok(ConfirmationResult::Failed(parse_program_error(err))))
                } else {
                    Ok(ConfirmationResult::Confirmed)
                };

                handle_confirmation_result(
                    state,
                    result,
                    tx.signature,
                    tx.compute_unit_price,
                    &tx.ctx,
                    tx.instruction,
                    tx.retry_policy,
                    &tx.extra_error_checks_policy,
                    storage_tx,
                )
                .await;
            }
            _ => {
                tx.poll_attempts += 1;
                if tx.poll_attempts >= MAX_POLL_ATTEMPTS_CONFIRMATION {
                    match tx.retry_policy {
                        RetryPolicy::None => {
                            metrics::OPERATOR_TRANSACTION_ERRORS
                                .with_label_values(&[
                                    state.program_type.as_label(),
                                    "confirmation_timeout_non_idempotent",
                                ])
                                .inc();
                            if tx.persisted {
                                leave_processing_for_recovery(
                                    state.program_type.as_label(),
                                    tx.ctx.transaction_id,
                                    &tx.signature,
                                    "confirmation timeout after write-ahead persist",
                                );
                            } else {
                                warn!(
                                    "Confirmation timeout for non-idempotent tx {} after {} polls - permanent failure",
                                    tx.signature, tx.poll_attempts,
                                );
                                handle_permanent_failure(
                                    state,
                                    &tx.ctx,
                                    storage_tx,
                                    "Confirmation failed - transaction status unknown, unsafe to retry",
                                )
                                .await;
                            }
                        }
                        RetryPolicy::Idempotent => {
                            // This resend broadcasts a fresh unpersisted signature, so a persisted
                            // mint must never reach it (Mint is RetryPolicy::None); assert it.
                            debug_assert!(
                                !tx.persisted,
                                "a write-ahead-persisted tx must not use the idempotent resend path"
                            );
                            metrics::OPERATOR_TRANSACTION_ERRORS
                                .with_label_values(&[
                                    state.program_type.as_label(),
                                    "confirmation_timeout",
                                ])
                                .inc();

                            let next_resend = tx.resend_count + 1;
                            if next_resend > state.retry_max_attempts {
                                metrics::OPERATOR_TRANSACTION_ERRORS
                                    .with_label_values(&[
                                        state.program_type.as_label(),
                                        "confirmation_timeout_resend_limit",
                                    ])
                                    .inc();
                                if tx.persisted {
                                    leave_processing_for_recovery(
                                        state.program_type.as_label(),
                                        tx.ctx.transaction_id,
                                        &tx.signature,
                                        "resend limit reached after write-ahead persist",
                                    );
                                } else {
                                    warn!(
                                        "Confirmation timeout for idempotent tx {} - resend limit ({}) reached, permanent failure",
                                        tx.signature, state.retry_max_attempts,
                                    );
                                    handle_permanent_failure(
                                        state,
                                        &tx.ctx,
                                        storage_tx,
                                        "Confirmation timeout: resend limit exceeded",
                                    )
                                    .await;
                                }
                            } else {
                                warn!(
                                    "Confirmation timeout for idempotent tx {} after {} polls — re-sending (attempt {}/{})",
                                    tx.signature, tx.poll_attempts, next_resend, state.retry_max_attempts,
                                );
                                fire_and_store(
                                    state,
                                    tx.instruction,
                                    tx.compute_unit_price,
                                    tx.ctx,
                                    tx.retry_policy,
                                    tx.extra_error_checks_policy,
                                    storage_tx,
                                    next_resend,
                                    tx.permit, // transfer permit to new InFlightTx
                                )
                                .await;
                            }
                        }
                    }
                } else {
                    // Still pending — push back into the shared queue.
                    // push() notifies the poll task so it wakes on the next cycle.
                    state.in_flight.push(tx);
                }
            }
        }
    }
}

/// Why a chunked status fetch was rejected. Both variants make the caller reinsert
/// the batch and retry; the split only picks the metric reason label.
#[derive(Debug)]
enum StatusFetchError {
    /// A chunk response length did not equal the request (short or oversized).
    MalformedLength,
    /// The RPC call itself failed after retries.
    Rpc,
}

impl StatusFetchError {
    fn reason(&self) -> &'static str {
        match self {
            StatusFetchError::MalformedLength => "malformed_status_response",
            StatusFetchError::Rpc => "status_poll_rpc_error",
        }
    }
}

/// Fetch statuses in `MAX_SIGS_PER_CALL` chunks. `getSignatureStatuses` is positional, so a
/// chunk whose length differs from the request would misalign every later status; reject it.
/// Returns `Err` on any RPC error or length mismatch so the caller reinserts the batch and retries.
async fn fetch_statuses_checked(
    rpc_client: &RpcClientWithRetry,
    signatures: &[Signature],
) -> Result<Vec<Option<solana_transaction_status::TransactionStatus>>, StatusFetchError> {
    let mut statuses = Vec::with_capacity(signatures.len());
    for chunk in signatures.chunks(MAX_SIGS_PER_CALL) {
        match rpc_client.get_signature_statuses(chunk).await {
            Ok(resp) if resp.value.len() == chunk.len() => statuses.extend(resp.value),
            Ok(resp) => {
                warn!(
                    "getSignatureStatuses returned {} statuses for {} signatures \
                     ({} in-flight) - treating as RPC failure, will retry next tick",
                    resp.value.len(),
                    chunk.len(),
                    signatures.len()
                );
                return Err(StatusFetchError::MalformedLength);
            }
            Err(e) => {
                warn!(
                    "getSignatureStatuses failed ({} in-flight) - will retry next tick: {}",
                    signatures.len(),
                    e
                );
                return Err(StatusFetchError::Rpc);
            }
        }
    }
    Ok(statuses)
}

/// Single-cycle poll: drain the shared queue, call `getSignatureStatuses`, then
/// route results via `route_poll_results`.
///
/// Used by `drain_in_flight` (shutdown) and by tests.  Normal production polling
/// is handled by the dedicated `run_poll_task` task so it doesn't block the send loop.
pub(super) async fn poll_in_flight(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    if state.in_flight.is_empty() {
        return;
    }
    let batch = state.in_flight.drain_all();
    let signatures: Vec<Signature> = batch.iter().map(|t| t.signature).collect();

    let statuses = match fetch_statuses_checked(&state.rpc_client, &signatures).await {
        Ok(s) => s,
        Err(e) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), e.reason()])
                .inc();
            // Reinsert the full batch so the next drain_in_flight iteration retries.
            state.in_flight.push_all(batch);
            return;
        }
    };

    let results: Vec<_> = batch.into_iter().zip(statuses).collect();
    route_poll_results(state, results, storage_tx).await;
}

/// Dedicated poll task: sleeps until entries arrive, then batches
/// `getSignatureStatuses` calls and forwards raw results to the sender loop.
///
/// Running in a separate task means `getSignatureStatuses` RPC latency (~50–200 ms)
/// never blocks the sender from processing new incoming transactions.
///
/// # No busy loop
/// The task waits on `in_flight.notify` (a `tokio::sync::Notify`) before each cycle.
/// Every `InFlightQueue::push` call fires `notify_one`, which stores at most one permit,
/// so the task wakes exactly once per "there is work" event even if many entries are
/// added simultaneously.  When the queue drains to zero and no new entries arrive the
/// task blocks indefinitely — zero CPU while idle.
/// Dedicated async task that owns the confirmation polling loop.
///
/// Confirmed-success entries are handled entirely within this task:
/// the `Completed` storage update is sent and `OPERATOR_MINTS_SENT` is
/// incremented without touching `SenderState`.  Only on-chain errors and
/// confirmation timeouts — rare events — are forwarded to the sender loop
/// via `result_tx` as `PollTaskResult::NeedsRouting`.  Unconfirmed entries
/// are pushed straight back into `in_flight`.
///
/// This means the `Some(results) = poll_result_rx.recv()` arm in the main
/// `select!` loop fires only for exceptions, keeping the common path off the
/// main task entirely.
pub(super) async fn run_poll_task(
    in_flight: Arc<InFlightQueue>,
    result_tx: mpsc::Sender<Vec<PollTaskResult>>,
    rpc_client: Arc<RpcClientWithRetry>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    program_type: ProgramType,
    poll_interval_ms: u64,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    // Reused across poll cycles to avoid per-cycle heap allocation.
    // Signature is Copy ([u8; 64]) so extend() is a plain memcopy.
    let mut signatures: Vec<Signature> = Vec::with_capacity(MAX_IN_FLIGHT);

    loop {
        // Block until at least one entry is present (no busy loop when idle).
        tokio::select! {
            _ = cancellation_token.cancelled() => break,
            _ = in_flight.notify.notified() => {},
        }

        // Sleep the poll interval to batch entries that arrive in quick succession.
        tokio::select! {
            _ = cancellation_token.cancelled() => break,
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)) => {},
        }

        let batch = in_flight.drain_all();
        if batch.is_empty() {
            continue;
        }

        signatures.clear();
        signatures.extend(batch.iter().map(|t| t.signature));

        let statuses = match fetch_statuses_checked(&rpc_client, &signatures).await {
            Ok(s) => s,
            Err(e) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[program_type.as_label(), e.reason()])
                    .inc();
                // Put everything back in one lock acquisition and retry next tick.
                in_flight.push_all(batch);
                continue;
            }
        };

        let mut results: Vec<PollTaskResult> = Vec::with_capacity(batch.len());

        for (mut tx, status_opt) in batch.into_iter().zip(statuses) {
            match status_opt {
                Some(status) if status.satisfies_commitment(CommitmentConfig::finalized()) => {
                    if status.err.is_none() {
                        // ── Confirmed success (hot path) ──────────────────────────────
                        // Handle entirely here — no need to wake the sender loop.
                        metrics::OPERATOR_MINTS_SENT
                            .with_label_values(&[program_type.as_label()])
                            .inc();

                        if let Some(txn_id) = tx.ctx.transaction_id {
                            if storage_tx
                                .send(TransactionStatusUpdate {
                                    transaction_id: txn_id,
                                    trace_id: tx.ctx.trace_id,
                                    status: TransactionStatus::Completed,
                                    counterpart_signature: Some(tx.signature.to_string()),
                                    processed_at: Some(Utc::now()),
                                    error_message: None,
                                    remint_signature: None,
                                    remint_attempted: false,
                                })
                                .await
                                .is_err()
                            {
                                warn!(
                                    "Storage channel closed — Completed update lost for txn {}",
                                    txn_id
                                );
                            }
                        }
                        // Notify sender loop to clean up mint_builders (O(1) HashMap remove).
                        results.push(PollTaskResult::ConfirmedSuccess(tx.ctx.transaction_id));
                    } else {
                        // ── Confirmed with on-chain error ─────────────────────────────
                        // Needs SenderState for error routing (cleanup, remint, etc.).
                        results.push(PollTaskResult::NeedsRouting(Box::new(tx), Some(status)));
                    }
                }
                _ => {
                    // ── Not yet confirmed ─────────────────────────────────────────────
                    // If we're one poll away from MAX, hand to the sender loop so it can
                    // run the timeout branch (which needs SenderState).  Otherwise push
                    // straight back — no result channel traffic needed.
                    if tx.poll_attempts + 1 >= MAX_POLL_ATTEMPTS_CONFIRMATION {
                        // Do NOT increment here; route_poll_results will increment it
                        // to MAX and fire the timeout branch.
                        results.push(PollTaskResult::NeedsRouting(Box::new(tx), None));
                    } else {
                        tx.poll_attempts += 1;
                        in_flight.push(tx);
                    }
                }
            }
        }

        if !results.is_empty() && result_tx.send(results).await.is_err() {
            break; // Sender loop gone — clean up and exit.
        }
    }
}

/// Helper for fatal errors (Failed status, no signature)
pub(super) async fn send_fatal_error(
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ctx: &TransactionContext,
    error_msg: &str,
) {
    if let Some(transaction_id) = ctx.transaction_id {
        send_guaranteed(
            storage_tx,
            TransactionStatusUpdate {
                transaction_id,
                trace_id: ctx.trace_id.clone(),
                status: TransactionStatus::Failed,
                counterpart_signature: None,
                processed_at: Some(Utc::now()),
                error_message: Some(error_msg.to_string()),
                remint_signature: None,
                remint_attempted: false,
            },
            "transaction status update",
        )
        .await
        .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProgramType;
    use crate::operator::sender::test_support::{
        ensure_test_signer, mock_bitmap_account, mock_initialized_mint, mock_with_processing_row,
        push_processing_deposit_row, push_withdrawal_with_nonce, row_status, row_updated_at,
        sender_state as make_sender_state_with_server, sender_state_with_storage,
    };
    use crate::operator::utils::instruction_util::MintToBuilder;
    use crate::operator::utils::instruction_util::{SourceEventId, WithdrawalRemintInfo};
    use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
    use crate::operator::SignerUtil;
    use crate::storage::common::models::DbObservedRelease;
    use crate::storage::common::storage::mock::MockStorage;
    use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
    use solana_keychain::Signer;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    fn dummy_instruction() -> InstructionWithSigners {
        InstructionWithSigners {
            instructions: vec![],
            fee_payer: Pubkey::default(),
            signers: Vec::<&'static Signer>::new(),
            compute_unit_price: None,
            compute_budget: None,
        }
    }

    fn make_sender_state() -> SenderState {
        make_sender_state_with_server("http://localhost:8899")
    }

    fn make_remint_info(txn_id: i64) -> WithdrawalRemintInfo {
        WithdrawalRemintInfo {
            transaction_id: txn_id,
            source_event_id: SourceEventId::new(&format!("sig-{txn_id}"), 0, None),
            trace_id: format!("trace-{txn_id}"),
            mint: solana_sdk::pubkey::Pubkey::new_unique(),
            user: solana_sdk::pubkey::Pubkey::new_unique(),
            user_ata: solana_sdk::pubkey::Pubkey::new_unique(),
            token_program: spl_token::id(),
            amount: 5000,
        }
    }

    // ── handle_permanent_failure ─────────────────────────────────────

    #[tokio::test]
    async fn permanent_failure_non_withdrawal_sends_failed_status() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(42),
            withdrawal_nonce: None, // not a withdrawal
            trace_id: Some("trace-42".to_string()),
            deposit_claim_lease: None,
        };

        handle_permanent_failure(&mut state, &ctx, &storage_tx, "some error").await;

        let update = storage_rx.try_recv().expect("should receive status update");
        assert_eq!(update.transaction_id, 42);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert_eq!(update.error_message.as_deref(), Some("some error"));
        assert!(update.remint_signature.is_none());
    }

    #[tokio::test]
    async fn permanent_failure_withdrawal_no_cache_sends_failed_status() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Withdrawal nonce but nothing in remint_cache
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(7),
            withdrawal_nonce: Some(99),
            trace_id: Some("trace-7".to_string()),
            deposit_claim_lease: None,
        };

        handle_permanent_failure(&mut state, &ctx, &storage_tx, "max retries").await;

        let update = storage_rx.try_recv().expect("should receive status update");
        assert_eq!(update.status, TransactionStatus::Failed);
        assert_eq!(update.error_message.as_deref(), Some("max retries"));
        assert!(update.remint_signature.is_none());
    }

    #[tokio::test]
    async fn permanent_failure_withdrawal_with_cache_defers_remint() {
        // `set_pending_remint` is a compare-and-set from Processing, so the row
        // has to be there for the deferral to persist.
        let mock = mock_with_processing_row(10);
        let sig = Signature::new_unique();
        // The finality gate reads the journal the broadcast wrote.
        mock.insert_release_signature(10, sig.to_string(), 0, None)
            .await
            .unwrap();
        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Populate remint cache and some pending signatures
        state.remint_cache.insert(5, make_remint_info(10));
        state.pending_signatures.insert(
            5,
            vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
            deposit_claim_lease: None,
        };

        handle_permanent_failure(&mut state, &ctx, &storage_tx, "release_funds failed").await;

        // No immediate status update — transaction remains in PendingRemint in DB
        // until process_pending_remints resolves it after the finality window.
        assert!(
            storage_rx.try_recv().is_err(),
            "should NOT send a status update while remint is deferred"
        );

        // Entry should be in pending_remints
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];
        assert_eq!(entry.ctx.transaction_id, Some(10));
        assert_eq!(entry.signatures.len(), 1);
        assert_eq!(entry.signatures[0].signature, sig);
        assert_eq!(entry.original_error, "release_funds failed");
        assert_eq!(entry.finality_check_attempts, 0);

        // remint_cache and pending_signatures should be drained
        assert!(!state.remint_cache.contains_key(&5));
        assert!(!state.pending_signatures.contains_key(&5));
    }

    #[tokio::test]
    async fn permanent_failure_zero_sigs_sends_manual_review() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // With write-ahead, the only zero-signature case is a build/sign failure; it still escalates to ManualReview (a blind remint is unsafe).
        state.remint_cache.insert(5, make_remint_info(10));
        // Note: not inserting into pending_signatures

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
            deposit_claim_lease: None,
        };

        handle_permanent_failure(&mut state, &ctx, &storage_tx, "rpc send error").await;

        // Should go straight to ManualReview — no deferred remint
        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview status");
        assert_eq!(update.transaction_id, 10);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap();
        assert!(
            err.contains("no signatures to verify"),
            "should mention no sigs: {err}"
        );

        // Nothing queued
        assert!(
            state.pending_remints.is_empty(),
            "should not queue deferred remint with zero sigs"
        );
    }

    /// A `Processing` withdrawal row carrying `nonce`, the state every release
    /// the deferred-remint handoff acts on starts from.
    fn processing_withdrawal_mock(transaction_id: i64, nonce: u64) -> MockStorage {
        let mock = MockStorage::new();
        push_withdrawal_with_nonce(
            &mock,
            transaction_id,
            nonce as i64,
            TransactionStatus::Processing,
        );
        mock
    }

    fn release_ctx(transaction_id: i64, nonce: u64) -> TransactionContext {
        TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(transaction_id),
            withdrawal_nonce: Some(nonce),
            trace_id: Some(format!("trace-{transaction_id}")),
            deposit_claim_lease: None,
        }
    }

    /// Every attempt is journaled before its send, so a send that errored
    /// ambiguously never reaches the in-memory stash. Classifying the stash
    /// alone can call a live release dead and remint on top of it.
    #[tokio::test]
    async fn permanent_failure_gate_includes_ambiguously_sent_attempt() {
        let txn_id = 20;
        let nonce = 15;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        let stashed_attempt = Signature::new_unique();
        let ambiguous_attempt = Signature::new_unique();
        for (signature, lvbh) in [(stashed_attempt, 100), (ambiguous_attempt, 200)] {
            mock.insert_release_signature(txn_id, signature.to_string(), lvbh, None)
                .await
                .unwrap();
        }

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        // Only attempt 1 reached the stash: the push runs in the send's Ok arm.
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: stashed_attempt,
                last_valid_block_height: 100,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        let (_, persisted_sigs, persisted_lvbhs, _, _) =
            mock.pending_remint_signatures.lock().unwrap()[0].clone();
        assert!(
            persisted_sigs.contains(&ambiguous_attempt.to_string()),
            "the ambiguously-sent attempt must be persisted for the finality check: {persisted_sigs:?}"
        );
        assert_eq!(
            persisted_lvbhs.iter().max(),
            Some(&200),
            "the deadline bound must cover the ambiguously-sent attempt"
        );
        let queued: Vec<Signature> = state.pending_remints[0]
            .signatures
            .iter()
            .map(|pending_sig| pending_sig.signature)
            .collect();
        assert!(
            queued.contains(&ambiguous_attempt),
            "the in-process gate must classify the ambiguously-sent attempt too: {queued:?}"
        );
    }

    /// Without the broadcast set there is nothing to prove the release dead
    /// against, so the compensation material goes back and the row stays
    /// Processing for recovery, which reads the same journal.
    #[tokio::test]
    async fn permanent_failure_unreadable_journal_leaves_row_to_recovery() {
        let txn_id = 21;
        let nonce = 16;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        mock.insert_release_signature(txn_id, Signature::new_unique().to_string(), 0, None)
            .await
            .unwrap();
        mock.set_should_fail("get_release_signatures", true);

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        assert!(
            storage_rx.try_recv().is_err(),
            "must not write a terminal status on an unreadable journal"
        );
        assert!(state.pending_remints.is_empty(), "no remint may be queued");
        assert!(state.remint_cache.contains_key(&nonce));
        assert!(state.pending_signatures.contains_key(&nonce));
        assert_eq!(
            row_status(&mock, txn_id),
            Some(TransactionStatus::Processing),
            "the row must stay Processing so the recovery sweep owns it"
        );
    }

    /// A failed `set_pending_remint` whose row still reads `Processing` proves
    /// nothing committed. Escalating here would strand a row recovery resolves
    /// on its own, so the caches go back instead.
    #[tokio::test]
    async fn permanent_failure_leaves_processing_row_to_recovery() {
        let txn_id = 22;
        let nonce = 17;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(txn_id, broadcast.to_string(), 0, None)
            .await
            .unwrap();
        mock.set_should_fail("set_pending_remint", true);

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        assert!(
            storage_rx.try_recv().is_err(),
            "must not write a terminal status while the row is still Processing"
        );
        assert!(
            state.pending_remints.is_empty(),
            "must not queue a remint without a durable PendingRemint row"
        );
        assert!(
            state.remint_cache.contains_key(&nonce),
            "remint info must be restored for a later attempt"
        );
        assert!(
            state.pending_signatures.contains_key(&nonce),
            "release signatures must be restored for a later attempt"
        );
        assert_eq!(
            mock.calls("set_pending_remint"),
            3,
            "the transient write must be retried before the row is read back"
        );
    }

    /// The write can commit and still return an error when the acknowledgement
    /// is lost. A row that reads back PendingRemint is durable, so this sender
    /// keeps driving the remint instead of escalating.
    #[tokio::test]
    async fn permanent_failure_adopts_committed_pending_remint_row() {
        let txn_id = 23;
        let nonce = 18;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(txn_id, broadcast.to_string(), 0, None)
            .await
            .unwrap();
        mock.set_should_fail("set_pending_remint", true);
        mock.pending_transactions.lock().unwrap()[0].status = TransactionStatus::PendingRemint;

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        assert_eq!(
            state.pending_remints.len(),
            1,
            "a committed PendingRemint row must be driven by this sender"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "must not overwrite a committed PendingRemint with a terminal status"
        );
    }

    /// The write can commit while every read-back also fails, so no read can say
    /// who owns the row. Replaying the idempotent write is the only safe probe:
    /// it succeeds against the committed handoff and proves it durable.
    #[tokio::test]
    async fn permanent_failure_adopts_committed_handoff_when_reads_fail() {
        let txn_id = 24;
        let nonce = 19;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(txn_id, broadcast.to_string(), 0, None)
            .await
            .unwrap();
        // The three write attempts fail; the retry after them finds the database
        // reachable and replays the identical payload against the committed row.
        mock.set_fail_times("set_pending_remint", 3);
        mock.set_should_fail("get_transaction_status", true);
        {
            let mut rows = mock.pending_transactions.lock().unwrap();
            rows[0].status = TransactionStatus::PendingRemint;
            rows[0].remint_signatures = Some(vec![broadcast.to_string()]);
        }

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        assert!(
            storage_rx.try_recv().is_err(),
            "must not queue a terminal status the writer would apply to a committed PendingRemint"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "a committed PendingRemint has no other driver until a restart, so it must be queued here"
        );
        assert_eq!(
            mock.calls("set_pending_remint"),
            4,
            "the three backoff attempts must be followed by the retry that resolves ownership"
        );
        assert_eq!(
            row_status(&mock, txn_id),
            Some(TransactionStatus::PendingRemint),
            "the committed handoff must survive"
        );
    }

    /// The retry can also miss because another writer moved the row: a recovery
    /// demote leaves it Pending. Adopting it would drive a remint the processor
    /// is about to re-release against, so the guard miss leaves it alone.
    #[tokio::test]
    async fn permanent_failure_does_not_adopt_row_moved_by_another_writer() {
        let txn_id = 25;
        let nonce = 20;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(txn_id, broadcast.to_string(), 0, None)
            .await
            .unwrap();
        mock.set_fail_times("set_pending_remint", 3);
        mock.set_should_fail("get_transaction_status", true);
        // Recovery demoted the row while the handoff was being retried, so the
        // PendingRemint write never committed and the retry finds no row to take.
        mock.pending_transactions.lock().unwrap()[0].status = TransactionStatus::Pending;

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        assert!(
            state.pending_remints.is_empty(),
            "must not drive a remint for a row another writer owns"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "must not write a terminal status over another writer's row"
        );
        assert!(
            state.remint_cache.contains_key(&nonce),
            "remint info must be held when the retry cannot prove the row moved on"
        );
        assert_eq!(
            row_status(&mock, txn_id),
            Some(TransactionStatus::Pending),
            "the other writer's status must survive"
        );
    }

    /// When neither the write nor the read-back establishes the row's state, the
    /// remint info and signature stash stay in memory and the row keeps its
    /// status. ManualReview here strands a still-Processing withdrawal that no
    /// sweep selects, after discarding the only live copy of both.
    #[tokio::test]
    async fn permanent_failure_holds_remint_info_and_stash_when_state_undeterminable() {
        let txn_id = 26;
        let nonce = 21;
        let mock = processing_withdrawal_mock(txn_id, nonce);
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(txn_id, broadcast.to_string(), 0, None)
            .await
            .unwrap();
        mock.set_should_fail("set_pending_remint", true);
        mock.set_should_fail("get_transaction_status", true);

        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(nonce, make_remint_info(txn_id));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        handle_permanent_failure(
            &mut state,
            &release_ctx(txn_id, nonce),
            &storage_tx,
            "release_funds failed",
        )
        .await;

        assert!(
            storage_rx.try_recv().is_err(),
            "must not write a terminal status over a row whose state is unknown"
        );
        assert!(
            state.pending_remints.is_empty(),
            "must not queue a remint when the row state is unknown"
        );
        assert!(
            state.remint_cache.contains_key(&nonce),
            "remint info must be held for recovery or a later attempt"
        );
        assert!(
            state.pending_signatures.contains_key(&nonce),
            "release signatures must be held for recovery or a later attempt"
        );
        assert_eq!(
            mock.calls("set_pending_remint"),
            4,
            "the handoff must be retried once more before the caches are restored"
        );
        assert_eq!(
            row_status(&mock, txn_id),
            Some(TransactionStatus::Processing),
            "the row must stay Processing so the recovery sweep owns it"
        );
    }

    /// The escalation exists because the outcome is unknown, and the signatures
    /// are the only thing that can still settle it. Dropping them at the moment
    /// of doubt destroys the process-local evidence a resolution needs.
    #[tokio::test]
    async fn send_manual_review_keeps_the_broadcast_signatures() {
        let mut state = make_sender_state();
        let sig = Signature::new_unique();
        state.pending_signatures.insert(
            5,
            vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1,
                blockhash_slot: None,
            }],
        );
        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
            deposit_claim_lease: None,
        };

        send_manual_review(&mut state, &ctx, &tx, "outcome unknown").await;

        assert_eq!(
            state.pending_signatures.get(&5).map(|sigs| sigs.len()),
            Some(1),
            "the evidence must survive the escalation that needs it"
        );
    }

    // ── read failures must not mark a row Failed ─────────────────────

    /// Nothing was broadcast when the build itself could not read chain or
    /// database state, so the row must stay Processing for the recovery worker.
    /// Writing Failed here would strand a withdrawal that never even left.
    #[tokio::test]
    async fn read_failure_leaves_row_processing_not_failed() {
        let ctx = withdrawal_ctx(10, 7);

        // Taken from the real read, so the arm is pinned against the error
        // production actually raises when the node is down. A hand-built one
        // would pass whether or not any read site ever produces it, which is
        // how this guard came to cover a case that could not happen.
        let mut server = mockito::Server::new_async().await;
        let _down = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("node down")
            .create_async()
            .await;
        let mut down_state = make_sender_state_with_server(&server.url());
        down_state.instance_pda = Some(Pubkey::new_unique());
        let bitmap_read_error = down_state
            .fetch_current_generation()
            .await
            .expect_err("a downed node must fail the bitmap read");

        let cases: Vec<(&str, OperatorError)> = vec![
            ("bitmap unreadable", bitmap_read_error),
            (
                "account fetch failed",
                crate::error::AccountError::InstanceNotFound {
                    instance: Pubkey::default(),
                }
                .into(),
            ),
            (
                "database read failed",
                crate::error::StorageError::DatabaseError {
                    message: "transient".to_string(),
                }
                .into(),
            ),
        ];

        for (label, err) in cases {
            let mut state = make_sender_state();
            let (storage_tx, mut storage_rx) = mpsc::channel(10);

            route_builder_error(&mut state, &ctx, &storage_tx, err).await;

            assert!(
                storage_rx.try_recv().is_err(),
                "{label} must not produce any status update (row stays Processing)"
            );
        }
    }

    /// A genuine build error MUST still mark the row Failed, so the exemption
    /// above does not swallow real failures.
    #[tokio::test]
    async fn genuine_build_error_still_marks_failed() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        route_builder_error(
            &mut state,
            &withdrawal_ctx(10, 7),
            &storage_tx,
            ProgramError::InvalidBuilder {
                reason: "bad".to_string(),
            }
            .into(),
        )
        .await;

        let update = storage_rx
            .try_recv()
            .expect("a genuine build error must send a Failed status");
        assert_eq!(update.status, TransactionStatus::Failed);
    }

    // ── pre-broadcast requeue ───────────────────────────────────────

    /// Nothing is broadcast when the build or the signing fails, so the row
    /// provably released nothing. Escalating it to a human strands a withdrawal
    /// that an ordinary retry would settle.
    #[tokio::test]
    async fn build_failure_requeues_the_withdrawal_instead_of_escalating() {
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(10));
        state.in_flight_withdrawals.insert(7);
        state.remint_cache.insert(7, make_remint_info(10));
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &withdrawal_ctx(10, 7),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_eq!(
            row_status(mock, 10),
            Some(TransactionStatus::Pending),
            "a withdrawal that never broadcast must go back on the queue"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no terminal status may be written for a row that never left"
        );
        assert!(
            !state.in_flight_withdrawals.contains(&7),
            "an unsent nonce must not hold the rotation barrier"
        );
    }

    /// The cap lives in the same write that requeues, so a row that has spent
    /// its budget escalates rather than cycling between Pending and Processing.
    #[tokio::test]
    async fn build_failure_at_the_requeue_cap_escalates_to_manual_review() {
        let mock = mock_with_processing_row(11);
        mock.pending_transactions.lock().unwrap()[0].recovery_requeue_attempts =
            MAX_RECOVERY_REQUEUE_ATTEMPTS;
        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        state.remint_cache.insert(8, make_remint_info(11));
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &withdrawal_ctx(11, 8),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        let update = storage_rx
            .try_recv()
            .expect("a row out of requeues must be escalated");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_eq!(
            row_status(mock, 11),
            Some(TransactionStatus::Processing),
            "the capped write must leave the row where it was"
        );
    }

    /// A stashed signature means an earlier attempt did broadcast, so the row
    /// may have released and must not be handed back to the fetcher.
    #[tokio::test]
    async fn a_stashed_signature_blocks_the_pre_broadcast_requeue() {
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(12));
        state.remint_cache.insert(6, make_remint_info(12));
        state.pending_signatures.insert(
            6,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1,
                blockhash_slot: None,
            }],
        );
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &withdrawal_ctx(12, 6),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_ne!(
            row_status(mock, 12),
            Some(TransactionStatus::Pending),
            "a nonce that may already be spent must not be requeued"
        );
    }

    /// A read that failed on the way to building the transaction is transient
    /// and nothing was broadcast, so the row takes a bounded retry rather than
    /// waiting for the recovery sweep to notice it.
    #[tokio::test]
    async fn read_failure_requeues_the_withdrawal_for_a_bounded_retry() {
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(13));
        state.in_flight_withdrawals.insert(9);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        route_builder_error(
            &mut state,
            &withdrawal_ctx(13, 9),
            &storage_tx,
            crate::error::StorageError::DatabaseError {
                message: "transient".to_string(),
            }
            .into(),
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_eq!(
            row_status(mock, 13),
            Some(TransactionStatus::Pending),
            "an unreadable chain or database must not freeze the row"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a read failure is not a terminal outcome"
        );
        assert!(!state.in_flight_withdrawals.contains(&9));
    }

    // ── handle_success ──────────────────────────────────────────────

    #[tokio::test]
    async fn success_clears_remint_cache_and_nonce_state() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(50),
            withdrawal_nonce: Some(3),
            trace_id: Some("trace-50".to_string()),
            deposit_claim_lease: None,
        };
        state.in_flight_withdrawals.insert(3);
        state.retry_counts.insert(3, 2);
        state.remint_cache.insert(3, make_remint_info(50));
        state.pending_signatures.insert(
            3,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        let sig = solana_sdk::signature::Signature::new_unique();
        handle_success(&mut state, &ctx, sig, &storage_tx).await;

        // All nonce-keyed state should be cleaned up
        assert!(!state.in_flight_withdrawals.contains(&3));
        assert!(!state.retry_counts.contains_key(&3));
        assert!(
            !state.remint_cache.contains_key(&3),
            "remint_cache should be cleared on success"
        );
        assert!(
            !state.pending_signatures.contains_key(&3),
            "pending_signatures should be cleared on success"
        );

        // Should send Completed status
        let update = storage_rx.try_recv().expect("should receive status update");
        assert_eq!(update.transaction_id, 50);
        assert_eq!(update.status, TransactionStatus::Completed);
    }

    #[tokio::test]
    async fn send_and_confirm_stashes_withdrawal_signature() {
        let mut state = make_sender_state();
        let nonce = 42u64;

        // Simulate what send_and_confirm does: stash a signature
        let sig = Signature::new_unique();
        state
            .pending_signatures
            .entry(nonce)
            .or_default()
            .push(PendingSig {
                signature: sig,
                last_valid_block_height: 0,
                blockhash_slot: None,
            });

        assert!(state.pending_signatures.contains_key(&nonce));
        assert_eq!(state.pending_signatures[&nonce].len(), 1);
        assert_eq!(state.pending_signatures[&nonce][0].signature, sig);

        // Stash another (simulating a retry)
        let sig2 = Signature::new_unique();
        state
            .pending_signatures
            .entry(nonce)
            .or_default()
            .push(PendingSig {
                signature: sig2,
                last_valid_block_height: 0,
                blockhash_slot: None,
            });
        assert_eq!(state.pending_signatures[&nonce].len(), 2);
    }

    // ── write-ahead release signature ─────────────────────────────

    fn withdrawal_ctx(txn_id: i64, nonce: u64) -> TransactionContext {
        TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(txn_id),
            withdrawal_nonce: Some(nonce),
            trace_id: Some(format!("trace-{txn_id}")),
            deposit_claim_lease: None,
        }
    }

    fn mock_blockhash(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getLatestBlockhash"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                            "lastValidBlockHeight": 100
                        }
                    }
                })
                .to_string(),
            )
            .create()
    }

    fn mock_get_signature_statuses_null(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"context": {"slot": 1}, "value": [null]}
                })
                .to_string(),
            )
            .create()
    }

    /// A successful send_and_confirm persists the signed transaction's signature (via `insert_release_signature`) before the broadcast.
    #[tokio::test]
    async fn release_persists_signature_before_send() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let _send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": Signature::default().to_string()
                })
                .to_string(),
            )
            .create();
        // Confirmation polls return null (Retry), but the persist already happened.
        let _status = mock_get_signature_statuses_null(&mut server);

        let mut state = make_sender_state_with_server(&server.url());
        seed_release_claim(&mut state, 10, 5);
        let ctx = withdrawal_ctx(10, 5);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &mpsc::channel(10).0,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        let stored = mock.get_release_signatures(10).await.unwrap();
        assert_eq!(stored.len(), 1, "exactly one release signature persisted");
        assert_eq!(
            stored[0].signature,
            Signature::default().to_string(),
            "persisted signature must be the signed transaction's signature"
        );
        assert_eq!(
            stored[0].last_valid_block_height, 100,
            "persisted lvbh must match the blockhash"
        );
    }

    /// A failed write-ahead persist must NOT broadcast, must write no terminal status (row left Processing), and must stash nothing.
    #[tokio::test]
    async fn release_aborts_send_when_persist_fails() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        // sendTransaction must never be called once persist fails.
        let send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .expect(0)
            .create();

        let mut state = make_sender_state_with_server(&server.url());
        seed_release_claim(&mut state, 10, 5);
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        mock.set_should_fail("insert_release_signature", true);

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = withdrawal_ctx(10, 5);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        send.assert();
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update must be sent; row stays Processing for recovery"
        );
        assert!(
            !state.pending_signatures.contains_key(&5),
            "nothing stashed when persist failed"
        );
    }

    // ── pre-broadcast ownership claim ─────────────────────────────

    /// Seed the `Processing` row a release claim CASes against and arm the sender
    /// with the matching lease, exactly as the submission path leaves them.
    fn seed_release_claim(state: &mut SenderState, txn_id: i64, nonce: u64) {
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        push_withdrawal_with_nonce(
            mock,
            txn_id,
            nonce as i64,
            crate::storage::common::models::TransactionStatus::Processing,
        );
        let token = row_updated_at(mock, txn_id).expect("seeded row present");
        state.release_leases.insert(nonce, token);
    }

    /// The row as recovery leaves it after a demote: still present, no longer
    /// `Processing`, so the lease the sender holds names a dead incarnation.
    fn demote_seeded_row(state: &SenderState, txn_id: i64) {
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        let mut rows = mock.pending_transactions.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|r| r.id == txn_id)
            .expect("seeded row present");
        row.status = crate::storage::common::models::TransactionStatus::Pending;
        row.updated_at = Utc::now();
    }

    fn mock_send_ok(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": Signature::default().to_string()
                })
                .to_string(),
            )
            .create()
    }

    /// The core safety property of the claim: once recovery has demoted the row,
    /// the lease the sender holds is dead, so a sender slow past the stale
    /// threshold must not broadcast and must leave no signature behind.
    #[tokio::test]
    async fn release_send_drops_builder_when_claim_lost() {
        let txn_id = 10;
        let nonce = 5;
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .expect(0)
            .create();

        let mut state = make_sender_state_with_server(&server.url());
        seed_release_claim(&mut state, txn_id, nonce);
        state.in_flight_withdrawals.insert(nonce);
        demote_seeded_row(&state, txn_id);

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &withdrawal_ctx(txn_id, nonce),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        send.assert();
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert!(
            mock.get_release_signatures(txn_id)
                .await
                .unwrap()
                .is_empty(),
            "a lost claim must persist no signature"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a lost claim writes no terminal status; the winning writer owns the row"
        );
        assert!(
            !state.pending_signatures.contains_key(&nonce),
            "nothing stashed when nothing broadcast"
        );
    }

    /// The normal path still broadcasts once and records its signature write-ahead.
    /// The row's `updated_at` advancing is what separates the claim from a bare
    /// insert, and is what makes a concurrent recovery demote lose.
    #[tokio::test]
    async fn release_send_broadcasts_and_bumps_row_when_claim_wins() {
        let txn_id = 10;
        let nonce = 5;
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let _send = mock_send_ok(&mut server);
        let _status = mock_get_signature_statuses_null(&mut server);

        let mut state = make_sender_state_with_server(&server.url());
        seed_release_claim(&mut state, txn_id, nonce);
        let arrival_token = state.release_leases[&nonce];

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &withdrawal_ctx(txn_id, nonce),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &mpsc::channel(10).0,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_eq!(
            mock.get_release_signatures(txn_id).await.unwrap().len(),
            1,
            "the claim persists the signature write-ahead"
        );
        assert_ne!(
            row_updated_at(mock, txn_id).expect("seeded row present"),
            arrival_token,
            "the claim must bump the row so a racing recovery CAS loses"
        );
    }

    /// The node answered the send with an error, so the release may still have
    /// reached the network. The stash is written only after a successful send and
    /// knows nothing about this attempt, but the write-ahead journal does, so the
    /// withdrawal takes the finality-checked remint path instead of escalating.
    #[tokio::test]
    async fn send_failure_defers_remint_on_journaled_signature() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let _send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Internal error"}
                })
                .to_string(),
            )
            .create();

        // The PendingRemint transition is a compare-and-set from Processing.
        let mock = mock_with_processing_row(10);
        let lease = row_updated_at(&mock, 10).expect("seeded row present");
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.release_leases.insert(5, lease);
        state.remint_cache.insert(5, make_remint_info(10));

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = withdrawal_ctx(10, 5);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        assert!(
            storage_rx.try_recv().is_err(),
            "an ambiguously-sent release must not be terminalized"
        );
        let journaled = mock.get_release_signatures(10).await.unwrap();
        assert_eq!(
            journaled.len(),
            1,
            "the send was preceded by a write-ahead persist"
        );
        assert!(
            !state.pending_signatures.contains_key(&5),
            "the failed send never stashed its signature"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].signatures[0].signature.to_string(),
            journaled[0].signature,
            "the gate must carry the journaled signature"
        );
    }

    // ── set_pending_remint persistence ───────────────────────────────

    /// When a withdrawal fails permanently and is eligible for remint,
    /// `handle_permanent_failure` must persist the PendingRemint state to
    /// the database before queuing the entry in memory.
    ///
    /// This test verifies three things that are critical for crash safety:
    ///   1. `set_pending_remint` is called exactly once with the correct transaction_id.
    ///   2. All withdrawal signatures are stored — missing even one could cause a
    ///      false "not finalized" result on recovery, leading to a duplicate remint.
    ///   3. The deadline is ~32s in the future so recovery restores the correct wait
    ///      time rather than firing the remint immediately on restart.
    #[tokio::test]
    async fn permanent_failure_calls_set_pending_remint_with_correct_args() {
        // `set_pending_remint` is a compare-and-set from Processing, so the row
        // has to be there for the deferral to persist.
        let mock = mock_with_processing_row(10);

        // Two signatures — simulating a withdrawal that was retried once before
        // failing permanently. Both must be persisted for a complete finality check.
        let sig1 = Signature::new_unique();
        let sig2 = Signature::new_unique();
        let sig1_lvbh: u64 = 100;
        let sig2_lvbh: u64 = 200;
        for (signature, lvbh) in [(sig1, sig1_lvbh), (sig2, sig2_lvbh)] {
            mock.insert_release_signature(10, signature.to_string(), lvbh as i64, None)
                .await
                .unwrap();
        }

        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        state.remint_cache.insert(5, make_remint_info(10));
        state.pending_signatures.insert(
            5,
            vec![
                PendingSig {
                    signature: sig1,
                    last_valid_block_height: sig1_lvbh,
                    blockhash_slot: None,
                },
                PendingSig {
                    signature: sig2,
                    last_valid_block_height: sig2_lvbh,
                    blockhash_slot: None,
                },
            ],
        );

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
            deposit_claim_lease: None,
        };

        let before = Utc::now();
        handle_permanent_failure(&mut state, &ctx, &storage_tx, "release_funds failed").await;
        let after = Utc::now();

        // Extract the mock to inspect what was written to storage.
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        let calls = mock.pending_remint_signatures.lock().unwrap();

        assert_eq!(
            calls.len(),
            1,
            "set_pending_remint should be called exactly once"
        );

        let (stored_id, stored_sigs, stored_lvbhs, stored_deadline, stored_refusal) = &calls[0];
        assert_eq!(*stored_id, 10, "wrong transaction_id persisted");
        assert!(
            !stored_refusal,
            "an ordinary failure proves nothing about the release, so it stays held to the bitmap gate"
        );

        assert_eq!(
            stored_sigs.len(),
            2,
            "both withdrawal signatures must be persisted"
        );
        assert!(
            stored_sigs.contains(&sig1.to_string()),
            "sig1 must be persisted"
        );
        assert!(
            stored_sigs.contains(&sig2.to_string()),
            "sig2 must be persisted"
        );

        // lvbh array must be index-paired with sig array and carry the values
        // we stashed at send time. Otherwise the remint gate can't tell a still-
        // live broadcast from a dead one.
        assert_eq!(
            stored_sigs.len(),
            stored_lvbhs.len(),
            "sig array and lvbh array must be the same length"
        );
        let sig1_idx = stored_sigs
            .iter()
            .position(|stored_sig| stored_sig == &sig1.to_string())
            .unwrap();
        let sig2_idx = stored_sigs
            .iter()
            .position(|stored_sig| stored_sig == &sig2.to_string())
            .unwrap();
        assert_eq!(
            stored_lvbhs[sig1_idx], sig1_lvbh as i64,
            "sig1's lvbh must be persisted"
        );
        assert_eq!(
            stored_lvbhs[sig2_idx], sig2_lvbh as i64,
            "sig2's lvbh must be persisted"
        );

        // Deadline must be ~FINALITY_SAFETY_DELAY (32s) from now.
        // We allow a ±3s window to absorb test execution time.
        let expected_min = before + chrono::Duration::seconds(29);
        let expected_max = after + chrono::Duration::seconds(35);
        assert!(
            *stored_deadline >= expected_min && *stored_deadline <= expected_max,
            "deadline should be ~32s from now, got {stored_deadline}"
        );
    }

    /// When the database write for `set_pending_remint` fails, the operator
    /// cannot safely defer the remint — it has no guarantee the state will
    /// survive a restart. Instead of silently losing the remint, it must
    /// immediately escalate to ManualReview so an operator can intervene.
    ///
    /// Equally important: nothing should be queued in `pending_remints`.
    /// Queuing in memory without the DB write would be a half-written state —
    /// the entry would disappear on the next crash, violating the atomicity
    /// invariant.
    #[tokio::test]
    async fn permanent_failure_sends_manual_review_when_storage_fails() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Instruct the mock to fail on set_pending_remint.
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        mock.set_should_fail("set_pending_remint", true);

        state.remint_cache.insert(5, make_remint_info(10));
        state.pending_signatures.insert(
            5,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
                blockhash_slot: None,
            }],
        );

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
            deposit_claim_lease: None,
        };

        handle_permanent_failure(&mut state, &ctx, &storage_tx, "release_funds failed").await;

        // Must escalate to ManualReview — human intervention is needed.
        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview status");
        assert_eq!(update.transaction_id, 10);
        assert_eq!(update.status, TransactionStatus::ManualReview);

        // Must not queue in memory — no DB write means no crash safety.
        assert!(
            state.pending_remints.is_empty(),
            "should not queue pending remint when storage write failed"
        );
    }

    /// `send_fatal_error` must emit a `Failed` status update with the exact error message
    /// and no counterpart signature when the context contains a transaction id.
    #[tokio::test]
    async fn send_fatal_error_with_transaction_id_sends_failed_status() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(42),
            withdrawal_nonce: None,
            trace_id: Some("trace-1".to_string()),
            deposit_claim_lease: None,
        };

        send_fatal_error(&tx, &ctx, "test error").await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 42);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert!(update.counterpart_signature.is_none());
        assert_eq!(update.error_message.as_deref(), Some("test error"));
    }

    /// Without a transaction id there is nothing to mark as failed, so `send_fatal_error`
    /// must silently drop the error and send nothing to the storage channel.
    #[tokio::test]
    async fn send_fatal_error_without_transaction_id_sends_nothing() {
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::InitializeMint,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        send_fatal_error(&tx, &ctx, "test error").await;

        drop(tx);
        assert!(rx.recv().await.is_none());
    }

    /// A successful mint (no withdrawal nonce) must emit `Completed` with the on-chain
    /// signature as `counterpart_signature`.
    #[tokio::test]
    async fn handle_success_mint_transaction_sends_completed_status() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(7),
            withdrawal_nonce: None,
            trace_id: Some("trace-mint".to_string()),
            deposit_claim_lease: None,
        };
        let sig = Signature::new_unique();

        handle_success(&mut state, &ctx, sig, &tx).await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 7);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(sig.to_string().as_str())
        );
    }

    /// A confirmed RotateBitmap carries neither a transaction id nor a nonce, so
    /// it must write no status update. Nothing local records the generation, so
    /// there is nothing else to assert: the chain is the only record.
    #[tokio::test]
    async fn handle_success_rotate_bitmap_writes_no_status() {
        let mut state = make_sender_state();

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::RotateBitmap,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_success(&mut state, &ctx, Signature::new_unique(), &tx).await;

        drop(tx);
        assert!(rx.recv().await.is_none());
    }

    /// After a successful withdrawal, the per-nonce retry counter must be removed so that
    /// a future submission with the same nonce starts from a clean slate.
    #[tokio::test]
    async fn handle_success_withdrawal_cleans_up_nonce_state() {
        let mut state = make_sender_state();
        state.instance_pda = Some(Pubkey::new_unique());
        state.retry_counts.insert(5, 2);

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(99),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-wd".to_string()),
            deposit_claim_lease: None,
        };
        let sig = Signature::new_unique();

        handle_success(&mut state, &ctx, sig, &tx).await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 99);
        assert_eq!(update.status, TransactionStatus::Completed);

        // Retry count should be cleaned up
        assert!(!state.retry_counts.contains_key(&5));
    }

    // ============================================================
    // handle_confirmation_result tests (code paths that don't need RPC)
    // ============================================================

    /// A generation rejection on a transaction with no nonce cannot be placed on
    /// either side of the window, so it stays a plain permanent failure.
    #[tokio::test]
    async fn confirmation_result_generation_rejection_without_nonce_fails() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(10),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration,
            ))),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 10);
        assert_eq!(update.status, TransactionStatus::Failed);
    }

    /// An unrecognised program error (None variant) is treated as a permanent failure;
    /// the transaction must be marked Failed with no retry attempt.
    #[tokio::test]
    async fn confirmation_result_other_program_error_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(11),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(None)),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 11);
        assert_eq!(update.status, TransactionStatus::Failed);
    }

    /// A rotation rejected with UnexpectedGeneration means one already landed.
    /// There is no local index to resync, so the arm must be inert: no status
    /// update (a rotation has no DB row) and no state change.
    #[tokio::test]
    async fn confirmation_result_unexpected_generation_is_inert() {
        let mut state = make_sender_state();
        state.instance_pda = Some(Pubkey::new_unique());

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::RotateBitmap,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::UnexpectedGeneration,
            ))),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        drop(tx);
        assert!(
            rx.recv().await.is_none(),
            "no status update expected for a rotation"
        );
    }

    /// A `Retry` result with `RetryPolicy::None` (non-idempotent operation) cannot be safely
    /// retried, so it must be converted to a fatal failure with an "unknown" error message.
    #[tokio::test]
    async fn confirmation_result_retry_with_none_policy_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(12),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Retry),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 12);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert!(update
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("unknown"));
    }

    /// An RPC transport error bubbled up as `TransactionError::Rpc` must result in a Failed
    /// status update; the error message must contain the original RPC error text.
    #[tokio::test]
    async fn confirmation_result_rpc_error_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(13),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        let rpc_err = Box::new(
            solana_rpc_client_api::client_error::Error::new_with_request(
                solana_rpc_client_api::client_error::ErrorKind::Custom(
                    "test rpc error".to_string(),
                ),
                solana_rpc_client_api::request::RpcRequest::GetBalance,
            ),
        );

        handle_confirmation_result(
            &mut state,
            Err(TransactionError::Rpc(rpc_err)),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 13);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert!(
            update
                .error_message
                .as_deref()
                .unwrap_or("")
                .contains("test rpc error"),
            "expected error message to contain RPC error text, got: {:?}",
            update.error_message
        );
    }

    /// When `MintNotInitialized` fires but no matching mint builder exists in state, the
    /// fallback path must emit a fatal error so the transaction is not silently dropped.
    #[tokio::test]
    async fn confirmation_result_mint_not_initialized_no_transaction_id_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(14),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::MintNotInitialized),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        // Should get a fatal error because no mint_builder in state
        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 14);
        assert_eq!(update.status, TransactionStatus::Failed);
    }

    /// The JIT re-fire is a second broadcast of an already-funded deposit, so it
    /// has to journal its signature write-ahead through the ownership claim. With
    /// no record, a crash right after the re-send leaves recovery unable to tell
    /// the mint apart from one that never went out.
    #[tokio::test]
    async fn jit_mint_retry_journals_signature_before_broadcast() {
        ensure_test_signer();
        let txn_id = 21;
        let mut server = mockito::Server::new_async().await;
        let _account = mock_initialized_mint(&mut server, SignerUtil::admin_signer().pubkey());
        let _hash = mock_blockhash(&mut server);
        let _send = mock_send_ok(&mut server);

        let mock = MockStorage::new();
        push_processing_deposit_row(&mock, txn_id);
        let lease = row_updated_at(&mock, txn_id).expect("seeded row present");
        let mut state = sender_state_with_storage(&server.url(), mock);

        let mut builder = MintToBuilder::new();
        builder.mint(Pubkey::new_unique());
        state.mint_builders.insert(txn_id, builder);

        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(txn_id),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: Some(lease),
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::MintNotInitialized),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &mpsc::channel(10).0,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_eq!(
            mock.get_release_signatures(txn_id).await.unwrap().len(),
            1,
            "the JIT retry must persist its signature before broadcasting"
        );
        assert_ne!(
            row_updated_at(mock, txn_id).expect("seeded row present"),
            lease,
            "the claim must bump the row so a racing recovery CAS loses"
        );
    }

    /// A JIT retry that arrives with no lease cannot prove it still owns the
    /// deposit, so it must not re-mint; the row waits for recovery instead.
    #[tokio::test]
    async fn jit_mint_retry_without_lease_does_not_broadcast() {
        ensure_test_signer();
        let txn_id = 22;
        let mut server = mockito::Server::new_async().await;
        let _account = mock_initialized_mint(&mut server, SignerUtil::admin_signer().pubkey());
        let _hash = mock_blockhash(&mut server);
        let send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .expect(0)
            .create();

        let mock = MockStorage::new();
        push_processing_deposit_row(&mock, txn_id);
        let mut state = sender_state_with_storage(&server.url(), mock);

        let mut builder = MintToBuilder::new();
        builder.mint(Pubkey::new_unique());
        state.mint_builders.insert(txn_id, builder);

        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(txn_id),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::MintNotInitialized),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &mpsc::channel(10).0,
        )
        .await;

        send.assert();
    }

    /// The confirmed arm must release the finalized entry's in-flight slot before the
    /// JIT retry asks for one. With the semaphore saturated the retry would otherwise
    /// be refused against this very entry's own permit and never re-mint.
    #[tokio::test]
    async fn jit_mint_retry_reuses_slot_of_finalized_entry_when_saturated() {
        ensure_test_signer();
        let txn_id = 23;
        let mut server = mockito::Server::new_async().await;
        let _account = mock_initialized_mint(&mut server, SignerUtil::admin_signer().pubkey());
        let _hash = mock_blockhash(&mut server);
        let _send = mock_send_ok(&mut server);

        let mock = MockStorage::new();
        push_processing_deposit_row(&mock, txn_id);
        let lease = row_updated_at(&mock, txn_id).expect("seeded row present");
        let mut state = sender_state_with_storage(&server.url(), mock);

        let mut builder = MintToBuilder::new();
        builder.mint(Pubkey::new_unique());
        state.mint_builders.insert(txn_id, builder);

        // Every slot but this entry's is held by other in-flight work.
        let _others: Vec<_> = (0..MAX_IN_FLIGHT - 1)
            .map(|_| state.semaphore.clone().try_acquire_owned().unwrap())
            .collect();
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        assert_eq!(state.semaphore.available_permits(), 0);

        let tx = InFlightTx {
            signature: Signature::new_unique(),
            ctx: TransactionContext {
                kind: TransactionKind::Mint,
                transaction_id: Some(txn_id),
                withdrawal_nonce: None,
                trace_id: None,
                deposit_claim_lease: Some(lease),
            },
            instruction: dummy_instruction(),
            compute_unit_price: None,
            retry_policy: RetryPolicy::None,
            extra_error_checks_policy: mint_extra_error_checks_policy(),
            poll_attempts: 0,
            resend_count: 0,
            persisted: true,
            permit,
        };

        let err = solana_sdk::transaction::TransactionError::InstructionError(
            0,
            solana_sdk::instruction::InstructionError::UninitializedAccount,
        );
        let status = solana_transaction_status::TransactionStatus {
            slot: 100,
            confirmations: None,
            status: Err(err.clone()),
            err: Some(err),
            confirmation_status: Some(
                solana_transaction_status::TransactionConfirmationStatus::Finalized,
            ),
        };

        route_poll_results(&mut state, vec![(tx, Some(status))], &mpsc::channel(10).0).await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert_eq!(
            mock.get_release_signatures(txn_id).await.unwrap().len(),
            1,
            "the JIT retry must take the slot the finalized entry gave back and broadcast"
        );
    }

    /// `MintNotInitialized` with no transaction_id means there is nothing to report to storage;
    /// `send_fatal_error` must be a no-op and the channel must remain empty.
    #[tokio::test]
    async fn confirmation_result_mint_not_initialized_without_transaction_id() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        // No transaction_id
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::MintNotInitialized),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        // No transaction_id, so send_fatal_error sends nothing
        drop(tx);
        assert!(rx.recv().await.is_none());
    }

    /// When the per-nonce retry counter has already reached the maximum, `send_and_confirm`
    /// must short-circuit immediately with a Failed status mentioning "retries".
    #[tokio::test]
    async fn send_and_confirm_max_retries_exceeded_sends_fatal_error() {
        let mut state = make_sender_state();
        // Pre-fill retry_counts to be at max
        state.retry_counts.insert(5, 3);
        state.retry_max_attempts = 3;

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(20),
            withdrawal_nonce: Some(5),
            trace_id: None,
            deposit_claim_lease: None,
        };

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 20);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert!(update
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("retries"));
    }

    /// A `Confirmed` result must emit `Completed` with the on-chain signature stored as
    /// `counterpart_signature`, confirming the happy-path status-update flow.
    #[tokio::test]
    async fn confirmation_result_confirmed_sends_completed_status() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(30),
            withdrawal_nonce: Some(2),
            trace_id: Some("trace-confirmed".to_string()),
            deposit_claim_lease: None,
        };
        let sig = Signature::new_unique();

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Confirmed),
            sig,
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.recv().await.unwrap();
        assert_eq!(update.transaction_id, 30);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(sig.to_string().as_str())
        );
    }

    // ── NonceAlreadyUsed routing ─────────────────────────────────────

    /// Drive the NonceAlreadyUsed arm against a server whose
    /// `getSignatureStatuses` reply is `status_body`, with one stashed signature.
    async fn route_nonce_already_used(
        server: &mut mockito::ServerGuard,
        status_body: &str,
        stash_signature: bool,
    ) -> (SenderState, mpsc::Receiver<TransactionStatusUpdate>) {
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(status_body)
            .create();

        // A deferral compare-and-sets the row from Processing, so it has to exist.
        let mock = mock_with_processing_row(70);
        let broadcast = Signature::new_unique();
        if stash_signature {
            // Every broadcast is journaled before its send, which is what the
            // deferral's finality gate reads.
            mock.insert_release_signature(70, broadcast.to_string(), 0, None)
                .await
                .unwrap();
        }
        let mut state = sender_state_with_storage(&server.url(), mock);
        state.instance_pda = Some(Pubkey::new_unique());
        state.remint_cache.insert(4, make_remint_info(70));
        if stash_signature {
            state.pending_signatures.insert(
                4,
                vec![PendingSig {
                    signature: broadcast,
                    last_valid_block_height: 0,
                    blockhash_slot: None,
                }],
            );
        }

        let (tx, rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(70),
            withdrawal_nonce: Some(4),
            trace_id: Some("trace-70".to_string()),
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceAlreadyUsed,
            ))),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        (state, rx)
    }

    const FINALIZED_OK: &str = r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
        "slot":100,"confirmations":null,"err":null,"status":{"Ok":null},
        "confirmationStatus":"finalized"}]},"id":0}"#;

    const FINALIZED_ERR: &str = r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
        "slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":12}]},
        "status":{"Err":{"InstructionError":[0,{"Custom":12}]}},
        "confirmationStatus":"finalized"}]},"id":0}"#;

    const STILL_CONFIRMING: &str = r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{
        "slot":100,"confirmations":5,"err":null,"status":{"Ok":null},
        "confirmationStatus":"confirmed"}]},"id":0}"#;

    /// The bit was set by our own earlier broadcast, and that signature finalized
    /// successfully. The withdrawal did happen, so the row is Completed against it
    /// rather than failed and reminted.
    #[tokio::test]
    async fn nonce_already_used_with_landed_signature_completes() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_already_used(&mut server, FINALIZED_OK, true).await;

        let update = rx.try_recv().expect("a landed release must be recorded");
        assert_eq!(update.transaction_id, 70);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert!(update.counterpart_signature.is_some());
        assert!(state.pending_remints.is_empty(), "no remint may be queued");
    }

    /// One of our broadcasts is still confirming, so which one consumed the nonce
    /// is not yet decidable. Defer through the existing deadline path instead of
    /// guessing; the bitmap gate will have the last word before any credit.
    #[tokio::test]
    async fn nonce_already_used_with_live_signature_defers() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_already_used(&mut server, STILL_CONFIRMING, true).await;

        assert_eq!(
            state.pending_remints.len(),
            1,
            "an undecided outcome must defer, not resolve"
        );
        assert!(
            rx.try_recv().is_err(),
            "deferring writes no terminal status"
        );
    }

    /// The nonce is spent but every signature of ours finalized as failed. Something
    /// we cannot account for consumed it, so a human decides rather than the
    /// operator reminting into a release that may have paid out.
    #[tokio::test]
    async fn nonce_already_used_with_dead_signatures_escalates() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_already_used(&mut server, FINALIZED_ERR, true).await;

        let update = rx
            .try_recv()
            .expect("an unexplained spend must be reported");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(state.pending_remints.is_empty(), "no remint may be queued");
    }

    /// We broadcast nothing that could have set the bit, so we cannot claim the
    /// release as ours in either direction.
    #[tokio::test]
    async fn nonce_already_used_without_signatures_escalates() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_already_used(&mut server, FINALIZED_OK, false).await;

        let update = rx
            .try_recv()
            .expect("a spend with no broadcast of ours must be reported");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(state.pending_remints.is_empty());
    }

    /// A restart empties the in-memory stash, but the signature was persisted
    /// before broadcast. Falling back to it is what stops a restart from sending
    /// a correctly-paid withdrawal to manual review.
    #[tokio::test]
    async fn nonce_already_used_falls_back_to_persisted_signatures() {
        let mut server = mockito::Server::new_async().await;
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(FINALIZED_OK)
            .create();

        let mut state = make_sender_state_with_server(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());
        // Nothing stashed in memory, everything on durable storage.
        state
            .storage
            .insert_release_signature(70, Signature::new_unique().to_string(), 1, None)
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(70),
            withdrawal_nonce: Some(4),
            trace_id: Some("trace-70".to_string()),
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceAlreadyUsed,
            ))),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        let update = rx.try_recv().expect("the landed release must be recorded");
        assert_eq!(update.status, TransactionStatus::Completed);
    }

    // ── NonceOutsideCurrentGeneration routing ────────────────────────

    /// The row the on-chain generation refusal is driven against.
    const REFUSED_ROW: i64 = 80;

    /// Drive the generation-rejection arm for `nonce` against a bitmap on
    /// `chain_generation`, or against a server with no bitmap route when
    /// `chain_generation` is `None` (the RPC-failure case).
    async fn route_nonce_outside_generation(
        server: &mut mockito::ServerGuard,
        nonce: u64,
        chain_generation: Option<u64>,
    ) -> (SenderState, mpsc::Receiver<TransactionStatusUpdate>) {
        route_nonce_outside_generation_with(
            server,
            nonce,
            chain_generation,
            mock_with_processing_row(REFUSED_ROW),
        )
        .await
    }

    /// The same drive against a caller-prepared storage mock, so a test can
    /// decide what the park CAS finds.
    async fn route_nonce_outside_generation_with(
        server: &mut mockito::ServerGuard,
        nonce: u64,
        chain_generation: Option<u64>,
        mock: MockStorage,
    ) -> (SenderState, mpsc::Receiver<TransactionStatusUpdate>) {
        if let Some(generation) = chain_generation {
            let _bitmap = mock_bitmap_account(server, generation, &[]);
        }

        // The release was broadcast before the program refused it, so in
        // production neither the journal nor the stash is empty on this path.
        //
        // Without them the remint path exits early on "no signatures to verify".
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(REFUSED_ROW, broadcast.to_string(), 1, None)
            .await
            .unwrap();

        let mut state = sender_state_with_storage(&server.url(), mock);
        state.instance_pda = Some(Pubkey::new_unique());
        state.in_flight_withdrawals.insert(nonce);
        state
            .remint_cache
            .insert(nonce, make_remint_info(REFUSED_ROW));
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 1,
                blockhash_slot: None,
            }],
        );
        // As if send_and_confirm had just counted this attempt against the nonce.
        state.retry_counts.insert(nonce, 2);

        let (tx, rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(REFUSED_ROW),
            withdrawal_nonce: Some(nonce),
            trace_id: Some("trace-80".to_string()),
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration,
            ))),
            Signature::new_unique(),
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        (state, rx)
    }

    /// The nonce belongs to a window that has not opened yet, which is a timing
    /// problem a rotation fixes. Queue it rather than failing a good withdrawal.
    #[tokio::test]
    async fn nonce_outside_generation_ahead_of_chain_requeues() {
        let mut server = mockito::Server::new_async().await;
        let nonce = NONCES_PER_GENERATION;
        let (state, mut rx) = route_nonce_outside_generation(&mut server, nonce, Some(0)).await;

        assert_eq!(
            state.rotation_retry_queue.len(),
            1,
            "a not-yet-open window must be retried after rotation"
        );
        assert_eq!(
            state.rotation_retry_queue[0].0.withdrawal_nonce,
            Some(nonce)
        );
        assert!(
            !state.in_flight_withdrawals.contains(&nonce),
            "a queued withdrawal must not hold the rotation barrier"
        );
        assert!(rx.try_recv().is_err(), "no terminal status while queued");
        assert_eq!(
            state.retry_counts.get(&nonce).copied(),
            Some(1),
            "a refusal we already expected must not spend the withdrawal's retries"
        );
    }

    /// Each retry cycle stashes another signature, so a confirmed rejection must go while an open outcome stays.
    #[tokio::test]
    async fn a_requeued_release_forgets_the_signature_the_chain_rejected() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let nonce = NONCES_PER_GENERATION;
        let mock = mock_with_processing_row(REFUSED_ROW);
        let still_open = Signature::new_unique();
        let rejected = Signature::new_unique();
        for signature in [still_open, rejected] {
            mock.insert_release_signature(REFUSED_ROW, signature.to_string(), 1, None)
                .await
                .unwrap();
        }

        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        state.in_flight_withdrawals.insert(nonce);
        state
            .remint_cache
            .insert(nonce, make_remint_info(REFUSED_ROW));
        state.pending_signatures.insert(
            nonce,
            vec![
                PendingSig {
                    signature: still_open,
                    last_valid_block_height: 1,
                    blockhash_slot: None,
                },
                PendingSig {
                    signature: rejected,
                    last_valid_block_height: 1,
                    blockhash_slot: None,
                },
            ],
        );

        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(REFUSED_ROW),
            withdrawal_nonce: Some(nonce),
            trace_id: Some("trace-80".to_string()),
            deposit_claim_lease: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration,
            ))),
            rejected,
            None,
            &ctx,
            dummy_instruction(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &tx,
        )
        .await;

        assert_eq!(
            state.rotation_retry_queue.len(),
            1,
            "the release is still queued for the rotation that opens its window"
        );
        let stashed: Vec<Signature> = state
            .pending_signatures
            .get(&nonce)
            .expect("the open signature keeps the stash alive")
            .iter()
            .map(|pending| pending.signature)
            .collect();
        assert_eq!(
            stashed,
            vec![still_open],
            "only the signature the chain confirmed rejected may be dropped"
        );
        let stored: Vec<String> = mock
            .get_release_signatures(REFUSED_ROW)
            .await
            .unwrap()
            .into_iter()
            .map(|stored| stored.signature)
            .collect();
        assert_eq!(
            stored,
            vec![still_open.to_string()],
            "the durable row must go with the stashed copy"
        );
    }

    /// The row has to carry the wait, not just the queue. A crash between the
    /// refusal and the rotation otherwise leaves a release that was never
    /// broadcast sitting in `Processing` with no signatures, which is exactly
    /// what the stale sweep quarantines.
    #[tokio::test]
    async fn a_release_queued_after_an_on_chain_refusal_is_parked() {
        let mut server = mockito::Server::new_async().await;
        let mock = mock_with_processing_row(REFUSED_ROW);
        let (state, _rx) = route_nonce_outside_generation_with(
            &mut server,
            NONCES_PER_GENERATION,
            Some(0),
            mock.clone(),
        )
        .await;

        assert_eq!(state.rotation_retry_queue.len(), 1);
        assert_eq!(
            row_status(&mock, REFUSED_ROW),
            Some(TransactionStatus::Parked),
            "the wait must outlive the process that is waiting"
        );
    }

    /// A park the database refused leaves the queue as the only copy again, so
    /// the entry is dropped and the row is left where the recovery sweep sees it.
    #[tokio::test]
    async fn an_on_chain_refusal_whose_park_was_refused_is_not_queued() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_outside_generation_with(
            &mut server,
            NONCES_PER_GENERATION,
            Some(0),
            MockStorage::new(),
        )
        .await;

        assert!(
            state.rotation_retry_queue.is_empty(),
            "an unparked release must be left to recovery, not held in memory"
        );
        assert!(
            rx.try_recv().is_err(),
            "the row keeps its status for recovery rather than a terminal write"
        );
    }

    /// An unreadable park is not a park.
    #[tokio::test]
    async fn an_on_chain_refusal_whose_park_errored_is_not_queued() {
        let mut server = mockito::Server::new_async().await;
        let mock = mock_with_processing_row(REFUSED_ROW);
        mock.set_should_fail("try_park_processing", true);
        let (state, _rx) =
            route_nonce_outside_generation_with(&mut server, NONCES_PER_GENERATION, Some(0), mock)
                .await;

        assert!(
            state.rotation_retry_queue.is_empty(),
            "an unconfirmed park must not queue financial work"
        );
    }

    /// The rotation can land between the program's refusal and the read that
    /// checks it, which makes the two generations equal. The nonce is releasable
    /// right now, so this is the retry case and not the unrecoverable one that
    /// writes a good withdrawal off.
    #[tokio::test]
    async fn nonce_outside_generation_equal_to_chain_requeues() {
        let mut server = mockito::Server::new_async().await;
        let nonce = NONCES_PER_GENERATION;
        let (state, mut rx) = route_nonce_outside_generation(&mut server, nonce, Some(1)).await;

        assert_eq!(
            state.rotation_retry_queue.len(),
            1,
            "a nonce inside the open window must be retried, not written off"
        );
        assert!(rx.try_recv().is_err(), "no terminal status while queued");
    }

    /// The window is gone, so this nonce can never be released.
    #[tokio::test]
    async fn nonce_outside_generation_behind_chain_reminds() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_outside_generation(&mut server, 1, Some(3)).await;

        assert!(
            rx.try_recv().is_err(),
            "an unreleasable nonce must be reminted, not escalated"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "the compensating remint must be queued"
        );
        assert!(
            state.pending_remints[0].release_refused_on_chain,
            "the refusal is what carries the remint past a bitmap that cannot answer"
        );
        assert!(state.rotation_retry_queue.is_empty());
    }

    /// The pre-send check already parked this one on the rotation retry queue.
    #[tokio::test]
    async fn withheld_release_ahead_of_the_window_writes_no_terminal_status() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(90),
            withdrawal_nonce: Some(NONCES_PER_GENERATION),
            trace_id: Some("trace-90".to_string()),
            deposit_claim_lease: None,
        };

        route_builder_error(
            &mut state,
            &ctx,
            &tx,
            ProgramError::GenerationMismatch {
                nonce: NONCES_PER_GENERATION,
                nonce_generation: 1,
                chain_generation: 0,
            }
            .into(),
        )
        .await;

        assert!(rx.try_recv().is_err(), "the withdrawal is only waiting");
        assert!(state.pending_remints.is_empty());
    }

    /// A release withheld because its window is gone takes the same compensating
    /// route as one the program refused. Nothing was broadcast, so the nonce is
    /// unspent, and the user must not be left holding neither the tokens they
    /// burned nor the funds they were owed.
    #[tokio::test]
    async fn withheld_release_behind_the_window_is_compensated() {
        let mock = mock_with_processing_row(91);
        // An earlier attempt on this nonce reached the network, and its journaled
        // signature is what the compensating remint has to classify first.
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(91, broadcast.to_string(), 1, None)
            .await
            .unwrap();
        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        state.remint_cache.insert(1, make_remint_info(91));
        state.pending_signatures.insert(
            1,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 1,
                blockhash_slot: None,
            }],
        );
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(91),
            withdrawal_nonce: Some(1),
            trace_id: Some("trace-91".to_string()),
            deposit_claim_lease: None,
        };

        route_builder_error(
            &mut state,
            &ctx,
            &tx,
            ProgramError::GenerationMismatch {
                nonce: 1,
                nonce_generation: 0,
                chain_generation: 3,
            }
            .into(),
        )
        .await;

        assert_eq!(state.pending_remints.len(), 1);
        assert!(state.pending_remints[0].release_refused_on_chain);
        assert!(rx.try_recv().is_err(), "reminted, not escalated");
    }

    /// The refusal has to reach the row in the same write that queues the
    /// refund. An operator restarted inside the finality window otherwise comes
    /// back holding the entry but not the one fact that lets it pay the user
    /// back without a human, and the refund stalls in manual review instead.
    #[tokio::test]
    async fn a_withheld_release_persists_the_refusal_with_the_pending_remint() {
        let mock = mock_with_processing_row(91);
        // An earlier attempt on this nonce reached the network, and its journaled
        // signature is what the compensating remint has to classify first.
        let broadcast = Signature::new_unique();
        mock.insert_release_signature(91, broadcast.to_string(), 1, None)
            .await
            .unwrap();
        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        state.remint_cache.insert(1, make_remint_info(91));
        state.pending_signatures.insert(
            1,
            vec![PendingSig {
                signature: broadcast,
                last_valid_block_height: 1,
                blockhash_slot: None,
            }],
        );
        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(91),
            withdrawal_nonce: Some(1),
            trace_id: Some("trace-91".to_string()),
            deposit_claim_lease: None,
        };

        route_builder_error(
            &mut state,
            &ctx,
            &tx,
            ProgramError::GenerationMismatch {
                nonce: 1,
                nonce_generation: 0,
                chain_generation: 3,
            }
            .into(),
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        let calls = mock.pending_remint_signatures.lock().unwrap();
        assert_eq!(calls.len(), 1, "the deferral must be persisted once");
        assert_eq!(calls[0].0, 91);
        assert!(
            calls[0].4,
            "the refusal must be durable, not only in the queued entry"
        );
    }

    // ── refund gate for a refusal with nothing to verify ─────────────

    /// The row every refund case is driven against.
    const REFUSED_TXID: i64 = 95;

    /// Drive the chain-refusal path for `nonce` with nothing stashed to verify.
    ///
    /// The refusal proves the attempt that carried it paid nothing, which is the
    /// strongest evidence this path ever has, and still not enough to refund on.
    async fn refuse_release_without_signatures(
        mock: MockStorage,
        nonce: u64,
    ) -> (SenderState, mpsc::Receiver<TransactionStatusUpdate>) {
        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        // No pending_signatures: the stash is what the gate finds empty.
        state
            .remint_cache
            .insert(nonce, make_remint_info(REFUSED_TXID));

        let (tx, rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(REFUSED_TXID),
            withdrawal_nonce: Some(nonce),
            trace_id: Some("trace-95".to_string()),
            deposit_claim_lease: None,
        };

        remint_after_onchain_refusal(&mut state, &ctx, &tx, "nonce generation rotated past").await;

        (state, rx)
    }

    /// Nothing on record is not the same as nothing happened. An absent release
    /// record can only ever refuse a refund; its silence is never the positive
    /// evidence an unattended payout would need, so a human settles it.
    #[tokio::test]
    async fn a_refused_release_with_no_observed_record_still_escalates() {
        let (state, mut rx) = refuse_release_without_signatures(MockStorage::new(), 7).await;

        let update = rx
            .try_recv()
            .expect("an absent record must be reported, not refunded");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            state.pending_remints.is_empty(),
            "an absent release record must not open a refund"
        );
    }

    /// A release for this nonce is on record, so it already paid out and
    /// refunding would credit the user a second time. The refusal that reached
    /// this path only rules out the attempt that carried it, never an earlier
    /// one that landed.
    #[tokio::test]
    async fn refused_release_with_an_observed_record_escalates() {
        let mock = MockStorage::new();
        mock.insert_observed_releases_batch(&[DbObservedRelease {
            withdrawal_nonce: 7,
            signature: "sig-observed-release".to_string(),
            slot: 4_000,
        }])
        .await
        .unwrap();

        let (state, mut rx) = refuse_release_without_signatures(mock, 7).await;

        let update = rx.try_recv().expect("a paid-out nonce must be reported");
        assert_eq!(update.transaction_id, REFUSED_TXID);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            state.pending_remints.is_empty(),
            "no refund may be queued for a release that already paid out"
        );
    }

    /// Without a readable bitmap we cannot tell which side of the window the
    /// nonce is on, and the two outcomes are terminal in opposite directions.
    /// Leave the row Processing for the recovery worker rather than guess.
    #[tokio::test]
    async fn nonce_outside_generation_rpc_failure_leaves_row_processing() {
        let mut server = mockito::Server::new_async().await;
        let (state, mut rx) = route_nonce_outside_generation(&mut server, 1, None).await;

        assert!(
            rx.try_recv().is_err(),
            "an unreadable bitmap must not write a terminal status"
        );
        assert!(state.rotation_retry_queue.is_empty());
    }

    // ── fire_and_store ────────────────────────────────────────────────

    /// A successful send must push exactly one InFlightTx with poll_attempts=0
    /// and the returned signature; no storage update must be emitted yet.
    #[tokio::test]
    async fn fire_and_store_success_pushes_to_in_flight() {
        let mut server = mockito::Server::new_async().await;

        let expected_sig = Signature::default().to_string();

        let _m_hash = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getLatestBlockhash"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                            "lastValidBlockHeight": 100
                        }
                    }
                })
                .to_string(),
            )
            .create();

        let _m_send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": expected_sig
                })
                .to_string(),
            )
            .create();

        let mut state = {
            SenderState {
                in_flight: InFlightQueue::new(),
                semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
                ..make_sender_state_with_server(&server.url())
            }
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(42),
            withdrawal_nonce: None,
            trace_id: Some("trace-fire".to_string()),
            deposit_claim_lease: None,
        };

        fire_and_store(
            &mut state,
            dummy_instruction(),
            None,
            ctx.clone(),
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            &storage_tx,
            0,
            Arc::new(Semaphore::new(MAX_IN_FLIGHT))
                .try_acquire_owned()
                .unwrap(),
        )
        .await;

        // No storage update yet — confirmation is deferred.
        assert!(
            storage_rx.try_recv().is_err(),
            "fire_and_store must not emit a status update immediately"
        );

        // Exactly one in-flight entry with the expected signature.
        assert_eq!(state.in_flight.len(), 1);
        let guard = state.in_flight.entries.lock().unwrap();
        let entry = &guard[0];
        assert_eq!(entry.signature.to_string(), expected_sig);
        assert_eq!(entry.ctx.transaction_id, Some(42));
        assert_eq!(entry.poll_attempts, 0);
    }

    /// When sendTransaction fails, fire_and_store must route to permanent failure
    /// and emit a Failed status — no in-flight entry should be added.
    #[tokio::test]
    async fn fire_and_store_send_failure_routes_to_permanent_failure() {
        let mut server = mockito::Server::new_async().await;

        // getLatestBlockhash succeeds
        let _m_hash = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getLatestBlockhash"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                            "lastValidBlockHeight": 100
                        }
                    }
                })
                .to_string(),
            )
            .create();

        // sendTransaction returns an RPC error
        let _m_send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Internal error"}
                })
                .to_string(),
            )
            .create();

        let mut state = {
            SenderState {
                in_flight: InFlightQueue::new(),
                semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
                ..make_sender_state_with_server(&server.url())
            }
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(55),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        fire_and_store(
            &mut state,
            dummy_instruction(),
            None,
            ctx,
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            &storage_tx,
            0,
            Arc::new(Semaphore::new(MAX_IN_FLIGHT))
                .try_acquire_owned()
                .unwrap(),
        )
        .await;

        // Failed status must be emitted immediately.
        let update = storage_rx
            .try_recv()
            .expect("expected Failed status update");
        assert_eq!(update.transaction_id, 55);
        assert_eq!(update.status, TransactionStatus::Failed);

        // Nothing pushed to in_flight.
        assert!(
            state.in_flight.is_empty(),
            "in_flight must stay empty on send failure"
        );
    }

    // ── poll_in_flight ────────────────────────────────────────────────

    fn make_in_flight_tx(sig: Signature, txn_id: i64) -> super::super::types::InFlightTx {
        super::super::types::InFlightTx {
            signature: sig,
            ctx: TransactionContext {
                kind: TransactionKind::Mint,
                transaction_id: Some(txn_id),
                withdrawal_nonce: None,
                trace_id: Some(format!("trace-{txn_id}")),
                deposit_claim_lease: None,
            },
            instruction: dummy_instruction(),
            compute_unit_price: None,
            retry_policy: RetryPolicy::None,
            extra_error_checks_policy: ExtraErrorCheckPolicy::None,
            poll_attempts: 0,
            resend_count: 0,
            // Default to not-persisted; tests that model a write-ahead-persisted
            // deposit mint set `persisted = true` on the returned value explicitly.
            persisted: false,
            permit: Arc::new(Semaphore::new(MAX_IN_FLIGHT))
                .try_acquire_owned()
                .unwrap(),
        }
    }

    /// A finalized signature in the batch must route to handle_success, emitting
    /// a Completed status and removing the entry from in_flight.
    #[tokio::test]
    async fn poll_in_flight_finalized_tx_emits_completed() {
        let mut server = mockito::Server::new_async().await;

        let sig = Signature::new_unique();

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "confirmationStatus": "finalized",
                            "confirmations": null,
                            "err": null,
                            "slot": 100,
                            "status": {"Ok": null}
                        }]
                    }
                })
                .to_string(),
            )
            .create();
        let mut state = SenderState {
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig, 77));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            ..make_sender_state_with_server(&server.url())
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // Entry removed from in_flight after confirmation.
        assert!(
            state.in_flight.is_empty(),
            "in_flight must be empty after confirmation"
        );

        // Completed status emitted.
        let update = storage_rx.try_recv().expect("expected Completed status");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.status, TransactionStatus::Completed);
    }

    /// A not-yet-confirmed tx should stay in in_flight with an incremented poll_attempts counter
    /// and no storage update must be emitted.
    #[tokio::test]
    async fn poll_in_flight_unconfirmed_tx_stays_in_flight() {
        let mut server = mockito::Server::new_async().await;

        let sig = Signature::new_unique();

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 10},
                        "value": [null]   // not yet seen by RPC
                    }
                })
                .to_string(),
            )
            .create();
        let mut state = SenderState {
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig, 88));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            ..make_sender_state_with_server(&server.url())
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // Still in-flight with incremented counter.
        assert_eq!(state.in_flight.len(), 1);
        assert_eq!(state.in_flight.entries.lock().unwrap()[0].poll_attempts, 1);

        // No storage update.
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update for pending tx"
        );
    }

    /// On RPC error, the entire batch must be kept in-flight untouched for retry on the
    /// next tick — poll_attempts must NOT be incremented (the RPC call did not count).
    #[tokio::test]
    async fn poll_in_flight_rpc_error_keeps_batch_unchanged() {
        let mut server = mockito::Server::new_async().await;

        let sig = Signature::new_unique();

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Internal error"}
                })
                .to_string(),
            )
            .create();
        let mut state = SenderState {
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig, 99));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            ..make_sender_state_with_server(&server.url())
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // Batch unchanged — RPC error is transient.
        assert_eq!(
            state.in_flight.len(),
            1,
            "in_flight must be unchanged on RPC error"
        );
        assert_eq!(
            state.in_flight.entries.lock().unwrap()[0].poll_attempts,
            0,
            "poll_attempts must not increment on RPC error"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no storage update on RPC error"
        );
    }

    /// When poll_attempts reaches MAX_POLL_ATTEMPTS_CONFIRMATION for a persisted
    /// RetryPolicy::None mint, the broadcast may have landed, so it must be removed
    /// from in_flight and left Processing for recovery (no terminal Failed write).
    #[tokio::test]
    async fn poll_in_flight_timeout_persisted_mint_left_processing() {
        let mut server = mockito::Server::new_async().await;

        let sig = Signature::new_unique();

        // Return "not confirmed" enough times to trigger timeout
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"context": {"slot": 10}, "value": [null]}
                })
                .to_string(),
            )
            .expect(1)
            .create();
        let mut state = SenderState {
            in_flight: {
                let q = InFlightQueue::new();
                let mut tx = make_in_flight_tx(sig, 101);
                // Pre-fill poll_attempts to one below MAX so this poll tips it over.
                tx.poll_attempts = MAX_POLL_ATTEMPTS_CONFIRMATION - 1;
                // A real None-policy mint reaches in_flight only after a write-ahead persist.
                tx.persisted = true;
                q.push(tx);
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            ..make_sender_state_with_server(&server.url())
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // Entry removed from in_flight.
        assert!(
            state.in_flight.is_empty(),
            "timed-out tx must leave in_flight"
        );

        // No terminal status: the row is left Processing for recovery to reconcile
        // against the persisted signature, never written Failed here.
        assert!(
            storage_rx.try_recv().is_err(),
            "persisted mint timeout must not write a terminal status",
        );
    }

    /// poll_in_flight with an empty in_flight must be a no-op (no RPC call, no storage update).
    #[tokio::test]
    async fn poll_in_flight_empty_is_noop() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // No mock server needed — should not make any RPC call.
        poll_in_flight(&mut state, &storage_tx).await;

        assert!(state.in_flight.is_empty());
        assert!(storage_rx.try_recv().is_err());
    }

    /// A mixed batch (one finalized, one pending) must resolve the finalized entry while
    /// keeping the pending entry in in_flight with an incremented poll_attempts.
    #[tokio::test]
    async fn poll_in_flight_mixed_batch_partial_resolution() {
        let mut server = mockito::Server::new_async().await;

        let sig1 = Signature::new_unique();
        let sig2 = Signature::new_unique();

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 200},
                        "value": [
                            // sig1 finalized
                            {
                                "confirmationStatus": "finalized",
                                "confirmations": null,
                                "err": null,
                                "slot": 200,
                                "status": {"Ok": null}
                            },
                            // sig2 not yet confirmed
                            null
                        ]
                    }
                })
                .to_string(),
            )
            .create();
        let mut state = SenderState {
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig1, 201));
                q.push(make_in_flight_tx(sig2, 202));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            ..make_sender_state_with_server(&server.url())
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // sig1 resolved — only sig2 remains.
        assert_eq!(state.in_flight.len(), 1, "only pending tx remains");
        {
            let guard = state.in_flight.entries.lock().unwrap();
            assert_eq!(guard[0].ctx.transaction_id, Some(202));
            assert_eq!(guard[0].poll_attempts, 1);
        }

        // Completed for sig1, nothing for sig2 yet.
        let update = storage_rx.try_recv().expect("expected Completed for sig1");
        assert_eq!(update.transaction_id, 201);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert!(storage_rx.try_recv().is_err(), "no update for pending sig2");
    }

    // ── poll_in_flight: chunking ──────────────────────────────────────

    /// When in_flight exceeds 256 entries (the getSignatureStatuses limit), poll_in_flight
    /// must issue multiple RPC calls, one per 256-sig chunk, and merge the results.
    ///
    /// Each chunk response is sized to exactly the number of signatures requested (256 then
    /// 44) so the length gate passes and the legitimate multi-chunk merge is exercised.
    #[tokio::test]
    async fn poll_in_flight_chunks_large_batch() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body_from_request(|req| {
                let v: serde_json::Value =
                    serde_json::from_slice(req.body().expect("request body present"))
                        .expect("request body is json");
                let requested = v["params"][0].as_array().map(|a| a.len()).unwrap_or(0);
                null_value_body(requested).into_bytes()
            })
            .expect_at_least(2) // 256 sigs -> chunk 1; 44 sigs -> chunk 2
            .create();

        let total = 300usize;
        let mut state = make_sender_state_with_server(&server.url());
        for i in 0..total {
            state
                .in_flight
                .push(make_in_flight_tx(Signature::new_unique(), i as i64 + 1));
        }

        let (storage_tx, _rx) = mpsc::channel(10);
        poll_in_flight(&mut state, &storage_tx).await;

        // All entries stay in-flight (all statuses were null → not confirmed).
        assert_eq!(
            state.in_flight.len(),
            total,
            "all entries must stay in-flight"
        );
        _m.assert(); // verifies ≥ 2 RPC calls were made
    }

    // ── status fetch length gate ─────────────────────────────────────

    // An RpcClientWithRetry pointed at a mockito server, failing fast.
    fn make_rpc_client(url: &str) -> RpcClientWithRetry {
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

    // A getSignatureStatuses response body with `count` null status slots.
    fn null_value_body(count: usize) -> String {
        let value = vec![serde_json::Value::Null; count];
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"context": {"slot": 1}, "value": value}
        })
        .to_string()
    }

    // A getSignatureStatuses response body with `count` finalized-success slots.
    fn finalized_value_body(count: usize) -> String {
        let one = serde_json::json!({
            "confirmationStatus": "finalized",
            "confirmations": null,
            "err": null,
            "slot": 100,
            "status": {"Ok": null}
        });
        let value = vec![one; count];
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"context": {"slot": 100}, "value": value}
        })
        .to_string()
    }

    // A getSignatureStatuses response body with `count` confirmed-success slots
    // that have not yet finalized, so a fork can still drop them.
    fn confirmed_value_body(count: usize) -> String {
        let one = serde_json::json!({
            "confirmationStatus": "confirmed",
            "confirmations": 1,
            "err": null,
            "slot": 100,
            "status": {"Ok": null}
        });
        let value = vec![one; count];
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"context": {"slot": 100}, "value": value}
        })
        .to_string()
    }

    // Mock getSignatureStatuses; the per-call counter lets a test shape one chunk
    // while sizing the rest to their request.
    fn mock_status_bodies<F>(server: &mut mockito::ServerGuard, f: F) -> mockito::Mock
    where
        F: Fn(usize, usize) -> String + Send + Sync + 'static,
    {
        let counter = Arc::new(AtomicUsize::new(0));
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body_from_request(move |req| {
                let idx = counter.fetch_add(1, Ordering::SeqCst);
                let body = req.body().expect("request body present");
                let v: serde_json::Value =
                    serde_json::from_slice(body).expect("request body is json");
                let requested = v["params"][0].as_array().map(|a| a.len()).unwrap_or(0);
                f(idx, requested).into_bytes()
            })
            .expect_at_least(1)
            .create()
    }

    /// An exactly-sized single chunk returns Ok with the requested length.
    #[tokio::test]
    async fn fetch_statuses_exact_single_chunk_ok() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| null_value_body(req));
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..10).map(|_| Signature::new_unique()).collect();

        let statuses = fetch_statuses_checked(&rpc, &sigs)
            .await
            .expect("exact chunk must be Ok");
        assert_eq!(statuses.len(), 10);
    }

    /// A short single chunk (N-1 for N) is rejected.
    #[tokio::test]
    async fn fetch_statuses_short_single_chunk_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| null_value_body(req - 1));
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..10).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// An oversized single chunk (N+1 for N) is rejected.
    #[tokio::test]
    async fn fetch_statuses_oversized_single_chunk_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| null_value_body(req + 1));
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..10).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// An empty value array for a non-empty request is rejected.
    #[tokio::test]
    async fn fetch_statuses_empty_value_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, _req| null_value_body(0));
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..5).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// Every chunk exact returns Ok, concatenated in request order.
    #[tokio::test]
    async fn fetch_statuses_multi_chunk_all_exact_ok_ordered() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 0 {
                finalized_value_body(req)
            } else {
                null_value_body(req)
            }
        });
        let rpc = make_rpc_client(&server.url());
        // 600 sigs -> chunks of 256 + 256 + 88.
        let sigs: Vec<Signature> = (0..600).map(|_| Signature::new_unique()).collect();

        let statuses = fetch_statuses_checked(&rpc, &sigs)
            .await
            .expect("all-exact multi-chunk must be Ok");
        assert_eq!(statuses.len(), 600);
        // First chunk finalized, remaining chunks null: proves concatenation order.
        assert!(statuses[0].is_some());
        assert!(statuses[255].is_some());
        assert!(statuses[256].is_none());
        assert!(statuses[599].is_none());
    }

    /// A short first chunk is rejected.
    #[tokio::test]
    async fn fetch_statuses_short_first_chunk_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 0 {
                null_value_body(req - 1)
            } else {
                null_value_body(req)
            }
        });
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..600).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// A short middle chunk is rejected.
    #[tokio::test]
    async fn fetch_statuses_short_middle_chunk_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 1 {
                null_value_body(req - 1)
            } else {
                null_value_body(req)
            }
        });
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..600).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// A short final chunk is rejected.
    #[tokio::test]
    async fn fetch_statuses_short_final_chunk_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 2 {
                null_value_body(req - 1)
            } else {
                null_value_body(req)
            }
        });
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..600).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// An oversized middle chunk is rejected.
    #[tokio::test]
    async fn fetch_statuses_oversized_middle_chunk_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 1 {
                null_value_body(req + 1)
            } else {
                null_value_body(req)
            }
        });
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..600).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// An RPC transport error on a chunk is surfaced as Err.
    #[tokio::test]
    async fn fetch_statuses_rpc_error_err() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Internal error"}
                })
                .to_string(),
            )
            .create();
        let rpc = make_rpc_client(&server.url());
        let sigs: Vec<Signature> = (0..3).map(|_| Signature::new_unique()).collect();

        assert!(fetch_statuses_checked(&rpc, &sigs).await.is_err());
    }

    /// An empty signature slice returns Ok(empty) and issues no RPC call.
    #[tokio::test]
    async fn fetch_statuses_empty_slice_ok_no_call() {
        let mut server = mockito::Server::new_async().await;
        // Any call would be a bug: assert the mock is never hit.
        let m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(null_value_body(0))
            .expect(0)
            .create();
        let rpc = make_rpc_client(&server.url());

        let statuses = fetch_statuses_checked(&rpc, &[])
            .await
            .expect("empty is Ok");
        assert!(statuses.is_empty());
        m.assert();
    }

    /// A short only-chunk reinserts the full batch and settles nothing.
    #[tokio::test]
    async fn poll_in_flight_short_chunk_full_reinsert_no_settlement() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| null_value_body(req - 1));

        let mut state = make_sender_state_with_server(&server.url());
        let ids: Vec<i64> = (1..=5).collect();
        for id in &ids {
            state
                .in_flight
                .push(make_in_flight_tx(Signature::new_unique(), *id));
        }

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        poll_in_flight(&mut state, &storage_tx).await;

        assert_eq!(state.in_flight.len(), 5, "all entries reinserted");
        {
            let guard = state.in_flight.entries.lock().unwrap();
            let mut present: Vec<i64> = guard.iter().filter_map(|t| t.ctx.transaction_id).collect();
            present.sort_unstable();
            assert_eq!(present, ids, "no entry dropped");
            assert!(
                guard.iter().all(|t| t.poll_attempts == 0),
                "poll_attempts not incremented on a malformed cycle"
            );
        }
        assert!(storage_rx.try_recv().is_err(), "no Completed emitted");
    }

    /// 257 entries across a chunk boundary, chunk 1 confirmed and chunk 2 short:
    /// nothing may settle and no entry may be dropped.
    #[tokio::test]
    async fn poll_in_flight_cross_chunk_short_no_misattribution() {
        let mut server = mockito::Server::new_async().await;
        // Chunk 0 (256 sigs) all finalized; chunk 1 (1 sig) returns an empty value.
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 0 {
                finalized_value_body(req)
            } else {
                null_value_body(0)
            }
        });

        let mut state = make_sender_state_with_server(&server.url());
        let ids: Vec<i64> = (1..=257).collect();
        for id in &ids {
            state
                .in_flight
                .push(make_in_flight_tx(Signature::new_unique(), *id));
        }

        let (storage_tx, mut storage_rx) = mpsc::channel(300);
        poll_in_flight(&mut state, &storage_tx).await;

        assert_eq!(state.in_flight.len(), 257, "all 257 entries reinserted");
        {
            let guard = state.in_flight.entries.lock().unwrap();
            let mut present: Vec<i64> = guard.iter().filter_map(|t| t.ctx.transaction_id).collect();
            present.sort_unstable();
            assert_eq!(present, ids, "tail entry (id 257) not dropped");
        }
        assert!(
            storage_rx.try_recv().is_err(),
            "no Completed for any transaction on a malformed cross-chunk cycle"
        );
    }

    /// An oversized chunk is caught by the same gate as the short-chunk cases.
    #[tokio::test]
    async fn poll_in_flight_oversized_chunk_full_reinsert_no_settlement() {
        let mut server = mockito::Server::new_async().await;
        // Chunk 0 (256) finalized; chunk 1 (1 sig) returns two statuses (oversized).
        let _m = mock_status_bodies(&mut server, |idx, req| {
            if idx == 0 {
                finalized_value_body(req)
            } else {
                null_value_body(req + 1)
            }
        });

        let mut state = make_sender_state_with_server(&server.url());
        let ids: Vec<i64> = (1..=257).collect();
        for id in &ids {
            state
                .in_flight
                .push(make_in_flight_tx(Signature::new_unique(), *id));
        }

        let (storage_tx, mut storage_rx) = mpsc::channel(300);
        poll_in_flight(&mut state, &storage_tx).await;

        assert_eq!(state.in_flight.len(), 257, "all entries reinserted");
        assert!(storage_rx.try_recv().is_err(), "no Completed emitted");
    }

    /// Happy-path multi-chunk: every chunk is exact, so each entry settles paired
    /// with its own signature.
    #[tokio::test]
    async fn poll_in_flight_multi_chunk_confirmed_settles_with_correct_pairing() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| finalized_value_body(req));

        let mut state = make_sender_state_with_server(&server.url());
        let total = 300usize;
        let mut sig_by_id: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();
        for i in 0..total {
            let sig = Signature::new_unique();
            let id = i as i64 + 1;
            sig_by_id.insert(id, sig.to_string());
            state.in_flight.push(make_in_flight_tx(sig, id));
        }

        let (storage_tx, mut storage_rx) = mpsc::channel(total + 10);
        poll_in_flight(&mut state, &storage_tx).await;

        assert!(state.in_flight.is_empty(), "all confirmed entries settled");
        let mut seen = 0usize;
        while let Ok(update) = storage_rx.try_recv() {
            assert_eq!(update.status, TransactionStatus::Completed);
            assert_eq!(
                update.counterpart_signature.as_deref(),
                sig_by_id.get(&update.transaction_id).map(|s| s.as_str()),
                "Completed must pair each transaction with its own signature"
            );
            seen += 1;
        }
        assert_eq!(seen, total, "exactly one Completed per transaction");
    }

    /// The production poll task applies the same gate: a short chunk reinserts the
    /// batch and settles nothing.
    #[tokio::test]
    async fn run_poll_task_short_chunk_reinserts_no_settlement() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| null_value_body(req - 1));

        let in_flight = InFlightQueue::new();
        let (result_tx, mut result_rx) = mpsc::channel::<Vec<PollTaskResult>>(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let rpc = Arc::new(make_rpc_client(&server.url()));
        let token = tokio_util::sync::CancellationToken::new();

        let ids: Vec<i64> = (1..=5).collect();
        for id in &ids {
            in_flight.push(make_in_flight_tx(Signature::new_unique(), *id));
        }

        let handle = tokio::spawn(run_poll_task(
            in_flight.clone(),
            result_tx,
            rpc,
            storage_tx,
            ProgramType::Escrow,
            5,
            token.clone(),
        ));

        // Give the task time to drain, poll, hit the short response, and reinsert.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("task must exit after cancellation")
            .expect("task must not panic");

        assert!(
            result_rx.try_recv().is_err(),
            "no PollTaskResult on malformed cycle"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no Completed on malformed cycle"
        );
        assert_eq!(in_flight.len(), 5, "batch reinserted after short chunk");
        let mut present: Vec<i64> = {
            let guard = in_flight.entries.lock().unwrap();
            guard.iter().filter_map(|t| t.ctx.transaction_id).collect()
        };
        present.sort_unstable();
        assert_eq!(present, ids, "no entry dropped");
        // Prove the task actually polled, so the negative assertions are not vacuous.
        _m.assert();
    }

    /// A confirmed-but-not-finalized status can still be forked out, so routing it
    /// as settled would credit a mint the chain never kept. It must stay in flight.
    #[tokio::test]
    async fn poll_in_flight_confirmed_not_finalized_does_not_settle() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| confirmed_value_body(req));

        let mut state = make_sender_state_with_server(&server.url());
        state
            .in_flight
            .push(make_in_flight_tx(Signature::new_unique(), 91));

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        poll_in_flight(&mut state, &storage_tx).await;

        assert_eq!(
            state.in_flight.len(),
            1,
            "a non-finalized entry must remain in flight"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no Completed may be written before finalization"
        );
        _m.assert();
    }

    /// The production poll task applies the same finality gate as poll_in_flight.
    #[tokio::test]
    async fn run_poll_task_confirmed_not_finalized_does_not_settle() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| confirmed_value_body(req));

        let in_flight = InFlightQueue::new();
        let (result_tx, mut result_rx) = mpsc::channel::<Vec<PollTaskResult>>(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let rpc = Arc::new(make_rpc_client(&server.url()));
        let token = tokio_util::sync::CancellationToken::new();

        in_flight.push(make_in_flight_tx(Signature::new_unique(), 92));

        let handle = tokio::spawn(run_poll_task(
            in_flight.clone(),
            result_tx,
            rpc,
            storage_tx,
            ProgramType::Escrow,
            5,
            token.clone(),
        ));

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("task must exit after cancellation")
            .expect("task must not panic");

        assert!(
            storage_rx.try_recv().is_err(),
            "no Completed may be written before finalization"
        );
        // Repeated polls eventually hand the entry over on the timeout path; what
        // must never happen is the task calling it a settled success.
        while let Ok(batch) = result_rx.try_recv() {
            for result in batch {
                assert!(
                    matches!(result, PollTaskResult::NeedsRouting(_, None)),
                    "a non-finalized status must not be routed as confirmed"
                );
            }
        }
        _m.assert();
    }

    /// Happy-path multi-chunk on the production task settles every entry with its
    /// own signature.
    #[tokio::test]
    async fn run_poll_task_multi_chunk_confirmed_settles() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_status_bodies(&mut server, |_idx, req| finalized_value_body(req));

        let in_flight = InFlightQueue::new();
        let (result_tx, _result_rx) = mpsc::channel::<Vec<PollTaskResult>>(8);
        let (storage_tx, mut storage_rx) = mpsc::channel(400);
        let rpc = Arc::new(make_rpc_client(&server.url()));
        let token = tokio_util::sync::CancellationToken::new();

        let total = 300usize;
        let mut sig_by_id: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();
        for i in 0..total {
            let sig = Signature::new_unique();
            let id = i as i64 + 1;
            sig_by_id.insert(id, sig.to_string());
            in_flight.push(make_in_flight_tx(sig, id));
        }

        let handle = tokio::spawn(run_poll_task(
            in_flight.clone(),
            result_tx,
            rpc,
            storage_tx,
            ProgramType::Escrow,
            5,
            token.clone(),
        ));

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("task must exit after cancellation")
            .expect("task must not panic");

        assert_eq!(in_flight.len(), 0, "all confirmed entries settled");
        let mut seen = 0usize;
        while let Ok(update) = storage_rx.try_recv() {
            assert_eq!(update.status, TransactionStatus::Completed);
            assert_eq!(
                update.counterpart_signature.as_deref(),
                sig_by_id.get(&update.transaction_id).map(|s| s.as_str()),
                "Completed must pair each transaction with its own signature"
            );
            seen += 1;
        }
        assert_eq!(seen, total, "exactly one Completed per transaction");
    }

    /// An idempotent tx that exhausts its resend_count budget must be declared a
    /// permanent failure rather than re-queued indefinitely (infinite loop guard).
    #[tokio::test]
    async fn poll_in_flight_idempotent_resend_limit_triggers_permanent_failure() {
        let mut server = mockito::Server::new_async().await;

        let sig = Signature::new_unique();

        // RPC returns null (not confirmed) — triggering the timeout arm.
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 10},
                        "value": [null]
                    }
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create();

        let retry_max = 2u32;
        let mut state = make_sender_state_with_server(&server.url());
        state.retry_max_attempts = retry_max;
        {
            let mut tx = make_in_flight_tx(sig, 77);
            tx.retry_policy = RetryPolicy::Idempotent;
            // Already at the cap — next_resend (3) > retry_max (2).
            tx.resend_count = retry_max;
            tx.poll_attempts = MAX_POLL_ATTEMPTS_CONFIRMATION; // trigger timeout arm
            *state.in_flight.entries.lock().unwrap() = vec![tx];
        }

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // Must have been removed from in_flight.
        assert!(
            state.in_flight.is_empty(),
            "exhausted tx must leave in_flight"
        );

        // Permanent failure status must be emitted.
        let update = storage_rx
            .try_recv()
            .expect("expected permanent-failure status update");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert!(
            update
                .error_message
                .as_deref()
                .unwrap_or("")
                .contains("resend limit"),
            "error message should mention resend limit: {:?}",
            update.error_message
        );
    }

    // ── fire_and_store_task: deposit-mint pre-broadcast persist ───────

    fn mint_ctx(txn_id: i64) -> TransactionContext {
        TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(txn_id),
            withdrawal_nonce: None,
            trace_id: Some(format!("trace-{txn_id}")),
            deposit_claim_lease: None,
        }
    }

    /// A sender holding the `Processing` deposit row its claim CASes against,
    /// plus the token the processor would have handed the builder.
    fn mint_state_with_lease(rpc_url: &str, txn_id: i64) -> (SenderState, chrono::DateTime<Utc>) {
        let mock = MockStorage::new();
        push_processing_deposit_row(&mock, txn_id);
        let lease = row_updated_at(&mock, txn_id).expect("seeded row present");
        (sender_state_with_storage(rpc_url, mock), lease)
    }

    /// A persisting (Mint) fire-and-store run writes the signed transaction's signature
    /// via `insert_release_signature` and then broadcasts that same signature.
    #[tokio::test]
    async fn mint_persists_signature_before_send() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": Signature::default().to_string()
                })
                .to_string(),
            )
            .expect(1)
            .create();

        let (state, lease) = mint_state_with_lease(&server.url(), 77);
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        let (storage_tx, _rx) = mpsc::channel(10);

        fire_and_store_task(
            state.rpc_client.clone(),
            state.storage.clone(),
            state.in_flight.clone(),
            state.program_type,
            dummy_instruction(),
            None,
            mint_ctx(77),
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Recoverable {
                deposit_expected_updated_at: lease,
            },
            permit,
        )
        .await;

        send.assert();
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        let stored = mock.get_release_signatures(77).await.unwrap();
        assert_eq!(stored.len(), 1, "exactly one mint signature persisted");
        assert_eq!(
            stored[0].signature,
            Signature::default().to_string(),
            "persisted signature must be the broadcast signature"
        );
        assert_eq!(
            stored[0].last_valid_block_height, 100,
            "persisted lvbh must match the blockhash"
        );
        assert_eq!(
            state.in_flight.len(),
            1,
            "successful broadcast stashes the in-flight tx"
        );
    }

    /// A failed write-ahead persist on the mint path must NOT broadcast, must stash no
    /// in-flight entry, and must write no terminal status (row left Processing).
    #[tokio::test]
    async fn mint_persist_failure_aborts_before_broadcast() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .expect(0)
            .create();

        let (state, lease) = mint_state_with_lease(&server.url(), 77);
        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        mock.set_should_fail("insert_release_signature", true);
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        let before = state.semaphore.available_permits();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        fire_and_store_task(
            state.rpc_client.clone(),
            state.storage.clone(),
            state.in_flight.clone(),
            state.program_type,
            dummy_instruction(),
            None,
            mint_ctx(77),
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Recoverable {
                deposit_expected_updated_at: lease,
            },
            permit,
        )
        .await;

        send.assert();
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update; row stays Processing for recovery"
        );
        assert!(
            state.in_flight.is_empty(),
            "nothing stashed when persist failed"
        );
        assert_eq!(
            state.semaphore.available_permits(),
            before + 1,
            "permit must be dropped on abort"
        );
    }

    /// Even an explicit node rejection can be a stale-node false negative, so once the
    /// mint signature is persisted no send error may terminalize the row: a Failed write
    /// would strand a funded deposit and drop the signature recovery reconciles against.
    #[tokio::test]
    async fn mint_send_error_after_persist_leaves_processing() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let _send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Internal error"}
                })
                .to_string(),
            )
            .create();

        let (state, lease) = mint_state_with_lease(&server.url(), 77);
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        let before = state.semaphore.available_permits();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        fire_and_store_task(
            state.rpc_client.clone(),
            state.storage.clone(),
            state.in_flight.clone(),
            state.program_type,
            dummy_instruction(),
            None,
            mint_ctx(77),
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Recoverable {
                deposit_expected_updated_at: lease,
            },
            permit,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert!(
            !mock.get_release_signatures(77).await.unwrap().is_empty(),
            "signature must be persisted before the failing broadcast",
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a persisted mint must not be written Failed on a send error",
        );
        assert!(
            state.in_flight.is_empty(),
            "a failed broadcast stashes no in-flight entry",
        );
        assert_eq!(
            state.semaphore.available_permits(),
            before + 1,
            "permit must be dropped on send error",
        );
    }

    /// A non-persisting run (persist = false) broadcasts without writing any signature
    /// even though a transaction_id is present, proving the `persist` gate (not the
    /// id-presence guard) is what excludes the on-chain-idempotent initialization path.
    #[tokio::test]
    async fn initialize_mint_does_not_persist() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash(&mut server);
        let _send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": Signature::default().to_string()
                })
                .to_string(),
            )
            .create();

        let state = make_sender_state_with_server(&server.url());
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        let (storage_tx, _rx) = mpsc::channel(10);

        // Carry a transaction_id so the assertion exercises the `persist` gate
        // itself rather than the inner id-presence guard short-circuiting.
        let ctx = TransactionContext {
            kind: TransactionKind::InitializeMint,
            transaction_id: Some(909),
            withdrawal_nonce: None,
            trace_id: Some("trace-init".to_string()),
            deposit_claim_lease: None,
        };

        fire_and_store_task(
            state.rpc_client.clone(),
            state.storage.clone(),
            state.in_flight.clone(),
            state.program_type,
            dummy_instruction(),
            None,
            ctx,
            RetryPolicy::Idempotent,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Terminal,
            permit,
        )
        .await;

        let Storage::Mock(ref mock) = *state.storage else {
            panic!("expected mock storage");
        };
        assert!(
            mock.get_release_signatures(909).await.unwrap().is_empty(),
            "persist = false must not write a signature even with a transaction_id"
        );
        assert_eq!(
            state.in_flight.len(),
            1,
            "broadcast still stashes in-flight"
        );
    }

    /// A blockhash fetch that fails, so `build_and_sign` returns before any
    /// signature exists. Paired with a `sendTransaction` mock that must stay unused.
    fn mock_blockhash_failure(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getLatestBlockhash"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32603, "message": "node behind"}
                })
                .to_string(),
            )
            .create()
    }

    fn mock_send_never_called(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .expect(0)
            .create()
    }

    /// Build and sign run before any signature exists, so a failure there broadcast
    /// nothing and minted nothing. A terminal Failed would strand a deposit whose
    /// source funds are already escrowed and that no worker re-claims.
    #[tokio::test]
    async fn recoverable_mint_build_sign_failure_leaves_processing() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash_failure(&mut server);
        let send = mock_send_never_called(&mut server);

        let (state, lease) = mint_state_with_lease(&server.url(), 78);
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        let before = state.semaphore.available_permits();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        fire_and_store_task(
            state.rpc_client.clone(),
            state.storage.clone(),
            state.in_flight.clone(),
            state.program_type,
            dummy_instruction(),
            None,
            mint_ctx(78),
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Recoverable {
                deposit_expected_updated_at: lease,
            },
            permit,
        )
        .await;

        send.assert();
        assert!(
            storage_rx.try_recv().is_err(),
            "a funded deposit must not be terminalized on a build/sign failure"
        );
        assert!(state.in_flight.is_empty(), "nothing was broadcast");
        assert_eq!(
            state.semaphore.available_permits(),
            before + 1,
            "permit must be dropped on abort"
        );
    }

    /// An InitializeMint moves no balance and is on-chain idempotent, so its
    /// build/sign failure still fails fast rather than waiting for recovery.
    #[tokio::test]
    async fn terminal_send_build_sign_failure_still_fails_fast() {
        let mut server = mockito::Server::new_async().await;
        let _hash = mock_blockhash_failure(&mut server);
        let send = mock_send_never_called(&mut server);

        let state = make_sender_state_with_server(&server.url());
        let permit = state.semaphore.clone().try_acquire_owned().unwrap();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::InitializeMint,
            transaction_id: Some(910),
            withdrawal_nonce: None,
            trace_id: Some("trace-init".to_string()),
            deposit_claim_lease: None,
        };

        fire_and_store_task(
            state.rpc_client.clone(),
            state.storage.clone(),
            state.in_flight.clone(),
            state.program_type,
            dummy_instruction(),
            None,
            ctx,
            RetryPolicy::Idempotent,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Terminal,
            permit,
        )
        .await;

        send.assert();
        let update = storage_rx.try_recv().expect("terminal send must escalate");
        assert_eq!(update.transaction_id, 910);
        assert_eq!(update.status, TransactionStatus::Failed);
    }

    /// The catch-all builder-error arm runs before anything is signed or sent, so
    /// a deposit reaching it is unspent on this side and still funded on the other.
    /// Marking it Failed hands the row to no worker and hides the escrowed funds.
    #[tokio::test]
    async fn builder_error_on_a_deposit_mint_writes_no_terminal_status() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);

        route_builder_error(
            &mut state,
            &mint_ctx(79),
            &tx,
            ProgramError::InvalidBuilder {
                reason: "No signers provided".to_string(),
            }
            .into(),
        )
        .await;

        assert!(
            rx.try_recv().is_err(),
            "a funded deposit must not be terminalized on a build error"
        );
    }

    /// The same arm still escalates a withdrawal: nothing about it is funded on
    /// the source side, and its remint path owns the compensation.
    #[tokio::test]
    async fn builder_error_on_a_withdrawal_still_escalates() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(80),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-80".to_string()),
            deposit_claim_lease: None,
        };

        route_builder_error(
            &mut state,
            &ctx,
            &tx,
            ProgramError::InvalidBuilder {
                reason: "No signers provided".to_string(),
            }
            .into(),
        )
        .await;

        let update = rx.try_recv().expect("a build error still escalates here");
        assert_eq!(update.transaction_id, 80);
    }

    // ── spawn_fire_and_store: cap enforcement ─────────────────────────

    /// When the semaphore is exhausted (all MAX_IN_FLIGHT slots occupied),
    /// `spawn_fire_and_store` must return `false` without spawning any task
    /// or emitting any storage update. DB status stays unchanged so the
    /// fetcher can re-emit the transaction on the next poll cycle.
    #[tokio::test]
    async fn spawn_fire_and_store_cap_exhausted_returns_false() {
        let state = make_sender_state();

        // Hold all permits — simulates MAX_IN_FLIGHT tasks in-flight.
        let _permits: Vec<_> = (0..MAX_IN_FLIGHT)
            .map(|_| state.semaphore.clone().try_acquire_owned().unwrap())
            .collect();
        assert_eq!(state.semaphore.available_permits(), 0);

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::Mint,
            transaction_id: Some(9999),
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        let result = spawn_fire_and_store(
            &state,
            dummy_instruction(),
            None,
            ctx,
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Terminal,
        );

        assert!(!result, "must return false when at capacity");
        // Yield to ensure any erroneously spawned tasks have time to run.
        tokio::task::yield_now().await;
        assert!(storage_rx.try_recv().is_err(), "no storage update expected");
        // Queue stays empty — no entry pushed.
        assert!(state.in_flight.is_empty());
    }

    /// When capacity is available, `spawn_fire_and_store` must return `true` and
    /// the permit must be consumed immediately (before the RPC call completes),
    /// so back-pressure is applied as soon as the task starts, not after it finishes.
    #[tokio::test]
    async fn spawn_fire_and_store_available_capacity_returns_true_and_consumes_permit() {
        let state = make_sender_state();
        assert_eq!(state.semaphore.available_permits(), MAX_IN_FLIGHT);

        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let result = spawn_fire_and_store(
            &state,
            dummy_instruction(),
            None,
            TransactionContext {
                kind: TransactionKind::Mint,
                transaction_id: Some(1),
                withdrawal_nonce: None,
                trace_id: None,
                deposit_claim_lease: None,
            },
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            SendDurability::Terminal,
        );

        assert!(result, "must return true when capacity is available");
        // Permit must be consumed before spawn returns — regardless of whether
        // the RPC call has completed yet.
        assert_eq!(
            state.semaphore.available_permits(),
            MAX_IN_FLIGHT - 1,
            "one permit must be held by the spawned task"
        );
    }

    // ── run_poll_task: cancellation ───────────────────────────────────

    /// Cancelling while the task is blocked waiting for entries (idle queue) must
    /// cause it to exit cleanly without hanging.
    #[tokio::test]
    async fn run_poll_task_cancels_while_waiting_for_entries() {
        let in_flight = InFlightQueue::new();
        let (result_tx, _result_rx) = mpsc::channel(8);
        let (storage_tx, _storage_rx) = mpsc::channel(8);
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        ));
        let token = tokio_util::sync::CancellationToken::new();

        let handle = tokio::spawn(run_poll_task(
            in_flight.clone(),
            result_tx,
            rpc,
            storage_tx,
            ProgramType::Escrow,
            50,
            token.clone(),
        ));

        // Cancel immediately — task is blocked on notified(), must wake and exit.
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("task must exit within 2s after cancellation")
            .expect("task must not panic");
    }

    /// Cancelling while the task is sleeping between notify and drain must cause
    /// it to exit without processing any entries.
    #[tokio::test]
    async fn run_poll_task_cancels_during_poll_interval_sleep() {
        let in_flight = InFlightQueue::new();
        let (result_tx, _result_rx) = mpsc::channel(8);
        let (storage_tx, _storage_rx) = mpsc::channel(8);
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        ));
        let token = tokio_util::sync::CancellationToken::new();

        let handle = tokio::spawn(run_poll_task(
            in_flight.clone(),
            result_tx,
            rpc,
            storage_tx,
            ProgramType::Escrow,
            60_000, // very long interval — task will be sleeping here when we cancel
            token.clone(),
        ));

        // Push an entry to unblock the first select (notified), then cancel
        // while the task is in the poll_interval sleep.
        in_flight.push(make_in_flight_tx(Signature::new_unique(), 1));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        token.cancel();

        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("task must exit within 2s after cancellation")
            .expect("task must not panic");
    }

    /// When the result_tx receiver is dropped (sender loop gone), the task must
    /// detect the closed channel and exit cleanly rather than looping forever.
    #[tokio::test]
    async fn run_poll_task_exits_when_result_channel_closed() {
        let mut server = mockito::Server::new_async().await;

        // Return a confirmed-with-error status so a NeedsRouting result is produced,
        // which forces a send on result_tx (the closed channel) → task must exit.
        let sig = Signature::new_unique();
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 5},
                        "value": [{
                            "slot": 5,
                            "confirmations": null,
                            "confirmationStatus": "finalized",
                            "err": {"InstructionError": [0, "GenericError"]},
                            "status": {"Err": {"InstructionError": [0, "GenericError"]}}
                        }]
                    }
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create();

        let in_flight = InFlightQueue::new();
        // Drop result_rx immediately to close the channel from the receiver side.
        let (result_tx, result_rx) = mpsc::channel::<Vec<PollTaskResult>>(8);
        drop(result_rx);
        let (storage_tx, _storage_rx) = mpsc::channel(8);
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        ));
        let token = tokio_util::sync::CancellationToken::new();

        in_flight.push(make_in_flight_tx(sig, 42));

        let handle = tokio::spawn(run_poll_task(
            in_flight.clone(),
            result_tx,
            rpc,
            storage_tx,
            ProgramType::Escrow,
            1, // minimal sleep
            token.clone(),
        ));

        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("task must exit within 3s when result channel is closed")
            .expect("task must not panic");
    }

    // ── rotation submit path ─────────────────────────────────────────

    /// The generation read is the only RPC on the rotation submit path, and
    /// nothing re-dispatches a rotation once its boundary row is done. A failed
    /// read must therefore park the builder, not drop it, or the next generation
    /// stays closed and every withdrawal in it is refused forever.
    #[tokio::test]
    async fn rotation_parks_itself_when_the_generation_read_fails() {
        let mut server = mockito::Server::new_async().await;
        let _down = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(500)
            .with_body("boom")
            .create();

        let mut state = make_sender_state_with_server(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());

        let mut builder =
            private_channel_escrow_program_client::instructions::RotateBitmapBuilder::new();
        let pk = Pubkey::new_unique();
        builder
            .payer(pk)
            .operator(pk)
            .instance(pk)
            .withdrawal_bitmap(pk)
            .operator_pda(pk);

        let result = state
            .handle_transaction_builder(TransactionBuilder::RotateBitmap(Box::new(builder)))
            .await;

        assert!(
            result.is_err(),
            "an unreadable bitmap must not produce a rotation"
        );
        assert!(
            state.pending_rotation.is_some(),
            "the rotation must be parked for the next tick, not dropped"
        );
    }

    /// A successful read binds the rotation to the generation the chain reports,
    /// which is what makes a replayed rotation fail instead of skipping a window.
    #[tokio::test]
    async fn rotation_binds_the_generation_it_reads() {
        let mut server = mockito::Server::new_async().await;
        let bitmap = mock_bitmap_account(&mut server, 3, &[]);

        let mut state = make_sender_state_with_server(&server.url());
        state.instance_pda = Some(Pubkey::new_unique());

        let mut builder =
            private_channel_escrow_program_client::instructions::RotateBitmapBuilder::new();
        let pk = Pubkey::new_unique();
        builder
            .payer(pk)
            .operator(pk)
            .instance(pk)
            .withdrawal_bitmap(pk)
            .operator_pda(pk);

        let instruction = state
            .handle_transaction_builder(TransactionBuilder::RotateBitmap(Box::new(builder)))
            .await
            .expect("a readable bitmap must produce a rotation");

        assert!(state.pending_rotation.is_none());
        assert_eq!(
            state.cached_generation,
            Some(3),
            "the authoritative read is what the cache is allowed to learn from"
        );
        // The only argument, little-endian after the one-byte discriminator.
        let data = &instruction.instructions[0].data;
        assert_eq!(
            u64::from_le_bytes(data[1..9].try_into().unwrap()),
            3,
            "the rotation must carry the generation the chain reported"
        );
        bitmap.assert();
    }

    /// A confirmed rotation is the one event that moves the window without a
    /// read, so the cache follows it. Leaving the cache behind here would put
    /// every nonce of the new generation through a confirming read, which is
    /// correct but pays for the boundary twice.
    #[tokio::test]
    async fn confirmed_rotation_advances_the_cached_generation() {
        let mut state = make_sender_state();
        state.cached_generation = Some(3);
        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::RotateBitmap,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: Some("trace-rotation".to_string()),
            deposit_claim_lease: None,
        };

        handle_success(&mut state, &ctx, Signature::new_unique(), &tx).await;

        assert_eq!(state.cached_generation, Some(4));
    }

    /// An unknown cache must stay unknown across a rotation, since inventing
    /// a value here is the one way it could ever run ahead of the chain, and a
    /// cache ahead of the chain is the only version of this that can refuse a
    /// withdrawal the chain would have accepted.
    #[tokio::test]
    async fn confirmed_rotation_leaves_an_unknown_generation_unknown() {
        let mut state = make_sender_state();
        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::RotateBitmap,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        };

        handle_success(&mut state, &ctx, Signature::new_unique(), &tx).await;

        assert_eq!(state.cached_generation, None);
    }

    // ── rotation retry budget ────────────────────────────────────────

    fn rotation_ctx() -> TransactionContext {
        TransactionContext {
            kind: TransactionKind::RotateBitmap,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        }
    }

    fn initialize_mint_ctx() -> TransactionContext {
        TransactionContext {
            kind: TransactionKind::InitializeMint,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
            deposit_claim_lease: None,
        }
    }

    fn mock_blockhash_regex(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "blockhash": "11111111111111111111111111111111",
                            "lastValidBlockHeight": 1000
                        }
                    }
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create()
    }

    /// Answers every `sendTransaction` and counts it, so a test can assert how many went out.
    fn mock_send_counted(
        server: &mut mockito::ServerGuard,
        sends: Arc<AtomicUsize>,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_body_from_request(move |_| {
                sends.fetch_add(1, Ordering::SeqCst);
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": Signature::default().to_string()
                })
                .to_string()
                .into_bytes()
            })
            .expect_at_least(1)
            .create()
    }

    /// A `getSignatureStatuses` value: unconfirmed unless `confirmed`, carrying `err` when the program refused it.
    fn statuses_body(confirmed: bool, err: Option<serde_json::Value>) -> Vec<u8> {
        let value = if confirmed {
            serde_json::json!([{
                "slot": 1,
                "confirmations": null,
                "confirmationStatus": "finalized",
                "err": err,
                "status": match &err {
                    Some(e) => serde_json::json!({"Err": e}),
                    None => serde_json::json!({"Ok": null}),
                }
            }])
        } else {
            serde_json::json!([null])
        };
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"context": {"slot": 1}, "value": value}
        })
        .to_string()
        .into_bytes()
    }

    /// Withholds confirmation until more than `after` sends have gone out, then confirms cleanly.
    fn mock_statuses_confirmed_after(
        server: &mut mockito::ServerGuard,
        sends: Arc<AtomicUsize>,
        after: usize,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body_from_request(move |_| {
                statuses_body(sends.load(Ordering::SeqCst) > after, None)
            })
            .expect_at_least(1)
            .create()
    }

    /// An InitializeMint carries a rotation's empty ids, and capping it on that resemblance leaves its deposit with no terminal status at all.
    #[tokio::test]
    async fn initialize_mint_resends_past_the_rotation_retry_limit() {
        let mut server = mockito::Server::new_async().await;
        let _blockhash = mock_blockhash_regex(&mut server);
        let sends = Arc::new(AtomicUsize::new(0));
        let _send = mock_send_counted(&mut server, sends.clone());
        // Confirms only on the send after the cap, which a bounded run never reaches.
        let _statuses = mock_statuses_confirmed_after(&mut server, sends.clone(), 3);

        let mut state = make_sender_state_with_server(&server.url());
        state.retry_max_attempts = 3;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let ran = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            send_and_confirm(
                &mut state,
                dummy_instruction(),
                None,
                &initialize_mint_ctx(),
                RetryPolicy::Idempotent,
                &ExtraErrorCheckPolicy::None,
                &storage_tx,
            ),
        )
        .await;

        assert!(ran.is_ok(), "the mint must reach its confirmation");
        assert_eq!(
            sends.load(Ordering::SeqCst),
            4,
            "a mint must keep re-sending past the bound that belongs to rotations"
        );
    }

    /// A rotation that never confirms re-enters the send path, so without a bound it recurses until the sender task dies.
    #[tokio::test]
    async fn rotation_send_stops_at_the_retry_limit() {
        let mut server = mockito::Server::new_async().await;
        let _blockhash = mock_blockhash_regex(&mut server);
        let sends = Arc::new(AtomicUsize::new(0));
        let _send = mock_send_counted(&mut server, sends.clone());
        // Never confirms, which is what drives the Retry arm every cycle.
        let _statuses = mock_statuses_confirmed_after(&mut server, sends.clone(), usize::MAX);

        let mut state = make_sender_state_with_server(&server.url());
        state.retry_max_attempts = 3;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let ran = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            send_and_confirm(
                &mut state,
                dummy_instruction(),
                None,
                &rotation_ctx(),
                RetryPolicy::Idempotent,
                &ExtraErrorCheckPolicy::None,
                &storage_tx,
            ),
        )
        .await;

        assert!(ran.is_ok(), "a rotation must still run out of retries");
        assert_eq!(
            sends.load(Ordering::SeqCst),
            3,
            "a rotation must stop at the retry limit"
        );
    }

    /// A rotation in hand, as the submit path records it before broadcasting.
    fn rotation_builder(
    ) -> Box<private_channel_escrow_program_client::instructions::RotateBitmapBuilder> {
        let mut builder =
            private_channel_escrow_program_client::instructions::RotateBitmapBuilder::new();
        let pk = Pubkey::new_unique();
        builder
            .payer(pk)
            .operator(pk)
            .instance(pk)
            .withdrawal_bitmap(pk)
            .operator_pda(pk)
            .expected_generation(0);
        Box::new(builder)
    }

    /// Nothing re-dispatches a rotation once its boundary row is done, so a lost one shuts the next generation.
    #[tokio::test]
    async fn a_rotation_that_runs_out_of_retries_is_re_armed() {
        let mut server = mockito::Server::new_async().await;
        let _blockhash = mock_blockhash_regex(&mut server);
        let sends = Arc::new(AtomicUsize::new(0));
        let _send = mock_send_counted(&mut server, sends.clone());
        let _statuses = mock_statuses_confirmed_after(&mut server, sends.clone(), usize::MAX);

        let mut state = make_sender_state_with_server(&server.url());
        state.retry_max_attempts = 3;
        state.rotation_in_flight = Some(rotation_builder());
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        send_and_confirm(
            &mut state,
            dummy_instruction(),
            None,
            &rotation_ctx(),
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        assert!(
            state.pending_rotation.is_some(),
            "a failed rotation must go back on the tick, not vanish"
        );
        assert_eq!(
            state.rotation_retry_attempts, 0,
            "the re-armed rotation needs its send budget back"
        );
    }

    /// Abandoning a rotation has to hand the next one a full budget. The driver
    /// starts a fresh rotation once nothing is in flight, so a re-arm count left
    /// at the limit would abandon that one on its first failure, and every one
    /// after it, with no retries at all.
    #[tokio::test]
    async fn abandoning_a_rotation_returns_the_re_arm_budget() {
        let mut state = make_sender_state();
        // Rotation is a withdraw-role concern, and the escrow label is what the
        // give-up metric test measures, so stay off its series.
        state.program_type = ProgramType::Withdraw;
        state.rotation_in_flight = Some(rotation_builder());
        state.rotation_rearm_attempts = MAX_ROTATION_REARMS;

        rearm_failed_rotation(&mut state, "send failed");

        assert!(
            state.pending_rotation.is_none(),
            "the rotation at its limit must be abandoned, not re-armed"
        );
        assert_eq!(
            state.rotation_rearm_attempts, 0,
            "the next rotation must start on a full re-arm budget"
        );
    }

    /// Re-arming cannot be unconditional, or a rotation the chain never accepts is broadcast for the life of the process.
    #[tokio::test]
    async fn a_rotation_that_keeps_failing_stops_being_re_armed() {
        let mut server = mockito::Server::new_async().await;
        let _blockhash = mock_blockhash_regex(&mut server);
        let sends = Arc::new(AtomicUsize::new(0));
        let _send = mock_send_counted(&mut server, sends.clone());
        let _statuses = mock_statuses_confirmed_after(&mut server, sends.clone(), usize::MAX);

        let mut state = make_sender_state_with_server(&server.url());
        state.retry_max_attempts = 1;
        state.rotation_in_flight = Some(rotation_builder());
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let lost_before = metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&["escrow", "rotation_lost"])
            .get();

        for _ in 0..=MAX_ROTATION_REARMS {
            state.pending_rotation = None;
            send_and_confirm(
                &mut state,
                dummy_instruction(),
                None,
                &rotation_ctx(),
                RetryPolicy::Idempotent,
                &ExtraErrorCheckPolicy::None,
                &storage_tx,
            )
            .await;
        }

        assert!(
            state.pending_rotation.is_none(),
            "a rotation that cannot land must stop being re-armed"
        );
        assert_eq!(
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&["escrow", "rotation_lost"])
                .get(),
            lost_before + 1.0,
            "giving up on a rotation must be visible"
        );
    }

    /// Only a rotation moves the window; an InitializeMint mistaken for one leaves the cache a generation ahead of the chain.
    #[tokio::test]
    async fn confirmed_initialize_mint_leaves_the_cached_generation_alone() {
        let mut state = make_sender_state();
        state.cached_generation = Some(3);
        let (tx, _rx) = mpsc::channel(10);

        handle_success(
            &mut state,
            &initialize_mint_ctx(),
            Signature::new_unique(),
            &tx,
        )
        .await;

        assert_eq!(state.cached_generation, Some(3));
    }

    /// A rotation refused as a duplicate says nothing about the next one, so charging it wedges every rotation after it.
    #[tokio::test]
    async fn rotation_refused_as_duplicate_leaves_the_next_rotation_sendable() {
        let mut server = mockito::Server::new_async().await;
        let _blockhash = mock_blockhash_regex(&mut server);
        let sends = Arc::new(AtomicUsize::new(0));
        let _send = mock_send_counted(&mut server, sends.clone());
        // The first three rotations are refused with UnexpectedGeneration (custom code 14); the fourth confirms.
        let refusals = sends.clone();
        let _statuses = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body_from_request(move |_| {
                let err = (refusals.load(Ordering::SeqCst) <= 3)
                    .then(|| serde_json::json!({"InstructionError": [0, {"Custom": 14}]}));
                statuses_body(true, err)
            })
            .expect_at_least(1)
            .create();

        let mut state = make_sender_state_with_server(&server.url());
        state.retry_max_attempts = 3;
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        for _ in 0..4 {
            send_and_confirm(
                &mut state,
                dummy_instruction(),
                None,
                &rotation_ctx(),
                RetryPolicy::Idempotent,
                &ExtraErrorCheckPolicy::None,
                &storage_tx,
            )
            .await;
        }

        assert_eq!(
            sends.load(Ordering::SeqCst),
            4,
            "a rotation must still be broadcast after earlier ones were refused as duplicates"
        );
    }
}
