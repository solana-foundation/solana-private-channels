use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::TransactionError;
use crate::error::{OperatorError, ProgramError};
use crate::metrics;
use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
use crate::operator::utils::instruction_util::{TransactionBuilder, TransactionKind};
use crate::operator::utils::transaction_util::parse_program_error;
use crate::operator::utils::transaction_util::{
    build_and_sign, check_transaction_status, send_signed, ConfirmationResult,
    MAX_POLL_ATTEMPTS_CONFIRMATION,
};
use crate::operator::{
    sign_and_send_transaction, ExtraErrorCheckPolicy, RetryPolicy, RpcClientWithRetry,
};
use crate::storage::common::models::TransactionStatus;
use crate::storage::common::storage::Storage;
use chrono::Utc;
use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
use private_channel_metrics::MetricLabel;
use solana_keychain::SolanaSigner;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::{error, info, info_span, warn, Instrument};

use super::mint::{cleanup_mint_builder, try_jit_mint_initialization, JitOutcome};
use super::proof::cleanup_failed_transaction;
use super::types::{
    InFlightQueue, InFlightTx, InstructionWithSigners, PendingRemint, PendingSig, PollTaskResult,
    SenderState, TransactionContext, TransactionStatusUpdate, MAX_IN_FLIGHT,
};
use super::{classify_release_signatures, SigFinality};

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
    };

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
                        // Only a real user-fund Mint persists write-ahead; InitializeMint
                        // mints no balance and is on-chain idempotent, so it is excluded.
                        let persist = matches!(tx_builder, TransactionBuilder::Mint(_));
                        spawn_fire_and_store(
                            state,
                            instruction,
                            compute_unit_price,
                            ctx.clone(),
                            retry_policy,
                            extra_error_checks_policy,
                            storage_tx.clone(),
                            persist,
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
            // the row never released: leave it Processing for the recovery worker
            // and never mark it Failed on what is only a read failure.
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "bitmap_unavailable"])
                .inc();
            error!(
                transaction_id = ctx.transaction_id,
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                "Could not read chain state to build the transaction; leaving row Processing for recovery: {}",
                e
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

/// Persist a broadcast signature write-ahead (DB only), fail-closed: `Err(())` means
/// "do not broadcast". On persist failure we count the error, log it with ids, and
/// return early so the caller aborts before sending; the row stays Processing for the
/// recovery worker to reconcile against the chain.
pub(super) async fn persist_signature_or_abort(
    storage: &Storage,
    pt: &str,
    transaction_id: i64,
    signature: &Signature,
    last_valid_block_height: u64,
) -> Result<(), ()> {
    if let Err(e) = storage
        .insert_release_signature(
            transaction_id,
            signature.to_string(),
            last_valid_block_height as i64,
            // Theirs' blockhash_slot bound arrives with the #187/#192 port.
            None,
        )
        .await
    {
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
        return Err(());
    }
    Ok(())
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
    let (transaction, signature, last_valid_block_height, _blockhash_slot) =
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
                handle_permanent_failure(state, ctx, storage_tx, &e.to_string()).await;
                return;
            }
        };

    // A withdrawal nonce is consumed on broadcast, so a release that lands must already
    // have a durable signature record for crash recovery to reconcile against.
    if let (Some(nonce), Some(txid)) = (ctx.withdrawal_nonce, ctx.transaction_id) {
        if persist_signature_or_abort(
            &state.storage,
            pt,
            txid,
            &signature,
            last_valid_block_height,
        )
        .await
        .is_err()
        {
            // Nothing was broadcast, so this nonce is not in flight and must not
            // keep holding the rotation barrier. The row stays Processing for the
            // recovery worker either way.
            state.in_flight_withdrawals.remove(&nonce);
            return;
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
                        info!("JIT verdict: Retry — re-issuing mint instruction");
                        send_and_confirm(
                            state,
                            new_instruction,
                            compute_unit_price,
                            ctx,
                            retry_policy,
                            extra_error_checks_policy,
                            storage_tx,
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
                    // Cantina #285's re-arm on a transient JIT verdict is a
                    // follow-up port; until then this keeps ours' routing.
                    JitOutcome::Transient(reason) => {
                        handle_permanent_failure(state, ctx, storage_tx, &reason).await;
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

    match classify_release_signatures(&state.rpc_client, &signatures).await {
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

/// Handle permanent transaction failure with deferred remint for withdrawals.
///
/// For withdrawal transactions: removes remint info from cache, runs cleanup,
/// then queues a deferred remint that will execute after the Solana finality
/// window passes. This prevents double-spend if the original withdrawal lands
/// on-chain after our polling window.
///
/// For non-withdrawal transactions: delegates to send_fatal_error.
/// Whether a send failure came back as an explicit RPC rejection from the node
/// (preflight simulation failure, blockhash errors, etc.). Such a response means the
/// transaction was never submitted to the cluster, so failing fast is safe. A transport
/// or IO error instead leaves the outcome ambiguous (the request may have reached the
/// cluster), so a persisted mint defers to recovery rather than risk stranding a landed one.
fn send_rejected_by_node(e: &TransactionError) -> bool {
    matches!(
        e,
        TransactionError::Rpc(err)
            if matches!(&err.kind, ErrorKind::RpcError(RpcError::RpcResponseError { .. }))
    )
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

    // Collect stashed signatures for finality check
    let signatures = ctx
        .withdrawal_nonce
        .and_then(|nonce| state.pending_signatures.remove(&nonce))
        .unwrap_or_default();

    cleanup_failed_transaction(state, ctx.withdrawal_nonce);

    let Some(info) = remint_info else {
        // Not a withdrawal, so use the normal fatal error path
        send_fatal_error(storage_tx, ctx, error_msg).await;
        return;
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
        if let Some(transaction_id) = ctx.transaction_id {
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
        }
        return;
    }

    let deadline = Utc::now() + chrono::Duration::from_std(FINALITY_SAFETY_DELAY).unwrap();

    // Atomically transition to PendingRemint, persisting the withdrawal signatures
    // needed for the finality check. This replaces the previous Failed write —
    // keeping status as Processing until the remint resolves avoids partial state
    // if the operator crashes during the finality window.
    if let Some(transaction_id) = ctx.transaction_id {
        let sig_strings: Vec<String> = signatures
            .iter()
            .map(|pending_sig| pending_sig.signature.to_string())
            .collect();
        let lvbhs: Vec<i64> = signatures
            .iter()
            .map(|pending_sig| pending_sig.last_valid_block_height as i64)
            .collect();

        if let Err(e) = state
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
            error!(
                "Failed to persist PendingRemint for transaction {} - sending to manual review: {}",
                transaction_id, e
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
                        "{} | failed to persist pending remint: {}",
                        error_msg, e
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

    // `transaction_id` is always `Some` at this point in practice — only
    // `ReleaseFunds` transactions populate `remint_cache`, and `ReleaseFunds`
    // always carries a DB transaction_id (see `TransactionBuilder::transaction_id`
    // in instruction_util.rs). `InitializeMint` and `RotateBitmap` return `None`
    // there and would have exited early above via `send_fatal_error`. This guard
    // exists to prevent silently enqueuing a `PendingRemint` with no DB record,
    // which would be lost on restart since recovery reads from the DB.
    if ctx.transaction_id.is_none() {
        error!(
            "Cannot defer remint for nonce {:?} — no transaction_id, entry would be unrecoverable on restart",
            ctx.withdrawal_nonce,
        );
        return;
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
    // True only for a real user-fund Mint
    persist: bool,
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
        persist,
        permit,
    ));

    true
}

/// Build, sign, persist the signature when `persist` is set, then broadcast and stash
/// the in-flight tx. A persist failure aborts before broadcast and leaves the row
/// Processing for recovery. Split from `spawn_fire_and_store` so tests can await it
/// directly without `tokio::spawn`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fire_and_store_task(
    rpc_client: Arc<RpcClientWithRetry>,
    storage: Arc<Storage>,
    in_flight: Arc<InFlightQueue>,
    program_type: ProgramType,
    instruction: InstructionWithSigners,
    compute_unit_price: Option<u64>,
    ctx: TransactionContext,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: ExtraErrorCheckPolicy,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    persist: bool,
    permit: OwnedSemaphorePermit,
) {
    let pt = program_type.as_label();
    let send_start = std::time::Instant::now();

    let (transaction, signature, last_valid_block_height, _blockhash_slot) =
        match build_and_sign(&rpc_client, instruction.clone()).await {
            Ok(signed) => signed,
            Err(e) => {
                drop(permit);
                metrics::OPERATOR_RPC_SEND_DURATION
                    .with_label_values(&[pt, "error"])
                    .observe(send_start.elapsed().as_secs_f64());
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "build_sign_error"])
                    .inc();
                error!("Failed to build/sign transaction (fire-and-forget): {}", e);
                send_fatal_error(&storage_tx, &ctx, &e.to_string()).await;
                return;
            }
        };

    let persisted = if persist {
        match ctx.transaction_id {
            Some(txid) => {
                if persist_signature_or_abort(
                    &storage,
                    pt,
                    txid,
                    &signature,
                    last_valid_block_height,
                )
                .await
                .is_err()
                {
                    drop(permit);
                    return;
                }
                true
            }
            // Persist required but no transaction_id to key on: abort before broadcasting an unrecoverable mint.
            None => {
                drop(permit);
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "pre_send_persist_error"])
                    .inc();
                error!("Persist required but transaction has no id; aborting before broadcast");
                return;
            }
        }
    } else {
        false
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
            // A node rejection (e.g. preflight) means the tx never reached the cluster, so
            // fail fast. An ambiguous transport error on a persisted mint may have landed,
            // so leave it Processing for recovery rather than strand a possibly-funded mint.
            if persisted && !send_rejected_by_node(&e) {
                leave_processing_for_recovery(
                    pt,
                    ctx.transaction_id,
                    &signature,
                    "ambiguous send error after write-ahead persist",
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
            Some(status) if status.satisfies_commitment(CommitmentConfig::confirmed()) => {
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
    let mut statuses: Vec<Option<_>> = Vec::with_capacity(signatures.len());

    for chunk in signatures.chunks(MAX_SIGS_PER_CALL) {
        match state.rpc_client.get_signature_statuses(chunk).await {
            Ok(resp) => statuses.extend(resp.value),
            Err(e) => {
                warn!(
                    "getSignatureStatuses failed ({} in-flight) — will retry next tick: {}",
                    batch.len(),
                    e
                );
                // Put everything back so the next drain_in_flight iteration retries.
                for tx in batch {
                    state.in_flight.push(tx);
                }
                return;
            }
        }
    }

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
        let mut statuses: Vec<Option<_>> = Vec::with_capacity(signatures.len());
        let mut rpc_ok = true;

        for chunk in signatures.chunks(MAX_SIGS_PER_CALL) {
            match rpc_client.get_signature_statuses(chunk).await {
                Ok(resp) => statuses.extend(resp.value),
                Err(e) => {
                    warn!(
                        "getSignatureStatuses failed ({} in-flight) — will retry next tick: {}",
                        batch.len(),
                        e
                    );
                    rpc_ok = false;
                    break;
                }
            }
        }

        if !rpc_ok {
            // Put everything back in one lock acquisition.
            in_flight.push_all(batch);
            continue;
        }

        let mut results: Vec<PollTaskResult> = Vec::with_capacity(batch.len());

        for (mut tx, status_opt) in batch.into_iter().zip(statuses) {
            match status_opt {
                Some(status) if status.satisfies_commitment(CommitmentConfig::confirmed()) => {
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
        mock_bitmap_account, mock_with_processing_row, row_status,
        sender_state as make_sender_state_with_server, sender_state_with_storage,
    };
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
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
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(10));
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Populate remint cache and some pending signatures
        state.remint_cache.insert(5, make_remint_info(10));
        let sig = Signature::new_unique();
        state.pending_signatures.insert(
            5,
            vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
        );

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
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
            }],
        );
        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
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
        };
        state.in_flight_withdrawals.insert(3);
        state.retry_counts.insert(3, 2);
        state.remint_cache.insert(3, make_remint_info(50));
        state.pending_signatures.insert(
            3,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
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

    /// The in-memory stash happens only after a successful broadcast, so a send that
    /// never reached the network leaves no signature to verify and routes to
    /// ManualReview, not a deferred remint. The write-ahead DB persist (for crash
    /// recovery) does not change this.
    #[tokio::test]
    async fn send_failure_routes_to_manual_review() {
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

        let mut state = make_sender_state_with_server(&server.url());
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
            state.pending_remints.is_empty(),
            "a never-broadcast send must not defer a remint"
        );
        let update = storage_rx
            .try_recv()
            .expect("send failure must surface a status update");
        assert_eq!(update.status, TransactionStatus::ManualReview);
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
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(10));
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        // Two signatures — simulating a withdrawal that was retried once before
        // failing permanently. Both must be persisted for a complete finality check.
        let sig1 = Signature::new_unique();
        let sig2 = Signature::new_unique();
        let sig1_lvbh: u64 = 100;
        let sig2_lvbh: u64 = 200;
        state.remint_cache.insert(5, make_remint_info(10));
        state.pending_signatures.insert(
            5,
            vec![
                PendingSig {
                    signature: sig1,
                    last_valid_block_height: sig1_lvbh,
                },
                PendingSig {
                    signature: sig2,
                    last_valid_block_height: sig2_lvbh,
                },
            ],
        );

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
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
            }],
        );

        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(10),
            withdrawal_nonce: Some(5),
            trace_id: Some("trace-10".to_string()),
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
        let mut state = sender_state_with_storage(&server.url(), mock_with_processing_row(70));
        state.instance_pda = Some(Pubkey::new_unique());
        state.remint_cache.insert(4, make_remint_info(70));
        if stash_signature {
            state.pending_signatures.insert(
                4,
                vec![PendingSig {
                    signature: Signature::new_unique(),
                    last_valid_block_height: 0,
                }],
            );
        }

        let (tx, rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(70),
            withdrawal_nonce: Some(4),
            trace_id: Some("trace-70".to_string()),
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

        let mut state = sender_state_with_storage(&server.url(), mock);
        state.instance_pda = Some(Pubkey::new_unique());
        state.in_flight_withdrawals.insert(nonce);
        state
            .remint_cache
            .insert(nonce, make_remint_info(REFUSED_ROW));
        // The release was broadcast before the program refused it, so in
        // production the signature stash is never empty on this path.
        //
        // Without it the remint path exits early on "no signatures to verify".
        state.pending_signatures.insert(
            nonce,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1,
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
                },
                PendingSig {
                    signature: rejected,
                    last_valid_block_height: 1,
                },
            ],
        );

        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(REFUSED_ROW),
            withdrawal_nonce: Some(nonce),
            trace_id: Some("trace-80".to_string()),
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
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(91));
        state.remint_cache.insert(1, make_remint_info(91));
        state.pending_signatures.insert(
            1,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1,
            }],
        );
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(91),
            withdrawal_nonce: Some(1),
            trace_id: Some("trace-91".to_string()),
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
        let mut state =
            sender_state_with_storage("http://localhost:8899", mock_with_processing_row(91));
        state.remint_cache.insert(1, make_remint_info(91));
        state.pending_signatures.insert(
            1,
            vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 1,
            }],
        );
        let (tx, _rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(91),
            withdrawal_nonce: Some(1),
            trace_id: Some("trace-91".to_string()),
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

    /// A confirmed signature in the batch must route to handle_success, emitting
    /// a Completed status and removing the entry from in_flight.
    #[tokio::test]
    async fn poll_in_flight_confirmed_tx_emits_completed() {
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
                            "confirmationStatus": "confirmed",
                            "confirmations": 1,
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

    /// A mixed batch (one confirmed, one pending) must resolve the confirmed entry while
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
                            // sig1 confirmed
                            {
                                "confirmationStatus": "confirmed",
                                "confirmations": 1,
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
    /// must issue multiple RPC calls — one per 256-sig chunk — and merge the results.
    ///
    /// Strategy: mock returns all-null statuses (not yet confirmed) so every entry stays
    /// in `remaining` after the call.  We seed 300 entries and assert the mock was hit
    /// at least twice (≥ 2 chunks: 256 + 44), and that all 300 entries are still in-flight.
    #[tokio::test]
    async fn poll_in_flight_chunks_large_batch() {
        // Build a response body with 256 null slots — enough for the largest chunk.
        // The zip in poll_in_flight stops at the shorter of (batch, statuses), so
        // returning 256 nulls for both the 256-sig chunk and the 44-sig chunk is fine:
        // extra slots are ignored, missing slots cause zip to stop early (entries stay).
        let null_statuses: Vec<serde_json::Value> = vec![serde_json::Value::Null; 256];
        let response_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "context": {"slot": 1},
                "value": null_statuses
            }
        })
        .to_string();

        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(response_body)
            .expect_at_least(2) // 256 sigs → chunk 1; 44 sigs → chunk 2
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
        }
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

        let state = make_sender_state_with_server(&server.url());
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
            true,
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

        let state = make_sender_state_with_server(&server.url());
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
            true,
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

    /// A send error (e.g. preflight rejection) means the broadcast never reached the
    /// network, so even with the signature already persisted the mint is terminal Failed,
    /// same as the withdrawal send path. (The broadcast-accepted-but-unconfirmed case is
    /// the one route_poll_results leaves Processing; see the poll timeout test.)
    #[tokio::test]
    async fn mint_send_error_after_persist_routes_to_failed() {
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

        let state = make_sender_state_with_server(&server.url());
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
            true,
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
        let update = storage_rx
            .try_recv()
            .expect("send error must emit a terminal status");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.status, TransactionStatus::Failed);
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

    /// A node RPC rejection (preflight, blockhash, etc.) is definitive - the tx was never
    /// submitted - so a persisted mint fails fast; a transport/IO error is ambiguous and a
    /// persisted mint instead defers to recovery. This classifier draws that line.
    #[test]
    fn send_rejected_by_node_distinguishes_rejection_from_transport_error() {
        use solana_rpc_client_api::client_error::{Error as ClientError, ErrorKind};
        use solana_rpc_client_api::request::{RpcError, RpcResponseErrorData};

        let node_rejection = TransactionError::Rpc(Box::new(ClientError::from(
            ErrorKind::RpcError(RpcError::RpcResponseError {
                code: -32002,
                message: "preflight failure".to_string(),
                data: RpcResponseErrorData::Empty,
            }),
        )));
        assert!(
            send_rejected_by_node(&node_rejection),
            "an RPC response error is a definitive node rejection"
        );

        let transport_error = TransactionError::Rpc(Box::new(ClientError::from(
            ErrorKind::Custom("connection reset".to_string()),
        )));
        assert!(
            !send_rejected_by_node(&transport_error),
            "a transport error is ambiguous, not a node rejection"
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
            false,
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
        };

        let result = spawn_fire_and_store(
            &state,
            dummy_instruction(),
            None,
            ctx,
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            false,
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
            },
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
            false,
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
        }
    }

    fn initialize_mint_ctx() -> TransactionContext {
        TransactionContext {
            kind: TransactionKind::InitializeMint,
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
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
