use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::TransactionError;
use crate::error::{OperatorError, ProgramError};
use crate::metrics;
use crate::operator::utils::instruction_util::TransactionBuilder;
use crate::operator::utils::transaction_util::parse_program_error;
use crate::operator::utils::transaction_util::{
    build_and_sign, check_transaction_status, send_signed, ConfirmationResult,
    MAX_POLL_ATTEMPTS_CONFIRMATION,
};
use crate::operator::{
    sign_and_send_transaction, ExtraErrorCheckPolicy, RetryPolicy, RpcClientWithRetry,
};
use crate::storage::common::models::TransactionStatus;
use chrono::Utc;
use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
use private_channel_metrics::MetricLabel;
use solana_keychain::SolanaSigner;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::{error, info, info_span, warn, Instrument};

use super::mint::{
    cleanup_mint_builder, find_existing_mint_signature, try_jit_mint_initialization, JitOutcome,
};
use super::proof::{cleanup_failed_transaction, rebuild_with_regenerated_proof};
use super::types::{
    InFlightQueue, InFlightTx, InstructionWithSigners, PendingRemint, PendingSig, PollTaskResult,
    SenderState, TransactionContext, TransactionStatusUpdate, MAX_IN_FLIGHT,
};

use std::sync::Arc;

use std::time::Duration;

/// Safety delay before checking finality and reminting.
/// Solana finalized ≈ 32 slots × 400ms = ~12.8s. We use 2.5× safety factor.
pub const FINALITY_SAFETY_DELAY: Duration = Duration::from_secs(32);

const MAX_SIGS_PER_CALL: usize = 256;

impl SenderState {
    /// Handle incoming transaction builder (either ReleaseFunds or Mint)
    /// For ReleaseFunds: Generate SMT proof and complete builder
    /// For Mint: Just build instruction (no proof needed)
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

                // Initialize SMT state lazily if needed
                if self.smt_state.is_none() {
                    self.initialize_smt_state().await?;
                }

                self.smt_state
                    .as_mut()
                    .ok_or(ProgramError::SmtNotInitialized)?
                    .handle_release_funds_transaction(
                        builder_with_nonce,
                        fee_payer,
                        signers,
                        compute_unit_price,
                        compute_budget,
                    )
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
            TransactionBuilder::ResetSmtRoot(mut builder) => {
                // Bind the reset to our local tree index so the on-chain program
                // rejects a replay. Initialize SMT state first in case a reset is
                // the first thing we process after a restart.
                if self.smt_state.is_none() {
                    self.initialize_smt_state().await?;
                }
                let smt = self
                    .smt_state
                    .as_ref()
                    .ok_or(ProgramError::SmtNotInitialized)?;
                let in_flight_count = smt.nonce_to_builder.len();
                let expected_current_tree_index = smt.smt_state.tree_index();

                if in_flight_count > 0 {
                    info!(
                        "Rotation transaction received but {} in-flight txs exist - queuing",
                        in_flight_count
                    );

                    self.pending_rotation = Some(builder);

                    return Err(ProgramError::RotationPending { in_flight_count }.into());
                }

                // No in-flight transactions - process immediately
                builder.expected_current_tree_index(expected_current_tree_index);
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
        if let TransactionBuilder::Mint(builder_with_txn_id) = &tx_builder {
            match find_existing_mint_signature(&state.rpc_client, builder_with_txn_id).await {
                Ok(Some(existing_signature)) => {
                    handle_success(state, &ctx, existing_signature, storage_tx).await;
                    return;
                }
                Ok(None) => {}
                // Deliberately fail-closed: an unverifiable lookup halts to
                // manual review rather than risk a blind double-mint.
                Err(e) => {
                    error!(
                        "Mint idempotency lookup failed for transaction_id {}: {}",
                        builder_with_txn_id.txn_id, e
                    );
                    send_fatal_error(storage_tx, &ctx, &e).await;
                    return;
                }
            }
        }

        match state.handle_transaction_builder(tx_builder.clone()).await {
            Ok(instruction) => {
                info!("Transaction instruction ready for submission");
                // Mint and InitializeMint use fire-and-forget: send immediately,
                // defer confirmation to the batch timer poll in `poll_in_flight`.
                // ReleaseFunds and ResetSmtRoot use the blocking path because SMT
                // proof ordering requires at-most-one in-flight withdrawal at a time.
                match &tx_builder {
                    TransactionBuilder::Mint(_) | TransactionBuilder::InitializeMint(_) => {
                        spawn_fire_and_store(
                            state,
                            instruction,
                            compute_unit_price,
                            ctx.clone(),
                            retry_policy,
                            extra_error_checks_policy,
                            storage_tx.clone(),
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
                route_builder_error(state, &ctx, tx_builder, storage_tx, e).await;
            }
        }
    }
    .instrument(span)
    .await;
}

/// Route a `handle_transaction_builder` error to its non-success path; separate from `handle_transaction_submission` so it is testable without real signers.
pub(super) async fn route_builder_error(
    state: &mut SenderState,
    ctx: &TransactionContext,
    tx_builder: TransactionBuilder,
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
        OperatorError::Program(ProgramError::TreeIndexMismatch {
            nonce,
            expected_tree_index,
            current_tree_index,
        }) => {
            if let TransactionBuilder::ReleaseFunds(builder_with_nonce) = tx_builder {
                info!(
                    "Tree index mismatch: nonce {} expects {} but current is {} - queuing for retry",
                    nonce, expected_tree_index, current_tree_index
                );
                state.rotation_retry_queue.push((
                    TransactionContext {
                        transaction_id: Some(builder_with_nonce.transaction_id),
                        withdrawal_nonce: Some(builder_with_nonce.nonce),
                        trace_id: Some(builder_with_nonce.trace_id),
                    },
                    builder_with_nonce.builder,
                ));
            } else {
                error!("TreeIndexMismatch for non-ReleaseFunds transaction");
            }
        }
        e @ OperatorError::Program(ProgramError::SmtRootMismatch { .. })
        | e @ OperatorError::Program(ProgramError::SmtNotInitialized)
        | e @ OperatorError::Account(_)
        | e @ OperatorError::Storage(_) => {
            // SMT-init-class failure: a root mismatch, an uninitialized tree, an
            // RPC/account error fetching the instance, or a DB error reading the
            // completed nonces during lazy init. The local SMT stays
            // uninitialized, so this row never released: leave it Processing for
            // the recovery worker and never mark it Failed.
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[state.program_type.as_label(), "smt_init_error"])
                .inc();
            error!(
                transaction_id = ctx.transaction_id,
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                "SMT init failed; leaving row Processing for recovery: {}",
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
    // Check retry limit - only for idempotent operations that can be retried at sender level
    if let Some(nonce) = ctx.withdrawal_nonce {
        match retry_policy {
            RetryPolicy::Idempotent => {
                let attempts = state.retry_counts.get(&nonce).copied().unwrap_or(0);
                if attempts >= state.retry_max_attempts {
                    metrics::OPERATOR_TRANSACTION_ERRORS
                        .with_label_values(&[state.program_type.as_label(), "max_retries_exceeded"])
                        .inc();
                    error!(
                        "Max retries ({}) exceeded for withdrawal_nonce {}",
                        state.retry_max_attempts, nonce
                    );
                    handle_permanent_failure(state, ctx, storage_tx, "Max retries exceeded").await;
                    return;
                }
                state.retry_counts.insert(nonce, attempts + 1);
                info!(
                    "Transaction attempt {}/{} for withdrawal_nonce {}",
                    attempts + 1,
                    state.retry_max_attempts,
                    nonce
                );
            }
            RetryPolicy::None => {
                info!("Sending non-idempotent transaction - single sender-level attempt");
            }
        }
    }

    let pt = state.program_type.as_label();
    let send_start = std::time::Instant::now();

    // Build and sign before broadcasting so the signature can be persisted write-ahead.
    let (transaction, signature, last_valid_block_height) =
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

    // Persist the release signature write-ahead (DB only), fail-closed. A withdrawal
    // nonce is consumed on broadcast, so a release that lands must already have a
    // durable signature record for crash recovery to reconcile against. On persist
    // failure we abort before broadcasting (the nonce stays unconsumed) and leave the
    // row Processing for the recovery worker.
    if let (Some(_nonce), Some(txid)) = (ctx.withdrawal_nonce, ctx.transaction_id) {
        if let Err(e) = state
            .storage
            .insert_release_signature(txid, signature.to_string(), last_valid_block_height as i64)
            .await
        {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt, "pre_send_persist_error"])
                .inc();
            let abort = TransactionError::PreSendPersistFailed {
                reason: e.to_string(),
            };
            error!(
                transaction_id = txid,
                signature = %signature,
                "Aborting release before broadcast, leaving row Processing for recovery: {}",
                abort
            );
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
                PrivateChannelEscrowProgramError::InvalidSmtProof,
            ))) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "invalid_smt_proof"])
                    .inc();
                warn!("InvalidSmtProof - removing nonce and rebuilding with fresh proof");
                if let (Some(nonce), Some(ref mut smt_state)) =
                    (ctx.withdrawal_nonce, state.smt_state.as_mut())
                {
                    smt_state.smt_state.remove_nonce(nonce);
                }
                if let Some(new_instruction) =
                    rebuild_with_regenerated_proof(state, ctx.withdrawal_nonce, instruction).await
                {
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
                } else {
                    handle_permanent_failure(state, ctx, storage_tx, "Failed to rebuild proof")
                        .await;
                }
            }
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::InvalidTransactionNonceForCurrentTreeIndex,
            ))) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "invalid_nonce_for_tree_index"])
                    .inc();
                error!("InvalidTransactionNonce - fatal error");
                handle_permanent_failure(state, ctx, storage_tx, "Invalid nonce for tree index")
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
                    JitOutcome::PermanentFailure(reason) => {
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
                PrivateChannelEscrowProgramError::UnexpectedTreeIndex,
            ))) => {
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[pt, "reset_tree_already_advanced"])
                    .inc();
                // Rejected because a reset already advanced the tree on-chain. Sync
                // local SMT to the authoritative index. On fetch failure, leave it
                // unchanged rather than guess; a restart re-syncs from chain.
                // smt_state is always Some here: the reset submit path initializes
                // it before sending, and nothing clears it back to None.
                match state.fetch_onchain_tree_index().await {
                    Ok(idx) => {
                        if let Some(ref mut smt_state) = state.smt_state {
                            smt_state.smt_state.reset(idx);
                            warn!("ResetSmtRoot rejected - synced local SMT to on-chain tree_index {idx}");
                        }
                    }
                    Err(e) => error!(
                        "ResetSmtRoot rejected but tree index re-fetch failed: {e} - local SMT left unchanged"
                    ),
                }
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

    // Handle ReleaseFunds (withdrawal nonce-based) transactions
    if let (Some(nonce), Some(ref mut smt_state)) = (ctx.withdrawal_nonce, state.smt_state.as_mut())
    {
        smt_state.nonce_to_builder.remove(&nonce);
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
    // Handle ResetSmtRoot (no transaction_id) - update local SMT tree index
    else if let Some(ref mut smt_state) = state.smt_state {
        let new_tree_index = smt_state.smt_state.tree_index() + 1;
        smt_state.smt_state.reset(new_tree_index);
        info!(
            "Tree rotation complete! Updated local SMT to tree_index {}",
            new_tree_index
        );
    }
}

/// Handle permanent transaction failure with deferred remint for withdrawals.
///
/// For withdrawal transactions: removes remint info from cache, runs cleanup
/// (which removes the nonce from SMT and builder caches), then queues a deferred
/// remint that will execute after the Solana finality window passes. This prevents
/// double-spend if the original withdrawal lands on-chain after our polling window.
///
/// For non-withdrawal transactions: delegates to send_fatal_error.
pub(super) async fn handle_permanent_failure(
    state: &mut SenderState,
    ctx: &TransactionContext,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    error_msg: &str,
) {
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
        // Not a withdrawal — use normal fatal error path
        send_fatal_error(storage_tx, ctx, error_msg).await;
        return;
    };

    // Zero signatures means sign_and_send itself failed — we have nothing to verify.
    // The RPC may have broadcast the tx before erroring, so blind remint is unsafe.
    if signatures.is_empty() {
        error!(
            "No signatures to verify for nonce {:?} — cannot safely remint, sending to ManualReview",
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
                        "{} | no signatures to verify — remint unsafe",
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
            .set_pending_remint(transaction_id, sig_strings, lvbhs, deadline)
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
    // in instruction_util.rs). `InitializeMint` and `ResetSmtRoot` return `None`
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
        Ok((signature, _last_valid_block_height)) => {
            metrics::OPERATOR_RPC_SEND_DURATION
                .with_label_values(&[pt, "in_flight"])
                .observe(send_start.elapsed().as_secs_f64());
            info!("Transaction sent: {}", signature);
            // push() also notifies the poll task if it is waiting on an empty queue.
            state.in_flight.push(InFlightTx {
                signature,
                ctx,
                instruction,
                compute_unit_price,
                retry_policy,
                extra_error_checks_policy,
                poll_attempts: 0,
                resend_count,
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
pub(super) fn spawn_fire_and_store(
    state: &SenderState,
    instruction: InstructionWithSigners,
    compute_unit_price: Option<u64>,
    ctx: TransactionContext,
    retry_policy: RetryPolicy,
    extra_error_checks_policy: ExtraErrorCheckPolicy,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
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

    tokio::spawn(async move {
        let send_start = std::time::Instant::now();
        match sign_and_send_transaction(rpc_client, instruction.clone(), retry_policy).await {
            Ok((signature, _last_valid_block_height)) => {
                metrics::OPERATOR_RPC_SEND_DURATION
                    .with_label_values(&[program_type.as_label(), "in_flight"])
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
                    permit,
                });
            }
            Err(e) => {
                drop(permit);
                metrics::OPERATOR_RPC_SEND_DURATION
                    .with_label_values(&[program_type.as_label(), "error"])
                    .observe(send_start.elapsed().as_secs_f64());
                metrics::OPERATOR_TRANSACTION_ERRORS
                    .with_label_values(&[program_type.as_label(), "rpc_send_error"])
                    .inc();
                error!("Failed to send transaction (fire-and-forget): {}", e);
                send_fatal_error(&storage_tx, &ctx, &e.to_string()).await;
            }
        }
    });

    true
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
                            warn!(
                                "Confirmation timeout for non-idempotent tx {} after {} polls — permanent failure",
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
                        RetryPolicy::Idempotent => {
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
                                warn!(
                                    "Confirmation timeout for idempotent tx {} — resend limit ({}) reached, permanent failure",
                                    tx.signature, state.retry_max_attempts,
                                );
                                handle_permanent_failure(
                                    state,
                                    &tx.ctx,
                                    storage_tx,
                                    "Confirmation timeout: resend limit exceeded",
                                )
                                .await;
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
    use crate::operator::sender::types::SenderSMTState;
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
    use crate::operator::utils::smt_util::SmtState;
    use crate::operator::MintCache;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::common::storage::Storage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
    use private_channel_escrow_program_client::instructions::ReleaseFundsBuilder;
    use private_channel_escrow_program_client::Instance;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_request::RpcRequest;
    use solana_keychain::Signer;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use std::collections::HashMap;
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
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        ));
        SenderState {
            rpc_client: rpc_client.clone(),
            source_rpc_client: rpc_client.clone(),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    fn make_sender_state_with_server(url: &str) -> SenderState {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        ));
        SenderState {
            rpc_client: rpc_client.clone(),
            source_rpc_client: rpc_client.clone(),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
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
        let mut state = make_sender_state();
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

    // ── SMT-init errors must not mark a row Failed ────────────────

    fn release_funds_builder(txn_id: i64, nonce: u64) -> TransactionBuilder {
        TransactionBuilder::ReleaseFunds(Box::new(
            crate::operator::utils::instruction_util::ReleaseFundsBuilderWithNonce {
                builder: ReleaseFundsBuilder::new(),
                nonce,
                transaction_id: txn_id,
                trace_id: format!("trace-{txn_id}"),
                remint_info: None,
            },
        ))
    }

    /// Asserts no status update was sent (the row is left Processing, never Failed).
    fn assert_no_status_update(rx: &mut mpsc::Receiver<TransactionStatusUpdate>) {
        assert!(
            rx.try_recv().is_err(),
            "SMT-init error must not produce any status update (row stays Processing)"
        );
    }

    /// An SMT-init-class error from lazy init (SmtRootMismatch, SmtNotInitialized,
    /// an OperatorError::Account from the instance fetch, or an OperatorError::Storage
    /// from reading the completed nonces) must leave the triggering withdrawal
    /// Processing, never Failed.
    #[tokio::test]
    async fn smt_init_error_leaves_row_processing_not_failed() {
        let ctx = withdrawal_ctx(10, 7);

        // Case 1: SmtRootMismatch.
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        route_builder_error(
            &mut state,
            &ctx,
            release_funds_builder(10, 7),
            &storage_tx,
            ProgramError::SmtRootMismatch {
                local_root: [0u8; 32],
                onchain_root: [1u8; 32],
            }
            .into(),
        )
        .await;
        assert_no_status_update(&mut storage_rx);

        // Case 2: OperatorError::Account (e.g. RPC/account error during init).
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        route_builder_error(
            &mut state,
            &ctx,
            release_funds_builder(10, 7),
            &storage_tx,
            crate::error::AccountError::InstanceNotFound {
                instance: Pubkey::default(),
            }
            .into(),
        )
        .await;
        assert_no_status_update(&mut storage_rx);

        // Case 3: OperatorError::Storage (a transient DB read error during init) must also leave the row Processing.
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        route_builder_error(
            &mut state,
            &ctx,
            release_funds_builder(10, 7),
            &storage_tx,
            crate::error::StorageError::DatabaseError {
                message: "transient".to_string(),
            }
            .into(),
        )
        .await;
        assert_no_status_update(&mut storage_rx);
    }

    /// A genuine build error (not SMT-init-class) MUST still mark the row Failed, so the exemption doesn't swallow real failures.
    #[tokio::test]
    async fn non_smt_build_error_still_marks_failed() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        route_builder_error(
            &mut state,
            &withdrawal_ctx(10, 7),
            release_funds_builder(10, 7),
            &storage_tx,
            ProgramError::InvalidBuilder {
                reason: "bad".to_string(),
            }
            .into(),
        )
        .await;

        let update = storage_rx
            .try_recv()
            .expect("non-SMT build error must send a Failed status");
        assert_eq!(update.status, TransactionStatus::Failed);
    }

    // ── handle_success ──────────────────────────────────────────────

    #[tokio::test]
    async fn success_clears_remint_cache_and_nonce_state() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Set up SMT state with a cached builder at nonce 3
        let mut smt = SenderSMTState {
            smt_state: SmtState::new(0),
            nonce_to_builder: HashMap::new(),
        };
        let ctx = TransactionContext {
            transaction_id: Some(50),
            withdrawal_nonce: Some(3),
            trace_id: Some("trace-50".to_string()),
        };
        smt.nonce_to_builder
            .insert(3, (ctx.clone(), ReleaseFundsBuilder::new()));
        state.smt_state = Some(smt);
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
        let smt = state.smt_state.as_ref().unwrap();
        assert!(!smt.nonce_to_builder.contains_key(&3));
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
            stored[0].0,
            Signature::default().to_string(),
            "persisted signature must be the signed transaction's signature"
        );
        assert_eq!(stored[0].1, 100, "persisted lvbh must match the blockhash");
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
        let mut state = make_sender_state();
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

        let (stored_id, stored_sigs, stored_lvbhs, stored_deadline) = &calls[0];
        assert_eq!(*stored_id, 10, "wrong transaction_id persisted");

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

    /// A confirmed ResetSmtRoot transaction (no transaction_id, no nonce) must advance the
    /// tree index and send no status update to the storage channel.
    #[tokio::test]
    async fn handle_success_reset_smt_root_increments_tree_index() {
        let mut state = make_sender_state();
        // Set up SMT state
        state.smt_state = Some(super::super::types::SenderSMTState {
            smt_state: crate::operator::utils::smt_util::SmtState::new(0),
            nonce_to_builder: HashMap::new(),
        });

        let (tx, mut rx) = mpsc::channel(10);
        // No transaction_id, no withdrawal_nonce = ResetSmtRoot context
        let ctx = TransactionContext {
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
        };
        let sig = Signature::new_unique();

        handle_success(&mut state, &ctx, sig, &tx).await;

        // No status update sent for ResetSmtRoot
        drop(tx);
        assert!(rx.recv().await.is_none());

        // Tree index should be incremented
        assert_eq!(state.smt_state.as_ref().unwrap().smt_state.tree_index(), 1);
    }

    /// After a successful withdrawal, the per-nonce retry counter must be removed so that
    /// a future submission with the same nonce starts from a clean slate.
    #[tokio::test]
    async fn handle_success_withdrawal_cleans_up_nonce_state() {
        let mut state = make_sender_state();
        state.instance_pda = Some(Pubkey::new_unique());
        state.smt_state = Some(super::super::types::SenderSMTState {
            smt_state: crate::operator::utils::smt_util::SmtState::new(0),
            nonce_to_builder: HashMap::new(),
        });
        state.retry_counts.insert(5, 2);

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
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

    /// `InvalidTransactionNonceForCurrentTreeIndex` is a permanent on-chain rejection; the
    /// transaction must be marked Failed and the error message must mention "nonce".
    #[tokio::test]
    async fn confirmation_result_invalid_nonce_for_tree_index_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            transaction_id: Some(10),
            withdrawal_nonce: None,
            trace_id: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::InvalidTransactionNonceForCurrentTreeIndex,
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
        assert!(update
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("nonce"));
    }

    /// An unrecognised program error (None variant) is treated as a permanent failure;
    /// the transaction must be marked Failed with no retry attempt.
    #[tokio::test]
    async fn confirmation_result_other_program_error_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
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

    /// A reset rejected with UnexpectedTreeIndex means a reset already landed on-chain.
    /// The sender must re-fetch the authoritative tree index, sync local SMT to it, and
    /// write nothing to the storage channel (a reset has no DB row).
    #[tokio::test]
    async fn confirmation_result_unexpected_tree_index_resyncs_local_smt() {
        let local_index = 4u64;
        let onchain_index = 5u64;

        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: [0u8; 32],
            current_tree_index: onchain_index,
        };
        let mut instance_bytes = Vec::new();
        instance.serialize(&mut instance_bytes).unwrap();

        let account_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&instance_bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let mut state = make_sender_state();
        state.instance_pda = Some(Pubkey::new_unique());
        state.rpc_client = Arc::new(RpcClientWithRetry {
            rpc_client: Arc::new(RpcClient::new_mock_with_mocks(
                "http://127.0.0.1:8899".to_string(),
                mocks,
            )),
            retry_config: RetryConfig::default(),
        });
        state.smt_state = Some(SenderSMTState {
            smt_state: SmtState::new(local_index),
            nonce_to_builder: HashMap::new(),
        });

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::UnexpectedTreeIndex,
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

        assert_eq!(
            state.smt_state.as_ref().unwrap().smt_state.tree_index(),
            onchain_index
        );
        drop(tx);
        assert!(
            rx.recv().await.is_none(),
            "no status update expected for reset"
        );
    }

    /// If the on-chain re-fetch fails (here: an undeserializable instance account), the
    /// sender must leave local SMT unchanged (fail-closed) rather than guessing the index.
    #[tokio::test]
    async fn confirmation_result_unexpected_tree_index_fetch_failure_leaves_smt_unchanged() {
        let local_index = 4u64;

        // Too-short account data so parse_instance fails after a successful fetch.
        let account_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode([0u8; 4]), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let mut state = make_sender_state();
        state.instance_pda = Some(Pubkey::new_unique());
        state.rpc_client = Arc::new(RpcClientWithRetry {
            rpc_client: Arc::new(RpcClient::new_mock_with_mocks(
                "http://127.0.0.1:8899".to_string(),
                mocks,
            )),
            retry_config: RetryConfig::default(),
        });
        state.smt_state = Some(SenderSMTState {
            smt_state: SmtState::new(local_index),
            nonce_to_builder: HashMap::new(),
        });

        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            transaction_id: None,
            withdrawal_nonce: None,
            trace_id: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::UnexpectedTreeIndex,
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

        assert_eq!(
            state.smt_state.as_ref().unwrap().smt_state.tree_index(),
            local_index,
            "local SMT must be unchanged when re-fetch fails"
        );
        drop(tx);
        assert!(rx.recv().await.is_none());
    }

    /// A `Retry` result with `RetryPolicy::None` (non-idempotent operation) cannot be safely
    /// retried, so it must be converted to a fatal failure with an "unknown" error message.
    #[tokio::test]
    async fn confirmation_result_retry_with_none_policy_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
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

        // No transaction_id → send_fatal_error sends nothing
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
        state.smt_state = Some(super::super::types::SenderSMTState {
            smt_state: crate::operator::utils::smt_util::SmtState::new(0),
            nonce_to_builder: HashMap::new(),
        });
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
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

    /// `InvalidSmtProof` without a nonce means there is no builder to regenerate a proof with,
    /// so the transaction must immediately fail rather than attempt a retry.
    #[tokio::test]
    async fn confirmation_result_invalid_smt_proof_no_nonce_sends_fatal_error() {
        let mut state = make_sender_state();
        let (tx, mut rx) = mpsc::channel(10);
        let ctx = TransactionContext {
            transaction_id: Some(15),
            withdrawal_nonce: None, // No nonce → rebuild_with_regenerated_proof returns None
            trace_id: None,
        };

        handle_confirmation_result(
            &mut state,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::InvalidSmtProof,
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
        assert_eq!(update.transaction_id, 15);
        assert_eq!(update.status, TransactionStatus::Failed);
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
            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            SenderState {
                rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                    server.url(),
                    crate::operator::utils::rpc_util::RetryConfig {
                        max_attempts: 1,
                        base_delay: std::time::Duration::from_millis(1),
                        max_delay: std::time::Duration::from_millis(1),
                    },
                    CommitmentConfig::confirmed(),
                )),
                source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                    server.url(),
                    crate::operator::utils::rpc_util::RetryConfig {
                        max_attempts: 1,
                        base_delay: std::time::Duration::from_millis(1),
                        max_delay: std::time::Duration::from_millis(1),
                    },
                    CommitmentConfig::confirmed(),
                )),
                storage: storage.clone(),
                instance_pda: None,
                smt_state: None,
                retry_counts: HashMap::new(),
                mint_builders: HashMap::new(),
                mint_cache: crate::operator::MintCache::new(storage),
                retry_max_attempts: 3,
                confirmation_poll_interval_ms: 400,
                rotation_retry_queue: Vec::new(),
                pending_rotation: None,
                program_type: ProgramType::Escrow,
                remint_cache: HashMap::new(),
                pending_signatures: HashMap::new(),
                pending_remints: Vec::new(),
                in_flight: InFlightQueue::new(),
                semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            }
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = TransactionContext {
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
            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            SenderState {
                rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                    server.url(),
                    crate::operator::utils::rpc_util::RetryConfig {
                        max_attempts: 1,
                        base_delay: std::time::Duration::from_millis(1),
                        max_delay: std::time::Duration::from_millis(1),
                    },
                    CommitmentConfig::confirmed(),
                )),
                source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                    server.url(),
                    crate::operator::utils::rpc_util::RetryConfig {
                        max_attempts: 1,
                        base_delay: std::time::Duration::from_millis(1),
                        max_delay: std::time::Duration::from_millis(1),
                    },
                    CommitmentConfig::confirmed(),
                )),
                storage: storage.clone(),
                instance_pda: None,
                smt_state: None,
                retry_counts: HashMap::new(),
                mint_builders: HashMap::new(),
                mint_cache: crate::operator::MintCache::new(storage),
                retry_max_attempts: 3,
                confirmation_poll_interval_ms: 400,
                rotation_retry_queue: Vec::new(),
                pending_rotation: None,
                program_type: ProgramType::Escrow,
                remint_cache: HashMap::new(),
                pending_signatures: HashMap::new(),
                pending_remints: Vec::new(),
                in_flight: InFlightQueue::new(),
                semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            }
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        let ctx = TransactionContext {
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

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut state = SenderState {
            rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: crate::operator::MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig, 77));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
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

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut state = SenderState {
            rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: crate::operator::MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig, 88));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
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

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut state = SenderState {
            rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: crate::operator::MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig, 99));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
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

    /// When poll_attempts reaches MAX_POLL_ATTEMPTS_CONFIRMATION for a RetryPolicy::None tx,
    /// it must be declared a permanent failure and removed from in_flight.
    #[tokio::test]
    async fn poll_in_flight_timeout_none_policy_permanent_failure() {
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

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut state = SenderState {
            rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: crate::operator::MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: {
                let q = InFlightQueue::new();
                let mut tx = make_in_flight_tx(sig, 101);
                // Pre-fill poll_attempts to one below MAX so this poll tips it over.
                tx.poll_attempts = MAX_POLL_ATTEMPTS_CONFIRMATION - 1;
                q.push(tx);
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        };

        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        poll_in_flight(&mut state, &storage_tx).await;

        // Entry removed from in_flight.
        assert!(
            state.in_flight.is_empty(),
            "timed-out tx must leave in_flight"
        );

        // Failed status emitted.
        let update = storage_rx
            .try_recv()
            .expect("expected Failed status for non-idempotent timeout");
        assert_eq!(update.transaction_id, 101);
        assert_eq!(update.status, TransactionStatus::Failed);
        assert!(
            update
                .error_message
                .as_deref()
                .unwrap_or("")
                .contains("unknown"),
            "error should mention unknown status: {:?}",
            update.error_message,
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

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut state = SenderState {
            rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            source_rpc_client: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                crate::operator::utils::rpc_util::RetryConfig {
                    max_attempts: 1,
                    base_delay: std::time::Duration::from_millis(1),
                    max_delay: std::time::Duration::from_millis(1),
                },
                CommitmentConfig::confirmed(),
            )),
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: crate::operator::MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: {
                let q = InFlightQueue::new();
                q.push(make_in_flight_tx(sig1, 201));
                q.push(make_in_flight_tx(sig2, 202));
                q
            },
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
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
                transaction_id: Some(1),
                withdrawal_nonce: None,
                trace_id: None,
            },
            RetryPolicy::None,
            ExtraErrorCheckPolicy::None,
            storage_tx,
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
}
