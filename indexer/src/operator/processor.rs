use crate::channel_utils::send_guaranteed;
use crate::error::{OperatorError, ProgramError};
use crate::metrics;
use crate::operator::instruction_util::{
    mint_idempotency_memo, MintToBuilder, TransactionBuilder, WithdrawalRemintInfo,
};
use crate::operator::sender::TransactionStatusUpdate;
use crate::operator::utils::mint_util::MintCache;
use crate::operator::{
    find_allowed_mint_pda, find_event_authority_pda, find_operator_pda,
    tree_constants::MAX_TREE_LEAVES, MintToBuilderWithTxnId, ReleaseFundsBuilderWithNonce,
    SignerUtil,
};
use crate::storage::common::models::{DbTransaction, TransactionStatus};
use crate::storage::Storage;
use crate::ProgramType;
use chrono::Utc;
use private_channel_escrow_program_client::instructions::{
    ReleaseFundsBuilder, ResetSmtRootBuilder,
};
use private_channel_metrics::MetricLabel;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, info_span, warn, Instrument};

pub struct ProcessorState {
    pub admin_pubkey: Pubkey,
    pub release_funds_state: Option<ReleaseFundsState>,
    pub mint_cache: MintCache,
}

pub struct ReleaseFundsState {
    pub instance_pda: Pubkey,
    pub operator_pubkey: Pubkey,
    pub operator_pda: Pubkey,
    pub event_authority_pda: Pubkey,
    pub allowed_mints: HashMap<String, Pubkey>,
    pub instance_atas: HashMap<String, Pubkey>,
}

impl ProcessorState {
    pub fn new_with_release_funds_state(
        instance_pda: Pubkey,
        storage: Arc<Storage>,
        rpc_client: Arc<crate::operator::RpcClientWithRetry>,
    ) -> Self {
        let operator_pubkey = SignerUtil::get_operator_pubkey();
        let operator_pda = find_operator_pda(&instance_pda, &operator_pubkey);

        let event_authority_pda = find_event_authority_pda();

        Self {
            admin_pubkey: SignerUtil::get_admin_pubkey(),
            release_funds_state: Some(ReleaseFundsState {
                instance_pda,
                operator_pubkey,
                operator_pda,
                event_authority_pda,
                allowed_mints: HashMap::new(),
                instance_atas: HashMap::new(),
            }),
            mint_cache: MintCache::with_rpc(storage, rpc_client),
        }
    }

    pub fn new_with_storage(
        storage: Arc<Storage>,
        mint_rpc_client: Arc<crate::operator::RpcClientWithRetry>,
    ) -> Self {
        Self {
            admin_pubkey: SignerUtil::get_admin_pubkey(),
            release_funds_state: None,
            mint_cache: MintCache::with_rpc(storage, mint_rpc_client),
        }
    }
}

impl ReleaseFundsState {
    pub fn get_allowed_mint_pda(&mut self, mint: &Pubkey) -> Pubkey {
        *self
            .allowed_mints
            .entry(mint.to_string())
            .or_insert_with(|| find_allowed_mint_pda(&self.instance_pda, mint))
    }

    pub fn get_instance_ata(&mut self, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
        *self
            .instance_atas
            .entry(mint.to_string())
            .or_insert_with(|| {
                get_associated_token_address_with_program_id(
                    &self.instance_pda,
                    mint,
                    token_program,
                )
            })
    }
}

/// Error classification for per-transaction handling.
///
/// `Quarantine` errors are deterministic — the row itself is bad and will keep
/// failing if retried.  The processor marks the row `ManualReview`, emits a
/// webhook (via the DbTransactionWriter path), and moves on so the pipeline
/// keeps flowing.
///
/// `Transient` errors are infrastructure issues that should heal on their
/// own — we bubble them up so the task exits and the supervisor restarts us.
/// This is deliberately conservative: on restart the row is re-locked and
/// re-attempted from `Pending` by the fetcher.
///
/// `Fatal` errors mean the processor itself is misconfigured (missing
/// builder, dead downstream channel) — letting the task exit fast surfaces
/// the problem at the supervisor instead of silently dropping work.
enum ErrorDisposition {
    Quarantine(&'static str),
    Transient,
    Fatal,
}

/// Classify an `OperatorError` surfaced inside the per-transaction body.
/// The reason string is used as a metric label
fn classify_processor_error(err: &OperatorError) -> ErrorDisposition {
    match err {
        OperatorError::InvalidPubkey { .. } => ErrorDisposition::Quarantine("invalid_pubkey"),
        OperatorError::Program(ProgramError::InvalidBuilder { .. }) => {
            ErrorDisposition::Quarantine("invalid_builder")
        }
        // Other Program(_) variants are from the sender-side proof/root checks and
        // cannot originate in the processor today — label them generically if they
        // ever surface here.
        OperatorError::Program(_) => ErrorDisposition::Quarantine("program_error"),
        // MissingBuilder means the processor was constructed without the state it
        // needs — configuration bug, not a row problem.  Exit to surface it.
        OperatorError::MissingBuilder => ErrorDisposition::Fatal,
        // A dead downstream channel means the sender or storage writer died; the
        // supervisor handles this by aborting the whole operator.
        OperatorError::ChannelSend(_)
        | OperatorError::ChannelClosed { .. }
        | OperatorError::ShutdownChannelSend => ErrorDisposition::Fatal,
        // DB + RPC + webhook errors are treated as infrastructure — retry on restart.
        OperatorError::Storage(_)
        | OperatorError::RpcError(_)
        | OperatorError::WebhookError(_)
        | OperatorError::Account(_)
        | OperatorError::Transaction(_) => ErrorDisposition::Transient,
    }
}

/// Emit a `ManualReview` status update for a single row via the shared storage
/// writer channel.  Reuses `TransactionStatusUpdate` so the existing
/// DbTransactionWriter path handles both the DB write and the alert webhook.
async fn quarantine_single(
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    transaction: &DbTransaction,
    error_message: String,
) {
    let update = TransactionStatusUpdate {
        transaction_id: transaction.id,
        trace_id: Some(transaction.trace_id.clone()),
        status: TransactionStatus::ManualReview,
        counterpart_signature: None,
        processed_at: Some(Utc::now()),
        error_message: Some(error_message),
        remint_signature: None,
        remint_attempted: false,
    };
    // send_guaranteed: losing a quarantine update is worse than blocking briefly —
    // the DB row would stay `Processing` and never alert.
    if let Err(e) = send_guaranteed(storage_tx, update, "quarantine status update").await {
        // The only way this can fail is a closed channel, which means the storage
        // writer is already gone and the supervisor is about to restart us anyway.
        error!(
            txn_id = transaction.id,
            trace_id = %transaction.trace_id,
            "Failed to send quarantine update (storage writer down): {}", e
        );
    }
}

/// Halt the withdrawal pipeline after a poison-pill is detected.
///
/// A quarantined withdrawal leaves a permanent nonce gap that the on-chain
/// program rejects for every subsequent nonce in the same tree. Rather
/// than bleed errors downstream, we stop cleanly:
///   1. Quarantine any rows the fetcher already handed us (drain the rx).
///   2. Flip every other `Pending`/`Processing` withdrawal in the DB to
///      `ManualReview` so the fetcher has nothing left to pull.
///
/// `poison_id` is the row the caller has already individually quarantined
/// via `storage_tx`; it is excluded from the DB sweep so we don't fire a
/// second `ManualReview` webhook for the same transaction if the async
/// status update has not yet committed.
///
/// Recovery is manual — see the
/// runbook `withdrawal_pipeline_halt_runbook.md`.
async fn halt_withdrawal_pipeline(
    storage: &Storage,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    fetcher_rx: &mut mpsc::Receiver<DbTransaction>,
    poison_id: Option<i64>,
) {
    // Drain anything already delivered by the fetcher.  These rows were
    // flipped to `Processing` by `get_and_lock_pending_transactions` but
    // have not yet been handed to the sender, so they would otherwise be
    // stranded in `Processing`.
    let mut drained = 0u64;
    while let Ok(buffered) = fetcher_rx.try_recv() {
        quarantine_single(
            storage_tx,
            &buffered,
            "withdrawal pipeline halted after poison-pill".to_string(),
        )
        .await;
        drained += 1;
    }

    // Sweep the rest of the pipeline: any row still `Pending` (never
    // fetched) or `Processing` (locked but unsent, e.g. a sibling was mid-
    // flight in another instance) is flipped to `ManualReview`.
    match storage.quarantine_all_active_withdrawals(poison_id).await {
        Ok(affected) => {
            warn!(
                drained_from_channel = drained,
                db_rows_quarantined = affected,
                "Halted withdrawal pipeline; all active rows moved to ManualReview"
            );
        }
        Err(e) => {
            // Even on DB failure we have already quarantined the poison row
            // plus anything buffered in the channel, so the offending leaf is
            // visible in the alert stream. Log and continue to shutdown —
            // the supervisor restart path will re-attempt on next boot via
            // the runbook.
            error!(
                drained_from_channel = drained,
                "quarantine_all_active_withdrawals failed: {}", e
            );
        }
    }
}

/// Processes and validates transactions before sending to blockchain
///
/// Receives transactions from fetcher, validates them, and forwards to sender.
/// Per-transaction errors are classified and handled locally so a single bad
/// row does not propagate out of the task.
#[allow(clippy::too_many_arguments)]
pub async fn run_processor(
    fetcher_rx: mpsc::Receiver<DbTransaction>,
    sender_tx: mpsc::Sender<TransactionBuilder>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    program_type: ProgramType,
    instance_pda: Option<Pubkey>,
    storage: Arc<Storage>,
    rpc_client: Arc<crate::operator::RpcClientWithRetry>,
    source_rpc_client: Option<Arc<crate::operator::RpcClientWithRetry>>,
) {
    info!("Starting processor");

    match program_type {
        ProgramType::Withdraw => {
            // A withdrawal operator without an instance_pda is misconfigured.
            let Some(instance_pda) = instance_pda else {
                error!(
                    "Withdraw operator missing escrow_instance_id, cannot build ReleaseFunds instructions; processor exiting"
                );
                return;
            };
            let mut processor_state = ProcessorState::new_with_release_funds_state(
                instance_pda,
                storage.clone(),
                rpc_client,
            );

            if let Err(e) = process_release_funds(
                &mut processor_state,
                fetcher_rx,
                sender_tx,
                storage_tx,
                storage,
                program_type,
            )
            .await
            {
                tracing::error!("Process release funds error: {}", e);
            }
        }
        ProgramType::Escrow => {
            // Use source_rpc_client for mint cache if available, otherwise fall back to rpc_client
            let mint_rpc_client = source_rpc_client.unwrap_or_else(|| rpc_client.clone());
            let mut processor_state = ProcessorState::new_with_storage(storage, mint_rpc_client);

            if let Err(e) = process_deposit_funds(
                &mut processor_state,
                fetcher_rx,
                sender_tx,
                storage_tx,
                program_type,
            )
            .await
            {
                tracing::error!("Deposit funds error: {}", e);
            }
        }
    }
}

/// Build the release_funds TransactionBuilder for a single withdrawal.
///
/// Kept out of the loop so error handling in the caller is a single
/// Result<TransactionBuilder, OperatorError> to match on.
async fn build_release_funds(
    processor_state: &mut ProcessorState,
    transaction: &DbTransaction,
) -> Result<TransactionBuilder, OperatorError> {
    // `withdrawal_nonce IS NOT NULL` is enforced by the insert-trigger for
    // withdrawal rows; a NULL here means the row was inserted by something
    // other than the normal path and cannot be processed safely.
    let Some(nonce_i64) = transaction.withdrawal_nonce else {
        return Err(OperatorError::Program(ProgramError::InvalidBuilder {
            reason: format!(
                "withdrawal row {} has NULL withdrawal_nonce",
                transaction.id
            ),
        }));
    };
    let nonce = nonce_i64 as u64;

    let release_funds_state = processor_state
        .release_funds_state
        .as_mut()
        .ok_or(OperatorError::MissingBuilder)?;

    let mut builder = ReleaseFundsBuilder::new();

    let mint = Pubkey::from_str(&transaction.mint).map_err(|e| OperatorError::InvalidPubkey {
        pubkey: transaction.mint.clone(),
        reason: e.to_string(),
    })?;
    let recipient =
        Pubkey::from_str(&transaction.recipient).map_err(|e| OperatorError::InvalidPubkey {
            pubkey: transaction.recipient.clone(),
            reason: e.to_string(),
        })?;

    // Fetch mint metadata from cache (or storage if not cached)
    let mint_metadata = processor_state.mint_cache.get_mint_metadata(&mint).await?;
    let token_program = mint_metadata.token_program;

    let allowed_mint_pda = release_funds_state.get_allowed_mint_pda(&mint);
    let instance_ata = release_funds_state.get_instance_ata(&mint, &token_program);

    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);

    // Sibling proofs and new withdrawal root are filled in by the sender once
    // the nonce reaches the front of the in-flight queue.
    builder
        .payer(processor_state.admin_pubkey)
        .operator(release_funds_state.operator_pubkey)
        .instance(release_funds_state.instance_pda)
        .operator_pda(release_funds_state.operator_pda)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .user_ata(recipient_ata)
        .instance_ata(instance_ata)
        .token_program(token_program)
        .user(recipient)
        .transaction_nonce(nonce);

    let amount = u64::try_from(transaction.amount).map_err(|_| {
        OperatorError::Program(ProgramError::InvalidBuilder {
            reason: format!(
                "negative withdrawal amount {} for transaction {}",
                transaction.amount, transaction.id
            ),
        })
    })?;
    builder.amount(amount);

    // Remint info for recovery-on-permanent-failure.  PrivateChannel token program, not
    // mainnet — remint happens on PrivateChannel.
    let private_channel_token_program = processor_state
        .mint_cache
        .get_private_channel_token_program();
    let remint_user_ata = get_associated_token_address_with_program_id(
        &recipient,
        &mint,
        &private_channel_token_program,
    );
    let remint_info = WithdrawalRemintInfo {
        transaction_id: transaction.id,
        trace_id: transaction.trace_id.clone(),
        mint,
        user: recipient,
        user_ata: remint_user_ata,
        token_program: private_channel_token_program,
        amount,
    };

    Ok(TransactionBuilder::ReleaseFunds(Box::new(
        ReleaseFundsBuilderWithNonce {
            builder,
            nonce,
            transaction_id: transaction.id,
            trace_id: transaction.trace_id.clone(),
            remint_info: Some(remint_info),
        },
    )))
}

/// Build the tree-rotation TransactionBuilder for a nonce landing on the
/// MAX_TREE_LEAVES boundary (normal, non-poison path).
fn build_scheduled_rotation(
    admin_pubkey: Pubkey,
    release_funds_state: &ReleaseFundsState,
) -> TransactionBuilder {
    let mut rotation_builder = ResetSmtRootBuilder::new();
    rotation_builder
        .payer(admin_pubkey)
        .operator(release_funds_state.operator_pubkey)
        .instance(release_funds_state.instance_pda)
        .operator_pda(release_funds_state.operator_pda)
        .event_authority(release_funds_state.event_authority_pda);
    TransactionBuilder::ResetSmtRoot(Box::new(rotation_builder))
}

/// Token-2022 pre-flight for a withdrawal.
///
/// Returns:
/// - `Ok(None)` — clean: proceed to build + dispatch.
/// - `Ok(Some(reason))` — row-specific bail: caller routes to ManualReview
///   via `quarantine_single` and continues the loop. Used for paused mints
///   and permanent-delegate drains where the row's data is fine but the
///   on-chain state would cause an immediate release-funds failure.
/// - `Err(_)` — transient infrastructure issue (RPC failure, malformed
///   mint data). Caller's classifier treats as Transient and restarts the
///   task, which is preferable to mass-quarantining rows during an RPC
///   blip.
async fn check_withdrawal_preflights(
    processor_state: &mut ProcessorState,
    transaction: &DbTransaction,
) -> Result<Option<String>, OperatorError> {
    let mint = Pubkey::from_str(&transaction.mint).map_err(|e| OperatorError::InvalidPubkey {
        pubkey: transaction.mint.clone(),
        reason: e.to_string(),
    })?;

    // PausableConfig and PermanentDelegate only exist on Token-2022 mints.
    // For legacy SPL Token, skip the pre-flight entirely — saves an RPC
    // round-trip on every withdrawal and avoids forcing extension-flag
    // resolution for mints that can't carry the extensions in the first
    // place. Falls back to RPC only if the mint isn't in the DB yet.
    let token_program = processor_state
        .mint_cache
        .get_mint_metadata(&mint)
        .await?
        .token_program;
    if token_program != spl_token_2022::ID {
        return Ok(None);
    }

    let (is_pausable, has_permanent_delegate) = processor_state
        .mint_cache
        .get_extension_flags(&mint)
        .await?;

    if is_pausable && processor_state.mint_cache.check_paused(&mint).await? {
        return Ok(Some(format!("mint paused: {mint}")));
    }

    if has_permanent_delegate {
        let amount = u64::try_from(transaction.amount).map_err(|_| {
            OperatorError::Program(ProgramError::InvalidBuilder {
                reason: format!(
                    "negative withdrawal amount {} for transaction {}",
                    transaction.amount, transaction.id
                ),
            })
        })?;

        let release_funds_state = processor_state
            .release_funds_state
            .as_mut()
            .ok_or(OperatorError::MissingBuilder)?;
        let instance_ata = release_funds_state.get_instance_ata(&mint, &token_program);

        let on_chain = processor_state
            .mint_cache
            .get_ata_balance(&instance_ata)
            .await?;
        if on_chain < amount {
            return Ok(Some(format!(
                "insufficient escrow balance: on_chain={on_chain}, needed={amount}"
            )));
        }
    }

    Ok(None)
}

pub async fn process_release_funds(
    processor_state: &mut ProcessorState,
    mut fetcher_rx: mpsc::Receiver<DbTransaction>,
    sender_tx: mpsc::Sender<TransactionBuilder>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    storage: Arc<Storage>,
    program_type: ProgramType,
) -> Result<(), OperatorError> {
    if processor_state.release_funds_state.is_none() {
        return Err(OperatorError::MissingBuilder);
    }

    let pt_label = program_type.as_label();

    while let Some(transaction) = fetcher_rx.recv().await {
        let span = info_span!("process", trace_id = %transaction.trace_id, txn_id = transaction.id);

        let outcome: Result<(), OperatorError> = async {
            // Build the withdrawal first so (a) rotation + withdrawal dispatch
            // are atomic from the sender's perspective, and (b) row-data
            // poison (e.g. NULL nonce, unparseable pubkey) surfaces here as
            // an `InvalidBuilder` for the classifier to halt the pipeline on.
            // Build also warms `MintCache.cache`, so the pre-flight below
            // doesn't pay an extra DB/RPC round-trip for `get_mint_metadata`.
            let release_funds_tx = build_release_funds(processor_state, &transaction).await?;

            // Pre-flight checks for Token-2022 extension state. Pause and
            // permanent-delegate-drain are row-specific: the mint or its
            // on-chain balance is the issue, but other withdrawals are
            // unaffected. Bails route to ManualReview via `quarantine_single`
            // and continue the loop — they intentionally do NOT trigger
            // `halt_withdrawal_pipeline` (which is reserved for poison-pill
            // rows that would corrupt the SMT).
            //
            // The pre-flight is best-effort, not a guarantee: a permanent
            // delegate can drain the escrow ATA between this balance read and
            // the on-chain `TransferChecked` CPI. In that race the CPI fails
            // on-chain and the row is handled by the normal sender
            // confirmation / retry path — the pre-flight just shrinks the
            // window in the common case.
            //
            // RPC errors during pre-flight bubble up via `?` and are
            // classified as Transient by `classify_processor_error`,
            // restarting the task. That's preferred over flooding the alert
            // stream with ManualReview entries while RPC flaps.
            if let Some(reason) = check_withdrawal_preflights(processor_state, &transaction).await?
            {
                quarantine_single(&storage_tx, &transaction, reason).await;
                return Ok(());
            }

            // Scheduled rotation (normal path): when a nonce lands on the
            // MAX_TREE_LEAVES boundary, rotate the tree BEFORE dispatching the
            // boundary withdrawal.
            if let Some(nonce_i64) = transaction.withdrawal_nonce {
                let nonce = nonce_i64 as u64;
                if nonce > 0 && nonce.is_multiple_of(MAX_TREE_LEAVES as u64) {
                    info!(
                        nonce,
                        "Tree rotation boundary detected, dispatching ResetSmtRoot"
                    );
                    let release_funds_state = processor_state
                        .release_funds_state
                        .as_ref()
                        .ok_or(OperatorError::MissingBuilder)?;
                    let rotation_tx =
                        build_scheduled_rotation(processor_state.admin_pubkey, release_funds_state);
                    send_guaranteed(&sender_tx, rotation_tx, "reset smt root")
                        .await
                        .map_err(OperatorError::ChannelSend)?;
                }
            }

            info!("Processing withdrawal");
            send_guaranteed(&sender_tx, release_funds_tx, "processed release funds")
                .await
                .map_err(OperatorError::ChannelSend)?;

            Ok(())
        }
        .instrument(span.clone())
        .await;

        // A per-row error is classified.  For a deterministic poison-pill
        // we quarantine the row, halt the whole withdrawal pipeline, and
        // return so the supervisor can shut down cleanly.  Transient or
        // fatal errors bubble up directly.
        if let Err(err) = outcome {
            match classify_processor_error(&err) {
                ErrorDisposition::Quarantine(reason) => {
                    warn!(
                        txn_id = transaction.id,
                        trace_id = %transaction.trace_id,
                        reason,
                        "Quarantining withdrawal and halting pipeline: {}",
                        err
                    );
                    metrics::OPERATOR_TRANSACTION_QUARANTINED
                        .with_label_values(&[pt_label, reason])
                        .inc();
                    quarantine_single(&storage_tx, &transaction, err.to_string()).await;
                    halt_withdrawal_pipeline(
                        &storage,
                        &storage_tx,
                        &mut fetcher_rx,
                        Some(transaction.id),
                    )
                    .await;
                    return Ok(());
                }
                ErrorDisposition::Transient => {
                    // Transient: surface the error so the supervisor can
                    // restart us cleanly.
                    return Err(err);
                }
                ErrorDisposition::Fatal => {
                    error!(
                        txn_id = transaction.id,
                        "Fatal processor error, exiting task: {}", err
                    );
                    return Err(err);
                }
            }
        }
    }

    Ok(())
}

pub async fn process_deposit_funds(
    processor_state: &mut ProcessorState,
    mut fetcher_rx: mpsc::Receiver<DbTransaction>,
    sender_tx: mpsc::Sender<TransactionBuilder>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    program_type: ProgramType,
) -> Result<(), OperatorError> {
    let pt_label = program_type.as_label();

    while let Some(transaction) = fetcher_rx.recv().await {
        let span = info_span!("process", trace_id = %transaction.trace_id, txn_id = transaction.id);

        let outcome: Result<(), OperatorError> = async {
            let proc_t0 = tokio::time::Instant::now();
            let mint =
                Pubkey::from_str(&transaction.mint).map_err(|e| OperatorError::InvalidPubkey {
                    pubkey: transaction.mint.clone(),
                    reason: e.to_string(),
                })?;
            let recipient = Pubkey::from_str(&transaction.recipient).map_err(|e| {
                OperatorError::InvalidPubkey {
                    pubkey: transaction.recipient.clone(),
                    reason: e.to_string(),
                }
            })?;

            let token_program = processor_state
                .mint_cache
                .get_private_channel_token_program();

            let recipient_ata =
                get_associated_token_address_with_program_id(&recipient, &mint, &token_program);

            let mut builder = MintToBuilder::new();
            builder
                .mint(mint)
                .recipient(recipient)
                .recipient_ata(recipient_ata)
                .payer(processor_state.admin_pubkey)
                .mint_authority(processor_state.admin_pubkey)
                .token_program(token_program)
                .amount(transaction.amount as u64)
                .idempotency_memo(mint_idempotency_memo(transaction.id));

            let proc_elapsed_ms = proc_t0.elapsed().as_millis();
            info!(proc_elapsed_ms, "Processing deposit");

            let wrapped = TransactionBuilder::Mint(Box::new(MintToBuilderWithTxnId {
                builder,
                txn_id: transaction.id,
                trace_id: transaction.trace_id.clone(),
            }));

            let send_t0 = tokio::time::Instant::now();
            send_guaranteed(&sender_tx, wrapped, "processed deposit")
                .await
                .map_err(OperatorError::ChannelSend)?;
            let send_elapsed_ms = send_t0.elapsed().as_millis();
            // Any wait >1ms means the sender channel is full — sender is the bottleneck.
            if send_elapsed_ms > 1 {
                debug!(
                    send_elapsed_ms,
                    sender_capacity = sender_tx.capacity(),
                    "Processor blocked sending to sender (sender back-pressure)"
                );
            }

            Ok(())
        }
        .instrument(span)
        .await;

        // Deposit-side quarantine. Unlike withdrawals, deposits have no
        // nonce, so a bad row is simply moved to
        // ManualReview and the loop continues. The user's on-chain tokens
        // are still locked in escrow until a human reviews the row.
        if let Err(err) = outcome {
            match classify_processor_error(&err) {
                ErrorDisposition::Quarantine(reason) => {
                    warn!(
                        txn_id = transaction.id,
                        trace_id = %transaction.trace_id,
                        reason,
                        "Quarantining deposit to ManualReview: {}",
                        err
                    );
                    metrics::OPERATOR_TRANSACTION_QUARANTINED
                        .with_label_values(&[pt_label, reason])
                        .inc();
                    quarantine_single(&storage_tx, &transaction, err.to_string()).await;
                }
                ErrorDisposition::Transient | ErrorDisposition::Fatal => {
                    return Err(err);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AccountError, StorageError, TransactionError};
    use crate::operator::find_allowed_mint_pda;
    use crate::storage::common::models::TransactionType;
    use crate::storage::common::storage::mock::MockStorage;

    fn make_release_funds_state() -> ReleaseFundsState {
        ReleaseFundsState {
            instance_pda: Pubkey::new_unique(),
            operator_pubkey: Pubkey::new_unique(),
            operator_pda: Pubkey::new_unique(),
            event_authority_pda: Pubkey::new_unique(),
            allowed_mints: HashMap::new(),
            instance_atas: HashMap::new(),
        }
    }

    #[test]
    fn get_allowed_mint_pda_derives_and_caches() {
        let mut state = make_release_funds_state();
        let mint = Pubkey::new_unique();

        let pda1 = state.get_allowed_mint_pda(&mint);
        let pda2 = state.get_allowed_mint_pda(&mint);

        assert_eq!(pda1, pda2);
        assert_eq!(pda1, find_allowed_mint_pda(&state.instance_pda, &mint));
        assert_eq!(state.allowed_mints.len(), 1);
    }

    #[test]
    fn get_allowed_mint_pda_different_mints() {
        let mut state = make_release_funds_state();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        assert_ne!(
            state.get_allowed_mint_pda(&mint_a),
            state.get_allowed_mint_pda(&mint_b)
        );
        assert_eq!(state.allowed_mints.len(), 2);
    }

    #[test]
    fn get_instance_ata_derives_and_caches() {
        let mut state = make_release_funds_state();
        let mint = Pubkey::new_unique();
        let tp = spl_token::id();

        let ata1 = state.get_instance_ata(&mint, &tp);
        let ata2 = state.get_instance_ata(&mint, &tp);

        assert_eq!(ata1, ata2);
        let expected =
            get_associated_token_address_with_program_id(&state.instance_pda, &mint, &tp);
        assert_eq!(ata1, expected);
        assert_eq!(state.instance_atas.len(), 1);
    }

    #[test]
    fn get_instance_ata_different_mints() {
        let mut state = make_release_funds_state();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let tp = spl_token::id();

        assert_ne!(
            state.get_instance_ata(&mint_a, &tp),
            state.get_instance_ata(&mint_b, &tp)
        );
        assert_eq!(state.instance_atas.len(), 2);
    }

    fn make_db_transaction(
        id: i64,
        mint: &str,
        recipient: &str,
        nonce: Option<i64>,
        txn_type: crate::storage::common::models::TransactionType,
    ) -> DbTransaction {
        DbTransaction {
            id,
            signature: format!("sig_{id}"),
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: "initiator".to_string(),
            recipient: recipient.to_string(),
            mint: mint.to_string(),
            amount: 1000,
            memo: None,
            transaction_type: txn_type,
            withdrawal_nonce: nonce,
            status: TransactionStatus::Processing,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            pending_remint_deadline_at: None,
        }
    }

    #[tokio::test]
    async fn process_release_funds_missing_state_errors() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };
        let (_tx, rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, _sender_rx) = mpsc::channel(1);
        let (storage_tx, _storage_rx) = mpsc::channel(1);

        let result = process_release_funds(
            &mut ps,
            rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(
            matches!(result, Err(crate::error::OperatorError::MissingBuilder)),
            "expected MissingBuilder, got: {:?}",
            result
        );
    }

    /// A valid withdrawal transaction is enriched with PDAs and ATA addresses then forwarded
    /// to the sender channel as a ReleaseFunds builder.
    #[tokio::test]
    async fn process_release_funds_sends_transaction_builder() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        {
            let mock_storage = match storage.as_ref() {
                Storage::Mock(m) => m,
                _ => unreachable!(),
            };
            mock_storage.mints.lock().unwrap().insert(
                mint_pubkey.to_string(),
                crate::storage::common::models::DbMint {
                    mint_address: mint_pubkey.to_string(),
                    decimals: 6,
                    token_program: spl_token::id().to_string(),
                    created_at: chrono::Utc::now(),
                    is_pausable: Some(false),
                    has_permanent_delegate: Some(false),
                },
            );
        }

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(5),
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        let msg = sender_rx.recv().await.unwrap();
        let TransactionBuilder::ReleaseFunds(b) = msg else {
            panic!("expected ReleaseFunds, got a different variant");
        };
        assert_eq!(b.nonce, 5);
        assert_eq!(b.transaction_id, 1);
        assert_eq!(b.trace_id, "trace-1");
    }

    /// When the nonce lands exactly on MAX_TREE_LEAVES, a ResetSmtRoot transaction must be
    /// sent before the ReleaseFunds transaction to rotate the SMT root.
    #[tokio::test]
    async fn process_release_funds_tree_rotation_sends_reset_first() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        {
            let mock_storage = match storage.as_ref() {
                Storage::Mock(m) => m,
                _ => unreachable!(),
            };
            mock_storage.mints.lock().unwrap().insert(
                mint_pubkey.to_string(),
                crate::storage::common::models::DbMint {
                    mint_address: mint_pubkey.to_string(),
                    decimals: 6,
                    token_program: spl_token::id().to_string(),
                    created_at: chrono::Utc::now(),
                    is_pausable: Some(false),
                    has_permanent_delegate: Some(false),
                },
            );
        }

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(MAX_TREE_LEAVES as i64),
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        // First message must be ResetSmtRoot — rotation happens before the boundary withdrawal
        let msg1 = sender_rx.recv().await.unwrap();
        assert!(
            matches!(msg1, TransactionBuilder::ResetSmtRoot(_)),
            "expected ResetSmtRoot first, got: {:?}",
            std::mem::discriminant(&msg1)
        );

        // Second message must be the ReleaseFunds for the boundary nonce itself
        let msg2 = sender_rx.recv().await.unwrap();
        let TransactionBuilder::ReleaseFunds(b) = msg2 else {
            panic!("expected ReleaseFunds second, got a different variant");
        };
        assert_eq!(b.nonce, MAX_TREE_LEAVES as u64);
        assert_eq!(b.transaction_id, 1);

        // No further messages — exactly two were sent
        assert!(sender_rx.try_recv().is_err(), "unexpected third message");
    }

    /// A mint field that cannot be parsed as a Pubkey halts the pipeline.
    /// The poison row is marked ManualReview, no rotation is dispatched,
    /// and subsequent active withdrawals are quarantined.
    #[tokio::test]
    async fn process_release_funds_invalid_mint_quarantines_and_halts() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            "not_a_valid_pubkey",
            &Pubkey::new_unique().to_string(),
            Some(1),
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        // Task must NOT crash on a poison row.
        assert!(
            result.is_ok(),
            "expected Ok on quarantine, got: {:?}",
            result
        );

        // A ManualReview status update was sent for the poison row.
        let update = storage_rx.recv().await.expect("quarantine update sent");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(update.transaction_id, 1);

        // Sender must not have received anything — rotation is no longer
        // part of the quarantine path.
        assert!(
            sender_rx.try_recv().is_err(),
            "unexpected message on sender channel"
        );
    }

    /// A valid deposit transaction is wrapped as a Mint builder with the correct ATA and
    /// idempotency memo, then forwarded to the sender channel.
    #[tokio::test]
    async fn process_deposit_funds_sends_mint_builder() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            None,
            crate::storage::common::models::TransactionType::Deposit,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            ProgramType::Escrow,
        )
        .await;
        assert!(result.is_ok());

        let msg = sender_rx.recv().await.unwrap();
        let TransactionBuilder::Mint(b) = msg else {
            panic!("expected Mint, got a different variant");
        };
        assert_eq!(b.txn_id, 1);
        assert_eq!(b.trace_id, "trace-1");
    }

    /// A non-base58 mint string is quarantined rather than propagated — the
    /// deposit task continues so other deposits still land.
    #[tokio::test]
    async fn process_deposit_funds_invalid_mint_quarantines() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            "not_a_valid_pubkey",
            &Pubkey::new_unique().to_string(),
            None,
            crate::storage::common::models::TransactionType::Deposit,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            ProgramType::Escrow,
        )
        .await;
        assert!(
            result.is_ok(),
            "expected Ok on quarantine, got: {:?}",
            result
        );

        let update = storage_rx.recv().await.expect("quarantine update sent");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(update.transaction_id, 1);
    }

    /// An already-closed fetcher channel means there are no transactions to process;
    /// the function should return Ok(()) immediately without touching the sender.
    #[tokio::test]
    async fn process_deposit_funds_empty_channel_returns_ok() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        drop(fetcher_tx); // close channel immediately — no transactions to process

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        // Nothing was sent; channel is empty and the sender was dropped by the function
        assert!(
            sender_rx.try_recv().is_err(),
            "expected empty sender channel"
        );
    }

    /// A recipient field that is not a valid base58 pubkey must quarantine
    /// the row (deposit has no tree to rotate — just the ManualReview alert).
    #[tokio::test]
    async fn process_deposit_funds_invalid_recipient_quarantines() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &Pubkey::new_unique().to_string(),
            "not_a_valid_pubkey",
            None,
            crate::storage::common::models::TransactionType::Deposit,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            ProgramType::Escrow,
        )
        .await;
        assert!(
            result.is_ok(),
            "expected Ok on quarantine, got: {:?}",
            result
        );

        let update = storage_rx.recv().await.expect("quarantine update sent");
        assert_eq!(update.status, TransactionStatus::ManualReview);
    }

    /// An unparseable recipient pubkey on a withdrawal quarantines the row
    /// and halts the pipeline without dispatching a rotation.
    #[tokio::test]
    async fn process_release_funds_invalid_recipient_quarantines_and_halts() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let mint_pubkey = Pubkey::new_unique();
        {
            let mock_storage = match storage.as_ref() {
                Storage::Mock(m) => m,
                _ => unreachable!(),
            };
            mock_storage.mints.lock().unwrap().insert(
                mint_pubkey.to_string(),
                crate::storage::common::models::DbMint {
                    mint_address: mint_pubkey.to_string(),
                    decimals: 6,
                    token_program: spl_token::id().to_string(),
                    created_at: chrono::Utc::now(),
                    is_pausable: Some(false),
                    has_permanent_delegate: Some(false),
                },
            );
        }

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            "not_a_valid_pubkey",
            Some(5),
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        let update = storage_rx.recv().await.expect("quarantine update sent");
        assert_eq!(update.status, TransactionStatus::ManualReview);

        assert!(
            sender_rx.try_recv().is_err(),
            "no rotation should be dispatched on quarantine"
        );
    }

    /// A withdrawal row missing `withdrawal_nonce` is poison — the builder
    /// cannot be constructed.  Must quarantine rather than panic so the
    /// task stays alive.
    #[tokio::test]
    async fn process_release_funds_missing_nonce_quarantines() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            None, // <- the poison: withdrawals should never have a NULL nonce
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        let update = storage_rx.recv().await.expect("quarantine update sent");
        assert_eq!(update.status, TransactionStatus::ManualReview);
    }

    // ── classify_processor_error ────────────────────────────────────────

    /// Every `OperatorError` variant that can surface inside the per-row
    /// async block must map to exactly one `ErrorDisposition`. A missing or
    /// mis-mapped variant is a silent correctness hole — any new variant
    /// added later will fail this test and force a conscious decision.
    #[test]
    fn classify_processor_error_covers_every_variant() {
        // Quarantine variants — deterministic, cannot succeed on retry.
        let invalid_pubkey = OperatorError::InvalidPubkey {
            pubkey: "xxx".into(),
            reason: "bad".into(),
        };
        assert!(matches!(
            classify_processor_error(&invalid_pubkey),
            ErrorDisposition::Quarantine("invalid_pubkey")
        ));

        let invalid_builder = OperatorError::Program(ProgramError::InvalidBuilder {
            reason: "missing field".into(),
        });
        assert!(matches!(
            classify_processor_error(&invalid_builder),
            ErrorDisposition::Quarantine("invalid_builder")
        ));

        let other_program = OperatorError::Program(ProgramError::SmtNotInitialized);
        assert!(matches!(
            classify_processor_error(&other_program),
            ErrorDisposition::Quarantine("program_error")
        ));

        // Fatal variants — processor is misconfigured or downstream is dead.
        assert!(matches!(
            classify_processor_error(&OperatorError::MissingBuilder),
            ErrorDisposition::Fatal
        ));
        assert!(matches!(
            classify_processor_error(&OperatorError::ChannelClosed {
                component: "sender".into()
            }),
            ErrorDisposition::Fatal
        ));
        assert!(matches!(
            classify_processor_error(&OperatorError::ShutdownChannelSend),
            ErrorDisposition::Fatal
        ));

        // Transient variants — infra blips, supervisor restart is correct.
        let storage_err = OperatorError::Storage(StorageError::DatabaseError {
            message: "connection reset".into(),
        });
        assert!(matches!(
            classify_processor_error(&storage_err),
            ErrorDisposition::Transient
        ));

        let rpc_err = OperatorError::RpcError("429".into());
        assert!(matches!(
            classify_processor_error(&rpc_err),
            ErrorDisposition::Transient
        ));

        let webhook_err = OperatorError::WebhookError("timeout".into());
        assert!(matches!(
            classify_processor_error(&webhook_err),
            ErrorDisposition::Transient
        ));

        let account_err = OperatorError::Account(AccountError::AccountNotFound {
            pubkey: Pubkey::new_unique(),
        });
        assert!(matches!(
            classify_processor_error(&account_err),
            ErrorDisposition::Transient
        ));

        let txn_err = OperatorError::Transaction(Box::new(TransactionError::Program(
            ProgramError::SmtNotInitialized,
        )));
        assert!(matches!(
            classify_processor_error(&txn_err),
            ErrorDisposition::Transient
        ));
    }

    // ── quarantine_single ───────────────────────────────────────────────

    /// `quarantine_single` is the single source of truth for the
    /// ManualReview status update.  Verify every field we write so a future
    /// refactor cannot silently drop an attribute the webhook relies on.
    #[tokio::test]
    async fn quarantine_single_writes_complete_status_update() {
        let (storage_tx, mut storage_rx) = mpsc::channel(1);
        let txn = make_db_transaction(
            77,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            Some(9),
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        quarantine_single(&storage_tx, &txn, "bad row".into()).await;

        let update = storage_rx.recv().await.expect("update was sent");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.trace_id.as_deref(), Some("trace-77"));
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(update.counterpart_signature, None);
        assert!(update.processed_at.is_some());
        assert_eq!(update.error_message.as_deref(), Some("bad row"));
        assert_eq!(update.remint_signature, None);
        assert!(!update.remint_attempted);
    }

    /// A closed `storage_tx` is observable at startup-shutdown race — we
    /// only log, we do not panic.  Without this the supervisor restart
    /// could infinite-loop on a half-torn-down process.
    #[tokio::test]
    async fn quarantine_single_survives_closed_channel() {
        let (storage_tx, storage_rx) = mpsc::channel(1);
        drop(storage_rx);
        let txn = make_db_transaction(
            1,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            Some(0),
            crate::storage::common::models::TransactionType::Withdrawal,
        );

        // Must not panic.  send_guaranteed will log and return Err; we swallow it.
        quarantine_single(&storage_tx, &txn, "closed".into()).await;
    }

    // ── halt_withdrawal_pipeline ────────────────────────────────────────

    /// Even when no rows are buffered in the fetcher channel, the DB sweep
    /// must still run so pipeline-pause semantics hold: any row a sibling
    /// instance already locked (`Processing`) is swept to `ManualReview`.
    #[tokio::test]
    async fn halt_withdrawal_pipeline_empty_channel_still_sweeps_db() {
        let mock = MockStorage::new();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            let mut processing = make_db_transaction(
                10,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                Some(1),
                TransactionType::Withdrawal,
            );
            processing.status = TransactionStatus::Processing;
            db.push(processing);
        }
        let storage = Storage::Mock(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(4);
        let (_fetcher_tx, mut fetcher_rx) = mpsc::channel::<DbTransaction>(4);

        halt_withdrawal_pipeline(&storage, &storage_tx, &mut fetcher_rx, None).await;

        // No in-flight rows were buffered — no channel-side quarantines.
        assert!(storage_rx.try_recv().is_err());

        // DB sweep still runs — the Processing row is now ManualReview.
        let rows = match &storage {
            Storage::Mock(m) => m.pending_transactions.lock().unwrap().clone(),
            _ => unreachable!(),
        };
        assert_eq!(rows[0].status, TransactionStatus::ManualReview);
    }

    /// Every row buffered in `fetcher_rx` is individually quarantined —
    /// the loop must drain, not short-circuit on first row.
    #[tokio::test]
    async fn halt_withdrawal_pipeline_drains_every_buffered_row() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(16);
        let (fetcher_tx, mut fetcher_rx) = mpsc::channel::<DbTransaction>(8);

        for id in 1..=5 {
            fetcher_tx
                .send(make_db_transaction(
                    id,
                    &Pubkey::new_unique().to_string(),
                    &Pubkey::new_unique().to_string(),
                    Some(id),
                    TransactionType::Withdrawal,
                ))
                .await
                .unwrap();
        }
        drop(fetcher_tx);

        halt_withdrawal_pipeline(&storage, &storage_tx, &mut fetcher_rx, None).await;

        let mut ids = Vec::new();
        while let Ok(update) = storage_rx.try_recv() {
            assert_eq!(update.status, TransactionStatus::ManualReview);
            ids.push(update.transaction_id);
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    /// A DB sweep failure must not prevent the channel drain from
    /// reporting what it already quarantined.  The offending row + buffered
    /// rows are still visible in the alert stream — a strictly better
    /// outcome than swallowing both.
    #[tokio::test]
    async fn halt_withdrawal_pipeline_db_failure_still_drains_channel() {
        let mock = MockStorage::new();
        mock.set_should_fail("quarantine_all_active_withdrawals", true);
        let storage = Storage::Mock(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(4);
        let (fetcher_tx, mut fetcher_rx) = mpsc::channel::<DbTransaction>(4);

        fetcher_tx
            .send(make_db_transaction(
                42,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                Some(7),
                TransactionType::Withdrawal,
            ))
            .await
            .unwrap();
        drop(fetcher_tx);

        // Must not panic; must complete.
        halt_withdrawal_pipeline(&storage, &storage_tx, &mut fetcher_rx, None).await;

        let update = storage_rx.recv().await.expect("buffered row quarantined");
        assert_eq!(update.transaction_id, 42);
        assert_eq!(update.status, TransactionStatus::ManualReview);
    }

    // ── process_release_funds: happy paths ──────────────────────────────

    /// Multiple valid withdrawals stream through the processor in FIFO order.
    /// Each emits a `ReleaseFunds` builder, nothing else is dispatched, and
    /// the processor returns `Ok(())` when the channel closes.
    #[tokio::test]
    async fn process_release_funds_streams_multiple_valid_rows() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mint_pubkey = Pubkey::new_unique();
        {
            let mock_storage = match storage.as_ref() {
                Storage::Mock(m) => m,
                _ => unreachable!(),
            };
            mock_storage.mints.lock().unwrap().insert(
                mint_pubkey.to_string(),
                crate::storage::common::models::DbMint {
                    mint_address: mint_pubkey.to_string(),
                    decimals: 6,
                    token_program: spl_token::id().to_string(),
                    created_at: chrono::Utc::now(),
                    is_pausable: None,
                    has_permanent_delegate: None,
                },
            );
        }
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(16);
        let (storage_tx, _storage_rx) = mpsc::channel(16);

        let recipients: Vec<Pubkey> = (0..3).map(|_| Pubkey::new_unique()).collect();
        for (i, r) in recipients.iter().enumerate() {
            fetcher_tx
                .send(make_db_transaction(
                    (i + 1) as i64,
                    &mint_pubkey.to_string(),
                    &r.to_string(),
                    Some((i + 1) as i64),
                    crate::storage::common::models::TransactionType::Withdrawal,
                ))
                .await
                .unwrap();
        }
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        let mut nonces = Vec::new();
        while let Ok(msg) = sender_rx.try_recv() {
            match msg {
                TransactionBuilder::ReleaseFunds(b) => nonces.push(b.nonce),
                other => panic!("unexpected builder: {:?}", std::mem::discriminant(&other)),
            }
        }
        assert_eq!(nonces, vec![1, 2, 3]);
    }

    /// A boundary nonce where the builder build itself fails must NOT
    /// dispatch the rotation — build_release_funds runs first, and an
    /// error short-circuits before the rotation send.  This locks in the
    /// §4.7 reorder: no sender-visible side effect without a successful
    /// builder.
    #[tokio::test]
    async fn process_release_funds_boundary_poison_never_dispatches_rotation() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Mint is bad → build_release_funds fails → rotation never dispatched.
        let txn = make_db_transaction(
            1,
            "not_a_valid_pubkey",
            &Pubkey::new_unique().to_string(),
            Some(MAX_TREE_LEAVES as i64),
            crate::storage::common::models::TransactionType::Withdrawal,
        );
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        let update = storage_rx.recv().await.expect("quarantine fired");
        assert_eq!(update.status, TransactionStatus::ManualReview);

        // Sender must be empty — no rotation, no release.
        assert!(
            sender_rx.try_recv().is_err(),
            "no dispatch should happen when build_release_funds fails"
        );
    }

    /// After a halt, the processor must STOP processing further buffered
    /// rows — subsequent rows must be quarantined, not turned into
    /// `ReleaseFunds` builders.
    #[tokio::test]
    async fn process_release_funds_halt_stops_processing_further_rows() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let mint_pubkey = Pubkey::new_unique();
        {
            let mock_storage = match storage.as_ref() {
                Storage::Mock(m) => m,
                _ => unreachable!(),
            };
            mock_storage.mints.lock().unwrap().insert(
                mint_pubkey.to_string(),
                crate::storage::common::models::DbMint {
                    mint_address: mint_pubkey.to_string(),
                    decimals: 6,
                    token_program: spl_token::id().to_string(),
                    created_at: chrono::Utc::now(),
                    is_pausable: None,
                    has_permanent_delegate: None,
                },
            );
        }

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        // Row 1: poison.  Row 2: would have been valid.
        fetcher_tx
            .send(make_db_transaction(
                1,
                "not_a_valid_pubkey",
                &Pubkey::new_unique().to_string(),
                Some(1),
                crate::storage::common::models::TransactionType::Withdrawal,
            ))
            .await
            .unwrap();
        fetcher_tx
            .send(make_db_transaction(
                2,
                &mint_pubkey.to_string(),
                &Pubkey::new_unique().to_string(),
                Some(2),
                crate::storage::common::models::TransactionType::Withdrawal,
            ))
            .await
            .unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        // No ReleaseFunds builder reached the sender — halt short-circuited row 2.
        assert!(sender_rx.try_recv().is_err());
    }

    // ── process_deposit_funds: happy + corner ───────────────────────────

    /// Multiple valid deposits stream through the processor; every row
    /// becomes a `Mint` builder in FIFO order.
    #[tokio::test]
    async fn process_deposit_funds_streams_multiple_valid_rows() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(16);
        let (storage_tx, _storage_rx) = mpsc::channel(16);

        for id in 1..=3 {
            fetcher_tx
                .send(make_db_transaction(
                    id,
                    &Pubkey::new_unique().to_string(),
                    &Pubkey::new_unique().to_string(),
                    None,
                    crate::storage::common::models::TransactionType::Deposit,
                ))
                .await
                .unwrap();
        }
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            ProgramType::Escrow,
        )
        .await;
        assert!(result.is_ok());

        let mut ids = Vec::new();
        while let Ok(msg) = sender_rx.try_recv() {
            match msg {
                TransactionBuilder::Mint(m) => ids.push(m.txn_id),
                other => panic!("unexpected builder: {:?}", std::mem::discriminant(&other)),
            }
        }
        assert_eq!(ids, vec![1, 2, 3]);
    }

    /// Deposits have NO pipeline halt — a quarantined deposit must not
    /// stop the loop.  Subsequent valid deposits still reach the sender.
    /// This is the critical asymmetry with withdrawals: deposits have no
    /// nonce gap to worry about.
    #[tokio::test]
    async fn process_deposit_funds_continues_after_quarantine() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(16);
        let (storage_tx, mut storage_rx) = mpsc::channel(16);

        // poison, valid, poison, valid
        fetcher_tx
            .send(make_db_transaction(
                1,
                "not_a_valid_pubkey",
                &Pubkey::new_unique().to_string(),
                None,
                crate::storage::common::models::TransactionType::Deposit,
            ))
            .await
            .unwrap();
        fetcher_tx
            .send(make_db_transaction(
                2,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                None,
                crate::storage::common::models::TransactionType::Deposit,
            ))
            .await
            .unwrap();
        fetcher_tx
            .send(make_db_transaction(
                3,
                &Pubkey::new_unique().to_string(),
                "not_a_valid_pubkey",
                None,
                crate::storage::common::models::TransactionType::Deposit,
            ))
            .await
            .unwrap();
        fetcher_tx
            .send(make_db_transaction(
                4,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                None,
                crate::storage::common::models::TransactionType::Deposit,
            ))
            .await
            .unwrap();
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            ProgramType::Escrow,
        )
        .await;
        assert!(result.is_ok());

        // Exactly two Mint builders (rows 2, 4) and two quarantines (rows 1, 3).
        let mut sent_ids = Vec::new();
        while let Ok(msg) = sender_rx.try_recv() {
            match msg {
                TransactionBuilder::Mint(m) => sent_ids.push(m.txn_id),
                _ => panic!("only Mint expected on deposit path"),
            }
        }
        sent_ids.sort();
        assert_eq!(sent_ids, vec![2, 4]);

        let mut quarantined = Vec::new();
        while let Ok(u) = storage_rx.try_recv() {
            assert_eq!(u.status, TransactionStatus::ManualReview);
            quarantined.push(u.transaction_id);
        }
        quarantined.sort();
        assert_eq!(quarantined, vec![1, 3]);
    }

    /// Poison-pill halts the whole withdrawal pipeline: buffered rows
    /// already in-flight from the fetcher are individually quarantined and
    /// every remaining Pending/Processing withdrawal in the DB is flipped
    /// to ManualReview. The processor does not process the second row.
    #[tokio::test]
    async fn process_release_funds_halt_quarantines_in_flight_and_db() {
        let mock = MockStorage::new();
        // Seed the mock DB with two Pending and one Processing withdrawal.
        // These represent rows that never left the fetcher (Pending) or
        // that a sibling instance locked and hasn't confirmed yet
        // (Processing).
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            let mut pending_a = make_db_transaction(
                100,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                Some(42),
                TransactionType::Withdrawal,
            );
            pending_a.status = TransactionStatus::Pending;
            let mut pending_b = make_db_transaction(
                101,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                Some(43),
                TransactionType::Withdrawal,
            );
            pending_b.status = TransactionStatus::Pending;
            let mut processing = make_db_transaction(
                102,
                &Pubkey::new_unique().to_string(),
                &Pubkey::new_unique().to_string(),
                Some(44),
                TransactionType::Withdrawal,
            );
            processing.status = TransactionStatus::Processing;
            db.push(pending_a);
            db.push(pending_b);
            db.push(processing);
        }

        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        // fetcher_rx capacity 4 so we can buffer three rows: the poison,
        // plus two in-flight rows already delivered by the fetcher.
        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let poison = make_db_transaction(
            1,
            "not_a_valid_pubkey",
            &Pubkey::new_unique().to_string(),
            Some(1),
            TransactionType::Withdrawal,
        );
        let in_flight_a: DbTransaction = make_db_transaction(
            2,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            Some(2),
            TransactionType::Withdrawal,
        );
        let in_flight_b: DbTransaction = make_db_transaction(
            3,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            Some(3),
            TransactionType::Withdrawal,
        );
        fetcher_tx.send(poison).await.unwrap();
        fetcher_tx.send(in_flight_a).await.unwrap();
        fetcher_tx.send(in_flight_b).await.unwrap();
        drop(fetcher_tx);

        let result = process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            ProgramType::Withdraw,
        )
        .await;
        assert!(
            result.is_ok(),
            "processor must exit cleanly, got {result:?}"
        );

        // Collect all status updates emitted on storage_tx.
        let mut updates = Vec::new();
        while let Ok(update) = storage_rx.try_recv() {
            updates.push(update);
        }
        // The poison row + two in-flight rows should all be marked
        // ManualReview on the channel (3 total).
        assert_eq!(
            updates.len(),
            3,
            "expected 3 channel-side quarantines, got: {updates:?}"
        );
        assert!(updates
            .iter()
            .all(|u| u.status == TransactionStatus::ManualReview));
        let ids: Vec<i64> = updates.iter().map(|u| u.transaction_id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));

        // Every Pending/Processing withdrawal in the mock DB is flipped to
        // ManualReview by quarantine_all_active_withdrawals.
        let mock_ref = match storage.as_ref() {
            Storage::Mock(m) => m,
            _ => unreachable!(),
        };
        let db_rows = mock_ref.pending_transactions.lock().unwrap();
        for txn in db_rows.iter() {
            assert_eq!(
                txn.status,
                TransactionStatus::ManualReview,
                "row {} was not quarantined",
                txn.id
            );
        }

        // No rotation was dispatched to the sender.
        assert!(
            sender_rx.try_recv().is_err(),
            "no sender-side dispatch expected on halt"
        );
    }

    /// When a mint carries the PermanentDelegate extension and the escrow ATA
    /// balance is below the withdrawal amount, the withdrawal must be routed to
    /// ManualReview via `storage_tx` (no TransactionBuilder emitted).
    #[tokio::test]
    async fn process_release_funds_permanent_delegate_insufficient_balance_routes_to_manual_review()
    {
        use crate::operator::rpc_util::RpcClientWithRetry;
        use solana_client::rpc_request::RpcRequest;

        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mock = MockStorage::new();
        mock.mints.lock().unwrap().insert(
            mint_pubkey.to_string(),
            crate::storage::common::models::DbMint {
                mint_address: mint_pubkey.to_string(),
                decimals: 6,
                token_program: spl_token_2022::id().to_string(),
                created_at: chrono::Utc::now(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(true),
            },
        );
        let storage = Arc::new(Storage::Mock(mock));

        // On-chain balance < amount → should bail to ManualReview.
        let balance_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "amount": "500",
                "decimals": 6,
                "uiAmount": 0.0005,
                "uiAmountString": "0.0005"
            }
        });
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetTokenAccountBalance, balance_response);
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let (storage_tx, mut storage_rx) = mpsc::channel(1);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);

        let txn = DbTransaction {
            id: 42,
            signature: "test_sig".to_string(),
            trace_id: "trace-42".to_string(),
            slot: 100,
            initiator: "initiator".to_string(),
            recipient: recipient.to_string(),
            mint: mint_pubkey.to_string(),
            amount: 1000, // > on-chain balance of 500
            memo: None,
            transaction_type: crate::storage::common::models::TransactionType::Withdrawal,
            withdrawal_nonce: Some(5),
            status: crate::storage::common::models::TransactionStatus::Processing,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            pending_remint_deadline_at: None,
        };

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            crate::config::ProgramType::Withdraw,
        )
        .await
        .unwrap();

        let update = storage_rx
            .try_recv()
            .expect("ManualReview status update should have been sent");
        assert_eq!(update.transaction_id, 42);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err_msg = update.error_message.expect("error_message must be set");
        assert!(
            err_msg.contains("insufficient escrow balance")
                && err_msg.contains("on_chain=500")
                && err_msg.contains("needed=1000"),
            "unexpected error_message: {err_msg}",
        );
        assert!(
            sender_rx.try_recv().is_err(),
            "no TransactionBuilder should have been emitted",
        );
    }

    /// When the escrow ATA balance is sufficient, the permanent-delegate
    /// pre-flight is a no-op and the withdrawal proceeds to the sender.
    #[tokio::test]
    async fn process_release_funds_permanent_delegate_sufficient_balance_proceeds() {
        use crate::operator::rpc_util::RpcClientWithRetry;
        use solana_client::rpc_request::RpcRequest;

        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mock = MockStorage::new();
        mock.mints.lock().unwrap().insert(
            mint_pubkey.to_string(),
            crate::storage::common::models::DbMint {
                mint_address: mint_pubkey.to_string(),
                decimals: 6,
                token_program: spl_token_2022::id().to_string(),
                created_at: chrono::Utc::now(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(true),
            },
        );
        let storage = Arc::new(Storage::Mock(mock));

        let balance_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "amount": "5000",
                "decimals": 6,
                "uiAmount": 0.005,
                "uiAmountString": "0.005"
            }
        });
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetTokenAccountBalance, balance_response);
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let (storage_tx, mut storage_rx) = mpsc::channel(1);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);

        let txn = DbTransaction {
            id: 7,
            signature: "test_sig".to_string(),
            trace_id: "trace-7".to_string(),
            slot: 100,
            initiator: "initiator".to_string(),
            recipient: recipient.to_string(),
            mint: mint_pubkey.to_string(),
            amount: 1000, // < on-chain balance of 5000
            memo: None,
            transaction_type: crate::storage::common::models::TransactionType::Withdrawal,
            withdrawal_nonce: Some(5),
            status: crate::storage::common::models::TransactionStatus::Processing,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            pending_remint_deadline_at: None,
        };

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            crate::config::ProgramType::Withdraw,
        )
        .await
        .unwrap();

        let msg = sender_rx.recv().await.expect("ReleaseFunds should be sent");
        let TransactionBuilder::ReleaseFunds(b) = msg else {
            panic!("expected ReleaseFunds, got a different variant");
        };
        assert_eq!(b.transaction_id, 7);
        assert!(
            storage_rx.try_recv().is_err(),
            "no ManualReview update should have been sent",
        );
    }
}
