use crate::channel_utils::send_guaranteed;
use crate::error::{AccountError, OperatorError, ProgramError};
use crate::metrics;
use crate::operator::instruction_util::{
    mint_idempotency_memo, MintToBuilder, SourceEventId, TransactionBuilder, WithdrawalRemintInfo,
};
use crate::operator::recovery::{
    classify_deposit_signatures, DepositOutcome, MAX_RECOVERY_REQUEUE_ATTEMPTS,
};
use crate::operator::sender::{FinalityRpc, TransactionStatusUpdate};
use crate::operator::utils::storage_util::with_storage_backoff;
use crate::operator::{
    fetch_current_tree_index, find_allowed_mint_pda, find_event_authority_pda, find_operator_pda,
    tree_constants::MAX_TREE_LEAVES, MintToBuilderWithTxnId, ReleaseFundsBuilderWithNonce,
    ResetSmtRootBuilderWithTarget, SignerUtil,
};
use crate::operator::{utils::mint_util::MintCache, RpcClientWithRetry};
use crate::storage::common::models::{DbTransaction, TransactionStatus};
use crate::storage::common::storage::RequeueOutcome;
use crate::storage::Storage;
use crate::ProgramType;
use chrono::Utc;
use private_channel_escrow_program_client::instructions::ReleaseFundsBuilder;
use private_channel_escrow_program_client::programs::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
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
        rpc_client: Arc<RpcClientWithRetry>,
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
        mint_rpc_client: Arc<RpcClientWithRetry>,
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
        OperatorError::MintNotAllowed { .. } => ErrorDisposition::Quarantine("mint_not_allowed"),
        OperatorError::Program(ProgramError::InvalidBuilder { .. }) => {
            ErrorDisposition::Quarantine("invalid_builder")
        }
        // Other Program(_) variants are from the sender-side proof/root checks and
        // cannot originate in the processor today — label them generically if they
        // ever surface here.
        OperatorError::Program(_) => ErrorDisposition::Quarantine("program_error"),
        // MissingBuilder means the processor was constructed without the state it
        // needs — configuration bug, not a row problem.  Exit to surface it.
        // SenderAlreadyRunning is a sender-startup error and never reaches the
        // processor, but it's Fatal in spirit, so classify it alongside.
        OperatorError::MissingBuilder | OperatorError::SenderAlreadyRunning { .. } => {
            ErrorDisposition::Fatal
        }
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

/// A row-specific reason to park one withdrawal without stopping the pipeline.
/// `label` is the metric dimension and `message` lands on the row and its alert.
/// Poison rows take the error classifier instead, which sweeps every active row.
struct BailReason {
    label: &'static str,
    message: String,
}

/// How far a row got before leaving the loop body, which decides whether the
/// rows the fetcher already handed us are still safe to dispatch.
enum RowOutcome {
    Continue,
    ParkedBeforeRotation,
}

impl BailReason {
    fn new(label: &'static str, message: String) -> Self {
        Self { label, message }
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
        release_signatures: None,
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

/// Park one row in `ManualReview` and record why, leaving the pipeline running.
async fn park_row(
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    pt_label: &str,
    transaction: &DbTransaction,
    bail: BailReason,
) {
    metrics::OPERATOR_TRANSACTION_QUARANTINED
        .with_label_values(&[pt_label, bail.label])
        .inc();
    quarantine_single(storage_tx, transaction, bail.message).await;
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

/// CAS one owned, unsent withdrawal `Processing -> Pending` so the fetcher can
/// re-claim it. Safe because a transient error before any send proves nothing
/// was broadcast and no signature was recorded. Mirrors the sender's
/// pre-broadcast requeue: only `Requeued` flips the row. `AtCap` (recovery cap
/// reached), `NotProcessing` (row already advanced) and `Err` (write failed)
/// all leave the row `Processing` for recovery to reconcile.
async fn requeue_single_prebroadcast(
    storage: &Storage,
    pt_label: &str,
    transaction: &DbTransaction,
) {
    match storage
        .try_requeue_prebroadcast(transaction.id, MAX_RECOVERY_REQUEUE_ATTEMPTS)
        .await
    {
        Ok(RequeueOutcome::Requeued { attempts }) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt_label, "prebroadcast_requeued"])
                .inc();
            info!(
                txn_id = transaction.id,
                trace_id = %transaction.trace_id,
                attempts,
                "Requeued withdrawal to Pending after pre-broadcast transient error"
            );
        }
        Ok(RequeueOutcome::AtCap) => {
            metrics::OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[pt_label, "prebroadcast_requeue_cap"])
                .inc();
            warn!(
                txn_id = transaction.id,
                trace_id = %transaction.trace_id,
                "Pre-broadcast requeue skipped: recovery cap reached, row left Processing"
            );
        }
        Ok(RequeueOutcome::NotProcessing) => warn!(
            txn_id = transaction.id,
            trace_id = %transaction.trace_id,
            "Pre-broadcast requeue skipped: row no longer Processing"
        ),
        Err(e) => warn!(
            txn_id = transaction.id,
            trace_id = %transaction.trace_id,
            "Pre-broadcast requeue failed, row left Processing for recovery: {e}"
        ),
    }
}

/// Rescue the head withdrawal after a transient error that occurred before its
/// own rotation was dispatched, so nothing reached the sender. Requeue it
/// `Processing -> Pending` for the fetcher to re-claim, instead of dropping it
/// stranded on `return Err`. No DB sweep: a blanket flip could hit an in-flight
/// sender row whose signature is not yet persisted; only this owned row and the
/// channel-buffered rows are provably safe.
///
/// Capped on the durable requeue counter (carried on the fetched row, so no
/// extra read): once it has already been requeued `MAX_RECOVERY_REQUEUE_ATTEMPTS`
/// times without building, it is quarantined to ManualReview instead of
/// requeued, so a deterministic error misclassified as transient cannot loop the
/// operator in a restart storm. This mirrors the sender's pre-broadcast requeue
/// cap; the nonce frontier then holds later withdrawals behind the quarantined
/// row until an operator resolves it.
async fn requeue_or_quarantine_head(
    storage: &Storage,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    pt_label: &str,
    transaction: &DbTransaction,
    reason: String,
) {
    if transaction.recovery_requeue_attempts >= MAX_RECOVERY_REQUEUE_ATTEMPTS {
        metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[pt_label, "prebroadcast_requeue_cap"])
            .inc();
        warn!(
            txn_id = transaction.id,
            trace_id = %transaction.trace_id,
            attempts = transaction.recovery_requeue_attempts,
            "Withdrawal build failed after max pre-broadcast requeues; quarantining"
        );
        quarantine_single(storage_tx, transaction, reason).await;
    } else {
        requeue_single_prebroadcast(storage, pt_label, transaction).await;
    }
}

/// Drain rows still buffered in `fetcher_rx` and requeue each `Processing ->
/// Pending`. They were flipped to `Processing` by the fetcher but never handed
/// on, and no rotation was dispatched for them, so requeuing is always safe -
/// even after a boundary rotation on the head, since their post-boundary nonces
/// cannot re-fire it. Draining them prevents a stranded higher nonce from
/// wedging the tree frontier. Mirrors the drain in `halt_withdrawal_pipeline`
/// but requeues rather than quarantines.
async fn drain_and_requeue_buffered(
    storage: &Storage,
    fetcher_rx: &mut mpsc::Receiver<DbTransaction>,
    pt_label: &str,
) {
    while let Ok(buffered) = fetcher_rx.try_recv() {
        requeue_single_prebroadcast(storage, pt_label, &buffered).await;
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
    rpc_client: Arc<RpcClientWithRetry>,
    fallback_rpc_client: Option<Arc<RpcClientWithRetry>>,
    source_rpc_client: Option<Arc<RpcClientWithRetry>>,
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
            let mut processor_state =
                ProcessorState::new_with_storage(storage.clone(), mint_rpc_client);

            // rpc_client is the channel chain for an escrow operator: the chain
            // its deposit mints broadcast to, which is what the reopened-row
            // gate must classify persisted signatures against.
            if let Err(e) = process_deposit_funds(
                &mut processor_state,
                fetcher_rx,
                sender_tx,
                storage_tx,
                storage,
                rpc_client,
                fallback_rpc_client,
                program_type,
            )
            .await
            {
                tracing::error!("Deposit funds error: {}", e);
            }
        }
    }
}

/// Reject a withdrawal the escrow would not accept a release for. The predicate is
/// the on-chain `AllowedMint` account, the same one `release_funds` requires, so a
/// row that fails here could never have landed and a row that passes is not blocked.
async fn check_withdrawal_mint_supported(
    processor_state: &mut ProcessorState,
    transaction: &DbTransaction,
) -> Result<Option<BailReason>, OperatorError> {
    let mint = Pubkey::from_str(&transaction.mint).map_err(|e| OperatorError::InvalidPubkey {
        pubkey: transaction.mint.clone(),
        reason: e.to_string(),
    })?;

    // A verdict already recorded for this mint stands for the process lifetime, so a
    // busy mint costs one allowlist read rather than one per withdrawal. An admin
    // blocking a mint mid-run is caught on the next restart.
    if processor_state.mint_cache.has_existence_floor(&mint) {
        return Ok(None);
    }

    let allowed_mint_pda = processor_state
        .release_funds_state
        .as_mut()
        .ok_or(OperatorError::MissingBuilder)?
        .get_allowed_mint_pda(&mint);

    let rpc = processor_state
        .mint_cache
        .rpc_client()
        .ok_or_else(|| OperatorError::RpcError("mint allowlist check requires RPC".to_string()))?;

    // A null only proves "never allowlisted" if the node has caught up. Anchor on the
    // tip it reports and require the read to answer at or past it, so a lagging backend
    // errors instead of denying an allowlist entry it simply has not seen yet.
    let commitment = rpc.rpc_client.commitment();
    let (ref_slot, _) = rpc
        .get_latest_blockhash_with_context(commitment)
        .await
        .map_err(|e| OperatorError::RpcError(format!("allowlist freshness anchor: {e}")))?;

    let response = rpc
        .get_account_with_context_min_slot(&allowed_mint_pda, commitment, Some(ref_slot))
        .await
        .map_err(|e| OperatorError::RpcError(format!("get_account({allowed_mint_pda}): {e}")))?;

    // Owned by anything else means the address collides with an unrelated account
    // rather than carrying the escrow's permission, which release would reject.
    let allowed = response
        .value
        .is_some_and(|account| account.owner == PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);
    if !allowed {
        return Ok(Some(BailReason::new(
            metrics::BAIL_REASON_UNSUPPORTED_MINT,
            format!("unsupported withdrawal mint: {mint} (no escrow allowlist account)"),
        )));
    }

    // Creating that account required the escrow to read the mint, so the mint existed
    // at or before this slot. Later mint reads bind to it, which is what lets a
    // missing mint be permanent instead of a node that has not caught up.
    processor_state
        .mint_cache
        .record_existence_floor(&mint, response.context.slot);
    Ok(None)
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
        // The generated client's defaults for these two accounts are stale (they
        // point at the previous escrow program and its event-authority PDA), so
        // set them explicitly from the configured program id. Without this the
        // release fails `verify_current_program` with IncorrectProgramId.
        .event_authority(release_funds_state.event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .transaction_nonce(nonce);

    let amount = transaction.amount.value();
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
        source_event_id: SourceEventId::from_row(transaction),
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
            // The post-lock token the sender proves ownership against.
            fetched_updated_at: transaction.updated_at,
        },
    )))
}

/// Token-2022 pre-flight for a withdrawal.
///
/// Returns:
/// - `Ok(None)` - clean: proceed to build + dispatch.
/// - `Ok(Some(bail))` - row-specific bail: caller routes to ManualReview
///   via `quarantine_single` and continues the loop. Used for paused mints,
///   permanent-delegate drains, and mints the target chain does not have,
///   where the row's data is fine but the on-chain state would cause an
///   immediate release-funds failure.
/// - `Err(_)` - transient infrastructure issue (RPC unreachable, malformed
///   mint data). Caller's classifier treats as Transient and restarts the
///   task, which is preferable to mass-quarantining rows during an RPC
///   blip.
async fn check_withdrawal_preflights(
    processor_state: &mut ProcessorState,
    transaction: &DbTransaction,
) -> Result<Option<BailReason>, OperatorError> {
    // The reads below only report a mint absent once the node has passed the slot that
    // allowlisted it, so the account was closed rather than merely not yet visible.
    // That is not fixed by retrying, so it parks the row instead of restarting us.
    match check_withdrawal_preflights_inner(processor_state, transaction).await {
        Err(OperatorError::Account(AccountError::TargetMintMissing { pubkey })) => {
            Ok(Some(BailReason::new(
                metrics::BAIL_REASON_TARGET_MINT_MISSING,
                format!("withdrawal mint absent on target chain: {pubkey}"),
            )))
        }
        other => other,
    }
}

/// The pre-flight checks themselves, wrapped above so one error shape can be
/// turned into a bail without repeating the conversion at each call that can
/// produce it.
async fn check_withdrawal_preflights_inner(
    processor_state: &mut ProcessorState,
    transaction: &DbTransaction,
) -> Result<Option<BailReason>, OperatorError> {
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
        return Ok(Some(BailReason::new(
            metrics::BAIL_REASON_MINT_PAUSED,
            format!("mint paused: {mint}"),
        )));
    }

    if has_permanent_delegate {
        let amount = transaction.amount.value();

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
            return Ok(Some(BailReason::new(
                metrics::BAIL_REASON_ESCROW_DRAINED,
                format!("insufficient escrow balance: on_chain={on_chain}, needed={amount}"),
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

        // Gates the transient rescue: once a boundary rotation is dispatched we
        // must not requeue, or the reprocess could fire a second rotation and
        // skip a tree generation. Read after the non-move async block completes.
        let mut rotation_dispatched = false;

        let outcome: Result<RowOutcome, OperatorError> = async {
            // Settle whether the escrow will accept a release for this mint before
            // building one, so a mint it would reject costs no further work.
            if let Some(bail) =
                check_withdrawal_mint_supported(processor_state, &transaction).await?
            {
                park_row(&storage_tx, pt_label, &transaction, bail).await;
                return Ok(RowOutcome::ParkedBeforeRotation);
            }

            // Build the withdrawal first so (a) rotation + withdrawal dispatch
            // are atomic from the sender's perspective, and (b) row-data
            // poison (e.g. NULL nonce, unparseable pubkey) surfaces here as
            // an `InvalidBuilder` for the classifier to halt the pipeline on.
            // Build also warms `MintCache.cache`, so the pre-flight below
            // doesn't pay an extra DB/RPC round-trip for `get_mint_metadata`.
            let release_funds_tx = build_release_funds(processor_state, &transaction).await?;

            // Rotate on a boundary nonce BEFORE the pre-flight, so a boundary row
            // that quarantines below still leaves later withdrawals on the new
            // tree. Skip if already rotated on-chain: a re-armed boundary row
            // reprocesses this nonce, and rotating twice skips a tree generation.
            if let Some(nonce_i64) = transaction.withdrawal_nonce {
                let nonce = nonce_i64 as u64;
                if nonce > 0 && nonce.is_multiple_of(MAX_TREE_LEAVES as u64) {
                    // Durable frontier guard: never rotate while a lower withdrawal
                    // is still active, or it gets stranded on the closed tree. The
                    // dequeue frontier normally keeps a boundary from arriving out of
                    // order; this catches paths that bypass it (e.g. a manual re-arm).
                    // Leave the row Processing for recovery rather than dispatch it.
                    if storage.has_active_withdrawal_below(nonce_i64).await? {
                        warn!(
                            nonce,
                            "Lower active withdrawal exists - deferring boundary rotation"
                        );
                        return Ok(RowOutcome::Continue);
                    }
                    let target_tree_index = nonce / MAX_TREE_LEAVES as u64;
                    let release_funds_state = processor_state
                        .release_funds_state
                        .as_ref()
                        .ok_or(OperatorError::MissingBuilder)?;
                    let instance_pda = release_funds_state.instance_pda;
                    let rpc_client = processor_state.mint_cache.rpc_client().ok_or_else(|| {
                        OperatorError::RpcError(
                            "tree index read requires an RPC client".to_string(),
                        )
                    })?;
                    let onchain_tree_index =
                        fetch_current_tree_index(rpc_client, &instance_pda).await?;
                    if onchain_tree_index < target_tree_index {
                        info!(
                            nonce,
                            target_tree_index,
                            "Tree rotation boundary detected, dispatching ResetSmtRoot"
                        );
                        // Record the owed generation before dispatching. The sender's
                        // arm is in-memory, so this row is what re-arms the rotation
                        // after a crash; persisting first means a lost dispatch is
                        // always recoverable, never the reverse.
                        storage
                            .set_owed_rotation_target(pt_label, target_tree_index)
                            .await?;
                        let rotation_tx = TransactionBuilder::ResetSmtRoot(Box::new(
                            ResetSmtRootBuilderWithTarget::new(
                                processor_state.admin_pubkey,
                                release_funds_state.operator_pubkey,
                                release_funds_state.instance_pda,
                                release_funds_state.operator_pda,
                                release_funds_state.event_authority_pda,
                                target_tree_index,
                            ),
                        ));
                        send_guaranteed(&sender_tx, rotation_tx, "reset smt root")
                            .await
                            .map_err(OperatorError::ChannelSend)?;
                        rotation_dispatched = true;
                    } else {
                        info!(
                            nonce,
                            target_tree_index,
                            onchain_tree_index,
                            "Boundary already rotated on-chain, skipping ResetSmtRoot"
                        );
                    }
                }
            }

            // Pre-flight for Token-2022 pause / permanent-delegate drain. These
            // are row-specific, so bails route to ManualReview and continue the
            // loop rather than halting the pipeline (reserved for poison rows);
            // the rotation above already fired, so later withdrawals proceed.
            // It is best-effort: a delegate can still drain between this read and
            // the on-chain CPI, leaving that to the sender retry path. RPC errors
            // bubble up as Transient and restart the task.
            if let Some(bail) = check_withdrawal_preflights(processor_state, &transaction).await? {
                park_row(&storage_tx, pt_label, &transaction, bail).await;
                return Ok(RowOutcome::Continue);
            }

            info!("Processing withdrawal");
            send_guaranteed(&sender_tx, release_funds_tx, "processed release funds")
                .await
                .map_err(OperatorError::ChannelSend)?;

            Ok(RowOutcome::Continue)
        }
        .instrument(span.clone())
        .await;

        // Parking before the boundary rotation leaves the tree on the old generation,
        // so buffered siblings go back to Pending rather than dispatch against an
        // index the chain rejects. The parked row blocks them until it is resolved.
        let err = match outcome {
            Ok(RowOutcome::Continue) => continue,
            Ok(RowOutcome::ParkedBeforeRotation) => {
                drain_and_requeue_buffered(&storage, &mut fetcher_rx, pt_label).await;
                continue;
            }
            Err(err) => err,
        };

        // A per-row error is classified.  For a deterministic poison-pill
        // we quarantine the row, halt the whole withdrawal pipeline, and
        // return so the supervisor can shut down cleanly.  Transient or
        // fatal errors bubble up directly.
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
                // The head row is safe to rescue only before its own rotation
                // dispatch; after that a reprocess could re-fire an unconfirmed
                // rotation, so leave it for recovery. Buffered siblings never
                // had a rotation dispatched for them, so drain and requeue them
                // either way rather than strand them Processing.
                if !rotation_dispatched {
                    requeue_or_quarantine_head(
                        &storage,
                        &storage_tx,
                        pt_label,
                        &transaction,
                        err.to_string(),
                    )
                    .await;
                }
                drain_and_requeue_buffered(&storage, &mut fetcher_rx, pt_label).await;
                // Surface the error so the supervisor can restart us cleanly.
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

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn process_deposit_funds(
    processor_state: &mut ProcessorState,
    mut fetcher_rx: mpsc::Receiver<DbTransaction>,
    sender_tx: mpsc::Sender<TransactionBuilder>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    storage: Arc<Storage>,
    channel_rpc: Arc<RpcClientWithRetry>,
    channel_fallback: Option<Arc<RpcClientWithRetry>>,
    program_type: ProgramType,
) -> Result<(), OperatorError> {
    let pt_label = program_type.as_label();

    // Classifies persisted write-ahead signatures on the channel
    let gate_finality = FinalityRpc::channel(&channel_rpc, channel_fallback.as_deref());

    while let Some(transaction) = fetcher_rx.recv().await {
        let span = info_span!("process", trace_id = %transaction.trace_id, txn_id = transaction.id);

        let outcome: Result<(), OperatorError> = async {
            // Idempotency gate for reopened rows. Read the write-ahead journal
            // (retrying a transient DB blip); an exhausted read propagates as a
            // transient error, and a first-time row (no signatures) falls
            // straight through with no RPC. Each outcome is handled below.
            let stored = with_storage_backoff("journal read", transaction.id, || {
                storage.get_release_signatures(transaction.id)
            })
            .await?;
            match classify_deposit_signatures(&stored, &gate_finality).await {
                DepositOutcome::NotLanded => {}
                DepositOutcome::Landed { signature } => {
                    // CAS on the fetch-time token; a miss means another writer
                    // took the row, and either way nothing is minted. A transient
                    // write error is retried; the mint already landed, so an
                    // exhausted write leaves the row Processing for the recovery
                    // sweep to complete rather than exiting the task.
                    let completed = with_storage_backoff(
                        "reopened-deposit complete",
                        transaction.id,
                        || {
                            storage.try_complete_processing(
                                transaction.id,
                                transaction.updated_at,
                                Some(signature.clone()),
                                // Deposit completion carries no release-attempt list.
                                None,
                            )
                        },
                    )
                    .await;
                    match completed {
                        Ok(true) => {
                            info!(
                                signature,
                                "Reopened deposit's prior mint landed; completed without re-mint"
                            );
                            metrics::OPERATOR_REOPENED_DEPOSIT_GATE
                                .with_label_values(&[pt_label, "completed"])
                                .inc();
                        }
                        // Row moved under us; another writer owns it now. The
                        // gate still detected a landed mint, so signal it.
                        Ok(false) => {
                            debug!(
                                "reopened-deposit complete skipped; another writer touched the row"
                            );
                            metrics::OPERATOR_REOPENED_DEPOSIT_GATE
                                .with_label_values(&[pt_label, "complete_raced"])
                                .inc();
                        }
                        // Retries exhausted; the row stays Processing for
                        // recovery. Counted so the failed record is observable.
                        Err(e) => {
                            warn!(
                                "reopened-deposit complete write error after retries; leaving for recovery: {}",
                                e
                            );
                            metrics::OPERATOR_REOPENED_DEPOSIT_GATE
                                .with_label_values(&[pt_label, "complete_write_failed"])
                                .inc();
                        }
                    }
                    return Ok(());
                }
                DepositOutcome::Live { reason } => {
                    // Still in flight: leave the row Processing; the recovery
                    // sweep re-examines it after the stale threshold.
                    info!(
                        reason = %reason,
                        "Reopened deposit's prior mint may still land; deferring to recovery"
                    );
                    metrics::OPERATOR_REOPENED_DEPOSIT_GATE
                        .with_label_values(&[pt_label, "deferred_live"])
                        .inc();
                    return Ok(());
                }
                DepositOutcome::Ambiguous { reason } => {
                    // Uncertain (transient channel RPC, or a corrupt stored
                    // signature). Do not mint and do not quarantine here: leaving
                    // the row Processing hands it to the recovery sweep, which
                    // re-checks on the same chain and self-heals a transient
                    // outage instead of dead-ending a healthy row in ManualReview.
                    warn!(
                        reason = %reason,
                        "Reopened deposit's prior mint unverifiable; deferring to recovery"
                    );
                    metrics::OPERATOR_REOPENED_DEPOSIT_GATE
                        .with_label_values(&[pt_label, "deferred_unverifiable"])
                        .inc();
                    return Ok(());
                }
            }

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

            // Refuse to mint when the mint was not in `allowed` status at
            // the deposit's slot, per `mint_status_history`. If we minted
            // anyway, two things would break:
            //   1. We'd issue PrivateChannel tokens with no Mainnet escrow
            //      backing them.
            //   2. Reconciliation wouldn't catch it: the balance check
            //      only scans mints listed in `mints`, so the mismatch
            //      never fires.
            processor_state
                .mint_cache
                .assert_mint_allowed_at_slot(&mint, transaction.slot, transaction.id)
                .await?;

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
                .amount(transaction.amount.value())
                .idempotency_memo(mint_idempotency_memo(&SourceEventId::from_row(
                    &transaction,
                )));

            let proc_elapsed_ms = proc_t0.elapsed().as_millis();
            info!(proc_elapsed_ms, "Processing deposit");

            let wrapped = TransactionBuilder::Mint(Box::new(MintToBuilderWithTxnId {
                builder,
                txn_id: transaction.id,
                trace_id: transaction.trace_id.clone(),
                // The post-lock token the sender proves ownership against.
                fetched_updated_at: transaction.updated_at,
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
    use crate::operator::rpc_util::RpcClientWithRetry;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::DbMint;
    use crate::storage::common::models::TransactionType;
    use crate::storage::common::storage::mock::MockStorage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;
    use solana_client::rpc_request::RpcRequest;

    /// Channel RPC client with a single fast attempt against `url`.
    fn channel_client(url: &str) -> Arc<RpcClientWithRetry> {
        Arc::new(RpcClientWithRetry::with_retry_config(
            url.to_string(),
            crate::operator::utils::rpc_util::RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ))
    }

    /// Channel RPC that refuses every connection: rows with no persisted
    /// signatures must pass the reopened-row gate without any RPC call.
    fn unreachable_channel_rpc() -> Arc<RpcClientWithRetry> {
        channel_client("http://localhost:1")
    }

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

    /// Insert a `mints` row with the given token program and extension flags,
    /// plus a slot-0 `allowed` status history entry so
    /// `assert_mint_allowed_at_slot` accepts the mint at any slot.
    fn insert_mint_row_with(
        storage: &Arc<Storage>,
        mint: &Pubkey,
        token_program: &Pubkey,
        flags: Option<(bool, bool)>,
    ) {
        let mock_storage = match storage.as_ref() {
            Storage::Mock(m) => m,
            _ => unreachable!("test helper expects Storage::Mock"),
        };
        let (is_pausable, has_permanent_delegate) = match flags {
            Some((p, d)) => (Some(p), Some(d)),
            None => (None, None),
        };
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: token_program.to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable,
                has_permanent_delegate,
            },
        );
        seed_mint_status(storage, mint, "allowed", 0);
    }

    /// Append a `mint_status_history` transition for a mint.
    fn seed_mint_status(storage: &Arc<Storage>, mint: &Pubkey, status: &str, slot: i64) {
        let mock_storage = match storage.as_ref() {
            Storage::Mock(m) => m,
            _ => unreachable!("test helper expects Storage::Mock"),
        };
        mock_storage.mint_status_history.lock().unwrap().push(
            crate::storage::common::models::DbMintStatus {
                mint_address: mint.to_string(),
                status: status.to_string(),
                effective_slot: slot,
                signature: format!("test-seed-{mint}-{status}-{slot}"),
                created_at: chrono::Utc::now(),
            },
        );
    }

    /// Insert a minimal legacy-SPL `mints` row with both extension flags resolved.
    fn insert_mint_row(storage: &Arc<Storage>, mint: &Pubkey) {
        insert_mint_row_with(storage, mint, &spl_token::id(), Some((false, false)));
    }

    /// Drive one withdrawal through `process_release_funds` and return whatever
    /// reached the storage writer and the sender. The loop's own result comes back
    /// too, since exiting with an error is what restarts the operator.
    async fn run_one_withdrawal(
        ps: &mut ProcessorState,
        storage: Arc<Storage>,
        txn: DbTransaction,
    ) -> (
        Result<(), OperatorError>,
        Option<TransactionStatusUpdate>,
        Option<TransactionBuilder>,
    ) {
        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let outcome = process_release_funds(
            ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await;

        (
            outcome,
            storage_rx.try_recv().ok(),
            sender_rx.try_recv().ok(),
        )
    }

    /// Mocked `getAccountInfo` response for an Instance account carrying the
    /// given on-chain tree index, used to drive the boundary-rotation read.
    fn instance_account_response(current_tree_index: u64) -> serde_json::Value {
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: [0u8; 32],
            current_tree_index,
        };
        let mut bytes = Vec::new();
        instance.serialize(&mut bytes).unwrap();
        serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        })
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
            amount: TokenAmount(1000),
            memo: None,
            transaction_type: txn_type,
            withdrawal_nonce: nonce,
            status: TransactionStatus::Processing,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
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
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);
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
                    status: "allowed".to_string(),
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

        // On-chain tree index 0 < boundary target 1, so the rotation must fire.
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, instance_account_response(0));
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };

        let mint_pubkey = Pubkey::new_unique();
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);
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
                    status: "allowed".to_string(),
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
            storage.clone(),
            ProgramType::Withdraw,
        )
        .await;
        assert!(result.is_ok());

        // First message must be ResetSmtRoot — rotation happens before the boundary withdrawal
        let msg1 = sender_rx.recv().await.unwrap();
        let TransactionBuilder::ResetSmtRoot(rotation) = msg1 else {
            panic!("expected ResetSmtRoot first, got a different variant");
        };
        // The target travels with the builder: it is the sender's only reference for
        // re-checking against chain, and it cannot be re-derived there.
        assert_eq!(rotation.target_tree_index, 1);

        // And it is durable before the dispatch, so a crash before the reset confirms
        // re-arms the rotation at boot instead of dropping it.
        let Storage::Mock(mock_storage) = storage.as_ref() else {
            unreachable!("mock storage")
        };
        assert_eq!(
            mock_storage
                .owed_rotation_targets
                .lock()
                .unwrap()
                .get("withdraw")
                .copied(),
            Some(1)
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

    /// The durable target is written before the dispatch, so if that write fails the
    /// rotation must not be sent at all: a reset in flight with no stored target is
    /// exactly the state a crash could drop.
    #[tokio::test]
    async fn process_release_funds_boundary_skips_dispatch_when_target_persist_fails() {
        let mock = MockStorage::new();
        mock.set_should_fail("set_owed_rotation_target", true);
        let storage = Arc::new(Storage::Mock(mock));

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, instance_account_response(0));
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };

        let mint_pubkey = Pubkey::new_unique();
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);
        let recipient = Pubkey::new_unique();
        {
            let Storage::Mock(mock_storage) = storage.as_ref() else {
                unreachable!("mock storage")
            };
            mock_storage.mints.lock().unwrap().insert(
                mint_pubkey.to_string(),
                DbMint {
                    mint_address: mint_pubkey.to_string(),
                    decimals: 6,
                    token_program: spl_token::id().to_string(),
                    created_at: chrono::Utc::now(),
                    status: "allowed".to_string(),
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

        assert!(
            result.is_err(),
            "a failed target write must surface, not proceed silently"
        );
        assert!(
            sender_rx.try_recv().is_err(),
            "neither the rotation nor the boundary withdrawal may be dispatched"
        );
    }

    /// A boundary nonce must not rotate or dispatch while a lower withdrawal is
    /// still active in the DB. Nothing is sent; the boundary row is left
    /// Processing for recovery.
    #[tokio::test]
    async fn process_release_funds_boundary_defers_when_lower_active() {
        let mock = MockStorage::new();

        // Allow the mint so build_release_funds succeeds before the guard runs.
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        mock.mints.lock().unwrap().insert(
            mint_pubkey.to_string(),
            DbMint {
                mint_address: mint_pubkey.to_string(),
                decimals: 6,
                token_program: spl_token::id().to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(false),
            },
        );

        // Seed a lower-nonce withdrawal stuck off the sender path (Parked, nonce 1
        // < boundary). This is what the guard must see and refuse to rotate past.
        // (Processing rows are excluded - the sender's in-flight guard covers those.)
        let mut lower = make_db_transaction(
            2,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(1),
            TransactionType::Withdrawal,
        );
        lower.status = TransactionStatus::Parked;
        mock.pending_transactions.lock().unwrap().push(lower);

        // Boundary nonce → target tree 1; on-chain index 0 means a rotation WOULD
        // fire here (0 < 1) absent the guard — so an empty sender proves the guard.
        let storage = Arc::new(Storage::Mock(mock));
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, instance_account_response(0));
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        // Feed the boundary withdrawal (nonce == MAX_TREE_LEAVES, first of next tree).
        let boundary = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(MAX_TREE_LEAVES as i64),
            TransactionType::Withdrawal,
        );
        fetcher_tx.send(boundary).await.unwrap();
        drop(fetcher_tx); // close the channel so the processor loop exits

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

        // Guard fired: neither the ResetSmtRoot nor the boundary ReleaseFunds
        // was dispatched, because lower nonce 1 is still active.
        assert!(
            sender_rx.try_recv().is_err(),
            "boundary must be deferred while a lower nonce is active"
        );
    }

    /// Regression for the boundary-quarantine wedge: a boundary nonce whose
    /// pre-flight bails (here, escrow underfunded) must STILL rotate the tree
    /// first. Otherwise the rotation is skipped and every later-tree withdrawal
    /// wedges on a tree-index mismatch. The boundary row itself is quarantined.
    #[tokio::test]
    async fn process_release_funds_boundary_quarantine_still_rotates() {
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mock = MockStorage::new();
        mock.mints.lock().unwrap().insert(
            mint_pubkey.to_string(),
            DbMint {
                mint_address: mint_pubkey.to_string(),
                decimals: 6,
                token_program: spl_token_2022::id().to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(true),
            },
        );
        let storage = Arc::new(Storage::Mock(mock));

        // Instance on tree 0 (rotation needed); escrow balance 500 < amount 1000.
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, instance_account_response(0));
        mocks.insert(
            RpcRequest::GetTokenAccountBalance,
            serde_json::json!({
                "context": {"slot": 1},
                "value": {"amount": "500", "decimals": 6, "uiAmount": 0.0005, "uiAmountString": "0.0005"}
            }),
        );
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(MAX_TREE_LEAVES as i64),
            TransactionType::Withdrawal,
        );
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await
        .unwrap();

        // Rotation fired despite the bail...
        let msg = sender_rx.recv().await.unwrap();
        assert!(
            matches!(msg, TransactionBuilder::ResetSmtRoot(_)),
            "expected ResetSmtRoot to be dispatched before the pre-flight bail"
        );
        // ...and no ReleaseFunds for the quarantined boundary row.
        assert!(
            sender_rx.try_recv().is_err(),
            "boundary row must not be released, only the rotation is sent"
        );

        // The boundary row is quarantined to ManualReview.
        let update = storage_rx.try_recv().expect("ManualReview update expected");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(update
            .error_message
            .expect("error_message must be set")
            .contains("insufficient escrow balance"));
    }

    /// A re-armed boundary row (manual-review recovery) reprocesses the same
    /// nonce. If the tree was already rotated on-chain, the rotation must be
    /// skipped, otherwise it advances the tree a second time and skips a whole
    /// generation. The withdrawal itself still proceeds.
    #[tokio::test]
    async fn process_release_funds_boundary_already_rotated_skips_reset() {
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        insert_mint_row(&storage, &mint_pubkey);

        // Instance already on tree 1 == boundary target, so no rotation is owed.
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, instance_account_response(1));
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(MAX_TREE_LEAVES as i64),
            TransactionType::Withdrawal,
        );
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_release_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage,
            ProgramType::Withdraw,
        )
        .await
        .unwrap();

        // The only message is the withdrawal — no redundant ResetSmtRoot.
        let msg = sender_rx.recv().await.unwrap();
        let TransactionBuilder::ReleaseFunds(b) = msg else {
            panic!("expected ReleaseFunds, got ResetSmtRoot or another variant");
        };
        assert_eq!(b.nonce, MAX_TREE_LEAVES as u64);
        assert!(sender_rx.try_recv().is_err(), "no second message expected");
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
        insert_mint_row(&storage, &mint_pubkey);

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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
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
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);
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
                    status: "allowed".to_string(),
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

        // The gate is not this test's subject; treat the mint as already proved.
        let mint_pubkey = Pubkey::new_unique();
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
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

    // ── transient pre-broadcast requeue ─────────────────────────────────

    /// Seed a Processing withdrawal into the mock's pending set, shaping its mint
    /// row so it either proceeds, hits a transient, or is unsupported.
    /// How the mint backing a seeded withdrawal is represented in storage.
    #[derive(Clone, Copy)]
    enum SeededMint {
        /// Allowlisted with its extension flags already resolved, so the
        /// withdrawal runs to completion without needing an RPC.
        Resolved,
        /// Allowlisted but with unresolved extension flags, so the pre-flight reaches
        /// for an RPC client the test does not configure. Manufactures a genuine
        /// infrastructure failure rather than a verdict about the row.
        NeedsRpc,
        /// Never allowlisted.
        Absent,
    }

    fn seed_processing_withdrawal(
        mock: &MockStorage,
        id: i64,
        nonce: i64,
        mint: &Pubkey,
        recipient: &Pubkey,
        seeded_mint: SeededMint,
    ) -> DbTransaction {
        let row = match seeded_mint {
            SeededMint::Resolved => Some((spl_token::id(), Some(false), Some(false))),
            SeededMint::NeedsRpc => Some((spl_token_2022::id(), None, None)),
            SeededMint::Absent => None,
        };
        if let Some((token_program, is_pausable, has_permanent_delegate)) = row {
            mock.mints.lock().unwrap().insert(
                mint.to_string(),
                DbMint {
                    mint_address: mint.to_string(),
                    decimals: 6,
                    token_program: token_program.to_string(),
                    created_at: chrono::Utc::now(),
                    status: "allowed".to_string(),
                    is_pausable,
                    has_permanent_delegate,
                },
            );
        }
        let txn = make_db_transaction(
            id,
            &mint.to_string(),
            &recipient.to_string(),
            Some(nonce),
            TransactionType::Withdrawal,
        );
        mock.pending_transactions.lock().unwrap().push(txn.clone());
        txn
    }

    /// Core fix: a pre-flight transient (extension flags unresolved, no RPC)
    /// requeues the current row Processing -> Pending instead of stranding it, and
    /// does not quarantine it. The error still bubbles so the supervisor restarts.
    #[tokio::test]
    async fn process_release_funds_transient_requeues_row_to_pending() {
        let mock = MockStorage::new();
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let txn =
            seed_processing_withdrawal(&mock, 1, 5, &mint_pubkey, &recipient, SeededMint::NeedsRpc);
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

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
        assert!(
            matches!(result, Err(OperatorError::RpcError(_))),
            "expected a transient RpcError, got: {result:?}"
        );

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Pending,
            "row must be requeued to Pending"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 1,
            "requeue must bump the counter once"
        );
        drop(after);

        assert!(
            sender_rx.try_recv().is_err(),
            "nothing was handed to the sender"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "row is rescued, not quarantined"
        );
    }

    /// Durable cap: a row that has already been requeued the maximum number of
    /// times is quarantined to ManualReview instead of requeued again, so a
    /// deterministic error misclassified as transient cannot loop forever.
    #[tokio::test]
    async fn process_release_funds_transient_requeue_cap_quarantines() {
        let mock = MockStorage::new();
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let mut txn =
            seed_processing_withdrawal(&mock, 1, 5, &mint_pubkey, &recipient, SeededMint::NeedsRpc);
        // Fetched row already at the cap: the next transient must quarantine.
        txn.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

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
        assert!(
            matches!(result, Err(OperatorError::RpcError(_))),
            "got: {result:?}"
        );

        // A ManualReview update was emitted; the row was not requeued.
        let update = storage_rx.try_recv().expect("expected a quarantine update");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(update.transaction_id, 1);
        assert!(
            sender_rx.try_recv().is_err(),
            "nothing was handed to the sender"
        );
        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "quarantine rides the writer channel, not a direct requeue"
        );
    }

    /// A transient on the head row also drains and requeues every row still
    /// buffered behind it in the fetcher channel, so a stranded higher nonce
    /// cannot wedge the tree frontier.
    #[tokio::test]
    async fn process_release_funds_transient_drains_and_requeues_buffered_rows() {
        let mock = MockStorage::new();
        let bad_mint = Pubkey::new_unique();
        let good_mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        // Row 1 triggers the transient; 2 and 3 are valid but buffered behind it.
        let t1 =
            seed_processing_withdrawal(&mock, 1, 5, &bad_mint, &recipient, SeededMint::NeedsRpc);
        let t2 =
            seed_processing_withdrawal(&mock, 2, 6, &good_mint, &recipient, SeededMint::Resolved);
        let t3 =
            seed_processing_withdrawal(&mock, 3, 7, &good_mint, &recipient, SeededMint::Resolved);
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        fetcher_tx.send(t1).await.unwrap();
        fetcher_tx.send(t2).await.unwrap();
        fetcher_tx.send(t3).await.unwrap();
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
        assert!(result.is_err());

        let after = mock.pending_transactions.lock().unwrap();
        for id in [1, 2, 3] {
            let row = after.iter().find(|t| t.id == id).unwrap();
            assert_eq!(
                row.status,
                TransactionStatus::Pending,
                "row {id} must be requeued"
            );
            assert_eq!(
                row.recovery_requeue_attempts, 1,
                "row {id} counter bumped once"
            );
        }
    }

    /// Fallback path: if the requeue CAS write itself fails, the row is left
    /// Processing for the recovery sweep to reconcile (no counter bump). This
    /// rescue is best-effort by design, since it writes through the database that
    /// just failed; the durable rescue is the sweep, which proves the nonce never
    /// released and re-arms the row on its next pass.
    #[tokio::test]
    async fn process_release_funds_transient_requeue_write_failure_left_for_recovery() {
        let mock = MockStorage::new();
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let txn =
            seed_processing_withdrawal(&mock, 1, 5, &mint_pubkey, &recipient, SeededMint::NeedsRpc);
        mock.set_should_fail("try_requeue_prebroadcast", true);
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

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
        assert!(result.is_err());

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "requeue write failed -> left for recovery"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 0,
            "no counter bump on a failed requeue"
        );
    }

    /// Once a boundary rotation is dispatched, a later transient (here an
    /// unreadable escrow balance) must NOT requeue the head, or the reprocess could
    /// re-fire the rotation; it is left Processing for recovery. Buffered siblings
    /// carry post-boundary nonces that cannot re-fire the rotation, so they are
    /// still drained and requeued rather than stranded. The ResetSmtRoot goes out.
    #[tokio::test]
    async fn process_release_funds_boundary_after_rotation_drains_siblings_not_head() {
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let mock = MockStorage::new();
        let storage_for_seed = Arc::new(Storage::Mock(mock.clone()));
        // A permanent-delegate mint sends the preflight to read the escrow balance and
        // the mocked reply carries an unparseable amount. That is a read failure, not a
        // verdict about the mint, so it stays transient and surfaces after the rotation.
        insert_mint_row_with(
            &storage_for_seed,
            &mint_pubkey,
            &spl_token_2022::id(),
            Some((false, true)),
        );
        let boundary = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(MAX_TREE_LEAVES as i64),
            TransactionType::Withdrawal,
        );
        // A sibling buffered behind the boundary head (higher, post-boundary nonce).
        let sibling = make_db_transaction(
            2,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(MAX_TREE_LEAVES as i64 + 1),
            TransactionType::Withdrawal,
        );
        {
            let mut rows = mock.pending_transactions.lock().unwrap();
            rows.push(boundary.clone());
            rows.push(sibling.clone());
        }
        let storage = Arc::new(Storage::Mock(mock.clone()));

        // On-chain tree index 0 < target 1, so the rotation fires first.
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, instance_account_response(0));
        mocks.insert(
            RpcRequest::GetTokenAccountBalance,
            serde_json::json!({
                "context": {"slot": 1},
                "value": {
                    "amount": "not-a-number",
                    "decimals": 6,
                    "uiAmount": 0.0,
                    "uiAmountString": "0"
                }
            }),
        );
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
        };
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        fetcher_tx.send(boundary).await.unwrap();
        fetcher_tx.send(sibling).await.unwrap();
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
        assert!(
            result.is_err(),
            "preflight blip after rotation must surface as an error"
        );

        let msg = sender_rx.recv().await.unwrap();
        assert!(
            matches!(msg, TransactionBuilder::ResetSmtRoot(_)),
            "the rotation must have been dispatched before the transient"
        );

        let after = mock.pending_transactions.lock().unwrap();
        let head = after.iter().find(|t| t.id == 1).unwrap();
        assert_eq!(
            head.status,
            TransactionStatus::Processing,
            "post-rotation head must not be requeued"
        );
        assert_eq!(head.recovery_requeue_attempts, 0);
        let sib = after.iter().find(|t| t.id == 2).unwrap();
        assert_eq!(
            sib.status,
            TransactionStatus::Pending,
            "buffered sibling must be drained and requeued, not stranded"
        );
        assert_eq!(sib.recovery_requeue_attempts, 1);
    }

    /// Integration: transient-requeue a row to Pending, then heal the transient
    /// (resolve the mint's extension flags) and feed the same row back Processing;
    /// it must now build and reach the sender. Proves the rescued row is
    /// genuinely re-fetchable.
    #[tokio::test]
    async fn transient_then_recovery_end_to_end() {
        let mock = MockStorage::new();
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let txn =
            seed_processing_withdrawal(&mock, 1, 5, &mint_pubkey, &recipient, SeededMint::NeedsRpc);
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

        // Pass 1: the pre-flight cannot reach an RPC, so the row is requeued to Pending.
        let (ftx1, frx1) = mpsc::channel::<DbTransaction>(4);
        let (stx1, _srx1) = mpsc::channel(10);
        let (gtx1, _grx1) = mpsc::channel(10);
        ftx1.send(txn).await.unwrap();
        drop(ftx1);
        let r1 = process_release_funds(
            &mut ps,
            frx1,
            stx1,
            gtx1,
            storage.clone(),
            ProgramType::Withdraw,
        )
        .await;
        assert!(r1.is_err());

        let requeued = {
            let after = mock.pending_transactions.lock().unwrap();
            assert_eq!(after[0].status, TransactionStatus::Pending);
            after[0].clone()
        };

        // Heal the transient: the extension flags are now resolved in the DB, so
        // the pre-flight no longer needs an RPC.
        mock.mints.lock().unwrap().insert(
            mint_pubkey.to_string(),
            DbMint {
                mint_address: mint_pubkey.to_string(),
                decimals: 6,
                token_program: spl_token_2022::id().to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(false),
            },
        );
        ps.mint_cache.clear();

        // Pass 2: the fetcher re-locks the row (Processing); it must now be sent.
        let mut relocked = requeued;
        relocked.status = TransactionStatus::Processing;
        let (ftx2, frx2) = mpsc::channel::<DbTransaction>(4);
        let (stx2, mut srx2) = mpsc::channel(10);
        let (gtx2, _grx2) = mpsc::channel(10);
        ftx2.send(relocked).await.unwrap();
        drop(ftx2);
        let r2 = process_release_funds(
            &mut ps,
            frx2,
            stx2,
            gtx2,
            storage.clone(),
            ProgramType::Withdraw,
        )
        .await;
        assert!(r2.is_ok(), "healed row must process cleanly: {r2:?}");

        let msg = srx2.recv().await.unwrap();
        let TransactionBuilder::ReleaseFunds(b) = msg else {
            panic!("expected ReleaseFunds after the transient healed");
        };
        assert_eq!(b.nonce, 5);
        assert_eq!(b.transaction_id, 1);
    }

    /// Phase 2: a single transient DB blip on the metadata read is absorbed by
    /// the read backoff, so the row is built and sent normally and never
    /// requeued.
    #[tokio::test]
    async fn process_release_funds_single_db_blip_does_not_strand() {
        let mock = MockStorage::new();
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let txn =
            seed_processing_withdrawal(&mock, 1, 5, &mint_pubkey, &recipient, SeededMint::Resolved);
        // One transient blip on get_mint; the backoff rides it out.
        mock.set_fail_times("get_mint", 1);
        let storage = Arc::new(Storage::Mock(mock.clone()));

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

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
        assert!(
            result.is_ok(),
            "backoff must absorb the single blip: {result:?}"
        );

        let msg = sender_rx.recv().await.unwrap();
        let TransactionBuilder::ReleaseFunds(b) = msg else {
            panic!("expected ReleaseFunds, blip should have been absorbed");
        };
        assert_eq!(b.nonce, 5);

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Processing,
            "row must not be requeued"
        );
        assert_eq!(after[0].recovery_requeue_attempts, 0);
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

        let mint_not_allowed = OperatorError::MintNotAllowed {
            transaction_id: 1,
            mint: "mint_a".into(),
        };
        assert!(matches!(
            classify_processor_error(&mint_not_allowed),
            ErrorDisposition::Quarantine("mint_not_allowed")
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
                    status: "allowed".to_string(),
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
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

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
                    status: "allowed".to_string(),
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
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(16);
        let (storage_tx, _storage_rx) = mpsc::channel(16);

        for id in 1..=3 {
            let mint = Pubkey::new_unique();
            insert_mint_row(&storage, &mint);
            fetcher_tx
                .send(make_db_transaction(
                    id,
                    &mint.to_string(),
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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
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
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(16);
        let (storage_tx, mut storage_rx) = mpsc::channel(16);

        let valid_mint_2 = Pubkey::new_unique();
        let valid_mint_4 = Pubkey::new_unique();
        insert_mint_row(&storage, &valid_mint_2);
        insert_mint_row(&storage, &valid_mint_4);

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
                &valid_mint_2.to_string(),
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
                &valid_mint_4.to_string(),
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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
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
                status: "allowed".to_string(),
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
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

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
            amount: TokenAmount(1000), // > on-chain balance of 500
            memo: None,
            transaction_type: crate::storage::common::models::TransactionType::Withdrawal,
            withdrawal_nonce: Some(5),
            status: crate::storage::common::models::TransactionStatus::Processing,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
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
                status: "allowed".to_string(),
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
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint_pubkey, 1);

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
            amount: TokenAmount(1000), // < on-chain balance of 5000
            memo: None,
            transaction_type: crate::storage::common::models::TransactionType::Withdrawal,
            withdrawal_nonce: Some(5),
            status: crate::storage::common::models::TransactionStatus::Processing,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
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

    /// A deposit whose mint has no `mints` row is quarantined for manual
    /// review, the quarantine reason mentions the allow-list, and no
    /// `Mint` builder is forwarded to the sender.
    #[tokio::test]
    async fn process_deposit_funds_quarantines_when_mint_not_in_db() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        // Mint pubkey is valid base58 but is NOT inserted into `mints`.
        let mint_pubkey = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            99,
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
            storage.clone(),
            unreachable_channel_rpc(),
            None,
            ProgramType::Escrow,
        )
        .await;
        assert!(
            result.is_ok(),
            "quarantine must not propagate as an error, got: {:?}",
            result
        );

        let update = storage_rx
            .try_recv()
            .expect("a quarantine update must be sent");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(update.transaction_id, 99);
        let reason = update
            .error_message
            .as_deref()
            .expect("quarantine update must include a reason");
        assert!(
            reason.contains("mint_status_history"),
            "expected reason to mention the mint status history gate, got: {reason}",
        );

        assert!(
            sender_rx.try_recv().is_err(),
            "no Mint builder should be forwarded for an unknown mint",
        );
    }

    // ── reopened-deposit gate (pre-broadcast idempotency) ───────────────

    fn mock_status_reply(server: &mut mockito::ServerGuard, body: &str) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(body)
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

    const FINALIZED_STATUS_BODY: &str = r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#;
    const NULL_STATUS_BODY: &str =
        r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#;

    /// Seed one Processing deposit row with a persisted write-ahead signature
    /// and return the txn to feed the processor (same updated_at as the store).
    async fn seed_reopened_deposit(
        mock: &MockStorage,
        mint: &Pubkey,
        sig: &str,
        lvbh: i64,
    ) -> DbTransaction {
        let txn = make_db_transaction(
            1,
            &mint.to_string(),
            &Pubkey::new_unique().to_string(),
            None,
            TransactionType::Deposit,
        );
        mock.pending_transactions.lock().unwrap().push(txn.clone());
        mock.insert_release_signature(txn.id, sig.to_string(), lvbh, Some(0))
            .await
            .unwrap();
        txn
    }

    /// A reopened deposit whose persisted mint signature finalized on the
    /// channel is completed in place; no second mint may reach the sender.
    #[tokio::test]
    async fn reopened_deposit_with_landed_sig_completes_without_mint() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_status_reply(&mut server, FINALIZED_STATUS_BODY);

        let mock = MockStorage::new();
        let landed_sig = solana_sdk::signature::Signature::new_unique();
        let mint = Pubkey::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &landed_sig.to_string(), 100).await;
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        assert!(
            sender_rx.try_recv().is_err(),
            "a landed mint must never be re-minted"
        );
        let rows = mock.pending_transactions.lock().unwrap();
        assert_eq!(rows[0].status, TransactionStatus::Completed);
        assert_eq!(
            rows[0].counterpart_signature.as_deref(),
            Some(landed_sig.to_string().as_str())
        );
        drop(rows);
        assert!(
            storage_rx.try_recv().is_err(),
            "no quarantine update for a landed mint"
        );
    }

    /// A reopened deposit whose signature could still land is deferred: no
    /// mint, no quarantine, row left Processing for the recovery sweep.
    #[tokio::test]
    async fn reopened_deposit_with_live_sig_defers_without_mint() {
        let mut server = mockito::Server::new_async().await;
        // Block height 200 is below lvbh 1000, so the sig can still land.
        let _status = mock_status_reply(&mut server, NULL_STATUS_BODY);
        let _height = mock_block_height(&mut server, 200);

        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = solana_sdk::signature::Signature::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &sig.to_string(), 1000).await;
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        assert!(
            sender_rx.try_recv().is_err(),
            "a possibly-live mint must not be re-minted"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Processing,
            "live signature defers the row to the recovery sweep"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no quarantine update for a live signature"
        );
    }

    /// A reopened deposit whose signatures are coverage-proven dead proceeds
    /// to a normal re-mint (the first attempt provably never landed).
    #[tokio::test]
    async fn reopened_deposit_with_dead_sigs_proceeds_to_mint() {
        let mut server = mockito::Server::new_async().await;
        // Block height 200 is past lvbh 100, so the absence is expired; floor 0
        // proves coverage.
        let _status = mock_status_reply(&mut server, NULL_STATUS_BODY);
        let _height = mock_block_height(&mut server, 200);
        let _floor = mock_first_available_block(&mut server, 0);

        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = solana_sdk::signature::Signature::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &sig.to_string(), 100).await;
        let storage = Arc::new(Storage::Mock(mock.clone()));
        insert_mint_row(&storage, &mint);
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        let msg = sender_rx
            .try_recv()
            .expect("proven-dead first attempt must still re-mint");
        let TransactionBuilder::Mint(b) = msg else {
            panic!("expected Mint builder");
        };
        assert_eq!(b.txn_id, 1);
    }

    /// A reopened deposit whose signatures cannot be classified (transient
    /// channel RPC) is deferred, not quarantined: uncertainty must never mint,
    /// and it must never dead-end a possibly-landed row in ManualReview. The row
    /// stays Processing for the recovery sweep to re-check on the same chain.
    #[tokio::test]
    async fn reopened_deposit_uncertain_defers_to_recovery() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("internal server error")
            .create();

        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = solana_sdk::signature::Signature::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &sig.to_string(), 100).await;
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        assert!(sender_rx.try_recv().is_err(), "uncertainty must never mint");
        assert!(
            storage_rx.try_recv().is_err(),
            "the gate must not quarantine; recovery owns that decision"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Processing,
            "an unverifiable reopened deposit stays Processing for the recovery sweep"
        );
    }

    /// A sustained storage failure reading the write-ahead journal (retries
    /// exhausted) surfaces as a transient error, never a quarantine. The gate's
    /// point-read runs on every deposit, so it must stay retryable, never a
    /// permanent ManualReview flip of a healthy row.
    #[tokio::test]
    async fn reopened_deposit_storage_read_error_is_transient_not_quarantine() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = solana_sdk::signature::Signature::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &sig.to_string(), 100).await;
        // The journal read fails; the gate must surface a transient error, not
        // classify the row as unverifiable.
        mock.set_should_fail("get_release_signatures", true);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client("http://localhost:1"),
            None,
            ProgramType::Escrow,
        )
        .await;

        assert!(
            matches!(result, Err(OperatorError::Storage(_))),
            "a journal read failure must be a transient Storage error, got {result:?}"
        );
        assert!(
            sender_rx.try_recv().is_err(),
            "no mint on a transient read error"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a transient read error must not quarantine the row"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Processing,
            "row must stay Processing for retry, not flip to ManualReview"
        );
    }

    /// A brief DB blip on the journal read is absorbed by the gate's bounded
    /// retry: the read recovers within budget and the landed prior mint
    /// completes the row, without exiting the processor task.
    #[tokio::test]
    async fn reopened_deposit_transient_read_blip_recovers_and_completes() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_status_reply(&mut server, FINALIZED_STATUS_BODY);

        let mock = MockStorage::new();
        let landed_sig = solana_sdk::signature::Signature::new_unique();
        let mint = Pubkey::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &landed_sig.to_string(), 100).await;
        // The first two reads fail, the third succeeds: inside the retry budget.
        mock.set_fail_times("get_release_signatures", 2);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        assert!(
            sender_rx.try_recv().is_err(),
            "a landed mint must never be re-minted"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a recovered blip must not quarantine the row"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Completed,
            "the retry absorbs the blip and the landed mint completes the row"
        );
    }

    /// A brief DB blip on the completion write is absorbed by the same bounded
    /// retry: the CAS recovers within budget and the landed row is completed.
    #[tokio::test]
    async fn reopened_deposit_transient_complete_blip_recovers_and_completes() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_status_reply(&mut server, FINALIZED_STATUS_BODY);

        let mock = MockStorage::new();
        let landed_sig = solana_sdk::signature::Signature::new_unique();
        let mint = Pubkey::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &landed_sig.to_string(), 100).await;
        // The first two completion writes fail, the third succeeds: inside budget.
        mock.set_fail_times("try_complete_processing", 2);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await
        .unwrap();

        assert!(
            sender_rx.try_recv().is_err(),
            "a landed mint must never be re-minted"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Completed,
            "the retry absorbs the write blip and the row completes"
        );
    }

    /// A sustained completion-write failure (retries exhausted) does not exit
    /// the task or quarantine: the mint already landed, so the row is left
    /// Processing for the recovery sweep to complete.
    #[tokio::test]
    async fn reopened_deposit_sustained_complete_failure_left_for_recovery() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_status_reply(&mut server, FINALIZED_STATUS_BODY);

        let mock = MockStorage::new();
        let landed_sig = solana_sdk::signature::Signature::new_unique();
        let mint = Pubkey::new_unique();
        let txn = seed_reopened_deposit(&mock, &mint, &landed_sig.to_string(), 100).await;
        mock.set_should_fail("try_complete_processing", true);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: None,
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(txn).await.unwrap();
        drop(fetcher_tx);

        let result = process_deposit_funds(
            &mut ps,
            fetcher_rx,
            sender_tx,
            storage_tx,
            storage.clone(),
            channel_client(&server.url()),
            None,
            ProgramType::Escrow,
        )
        .await;

        assert!(
            result.is_ok(),
            "a completion-write failure must not exit the task: {result:?}"
        );
        assert!(
            sender_rx.try_recv().is_err(),
            "a landed mint must never be re-minted"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "leaving for recovery must not quarantine the row"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Processing,
            "the row stays Processing for the recovery sweep to complete"
        );
    }

    /// Mocked `getAccountInfo` reply for an account that does not exist.
    fn absent_account_response() -> serde_json::Value {
        serde_json::json!({"context": {"slot": 1}, "value": null})
    }

    /// A `MintCache` whose RPC reports every account as absent.
    fn mint_cache_over_absent_chain(storage: Arc<Storage>) -> crate::operator::MintCache {
        use crate::operator::rpc_util::RpcClientWithRetry;
        use solana_client::rpc_request::RpcRequest;

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, absent_account_response());
        crate::operator::MintCache::with_rpc(
            storage,
            Arc::new(RpcClientWithRetry::new_mocked(mocks)),
        )
    }

    /// A Token-2022 mint the indexer knows about, whose account is absent from the
    /// target chain, must park the one row instead of taking the operator down.
    #[tokio::test]
    async fn process_release_funds_target_mint_missing_routes_to_manual_review() {
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        // Flags unresolved, so the pre-flight has to ask the chain about this mint.
        insert_mint_row_with(&storage, &mint, &spl_token_2022::id(), None);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: mint_cache_over_absent_chain(storage.clone()),
        };
        // The gate is not this test's subject; treat the mint as already proved.
        ps.mint_cache.record_existence_floor(&mint, 1);

        let txn = make_db_transaction(
            77,
            &mint.to_string(),
            &recipient.to_string(),
            Some(5),
            TransactionType::Withdrawal,
        );

        let (outcome, update, builder) = run_one_withdrawal(&mut ps, storage, txn).await;

        assert!(outcome.is_ok(), "a missing mint must not exit the task");
        let update = update.expect("row must be routed to ManualReview");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let msg = update.error_message.expect("error_message must be set");
        assert!(
            msg.contains("withdrawal mint absent on target chain")
                && msg.contains(&mint.to_string()),
            "unexpected error_message: {msg}"
        );
        assert!(builder.is_none(), "no builder may be dispatched");
    }

    /// Mocked `getAccountInfo` reply for a well-formed legacy SPL mint.
    fn spl_mint_account_response(decimals: u8) -> serde_json::Value {
        // Base SPL mint layout is 82 bytes; decimals sits at offset 44 and the
        // is_initialized flag at offset 45.
        let mut data = vec![0u8; 82];
        data[44] = decimals;
        data[45] = 1;
        serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": spl_token::id().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&data), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        })
    }

    /// Mocked `getAccountInfo` reply for an escrow-owned AllowedMint account.
    fn allowed_mint_account_response(slot: u64) -> serde_json::Value {
        serde_json::json!({
            "context": {"slot": slot},
            "value": {
                "owner": PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode([2u8, 255u8]), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        })
    }

    /// A withdrawal processor whose target chain answers every account read with
    /// `response`, which for these tests is the allowlist account the gate reads.
    fn processor_state_answering(
        storage: &Arc<Storage>,
        response: serde_json::Value,
    ) -> ProcessorState {
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, response);
        ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(
                storage.clone(),
                Arc::new(RpcClientWithRetry::new_mocked(mocks)),
            ),
        }
    }

    /// A withdrawal row for `mint` at `nonce`.
    fn withdrawal_for(mint: &Pubkey, nonce: i64) -> DbTransaction {
        make_db_transaction(
            9,
            &mint.to_string(),
            &Pubkey::new_unique().to_string(),
            Some(nonce),
            TransactionType::Withdrawal,
        )
    }

    /// No escrow allowlist account means the escrow program would reject the
    /// release, so the row is parked rather than retried forever.
    #[tokio::test]
    async fn process_release_funds_unsupported_mint_routes_to_manual_review() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut ps = processor_state_answering(&storage, absent_account_response());

        let (outcome, update, builder) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok(), "one bad mint must not end the loop");
        let update = update.expect("row must be routed to ManualReview");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(update
            .error_message
            .expect("error_message must be set")
            .contains("unsupported withdrawal mint:"));
        assert!(builder.is_none(), "nothing may be dispatched");
    }

    /// An allowlisted mint passes the gate untouched.
    #[tokio::test]
    async fn process_release_funds_allowlisted_mint_proceeds() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        insert_mint_row(&storage, &mint);
        let mut ps = processor_state_answering(&storage, allowed_mint_account_response(500));

        let (outcome, update, builder) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok());
        assert!(update.is_none(), "a supported mint must not be quarantined");
        assert!(
            matches!(builder, Some(TransactionBuilder::ReleaseFunds(_))),
            "the withdrawal must be dispatched"
        );
    }

    /// Passing the gate records the slot the allowlist account was seen at, which is
    /// what later lets a missing mint account be permanent rather than node lag.
    #[tokio::test]
    async fn process_release_funds_allowlist_hit_records_the_existence_floor() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        insert_mint_row(&storage, &mint);
        let mut ps = processor_state_answering(&storage, allowed_mint_account_response(500));

        let (outcome, _, _) = run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok());
        assert!(
            ps.mint_cache.has_existence_floor(&mint),
            "the gate must record what it proved"
        );
    }

    /// `BlockMint` closes the allowlist account and `release_funds` requires it, so a
    /// blocked mint's release can never land. Parking makes that visible instead of
    /// dispatching a transaction the escrow program is certain to reject.
    #[tokio::test]
    async fn process_release_funds_blocked_mint_parks_rather_than_dispatching() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        insert_mint_row(&storage, &mint);
        seed_mint_status(&storage, &mint, "blocked", 50);
        let mut ps = processor_state_answering(&storage, absent_account_response());

        let (outcome, update, builder) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok());
        assert_eq!(
            update.expect("row must be parked").status,
            TransactionStatus::ManualReview
        );
        assert!(
            builder.is_none(),
            "a release that cannot land is not dispatched"
        );
    }

    /// An account squatting the allowlist address carries no escrow permission, so
    /// presence alone must not open the gate.
    #[tokio::test]
    async fn process_release_funds_foreign_owned_allowlist_account_is_rejected() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        insert_mint_row(&storage, &mint);
        let mut ps = processor_state_answering(&storage, spl_mint_account_response(6));

        let (outcome, update, _) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok());
        assert_eq!(
            update.expect("row must be parked").status,
            TransactionStatus::ManualReview,
            "only an escrow-owned account grants permission"
        );
    }

    /// A node we cannot reach is not a verdict about the mint, so the row must stay
    /// eligible rather than be parked on an unanswered question.
    #[tokio::test]
    async fn process_release_funds_unreadable_allowlist_is_transient() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (outcome, update, _) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(
            matches!(outcome, Err(OperatorError::RpcError(_))),
            "an unreadable allowlist must stay transient, got: {outcome:?}"
        );
        assert!(update.is_none(), "no verdict means no park");
    }

    /// A node behind the anchor slot must not be able to answer the allowlist read at
    /// all: its null would deny a mint the escrow did allow, parking a burned row.
    #[tokio::test]
    async fn process_release_funds_lagging_allowlist_read_is_transient() {
        let mut server = mockito::Server::new_async().await;
        // Anchor the gate at slot 500, then refuse to serve there as a lagging node does.
        let _anchor = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":500},"value":{"blockhash":"11111111111111111111111111111111","lastValidBlockHeight":600}},"id":1}"#,
            )
            .create();
        let _lagging = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""minContextSlot"\s*:\s*500"#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","error":{"code":-32016,"message":"Minimum context slot has not been reached"},"id":1}"#,
            )
            .expect_at_least(1)
            .create();

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(
                storage.clone(),
                Arc::new(RpcClientWithRetry::with_retry_config(
                    server.url(),
                    crate::operator::utils::rpc_util::RetryConfig {
                        max_attempts: 1,
                        base_delay: std::time::Duration::from_millis(1),
                        max_delay: std::time::Duration::from_millis(2),
                    },
                    solana_sdk::commitment_config::CommitmentConfig::confirmed(),
                )),
            ),
        };

        let (outcome, update, _) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&Pubkey::new_unique(), 5)).await;

        assert!(
            matches!(outcome, Err(OperatorError::RpcError(_))),
            "a node that cannot answer at the anchor slot must stay transient, got: {outcome:?}"
        );
        assert!(
            update.is_none(),
            "a lagging node is not a verdict, so no park"
        );
    }

    /// A boundary nonce whose mint is unsupported must not dispatch a rotation,
    /// exactly as it did not before the gate existed.
    #[tokio::test]
    async fn process_release_funds_unsupported_boundary_mint_skips_rotation() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut ps = processor_state_answering(&storage, absent_account_response());

        let (outcome, update, builder) = run_one_withdrawal(
            &mut ps,
            storage,
            withdrawal_for(&mint, MAX_TREE_LEAVES as i64),
        )
        .await;

        assert!(outcome.is_ok());
        assert_eq!(
            update.expect("row must be parked").status,
            TransactionStatus::ManualReview
        );
        assert!(builder.is_none(), "no rotation may be dispatched");
    }

    /// Parking a boundary row leaves the tree on the old generation, so buffered
    /// siblings must go back to Pending. Dispatching one would build it against an
    /// index the chain rejects, waiting on a rotation this row alone could trigger.
    #[tokio::test]
    async fn process_release_funds_parked_boundary_requeues_buffered_siblings() {
        let mock = MockStorage::new();
        let unsupported = Pubkey::new_unique();
        let good_mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let boundary = MAX_TREE_LEAVES as i64;
        let head = seed_processing_withdrawal(
            &mock,
            1,
            boundary,
            &unsupported,
            &recipient,
            SeededMint::Absent,
        );
        let sibling = seed_processing_withdrawal(
            &mock,
            2,
            boundary + 1,
            &good_mint,
            &recipient,
            SeededMint::Resolved,
        );
        let storage = Arc::new(Storage::Mock(mock.clone()));

        // The head's mint has no allowlist account; the sibling's is already proved,
        // so only the head reaches the gate.
        let mut ps = processor_state_answering(&storage, absent_account_response());
        ps.mint_cache.record_existence_floor(&good_mint, 1);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(4);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        fetcher_tx.send(head).await.unwrap();
        fetcher_tx.send(sibling).await.unwrap();
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

        assert!(result.is_ok(), "parking a row must not end the task");
        assert_eq!(
            storage_rx.try_recv().expect("head must be parked").status,
            TransactionStatus::ManualReview
        );
        assert!(
            sender_rx.try_recv().is_err(),
            "no rotation and no sibling may be dispatched"
        );

        let after = mock.pending_transactions.lock().unwrap();
        let sibling_row = after.iter().find(|t| t.id == 2).unwrap();
        assert_eq!(
            sibling_row.status,
            TransactionStatus::Pending,
            "buffered sibling must be requeued, not dispatched"
        );
    }
}
