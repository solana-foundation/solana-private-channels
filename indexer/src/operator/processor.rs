use crate::channel_utils::send_guaranteed;
use crate::error::{AccountError, OperatorError, ProgramError};
use crate::metrics;
use crate::operator::instruction_util::{
    mint_idempotency_memo, MintToBuilder, TransactionBuilder, WithdrawalRemintInfo,
};
use crate::operator::recovery::MAX_RECOVERY_REQUEUE_ATTEMPTS;
use crate::operator::sender::TransactionStatusUpdate;
use crate::operator::utils::mint_util::MintCache;
use crate::operator::{
    find_allowed_mint_pda, find_event_authority_pda, find_operator_pda, find_withdrawal_bitmap_pda,
    MintToBuilderWithTxnId, ReleaseFundsBuilderWithNonce, SignerUtil,
};
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
    pub withdrawal_bitmap_pda: Pubkey,
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
                withdrawal_bitmap_pda: find_withdrawal_bitmap_pda(&instance_pda),
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
        OperatorError::MintNotAllowed { .. } => ErrorDisposition::Quarantine("mint_not_allowed"),
        OperatorError::Program(ProgramError::InvalidBuilder { .. }) => {
            ErrorDisposition::Quarantine("invalid_builder")
        }
        // A bitmap the node would not serve says nothing about the row.
        OperatorError::Program(ProgramError::BitmapUnavailable { .. }) => {
            ErrorDisposition::Transient
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
/// The bitmap rotates on boundary nonces, so draining on while a row waits on
/// a human could rotate past that row's generation. The program then rejects
/// its nonce for good and the re-arm path dies. Halting keeps the bitmap still:
/// drain the fetcher channel, then flip remaining active rows at or above the
/// poison's nonce to `ManualReview`.
///
/// The floor spares lower rows: one may already be signed or broadcast, and
/// terminalizing it discards its later `Completed` write. Those stay
/// `Processing`/`Parked` for the recovery worker to pick up.
///
/// `poison` supplies both the excluded id (no duplicate webhook) and the
/// floor. Recovery is manual, see `withdrawal_manual_review.md`.
async fn halt_withdrawal_pipeline(
    storage: &Storage,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    fetcher_rx: &mut mpsc::Receiver<DbTransaction>,
    poison: Option<&DbTransaction>,
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

    // Sweep the rest of the pipeline: any row at or above the poison's nonce
    // still `Pending` (never fetched), `Processing` (locked but unsent) or
    // `Parked` is flipped to `ManualReview`. A poison row with no nonce
    // yields no floor, so the sweep stays unbounded.
    let poison_id = poison.map(|txn| txn.id);
    let min_nonce = poison.and_then(|txn| txn.withdrawal_nonce);
    match storage
        .quarantine_active_withdrawals(poison_id, min_nonce)
        .await
    {
        Ok(affected) => {
            warn!(
                drained_from_channel = drained,
                db_rows_quarantined = affected,
                nonce_floor = min_nonce,
                "Halted withdrawal pipeline; active rows at or above the poison nonce moved to ManualReview"
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
                "quarantine_active_withdrawals failed: {}", e
            );
        }
    }
}

/// CAS one owned, unsent withdrawal `Processing -> Pending` so the fetcher can
/// re-claim it. Safe because a transient error before the sender handoff proves
/// nothing was broadcast and no signature was recorded. Mirrors the sender's
/// pre-broadcast requeue: only `Requeued` flips the row, and `AtCap`,
/// `NotProcessing` and a failed write all leave it Processing for recovery.
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
                "Requeued withdrawal to Pending after a pre-broadcast transient error"
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

/// Rescue the head withdrawal after a transient error, which happened before
/// anything was handed to the sender, instead of dropping it stranded in
/// `Processing` on `return Err`. No DB sweep: a blanket flip could hit an
/// in-flight sender row whose signature is not yet persisted, so only this owned
/// row and the channel-buffered ones are provably safe.
///
/// Capped on the durable counter the fetched row carries, so an error that only
/// looks transient cannot loop the operator in a restart storm; at the cap the
/// row is quarantined for a human instead.
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
            "Withdrawal failed after max pre-broadcast requeues; quarantining"
        );
        quarantine_single(storage_tx, transaction, reason).await;
    } else {
        requeue_single_prebroadcast(storage, pt_label, transaction).await;
    }
}

/// Requeue every row still buffered in `fetcher_rx`. The fetcher flipped them to
/// `Processing` but never handed them on, so nothing about them was broadcast
/// and requeueing is always safe. Mirrors the drain in `halt_withdrawal_pipeline`
/// but hands the rows back rather than quarantining them.
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

    builder
        .payer(processor_state.admin_pubkey)
        .operator(release_funds_state.operator_pubkey)
        .instance(release_funds_state.instance_pda)
        .withdrawal_bitmap(release_funds_state.withdrawal_bitmap_pda)
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
    // The burn debited the initiator, so the remint credits them, not `recipient`
    // (the Solana destination, which only the release leg above uses).
    let initiator =
        Pubkey::from_str(&transaction.initiator).map_err(|e| OperatorError::InvalidPubkey {
            pubkey: transaction.initiator.clone(),
            reason: e.to_string(),
        })?;
    let remint_user_ata = get_associated_token_address_with_program_id(
        &initiator,
        &mint,
        &private_channel_token_program,
    );
    let remint_info = WithdrawalRemintInfo {
        transaction_id: transaction.id,
        trace_id: transaction.trace_id.clone(),
        mint,
        user: initiator,
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

/// Token-2022 pre-flight for a withdrawal.
///
/// Returns:
/// - `Ok(None)` — clean: proceed to build + dispatch.
/// - `Ok(Some(bail))` — row-specific bail: caller parks the row and continues
///   the loop. Used for paused mints, permanent-delegate drains, and mints the
///   target chain does not have, where the row's data is fine but the on-chain
///   state would cause an immediate release-funds failure.
/// - `Err(_)` — transient infrastructure issue (RPC failure, malformed
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

        let outcome: Result<(), OperatorError> = async {
            // Settle whether the escrow will accept a release for this mint before
            // building one, so a mint it would reject costs no further work and no
            // target-chain lookup that would read as an infrastructure failure.
            if let Some(bail) =
                check_withdrawal_mint_supported(processor_state, &transaction).await?
            {
                park_row(&storage_tx, pt_label, &transaction, bail).await;
                return Ok(());
            }

            // Build first so row-data poison, such as a NULL nonce or an
            // unparseable pubkey, surfaces here as an `InvalidBuilder` for the
            // classifier to halt the pipeline on. Building also warms
            // `MintCache.cache`, so the pre-flight below does not pay an extra
            // database or RPC round-trip for `get_mint_metadata`.
            let release_funds_tx = build_release_funds(processor_state, &transaction).await?;

            // Pre-flight for Token-2022 pause / permanent-delegate drain. These
            // are row-specific, so bails route to ManualReview and continue the
            // loop rather than halting the pipeline (reserved for poison rows).
            // It is best-effort: a delegate can still drain between this read and
            // the on-chain CPI, leaving that to the sender retry path. RPC errors
            // bubble up as Transient and restart the task.
            if let Some(bail) = check_withdrawal_preflights(processor_state, &transaction).await? {
                park_row(&storage_tx, pt_label, &transaction, bail).await;
                return Ok(());
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
                        Some(&transaction),
                    )
                    .await;
                    return Ok(());
                }
                ErrorDisposition::Transient => {
                    // Nothing reached the sender, for this row or for the ones
                    // still buffered, so hand them back to the fetcher instead
                    // of leaving them Processing until the recovery sweep.
                    requeue_or_quarantine_head(
                        &storage,
                        &storage_tx,
                        pt_label,
                        &transaction,
                        err.to_string(),
                    )
                    .await;
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
                .idempotency_memo(mint_idempotency_memo(transaction.id));

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
    use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
    use crate::operator::find_allowed_mint_pda;
    use crate::operator::rpc_util::RpcClientWithRetry;
    use crate::operator::utils::account_util::bitmap_account_bytes;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::DbMint;
    use crate::storage::common::models::TransactionType;
    use crate::storage::common::storage::mock::MockStorage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use solana_client::rpc_request::RpcRequest;

    fn make_release_funds_state() -> ReleaseFundsState {
        let instance_pda = Pubkey::new_unique();
        ReleaseFundsState {
            instance_pda,
            withdrawal_bitmap_pda: find_withdrawal_bitmap_pda(&instance_pda),
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

    /// Insert a minimal `mints` row AND a slot-0 `allowed` status history
    /// entry so `assert_mint_allowed_at_slot` accepts the mint at any slot.
    fn insert_mint_row(storage: &Arc<Storage>, mint: &Pubkey) {
        let mock_storage = match storage.as_ref() {
            Storage::Mock(m) => m,
            _ => unreachable!("test helper expects Storage::Mock"),
        };
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: spl_token::id().to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(false),
            },
        );
        mock_storage.mint_status_history.lock().unwrap().push(
            crate::storage::common::models::DbMintStatus {
                mint_address: mint.to_string(),
                status: "allowed".to_string(),
                effective_slot: 0,
                signature: format!("test-seed-{mint}"),
                created_at: chrono::Utc::now(),
            },
        );
    }

    /// A Token-2022 `mints` row with both extension flags unresolved, so the
    /// pre-flight has to read the mint account from the chain.
    fn insert_token_2022_mint_row(storage: &Arc<Storage>, mint: &Pubkey) {
        let mock_storage = match storage.as_ref() {
            Storage::Mock(m) => m,
            _ => unreachable!("test helper expects Storage::Mock"),
        };
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: spl_token_2022::id().to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable: None,
                has_permanent_delegate: None,
            },
        );
    }

    /// Mocked `getAccountInfo` response for a withdrawal bitmap account on the
    /// given generation, used to drive the boundary-rotation read.
    fn bitmap_account_response(generation: u64) -> serde_json::Value {
        let bytes = bitmap_account_bytes(generation, &[], 255);
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
            initiator: Pubkey::new_unique().to_string(),
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
            release_refused_on_chain: false,
        }
    }

    /// Treat `mint` as already proved allowlisted, skipping the gate in tests
    /// whose subject is a later step of the loop.
    fn assume_mint_allowlisted(ps: &mut ProcessorState, mint: &Pubkey) {
        ps.mint_cache.record_existence_floor(mint, 1);
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

    fn absent_account_response() -> serde_json::Value {
        serde_json::json!({"context": {"slot": 1}, "value": null})
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

    /// Drive one withdrawal through `process_release_funds` and return whatever
    /// reached the storage writer and the sender, plus the loop's own result.
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

    // ── escrow allowlist gate ───────────────────────────────────────

    /// No escrow allowlist account means the escrow program would reject the
    /// release, so the row is parked rather than retried forever.
    #[tokio::test]
    async fn unsupported_mint_is_parked_without_stopping_the_loop() {
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

    /// An allowlisted mint passes the gate untouched, and passing records the
    /// slot it was seen at so a later missing mint reads as permanent.
    #[tokio::test]
    async fn allowlisted_mint_proceeds_and_records_the_existence_floor() {
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
        assert!(
            ps.mint_cache.has_existence_floor(&mint),
            "the gate must record what it proved"
        );
    }

    /// An account squatting the allowlist address carries no escrow permission,
    /// so presence alone must not open the gate.
    #[tokio::test]
    async fn foreign_owned_allowlist_account_is_rejected() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        insert_mint_row(&storage, &mint);
        let mut ps = processor_state_answering(&storage, bitmap_account_response(0));

        let (outcome, update, _) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok());
        assert_eq!(
            update.expect("row must be parked").status,
            TransactionStatus::ManualReview,
            "only an escrow-owned account grants permission"
        );
    }

    /// A mint proved to exist but absent from the target chain was closed, not
    /// merely unseen, so the row is parked instead of restarting the operator.
    #[tokio::test]
    async fn a_mint_absent_from_the_target_chain_parks_the_row() {
        let mint = Pubkey::new_unique();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        // Token-2022 with unresolved flags, so the pre-flight has to read the chain.
        insert_token_2022_mint_row(&storage, &mint);
        let mut ps = processor_state_answering(&storage, absent_account_response());
        assume_mint_allowlisted(&mut ps, &mint);

        let (outcome, update, builder) =
            run_one_withdrawal(&mut ps, storage, withdrawal_for(&mint, 5)).await;

        assert!(outcome.is_ok(), "a missing mint must not exit the task");
        let msg = update
            .expect("row must be parked")
            .error_message
            .expect("error_message must be set");
        assert!(
            msg.contains("withdrawal mint absent on target chain")
                && msg.contains(&mint.to_string()),
            "unexpected error_message: {msg}"
        );
        assert!(builder.is_none(), "no builder may be dispatched");
    }

    /// The remint reverses a burn on the private channel, so it must credit the
    /// account that was debited (`initiator`), not the withdrawal's Solana
    /// destination (`recipient`).
    #[tokio::test]
    async fn build_release_funds_remints_to_initiator_not_destination() {
        let mint_pubkey = Pubkey::new_unique();
        let initiator = Pubkey::new_unique();
        let destination = Pubkey::new_unique();

        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        insert_mint_row(&storage, &mint_pubkey);

        let mut processor_state = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage),
        };

        let mut txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &destination.to_string(),
            Some(3),
            TransactionType::Withdrawal,
        );
        txn.initiator = initiator.to_string();

        let builder = build_release_funds(&mut processor_state, &txn)
            .await
            .expect("a valid withdrawal row must build");
        let TransactionBuilder::ReleaseFunds(release) = builder else {
            panic!("a withdrawal must build a ReleaseFunds builder");
        };
        let remint = release
            .remint_info
            .expect("a withdrawal must carry remint info");

        assert_eq!(remint.user, initiator);
        assert_ne!(remint.user, destination);
        assert_eq!(
            remint.user_ata,
            get_associated_token_address_with_program_id(
                &initiator,
                &mint_pubkey,
                &spl_token::id()
            ),
            "remint must mint into the initiator's private channel ATA"
        );
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
                    status: "allowed".to_string(),
                    is_pausable: Some(false),
                    has_permanent_delegate: Some(false),
                },
            );
        }

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

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

    /// A boundary nonce is an ordinary withdrawal here. Rotation is driven from
    /// the bitmap's own state elsewhere, so coupling it back to one row would
    /// reintroduce a single point of failure for every withdrawal after it.
    #[tokio::test]
    async fn boundary_nonce_dispatches_only_the_release() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));

        // On-chain tree index 0 < boundary target 1, so the rotation must fire.
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, bitmap_account_response(0));
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::with_rpc(storage.clone(), Arc::new(rpc_client)),
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
                    status: "allowed".to_string(),
                    is_pausable: Some(false),
                    has_permanent_delegate: Some(false),
                },
            );
        }

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(NONCES_PER_GENERATION as i64),
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
            panic!("a boundary nonce must dispatch its release and nothing else");
        };
        assert_eq!(b.nonce, NONCES_PER_GENERATION);
        assert_eq!(b.transaction_id, 1);

        assert!(
            sender_rx.try_recv().is_err(),
            "the processor must not dispatch a rotation"
        );
    }

    /// A boundary nonce that bails its pre-flight is quarantined like any other
    /// row and dispatches nothing at all. Nothing here is owed to the bitmap, so
    /// the row's fate cannot decide whether a later generation ever opens.
    #[tokio::test]
    async fn boundary_row_preflight_bail_dispatches_nothing() {
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

        // Escrow balance 500 is short of the 1000 the withdrawal needs.
        let mut mocks = std::collections::HashMap::new();
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

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let txn = make_db_transaction(
            1,
            &mint_pubkey.to_string(),
            &recipient.to_string(),
            Some(NONCES_PER_GENERATION as i64),
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

        assert!(
            sender_rx.try_recv().is_err(),
            "a bailed row must dispatch nothing, not even on a boundary nonce"
        );

        // The boundary row is quarantined to ManualReview.
        let update = storage_rx.try_recv().expect("ManualReview update expected");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(update
            .error_message
            .expect("error_message must be set")
            .contains("insufficient escrow balance"));
    }

    /// A mint field that cannot be parsed as a Pubkey halts the pipeline.
    /// The poison row is marked ManualReview and subsequent active withdrawals
    /// are quarantined. A boundary nonce is no exception: nothing it could have
    /// dispatched is load-bearing for the bitmap any more.
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

    // ── transient rescue before the sender handoff ──────────────────

    /// Seed `mock` with `txn` as a Processing row, which is the state the
    /// fetcher leaves behind when it hands a row to the processor.
    fn seed_processing_row(storage: &Arc<Storage>, txn: &DbTransaction) {
        let Storage::Mock(ref mock) = **storage else {
            unreachable!("test helper expects Storage::Mock");
        };
        mock.pending_transactions.lock().unwrap().push(txn.clone());
    }

    fn row_status(storage: &Arc<Storage>, id: i64) -> Option<TransactionStatus> {
        let Storage::Mock(ref mock) = **storage else {
            unreachable!("test helper expects Storage::Mock");
        };
        let rows = mock.pending_transactions.lock().unwrap();
        rows.iter().find(|txn| txn.id == id).map(|txn| txn.status)
    }

    /// A transient error stops the loop before anything reaches the sender, so
    /// neither the head row nor the rows the fetcher already handed over were
    /// broadcast. Leaving them Processing strands them until the recovery sweep
    /// ages them out; requeueing hands them straight back to the fetcher.
    #[tokio::test]
    async fn transient_error_requeues_the_head_and_the_buffered_rows() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        // No mint row and no RPC, so the metadata read fails as transient.
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(2);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let mint = Pubkey::new_unique().to_string();
        for id in [1, 2] {
            let txn = make_db_transaction(
                id,
                &mint,
                &Pubkey::new_unique().to_string(),
                Some(id),
                TransactionType::Withdrawal,
            );
            seed_processing_row(&storage, &txn);
            fetcher_tx.send(txn).await.unwrap();
        }
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

        assert!(result.is_err(), "a transient error still restarts the task");
        assert_eq!(
            row_status(&storage, 1),
            Some(TransactionStatus::Pending),
            "the head row never reached the sender and must be re-claimable"
        );
        assert_eq!(
            row_status(&storage, 2),
            Some(TransactionStatus::Pending),
            "a buffered row was never even looked at"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a transient error is not a terminal verdict on any row"
        );
    }

    /// The rescue is capped on the durable counter the fetched row carries, so
    /// an error only looking transient cannot loop the operator forever.
    #[tokio::test]
    async fn a_head_row_out_of_requeues_is_quarantined_instead() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut ps = ProcessorState {
            admin_pubkey: Pubkey::new_unique(),
            release_funds_state: Some(make_release_funds_state()),
            mint_cache: crate::operator::MintCache::new(storage.clone()),
        };

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, _sender_rx) = mpsc::channel(10);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let mut txn = make_db_transaction(
            1,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            Some(1),
            TransactionType::Withdrawal,
        );
        txn.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        seed_processing_row(&storage, &txn);
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

        assert!(result.is_err());
        let update = storage_rx
            .try_recv()
            .expect("a row out of requeues must be escalated");
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(update.transaction_id, 1);
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
                    status: "allowed".to_string(),
                    is_pausable: Some(false),
                    has_permanent_delegate: Some(false),
                },
            );
        }

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

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

        let mint_pubkey = Pubkey::new_unique();
        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

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

        let other_program = OperatorError::Program(ProgramError::InvalidProof {
            reason: "stale".into(),
        });
        assert!(matches!(
            classify_processor_error(&other_program),
            ErrorDisposition::Quarantine("program_error")
        ));

        // Quarantining on an outage would halt the pipeline over an RPC blip.
        let bitmap_unavailable = OperatorError::Program(ProgramError::BitmapUnavailable {
            reason: "rpc down".into(),
        });
        assert!(matches!(
            classify_processor_error(&bitmap_unavailable),
            ErrorDisposition::Transient
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
            ProgramError::BitmapUnavailable {
                reason: "rpc down".into(),
            },
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
        mock.set_should_fail("quarantine_active_withdrawals", true);
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

    /// Seed an active withdrawal into the mock at the given id and nonce.
    fn seed_active_withdrawal(
        mock: &MockStorage,
        id: i64,
        nonce: Option<i64>,
        status: TransactionStatus,
    ) -> DbTransaction {
        let mut txn = make_db_transaction(
            id,
            &Pubkey::new_unique().to_string(),
            &Pubkey::new_unique().to_string(),
            nonce,
            TransactionType::Withdrawal,
        );
        txn.status = status;
        mock.pending_transactions.lock().unwrap().push(txn.clone());
        txn
    }

    /// Rows below the poison nonce are the ones the sender already signed or
    /// broadcast. Terminalizing one drops its later `Completed` write and
    /// leaves the next boot's bitmap diff seeing a set bit with no `Completed`
    /// row, so the halt sweep has to stop at the poison's own nonce.
    #[tokio::test]
    async fn halt_sweep_spares_withdrawals_below_poison_nonce() {
        let mock = MockStorage::new();
        seed_active_withdrawal(&mock, 10, Some(4), TransactionStatus::Processing);
        let poison = seed_active_withdrawal(&mock, 11, Some(5), TransactionStatus::Processing);
        seed_active_withdrawal(&mock, 12, Some(6), TransactionStatus::Pending);

        let storage = Storage::Mock(mock);
        let (storage_tx, _storage_rx) = mpsc::channel(4);
        let (_fetcher_tx, mut fetcher_rx) = mpsc::channel::<DbTransaction>(4);

        halt_withdrawal_pipeline(&storage, &storage_tx, &mut fetcher_rx, Some(&poison)).await;

        let rows = match &storage {
            Storage::Mock(m) => m.pending_transactions.lock().unwrap().clone(),
            _ => unreachable!(),
        };
        let status_of = |id: i64| rows.iter().find(|t| t.id == id).unwrap().status;
        assert_eq!(status_of(10), TransactionStatus::Processing);
        assert_eq!(status_of(11), TransactionStatus::Processing);
        assert_eq!(status_of(12), TransactionStatus::ManualReview);
    }

    /// A withdrawal reaching the halt with no nonce means the assign-nonce
    /// trigger was bypassed, so the queue cannot be reasoned about at all.
    /// The sweep stays unbounded in that case, which is fail-closed.
    #[tokio::test]
    async fn halt_sweep_with_null_poison_nonce_sweeps_everything() {
        let mock = MockStorage::new();
        let poison = seed_active_withdrawal(&mock, 20, None, TransactionStatus::Processing);
        seed_active_withdrawal(&mock, 21, Some(3), TransactionStatus::Processing);

        let storage = Storage::Mock(mock);
        let (storage_tx, _storage_rx) = mpsc::channel(4);
        let (_fetcher_tx, mut fetcher_rx) = mpsc::channel::<DbTransaction>(4);

        halt_withdrawal_pipeline(&storage, &storage_tx, &mut fetcher_rx, Some(&poison)).await;

        let rows = match &storage {
            Storage::Mock(m) => m.pending_transactions.lock().unwrap().clone(),
            _ => unreachable!(),
        };
        let sibling = rows.iter().find(|t| t.id == 21).unwrap();
        assert_eq!(sibling.status, TransactionStatus::ManualReview);
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

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

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

    /// A poison boundary row is quarantined and the pipeline halts, with nothing
    /// reaching the sender. The row used to owe the bitmap a rotation, so its
    /// death took every later generation with it; now it owes nothing and only
    /// its own release is lost.
    #[tokio::test]
    async fn boundary_poison_row_quarantines_and_dispatches_nothing() {
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

        // An unparseable mint makes build_release_funds fail, which is the
        // deterministic poison the quarantine path exists for.
        let txn = make_db_transaction(
            1,
            "not_a_valid_pubkey",
            &Pubkey::new_unique().to_string(),
            Some(NONCES_PER_GENERATION as i64),
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

        // Every seeded DB row sits above the poison's nonce, so the bounded
        // sweep in quarantine_active_withdrawals still flips all of them.
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

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);

        let txn = DbTransaction {
            id: 42,
            signature: "test_sig".to_string(),
            trace_id: "trace-42".to_string(),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
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
            release_refused_on_chain: false,
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

        // The allowlist gate is not this test's subject; treat the mint as proved.
        assume_mint_allowlisted(&mut ps, &mint_pubkey);

        let (fetcher_tx, fetcher_rx) = mpsc::channel::<DbTransaction>(1);
        let (sender_tx, mut sender_rx) = mpsc::channel(10);

        let txn = DbTransaction {
            id: 7,
            signature: "test_sig".to_string(),
            trace_id: "trace-7".to_string(),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
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
            release_refused_on_chain: false,
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
}
