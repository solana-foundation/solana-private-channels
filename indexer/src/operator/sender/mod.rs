mod mint;
mod proof;
mod remint;
mod state;
#[cfg(test)]
mod test_support;
mod transaction;
pub mod types;

pub use mint::{
    enumerate_consumed_mints, find_existing_mint_signature_with_memo, ConsumedMintKind,
    ConsumedSet, JitOutcome,
};
pub(crate) use remint::{classify_signatures, FinalityRpc, SigFinality};
pub(crate) use state::{validate_bitmap_consistency, verify_release_landed, ReleaseVerdict};
pub use types::TransactionStatusUpdate;

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only re-exports of `pub(super)` constructors/recovery paths.
    //! Gated behind `test-mock-storage` so production builds get the
    //! same narrow API surface they always have.
    use super::*;
    use solana_sdk::commitment_config::CommitmentLevel;
    use std::sync::Arc;

    pub use super::remint::DeferredRemintOutcome;

    pub fn new_sender_state(
        config: &PrivateChannelIndexerConfig,
        operator_commitment: CommitmentLevel,
        instance_pda: Option<solana_sdk::pubkey::Pubkey>,
        storage: Arc<Storage>,
        retry_max_attempts: u32,
        confirmation_poll_interval_ms: u64,
        source_rpc_client: Option<Arc<RpcClientWithRetry>>,
    ) -> Result<SenderState, OperatorError> {
        SenderState::new(
            config,
            operator_commitment,
            instance_pda,
            storage,
            retry_max_attempts,
            confirmation_poll_interval_ms,
            source_rpc_client,
        )
    }

    pub async fn recover_pending_remints(
        state: &mut SenderState,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        state.recover_pending_remints(storage_tx).await
    }

    pub async fn jit_mint_init(
        state: &mut SenderState,
        transaction_id: i64,
        instruction: super::types::InstructionWithSigners,
    ) -> super::mint::JitOutcome {
        super::mint::try_jit_mint_initialization(state, transaction_id, instruction).await
    }

    /// Drives `process_pending_remints` end-to-end. Each call walks
    /// every matured `PendingRemint` in `state.pending_remints` and
    /// either re-queues it (RPC error), promotes to `Completed`
    /// (withdrawal finalized), or hands off to `execute_deferred_remint`.
    pub async fn process_pending_remints(
        state: &mut SenderState,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) {
        super::remint::process_pending_remints(state, storage_tx).await
    }

    /// Drives `execute_deferred_remint` for a single matured entry.
    /// Skips the queue-management layer of `process_pending_remints`,
    /// allowing tests to pin the `attempt_remint → status update`
    /// transition in isolation.
    pub async fn execute_deferred_remint(
        state: &SenderState,
        entry: super::types::PendingRemint,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> DeferredRemintOutcome {
        super::remint::execute_deferred_remint(state, entry, storage_tx).await
    }

    /// Drives a single `poll_in_flight` cycle. Drains
    /// `state.in_flight`, calls `getSignatureStatuses`, and either
    /// routes results via `route_poll_results` or — on RPC error —
    /// puts the batch back unchanged for the next tick.
    pub async fn poll_in_flight(
        state: &mut SenderState,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) {
        super::transaction::poll_in_flight(state, storage_tx).await
    }

    /// Drives `handle_confirmation_result` end-to-end. Used by tests
    /// that synthesise a `Result<ConfirmationResult, TransactionError>`
    /// to pin which on-chain error arm routes where without going
    /// through the wire layer.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_confirmation_result(
        state: &mut SenderState,
        result: Result<
            crate::operator::utils::transaction_util::ConfirmationResult,
            crate::error::TransactionError,
        >,
        signature: solana_sdk::signature::Signature,
        compute_unit_price: Option<u64>,
        ctx: &super::types::TransactionContext,
        instruction: super::types::InstructionWithSigners,
        retry_policy: crate::operator::utils::instruction_util::RetryPolicy,
        extra_error_checks_policy: &crate::operator::utils::instruction_util::ExtraErrorCheckPolicy,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) {
        super::transaction::handle_confirmation_result(
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
        .await
    }

    /// Drives `send_and_confirm` end-to-end against whatever RPC the
    /// `state.rpc_client` is wired to. Fire-and-forget: results land on
    /// `storage_tx` and `state.{retry_counts, in_flight, pending_remints}`.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_send_and_confirm(
        state: &mut SenderState,
        instruction: super::types::InstructionWithSigners,
        compute_unit_price: Option<u64>,
        ctx: &super::types::TransactionContext,
        retry_policy: crate::operator::utils::instruction_util::RetryPolicy,
        extra_error_checks_policy: &crate::operator::utils::instruction_util::ExtraErrorCheckPolicy,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) {
        super::transaction::send_and_confirm(
            state,
            instruction,
            compute_unit_price,
            ctx,
            retry_policy,
            extra_error_checks_policy,
            storage_tx,
        )
        .await
    }

    /// Drives one pass of the rotation driver: reads what is still unreleased,
    /// compares it against the chain, and arms a rotation if one is owed.
    pub async fn originate_rotation_if_needed(state: &mut SenderState) {
        super::proof::originate_rotation_if_needed(state).await
    }

    /// Takes an armed rotation once the in-flight barrier allows it, which is
    /// what the periodic tick does before submitting one.
    pub async fn take_pending_rotation_if_ready(
        state: &mut SenderState,
    ) -> Option<Box<private_channel_escrow_program_client::instructions::RotateBitmapBuilder>> {
        super::proof::take_pending_rotation_if_ready(state).await
    }
}

use crate::error::OperatorError;
use crate::operator::utils::instruction_util::{
    ExtraErrorCheckPolicy, RetryPolicy, TransactionBuilder,
};
use crate::operator::RpcClientWithRetry;
use crate::storage::common::storage::Storage;
use crate::PrivateChannelIndexerConfig;
use crate::ProgramType;
use private_channel_metrics::MetricLabel;
use solana_sdk::commitment_config::CommitmentLevel;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use proof::{originate_rotation_if_needed, take_pending_rotation_if_ready};
use transaction::{
    handle_transaction_submission, poll_in_flight, route_poll_results, run_poll_task,
    send_and_confirm, GenerationWindow,
};
use types::{PollTaskResult, SenderState};

/// Advisory-lock keys per sender role. Distinct, namespaced 64-bit values
/// (ASCII tags, matching `TRUNCATE_ADVISORY_LOCK_ID`) so senders never collide
/// with each other, the truncate lock, or third-party tooling that grabs
/// small-integer advisory locks on a shared database.
const ESCROW_SENDER_LOCK_KEY: i64 = 0x53_4E_44_5F_45_53_43_52; // "SND_ESCR"
const WITHDRAW_SENDER_LOCK_KEY: i64 = 0x53_4E_44_5F_57_44_52_57; // "SND_WDRW"

/// How often the driver asks whether a rotation is owed. A generation covers
/// tens of thousands of nonces, so this is slow on purpose: the cost of a late
/// rotation is one interval of extra wait for withdrawals that are already
/// parked, and the cost of a fast one is a database read on every tick forever.
const ROTATION_ORIGINATION_INTERVAL: Duration = Duration::from_secs(5);

/// How often the sender re-proves it owns its advisory lock.
///
/// Well under the 32s finality delay and the 60s recovery tick, so a sender that
/// loses its lock is gone before anything it still holds matures. Deliberately
/// not operator-configurable: the value only makes sense alongside the probe and
/// fenced-write timeouts it is tuned against, and tests inject their own.
pub const SENDER_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Advisory-lock key per operator role. Distinct keys so an escrow and a
/// withdraw sender never contend on the same lock if they share a database.
pub fn sender_lock_key(program_type: ProgramType) -> i64 {
    match program_type {
        ProgramType::Escrow => ESCROW_SENDER_LOCK_KEY,
        ProgramType::Withdraw => WITHDRAW_SENDER_LOCK_KEY,
    }
}

/// Sends transactions to the blockchain and updates their status
///
/// Receives TransactionBuilder (either ReleaseFunds or Mint) from processor,
/// submits to blockchain, and handles failures
#[allow(clippy::too_many_arguments)]
pub async fn run_sender(
    config: &PrivateChannelIndexerConfig,
    operator_commitment: CommitmentLevel,
    mut processor_rx: mpsc::Receiver<TransactionBuilder>,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: tokio_util::sync::CancellationToken,
    storage: Arc<Storage>,
    retry_max_attempts: u32,
    confirmation_poll_interval_ms: u64,
    source_rpc_client: Option<Arc<RpcClientWithRetry>>,
    sender_lock_heartbeat_interval: Duration,
) -> Result<(), OperatorError> {
    info!("Starting sender");

    let instance_pda = match config.program_type {
        ProgramType::Withdraw => config.escrow_instance_id,
        ProgramType::Escrow => None,
    };

    let mut state = SenderState::new(
        config,
        operator_commitment,
        instance_pda,
        storage,
        retry_max_attempts,
        confirmation_poll_interval_ms,
        source_rpc_client,
    )?;

    // Refuse to start if another sender for this role already holds the lock.
    // Held for the rest of run_sender; released on drop or process crash. Stops
    // two overlapping senders (e.g. a rolling restart) from both reminting the
    // same row before either confirms on-chain.
    //
    // Declared after `state` on purpose. Locals drop in reverse declaration
    // order, so the guard drops first and the storage Arc carrying the pool is
    // still alive for the release query. Moving it above `state` would invert
    // that and must not be done.
    let _sender_lock = match state
        .storage
        .try_acquire_sender_lock(
            sender_lock_key(config.program_type),
            config.program_type.as_label(),
            cancellation_token.clone(),
            sender_lock_heartbeat_interval,
        )
        .await?
    {
        Some(guard) => guard,
        None => {
            return Err(OperatorError::SenderAlreadyRunning {
                program_type: config.program_type,
            });
        }
    };

    // Re-hydrate the deferred remint queue from any PendingRemint rows written
    // before a crash. These will be picked up by process_pending_remints on the
    // next tick
    state.recover_pending_remints(&storage_tx).await?;

    // Periodic check for pending rotation (every 500ms)
    let mut rotation_check_interval = interval(Duration::from_millis(500));

    // Rotation is needed once per generation of nonces, so the check that starts
    // one runs far more slowly than the one that submits it. The slower cadence
    // also paces re-origination after a rotation has used up its send budget.
    let mut rotation_origination_interval = interval(ROTATION_ORIGINATION_INTERVAL);

    // Channel for the poll task to deliver batched confirmation results back to the sender loop.
    let (poll_result_tx, mut poll_result_rx) = mpsc::channel(32);

    // Separate shutdown token for the poll task
    let poll_shutdown = tokio_util::sync::CancellationToken::new();

    // Spawn the dedicated poll task.
    //
    // The task handles confirmed-success entirely in-task (fires storage update +
    // metric) and pushes unconfirmed entries straight back to `in_flight`.  Only
    // on-chain errors and confirmation timeouts — rare events — come back via
    // `poll_result_rx` as `PollTaskResult::NeedsRouting`.
    // The task blocks on `in_flight.notify` when the queue is empty — zero CPU idle.
    let poll_task_handle = tokio::spawn(run_poll_task(
        state.in_flight.clone(),
        poll_result_tx,
        state.rpc_client.clone(),
        storage_tx.clone(),
        config.program_type,
        state.confirmation_poll_interval_ms,
        poll_shutdown.clone(),
    ));

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Sender received cancellation signal, draining pipeline...");
                // Drain processor channel so all pending txs are submitted.
                let mut drained_count = 0;
                while let Some(tx_builder) = processor_rx.recv().await {
                    handle_transaction_submission(&mut state, tx_builder, &storage_tx).await;
                    drained_count += 1;
                }
                info!("Sender drained {} new transactions from channel", drained_count);
                // Stop the poll task before draining so it no longer races with
                // drain_in_flight over state.in_flight entries.  Any NeedsRouting
                // results it may have queued in poll_result_rx are discarded — those
                // transactions remain in Processing and are recovered on restart.
                poll_shutdown.cancel();
                drop(poll_result_rx);
                let _ = poll_task_handle.await;
                drain_in_flight(&mut state, &storage_tx).await;
                break;
            }

            // Receive results from the dedicated poll task.
            //
            // In the common case this arm carries only `ConfirmedSuccess` items
            // (O(1) mint_builders cleanup each).  `NeedsRouting` items — on-chain
            // errors and confirmation timeouts — are rare and go through the full
            // route_poll_results path.
            Some(results) = poll_result_rx.recv() => {
                let mut to_route = Vec::new();
                let mut confirmed_count = 0usize;
                for result in results {
                    match result {
                        PollTaskResult::ConfirmedSuccess(txn_id) => {
                            confirmed_count += 1;
                            if let Some(id) = txn_id {
                                state.mint_builders.remove(&id);
                            }
                        }
                        PollTaskResult::NeedsRouting(tx, status) => {
                            to_route.push((*tx, status));
                        }
                    }
                }
                debug!(
                    confirmed = confirmed_count,
                    needs_routing = to_route.len(),
                    in_flight = state.in_flight.len(),
                    "Poll results received from poll task"
                );
                if !to_route.is_empty() {
                    route_poll_results(&mut state, to_route, &storage_tx).await;
                }
            }

            _ = rotation_origination_interval.tick() => {
                originate_rotation_if_needed(&mut state).await;
            }

            _ = rotation_check_interval.tick() => {
                // Check if pending rotation can now be executed
                if let Some(rotation_builder) = take_pending_rotation_if_ready(&mut state).await {
                    info!("Executing queued RotateBitmap transaction");
                    let tx_builder = TransactionBuilder::RotateBitmap(rotation_builder);
                    handle_transaction_submission(&mut state, tx_builder, &storage_tx).await;
                }

                // Process matured deferred remints
                remint::process_pending_remints(&mut state, &storage_tx).await;

                drain_rotation_retry_queue(&mut state, &storage_tx).await;
            }

            // Back-pressure: stop consuming new transactions when in_flight is full.
            // `available_permits()` reflects both in-flight entries AND spawned send
            // tasks that have not yet pushed to the queue, so this guard is tight.
            // The channel fills up → processor blocks → fetcher stops polling the DB.
            // Resumes automatically once the poll task confirms entries and permits are returned.
            tx_builder = processor_rx.recv(), if state.semaphore.available_permits() > 0 => {
                if let Some(tx_builder) = tx_builder {
                    debug!(
                        in_flight = state.in_flight.len(),
                        available_permits = state.semaphore.available_permits(),
                        processor_channel_capacity = processor_rx.len(),
                        "Sender received transaction from processor"
                    );
                    handle_transaction_submission(&mut state, tx_builder, &storage_tx).await;
                } else {
                    info!("Sender channel closed");
                    // Stop the poll task before draining (same reasoning as the
                    // cancellation path above — prevent races over in_flight).
                    poll_shutdown.cancel();
                    drop(poll_result_rx);
                    let _ = poll_task_handle.await;
                    drain_in_flight(&mut state, &storage_tx).await;
                    break;
                }
            }
        }
    }

    info!("Sender stopped gracefully");
    Ok(())
}

/// Retry withdrawals refused for a generation that had not opened yet: one
/// attempt per entry per tick, and none while a rotation is still queued.
///
/// Every entry here has a `Parked` row behind it, so each one is taken back with
/// a CAS before it is broadcast. Only a positive answer authorises the send: an
/// unparked row belongs to someone else now, and an unreadable one says nothing
/// about who owns it.
pub(super) async fn drain_rotation_retry_queue(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    for (ctx, instruction) in std::mem::take(&mut state.rotation_retry_queue) {
        let Some(transaction_id) = ctx.transaction_id else {
            error!("Dropping a queued release that carries no row to unpark");
            continue;
        };

        // Read before any CAS, so an entry the drain cannot act on strands no row mid-transition.
        let Some(nonce) = ctx.withdrawal_nonce else {
            error!(
                transaction_id,
                "Dropping a queued release that names no nonce; leaving its row for recovery"
            );
            continue;
        };

        if state.pending_rotation.is_some() {
            hold_queued_release(state, ctx, instruction, transaction_id).await;
            continue;
        }

        // The rotation this entry waits on can be lost, and a send into a shut window pays a fee to be refused.
        let (window, chain_generation) = state.release_window(nonce).await;
        match window {
            GenerationWindow::Open => {}
            GenerationWindow::NotYetOpen => {
                hold_queued_release(state, ctx, instruction, transaction_id).await;
                continue;
            }
            // No rotation reopens this window, so holding the row only starves the sweep that would settle it.
            GenerationWindow::Closed => {
                error!(
                    nonce,
                    chain_generation,
                    transaction_id,
                    "Queued release belongs to a generation the bitmap has rotated past; \
                     leaving its parked row for recovery"
                );
                continue;
            }
        }

        match state.storage.try_unpark_to_processing(transaction_id).await {
            // The park heartbeat and this unpark both bumped the row, so the token
            // the entry arrived with is dead; carry the one we just won.
            Ok(Some(lease)) => {
                state.release_leases.insert(nonce, lease);
            }
            // Another actor took the row, so this sender no longer owns the
            // work and must stop rather than broadcast it a second time.
            Ok(None) => {
                warn!(
                    transaction_id,
                    "Queued release is no longer parked; another actor owns it now"
                );
                continue;
            }
            // Not knowing who owns the row is not permission to send. The row is
            // still parked, so the entry simply waits for a tick that can read it.
            Err(e) => {
                warn!(transaction_id, "Could not unpark the queued release: {e}");
                state.rotation_retry_queue.push((ctx, instruction));
                continue;
            }
        }

        info!(
            trace_id = ctx.trace_id.as_deref().unwrap_or("none"),
            "Retrying blocked nonce {} after rotation", nonce
        );
        // Back in flight from here, so a queued rotation waits on it.
        state.in_flight_withdrawals.insert(nonce);
        let compute_unit_price = instruction.compute_unit_price;
        // The same policies `TransactionBuilder::ReleaseFunds` reports.
        send_and_confirm(
            state,
            instruction,
            compute_unit_price,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            storage_tx,
        )
        .await;
    }
}

/// Keep waiting on a queued release, refreshing its park so recovery does not take work this sender still holds.
async fn hold_queued_release(
    state: &mut SenderState,
    ctx: types::TransactionContext,
    instruction: types::InstructionWithSigners,
    transaction_id: i64,
) {
    if let Err(e) = state.storage.try_park_processing(transaction_id).await {
        warn!(transaction_id, "Could not refresh the park: {e}");
    }
    state.rotation_retry_queue.push((ctx, instruction));
}

/// Wait for all in-flight fire-and-forget transactions to reach a terminal state.
///
/// Polls at state.confirmation_poll_interval_ms intervals with a 30-second wall-clock timeout.  Called on both
/// graceful shutdown paths (cancellation and channel close) so no confirmed Mint
/// transactions are orphaned at process exit.
///
/// If the timeout expires with entries still in-flight, a warning is logged and
/// the operator exits anyway — on restart the processor will re-emit any transactions
/// that lack a terminal DB status, and the idempotency memo check will prevent
/// duplicate mints if the original tx did land.
async fn drain_in_flight(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    if state.in_flight.is_empty() {
        return;
    }

    info!(
        count = state.in_flight.len(),
        "Draining in-flight transactions before shutdown"
    );

    let timeout_at = tokio::time::Instant::now() + Duration::from_secs(30);

    while !state.in_flight.is_empty() {
        if tokio::time::Instant::now() >= timeout_at {
            warn!(
                count = state.in_flight.len(),
                "Shutdown drain timeout — {} in-flight transactions unresolved; \
                 they will be re-processed on restart",
                state.in_flight.len(),
            );
            return;
        }

        poll_in_flight(state, storage_tx).await;

        if !state.in_flight.is_empty() {
            tokio::time::sleep(Duration::from_millis(state.confirmation_poll_interval_ms)).await;
        }
    }

    info!("All in-flight transactions resolved");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_CONFIRMATION_POLL_INTERVAL_MS;
    use crate::config::{PostgresConfig, ProgramType, StorageType};
    use crate::operator::sender::test_support::{
        mock_with_parked_row, mock_with_processing_row, row_status, row_updated_at,
        sender_state as make_sender_state, sender_state_with_storage,
    };
    use crate::operator::sender::types::{
        InFlightTx, InstructionWithSigners, TransactionContext, MAX_IN_FLIGHT,
    };
    use crate::operator::utils::instruction_util::{
        ExtraErrorCheckPolicy, RetryPolicy, TransactionKind,
    };
    use crate::storage::common::models::TransactionStatus;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::PrivateChannelIndexerConfig;
    use solana_keychain::Signer;
    use solana_sdk::commitment_config::CommitmentLevel;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use std::sync::Arc;
    use tokio::sync::{mpsc, Semaphore};
    use tokio_util::sync::CancellationToken;

    fn make_in_flight_tx(txn_id: i64) -> InFlightTx {
        InFlightTx {
            signature: Signature::new_unique(),
            ctx: TransactionContext {
                kind: TransactionKind::Mint,
                transaction_id: Some(txn_id),
                withdrawal_nonce: None,
                trace_id: None,
                deposit_claim_lease: None,
            },
            instruction: InstructionWithSigners {
                instructions: vec![],
                fee_payer: Pubkey::default(),
                signers: Vec::<&'static Signer>::new(),
                compute_unit_price: None,
                compute_budget: None,
            },
            compute_unit_price: None,
            retry_policy: RetryPolicy::None,
            extra_error_checks_policy: ExtraErrorCheckPolicy::None,
            poll_attempts: 0,
            resend_count: 0,
            persisted: false,
            permit: Arc::new(Semaphore::new(MAX_IN_FLIGHT))
                .try_acquire_owned()
                .unwrap(),
        }
    }

    fn minimal_config() -> PrivateChannelIndexerConfig {
        PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            storage_type: StorageType::Postgres,
            rpc_url: "http://localhost:8899".to_string(),
            fallback_rpc_url: None,
            source_rpc_url: None,
            postgres: PostgresConfig {
                database_url: "postgresql://localhost/test".to_string(),
                max_connections: 5,
            },
            escrow_instance_id: None,
        }
    }

    /// Cancellation with an already-closed processor channel must drain zero transactions
    /// and return Ok(()), confirming the graceful-shutdown path terminates without hanging.
    #[tokio::test]
    async fn run_sender_exits_when_cancelled_with_empty_channel() {
        let config = minimal_config();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let (processor_tx, processor_rx) = mpsc::channel(10);
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();

        // Cancel before calling run_sender so the cancellation arm fires immediately
        cancellation_token.cancel();
        // Drop processor sender so the drain loop (while let Some) completes quickly
        drop(processor_tx);

        let result = run_sender(
            &config,
            CommitmentLevel::Confirmed,
            processor_rx,
            storage_tx,
            cancellation_token,
            storage,
            3,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
            None,
            SENDER_LOCK_HEARTBEAT_INTERVAL,
        )
        .await;

        assert!(result.is_ok());
    }

    /// When the processor drops its sender before any messages are sent, run_sender must
    /// detect the closed channel in the normal recv arm and return Ok(()) without cancellation.
    #[tokio::test]
    async fn run_sender_exits_when_processor_channel_closed() {
        let config = minimal_config();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));

        // Create a channel and immediately close the sender side
        let processor_rx = {
            let (tx, rx) = mpsc::channel::<TransactionBuilder>(10);
            drop(tx);
            rx
        };

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();

        let result = run_sender(
            &config,
            CommitmentLevel::Confirmed,
            processor_rx,
            storage_tx,
            cancellation_token,
            storage,
            3,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
            None,
            SENDER_LOCK_HEARTBEAT_INTERVAL,
        )
        .await;

        assert!(result.is_ok());
    }

    // ── drain_rotation_retry_queue ────────────────────────────────────

    /// Script a node that accepts the broadcast, refuses it with the generation
    /// error, and reports a bitmap still one generation behind the nonce.
    ///
    /// That is exactly the state a queued withdrawal is retried into before its
    /// rotation lands, which is the case the drain has to survive.
    async fn mock_generation_refusal(server: &mut mockito::ServerGuard) -> mockito::Mock {
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
            .create_async()
            .await;

        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "slot": 10,
                            "confirmations": null,
                            "err": {"InstructionError": [0, {"Custom": 13}]},
                            "status": {"Err": {"InstructionError": [0, {"Custom": 13}]}},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create_async()
            .await;

        crate::operator::sender::test_support::mock_bitmap_account(server, 0, &[]);

        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    // The client rejects a reply carrying another signature.
                    "result": Signature::default().to_string()
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await
    }

    /// The row every queued release in these tests belongs to.
    const QUEUED_ROW: i64 = 70;

    /// A `sendTransaction` route that must never be taken, behind a blockhash
    /// route that works, so the send would go through if anything let it.
    async fn expect_no_broadcast(server: &mut mockito::ServerGuard) -> mockito::Mock {
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
            .create_async()
            .await;

        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"sendTransaction""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": Signature::default().to_string()
                })
                .to_string(),
            )
            .expect(0)
            .create_async()
            .await
    }

    fn queued_release(nonce: u64) -> (TransactionContext, InstructionWithSigners) {
        (
            TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(QUEUED_ROW),
                withdrawal_nonce: Some(nonce),
                trace_id: Some("trace-70".to_string()),
                deposit_claim_lease: None,
            },
            InstructionWithSigners {
                instructions: vec![],
                fee_payer: Pubkey::new_unique(),
                signers: Vec::<&'static Signer>::new(),
                compute_unit_price: None,
                compute_budget: None,
            },
        )
    }

    /// A withdrawal the program refuses goes straight back on the queue, so a
    /// drain that reads the live queue re-sends the same nonce over and over
    /// within one tick and pays a fee for every refusal. One tick must cost one
    /// attempt per entry, whatever the rejection handler does behind it.
    #[tokio::test]
    async fn rotation_retry_drain_gives_each_entry_one_attempt_per_tick() {
        let mut server = mockito::Server::new_async().await;
        let send = mock_generation_refusal(&mut server).await;

        // Inside the window the bitmap reports, so the send happens and its refusal is what the entry survives.
        let nonce = 5;
        let mut state = sender_state_with_storage(&server.url(), mock_with_parked_row(QUEUED_ROW));
        state.instance_pda = Some(Pubkey::new_unique());
        state.rotation_retry_queue.push(queued_release(nonce));

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert_eq!(
            state.rotation_retry_queue.len(),
            1,
            "a still-blocked withdrawal must wait for the next tick"
        );
        assert_eq!(
            state.retry_counts.get(&nonce).copied().unwrap_or(0),
            0,
            "a refusal we already expected must not spend the withdrawal's retries"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "a queued withdrawal has no terminal outcome yet"
        );
    }

    /// A lost rotation releases the drain's only hold, and each tick then pays a fee per entry for a certain refusal.
    #[tokio::test]
    async fn rotation_retry_drain_holds_an_entry_whose_window_has_not_opened() {
        let mut server = mockito::Server::new_async().await;
        let send = expect_no_broadcast(&mut server).await;
        crate::operator::sender::test_support::mock_bitmap_account(&mut server, 0, &[]);

        let nonce = crate::operator::bitmap_constants::NONCES_PER_GENERATION;
        let mock = mock_with_parked_row(QUEUED_ROW);
        let before = row_updated_at(&mock, QUEUED_ROW).expect("seeded row");
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        state.rotation_retry_queue.push(queued_release(nonce));

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert_eq!(
            state.rotation_retry_queue.len(),
            1,
            "the entry still waits for the window to open"
        );
        assert_eq!(
            row_status(&mock, QUEUED_ROW),
            Some(TransactionStatus::Parked),
            "a release that was never sent must stay parked"
        );
        assert!(
            row_updated_at(&mock, QUEUED_ROW).expect("row still present") > before,
            "a held entry must keep its row fresh or recovery will take it"
        );
    }

    /// A window the chain rotated past never reopens, so holding the row only keeps recovery from settling it.
    #[tokio::test]
    async fn rotation_retry_drain_releases_an_entry_whose_window_has_closed() {
        let mut server = mockito::Server::new_async().await;
        let send = expect_no_broadcast(&mut server).await;
        crate::operator::sender::test_support::mock_bitmap_account(&mut server, 2, &[]);

        let mock = mock_with_parked_row(QUEUED_ROW);
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        state.rotation_retry_queue.push(queued_release(7));

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert!(
            state.rotation_retry_queue.is_empty(),
            "an entry no rotation can unblock must stop being held"
        );
        assert_eq!(
            row_status(&mock, QUEUED_ROW),
            Some(TransactionStatus::Parked),
            "the row is left where the recovery sweep can find it"
        );
    }

    /// An entry with no nonce must be caught before the row moves, or the sender dies leaving it mid-transition.
    #[tokio::test]
    async fn rotation_retry_drain_drops_an_entry_that_names_no_nonce() {
        let mut server = mockito::Server::new_async().await;
        let send = expect_no_broadcast(&mut server).await;

        let mock = mock_with_parked_row(QUEUED_ROW);
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        let (mut ctx, instruction) = queued_release(7);
        ctx.withdrawal_nonce = None;
        state.rotation_retry_queue.push((ctx, instruction));

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert!(state.rotation_retry_queue.is_empty());
        assert_eq!(
            row_status(&mock, QUEUED_ROW),
            Some(TransactionStatus::Parked),
            "the row must not be left mid-transition"
        );
    }

    /// The drain takes the row back before it broadcasts. Sending first would
    /// put a release on chain for a row this sender may no longer own.
    ///
    /// The refusal that follows the send would normally re-park the row, so the
    /// re-park is armed to fail here and the row's final state still shows the
    /// unpark that preceded the broadcast.
    #[tokio::test]
    async fn rotation_retry_drain_unparks_before_it_sends() {
        let mut server = mockito::Server::new_async().await;
        let send = mock_generation_refusal(&mut server).await;

        // Inside the window, so the drain reaches the broadcast under test.
        let nonce = 5;
        let mock = mock_with_parked_row(QUEUED_ROW);
        mock.set_should_fail("try_park_processing", true);
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        state.rotation_retry_queue.push(queued_release(nonce));

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert_eq!(
            row_status(&mock, QUEUED_ROW),
            Some(TransactionStatus::Processing),
            "the row must be taken back out of Parked before the broadcast"
        );
    }

    /// `Ok(false)` means the row is no longer parked, so another actor owns it.
    /// Broadcasting anyway would send work this sender has already lost.
    #[tokio::test]
    async fn rotation_retry_drain_drops_an_entry_it_no_longer_owns() {
        let mut server = mockito::Server::new_async().await;
        let send = expect_no_broadcast(&mut server).await;

        // A Processing row: recovery or another sender already took it back.
        let mock = mock_with_processing_row(QUEUED_ROW);
        let mut state = sender_state_with_storage(&server.url(), mock);
        state.instance_pda = Some(Pubkey::new_unique());
        state.rotation_retry_queue.push(queued_release(7));

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert!(
            state.rotation_retry_queue.is_empty(),
            "a lost lease must drop the entry, not resend it"
        );
    }

    /// An unreadable ownership answer is not permission to send. The row is
    /// still parked durably, so the entry waits for a tick that can read it.
    #[tokio::test]
    async fn rotation_retry_drain_keeps_an_entry_whose_unpark_errored() {
        let mut server = mockito::Server::new_async().await;
        let send = expect_no_broadcast(&mut server).await;

        let mock = mock_with_parked_row(QUEUED_ROW);
        mock.set_should_fail("try_unpark_to_processing", true);
        let mut state = sender_state_with_storage(&server.url(), mock.clone());
        state.instance_pda = Some(Pubkey::new_unique());
        state.rotation_retry_queue.push(queued_release(7));

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        send.assert_async().await;
        assert_eq!(
            state.rotation_retry_queue.len(),
            1,
            "an unreadable answer retries next tick rather than sending"
        );
        assert_eq!(
            row_status(&mock, QUEUED_ROW),
            Some(TransactionStatus::Parked),
            "the durable copy is untouched by a failed read"
        );
    }

    /// A rotation wait can outlast the stale threshold. Without the per-tick
    /// refresh the recovery sweep ages the row out and requeues it to Pending
    /// underneath an entry this sender still holds.
    #[tokio::test]
    async fn rotation_retry_drain_refreshes_the_park_while_it_holds_an_entry() {
        let mock = mock_with_parked_row(QUEUED_ROW);
        let before = row_updated_at(&mock, QUEUED_ROW).expect("seeded row");
        let mut state = sender_state_with_storage("http://localhost:8899", mock.clone());
        state.rotation_retry_queue.push(queued_release(7));
        state.pending_rotation = Some(pending_rotation_builder());

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        assert_eq!(state.rotation_retry_queue.len(), 1);
        assert!(
            row_updated_at(&mock, QUEUED_ROW).expect("row still present") > before,
            "a held entry must keep its row fresh or recovery will take it"
        );
    }

    fn pending_rotation_builder(
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

    /// A queued rotation is what the blocked withdrawals are waiting on.
    #[tokio::test]
    async fn rotation_retry_drain_holds_entries_while_a_rotation_is_queued() {
        let mock = mock_with_parked_row(QUEUED_ROW);
        let mut state = sender_state_with_storage("http://localhost:8899", mock);
        state.rotation_retry_queue.push(queued_release(7));
        state.pending_rotation = Some(pending_rotation_builder());

        let (storage_tx, _storage_rx) = mpsc::channel(10);
        drain_rotation_retry_queue(&mut state, &storage_tx).await;

        assert_eq!(state.rotation_retry_queue.len(), 1);
        assert!(state.in_flight_withdrawals.is_empty());
    }

    // ── drain_in_flight ───────────────────────────────────────────────

    /// An empty in-flight queue must return immediately without any RPC calls or
    /// storage updates.
    #[tokio::test]
    async fn drain_in_flight_empty_queue_returns_immediately() {
        let mut state = make_sender_state("http://localhost:8899");
        assert!(state.in_flight.is_empty());

        let (storage_tx, mut storage_rx) = mpsc::channel(10);
        drain_in_flight(&mut state, &storage_tx).await;

        assert!(state.in_flight.is_empty());
        assert!(storage_rx.try_recv().is_err(), "no storage update expected");
    }

    /// When in-flight entries never confirm, drain_in_flight must stop after the
    /// 30-second wall-clock timeout and log a warning rather than hanging forever.
    #[tokio::test(start_paused = true)]
    async fn drain_in_flight_timeout_exits_with_unresolved_entries() {
        let mut server = mockito::Server::new_async().await;

        // Always return null — entry never confirms.
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
                    "result": { "context": {"slot": 1}, "value": [null] }
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create();

        let mut state = make_sender_state(&server.url());
        state.confirmation_poll_interval_ms = 100;
        state.in_flight.push(make_in_flight_tx(1));

        let (storage_tx, _storage_rx) = mpsc::channel(10);

        let drain = tokio::spawn(async move {
            drain_in_flight(&mut state, &storage_tx).await;
            state.in_flight.len() // return remaining count to assert on
        });

        // Yield once so the spawned task starts and computes `timeout_at` based on
        // time=0 (before we advance).  After this yield drain is blocked inside
        // poll_in_flight awaiting the RPC response.
        tokio::task::yield_now().await;

        // Advance the mock clock past the 30-second timeout.  The pending 100ms
        // sleep inside drain_in_flight will also be resolved by this advance.
        tokio::time::advance(Duration::from_secs(31)).await;

        let remaining = tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("drain must complete after timeout advance")
            .expect("task must not panic");

        assert_eq!(
            remaining, 1,
            "unresolved entry must still be in in_flight after timeout"
        );
    }
}
