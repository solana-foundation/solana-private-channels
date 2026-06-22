mod mint;
mod proof;
mod remint;
mod state;
mod transaction;
pub mod types;

pub use mint::{find_existing_mint_signature_with_memo, JitOutcome};
pub(crate) use remint::{classify_release_signatures, SigFinality};
pub(crate) use state::validate_smt_root;
pub use types::TransactionStatusUpdate;

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only re-exports of `pub(super)` constructors/recovery paths.
    //! Gated behind `test-mock-storage` so production builds get the
    //! same narrow API surface they always have.
    use super::*;
    use solana_sdk::commitment_config::CommitmentLevel;
    use std::sync::Arc;

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
        entry: &super::types::PendingRemint,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) {
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
}

use crate::error::OperatorError;
use crate::operator::utils::instruction_util::TransactionBuilder;
use crate::operator::ReleaseFundsBuilderWithNonce;
use crate::operator::RpcClientWithRetry;
use crate::storage::common::storage::Storage;
use crate::PrivateChannelIndexerConfig;
use crate::ProgramType;
use solana_sdk::commitment_config::CommitmentLevel;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use proof::take_pending_rotation_if_ready;
use transaction::{
    handle_transaction_submission, poll_in_flight, route_poll_results, run_poll_task,
};
use types::{PollTaskResult, SenderState};

/// Advisory-lock keys per sender role. Distinct, namespaced 64-bit values
/// (ASCII tags, matching `TRUNCATE_ADVISORY_LOCK_ID`) so senders never collide
/// with each other, the truncate lock, or third-party tooling that grabs
/// small-integer advisory locks on a shared database.
const ESCROW_SENDER_LOCK_KEY: i64 = 0x53_4E_44_5F_45_53_43_52; // "SND_ESCR"
const WITHDRAW_SENDER_LOCK_KEY: i64 = 0x53_4E_44_5F_57_44_52_57; // "SND_WDRW"

/// Advisory-lock key per operator role. Distinct keys so an escrow and a
/// withdraw sender never contend on the same lock if they share a database.
fn sender_lock_key(program_type: ProgramType) -> i64 {
    match program_type {
        ProgramType::Escrow => ESCROW_SENDER_LOCK_KEY,
        ProgramType::Withdraw => WITHDRAW_SENDER_LOCK_KEY,
    }
}

/// Sends transactions to the blockchain and updates their status
///
/// Receives TransactionBuilder (either ReleaseFunds or Mint) from processor,
/// completes with SMT proofs if needed, submits to blockchain, and handles failures
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
    let _sender_lock = match state
        .storage
        .try_acquire_sender_lock(sender_lock_key(config.program_type))
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

            _ = rotation_check_interval.tick() => {
                // Check if pending rotation can now be executed
                if let Some(rotation_builder) = take_pending_rotation_if_ready(&mut state) {
                    info!("Executing queued ResetSmtRoot transaction");
                    let tx_builder = TransactionBuilder::ResetSmtRoot(rotation_builder);
                    handle_transaction_submission(&mut state, tx_builder, &storage_tx).await;
                }

                // Process matured deferred remints
                remint::process_pending_remints(&mut state, &storage_tx).await;

                // Process any transactions that were blocked by rotation
                while let Some((ctx, builder)) = state.rotation_retry_queue.pop() {
                    let nonce = ctx.withdrawal_nonce.expect("rotation retry must have nonce");
                    let transaction_id = ctx.transaction_id.expect("rotation retry must have transaction_id");
                    let trace_id = ctx.trace_id.clone().expect("rotation retry must have trace_id");
                    let remint_info = state.remint_cache.get(&nonce).cloned();
                    if remint_info.is_none() {
                        error!("Missing remint_info for rotation retry nonce {} - remint will not be possible on failure", nonce);
                    }
                    info!(trace_id = %trace_id, "Retrying blocked nonce {} after rotation", nonce);
                    let tx_builder = TransactionBuilder::ReleaseFunds(Box::new(
                        ReleaseFundsBuilderWithNonce { builder, nonce, transaction_id, trace_id, remint_info },
                    ));
                    handle_transaction_submission(&mut state, tx_builder, &storage_tx).await;
                }
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
    use crate::operator::sender::types::{
        InFlightQueue, InFlightTx, InstructionWithSigners, SenderState, TransactionContext,
        MAX_IN_FLIGHT,
    };
    use crate::operator::utils::instruction_util::{ExtraErrorCheckPolicy, RetryPolicy};
    use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
    use crate::operator::MintCache;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::PrivateChannelIndexerConfig;
    use solana_keychain::Signer;
    use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::{mpsc, Semaphore};
    use tokio_util::sync::CancellationToken;

    fn make_sender_state(rpc_url: &str) -> SenderState {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            rpc_url.to_string(),
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
            confirmation_poll_interval_ms: 1,
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

    fn make_in_flight_tx(txn_id: i64) -> InFlightTx {
        InFlightTx {
            signature: Signature::new_unique(),
            ctx: TransactionContext {
                transaction_id: Some(txn_id),
                withdrawal_nonce: None,
                trace_id: None,
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
        )
        .await;

        assert!(result.is_ok());
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
