use crate::{
    config::{BackfillConfig, ProgramType},
    error::{indexer::ReconciliationError, DataSourceError, IndexerError, StorageError},
    indexer::{
        backfill::BackfillService, checkpoint::CheckpointWriter,
        datasource::rpc_polling::rpc::RpcPoller, transaction_processor::TransactionProcessor,
    },
    operator::{
        enumerate_consumed_mints, fetch_consumed_nonces, find_withdrawal_bitmap_pda, ConsumedSet,
        RetryConfig, RpcClientWithRetry, CONSUMED_SET_PAGE_SIZE,
    },
    shutdown_utils::WRITER_STOP_TIMEOUT,
    storage::common::models::ResyncBlockers,
    storage::common::storage::live_lock::{
        under_live_lock, LiveLockMode, LIVE_LOCK_HEARTBEAT_INTERVAL, LIVE_LOCK_UNCONFIRMED_BUDGET,
    },
    storage::common::storage::resync_state::resync_halt_reason,
    storage::postgres::lock_connection::PROBE_TIMEOUT,
    storage::Storage,
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// How far behind wall clock the bitmap RPC's finalized tip may sit and still count as
/// evidence about the live chain. Generous next to Solana's finalization, because the
/// failure it exists to catch is a node hours behind, not one a few slots behind.
const MAX_BITMAP_RPC_TIP_LAG_SECS: i64 = 120;

/// Metric label for the live-state lock this service holds.
const RESYNC_LOCK_ROLE: &str = "resync";

/// How long a worker whose lock session died can keep working: one heartbeat and probe,
/// or the full unconfirmed budget, then the wait for its aborted writers, plus a margin.
pub const STALE_HOLDER_GRACE: Duration = LIVE_LOCK_HEARTBEAT_INTERVAL
    .saturating_add(LIVE_LOCK_UNCONFIRMED_BUDGET)
    .saturating_add(PROBE_TIMEOUT)
    .saturating_add(WRITER_STOP_TIMEOUT)
    .saturating_add(Duration::from_secs(5));

/// Why a resync must not wipe its rows yet, or `None` when it may. Only own-program rows count
/// because the wipe deletes nothing else; withdraw also restarts nonces, so it needs proof none is spent.
fn wipe_blocker(
    program: ProgramType,
    blockers: &ResyncBlockers,
    genesis_slot: u64,
) -> Option<ReconciliationError> {
    if blockers.unsettled_work {
        return Some(ReconciliationError::UnsettledWork);
    }
    // The wipe deletes every own row but the rebuild replays only from genesis.
    if let Some(earliest_slot) = blockers.earliest_slot {
        if i128::from(genesis_slot) > i128::from(earliest_slot) {
            return Some(ReconciliationError::GenesisAboveExistingRows {
                genesis_slot,
                earliest_slot,
            });
        }
    }
    if program != ProgramType::Withdraw {
        return None;
    }
    if blockers.failed_withdrawals {
        return Some(ReconciliationError::ReleaseEvidenceRecorded {
            what: "failed withdrawals",
        });
    }
    if blockers.observed_releases {
        return Some(ReconciliationError::ReleaseEvidenceRecorded {
            what: "observed releases",
        });
    }
    None
}

/// How to reach the PrivateChannel and whose mints to enumerate for the consumed-set.
#[derive(Clone, Debug)]
pub struct ChannelReconcileConfig {
    /// PrivateChannel RPC URL (the chain where mints land).
    pub channel_rpc_url: String,
    /// Mint authority (admin) whose confirmed mints carry the idempotency memos.
    pub authority: Pubkey,
}

/// Resync service for rebuilding indexer database from chain history
pub struct ResyncService {
    storage: Arc<Storage>,
    rpc_poller: Arc<RpcPoller>,
    program_type: ProgramType,
    backfill_config_base: BackfillConfig,
    escrow_instance_id: Option<Pubkey>,
    // When set, the rebuild reconciles each row against the channel's existing mints and
    // fails closed if that set cannot be built. None preserves the legacy rebuild.
    channel_reconcile: Option<ChannelReconcileConfig>,
    // Solana RPC that can read the escrow's withdrawal bitmap. A withdraw rebuild refuses
    // to run without it, because it cannot prove the chain has not issued nonces yet.
    withdrawal_bitmap_rpc_url: Option<String>,
    // How often the live-state lock re-proves itself. Only tests override it.
    lock_heartbeat_interval: Duration,
    // How long to wait after taking the lock before reading anything. Only tests override it.
    stale_holder_grace: Duration,
}

impl ResyncService {
    pub fn new(
        storage: Arc<Storage>,
        rpc_poller: Arc<RpcPoller>,
        program_type: ProgramType,
        backfill_config_base: BackfillConfig,
        escrow_instance_id: Option<Pubkey>,
    ) -> Self {
        Self {
            storage,
            rpc_poller,
            program_type,
            backfill_config_base,
            escrow_instance_id,
            channel_reconcile: None,
            withdrawal_bitmap_rpc_url: None,
            lock_heartbeat_interval: LIVE_LOCK_HEARTBEAT_INTERVAL,
            stale_holder_grace: STALE_HOLDER_GRACE,
        }
    }

    /// Probe the live-state lock on `interval` instead of the production one, so a
    /// test can drive a lock loss without waiting out a rebuild.
    pub fn with_lock_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.lock_heartbeat_interval = interval;
        self
    }

    /// Wait `grace` instead of the production grace after taking the lock, so tests stay fast.
    pub fn with_stale_holder_grace(mut self, grace: Duration) -> Self {
        self.stale_holder_grace = grace;
        self
    }

    /// Enable reconcile-on-rebuild against the PrivateChannel's existing mints.
    pub fn with_channel_reconcile(mut self, config: ChannelReconcileConfig) -> Self {
        self.channel_reconcile = Some(config);
        self
    }

    /// Point the withdrawal bitmap pre-flight at the Solana RPC holding the escrow.
    pub fn with_withdrawal_bitmap_rpc(mut self, rpc_url: String) -> Self {
        self.withdrawal_bitmap_rpc_url = Some(rpc_url);
        self
    }

    /// Build the consumed-set from the channel, failing closed on any error.
    ///
    /// The production entrypoint (run_resync) guarantees a channel RPC is configured and
    /// refuses to run otherwise, so the None branch below (warn + Ok(None)) only applies to
    /// direct/test construction.
    async fn build_consumed_set(&self) -> Result<Option<Arc<ConsumedSet>>, IndexerError> {
        let Some(reconcile) = self.channel_reconcile.as_ref() else {
            warn!(
                "Resync running WITHOUT channel reconciliation (no channel RPC configured); \
                 rebuilt deposit/withdrawal rows will be pending. Only safe on an empty channel."
            );
            return Ok(None);
        };

        info!(
            "Building consumed-set from PrivateChannel authority {} before any destruction...",
            reconcile.authority
        );
        let channel_rpc = RpcClientWithRetry::with_retry_config(
            reconcile.channel_rpc_url.clone(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        );
        let set =
            enumerate_consumed_mints(&channel_rpc, &reconcile.authority, CONSUMED_SET_PAGE_SIZE)
                .await
                .map_err(|reason| {
                    error!(
                        authority = %reconcile.authority,
                        "Consumed-set enumeration failed; aborting resync before drop: {reason}"
                    );
                    IndexerError::Reconciliation(ReconciliationError::ConsumedSetUnavailable {
                        reason,
                    })
                })?;
        info!(
            "Consumed-set built: {} serviced mint(s) on the channel",
            set.len()
        );
        Ok(Some(Arc::new(set)))
    }

    /// Replay `(from_slot, to_slot]` through the rebuild's own event conversion without
    /// writing anything, and fail with `ConsumedMintMismatch` on the first channel mint
    /// that names a rebuilt event but does not pay it. The live database is untouched.
    async fn validate_before_drop(
        &self,
        backfill_service: &BackfillService,
        from_slot: u64,
        to_slot: u64,
        consumed: Arc<ConsumedSet>,
    ) -> Result<(), IndexerError> {
        info!(
            "Validating the consumed-set against source slots {}..={} before any destruction...",
            from_slot + 1,
            to_slot
        );
        let (instruction_tx, instruction_rx) = mpsc::channel(1000);
        // Validation sends no checkpoints; the receiver only keeps the type satisfied.
        let (checkpoint_tx, _checkpoint_rx) = mpsc::channel(1);
        let mut validator = TransactionProcessor::new(self.storage.clone(), checkpoint_tx)
            .with_consumed_set(consumed)
            .validate_only();
        if let Some(instance_id) = self.escrow_instance_id {
            validator = validator.with_escrow_instance_id(instance_id);
        }
        let validator_handle = tokio::spawn(async move { validator.start(instruction_rx).await });

        // run_range consumes the sender, so the validator sees the channel close when it ends.
        let fill = backfill_service
            .run_range(from_slot, to_slot, instruction_tx)
            .await;

        // A mismatch stops the validator, which in turn fails the fill's sends, so the
        // validator's verdict is the one to report.
        match validator_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("Consumed-set validation failed; aborting resync before drop: {e}");
                return Err(e);
            }
            Err(e) => {
                error!("Consumed-set validation task panicked: {e:?}");
                return Err(IndexerError::ShutdownChannelSend);
            }
        }
        fill?;
        info!("Consumed-set validated against every rebuilt event");
        Ok(())
    }

    /// Refuse a withdraw rebuild unless the chain has issued no nonce at all. The rebuild
    /// restarts the nonce sequence at 0, so rebuilt rows would reuse nonces the chain has
    /// spent. Renumbering is not offered because it rewrites which withdrawal a nonce names.
    ///
    /// Two independent proofs, in order. The database's own completed rows settle it without
    /// any RPC. Only then is the bitmap read, and it is read bound to a finalized tip proven
    /// fresh against wall clock, so a lagging or snapshot-replaying node errors instead of
    /// serving an old empty bitmap whose clear bits would read as a fresh chain.
    async fn refuse_if_bitmap_advanced(&self) -> Result<(), IndexerError> {
        if self.program_type != ProgramType::Withdraw {
            return Ok(());
        }
        let unverified = |reason: String| {
            error!("Withdrawal bitmap unverified; aborting resync before drop: {reason}");
            IndexerError::Reconciliation(ReconciliationError::WithdrawalBitmapUnverified { reason })
        };

        // Local proof first. A completed withdrawal is this database's own record that its
        // nonce was released, which no RPC answer can contradict. Bounded by i64::MAX
        // because the column is signed.
        let completed = self
            .storage
            .get_completed_withdrawal_nonces(0, i64::MAX as u64)
            .await?;
        if !completed.is_empty() {
            error!(
                completed = completed.len(),
                "Database holds completed withdrawals; aborting resync before drop"
            );
            return Err(IndexerError::Reconciliation(
                ReconciliationError::WithdrawalNoncesReleased {
                    completed: completed.len(),
                },
            ));
        }

        let (Some(instance), Some(rpc_url)) = (
            self.escrow_instance_id,
            self.withdrawal_bitmap_rpc_url.as_ref(),
        ) else {
            return Err(unverified(
                "a withdraw resync needs the escrow instance id and a Solana RPC that can \
                 read its withdrawal bitmap, and one of them is not configured"
                    .to_string(),
            ));
        };

        let bitmap_pda = find_withdrawal_bitmap_pda(&instance);
        info!(
            instance = %instance,
            bitmap = %bitmap_pda,
            "Checking the withdrawal bitmap has issued no nonces before any destruction..."
        );
        let rpc = RpcClientWithRetry::with_retry_config(
            rpc_url.clone(),
            RetryConfig::default(),
            CommitmentConfig::finalized(),
        );

        // Anchor on the node's own finalized tip, then prove that tip is recent. A node
        // replaying a snapshot answers consistently about a chain that is hours old, and
        // its empty bitmap would otherwise pass this check.
        let (ref_slot, _) = rpc
            .get_latest_blockhash_with_context(CommitmentConfig::finalized())
            .await
            .map_err(|e| unverified(format!("finalized tip read failed: {e}")))?;
        let tip_time = rpc.get_block_time(ref_slot).await.map_err(|e| {
            unverified(format!(
                "finalized tip slot {ref_slot} has no block time, so its freshness cannot \
                 be shown: {e}"
            ))
        })?;
        let lag = chrono::Utc::now().timestamp().saturating_sub(tip_time);
        if lag > MAX_BITMAP_RPC_TIP_LAG_SECS {
            return Err(unverified(format!(
                "the bitmap RPC's finalized tip (slot {ref_slot}) is {lag}s behind wall clock, \
                 past the {MAX_BITMAP_RPC_TIP_LAG_SECS}s limit; a lagging node cannot show the \
                 chain has issued no nonce"
            )));
        }

        // Bound to that slot, so a load balancer routing this read to an older backend
        // returns an error rather than a staler bitmap.
        // The client's commitment on purpose: here a set bit blocks the drop, so the
        // fresher view is the safe one.
        let bitmap = fetch_consumed_nonces(
            &rpc,
            &bitmap_pda,
            Some(ref_slot),
            rpc.rpc_client.commitment(),
        )
        .await
        .map_err(|e| unverified(e.to_string()))?;

        if bitmap.generation != 0 || !bitmap.consumed.is_empty() {
            error!(
                generation = bitmap.generation,
                set_bits = bitmap.consumed.len(),
                "Withdrawal bitmap has advanced; aborting resync before drop"
            );
            return Err(IndexerError::Reconciliation(
                ReconciliationError::WithdrawalBitmapAdvanced {
                    generation: bitmap.generation,
                    set_bits: bitmap.consumed.len(),
                },
            ));
        }
        info!(
            ref_slot,
            "Withdrawal bitmap is fresh: generation 0, no set bits, read at the finalized tip"
        );
        Ok(())
    }

    /// Run the resync process
    /// Returns Ok(()) if resync successful, Err otherwise
    pub async fn run(&self, genesis_slot: u64) -> Result<(), IndexerError> {
        info!(
            "Starting database resync for {:?} from slot {}...",
            self.program_type, genesis_slot
        );

        // ---- Pre-flight: every check runs BEFORE any destruction (fail closed). ----
        // On any failure below we return Err with the live DB completely untouched, so an
        // advanced bitmap, a future-slot, an unreachable channel, a legacy-scheme memo, a
        // live worker or an unresolved halt can never leave a half-wiped database.

        // Pre-flight 0: take the live-state lock exclusively, before anything else.
        // It is what proves no indexer or operator is writing to this database, and
        // holding it for the whole rebuild also refuses any worker that tries to start
        // while we run. Every later check is pointless without it.
        let lock_lost = CancellationToken::new();
        let live_lock = self
            .storage
            .try_acquire_live_lock(
                LiveLockMode::Exclusive,
                RESYNC_LOCK_ROLE,
                lock_lost.clone(),
                self.lock_heartbeat_interval,
            )
            .await
            .inspect_err(|e| error!("Refusing to resync: {}", e))?;
        info!("Live-state lock acquired; no indexer or operator can run against this database");

        // A worker whose session died just before we got the lock can still be writing until
        // its heartbeat notices, so read nothing until it has had time to stop.
        info!(
            "Waiting {:?} for any worker that just lost its lock to stop",
            self.stale_holder_grace
        );
        under_live_lock(&lock_lost, async {
            tokio::time::sleep(self.stale_holder_grace).await;
            Ok::<_, StorageError>(())
        })
        .await?;

        // The reads below need the tables to exist, and a resync is also the supported
        // way to build a database from nothing. Creating the schema is idempotent and
        // matches what both workers do at startup.
        self.storage.init_schema().await?;

        // Pre-flight 0a: a resync that deleted rows and died left a marker. Only the same
        // program may finish it, or the other program's half-built rows would look complete.
        let own_key = crate::indexer::checkpoint::program_key(self.program_type);
        let unfinished = self.storage.get_unfinished_resync().await?;
        if let Some(program) = unfinished.clone() {
            if program != own_key {
                error!(
                    unfinished = %program,
                    "Refusing to resync: another program's resync has not finished"
                );
                return Err(StorageError::UnfinishedResync { program }.into());
            }
            warn!("Finishing an earlier {own_key} resync that did not complete");
        }

        // Pre-flight 0b: a reconciliation halt means custody and the ledger already
        // disagree. Rebuilding now would hide the evidence behind an unresolved halt. The
        // one exception is the halt our own interrupted wipe set, next to our own marker.
        let own_rerun_halt = resync_halt_reason(self.program_type);
        if let Some(halt) = self.storage.is_reconciliation_halted().await? {
            if halt.reason == own_rerun_halt && unfinished.as_deref() == Some(own_key.as_str()) {
                warn!("Keeping the halt this {own_key} resync set until its rebuild completes");
            } else {
                error!(
                    "Refusing to resync a halted database; halt reason: {}",
                    halt.reason
                );
                return Err(IndexerError::Reconciliation(
                    ReconciliationError::ReconciliationHalted {
                        reason: halt.reason,
                    },
                ));
            }
        }

        // Pre-flight 1: an escrow rebuild needs its instance scope. The processor filters
        // escrow instructions by it and an unset scope drops every one of them, so a rebuild
        // without it would empty the tables, refill them with nothing, and still advance the
        // checkpoint to the tip. That leaves no gap for a later run to detect, which makes it
        // the one failure here that is not recoverable by repeating the operation.
        if self.program_type == ProgramType::Escrow && self.escrow_instance_id.is_none() {
            error!("Refusing to resync the escrow indexer with no escrow instance id");
            return Err(IndexerError::Reconciliation(
                ReconciliationError::InvalidPubkey {
                    pubkey: "<missing>".to_string(),
                    reason: "escrow_instance_id is required to resync the escrow indexer"
                        .to_string(),
                },
            ));
        }

        // Pre-flight 2: nothing this resync deletes may be in flight or below genesis, and a
        // withdraw resync needs local proof no nonce was spent. Local reads, so before any RPC.
        let blockers = self
            .storage
            .get_resync_blockers(self.program_type.owned_transaction_type())
            .await?;
        if let Some(refusal) = wipe_blocker(self.program_type, &blockers, genesis_slot) {
            error!(?blockers, "Refusing to resync: {}", refusal);
            return Err(refusal.into());
        }

        // Pre-flight 2b: a withdraw rebuild restarts the nonce sequence, so the chain must
        // not have issued any nonce yet. Refuses on an unreadable bitmap as well.
        self.refuse_if_bitmap_advanced().await?;

        // Pre-flight 3: genesis_slot must not be ahead of the chain tip.
        let current_slot = self.rpc_poller.get_latest_slot().await.map_err(|e| {
            error!("Failed to fetch current slot before resync backfill: {}", e);
            IndexerError::DataSource(e.into())
        })?;
        if genesis_slot > current_slot {
            error!(
                "Invalid genesis_slot {}: cannot be ahead of current_slot {}",
                genesis_slot, current_slot
            );
            return Err(IndexerError::from(DataSourceError::InvalidConfig {
                reason: format!(
                    "genesis_slot {} is ahead of current_slot {}",
                    genesis_slot, current_slot
                ),
            }));
        }

        // Pre-flight 4+5: channel reachability + consumed-set completeness + cross-scheme
        // guard, all inside build_consumed_set, which returns Err on any of them.
        let consumed = self.build_consumed_set().await?;

        let backfill_service = BackfillService::new(
            self.storage.clone(),
            self.rpc_poller.clone(),
            self.program_type,
            BackfillConfig {
                enabled: true,
                exit_after_backfill: false,
                rpc_url: self.backfill_config_base.rpc_url.clone(),
                batch_size: self.backfill_config_base.batch_size,
                max_gap_slots: u64::MAX, // No limit for full resync
                start_slot: Some(genesis_slot),
            },
            self.escrow_instance_id,
        );
        // One fixed range for both passes, so the rebuild covers exactly what was validated.
        let (from_slot, to_slot) = backfill_service
            .fixed_range(genesis_slot, current_slot)
            .await?;

        // Pre-flight 6: every channel mint that names a rebuilt event must pay it. Needs
        // the rebuilt rows, so it replays the source history once without writing.
        if let Some(consumed) = &consumed {
            self.validate_before_drop(&backfill_service, from_slot, to_slot, consumed.clone())
                .await?;
        }

        // ---- Destruction: only now, with a complete consumed-set in hand. ----
        // A loss verdict that was wrong leaves the session alive and holding the lock, so
        // only this check stops the wipe; a dead session takes the wipe with it anyway.
        live_lock.ensure_held().await.inspect_err(|e| {
            error!("Refusing to delete rows: {}", e);
        })?;

        // Step 1: delete this program's rows and set the unfinished-resync marker in one
        // transaction on the lock session, so a worker can never start on the half-built rows.
        self.storage
            .wipe_program_fenced(&live_lock, self.program_type)
            .await
            .map_err(|e| {
                error!("Failed to delete this program's rows during resync: {}", e);
                e
            })?;

        // Step 2: Setup processing pipeline
        // Create channels for instruction flow and checkpoint updates
        let (instruction_tx, instruction_rx) = mpsc::channel(1000);
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(1000);

        // Start checkpoint writer service
        let checkpoint_writer = CheckpointWriter::new(self.storage.clone());
        let checkpoint_handle = checkpoint_writer.start(checkpoint_rx);
        info!("CheckpointWriter service started");

        // Start transaction processor as separate tokio task
        let mut transaction_processor =
            TransactionProcessor::new(self.storage.clone(), checkpoint_tx.clone());
        // Wire the escrow instance scope. Pre-flight 1 guarantees Some for the Escrow
        // program. For Withdraw it names the bitmap checked above; the processor only
        // scopes escrow instructions by it, so withdraw rows are unaffected.
        if let Some(instance_id) = self.escrow_instance_id {
            transaction_processor = transaction_processor.with_escrow_instance_id(instance_id);
        }
        // Inject the pre-drop consumed-set so the rebuild reconciles each row in place.
        if let Some(consumed) = consumed {
            transaction_processor = transaction_processor.with_consumed_set(consumed);
        }
        let processor_handle =
            tokio::spawn(async move { transaction_processor.start(instruction_rx).await });
        info!("TransactionProcessor task spawned");

        let total_slots = current_slot.saturating_sub(genesis_slot);

        info!(
            "Starting backfill from slot {} to slot {} ({} slots to process)...",
            genesis_slot, current_slot, total_slots
        );

        // Kept so the loss branch below can stop both writers after their join handles
        // have moved into the rebuild future.
        let processor_abort = processor_handle.abort_handle();
        let checkpoint_abort = checkpoint_handle.abort_handle();
        let storage = self.storage.clone();

        // Everything that writes to the rebuilt database, as one future. The whole of it
        // is watched below, not just the fill: the checkpoint flush at the end is the most
        // dangerous write here, since a durable frontier over a half-rebuilt database
        // leaves no gap for a later run to detect.
        let rebuild = async move {
            backfill_service
                .run_range(from_slot, to_slot, instruction_tx.clone())
                .await
                .map_err(|e| {
                    error!(
                        "Backfill service failed during resync from slot {} to {}: {}",
                        genesis_slot, current_slot, e
                    );
                    e
                })?;
            info!("Backfill service completed");

            // Drop instruction_tx to signal no more instructions coming
            drop(instruction_tx);

            // Wait for processor to finish processing all instructions
            match processor_handle.await {
                Ok(Ok(())) => info!("Transaction processor completed successfully"),
                Ok(Err(e)) => {
                    error!("Transaction processor failed during resync: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    error!("Transaction processor task panicked during resync: {:?}", e);
                    return Err(IndexerError::ShutdownChannelSend);
                }
            }

            // Perform cleanup after backfill, with no completeness target to check. A rebuild
            // resolves its range inside the backfill service and never surfaces the top slot,
            // so there is nothing to compare against here. Leaving it unchecked is acceptable
            // because a stale checkpoint after a rebuild heals itself: the next live start
            // detects the gap below the tip and fills it.
            if let Err(e) = crate::shutdown_utils::cleanup_after_backfill(
                checkpoint_handle,
                checkpoint_tx,
                storage,
                None,
            )
            .await
            {
                error!("Cleanup after resync backfill failed: {}", e);
                // Returned as-is so the operator sees which stage failed, not a generic one.
                return Err(e);
            }
            Ok(())
        };

        // Losing the lock mid-rebuild means a worker can now start against a database
        // that is only half rebuilt, so stop writing rather than race it. The rebuild is
        // repeatable, and the next run starts from a lock it actually holds.
        tokio::select! {
            biased;
            _ = lock_lost.cancelled() => {
                error!("Live-state lock lost during the rebuild; stopping it");
                // Both writers are stopped outright rather than drained. Closing the
                // checkpoint channel is the writer's cue to flush what it has, which
                // would commit a durable frontier over a database this run only half
                // rebuilt and leave no gap for a later run to detect. Waited on, because
                // returning frees the lock and an aborted write can still be in flight.
                crate::shutdown_utils::abort_and_await_writers(&[
                    processor_abort,
                    checkpoint_abort,
                ])
                .await;
                return Err(IndexerError::Storage(StorageError::LiveStateLockLost));
            }
            result = rebuild => result?,
        }

        // Only a completed rebuild clears the marker and its halt, and only while this session
        // still holds the lock.
        self.storage
            .clear_unfinished_resync_fenced(&live_lock, self.program_type)
            .await
            .inspect_err(|e| {
                error!(
                    "Rebuild finished but the resync marker was not cleared: {}",
                    e
                )
            })?;

        info!(
            "Resync complete for {:?}. Processed {} slots (from {} to {})",
            self.program_type, total_slots, genesis_slot, current_slot
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackfillConfig, ProgramType};
    use crate::indexer::datasource::rpc_polling::rpc::RpcPoller;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_commitment_config::CommitmentLevel;
    use solana_transaction_status::UiTransactionEncoding;
    use std::sync::Arc;

    #[test]
    fn resync_service_new_with_escrow_instance_id() {
        use solana_sdk::pubkey::Pubkey;
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let rpc_poller = Arc::new(RpcPoller::new(
            "http://localhost:8899".to_string(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        ));
        let backfill_config = BackfillConfig {
            enabled: false,
            exit_after_backfill: false,
            rpc_url: "http://localhost:8899".to_string(),
            batch_size: 50,
            max_gap_slots: 500,
            start_slot: Some(1000),
        };
        let instance_id = Pubkey::new_unique();

        let service = ResyncService::new(
            storage,
            rpc_poller,
            ProgramType::Withdraw,
            backfill_config,
            Some(instance_id),
        );

        assert_eq!(service.program_type, ProgramType::Withdraw);
        assert_eq!(service.escrow_instance_id, Some(instance_id));
        assert_eq!(service.backfill_config_base.start_slot, Some(1000));
    }

    /// An escrow rebuild with no instance scope would drop every table and refill them with
    /// nothing, so it must abort before the drop rather than after it.
    ///
    /// The RPC points at a dead port, which is what makes the ordering observable: the tip
    /// fetch sits between this guard and the drop, so an `InvalidPubkey` here can only mean
    /// the guard ran first. Had it run later, the unreachable node would have produced a
    /// datasource error instead, and the tables would already be gone.
    #[tokio::test]
    async fn run_refuses_escrow_resync_without_instance_id_before_dropping_tables() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let rpc_poller = Arc::new(RpcPoller::new(
            "http://127.0.0.1:1".to_string(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        ));
        let backfill_config = BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: "http://127.0.0.1:1".to_string(),
            batch_size: 50,
            max_gap_slots: 500,
            start_slot: None,
        };

        let service = ResyncService::new(
            storage,
            rpc_poller,
            ProgramType::Escrow,
            backfill_config,
            None,
        )
        .with_stale_holder_grace(Duration::ZERO);

        match service.run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::InvalidPubkey {
                reason,
                ..
            })) => assert!(
                reason.contains("escrow_instance_id"),
                "reason must name the missing scope, got: {reason}"
            ),
            other => panic!("escrow resync with no instance id must fail closed, got: {other:?}"),
        }
    }

    // ── withdrawal bitmap pre-flight ─────────────────────────────────

    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};

    /// Context slot every mocked finalized tip reports. The bitmap mock only answers a
    /// read bound to it, so an unbound read fails every test that expects the check to pass.
    const REF_SLOT: u64 = 7;

    /// Mount the node's finalized tip: `getLatestBlockhash` at `REF_SLOT` and a
    /// `getBlockTime` for it. `None` answers the block time with null.
    fn mock_fresh_tip(server: &mut mockito::ServerGuard, block_time: Option<i64>) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getLatestBlockhash""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": REF_SLOT},
                        "value": {
                            "blockhash": solana_sdk::hash::Hash::new_unique().to_string(),
                            "lastValidBlockHeight": 1_000u64
                        }
                    }
                })
                .to_string(),
            )
            .create();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockTime""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": block_time}).to_string(),
            )
            .create();
    }

    /// Mount a withdrawal bitmap account as the server's `getAccountInfo` reply, answering
    /// only a read bound to `REF_SLOT`.
    fn mock_bitmap_account(
        server: &mut mockito::ServerGuard,
        generation: u64,
        consumed: &[u64],
    ) -> mockito::Mock {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let bytes = crate::operator::bitmap_account_bytes(generation, consumed, 255);
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getAccountInfo""#.into()),
                mockito::Matcher::Regex(format!(r#""minContextSlot"\s*:\s*{REF_SLOT}\b"#)),
            ]))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": REF_SLOT},
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

    /// A populated mock database, so a refused resync can be shown to have left
    /// its tables alone rather than merely returned an error.
    fn populated_storage() -> (MockStorage, Arc<Storage>) {
        let mock = MockStorage::new();
        mock.set_checkpoint("withdraw", 1_000);
        mock.mints.lock().unwrap().insert(
            "mint".to_string(),
            crate::storage::common::models::DbMint::new("mint".to_string(), 6, "token".to_string()),
        );
        let storage = Arc::new(Storage::Mock(mock.clone()));
        (mock, storage)
    }

    fn seed_completed_withdrawal(mock: &MockStorage, nonce: u64) {
        seed_row(
            mock,
            nonce,
            TransactionType::Withdrawal,
            TransactionStatus::Completed,
        );
    }

    /// Push one row of `ty` in `status`; `n` doubles as its nonce and makes its id unique.
    fn seed_row(mock: &MockStorage, n: u64, ty: TransactionType, status: TransactionStatus) {
        let now = chrono::Utc::now();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id: n as i64 + 1,
                signature: solana_sdk::signature::Signature::new_unique().to_string(),
                trace_id: format!("trace-{n}"),
                // The genesis these tests resync from, so the pre-genesis guard stays out of their way.
                slot: 100,
                initiator: Pubkey::new_unique().to_string(),
                recipient: Pubkey::new_unique().to_string(),
                mint: Pubkey::new_unique().to_string(),
                amount: TokenAmount(1_000),
                memo: None,
                transaction_type: ty,
                withdrawal_nonce: (ty == TransactionType::Withdrawal).then_some(n as i64),
                status,
                created_at: now,
                updated_at: now,
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
            });
    }

    fn assert_db_intact(mock: &MockStorage) {
        assert_eq!(mock.calls("wipe_program"), 0, "the wipe must not run");
        assert!(
            mock.unfinished_resync.lock().unwrap().is_none(),
            "a refused resync must not leave a marker"
        );
        // The schema is created up front so the halt flag is readable on a database
        // that was never indexed, so exactly one idempotent call is expected here. A
        // second one would mean the rebuild ran.
        assert_eq!(
            mock.calls("init_schema"),
            1,
            "only the pre-flight schema creation may run"
        );
        assert_eq!(mock.mints.lock().unwrap().len(), 1, "mint row must survive");
        assert_eq!(
            mock.committed_checkpoints.lock().unwrap().get("withdraw"),
            Some(&1_000),
            "checkpoint must survive"
        );
    }

    /// Withdraw service whose tip RPC is a dead port. The tip fetch follows the bitmap
    /// pre-flight, so a datasource error proves the check passed, and any bitmap error
    /// proves the check ran before the tip fetch and therefore before the drop.
    fn withdraw_service(
        storage: Arc<Storage>,
        instance: Option<Pubkey>,
        bitmap_rpc_url: Option<String>,
    ) -> ResyncService {
        let rpc_poller = Arc::new(RpcPoller::new(
            "http://127.0.0.1:1".to_string(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        ));
        let backfill_config = BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: "http://127.0.0.1:1".to_string(),
            batch_size: 50,
            max_gap_slots: 500,
            start_slot: None,
        };
        let service = ResyncService::new(
            storage,
            rpc_poller,
            ProgramType::Withdraw,
            backfill_config,
            instance,
        )
        .with_stale_holder_grace(Duration::ZERO);
        match bitmap_rpc_url {
            Some(url) => service.with_withdrawal_bitmap_rpc(url),
            None => service,
        }
    }

    /// A completed row is the database's own record that a nonce was released, so the
    /// resync refuses on it without consulting any RPC. The bitmap RPC is a dead port
    /// here, which is what proves no read was attempted.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_when_db_holds_completed_withdrawals_before_any_rpc() {
        let (mock, storage) = populated_storage();
        seed_completed_withdrawal(&mock, 4);
        seed_completed_withdrawal(&mock, 9);
        let service = withdraw_service(
            storage,
            Some(Pubkey::new_unique()),
            Some("http://127.0.0.1:1".to_string()),
        );

        match service.run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::WithdrawalNoncesReleased {
                completed,
            })) => assert_eq!(completed, 2),
            other => panic!("completed withdrawals must abort the resync locally, got: {other:?}"),
        }
        assert_db_intact(&mock);
    }

    /// A node whose finalized tip is far behind wall clock is replaying old state, and an
    /// empty bitmap from it says nothing about the live chain. The bitmap must not be read.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_when_bitmap_rpc_tip_is_stale_db_intact() {
        let mut server = mockito::Server::new_async().await;
        mock_fresh_tip(&mut server, Some(chrono::Utc::now().timestamp() - 3_600));
        let bitmap = mock_bitmap_account(&mut server, 0, &[]).expect(0);
        let (mock, storage) = populated_storage();
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));

        match service.run(100).await {
            Err(IndexerError::Reconciliation(
                ReconciliationError::WithdrawalBitmapUnverified { reason },
            )) => assert!(
                reason.contains("behind wall clock"),
                "reason must name the lag, got: {reason}"
            ),
            other => panic!("a stale tip must abort the resync, got: {other:?}"),
        }
        bitmap.assert();
        assert_db_intact(&mock);
    }

    /// Without a block time for the finalized tip its freshness cannot be shown, which is
    /// the same refusal as an unreadable bitmap.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_when_tip_block_time_unavailable_db_intact() {
        let mut server = mockito::Server::new_async().await;
        mock_fresh_tip(&mut server, None);
        let bitmap = mock_bitmap_account(&mut server, 0, &[]).expect(0);
        let (mock, storage) = populated_storage();
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));

        match service.run(100).await {
            Err(IndexerError::Reconciliation(
                ReconciliationError::WithdrawalBitmapUnverified { reason },
            )) => assert!(
                reason.contains("block time"),
                "reason must name the missing block time, got: {reason}"
            ),
            other => panic!("a tip with no block time must abort the resync, got: {other:?}"),
        }
        bitmap.assert();
        assert_db_intact(&mock);
    }

    /// A set bit means the chain has issued a nonce. Rebuilding would restart the
    /// sequence at 0 underneath it, so the resync must refuse with the tables intact.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_when_bitmap_has_set_bits_db_intact() {
        let mut server = mockito::Server::new_async().await;
        mock_fresh_tip(&mut server, Some(chrono::Utc::now().timestamp()));
        let _bitmap = mock_bitmap_account(&mut server, 0, &[3]);
        let (mock, storage) = populated_storage();
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));

        match service.run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::WithdrawalBitmapAdvanced {
                generation,
                set_bits,
            })) => {
                assert_eq!(generation, 0);
                assert_eq!(set_bits, 1);
            }
            other => panic!("a set bit must abort the resync, got: {other:?}"),
        }
        assert_db_intact(&mock);
    }

    /// A rotated bitmap has issued a whole window of nonces even when its bits are
    /// clear, so a non-zero generation refuses on its own.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_when_bitmap_generation_advanced_db_intact() {
        let mut server = mockito::Server::new_async().await;
        mock_fresh_tip(&mut server, Some(chrono::Utc::now().timestamp()));
        let _bitmap = mock_bitmap_account(&mut server, 2, &[]);
        let (mock, storage) = populated_storage();
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));

        match service.run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::WithdrawalBitmapAdvanced {
                generation,
                set_bits,
            })) => {
                assert_eq!(generation, 2);
                assert_eq!(set_bits, 0);
            }
            other => panic!("an advanced generation must abort the resync, got: {other:?}"),
        }
        assert_db_intact(&mock);
    }

    /// Generation 0 with no bits set, read bound to a fresh finalized tip, is a chain that
    /// has issued nothing, so the resync continues to the next pre-flight. The dead tip
    /// RPC is what it hits. The bitmap mock only answers a read bound to `REF_SLOT`.
    #[tokio::test]
    async fn run_proceeds_past_bitmap_check_on_fresh_bitmap_bound_to_finalized_tip() {
        let mut server = mockito::Server::new_async().await;
        mock_fresh_tip(&mut server, Some(chrono::Utc::now().timestamp()));
        let bitmap = mock_bitmap_account(&mut server, 0, &[]);
        let (_mock, storage) = populated_storage();
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));

        match service.run(100).await {
            Err(IndexerError::DataSource(_)) => {}
            other => {
                panic!("a fresh bitmap must pass the check and reach the tip fetch, got: {other:?}")
            }
        }
        bitmap.assert();
    }

    /// An unreadable bitmap says nothing about whether the chain is fresh, so the
    /// resync refuses instead of skipping the check.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_when_bitmap_unreadable_db_intact() {
        let (mock, storage) = populated_storage();
        let service = withdraw_service(
            storage,
            Some(Pubkey::new_unique()),
            Some("http://127.0.0.1:1".to_string()),
        );

        match service.run(100).await {
            Err(IndexerError::Reconciliation(
                ReconciliationError::WithdrawalBitmapUnverified { reason },
            )) => assert!(
                reason.contains("bitmap") || reason.contains("finalized"),
                "reason must name the failed read, got: {reason}"
            ),
            other => panic!("an unreadable bitmap must abort the resync, got: {other:?}"),
        }
        assert_db_intact(&mock);
    }

    /// Without an escrow instance or a bitmap RPC the check cannot run at all, which
    /// is the same refusal: absence of evidence is not evidence of a fresh chain.
    #[tokio::test]
    async fn run_refuses_withdraw_resync_without_bitmap_inputs_db_intact() {
        for (instance, rpc) in [
            (None, Some("http://127.0.0.1:1".to_string())),
            (Some(Pubkey::new_unique()), None),
        ] {
            let (mock, storage) = populated_storage();
            let service = withdraw_service(storage, instance, rpc);
            match service.run(100).await {
                Err(IndexerError::Reconciliation(
                    ReconciliationError::WithdrawalBitmapUnverified { .. },
                )) => {}
                other => panic!("missing bitmap inputs must abort the resync, got: {other:?}"),
            }
            assert_db_intact(&mock);
        }
    }

    // ── reconciliation halt refusal ──────────────────────────────────

    /// A withdraw service on a mock store, with every RPC pointed at a dead port
    /// and no bitmap RPC configured.
    ///
    /// That is what makes ordering observable. Both the bitmap pre-flight and the
    /// chain-tip fetch sit between the halt check and the drop, so any error other
    /// than the halt one proves the halt check did not run first.
    fn halt_test_service(storage: Arc<Storage>) -> ResyncService {
        let rpc_poller = Arc::new(RpcPoller::new(
            "http://127.0.0.1:1".to_string(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        ));
        let backfill_config = BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: "http://127.0.0.1:1".to_string(),
            batch_size: 50,
            max_gap_slots: 500,
            start_slot: None,
        };
        ResyncService::new(
            storage,
            rpc_poller,
            ProgramType::Withdraw,
            backfill_config,
            None,
        )
        .with_stale_holder_grace(Duration::ZERO)
    }

    /// A reconciliation halt is a solvency interlock, and a rebuild would drop the
    /// table holding it. Refuse before the drop so the evidence survives.
    #[tokio::test]
    async fn run_refuses_when_reconciliation_halt_is_set() {
        let mock = MockStorage::new();
        mock.set_reconciliation_halt("supply above custody")
            .await
            .unwrap();
        let storage = Arc::new(Storage::Mock(mock));

        match halt_test_service(storage.clone()).run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::ReconciliationHalted {
                reason,
            })) => assert!(
                reason.contains("supply above custody"),
                "the refusal must carry the halt reason, got: {reason}"
            ),
            other => panic!("a halted database must refuse to resync, got: {other:?}"),
        }
        match storage.as_ref() {
            Storage::Mock(mock) => assert_eq!(
                mock.calls("wipe_program"),
                0,
                "the refusal must land before any destruction"
            ),
            _ => unreachable!(),
        }
    }

    /// An unreadable halt flag is not proof there is no halt, so it must stop the
    /// rebuild too rather than destroy state it could not check.
    #[tokio::test]
    async fn run_aborts_when_the_halt_flag_is_unreadable() {
        let mock = MockStorage::new();
        mock.set_should_fail("is_reconciliation_halted", true);
        let storage = Arc::new(Storage::Mock(mock));

        match halt_test_service(storage.clone()).run(100).await {
            Err(IndexerError::Storage(_)) => {}
            other => panic!("an unreadable halt flag must fail closed, got: {other:?}"),
        }
        match storage.as_ref() {
            Storage::Mock(mock) => assert_eq!(
                mock.calls("wipe_program"),
                0,
                "the refusal must land before any destruction"
            ),
            _ => unreachable!(),
        }
    }

    // ── pre-wipe gate ────────────────────────────────────────────────

    /// Escrow never touches nonces, so only in-flight work or pre-genesis rows block it; withdraw
    /// also needs proof no nonce is spent.
    #[test]
    fn wipe_blocker_matrix() {
        use crate::storage::common::models::ResyncBlockers;
        let unsettled = ResyncBlockers {
            unsettled_work: true,
            ..Default::default()
        };
        let failed = ResyncBlockers {
            failed_withdrawals: true,
            ..Default::default()
        };
        let observed = ResyncBlockers {
            observed_releases: true,
            ..Default::default()
        };
        let all = ResyncBlockers {
            unsettled_work: true,
            failed_withdrawals: true,
            observed_releases: true,
            earliest_slot: Some(1),
        };
        let rows_from = |slot| ResyncBlockers {
            earliest_slot: Some(slot),
            ..Default::default()
        };

        let label = |r: Option<ReconciliationError>| match r {
            None => "none",
            Some(ReconciliationError::UnsettledWork) => "unsettled",
            Some(ReconciliationError::ReleaseEvidenceRecorded { what })
                if what.contains("failed") =>
            {
                "failed"
            }
            Some(ReconciliationError::ReleaseEvidenceRecorded { .. }) => "observed",
            Some(ReconciliationError::GenesisAboveExistingRows {
                genesis_slot: 100,
                earliest_slot: 99,
            }) => "genesis",
            Some(other) => panic!("unexpected verdict {other:?}"),
        };

        let cases = [
            (ProgramType::Escrow, ResyncBlockers::default(), "none"),
            (ProgramType::Escrow, unsettled, "unsettled"),
            (ProgramType::Escrow, failed, "none"),
            (ProgramType::Escrow, observed, "none"),
            (ProgramType::Withdraw, ResyncBlockers::default(), "none"),
            (ProgramType::Withdraw, unsettled, "unsettled"),
            (ProgramType::Withdraw, failed, "failed"),
            (ProgramType::Withdraw, observed, "observed"),
            (ProgramType::Withdraw, all, "unsettled"),
            (
                ProgramType::Withdraw,
                ResyncBlockers {
                    failed_withdrawals: true,
                    observed_releases: true,
                    ..Default::default()
                },
                "failed",
            ),
            (ProgramType::Escrow, rows_from(99), "genesis"),
            (ProgramType::Escrow, rows_from(100), "none"),
            (ProgramType::Escrow, rows_from(101), "none"),
            (ProgramType::Withdraw, rows_from(99), "genesis"),
            (ProgramType::Withdraw, rows_from(100), "none"),
            (
                ProgramType::Escrow,
                ResyncBlockers {
                    unsettled_work: true,
                    earliest_slot: Some(99),
                    ..Default::default()
                },
                "unsettled",
            ),
        ];
        for (program, blockers, expected) in cases {
            assert_eq!(
                label(wipe_blocker(program, &blockers, 100)),
                expected,
                "{program:?} with {blockers:?}"
            );
        }
    }

    /// Escrow service with a scope set and every RPC on a dead port, so any network step errors.
    fn escrow_service(storage: Arc<Storage>) -> ResyncService {
        let rpc_poller = Arc::new(RpcPoller::new(
            "http://127.0.0.1:1".to_string(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        ));
        let backfill_config = BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: "http://127.0.0.1:1".to_string(),
            batch_size: 50,
            max_gap_slots: 500,
            start_slot: None,
        };
        ResyncService::new(
            storage,
            rpc_poller,
            ProgramType::Escrow,
            backfill_config,
            Some(Pubkey::new_unique()),
        )
        .with_stale_holder_grace(Duration::ZERO)
    }

    /// The gate is local, so it must refuse before the bitmap read, the tip fetch and the wipe.
    #[tokio::test]
    async fn gate_runs_before_any_rpc_and_wipe() {
        let mock = MockStorage::new();
        seed_row(
            &mock,
            1,
            TransactionType::Deposit,
            TransactionStatus::Processing,
        );
        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::Reconciliation(ReconciliationError::UnsettledWork)) => {}
            other => panic!("an in-flight deposit must block an escrow resync, got: {other:?}"),
        }
        assert_eq!(mock.calls("wipe_program"), 0);
        assert_eq!(mock.pending_transactions.lock().unwrap().len(), 1);

        let mut server = mockito::Server::new_async().await;
        let untouched = server.mock("POST", "/").expect(0).create();
        let (mock, storage) = populated_storage();
        seed_row(
            &mock,
            2,
            TransactionType::Withdrawal,
            TransactionStatus::Failed,
        );
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));
        match service.run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::ReleaseEvidenceRecorded {
                ..
            })) => {}
            other => panic!("a failed withdrawal must block a withdraw resync, got: {other:?}"),
        }
        untouched.assert();
        assert_db_intact(&mock);
    }

    /// Rows below the genesis would be wiped and never rebuilt, so both programs refuse locally.
    #[tokio::test]
    async fn genesis_above_earliest_row_refuses_before_any_rpc_and_wipe() {
        let mock = MockStorage::new();
        seed_row(
            &mock,
            1,
            TransactionType::Deposit,
            TransactionStatus::Pending,
        );
        mock.pending_transactions.lock().unwrap()[0].slot = 99;
        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::Reconciliation(ReconciliationError::GenesisAboveExistingRows {
                genesis_slot: 100,
                earliest_slot: 99,
            })) => {}
            other => panic!("a deposit below genesis must block an escrow resync, got: {other:?}"),
        }
        assert_eq!(mock.calls("wipe_program"), 0);
        assert_eq!(mock.pending_transactions.lock().unwrap().len(), 1);

        let mut server = mockito::Server::new_async().await;
        let untouched = server.mock("POST", "/").expect(0).create();
        let (mock, storage) = populated_storage();
        seed_row(
            &mock,
            2,
            TransactionType::Withdrawal,
            TransactionStatus::Pending,
        );
        mock.pending_transactions.lock().unwrap()[0].slot = 99;
        let service = withdraw_service(storage, Some(Pubkey::new_unique()), Some(server.url()));
        match service.run(100).await {
            Err(IndexerError::Reconciliation(ReconciliationError::GenesisAboveExistingRows {
                genesis_slot: 100,
                earliest_slot: 99,
            })) => {}
            other => {
                panic!("a withdrawal below genesis must block a withdraw resync, got: {other:?}")
            }
        }
        untouched.assert();
        assert_db_intact(&mock);
    }

    /// An interrupted wipe already deleted the rows, so its marker keeps their slot and a
    /// rerun from a later genesis is refused the same way.
    #[tokio::test]
    async fn rerun_after_interrupted_wipe_refuses_genesis_above_deleted_rows() {
        let mock = MockStorage::new();
        seed_row(
            &mock,
            1,
            TransactionType::Deposit,
            TransactionStatus::Completed,
        );
        mock.pending_transactions.lock().unwrap()[0].slot = 99;
        mock.wipe_program(ProgramType::Escrow).unwrap();
        assert!(mock.pending_transactions.lock().unwrap().is_empty());

        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::Reconciliation(ReconciliationError::GenesisAboveExistingRows {
                genesis_slot: 100,
                earliest_slot: 99,
            })) => {}
            other => panic!("a rerun must not skip rows its first wipe deleted, got: {other:?}"),
        }
        assert_eq!(mock.calls("wipe_program"), 1);
        assert_eq!(
            mock.unfinished_resync.lock().unwrap().as_deref(),
            Some("escrow")
        );
    }

    /// Another program's unfinished resync owns the database; only that program may finish it.
    #[tokio::test]
    async fn other_program_marker_refuses_before_any_rpc() {
        let mock = MockStorage::new();
        *mock.unfinished_resync.lock().unwrap() = Some("withdraw".to_string());
        seed_row(
            &mock,
            1,
            TransactionType::Deposit,
            TransactionStatus::Pending,
        );
        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::Storage(StorageError::UnfinishedResync { program })) => {
                assert_eq!(program, "withdraw")
            }
            other => panic!("a withdraw marker must refuse an escrow resync, got: {other:?}"),
        }
        assert_eq!(mock.calls("wipe_program"), 0);
        assert_eq!(
            mock.unfinished_resync.lock().unwrap().as_deref(),
            Some("withdraw")
        );
    }

    /// A marker left by this same program is a rerun, so the resync goes on to its RPC steps.
    #[tokio::test]
    async fn same_program_marker_proceeds() {
        let mock = MockStorage::new();
        *mock.unfinished_resync.lock().unwrap() = Some("escrow".to_string());
        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::DataSource(_)) => {}
            other => panic!("a same-program rerun must reach the tip fetch, got: {other:?}"),
        }
    }

    /// A rerun finds the halt its own interrupted wipe set, next to its own marker, and goes on.
    #[tokio::test]
    async fn own_resync_halt_with_matching_marker_proceeds() {
        use crate::storage::common::storage::resync_state::resync_halt_reason;
        let mock = MockStorage::new();
        *mock.unfinished_resync.lock().unwrap() = Some("escrow".to_string());
        mock.set_reconciliation_halt(&resync_halt_reason(ProgramType::Escrow))
            .await
            .unwrap();
        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::DataSource(_)) => {}
            other => panic!("a rerun under its own halt must reach the tip fetch, got: {other:?}"),
        }
    }

    /// Without the matching marker a resync-reason halt is just a halt, so it refuses.
    #[tokio::test]
    async fn resync_halt_without_matching_marker_refuses() {
        use crate::storage::common::storage::resync_state::resync_halt_reason;
        for marker in [None, Some("withdraw")] {
            let mock = MockStorage::new();
            *mock.unfinished_resync.lock().unwrap() = marker.map(str::to_string);
            mock.set_reconciliation_halt(&resync_halt_reason(ProgramType::Escrow))
                .await
                .unwrap();
            let result = escrow_service(Arc::new(Storage::Mock(mock.clone())))
                .run(100)
                .await;
            assert!(
                matches!(
                    result,
                    Err(IndexerError::Reconciliation(
                        ReconciliationError::ReconciliationHalted { .. }
                    )) | Err(IndexerError::Storage(StorageError::UnfinishedResync { .. }))
                ),
                "marker {marker:?}: must refuse, got: {result:?}"
            );
            if marker.is_none() {
                assert!(matches!(
                    result,
                    Err(IndexerError::Reconciliation(
                        ReconciliationError::ReconciliationHalted { .. }
                    ))
                ));
            }
            assert_eq!(mock.calls("wipe_program"), 0);
        }
    }

    /// A halt of another program's resync is foreign to this one, even with its own marker set.
    #[tokio::test]
    async fn other_program_resync_halt_refuses() {
        use crate::storage::common::storage::resync_state::resync_halt_reason;
        let mock = MockStorage::new();
        *mock.unfinished_resync.lock().unwrap() = Some("escrow".to_string());
        mock.set_reconciliation_halt(&resync_halt_reason(ProgramType::Withdraw))
            .await
            .unwrap();
        match escrow_service(Arc::new(Storage::Mock(mock.clone())))
            .run(100)
            .await
        {
            Err(IndexerError::Reconciliation(ReconciliationError::ReconciliationHalted {
                ..
            })) => {}
            other => panic!("another program's resync halt must refuse, got: {other:?}"),
        }
    }

    /// A worker whose session just died may still be writing, so nothing is read inside the grace.
    #[tokio::test(start_paused = true)]
    async fn run_reads_nothing_until_stale_workers_have_had_time_to_stop() {
        let mock = MockStorage::new();
        let grace = Duration::from_secs(60);
        let service =
            halt_test_service(Arc::new(Storage::Mock(mock.clone()))).with_stale_holder_grace(grace);
        let run = tokio::spawn(async move { service.run(100).await });

        tokio::time::sleep(grace - Duration::from_millis(1)).await;
        assert_eq!(
            mock.calls("init_schema"),
            0,
            "nothing may be read inside the grace"
        );

        tokio::time::sleep(Duration::from_millis(2)).await;
        for _ in 0..100 {
            if mock.calls("init_schema") > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            mock.calls("init_schema"),
            1,
            "the resync must go on once the grace is over"
        );
        run.abort();
    }

    /// The grace must outlast both ways a stale worker notices the loss, plus its writer stop.
    #[test]
    fn stale_holder_grace_outlasts_a_stale_worker() {
        assert!(
            STALE_HOLDER_GRACE > LIVE_LOCK_HEARTBEAT_INTERVAL + PROBE_TIMEOUT + WRITER_STOP_TIMEOUT
        );
        assert!(STALE_HOLDER_GRACE > LIVE_LOCK_UNCONFIRMED_BUDGET + WRITER_STOP_TIMEOUT);
    }
}
