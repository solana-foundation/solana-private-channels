use crate::metrics;
use crate::{
    channel_utils::send_guaranteed,
    config::{BackfillConfig, ProgramType},
    error::{BackfillError, DataSourceError, IndexerError},
    indexer::{
        checkpoint::{get_last_checkpoint, program_key, start_floor, BACKFILL_START_SETTING},
        datasource::{
            common::types::{InstructionSender, ProcessorMessage},
            rpc_polling::{decoder, rpc::RpcPoller, rpc::MAX_LOOKAHEAD_SLOTS, types::BlockFetch},
        },
    },
    storage::Storage,
};
use private_channel_metrics::MetricLabel;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

#[cfg(not(test))]
const BACKFILL_RETRY_DELAY_MS: u64 = 5000;
/// Tests exercise all three attempts, so the real backoff would cost 15s per case.
#[cfg(test)]
const BACKFILL_RETRY_DELAY_MS: u64 = 5;
const BACKFILL_MAX_RETRIES: usize = 3;

/// Validate gap between current slot and a reference slot.
/// Returns Ok(None) if no gap, Ok(Some(gap)) if valid gap, Err if gap too large.
pub fn validate_gap(
    current_slot: u64,
    last_checkpoint: u64,
    max_gap_slots: u64,
) -> Result<Option<u64>, BackfillError> {
    if current_slot <= last_checkpoint {
        return Ok(None);
    }

    let gap = current_slot - last_checkpoint;

    if gap > max_gap_slots {
        return Err(BackfillError::GapTooLarge {
            gap,
            max_gap: max_gap_slots,
        });
    }

    Ok(Some(gap))
}

fn calculate_batches(from_slot: u64, to_slot: u64, batch_size: usize) -> Vec<Vec<u64>> {
    let mut batches = vec![];
    let mut next_slot = from_slot + 1;

    while next_slot <= to_slot {
        let batch_end = std::cmp::min(next_slot + batch_size as u64, to_slot + 1);
        let batch: Vec<u64> = (next_slot..batch_end).collect();
        batches.push(batch);
        next_slot = batch_end;
    }

    batches
}

/// The highest slot at or below `tip` that produced a block, floored at `floor`.
/// Failing is deliberate: the tip is the one anchor no parent link can witness,
/// so falling back to it would plant the failure this lookup exists to prevent.
async fn last_produced_at_or_below(
    rpc_poller: &RpcPoller,
    floor: u64,
    tip: u64,
) -> Result<u64, IndexerError> {
    // Nothing below the tip to search, so there is no producer to miss.
    if tip <= floor {
        return Ok(tip);
    }
    let start = std::cmp::max(floor, tip.saturating_sub(MAX_LOOKAHEAD_SLOTS));

    let mut retry_count = 0;
    let produced = loop {
        match rpc_poller.get_blocks(start, tip).await {
            Ok(produced) => break produced,
            Err(source) => {
                retry_count += 1;
                if retry_count >= BACKFILL_MAX_RETRIES {
                    error!(
                        "Could not list produced blocks in slots {start}..={tip} after {} retries: {source}",
                        BACKFILL_MAX_RETRIES
                    );
                    return Err(BackfillError::ProducerLookupFailed {
                        from: start,
                        to: tip,
                        source,
                    }
                    .into());
                }
                warn!(
                    "Retry {}/{} listing produced blocks in slots {start}..={tip}: {source}",
                    retry_count, BACKFILL_MAX_RETRIES
                );
                tokio::time::sleep(Duration::from_millis(
                    BACKFILL_RETRY_DELAY_MS * retry_count as u64,
                ))
                .await;
            }
        }
    };

    // An empty window is not the unwitnessable case: the batch walk witnesses a
    // tail from the first producer above it, and fails closed itself if none is
    // servable. Refusing here would turn a long skipped run into a failed boot.
    Ok(produced.into_iter().max().unwrap_or_else(|| {
        warn!("No block was produced in slots {start}..={tip}, so the backfill boundary is the tip and its tail must be witnessed from above");
        tip
    }))
}

async fn fetch_blocks_with_retry(
    rpc_poller: &RpcPoller,
    slots: &[u64],
    retry_count: usize,
) -> Result<Vec<(u64, BlockFetch)>, IndexerError> {
    if retry_count > 0 {
        tokio::time::sleep(Duration::from_millis(
            BACKFILL_RETRY_DELAY_MS * retry_count as u64,
        ))
        .await;
    }

    // A batch-level RPC failure lands as Err on every slot, so burying it per-slot
    // left the caller's retry loop unreachable. Surfacing the first one as a batch
    // error fires before any slot here has been processed, so a retry cannot
    // double-send instructions for the slots that did resolve.
    let mut fetched = Vec::with_capacity(slots.len());
    for (slot, result) in rpc_poller.get_blocks_batch(slots.to_vec()).await {
        match result {
            Ok(block) => fetched.push((slot, block)),
            Err(source) => return Err(BackfillError::SlotFetchFailed { slot, source }.into()),
        }
    }
    Ok(fetched)
}

/// Fill a range of slots by fetching blocks via RPC and sending parsed instructions.
/// Shared by startup backfill and reconnect gap-fill.
/// Returns the number of processed slots.
pub async fn fill_slot_range(
    rpc_poller: &RpcPoller,
    from_slot: u64,
    to_slot: u64,
    batch_size: usize,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
    instruction_tx: &InstructionSender,
) -> Result<u64, IndexerError> {
    let mut processed_count: u64 = 0;
    let gap = to_slot - from_slot;

    metrics::INDEXER_BACKFILL_SLOTS_REMAINING
        .with_label_values(&[program_type.as_label()])
        .set(gap as f64);

    let all_batches = calculate_batches(from_slot, to_slot, batch_size);

    for slots in all_batches {
        let mut retry_count = 0;
        let blocks = loop {
            match fetch_blocks_with_retry(rpc_poller, &slots, retry_count).await {
                Ok(blocks) => break blocks,
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= BACKFILL_MAX_RETRIES {
                        error!(
                            "Failed to fetch blocks after {} retries: {}",
                            BACKFILL_MAX_RETRIES, e
                        );
                        return Err(e);
                    }
                    // The backoff itself lives in `fetch_blocks_with_retry`, keyed to
                    // `retry_count`; sleeping again here would double every wait.
                    warn!(
                        "Retry {}/{} after error: {}",
                        retry_count, BACKFILL_MAX_RETRIES, e
                    );
                }
            }
        };

        for (slot, block_fetch) in blocks {
            match block_fetch {
                BlockFetch::Present(block) => {
                    // A missing-meta block is unverifiable: abort before the SlotComplete send so the
                    // checkpoint never advances past it; the caller surfaces the error and retries.
                    if let Some(signature) = decoder::first_missing_meta(&block) {
                        error!(
                            "Backfill slot {} has transaction {} missing meta; aborting before checkpoint",
                            slot, signature
                        );
                        metrics::INDEXER_RPC_ERRORS
                            .with_label_values(&[program_type.as_label(), "missing_meta"])
                            .inc();
                        return Err(BackfillError::MissingMeta { slot, signature }.into());
                    }

                    let instructions_with_meta = decoder::parse_block(
                        &block,
                        slot,
                        program_type,
                        escrow_instance_id.as_ref(),
                    );

                    for instruction_meta in instructions_with_meta {
                        send_guaranteed(
                            instruction_tx,
                            ProcessorMessage::Instruction(instruction_meta),
                            "instruction (backfill)",
                        )
                        .await
                        .map_err(BackfillError::ChannelSend)?;
                    }
                    processed_count += 1;
                }
                BlockFetch::Skipped => {
                    processed_count += 1;
                }
                BlockFetch::Unavailable => {
                    error!(
                        "Backfill slot {} is unavailable: a block exists here that this endpoint will not serve; aborting before checkpoint",
                        slot
                    );
                    metrics::INDEXER_RPC_ERRORS
                        .with_label_values(&[program_type.as_label(), "block_unavailable"])
                        .inc();
                    return Err(BackfillError::SlotUnavailable { slot }.into());
                }
            }

            send_guaranteed(
                instruction_tx,
                ProcessorMessage::SlotComplete { slot, program_type },
                "SlotComplete marker (backfill)",
            )
            .await
            .map_err(|e| DataSourceError::from(BackfillError::ChannelSend(e)))?;
        }

        metrics::INDEXER_BACKFILL_SLOTS_REMAINING
            .with_label_values(&[program_type.as_label()])
            .set((gap - processed_count) as f64);

        if processed_count.is_multiple_of(1000) {
            let progress = ((processed_count as f64 / gap as f64) * 100.0) as u32;
            info!(
                "Backfill progress for {:?}: {}/{} slots ({}%)",
                program_type, processed_count, gap, progress
            );
        }
    }

    metrics::INDEXER_BACKFILL_SLOTS_REMAINING
        .with_label_values(&[program_type.as_label()])
        .set(0.0);

    info!(
        "Backfill complete for {:?}. Processed {} slots from {} to {}",
        program_type, processed_count, from_slot, to_slot
    );
    Ok(processed_count)
}

/// Fetch the chain tip, retrying transient RPC failures on the same backoff as block fetches.
pub(crate) async fn latest_slot_with_retry(rpc_poller: &RpcPoller) -> Result<u64, IndexerError> {
    let mut retry_count = 0;
    loop {
        match rpc_poller.get_latest_slot().await {
            Ok(slot) => return Ok(slot),
            Err(e) => {
                retry_count += 1;
                if retry_count >= BACKFILL_MAX_RETRIES {
                    error!(
                        "Failed to fetch latest slot after {} retries: {}",
                        BACKFILL_MAX_RETRIES, e
                    );
                    return Err(BackfillError::SlotFetchFailed { slot: 0, source: e }.into());
                }
                warn!(
                    "Retry {}/{} fetching latest slot after error: {}",
                    retry_count, BACKFILL_MAX_RETRIES, e
                );
                tokio::time::sleep(Duration::from_millis(
                    BACKFILL_RETRY_DELAY_MS * retry_count as u64,
                ))
                .await;
            }
        }
    }
}

/// Make sure a durable recovery anchor exists before any live slot is processed, and
/// return the anchor now in force.
///
/// Reconnect repair replays everything between the durable checkpoint and the slot the
/// replacement stream resumes at, so it needs a lower bound it can trust. An existing
/// checkpoint is that bound and is returned untouched; only a store that has never held one
/// gets a row written. The value is the bottom of the startup range when one was resolved,
/// because the range above it has not been filled yet and claiming it would lose those
/// slots; with no startup backfill there is no range to inherit, so the chain tip is the
/// boundary the deployment is choosing and persisting it is what makes it recoverable.
pub async fn ensure_startup_anchor(
    storage: &Arc<Storage>,
    program_type: ProgramType,
    rpc_poller: &RpcPoller,
    resolved_from_slot: Option<u64>,
) -> Result<u64, IndexerError> {
    if let Some(existing) = get_last_checkpoint(storage, program_type).await? {
        return Ok(existing);
    }

    let anchor = match resolved_from_slot {
        Some(from_slot) => from_slot,
        None => latest_slot_with_retry(rpc_poller).await?,
    };

    storage
        .update_committed_checkpoint(&program_key(program_type), anchor)
        .await?;

    info!(
        "Startup anchor persisted for {:?}: slot {}",
        program_type, anchor
    );
    Ok(anchor)
}

/// Highest slot startup owns, one below the live boundary, or the anchor with no backfill.
/// Not capped at the chain tip: a node lagging the provider that wrote the checkpoint is normal.
#[cfg(feature = "datasource-yellowstone")]
pub fn resolve_startup_floor(
    live_start_slot: Option<u64>,
    anchor: u64,
) -> Result<u64, IndexerError> {
    let floor = live_start_slot.map_or(anchor, |slot| slot.saturating_sub(1));

    if floor < anchor {
        return Err(DataSourceError::InvalidConfig {
            reason: format!(
                "startup floor {floor} is below the durable anchor {anchor}; the live boundary \
                 and the anchor were resolved from different ranges"
            ),
        }
        .into());
    }

    Ok(floor)
}

/// Resolved startup boundary shared by both producers. Backfill fills `gap` and the
/// live RPC source resumes at `live_start_slot`, so the two meet with no hole and no
/// overlap: `live_start_slot` is one past the highest slot backfill covers (or one past
/// the durable checkpoint when there is no gap).
pub struct StartupRange {
    /// Range backfill must fill `(from_slot, target]`, or `None` when there's no gap.
    pub gap: Option<(u64, u64)>,
    /// First slot the live RPC source must request.
    pub live_start_slot: u64,
    /// Bottom of the resolved range; carried separately because it outlives an empty `gap`.
    pub anchor: u64,
}

/// Backfill service for recovering missed slots on startup
pub struct BackfillService {
    storage: Arc<Storage>,
    rpc_poller: Arc<RpcPoller>,
    program_type: ProgramType,
    config: BackfillConfig,
    escrow_instance_id: Option<Pubkey>,
}

impl BackfillService {
    pub fn new(
        storage: Arc<Storage>,
        rpc_poller: Arc<RpcPoller>,
        program_type: ProgramType,
        config: BackfillConfig,
        escrow_instance_id: Option<Pubkey>,
    ) -> Self {
        Self {
            storage,
            rpc_poller,
            program_type,
            config,
            escrow_instance_id,
        }
    }

    /// Work out which slots backfill needs to fill: `Some((from_slot, target))`, or
    /// `None` if there's no gap. `from_slot` is exclusive (the last durable
    /// checkpoint) and `target` is inclusive, so the range to fill is
    /// `(from_slot, target]` — derived from the stored checkpoint, the configured
    /// `start_slot`, the current chain tip, and the max gap size.
    ///
    /// The caller resolves the range once and uses it for two things — gating the
    /// checkpoint writer and driving the fill — so both see the exact same bounds.
    /// It also carries `live_start_slot` so the live source resumes on the same
    /// boundary and no slot is skipped between backfill and the live stream.
    pub async fn resolve_range(&self) -> Result<StartupRange, IndexerError> {
        info!(
            "Checking for gaps in indexed data for {:?}...",
            self.program_type
        );

        // Absence and slot zero must stay apart here: only a ledger that has never been
        // indexed lets the configured start_slot pick the floor.
        let last_checkpoint = get_last_checkpoint(&self.storage, self.program_type).await?;

        let from_slot = start_floor(
            BACKFILL_START_SETTING,
            self.program_type,
            last_checkpoint,
            self.config.start_slot,
        )?;

        // One line naming the floor and both inputs, so a log says which one decided it.
        info!(
            "Backfill floor for {:?}: slot {} (durable checkpoint {:?}, configured start_slot {:?})",
            self.program_type, from_slot, last_checkpoint, self.config.start_slot
        );

        let chain_tip = latest_slot_with_retry(&self.rpc_poller).await?;
        // Backfill stops at the last produced block. A slot is proven by the next
        // block's parent link, and the tip is a tick that usually carries none, so
        // ending there leaves a tail nothing can witness.
        let current_slot =
            last_produced_at_or_below(&self.rpc_poller, from_slot, chain_tip).await?;

        // A boundary this far past the chain is a misconfiguration, not lag. Warn rather than
        // refuse, because a node trailing the provider that wrote the checkpoint is routine.
        if from_slot.saturating_sub(current_slot) > self.config.max_gap_slots {
            warn!(
                "Startup boundary {} is {} slots past the tip {} this endpoint reports; check \
                 backfill.start_slot against the chain it serves",
                from_slot,
                from_slot - current_slot,
                current_slot
            );
        }

        // One past the highest slot backfill covers (gap) or the durable checkpoint
        // (no gap); max guards against an RPC node lagging behind the checkpoint.
        let live_start_slot = std::cmp::max(from_slot, current_slot) + 1;

        match validate_gap(current_slot, from_slot, self.config.max_gap_slots)
            .map_err(DataSourceError::from)?
        {
            None => {
                info!(
                    "No gap detected for {:?}. Current slot: {}, From slot: {}",
                    self.program_type, current_slot, from_slot
                );
                Ok(StartupRange {
                    gap: None,
                    live_start_slot,
                    anchor: from_slot,
                })
            }
            Some(gap) => {
                info!(
                    "Gap detected for {:?}: {} slots (from {} to {}). Starting backfill...",
                    self.program_type, gap, from_slot, current_slot
                );
                Ok(StartupRange {
                    gap: Some((from_slot, current_slot)),
                    live_start_slot,
                    anchor: from_slot,
                })
            }
        }
    }

    /// Fill the resolved range `(from_slot, to_slot]` over the instruction channel.
    pub async fn run_range(
        &self,
        from_slot: u64,
        to_slot: u64,
        instruction_tx: InstructionSender,
    ) -> Result<(), IndexerError> {
        fill_slot_range(
            &self.rpc_poller,
            from_slot,
            to_slot,
            self.config.batch_size,
            self.program_type,
            self.escrow_instance_id,
            &instruction_tx,
        )
        .await?;

        info!("Backfill complete for {:?}", self.program_type);
        Ok(())
    }

    /// Run the backfill process
    /// Returns Ok(()) if no gap or backfill successful, Err if gap too large or backfill failed
    pub async fn run(&self, instruction_tx: InstructionSender) -> Result<(), IndexerError> {
        match self.resolve_range().await?.gap {
            None => Ok(()),
            Some((from_slot, to_slot)) => self.run_range(from_slot, to_slot, instruction_tx).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackfillConfig;
    use crate::indexer::datasource::rpc_polling::rpc::MAX_IDLE_GAP_SLOTS;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::test_utils::rpc_mocks::mock_get_slot;
    use mockito::Server;
    use solana_sdk::commitment_config::CommitmentLevel;
    use solana_transaction_status::UiTransactionEncoding;

    // ============================================================================
    // Startup anchor Tests
    // ============================================================================

    /// The tip is a tick and usually carries no block, so backfill has to stop at
    /// the last produced one. Ending above it leaves a tail no parent link can
    /// witness, and the walk fails closed on every retry.
    #[tokio::test]
    async fn last_produced_stops_at_the_last_block_below_the_tip() {
        let mut server = Server::new_async().await;
        let _m = crate::test_utils::rpc_mocks::mock_get_blocks(&mut server, 0, 19, &[0, 10]);
        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        assert_eq!(last_produced_at_or_below(&poller, 0, 19).await.unwrap(), 10);
    }

    /// Anchoring on the tip is exactly the unwitnessable tail this lookup exists
    /// to avoid, so a listing that cannot be read stops startup instead of
    /// quietly producing it.
    #[tokio::test]
    async fn last_produced_fails_when_the_listing_cannot_be_read() {
        let server = Server::new_async().await;
        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        let err = last_produced_at_or_below(&poller, 0, 19)
            .await
            .expect_err("an unreadable listing must not fall back to the tip");
        let msg = err.to_string();
        assert!(
            msg.contains("19"),
            "the error must name the range it searched: {msg}"
        );
    }

    /// A window with no block is answerable, unlike one that could not be read:
    /// the walk witnesses that tail from the first producer above it, so the tip
    /// stands as the boundary rather than stopping the run.
    #[tokio::test]
    async fn last_produced_keeps_the_tip_when_the_window_holds_no_block() {
        let mut server = Server::new_async().await;
        let _m = crate::test_utils::rpc_mocks::mock_get_blocks(&mut server, 0, 19, &[]);
        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        assert_eq!(last_produced_at_or_below(&poller, 0, 19).await.unwrap(), 19);
    }

    /// Nothing below the tip to search, so there is no listing to fail and no
    /// producer to miss. Registering no route proves the short-circuit is real.
    #[tokio::test]
    async fn last_produced_returns_the_tip_when_there_is_nothing_below_it() {
        let server = Server::new_async().await;
        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        assert_eq!(
            last_produced_at_or_below(&poller, 19, 19).await.unwrap(),
            19
        );
    }

    /// The lookback has to reach past the widest gap an idle node can leave, and
    /// past what another chain's skipped run can leave, so it searches the same
    /// distance the poller's lookahead does.
    #[tokio::test]
    async fn last_produced_searches_past_the_widest_idle_gap() {
        let tip = MAX_IDLE_GAP_SLOTS * 5;
        let producer = tip - MAX_IDLE_GAP_SLOTS * 2;
        let mut server = Server::new_async().await;
        let _m = crate::test_utils::rpc_mocks::mock_get_blocks(&mut server, 0, tip, &[producer]);
        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );

        assert_eq!(
            last_produced_at_or_below(&poller, 0, tip).await.unwrap(),
            producer
        );
    }

    /// Mock RPC plus a store either seeded with a checkpoint or left empty.
    async fn anchor_fixture(
        seeded: Option<u64>,
    ) -> (mockito::ServerGuard, Arc<Storage>, RpcPoller) {
        let server = Server::new_async().await;
        let mock = MockStorage::new();
        if let Some(slot) = seeded {
            mock.set_checkpoint("escrow", slot);
        }
        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        );
        (server, Arc::new(Storage::Mock(mock)), poller)
    }

    /// Read back the escrow checkpoint the fixture's store currently holds.
    async fn stored_checkpoint(storage: &Arc<Storage>) -> Option<u64> {
        storage.get_committed_checkpoint("escrow").await.unwrap()
    }

    fn backfill_config(start_slot: Option<u64>, max_gap_slots: u64) -> BackfillConfig {
        BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: String::new(),
            batch_size: 10,
            max_gap_slots,
            start_slot,
        }
    }

    /// The anchor must be the range bottom in both arms, including the one with no gap.
    #[tokio::test]
    async fn resolve_range_anchor_is_from_slot_in_both_arms() {
        // (tip, expected gap, expected live_start_slot)
        let cases = [(150u64, Some((100u64, 150u64)), 151u64), (90, None, 101)];

        for (tip, want_gap, want_live_start) in cases {
            let mut server = Server::new_async().await;
            let _slot = mock_get_slot(&mut server, tip);
            // Below the checkpoint there is nothing to search, so only the gap arm lists.
            let _blocks = (tip > 100).then(|| {
                crate::test_utils::rpc_mocks::mock_get_blocks(&mut server, 100, tip, &[tip])
            });
            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage: Arc<Storage> = Arc::new(Storage::Mock(mock));
            let poller = Arc::new(RpcPoller::new(
                server.url(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            ));

            let service = BackfillService::new(
                storage,
                poller,
                ProgramType::Escrow,
                backfill_config(None, 1000),
                None,
            );
            let range = service.resolve_range().await.unwrap();

            assert_eq!(range.gap, want_gap, "gap for tip {tip}");
            assert_eq!(
                range.anchor, 100,
                "anchor must be the range bottom for tip {tip}"
            );
            assert_eq!(
                range.live_start_slot, want_live_start,
                "live_start_slot for tip {tip}"
            );
        }
    }

    /// A resolved boundary is written straight through, with no tip probe.
    #[tokio::test]
    async fn ensure_startup_anchor_persists_hint_without_rpc() {
        let (mut server, storage, poller) = anchor_fixture(None).await;
        let untouched = server.mock("POST", "/").expect(0).create();

        let anchor = ensure_startup_anchor(&storage, ProgramType::Escrow, &poller, Some(500))
            .await
            .unwrap();

        assert_eq!(anchor, 500);
        assert_eq!(stored_checkpoint(&storage).await, Some(500));
        untouched.assert();
    }

    /// With no boundary to inherit, the tip the stream begins at is what gets persisted.
    #[tokio::test]
    async fn ensure_startup_anchor_probes_tip_when_no_hint() {
        let (mut server, storage, poller) = anchor_fixture(None).await;
        let _slot = mock_get_slot(&mut server, 900);

        let anchor = ensure_startup_anchor(&storage, ProgramType::Escrow, &poller, None)
            .await
            .unwrap();

        assert_eq!(anchor, 900);
        assert_eq!(stored_checkpoint(&storage).await, Some(900));
    }

    /// The dangerous case. Moving a live checkpoint forward would mark unread slots as
    /// handled, which is the exact loss this anchor exists to prevent. An existing row must
    /// therefore win over a hint above it and over a tip above it, and must cost no RPC
    /// call at all, since a probe that never runs cannot return a value that overwrites it.
    #[tokio::test]
    async fn ensure_startup_anchor_never_moves_an_existing_checkpoint() {
        let (mut server, storage, poller) = anchor_fixture(Some(100)).await;
        let untouched = server.mock("POST", "/").expect(0).create();

        let from_hint = ensure_startup_anchor(&storage, ProgramType::Escrow, &poller, Some(5_000))
            .await
            .unwrap();
        assert_eq!(from_hint, 100, "a hint above the row must not move it");
        assert_eq!(stored_checkpoint(&storage).await, Some(100));

        let from_probe = ensure_startup_anchor(&storage, ProgramType::Escrow, &poller, None)
            .await
            .unwrap();
        assert_eq!(from_probe, 100, "the tip probe must not be reached at all");
        assert_eq!(stored_checkpoint(&storage).await, Some(100));

        untouched.assert();
    }

    /// A failed anchor write must abort startup, not fall through into live streaming.
    #[tokio::test]
    async fn ensure_startup_anchor_fails_closed_on_write_failure() {
        let (_server, storage, poller) = anchor_fixture(None).await;
        if let Storage::Mock(mock) = storage.as_ref() {
            // The mock keys its write fault on the program string, the read on the method.
            mock.set_should_fail("escrow", true);
        }

        let result = ensure_startup_anchor(&storage, ProgramType::Escrow, &poller, Some(7)).await;

        assert!(result.is_err(), "a failed anchor write must abort startup");
        assert_eq!(stored_checkpoint(&storage).await, None);
    }

    // ============================================================================
    // resolve_startup_floor Tests
    // ============================================================================

    /// Backfill owns everything below the live boundary, so the floor sits one slot under it.
    #[cfg(feature = "datasource-yellowstone")]
    #[test]
    fn startup_floor_is_the_slot_below_the_live_boundary() {
        assert_eq!(resolve_startup_floor(Some(1_001), 500).unwrap(), 1_000);
    }

    /// With no backfill there is no range to inherit and the anchor is the boundary itself.
    #[cfg(feature = "datasource-yellowstone")]
    #[test]
    fn startup_floor_falls_back_to_the_anchor() {
        assert_eq!(resolve_startup_floor(None, 500).unwrap(), 500);
    }

    /// The anchor is the lowest slot startup owns, so a floor under it means the two were
    /// derived from different ranges and the fill would replay below the durable checkpoint.
    #[cfg(feature = "datasource-yellowstone")]
    #[test]
    fn startup_floor_rejects_a_boundary_below_the_anchor() {
        assert!(resolve_startup_floor(Some(401), 500).is_err());
    }

    // ============================================================================
    // validate_gap Tests
    // ============================================================================

    #[test]
    fn test_validate_gap_no_gap() {
        let result = validate_gap(100, 100, 1000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_validate_gap_current_behind_checkpoint() {
        let result = validate_gap(50, 100, 1000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_validate_gap_within_limit() {
        let result = validate_gap(150, 100, 1000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(50));
    }

    #[test]
    fn test_validate_gap_exceeds_limit() {
        let result = validate_gap(2000, 100, 1000);
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        let err_str = err_msg.to_string();
        assert!(err_str.contains("Gap too large"), "Error: {}", err_str);
        assert!(err_str.contains("1900 slots"), "Error: {}", err_str);
    }

    #[test]
    fn test_validate_gap_exactly_at_limit() {
        let result = validate_gap(1100, 100, 1000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(1000));
    }

    // ============================================================================
    // calculate_batches Tests
    // ============================================================================

    #[test]
    fn test_calculate_batches_full_batches() {
        let batches = calculate_batches(100, 109, 3);

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], vec![101, 102, 103]);
        assert_eq!(batches[1], vec![104, 105, 106]);
        assert_eq!(batches[2], vec![107, 108, 109]);
    }

    #[test]
    fn test_calculate_batches_partial_last_batch() {
        let batches = calculate_batches(100, 105, 3);

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![101, 102, 103]);
        assert_eq!(batches[1], vec![104, 105]);
    }

    #[test]
    fn test_calculate_batches_single_slot() {
        let batches = calculate_batches(100, 101, 10);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], vec![101]);
    }

    #[test]
    fn test_calculate_batches_same_from_to_slot() {
        // from_slot == to_slot: next_slot = from_slot+1 > to_slot, so no iterations
        let batches = calculate_batches(100, 100, 10);
        assert!(batches.is_empty());
    }

    #[cfg(feature = "datasource-rpc")]
    mod fill_slot_range_tests {
        use super::*;
        use crate::indexer::datasource::rpc_polling::rpc::RpcPoller;
        use crate::test_utils::rpc_mocks::{
            chain, mock_get_block_at, mock_get_blocks, mock_get_blocks_with_limit,
        };
        use mockito::Server;
        use serde_json::json;
        use solana_sdk::commitment_config::CommitmentLevel;
        use solana_transaction_status::UiTransactionEncoding;
        use tokio::sync::mpsc;

        fn poller(server: &Server) -> RpcPoller {
            RpcPoller::new(
                server.url(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            )
        }

        fn mock_get_block_error(server: &mut Server, slot: u64) -> mockito::Mock {
            server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(json!({
                    "method": "getBlock",
                    "params": [slot]
                })))
                .with_status(200)
                .with_body(
                    json!({
                        "jsonrpc": "2.0",
                        "error": { "code": -32600, "message": "Invalid request" },
                        "id": 1
                    })
                    .to_string(),
                )
                .create()
        }

        /// getBlock returns a well-formed block carrying one transaction with
        /// `meta: null`, which the guard must reject as incomplete.
        fn mock_get_block_missing_meta(server: &mut Server, slot: u64) -> mockito::Mock {
            server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(json!({
                    "method": "getBlock",
                    "params": [slot]
                })))
                .with_status(200)
                .with_body(
                    json!({
                        "jsonrpc": "2.0",
                        "result": {
                            "blockhash": "TestBlockHash11111111111111111111111111111",
                            "parentSlot": slot - 1,
                            "transactions": [{
                                "transaction": {
                                    "signatures": ["sig_missing_meta"],
                                    "message": { "accountKeys": [], "instructions": [] }
                                },
                                "meta": null
                            }]
                        },
                        "id": 1
                    })
                    .to_string(),
                )
                .create()
        }

        #[tokio::test]
        async fn fill_slot_range_empty_blocks() {
            let mut server = Server::new_async().await;

            let _c = chain(&mut server, 101, 103, &[(101, 100), (102, 101), (103, 102)]);

            let poller = poller(&server);

            let (tx, mut rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 103, 10, ProgramType::Escrow, None, &tx).await;

            assert_eq!(result.unwrap(), 3);
            drop(tx);

            let mut messages = vec![];
            while let Some(msg) = rx.recv().await {
                messages.push(msg);
            }

            assert_eq!(messages.len(), 3);
            for (i, msg) in messages.iter().enumerate() {
                match msg {
                    ProcessorMessage::SlotComplete { slot, .. } => {
                        assert_eq!(*slot, 101 + i as u64);
                    }
                    ProcessorMessage::Instruction(_) => {
                        panic!("Expected no Instruction messages for empty blocks");
                    }
                    ProcessorMessage::Regate { .. } => {
                        panic!("Expected no Regate messages from backfill");
                    }
                }
            }
        }

        #[tokio::test]
        async fn fill_slot_range_skipped_slots() {
            let mut server = Server::new_async().await;

            // Neither slot produced a block, and the first producer past the range
            // links straight back to the anchor, proving both empty.
            let _blocks = mock_get_blocks(&mut server, 101, 102, &[]);
            let _witness = mock_get_blocks_with_limit(&mut server, 103, &[103]);
            let _witness_block = mock_get_block_at(&mut server, 103, 100);

            let poller = poller(&server);

            let (tx, mut rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 102, 10, ProgramType::Escrow, None, &tx).await;

            assert_eq!(result.unwrap(), 2);
            drop(tx);

            let mut messages = vec![];
            while let Some(msg) = rx.recv().await {
                messages.push(msg);
            }

            assert_eq!(messages.len(), 2);
            for msg in &messages {
                assert!(matches!(msg, ProcessorMessage::SlotComplete { .. }));
            }
        }

        /// A batch where a later block's parent link proves slot N holds a block the
        /// endpoint will not serve (Unavailable) must abort with
        /// `SlotUnavailable { slot: N }` before sending SlotComplete{N}
        /// (and before any later slot's SlotComplete), so the checkpoint never
        /// advances past a slot whose contents are unknown.
        #[tokio::test]
        async fn fill_slot_range_unavailable_aborts_before_slot_complete() {
            let mut server = Server::new_async().await;

            // Batch over (100, 102] = [101, 102]. Slot 102 names 101 as its parent,
            // so a real block sits at 101 that the enumeration did not list and this
            // endpoint will not serve.
            let _c = chain(&mut server, 101, 102, &[(102, 101)]);

            let poller = poller(&server);

            let (tx, mut rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 102, 10, ProgramType::Escrow, None, &tx).await;

            let err = result.unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("unavailable"), "unexpected error: {msg}");
            assert!(msg.contains("101"), "error should name the slot: {msg}");

            drop(tx);
            let messages: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            let advanced = messages
                .iter()
                .any(|m| matches!(m, ProcessorMessage::SlotComplete { slot, .. } if *slot >= 101));
            assert!(
                !advanced,
                "no SlotComplete must be sent for slot 101 or beyond on an unavailable block"
            );
        }

        #[tokio::test]
        async fn fill_slot_range_block_fetch_error() {
            let mut server = Server::new_async().await;

            let _blocks = mock_get_blocks(&mut server, 101, 101, &[101]);
            let _m1 = mock_get_block_error(&mut server, 101);

            let poller = poller(&server);

            let (tx, _rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 101, 10, ProgramType::Escrow, None, &tx).await;

            assert!(result.is_err());
        }

        /// A batch-level RPC failure has to reach the retry loop. It used to be
        /// buried in the per-slot results, so the loop saw `Ok` every time and a
        /// single transient blip aborted the whole backfill on the first attempt.
        #[tokio::test]
        async fn fill_slot_range_retries_a_batch_level_failure() {
            let mut server = Server::new_async().await;

            // The enumeration fails, which fails every slot in the batch at once.
            let enumeration = server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(
                    serde_json::json!({"method": "getBlocks"}),
                ))
                .with_status(500)
                .expect(BACKFILL_MAX_RETRIES)
                .create();

            let poller = poller(&server);

            let (tx, _rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 102, 10, ProgramType::Escrow, None, &tx).await;

            assert!(result.is_err());
            // Every attempt was made, not just the first.
            enumeration.assert();
        }

        /// A trailing run of non-producers with no witness is undetermined, so
        /// backfill must abort and retry rather than treat it as proven empty.
        #[tokio::test]
        async fn fill_slot_range_unwitnessed_tail_aborts_before_slot_complete() {
            let mut server = Server::new_async().await;

            let _blocks = mock_get_blocks(&mut server, 101, 102, &[]);
            let _witness = mock_get_blocks_with_limit(&mut server, 103, &[]);

            let poller = poller(&server);

            let (tx, mut rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 102, 10, ProgramType::Escrow, None, &tx).await;

            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("unwitnessed"), "unexpected error: {msg}");

            drop(tx);
            let messages: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            assert!(
                messages.is_empty(),
                "an unwitnessed tail must not emit any SlotComplete"
            );
        }

        /// A batch where slot N's block has a `meta: null` transaction must
        /// abort with `MissingMeta { slot: N }` before sending SlotComplete{N} (and
        /// before any SlotComplete for slots after N), so the checkpoint never
        /// advances past the incomplete slot.
        #[tokio::test]
        async fn fill_slot_range_missing_meta_aborts_before_slot_complete() {
            let mut server = Server::new_async().await;

            // Batch over (100, 102] = [101, 102]; slot 101 is incomplete.
            let _blocks = mock_get_blocks(&mut server, 101, 102, &[101, 102]);
            let _m1 = mock_get_block_missing_meta(&mut server, 101);
            let _m2 = mock_get_block_at(&mut server, 102, 101);

            let poller = poller(&server);

            let (tx, mut rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 102, 10, ProgramType::Escrow, None, &tx).await;

            let err = result.unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("missing metadata"), "unexpected error: {msg}");
            assert!(msg.contains("101"), "error should name the slot: {msg}");

            drop(tx);
            let messages: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            let advanced = messages
                .iter()
                .any(|m| matches!(m, ProcessorMessage::SlotComplete { slot, .. } if *slot >= 101));
            assert!(
                !advanced,
                "no SlotComplete must be sent for slot 101 or beyond on an incomplete block"
            );
        }

        #[tokio::test]
        async fn fill_slot_range_no_slots_in_range() {
            let mut server = Server::new_async().await;
            let untouched = server.mock("POST", "/").expect(0).create();

            let poller = poller(&server);

            let (tx, _rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 100, 10, ProgramType::Escrow, None, &tx).await;

            assert_eq!(result.unwrap(), 0);
            untouched.assert();
        }
    }

    // ============================================================================
    // BackfillService Tests
    // ============================================================================

    #[cfg(feature = "datasource-rpc")]
    mod backfill_service_tests {
        use super::*;
        use crate::config::BackfillConfig;
        use crate::error::CheckpointError;
        use crate::indexer::datasource::rpc_polling::rpc::RpcPoller;
        use crate::storage::common::storage::mock::MockStorage;
        use crate::test_utils::rpc_mocks::mock_get_blocks;
        use mockito::Server;
        use serde_json::json;
        use solana_sdk::commitment_config::CommitmentLevel;
        use solana_transaction_status::UiTransactionEncoding;
        use std::sync::Arc;
        use tokio::sync::mpsc;

        fn make_config(rpc_url: &str, max_gap_slots: u64) -> BackfillConfig {
            BackfillConfig {
                enabled: true,
                exit_after_backfill: false,
                rpc_url: rpc_url.to_string(),
                batch_size: 10,
                max_gap_slots,
                start_slot: None,
            }
        }

        fn make_poller(url: &str) -> Arc<RpcPoller> {
            Arc::new(RpcPoller::new(
                url.to_string(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            ))
        }

        fn mock_get_slot(server: &mut Server, slot: u64) -> mockito::Mock {
            server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(json!({"method": "getSlot"})))
                .with_status(200)
                .with_body(json!({"jsonrpc": "2.0", "result": slot, "id": 1}).to_string())
                .create()
        }

        fn mock_get_block_empty(server: &mut Server, slot: u64) -> mockito::Mock {
            server
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(json!({
                    "method": "getBlock",
                    "params": [slot]
                })))
                .with_status(200)
                .with_body(
                    json!({
                        "jsonrpc": "2.0",
                        "result": {
                            "blockhash": "TestBlockHash111111111111111111111111111",
                            "parentSlot": slot - 1,
                            "transactions": []
                        },
                        "id": 1
                    })
                    .to_string(),
                )
                .create()
        }

        // ---- BackfillService::new ----

        /// All five constructor arguments are stored verbatim; no transformation occurs.
        #[test]
        fn new_stores_escrow_instance_id() {
            use solana_sdk::pubkey::Pubkey;
            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            let poller = make_poller("http://localhost:8899");
            let config = make_config("http://localhost:8899", 500);
            let key = Pubkey::new_unique();

            let service =
                BackfillService::new(storage, poller, ProgramType::Withdraw, config, Some(key));

            assert_eq!(service.program_type, ProgramType::Withdraw);
            assert_eq!(service.config.max_gap_slots, 500);
            assert_eq!(service.escrow_instance_id, Some(key));
        }

        // ---- BackfillService::run ----

        /// checkpoint == current_slot means validate_gap returns None; run exits early
        /// without sending any messages or fetching blocks.
        #[tokio::test]
        async fn run_no_gap_returns_ok_without_fetching_blocks() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 100);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            service.run(tx).await.unwrap();

            // tx dropped by run(); channel is empty — no SlotComplete or Instruction sent
            assert!(
                rx.try_recv().is_err(),
                "expected no messages when there is no gap"
            );
        }

        /// current_slot < checkpoint means the RPC node is lagging; treated as no gap,
        /// no backfill attempted, no messages sent.
        #[tokio::test]
        async fn run_current_slot_behind_checkpoint_no_gap() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 50);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            service.run(tx).await.unwrap();

            assert!(
                rx.try_recv().is_err(),
                "expected no messages when RPC slot is behind checkpoint"
            );
        }

        // ---- BackfillService::run — gap too large ----

        /// A gap of 5000 slots with max_gap_slots=1000 must be rejected with a descriptive
        /// error rather than silently attempting an oversized backfill.
        #[tokio::test]
        async fn run_gap_too_large_returns_err() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 5000); // checkpoint=0, gap=5000
            let _m_blocks = mock_get_blocks(&mut server, 0, 5000, &[5000]);

            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);
            let (tx, _rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            let err = service.run(tx).await.unwrap_err();

            let msg = err.to_string();
            assert!(msg.contains("Gap too large"), "unexpected error: {msg}");
            assert!(
                msg.contains("5000"),
                "error should report the actual gap: {msg}"
            );
        }

        // ---- BackfillService::run — fills actual gap ----

        /// For a 3-slot gap (checkpoint=100, tip=103), run fetches each block and emits
        /// exactly one ordered SlotComplete per slot with no Instruction messages.
        #[tokio::test]
        async fn run_fills_gap_sends_slot_complete_per_slot() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 103);
            let _m_anchor = mock_get_blocks(&mut server, 100, 103, &[101, 102, 103]);
            let _m_blocks = mock_get_blocks(&mut server, 101, 103, &[101, 102, 103]);
            let _m_b101 = mock_get_block_empty(&mut server, 101);
            let _m_b102 = mock_get_block_empty(&mut server, 102);
            let _m_b103 = mock_get_block_empty(&mut server, 103);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            service.run(tx).await.unwrap();

            // Collect all messages; tx was dropped by run() so the channel is now closed
            let messages: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            assert_eq!(messages.len(), 3, "expected one SlotComplete per slot");

            let slots: Vec<u64> = messages
                .iter()
                .map(|m| match m {
                    ProcessorMessage::SlotComplete { slot, .. } => *slot,
                    ProcessorMessage::Instruction(_) => panic!("unexpected Instruction message"),
                    ProcessorMessage::Regate { .. } => panic!("unexpected Regate message"),
                })
                .collect();
            assert_eq!(slots, vec![101, 102, 103]);
        }

        // ---- BackfillService::run — start_slot configured ----

        /// A start_slot of 200 over a checkpoint of 100 would leave 101..=199 unfetched
        /// with nothing recording them as owed, so the run must refuse instead.
        #[tokio::test]
        async fn run_start_slot_ahead_of_checkpoint_is_refused() {
            let mut server = Server::new_async().await;
            // Never requested: the floor is resolved before the tip is probed.
            let untouched = server.mock("POST", "/").expect(0).create();

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let mut config = make_config(&server.url(), 10_000);
            config.start_slot = Some(200);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            let err = service
                .run(tx)
                .await
                .expect_err("a start_slot past the checkpoint must refuse");

            assert!(
                matches!(
                    err,
                    IndexerError::Checkpoint(CheckpointError::StartSlotAheadOfCheckpoint {
                        setting: "indexer.backfill.start_slot",
                        start_slot: 200,
                        checkpoint: 100,
                        ..
                    })
                ),
                "expected a start-slot refusal naming the backfill key, got {err:?}"
            );
            assert!(
                rx.try_recv().is_err(),
                "no messages expected; the run refused before fetching anything"
            );
            untouched.assert();
        }

        /// When the DB checkpoint=200 is ahead of start_slot=50, the checkpoint wins
        /// (max logic), so already-processed slots are not re-fetched.
        #[tokio::test]
        async fn run_checkpoint_ahead_of_start_slot_uses_checkpoint() {
            let mut server = Server::new_async().await;
            // effective from_slot=200; current_slot=200 → no gap
            let _m_slot = mock_get_slot(&mut server, 200);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 200);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let mut config = make_config(&server.url(), 10_000);
            config.start_slot = Some(50);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            service.run(tx).await.unwrap();

            assert!(
                rx.try_recv().is_err(),
                "no messages expected; checkpoint supersedes start_slot"
            );
        }

        /// start_slot=0 is the genesis edge case: configured_checkpoint clamps to 0
        /// (avoids u64 underflow), which is identical to having no checkpoint at all.
        #[tokio::test]
        async fn run_start_slot_zero_uses_zero_checkpoint() {
            let mut server = Server::new_async().await;
            // from_slot=0, current_slot=0 → no gap
            let _m_slot = mock_get_slot(&mut server, 0);

            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            let poller = make_poller(&server.url());
            let mut config = make_config(&server.url(), 10_000);
            config.start_slot = Some(0);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            service.run(tx).await.unwrap();

            assert!(
                rx.try_recv().is_err(),
                "no messages expected for zero-slot no-gap case"
            );
        }

        // ---- BackfillService::resolve_range — shared startup boundary ----

        /// With a gap, the live source's first slot is exactly one past backfill's
        /// target, so the two producers meet with no hole and no overlap.
        #[tokio::test]
        async fn resolve_range_gap_sets_live_start_to_target_plus_one() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 110);
            let _m_blocks = mock_get_blocks(&mut server, 100, 110, &[110]);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            let range = service.resolve_range().await.unwrap();

            assert_eq!(range.gap, Some((100, 110)));
            assert_eq!(range.live_start_slot, 111);
        }

        /// No gap (tip == checkpoint): the live boundary is pinned one past the
        /// durable checkpoint, not a freshly sampled tip.
        #[tokio::test]
        async fn resolve_range_no_gap_sets_live_start_to_checkpoint_plus_one() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 100);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            let range = service.resolve_range().await.unwrap();

            assert_eq!(range.gap, None);
            assert_eq!(range.live_start_slot, 101);
        }

        /// RPC node lagging behind the checkpoint: max guard keeps the live boundary
        /// at checkpoint+1, never rewinding below the durable checkpoint.
        #[tokio::test]
        async fn resolve_range_no_gap_rpc_behind_uses_checkpoint_plus_one() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 50);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let config = make_config(&server.url(), 1000);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            let range = service.resolve_range().await.unwrap();

            assert_eq!(range.gap, None);
            assert_eq!(range.live_start_slot, 101);
        }

        /// An empty ledger is the one case where a configured start_slot sets the floor,
        /// including above the tip, so the live source begins exactly there.
        #[tokio::test]
        async fn resolve_range_start_slot_initialises_an_empty_ledger() {
            let mut server = Server::new_async().await;
            let _m_slot = mock_get_slot(&mut server, 150);

            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            let poller = make_poller(&server.url());
            let mut config = make_config(&server.url(), 10_000);
            config.start_slot = Some(200);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            let range = service.resolve_range().await.unwrap();

            assert_eq!(range.gap, None);
            assert_eq!(range.anchor, 199);
            assert_eq!(range.live_start_slot, 200);
        }
    }
}
