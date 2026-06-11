use crate::metrics;
use crate::{
    channel_utils::send_guaranteed,
    config::{BackfillConfig, ProgramType},
    error::{BackfillError, DataSourceError, IndexerError},
    indexer::{
        checkpoint::get_last_checkpoint,
        datasource::{
            common::types::{InstructionSender, ProcessorMessage},
            rpc_polling::{decoder, rpc::RpcPoller, types::RpcBlock},
        },
    },
    storage::Storage,
};
use private_channel_metrics::MetricLabel;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

const BACKFILL_RETRY_DELAY_MS: u64 = 5000;
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

async fn fetch_blocks_with_retry(
    rpc_poller: &RpcPoller,
    slots: &[u64],
    retry_count: usize,
) -> Result<Vec<(u64, Result<Option<RpcBlock>, BackfillError>)>, IndexerError> {
    if retry_count > 0 {
        tokio::time::sleep(Duration::from_millis(
            BACKFILL_RETRY_DELAY_MS * retry_count as u64,
        ))
        .await;
    }

    Ok(rpc_poller
        .get_blocks_batch(slots.to_vec())
        .await
        .into_iter()
        .map(|(slot, result)| {
            (
                slot,
                result.map_err(|e| BackfillError::SlotFetchFailed { slot, source: e }),
            )
        })
        .collect::<Vec<(u64, Result<Option<RpcBlock>, BackfillError>)>>())
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
                    warn!(
                        "Retry {}/{} after error: {}",
                        retry_count, BACKFILL_MAX_RETRIES, e
                    );
                    tokio::time::sleep(Duration::from_millis(
                        BACKFILL_RETRY_DELAY_MS * retry_count as u64,
                    ))
                    .await;
                }
            }
        };

        for (slot, block_result) in blocks {
            match block_result {
                Ok(Some(block)) => {
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
                Ok(None) => {
                    processed_count += 1;
                }
                Err(e) => {
                    warn!("Error fetching block {}: {}", slot, e);
                    return Err(DataSourceError::from(e).into());
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
    pub async fn resolve_range(&self) -> Result<Option<(u64, u64)>, IndexerError> {
        info!(
            "Checking for gaps in indexed data for {:?}...",
            self.program_type
        );

        let last_checkpoint = get_last_checkpoint(&self.storage, self.program_type).await?;

        // Use the larger of configured start_slot and database checkpoint
        // Note: start_slot is inclusive (first slot to process), checkpoint is exclusive (last processed)
        let from_slot = if let Some(configured_start) = self.config.start_slot {
            // Convert inclusive start_slot to exclusive checkpoint format
            let configured_checkpoint = if configured_start > 0 {
                configured_start - 1
            } else {
                0
            };

            let effective_slot = std::cmp::max(configured_checkpoint, last_checkpoint);
            if configured_checkpoint > last_checkpoint {
                info!(
                    "Using configured start_slot {} (will process from slot {}, ahead of database checkpoint {})",
                    configured_start, configured_start, last_checkpoint
                );
            } else {
                info!(
                    "Database checkpoint {} is ahead of configured start_slot {}, using checkpoint",
                    last_checkpoint, configured_start
                );
            }
            effective_slot
        } else {
            last_checkpoint
        };

        // Retry transient RPC failures with backoff (same policy as block fetches), so a
        // single hiccup gating the checkpoint writer doesn't force an ungated fallback.
        let mut retry_count = 0;
        let current_slot = loop {
            match self.rpc_poller.get_latest_slot().await {
                Ok(slot) => break slot,
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
        };

        match validate_gap(current_slot, from_slot, self.config.max_gap_slots)
            .map_err(DataSourceError::from)?
        {
            None => {
                info!(
                    "No gap detected for {:?}. Current slot: {}, From slot: {}",
                    self.program_type, current_slot, from_slot
                );
                Ok(None)
            }
            Some(gap) => {
                info!(
                    "Gap detected for {:?}: {} slots (from {} to {}). Starting backfill...",
                    self.program_type, gap, from_slot, current_slot
                );
                Ok(Some((from_slot, current_slot)))
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
        match self.resolve_range().await? {
            None => Ok(()),
            Some((from_slot, to_slot)) => self.run_range(from_slot, to_slot, instruction_tx).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        use mockito::Server;
        use serde_json::json;
        use solana_sdk::commitment_config::CommitmentLevel;
        use solana_transaction_status::UiTransactionEncoding;
        use tokio::sync::mpsc;

        fn empty_block_json() -> serde_json::Value {
            json!({
                "blockhash": "TestBlockHash11111111111111111111111111111",
                "parentSlot": 0,
                "transactions": []
            })
        }

        fn mock_get_block_success(server: &mut Server, slot: u64) -> mockito::Mock {
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
                        "result": empty_block_json(),
                        "id": 1
                    })
                    .to_string(),
                )
                .create()
        }

        fn mock_get_block_skipped(server: &mut Server, slot: u64) -> mockito::Mock {
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
                        "error": { "code": -32009, "message": "Slot was skipped" },
                        "id": 1
                    })
                    .to_string(),
                )
                .create()
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

        #[tokio::test]
        async fn fill_slot_range_empty_blocks() {
            let mut server = Server::new_async().await;

            let _m1 = mock_get_block_success(&mut server, 101);
            let _m2 = mock_get_block_success(&mut server, 102);
            let _m3 = mock_get_block_success(&mut server, 103);

            let poller = RpcPoller::new(
                server.url(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            );

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
                }
            }
        }

        #[tokio::test]
        async fn fill_slot_range_skipped_slots() {
            let mut server = Server::new_async().await;

            let _m1 = mock_get_block_skipped(&mut server, 101);
            let _m2 = mock_get_block_skipped(&mut server, 102);

            let poller = RpcPoller::new(
                server.url(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            );

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

        #[tokio::test]
        async fn fill_slot_range_block_fetch_error() {
            let mut server = Server::new_async().await;

            let _m1 = mock_get_block_error(&mut server, 101);

            let poller = RpcPoller::new(
                server.url(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            );

            let (tx, _rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 101, 10, ProgramType::Escrow, None, &tx).await;

            assert!(result.is_err());
        }

        #[tokio::test]
        async fn fill_slot_range_no_slots_in_range() {
            let server = Server::new_async().await;

            let poller = RpcPoller::new(
                server.url(),
                UiTransactionEncoding::Json,
                CommitmentLevel::Finalized,
            );

            let (tx, _rx) = mpsc::channel(64);
            let result =
                fill_slot_range(&poller, 100, 100, 10, ProgramType::Escrow, None, &tx).await;

            assert_eq!(result.unwrap(), 0);
        }
    }

    // ============================================================================
    // BackfillService Tests
    // ============================================================================

    #[cfg(feature = "datasource-rpc")]
    mod backfill_service_tests {
        use super::*;
        use crate::config::BackfillConfig;
        use crate::indexer::datasource::rpc_polling::rpc::RpcPoller;
        use crate::storage::common::storage::mock::MockStorage;
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
                })
                .collect();
            assert_eq!(slots, vec![101, 102, 103]);
        }

        // ---- BackfillService::run — start_slot configured ----

        /// When start_slot=200 is ahead of the DB checkpoint=100, the effective from_slot
        /// becomes 199 (start_slot-1), so nothing before slot 200 is re-processed.
        #[tokio::test]
        async fn run_start_slot_ahead_of_checkpoint_uses_start_slot() {
            let mut server = Server::new_async().await;
            // effective from_slot=199; current_slot=199 → no gap, no blocks fetched
            let _m_slot = mock_get_slot(&mut server, 199);

            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", 100);
            let storage = Arc::new(Storage::Mock(mock));
            let poller = make_poller(&server.url());
            let mut config = make_config(&server.url(), 10_000);
            config.start_slot = Some(200);
            let (tx, mut rx) = mpsc::channel(64);

            let service = BackfillService::new(storage, poller, ProgramType::Escrow, config, None);
            service.run(tx).await.unwrap();

            assert!(
                rx.try_recv().is_err(),
                "no messages expected; start_slot skipped past the gap"
            );
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
    }
}
