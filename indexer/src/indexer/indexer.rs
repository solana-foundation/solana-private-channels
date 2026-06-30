use crate::config::ProgramType;
use crate::error::{DataSourceError, IndexerError, ReconciliationError};
use crate::{
    indexer::{
        checkpoint::CheckpointWriter, datasource::common::datasource::DataSource,
        reconciliation::run_startup_reconciliation, transaction_processor::TransactionProcessor,
    },
    shutdown_utils::{cleanup_after_backfill, shutdown_indexer},
    storage::{PostgresDb, Storage},
    DatasourceType, IndexerConfig, PrivateChannelIndexerConfig, StorageType,
};

#[cfg(feature = "datasource-rpc")]
use crate::indexer::backfill::BackfillService;

#[cfg(feature = "datasource-rpc")]
use crate::indexer::checkpoint::get_last_checkpoint;

#[cfg(feature = "datasource-rpc")]
use crate::indexer::datasource::rpc_polling::{rpc::RpcPoller, RpcPollingSource};

#[cfg(feature = "datasource-yellowstone")]
use crate::indexer::datasource::yellowstone::YellowstoneSource;
use private_channel_metrics::HealthState;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// The durable frontier the live datasource must resume from: a resolved
/// backfill gap target dominates, else the committed checkpoint, else `None`
/// for a genuinely fresh node (checkpoint 0, no gap) so we never replay genesis.
#[cfg(feature = "datasource-rpc")]
fn durable_frontier(resolved_gap_target: Option<u64>, committed_checkpoint: u64) -> Option<u64> {
    match resolved_gap_target {
        Some(target) => Some(target),
        None if committed_checkpoint > 0 => Some(committed_checkpoint),
        None => None,
    }
}

/// Slot the live RPC poller seeds at, one past the durable frontier so the
/// backfill->live handoff is contiguous and cannot leapfrog an unfilled slot.
/// A resolved gap dominates a stale `from_slot` (the bug); an explicit
/// `from_slot` is honored only when no gap was resolved; a fresh node returns
/// `None` so the poller seeds at the live tip rather than from genesis.
#[cfg(feature = "datasource-rpc")]
fn rpc_live_from_slot(
    resolved_gap_target: Option<u64>,
    config_from_slot: Option<u64>,
    committed_checkpoint: u64,
) -> Option<u64> {
    match (resolved_gap_target, config_from_slot, committed_checkpoint) {
        (Some(target), _, _) => Some(target + 1),
        (None, Some(configured), _) => Some(configured),
        (None, None, cp) if cp > 0 => Some(cp + 1),
        (None, None, _) => None,
    }
}

pub async fn run(
    common_config: PrivateChannelIndexerConfig,
    indexer_config: IndexerConfig,
    health: Option<Arc<HealthState>>,
) -> Result<(), IndexerError> {
    info!("Starting PrivateChannel Indexer");
    info!("Program: {:?}", common_config.program_type);
    info!("Datasource: {:?}", indexer_config.datasource_type);
    info!("Storage: {:?}", common_config.storage_type);
    info!("RPC URL: {}", common_config.rpc_url);
    info!("Backfill enabled: {}", indexer_config.backfill.enabled);

    // 1. Initialize storage
    let storage: Arc<Storage> = match common_config.storage_type {
        StorageType::Postgres => Arc::new(Storage::Postgres(
            PostgresDb::new(&common_config.postgres)
                .await
                .map_err(|e| IndexerError::Storage(e.into()))?,
        )),
    };
    storage.init_schema().await?;
    info!("Storage initialized");

    // 2. Startup reconciliation (escrow only, before any data processing).
    //
    // Skip when running in backfill-only mode (backfill.enabled &&
    // backfill.exit_after_backfill). In that mode the DB is intentionally
    // incomplete — reconciling it against the current on-chain state would
    // produce false positives and block the very operation that repairs the
    // discrepancy. Concurrent backfill (exit_after_backfill = false) still
    // runs reconciliation because the live datasource is about to start.
    let backfill_only =
        indexer_config.backfill.enabled && indexer_config.backfill.exit_after_backfill;
    if !backfill_only {
        match (common_config.program_type, common_config.escrow_instance_id) {
            (ProgramType::Escrow, Some(seed)) => {
                run_startup_reconciliation(
                    &indexer_config.reconciliation,
                    common_config.program_type,
                    &storage,
                    &common_config.rpc_url,
                    &seed,
                )
                .await?;
            }
            (ProgramType::Escrow, None) => {
                return Err(IndexerError::Reconciliation(
                    ReconciliationError::InvalidPubkey {
                        pubkey: "<missing>".to_string(),
                        reason: "escrow_instance_id is required for escrow reconciliation"
                            .to_string(),
                    },
                ));
            }
            _ => {
                info!("Startup reconciliation skipped (non-escrow program)");
            }
        }
    } else {
        info!("Startup reconciliation skipped (backfill-only mode)");
    }

    // 3. Create channels
    let (instruction_tx, instruction_rx) = mpsc::channel(1000);
    let (checkpoint_tx, checkpoint_rx) = mpsc::channel(1000);

    // 4. Resolve the backfill range, gate the checkpoint writer to it, then start
    //    the writer. Checkpoint updates only begin once the processor starts further
    //    below (step 8), so the gate is always in place before the first update —
    //    no live-tip slot can slip past it and push the checkpoint over the gap.
    let mut checkpoint_writer = CheckpointWriter::new(storage.clone());

    // The inclusive backfill target when a gap is resolved; seeds the live
    // datasource so it resumes contiguously instead of from an independent tip.
    #[cfg(feature = "datasource-rpc")]
    let mut resolved_gap_target: Option<u64> = None;

    if indexer_config.backfill.enabled {
        #[cfg(not(feature = "datasource-rpc"))]
        return Err(DataSourceError::InvalidConfig {
            reason: "Datasource rpc needs to be enabled for backfilling".to_string(),
        });

        #[cfg(feature = "datasource-rpc")]
        {
            use crate::error::DataSourceError;

            let rpc_polling_config = indexer_config.rpc_polling.as_ref().ok_or_else(|| {
                DataSourceError::InvalidConfig {
                    reason: "RPC polling config required for backfill".to_string(),
                }
            })?;
            let rpc_poller = Arc::new(RpcPoller::new(
                indexer_config.backfill.rpc_url.clone(),
                rpc_polling_config.encoding,
                rpc_polling_config.commitment,
            ));

            let backfill_service = BackfillService::new(
                storage.clone(),
                rpc_poller,
                common_config.program_type,
                indexer_config.backfill.clone(),
                common_config.escrow_instance_id,
            );

            if indexer_config.backfill.exit_after_backfill {
                // Backfill-only: gate the writer to the fill range so a withheld
                // (failed-write) slot stalls the checkpoint instead of being
                // leapfrogged by a later one. No live stream, so a resolve failure
                // fails closed rather than falling back to ungated.
                let range = backfill_service.resolve_range().await?;
                if let Some((from_slot, target)) = range {
                    checkpoint_writer = checkpoint_writer.with_gate(from_slot, target);
                }
                let checkpoint_handle = checkpoint_writer.start(checkpoint_rx);
                info!("CheckpointWriter service started");
                if let Some((from_slot, target)) = range {
                    backfill_service
                        .run_range(from_slot, target, instruction_tx.clone())
                        .await?;
                }
                info!("Backfill completed, performing graceful cleanup...");
                if let Err(e) =
                    cleanup_after_backfill(checkpoint_handle, checkpoint_tx, storage).await
                {
                    error!("Cleanup after backfill failed: {}", e);
                    return Err(IndexerError::ShutdownChannelSend);
                }
                return Ok(());
            } else {
                // Gate the writer to the range backfill will fill. resolve_range retries
                // transient RPC failures; a persistent failure fails closed (see below).
                match backfill_service.resolve_range().await {
                    Ok(Some((from_slot, target))) => {
                        checkpoint_writer = checkpoint_writer.with_gate(from_slot, target);
                        resolved_gap_target = Some(target);
                        let instruction_tx_clone = instruction_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = backfill_service
                                .run_range(from_slot, target, instruction_tx_clone)
                                .await
                            {
                                error!("Backfill failed: {}", e);
                            } else {
                                info!("Backfill completed successfully");
                            }
                        });
                    }
                    Ok(None) => {
                        info!("No backfill gap; checkpoint writer left ungated");
                    }
                    Err(e) => {
                        error!(
                            "Backfill range resolution failed after retries; refusing to start \
                             rather than running ungated past the unfilled gap: {}",
                            e
                        );
                        return Err(e);
                    }
                }
            }
        }
    }

    let checkpoint_handle = checkpoint_writer.start(checkpoint_rx);
    info!("CheckpointWriter service started");

    // Resolve the durable frontier that seeds the live datasource. Read the
    // committed checkpoint once (the processor, its only producer, starts last,
    // so no update has flowed yet); fail closed via `?` rather than mis-seed.
    #[cfg(feature = "datasource-rpc")]
    let committed_checkpoint = get_last_checkpoint(&storage, common_config.program_type).await?;
    #[cfg(feature = "datasource-rpc")]
    let frontier = durable_frontier(resolved_gap_target, committed_checkpoint);

    // 6. Start datasource
    let mut datasource: Box<dyn DataSource> = match indexer_config.datasource_type {
        #[cfg(feature = "datasource-rpc")]
        DatasourceType::RpcPolling => {
            let rpc_config = indexer_config.rpc_polling.as_ref().ok_or_else(|| {
                DataSourceError::InvalidConfig {
                    reason: "RPC polling config required for RpcPolling datasource".to_string(),
                }
            })?;

            let mut source = RpcPollingSource::new(
                common_config.rpc_url.clone(),
                rpc_live_from_slot(
                    resolved_gap_target,
                    rpc_config.from_slot,
                    committed_checkpoint,
                ),
                rpc_config.poll_interval_ms,
                rpc_config.error_retry_interval_ms,
                rpc_config.batch_size,
                rpc_config.encoding,
                rpc_config.commitment,
                common_config.program_type,
                common_config.escrow_instance_id,
            );
            if let Some(h) = health.clone() {
                source = source.with_health(h);
            }
            Box::new(source)
        }

        #[cfg(feature = "datasource-yellowstone")]
        DatasourceType::Yellowstone => {
            let yellowstone_config = indexer_config.yellowstone.as_ref().ok_or_else(|| {
                DataSourceError::InvalidConfig {
                    reason: "Yellowstone config required for Yellowstone datasource".to_string(),
                }
            })?;

            info!(
                "Starting Yellowstone datasource from {} (commitment: {})",
                yellowstone_config.endpoint, yellowstone_config.commitment
            );

            let source = YellowstoneSource::new(
                yellowstone_config.endpoint.clone(),
                yellowstone_config.x_token.clone(),
                yellowstone_config.commitment.clone(),
                common_config.program_type,
                common_config.escrow_instance_id,
            );

            #[cfg(feature = "datasource-rpc")]
            let source = {
                use solana_sdk::commitment_config::CommitmentLevel as SdkCommitmentLevel;
                use solana_transaction_status::UiTransactionEncoding;

                let encoding = indexer_config
                    .rpc_polling
                    .as_ref()
                    .map(|c| c.encoding)
                    .unwrap_or(UiTransactionEncoding::Json);

                let commitment = match yellowstone_config.commitment.to_lowercase().as_str() {
                    "processed" => SdkCommitmentLevel::Processed,
                    "finalized" => SdkCommitmentLevel::Finalized,
                    _ => SdkCommitmentLevel::Confirmed,
                };

                let gap_rpc_poller = Arc::new(RpcPoller::new(
                    indexer_config.backfill.rpc_url.clone(),
                    encoding,
                    commitment,
                ));

                info!(
                    "Yellowstone gap detection enabled (max_gap: {}, batch_size: {})",
                    indexer_config.backfill.max_gap_slots, indexer_config.backfill.batch_size
                );

                source
                    .with_gap_detection(
                        gap_rpc_poller,
                        indexer_config.backfill.max_gap_slots,
                        indexer_config.backfill.batch_size,
                    )
                    .with_storage(storage.clone())
                    .with_initial_gap_floor(frontier)
            };

            let source = if let Some(h) = health.clone() {
                source.with_health(h)
            } else {
                source
            };

            Box::new(source)
        }

        // Catch-all for disabled features
        #[allow(unreachable_patterns)]
        _ => {
            return Err(DataSourceError::InvalidConfig {
                reason: format!(
                    "Datasource {:?} is not compiled. Rebuild with the appropriate feature flag",
                    indexer_config.datasource_type
                ),
            }
            .into());
        }
    };

    // 7. Create cancellation token for graceful shutdown
    let cancellation_token = CancellationToken::new();

    info!("Starting datasource...");
    let datasource_handle = datasource
        .start(instruction_tx.clone(), cancellation_token.clone())
        .await?;

    // 8. Start transaction processor
    let mut transaction_processor =
        TransactionProcessor::new(storage.clone(), checkpoint_tx.clone());
    // Wire the escrow instance scope. Config validation guarantees Some for the
    // Escrow program; None here means the Withdraw program, where no instance
    // scoping applies.
    if let Some(instance_id) = common_config.escrow_instance_id {
        transaction_processor = transaction_processor.with_escrow_instance_id(instance_id);
    }
    if let Some(h) = health.clone() {
        transaction_processor = transaction_processor.with_health(h);
    }
    let processor_handle = tokio::spawn(async move {
        if let Err(e) = transaction_processor.start(instruction_rx).await {
            error!("TransactionProcessor error: {}", e);
        }
    });

    info!("Indexer started, waiting for shutdown signal...");

    // 9. Wait for shutdown signal
    signal::ctrl_c()
        .await
        .map_err(|_| IndexerError::ShutdownChannelSend)?;
    info!("Shutdown signal received, initiating graceful shutdown...");

    // 10. Graceful shutdown
    shutdown_indexer(
        cancellation_token,
        storage,
        datasource,
        datasource_handle,
        instruction_tx,
        checkpoint_tx,
        checkpoint_handle,
        processor_handle,
    )
    .await
    .map_err(|_| IndexerError::ShutdownChannelSend)?;

    info!("Indexer shutdown complete");
    Ok(())
}

#[cfg(all(test, feature = "datasource-rpc"))]
mod tests {
    use super::{durable_frontier, rpc_live_from_slot};

    // T0 inclusive backfill target; CP a prior durable checkpoint; CFG a stale
    // operator-set from_slot. Distinct values so a regressed branch can't alias.
    const T0: u64 = 110;
    const CP: u64 = 90;
    const CFG: u64 = 500;

    #[test]
    fn rpc_live_from_slot_covers_every_case() {
        let cases = [
            // (resolved_gap_target, config_from_slot, checkpoint, expected)
            (Some(T0), None, 0, Some(T0 + 1)),
            (Some(T0), Some(CFG), CP, Some(T0 + 1)),
            (None, Some(CFG), CP, Some(CFG)),
            (None, None, CP, Some(CP + 1)),
            (None, None, 0, None),
            (None, Some(CFG), 0, Some(CFG)),
        ];
        for (gap, cfg, cp, expected) in cases {
            assert_eq!(
                rpc_live_from_slot(gap, cfg, cp),
                expected,
                "gap={gap:?} cfg={cfg:?} cp={cp}"
            );
        }
    }

    #[test]
    fn durable_frontier_covers_every_case() {
        assert_eq!(durable_frontier(Some(T0), 0), Some(T0));
        assert_eq!(durable_frontier(Some(T0), CP), Some(T0));
        assert_eq!(durable_frontier(None, CP), Some(CP));
        assert_eq!(durable_frontier(None, 0), None);
    }
}
