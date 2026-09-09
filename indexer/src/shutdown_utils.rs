//! Graceful shutdown utilities for both indexer and operator modes
//!
//! This module provides coordinated shutdown logic with:
//! - Total timeout enforcement
//! - Stage-by-stage shutdown with individual timeouts
//! - Progress logging for long-running operations
//! - Forced exit on timeout (configurable)
//! - Buffer time before storage closure

use crate::config::ProgramType;
use crate::error::IndexerError;
use crate::indexer::checkpoint::{get_last_checkpoint, CheckpointMsg};
use crate::indexer::datasource::common::datasource::DataSource;
use crate::indexer::datasource::common::types::ProcessorMessage;
use crate::storage::Storage;
use private_channel_metrics::MetricLabel;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub const SHUTDOWN_TOTAL_TIMEOUT_SECS: u64 = 90;

pub const SHUTDOWN_DATASOURCE_TIMEOUT_SECS: u64 = 10;
pub const SHUTDOWN_DATASOURCE_COMPLETION_TIMEOUT_SECS: u64 = 5;
pub const SHUTDOWN_PROCESSOR_DRAIN_TIMEOUT_SECS: u64 = 20;
pub const SHUTDOWN_CHECKPOINT_DRAIN_TIMEOUT_SECS: u64 = 10;
/// Drain bound for a one-shot backfill's final flush, which is deliberately far longer than
/// the live-shutdown one above.
///
/// That flush has to be allowed to finish, because the checkpoint it writes is what the
/// completeness check then reads. The pool waits up to 30 seconds for a connection before it
/// gives up on its own, so anything below that cancels writers that were never stuck and
/// turns a finished repair into a reported failure. Nothing else is running by this point,
/// so the only cost of waiting is a slower exit on a database that is already in trouble.
pub const SHUTDOWN_BACKFILL_DRAIN_TIMEOUT_SECS: u64 = 45;

/// How long the pool waits for a connection before giving up on its own.
const POOL_ACQUIRE_TIMEOUT_SECS: u64 = 30;
/// Enforced at build time: below this line the cancellation above stops being a backstop for a
/// stuck writer and starts cutting off healthy ones.
const _: () = assert!(SHUTDOWN_BACKFILL_DRAIN_TIMEOUT_SECS > POOL_ACQUIRE_TIMEOUT_SECS);

pub const SHUTDOWN_SENDER_BASE_TIMEOUT_SECS: u64 = 10;
pub const SHUTDOWN_SENDER_TIME_PER_TX_SECS: u64 = 2;
pub const SHUTDOWN_STORAGE_WRITER_DRAIN_TIMEOUT_SECS: u64 = 10;

pub const SHUTDOWN_STORAGE_CLOSE_TIMEOUT_SECS: u64 = 15;
pub const SHUTDOWN_SHUTDOWN_BUFFER_TIME_SECS: u64 = 2;

pub const SHUTDOWN_FORCE_EXIT_ON_TIMEOUT: bool = true;

// TODO: add to cli args
/// Shutdown configuration for graceful termination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Total timeout for entire shutdown process (seconds)
    pub total_timeout_secs: u64,

    // Indexer-specific timeouts
    /// Timeout for datasource shutdown call (seconds)
    pub datasource_shutdown_timeout_secs: u64,
    /// Timeout for datasource task to complete after shutdown (seconds)
    pub datasource_completion_timeout_secs: u64,
    /// Timeout for transaction processor to drain pipeline (seconds)
    pub processor_drain_timeout_secs: u64,
    /// Timeout for checkpoint writer to drain (seconds)
    pub checkpoint_drain_timeout_secs: u64,
    /// Timeout for a one-shot backfill's final checkpoint flush (seconds)
    pub backfill_drain_timeout_secs: u64,

    // Operator-specific timeouts
    /// Base timeout for sender to complete in-flight transactions (seconds)
    /// Actual timeout will be calculated based on batch size
    pub sender_base_timeout_secs: u64,
    /// Estimated time per transaction for sender (seconds)
    /// Used to calculate dynamic sender timeout: base + (time_per_tx * batch_size)
    pub sender_time_per_tx_secs: u64,
    /// Timeout for storage writer to drain status updates (seconds)
    pub storage_writer_drain_timeout_secs: u64,

    // Common timeouts
    /// Timeout for storage connection pool to close (seconds)
    pub storage_close_timeout_secs: u64,
    /// Buffer time for shutdown (seconds)
    pub shutdown_buffer_time_secs: u64,

    /// Force exit if timeout is exceeded
    pub force_exit_on_timeout: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            total_timeout_secs: SHUTDOWN_TOTAL_TIMEOUT_SECS,

            // Indexer defaults
            datasource_shutdown_timeout_secs: SHUTDOWN_DATASOURCE_TIMEOUT_SECS,
            datasource_completion_timeout_secs: SHUTDOWN_DATASOURCE_COMPLETION_TIMEOUT_SECS,
            processor_drain_timeout_secs: SHUTDOWN_PROCESSOR_DRAIN_TIMEOUT_SECS,
            checkpoint_drain_timeout_secs: SHUTDOWN_CHECKPOINT_DRAIN_TIMEOUT_SECS,
            backfill_drain_timeout_secs: SHUTDOWN_BACKFILL_DRAIN_TIMEOUT_SECS,

            // Operator defaults
            sender_base_timeout_secs: SHUTDOWN_SENDER_BASE_TIMEOUT_SECS,
            sender_time_per_tx_secs: SHUTDOWN_SENDER_TIME_PER_TX_SECS,
            storage_writer_drain_timeout_secs: SHUTDOWN_STORAGE_WRITER_DRAIN_TIMEOUT_SECS,

            // Common defaults
            storage_close_timeout_secs: SHUTDOWN_STORAGE_CLOSE_TIMEOUT_SECS,
            shutdown_buffer_time_secs: SHUTDOWN_SHUTDOWN_BUFFER_TIME_SECS,

            force_exit_on_timeout: SHUTDOWN_FORCE_EXIT_ON_TIMEOUT,
        }
    }
}

/// Gracefully shutdown the indexer with timeouts and proper resource cleanup
#[allow(clippy::too_many_arguments)]
pub async fn shutdown_indexer(
    cancellation_token: CancellationToken,
    storage: Arc<Storage>,
    datasource: Box<dyn DataSource>,
    datasource_handle: tokio::task::JoinHandle<()>,
    instruction_tx: mpsc::Sender<ProcessorMessage>,
    checkpoint_tx: mpsc::Sender<CheckpointMsg>,
    checkpoint_handle: tokio::task::JoinHandle<()>,
    processor_handle: tokio::task::JoinHandle<Result<(), IndexerError>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shutdown_start = std::time::Instant::now();

    let config = ShutdownConfig::default();

    let shutdown_result = tokio::time::timeout(
        Duration::from_secs(config.total_timeout_secs),
        perform_indexer_shutdown_stages(
            cancellation_token,
            storage,
            datasource,
            datasource_handle,
            instruction_tx,
            checkpoint_tx,
            checkpoint_handle,
            processor_handle,
            &config,
        ),
    )
    .await;

    let shutdown_duration = shutdown_start.elapsed();

    match shutdown_result {
        Ok(Ok(_)) => {
            info!(
                "Indexer shutdown completed successfully in {:.2}s",
                shutdown_duration.as_secs_f64()
            );
            Ok(())
        }
        Ok(Err(e)) => {
            warn!(
                "Indexer shutdown completed with errors in {:.2}s: {}",
                shutdown_duration.as_secs_f64(),
                e
            );
            Err(e)
        }
        Err(_) => {
            error!(
                "Indexer shutdown exceeded total timeout of {}s (actual: {:.2}s)",
                config.total_timeout_secs,
                shutdown_duration.as_secs_f64()
            );

            if config.force_exit_on_timeout {
                error!("Force exit enabled, terminating process");
                std::process::exit(1);
            }

            Err("Shutdown timeout exceeded".into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn perform_indexer_shutdown_stages(
    cancellation_token: CancellationToken,
    storage: Arc<Storage>,
    mut datasource: Box<dyn DataSource>,
    datasource_handle: tokio::task::JoinHandle<()>,
    instruction_tx: mpsc::Sender<ProcessorMessage>,
    checkpoint_tx: mpsc::Sender<CheckpointMsg>,
    checkpoint_handle: tokio::task::JoinHandle<()>,
    processor_handle: tokio::task::JoinHandle<Result<(), IndexerError>>,
    config: &ShutdownConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Stage 1: Signal cancellation to datasource
    info!("Stage 1: Signaling cancellation to datasource...");
    cancellation_token.cancel();

    // Stage 2: Shutdown datasource with timeout
    info!("Stage 2: Shutting down datasource...");
    match tokio::time::timeout(
        Duration::from_secs(config.datasource_shutdown_timeout_secs),
        datasource.shutdown(),
    )
    .await
    {
        Ok(Ok(_)) => info!("Datasource shutdown complete"),
        Ok(Err(e)) => warn!("Datasource shutdown error: {}", e),
        Err(_) => warn!(
            "Datasource shutdown timed out after {}s",
            config.datasource_shutdown_timeout_secs
        ),
    }

    // Wait for datasource task to complete with progress logging
    match wait_with_progress(
        datasource_handle,
        Duration::from_secs(config.datasource_completion_timeout_secs),
        "datasource task",
    )
    .await
    {
        Ok(_) => info!("Datasource task completed"),
        Err(_) => {
            warn!(
                "Datasource task did not complete in {}s",
                config.datasource_completion_timeout_secs
            );
        }
    }

    // Stage 3: Close instruction channel and drain processor
    info!("Stage 3: Draining transaction processor...");
    drop(instruction_tx);

    let processor_result = tokio::time::timeout(
        Duration::from_secs(config.processor_drain_timeout_secs),
        processor_handle,
    )
    .await;

    match processor_result {
        Ok(Ok(_)) => info!("Transaction processor drained successfully"),
        Ok(Err(e)) => warn!("Transaction processor error: {:?}", e),
        Err(_) => {
            warn!(
                "Transaction processor drain timed out after {}s",
                config.processor_drain_timeout_secs
            );
        }
    }

    // Stage 4: Close checkpoint channel and drain checkpoint writer
    info!("Stage 4: Draining checkpoint writer...");
    drop(checkpoint_tx);

    let checkpoint_result = tokio::time::timeout(
        Duration::from_secs(config.checkpoint_drain_timeout_secs),
        checkpoint_handle,
    )
    .await;

    match checkpoint_result {
        Ok(Ok(_)) => info!("Checkpoint writer drained successfully"),
        Ok(Err(e)) => warn!("Checkpoint writer error: {:?}", e),
        Err(_) => {
            warn!(
                "Checkpoint writer drain timed out after {}s",
                config.checkpoint_drain_timeout_secs
            );
        }
    }

    // Stage 5: Add buffer time before storage close for safety
    info!(
        "Stage 5: Waiting {}s buffer time before storage close...",
        config.shutdown_buffer_time_secs
    );
    tokio::time::sleep(Duration::from_secs(config.shutdown_buffer_time_secs)).await;

    // Close storage connections
    info!("Closing storage connections...");
    match tokio::time::timeout(
        Duration::from_secs(config.storage_close_timeout_secs),
        storage.close(),
    )
    .await
    {
        Ok(Ok(_)) => info!("Storage closed successfully"),
        Ok(Err(e)) => warn!("Storage close error: {}", e),
        Err(_) => warn!(
            "Storage close timed out after {}s",
            config.storage_close_timeout_secs
        ),
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn shutdown_operator(
    cancellation_token: CancellationToken,
    storage: Arc<Storage>,
    fetcher_handle: tokio::task::JoinHandle<()>,
    processor_handle: tokio::task::JoinHandle<()>,
    sender_handle: tokio::task::JoinHandle<()>,
    storage_writer_handle: tokio::task::JoinHandle<()>,
    reconciliation_handle: tokio::task::JoinHandle<()>,
    feepayer_monitor_handle: tokio::task::JoinHandle<()>,
    recovery_handle: tokio::task::JoinHandle<()>,
    batch_size: u16,
    db_poll_interval: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let shutdown_start = std::time::Instant::now();

    let config = ShutdownConfig::default();

    let shutdown_result = tokio::time::timeout(
        Duration::from_secs(config.total_timeout_secs),
        perform_operator_shutdown_stages(
            cancellation_token,
            storage,
            fetcher_handle,
            processor_handle,
            sender_handle,
            storage_writer_handle,
            reconciliation_handle,
            feepayer_monitor_handle,
            recovery_handle,
            batch_size,
            db_poll_interval,
            &config,
        ),
    )
    .await;

    let shutdown_duration = shutdown_start.elapsed();

    match shutdown_result {
        Ok(Ok(_)) => {
            info!(
                "Operator shutdown completed successfully in {:.2}s",
                shutdown_duration.as_secs_f64()
            );
            Ok(())
        }
        Ok(Err(e)) => {
            warn!(
                "Operator shutdown completed with errors in {:.2}s: {}",
                shutdown_duration.as_secs_f64(),
                e
            );
            Err(e)
        }
        Err(_) => {
            error!(
                "Operator shutdown exceeded total timeout of {}s (actual: {:.2}s)",
                config.total_timeout_secs,
                shutdown_duration.as_secs_f64()
            );

            if config.force_exit_on_timeout {
                error!("Force exit enabled, terminating process");
                std::process::exit(1);
            }

            Err("Shutdown timeout exceeded".into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn perform_operator_shutdown_stages(
    cancellation_token: CancellationToken,
    storage: Arc<Storage>,
    fetcher_handle: tokio::task::JoinHandle<()>,
    processor_handle: tokio::task::JoinHandle<()>,
    sender_handle: tokio::task::JoinHandle<()>,
    storage_writer_handle: tokio::task::JoinHandle<()>,
    reconciliation_handle: tokio::task::JoinHandle<()>,
    feepayer_monitor_handle: tokio::task::JoinHandle<()>,
    recovery_handle: tokio::task::JoinHandle<()>,
    batch_size: u16,
    db_poll_interval: Duration,
    config: &ShutdownConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Stage 1: Signal cancellation to stop fetcher and sender
    info!("Stage 1: Signaling cancellation to fetcher and sender...");
    cancellation_token.cancel();

    // Stage 2: Wait for fetcher to stop polling (should exit within one poll interval + buffer)
    let fetcher_timeout = db_poll_interval.as_secs() + config.shutdown_buffer_time_secs;
    info!(
        "Stage 2: Waiting for fetcher to stop (timeout: {}s)...",
        fetcher_timeout
    );

    match wait_with_progress(
        fetcher_handle,
        Duration::from_secs(fetcher_timeout),
        "fetcher",
    )
    .await
    {
        Ok(_) => info!("Fetcher stopped successfully"),
        Err(_) => {
            warn!("Fetcher did not stop in {}s", fetcher_timeout);
        }
    }

    // Stage 3: Drain processor pipeline (processor_tx already moved into fetcher)
    info!("Stage 3: Waiting for processor to drain...");
    let processor_result = tokio::time::timeout(
        Duration::from_secs(config.processor_drain_timeout_secs),
        processor_handle,
    )
    .await;

    match processor_result {
        Ok(Ok(_)) => info!("Processor drained successfully"),
        Ok(Err(e)) => warn!("Processor error: {:?}", e),
        Err(_) => {
            warn!("Processor drain timed out");
        }
    }

    // Stage 4: Drain sender (complete in-flight transactions)
    // Dynamic timeout: base + (time_per_tx * batch_size)
    let sender_timeout =
        config.sender_base_timeout_secs + (config.sender_time_per_tx_secs * batch_size as u64);
    info!(
        "Stage 4: Waiting for sender to drain (timeout: {}s = base {}s + {}s per tx * {} batch size)...",
        sender_timeout, config.sender_base_timeout_secs, config.sender_time_per_tx_secs, batch_size
    );

    match wait_with_progress(sender_handle, Duration::from_secs(sender_timeout), "sender").await {
        Ok(_) => info!("Sender drained successfully"),
        Err(_) => {
            warn!("Sender drain timed out after {}s", sender_timeout);
        }
    }

    // Stage 4b: drain recovery before the storage writer (it's a producer on storage_tx).
    info!("Stage 4b: Waiting for recovery worker to drain...");
    let recovery_result = tokio::time::timeout(
        Duration::from_secs(config.storage_writer_drain_timeout_secs),
        recovery_handle,
    )
    .await;

    match recovery_result {
        Ok(Ok(_)) => info!("Recovery worker drained successfully"),
        Ok(Err(e)) => warn!("Recovery worker error: {:?}", e),
        Err(_) => {
            warn!(
                "Recovery worker drain timed out after {}s",
                config.storage_writer_drain_timeout_secs
            );
        }
    }

    // Stage 5: drain storage writer (consumer); all producers have exited.
    info!("Stage 5: Waiting for storage writer to drain (writing final status updates)...");
    let storage_writer_result = tokio::time::timeout(
        Duration::from_secs(config.storage_writer_drain_timeout_secs),
        storage_writer_handle,
    )
    .await;

    match storage_writer_result {
        Ok(Ok(_)) => info!("Storage writer drained successfully"),
        Ok(Err(e)) => warn!("Storage writer error: {:?}", e),
        Err(_) => {
            warn!(
                "Storage writer drain timed out after {}s",
                config.storage_writer_drain_timeout_secs
            );
        }
    }

    // Stage 6: Drain reconciliation and feepayer monitor loops
    info!("Stage 6: Waiting for reconciliation loop to drain...");
    let reconciliation_result = tokio::time::timeout(
        Duration::from_secs(config.storage_writer_drain_timeout_secs),
        reconciliation_handle,
    )
    .await;

    match reconciliation_result {
        Ok(Ok(_)) => info!("Reconciliation loop drained successfully"),
        Ok(Err(e)) => warn!("Reconciliation loop error: {:?}", e),
        Err(_) => {
            warn!(
                "Reconciliation loop drain timed out after {}s",
                config.storage_writer_drain_timeout_secs
            );
        }
    }

    info!("Stage 6b: Waiting for feepayer monitor to drain...");
    let feepayer_result = tokio::time::timeout(
        Duration::from_secs(config.storage_writer_drain_timeout_secs),
        feepayer_monitor_handle,
    )
    .await;

    match feepayer_result {
        Ok(Ok(_)) => info!("Feepayer monitor drained successfully"),
        Ok(Err(e)) => warn!("Feepayer monitor error: {:?}", e),
        Err(_) => {
            warn!(
                "Feepayer monitor drain timed out after {}s",
                config.storage_writer_drain_timeout_secs
            );
        }
    }

    // Stage 7: Add buffer time before storage close for safety
    info!(
        "Stage 7: Waiting {}s buffer time before storage close...",
        config.shutdown_buffer_time_secs
    );
    tokio::time::sleep(Duration::from_secs(config.shutdown_buffer_time_secs)).await;

    // Close storage connections
    info!("Closing storage connections...");
    match tokio::time::timeout(
        Duration::from_secs(config.storage_close_timeout_secs),
        storage.close(),
    )
    .await
    {
        Ok(Ok(_)) => info!("Storage closed successfully"),
        Ok(Err(e)) => warn!("Storage close error: {}", e),
        Err(_) => warn!(
            "Storage close timed out after {}s",
            config.storage_close_timeout_secs
        ),
    }

    Ok(())
}

/// Graceful cleanup after backfill completion.
///
/// `verify` names the program and the inclusive top slot a one-shot backfill was asked
/// to reach. When set, the durable checkpoint is re-read after the writer has drained
/// and the run only succeeds if it got that far. Pass `None` to keep the drain-and-close
/// behaviour with no completeness claim.
pub async fn cleanup_after_backfill(
    mut checkpoint_handle: tokio::task::JoinHandle<()>,
    checkpoint_tx: mpsc::Sender<CheckpointMsg>,
    storage: Arc<Storage>,
    verify: Option<(ProgramType, u64)>,
) -> Result<(), IndexerError> {
    info!("Cleaning up after backfill completion...");

    let config = ShutdownConfig::default();

    // Stage 1: Close checkpoint channel and drain
    info!("Draining checkpoint writer...");
    drop(checkpoint_tx);

    // Set when the writer had to be cancelled, which changes what a short checkpoint means.
    let mut cancelled_after_secs = None;
    match tokio::time::timeout(
        Duration::from_secs(config.backfill_drain_timeout_secs),
        &mut checkpoint_handle,
    )
    .await
    {
        Ok(Ok(_)) => info!("Checkpoint writer drained successfully"),
        Ok(Err(e)) => warn!("Checkpoint writer error: {:?}", e),
        Err(_) => {
            // Timing out only stops waiting; the task keeps running unless it is cancelled.
            // Both stages below would then race it: the read could miss a flush that lands
            // moments later and call a finished backfill incomplete, and the close would
            // pull the pool out from under a write still in flight. Cancelling first makes
            // the checkpoint read afterwards a settled fact rather than a snapshot.
            //
            // The bound above is set so that reaching here means the writer is genuinely
            // stuck rather than slow, which is what makes cancelling it the right call.
            // Waiting without a bound is not the alternative it looks like: a connection that
            // never answers would hold a one-shot job open forever, so the wait stays bounded
            // and the shortfall is reported under its own error instead.
            warn!(
                "Checkpoint writer still running after {}s; cancelling it before verifying",
                config.backfill_drain_timeout_secs
            );
            checkpoint_handle.abort();
            let _ = checkpoint_handle.await;
            cancelled_after_secs = Some(config.backfill_drain_timeout_secs);
        }
    }

    // Stage 2: Confirm the range was recorded, after the drain flushed the last batch and
    // before stage 3 closes the pool the read needs. A drained writer proves nothing on its
    // own: a stalled gate holds the frontier short of the target, and a failed final write
    // is only logged, so the sole trustworthy signal is the checkpoint the database holds.
    let verdict = match verify {
        Some((program_type, target)) => {
            verify_checkpoint_reached(&storage, program_type, target, cancelled_after_secs)
                .await
                .inspect_err(|e| error!("Backfill completeness check failed: {}", e))
        }
        None => Ok(()),
    };

    // Stage 3: Close storage connections
    info!("Closing storage connections...");
    match tokio::time::timeout(
        Duration::from_secs(config.storage_close_timeout_secs),
        storage.close(),
    )
    .await
    {
        Ok(Ok(_)) => info!("Storage closed successfully"),
        Ok(Err(e)) => warn!("Storage close error: {}", e),
        Err(_) => warn!("Storage close timed out"),
    }

    verdict?;
    info!("Backfill cleanup complete");
    Ok(())
}

/// Fail unless the durable checkpoint for `program_type` reached `target`.
///
/// Compared with `>=` so a live indexer that pushed the checkpoint on cannot fail a repair.
/// `cancelled_after_secs` carries how long the writer was given before it was stopped, which
/// separates a checkpoint that is short because a slot never made it from one that is short
/// only because the flush never got to confirm. Both still fail, since a cancelled writer
/// leaves the two indistinguishable in the database, but they need different operator action.
async fn verify_checkpoint_reached(
    storage: &Arc<Storage>,
    program_type: ProgramType,
    target: u64,
    cancelled_after_secs: Option<u64>,
) -> Result<(), IndexerError> {
    let committed = get_last_checkpoint(storage, program_type).await?;
    if committed.is_some_and(|slot| slot >= target) {
        info!(
            "Backfill verified for {:?}: checkpoint {:?} reached target {}",
            program_type, committed, target
        );
        return Ok(());
    }
    match cancelled_after_secs {
        Some(waited_secs) => Err(IndexerError::BackfillCheckpointUnconfirmed {
            program_type: program_type.as_label().to_string(),
            committed,
            target,
            waited_secs,
        }),
        None => Err(IndexerError::BackfillIncomplete {
            program_type: program_type.as_label().to_string(),
            committed,
            target,
        }),
    }
}

async fn wait_with_progress<T>(
    mut handle: tokio::task::JoinHandle<T>,
    timeout: Duration,
    task_name: &str,
) -> Result<(), ()> {
    let interval_secs = 5;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let timeout_future = tokio::time::sleep(timeout);
    tokio::pin!(timeout_future);

    let mut elapsed_secs = 0u64;

    loop {
        tokio::select! {
            _ = &mut timeout_future => {
                return Err(());
            }
            result = &mut handle => {
                // Task completed (successfully or with panic)
                match result {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        warn!("{} task panicked: {:?}", task_name, e);
                        return Ok(());  // Still count as "completed"
                    }
                }
            }
            _ = interval.tick() => {
                elapsed_secs += interval_secs;
                info!("Still waiting for {} ({} seconds elapsed)...", task_name, elapsed_secs);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::storage::mock::MockStorage;

    async fn wait_with_progress_test<T>(
        handle: tokio::task::JoinHandle<T>,
        timeout: Duration,
        task_name: &str,
    ) -> Result<(), ()> {
        wait_with_progress(handle, timeout, task_name).await
    }

    // ====================================================================
    // ShutdownConfig::default()
    // ====================================================================

    #[test]
    fn shutdown_config_default_values() {
        let config = ShutdownConfig::default();
        assert_eq!(config.total_timeout_secs, SHUTDOWN_TOTAL_TIMEOUT_SECS);
        assert_eq!(
            config.datasource_shutdown_timeout_secs,
            SHUTDOWN_DATASOURCE_TIMEOUT_SECS
        );
        assert_eq!(
            config.datasource_completion_timeout_secs,
            SHUTDOWN_DATASOURCE_COMPLETION_TIMEOUT_SECS
        );
        assert_eq!(
            config.processor_drain_timeout_secs,
            SHUTDOWN_PROCESSOR_DRAIN_TIMEOUT_SECS
        );
        assert_eq!(
            config.checkpoint_drain_timeout_secs,
            SHUTDOWN_CHECKPOINT_DRAIN_TIMEOUT_SECS
        );
        assert_eq!(
            config.backfill_drain_timeout_secs,
            SHUTDOWN_BACKFILL_DRAIN_TIMEOUT_SECS
        );
        assert_eq!(
            config.sender_base_timeout_secs,
            SHUTDOWN_SENDER_BASE_TIMEOUT_SECS
        );
        assert_eq!(
            config.sender_time_per_tx_secs,
            SHUTDOWN_SENDER_TIME_PER_TX_SECS
        );
        assert_eq!(
            config.storage_writer_drain_timeout_secs,
            SHUTDOWN_STORAGE_WRITER_DRAIN_TIMEOUT_SECS
        );
        assert_eq!(
            config.storage_close_timeout_secs,
            SHUTDOWN_STORAGE_CLOSE_TIMEOUT_SECS
        );
        assert_eq!(
            config.shutdown_buffer_time_secs,
            SHUTDOWN_SHUTDOWN_BUFFER_TIME_SECS
        );
        assert_eq!(config.force_exit_on_timeout, SHUTDOWN_FORCE_EXIT_ON_TIMEOUT);
    }

    // ====================================================================
    // cleanup_after_backfill
    // ====================================================================

    /// Drain-only checkpoint writer plus the channel that feeds it.
    fn drained_writer() -> (tokio::task::JoinHandle<()>, mpsc::Sender<CheckpointMsg>) {
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel::<CheckpointMsg>(10);
        let handle = tokio::spawn(async move {
            let mut rx = checkpoint_rx;
            while rx.recv().await.is_some() {}
        });
        (handle, checkpoint_tx)
    }

    /// No target means no verification, which is what the resync rebuild relies on.
    #[tokio::test]
    async fn cleanup_after_backfill_without_target_skips_verification() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let (checkpoint_handle, checkpoint_tx) = drained_writer();

        let result = cleanup_after_backfill(checkpoint_handle, checkpoint_tx, storage, None).await;

        assert!(
            result.is_ok(),
            "an absent checkpoint must not fail a run that asked for no verification"
        );
    }

    /// A writer that outlasts the drain timeout is cancelled, not left running.
    ///
    /// Waiting on a task and stopping it are different things, and only the second one makes
    /// the stages that follow safe. The probe is an `Arc` the task owns: a cancelled task is
    /// dropped and releases its clone, while a merely detached one keeps it alive, so the
    /// strong count separates the two cases that otherwise look identical from out here.
    #[tokio::test(start_paused = true)]
    async fn cleanup_after_backfill_cancels_a_writer_that_overruns_the_drain_timeout() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let (checkpoint_tx, _checkpoint_rx) = mpsc::channel::<CheckpointMsg>(1);

        let probe = Arc::new(());
        let held = probe.clone();
        let checkpoint_handle = tokio::spawn(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        // Let the task reach its await point so it really owns the clone.
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&probe), 2, "task must hold the probe");

        let result = cleanup_after_backfill(checkpoint_handle, checkpoint_tx, storage, None).await;

        assert!(result.is_ok(), "a drain timeout alone is not a failed run");
        assert_eq!(
            Arc::strong_count(&probe),
            1,
            "the writer must be cancelled before the checkpoint is read and the pool closed"
        );
    }

    /// A cancelled writer is reported apart from a genuinely short frontier.
    ///
    /// Both leave the same stale checkpoint behind, so the database cannot tell them apart and
    /// both have to fail. What differs is what the operator does next: chase a slot the
    /// pipeline could not store, or wait for the database to recover and run it again.
    #[tokio::test(start_paused = true)]
    async fn cleanup_after_backfill_names_a_cancelled_writer_rather_than_a_missing_slot() {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 100);
        let storage = Arc::new(Storage::Mock(mock));
        let (checkpoint_tx, _checkpoint_rx) = mpsc::channel::<CheckpointMsg>(1);
        let checkpoint_handle = tokio::spawn(std::future::pending::<()>());

        let result = cleanup_after_backfill(
            checkpoint_handle,
            checkpoint_tx,
            storage,
            Some((ProgramType::Escrow, 150)),
        )
        .await;

        match result {
            Err(IndexerError::BackfillCheckpointUnconfirmed {
                committed,
                target,
                waited_secs,
                ..
            }) => {
                assert_eq!(committed, Some(100));
                assert_eq!(target, 150);
                assert_eq!(waited_secs, SHUTDOWN_BACKFILL_DRAIN_TIMEOUT_SECS);
            }
            other => panic!("a cancelled writer must not read as a missing slot, got: {other:?}"),
        }
    }

    /// A frontier level with or ahead of the target is a complete run.
    #[tokio::test]
    async fn cleanup_after_backfill_accepts_frontier_at_or_past_target() {
        // A live indexer running alongside a repair can push the checkpoint past the target.
        for committed in [150u64, 151] {
            let mock = MockStorage::new();
            mock.set_checkpoint("escrow", committed);
            let storage = Arc::new(Storage::Mock(mock));
            let (checkpoint_handle, checkpoint_tx) = drained_writer();

            let result = cleanup_after_backfill(
                checkpoint_handle,
                checkpoint_tx,
                storage,
                Some((ProgramType::Escrow, 150)),
            )
            .await;

            assert!(
                result.is_ok(),
                "committed {committed} must pass: {result:?}"
            );
        }
    }

    /// A frontier short of the target means the range was not fully recorded.
    #[tokio::test]
    async fn cleanup_after_backfill_rejects_frontier_below_target() {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 149);
        let storage = Arc::new(Storage::Mock(mock));
        let (checkpoint_handle, checkpoint_tx) = drained_writer();

        let result = cleanup_after_backfill(
            checkpoint_handle,
            checkpoint_tx,
            storage,
            Some((ProgramType::Escrow, 150)),
        )
        .await;

        match result {
            Err(IndexerError::BackfillIncomplete {
                program_type,
                committed,
                target,
            }) => {
                assert_eq!(program_type, "escrow");
                assert_eq!(committed, Some(149));
                assert_eq!(target, 150);
            }
            other => panic!("a short frontier must be reported, got: {other:?}"),
        }
    }

    /// An unreadable checkpoint is not evidence of success.
    #[tokio::test]
    async fn cleanup_after_backfill_surfaces_checkpoint_read_failure() {
        let mock = MockStorage::new();
        mock.set_should_fail("get_committed_checkpoint", true);
        let storage = Arc::new(Storage::Mock(mock));
        let (checkpoint_handle, checkpoint_tx) = drained_writer();

        let result = cleanup_after_backfill(
            checkpoint_handle,
            checkpoint_tx,
            storage,
            Some((ProgramType::Escrow, 150)),
        )
        .await;

        assert!(
            result.is_err(),
            "a checkpoint read that fails must fail the run rather than pass it"
        );
    }

    // ====================================================================
    // wait_with_progress (via test wrapper)
    // ====================================================================

    #[tokio::test]
    async fn wait_with_progress_immediate_task_ok() {
        let handle = tokio::spawn(async { 42 });
        let result = wait_with_progress_test(handle, Duration::from_secs(5), "test-task").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn wait_with_progress_timeout_returns_err() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let result = wait_with_progress_test(handle, Duration::from_millis(50), "slow-task").await;
        assert!(result.is_err());
    }
}
