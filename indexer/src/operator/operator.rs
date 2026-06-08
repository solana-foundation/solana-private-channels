use crate::config::OperatorConfig;
use crate::error::OperatorError;
use crate::metrics;
use crate::operator::{
    feepayer_monitor, fetcher, processor, reconciliation, recovery, sender, DbTransactionWriter,
    RetryConfig, RpcClientWithRetry, SignerUtil,
};
use crate::shutdown_utils::shutdown_operator;
use crate::storage::Storage;
use crate::PrivateChannelIndexerConfig;
use private_channel_metrics::{HealthState, MetricLabel};
use solana_sdk::commitment_config::CommitmentConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn run(
    storage: Arc<Storage>,
    common_config: PrivateChannelIndexerConfig,
    config: OperatorConfig,
    health: Option<Arc<HealthState>>,
) -> Result<(), OperatorError> {
    info!("Starting PrivateChannel Operator");
    info!("Program: {:?}", common_config.program_type);
    info!("Poll interval: {:?}", config.db_poll_interval);
    info!("Batch size: {}", config.batch_size);
    info!("Channel buffer size: {}", config.channel_buffer_size);
    info!(
        "Confirmation poll interval: {}ms",
        config.confirmation_poll_interval_ms
    );
    info!("Retry max attempts: {}", config.retry_max_attempts);

    let cancellation_token = CancellationToken::new();

    // Initialize global RPC client with retry
    let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
        common_config.rpc_url.clone(),
        RetryConfig::default(),
        CommitmentConfig {
            commitment: config.rpc_commitment,
        },
    ));

    // Initialize source RPC client if configured
    let source_rpc_client = common_config.source_rpc_url.as_ref().map(|url| {
        Arc::new(RpcClientWithRetry::with_retry_config(
            url.clone(),
            RetryConfig::default(),
            CommitmentConfig {
                commitment: config.rpc_commitment,
            },
        ))
    });

    let (processor_tx, processor_rx) = mpsc::channel(config.channel_buffer_size);
    let (sender_tx, sender_rx) = mpsc::channel(config.channel_buffer_size);
    let (storage_tx, storage_rx) = mpsc::channel::<sender::TransactionStatusUpdate>(100);

    // Start fetcher task
    let fetcher_storage = storage.clone();
    let fetcher_config = config.clone();
    let fetcher_token = cancellation_token.clone();
    let fetcher_health = health.clone();
    let fetcher_handle = tokio::spawn(async move {
        if let Err(e) = fetcher::run_fetcher(
            fetcher_storage,
            processor_tx,
            fetcher_config,
            common_config.program_type,
            fetcher_token,
            fetcher_health,
        )
        .await
        {
            tracing::error!("Fetcher error: {}", e);
        }
    });

    // Start processor task
    //
    // storage_tx is cloned into the processor so per-transaction quarantine
    // updates (ManualReview) flow through the same DbTransactionWriter path
    // the sender uses for status updates.
    let program_type = common_config.program_type;
    let instance_pda = common_config.escrow_instance_id;
    let processor_storage = storage.clone();
    let processor_rpc = rpc_client.clone();
    let processor_source_rpc = source_rpc_client.clone();
    let processor_storage_tx = storage_tx.clone();
    let processor_handle = tokio::spawn(async move {
        processor::run_processor(
            processor_rx,
            sender_tx,
            processor_storage_tx,
            program_type,
            instance_pda,
            processor_storage,
            processor_rpc,
            processor_source_rpc,
        )
        .await;
    });

    // Start storage writer task (receives updates from sender + processor)
    let writer_storage = storage.clone();
    let storage_writer = DbTransactionWriter::new(
        writer_storage,
        storage_rx,
        config.alert_webhook_url.clone(),
        common_config.program_type,
    );
    let storage_writer_handle = tokio::spawn(async move {
        if let Err(e) = storage_writer.start().await {
            tracing::error!("Storage writer error: {}", e);
        }
    });

    // Start sender task
    let sender_token = cancellation_token.clone();
    let sender_storage = storage.clone();
    let sender_commitment = config.rpc_commitment;
    let sender_source_rpc = source_rpc_client.clone();
    let sender_common_config = common_config.clone();
    let recovery_storage_tx = storage_tx.clone();
    let sender_handle = tokio::spawn(async move {
        if let Err(e) = sender::run_sender(
            &sender_common_config,
            sender_commitment,
            sender_rx,
            storage_tx,
            sender_token,
            sender_storage,
            config.retry_max_attempts,
            config.confirmation_poll_interval_ms,
            sender_source_rpc,
        )
        .await
        {
            tracing::error!("Sender error: {}", e);
        }
    });

    // Start reconciliation task for escrow operators only.
    // Withdraw operators don't maintain escrow ATA balances, so reconciliation is skipped.
    let reconciliation_handle = if common_config.program_type == crate::config::ProgramType::Escrow
    {
        if let Some(reconciliation_escrow) = common_config.escrow_instance_id {
            let reconciliation_storage = storage.clone();
            let reconciliation_config = config.clone();
            let reconciliation_rpc = source_rpc_client
                .clone()
                .unwrap_or_else(|| rpc_client.clone());
            let reconciliation_token = cancellation_token.clone();
            tokio::spawn(async move {
                if let Err(e) = reconciliation::run_reconciliation(
                    reconciliation_storage,
                    reconciliation_config,
                    reconciliation_rpc,
                    reconciliation_escrow,
                    reconciliation_token,
                )
                .await
                {
                    tracing::error!("Reconciliation error: {}", e);
                }
            })
        } else {
            warn!("Skipping reconciliation: escrow_instance_id is not configured");
            tokio::spawn(async {})
        }
    } else {
        tokio::spawn(async {})
    };

    // Recovery worker: resolves rows stuck in Processing after a crash.
    let recovery_handle = {
        let recovery_storage = storage.clone();
        let recovery_rpc = rpc_client.clone();
        let recovery_program_type = common_config.program_type;
        let recovery_token = cancellation_token.clone();
        let admin_pubkey = SignerUtil::get_admin_pubkey();
        tokio::spawn(async move {
            if let Err(e) = recovery::run_recovery_worker(
                recovery_storage,
                recovery_rpc,
                admin_pubkey,
                recovery_program_type,
                recovery_storage_tx,
                recovery_token,
            )
            .await
            {
                tracing::error!("Recovery worker error: {}", e);
            }
        })
    };

    // Start feepayer balance monitor for escrow operators only.
    // Monitors SOL balance of the feepayer wallet used for ReleaseFunds transactions.
    let feepayer_monitor_handle =
        if common_config.program_type == crate::config::ProgramType::Escrow {
            let feepayer_config = config.clone();
            let feepayer_rpc = source_rpc_client
                .clone()
                .unwrap_or_else(|| rpc_client.clone());
            let feepayer_program_type = common_config.program_type;
            let feepayer_token = cancellation_token.clone();
            tokio::spawn(async move {
                if let Err(e) = feepayer_monitor::run_feepayer_monitor(
                    feepayer_config,
                    feepayer_rpc,
                    feepayer_program_type,
                    feepayer_token,
                )
                .await
                {
                    tracing::error!("Feepayer monitor error: {}", e);
                }
            })
        } else {
            tokio::spawn(async {})
        };

    info!("Operator started, waiting for shutdown signal or task exit...");

    // Task supervision.
    //
    // We race ctrl-c against each critical task's JoinHandle — whichever
    // fires first wins — and fall through to the shutdown path either way.
    // A task exit increments the OPERATOR_TASK_EXIT metric with a task
    // label so dashboards can tell which one failed without tailing logs.
    //
    // The recovery worker is critical: if it dies, stuck-Processing rows stop
    // being recovered, so an unexpected exit must page and restart like the
    // pipeline stages. Non-critical tasks (reconciliation, feepayer monitor)
    // are not watched here.
    //
    // Handles are polled by mutable reference so ownership stays here and
    // they can still be moved into `shutdown_operator` below — awaiting an
    // already-completed JoinHandle is a no-op.
    let mut fetcher_handle = fetcher_handle;
    let mut processor_handle = processor_handle;
    let mut sender_handle = sender_handle;
    let mut storage_writer_handle = storage_writer_handle;
    let mut recovery_handle = recovery_handle;
    let pt_label = program_type.as_label();

    // `biased;` makes ctrl-c win on concurrent readiness — avoids a
    // false-positive `critical_exit` when a task ends at the same instant.
    tokio::select! {
        biased;
        result = tokio::signal::ctrl_c() => {
            result.map_err(|_| OperatorError::ShutdownChannelSend)?;
            info!("Shutdown signal received, initiating graceful shutdown...");
        }
        _ = &mut fetcher_handle => {
            critical_exit(pt_label, "fetcher");
        }
        _ = &mut processor_handle => {
            critical_exit(pt_label, "processor");
        }
        _ = &mut sender_handle => {
            critical_exit(pt_label, "sender");
        }
        _ = &mut storage_writer_handle => {
            critical_exit(pt_label, "storage_writer");
        }
        _ = &mut recovery_handle => {
            critical_exit(pt_label, "recovery");
        }
    }

    // Graceful shutdown — runs on both the ctrl-c path and the critical-task-
    // exit path.  On the exit path, the handle that tripped the select is
    // already completed; shutdown_operator will wait on the others.
    shutdown_operator(
        cancellation_token,
        storage,
        fetcher_handle,
        processor_handle,
        sender_handle,
        storage_writer_handle,
        reconciliation_handle,
        feepayer_monitor_handle,
        recovery_handle,
        config.batch_size,
        config.db_poll_interval,
    )
    .await
    .map_err(|_| OperatorError::ShutdownChannelSend)?;

    info!("Operator shutdown complete");
    Ok(())
}

/// Log + metric for a critical task that exited before cancellation.
///
/// We don't abort the process here — the caller falls through to
/// `shutdown_operator` so the remaining tasks get the usual graceful-shutdown
/// treatment.  The process will exit naturally once `shutdown_operator`
/// returns, and the supervisor will restart the operator.
fn critical_exit(program_type_label: &str, task_name: &str) {
    error!(
        task = task_name,
        "Critical operator task exited unexpectedly — triggering shutdown",
    );
    metrics::OPERATOR_TASK_EXIT
        .with_label_values(&[program_type_label, task_name])
        .inc();
}
