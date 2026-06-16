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

    // The withdraw operator's compensating remint MintTo must broadcast on the source
    // chain (PrivateChannel), where the burn happened. Without source_rpc_url the sender
    // falls back to rpc_client (the Solana ReleaseFunds destination), silently reminting
    // to the wrong chain and never restoring the burned balance. Fail closed at startup.
    if common_config.program_type == crate::config::ProgramType::Withdraw
        && common_config.source_rpc_url.is_none()
    {
        return Err(OperatorError::RpcError(
            "source_rpc_url required for Withdraw operator: remints must target the source \
             PrivateChannel, not the Solana destination"
                .to_string(),
        ));
    }

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

    let program_type = common_config.program_type;
    let instance_pda = common_config.escrow_instance_id;

    // Started first so the boot pre-flight's reconcile can drain its quarantine sends.
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

    // Boot pre-flight for withdraw operators: reconcile in-flight releases, then
    // validate the local SMT against the on-chain root BEFORE any row is fetched,
    // locked, or processed. A residual mismatch the reconcile cannot resolve is a
    // fail-closed refuse-to-start; it should never fire once the write-ahead
    // signatures and this reconcile have run, and guards an unforeseen divergence.
    if program_type == crate::config::ProgramType::Withdraw {
        if let Some(preflight_instance) = instance_pda {
            let admin_pubkey = SignerUtil::get_admin_pubkey();
            // The main rpc_client is the chain where the instance and releases live.
            let preflight = run_withdraw_preflight(
                &storage,
                &rpc_client,
                admin_pubkey,
                preflight_instance,
                &storage_tx,
                &cancellation_token,
            )
            .await;

            if let Err(e) = preflight {
                error!("Withdraw boot pre-flight failed, refusing to start: {}", e);
                // Drop the sole storage_tx so the writer's recv() returns None and
                // the task exits, then await it so the reconcile's queued
                // ManualReview alerts are flushed before we return. The writer
                // watches only its channel, not the cancellation token, so without
                // this drop the await would block forever. No storage_tx clones
                // exist yet: the processor/sender/recovery senders are created
                // after this block.
                cancellation_token.cancel();
                drop(storage_tx);
                if let Err(join_err) = storage_writer_handle.await {
                    error!(
                        "Storage writer join error during refuse-to-start: {}",
                        join_err
                    );
                }
                return Err(e);
            }
        } else {
            warn!("Withdraw operator has no escrow_instance_id; skipping boot pre-flight");
        }
    }

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

/// Reconcile in-flight releases, then validate the local SMT against the on-chain root.
/// Only a genuine `SmtRootMismatch` returns `Err` (refuse to start).
async fn run_withdraw_preflight(
    storage: &Arc<Storage>,
    rpc_client: &Arc<RpcClientWithRetry>,
    admin_pubkey: solana_sdk::pubkey::Pubkey,
    instance_pda: solana_sdk::pubkey::Pubkey,
    storage_tx: &mpsc::Sender<sender::TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
) -> Result<(), OperatorError> {
    // Idempotent passes absorb rows that flip Processing to terminal across iterations.
    const MAX_RECONCILE_PASSES: u32 = 8;

    // Best-effort: a reconcile error must not block startup. Validation is the gate,
    // and a transient DB error here would otherwise crash-loop the operator at boot.
    if let Err(e) = recovery::boot_reconcile_processing(
        storage,
        rpc_client,
        admin_pubkey,
        crate::config::ProgramType::Withdraw,
        storage_tx,
        cancellation_token,
        MAX_RECONCILE_PASSES,
    )
    .await
    {
        warn!("Boot reconcile failed, proceeding to SMT validation: {}", e);
    }

    // Only a genuine root mismatch is a refuse-to-start. Any other error (instance
    // not yet on-chain, RPC failure, DB read failure) means we could not run the
    // check; start anyway and let the sender's lazy init plus the recovery worker
    // re-validate, neither of which marks a row Failed. Refusing on those would
    // crash-loop the operator on any transient boot condition.
    match sender::validate_smt_root(storage, rpc_client, Some(instance_pda)).await {
        Ok(_) => Ok(()),
        Err(e)
            if matches!(
                e,
                OperatorError::Program(crate::error::ProgramError::SmtRootMismatch { .. })
            ) =>
        {
            Err(e)
        }
        Err(e) => {
            warn!(
                "Could not validate SMT root at boot, starting anyway (lazy init will re-check): {}",
                e
            );
            Ok(())
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::utils::rpc_util::RetryConfig;
    use crate::operator::utils::smt_util::SmtState;
    use crate::storage::common::storage::mock::MockStorage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;
    use solana_sdk::pubkey::Pubkey;
    use std::time::Duration;

    // Single attempt with negligible backoff so an AccountNotFound resolves fast.
    fn make_rpc_client(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    fn mock_instance_account(server: &mut mockito::ServerGuard, root: [u8; 32]) -> mockito::Mock {
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: root,
            current_tree_index: 0,
        };
        let mut bytes = Vec::new();
        instance.serialize(&mut bytes).unwrap();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
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

    // getAccountInfo with a null value: the instance does not exist on-chain yet.
    fn mock_instance_not_found(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":null}}"#)
            .create()
    }

    async fn run_preflight(client: RpcClientWithRetry) -> Result<(), OperatorError> {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let client = Arc::new(client);
        let (storage_tx, _rx) = mpsc::channel::<sender::TransactionStatusUpdate>(8);
        let token = CancellationToken::new();
        run_withdraw_preflight(
            &storage,
            &client,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            &storage_tx,
            &token,
        )
        .await
    }

    /// Matching local and on-chain roots: the pre-flight passes and the operator starts.
    #[tokio::test]
    async fn preflight_starts_when_root_matches() {
        let mut server = mockito::Server::new_async().await;
        // Empty DB rebuilds an empty tree, so the on-chain root must be the empty-tree root.
        let _account = mock_instance_account(&mut server, SmtState::new(0).current_root());
        let result = run_preflight(make_rpc_client(&server.url())).await;
        assert!(result.is_ok(), "matching root must start: {result:?}");
    }

    /// Regression guard (the integration failure): an instance not yet on-chain surfaces
    /// as AccountNotFound, which must NOT refuse to start (only a real mismatch does).
    /// Refusing here would crash-loop the operator at boot.
    #[tokio::test]
    async fn preflight_starts_when_instance_not_found() {
        let mut server = mockito::Server::new_async().await;
        let _account = mock_instance_not_found(&mut server);
        let result = run_preflight(make_rpc_client(&server.url())).await;
        assert!(
            result.is_ok(),
            "AccountNotFound must start anyway, not refuse: {result:?}"
        );
    }

    /// A genuine root divergence is the only refuse-to-start: the operator returns
    /// `Err(SmtRootMismatch)` so it never consumes nonces against a tree it cannot reason about.
    #[tokio::test]
    async fn preflight_refuses_to_start_on_root_mismatch() {
        let mut server = mockito::Server::new_async().await;
        // On-chain root carries a nonce the empty DB will never reconcile.
        let mut onchain = SmtState::new(0);
        onchain.insert_nonce(7);
        let _account = mock_instance_account(&mut server, onchain.current_root());
        let result = run_preflight(make_rpc_client(&server.url())).await;
        assert!(
            matches!(
                result,
                Err(OperatorError::Program(
                    crate::error::ProgramError::SmtRootMismatch { .. }
                ))
            ),
            "a real mismatch must refuse to start: {result:?}"
        );
    }
}
