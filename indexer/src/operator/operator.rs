use crate::config::OperatorConfig;
use crate::error::OperatorError;
use crate::metrics;
use crate::operator::{
    feepayer_monitor, fetcher, processor, reconciliation, recovery, sender, DbTransactionWriter,
    RetryConfig, RpcClientWithRetry,
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

    // Optional destination fallback for recovery and the boot pre-flight to
    // re-check a Dead verdict. Empty means unset (env renders "") and maps to None.
    let normalized_fallback_url = common_config
        .fallback_rpc_url
        .as_deref()
        .filter(|s| !s.is_empty());
    let fallback_rpc_client = normalized_fallback_url.map(|url| {
        Arc::new(RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig::default(),
            CommitmentConfig {
                commitment: config.rpc_commitment,
            },
        ))
    });

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

    // A lone prunable Solana RPC's absent status is not proof of non-inclusion, so require
    // an independent, same-cluster, reachable fallback before starting.
    validate_withdraw_fallback(
        common_config.program_type,
        &rpc_client,
        fallback_rpc_client.as_deref(),
        &common_config.rpc_url,
        normalized_fallback_url,
    )
    .await?;

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
    // diff the on-chain bitmap against the database BEFORE any row is fetched,
    // locked, or processed.
    //
    // Only a database that claims a release the chain never made is a
    // refuse-to-start. The opposite direction, a release the chain made that the
    // database never recorded, is repaired in place and startup continues.
    if program_type == crate::config::ProgramType::Withdraw {
        if let Some(preflight_instance) = instance_pda {
            // The main rpc_client is the chain where the instance and releases live.
            let preflight = run_withdraw_preflight(
                &storage,
                &rpc_client,
                fallback_rpc_client.as_deref(),
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
    let processor_fallback_rpc = fallback_rpc_client.clone();
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
            processor_fallback_rpc,
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
            sender::SENDER_LOCK_HEARTBEAT_INTERVAL,
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
        // Both are guaranteed present for a validated escrow config: source_rpc_url
        // is enforced above and escrow_instance_id by config validation. Fail loud
        // rather than silently skip if that ever regresses.
        match (common_config.escrow_instance_id, source_rpc_client.clone()) {
            (Some(reconciliation_escrow), Some(reconciliation_rpc)) => {
                let reconciliation_storage = storage.clone();
                let reconciliation_config = config.clone();
                // Custody (Solana escrow ATAs) is read from source_rpc_client;
                // channel-token supply lives on rpc_url (the PrivateChannel chain
                // the escrow operator mints to). Custody must never be read from
                // rpc_client: the escrow ATAs do not exist on the channel, so it
                // would read 0 and trip a false halt.
                let reconciliation_channel_rpc = rpc_client.clone();
                let reconciliation_health = health.clone();
                let reconciliation_token = cancellation_token.clone();
                tokio::spawn(async move {
                    if let Err(e) = reconciliation::run_reconciliation(
                        reconciliation_storage,
                        reconciliation_config,
                        reconciliation_rpc,
                        reconciliation_channel_rpc,
                        reconciliation_escrow,
                        reconciliation_health,
                        reconciliation_token,
                    )
                    .await
                    {
                        tracing::error!("Reconciliation error: {}", e);
                    }
                })
            }
            _ => {
                return Err(OperatorError::RpcError(
                    "escrow reconciliation requires both escrow_instance_id and \
                     source_rpc_url; one is missing after startup validation"
                        .to_string(),
                ));
            }
        }
    } else {
        tokio::spawn(async {})
    };

    // Recovery worker: resolves rows stuck in Processing after a crash.
    let recovery_handle = {
        let recovery_storage = storage.clone();
        let recovery_rpc = rpc_client.clone();
        let recovery_fallback = fallback_rpc_client.clone();
        let recovery_program_type = common_config.program_type;
        let recovery_instance = instance_pda;
        let recovery_token = cancellation_token.clone();
        tokio::spawn(async move {
            if let Err(e) = recovery::run_recovery_worker(
                recovery_storage,
                recovery_rpc,
                recovery_fallback,
                recovery_program_type,
                recovery_instance,
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

/// Reconcile in-flight releases, then diff the on-chain bitmap against the
/// database. Only a genuine `BitmapDivergence` returns `Err` (refuse to start).
async fn run_withdraw_preflight(
    storage: &Arc<Storage>,
    rpc_client: &Arc<RpcClientWithRetry>,
    fallback_rpc_client: Option<&RpcClientWithRetry>,
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
        fallback_rpc_client,
        crate::config::ProgramType::Withdraw,
        Some(instance_pda),
        storage_tx,
        cancellation_token,
        MAX_RECONCILE_PASSES,
    )
    .await
    {
        warn!(
            "Boot reconcile failed, proceeding to bitmap validation: {}",
            e
        );
    }

    // Boot is the only safe window for the pending_remint pass. Once run_sender
    // starts it rehydrates every such row into its in-memory queue and may put a
    // remint in flight; completing one from underneath it would pay the
    // withdrawal and remint the burn. This returns before the sender is spawned.
    // Best-effort for the same reason as the reconcile above: validation is the
    // gate, and a transient error here must not crash-loop the operator.
    // Time-bounded because startup waits on it: an unbounded pass over a large
    // backlog on a degraded RPC would hold withdrawals down indefinitely.
    if let Err(e) = recovery::reconcile_landed_withdrawals(
        storage,
        &recovery::RecoveryFinality::new(rpc_client, fallback_rpc_client),
        crate::storage::common::models::TransactionStatus::PendingRemint,
        recovery::BOOT_RECONCILE_BUDGET,
        // Boot runs once, so the cursor has nowhere to resume to. If the budget
        // is spent the bitmap check below decides whether the operator may start.
        &mut 0,
        cancellation_token,
    )
    .await
    {
        warn!(
            "Pending-remint reconcile failed, proceeding to bitmap validation: {}",
            e
        );
    }

    // Only a genuine divergence is a refuse-to-start. Any other error (instance or
    // bitmap not yet on-chain, RPC failure, DB read failure) means we could not run
    // the check at all; start anyway and let the recovery worker re-validate, which
    // never marks a row Failed. Refusing on those would crash-loop the operator on
    // any transient boot condition.
    match sender::validate_bitmap_consistency(
        storage,
        rpc_client,
        fallback_rpc_client,
        Some(instance_pda),
        storage_tx,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(e)
            if matches!(
                e,
                OperatorError::Program(crate::error::ProgramError::BitmapDivergence { .. })
            ) =>
        {
            Err(e)
        }
        Err(e) => {
            warn!(
                "Could not validate the withdrawal bitmap at boot, starting anyway: {}",
                e
            );
            Ok(())
        }
    }
}

/// Withdraw-only gate for the Solana fallback. A missing fallback only warns: the on-chain
/// bitmap is the release-side authority, so a second endpoint is defense-in-depth. A
/// configured one must be independent, same-cluster and reachable, else refuse to start.
/// Archival depth is left to the per-attempt ledger-floor check.
async fn validate_withdraw_fallback(
    program_type: crate::config::ProgramType,
    rpc_client: &RpcClientWithRetry,
    fallback: Option<&RpcClientWithRetry>,
    rpc_url: &str,
    fallback_url: Option<&str>,
) -> Result<(), OperatorError> {
    if program_type != crate::config::ProgramType::Withdraw {
        return Ok(());
    }

    let (Some(fallback), Some(fallback_url)) = (fallback, fallback_url) else {
        warn!(
            "withdraw operator started without a fallback_rpc_url: release-side Dead is gated by \
             the on-chain withdrawal bitmap; a second endpoint is recommended defense-in-depth"
        );
        return Ok(());
    };

    if fallback_url == rpc_url {
        return Err(OperatorError::RpcError(
            "fallback_rpc_url must differ from rpc_url: an independent endpoint, not the same node"
                .to_string(),
        ));
    }

    // getGenesisHash doubles as a reachability probe for each endpoint.
    let primary_genesis = rpc_client
        .get_genesis_hash()
        .await
        .map_err(|e| OperatorError::RpcError(format!("rpc_url unreachable at startup: {e}")))?;
    let fallback_genesis = fallback.get_genesis_hash().await.map_err(|e| {
        OperatorError::RpcError(format!("fallback_rpc_url unreachable at startup: {e}"))
    })?;

    if primary_genesis != fallback_genesis {
        return Err(OperatorError::RpcError(
            "fallback_rpc_url is on a different cluster than rpc_url (genesis hash mismatch)"
                .to_string(),
        ));
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::utils::account_util::bitmap_account_bytes;
    use crate::operator::utils::rpc_util::RetryConfig;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
    use crate::storage::common::storage::mock::MockStorage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use solana_sdk::hash::Hash;
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

    fn mock_bitmap_account(
        server: &mut mockito::ServerGuard,
        generation: u64,
        consumed: &[u64],
    ) -> mockito::Mock {
        let bytes = bitmap_account_bytes(generation, consumed, 255);
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

    // getAccountInfo with a null value: the bitmap does not exist on-chain yet.
    fn mock_bitmap_not_found(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":null}}"#)
            .create()
    }

    async fn run_preflight_with(
        storage: Arc<Storage>,
        client: RpcClientWithRetry,
    ) -> Result<(), OperatorError> {
        run_preflight_capturing(storage, client).await.0
    }

    /// Same pre-flight, but hands back the status updates it emitted. The
    /// chain-ahead repair reports through the channel, not the storage mock,
    /// so an escalation is only visible here.
    async fn run_preflight_capturing(
        storage: Arc<Storage>,
        client: RpcClientWithRetry,
    ) -> (
        Result<(), OperatorError>,
        Vec<sender::TransactionStatusUpdate>,
    ) {
        let client = Arc::new(client);
        let (storage_tx, mut rx) = mpsc::channel::<sender::TransactionStatusUpdate>(8);
        let token = CancellationToken::new();
        let result = run_withdraw_preflight(
            &storage,
            &client,
            None,
            Pubkey::new_unique(),
            &storage_tx,
            &token,
        )
        .await;
        drop(storage_tx);

        let mut updates = Vec::new();
        while let Ok(update) = rx.try_recv() {
            updates.push(update);
        }
        (result, updates)
    }

    async fn run_preflight(client: RpcClientWithRetry) -> Result<(), OperatorError> {
        run_preflight_with(Arc::new(Storage::Mock(MockStorage::new())), client).await
    }

    /// An empty database against an empty bitmap agrees, so the operator starts.
    #[tokio::test]
    async fn preflight_starts_when_bitmap_agrees_with_db() {
        let mut server = mockito::Server::new_async().await;
        let _account = mock_bitmap_account(&mut server, 0, &[]);
        let result = run_preflight(make_rpc_client(&server.url())).await;
        assert!(result.is_ok(), "agreeing state must start: {result:?}");
    }

    /// Regression guard: a bitmap not yet on-chain surfaces as AccountNotFound,
    /// which must NOT refuse to start. Refusing here would crash-loop the
    /// operator on a fresh deployment.
    #[tokio::test]
    async fn preflight_starts_when_bitmap_not_found() {
        let mut server = mockito::Server::new_async().await;
        let _account = mock_bitmap_not_found(&mut server);
        let result = run_preflight(make_rpc_client(&server.url())).await;
        assert!(
            result.is_ok(),
            "AccountNotFound must start anyway, not refuse: {result:?}"
        );
    }

    /// Chain ahead of the database: the release landed and only the bookkeeping
    /// is missing, so the operator repairs what it can and starts.
    #[tokio::test]
    async fn preflight_starts_when_chain_is_ahead() {
        let mut server = mockito::Server::new_async().await;
        let _account = mock_bitmap_account(&mut server, 0, &[7]);
        let result = run_preflight(make_rpc_client(&server.url())).await;
        assert!(
            result.is_ok(),
            "a landed-but-unrecorded release must not halt boot: {result:?}"
        );
    }

    /// The one refuse-to-start: the database claims a release the chain never
    /// made, so every later decision would rest on a false history.
    #[tokio::test]
    async fn preflight_refuses_to_start_when_db_is_ahead() {
        let mut server = mockito::Server::new_async().await;
        let _account = mock_bitmap_account(&mut server, 0, &[]);

        let mock = MockStorage::new();
        let now = chrono::Utc::now();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id: 1,
                signature: "sig".to_string(),
                trace_id: "trace".to_string(),
                slot: 1,
                initiator: Pubkey::new_unique().to_string(),
                recipient: Pubkey::new_unique().to_string(),
                mint: Pubkey::new_unique().to_string(),
                amount: TokenAmount(1_000),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(7),
                status: TransactionStatus::Completed,
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

        let result = run_preflight_with(
            Arc::new(Storage::Mock(mock)),
            make_rpc_client(&server.url()),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(OperatorError::Program(
                    crate::error::ProgramError::BitmapDivergence { .. }
                ))
            ),
            "a real divergence must refuse to start: {result:?}"
        );
    }

    /// A withdraw operator whose only unrecorded nonce sits in a `PendingRemint`
    /// row carrying its release signature. Paired with a bitmap that has the
    /// matching bit set, this is the shape that used to wedge the boot gate.
    fn preflight_fixture(nonce: i64, signature: &str) -> MockStorage {
        let now = chrono::Utc::now();
        let row = DbTransaction {
            id: 1,
            signature: "burn-sig".to_string(),
            trace_id: "trace-1".to_string(),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
            recipient: Pubkey::new_unique().to_string(),
            mint: Pubkey::new_unique().to_string(),
            amount: TokenAmount(1_000),
            memo: None,
            transaction_type: TransactionType::Withdrawal,
            withdrawal_nonce: Some(nonce),
            status: TransactionStatus::PendingRemint,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: Some(vec![signature.to_string()]),
            remint_last_valid_block_heights: Some(vec![100]),
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
            release_refused_on_chain: false,
        };
        let mock = MockStorage::new();
        mock.pending_transactions.lock().unwrap().push(row);
        mock
    }

    fn mock_signature_statuses(
        server: &mut mockito::ServerGuard,
        status: usize,
        body: &str,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(status)
            .with_body(body)
            .create()
    }

    const FINALIZED_SUCCESS: &str = r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#;

    /// The regression test for the whole issue: a landed-but-unrecorded release
    /// held in `pending_remint` is reconciled by the pre-flight, so the bitmap
    /// now agrees and the operator starts instead of crash-looping.
    #[tokio::test]
    async fn preflight_completes_landed_pending_remint_and_starts() {
        let landed_sig = solana_sdk::signature::Signature::new_unique().to_string();
        let mock = preflight_fixture(7, &landed_sig);
        let mut server = mockito::Server::new_async().await;
        let _account = mock_bitmap_account(&mut server, 0, &[7]);
        let _status = mock_signature_statuses(&mut server, 200, FINALIZED_SUCCESS);

        let result = run_preflight_with(
            Arc::new(Storage::Mock(mock.clone())),
            make_rpc_client(&server.url()),
        )
        .await;

        assert!(
            result.is_ok(),
            "a reconcilable divergence must start: {result:?}"
        );
        let rows = mock.pending_transactions.lock().unwrap();
        assert_eq!(rows[0].status, TransactionStatus::Completed);
        assert_eq!(
            rows[0].counterpart_signature.as_deref(),
            Some(landed_sig.as_str())
        );
    }

    /// The unprovable case. Under the SMT this was a refuse-to-start, because
    /// any root mismatch was. The bitmap narrows the halt to db-ahead only: a
    /// consumed nonce the reconcile cannot attribute is a payout that really
    /// happened, so boot continues and the row is escalated to manual_review
    /// instead of being completed on a guess.
    #[tokio::test]
    async fn preflight_escalates_when_chain_ahead_survives_reconcile() {
        let landed_sig = solana_sdk::signature::Signature::new_unique().to_string();
        let mock = preflight_fixture(7, &landed_sig);
        let mut server = mockito::Server::new_async().await;
        let _account = mock_bitmap_account(&mut server, 0, &[7]);
        let _status = mock_signature_statuses(&mut server, 500, "internal server error");

        let (result, updates) = run_preflight_capturing(
            Arc::new(Storage::Mock(mock.clone())),
            make_rpc_client(&server.url()),
        )
        .await;

        assert!(
            result.is_ok(),
            "an unattributable payout must not halt boot: {result:?}"
        );
        assert!(
            updates
                .iter()
                .any(|u| u.status == TransactionStatus::ManualReview),
            "the row must be escalated for a human, not left silent: {updates:?}"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::PendingRemint,
            "an uncertain verdict must not complete the row"
        );
    }

    // ── withdraw fallback startup gate ───────────────────────────────

    /// Register a `getGenesisHash` reply of `hash` (base58) on `server`.
    fn mock_genesis(server: &mut mockito::ServerGuard, hash: &str) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getGenesisHash""#.into(),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(format!(r#"{{"jsonrpc":"2.0","result":"{hash}","id":0}}"#))
            .create()
    }

    /// A withdraw operator with no fallback warns and starts: the on-chain bitmap
    /// is the release-side authority, so a second endpoint is defense-in-depth.
    #[tokio::test]
    async fn withdraw_missing_fallback_warns_and_starts() {
        let primary = make_rpc_client("http://localhost:8899");
        let result = validate_withdraw_fallback(
            crate::config::ProgramType::Withdraw,
            &primary,
            None,
            "http://localhost:8899",
            None,
        )
        .await;
        assert!(
            result.is_ok(),
            "missing fallback must warn and start: {result:?}"
        );
    }

    /// A fallback whose URL equals rpc_url is not independent; refuse to start.
    #[tokio::test]
    async fn withdraw_fallback_same_url_refuses_start() {
        let primary = make_rpc_client("http://localhost:8899");
        let fallback = make_rpc_client("http://localhost:8899");
        let result = validate_withdraw_fallback(
            crate::config::ProgramType::Withdraw,
            &primary,
            Some(&fallback),
            "http://localhost:8899",
            Some("http://localhost:8899"),
        )
        .await;
        assert!(matches!(result, Err(OperatorError::RpcError(_))));
    }

    /// Two reachable endpoints on different clusters (differing genesis) refuse.
    #[tokio::test]
    async fn withdraw_fallback_different_genesis_refuses_start() {
        let mut primary_server = mockito::Server::new_async().await;
        let mut fallback_server = mockito::Server::new_async().await;
        let _p = mock_genesis(&mut primary_server, &Hash::new_unique().to_string());
        let _f = mock_genesis(&mut fallback_server, &Hash::new_unique().to_string());

        let primary = make_rpc_client(&primary_server.url());
        let fallback = make_rpc_client(&fallback_server.url());
        let result = validate_withdraw_fallback(
            crate::config::ProgramType::Withdraw,
            &primary,
            Some(&fallback),
            &primary_server.url(),
            Some(&fallback_server.url()),
        )
        .await;
        assert!(matches!(result, Err(OperatorError::RpcError(_))));
    }

    /// An unreachable fallback (genesis RPC error) refuses to start.
    #[tokio::test]
    async fn withdraw_fallback_unreachable_refuses_start() {
        let mut primary_server = mockito::Server::new_async().await;
        let mut fallback_server = mockito::Server::new_async().await;
        let _p = mock_genesis(&mut primary_server, &Hash::new_unique().to_string());
        let _f = fallback_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getGenesisHash""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"down"},"id":0}"#)
            .create();

        let primary = make_rpc_client(&primary_server.url());
        let fallback = make_rpc_client(&fallback_server.url());
        let result = validate_withdraw_fallback(
            crate::config::ProgramType::Withdraw,
            &primary,
            Some(&fallback),
            &primary_server.url(),
            Some(&fallback_server.url()),
        )
        .await;
        assert!(matches!(result, Err(OperatorError::RpcError(_))));
    }

    /// Independent, same-cluster, reachable fallback: the gate passes.
    #[tokio::test]
    async fn withdraw_valid_fallback_passes() {
        let mut primary_server = mockito::Server::new_async().await;
        let mut fallback_server = mockito::Server::new_async().await;
        let genesis = Hash::new_unique().to_string();
        let _p = mock_genesis(&mut primary_server, &genesis);
        let _f = mock_genesis(&mut fallback_server, &genesis);

        let primary = make_rpc_client(&primary_server.url());
        let fallback = make_rpc_client(&fallback_server.url());
        let result = validate_withdraw_fallback(
            crate::config::ProgramType::Withdraw,
            &primary,
            Some(&fallback),
            &primary_server.url(),
            Some(&fallback_server.url()),
        )
        .await;
        assert!(
            result.is_ok(),
            "matching genesis + distinct URLs must pass: {result:?}"
        );
    }

    /// The fallback rule is withdraw-only: an escrow operator with no fallback
    /// passes the gate untouched.
    #[tokio::test]
    async fn escrow_no_fallback_passes() {
        let primary = make_rpc_client("http://localhost:8899");
        let result = validate_withdraw_fallback(
            crate::config::ProgramType::Escrow,
            &primary,
            None,
            "http://localhost:8899",
            None,
        )
        .await;
        assert!(
            result.is_ok(),
            "escrow must not require a fallback: {result:?}"
        );
    }
}
