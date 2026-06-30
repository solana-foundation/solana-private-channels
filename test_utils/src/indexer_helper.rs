use {
    private_channel_indexer::{
        storage::{PostgresDb, Storage},
        BackfillConfig, DatasourceType, IndexerConfig, PostgresConfig, PrivateChannelIndexerConfig,
        ProgramType, RpcPollingConfig, StorageType, YellowstoneConfig,
    },
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey},
    solana_transaction_status::UiTransactionEncoding,
    std::sync::Arc,
    tokio::task::JoinHandle,
};

pub struct IndexerHandle {
    _handles: Vec<JoinHandle<()>>,
}

impl IndexerHandle {
    pub fn abort(&self) {
        for handle in &self._handles {
            handle.abort();
        }
    }
}

/// Start the PrivateChannel indexer.
/// If geyser_endpoint is Some, uses Yellowstone datasource; otherwise uses RPC polling.
pub async fn start_private_channel_indexer(
    geyser_endpoint: Option<String>,
    rpc_url: String,
    database_url: String,
) -> Result<(IndexerHandle, Storage), Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url,
        max_connections: 50,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));
    storage.init_schema().await?;

    let rpc_polling_config = RpcPollingConfig {
        poll_interval_ms: 200,
        error_retry_interval_ms: 1000,
        batch_size: 10,
        from_slot: Some(1),
        encoding: UiTransactionEncoding::Json,
        commitment: CommitmentLevel::Finalized,
        fallback_rpc_url: None,
    };

    let (datasource_type, yellowstone_config) = if let Some(endpoint) = geyser_endpoint {
        (
            DatasourceType::Yellowstone,
            Some(YellowstoneConfig {
                endpoint,
                x_token: None,
                commitment: "finalized".to_string(),
            }),
        )
    } else {
        (DatasourceType::RpcPolling, None)
    };

    let backfill_config = BackfillConfig {
        enabled: true,
        batch_size: 100,
        max_gap_slots: 100,
        exit_after_backfill: false,
        rpc_url: rpc_url.clone(),
        start_slot: None,
    };

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        postgres: postgres_config,
        rpc_url,
        source_rpc_url: None,
        escrow_instance_id: None,
    };

    let indexer_config = IndexerConfig {
        datasource_type,
        rpc_polling: Some(rpc_polling_config),
        yellowstone: yellowstone_config,
        backfill: backfill_config,
        // Disable the mismatch guard: gap-recovery restarts (indexer stopped
        // while deposits arrived) must not be blocked by startup reconciliation.
        reconciliation: private_channel_indexer::ReconciliationConfig {
            mismatch_threshold_raw: u64::MAX,
        },
    };

    indexer_config.validate()?;
    common_config.validate()?;

    let indexer_handle = tokio::spawn(async move {
        if let Err(e) = private_channel_indexer::run(common_config, indexer_config, None).await {
            eprintln!("Indexer error: {}", e);
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    Ok((
        IndexerHandle {
            _handles: vec![indexer_handle],
        },
        (*storage).clone(),
    ))
}

/// Start the Solana indexer using RPC polling (no Yellowstone geyser required).
/// Suitable for environments where the test validator has no geyser plugin.
pub async fn start_solana_indexer_rpc_polling(
    rpc_url: String,
    database_url: String,
    escrow_instance_id: Option<Pubkey>,
) -> Result<(IndexerHandle, Storage), Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url,
        max_connections: 50,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));
    storage.init_schema().await?;

    let rpc_polling_config = RpcPollingConfig {
        poll_interval_ms: 200,
        error_retry_interval_ms: 1000,
        batch_size: 10,
        from_slot: Some(1),
        encoding: UiTransactionEncoding::Json,
        // `Confirmed` matches the test client. `getBlock` rejects
        // `Processed` outright (-32602 "Method does not support commitment
        // below `confirmed`"). At `Finalized`, `solana-test-validator`
        // without geyser / tower-bft leaves the deposit slots indefinitely
        // unfinalized and `getBlock` returns -32009.
        commitment: CommitmentLevel::Confirmed,
        fallback_rpc_url: None,
    };

    let backfill_config = BackfillConfig {
        enabled: true,
        batch_size: 100,
        max_gap_slots: u64::MAX,
        exit_after_backfill: false,
        rpc_url: rpc_url.clone(),
        start_slot: None,
    };

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        postgres: postgres_config,
        rpc_url,
        source_rpc_url: None,
        escrow_instance_id,
    };

    let indexer_config = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: Some(rpc_polling_config),
        yellowstone: None,
        backfill: backfill_config,
        // Disable the mismatch guard: gap-recovery restarts (indexer stopped
        // while deposits arrived) must not be blocked by startup reconciliation.
        reconciliation: private_channel_indexer::ReconciliationConfig {
            mismatch_threshold_raw: u64::MAX,
        },
    };

    common_config.validate()?;
    indexer_config.validate()?;

    let indexer_handle = tokio::spawn(async move {
        if let Err(e) = private_channel_indexer::run(common_config, indexer_config, None).await {
            eprintln!("Solana RPC-polling Indexer error: {}", e);
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    Ok((
        IndexerHandle {
            _handles: vec![indexer_handle],
        },
        (*storage).clone(),
    ))
}

/// Start the Solana indexer using Yellowstone geyser.
pub async fn start_solana_indexer(
    geyser_endpoint: String,
    rpc_url: String,
    database_url: String,
    escrow_instance_id: Option<Pubkey>,
) -> Result<(IndexerHandle, Storage), Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url,
        max_connections: 50,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));
    storage.init_schema().await?;

    let yellowstone_config = YellowstoneConfig {
        endpoint: geyser_endpoint,
        x_token: None,
        commitment: "finalized".to_string(),
    };

    let rpc_polling_config = RpcPollingConfig {
        poll_interval_ms: 200,
        error_retry_interval_ms: 1000,
        batch_size: 10,
        from_slot: Some(1),
        encoding: UiTransactionEncoding::Json,
        commitment: CommitmentLevel::Finalized,
        fallback_rpc_url: None,
    };

    let backfill_config = BackfillConfig {
        enabled: true,
        batch_size: 100,
        max_gap_slots: 100,
        exit_after_backfill: false,
        rpc_url: rpc_url.clone(),
        start_slot: None,
    };

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        postgres: postgres_config,
        rpc_url,
        source_rpc_url: None,
        escrow_instance_id,
    };

    let indexer_config = IndexerConfig {
        datasource_type: DatasourceType::Yellowstone,
        rpc_polling: Some(rpc_polling_config),
        yellowstone: Some(yellowstone_config),
        backfill: backfill_config,
        // Disable the mismatch guard: gap-recovery restarts (indexer stopped
        // while deposits arrived) must not be blocked by startup reconciliation.
        reconciliation: private_channel_indexer::ReconciliationConfig {
            mismatch_threshold_raw: u64::MAX,
        },
    };

    common_config.validate()?;
    indexer_config.validate()?;

    let indexer_handle = tokio::spawn(async move {
        if let Err(e) = private_channel_indexer::run(common_config, indexer_config, None).await {
            eprintln!("Solana Indexer error: {}", e);
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    Ok((
        IndexerHandle {
            _handles: vec![indexer_handle],
        },
        (*storage).clone(),
    ))
}
