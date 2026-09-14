//! Live-state lock, from the worker side.
//!
//! Target: the shared acquire at the top of `indexer::run` and `operator::run`,
//! which is what stops either from starting against a database a resync is
//! rebuilding. A resync holding the key exclusively stands in for that rebuild.
//!
//! Both workers are given RPC URLs on a dead port, so anything other than the
//! lock refusal would surface as a datasource or RPC error instead. That is the
//! assertion: the lock is checked before any network call and before the schema.

use {
    private_channel_indexer::{
        config::{
            BackfillConfig, IndexerConfig, OperatorConfig, PostgresConfig,
            PrivateChannelIndexerConfig, ProgramType, ReconciliationConfig, RpcPollingConfig,
            StorageType, DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
        },
        error::{IndexerError, OperatorError, StorageError},
        operator,
        storage::{
            common::storage::live_lock::{LiveLockGuard, LiveLockMode},
            PostgresDb, Storage,
        },
        DatasourceType,
    },
    solana_sdk::commitment_config::CommitmentLevel,
    std::{sync::Arc, time::Duration},
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
    tokio_util::sync::CancellationToken,
};

/// An RPC endpoint that refuses every connection, so any network attempt fails loudly.
const DEAD_RPC: &str = "http://127.0.0.1:1";

async fn start_postgres(db_name: &str) -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{host}:{port}/{db_name}");
    (url, container)
}

async fn connect(url: &str) -> Arc<Storage> {
    Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: url.to_string(),
            max_connections: 5,
        })
        .await
        .expect("connect"),
    ))
}

/// Hold the key exclusively, exactly as a running resync does.
async fn hold_as_resync(url: &str) -> LiveLockGuard {
    connect(url)
        .await
        .try_acquire_live_lock(
            LiveLockMode::Exclusive,
            "test_resync",
            CancellationToken::new(),
            Duration::ZERO,
        )
        .await
        .expect("resync must be able to take the lock on an idle database")
}

fn common_config(postgres: PostgresConfig) -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: DEAD_RPC.to_string(),
        source_rpc_url: Some(DEAD_RPC.to_string()),
        fallback_rpc_url: None,
        postgres,
        escrow_instance_id: Some(solana_sdk::pubkey::Pubkey::new_unique()),
    }
}

/// I9. An operator starting during a resync would mint and release against tables
/// the resync is about to drop, so it must refuse before it touches anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_refuses_to_start_during_a_resync() {
    let (url, _container) = start_postgres("live_lock_operator").await;
    let _resync = hold_as_resync(&url).await;

    let postgres = PostgresConfig {
        database_url: url.clone(),
        max_connections: 5,
    };
    let operator_config = OperatorConfig {
        db_poll_interval: Duration::from_secs(60),
        batch_size: 10,
        retry_max_attempts: 1,
        retry_base_delay: Duration::from_secs(1),
        channel_buffer_size: 10,
        rpc_commitment: CommitmentLevel::Finalized,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(300),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: Some("http://127.0.0.1:1/hook".to_string()),
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
    };

    let result = operator::run(
        connect(&url).await,
        common_config(postgres),
        operator_config,
        None,
    )
    .await;

    assert!(
        matches!(
            result,
            Err(OperatorError::Storage(StorageError::LiveStateLockHeld {
                requested: LiveLockMode::Shared
            }))
        ),
        "the operator must refuse to start under a resync, got: {result:?}"
    );
}

/// I10. Same for the indexer, which would otherwise write rows into tables the
/// resync is about to drop and advance a checkpoint over them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexer_refuses_to_start_during_a_resync() {
    let (url, _container) = start_postgres("live_lock_indexer").await;
    let _resync = hold_as_resync(&url).await;

    let postgres = PostgresConfig {
        database_url: url.clone(),
        max_connections: 5,
    };
    let indexer_config = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: Some(RpcPollingConfig {
            poll_interval_ms: 1_000,
            error_retry_interval_ms: 1_000,
            batch_size: 10,
            from_slot: None,
            encoding: solana_transaction_status::UiTransactionEncoding::Json,
            commitment: CommitmentLevel::Finalized,
        }),
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: false,
            exit_after_backfill: false,
            rpc_url: DEAD_RPC.to_string(),
            batch_size: 10,
            max_gap_slots: 1_000,
            start_slot: None,
        },
        reconciliation: ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
    };

    let result = private_channel_indexer::run(common_config(postgres), indexer_config, None).await;

    assert!(
        matches!(
            result,
            Err(IndexerError::Storage(StorageError::LiveStateLockHeld {
                requested: LiveLockMode::Shared
            }))
        ),
        "the indexer must refuse to start under a resync, got: {result:?}"
    );
}
