//! Sender singleton advisory lock.
//!
//! Target: `run_sender` in `indexer/src/operator/sender/mod.rs`, which acquires
//! a per-role advisory lock before recovery and refuses to start if another
//! sender already holds it.
//! Binary: `reconciliation_integration` (attached via `#[path]` mod from
//! `tests/indexer/reconciliation.rs`).
//!
//! Two real `run_sender` futures run against one Postgres database, each with
//! its own connection pool, standing in for two operator processes. A spawned
//! sender that stays pending has acquired the lock; one that resolves to `Err`
//! was refused.

use {
    private_channel_indexer::{
        config::{
            PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
        },
        error::OperatorError,
        operator::{run_sender, utils::TransactionBuilder},
        storage::{PostgresDb, Storage},
    },
    solana_sdk::commitment_config::CommitmentLevel,
    std::{sync::Arc, time::Duration},
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
    tokio::{sync::mpsc, task::JoinHandle},
    tokio_util::sync::CancellationToken,
};

fn escrow_config() -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        // No RPC traffic needed: the holder idles in its loop and the refused
        // sender never gets past the lock check.
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        // Unused by run_sender; storage is passed in directly.
        postgres: PostgresConfig {
            database_url: "mock://unused".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: None,
    }
}

async fn start_postgres() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("sender_lock")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{host}:{port}/sender_lock");
    (url, container)
}

async fn connect(url: &str) -> Arc<Storage> {
    let db = PostgresDb::new(&PostgresConfig {
        database_url: url.to_string(),
        max_connections: 5,
    })
    .await
    .unwrap();
    Arc::new(Storage::Postgres(db))
}

/// Spawn a `run_sender`. The returned handle stays pending while the sender
/// holds the lock and resolves to `Err` if it was refused. Drop the returned
/// processor sender to shut it down via the channel-close path. The storage
/// `Arc` lives only inside the task, so joining the handle drops its pool and
/// releases the lock.
fn spawn_sender(
    storage: Arc<Storage>,
) -> (
    JoinHandle<Result<(), OperatorError>>,
    mpsc::Sender<TransactionBuilder>,
) {
    let (processor_tx, processor_rx) = mpsc::channel(10);
    let handle = tokio::spawn(async move {
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        run_sender(
            &escrow_config(),
            CommitmentLevel::Confirmed,
            processor_rx,
            storage_tx,
            CancellationToken::new(),
            storage,
            3,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
            None,
        )
        .await
    });
    (handle, processor_tx)
}

/// A second sender is refused while the first holds the lock, and a new sender
/// can take the lock once the first exits (the rolling-restart handoff).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_sender_is_refused_until_the_first_exits() {
    let (url, _container) = start_postgres().await;

    // Schema must exist so the holder's startup recovery succeeds and it reaches
    // the loop still holding the lock, rather than erroring out and releasing it.
    connect(&url).await.init_schema().await.unwrap();

    // First sender acquires the lock and idles.
    let (first, first_tx) = spawn_sender(connect(&url).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !first.is_finished(),
        "first sender should be running and holding the lock"
    );

    // Second sender against the same database is refused.
    let (second, _second_tx) = spawn_sender(connect(&url).await);
    let second_result = second.await.expect("second task panicked");
    assert!(
        matches!(
            second_result,
            Err(OperatorError::SenderAlreadyRunning {
                program_type: ProgramType::Escrow
            })
        ),
        "second sender must be refused with SenderAlreadyRunning; got {second_result:?}"
    );

    // First sender exits; its pool closes and the lock releases.
    drop(first_tx);
    first
        .await
        .expect("first task panicked")
        .expect("first sender should exit cleanly");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // A new sender can now acquire the lock and run.
    let (third, third_tx) = spawn_sender(connect(&url).await);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !third.is_finished(),
        "new sender should acquire the lock after the first exits"
    );

    drop(third_tx);
    let _ = third.await;
}
