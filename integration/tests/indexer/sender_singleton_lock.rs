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
        metrics::OPERATOR_SENDER_LOCK_LOST,
        operator::{run_sender, sender_lock_key, utils::TransactionBuilder},
        storage::{PostgresDb, Storage},
    },
    private_channel_metrics::MetricLabel,
    solana_sdk::commitment_config::CommitmentLevel,
    std::{sync::Arc, time::Duration},
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
    tokio::{sync::mpsc, task::JoinHandle},
    tokio_util::sync::CancellationToken,
};

fn role_config(program_type: ProgramType) -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type,
        storage_type: StorageType::Postgres,
        // No RPC traffic needed: the holder idles in its loop and the refused
        // sender never gets past the lock check.
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        fallback_rpc_url: None,
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
/// releases the lock. The returned token is the one the sender was given, so a
/// test can both observe a heartbeat-driven cancel and drive a graceful one.
fn spawn_sender(
    storage: Arc<Storage>,
    heartbeat_interval: Duration,
    program_type: ProgramType,
) -> (
    JoinHandle<Result<(), OperatorError>>,
    mpsc::Sender<TransactionBuilder>,
    CancellationToken,
) {
    let (processor_tx, processor_rx) = mpsc::channel(10);
    let token = CancellationToken::new();
    let sender_token = token.clone();
    let handle = tokio::spawn(async move {
        let (storage_tx, _storage_rx) = mpsc::channel(10);
        run_sender(
            &role_config(program_type),
            CommitmentLevel::Confirmed,
            processor_rx,
            storage_tx,
            sender_token,
            storage,
            3,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
            None,
            heartbeat_interval,
        )
        .await
    });
    (handle, processor_tx, token)
}

/// Sum every lock-loss reason for one role. The counter is process-global, so a
/// test that asserts an exact value must own its role label: a sibling test
/// spawning the same role in the same binary would otherwise land an increment
/// between the before-read and the assertion and fail a correct change.
fn lock_lost_total(program_type: ProgramType) -> f64 {
    ["not_held", "probe_error", "probe_timeout", "fenced_write"]
        .iter()
        .map(|reason| {
            OPERATOR_SENDER_LOCK_LOST
                .with_label_values(&[program_type.as_label(), reason])
                .get()
        })
        .sum()
}

/// Kill whichever backend holds `key`, standing in for a failover or an idle-session reap.
async fn terminate_advisory_lock_holder(url: &str, key: i64) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(url)
        .await
        .expect("admin connect");
    let pid: i32 = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
         AND granted AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(key)
    .fetch_one(&mut conn)
    .await
    .expect("exactly one backend must hold the sender key");
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut conn)
        .await
        .expect("terminate");
}

/// Poll until a fresh pool can take the lock. `Drop` only signals release, it cannot wait.
async fn wait_for_lock_available(url: &str, within: Duration, program_type: ProgramType) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        let storage = connect(url).await;
        let acquired = storage
            .try_acquire_sender_lock(
                sender_lock_key(program_type),
                program_type.as_label(),
                CancellationToken::new(),
                Duration::ZERO,
            )
            .await
            .expect("lock query");
        if acquired.is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
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
    let (first, first_tx, _first_token) =
        spawn_sender(connect(&url).await, HEARTBEAT, ProgramType::Escrow);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !first.is_finished(),
        "first sender should be running and holding the lock"
    );

    // Second sender against the same database is refused.
    let (second, _second_tx, _second_token) =
        spawn_sender(connect(&url).await, HEARTBEAT, ProgramType::Escrow);
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
    let (third, third_tx, _third_token) =
        spawn_sender(connect(&url).await, HEARTBEAT, ProgramType::Escrow);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !third.is_finished(),
        "new sender should acquire the lock after the first exits"
    );

    drop(third_tx);
    let _ = third.await;
}

/// Short detection interval so the assertions do not wait out the 5s production default.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// I2. The finding, end to end: a terminated backend must be noticed, cancel, and free the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminated_backend_kills_the_sender_and_frees_the_lock() {
    let (url, _container) = start_postgres().await;
    connect(&url).await.init_schema().await.unwrap();

    let before = lock_lost_total(ProgramType::Escrow);
    let (sender, sender_tx, token) =
        spawn_sender(connect(&url).await, HEARTBEAT, ProgramType::Escrow);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!sender.is_finished(), "the sender must hold the lock");

    // Terminate the backend holding the escrow key, from a separate session.
    terminate_advisory_lock_holder(&url, sender_lock_key(ProgramType::Escrow)).await;

    // Detection is bounded by one interval plus the probe timeout.
    assert!(
        tokio::time::timeout(Duration::from_secs(15), token.cancelled())
            .await
            .is_ok(),
        "losing the lock must cancel the shared operator token"
    );
    assert!(
        lock_lost_total(ProgramType::Escrow) >= before + 1.0,
        "losing the lock must be counted"
    );

    // The real operator closes this channel on cancel, and the drain needs that to finish.
    drop(sender_tx);
    let exited = tokio::time::timeout(Duration::from_secs(30), sender)
        .await
        .expect("the cancel must propagate through the drain without hanging")
        .expect("sender task panicked");
    assert!(
        exited.is_ok(),
        "the sender should exit cleanly; got {exited:?}"
    );

    // The terminated backend really released the lock, so a replacement starts.
    assert!(
        wait_for_lock_available(&url, Duration::from_secs(15), ProgramType::Escrow).await,
        "a terminated backend must leave the lock free for a replacement"
    );
}

/// I3. Every deploy takes this path, so it must unlock explicitly and not look like a loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_cancellation_releases_the_lock_without_a_lost_signal() {
    let (url, _container) = start_postgres().await;
    connect(&url).await.init_schema().await.unwrap();

    // The withdraw role, so the exact-value counter assertion below owns its
    // series and no sibling test can perturb it. Behaviourally identical here:
    // `run_sender` derives `instance_pda` from `escrow_instance_id`, which this
    // config leaves unset, so both roles start with no instance either way.
    let role = ProgramType::Withdraw;
    let before = lock_lost_total(role);
    let (sender, sender_tx, token) = spawn_sender(connect(&url).await, HEARTBEAT, role);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!sender.is_finished());

    token.cancel();
    drop(sender_tx);
    let exited = tokio::time::timeout(Duration::from_secs(30), sender)
        .await
        .expect("a cancelled sender must exit")
        .expect("sender task panicked");
    assert!(exited.is_ok(), "graceful exit should be Ok; got {exited:?}");
    assert_eq!(
        lock_lost_total(role),
        before,
        "a graceful shutdown must not emit any lock-lost signal"
    );

    // A different pool taking the lock proves the unlock ran, not that the pool closed.
    assert!(
        wait_for_lock_available(&url, Duration::from_secs(15), role).await,
        "the lock must be released explicitly on graceful shutdown"
    );
}
