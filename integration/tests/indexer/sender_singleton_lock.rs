//! Sender singleton advisory lock.
//!
//! Target: `acquire_sender_lock` in `indexer/src/operator/sender/mod.rs`, which
//! `operator::run` calls before the boot pre-flight to take a per-role advisory
//! lock, refusing to start if another sender already holds it.
//! Binary: `reconciliation_integration` (attached via `#[path]` mod from
//! `tests/indexer/reconciliation.rs`).
//!
//! Two real `run_sender` futures run against one Postgres database, each with
//! its own connection pool, standing in for two operator processes. A spawned
//! sender that stays pending has acquired the lock; one that resolves to `Err`
//! was refused.

use solana_commitment_config::CommitmentLevel;
use {
    chrono::{Duration as ChronoDuration, Utc},
    private_channel_indexer::{
        config::{
            OperatorConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
        },
        error::OperatorError,
        metrics::OPERATOR_SENDER_LOCK_LOST,
        operator::{
            self, acquire_sender_lock, run_sender, sender_lock_key, utils::TransactionBuilder,
        },
        storage::{common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType},
    },
    private_channel_metrics::MetricLabel,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    sqlx::PgPool,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
    tokio::{
        sync::{mpsc, Notify},
        task::JoinHandle,
    },
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
        let sender_lock = acquire_sender_lock(
            &storage,
            program_type,
            sender_token.clone(),
            heartbeat_interval,
        )
        .await?;
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
            sender_lock,
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

/// Held by every test that moves or asserts the withdraw lock-lost counter, since the
/// registry is process-global and the graceful test asserts it exactly.
static WITHDRAW_LOCK_LOST_METRIC: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    let _metrics_guard = WITHDRAW_LOCK_LOST_METRIC.lock().await;
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

/// A withdraw operator on `url`. Pass a refusing `rpc_url` so anything past the
/// startup lock checks surfaces as an RPC failure instead.
fn withdraw_operator_configs(
    url: &str,
    rpc_url: &str,
    source_rpc_url: Option<String>,
) -> (PrivateChannelIndexerConfig, OperatorConfig) {
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.to_string(),
        source_rpc_url,
        fallback_rpc_url: None,
        postgres: PostgresConfig {
            database_url: url.to_string(),
            max_connections: 5,
        },
        escrow_instance_id: Some(solana_sdk::pubkey::Pubkey::new_unique()),
    };
    let operator = OperatorConfig {
        db_poll_interval: Duration::from_secs(60),
        batch_size: 10,
        retry_max_attempts: 1,
        retry_base_delay: Duration::from_secs(1),
        channel_buffer_size: 10,
        rpc_commitment: CommitmentLevel::Finalized,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(300),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: None,
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
    };
    (common, operator)
}

/// The boot pre-flight completes pending_remint rows the running sender may be
/// reminting, so a second operator must be refused before it gets that far.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_operator_is_refused_before_its_boot_preflight() {
    let (url, _container) = start_postgres().await;
    let holder = connect(&url).await;
    let _held = holder
        .try_acquire_sender_lock(
            sender_lock_key(ProgramType::Withdraw),
            ProgramType::Withdraw.as_label(),
            CancellationToken::new(),
            Duration::ZERO,
        )
        .await
        .expect("lock query")
        .expect("the lock must be free");

    let refusing_rpc = "http://127.0.0.1:1";
    let (common, operator_config) =
        withdraw_operator_configs(&url, refusing_rpc, Some(refusing_rpc.to_string()));
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        operator::run(connect(&url).await, common, operator_config, None),
    )
    .await
    .expect("a refused operator must return promptly");

    assert!(
        matches!(
            result,
            Err(OperatorError::SenderAlreadyRunning {
                program_type: ProgramType::Withdraw
            })
        ),
        "the second operator must be refused at startup; got {result:?}"
    );
}

/// A startup failure after the lock is taken must not strand it, or the restart
/// that follows would be refused forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_startup_releases_the_sender_lock() {
    let (url, _container) = start_postgres().await;

    // No source_rpc_url is a withdraw refuse-to-start.
    let (common, operator_config) = withdraw_operator_configs(&url, "http://127.0.0.1:1", None);
    let result = operator::run(connect(&url).await, common, operator_config, None).await;
    assert!(
        matches!(result, Err(OperatorError::RpcError(_))),
        "startup must fail on the missing source_rpc_url; got {result:?}"
    );

    assert!(
        wait_for_lock_available(&url, Duration::from_secs(15), ProgramType::Withdraw).await,
        "a failed startup must leave the lock free for the restart"
    );
}

/// A lock lost mid pre-flight means a replacement may already own the pending_remint
/// rows. The pre-flight swallows its own errors, so the operator must still refuse to
/// start rather than spawn a sender that no longer holds the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_lost_during_the_boot_preflight_refuses_to_start() {
    let _metrics_guard = WITHDRAW_LOCK_LOST_METRIC.lock().await;
    let (url, _container) = start_postgres().await;
    let db = PostgresDb::new(&PostgresConfig {
        database_url: url.clone(),
        max_connections: 2,
    })
    .await
    .unwrap();
    db.init_schema().await.unwrap();
    let pool = PgPool::connect(&url).await.unwrap();

    // A pending_remint row whose release landed, so the pre-flight tries to complete it.
    let recipient = Pubkey::new_unique().to_string();
    let mut withdrawal = DbTransactionBuilder::new(
        Signature::new_unique().to_string(),
        1,
        Pubkey::new_unique().to_string(),
        10_000u64,
    )
    .initiator(recipient.clone())
    .recipient(recipient)
    .transaction_type(TransactionType::Withdrawal)
    .build();
    withdrawal.withdrawal_nonce = Some(0);
    let transaction_id = db.insert_transaction_internal(&withdrawal).await.unwrap();
    sqlx::query("UPDATE transactions SET status = 'processing' WHERE id = $1")
        .bind(transaction_id)
        .execute(&pool)
        .await
        .unwrap();
    db.set_pending_remint_internal(
        transaction_id,
        vec![Signature::new_unique().to_string()],
        vec![0],
        Utc::now() + ChronoDuration::minutes(10),
        false,
    )
    .await
    .unwrap();

    // Hold the finality answer until the lock's backend is dead, so the completion
    // that follows it has to run on a lost session.
    let status_requested = Arc::new(Notify::new());
    let lock_killed = Arc::new(AtomicBool::new(false));
    let mut rpc = mockito::Server::new_async().await;
    let _statuses = {
        let status_requested = status_requested.clone();
        let lock_killed = lock_killed.clone();
        rpc.mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body_from_request(move |_| {
                status_requested.notify_one();
                while !lock_killed.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(10));
                }
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#.into()
            })
            .create_async()
            .await
    };
    let killer = {
        let url = url.clone();
        let lock_killed = lock_killed.clone();
        tokio::spawn(async move {
            status_requested.notified().await;
            terminate_advisory_lock_holder(&url, sender_lock_key(ProgramType::Withdraw)).await;
            lock_killed.store(true, Ordering::SeqCst);
        })
    };

    let (common, operator_config) = withdraw_operator_configs(
        &url,
        &rpc.url(),
        Some("http://127.0.0.1:1".to_string()),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        operator::run(connect(&url).await, common, operator_config, None),
    )
    .await
    .expect("an operator that lost its lock at boot must return");
    killer.await.expect("killer task panicked");

    assert!(
        matches!(
            result,
            Err(OperatorError::SenderLockLostAtBoot {
                program_type: ProgramType::Withdraw
            })
        ),
        "an operator that lost its lock during the pre-flight must refuse to start; got {result:?}"
    );
    let status: String =
        sqlx::query_scalar("SELECT status::text FROM transactions WHERE id = $1")
            .bind(transaction_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        status, "pending_remint",
        "a completion on the lost session must not apply"
    );
}
