//! Live-state lock, from the worker side.
//!
//! Target: the shared acquire at the top of `indexer::run` and `operator::run`,
//! which is what stops either from starting against a database a resync is
//! rebuilding. A resync holding the key exclusively stands in for that rebuild.
//!
//! Both workers are given RPC URLs on a dead port, so anything other than the
//! lock refusal would surface as a datasource or RPC error instead. That is the
//! assertion: the lock is checked before any network call and before the schema.

use crate::sender_singleton_lock::{terminate_advisory_lock_holder, withdraw_operator_configs};
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
            common::storage::live_lock::{LiveLockGuard, LiveLockMode, LIVE_STATE_LOCK_KEY},
            postgres::db::SCHEMA_INIT_LOCK_KEY,
            PostgresDb, Storage,
        },
        DatasourceType,
    },
    solana_commitment_config::CommitmentLevel,
    sqlx::Connection,
    std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
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

fn operator_config() -> OperatorConfig {
    OperatorConfig {
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
    }
}

fn indexer_config() -> IndexerConfig {
    IndexerConfig {
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
            ..Default::default()
        },
    }
}

/// Leave the marker a resync writes when it deletes rows, as if it died before rebuilding them.
async fn seed_unfinished_resync(url: &str, program: &str) {
    connect(url).await.init_schema().await.expect("schema");
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    sqlx::query("INSERT INTO resync_state (program_type) VALUES ($1)")
        .bind(program)
        .execute(&pool)
        .await
        .expect("seed the unfinished-resync marker");
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
    let result = operator::run(
        connect(&url).await,
        common_config(postgres),
        operator_config(),
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
    let result =
        private_channel_indexer::run(common_config(postgres), indexer_config(), None).await;

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

/// I11. A resync that deleted rows and died no longer holds the lock, so only the marker
/// stops an operator from minting or releasing against the half-built rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_refuses_to_start_after_an_unfinished_resync() {
    let (url, _container) = start_postgres("unfinished_resync_operator").await;
    seed_unfinished_resync(&url, "escrow").await;
    let postgres = PostgresConfig {
        database_url: url.clone(),
        max_connections: 5,
    };

    let result = operator::run(
        connect(&url).await,
        common_config(postgres),
        operator_config(),
        None,
    )
    .await;

    assert!(
        matches!(
            &result,
            Err(OperatorError::Storage(StorageError::UnfinishedResync { program })) if program == "escrow"
        ),
        "the operator must refuse to start after an unfinished resync, got: {result:?}"
    );
}

/// I12. Same for the indexer, which would otherwise index on top of the half-built rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexer_refuses_to_start_after_an_unfinished_resync() {
    let (url, _container) = start_postgres("unfinished_resync_indexer").await;
    seed_unfinished_resync(&url, "withdraw").await;
    let postgres = PostgresConfig {
        database_url: url.clone(),
        max_connections: 5,
    };

    let result =
        private_channel_indexer::run(common_config(postgres), indexer_config(), None).await;

    assert!(
        matches!(
            &result,
            Err(IndexerError::Storage(StorageError::UnfinishedResync { program })) if program == "withdraw"
        ),
        "the indexer must refuse to start after an unfinished resync, got: {result:?}"
    );
}

/// How long a startup stall may last once the lock is gone. Detection takes about one 5s
/// heartbeat, and every stall below lasts 30s or more if nothing stops it.
const LOCK_LOST_STOP_BOUND: Duration = Duration::from_secs(15);

/// Poll `cond` every 50ms until it holds, or panic naming what never happened.
async fn wait_until<F, Fut>(what: &str, within: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + within;
    while !cond().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Count the sessions holding (or waiting for) the advisory `key`.
async fn advisory_sessions(url: &str, key: i64, granted: bool) -> i64 {
    let mut conn = sqlx::PgConnection::connect(url)
        .await
        .expect("admin connect");
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
         AND granted = $2 AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(key)
    .bind(granted)
    .fetch_one(&mut conn)
    .await
    .expect("read pg_locks")
}

/// An RPC endpoint that accepts every connection and never answers, counting accepts.
async fn black_hole_rpc() -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind black hole");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        // Sockets are kept open so each request hangs until the client gives up.
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(socket);
        }
    });
    (url, accepted)
}

/// Start the operator, wait until it holds the live-state lock and `stalled` reports it is
/// parked inside the step under test, then kill the lock's session and return how `run` ended.
async fn run_until_lock_lost<S, Fut>(
    url: &str,
    common: PrivateChannelIndexerConfig,
    operator_config: OperatorConfig,
    stalled: S,
) -> Result<(), OperatorError>
where
    S: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let run = tokio::spawn(operator::run(
        connect(url).await,
        common,
        operator_config,
        None,
    ));
    wait_until(
        "the operator to take the live-state lock",
        Duration::from_secs(30),
        || async { advisory_sessions(url, LIVE_STATE_LOCK_KEY, true).await == 1 },
    )
    .await;
    wait_until("the operator to stall", Duration::from_secs(30), stalled).await;

    terminate_advisory_lock_holder(url, LIVE_STATE_LOCK_KEY).await;

    tokio::time::timeout(LOCK_LOST_STOP_BOUND, run)
        .await
        .expect("the operator must stop soon after losing the live-state lock")
        .expect("operator task must not panic")
}

fn assert_stopped_on_lock_loss(result: Result<(), OperatorError>) {
    assert!(
        matches!(
            result,
            Err(OperatorError::Storage(StorageError::LiveStateLockLost))
        ),
        "the operator must stop on the lost lock, got: {result:?}"
    );
}

/// I13. A lock lost while schema init waits must stop startup, not carry on after the wait.
/// The test holds the schema-init lock so init stays parked where the kill can land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_stops_when_the_live_lock_is_lost_during_schema_init() {
    let (url, _container) = start_postgres("live_lock_lost_schema_init").await;
    let mut schema_holder = sqlx::PgConnection::connect(&url)
        .await
        .expect("schema holder connect");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SCHEMA_INIT_LOCK_KEY)
        .execute(&mut schema_holder)
        .await
        .expect("hold the schema-init lock");

    let postgres = PostgresConfig {
        database_url: url.clone(),
        max_connections: 5,
    };
    let result = run_until_lock_lost(&url, common_config(postgres), operator_config(), || async {
        advisory_sessions(&url, SCHEMA_INIT_LOCK_KEY, false).await == 1
    })
    .await;

    assert_stopped_on_lock_loss(result);
}

/// I14. A lock lost while the withdraw fallback check waits on RPC must stop startup before
/// the storage writer starts. Both endpoints never answer, so the check stays parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_stops_when_the_live_lock_is_lost_during_fallback_validation() {
    let (url, _container) = start_postgres("live_lock_lost_fallback").await;
    let (primary, primary_accepts) = black_hole_rpc().await;
    let (fallback, _) = black_hole_rpc().await;
    let (mut common, operator_config) =
        withdraw_operator_configs(&url, &primary, Some(DEAD_RPC.to_string()));
    common.fallback_rpc_url = Some(fallback);

    let result = run_until_lock_lost(&url, common, operator_config, || async {
        primary_accepts.load(Ordering::SeqCst) >= 1
    })
    .await;

    assert_stopped_on_lock_loss(result);
}

/// I15. A lock lost while the withdraw boot preflight waits on RPC must stop startup before
/// any worker starts. With no fallback the preflight makes the first RPC call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_stops_when_the_live_lock_is_lost_during_the_withdraw_preflight() {
    let (url, _container) = start_postgres("live_lock_lost_preflight").await;
    let (rpc, accepts) = black_hole_rpc().await;
    // The helper sets escrow_instance_id; without it the preflight would be skipped.
    let (common, operator_config) =
        withdraw_operator_configs(&url, &rpc, Some(DEAD_RPC.to_string()));

    let result = run_until_lock_lost(&url, common, operator_config, || async {
        accepts.load(Ordering::SeqCst) >= 1
    })
    .await;

    assert_stopped_on_lock_loss(result);
}
