//! E2E tests for the `Parked` withdrawal paths:
//!   * the Postgres CAS primitives (`try_park_processing`,
//!     `try_unpark_to_processing`, `try_requeue_parked`,
//!     `get_stale_parked_transactions`), exercised against a real database, and
//!   * the recovery worker's parked-sweep, which rescues `Parked` rows orphaned
//!     by a restart.
//!
//! The sender-side gate/drain that drive park/unpark in production run over
//! `Storage::Mock` in the sender test harness, so they cannot reach the Postgres
//! SQL these CAS methods live in; that SQL is covered here directly (matching the
//! conditional-write tests in `stuck_processing_recovery`). Recovery genuinely
//! runs over Postgres, so the parked-sweep is driven end-to-end.
//!
//! Uses testcontainers for isolated Postgres instances.

use {
    chrono::{Duration as ChronoDuration, Utc},
    private_channel_indexer::{
        config::ProgramType,
        metrics::OPERATOR_STALE_PROCESSING_RECOVERED,
        operator::{
            recovery::test_hooks,
            utils::rpc_util::{RetryConfig, RpcClientWithRetry},
            TransactionStatusUpdate,
        },
        storage::{common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    std::{sync::Arc, time::Duration},
    tokio::sync::mpsc,
};

// ── fixture helpers ─────────────────────────────────────────────────────────

async fn start_pg(
    db_name: &str,
) -> (
    PostgresDb,
    String,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);
    let db = PostgresDb::new(&PostgresConfig {
        database_url: url.clone(),
        max_connections: 10,
    })
    .await
    .unwrap();
    (db, url, container)
}

fn make_withdrawal(
    sig: &str,
    nonce: i64,
) -> private_channel_indexer::storage::common::models::DbTransaction {
    let mint = Pubkey::new_unique().to_string();
    let recipient = Pubkey::new_unique().to_string();
    let mut tx = DbTransactionBuilder::new(sig.to_string(), 1, mint, 10_000u64)
        .initiator(recipient.clone())
        .recipient(recipient)
        .transaction_type(TransactionType::Withdrawal)
        .build();
    tx.withdrawal_nonce = Some(nonce);
    tx
}

async fn set_status(pool: &sqlx::PgPool, id: i64, status: &str) {
    sqlx::query(&format!(
        "UPDATE transactions SET status = '{status}'::transaction_status WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

/// Force `updated_at` into the past, bypassing the `BEFORE UPDATE` trigger that
/// would otherwise rewrite it to `NOW()`. Mirrors `stuck_processing_recovery`.
async fn backdate(pool: &sqlx::PgPool, id: i64, age: ChronoDuration) {
    let backdated = Utc::now() - age;
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET updated_at = $1 WHERE id = $2")
        .bind(backdated)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await
        .unwrap();
}

async fn status_of(pool: &sqlx::PgPool, id: i64) -> String {
    sqlx::query_scalar::<_, String>("SELECT status::text FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn updated_at_of(pool: &sqlx::PgPool, id: i64) -> chrono::DateTime<Utc> {
    sqlx::query_scalar("SELECT updated_at FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn requeue_attempts_of(pool: &sqlx::PgPool, id: i64) -> i32 {
    sqlx::query_scalar("SELECT recovery_requeue_attempts FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The parked-rescue path never touches the chain, so recovery is handed a
/// client pointed at a dead address: any RPC call would fail the test.
fn dead_client() -> RpcClientWithRetry {
    RpcClientWithRetry::with_retry_config(
        "http://localhost:1".to_string(),
        RetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        },
        CommitmentConfig::confirmed(),
    )
}

// ── Postgres CAS primitives ───────────────────────────────────────────────

/// `try_park_processing`: `Processing → Parked` succeeds, a re-park
/// (`Parked → Parked`) is the heartbeat — it succeeds AND advances `updated_at`,
/// and a row in any other state is a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn try_park_processing_cas() {
    let (db, url, _c) = start_pg("park_cas").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let id = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 1))
        .await
        .unwrap();
    set_status(&pool, id, "processing").await;

    // Processing → Parked.
    assert!(storage.try_park_processing(id).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "parked");

    // Heartbeat: Parked → Parked succeeds and the trigger bumps updated_at.
    let before = updated_at_of(&pool, id).await;
    assert!(storage.try_park_processing(id).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "parked");
    assert!(
        updated_at_of(&pool, id).await > before,
        "re-park must refresh updated_at so recovery treats the row as live"
    );

    // Wrong state → no-op.
    set_status(&pool, id, "completed").await;
    assert!(!storage.try_park_processing(id).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "completed");
}

/// `try_unpark_to_processing`: strict `Parked → Processing`. A non-`Parked` row
/// is a no-op — this is what makes the drain drop its builder instead of
/// double-sending after recovery already requeued the nonce.
#[tokio::test(flavor = "multi_thread")]
async fn try_unpark_to_processing_cas() {
    let (db, url, _c) = start_pg("unpark_cas").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let id = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 2))
        .await
        .unwrap();

    // Parked → Processing.
    set_status(&pool, id, "parked").await;
    assert!(storage.try_unpark_to_processing(id).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "processing");

    // Not parked (e.g. recovery already requeued it to pending) → no-op.
    set_status(&pool, id, "pending").await;
    assert!(!storage.try_unpark_to_processing(id).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "pending");
}

/// `try_requeue_parked` is an optimistic CAS keyed on `updated_at`: a matching
/// timestamp flips `Parked → Pending`, a stale one (a live sender heartbeated
/// the row between recovery's SELECT and this write) no-ops and leaves it parked.
#[tokio::test(flavor = "multi_thread")]
async fn try_requeue_parked_cas() {
    let (db, url, _c) = start_pg("requeue_parked_cas").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let id = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 3))
        .await
        .unwrap();
    set_status(&pool, id, "parked").await;

    // Stale timestamp (a heartbeat raced in after recovery read the row) → no-op.
    let stale = updated_at_of(&pool, id).await - ChronoDuration::seconds(60);
    assert!(!storage.try_requeue_parked(id, stale).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "parked");

    // Matching timestamp (genuinely orphaned) → requeue to pending.
    let captured = updated_at_of(&pool, id).await;
    assert!(storage.try_requeue_parked(id, captured).await.unwrap());
    assert_eq!(status_of(&pool, id).await, "pending");
}

/// `get_stale_parked_transactions` is the read recovery uses to find orphaned
/// rows: only `Parked` rows older than the threshold, oldest-first, capped at
/// the limit. A freshly heartbeated parked row and rows in other states are
/// excluded — that exclusion is what keeps recovery off rows a live sender owns.
#[tokio::test(flavor = "multi_thread")]
async fn get_stale_parked_filters_and_orders() {
    let (db, url, _c) = start_pg("stale_parked_query").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    // Two stale parked rows (different ages) + one fresh parked + one stale
    // processing. Only the two stale parked rows should be selected.
    let old = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 10))
        .await
        .unwrap();
    set_status(&pool, old, "parked").await;
    backdate(&pool, old, ChronoDuration::minutes(20)).await;

    let newer = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 11))
        .await
        .unwrap();
    set_status(&pool, newer, "parked").await;
    backdate(&pool, newer, ChronoDuration::minutes(10)).await;

    let fresh = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 12))
        .await
        .unwrap();
    set_status(&pool, fresh, "parked").await; // updated_at ≈ now → within threshold

    let processing = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 13))
        .await
        .unwrap();
    set_status(&pool, processing, "processing").await;
    backdate(&pool, processing, ChronoDuration::minutes(20)).await;

    // Stale parked only, oldest updated_at first.
    let stale = storage
        .get_stale_parked_transactions(Duration::from_secs(5 * 60), 100)
        .await
        .unwrap();
    let ids: Vec<i64> = stale.iter().map(|t| t.id).collect();
    assert_eq!(ids, vec![old, newer]);

    // Limit is honored (FIFO over stale → the oldest).
    let limited = storage
        .get_stale_parked_transactions(Duration::from_secs(5 * 60), 1)
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].id, old);
}

// ── recovery parked-sweep (E2E through run_recovery_once) ─────────────────

/// A stale `Parked` row orphaned by a restart is rescued by the recovery worker:
/// flipped back to `Pending`, counted via the `requeued_parked` metric, and
/// done silently (no webhook) without burning the recovery retry cap — the row
/// was never broadcast, so there is nothing to cap.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_requeues_stale_parked_to_pending() {
    let (db, url, _c) = start_pg("recovery_parked_stale").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let id = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 20))
        .await
        .unwrap();
    set_status(&pool, id, "parked").await;
    // Older than the worker's STALE_THRESHOLD (5m) so the sweep selects it.
    backdate(&pool, id, ChronoDuration::minutes(10)).await;

    let metric_before = OPERATOR_STALE_PROCESSING_RECOVERED
        .with_label_values(&["withdraw", "requeued_parked", "withdrawal"])
        .get();
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &dead_client(), ProgramType::Withdraw, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, id).await,
        "pending",
        "stale parked row must be requeued for reprocessing"
    );
    assert_eq!(
        requeue_attempts_of(&pool, id).await,
        0,
        "parked requeue must not consume the recovery cap"
    );
    assert!(
        OPERATOR_STALE_PROCESSING_RECOVERED
            .with_label_values(&["withdraw", "requeued_parked", "withdrawal"])
            .get()
            > metric_before,
        "requeued_parked metric must increment"
    );
    assert!(
        storage_rx.try_recv().is_err(),
        "parked rescue is routine cleanup — no webhook alert"
    );
}

/// A fresh `Parked` row — one a live sender just heartbeated — is within the
/// staleness window, so recovery must leave it alone rather than steal a row
/// another worker still owns.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_leaves_fresh_parked_untouched() {
    let (db, url, _c) = start_pg("recovery_parked_fresh").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let id = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 21))
        .await
        .unwrap();
    set_status(&pool, id, "parked").await; // updated_at ≈ now → within threshold

    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);
    test_hooks::run_recovery_once(&storage, &dead_client(), ProgramType::Withdraw, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, id).await,
        "parked",
        "a freshly heartbeated parked row must not be requeued"
    );
}
