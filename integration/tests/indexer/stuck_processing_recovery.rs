//! E2E tests for the stuck-`Processing` recovery worker.

use {
    chrono::{Duration as ChronoDuration, Utc},
    private_channel_indexer::{
        config::ProgramType,
        metrics::OPERATOR_STALE_PROCESSING_RECOVERED,
        operator::{
            recovery::{boot_reconcile_processing, test_hooks},
            utils::rpc_util::{RetryConfig, RpcClientWithRetry},
            TransactionStatusUpdate,
        },
        storage::{common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    serde_json::json,
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    std::{sync::Arc, time::Duration},
    test_utils::mock_rpc::{MockRpcServer, Reply},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

/// Pre-test reading of a recovery-metric cell; assert `>snapshot` after.
fn snapshot_recovered(program: &str, outcome: &str, txn_type: &str) -> f64 {
    OPERATOR_STALE_PROCESSING_RECOVERED
        .with_label_values(&[program, outcome, txn_type])
        .get()
}

fn assert_recovered_increment(
    program: &str,
    outcome: &str,
    txn_type: &str,
    before: f64,
    label: &str,
) {
    let after = OPERATOR_STALE_PROCESSING_RECOVERED
        .with_label_values(&[program, outcome, txn_type])
        .get();
    assert!(
        after > before,
        "{label}: OPERATOR_STALE_PROCESSING_RECOVERED{{program={program},outcome={outcome},type={txn_type}}} \
         should have incremented (before={before}, after={after})"
    );
}

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

fn make_deposit(
    sig: &str,
    mint: Pubkey,
    recipient: Pubkey,
    amount: u64,
) -> private_channel_indexer::storage::common::models::DbTransaction {
    DbTransactionBuilder::new(sig.to_string(), 1, mint.to_string(), amount)
        .initiator(recipient.to_string())
        .recipient(recipient.to_string())
        .transaction_type(TransactionType::Deposit)
        .build()
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

/// Insert + flip to `processing` + backdate `updated_at` past the trigger.
async fn seed_backdated_processing(
    pool: &sqlx::PgPool,
    tx_id: i64,
    age: ChronoDuration,
) -> chrono::DateTime<Utc> {
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(pool)
        .await
        .unwrap();

    let backdated = Utc::now() - age;
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET updated_at = $1 WHERE id = $2")
        .bind(backdated)
        .bind(tx_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await
        .unwrap();
    backdated
}

async fn status_of(pool: &sqlx::PgPool, id: i64) -> String {
    sqlx::query_scalar::<_, String>("SELECT status::text FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn counterpart_sig_of(pool: &sqlx::PgPool, id: i64) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT counterpart_signature FROM transactions WHERE id = $1",
    )
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

fn test_client(url: String) -> RpcClientWithRetry {
    RpcClientWithRetry::with_retry_config(
        url,
        RetryConfig {
            max_attempts: 2,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
        },
        CommitmentConfig::confirmed(),
    )
}

// IT-1 / IT-D1: deposit whose persisted broadcast signature finalized to Completed,
// recovered from the durable signature with no double-mint (no sendTransaction).

#[tokio::test(flavor = "multi_thread")]
async fn it1_deposit_landed_promoted_to_completed() {
    let (db, url, _container) = start_pg("it1_landed").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        mint,
        recipient,
        12_345,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    // The mint persisted this signature write-ahead before broadcast; it then landed.
    let landed_sig = Signature::new_unique();
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": {"slot": 200},
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": null,
                "status": {"Ok": null},
                "confirmationStatus": "finalized"
            }]
        })),
    );
    let client = test_client(mock.url());
    let (storage_tx, _storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "completed", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    // Recovery never re-mints a landed deposit (no double-mint).
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("escrow", "completed", "deposit", metric_before, "IT-1");
    mock.shutdown().await;
}

// IT-2 / IT-D2: deposit with no persisted signature, provably never broadcast,
// demoted to Pending for a safe re-mint, consulting no RPC.

#[tokio::test(flavor = "multi_thread")]
async fn it2_deposit_not_landed_demoted_to_pending() {
    let (db, url, _container) = start_pg("it2_demote").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    // No persisted signature and no RPC mocks: empty-sigs demotes without any RPC call.
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "requeued", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "pending");
    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        0,
        "empty-sigs demote must not consult the RPC"
    );
    // Live fetcher picks it up on the next tick (out of scope here).
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("escrow", "requeued", "deposit", metric_before, "IT-2");
    mock.shutdown().await;
}

// IT-2b: deposit that WAS broadcast (persisted signature present) but whose mint is
// provably dead (null status, blockhash expired) is demoted for a safe re-mint. Unlike
// IT-2 (no signature, no RPC), this exercises the RPC finality classification driving
// the re-mint decision, the case-(B)-dead double-mint boundary for deposits.

#[tokio::test(flavor = "multi_thread")]
async fn it2b_deposit_dead_signature_demoted() {
    let (db, url, _container) = start_pg("it2b_dep_dead").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // Persisted write-ahead before broadcast; the mint never landed and the blockhash expired.
    // Journal the blockhash slot the attempt was built against: absence is only
    // proof of non-inclusion when the ledger is known to cover that window.
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, Some(50))
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // Status null + current height (1000) > lvbh (100) → expired/dead.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(1000)));
    // Ledger floor below the journaled slot, so the window is covered.
    mock.enqueue("getFirstAvailableBlock", Reply::result(json!(1)));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "requeued", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "pending");
    // Recovery classifies the dead signature but never re-mints itself (the fetcher does).
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("escrow", "requeued", "deposit", metric_before, "IT-2b");
    mock.shutdown().await;
}

// IT-3: withdrawal whose recorded release signature is dead (null status, blockhash
// expired) and no escrow instance is configured → quarantine, not demote. With no
// bitmap to check the nonce against, the release may have landed under a signature
// that was never journaled, so re-arming could pay twice.

#[tokio::test(flavor = "multi_thread")]
async fn it3_withdrawal_dead_signature_quarantines_without_instance() {
    let (db, url, _container) = start_pg("it3_wd_quarantine").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 7);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // Journal the blockhash slot the attempt was built against: absence is only
    // proof of non-inclusion when the ledger is known to cover that window.
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, Some(50))
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // Status null + current height (1000) > lvbh (100) → expired/dead.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(1000)));
    // Ledger floor below the journaled slot, so the window is covered.
    mock.enqueue("getFirstAvailableBlock", Reply::result(json!(1)));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "quarantined", "withdrawal");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "manual_review");
    let fresh = updated_at_of(&pool, tx_id).await;
    assert!(
        fresh > Utc::now() - ChronoDuration::seconds(5),
        "updated_at should be fresh"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment(
        "withdraw",
        "quarantined",
        "withdrawal",
        metric_before,
        "IT-3",
    );
    mock.shutdown().await;
}

// IT-4: withdrawal whose recorded release signature finalized → Completed, no re-send.

#[tokio::test(flavor = "multi_thread")]
async fn it4_withdrawal_landed_signature_completed_no_resend() {
    let (db, url, _container) = start_pg("it4_landed").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 1);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    let landed_sig = Signature::new_unique();
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": {"slot": 200},
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": null,
                "status": {"Ok": null},
                "confirmationStatus": "finalized"
            }]
        })),
    );
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "completed", "withdrawal");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("withdraw", "completed", "withdrawal", metric_before, "IT-4");
    mock.shutdown().await;
}

// IT-4b: withdrawal whose recorded signature is still live → left in Processing (no CAS write).

#[tokio::test(flavor = "multi_thread")]
async fn it4b_withdrawal_live_signature_left_processing() {
    let (db, url, _container) = start_pg("it4b_live").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 2);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let _captured = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 1000, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // Status null + current height (50) <= lvbh (1000) → still live.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(50)));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "live signature must leave the row in Processing for the next sweep"
    );
    // No CAS write → updated_at stays backdated, not refreshed to "now".
    assert!(
        updated_at_of(&pool, tx_id).await < Utc::now() - ChronoDuration::minutes(5),
        "no CAS write means updated_at must stay backdated, not refreshed"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    mock.shutdown().await;
}

// IT-4c: withdrawal with no recorded signatures → quarantine (can't verify, double-payout risk).

#[tokio::test(flavor = "multi_thread")]
async fn it4c_withdrawal_no_signatures_quarantined() {
    let (db, url, _container) = start_pg("it4c_no_sigs").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 3);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "quarantined", "withdrawal");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "manual_review");
    // No RPC needed — empty signature set short-circuits before classification.
    assert_eq!(mock.call_count("getSignatureStatuses"), 0);
    assert_eq!(mock.call_count("sendTransaction"), 0);
    let update = storage_rx
        .try_recv()
        .expect("manual_review update should be sent");
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(
        err.contains("no broadcast signatures recorded"),
        "reason: {err}"
    );
    assert_recovered_increment(
        "withdraw",
        "quarantined",
        "withdrawal",
        metric_before,
        "IT-4c",
    );
    mock.shutdown().await;
}

// IT-4d: RPC uncertainty during classification → quarantine, never demote.

#[tokio::test(flavor = "multi_thread")]
async fn it4d_withdrawal_rpc_uncertain_quarantined() {
    let (db, url, _container) = start_pg("it4d_uncertain").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 4);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // getSignatureStatuses fails on every retry → Uncertain.
    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "internal"),
            Reply::error(-32000, "internal"),
            Reply::error(-32000, "internal"),
        ],
    );
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "quarantined", "withdrawal");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "manual_review",
        "RPC uncertainty must quarantine, never silently demote"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    let update = storage_rx
        .try_recv()
        .expect("manual_review update should be sent");
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(
        err.contains("could not verify release landed"),
        "reason: {err}"
    );
    assert_recovered_increment(
        "withdraw",
        "quarantined",
        "withdrawal",
        metric_before,
        "IT-4d",
    );
    mock.shutdown().await;
}

// IT-4e: GC backstop reclaims release sigs whose parent left Processing.

#[tokio::test(flavor = "multi_thread")]
async fn it4e_gc_reclaims_non_processing_release_sigs() {
    let (db, url, _container) = start_pg("it4e_gc").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    // One processing withdrawal (sig retained) and one completed (sig GC'd).
    let proc = make_withdrawal(&Signature::new_unique().to_string(), 10);
    let proc_id = db.insert_transaction_internal(&proc).await.unwrap();
    let done = make_withdrawal(&Signature::new_unique().to_string(), 11);
    let done_id = db.insert_transaction_internal(&done).await.unwrap();
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(proc_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET status = 'completed'::transaction_status WHERE id = $1")
        .bind(done_id)
        .execute(&pool)
        .await
        .unwrap();
    db.insert_release_signature_internal(proc_id, Signature::new_unique().to_string(), 1, None)
        .await
        .unwrap();
    db.insert_release_signature_internal(done_id, Signature::new_unique().to_string(), 2, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    // recover_once runs gc_stale_release_signatures at the top of the sweep.
    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    let remaining_done: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pending_release_signatures WHERE transaction_id = $1",
    )
    .bind(done_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(remaining_done, 0, "completed txn's sig must be GC'd");
    let remaining_proc: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pending_release_signatures WHERE transaction_id = $1",
    )
    .bind(proc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(remaining_proc, 1, "processing txn's sig must be retained");
    mock.shutdown().await;
}

// IT-4f: the GC leaves an escalated row's signatures alone.

/// `ManualReview` means the outcome is unknown, and the broadcast signatures are
/// the only thing that can still decide it: the boot divergence check attributes
/// a consumed nonce with them, and without them it alerts instead of repairing.
/// Reclaiming them at the moment of doubt is the one case the GC must not touch.
#[tokio::test(flavor = "multi_thread")]
async fn it4f_gc_retains_manual_review_release_sigs() {
    let (db, url, _container) = start_pg("it4f_gc_manual").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let escalated = make_withdrawal(&Signature::new_unique().to_string(), 40);
    let id = db.insert_transaction_internal(&escalated).await.unwrap();
    sqlx::query(
        "UPDATE transactions SET status = 'manual_review'::transaction_status WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    db.insert_release_signature_internal(id, Signature::new_unique().to_string(), 7, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pending_release_signatures WHERE transaction_id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        remaining, 1,
        "an escalated row must keep the evidence that can resolve it"
    );
    mock.shutdown().await;
}

// IT-5 / IT-D5: deposit with a persisted signature but an RPC that cannot classify it.
// ManualReview (never a silent demote, which would risk a double-mint).

#[tokio::test(flavor = "multi_thread")]
async fn it5_rpc_failure_deposit_quarantines_to_manual_review() {
    let (db, url, _container) = start_pg("it5_rpc_down").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 500);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // The classifier's status RPC errors every attempt, so Uncertain, so quarantine.
    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "internal"),
            Reply::error(-32000, "internal"),
            Reply::error(-32000, "internal"),
        ],
    );
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "quarantined", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "manual_review",
        "RPC failure must NOT silently demote — fail-loud is the contract"
    );
    let update = storage_rx
        .try_recv()
        .expect("manual_review update should be sent");
    assert_eq!(update.transaction_id, tx_id);
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(
        err.contains("could not verify mint landed"),
        "reason should match runbook substring: {err}"
    );
    assert_recovered_increment("escrow", "quarantined", "deposit", metric_before, "IT-5");
    mock.shutdown().await;
}

// IT-6: a malformed persisted signature is uncertainty (never read as "dead"),
// quarantine via the shared load_pending_sigs path, with no RPC consulted.

#[tokio::test(flavor = "multi_thread")]
async fn it6_malformed_stored_sig_quarantines_deposit() {
    let (db, url, _container) = start_pg("it6_malformed_sig").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 700);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(tx_id, "not-a-valid-signature".to_string(), 100, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "quarantined", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        0,
        "a malformed stored signature must quarantine before any RPC"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "manual_review",
        "malformed signature is uncertainty so quarantine, never silent demote"
    );
    let update = storage_rx
        .try_recv()
        .expect("manual_review update should be sent");
    assert_eq!(update.transaction_id, tx_id);
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(
        err.contains("malformed stored release signature"),
        "reason should name the malformed signature: {err}"
    );
    assert_recovered_increment("escrow", "quarantined", "deposit", metric_before, "IT-6");
    mock.shutdown().await;
}

// IT-7: fresh row is untouched (no RPC, no DB write).

#[tokio::test(flavor = "multi_thread")]
async fn it7_fresh_processing_row_untouched() {
    let (db, url, _container) = start_pg("it7_fresh").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // Flip to processing without backdating — updated_at is "now".
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    let pre_updated = updated_at_of(&pool, tx_id).await;

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "fresh row must not be picked up by recovery"
    );
    assert_eq!(
        updated_at_of(&pool, tx_id).await,
        pre_updated,
        "fresh row's updated_at must not change"
    );
    for method in &["getSignaturesForAddress", "getTransaction"] {
        assert_eq!(
            mock.call_count(method),
            0,
            "{method} should have 0 calls for fresh row"
        );
    }
    mock.shutdown().await;
}

// IT-8: conditional write is a no-op if the row moved between SELECT and write.

#[tokio::test(flavor = "multi_thread")]
async fn it8_conditional_write_noops_when_row_moved() {
    let (db, url, _container) = start_pg("it8_cond").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let _captured = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    // Race: row already moved off Processing → try_requeue returns false.
    sqlx::query("UPDATE transactions SET status = 'completed'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();

    // Call the conditional write directly with the original captured timestamp.
    let moved = storage
        .try_requeue_processing(tx_id, _captured)
        .await
        .unwrap();
    assert!(
        !moved,
        "conditional write must no-op when row moved off Processing"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "completed",
        "row must remain at the new status"
    );
}

// IT-9: lagging terminal write cannot stomp a recovery demote.

#[tokio::test(flavor = "multi_thread")]
async fn it9_lagging_terminal_write_no_ops_after_recovery_demote() {
    let (db, url, _container) = start_pg("it9_lagging").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    // No persisted signature, so demote with no RPC call.
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();
    assert_eq!(status_of(&pool, tx_id).await, "pending");

    // Lagging in-flight write from dead operator — must no-op.
    db.update_transaction_status_internal(
        tx_id,
        private_channel_indexer::storage::common::models::TransactionStatus::Completed,
        Some("lagging-sig".to_string()),
        Utc::now(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        status_of(&pool, tx_id).await,
        "pending",
        "tightened terminal write must NOT overwrite a recovery demote"
    );
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        None,
        "lagging sig must NOT be persisted"
    );
    mock.shutdown().await;
}

// IT-10: 250-row backlog drained across multiple ticks.

#[tokio::test(flavor = "multi_thread")]
async fn it10_backlog_batched_across_ticks() {
    let (db, url, _container) = start_pg("it10_batched").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mut ids: Vec<i64> = Vec::with_capacity(250);
    for _ in 0..250 {
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
        let id = db.insert_transaction_internal(&tx).await.unwrap();
        ids.push(id);
    }
    // Bulk: flip all to processing then backdate once.
    sqlx::query(
        "UPDATE transactions SET status = 'processing'::transaction_status WHERE id = ANY($1)",
    )
    .bind(&ids)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET updated_at = $1 WHERE id = ANY($2)")
        .bind(Utc::now() - ChronoDuration::minutes(10))
        .bind(&ids)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();

    // No persisted signatures, so demote-all path, with no RPC consulted.
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    // Tick 1: should heal exactly RECOVERY_BATCH_LIMIT (100) rows.
    let t0 = std::time::Instant::now();
    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();
    assert!(
        t0.elapsed() < Duration::from_secs(20),
        "single tick should not starve the live path"
    );
    let pending_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE status = 'pending'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pending_count, 100, "tick 1 must heal exactly the batch cap");

    // Ticks 2-3: drain the rest. Healed rows are excluded (trigger bumped updated_at).
    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();
    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();
    let pending_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE status = 'pending'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        pending_count, 250,
        "all 250 rows must be healed across 3 ticks"
    );
    mock.shutdown().await;
}

// IT-11: PendingRemint rows are NOT touched by recovery.

#[tokio::test(flavor = "multi_thread")]
async fn it11_pending_remint_rows_untouched() {
    let (db, url, _container) = start_pg("it11_pending_remint").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 42);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // Set up as pending_remint with backdated updated_at.
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    db.set_pending_remint_internal(
        tx_id,
        vec!["fake-sig".to_string()],
        vec![1],
        Utc::now() + ChronoDuration::minutes(30),
        false,
    )
    .await
    .unwrap();

    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET updated_at = $1 WHERE id = $2")
        .bind(Utc::now() - ChronoDuration::minutes(10))
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "pending_remint",
        "pending_remint rows must not be touched by stuck-Processing recovery"
    );
    mock.shutdown().await;
}

// IT-12: withdrawal with NULL nonce → ManualReview (runbook reason string).

#[tokio::test(flavor = "multi_thread")]
async fn it12_withdrawal_missing_nonce_quarantines() {
    let (db, url, _container) = start_pg("it12_missing_nonce").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 99);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // Force-null the nonce after insert (simulates a corrupt row).
    sqlx::query("UPDATE transactions SET withdrawal_nonce = NULL WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "quarantined", "withdrawal");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "manual_review");
    let update = storage_rx
        .try_recv()
        .expect("manual_review update should be sent");
    assert_eq!(
        update.error_message.as_deref(),
        Some("withdrawal row missing nonce")
    );
    assert_recovered_increment(
        "withdraw",
        "quarantined",
        "withdrawal",
        metric_before,
        "IT-12",
    );
    mock.shutdown().await;
}

// IT-13: a deposit that keeps coming back NotLanded is quarantined once it hits
// the requeue cap instead of looping pending→processing→pending forever.

#[tokio::test(flavor = "multi_thread")]
async fn it13_recovery_requeue_cap_quarantines_after_max() {
    let (db, url, _container) = start_pg("it13_requeue_cap").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    // Seed the durable counter to MAX_RECOVERY_REQUEUE_ATTEMPTS (= 3); the row
    // has already used its requeue budget, so the next demote is quarantined.
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactions SET recovery_requeue_attempts = 3 WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();

    // No persisted signatures means would Demote, but the requeue cap intercepts it.
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "quarantined", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "manual_review",
        "row at the requeue cap must quarantine, not loop back to pending"
    );
    let update = storage_rx
        .try_recv()
        .expect("cap must fire the manual_review alert webhook");
    assert_eq!(update.transaction_id, tx_id);
    let err = update.error_message.as_deref().unwrap_or("");
    // Count tracks MAX_RECOVERY_REQUEUE_ATTEMPTS (= 3, see the seed above); pin it to catch an off-by-one cap.
    assert!(
        err.contains("3 recovery requeues"),
        "alert must name the requeue cap and its count: {err}"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("escrow", "quarantined", "deposit", metric_before, "IT-13");
    mock.shutdown().await;
}

// I1: the stale-Processing read is scoped to one transaction type. Only this can
// prove the SQL predicate, including that renumbering the placeholders left the
// threshold and limit binds intact.

#[tokio::test(flavor = "multi_thread")]
async fn stale_queries_filter_by_transaction_type() {
    let (db, url, _container) = start_pg("role_scope_query").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let deposit = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        1_000,
    );
    let deposit_id = db.insert_transaction_internal(&deposit).await.unwrap();
    seed_backdated_processing(&pool, deposit_id, ChronoDuration::minutes(10)).await;

    let withdrawal = make_withdrawal(&Signature::new_unique().to_string(), 55);
    let withdrawal_id = db.insert_transaction_internal(&withdrawal).await.unwrap();
    seed_backdated_processing(&pool, withdrawal_id, ChronoDuration::minutes(10)).await;

    let deposits = db
        .get_stale_processing_transactions_internal(
            Duration::from_secs(5 * 60),
            100,
            TransactionType::Deposit,
        )
        .await
        .unwrap();
    assert_eq!(
        deposits.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![deposit_id],
        "asking for deposits must not return the withdrawal"
    );

    let withdrawals = db
        .get_stale_processing_transactions_internal(
            Duration::from_secs(5 * 60),
            100,
            TransactionType::Withdrawal,
        )
        .await
        .unwrap();
    assert_eq!(
        withdrawals.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![withdrawal_id],
        "asking for withdrawals must not return the deposit"
    );

    // The type bind must not have displaced the threshold bind: a threshold
    // wider than the rows' age still excludes them.
    let too_young = db
        .get_stale_processing_transactions_internal(
            Duration::from_secs(60 * 60),
            100,
            TransactionType::Deposit,
        )
        .await
        .unwrap();
    assert!(too_young.is_empty(), "threshold bind must still be $1");

    // Nor the limit bind.
    let capped = db
        .get_stale_processing_transactions_internal(
            Duration::from_secs(5 * 60),
            0,
            TransactionType::Deposit,
        )
        .await
        .unwrap();
    assert!(capped.is_empty(), "limit bind must still be $2");
}

// I2: a withdraw operator must never sweep an escrow deposit row. Its RPC client
// points at the withdrawal destination chain, so classifying a deposit's mint
// signature there reads a chain the signature was never sent to. Ownership is
// checked before any request leaves the process, which the zero call counts pin.

#[tokio::test(flavor = "multi_thread")]
async fn withdraw_recovery_never_touches_escrow_deposit() {
    let (db, url, _container) = start_pg("role_scope_processing").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        4_242,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // The persisted mint signature is what a cross-role sweep would classify.
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        0,
        "ownership must be verified before any RPC to the wrong chain"
    );
    assert_eq!(
        mock.call_count("getBlockHeight"),
        0,
        "no height comparison may run against a foreign chain"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "a deposit is not the withdraw role's row to recover"
    );
    assert!(
        updated_at_of(&pool, tx_id).await < Utc::now() - ChronoDuration::minutes(5),
        "no write means updated_at stays backdated"
    );
    assert!(
        storage_rx.try_recv().is_err(),
        "skipping a foreign row must not alert"
    );
    mock.shutdown().await;
}

// I4: the boot reconcile sweeps with a ZERO threshold, so age protects nothing.
// A withdraw operator booting beside a live escrow operator must still leave every
// deposit alone, including ones actively being minted.

#[tokio::test(flavor = "multi_thread")]
async fn withdraw_boot_reconcile_ignores_foreign_processing_rows() {
    let (db, url, _container) = start_pg("role_scope_boot").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mut ids = Vec::new();
    for amount in [10u64, 20, 30] {
        let tx = make_deposit(
            &Signature::new_unique().to_string(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            amount,
        );
        let id = db.insert_transaction_internal(&tx).await.unwrap();
        seed_backdated_processing(&pool, id, ChronoDuration::minutes(10)).await;
        db.insert_release_signature_internal(id, Signature::new_unique().to_string(), 100, None)
            .await
            .unwrap();
        ids.push(id);
    }

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    boot_reconcile_processing(
        &storage,
        &client,
        None,
        ProgramType::Withdraw,
        None,
        &storage_tx,
        &CancellationToken::new(),
        2,
    )
    .await
    .unwrap();

    for id in ids {
        assert_eq!(
            status_of(&pool, id).await,
            "processing",
            "boot reconcile must leave foreign deposits untouched"
        );
    }
    assert_eq!(mock.call_count("getSignatureStatuses"), 0);
    assert_eq!(mock.call_count("getBlockHeight"), 0);
    mock.shutdown().await;
}

// Threshold boundary: three rows at -4:59 / -5:00 / -5:01, expect the two older returned.

#[tokio::test(flavor = "multi_thread")]
async fn threshold_boundary_returns_only_strictly_older_rows() {
    let (db, url, _container) = start_pg("it_boundary").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mut ids = Vec::new();
    for _ in 0..3 {
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 1);
        ids.push(db.insert_transaction_internal(&tx).await.unwrap());
    }
    sqlx::query(
        "UPDATE transactions SET status = 'processing'::transaction_status WHERE id = ANY($1)",
    )
    .bind(&ids)
    .execute(&pool)
    .await
    .unwrap();

    let ages = [
        ChronoDuration::seconds(4 * 60 + 59),
        ChronoDuration::seconds(5 * 60),
        ChronoDuration::seconds(5 * 60 + 1),
    ];
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();
    for (id, age) in ids.iter().zip(ages.iter()) {
        sqlx::query("UPDATE transactions SET updated_at = $1 WHERE id = $2")
            .bind(Utc::now() - *age)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(&pool)
        .await
        .unwrap();

    let stale = db
        .get_stale_processing_transactions_internal(
            Duration::from_secs(5 * 60),
            100,
            TransactionType::Deposit,
        )
        .await
        .unwrap();
    // 4:59 excluded; 5:00 is timing-dependent (Postgres `<` is strict).
    let returned_ids: std::collections::HashSet<i64> = stale.iter().map(|r| r.id).collect();
    assert!(
        !returned_ids.contains(&ids[0]),
        "4:59-old row must NOT be returned (younger than threshold)"
    );
    assert!(
        returned_ids.contains(&ids[2]),
        "5:01-old row MUST be returned (older than threshold)"
    );
}

// IT-14: a withdrawal quarantined on RPC uncertainty carries its release
// signatures on the row, so a later tick can prove the release landed and clear
// it, even though the signature journal has since been GC'd.

async fn remint_signatures_of(pool: &sqlx::PgPool, id: i64) -> Option<Vec<String>> {
    sqlx::query_scalar::<_, Option<Vec<String>>>(
        "SELECT remint_signatures FROM transactions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn journal_len(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM pending_release_signatures WHERE transaction_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn it14_manual_review_landed_release_clears_to_completed() {
    let (db, url, _container) = start_pg("it8_clears").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 21);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    let landed_sig = Signature::new_unique();
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100, None)
        .await
        .unwrap();

    // Pass 1: nothing scripted, so every getSignatureStatuses errors and the
    // classifier reports Uncertain, which quarantines.
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "manual_review");
    assert_eq!(
        remint_signatures_of(&pool, tx_id).await,
        Some(vec![landed_sig.to_string()]),
        "the quarantine must copy the evidence onto the row"
    );

    // Pass 2: the RPC recovers and reports the release finalized.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": {"slot": 200},
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": null,
                "status": {"Ok": null},
                "confirmationStatus": "finalized"
            }]
        })),
    );
    // Drop the journal by hand: the GC keeps `manual_review` signatures on
    // purpose, so clearing it here is what proves the promotion below came
    // from the row's own columns rather than the journal.
    storage.delete_release_signatures(tx_id).await.unwrap();
    assert_eq!(
        journal_len(&pool, tx_id).await,
        0,
        "precondition: the journal must be empty before the promoting pass"
    );

    let metric_before = snapshot_recovered("withdraw", "manual_review_cleared", "withdrawal");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment(
        "withdraw",
        "manual_review_cleared",
        "withdrawal",
        metric_before,
        "IT-14",
    );
    mock.shutdown().await;
}

// IT-15: a withdrawal quarantined with no evidence at all stays quarantined and
// never costs an RPC round trip, on this tick or any later one.

#[tokio::test(flavor = "multi_thread")]
async fn it15_manual_review_without_signatures_stays_quarantined() {
    let (db, url, _container) = start_pg("it9_no_evidence").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 22);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    for pass in 1..=2 {
        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, None, &storage_tx)
            .await
            .unwrap();
        assert_eq!(
            status_of(&pool, tx_id).await,
            "manual_review",
            "pass {pass}: a row with no evidence must stay for a human"
        );
    }

    assert_eq!(
        remint_signatures_of(&pool, tx_id).await,
        None,
        "there was nothing to record, so COALESCE must leave the column NULL"
    );
    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        0,
        "the permanently-stuck population must be filtered in SQL, never re-classified"
    );
    mock.shutdown().await;
}

// IT-16: the boot pre-flight's own path, against real Postgres. A PendingRemint
// withdrawal whose release finalized is promoted, and the nonce then appears in
// the completed set the bitmap diff is taken against. That last assertion is the whole
// point: it proves the promotion feeds the exact query that refuses the boot,
// which the MockStorage unit test cannot show.

#[tokio::test(flavor = "multi_thread")]
async fn it16_pending_remint_landed_release_enters_completed_nonce_set() {
    let (db, url, _container) = start_pg("it16_preflight").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 30);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // The insert omits withdrawal_nonce, so a trigger assigns it from a sequence.
    // Read back the value the row actually holds rather than the seeded one.
    let nonce_u64 =
        sqlx::query_scalar::<_, i64>("SELECT withdrawal_nonce FROM transactions WHERE id = $1")
            .bind(tx_id)
            .fetch_one(&pool)
            .await
            .unwrap() as u64;
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();

    // The release broadcast that could not be confirmed, parked for a later check.
    let landed_sig = Signature::new_unique();
    storage
        .set_pending_remint(
            tx_id,
            vec![landed_sig.to_string()],
            vec![100],
            Utc::now() + ChronoDuration::seconds(32),
            false,
        )
        .await
        .unwrap();

    // The wedge: the chain has this nonce, the completed set the gate diffs
    // against does not, so the bitmap check would see it as chain-ahead.
    assert!(
        !storage
            .get_completed_withdrawal_nonces(0, 1000)
            .await
            .unwrap()
            .contains(&nonce_u64),
        "a pending_remint row must start outside the completed set"
    );

    let mock = MockRpcServer::start().await;
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": {"slot": 200},
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": null,
                "status": {"Ok": null},
                "confirmationStatus": "finalized"
            }]
        })),
    );
    let client = test_client(mock.url());
    let metric_before = snapshot_recovered("withdraw", "pending_remint_cleared", "withdrawal");

    test_hooks::reconcile_stalled_withdrawals_once(
        &storage,
        &client,
        private_channel_indexer::storage::TransactionStatus::PendingRemint,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    assert!(
        storage
            .get_completed_withdrawal_nonces(0, 1000)
            .await
            .unwrap()
            .contains(&nonce_u64),
        "the promoted nonce must now be in the set the bitmap diff is taken against, \
         or the boot pre-flight would still see it as chain-ahead"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment(
        "withdraw",
        "pending_remint_cleared",
        "withdrawal",
        metric_before,
        "IT-16",
    );
    mock.shutdown().await;
}

// IT-17: a full batch of rows that can never clear must not hide the rows behind
// them. Nothing is written for a non-landed verdict, so an ordering that is
// stable across sweeps would hand back the same blocked rows forever and starve
// every later row, including a landed one that is still wedging the boot gate.

#[tokio::test(flavor = "multi_thread")]
async fn it17_stuck_batch_does_not_starve_later_stalled_rows() {
    let (db, url, _container) = start_pg("it17_starve").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let deadline = Utc::now() + ChronoDuration::seconds(32);

    // Exactly one full batch of rows that are fetched but can never classify:
    // the stored signature does not parse, so each is skipped without an RPC.
    let mut blocked = Vec::new();
    for _ in 0..100 {
        let tx = make_withdrawal(&Signature::new_unique().to_string(), 0);
        let id = db.insert_transaction_internal(&tx).await.unwrap();
        sqlx::query(
            "UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
        storage
            .set_pending_remint(
                id,
                vec!["not-a-signature".to_string()],
                vec![100],
                deadline,
                false,
            )
            .await
            .unwrap();
        blocked.push(id);
    }

    // Inserted last, so it sorts behind the whole blocked batch under any
    // ordering the sweep might use.
    let landed_sig = Signature::new_unique();
    let target = make_withdrawal(&Signature::new_unique().to_string(), 0);
    let target_id = db.insert_transaction_internal(&target).await.unwrap();
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(target_id)
        .execute(&pool)
        .await
        .unwrap();
    storage
        .set_pending_remint(
            target_id,
            vec![landed_sig.to_string()],
            vec![100],
            deadline,
            false,
        )
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": {"slot": 200},
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": null,
                "status": {"Ok": null},
                "confirmationStatus": "finalized"
            }]
        })),
    );
    let client = test_client(mock.url());

    test_hooks::reconcile_stalled_withdrawals_once(
        &storage,
        &client,
        private_channel_indexer::storage::TransactionStatus::PendingRemint,
    )
    .await
    .unwrap();

    assert_eq!(
        status_of(&pool, target_id).await,
        "completed",
        "a landed row behind a full batch of unclearable rows must still be reached"
    );
    assert_eq!(
        status_of(&pool, blocked[0]).await,
        "pending_remint",
        "rows that cannot classify stay exactly where they are"
    );
    mock.shutdown().await;
}
