//! E2E tests for the stuck-`Processing` recovery worker.

use {
    chrono::{Duration as ChronoDuration, Utc},
    private_channel_indexer::{
        config::ProgramType,
        metrics::OPERATOR_STALE_PROCESSING_RECOVERED,
        operator::{
            recovery::test_hooks,
            utils::{
                instruction_util::mint_idempotency_memo,
                rpc_util::{RetryConfig, RpcClientWithRetry},
            },
            TransactionStatusUpdate,
        },
        storage::{common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    serde_json::json,
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    spl_associated_token_account::get_associated_token_address_with_program_id,
    std::{sync::Arc, time::Duration},
    test_utils::mock_rpc::{MockRpcServer, Reply},
    tokio::sync::mpsc,
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

/// Scripted `getTransaction` reply satisfying `transaction_matches_expected_mint`.
fn get_transaction_reply(
    signature: &Signature,
    mint: &Pubkey,
    recipient_ata: &Pubkey,
    mint_authority: &Pubkey,
    token_program: &Pubkey,
    amount: u64,
    memo: &str,
) -> Reply {
    let memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
    Reply::result(json!({
        "slot": 100,
        "blockTime": 1_700_000_000i64,
        "meta": {
            "err": null,
            "status": { "Ok": null },
            "fee": 5000u64,
            "innerInstructions": [],
            "preBalances": [1_000_000u64],
            "postBalances": [999_995u64],
            "logMessages": [],
            "preTokenBalances": [],
            "postTokenBalances": [],
            "rewards": [],
            "computeUnitsConsumed": 0u64,
        },
        "transaction": {
            "signatures": [signature.to_string()],
            "message": {
                "accountKeys": [
                    {"pubkey": mint_authority.to_string(), "signer": true, "writable": true, "source": "transaction"},
                    {"pubkey": recipient_ata.to_string(), "signer": false, "writable": true, "source": "transaction"},
                    {"pubkey": mint.to_string(), "signer": false, "writable": true, "source": "transaction"},
                    {"pubkey": token_program.to_string(), "signer": false, "writable": false, "source": "transaction"},
                    {"pubkey": memo_program_id, "signer": false, "writable": false, "source": "transaction"},
                ],
                "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                "instructions": [
                    {"program": "spl-memo", "programId": memo_program_id, "parsed": memo},
                    {
                        "program": "spl-token",
                        "programId": token_program.to_string(),
                        "parsed": {
                            "type": "mintTo",
                            "info": {
                                "mint": mint.to_string(),
                                "account": recipient_ata.to_string(),
                                "mintAuthority": mint_authority.to_string(),
                                "amount": amount.to_string(),
                            },
                        },
                    },
                ],
            },
        },
    }))
}

// IT-1: deposit landed → Completed (with on-chain sig).

#[tokio::test(flavor = "multi_thread")]
async fn it1_deposit_landed_promoted_to_completed() {
    let (db, url, _container) = start_pg("it1_landed").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let admin_pubkey = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let amount: u64 = 12_345;
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);

    let deposit_sig = Signature::new_unique();
    let tx = make_deposit(&deposit_sig.to_string(), mint, recipient, amount);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let _ = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let memo = mint_idempotency_memo(tx_id);
    let prior_sig = Signature::new_unique();

    let mock = MockRpcServer::start().await;
    mock.enqueue(
        "getSignaturesForAddress",
        Reply::result(json!([{
            "signature": prior_sig.to_string(),
            "slot": 100u64,
            "err": serde_json::Value::Null,
            "memo": format!("[{}] {}", memo.len(), memo),
            "blockTime": 1_700_000_000i64,
            "confirmationStatus": "finalized",
        }])),
    );
    mock.enqueue(
        "getTransaction",
        get_transaction_reply(
            &prior_sig,
            &mint,
            &recipient_ata,
            &admin_pubkey,
            &token_program,
            amount,
            &memo,
        ),
    );

    let client = test_client(mock.url());
    let (storage_tx, _storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "completed", "deposit");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        admin_pubkey,
        ProgramType::Escrow,
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(prior_sig.to_string())
    );
    // Recovery never sends transactions.
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("escrow", "completed", "deposit", metric_before, "IT-1");
    mock.shutdown().await;
}

// IT-2: deposit not landed → Pending (demote step).

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

    let mock = MockRpcServer::start().await;
    mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    let client = test_client(mock.url());
    let (storage_tx, _storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "requeued", "deposit");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "pending");
    // Live fetcher picks it up on the next tick (out of scope here).
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("escrow", "requeued", "deposit", metric_before, "IT-2");
    mock.shutdown().await;
}

// IT-3: withdrawal whose recorded release signature is dead (null status, blockhash expired) → demote.

#[tokio::test(flavor = "multi_thread")]
async fn it3_withdrawal_dead_signature_demoted() {
    let (db, url, _container) = start_pg("it3_wd_demote").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 7);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // Status null + current height (1000) > lvbh (100) → expired/dead.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(1000)));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "requeued", "withdrawal");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "pending");
    let fresh = updated_at_of(&pool, tx_id).await;
    assert!(
        fresh > Utc::now() - ChronoDuration::seconds(5),
        "updated_at should be fresh"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment("withdraw", "requeued", "withdrawal", metric_before, "IT-3");
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
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100)
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 1000)
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100)
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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
    db.insert_release_signature_internal(proc_id, Signature::new_unique().to_string(), 1)
        .await
        .unwrap();
    db.insert_release_signature_internal(done_id, Signature::new_unique().to_string(), 2)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    // recover_once runs gc_stale_release_signatures at the top of the sweep.
    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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

// IT-5: deposit RPC failure → ManualReview (never silent demote).

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

    let mock = MockRpcServer::start().await;
    // Always return a generic JSON-RPC error → transport / RPC error path.
    mock.enqueue_sequence(
        "getSignaturesForAddress",
        vec![
            Reply::error(-32000, "internal"),
            Reply::error(-32000, "internal"),
            Reply::error(-32000, "internal"),
        ],
    );
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "quarantined", "deposit");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
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
        err.starts_with("deposit idempotency:"),
        "reason should match runbook substring: {err}"
    );
    assert_recovered_increment("escrow", "quarantined", "deposit", metric_before, "IT-5");
    mock.shutdown().await;
}

// IT-6: idempotency RPC -32601 (method not found) → Ambiguous → quarantine,
// NOT demote. A demote would re-mint a deposit we couldn't verify (double-mint).

#[tokio::test(flavor = "multi_thread")]
async fn it6_method_not_found_quarantines_deposit() {
    let (db, url, _container) = start_pg("it6_method_not_found").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 700);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    // -32601 is permanent (no retry) → strict lookup returns Err → Ambiguous.
    mock.enqueue(
        "getSignaturesForAddress",
        Reply::error(-32601, "Method not found"),
    );
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "quarantined", "deposit");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
    .await
    .unwrap();

    // Proves the RPC was actually reached (not a pre-RPC bail) and not retried.
    assert_eq!(
        mock.call_count("getSignaturesForAddress"),
        1,
        "the -32601 branch must be reached via exactly one RPC call"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "manual_review",
        "method-not-found is uncertainty → quarantine, never silent demote"
    );
    let update = storage_rx
        .try_recv()
        .expect("manual_review update should be sent");
    assert_eq!(update.transaction_id, tx_id);
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(
        err.starts_with("deposit idempotency:"),
        "reason should match runbook substring: {err}"
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
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

    let mock = MockRpcServer::start().await;
    mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(status_of(&pool, tx_id).await, "pending");

    // Lagging in-flight write from dead operator — must no-op.
    db.update_transaction_status_internal(
        tx_id,
        private_channel_indexer::storage::common::models::TransactionStatus::Completed,
        Some("lagging-sig".to_string()),
        Utc::now(),
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

    let mock = MockRpcServer::start().await;
    // Every getSignaturesForAddress returns empty → demote-all path.
    for _ in 0..300 {
        mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    }
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    // Tick 1: should heal exactly RECOVERY_BATCH_LIMIT (100) rows.
    let t0 = std::time::Instant::now();
    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
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
    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
    .await
    .unwrap();
    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Withdraw,
        &storage_tx,
    )
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

    let mock = MockRpcServer::start().await;
    // Empty signatures → NotLanded → would Demote, but the cap intercepts it.
    mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "quarantined", "deposit");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        Pubkey::new_unique(),
        ProgramType::Escrow,
        &storage_tx,
    )
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
        .get_stale_processing_transactions_internal(Duration::from_secs(5 * 60), 100)
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
