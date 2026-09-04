//! E2E tests for the stuck-`Processing` recovery worker.

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    chrono::{Duration as ChronoDuration, Utc},
    private_channel_indexer::{
        config::ProgramType,
        metrics::{OPERATOR_STALE_PROCESSING_RECOVERED, OPERATOR_TRANSACTION_ERRORS},
        operator::{
            recovery::test_hooks,
            sender::{test_hooks as sender_hooks, types::SendDurability, types::SenderState},
            utils::instruction_util::{ExtraErrorCheckPolicy, MintToBuilder, RetryPolicy},
            utils::rpc_util::{RetryConfig, RpcClientWithRetry},
            utils::transaction_util::ConfirmationResult,
            SignerUtil, TransactionStatusUpdate,
        },
        storage::{common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    sender_fixtures::{
        account_info_reply_bytes, blockhash_reply, deposit_ctx, deposit_ctx_with_lease,
        make_config, make_instruction, pack_mint_with_authority, send_transaction_echo_reply,
    },
    serde_json::json,
    solana_keychain::SolanaSigner,
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        pubkey::Pubkey,
        signature::Signature,
    },
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

// -- fixture helpers ---------------------------------------------------------

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

async fn requeue_attempts_of(pool: &sqlx::PgPool, id: i64) -> i32 {
    sqlx::query_scalar("SELECT recovery_requeue_attempts FROM transactions WHERE id = $1")
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

/// SMT root of a fresh tree carrying `nonces`, computed with the same lib helper
/// the release-verify gate rebuilds from, so the crafted on-chain root is exact.
fn smt_root(tree_index: u64, nonces: &[u64]) -> [u8; 32] {
    use private_channel_indexer::operator::utils::smt_util::SmtState;
    let mut smt = SmtState::new(tree_index);
    for n in nonces {
        smt.insert_nonce(*n);
    }
    smt.current_root()
}

/// A `getAccountInfo` reply carrying a borsh-serialized escrow `Instance` with the
/// given SMT root and tree index. The verify gate reads this to prove/deny a release.
fn instance_account_reply(root: [u8; 32], tree_index: u64) -> Reply {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;
    use solana_pubkey::Pubkey as EscrowPubkey;

    let instance = Instance {
        discriminator: 0,
        bump: 0,
        version: 0,
        instance_seed: EscrowPubkey::new_unique(),
        admin: EscrowPubkey::new_unique(),
        withdrawal_transactions_root: root,
        current_tree_index: tree_index,
    };
    let mut bytes = Vec::new();
    instance.serialize(&mut bytes).expect("serialize instance");
    Reply::result(json!({
        "context": {"slot": 1000},
        "value": {
            "owner": EscrowPubkey::new_unique().to_string(),
            "lamports": 1_000_000u64,
            "data": [STANDARD.encode(&bytes), "base64"],
            "executable": false,
            "rentEpoch": 0
        }
    }))
}

// Deposit whose persisted broadcast signature finalized to Completed,
// recovered from the durable signature with no double-mint (no sendTransaction).

#[tokio::test(flavor = "multi_thread")]
async fn deposit_landed_promoted_to_completed() {
    let (db, url, _container) = start_pg("dep_landed").await;
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
    assert_recovered_increment(
        "escrow",
        "completed",
        "deposit",
        metric_before,
        "deposit landed completed",
    );
    mock.shutdown().await;
}

// Deposit with no persisted signature, provably never broadcast,
// demoted to Pending for a safe re-mint, consulting no RPC.

#[tokio::test(flavor = "multi_thread")]
async fn deposit_not_landed_demoted_to_pending() {
    let (db, url, _container) = start_pg("dep_demote").await;
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
    assert_recovered_increment(
        "escrow",
        "requeued",
        "deposit",
        metric_before,
        "deposit not landed requeued",
    );
    mock.shutdown().await;
}

// Deposit that WAS broadcast (persisted signature present) but whose mint is
// provably dead (null status, blockhash expired) is demoted for a safe re-mint. Unlike
// the no-signature case above, this exercises the RPC finality classification driving
// the re-mint decision, the case-(B)-dead double-mint boundary for deposits.

#[tokio::test(flavor = "multi_thread")]
async fn deposit_dead_signature_demoted() {
    let (db, url, _container) = start_pg("dep_dead_sig").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // Persisted write-ahead before broadcast, journaling the slot its blockhash was
    // read at; the mint never landed and the blockhash expired.
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, Some(0))
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    // Block height 200 is past lvbh 100, so the absence has expired. The height
    // is its own read on every chain: a response context slot is a slot, and
    // slots outrun heights.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(200)));
    // Ledger floor 0 covers the attempt window, so the expired absence is proven dead, not uncertain.
    mock.enqueue("getFirstAvailableBlock", Reply::result(json!(0)));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("escrow", "requeued", "deposit");

    test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, None, &storage_tx)
        .await
        .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "pending");
    assert_eq!(
        mock.call_count("getBlockHeight"),
        1,
        "the expiry check must be judged against a block height, not a context slot"
    );
    // Recovery classifies the dead signature but never re-mints itself (the fetcher does).
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment(
        "escrow",
        "requeued",
        "deposit",
        metric_before,
        "deposit dead signature requeued",
    );
    mock.shutdown().await;
}

// Withdrawal whose recorded release signature is dead (null status, blockhash expired) -> demote.

#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_dead_signature_demoted() {
    let (db, url, _container) = start_pg("wd_demote").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 7);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, None)
        .await
        .unwrap();

    let instance_pda = Pubkey::new_unique();
    let mock = MockRpcServer::start().await;
    // Status null + current height (1000) > lvbh (100) -> expired/dead.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(1000)));
    // Ledger floor 0 covers the attempt window, so the expired absence is proven dead, not uncertain.
    mock.enqueue("getFirstAvailableBlock", Reply::result(json!(0)));
    // Release-verify gate on the Dead arm: a finalized blockhash whose tip height
    // (1000 - 150 = 850) is past the attempt lvbh (100), plus an on-chain root that
    // excludes nonce 7, prove NotLanded, so the withdrawal is safe to demote. The
    // verifier binds the account read to this blockhash's context slot.
    mock.enqueue(
        "getLatestBlockhash",
        Reply::result(json!({
            "context": {"slot": 500},
            "value": {"blockhash": "11111111111111111111111111111111", "lastValidBlockHeight": 1000}
        })),
    );
    mock.enqueue(
        "getAccountInfo",
        instance_account_reply(smt_root(0, &[]), 0),
    );
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "requeued", "withdrawal");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(instance_pda),
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
    assert_recovered_increment(
        "withdraw",
        "requeued",
        "withdrawal",
        metric_before,
        "withdrawal dead signature requeued",
    );
    mock.shutdown().await;
}

/// The original issue-15 double-pay bug, closed: a pruned/lagging endpoint hides a
/// release that actually landed on-chain, so the classifier says Dead; the SMT-root
/// verifier proves Landed and we complete the row instead of re-paying it.
#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_dead_but_landed_completes_without_double_pay() {
    let (db, url, _container) = start_pg("wd_dead_but_landed").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let nonce = 7i64;
    let tx = make_withdrawal(&Signature::new_unique().to_string(), nonce);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // insert_transaction_internal does not persist the nonce column, so set it
    // explicitly: the SMT gate keys the on-chain membership proof off this value.
    sqlx::query("UPDATE transactions SET withdrawal_nonce = $1 WHERE id = $2")
        .bind(nonce)
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // The release write-ahead that actually landed, though the endpoint hides it.
    let landed_sig = Signature::new_unique();
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100, None)
        .await
        .unwrap();

    let instance_pda = Pubkey::new_unique();
    let mock = MockRpcServer::start().await;
    // Classifier says Dead: null status + current height (1000) > lvbh (100),
    // ledger floor 0 covers the attempt window (coverage-proven absence).
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(1000)));
    mock.enqueue("getFirstAvailableBlock", Reply::result(json!(0)));
    // Release-verify gate: a finalized blockhash whose tip height (1000 - 150 = 850)
    // is past the attempt lvbh (100), plus an on-chain root that INCLUDES nonce 7,
    // prove the release Landed, so the withdrawal is completed, not re-paid. The
    // verifier binds the account read to this blockhash's context slot.
    mock.enqueue(
        "getLatestBlockhash",
        Reply::result(json!({
            "context": {"slot": 500},
            "value": {"blockhash": "11111111111111111111111111111111", "lastValidBlockHeight": 1000}
        })),
    );
    mock.enqueue(
        "getAccountInfo",
        instance_account_reply(smt_root(0, &[nonce as u64]), 0),
    );
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "completed", "withdrawal");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(instance_pda),
        &storage_tx,
    )
    .await
    .unwrap();

    // Landed-but-hidden release completes the row from its recorded signature.
    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    // The whole point: nothing is re-broadcast, so no double-pay.
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert_recovered_increment(
        "withdraw",
        "completed",
        "withdrawal",
        metric_before,
        "withdrawal dead but landed completed",
    );
    mock.shutdown().await;
}

// Withdrawal whose recorded release signature finalized -> Completed, no re-send.

#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_landed_signature_completed_no_resend() {
    let (db, url, _container) = start_pg("wd_landed").await;
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
    assert_recovered_increment(
        "withdraw",
        "completed",
        "withdrawal",
        metric_before,
        "withdrawal landed completed",
    );
    mock.shutdown().await;
}

// Withdrawal whose recorded signature is still live -> left in Processing (no CAS write).

#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_live_signature_left_processing() {
    let (db, url, _container) = start_pg("wd_live").await;
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
    // Status null + current height (50) <= lvbh (1000) -> still live.
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
    // No CAS write -> updated_at stays backdated, not refreshed to "now".
    assert!(
        updated_at_of(&pool, tx_id).await < Utc::now() - ChronoDuration::minutes(5),
        "no CAS write means updated_at must stay backdated, not refreshed"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    mock.shutdown().await;
}

// Withdrawal with no recorded signatures -> requeue, gated on proving the
// nonce is absent from the on-chain root.

/// The two reads the signatureless proof makes: a finalized blockhash whose tip
/// height binds the snapshot, then the instance account holding `root`.
fn enqueue_release_proof(mock: &MockRpcServer, root: [u8; 32], tree_index: u64) {
    mock.enqueue(
        "getLatestBlockhash",
        Reply::result(json!({
            "context": {"slot": 500},
            "value": {"blockhash": "11111111111111111111111111111111", "lastValidBlockHeight": 1000}
        })),
    );
    mock.enqueue("getAccountInfo", instance_account_reply(root, tree_index));
}

// A `Processing` withdrawal stranded with no recorded signature never
// broadcast, and the on-chain root proves the nonce is absent, so the sweep
// re-arms it instead of paging a human. Before this it went to manual review,
// which wedged every higher nonce behind it.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_requeues_signatureless_withdrawal() {
    let (db, url, _container) = start_pg("wd_no_sigs").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 3);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    // Tree 0 holds no completed nonces, so its root excludes nonce 3.
    enqueue_release_proof(&mock, smt_root(0, &[]), 0);
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    let metric_before = snapshot_recovered("withdraw", "requeued", "withdrawal");

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "pending");
    assert_eq!(requeue_attempts_of(&pool, tx_id).await, 1);
    // No signature to classify, and re-arming never broadcasts.
    assert_eq!(mock.call_count("getSignatureStatuses"), 0);
    assert_eq!(mock.call_count("sendTransaction"), 0);
    assert!(
        storage_rx.try_recv().is_err(),
        "a proven-safe requeue must not page on-call"
    );
    assert_recovered_increment(
        "withdraw",
        "requeued",
        "withdrawal",
        metric_before,
        "signatureless withdrawal requeued",
    );
    mock.shutdown().await;
}

// The property the whole design rests on, in both orders against the real
// trigger and the real CAS: a recovery demote and a sender claim contend on one
// `updated_at`, so exactly one wins and the loser never broadcasts.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_demote_and_claim_yield_exactly_one_broadcast() {
    let nonce = 3u64;

    // Order 1: the sweep demotes first, so the sender's claim must lose.
    {
        let (db, url, _container) = start_pg("wd_race_demote_first").await;
        let storage = Arc::new(Storage::Postgres(db.clone()));
        storage.init_schema().await.unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();

        let tx = make_withdrawal(&Signature::new_unique().to_string(), nonce as i64);
        let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
        let held = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

        let mock = MockRpcServer::start().await;
        enqueue_release_proof(&mock, smt_root(0, &[]), 0);
        // The sender still builds and signs before it claims.
        mock.enqueue("getLatestBlockhash", blockhash_reply());
        mock.enqueue("sendTransaction", send_transaction_echo_reply());
        let client = test_client(mock.url());
        let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

        test_hooks::run_recovery_once(
            &storage,
            &client,
            ProgramType::Withdraw,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await
        .unwrap();
        assert_eq!(status_of(&pool, tx_id).await, "pending");

        let mut state = build_pg_sender_state(storage.clone(), mock.url()).await;
        state.release_leases.insert(nonce, held);
        sender_hooks::run_send_and_confirm(
            &mut state,
            make_instruction(),
            None,
            &sender_fixtures::withdrawal_ctx(tx_id, nonce),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;

        assert_eq!(
            mock.call_count("sendTransaction"),
            0,
            "the sender lost the claim and must not release"
        );
        assert!(
            db.get_release_signatures_internal(tx_id)
                .await
                .unwrap()
                .is_empty(),
            "a lost claim writes no signature"
        );
        assert_eq!(status_of(&pool, tx_id).await, "pending");
        mock.shutdown().await;
    }

    // Order 2: the sender claims first, so the sweep's demote must lose.
    {
        let (db, url, _container) = start_pg("wd_race_claim_first").await;
        let storage = Arc::new(Storage::Postgres(db.clone()));
        storage.init_schema().await.unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();

        let tx = make_withdrawal(&Signature::new_unique().to_string(), nonce as i64);
        let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
        let held = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

        let mock = MockRpcServer::start().await;
        mock.enqueue("getLatestBlockhash", blockhash_reply());
        mock.enqueue("sendTransaction", send_transaction_echo_reply());
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
        let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

        let mut state = build_pg_sender_state(storage.clone(), mock.url()).await;
        state.release_leases.insert(nonce, held);
        sender_hooks::run_send_and_confirm(
            &mut state,
            make_instruction(),
            None,
            &sender_fixtures::withdrawal_ctx(tx_id, nonce),
            RetryPolicy::None,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;
        assert_eq!(
            mock.call_count("sendTransaction"),
            1,
            "the owning sender releases exactly once"
        );

        // `held` is the token the sweep captured before its RPC round trip; the
        // claim has since bumped the row, so the demote write must find nothing.
        assert!(
            !storage.try_requeue_processing(tx_id, held).await.unwrap(),
            "a demote on a stale token must lose to the claim"
        );
        assert_eq!(
            status_of(&pool, tx_id).await,
            "processing",
            "the released row must not be re-armed"
        );
        mock.shutdown().await;
    }
}

// An unreadable proof is not evidence of a problem: the row is left exactly
// where it was, with no write at all, until it ages past the escalation window.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_leaves_row_processing_when_proof_unavailable() {
    let (db, url, _container) = start_pg("wd_proof_down").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 3);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // Past the 5 minute stale threshold so the sweep selects the row, but inside
    // the 10 minute escalation window so an unreadable proof still waits.
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(6)).await;
    // Read back rather than trusting the seed's return: Postgres stores
    // microseconds, so a nanosecond-precision local timestamp never compares equal.
    let backdated = updated_at_of(&pool, tx_id).await;

    let mock = MockRpcServer::start().await;
    // Nothing scripted: every freshness read errors, so the proof is inconclusive.
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "processing");
    assert_eq!(
        updated_at_of(&pool, tx_id).await,
        backdated,
        "the Uncertain path must not write, so updated_at stays untouched"
    );
    assert!(
        storage_rx.try_recv().is_err(),
        "a transient outage must not page on-call"
    );

    // Well past the 10 minute window, the same unreadable proof does escalate.
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(45)).await;
    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "manual_review");
    let update = storage_rx
        .try_recv()
        .expect("an escalated row must page on-call");
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(err.contains("still uncertain"), "reason: {err}");
    mock.shutdown().await;
}

// The sweep's own journal read is a DB read, and its internal retries cover only
// a moment. An outage past that must not quarantine the row: that would page on
// the very outage the row is recovering from, and wedge every higher nonce.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_leaves_row_processing_when_journal_unreadable() {
    let (db, url, _container) = start_pg("wd_journal_down").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_withdrawal(&Signature::new_unique().to_string(), 3);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // Past the 5 minute stale threshold so the sweep selects the row, but inside
    // the 10 minute escalation window so an unreadable journal still waits.
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(6)).await;
    let backdated = updated_at_of(&pool, tx_id).await;

    // Rename the journal table out from under the sweep so every read errors.
    // This is the DB-outage case with the rest of the row still reachable.
    sqlx::query("ALTER TABLE pending_release_signatures RENAME TO prs_hidden")
        .execute(&pool)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "an unreadable journal must not quarantine inside the window"
    );
    assert_eq!(
        updated_at_of(&pool, tx_id).await,
        backdated,
        "the wait must not write, so updated_at stays untouched"
    );
    assert!(
        storage_rx.try_recv().is_err(),
        "a transient outage must not page on-call"
    );

    // Restoring the table lets the very next sweep resolve the row normally,
    // proving the wait held it recoverable rather than merely deferring a page.
    sqlx::query("ALTER TABLE prs_hidden RENAME TO pending_release_signatures")
        .execute(&pool)
        .await
        .unwrap();
    // Tree 0 holds no completed nonces, so its root excludes nonce 3.
    enqueue_release_proof(&mock, smt_root(0, &[]), 0);
    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(
        status_of(&pool, tx_id).await,
        "pending",
        "once the journal reads again the row is re-armed, not escalated"
    );
    mock.shutdown().await;
}

// Boot reconcile converges on a mixed batch: the demotable row leaves the
// Processing set, the row whose recorded signature can still land stays put,
// and the pass loop terminates on its budget rather than spinning.
#[tokio::test(flavor = "multi_thread")]
async fn boot_reconcile_converges_with_mixed_rows() {
    let (db, url, _container) = start_pg("wd_boot_mixed").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let signatureless = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 3))
        .await
        .unwrap();
    seed_backdated_processing(&pool, signatureless, ChronoDuration::minutes(10)).await;

    let live = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 4))
        .await
        .unwrap();
    seed_backdated_processing(&pool, live, ChronoDuration::minutes(10)).await;
    db.insert_release_signature_internal(live, Signature::new_unique().to_string(), 5_000, None)
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    enqueue_release_proof(&mock, smt_root(0, &[]), 0);
    // The live row is re-classified every pass: a null status below its lvbh.
    mock.enqueue_sequence(
        "getSignatureStatuses",
        (0..16).map(|_| Reply::result(json!({"context": {"slot": 200}, "value": [null]}))),
    );
    mock.enqueue_sequence("getBlockHeight", (0..16).map(|_| Reply::result(json!(100))));
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    private_channel_indexer::operator::recovery::boot_reconcile_processing(
        &storage,
        &client,
        None,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
        &tokio_util::sync::CancellationToken::new(),
        8,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, signatureless).await, "pending");
    assert_eq!(
        status_of(&pool, live).await,
        "processing",
        "a still-live signature keeps its row for the next sweep"
    );
    // One proof only: the demoted row leaves the Processing set after pass 1.
    assert_eq!(mock.call_count("getAccountInfo"), 1);
    mock.shutdown().await;
}

// The requeue cap is the backstop: a row that keeps coming back is escalated
// rather than cycled between Pending and Processing forever.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_quarantines_after_requeue_cap() {
    let (db, url, _container) = start_pg("wd_requeue_cap").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx_id = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 3))
        .await
        .unwrap();
    // Set the counter first: any write fires the trigger and refreshes
    // updated_at, which would lift the row back out of the stale window.
    sqlx::query("UPDATE transactions SET recovery_requeue_attempts = 3 WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    // The proof still says NotLanded; the cap is what overrides the demote.
    enqueue_release_proof(&mock, smt_root(0, &[]), 0);
    let client = test_client(mock.url());
    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();

    assert_eq!(status_of(&pool, tx_id).await, "manual_review");
    let update = storage_rx.try_recv().expect("a capped row must page");
    let err = update.error_message.as_deref().unwrap_or("");
    assert!(err.contains("recovery requeues"), "reason: {err}");
    mock.shutdown().await;
}

// The point of re-arming: a requeued withdrawal is handed back out by the
// dequeue frontier, ahead of its higher-nonce sibling, so the demote unwedges
// the queue instead of blocking it the way manual_review does.
#[tokio::test(flavor = "multi_thread")]
async fn requeued_withdrawal_is_refetched_in_nonce_order() {
    let (db, url, _container) = start_pg("wd_refetch_order").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let stranded = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 3))
        .await
        .unwrap();
    seed_backdated_processing(&pool, stranded, ChronoDuration::minutes(10)).await;
    // A higher nonce waiting behind it, already Pending.
    let sibling = db
        .insert_transaction_internal(&make_withdrawal(&Signature::new_unique().to_string(), 9))
        .await
        .unwrap();

    let mock = MockRpcServer::start().await;
    enqueue_release_proof(&mock, smt_root(0, &[]), 0);
    let client = test_client(mock.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(
        &storage,
        &client,
        ProgramType::Withdraw,
        Some(Pubkey::new_unique()),
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(status_of(&pool, stranded).await, "pending");

    let locked = storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 10)
        .await
        .unwrap();
    let ids: Vec<i64> = locked.iter().map(|r| r.id).collect();
    assert!(
        ids.contains(&stranded),
        "the re-armed withdrawal must be re-fetchable: {ids:?}"
    );
    let position = |id: i64| ids.iter().position(|candidate| *candidate == id);
    assert!(
        position(sibling).is_none() || position(stranded) < position(sibling),
        "nonce order must be preserved: {ids:?}"
    );
    mock.shutdown().await;
}

// RPC uncertainty during classification -> quarantine, never demote.

#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_rpc_uncertain_quarantined() {
    let (db, url, _container) = start_pg("wd_uncertain").await;
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
    // getSignatureStatuses fails on every retry -> Uncertain.
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
        "withdrawal rpc uncertain quarantined",
    );
    mock.shutdown().await;
}

// GC backstop reclaims release sigs whose parent left Processing.

#[tokio::test(flavor = "multi_thread")]
async fn gc_reclaims_non_processing_release_sigs() {
    let (db, url, _container) = start_pg("gc_reclaim").await;
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

// Deposit with a persisted signature but an RPC that cannot classify it.
// ManualReview (never a silent demote, which would risk a double-mint).

#[tokio::test(flavor = "multi_thread")]
async fn rpc_failure_deposit_quarantines_to_manual_review() {
    let (db, url, _container) = start_pg("dep_rpc_down").await;
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
    assert_recovered_increment(
        "escrow",
        "quarantined",
        "deposit",
        metric_before,
        "deposit rpc failure quarantined",
    );
    mock.shutdown().await;
}

// A malformed persisted signature is uncertainty (never read as "dead"),
// quarantine via the shared load_pending_sigs path, with no RPC consulted.

#[tokio::test(flavor = "multi_thread")]
async fn malformed_stored_sig_quarantines_deposit() {
    let (db, url, _container) = start_pg("dep_malformed_sig").await;
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
    assert_recovered_increment(
        "escrow",
        "quarantined",
        "deposit",
        metric_before,
        "deposit malformed signature quarantined",
    );
    mock.shutdown().await;
}

// A fresh row is untouched (no RPC, no DB write).

#[tokio::test(flavor = "multi_thread")]
async fn fresh_processing_row_untouched() {
    let (db, url, _container) = start_pg("fresh_row").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // Flip to processing without backdating - updated_at is "now".
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

// The conditional write is a no-op if the row moved between SELECT and write.

#[tokio::test(flavor = "multi_thread")]
async fn conditional_write_noops_when_row_moved() {
    let (db, url, _container) = start_pg("cond_write").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let tx = make_deposit(&Signature::new_unique().to_string(), mint, recipient, 100);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let _captured = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    // Race: row already moved off Processing -> try_requeue returns false.
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

// A lagging terminal write cannot stomp a recovery demote.

#[tokio::test(flavor = "multi_thread")]
async fn lagging_terminal_write_no_ops_after_recovery_demote() {
    let (db, url, _container) = start_pg("lagging_write").await;
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

    // Lagging in-flight write from dead operator - must no-op.
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

// A 250-row backlog drained across multiple ticks.

#[tokio::test(flavor = "multi_thread")]
async fn backlog_batched_across_ticks() {
    let (db, url, _container) = start_pg("backlog_batched").await;
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

// PendingRemint rows are NOT touched by recovery.

#[tokio::test(flavor = "multi_thread")]
async fn pending_remint_rows_untouched() {
    let (db, url, _container) = start_pg("pending_remint_db").await;
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

// Withdrawal with NULL nonce -> ManualReview (runbook reason string).

#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_missing_nonce_quarantines() {
    let (db, url, _container) = start_pg("missing_nonce").await;
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
        "withdrawal missing nonce quarantined",
    );
    mock.shutdown().await;
}

// A deposit that keeps coming back NotLanded is quarantined once it hits
// the requeue cap instead of looping pending->processing->pending forever.

#[tokio::test(flavor = "multi_thread")]
async fn recovery_requeue_cap_quarantines_after_max() {
    let (db, url, _container) = start_pg("requeue_cap").await;
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
    assert_recovered_increment(
        "escrow",
        "quarantined",
        "deposit",
        metric_before,
        "deposit requeue cap quarantined",
    );
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
            TransactionType::Deposit,
            Duration::from_secs(5 * 60),
            100,
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

// -- ownership-checked deposit claim: the double-mint invariant end-to-end -----
//
// One escrow deposit must produce at most one channel mint even when the
// recovery worker demotes a row while a live in-memory Mint builder still
// holds it. These drive the production sender's first-fire path
// (`fire_and_store_task` via `run_fire_and_store_task`) against a real
// Postgres, so the claim CAS and recovery's demote race on the same rows.

const OWNERSHIP_LOST_REASON: &str = "deposit_ownership_lost";
const MINT_BROADCAST_METHOD: &str = "sendTransaction";

/// Count private-channel mint broadcasts so each assertion is falsifiable.
fn mint_broadcast_count(mock: &MockRpcServer) -> usize {
    mock.call_count(MINT_BROADCAST_METHOD)
}

async fn build_pg_sender_state(storage: Arc<Storage>, rpc_url: String) -> SenderState {
    sender_fixtures::ensure_admin_signer_env();
    sender_hooks::new_sender_state(
        &make_config(rpc_url, ProgramType::Escrow),
        CommitmentLevel::Confirmed,
        None,
        storage,
        1,
        1,
        None,
    )
    .expect("sender state construction against Postgres storage")
}

/// Drive one deposit first-fire builder through the production persist/claim
/// path with the given ownership token.
async fn drive_first_fire(
    state: &SenderState,
    tx_id: i64,
    token: chrono::DateTime<Utc>,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    sender_hooks::run_fire_and_store_task(
        state,
        make_instruction(),
        None,
        deposit_ctx(tx_id),
        RetryPolicy::None,
        ExtraErrorCheckPolicy::None,
        storage_tx,
        SendDurability::Recoverable {
            deposit_expected_updated_at: token,
        },
    )
    .await;
}

// A stale sender-owned builder whose row recovery already demoted must NOT
// broadcast. This is the exact reported bug, closed.
#[tokio::test(flavor = "multi_thread")]
async fn stale_owned_builder_does_not_double_mint() {
    let (db, url, _container) = start_pg("it_claim_stale").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        100,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // The row was locked at T_lock; the stale builder still carries this token.
    let t_lock = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    // build_and_sign needs a blockhash; the claim aborts before any broadcast.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    let recovery_client = test_client(mock.url());
    let state = build_pg_sender_state(storage.clone(), mock.url()).await;
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    // Recovery sees empty sigs and demotes the row to Pending (bumping updated_at).
    test_hooks::run_recovery_once(
        &storage,
        &recovery_client,
        ProgramType::Escrow,
        None,
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(status_of(&pool, tx_id).await, "pending");

    let metric = OPERATOR_TRANSACTION_ERRORS.with_label_values(&["escrow", OWNERSHIP_LOST_REASON]);
    let metric_before = metric.get();

    drive_first_fire(&state, tx_id, t_lock, &storage_tx).await;

    assert_eq!(
        mint_broadcast_count(&mock),
        0,
        "a demoted row's stale builder must not broadcast a mint"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "pending",
        "the lost claim must leave the row untouched for its current owner"
    );
    assert!(
        db.get_release_signatures_internal(tx_id)
            .await
            .unwrap()
            .is_empty(),
        "a lost claim persists no signature"
    );
    assert!(
        metric.get() > metric_before,
        "a lost claim increments deposit_ownership_lost"
    );
    mock.shutdown().await;
}

// The mid-JIT double-mint window, closed: a first mint claims and broadcasts,
// recovery demotes the row while the JIT verdict is pending, and the JIT
// re-fire then presents the lease of its own (now superseded) claim. The
// re-claim must lose, so nothing new is journaled or broadcast and the row
// stays with its current owner.
#[tokio::test(flavor = "multi_thread")]
async fn stale_jit_refire_does_not_double_mint() {
    let (db, url, _container) = start_pg("it_stale_jit_refire").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        100,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let t_lock = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    // First fire: build/sign, claim the row, journal one signature, broadcast.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    let mut state = build_pg_sender_state(storage.clone(), mock.url()).await;
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    drive_first_fire(&state, tx_id, t_lock, &storage_tx).await;
    assert_eq!(
        mint_broadcast_count(&mock),
        1,
        "the owned first fire broadcasts exactly once"
    );
    // The row's committed post-claim updated_at is the lease the first claim
    // returned; the JIT re-fire below carries it as its ownership token.
    let claim_lease = updated_at_of(&pool, tx_id).await;
    assert_ne!(claim_lease, t_lock, "the first claim advances the token");

    // Recovery demotes mid-JIT: age the row past the staleness threshold and
    // classify the journaled signature dead (null status, expired blockhash,
    // covered attempt window), so the deposit is requeued to Pending.
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(200)));
    mock.enqueue("getFirstAvailableBlock", Reply::result(json!(0)));
    // The coverage proof reads the channel's live blockhash window per verdict.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    let recovery_client = test_client(mock.url());
    test_hooks::run_recovery_once(
        &storage,
        &recovery_client,
        ProgramType::Escrow,
        None,
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(status_of(&pool, tx_id).await, "pending");
    let sigs_after_demote = db.get_release_signatures_internal(tx_id).await.unwrap();

    // The JIT re-fire: MintNotInitialized verdict, pre-check reads an
    // admin-authority initialized mint (Retry), build/sign gets a blockhash,
    // then the re-claim runs with the stale lease and must abort.
    let mut builder = MintToBuilder::new();
    builder.mint(Pubkey::new_unique());
    state.mint_builders.insert(tx_id, builder);
    let admin_bytes =
        pack_mint_with_authority(spl_token::solana_program::program_option::COption::Some(
            SignerUtil::admin_signer().pubkey(),
        ));
    mock.enqueue("getAccountInfo", account_info_reply_bytes(&admin_bytes));
    mock.enqueue("getLatestBlockhash", blockhash_reply());

    let metric = OPERATOR_TRANSACTION_ERRORS.with_label_values(&["escrow", OWNERSHIP_LOST_REASON]);
    let metric_before = metric.get();
    let ctx = deposit_ctx_with_lease(tx_id, claim_lease);

    sender_hooks::handle_confirmation_result(
        &mut state,
        Ok(ConfirmationResult::MintNotInitialized),
        Signature::new_unique(),
        None,
        &ctx,
        make_instruction(),
        RetryPolicy::None,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;

    assert_eq!(
        mint_broadcast_count(&mock),
        1,
        "the stale JIT re-fire must not broadcast a second mint"
    );
    assert_eq!(
        db.get_release_signatures_internal(tx_id).await.unwrap(),
        sigs_after_demote,
        "a lost JIT claim journals no new signature"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "pending",
        "the lost claim leaves the row to its current owner"
    );
    assert!(
        metric.get() > metric_before,
        "a lost JIT claim increments deposit_ownership_lost"
    );
    mock.shutdown().await;
}

// An owned deposit mints exactly once. The happy-path oracle proving the
// guard does not strangle a legitimate mint.
#[tokio::test(flavor = "multi_thread")]
async fn owned_deposit_mints_once() {
    let (db, url, _container) = start_pg("it_claim_owned").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        100,
    );
    db.insert_transaction_internal(&tx).await.unwrap();

    // Lock the deposit the way the fetcher does and carry its true post-lock token.
    let locked = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await
        .unwrap();
    let row = locked.first().expect("locked deposit");
    let tx_id = row.id;
    let token = row.updated_at;

    let mock = MockRpcServer::start().await;
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    let state = build_pg_sender_state(storage.clone(), mock.url()).await;
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    drive_first_fire(&state, tx_id, token, &storage_tx).await;

    assert_eq!(
        mint_broadcast_count(&mock),
        1,
        "an owned deposit must broadcast exactly one mint"
    );
    assert_eq!(
        db.get_release_signatures_internal(tx_id)
            .await
            .unwrap()
            .len(),
        1,
        "the owned claim persists exactly one write-ahead signature"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "the claim keeps the row Processing (its terminal write is status-guarded)"
    );
    assert_ne!(
        updated_at_of(&pool, tx_id).await,
        token,
        "a successful claim bumps updated_at"
    );
    mock.shutdown().await;
}

// Demote then re-fetch, then drive BOTH the stale first builder and the
// second builder. Exactly one mint broadcasts across the whole sequence.
#[tokio::test(flavor = "multi_thread")]
async fn demote_then_refetch_mints_exactly_once() {
    let (db, url, _container) = start_pg("it_claim_refetch").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        100,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    // First lock at T_lock1, held by the stale builder B1.
    let t_lock1 = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;

    let mock = MockRpcServer::start().await;
    // Two first-fires each build+sign (blockhash); only the owned one broadcasts.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    let recovery_client = test_client(mock.url());
    let state = build_pg_sender_state(storage.clone(), mock.url()).await;
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    // Recovery demotes B1's row to Pending.
    test_hooks::run_recovery_once(
        &storage,
        &recovery_client,
        ProgramType::Escrow,
        None,
        &storage_tx,
    )
    .await
    .unwrap();

    // A fresh fetch re-locks the row as a new incarnation B2 with token T_lock2.
    let relocked = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await
        .unwrap();
    let t_lock2 = relocked
        .iter()
        .find(|r| r.id == tx_id)
        .expect("row re-locked")
        .updated_at;
    assert_ne!(t_lock1, t_lock2, "the re-lock must advance the token");

    // Drive the stale B1 first (must abort), then the owned B2 (mints once).
    drive_first_fire(&state, tx_id, t_lock1, &storage_tx).await;
    drive_first_fire(&state, tx_id, t_lock2, &storage_tx).await;

    assert_eq!(
        mint_broadcast_count(&mock),
        1,
        "across demote + re-fetch, exactly one mint broadcasts"
    );
    assert_eq!(
        db.get_release_signatures_internal(tx_id)
            .await
            .unwrap()
            .len(),
        1,
        "only the owned incarnation persists a signature"
    );
    mock.shutdown().await;
}

// -- cross-operator recovery and the reopened-row gate -----------------------

// The reported exploit end-to-end: a stale Processing deposit whose mint landed
// on the channel is invisible to the withdraw operator's sweep (whose Solana
// RPC would prove a coverage-backed false absence) and is completed, not
// re-minted, by the escrow operator's own sweep.
#[tokio::test(flavor = "multi_thread")]
async fn cross_operator_recovery_does_not_replay_deposit_mint() {
    let (db, url, _container) = start_pg("cross_op_recovery").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        777,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    let landed_sig = Signature::new_unique();
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100, None)
        .await
        .unwrap();

    // Solana mock: absent-but-covered (null status, expired blockhash, floor 0).
    // Pre-fix, this coverage-proven wrong-chain Dead is what demoted the row.
    let solana = MockRpcServer::start().await;
    solana.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({"context": {"slot": 200}, "value": [null]})),
    );
    solana.enqueue("getFirstAvailableBlock", Reply::result(json!(0)));
    let solana_client = test_client(solana.url());
    let (storage_tx, _rx) = mpsc::channel::<TransactionStatusUpdate>(8);

    test_hooks::run_recovery_once(
        &storage,
        &solana_client,
        ProgramType::Withdraw,
        None,
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "the withdraw sweep must not touch a deposit row"
    );
    assert_eq!(
        solana.call_count("getSignatureStatuses"),
        0,
        "a deposit's mint signatures must never be classified on Solana"
    );

    // Escrow sweep against the channel: the landed mint completes the row.
    let channel = MockRpcServer::start().await;
    channel.enqueue(
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
    let channel_client = test_client(channel.url());
    test_hooks::run_recovery_once(
        &storage,
        &channel_client,
        ProgramType::Escrow,
        None,
        &storage_tx,
    )
    .await
    .unwrap();
    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    assert_eq!(channel.call_count("sendTransaction"), 0);
    solana.shutdown().await;
    channel.shutdown().await;
}

// A demoted deposit keeps its write-ahead signature (GC retention), and when the
// row is re-locked the processor gate classifies it on the channel and completes
// the row instead of dispatching a second mint: one broadcast total.
#[tokio::test(flavor = "multi_thread")]
async fn demoted_deposit_reopens_through_gate_without_second_mint() {
    use private_channel_indexer::operator::{
        processor::{process_deposit_funds, ProcessorState},
        utils::instruction_util::TransactionBuilder,
        MintCache,
    };

    let (db, url, _container) = start_pg("reopened_gate").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        888,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let captured = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // Write-ahead persist of the first (and only) mint broadcast.
    let landed_sig = Signature::new_unique();
    db.insert_release_signature_internal(tx_id, landed_sig.to_string(), 100, None)
        .await
        .unwrap();

    // Recovery-style demote, then the GC pass that used to destroy the evidence.
    assert!(storage
        .try_requeue_processing(tx_id, captured)
        .await
        .unwrap());
    storage.gc_stale_release_signatures().await.unwrap();
    assert_eq!(
        db.get_release_signatures_internal(tx_id)
            .await
            .unwrap()
            .len(),
        1,
        "a demoted row's write-ahead signature must survive the GC"
    );

    // The escrow fetcher re-locks the row; the gate must classify the retained
    // signature on the channel before any mint is built.
    let relocked = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 10)
        .await
        .unwrap();
    let row = relocked
        .into_iter()
        .find(|r| r.id == tx_id)
        .expect("row re-locked");

    let channel = MockRpcServer::start().await;
    channel.enqueue(
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
    let channel_client = Arc::new(test_client(channel.url()));

    let mut ps = ProcessorState {
        admin_pubkey: Pubkey::new_unique(),
        release_funds_state: None,
        mint_cache: MintCache::new(storage.clone()),
    };
    let (fetcher_tx, fetcher_rx) = tokio::sync::mpsc::channel(1);
    let (sender_tx, mut sender_rx) = tokio::sync::mpsc::channel::<TransactionBuilder>(8);
    let (storage_tx, _storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);
    fetcher_tx.send(row).await.unwrap();
    drop(fetcher_tx);

    process_deposit_funds(
        &mut ps,
        fetcher_rx,
        sender_tx,
        storage_tx,
        storage.clone(),
        channel_client,
        None,
        ProgramType::Escrow,
    )
    .await
    .unwrap();

    assert!(
        sender_rx.try_recv().is_err(),
        "the gate must complete the row, never dispatch a second mint"
    );
    assert_eq!(status_of(&pool, tx_id).await, "completed");
    assert_eq!(
        counterpart_sig_of(&pool, tx_id).await,
        Some(landed_sig.to_string())
    );
    assert_eq!(
        channel.call_count("sendTransaction"),
        0,
        "exactly one mint broadcast total (the original write-ahead one)"
    );
    channel.shutdown().await;
}

// O2, the double-mint this issue is about: the channel node now reports an
// internal storage failure as a JSON-RPC error instead of a null status. The
// gate must read that as "cannot verify", leave the row Processing and dispatch
// no mint; before the core fix the same failure arrived as a null and, once past
// blockhash validity, would have been proven Dead and re-minted.
#[tokio::test(flavor = "multi_thread")]
async fn deposit_gate_channel_db_error_does_not_mint() {
    use private_channel_indexer::operator::{
        processor::{process_deposit_funds, ProcessorState},
        utils::instruction_util::TransactionBuilder,
        MintCache,
    };

    let (db, url, _container) = start_pg("gate_db_error").await;
    let storage = Arc::new(Storage::Postgres(db.clone()));
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let tx = make_deposit(
        &Signature::new_unique().to_string(),
        Pubkey::new_unique(),
        Pubkey::new_unique(),
        999,
    );
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();
    let captured = seed_backdated_processing(&pool, tx_id, ChronoDuration::minutes(10)).await;
    // Write-ahead persist of a broadcast whose blockhash has long expired.
    db.insert_release_signature_internal(tx_id, Signature::new_unique().to_string(), 100, None)
        .await
        .unwrap();

    // Demote and re-lock so the fetcher hands the gate a genuinely reopened row.
    assert!(storage
        .try_requeue_processing(tx_id, captured)
        .await
        .unwrap());
    let relocked = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 10)
        .await
        .unwrap();
    let row = relocked
        .into_iter()
        .find(|r| r.id == tx_id)
        .expect("row re-locked");

    let channel = MockRpcServer::start().await;
    channel.enqueue(
        "getSignatureStatuses",
        Reply::error(
            -32000,
            "Failed to get transaction status: connection closed",
        ),
    );
    let channel_client = Arc::new(test_client(channel.url()));

    let mut ps = ProcessorState {
        admin_pubkey: Pubkey::new_unique(),
        release_funds_state: None,
        mint_cache: MintCache::new(storage.clone()),
    };
    let (fetcher_tx, fetcher_rx) = tokio::sync::mpsc::channel(1);
    let (sender_tx, mut sender_rx) = tokio::sync::mpsc::channel::<TransactionBuilder>(8);
    let (storage_tx, _storage_rx) = mpsc::channel::<TransactionStatusUpdate>(8);
    fetcher_tx.send(row).await.unwrap();
    drop(fetcher_tx);

    process_deposit_funds(
        &mut ps,
        fetcher_rx,
        sender_tx,
        storage_tx,
        storage.clone(),
        channel_client,
        None,
        ProgramType::Escrow,
    )
    .await
    .unwrap();

    assert!(
        sender_rx.try_recv().is_err(),
        "an unverifiable channel must never dispatch a mint"
    );
    assert_eq!(
        status_of(&pool, tx_id).await,
        "processing",
        "the row stays Processing for the recovery sweep to re-check"
    );
    channel.shutdown().await;
}
