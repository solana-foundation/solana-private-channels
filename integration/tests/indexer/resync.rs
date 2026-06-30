//! Integration tests for [`ResyncService`].
//!
//! Two groups:
//!
//! 1. Legacy rebuild behavior (source-RPC only): resync drops the DB, recreates
//!    the schema, and backfills from a caller-supplied genesis slot.
//! 2. Reconcile-on-rebuild: with a PrivateChannel RPC configured via
//!    `.with_channel_reconcile(...)`, resync builds a consumed-set from the
//!    channel BEFORE dropping tables (fail closed) and rebuilds each already
//!    serviced deposit/remint in its terminal state instead of `pending`. All
//!    pre-flight runs before the drop, so any abort leaves the live DB intact.
//!
//! The channel is scripted with `test_utils::mock_rpc::{MockRpcServer, Reply}`;
//! real escrow/withdraw events are produced on a `solana-test-validator` so the
//! backfill re-derives genuine rows. Because a rebuilt row's source-event-id is
//! computed from its on-chain coordinates (signature, instruction_index,
//! inner_index), every test first runs resync against an empty channel to discover
//! those exact coordinates, then scripts the matching memo and runs again. The
//! coordinates are chain-derived, hence stable across runs.

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

use private_channel_escrow_program_client::{
    instructions::DepositBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_indexer::{
    config::{BackfillConfig, ProgramType, ReconciliationConfig},
    error::{IndexerError, ReconciliationError},
    indexer::{
        datasource::rpc_polling::rpc::RpcPoller,
        reconciliation::run_startup_reconciliation,
        resync::{ChannelReconcileConfig, ResyncService},
    },
    operator::{
        utils::instruction_util::{mint_idempotency_memo, remint_idempotency_memo, SourceEventId},
        ConsumedMintKind, MINT_IDEMPOTENCY_SIGNATURE_LOOKBACK_LIMIT,
    },
    storage::{PostgresDb, Storage},
    PostgresConfig,
};
use serde_json::{json, Value};
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use test_utils::{
    mock_rpc::{MockRpcServer, Reply},
    validator_helper::{start_test_validator, start_test_validator_no_geyser},
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── constants ───────────────────────────────────────────────────────────────

/// Upper bound for a full resync (drop + schema + backfill + drain).
const RESYNC_TIMEOUT_SECS: u64 = 180;
/// Resync run attempts: the one-shot backfill can hit a transient `getBlock`
/// -32004 near the finalized edge; each retry re-runs the whole resync (including
/// consumed-set enumeration), so channel scripts enqueue this many copies.
const RESYNC_ATTEMPTS: usize = 4;
/// Per-user SPL balance minted at setup, large enough to fund deposits.
const USER_BALANCE: u64 = 1_000_000;
/// Deposit amount used by single-deposit scenarios.
const DEPOSIT_AMOUNT: u64 = 50_000;
/// Two distinct deposit amounts for the same-signature, two-inner-event case.
const DEPOSIT_AMOUNT_A: u64 = 11_000;
const DEPOSIT_AMOUNT_B: u64 = 22_000;
/// Withdrawal burn amount used by withdrawal scenarios.
const WITHDRAW_AMOUNT: u64 = 7_000;
/// Page size resync uses to enumerate the channel; mirrored so the page-2 test
/// can fill an exact first page and force the `before`-cursor to advance.
const CHANNEL_PAGE_LIMIT: usize = MINT_IDEMPOTENCY_SIGNATURE_LOOKBACK_LIMIT;

// ── postgres + service harness ──────────────────────────────────────────────

async fn start_postgres_for_resync(
    db_name: &str,
) -> Result<
    (
        String,
        Arc<Storage>,
        testcontainers::ContainerAsync<Postgres>,
    ),
    Box<dyn std::error::Error>,
> {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);

    let storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 5,
        })
        .await?,
    ));
    storage.init_schema().await?;

    Ok((db_url, storage, container))
}

/// Source-RPC-only resync service (no channel reconciliation): legacy behavior.
fn make_resync_service(rpc_url: String, storage: Arc<Storage>) -> ResyncService {
    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_url.clone(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
    ));
    let backfill_config = BackfillConfig {
        enabled: true,
        exit_after_backfill: true,
        rpc_url,
        batch_size: 50,
        max_gap_slots: u64::MAX,
        start_slot: None,
    };
    ResyncService::new(
        storage,
        rpc_poller,
        ProgramType::Escrow,
        backfill_config,
        None,
    )
}

/// Resync service that reconciles each rebuilt row against the PrivateChannel
/// at `channel_rpc_url` (D4). `escrow_instance_id` filters deposits to this
/// instance; pass `Some(instance)` for Escrow, `None` for Withdraw.
fn make_channel_resync_service(
    source_rpc_url: String,
    storage: Arc<Storage>,
    program_type: ProgramType,
    escrow_instance_id: Option<Pubkey>,
    channel_rpc_url: String,
    authority: Pubkey,
) -> ResyncService {
    let rpc_poller = Arc::new(RpcPoller::new(
        source_rpc_url.clone(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
    ));
    let backfill_config = BackfillConfig {
        enabled: true,
        exit_after_backfill: true,
        rpc_url: source_rpc_url,
        batch_size: 50,
        max_gap_slots: u64::MAX,
        start_slot: None,
    };
    ResyncService::new(
        storage,
        rpc_poller,
        program_type,
        backfill_config,
        escrow_instance_id,
    )
    .with_channel_reconcile(ChannelReconcileConfig {
        channel_rpc_url,
        authority,
    })
}

/// Wait until the confirmed tip reaches `target`. Backfill fetches blocks at
/// confirmed commitment; giving a few slots of headroom past the events ensures
/// each event's block is retrievable and strictly inside the backfill range
/// (the top slot is otherwise raced and `getBlock` may still return null).
async fn wait_for_finalized_slot(rpc_url: &str, target: u64) {
    let client = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::finalized());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if client
            .get_slot()
            .await
            .map(|s| s >= target)
            .unwrap_or(false)
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for confirmed slot to reach {target}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// True for transient `getBlock` errors near the finalized bleeding edge: the
/// validator reports a not-yet-served block (`-32004` "Block not available") that
/// the one-shot backfill cannot distinguish from a hard error. Retrying after a
/// short wait lets finalization catch up. This is an environment artifact of
/// solana-test-validator, not a resync behavior.
fn is_transient_block_error(e: &IndexerError) -> bool {
    let msg = e.to_string();
    msg.contains("Block not available") || msg.contains("SlotFetchFailed")
}

async fn run_resync(service: &ResyncService, genesis_slot: u64) -> Result<(), IndexerError> {
    for attempt in 1..=RESYNC_ATTEMPTS {
        let result = tokio::time::timeout(
            Duration::from_secs(RESYNC_TIMEOUT_SECS),
            service.run(genesis_slot),
        )
        .await
        .expect("ResyncService::run timed out");
        match result {
            Err(e) if attempt < RESYNC_ATTEMPTS && is_transient_block_error(&e) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
            other => return other,
        }
    }
    unreachable!("run_resync loop always returns within RESYNC_ATTEMPTS")
}

// ── channel scripting (consumed-set source) ─────────────────────────────────

/// One `getSignaturesForAddress` entry carrying `memo` on the landed channel mint.
fn channel_sig_entry(landed: &Signature, memo: &str) -> Value {
    json!({
        "signature": landed.to_string(),
        "slot": 100u64,
        "err": null,
        "memo": memo,
        "blockTime": 1_700_000_000i64,
        "confirmationStatus": "finalized",
    })
}

/// A non-idempotency filler entry (null memo) used only to pad a full page.
fn channel_filler_entry() -> Value {
    json!({
        "signature": Signature::new_unique().to_string(),
        "slot": 1u64,
        "err": null,
        "memo": null,
        "blockTime": null,
        "confirmationStatus": "confirmed",
    })
}

/// Memo string a serviced mint of `kind` for `id` carries on the channel.
fn memo_for(id: &SourceEventId, kind: ConsumedMintKind) -> String {
    match kind {
        ConsumedMintKind::Deposit => mint_idempotency_memo(id),
        ConsumedMintKind::Remint => remint_idempotency_memo(id),
    }
}

/// Enqueue one `getSignaturesForAddress` page that makes the channel report each
/// `(id, kind, landed)` as an already-serviced mint. Reused by every reconcile
/// case so per-test wire boilerplate stays a single line.
fn script_channel_consumed(
    mock: &MockRpcServer,
    entries: &[(SourceEventId, ConsumedMintKind, Signature)],
) {
    for _ in 0..RESYNC_ATTEMPTS {
        let page: Vec<Value> = entries
            .iter()
            .map(|(id, kind, landed)| channel_sig_entry(landed, &memo_for(id, *kind)))
            .collect();
        mock.enqueue("getSignaturesForAddress", Reply::result(Value::Array(page)));
    }
}

/// Enqueue an empty channel history: no serviced mints. Used by the discovery
/// run (which only needs the rebuilt rows' coordinates) and by IT-R12.
fn script_channel_empty(mock: &MockRpcServer) {
    for _ in 0..RESYNC_ATTEMPTS {
        mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    }
}

/// Place the single serviced mint on page 2: page 1 is a full `CHANNEL_PAGE_LIMIT`
/// page of fillers so the `before` cursor advances, then page 2 carries the memo.
fn script_channel_consumed_on_page2(
    mock: &MockRpcServer,
    id: &SourceEventId,
    kind: ConsumedMintKind,
    landed: &Signature,
) {
    for _ in 0..RESYNC_ATTEMPTS {
        let mut page1 = Vec::with_capacity(CHANNEL_PAGE_LIMIT);
        for _ in 0..CHANNEL_PAGE_LIMIT {
            page1.push(channel_filler_entry());
        }
        mock.enqueue(
            "getSignaturesForAddress",
            Reply::result(Value::Array(page1)),
        );
        mock.enqueue(
            "getSignaturesForAddress",
            Reply::result(json!([channel_sig_entry(landed, &memo_for(id, kind))])),
        );
    }
}

// ── DB assertion helpers (fresh pool: drop_tables invalidates old caches) ────

/// Natural key of a rebuilt row, used to script the exact consumed-set memo the
/// reconcile will look up.
#[derive(Clone, Debug)]
struct RowKey {
    signature: String,
    instruction_index: i32,
    inner_index: Option<i32>,
    transaction_type: String,
}

impl RowKey {
    /// Source-event-id the reconcile derives for this row.
    fn source_event_id(&self) -> SourceEventId {
        SourceEventId::new(&self.signature, self.instruction_index, self.inner_index)
    }
}

#[derive(Debug)]
struct RowStatus {
    status: String,
    counterpart_signature: Option<String>,
    landed_remint_signature: Option<String>,
}

async fn fresh_pool(db_url: &str) -> PgPool {
    PgPool::connect(db_url).await.expect("connect postgres")
}

async fn all_row_keys(db_url: &str) -> Vec<RowKey> {
    let pool = fresh_pool(db_url).await;
    let rows: Vec<(String, i32, Option<i32>, String)> = sqlx::query_as(
        "SELECT signature, instruction_index, inner_index, transaction_type::text \
         FROM transactions ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("query row keys");
    rows.into_iter()
        .map(
            |(signature, instruction_index, inner_index, transaction_type)| RowKey {
                signature,
                instruction_index,
                inner_index,
                transaction_type,
            },
        )
        .collect()
}

fn keys_of_type<'a>(keys: &'a [RowKey], ty: &str) -> Vec<&'a RowKey> {
    keys.iter().filter(|k| k.transaction_type == ty).collect()
}

async fn pending_count(db_url: &str) -> i64 {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM transactions WHERE status = 'pending'::transaction_status",
    )
    .fetch_one(&pool)
    .await
    .expect("count pending")
}

async fn row_count(db_url: &str) -> i64 {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(&pool)
        .await
        .expect("count rows")
}

async fn status_of(db_url: &str, key: &RowKey) -> RowStatus {
    let pool = fresh_pool(db_url).await;
    let (status, counterpart_signature, landed_remint_signature): (
        String,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT status::text, counterpart_signature, landed_remint_signature FROM transactions \
         WHERE signature = $1 AND instruction_index = $2 AND inner_index IS NOT DISTINCT FROM $3",
    )
    .bind(&key.signature)
    .bind(key.instruction_index)
    .bind(key.inner_index)
    .fetch_one(&pool)
    .await
    .expect("query row status");
    RowStatus {
        status,
        counterpart_signature,
        landed_remint_signature,
    }
}

async fn seed_pending_deposit(db_url: &str, signature: &str) {
    let pool = fresh_pool(db_url).await;
    sqlx::query(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, created_at, updated_at)
         VALUES ($1, 1, 'seed', 'seed', 'seed_mint', 100,
                 'deposit'::transaction_type, 'pending'::transaction_status,
                 NOW(), NOW())",
    )
    .bind(signature)
    .execute(&pool)
    .await
    .expect("seed pending deposit");
}

async fn mint_row_count(db_url: &str, mint: &str) -> i64 {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar("SELECT COUNT(*) FROM mints WHERE mint_address = $1")
        .bind(mint)
        .fetch_one(&pool)
        .await
        .expect("count mint rows")
}

// ── on-chain event production ───────────────────────────────────────────────

fn deposit_ix(user: &Keypair, instance: Pubkey, mint: Pubkey, amount: u64) -> Instruction {
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_PROGRAM_ID);
    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_PROGRAM_ID);
    DepositBuilder::new()
        .payer(user.pubkey())
        .user(user.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(spl_associated_token_account::ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(amount)
        .instruction()
}

async fn do_deposit(
    client: &RpcClient,
    user: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    amount: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let ix = deposit_ix(user, instance, mint, amount);
    helpers::send_and_confirm_instructions(client, &[ix], user, &[user], "Deposit").await?;
    Ok(())
}

/// Two Deposit instructions in ONE transaction (same signature, distinct
/// instruction/inner index): the identity-granularity fixture for IT-R10.
async fn do_double_deposit(
    client: &RpcClient,
    user: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    amount_a: u64,
    amount_b: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let ixs = [
        deposit_ix(user, instance, mint, amount_a),
        deposit_ix(user, instance, mint, amount_b),
    ];
    helpers::send_and_confirm_instructions(client, &ixs, user, &[user], "DoubleDeposit").await?;
    Ok(())
}

async fn new_storage(db_url: &str) -> Arc<Storage> {
    Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.to_string(),
            max_connections: 5,
        })
        .await
        .expect("connect storage"),
    ))
}

/// Parameters for a reconcile-on-rebuild resync. Each run builds a FRESH Storage:
/// `cleanup_after_backfill` closes the pool, so a Storage cannot be reused across
/// runs (discovery + reconcile, or an idempotency rerun).
struct Harness {
    db_url: String,
    source_rpc_url: String,
    program_type: ProgramType,
    instance: Option<Pubkey>,
    channel_url: String,
    genesis: u64,
}

impl Harness {
    async fn run(&self) -> Result<(), IndexerError> {
        let storage = new_storage(&self.db_url).await;
        let service = make_channel_resync_service(
            self.source_rpc_url.clone(),
            storage,
            self.program_type,
            self.instance,
            self.channel_url.clone(),
            Pubkey::new_unique(),
        );
        run_resync(&service, self.genesis).await
    }

    /// Run once against an empty channel so every row rebuilds pending, then read
    /// back each rebuilt row's natural key. A later run re-derives the same keys
    /// because they are chain-coordinates. Uses its own throwaway empty-channel
    /// mock so it never consumes the real run's scripted (consumed-set) replies.
    async fn discover(&self) -> Vec<RowKey> {
        let mock = MockRpcServer::start().await;
        script_channel_empty(&mock);
        let storage = new_storage(&self.db_url).await;
        let service = make_channel_resync_service(
            self.source_rpc_url.clone(),
            storage,
            self.program_type,
            self.instance,
            mock.url(),
            Pubkey::new_unique(),
        );
        run_resync(&service, self.genesis)
            .await
            .expect("discovery resync should succeed against an empty channel");
        mock.shutdown().await;
        all_row_keys(&self.db_url).await
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Legacy rebuild behavior (source-RPC only)
// ════════════════════════════════════════════════════════════════════════════

/// ResyncService drops all DB tables, recreates the schema, then runs a short
/// backfill (genesis_slot ~= current_slot -> very few slots to process).
/// After `run()` returns `Ok`, the transactions table must be empty.
#[tokio::test(flavor = "multi_thread")]
async fn test_resync_clears_db_and_returns_ok() -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();

    let (db_url, storage, _container) = start_postgres_for_resync("resync_clear_test").await?;

    // Insert a dummy row so we can verify that resync wipes it.
    seed_pending_deposit(&db_url, "resync_sig_001").await;
    assert_eq!(
        row_count(&db_url).await,
        1,
        "Should have 1 row before resync"
    );

    let current_slot = {
        let client = solana_client::rpc_client::RpcClient::new(rpc_url.clone());
        client.get_slot()?
    };

    let service = make_resync_service(rpc_url.clone(), storage);
    run_resync(&service, current_slot)
        .await
        .expect("resync should succeed");

    assert_eq!(
        row_count(&db_url).await,
        0,
        "Transactions table must be empty after resync"
    );
    Ok(())
}

/// D3: the genesis-slot check now runs BEFORE the drop, so a future-slot error
/// leaves the pre-existing DB completely intact (it is no longer recreated).
#[tokio::test(flavor = "multi_thread")]
async fn test_resync_rejects_future_genesis_slot() -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();

    let (db_url, storage, _container) =
        start_postgres_for_resync("resync_future_slot_test").await?;

    // Pre-seed a row that MUST survive the aborted run.
    seed_pending_deposit(&db_url, "resync_future_seed").await;

    let service = make_resync_service(rpc_url, storage);
    let result = service.run(u64::MAX).await;

    assert!(
        result.is_err(),
        "ResyncService::run with a future genesis_slot must return Err"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("genesis_slot")
            || err_msg.contains("current_slot")
            || err_msg.contains("ahead"),
        "Error should mention slot context, got: {err_msg}"
    );

    // The pre-existing row is intact: the drop never ran (D3).
    assert_eq!(
        row_count(&db_url).await,
        1,
        "pre-existing row must survive a future-slot abort (drop runs after the genesis check)"
    );
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// Reconcile-on-rebuild: deposit axis
// ════════════════════════════════════════════════════════════════════════════

/// IT-R1: a deposit the channel already minted is rebuilt `completed` with its
/// mint signature, never `pending` (no re-mint would be emitted for it).
#[tokio::test(flavor = "multi_thread")]
async fn resync_does_not_remint_serviced_deposit() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_serviced_deposit").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let deposits = keys_of_type(&keys, "deposit");
    assert_eq!(deposits.len(), 1, "exactly one deposit row expected");
    let dep = deposits[0].clone();

    let landed = Signature::new_unique();
    let id = dep.source_event_id();
    script_channel_consumed(&mock, &[(id, ConsumedMintKind::Deposit, landed)]);
    h.run().await.expect("reconciling resync should succeed");

    let st = status_of(&db_url, &dep).await;
    assert_eq!(
        st.status, "completed",
        "serviced deposit must rebuild completed"
    );
    assert_eq!(
        st.counterpart_signature.as_deref(),
        Some(landed.to_string().as_str()),
        "completed deposit must carry the channel mint signature"
    );
    assert_eq!(
        pending_count(&db_url).await,
        0,
        "no serviceable pending row may remain"
    );

    mock.shutdown().await;
    Ok(())
}

/// IT-R2: a deposit the channel never minted is rebuilt `pending` (it must be
/// minted exactly once later), and is NOT falsely marked completed.
#[tokio::test(flavor = "multi_thread")]
async fn resync_mints_genuinely_new_deposit_once() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_new_deposit").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    // Discovery doubles as the assertion run: the channel is empty, so the
    // genuine deposit must stay pending (not falsely completed).
    let keys = h.discover().await;
    let deposits = keys_of_type(&keys, "deposit");
    assert_eq!(deposits.len(), 1, "exactly one deposit row expected");
    let st = status_of(&db_url, deposits[0]).await;
    assert_eq!(
        st.status, "pending",
        "unserviced deposit must rebuild pending"
    );
    assert!(
        st.counterpart_signature.is_none(),
        "pending deposit must not carry a mint signature"
    );
    assert_eq!(pending_count(&db_url).await, 1);

    mock.shutdown().await;
    Ok(())
}

/// IT-R5: a mixed batch of deposits, some serviced and some not, each gets its
/// own correct disposition independently; counts match exactly.
///
/// A single resync processes one program type, so true deposit+withdrawal
/// mixing is impossible in one run; the withdrawal disposition is covered by
/// IT-R3/IT-R4. Here the mix is across deposits with differing service state.
#[tokio::test(flavor = "multi_thread")]
async fn resync_mixed_batch_classifies_each() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_mixed_batch").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    let user = &env.users[0];
    do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT).await?;
    do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT + 1).await?;
    do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT + 2).await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let deposits: Vec<RowKey> = keys_of_type(&keys, "deposit")
        .into_iter()
        .cloned()
        .collect();
    assert_eq!(deposits.len(), 3, "three deposit rows expected");

    // Service the first two; leave the third unserviced.
    let landed0 = Signature::new_unique();
    let landed1 = Signature::new_unique();
    script_channel_consumed(
        &mock,
        &[
            (
                deposits[0].source_event_id(),
                ConsumedMintKind::Deposit,
                landed0,
            ),
            (
                deposits[1].source_event_id(),
                ConsumedMintKind::Deposit,
                landed1,
            ),
        ],
    );
    h.run().await.expect("resync should succeed");

    assert_eq!(status_of(&db_url, &deposits[0]).await.status, "completed");
    assert_eq!(status_of(&db_url, &deposits[1]).await.status, "completed");
    assert_eq!(status_of(&db_url, &deposits[2]).await.status, "pending");
    assert_eq!(
        pending_count(&db_url).await,
        1,
        "exactly one row stays pending"
    );

    mock.shutdown().await;
    Ok(())
}

/// IT-R9: a serviced mint that sits on page 2 of `getSignaturesForAddress`
/// (reached via the `before` cursor) is still recognized -> deposit completed.
/// Guards the bounded-lookback blind spot.
#[tokio::test(flavor = "multi_thread")]
async fn resync_matches_serviced_mint_beyond_first_rpc_page(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_page2").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let deposits = keys_of_type(&keys, "deposit");
    assert_eq!(deposits.len(), 1);
    let dep = deposits[0].clone();

    let landed = Signature::new_unique();
    let id = dep.source_event_id();
    script_channel_consumed_on_page2(&mock, &id, ConsumedMintKind::Deposit, &landed);
    h.run().await.expect("resync should succeed");

    let st = status_of(&db_url, &dep).await;
    assert_eq!(
        st.status, "completed",
        "mint on page 2 must still reconcile"
    );
    assert_eq!(
        st.counterpart_signature.as_deref(),
        Some(landed.to_string().as_str())
    );
    // Two pages must have been fetched on the reconcile run (the `before` cursor
    // advanced past the full first page). Discovery runs against its own throwaway
    // mock (see `Harness::discover`), so it does not contribute to this count.
    assert!(
        mock.call_count("getSignaturesForAddress") >= 2,
        "expected two pages on the reconcile run, got {}",
        mock.call_count("getSignaturesForAddress")
    );

    mock.shutdown().await;
    Ok(())
}

/// IT-R10: two deposits share one source signature (distinct instruction/inner
/// index); only one is minted on the channel, so only that row completes and the
/// other stays pending. Locks identity granularity below the signature.
#[tokio::test(flavor = "multi_thread")]
async fn resync_distinguishes_deposits_by_inner_index() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_inner_index").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_double_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT_A,
        DEPOSIT_AMOUNT_B,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let deposits: Vec<RowKey> = keys_of_type(&keys, "deposit")
        .into_iter()
        .cloned()
        .collect();
    assert_eq!(
        deposits.len(),
        2,
        "two deposits expected from one transaction"
    );
    assert_eq!(
        deposits[0].signature, deposits[1].signature,
        "both deposits must share the source signature"
    );
    assert_ne!(
        (deposits[0].instruction_index, deposits[0].inner_index),
        (deposits[1].instruction_index, deposits[1].inner_index),
        "the two deposits must differ in instruction/inner index"
    );

    // Service only the first.
    let landed = Signature::new_unique();
    script_channel_consumed(
        &mock,
        &[(
            deposits[0].source_event_id(),
            ConsumedMintKind::Deposit,
            landed,
        )],
    );
    h.run().await.expect("resync should succeed");

    assert_eq!(status_of(&db_url, &deposits[0]).await.status, "completed");
    assert_eq!(
        status_of(&db_url, &deposits[1]).await.status,
        "pending",
        "the unminted sibling must stay pending despite sharing the signature"
    );

    mock.shutdown().await;
    Ok(())
}

/// IT-R11: running resync twice back-to-back converges to the identical end
/// state (ON CONFLICT keeps it idempotent: no duplicate rows, no status flips).
#[tokio::test(flavor = "multi_thread")]
async fn resync_rerun_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_idempotent").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let dep = keys_of_type(&keys, "deposit")[0].clone();
    let landed = Signature::new_unique();
    let id = dep.source_event_id();

    // Run 1.
    script_channel_consumed(&mock, &[(id.clone(), ConsumedMintKind::Deposit, landed)]);
    h.run().await.expect("first reconciling resync");
    let count_1 = row_count(&db_url).await;
    let st_1 = status_of(&db_url, &dep).await;

    // Run 2 (same scripted consumed-set).
    script_channel_consumed(&mock, &[(id, ConsumedMintKind::Deposit, landed)]);
    h.run().await.expect("second reconciling resync");
    let count_2 = row_count(&db_url).await;
    let st_2 = status_of(&db_url, &dep).await;

    assert_eq!(
        count_1, count_2,
        "row count must be identical across reruns"
    );
    assert_eq!(
        st_1.status, st_2.status,
        "status must not flip across reruns"
    );
    assert_eq!(st_2.status, "completed");
    assert_eq!(st_2.counterpart_signature, st_1.counterpart_signature);

    mock.shutdown().await;
    Ok(())
}

/// IT-R12: after a reconciling resync, the `mints` allowlist is repopulated and
/// `run_startup_reconciliation` returns Ok (preserves the startup-reconcile
/// contract). The deposit is genuine, so custody (instance ATA) matches the DB.
#[tokio::test(flavor = "multi_thread")]
async fn resync_preserves_startup_reconciliation_pass() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_startup_recon").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    // A single reconciling run against an empty channel is enough: the AllowMint
    // is replayed (genesis precedes setup) and the deposit row is rebuilt.
    script_channel_empty(&mock);
    h.run().await.expect("resync should succeed");

    assert_eq!(
        mint_row_count(&db_url, &env.mint.to_string()).await,
        1,
        "AllowMint replay must repopulate the mints allowlist"
    );

    // Wait for the finalized instance-ATA custody balance to match the deposit,
    // then assert startup reconciliation passes.
    let instance_ata =
        get_associated_token_address_with_program_id(&env.instance, &env.mint, &TOKEN_PROGRAM_ID);
    let fin_client =
        RpcClient::new_with_commitment(validator.rpc_url(), CommitmentConfig::finalized());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(b) = fin_client.get_token_account_balance(&instance_ata).await {
            if b.amount.parse::<u64>().unwrap_or(0) == DEPOSIT_AMOUNT {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for finalized custody balance"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Fresh storage: the resync run above closed its own pool, and the tables were
    // dropped + recreated, so an old pool's cached plans would be stale.
    let recon_storage = new_storage(&db_url).await;
    let recon = run_startup_reconciliation(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
        ProgramType::Escrow,
        &recon_storage,
        &validator.rpc_url(),
        &env.instance,
    )
    .await;
    assert!(
        recon.is_ok(),
        "startup reconciliation must pass after a reconciling resync: {recon:?}"
    );

    mock.shutdown().await;
    Ok(())
}

/// IT-R8: channel mints that do not match this deposit's source event must never
/// mark it completed: (a) a valid memo for a DIFFERENT source event, (b) a
/// non-idempotency memo, (c) a wrong-prefix memo. The deposit stays pending.
#[tokio::test(flavor = "multi_thread")]
async fn resync_ignores_foreign_event_and_nonidempotency_memos(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_foreign_memos").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    do_deposit(
        &client,
        &env.users[0],
        env.instance,
        env.mint,
        DEPOSIT_AMOUNT,
    )
    .await?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Escrow,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let dep = keys_of_type(&keys, "deposit")[0].clone();

    // (a) Valid current-scheme memo, but for a DIFFERENT source event -> wrong id.
    let foreign_id = SourceEventId::new("some-other-source-event", 7, None);
    let foreign_memo = mint_idempotency_memo(&foreign_id);
    // (b) non-idempotency memo, (c) wrong-prefix memo. Enqueue one page per
    // possible resync attempt (transient-block retries re-enumerate the channel).
    for _ in 0..RESYNC_ATTEMPTS {
        let page = json!([
            channel_sig_entry(&Signature::new_unique(), &foreign_memo),
            channel_sig_entry(&Signature::new_unique(), "just a normal user memo"),
            channel_sig_entry(
                &Signature::new_unique(),
                &format!("private_channel:not-idempotency:{}", foreign_id.as_str())
            ),
        ]);
        mock.enqueue("getSignaturesForAddress", Reply::result(page));
    }
    h.run().await.expect("resync should succeed");

    let st = status_of(&db_url, &dep).await;
    assert_eq!(
        st.status, "pending",
        "no foreign-event/non-idempotency memo may mark this deposit completed"
    );
    assert!(st.counterpart_signature.is_none());

    mock.shutdown().await;
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// Reconcile-on-rebuild: withdrawal axis (Withdraw program; instance = default)
// ════════════════════════════════════════════════════════════════════════════

/// IT-R3: a withdrawal whose release failed and was reminted (remint memo on the
/// channel) is rebuilt `failed_reminted` with the landed remint signature, so the
/// operator does not release escrow again (no double payout).
#[tokio::test(flavor = "multi_thread")]
async fn resync_reclassifies_failed_reminted_withdrawal() -> Result<(), Box<dyn std::error::Error>>
{
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_reminted_withdrawal").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    helpers::execute_user_withdrawal(&client, &env.users[0], env.mint, WITHDRAW_AMOUNT)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    // Withdraw program: no escrow instance; the source-event-id is instance-independent.
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Withdraw,
        instance: None,
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let withdrawals = keys_of_type(&keys, "withdrawal");
    assert_eq!(withdrawals.len(), 1, "exactly one withdrawal row expected");
    let wd = withdrawals[0].clone();

    let landed = Signature::new_unique();
    let id = wd.source_event_id();
    script_channel_consumed(&mock, &[(id, ConsumedMintKind::Remint, landed)]);
    h.run().await.expect("resync should succeed");

    let st = status_of(&db_url, &wd).await;
    assert_eq!(
        st.status, "failed_reminted",
        "a reminted withdrawal must rebuild failed_reminted, not pending"
    );
    assert_eq!(
        st.landed_remint_signature.as_deref(),
        Some(landed.to_string().as_str()),
        "failed_reminted row must carry the landed remint signature"
    );
    assert_eq!(pending_count(&db_url).await, 0);

    mock.shutdown().await;
    Ok(())
}

/// IT-R4: a withdrawal with NO remint memo on the channel is rebuilt `pending`.
/// (A re-attempted release is blocked on-chain by the SMT nonce-in-root guard,
/// which is outside resync's scope; here we lock that resync itself does not
/// fabricate a terminal state, leaving the on-chain guard as the sole arbiter.)
#[tokio::test(flavor = "multi_thread")]
async fn resync_leaves_released_withdrawal_for_smt_guard() -> Result<(), Box<dyn std::error::Error>>
{
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_released_withdrawal").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    helpers::execute_user_withdrawal(&client, &env.users[0], env.mint, WITHDRAW_AMOUNT)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Withdraw,
        instance: None,
        channel_url: mock.url(),
        genesis,
    };

    // Channel has no remint memo for this withdrawal -> stays pending.
    let keys = h.discover().await;
    let withdrawals = keys_of_type(&keys, "withdrawal");
    assert_eq!(withdrawals.len(), 1, "exactly one withdrawal row expected");
    let st = status_of(&db_url, withdrawals[0]).await;
    assert_eq!(
        st.status, "pending",
        "a withdrawal with no remint memo must rebuild pending"
    );
    assert!(st.landed_remint_signature.is_none());

    mock.shutdown().await;
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// Reconcile-on-rebuild: pre-drop fail-closed gates (no real source needed)
// ════════════════════════════════════════════════════════════════════════════

/// IT-R6: when the channel RPC errors mid-enumeration, `run()` returns Err AND
/// the pre-existing DB rows are still present (the drop never ran -- D3).
#[tokio::test(flavor = "multi_thread")]
async fn resync_aborts_when_channel_unreachable_db_intact() -> Result<(), Box<dyn std::error::Error>>
{
    let (validator, _faucet) = start_test_validator_no_geyser().await;
    let client = RpcClient::new(validator.rpc_url());
    let current_slot = client.get_slot().await?;
    let (db_url, storage, _pg) = start_postgres_for_resync("resync_channel_unreachable").await?;

    seed_pending_deposit(&db_url, "resync_unreachable_seed").await;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    // Channel enumeration fails: the RPC returns an error on every attempt.
    mock.enqueue_sequence(
        "getSignaturesForAddress",
        std::iter::repeat_with(|| Reply::error(-32000, "channel rpc boom")).take(8),
    );
    let service = make_channel_resync_service(
        validator.rpc_url(),
        storage,
        ProgramType::Escrow,
        Some(Pubkey::new_unique()),
        mock.url(),
        Pubkey::new_unique(),
    );

    // genesis == current_slot so the genesis check passes and execution reaches
    // the consumed-set enumeration, which then fails closed.
    let result = service.run(current_slot).await;
    assert!(
        matches!(
            result,
            Err(IndexerError::Reconciliation(
                ReconciliationError::ConsumedSetUnavailable { .. }
            ))
        ),
        "unreachable channel must abort with ConsumedSetUnavailable, got {result:?}"
    );

    assert_eq!(
        row_count(&db_url).await,
        1,
        "pre-existing row must survive: the drop never ran"
    );

    mock.shutdown().await;
    Ok(())
}

/// IT-R7: a legacy serial-id idempotency memo on the channel (unparseable under
/// the current scheme) aborts resync (cross-scheme guard) with the DB untouched,
/// and the error names the cutover.
#[tokio::test(flavor = "multi_thread")]
async fn resync_aborts_on_legacy_scheme_memo_db_intact() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, _faucet) = start_test_validator_no_geyser().await;
    let client = RpcClient::new(validator.rpc_url());
    let current_slot = client.get_slot().await?;
    let (db_url, storage, _pg) = start_postgres_for_resync("resync_legacy_memo").await?;

    seed_pending_deposit(&db_url, "resync_legacy_seed").await;

    // Headroom so every event's block is confirmed-available and inside the range.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&validator.rpc_url(), tip + 5).await;

    let mock = MockRpcServer::start().await;
    // Legacy serial-id memo: prefix present, value is a bare number (not a digest).
    let legacy = json!([channel_sig_entry(
        &Signature::new_unique(),
        "private_channel:mint-idempotency:42"
    )]);
    mock.enqueue("getSignaturesForAddress", Reply::result(legacy));
    let service = make_channel_resync_service(
        validator.rpc_url(),
        storage,
        ProgramType::Escrow,
        Some(Pubkey::new_unique()),
        mock.url(),
        Pubkey::new_unique(),
    );

    let result = service.run(current_slot).await;
    let err = match result {
        Err(IndexerError::Reconciliation(ReconciliationError::ConsumedSetUnavailable {
            reason,
        })) => reason,
        other => panic!("legacy-scheme memo must abort with ConsumedSetUnavailable, got {other:?}"),
    };
    assert!(
        err.contains("cutover"),
        "abort reason should name the memo cutover, got: {err}"
    );

    assert_eq!(
        row_count(&db_url).await,
        1,
        "pre-existing row must survive a cross-scheme abort"
    );

    mock.shutdown().await;
    Ok(())
}
