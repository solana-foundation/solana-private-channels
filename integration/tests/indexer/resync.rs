//! Integration tests for [`ResyncService`].
//!
//! Two groups:
//!
//! 1. Legacy rebuild behavior (source-RPC only): resync deletes its own program's
//!    rows and backfills them from a caller-supplied genesis slot.
//! 2. Reconcile-on-rebuild: with a PrivateChannel RPC configured via
//!    `.with_channel_reconcile(...)`, resync builds a consumed-set from the
//!    channel BEFORE deleting rows (fail closed) and rebuilds each already
//!    serviced deposit/remint in its terminal state instead of `pending`. All
//!    pre-flight runs before the delete, so any abort leaves the live DB intact.
//!
//! The channel is scripted with `test_utils::mock_rpc::{MockRpcServer, Reply}`;
//! real escrow/withdraw events are produced on a `solana-test-validator` so the
//! backfill re-derives genuine rows. Because a rebuilt row's source-event-id is
//! computed from its on-chain coordinates (signature, instruction_index,
//! inner_index), every test first runs resync against an empty channel to discover
//! those exact coordinates, then scripts the matching memo and runs again. The
//! coordinates are chain-derived, hence stable across runs.

// Shared `#[path]` helper modules (helpers, setup) expose more than this binary
// uses; match the sibling integration test crates and allow the unused items.
#![allow(dead_code)]

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

use helpers::WAIT_TIMEOUT_SECS;
use private_channel_escrow_program_client::{
    instructions::DepositBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_indexer::{
    config::{
        BackfillConfig, IndexerConfig, OperatorConfig, PrivateChannelIndexerConfig, ProgramType,
        ReconciliationConfig, RpcPollingConfig, StorageType,
    },
    error::{DataSourceError, IndexerError, OperatorError, ReconciliationError, StorageError},
    indexer::{
        datasource::rpc_polling::rpc::RpcPoller,
        reconciliation::run_startup_reconciliation,
        resync::{ChannelReconcileConfig, ResyncService},
    },
    operator::{
        self,
        utils::instruction_util::{mint_idempotency_memo, remint_idempotency_memo, SourceEventId},
        ConsumedMintKind, CONSUMED_SET_PAGE_SIZE,
    },
    storage::{
        common::storage::live_lock::{LiveLockMode, LIVE_STATE_LOCK_KEY},
        common::storage::resync_state::resync_halt_reason,
        PostgresDb, Storage,
    },
    DatasourceType, PostgresConfig,
};
use serde_json::{json, Value};
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment, TEST_ADMIN_KEYPAIR};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;
use sqlx::{PgPool, Row};
use std::{str::FromStr, sync::Arc, time::Duration};
use test_utils::{
    indexer_helper::{start_private_channel_indexer, start_solana_indexer_rpc_polling},
    mock_rpc::{MockRpcServer, Reply},
    operator_helper::{
        default_operator_config, start_private_channel_to_solana_operator_with_config,
        start_solana_to_private_channel_operator_with_config,
    },
    validator_helper::{start_test_validator, start_test_validator_no_geyser},
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_util::sync::CancellationToken;

// ── constants ───────────────────────────────────────────────────────────────

/// Slots the mid-rebuild loss test backfills over, one round trip each. Wide enough
/// that the fill is still running well after the lock is pulled, and recent enough
/// that every block in the range is retrievable.
const BACKFILL_SPAN_SLOTS: u64 = 300;
/// Finalized slots the mid-rebuild loss test waits for, so its fill is not over in one heartbeat.
const MIN_FILL_SLOTS: u64 = 40;

/// Upper bound for a full resync (wipe + backfill + drain).
const RESYNC_TIMEOUT_SECS: u64 = 180;
/// Resync run attempts: the one-shot backfill can hit a transient `getBlock`
/// -32004 near the finalized edge; each retry re-runs the whole resync (including
/// consumed-set enumeration), so channel scripts enqueue this many copies.
const RESYNC_ATTEMPTS: usize = 4;
/// Channel mint authority the reconcile harness enumerates; scripted mints are signed by it.
const CHANNEL_AUTHORITY: Pubkey = Pubkey::new_from_array([7u8; 32]);
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
const CHANNEL_PAGE_LIMIT: usize = CONSUMED_SET_PAGE_SIZE;

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

/// Same as above but leaves the database empty, so a test can prove resync builds
/// its own schema rather than assuming one is already there.
async fn start_bare_postgres_for_resync(
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

    Ok((db_url, storage, container))
}

/// Source-RPC-only resync service (no channel reconciliation): legacy behavior.
///
/// An escrow rebuild is refused without an instance scope, since an unset scope
/// filters out every escrow instruction and would rebuild an empty DB. These
/// tests run against a bare validator with no escrow deposits, so the scope only
/// has to be present; which key it names does not change the outcome.
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
        Some(Pubkey::new_unique()),
    )
    .with_stale_holder_grace(Duration::ZERO)
}

/// Resync service that reconciles each rebuilt row against the PrivateChannel
/// at `channel_rpc_url` (D4). `escrow_instance_id` filters deposits for Escrow
/// and names the bitmap a Withdraw rebuild checks. Both chains are the one
/// validator here, so the bitmap is read from `source_rpc_url`.
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
        rpc_url: source_rpc_url.clone(),
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
    .with_stale_holder_grace(Duration::ZERO)
    .with_channel_reconcile(ChannelReconcileConfig {
        channel_rpc_url,
        authority,
    })
    .with_withdrawal_bitmap_rpc(source_rpc_url)
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

/// A spl-token `MintTo` a scripted channel tx executes under its signer.
struct ScriptedMintTo {
    mint: Pubkey,
    recipient_ata: Pubkey,
    amount: u64,
}

/// `getTransaction` result for a successful channel tx signed only by `signer`, with
/// `mentioned` as a non-signing account key, one Memo instruction carrying `memo` and,
/// when given, a `MintTo` under `signer`.
fn channel_transaction(
    signer: &Pubkey,
    mentioned: &Pubkey,
    memo: &str,
    mint_to: Option<&ScriptedMintTo>,
) -> Value {
    let mut account_keys = vec![
        signer.to_string(),
        mentioned.to_string(),
        spl_memo::id().to_string(),
    ];
    let mut instructions = vec![json!({
        "programIdIndex": 2,
        "accounts": [],
        "data": bs58::encode(memo).into_string(),
    })];
    if let Some(mint_to) = mint_to {
        account_keys.extend([
            mint_to.mint.to_string(),
            mint_to.recipient_ata.to_string(),
            TOKEN_PROGRAM_ID.to_string(),
        ]);
        let data = spl_token::instruction::mint_to(
            &TOKEN_PROGRAM_ID,
            &mint_to.mint,
            &mint_to.recipient_ata,
            signer,
            &[],
            mint_to.amount,
        )
        .expect("build MintTo")
        .data;
        instructions.push(json!({
            "programIdIndex": 5,
            "accounts": [3, 4, 0],
            "data": bs58::encode(data).into_string(),
        }));
    }
    let balances = vec![0u64; account_keys.len()];

    json!({
        "slot": 100u64,
        "blockTime": 1_700_000_000i64,
        "transaction": {
            "signatures": [Signature::new_unique().to_string()],
            "message": {
                "header": {
                    "numRequiredSignatures": 1,
                    "numReadonlySignedAccounts": 0,
                    "numReadonlyUnsignedAccounts": account_keys.len() - 1,
                },
                "accountKeys": account_keys,
                "recentBlockhash": "11111111111111111111111111111111",
                "instructions": instructions,
            },
        },
        "meta": {
            "err": null,
            "status": {"Ok": null},
            "fee": 5000u64,
            "preBalances": balances,
            "postBalances": balances,
        },
    })
}

/// The mint the operator would have landed for `key`: its mint and amount into the
/// deposit recipient's or, for a remint, the withdrawal initiator's spl-token ATA.
fn landed_mint_to(key: &RowKey, kind: ConsumedMintKind) -> ScriptedMintTo {
    let owner = match kind {
        ConsumedMintKind::Deposit => &key.recipient,
        ConsumedMintKind::Remint => &key.initiator,
    };
    let owner = Pubkey::from_str(owner).expect("row owner is a pubkey");
    let mint = Pubkey::from_str(&key.mint).expect("row mint is a pubkey");
    ScriptedMintTo {
        mint,
        recipient_ata: get_associated_token_address_with_program_id(
            &owner,
            &mint,
            &TOKEN_PROGRAM_ID,
        ),
        amount: key.amount,
    }
}

/// The channel tx the authority landed for `key`: its memo and matching `MintTo`.
fn authority_signed_mint(key: &RowKey, kind: ConsumedMintKind) -> Value {
    channel_transaction(
        &CHANNEL_AUTHORITY,
        &Pubkey::new_unique(),
        &memo_for(&key.source_event_id(), kind),
        Some(&landed_mint_to(key, kind)),
    )
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
/// `(key, kind, landed)` as an already-serviced mint, and the matching signed mint
/// for each fetch. Reused by every reconcile case so per-test wire boilerplate stays
/// a single line.
fn script_channel_consumed(
    mock: &MockRpcServer,
    entries: &[(&RowKey, ConsumedMintKind, Signature)],
) {
    for _ in 0..RESYNC_ATTEMPTS {
        let page: Vec<Value> = entries
            .iter()
            .map(|(key, kind, landed)| {
                channel_sig_entry(landed, &memo_for(&key.source_event_id(), *kind))
            })
            .collect();
        mock.enqueue("getSignaturesForAddress", Reply::result(Value::Array(page)));
        for (key, kind, _) in entries {
            mock.enqueue(
                "getTransaction",
                Reply::result(authority_signed_mint(key, *kind)),
            );
        }
    }
}

/// Enqueue an empty channel history: no serviced mints. Used by the discovery
/// run (which only needs the rebuilt rows' coordinates) and by IT-R12.
fn script_channel_empty(mock: &MockRpcServer) {
    for _ in 0..RESYNC_ATTEMPTS {
        mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    }
}

/// Answer the startup supply invariant with "no channel mint exists yet".
///
/// An absent mint account reads as zero supply, which can never exceed custody, so the
/// invariant passes and the custody comparison stays the only thing under test. Startup
/// now refuses to boot on a supply it could not read, so leaving the read unscripted
/// would fail the boot rather than be ignored.
fn script_channel_empty_supply(mock: &MockRpcServer) {
    let reply = Reply::result(json!({"context": {"slot": 1}, "value": null}));
    mock.enqueue_sequence("getAccountInfo", std::iter::repeat_n(reply, 256));
}

/// Place the single serviced mint on page 2: page 1 is a full `CHANNEL_PAGE_LIMIT`
/// page of fillers so the `before` cursor advances, then page 2 carries the memo.
fn script_channel_consumed_on_page2(
    mock: &MockRpcServer,
    key: &RowKey,
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
            Reply::result(json!([channel_sig_entry(
                landed,
                &memo_for(&key.source_event_id(), kind)
            )])),
        );
        mock.enqueue(
            "getTransaction",
            Reply::result(authority_signed_mint(key, kind)),
        );
    }
}

// ── DB assertion helpers (fresh pool: drop_tables invalidates old caches) ────

/// Natural key of a rebuilt row, used to script the exact consumed-set memo the
/// reconcile will look up, plus the fields a matching channel mint must pay.
#[derive(Clone, Debug)]
struct RowKey {
    signature: String,
    instruction_index: i32,
    inner_index: Option<i32>,
    transaction_type: String,
    initiator: String,
    recipient: String,
    mint: String,
    amount: u64,
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
    let rows = sqlx::query(
        "SELECT signature, instruction_index, inner_index, \
         transaction_type::text AS transaction_type, initiator, recipient, mint, \
         amount::text AS amount \
         FROM transactions ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("query row keys");
    rows.iter()
        .map(|row| RowKey {
            signature: row.get("signature"),
            instruction_index: row.get("instruction_index"),
            inner_index: row.get("inner_index"),
            transaction_type: row.get("transaction_type"),
            initiator: row.get("initiator"),
            recipient: row.get("recipient"),
            mint: row.get("mint"),
            amount: row
                .get::<String, _>("amount")
                .parse()
                .expect("row amount fits u64"),
        })
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
    seed_tx(db_url, signature, "deposit", "pending").await;
}

/// Slot of every seeded row: above any slot a test validator reaches, so a seed never sits
/// below the genesis a test resyncs from.
const SEED_SLOT: i64 = 1_000_000_000;

/// Insert one row of `ty` in `status`; a withdrawal takes its nonce from the sequence.
async fn seed_tx(db_url: &str, signature: &str, ty: &str, status: &str) -> i64 {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, created_at, updated_at)
         VALUES ($1, $4, 'seed', 'seed', 'seed_mint', 100,
                 $2::transaction_type, $3::transaction_status, NOW(), NOW())
         RETURNING id",
    )
    .bind(signature)
    .bind(ty)
    .bind(status)
    .bind(SEED_SLOT)
    .fetch_one(&pool)
    .await
    .expect("seed transaction")
}

/// Journal one broadcast attempt for `tx_id` in `table`.
async fn seed_journal(db_url: &str, table: &str, tx_id: i64, signature: &str) {
    let pool = fresh_pool(db_url).await;
    sqlx::query(&format!(
        "INSERT INTO {table} (transaction_id, signature, last_valid_block_height) VALUES ($1, $2, 1)"
    ))
    .bind(tx_id)
    .bind(signature)
    .execute(&pool)
    .await
    .expect("seed journal");
}

async fn seed_sql(db_url: &str, sql: &str) {
    let pool = fresh_pool(db_url).await;
    sqlx::query(sql).execute(&pool).await.expect("seed sql");
}

/// Paid, reminted and in-flight withdrawals with journals, an observed release and a checkpoint.
async fn seed_withdrawal_side(db_url: &str) {
    seed_tx(db_url, "wd-paid", "withdrawal", "completed").await;
    seed_tx(db_url, "wd-reminted", "withdrawal", "failed_reminted").await;
    let live = seed_tx(db_url, "wd-live", "withdrawal", "processing").await;
    seed_journal(
        db_url,
        "pending_release_signatures",
        live,
        "wd-live-attempt",
    )
    .await;
    let remint = seed_tx(db_url, "wd-remint", "withdrawal", "pending_remint").await;
    seed_journal(
        db_url,
        "pending_remint_signatures",
        remint,
        "wd-remint-attempt",
    )
    .await;
    seed_sql(
        db_url,
        "INSERT INTO observed_releases (withdrawal_nonce, signature, slot) VALUES (0, 'obs-0', 5)",
    )
    .await;
    seed_sql(
        db_url,
        "INSERT INTO indexer_state (program_type, last_committed_slot) VALUES ('withdraw', 222)",
    )
    .await;
}

/// A deposit, a mints row and the escrow checkpoint.
async fn seed_escrow_side(db_url: &str) {
    seed_tx(db_url, "dep-seed", "deposit", "completed").await;
    seed_sql(db_url, "INSERT INTO mints (mint_address, decimals, token_program) VALUES ('seed_mint', 6, 'token')").await;
    seed_sql(
        db_url,
        "INSERT INTO indexer_state (program_type, last_committed_slot) VALUES ('escrow', 111)",
    )
    .await;
}

/// Everything a resync of the other program must leave alone, as comparable text.
async fn side_fingerprint(db_url: &str, ty: &str) -> Vec<String> {
    let pool = fresh_pool(db_url).await;
    let mut out: Vec<String> = sqlx::query_scalar(
        "SELECT t.signature || '|' || t.status::text || '|' || COALESCE(t.withdrawal_nonce::text, '-')
                || '|' || COALESCE((SELECT string_agg(signature, ',' ORDER BY signature)
                                    FROM pending_release_signatures WHERE transaction_id = t.id), '-')
                || '|' || COALESCE((SELECT string_agg(signature, ',' ORDER BY signature)
                                    FROM pending_remint_signatures WHERE transaction_id = t.id), '-')
         FROM transactions t WHERE t.transaction_type::text = $1 ORDER BY t.signature",
    )
    .bind(ty)
    .fetch_all(&pool)
    .await
    .expect("rows");
    let program = if ty == "deposit" {
        "escrow"
    } else {
        "withdraw"
    };
    let checkpoint: Option<i64> =
        sqlx::query_scalar("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
            .bind(program)
            .fetch_optional(&pool)
            .await
            .expect("checkpoint")
            .flatten();
    out.push(format!("checkpoint={checkpoint:?}"));
    if ty == "deposit" {
        let mints: Vec<String> = sqlx::query_scalar("SELECT mint_address FROM mints ORDER BY 1")
            .fetch_all(&pool)
            .await
            .expect("mints");
        out.push(format!("mints={mints:?}"));
    } else {
        let seq: i64 = sqlx::query_scalar("SELECT last_value FROM withdrawal_nonce_seq")
            .fetch_one(&pool)
            .await
            .expect("seq");
        let observed: Vec<i64> =
            sqlx::query_scalar("SELECT withdrawal_nonce FROM observed_releases ORDER BY 1")
                .fetch_all(&pool)
                .await
                .expect("observed");
        out.push(format!("seq={seq} observed={observed:?}"));
    }
    out
}

async fn marker(db_url: &str) -> Option<String> {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar("SELECT program_type FROM resync_state")
        .fetch_optional(&pool)
        .await
        .expect("marker read")
}

/// The active halt's reason, or `None` when no halt is set.
async fn active_halt(db_url: &str) -> Option<String> {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar("SELECT reason FROM reconciliation_halt WHERE id = TRUE AND halted")
        .fetch_optional(&pool)
        .await
        .expect("halt read")
}

async fn deposit_count(db_url: &str) -> i64 {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE transaction_type = 'deposit'")
        .fetch_one(&pool)
        .await
        .expect("count deposits")
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
) -> Result<Signature, Box<dyn std::error::Error>> {
    let ix = deposit_ix(user, instance, mint, amount);
    helpers::send_and_confirm_instructions(client, &[ix], user, &[user], "Deposit").await
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
            CHANNEL_AUTHORITY,
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

/// An escrow resync deletes its deposits and rebuilds them from a short backfill
/// (genesis_slot ~= current_slot), while every withdrawal, nonce, journal, observed
/// release and the withdraw checkpoint survive untouched, even with work in flight.
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

    // A durable checkpoint far below the genesis slot resync is about to use. Ordinary
    // startup refuses that combination; resync must not, because it drops the checkpoint
    // before resolving and rebuilds everything above the genesis slot from chain.
    {
        let pool = fresh_pool(&db_url).await;
        sqlx::query(
            "INSERT INTO indexer_state (program_type, last_committed_slot, updated_at)
             VALUES ('escrow', 1, NOW())
             ON CONFLICT (program_type) DO UPDATE SET last_committed_slot = 1",
        )
        .execute(&pool)
        .await?;
    }

    let current_slot = {
        let client = solana_client::rpc_client::RpcClient::new(rpc_url.clone());
        client.get_slot()?
    };

    seed_withdrawal_side(&db_url).await;
    let withdrawals = side_fingerprint(&db_url, "withdrawal").await;

    let service = make_resync_service(rpc_url.clone(), storage);
    run_resync(&service, current_slot)
        .await
        .expect("resync should succeed");

    assert_eq!(
        deposit_count(&db_url).await,
        0,
        "the seeded deposit must be deleted"
    );
    assert_eq!(
        side_fingerprint(&db_url, "withdrawal").await,
        withdrawals,
        "an escrow resync must not touch the withdrawal side"
    );
    assert_eq!(
        marker(&db_url).await,
        None,
        "a finished resync clears its marker"
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
    // Ahead of the tip but at the seed, so only the tip check can refuse it.
    let result = service.run(SEED_SLOT as u64).await;

    assert!(
        matches!(
            result,
            Err(IndexerError::DataSource(DataSourceError::InvalidConfig { .. }))
        ),
        "ResyncService::run with a future genesis_slot must refuse at the tip check, got: {result:?}"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("ahead"),
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
    script_channel_consumed(&mock, &[(&dep, ConsumedMintKind::Deposit, landed)]);
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
            (&deposits[0], ConsumedMintKind::Deposit, landed0),
            (&deposits[1], ConsumedMintKind::Deposit, landed1),
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
    script_channel_consumed_on_page2(&mock, &dep, ConsumedMintKind::Deposit, &landed);
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
    script_channel_consumed(&mock, &[(&deposits[0], ConsumedMintKind::Deposit, landed)]);
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

    // Run 1.
    script_channel_consumed(&mock, &[(&dep, ConsumedMintKind::Deposit, landed)]);
    h.run().await.expect("first reconciling resync");
    let count_1 = row_count(&db_url).await;
    let st_1 = status_of(&db_url, &dep).await;

    // Run 2 (same scripted consumed-set).
    script_channel_consumed(&mock, &[(&dep, ConsumedMintKind::Deposit, landed)]);
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
    script_channel_empty_supply(&mock);
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
            ..Default::default()
        },
        ProgramType::Escrow,
        &recon_storage,
        &validator.rpc_url(),
        // The channel is modeled by the mock (empty), where the supply invariant reads.
        Some(&mock.url()),
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
/// non-idempotency memo, (c) a wrong-prefix memo, (d) this deposit's memo on a user
/// tx that only names the authority as an account. The deposit stays pending.
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
    let foreign_mint_to = ScriptedMintTo {
        mint: Pubkey::new_unique(),
        recipient_ata: Pubkey::new_unique(),
        amount: DEPOSIT_AMOUNT,
    };
    let attacker = Pubkey::new_unique();
    let forged_memo = mint_idempotency_memo(&dep.source_event_id());
    // (b) non-idempotency memo, (c) wrong-prefix memo, (d) forged memo. Enqueue one
    // page per possible resync attempt (transient-block retries re-enumerate the channel).
    for _ in 0..RESYNC_ATTEMPTS {
        let page = json!([
            channel_sig_entry(&Signature::new_unique(), &foreign_memo),
            channel_sig_entry(&Signature::new_unique(), "just a normal user memo"),
            channel_sig_entry(
                &Signature::new_unique(),
                &format!("private_channel:not-idempotency:{}", foreign_id.as_str())
            ),
            channel_sig_entry(&Signature::new_unique(), &forged_memo),
        ]);
        mock.enqueue("getSignaturesForAddress", Reply::result(page));
        mock.enqueue(
            "getTransaction",
            Reply::result(channel_transaction(
                &CHANNEL_AUTHORITY,
                &Pubkey::new_unique(),
                &foreign_memo,
                Some(&foreign_mint_to),
            )),
        );
        mock.enqueue(
            "getTransaction",
            Reply::result(channel_transaction(
                &attacker,
                &CHANNEL_AUTHORITY,
                &forged_memo,
                None,
            )),
        );
    }
    h.run().await.expect("resync should succeed");

    let st = status_of(&db_url, &dep).await;
    assert_eq!(
        st.status, "pending",
        "no foreign-event/non-idempotency/unsigned memo may mark this deposit completed"
    );
    assert!(st.counterpart_signature.is_none());

    mock.shutdown().await;
    Ok(())
}

/// IT-R13: an authority-signed mint carrying this deposit's memo but paying a
/// different amount contradicts the source event. The pre-drop validation pass
/// aborts the resync and the live database is left exactly as it was.
#[tokio::test(flavor = "multi_thread")]
async fn resync_aborts_on_signed_mint_that_does_not_pay_the_deposit_db_intact(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_mismatched_mint").await?;

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
    let memo = mint_idempotency_memo(&dep.source_event_id());
    let overpaying_mint_to = ScriptedMintTo {
        amount: dep.amount + 1,
        ..landed_mint_to(&dep, ConsumedMintKind::Deposit)
    };
    for _ in 0..RESYNC_ATTEMPTS {
        mock.enqueue(
            "getSignaturesForAddress",
            Reply::result(json!([channel_sig_entry(&Signature::new_unique(), &memo)])),
        );
        mock.enqueue(
            "getTransaction",
            Reply::result(channel_transaction(
                &CHANNEL_AUTHORITY,
                &Pubkey::new_unique(),
                &memo,
                Some(&overpaying_mint_to),
            )),
        );
    }
    // Only the live table holds this row; any drop and rebuild would erase it.
    let sentinel_signature = "resync_mismatch_sentinel";
    seed_pending_deposit(&db_url, sentinel_signature).await;
    let rows_before = row_count(&db_url).await;

    let result = h.run().await;
    assert!(
        matches!(
            result,
            Err(IndexerError::Reconciliation(
                ReconciliationError::ConsumedMintMismatch { .. }
            ))
        ),
        "a signed mint that does not pay this deposit must abort the resync: {result:?}"
    );

    assert_eq!(
        row_count(&db_url).await,
        rows_before,
        "no row may be dropped"
    );
    let pool = fresh_pool(&db_url).await;
    let sentinel: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
            .bind(sentinel_signature)
            .fetch_one(&pool)
            .await?;
    assert_eq!(sentinel, 1, "the live table must never have been dropped");
    let st = status_of(&db_url, &dep).await;
    assert_eq!(
        st.status, "pending",
        "the deposit keeps its pre-resync state"
    );
    assert!(st.counterpart_signature.is_none());

    mock.shutdown().await;
    Ok(())
}

/// IT-R14: an authority-signed mint that pays this deposit exactly but carries the
/// remint marker for its id is contradictory evidence. Left `pending` it would be
/// minted again, so the pre-drop validation aborts and the live database is kept.
#[tokio::test(flavor = "multi_thread")]
async fn resync_aborts_on_remint_marker_for_a_deposit_db_intact(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("resync_kind_mismatch").await?;

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
    let remint_memo = remint_idempotency_memo(&dep.source_event_id());
    let deposit_mint_to = landed_mint_to(&dep, ConsumedMintKind::Deposit);
    for _ in 0..RESYNC_ATTEMPTS {
        mock.enqueue(
            "getSignaturesForAddress",
            Reply::result(json!([channel_sig_entry(
                &Signature::new_unique(),
                &remint_memo
            )])),
        );
        mock.enqueue(
            "getTransaction",
            Reply::result(channel_transaction(
                &CHANNEL_AUTHORITY,
                &Pubkey::new_unique(),
                &remint_memo,
                Some(&deposit_mint_to),
            )),
        );
    }

    // Only the live table holds this row; any drop and rebuild would erase it.
    let sentinel_signature = "resync_kind_mismatch_sentinel";
    seed_pending_deposit(&db_url, sentinel_signature).await;
    let rows_before = row_count(&db_url).await;

    let result = h.run().await;
    assert!(
        matches!(
            result,
            Err(IndexerError::Reconciliation(
                ReconciliationError::ConsumedMintMismatch { .. }
            ))
        ),
        "a remint marker naming a deposit must abort the resync: {result:?}"
    );

    assert_eq!(
        row_count(&db_url).await,
        rows_before,
        "no row may be dropped"
    );
    let pool = fresh_pool(&db_url).await;
    let sentinel: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
            .bind(sentinel_signature)
            .fetch_one(&pool)
            .await?;
    assert_eq!(sentinel, 1, "the live table must never have been dropped");
    assert_eq!(status_of(&db_url, &dep).await.status, "pending");

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
    // Withdraw program: the instance only names the bitmap the pre-flight reads, which
    // is fresh here because no release ran. The source-event-id is instance-independent.
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: validator.rpc_url(),
        program_type: ProgramType::Withdraw,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };

    let keys = h.discover().await;
    let withdrawals = keys_of_type(&keys, "withdrawal");
    assert_eq!(withdrawals.len(), 1, "exactly one withdrawal row expected");
    let wd = withdrawals[0].clone();

    let landed = Signature::new_unique();
    // Escrow data a withdraw resync cannot rebuild, so it must come through untouched.
    seed_escrow_side(&db_url).await;
    let deposits = side_fingerprint(&db_url, "deposit").await;
    script_channel_consumed(&mock, &[(&wd, ConsumedMintKind::Remint, landed)]);
    h.run().await.expect("resync should succeed");
    assert_eq!(
        side_fingerprint(&db_url, "deposit").await,
        deposits,
        "a withdraw resync must not touch the escrow side"
    );

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
        instance: Some(env.instance),
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
    let legacy_memo = "private_channel:mint-idempotency:42";
    let legacy = json!([channel_sig_entry(&Signature::new_unique(), legacy_memo)]);
    mock.enqueue("getSignaturesForAddress", Reply::result(legacy));
    mock.enqueue(
        "getTransaction",
        Reply::result(channel_transaction(
            &CHANNEL_AUTHORITY,
            &Pubkey::new_unique(),
            legacy_memo,
            None,
        )),
    );
    let service = make_channel_resync_service(
        validator.rpc_url(),
        storage,
        ProgramType::Escrow,
        Some(Pubkey::new_unique()),
        mock.url(),
        CHANNEL_AUTHORITY,
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

// ════════════════════════════════════════════════════════════════════════════
// Live-state lock and halt refusal
// ════════════════════════════════════════════════════════════════════════════

/// I6. The finding itself: a resync must not destroy a database that live workers
/// are still using. A worker's shared lock stands in for the running process, and
/// the refusal has to land before the drop, not after it.
#[tokio::test(flavor = "multi_thread")]
async fn resync_refuses_while_a_worker_holds_the_live_lock(
) -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();
    let (db_url, storage, _container) = start_postgres_for_resync("resync_live_lock_test").await?;

    seed_pending_deposit(&db_url, "resync_live_lock_seed").await;

    // A separate pool, standing in for a live indexer or operator process.
    let worker_storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 5,
        })
        .await?,
    ));
    let worker_lock = worker_storage
        .try_acquire_live_lock(
            LiveLockMode::Shared,
            "i6_worker",
            CancellationToken::new(),
            Duration::ZERO,
        )
        .await?;

    let service = make_resync_service(rpc_url.clone(), storage);
    let refused = service.run(0).await;
    assert!(
        matches!(
            refused,
            Err(IndexerError::Storage(StorageError::LiveStateLockHeld {
                requested: LiveLockMode::Exclusive
            }))
        ),
        "resync must refuse while a worker holds the lock, got: {refused:?}"
    );
    assert_eq!(
        row_count(&db_url).await,
        1,
        "the refusal must leave the live database untouched"
    );

    // With the worker stopped the same resync goes through, so the guard blocks the
    // dangerous case without blocking the supported one.
    worker_lock.stop_and_wait().await;
    let storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 5,
        })
        .await?,
    ));
    let current_slot = {
        let client = solana_client::rpc_client::RpcClient::new(rpc_url.clone());
        client.get_slot()?
    };
    let service = make_resync_service(rpc_url, storage);
    run_resync(&service, current_slot)
        .await
        .expect("resync must succeed once no worker holds the lock");
    assert_eq!(
        row_count(&db_url).await,
        0,
        "the rebuild must have replaced the seeded row"
    );
    Ok(())
}

/// I7. A reconciliation halt is a solvency interlock living in a table the rebuild
/// drops. Refuse rather than clear it silently, and leave the evidence in place.
#[tokio::test(flavor = "multi_thread")]
async fn resync_refuses_when_reconciliation_halt_is_set() -> Result<(), Box<dyn std::error::Error>>
{
    let (test_validator, _faucet) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();
    let (db_url, storage, _container) = start_postgres_for_resync("resync_halt_test").await?;

    seed_pending_deposit(&db_url, "resync_halt_seed").await;
    storage
        .set_reconciliation_halt("supply above custody on mint X")
        .await?;

    let service = make_resync_service(rpc_url, storage.clone());
    let refused = service.run(0).await;
    assert!(
        matches!(
            refused,
            Err(IndexerError::Reconciliation(
                ReconciliationError::ReconciliationHalted { .. }
            ))
        ),
        "a halted database must refuse to resync, got: {refused:?}"
    );
    assert_eq!(
        row_count(&db_url).await,
        1,
        "the refusal must leave the live database untouched"
    );
    assert!(
        storage.is_reconciliation_halted().await?.is_some(),
        "the halt flag itself must survive the refusal"
    );
    Ok(())
}

/// I8. Resync also has to work on a database that has never been indexed, which is
/// the case the halt read would otherwise fail on with a missing table.
#[tokio::test(flavor = "multi_thread")]
async fn resync_initializes_schema_on_a_fresh_database() -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();
    let (db_url, storage, _container) = start_bare_postgres_for_resync("resync_fresh_db").await?;

    let current_slot = {
        let client = solana_client::rpc_client::RpcClient::new(rpc_url.clone());
        client.get_slot()?
    };
    let service = make_resync_service(rpc_url, storage);
    run_resync(&service, current_slot)
        .await
        .expect("resync must build its own schema on an empty database");

    assert_eq!(
        row_count(&db_url).await,
        0,
        "a rebuild on an empty database must leave a usable, empty schema"
    );
    Ok(())
}

/// Is the pre-resync seed still there? False once the rebuild has dropped and
/// recreated the table, and also while the table is briefly missing, which is the
/// moment this is polled across.
async fn seeded_rows_remain(pool: &sqlx::PgPool) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM transactions")
        .fetch_one(pool)
        .await
        .is_ok_and(|n| n > 0)
}

/// Kill the backend holding the live-state key, standing in for a failover or an
/// idle-session reap pulling the lock out from under a running rebuild.
async fn terminate_live_lock_holder(pool: &sqlx::PgPool) {
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_locks
         WHERE locktype = 'advisory' AND objsubid = 1 AND granted
           AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(LIVE_STATE_LOCK_KEY)
    .execute(pool)
    .await
    .expect("terminate the live-state lock holder");
}

/// I12. Losing the lock mid-rebuild means a worker can start against a database
/// that is only half rebuilt. The rebuild must stop rather than race it, and stop
/// both writers outright: letting the checkpoint writer flush would commit a
/// durable frontier over a half-rebuilt database and leave no gap to detect later.
///
/// The kill is gated on the drop having already happened, so this can only be the
/// mid-rebuild arm. The synchronous check that guards the drop runs strictly
/// earlier, and nothing after it re-checks, so no other path returns this error.
#[tokio::test(flavor = "multi_thread")]
async fn resync_aborts_when_the_live_lock_is_lost_mid_rebuild(
) -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();
    let (db_url, storage, _container) = start_postgres_for_resync("resync_lock_lost_mid").await?;

    seed_pending_deposit(&db_url, "resync_lock_lost_seed").await;

    // A fresh validator has only a few slots, and the wipe no longer rebuilds the schema,
    // so wait for enough finalized slots that the fill outlasts a few heartbeats.
    wait_for_finalized_slot(&rpc_url, MIN_FILL_SLOTS).await;

    let current_slot = {
        let client = solana_client::rpc_client::RpcClient::new(rpc_url.clone());
        client.get_slot()?
    };
    // Recent slots only, so every block is retrievable, and one round trip per slot
    // so the fill is long enough to still be running when the lock goes.
    let genesis_slot = current_slot.saturating_sub(BACKFILL_SPAN_SLOTS);

    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_url.clone(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
    ));
    let service = ResyncService::new(
        storage,
        rpc_poller,
        ProgramType::Escrow,
        BackfillConfig {
            enabled: true,
            exit_after_backfill: true,
            rpc_url: rpc_url.clone(),
            batch_size: 1,
            max_gap_slots: u64::MAX,
            start_slot: None,
        },
        Some(Pubkey::new_unique()),
    )
    .with_stale_holder_grace(Duration::ZERO)
    .with_lock_heartbeat_interval(Duration::from_millis(5));

    // Pull the lock the moment the seeded row is gone, which is the wipe landing. One
    // pool for the whole task, since opening a fresh one per poll would cost more than
    // the window being aimed at.
    let killer_url = db_url.clone();
    let killer = tokio::spawn(async move {
        let pool = fresh_pool(&killer_url).await;
        // Bounded, so a resync that failed before the wipe fails this test instead of
        // hanging it: nothing would ever clear the seed.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while seeded_rows_remain(&pool).await {
            if std::time::Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        terminate_live_lock_holder(&pool).await;
        true
    });

    let result = service.run(genesis_slot).await;
    assert!(
        killer.await.expect("killer task"),
        "the rebuild never reached the drop, so the lock was never pulled mid-rebuild"
    );

    assert!(
        matches!(
            result,
            Err(IndexerError::Storage(StorageError::LiveStateLockLost))
        ),
        "a lock lost mid-rebuild must abort the rebuild, got: {result:?}"
    );

    // The frontier is what makes a half-rebuilt database look complete, so it is the one
    // thing that must never survive a lost lock. Every phase of the rebuild is watched
    // for exactly this reason, including the flush that runs after the fill returns.
    let pool = fresh_pool(&db_url).await;
    let committed: Option<i64> = sqlx::query_scalar(
        "SELECT last_committed_slot FROM indexer_state WHERE program_type = 'escrow'",
    )
    .fetch_optional(&pool)
    .await
    .ok()
    .flatten();
    assert!(
        committed.is_none_or(|slot| slot <= genesis_slot as i64),
        "a lost lock must not leave a durable frontier over a half-rebuilt database, got {committed:?}"
    );

    // The lock is gone, so the marker (new workers) and the halt (older operators) keep them off.
    assert_eq!(marker(&db_url).await.as_deref(), Some("escrow"));
    assert_eq!(
        active_halt(&db_url).await,
        Some(resync_halt_reason(ProgramType::Escrow))
    );
    assert!(matches!(
        new_storage(&db_url)
            .await
            .ensure_no_unfinished_resync()
            .await,
        Err(StorageError::UnfinishedResync { .. })
    ));

    // A rerun of the same program finishes the rebuild and clears it.
    let current_slot = {
        let client = solana_client::rpc_client::RpcClient::new(rpc_url.clone());
        client.get_slot()?
    };
    let rerun = make_resync_service(rpc_url, new_storage(&db_url).await);
    run_resync(&rerun, current_slot)
        .await
        .expect("a same-program rerun must finish the interrupted resync");
    assert_eq!(marker(&db_url).await, None);
    assert_eq!(
        active_halt(&db_url).await,
        None,
        "the rerun clears its own halt"
    );
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// Pre-wipe gates: refusals that must land before any RPC and any delete
// ════════════════════════════════════════════════════════════════════════════

/// Every RPC points at a dead port, so a refusal that is not the gate's surfaces as an RPC error.
fn dead_rpc_service(storage: Arc<Storage>, program: ProgramType) -> ResyncService {
    const DEAD: &str = "http://127.0.0.1:1";
    let rpc_poller = Arc::new(RpcPoller::new(
        DEAD.to_string(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
    ));
    let backfill_config = BackfillConfig {
        enabled: true,
        exit_after_backfill: true,
        rpc_url: DEAD.to_string(),
        batch_size: 50,
        max_gap_slots: u64::MAX,
        start_slot: None,
    };
    ResyncService::new(
        storage,
        rpc_poller,
        program,
        backfill_config,
        Some(Pubkey::new_unique()),
    )
    .with_stale_holder_grace(Duration::ZERO)
    .with_withdrawal_bitmap_rpc(DEAD.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Refusal {
    Unsettled,
    ReleaseEvidence,
    OtherMarker,
    GenesisAboveRows,
}

/// The unsafe state a refusal case starts from, so each case carries its own setup.
#[derive(Debug, Clone, Copy)]
enum Seed {
    /// One row of this type in this status.
    Tx(&'static str, &'static str),
    /// One row of this type in this status, with a broadcast attempt journaled for it.
    JournaledTx(&'static str, &'static str),
    /// An observed release and no withdrawal row.
    Observed,
    /// Escrow data plus another program's unfinished resync.
    OtherMarker,
    /// One row of this type in this status at this slot.
    TxAt(&'static str, &'static str, i64),
}

struct RefusalCase {
    name: &'static str,
    program: ProgramType,
    seed: Seed,
    genesis: u64,
    expected: Refusal,
}

async fn seed_refusal(db_url: &str, seed: Seed) {
    match seed {
        Seed::Tx(ty, status) => {
            seed_tx(db_url, "row", ty, status).await;
        }
        Seed::JournaledTx(ty, status) => {
            let id = seed_tx(db_url, "row", ty, status).await;
            seed_journal(db_url, "pending_release_signatures", id, "row-attempt").await;
        }
        Seed::Observed => {
            seed_sql(db_url, "INSERT INTO observed_releases (withdrawal_nonce, signature, slot) VALUES (4, 'o', 1)").await;
        }
        Seed::OtherMarker => {
            seed_escrow_side(db_url).await;
            seed_sql(
                db_url,
                "INSERT INTO resync_state (program_type) VALUES ('withdraw')",
            )
            .await;
        }
        Seed::TxAt(ty, status, slot) => {
            let id = seed_tx(db_url, "row", ty, status).await;
            seed_sql(
                db_url,
                &format!("UPDATE transactions SET slot = {slot} WHERE id = {id}"),
            )
            .await;
        }
    }
}

/// IT-G1: each unsafe state refuses before any RPC, and leaves both programs' data and
/// the marker exactly as they were.
#[tokio::test(flavor = "multi_thread")]
async fn resync_refuses_unsafe_database_before_any_rpc() -> Result<(), Box<dyn std::error::Error>> {
    let (db_url, _storage, _container) = start_postgres_for_resync("resync_gates").await?;
    let cases = [
        RefusalCase {
            name: "withdraw: processing withdrawal",
            program: ProgramType::Withdraw,
            seed: Seed::Tx("withdrawal", "processing"),
            genesis: 0,
            expected: Refusal::Unsettled,
        },
        RefusalCase {
            name: "withdraw: journaled pending withdrawal",
            program: ProgramType::Withdraw,
            seed: Seed::JournaledTx("withdrawal", "pending"),
            genesis: 0,
            expected: Refusal::Unsettled,
        },
        RefusalCase {
            name: "withdraw: pending remint",
            program: ProgramType::Withdraw,
            seed: Seed::Tx("withdrawal", "pending_remint"),
            genesis: 0,
            expected: Refusal::Unsettled,
        },
        RefusalCase {
            name: "escrow: processing deposit",
            program: ProgramType::Escrow,
            seed: Seed::Tx("deposit", "processing"),
            genesis: 0,
            expected: Refusal::Unsettled,
        },
        RefusalCase {
            name: "escrow: journaled pending deposit",
            program: ProgramType::Escrow,
            seed: Seed::JournaledTx("deposit", "pending"),
            genesis: 0,
            expected: Refusal::Unsettled,
        },
        RefusalCase {
            name: "withdraw: failed withdrawal",
            program: ProgramType::Withdraw,
            seed: Seed::Tx("withdrawal", "failed"),
            genesis: 0,
            expected: Refusal::ReleaseEvidence,
        },
        RefusalCase {
            name: "withdraw: observed release",
            program: ProgramType::Withdraw,
            seed: Seed::Observed,
            genesis: 0,
            expected: Refusal::ReleaseEvidence,
        },
        RefusalCase {
            name: "escrow: withdraw resync unfinished",
            program: ProgramType::Escrow,
            seed: Seed::OtherMarker,
            genesis: 0,
            expected: Refusal::OtherMarker,
        },
        RefusalCase {
            name: "escrow: pending deposit below genesis",
            program: ProgramType::Escrow,
            seed: Seed::TxAt("deposit", "pending", 100),
            genesis: 101,
            expected: Refusal::GenesisAboveRows,
        },
        RefusalCase {
            name: "escrow: completed deposit below genesis",
            program: ProgramType::Escrow,
            seed: Seed::TxAt("deposit", "completed", 100),
            genesis: 101,
            expected: Refusal::GenesisAboveRows,
        },
        RefusalCase {
            name: "withdraw: pending withdrawal below genesis",
            program: ProgramType::Withdraw,
            seed: Seed::TxAt("withdrawal", "pending", 100),
            genesis: 101,
            expected: Refusal::GenesisAboveRows,
        },
    ];
    for RefusalCase {
        name: case,
        program,
        seed,
        genesis,
        expected,
    } in cases
    {
        seed_sql(
            &db_url,
            "TRUNCATE transactions, observed_releases, resync_state, indexer_state, mints CASCADE",
        )
        .await;
        seed_refusal(&db_url, seed).await;
        let deposits = side_fingerprint(&db_url, "deposit").await;
        let withdrawals = side_fingerprint(&db_url, "withdrawal").await;
        let before = marker(&db_url).await;

        let result = dead_rpc_service(new_storage(&db_url).await, program)
            .run(genesis)
            .await;
        let got = match &result {
            Err(IndexerError::Reconciliation(ReconciliationError::UnsettledWork)) => {
                Refusal::Unsettled
            }
            Err(IndexerError::Reconciliation(ReconciliationError::ReleaseEvidenceRecorded {
                ..
            })) => Refusal::ReleaseEvidence,
            Err(IndexerError::Storage(StorageError::UnfinishedResync { program }))
                if program == "withdraw" =>
            {
                Refusal::OtherMarker
            }
            Err(IndexerError::Reconciliation(ReconciliationError::GenesisAboveExistingRows {
                genesis_slot: 101,
                earliest_slot: 100,
            })) => Refusal::GenesisAboveRows,
            other => panic!("{case}: expected {expected:?}, got {other:?}"),
        };
        assert_eq!(got, expected, "{case}");
        assert_eq!(
            side_fingerprint(&db_url, "deposit").await,
            deposits,
            "{case}"
        );
        assert_eq!(
            side_fingerprint(&db_url, "withdrawal").await,
            withdrawals,
            "{case}"
        );
        assert_eq!(marker(&db_url).await, before, "{case}");
    }
    Ok(())
}

// ════════════════════════════════════════════════════════════════════════════
// End to end: real indexers and operators around a resync, on one validator
// ════════════════════════════════════════════════════════════════════════════

/// A worker on its own runtime, so stopping it ends every task it spawned, like a process exit.
struct Worker {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    /// Returns once `start` has finished, so workers start one at a time.
    async fn spawn<F, Fut>(start: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (ready, started) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("worker runtime");
            runtime.block_on(async move {
                start().await;
                let _ = ready.send(());
                let _ = stopped.await;
            });
            runtime.shutdown_timeout(Duration::from_secs(5));
        });
        let _ = started.await;
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .expect("join the worker thread")
                .expect("a worker failed to start");
        }
    }
}

/// The helper default gives up confirming after five 400ms polls, but this validator confirms a
/// mint in several seconds under four workers, which would leave every mint to the 5-minute recovery.
fn e2e_operator_config() -> OperatorConfig {
    OperatorConfig {
        confirmation_poll_interval_ms: 4_000,
        ..default_operator_config()
    }
}

fn admin() -> Keypair {
    Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).expect("admin keypair")
}

/// Both indexers and both operators, wired to one validator as the single-node harness runs them.
struct Stack(Vec<Worker>);

impl Stack {
    /// Started one at a time: concurrent schema creation races in Postgres ("tuple concurrently
    /// updated"), which is a startup artefact of this harness, not of the resync.
    async fn start(rpc_url: &str, db_url: &str, instance: Pubkey) -> Self {
        let (rpc, db) = (rpc_url.to_string(), db_url.to_string());
        let mut workers = Vec::new();
        {
            let (rpc, db) = (rpc.clone(), db.clone());
            workers.push(
                Worker::spawn(move || async move {
                    start_solana_indexer_rpc_polling(rpc, db, Some(instance))
                        .await
                        .expect("start the escrow indexer");
                })
                .await,
            );
        }
        {
            let (rpc, db) = (rpc.clone(), db.clone());
            workers.push(
                Worker::spawn(move || async move {
                    start_private_channel_indexer(None, rpc, db)
                        .await
                        .expect("start the withdraw indexer");
                })
                .await,
            );
        }
        {
            let (rpc, db) = (rpc.clone(), db.clone());
            workers.push(
                Worker::spawn(move || async move {
                    start_solana_to_private_channel_operator_with_config(
                        rpc,
                        db,
                        admin(),
                        instance,
                        e2e_operator_config(),
                    )
                    .await
                    .expect("start the escrow operator");
                    // The helper returns before the operator creates its schema.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                })
                .await,
            );
        }
        workers.push(
            Worker::spawn(move || async move {
                start_private_channel_to_solana_operator_with_config(
                    rpc.clone(),
                    rpc,
                    db,
                    admin(),
                    instance,
                    e2e_operator_config(),
                )
                .await
                .expect("start the withdraw operator");
                tokio::time::sleep(Duration::from_millis(500)).await;
            })
            .await,
        );
        Self(workers)
    }

    async fn stop(self) {
        for worker in self.0 {
            worker.stop().await;
        }
    }
}

/// An escrow resync reconciled against the real channel, retried while stopped workers' sessions close.
async fn escrow_resync(
    rpc_url: &str,
    db_url: &str,
    instance: Pubkey,
    genesis: u64,
) -> Result<(), IndexerError> {
    for _ in 0..40 {
        let service = make_channel_resync_service(
            rpc_url.to_string(),
            new_storage(db_url).await,
            ProgramType::Escrow,
            Some(instance),
            rpc_url.to_string(),
            admin().pubkey(),
        );
        match run_resync(&service, genesis).await {
            Err(IndexerError::Storage(StorageError::LiveStateLockHeld { .. })) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            other => return other,
        }
    }
    panic!("stopped workers never released the live-state lock");
}

async fn checkpoint_of(db_url: &str, program: &str) -> Option<i64> {
    let pool = fresh_pool(db_url).await;
    sqlx::query_scalar("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
        .bind(program)
        .fetch_optional(&pool)
        .await
        .expect("checkpoint read")
        .flatten()
}

/// Each deposit's status and mint signature: what a rebuild must reproduce exactly.
async fn deposit_outcomes(db_url: &str) -> Vec<(String, String, Option<String>)> {
    let pool = fresh_pool(db_url).await;
    sqlx::query_as(
        "SELECT signature, status::text, counterpart_signature FROM transactions
         WHERE transaction_type = 'deposit' ORDER BY signature",
    )
    .fetch_all(&pool)
    .await
    .expect("deposit read")
}

/// Status, nonce and counterpart of the row indexed from `signature`.
async fn row_of(db_url: &str, signature: &str) -> (String, Option<i64>, Option<String>) {
    let pool = fresh_pool(db_url).await;
    sqlx::query_as(
        "SELECT status::text, withdrawal_nonce, counterpart_signature
         FROM transactions WHERE signature = $1",
    )
    .bind(signature)
    .fetch_one(&pool)
    .await
    .expect("row read")
}

async fn wait_for_status(db_url: &str, signature: &str, status: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(*WAIT_TIMEOUT_SECS);
    let pool = fresh_pool(db_url).await;
    loop {
        let current: Option<String> =
            sqlx::query_scalar("SELECT status::text FROM transactions WHERE signature = $1")
                .bind(signature)
                .fetch_optional(&pool)
                .await
                .expect("status read");
        if current.as_deref() == Some(status) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{signature} never reached {status} (last {current:?})"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_any_status(db_url: &str, signature: &str, statuses: &[&str]) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(*WAIT_TIMEOUT_SECS);
    let pool = fresh_pool(db_url).await;
    loop {
        let current: Option<String> =
            sqlx::query_scalar("SELECT status::text FROM transactions WHERE signature = $1")
                .bind(signature)
                .fetch_optional(&pool)
                .await
                .expect("status read");
        if let Some(status) = current.as_deref().filter(|s| statuses.contains(s)) {
            return status.to_string();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{signature} never reached any of {statuses:?} (last {current:?})"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Remint a failed withdrawal the way the operator does: a memo'd mint back to the burner,
/// recorded on the row as `failed_reminted` with the landed signature.
async fn remint_as_operator(
    client: &RpcClient,
    db_url: &str,
    signature: &str,
    burner: Pubkey,
    mint: Pubkey,
) {
    let pool = fresh_pool(db_url).await;
    let (ix, inner, amount): (i32, Option<i32>, i64) = sqlx::query_as(
        "SELECT instruction_index, inner_index, amount::bigint FROM transactions WHERE signature = $1",
    )
    .bind(signature)
    .fetch_one(&pool)
    .await
    .expect("withdrawal row");
    let memo = remint_idempotency_memo(&SourceEventId::new(signature, ix, inner));
    let (landed, _) = send_memo_mint(client, &memo, burner, mint, amount as u64).await;
    sqlx::query(
        "UPDATE transactions SET status = 'failed_reminted'::transaction_status,
                landed_remint_signature = $2 WHERE signature = $1",
    )
    .bind(signature)
    .bind(landed.to_string())
    .execute(&pool)
    .await
    .expect("record the remint");
}

/// An admin mint to `owner` carrying an idempotency `memo`, as the operator sends it. Returns
/// the signature and the blockhash's last valid block height, which the operator journals.
async fn send_memo_mint(
    client: &RpcClient,
    memo: &str,
    owner: Pubkey,
    mint: Pubkey,
    amount: u64,
) -> (Signature, u64) {
    let admin = admin();
    let ata = get_associated_token_address_with_program_id(&owner, &mint, &TOKEN_PROGRAM_ID);
    let (blockhash, last_valid) = client
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
        .await
        .expect("blockhash");
    let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[
            Instruction {
                program_id: spl_memo::id(),
                accounts: vec![solana_sdk::instruction::AccountMeta::new_readonly(
                    admin.pubkey(),
                    true,
                )],
                data: memo.as_bytes().to_vec(),
            },
            spl_token::instruction::mint_to(
                &TOKEN_PROGRAM_ID,
                &mint,
                &ata,
                &admin.pubkey(),
                &[],
                amount,
            )
            .expect("mint_to"),
        ],
        Some(&admin.pubkey()),
        &[&admin],
        blockhash,
    );
    let signature = client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("memo mint");
    (signature, last_valid)
}

/// The nonces the escrow's withdrawal bitmap records as released.
async fn consumed_nonces(client: &RpcClient, instance: Pubkey) -> Vec<u64> {
    let pda = private_channel_indexer::operator::find_withdrawal_bitmap_pda(&instance);
    private_channel_indexer::operator::parse_withdrawal_bitmap(
        &client.get_account_data(&pda).await.expect("bitmap account"),
    )
    .expect("bitmap parse")
    .consumed
}

async fn token_balance(client: &RpcClient, owner: Pubkey, mint: Pubkey) -> u64 {
    helpers::get_token_balance(client, &owner, &mint)
        .await
        .unwrap_or(0)
}

async fn custody(client: &RpcClient, instance: Pubkey, mint: Pubkey) -> u64 {
    let ata = get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_PROGRAM_ID);
    client
        .get_token_account_balance(&ata)
        .await
        .expect("custody balance")
        .amount
        .parse()
        .expect("custody amount")
}

async fn supply(client: &RpcClient, mint: Pubkey) -> u64 {
    client
        .get_token_supply(&mint)
        .await
        .expect("mint supply")
        .amount
        .parse()
        .expect("supply amount")
}

/// Let the restarted stack index up to now and give the operators a few polls to act.
async fn let_stack_settle(db_url: &str, client: &RpcClient) {
    let tip = client.get_slot().await.expect("slot");
    let pool = fresh_pool(db_url).await;
    for program in ["escrow", "withdraw"] {
        assert!(
            helpers::db::wait_for_checkpoint(&pool, program, tip, *WAIT_TIMEOUT_SECS)
                .await
                .expect("checkpoint wait"),
            "the {program} indexer never caught up to slot {tip}"
        );
    }
    tokio::time::sleep(Duration::from_secs(15)).await;
}

/// E2E-1 (#9, 26): a reminted withdrawal whose destination account appears later must not be
/// released by an escrow resync and restart. Custody, destination, row and bitmap bit all hold.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_escrow_resync_does_not_release_a_reminted_withdrawal(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("e2e_reminted").await?;
    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    TestEnvironment::setup_operator(&client, &faucet, env.instance).await?;
    let user = &env.users[0];

    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    let deposit = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT).await?;
    wait_for_status(&db_url, &deposit.to_string(), "completed").await;

    // The release fails because the destination has no token account, so the burn is reminted.
    let destination = Keypair::new().pubkey();
    let withdrawal =
        helpers::execute_user_withdrawal_to(&client, user, env.mint, WITHDRAW_AMOUNT, destination)
            .await?;
    let settled = wait_for_any_status(
        &db_url,
        &withdrawal.signature,
        &["pending_remint", "manual_review", "failed_reminted"],
    )
    .await;
    // The operator proves non-release before reminting, and on this validator finality and
    // indexing lag can outrun its three 32s tries. If it did not finish, remint as it would.
    if settled != "failed_reminted" {
        stack.stop().await;
        remint_as_operator(
            &client,
            &db_url,
            &withdrawal.signature,
            user.pubkey(),
            env.mint,
        )
        .await;
    } else {
        stack.stop().await;
    }
    assert_eq!(
        row_of(&db_url, &withdrawal.signature).await.0,
        "failed_reminted"
    );
    let (_, nonce, _) = row_of(&db_url, &withdrawal.signature).await;
    let nonce = nonce.expect("a withdrawal carries a nonce") as u64;

    // The attacker now creates the destination account, so a second release would land.
    let attacker_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &user.pubkey(),
            &destination,
            &env.mint,
            &TOKEN_PROGRAM_ID,
        );
    helpers::send_and_confirm_instructions(&client, &[attacker_ata_ix], user, &[user], "ATA")
        .await?;
    let custody_before = custody(&client, env.instance, env.mint).await;
    let user_before = token_balance(&client, user.pubkey(), env.mint).await;

    escrow_resync(&rpc_url, &db_url, env.instance, genesis)
        .await
        .expect("the escrow resync must succeed on a live database");
    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    let_stack_settle(&db_url, &client).await;
    stack.stop().await;

    assert_eq!(
        token_balance(&client, destination, env.mint).await,
        0,
        "nothing may be released to the destination"
    );
    assert_eq!(
        custody(&client, env.instance, env.mint).await,
        custody_before,
        "escrow custody must not move"
    );
    assert_eq!(
        token_balance(&client, user.pubkey(), env.mint).await,
        user_before,
        "the user must not be reminted twice"
    );
    assert_eq!(
        row_of(&db_url, &withdrawal.signature).await.0,
        "failed_reminted"
    );
    assert_eq!(
        row_of(&db_url, &withdrawal.signature).await.1,
        Some(nonce as i64)
    );
    assert!(
        !consumed_nonces(&client, env.instance)
            .await
            .contains(&nonce),
        "the reminted withdrawal's bit must stay clear"
    );
    Ok(())
}

/// E2E-2 (26): an escrow resync on a busy system keeps every withdrawal and nonce, the withdraw
/// indexer resumes from its kept checkpoint, and nothing is minted or released twice.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_escrow_resync_on_a_busy_system() -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("e2e_busy").await?;
    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    // Strictly after the AllowMint, so a resync from here cannot recreate the mint row itself.
    let after_allow_mint = client.get_slot().await? + 1;
    TestEnvironment::setup_operator(&client, &faucet, env.instance).await?;
    let user = &env.users[0];

    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    let deposit_a = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT_A).await?;
    let deposit_b = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT_B).await?;
    wait_for_status(&db_url, &deposit_a.to_string(), "completed").await;
    wait_for_status(&db_url, &deposit_b.to_string(), "completed").await;
    let paid = helpers::execute_user_withdrawal(&client, user, env.mint, WITHDRAW_AMOUNT).await?;
    wait_for_status(&db_url, &paid.signature, "completed").await;
    stack.stop().await;

    let deposits = deposit_outcomes(&db_url).await;
    let withdrawals = side_fingerprint(&db_url, "withdrawal").await;
    let kept_checkpoint = checkpoint_of(&db_url, "withdraw").await;
    assert!(
        kept_checkpoint.is_some(),
        "the withdraw indexer had a checkpoint"
    );

    escrow_resync(&rpc_url, &db_url, env.instance, genesis)
        .await
        .expect("the escrow resync must succeed on a busy database");
    assert_eq!(side_fingerprint(&db_url, "withdrawal").await, withdrawals);
    assert_eq!(checkpoint_of(&db_url, "withdraw").await, kept_checkpoint);
    // Deposits come back completed with the same mint signatures, so none is minted again.
    assert_eq!(deposit_outcomes(&db_url).await, deposits);

    // A withdrawal lands while every worker is down.
    let supply_before = supply(&client, env.mint).await;
    let custody_before = custody(&client, env.instance, env.mint).await;
    let user_before = token_balance(&client, user.pubkey(), env.mint).await;
    let downtime =
        helpers::execute_user_withdrawal(&client, user, env.mint, WITHDRAW_AMOUNT + 1).await?;

    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    wait_for_status(&db_url, &downtime.signature, "completed").await;
    let_stack_settle(&db_url, &client).await;
    stack.stop().await;

    let withdrawal_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transactions WHERE transaction_type = 'withdrawal'",
    )
    .fetch_one(&fresh_pool(&db_url).await)
    .await?;
    assert_eq!(
        withdrawal_rows, 2,
        "the downtime withdrawal is indexed exactly once"
    );
    assert_eq!(row_of(&db_url, &paid.signature).await.1, Some(0));
    assert_eq!(row_of(&db_url, &downtime.signature).await.1, Some(1));
    assert_eq!(consumed_nonces(&client, env.instance).await, vec![0, 1]);
    assert_eq!(
        supply(&client, env.mint).await,
        supply_before - (WITHDRAW_AMOUNT + 1),
        "only the downtime burn changes supply: no mint and no remint"
    );
    assert_eq!(
        custody(&client, env.instance, env.mint).await,
        custody_before - (WITHDRAW_AMOUNT + 1),
        "only the downtime release leaves custody"
    );
    assert_eq!(
        token_balance(&client, user.pubkey(), env.mint).await,
        user_before,
        "burned and released once"
    );

    // Custody against the rebuilt ledger. The single-validator harness cannot model separate
    // channel supply, so the supply invariant reads an empty mock channel, as IT-R12 does.
    let mock = MockRpcServer::start().await;
    script_channel_empty_supply(&mock);
    let recon_storage = new_storage(&db_url).await;
    let recon = run_startup_reconciliation(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
            ..Default::default()
        },
        ProgramType::Escrow,
        &recon_storage,
        &rpc_url,
        Some(&mock.url()),
        &env.instance,
    )
    .await;
    assert!(recon.is_ok(), "startup reconciliation must pass: {recon:?}");

    // A resync from after the AllowMint keeps the mint row, so reconciliation still sees
    // the rebuilt deposits and the kept withdrawals for this mint.
    escrow_resync(&rpc_url, &db_url, env.instance, after_allow_mint)
        .await
        .expect("an escrow resync from after the AllowMint");
    assert_eq!(mint_row_count(&db_url, &env.mint.to_string()).await, 1);
    let balances = new_storage(&db_url)
        .await
        .get_mint_balances_for_reconciliation(i64::MAX as u64)
        .await?;
    let ours = balances
        .iter()
        .find(|b| b.mint_address == env.mint.to_string())
        .expect("the mint must stay in the reconciliation universe");
    assert_eq!(
        ours.total_deposits.to_string(),
        (DEPOSIT_AMOUNT_A + DEPOSIT_AMOUNT_B).to_string()
    );
    assert_eq!(
        ours.total_withdrawals.to_string(),
        (2 * WITHDRAW_AMOUNT + 1).to_string()
    );
    script_channel_empty_supply(&mock);
    let recon_storage = new_storage(&db_url).await;
    let recon = run_startup_reconciliation(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
            ..Default::default()
        },
        ProgramType::Escrow,
        &recon_storage,
        &rpc_url,
        Some(&mock.url()),
        &env.instance,
    )
    .await;
    assert!(
        recon.is_ok(),
        "reconciliation after the later-genesis resync: {recon:?}"
    );
    mock.shutdown().await;
    Ok(())
}

/// Escrow indexer and operator configs for calling the real `run` functions directly.
fn escrow_worker_configs(
    rpc_url: &str,
    db_url: &str,
    instance: Pubkey,
) -> (PrivateChannelIndexerConfig, IndexerConfig, OperatorConfig) {
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.to_string(),
        source_rpc_url: Some(rpc_url.to_string()),
        fallback_rpc_url: None,
        postgres: PostgresConfig {
            database_url: db_url.to_string(),
            max_connections: 5,
        },
        escrow_instance_id: Some(instance),
    };
    let indexer = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: Some(RpcPollingConfig {
            poll_interval_ms: 200,
            error_retry_interval_ms: 1_000,
            batch_size: 10,
            from_slot: Some(1),
            encoding: UiTransactionEncoding::Json,
            commitment: CommitmentLevel::Confirmed,
        }),
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: true,
            exit_after_backfill: false,
            rpc_url: rpc_url.to_string(),
            batch_size: 100,
            max_gap_slots: u64::MAX,
            start_slot: None,
        },
        reconciliation: ReconciliationConfig {
            mismatch_threshold_raw: u64::MAX,
            ..Default::default()
        },
    };
    let operator = OperatorConfig {
        db_poll_interval: Duration::from_millis(500),
        batch_size: 10,
        retry_max_attempts: 3,
        retry_base_delay: Duration::from_secs(1),
        channel_buffer_size: 100,
        rpc_commitment: CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(60 * 60),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: Some("http://127.0.0.1:0/recon-test".to_string()),
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: 400,
    };
    (common, indexer, operator)
}

/// E2E-3 (94): a resync killed mid-rebuild leaves the marker and halt; the real indexer and
/// operator refuse to start; a rerun finishes and clears both; the workers then run normally.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_interrupted_resync_blocks_workers_until_rerun(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("e2e_interrupted").await?;
    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    TestEnvironment::setup_operator(&client, &faucet, env.instance).await?;
    let user = &env.users[0];

    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    let deposit_a = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT_A).await?;
    let deposit_b = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT_B).await?;
    wait_for_status(&db_url, &deposit_a.to_string(), "completed").await;
    wait_for_status(&db_url, &deposit_b.to_string(), "completed").await;
    stack.stop().await;
    let deposits = deposit_outcomes(&db_url).await;
    let supply_before = supply(&client, env.mint).await;

    // One slot per round trip and a fast heartbeat, so the kill lands while the fill runs.
    wait_for_finalized_slot(&rpc_url, genesis + MIN_FILL_SLOTS).await;
    let service = ResyncService::new(
        new_storage(&db_url).await,
        Arc::new(RpcPoller::new(
            rpc_url.clone(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        )),
        ProgramType::Escrow,
        BackfillConfig {
            enabled: true,
            exit_after_backfill: true,
            rpc_url: rpc_url.clone(),
            batch_size: 1,
            max_gap_slots: u64::MAX,
            start_slot: None,
        },
        Some(env.instance),
    )
    .with_stale_holder_grace(Duration::ZERO)
    .with_channel_reconcile(ChannelReconcileConfig {
        channel_rpc_url: rpc_url.clone(),
        authority: admin().pubkey(),
    })
    .with_lock_heartbeat_interval(Duration::from_millis(5));
    let killer_url = db_url.clone();
    let killer = tokio::spawn(async move {
        let pool = fresh_pool(&killer_url).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            let marked: Option<String> =
                sqlx::query_scalar("SELECT program_type FROM resync_state")
                    .fetch_optional(&pool)
                    .await
                    .ok()
                    .flatten();
            if marked.is_some() {
                break;
            }
            if std::time::Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        terminate_live_lock_holder(&pool).await;
        true
    });
    let aborted = service.run(genesis).await;
    assert!(killer.await?, "the resync never reached the wipe");
    assert!(
        matches!(
            aborted,
            Err(IndexerError::Storage(StorageError::LiveStateLockLost))
        ),
        "a lock lost mid-rebuild must abort, got {aborted:?}"
    );
    assert_eq!(marker(&db_url).await.as_deref(), Some("escrow"));
    assert_eq!(
        active_halt(&db_url).await,
        Some(resync_halt_reason(ProgramType::Escrow))
    );

    // The real worker entry points refuse; a timeout means one started on the half-built rows.
    let (common, indexer_config, operator_config) =
        escrow_worker_configs(&rpc_url, &db_url, env.instance);
    let indexer = tokio::time::timeout(
        Duration::from_secs(60),
        private_channel_indexer::run(common.clone(), indexer_config, None),
    )
    .await
    .expect("the indexer must refuse, not start");
    assert!(
        matches!(&indexer, Err(IndexerError::Storage(StorageError::UnfinishedResync { program })) if program == "escrow"),
        "indexer: {indexer:?}"
    );
    let operator = tokio::time::timeout(
        Duration::from_secs(60),
        operator::run(new_storage(&db_url).await, common, operator_config, None),
    )
    .await
    .expect("the operator must refuse, not start");
    assert!(
        matches!(&operator, Err(OperatorError::Storage(StorageError::UnfinishedResync { program })) if program == "escrow"),
        "operator: {operator:?}"
    );

    escrow_resync(&rpc_url, &db_url, env.instance, genesis)
        .await
        .expect("a same-program rerun must finish the interrupted resync");
    assert_eq!(marker(&db_url).await, None);
    assert_eq!(active_halt(&db_url).await, None);
    assert_eq!(deposit_outcomes(&db_url).await, deposits);

    // The workers run normally again: one new deposit is minted once and nothing else is.
    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    let deposit_c = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT).await?;
    wait_for_status(&db_url, &deposit_c.to_string(), "completed").await;
    let_stack_settle(&db_url, &client).await;
    stack.stop().await;
    assert_eq!(deposit_count(&db_url).await, 3, "no duplicate deposit rows");
    assert_eq!(
        supply(&client, env.mint).await,
        supply_before + DEPOSIT_AMOUNT,
        "only the new deposit is minted"
    );
    Ok(())
}

/// Withdrawal nonces in chain order, and the nonce the sequence hands out next.
async fn withdrawal_nonces(db_url: &str) -> (Vec<(String, Option<i64>)>, i64) {
    let pool = fresh_pool(db_url).await;
    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT signature, withdrawal_nonce FROM transactions \
         WHERE transaction_type = 'withdrawal' ORDER BY slot, instruction_index",
    )
    .fetch_all(&pool)
    .await
    .expect("read withdrawal nonces");
    // Read without nextval so checking the sequence does not move it.
    let (last, called): (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM withdrawal_nonce_seq")
            .fetch_one(&pool)
            .await
            .expect("read the nonce sequence");
    (rows, if called { last + 1 } else { last })
}

/// E2E-3b (94, withdraw): a withdraw resync killed after rebuilding a reminted withdrawal leaves
/// a partial failed_reminted row behind; the rerun passes every gate and renumbers from 0.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_interrupted_withdraw_resync_reruns_cleanly() -> Result<(), Box<dyn std::error::Error>>
{
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("e2e_interrupted_withdraw").await?;
    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    let user = &env.users[0];

    // Slot gaps between withdrawals, so the kill lands after the first row and before the last.
    let mut withdrawals = Vec::new();
    for _ in 0..3 {
        let w = helpers::execute_user_withdrawal(&client, user, env.mint, WITHDRAW_AMOUNT)
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        wait_for_finalized_slot(&rpc_url, client.get_slot().await? + MIN_FILL_SLOTS).await;
        withdrawals.push(w.signature);
    }

    let mock = MockRpcServer::start().await;
    let h = Harness {
        db_url: db_url.clone(),
        source_rpc_url: rpc_url.clone(),
        program_type: ProgramType::Withdraw,
        instance: Some(env.instance),
        channel_url: mock.url(),
        genesis,
    };
    let keys = h.discover().await;
    let reminted = keys
        .iter()
        .find(|k| k.signature == withdrawals[0])
        .expect("the first withdrawal was indexed")
        .clone();
    script_channel_consumed(
        &mock,
        &[(&reminted, ConsumedMintKind::Remint, Signature::new_unique())],
    );

    // One slot per round trip and a fast heartbeat, so the kill lands while the fill runs.
    let service = ResyncService::new(
        new_storage(&db_url).await,
        Arc::new(RpcPoller::new(
            rpc_url.clone(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        )),
        ProgramType::Withdraw,
        BackfillConfig {
            enabled: true,
            exit_after_backfill: true,
            rpc_url: rpc_url.clone(),
            batch_size: 1,
            max_gap_slots: u64::MAX,
            start_slot: None,
        },
        Some(env.instance),
    )
    .with_stale_holder_grace(Duration::ZERO)
    .with_channel_reconcile(ChannelReconcileConfig {
        channel_rpc_url: mock.url(),
        authority: CHANNEL_AUTHORITY,
    })
    .with_withdrawal_bitmap_rpc(rpc_url.clone())
    .with_lock_heartbeat_interval(Duration::from_millis(5));
    let killer_url = db_url.clone();
    let reminted_signature = reminted.signature.clone();
    let killer = tokio::spawn(async move {
        let pool = fresh_pool(&killer_url).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            // Kill once the marker is set and the reminted row exists; return the withdrawal count.
            let rows: Option<i64> = sqlx::query_scalar(
                "SELECT COUNT(*) FROM transactions WHERE transaction_type = 'withdrawal' \
                 AND EXISTS (SELECT 1 FROM resync_state) \
                 HAVING bool_or(signature = $1)",
            )
            .bind(&reminted_signature)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
            if let Some(rows) = rows {
                terminate_live_lock_holder(&pool).await;
                return Some(rows);
            }
            if std::time::Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let aborted = service.run(genesis).await;
    let rows_at_kill = killer
        .await?
        .expect("the rebuild never wrote the reminted row");
    assert!(
        matches!(
            aborted,
            Err(IndexerError::Storage(StorageError::LiveStateLockLost))
        ),
        "a lock lost mid-rebuild must abort, got {aborted:?}"
    );
    assert!(
        rows_at_kill < 3,
        "the kill must land mid-rebuild, saw {rows_at_kill} rows"
    );
    assert_eq!(marker(&db_url).await.as_deref(), Some("withdraw"));
    assert_eq!(
        active_halt(&db_url).await,
        Some(resync_halt_reason(ProgramType::Withdraw))
    );

    // The rerun has to pass wipe_blocker and the bitmap check over the partial rows.
    h.run()
        .await
        .expect("a same-program rerun must finish the interrupted withdraw resync");
    assert_eq!(marker(&db_url).await, None);
    assert_eq!(active_halt(&db_url).await, None);
    let (rows, next) = withdrawal_nonces(&db_url).await;
    let expected: Vec<(String, Option<i64>)> = withdrawals
        .iter()
        .enumerate()
        .map(|(n, sig)| (sig.clone(), Some(n as i64)))
        .collect();
    assert_eq!(rows, expected, "nonces restart at 0 in chain order");
    assert_eq!(next, 3, "the next withdrawal takes nonce 3");
    assert_eq!(
        status_of(&db_url, &reminted).await.status,
        "failed_reminted"
    );
    assert_eq!(pending_count(&db_url).await, 2);

    mock.shutdown().await;
    Ok(())
}

/// E2E-4 (#2): a deposit left processing with a landed, journaled mint refuses the resync with
/// the database untouched; once the operator settles it, the resync rebuilds it completed with one mint.
#[tokio::test(flavor = "multi_thread")]
async fn e2e_in_flight_mint_refuses_then_resync_succeeds() -> Result<(), Box<dyn std::error::Error>>
{
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let genesis = client.get_slot().await?;
    let (db_url, _storage, _pg) = start_postgres_for_resync("e2e_in_flight").await?;
    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    TestEnvironment::setup_operator(&client, &faucet, env.instance).await?;
    let user = &env.users[0];
    let deposit = do_deposit(&client, user, env.instance, env.mint, DEPOSIT_AMOUNT).await?;
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&rpc_url, tip + 5).await;

    // Index the real deposit with a resync against the (still empty) channel.
    escrow_resync(&rpc_url, &db_url, env.instance, genesis)
        .await
        .expect("indexing resync");
    let pool = fresh_pool(&db_url).await;
    let (id, ix, inner): (i64, i32, Option<i32>) = sqlx::query_as(
        "SELECT id, instruction_index, inner_index FROM transactions WHERE signature = $1",
    )
    .bind(deposit.to_string())
    .fetch_one(&pool)
    .await?;

    // The operator's crash after broadcast: its memo'd mint landed, the row stays processing
    // with the attempt journaled, and it is old enough for the recovery sweep.
    let memo = mint_idempotency_memo(&SourceEventId::new(&deposit.to_string(), ix, inner));
    let (landed, last_valid) =
        send_memo_mint(&client, &memo, user.pubkey(), env.mint, DEPOSIT_AMOUNT).await;
    let user_after_mint = token_balance(&client, user.pubkey(), env.mint).await;
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;
    seed_journal(
        &db_url,
        "pending_release_signatures",
        id,
        &landed.to_string(),
    )
    .await;
    sqlx::query("UPDATE pending_release_signatures SET last_valid_block_height = $2 WHERE transaction_id = $1")
        .bind(id)
        .bind(last_valid as i64)
        .execute(&pool)
        .await?;
    seed_sql(
        &db_url,
        "ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at",
    )
    .await;
    sqlx::query("UPDATE transactions SET updated_at = NOW() - INTERVAL '10 minutes' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;
    seed_sql(
        &db_url,
        "ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at",
    )
    .await;

    let before = side_fingerprint(&db_url, "deposit").await;
    let refused = escrow_resync(&rpc_url, &db_url, env.instance, genesis).await;
    assert!(
        matches!(
            refused,
            Err(IndexerError::Reconciliation(
                ReconciliationError::UnsettledWork
            ))
        ),
        "an in-flight mint must refuse the resync, got {refused:?}"
    );
    assert_eq!(side_fingerprint(&db_url, "deposit").await, before);
    assert_eq!(marker(&db_url).await, None);

    // The operator's recovery sweep finds the landed mint and completes the row.
    let tip = client.get_slot().await?;
    wait_for_finalized_slot(&rpc_url, tip + 1).await;
    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    wait_for_status(&db_url, &deposit.to_string(), "completed").await;
    stack.stop().await;
    assert_eq!(
        row_of(&db_url, &deposit.to_string()).await.2,
        Some(landed.to_string())
    );

    escrow_resync(&rpc_url, &db_url, env.instance, genesis)
        .await
        .expect("the resync succeeds once the mint has settled");
    let (status, _, counterpart) = row_of(&db_url, &deposit.to_string()).await;
    assert_eq!(status, "completed");
    assert_eq!(counterpart, Some(landed.to_string()));

    // Restarted workers must not mint it again.
    let stack = Stack::start(&rpc_url, &db_url, env.instance).await;
    let_stack_settle(&db_url, &client).await;
    stack.stop().await;
    assert_eq!(
        token_balance(&client, user.pubkey(), env.mint).await,
        user_after_mint,
        "exactly one mint for the deposit"
    );
    Ok(())
}
