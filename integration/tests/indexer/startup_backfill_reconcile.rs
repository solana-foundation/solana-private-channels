//! Startup ordering between backfill and escrow reconciliation in
//! `private_channel_indexer::indexer::run`.
//!
//! A deposit that finalizes while the indexer is down leaves on-chain custody ahead of the
//! database. Reconciliation compares the two, so running it before backfill has imported
//! that deposit compares current custody against a ledger that is knowingly incomplete,
//! and at the default zero tolerance that aborts the boot before backfill can repair
//! anything. Every restart then repeats it.
//!
//! These tests pin both halves of the fix: the ordinary recovery path must import the
//! deposit and then reconcile clean at the strictest threshold, and a mismatch that
//! backfill cannot explain must still stop startup.

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
#[allow(dead_code)]
mod setup;

use mockito::{Matcher, Server as MockitoServer};
// The indexer decides what counts as an escrow instruction by this constant, so a
// synthetic block has to carry the same id or the fill sees nothing to import.
use private_channel_indexer::indexer::datasource::common::parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_indexer::{
    config::{BackfillConfig, ReconciliationConfig, RpcPollingConfig},
    error::{IndexerError, ReconciliationError},
    indexer::run,
    storage::{PostgresDb, Storage},
    test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes},
    DatasourceType, IndexerConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType,
    StorageType,
};
use serde_json::json;
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    commitment_config::CommitmentLevel,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use test_utils::mock_rpc::{MockRpcServer, Reply};
use test_utils::validator_helper::start_test_validator_no_geyser;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::task::JoinHandle;

const USER_BALANCE: u64 = 1_000_000;
const DEPOSIT_AMOUNT: u64 = 50_000;

/// Raw amount of the phantom deposit row that no on-chain balance backs.
const PHANTOM_AMOUNT: i64 = 12_345;

/// Scripted chain tip and first slot for the fully mocked halt test.
const MOCK_TIP: u64 = 900;
const MOCK_START_SLOT: u64 = 895;

/// Generous enough for a validator plus a full backfill and, for the halting case, three
/// reconcile attempts each waiting on a checkpoint flush.
const STARTUP_TIMEOUT_SECS: u64 = 180;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();
}

/// Real Postgres via testcontainers, with the indexer schema already created so a test can
/// seed rows before `run` starts. The container must stay alive for the whole test.
async fn start_postgres(
    db: &str,
) -> (
    testcontainers::ContainerAsync<Postgres>,
    PgPool,
    PostgresConfig,
) {
    let container = Postgres::default()
        .with_db_name(db)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/{db}");

    let config = PostgresConfig {
        database_url: url.clone(),
        max_connections: 10,
    };
    let storage = Storage::Postgres(PostgresDb::new(&config).await.expect("storage"));
    storage.init_schema().await.expect("schema");

    let pool = PgPool::connect(&url).await.expect("pg pool");
    (container, pool, config)
}

fn deposit_ix(user: &Keypair, instance: Pubkey, mint: Pubkey, amount: u64) -> Instruction {
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_PROGRAM_ID);
    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_PROGRAM_ID);

    private_channel_escrow_program_client::instructions::DepositBuilder::new()
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
        .private_channel_escrow_program(
            PRIVATE_CHANNEL_ESCROW_PROGRAM_ID
                .parse()
                .expect("the parser's escrow program id must be a valid pubkey"),
        )
        .amount(amount)
        .instruction()
}

/// Wait until the instance ATA's finalized balance is `expected`, so the escrow sweep that
/// reconciliation performs is guaranteed to observe the deposit.
async fn wait_for_finalized_custody(rpc_url: &str, ata: &Pubkey, expected: u64) {
    let client = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::finalized());
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if let Ok(balance) = client.get_token_account_balance(ata).await {
            if balance.amount.parse::<u64>().unwrap_or(0) == expected {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for finalized custody to reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Answer the startup supply invariant with "no channel mint exists yet".
///
/// An absent mint account reads as zero supply, which can never exceed custody, so the
/// invariant passes for every mint and these tests keep testing the custody comparison.
/// An unanswered read is no longer harmless: startup refuses to boot on a supply it could
/// not check. Answering absence rather than a fabricated mint keeps any other reader on
/// this RPC seeing the truth, and the queue is stocked well past what a run consumes.
fn mock_empty_channel_supply(rpc: &MockRpcServer) {
    let reply = Reply::result(json!({"context": {"slot": 1}, "value": null}));
    rpc.enqueue_sequence("getAccountInfo", std::iter::repeat_n(reply, 4096));
}

/// Spawn `run` on the ordinary recovery configuration and hand back its result.
///
/// `mismatch_threshold_raw` is deliberately 0, the shipped default, because the whole
/// finding is that this configuration cannot boot. `start_slot` is the bottom of the slice
/// the indexer missed, so the fill has exactly that slice to recover. The channel RPC is
/// scripted with an empty supply, which satisfies the supply invariant without ever
/// tripping it and leaves the custody comparison as the only thing that can fail the boot.
fn spawn_indexer(
    postgres: PostgresConfig,
    rpc_url: String,
    channel_rpc_url: String,
    instance: Pubkey,
    start_slot: u64,
) -> JoinHandle<Result<(), IndexerError>> {
    spawn_indexer_with(
        postgres,
        rpc_url,
        channel_rpc_url,
        instance,
        Some(start_slot),
        None,
    )
}

/// Same startup, with the two start-slot knobs opened up.
///
/// `backfill_start` of `None` disables backfill entirely, which is the only shape where
/// `rpc_polling_start` decides where the live source begins.
fn spawn_indexer_with(
    postgres: PostgresConfig,
    rpc_url: String,
    channel_rpc_url: String,
    instance: Pubkey,
    backfill_start: Option<u64>,
    rpc_polling_start: Option<u64>,
) -> JoinHandle<Result<(), IndexerError>> {
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.clone(),
        fallback_rpc_url: None,
        source_rpc_url: Some(channel_rpc_url),
        postgres,
        escrow_instance_id: Some(instance),
    };

    let indexer = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: Some(RpcPollingConfig {
            from_slot: rpc_polling_start,
            poll_interval_ms: 200,
            error_retry_interval_ms: 1_000,
            batch_size: 10,
            encoding: UiTransactionEncoding::Json,
            // Confirmed, matching the other RPC-polling harness in this repo. A
            // solana-test-validator started without geyser never finalizes the deposit
            // slots, so at Finalized getBlock refuses to serve them indefinitely. The
            // deposit is still waited out to finalized custody before startup, so the
            // sweep and the ledger agree on it either way.
            commitment: CommitmentLevel::Confirmed,
        }),
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: backfill_start.is_some(),
            exit_after_backfill: false,
            rpc_url,
            batch_size: 100,
            max_gap_slots: u64::MAX,
            start_slot: backfill_start,
        },
        reconciliation: ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
    };

    common.validate().expect("common config");
    indexer.validate().expect("indexer config");

    tokio::spawn(async move { run(common, indexer, None).await })
}

/// Wait until `slot`'s block can actually be served.
///
/// A block is listed as produced before the RPC will hand it over, and the fill treats a
/// listed-but-unservable block as a hard error rather than something to retry, by design.
/// Waiting on the one slot that must be imported keeps the test measuring startup ordering
/// rather than how quickly the validator's block store catches up.
async fn wait_for_block_servable(rpc_url: &str, slot: u64) {
    let client = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());
    let config = RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Json),
        transaction_details: Some(TransactionDetails::Full),
        rewards: Some(false),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if client.get_block_with_config(slot, config).await.is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for slot {slot} to become servable"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Mock the escrow sweep. `holdings` is the per-mint custody the instance is reported to
/// hold; an empty slice means the escrow holds nothing.
///
/// Answers every attempt's two token programs, so it is left uncounted rather than pinned
/// to an exact hit count.
async fn mock_escrow_custody(rpc: &mut MockitoServer, holdings: &[(String, i64)]) -> mockito::Mock {
    mock_escrow_custody_at(rpc, holdings, MOCK_TIP).await
}

/// Same, with the slot the reading is reported as valid at. Startup fills up to that slot
/// and no further, so a value below the tip is what leaves a band for the live source.
async fn mock_escrow_custody_at(
    rpc: &mut MockitoServer,
    holdings: &[(String, i64)],
    context_slot: u64,
) -> mockito::Mock {
    let accounts: Vec<serde_json::Value> = holdings
        .iter()
        .map(|(mint, amount)| {
            json!({
                "pubkey": Pubkey::new_unique().to_string(),
                "account": {
                    "lamports": 2_039_280,
                    "owner": TOKEN_PROGRAM_ID.to_string(),
                    "executable": false,
                    "rentEpoch": 0,
                    "space": 165,
                    "data": {
                        "program": "spl-token",
                        "space": 165,
                        "parsed": {
                            "type": "account",
                            "info": {
                                "mint": mint,
                                "tokenAmount": { "amount": amount.to_string() }
                            }
                        }
                    }
                }
            })
        })
        .collect();

    rpc.mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getTokenAccountsByOwner"}),
        ))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "result": {"context": {"slot": context_slot}, "value": accounts},
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(1)
        .create_async()
        .await
}

/// A block whose one transaction is a top-level escrow Deposit, shaped the way the fill's
/// decoder reads it: the instruction supplies the accounts, and the DepositEvent self-CPI
/// that the program emits underneath it supplies the amount.
///
/// Account order is the Deposit instruction's own, so index 2 is the instance the indexer
/// filters on and index 3 is the mint the row is recorded against.
fn deposit_block_json(slot: u64, instance: Pubkey, mint: Pubkey, amount: u64) -> serde_json::Value {
    const ESCROW_PROGRAM_INDEX: u8 = 11;

    let mut account_keys: Vec<String> = (0..12).map(|_| Pubkey::new_unique().to_string()).collect();
    account_keys[2] = instance.to_string();
    account_keys[3] = mint.to_string();
    account_keys[ESCROW_PROGRAM_INDEX as usize] = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string();

    let ix_data = bs58::encode(deposit_ix_bytes(amount, None)).into_string();
    let event_data = bs58::encode(deposit_event_bytes(amount)).into_string();

    json!({
        "blockhash": "TestBlockHash11111111111111111111111111111",
        "parentSlot": slot - 1,
        "transactions": [{
            "transaction": {
                "signatures": [format!("mocked_deposit_sig_{slot}")],
                "message": {
                    "accountKeys": account_keys,
                    "instructions": [{
                        "programIdIndex": ESCROW_PROGRAM_INDEX,
                        "accounts": (0u8..12).collect::<Vec<u8>>(),
                        "data": ix_data
                    }]
                }
            },
            "meta": {
                "err": null,
                "innerInstructions": [{
                    "index": 0,
                    "instructions": [{
                        "programIdIndex": ESCROW_PROGRAM_INDEX,
                        "accounts": Vec::<u8>::new(),
                        "data": event_data,
                        "stackHeight": 2
                    }]
                }]
            }
        }]
    })
}

/// Mock the block enumeration and every block in the fill range, all empty.
async fn mock_fill_range(rpc: &mut MockitoServer) -> Vec<mockito::Mock> {
    mock_fill_range_carrying(rpc, None).await
}

/// Same, except one slot's block is replaced by `carrying`, so the fill has something to
/// import rather than walking an empty range.
async fn mock_fill_range_carrying(
    rpc: &mut MockitoServer,
    carrying: Option<(u64, serde_json::Value)>,
) -> Vec<mockito::Mock> {
    let mut mocks = Vec::new();
    mocks.push(
        rpc.mock("POST", "/")
            .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
            .with_status(200)
            .with_body(json!({"jsonrpc": "2.0", "result": MOCK_TIP, "id": 1}).to_string())
            .expect_at_least(1)
            .create_async()
            .await,
    );
    let produced: Vec<u64> = (MOCK_START_SLOT..=MOCK_TIP).collect();
    // The backfill floor is exclusive, so the anchor lookup that picks the range's
    // last produced block asks from one slot below it. Same producers answer it.
    mocks.push(
        rpc.mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({"method": "getBlocks", "params": [MOCK_START_SLOT - 1, MOCK_TIP]}),
            ))
            .with_status(200)
            .with_body(json!({"jsonrpc": "2.0", "result": produced, "id": 1}).to_string())
            .expect_at_least(1)
            .create_async()
            .await,
    );
    mocks.push(
        rpc.mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({"method": "getBlocks", "params": [MOCK_START_SLOT, MOCK_TIP]}),
            ))
            .with_status(200)
            .with_body(json!({"jsonrpc": "2.0", "result": produced, "id": 1}).to_string())
            .expect_at_least(1)
            .create_async()
            .await,
    );
    for slot in MOCK_START_SLOT..=MOCK_TIP {
        let block = match &carrying {
            Some((carried_slot, block)) if *carried_slot == slot => block.clone(),
            _ => json!({
                "blockhash": "TestBlockHash11111111111111111111111111111",
                "parentSlot": slot - 1,
                "transactions": []
            }),
        };
        mocks.push(
            rpc.mock("POST", "/")
                .match_body(Matcher::PartialJson(
                    json!({"method": "getBlock", "params": [slot]}),
                ))
                .with_status(200)
                .with_body(json!({"jsonrpc": "2.0", "result": block, "id": 1}).to_string())
                .expect_at_least(1)
                .create_async()
                .await,
        );
    }
    mocks
}

/// Slot a confirmed signature landed in.
async fn slot_of(client: &RpcClient, signature: &Signature) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(statuses) = client.get_signature_statuses(&[*signature]).await {
            if let Some(Some(status)) = statuses.value.first() {
                return status.slot;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out resolving the deposit's slot"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Register a mint, as an AllowMint indexed before the outage would have.
async fn seed_allowed_mint(pool: &PgPool, mint_address: &str) {
    sqlx::query(
        "INSERT INTO mints (mint_address, decimals, token_program, created_at)
         VALUES ($1, 6, $2, NOW())
         ON CONFLICT (mint_address) DO NOTHING",
    )
    .bind(mint_address)
    .bind(TOKEN_PROGRAM_ID.to_string())
    .execute(pool)
    .await
    .expect("seed mint");
}

/// Insert a `mints` row and a deposit for a mint that holds no tokens on-chain. Nothing
/// backfill can fetch will ever explain this, so it stands in for a genuine divergence.
async fn seed_phantom_deposit(pool: &PgPool, mint_address: &str, amount: i64) {
    seed_allowed_mint(pool, mint_address).await;

    sqlx::query(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, created_at, updated_at)
         VALUES ($1, 1, 'phantom', 'phantom', $2, $3,
                 'deposit'::transaction_type, 'pending'::transaction_status, NOW(), NOW())",
    )
    .bind(format!("phantom_sig_{mint_address}"))
    .bind(mint_address)
    .bind(amount)
    .execute(pool)
    .await
    .expect("seed deposit");
}

/// Slot and amount of the single indexed deposit for `mint`, if it has been written yet.
async fn deposit_row(pool: &PgPool, mint: &str) -> Option<(i64, String)> {
    sqlx::query_as(
        "SELECT slot, amount::TEXT FROM transactions
         WHERE mint = $1 AND transaction_type = 'deposit'",
    )
    .bind(mint)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

async fn checkpoint_of(pool: &PgPool, program: &str) -> Option<i64> {
    // The column is nullable, so a row can exist before any slot has been committed.
    let row: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
            .bind(program)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.and_then(|(slot,)| slot)
}

/// Put a durable checkpoint in place before startup, standing in for an earlier run.
async fn seed_checkpoint(pool: &PgPool, program: &str, slot: i64) {
    sqlx::query(
        "INSERT INTO indexer_state (program_type, last_committed_slot, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (program_type) DO UPDATE SET last_committed_slot = EXCLUDED.last_committed_slot",
    )
    .bind(program)
    .bind(slot)
    .execute(pool)
    .await
    .expect("seed checkpoint");
}

/// The finding, verbatim: a supported deposit finalizes while the indexer is down, so
/// custody holds tokens the database has never seen. Startup must import the deposit and
/// only then reconcile, at the strictest threshold, and stay up.
///
/// Before the fix this fails in seconds: reconciliation runs first, sees custody it cannot
/// explain, and `run` returns MismatchExceedsThreshold with no deposit row ever written.
///
/// Ignored for the same reason the RPC-polling gap-detection test is: on a test validator
/// the RPC serves a recent block before its innerInstructions are written, so the escrow
/// DepositEvent decodes to nothing and the deposit is never imported. The block-listing
/// side has the mirror-image problem, enumerating slots as produced that getBlock will not
/// serve, which the fill now treats as fatal. Neither is anything this change introduces.
/// The mocked sibling below covers the same ordering deterministically, and the Yellowstone
/// gap-detection test covers deposit recovery against a real validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "test-validator serves a recent block before its innerInstructions are \
            written, so the escrow DepositEvent decodes to nothing"]
async fn deposit_finalized_while_down_is_backfilled_then_reconciles() {
    init_tracing();
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let (_pg, pool, postgres) = start_postgres("startup_backfill_ok").await;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None)
        .await
        .expect("environment");
    let user = &env.users[0];

    let ix = deposit_ix(user, env.instance, env.mint, DEPOSIT_AMOUNT);
    let signature =
        helpers::send_and_confirm_instructions(&client, &[ix], user, &[user], "Deposit")
            .await
            .expect("deposit");

    // The indexer is "down" from here on, and the slice it has to recover starts at the
    // deposit's own slot. Anchoring on the deposit rather than on a wall-clock reading
    // keeps the range's first slot a real producer, which is what lets the fill prove
    // block continuity across it on a validator that skips slots freely.
    let start_slot = slot_of(&client, &signature).await;

    // The mint was allowed before the outage, so its row predates the missing slice. Only
    // registered mints contribute to the ledger side of the comparison, and replaying the
    // AllowMint is a separate concern from the ordering under test.
    seed_allowed_mint(&pool, &env.mint.to_string()).await;

    let instance_ata =
        get_associated_token_address_with_program_id(&env.instance, &env.mint, &TOKEN_PROGRAM_ID);
    wait_for_finalized_custody(&validator.rpc_url(), &instance_ata, DEPOSIT_AMOUNT).await;
    wait_for_block_servable(&validator.rpc_url(), start_slot).await;

    let channel = MockRpcServer::start().await;
    mock_empty_channel_supply(&channel);
    let mut handle = spawn_indexer(
        postgres,
        validator.rpc_url(),
        channel.url(),
        env.instance,
        start_slot,
    );

    // Poll for the deposit row, but fail fast and loudly if startup aborts instead.
    let mint = env.mint.to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    let row = loop {
        if let Some(row) = deposit_row(&pool, &mint).await {
            break row;
        }
        if handle.is_finished() {
            panic!(
                "startup exited before importing the deposit: {:?}",
                (&mut handle).await
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the deposit was never indexed"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    let (deposit_slot, amount) = row;
    assert_eq!(
        amount,
        DEPOSIT_AMOUNT.to_string(),
        "the indexed deposit must carry the on-chain amount"
    );

    // The checkpoint must cover the deposit's slot, which is what makes the import durable.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if checkpoint_of(&pool, "escrow")
            .await
            .is_some_and(|cp| cp >= deposit_slot)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the checkpoint never reached the deposit's slot {deposit_slot}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    assert!(
        !handle.is_finished(),
        "reconciliation must pass at threshold 0 once the deposit is indexed"
    );

    handle.abort();
    channel.shutdown().await;
}

/// The retry loop must narrow the race, never disarm the check. A deposit row that no
/// on-chain balance backs cannot be explained by any amount of backfilling, so startup
/// still has to stop, and it has to stop only after backfill has actually run.
///
/// Fully mocked on purpose. What is under test is the ordering and the halt, neither of
/// which needs a real chain, and a scripted tip keeps the fill range exact so the
/// checkpoint assertion below can name the slot it expects rather than merely "some".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unexplainable_mismatch_still_halts_startup() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_backfill_halt").await;

    let phantom_mint = Pubkey::new_unique().to_string();
    seed_phantom_deposit(&pool, &phantom_mint, PHANTOM_AMOUNT).await;

    // Blocks the fill will walk, all empty: the phantom row is the only ledger content.
    let mut rpc = MockitoServer::new_async().await;
    let _range = mock_fill_range(&mut rpc).await;
    // Escrow custody holds nothing, so nothing backs the phantom row.
    let _custody = mock_escrow_custody(&mut rpc, &[]).await;

    // Only the supply invariant reads this, and it errors on every method, so each mint
    // is skipped with a warning and the custody comparison is what fails the boot.
    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);

    let handle = spawn_indexer(
        postgres,
        rpc.url(),
        chain.url(),
        Pubkey::new_unique(),
        MOCK_START_SLOT,
    );

    let result = tokio::time::timeout(Duration::from_secs(STARTUP_TIMEOUT_SECS), handle)
        .await
        .expect("startup must terminate rather than serve an unexplained mismatch")
        .expect("run task must not panic");

    match result {
        Err(IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
            threshold,
            ..
        })) => assert_eq!(
            threshold, 0,
            "the strict threshold must be the one enforced"
        ),
        other => panic!("expected a mismatch halt, got {other:?}"),
    }

    assert_eq!(
        checkpoint_of(&pool, "escrow").await,
        Some(MOCK_TIP as i64),
        "the fill must have run and committed through its target before the halt, \
         otherwise the halt is still happening against an unrepaired ledger"
    );

    chain.shutdown().await;
}

/// The claim the whole change rests on, deterministically: the fill imports the deposit
/// that explains custody, and only then does reconciliation run and pass.
///
/// The database starts with no deposit at all while custody already holds one, which is
/// the finding's own starting state. Nothing here is seeded to make the sums agree: the
/// row has to arrive from the mocked block the fill walks, and the checkpoint has to reach
/// the fill target before the comparison happens. Before the fix this configuration cannot
/// boot, because reconciliation runs first and aborts on custody it cannot account for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fill_imports_the_deposit_that_explains_custody() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_backfill_import").await;

    let instance = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    let deposit_slot = MOCK_START_SLOT + 2;
    seed_allowed_mint(&pool, &mint.to_string()).await;

    let mut rpc = MockitoServer::new_async().await;
    let _range = mock_fill_range_carrying(
        &mut rpc,
        Some((
            deposit_slot,
            deposit_block_json(deposit_slot, instance, mint, DEPOSIT_AMOUNT),
        )),
    )
    .await;
    // Custody holds exactly the deposit the block carries, so the two agree only once
    // that block has been imported.
    let _custody =
        mock_escrow_custody(&mut rpc, &[(mint.to_string(), DEPOSIT_AMOUNT as i64)]).await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);
    let mut handle = spawn_indexer(postgres, rpc.url(), chain.url(), instance, MOCK_START_SLOT);

    let deadline = std::time::Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    loop {
        if checkpoint_of(&pool, "escrow").await == Some(MOCK_TIP as i64) {
            break;
        }
        if handle.is_finished() {
            panic!(
                "startup exited instead of importing the deposit and reconciling: {:?}",
                (&mut handle).await
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the fill never committed through its target"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let (slot, amount) = deposit_row(&pool, &mint.to_string())
        .await
        .expect("the fill must have written the deposit the block carried");
    assert_eq!(
        slot, deposit_slot as i64,
        "the deposit's own slot is recorded"
    );
    assert_eq!(
        amount,
        DEPOSIT_AMOUNT.to_string(),
        "the amount must come from the deposit's own event"
    );

    // With the row imported the sums agree, so reconciliation passes and startup carries on.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !handle.is_finished(),
        "custody explained by the imported deposit must not stop startup at threshold 0"
    );

    handle.abort();
    chain.shutdown().await;
}

/// Custody read from behind our own ledger must stop the boot, not produce a verdict.
///
/// The checkpoint says slots up to 899 are indexed while the custody read only speaks for
/// slot 890. There is no slot at which the two can be compared: the ledger cannot be
/// rewound and custody cannot be rolled forward. Comparing anyway would judge nine slots of
/// indexed deposits against a custody view that predates them, which reads as an unbacked
/// ledger and would look exactly like insolvency. Startup retries the read first, since a
/// lagging node usually catches up, and fails closed when it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custody_read_from_behind_the_ledger_stops_startup() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_backfill_behind").await;

    // A ledger that has already committed past where custody can speak for.
    sqlx::query(
        "INSERT INTO indexer_state (program_type, last_committed_slot, updated_at)
         VALUES ('escrow', 899, NOW())
         ON CONFLICT (program_type) DO UPDATE SET last_committed_slot = 899",
    )
    .execute(&pool)
    .await
    .expect("seed checkpoint");

    let mut rpc = MockitoServer::new_async().await;
    let _range = mock_fill_range(&mut rpc).await;
    let _custody = mock_escrow_custody_at(&mut rpc, &[], 890).await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);
    let handle = spawn_indexer(
        postgres,
        rpc.url(),
        chain.url(),
        Pubkey::new_unique(),
        MOCK_START_SLOT,
    );

    let result = tokio::time::timeout(Duration::from_secs(STARTUP_TIMEOUT_SECS), handle)
        .await
        .expect("startup must terminate rather than compare across a gap it cannot close")
        .expect("run task must not panic");

    match result {
        Err(IndexerError::Reconciliation(ReconciliationError::CustodyBehindLedger {
            snapshot_slot,
            committed,
        })) => {
            assert_eq!(snapshot_slot, 890);
            assert_eq!(committed, 899);
        }
        other => panic!("expected a fail-closed custody-behind-ledger halt, got {other:?}"),
    }

    chain.shutdown().await;
}

/// The fill stops where custody was measured, so the live source must resume from there
/// and not from the chain tip.
///
/// Custody is read at a slot below the tip, which leaves a band of slots that the fill
/// deliberately does not cover. Nothing else will ever read them: startup is finished with
/// the range and the live source only walks forward from wherever it is told to start. A
/// deposit is planted inside that band, so if the resume slot were taken from the resolved
/// range, one past the tip, the deposit would be skipped and never indexed by anything.
/// The row appearing is the proof that the two producers still meet with no hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_source_resumes_where_the_fill_stopped() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_backfill_resume").await;

    let instance = Pubkey::new_unique();
    let mint = Pubkey::new_unique();
    // Custody is measured here; the fill covers up to it and stops.
    let custody_slot = MOCK_TIP - 3;
    // Above the fill's reach, so only the live source can bring it in.
    let deposit_slot = MOCK_TIP - 1;
    assert!(
        deposit_slot > custody_slot,
        "the deposit must sit in the band the fill skips"
    );
    seed_allowed_mint(&pool, &mint.to_string()).await;

    let mut rpc = MockitoServer::new_async().await;
    // The fill enumerates only up to the measured slot, so that shorter range needs its own
    // listing; the tip-length one the other tests use would never be requested here.
    let capped: Vec<u64> = (MOCK_START_SLOT..=custody_slot).collect();
    let _capped_range = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [MOCK_START_SLOT, custody_slot]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": capped, "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;
    // The live source enumerates its own batch before fetching it, starting one past the
    // fill target and running to the tip: exactly the band under test.
    let live: Vec<u64> = (custody_slot + 1..MOCK_TIP).collect();
    let _live_range = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [custody_slot + 1, MOCK_TIP - 1]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": live, "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;
    let _range = mock_fill_range_carrying(
        &mut rpc,
        Some((
            deposit_slot,
            deposit_block_json(deposit_slot, instance, mint, DEPOSIT_AMOUNT),
        )),
    )
    .await;
    // Empty at the measured slot: the deposit has not happened yet as of then, so startup
    // reconciles clean and the deposit is purely the live source's to find.
    let _custody = mock_escrow_custody_at(&mut rpc, &[], custody_slot).await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);
    let mut handle = spawn_indexer(postgres, rpc.url(), chain.url(), instance, MOCK_START_SLOT);

    // The fill stops at the measured slot, not the tip.
    let deadline = std::time::Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    loop {
        if checkpoint_of(&pool, "escrow").await == Some(custody_slot as i64) {
            break;
        }
        if handle.is_finished() {
            panic!(
                "startup exited instead of filling to the measured slot: {:?}",
                (&mut handle).await
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the fill never committed through the slot custody was measured at"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Now the live source has to cover the band the fill left behind.
    let deadline = std::time::Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    loop {
        if let Some((slot, amount)) = deposit_row(&pool, &mint.to_string()).await {
            assert_eq!(slot, deposit_slot as i64);
            assert_eq!(amount, DEPOSIT_AMOUNT.to_string());
            break;
        }
        if handle.is_finished() {
            panic!(
                "startup exited before the live source read the band: {:?}",
                (&mut handle).await
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the deposit above the fill target was never indexed, so the resume slot skipped it"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    handle.abort();
    chain.shutdown().await;
}

/// The success path, deterministically: a ledger that matches custody must reconcile clean
/// at the strictest threshold and leave startup running.
///
/// This is the counterpart to the halt test rather than a second ordering proof. The halt
/// test is what pins the ordering, because before the fix reconciliation aborted the boot
/// with no checkpoint written at all. What this one adds is the other exit from the retry
/// loop: a passing reconcile has to break out of it and let startup carry on, rather than
/// spend all three attempts or stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_ledger_reconciles_after_the_fill_and_startup_continues() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_backfill_match").await;

    // A deposit already indexed, and custody holding exactly it.
    let mint = Pubkey::new_unique().to_string();
    seed_phantom_deposit(&pool, &mint, PHANTOM_AMOUNT).await;

    let mut rpc = MockitoServer::new_async().await;
    let _range = mock_fill_range(&mut rpc).await;
    let _custody = mock_escrow_custody(&mut rpc, &[(mint.clone(), PHANTOM_AMOUNT)]).await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);
    let mut handle = spawn_indexer(
        postgres,
        rpc.url(),
        chain.url(),
        Pubkey::new_unique(),
        MOCK_START_SLOT,
    );

    // The fill has to commit through its target before reconciliation is even attempted.
    let deadline = std::time::Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    loop {
        if checkpoint_of(&pool, "escrow").await == Some(MOCK_TIP as i64) {
            break;
        }
        if handle.is_finished() {
            panic!(
                "startup exited instead of reconciling a matching ledger: {:?}",
                (&mut handle).await
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the fill never committed through its target"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Past the fill, reconciliation must pass and startup must carry on into the datasource.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !handle.is_finished(),
        "a ledger that matches custody must not stop startup at threshold 0"
    );

    handle.abort();
    chain.shutdown().await;
}

/// A configured backfill start above the durable checkpoint would leave the slots between
/// them unfetched, and nothing downstream records them as still owed, so startup refuses.
///
/// The refusal has to land before any block is fetched and before the custody comparison.
/// Reconciliation would otherwise report the missing deposits as a custody mismatch, which
/// sends an operator after the wrong fault entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_slot_ahead_of_checkpoint_refuses_startup() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_start_slot_ahead").await;

    // An earlier run stopped here, well below the slice the configured start would begin at.
    let stale_checkpoint = (MOCK_START_SLOT - 10) as i64;
    seed_checkpoint(&pool, "escrow", stale_checkpoint).await;

    let mut rpc = MockitoServer::new_async().await;
    // Custody is captured before the floor is resolved, so that read still has to succeed.
    let _custody = mock_escrow_custody(&mut rpc, &[]).await;
    // The fill never starts, so no slot is ever enumerated.
    let no_enumeration = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getBlocks"})))
        .expect(0)
        .create_async()
        .await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);

    let handle = spawn_indexer(
        postgres,
        rpc.url(),
        chain.url(),
        Pubkey::new_unique(),
        MOCK_START_SLOT,
    );

    let err = tokio::time::timeout(Duration::from_secs(STARTUP_TIMEOUT_SECS), handle)
        .await
        .expect("startup must not hang")
        .expect("startup task must not panic")
        .expect_err("a start slot past the checkpoint must refuse to boot");

    let rendered = err.to_string();
    assert!(
        rendered.contains("indexer.backfill.start_slot"),
        "the refusal must name the offending key, got: {rendered}"
    );
    assert_eq!(
        checkpoint_of(&pool, "escrow").await,
        Some(stale_checkpoint),
        "a refused boot must leave the durable checkpoint exactly where it was"
    );
    no_enumeration.assert_async().await;
    chain.shutdown().await;
}

/// With backfill off the live stream is the only producer, so a configured polling start
/// above the checkpoint strands those slots with nothing able to go back for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_polling_start_slot_ahead_of_checkpoint_refuses_startup() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_polling_start_ahead").await;

    let stale_checkpoint = (MOCK_START_SLOT - 10) as i64;
    seed_checkpoint(&pool, "escrow", stale_checkpoint).await;

    let mut rpc = MockitoServer::new_async().await;
    let _custody = mock_escrow_custody(&mut rpc, &[]).await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);

    let handle = spawn_indexer_with(
        postgres,
        rpc.url(),
        chain.url(),
        Pubkey::new_unique(),
        None,
        Some(MOCK_START_SLOT),
    );

    let err = tokio::time::timeout(Duration::from_secs(STARTUP_TIMEOUT_SECS), handle)
        .await
        .expect("startup must not hang")
        .expect("startup task must not panic")
        .expect_err("a polling start past the checkpoint must refuse to boot");

    let rendered = err.to_string();
    assert!(
        rendered.contains("indexer.rpc_polling.start_slot"),
        "the refusal must name the polling key, not the backfill one, got: {rendered}"
    );
    assert_eq!(
        checkpoint_of(&pool, "escrow").await,
        Some(stale_checkpoint),
        "a refused boot must leave the durable checkpoint exactly where it was"
    );
    chain.shutdown().await;
}

/// With backfill off and no configured polling start, the source would otherwise begin at
/// the chain tip and strand everything above the checkpoint. Nothing else fetches those
/// slots, so the stream has to resume from the checkpoint instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_source_resumes_from_checkpoint_when_nothing_configures_a_start() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_polling_default_start").await;

    let checkpoint = (MOCK_TIP - 20) as i64;
    seed_checkpoint(&pool, "escrow", checkpoint).await;

    let mut rpc = MockitoServer::new_async().await;
    let _custody = mock_escrow_custody(&mut rpc, &[]).await;
    // Answering the tip proves the resume slot is not taken from it.
    let _slot = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": MOCK_TIP, "id": 1}).to_string())
        .expect_at_least(0)
        .create_async()
        .await;
    // The stream must ask for the slot right after the checkpoint, never the tip.
    let resumes_at_checkpoint = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({
            "method": "getBlocks", "params": [checkpoint + 1]
        })))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": [], "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;

    let chain = MockRpcServer::start().await;
    mock_empty_channel_supply(&chain);

    let handle = spawn_indexer_with(
        postgres,
        rpc.url(),
        chain.url(),
        Pubkey::new_unique(),
        None,
        None,
    );

    // The enumeration is the assertion; give the poller a few intervals to issue it.
    tokio::time::sleep(Duration::from_secs(5)).await;
    resumes_at_checkpoint.assert_async().await;

    handle.abort();
    chain.shutdown().await;
}
