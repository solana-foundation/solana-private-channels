//! Custody against ledger liabilities, driven by a real validator, a real escrow indexer
//! and the real `run_reconciliation`. Channel supply comes from a mock answering "no such
//! mint", so the one validator playing both chains cannot make the supply arm trip first.

use crate::helpers::{
    self, db, drain_via_permanent_delegate, generate_permanent_delegate_mint_2022,
    mint_2022_to_owner,
};
use crate::setup::{
    allow_mint_for_program, find_allowed_mint_pda, find_event_authority_pda, find_instance_pda,
    TestEnvironment, TEST_ADMIN_KEYPAIR,
};
use private_channel_escrow_program_client::{
    instructions::DepositBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_indexer::config::{
    BackfillConfig, IndexerConfig, OperatorConfig, PostgresConfig, PrivateChannelIndexerConfig,
    ProgramType, ReconciliationConfig, RpcPollingConfig, StorageType,
};
use private_channel_indexer::error::{IndexerError, ReconciliationError};
use private_channel_indexer::indexer::reconciliation::{
    capture_custody_snapshot, reconcile_against_snapshot,
};
use private_channel_indexer::operator::escrow_sweep::fetch_escrow_balances_by_mint;
use private_channel_indexer::operator::reconciliation::run_reconciliation;
use private_channel_indexer::operator::{RetryConfig, RpcClientWithRetry};
use private_channel_indexer::storage::common::models::{DbTransactionBuilder, TransactionType};
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::{DatasourceType, YellowstoneConfig};
use private_channel_metrics::{HealthConfig, HealthState};
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use test_utils::mock_rpc::{MockRpcServer, Reply};
use test_utils::validator_helper::start_test_validator;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const MINT_DECIMALS: u8 = 6;
const DEPOSIT_AMOUNT: u64 = 200_000;
const DRAIN_AMOUNT: u64 = 60_000;
const WITHDRAWAL_AMOUNT: u64 = 40_000;
const USER_BALANCE: u64 = 1_000_000;

/// Ticks are one second apart, so a halt that needs three of them lands in seconds.
const TICK_INTERVAL: Duration = Duration::from_millis(1_000);

/// Longer than three ticks that each spend the full ledger-catchup budget waiting on a
/// checkpoint that never arrives, so a liability arm that halted on an unpinnable ledger
/// would have done it inside this window.
const UNPINNABLE_QUIET_SECS: u64 = 105;

/// Generous enough for a validator, an indexer catching up and a boot that reconciles.
const STARTUP_TIMEOUT_SECS: u64 = 240;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Real Postgres with the indexer schema already created, kept alive by the returned
/// container for as long as the test holds it.
async fn start_postgres(
    db_name: &str,
) -> (
    testcontainers::ContainerAsync<Postgres>,
    PgPool,
    String,
    Storage,
) {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/{db_name}");

    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: url.clone(),
            max_connections: 10,
        })
        .await
        .expect("storage"),
    );
    storage.init_schema().await.expect("schema");
    let pool = db::connect(&url).await.expect("pg pool");
    (container, pool, url, storage)
}

/// Answer every channel-supply read with "this mint does not exist", which the invariant
/// reads as zero supply. The queue is stocked well past what any run here consumes.
fn mock_absent_channel_mint(rpc: &MockRpcServer) {
    let reply = Reply::result(json!({"context": {"slot": 1}, "value": null}));
    rpc.enqueue_sequence("getAccountInfo", std::iter::repeat_n(reply, 4096));
}

/// A real escrow indexer over geyser, on a runtime of its own so it can actually be
/// stopped: `run` spawns its processor and datasource as detached tasks that outlive an
/// abort of the future that started them. Its own startup comparison is disarmed.
struct EscrowIndexer {
    runtime: Option<tokio::runtime::Runtime>,
    task: JoinHandle<Result<(), IndexerError>>,
}

impl EscrowIndexer {
    fn start(
        db_url: &str,
        geyser_endpoint: &str,
        rpc_url: &str,
        channel_rpc_url: &str,
        instance: Pubkey,
    ) -> Self {
        let postgres = PostgresConfig {
            database_url: db_url.to_string(),
            max_connections: 20,
        };

        let common = PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            storage_type: StorageType::Postgres,
            rpc_url: rpc_url.to_string(),
            fallback_rpc_url: None,
            source_rpc_url: Some(channel_rpc_url.to_string()),
            postgres,
            escrow_instance_id: Some(instance),
        };

        let indexer = IndexerConfig {
            datasource_type: DatasourceType::Yellowstone,
            rpc_polling: Some(RpcPollingConfig {
                from_slot: None,
                poll_interval_ms: 200,
                error_retry_interval_ms: 1_000,
                batch_size: 10,
                encoding: UiTransactionEncoding::Json,
                commitment: CommitmentLevel::Finalized,
            }),
            yellowstone: Some(YellowstoneConfig {
                endpoint: geyser_endpoint.to_string(),
                x_token: None,
                commitment: "finalized".to_string(),
            }),
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
            },
        };

        common.validate().expect("common config");
        indexer.validate().expect("indexer config");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("indexer runtime");
        let task = runtime.spawn(async move {
            private_channel_indexer::indexer::run(common, indexer, None).await
        });

        Self {
            runtime: Some(runtime),
            task,
        }
    }

    /// Drop every task the indexer is running. Shutting the runtime down in the background
    /// rather than dropping it keeps this callable from inside the test's own runtime.
    fn stop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl Drop for EscrowIndexer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The reconciliation config the runtime tests drive: fast ticks, no tolerance, and a
/// webhook the test can assert on.
fn tick_config(webhook_url: String) -> OperatorConfig {
    OperatorConfig {
        db_poll_interval: Duration::from_millis(500),
        batch_size: 10,
        retry_max_attempts: 15,
        retry_base_delay: Duration::from_millis(500),
        channel_buffer_size: 100,
        rpc_commitment: CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        reconciliation_interval: TICK_INTERVAL,
        reconciliation_tolerance_bps: 0,
        reconciliation_webhook_url: Some(webhook_url),
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: 400,
    }
}

/// Spawn `run_reconciliation` against the real validator for custody and the mock for
/// channel supply, the split a single-validator harness has to make.
fn spawn_reconciliation(
    db_url: &str,
    rpc_url: &str,
    channel_rpc_url: &str,
    instance: Pubkey,
    config: OperatorConfig,
    health: Arc<HealthState>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let db_url = db_url.to_string();
    let rpc_url = rpc_url.to_string();
    let channel_rpc_url = channel_rpc_url.to_string();
    tokio::spawn(async move {
        let storage = Arc::new(Storage::Postgres(
            PostgresDb::new(&PostgresConfig {
                database_url: db_url,
                max_connections: 5,
            })
            .await
            .expect("reconciliation storage"),
        ));
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            rpc_url,
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        ));
        let channel_rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            channel_rpc_url,
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        ));
        if let Err(e) = run_reconciliation(
            storage,
            config,
            rpc,
            channel_rpc,
            instance,
            Some(health),
            cancel,
        )
        .await
        {
            tracing::error!("Reconciliation task error: {}", e);
        }
    })
}

/// Deposit into the escrow instance, for either token program.
async fn deposit(
    client: &RpcClient,
    user: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    token_program: Pubkey,
    amount: u64,
) -> Result<Signature, Box<dyn std::error::Error>> {
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    let deposit_ix = DepositBuilder::new()
        .payer(user.pubkey())
        .user(user.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .user_ata(get_associated_token_address_with_program_id(
            &user.pubkey(),
            &mint,
            &token_program,
        ))
        .instance_ata(get_associated_token_address_with_program_id(
            &instance,
            &mint,
            &token_program,
        ))
        .system_program(solana_system_interface::program::ID)
        .token_program(token_program)
        .associated_token_program(spl_associated_token_account::ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(amount)
        .instruction();

    helpers::send_and_confirm_instructions(client, &[deposit_ix], user, &[user], "Deposit").await
}

/// Seed the withdrawal row an operator would be working on: a real nonce, an amount, and a
/// status that has not settled. Nothing about it is released until a payout is observed.
async fn seed_pending_withdrawal(
    storage: &Storage,
    mint: Pubkey,
    amount: u64,
    nonce: i64,
) -> String {
    let signature = Signature::new_unique().to_string();
    let mut withdrawal = DbTransactionBuilder::new(signature.clone(), 1, mint.to_string(), amount)
        .initiator(Pubkey::new_unique().to_string())
        .recipient(Pubkey::new_unique().to_string())
        .transaction_type(TransactionType::Withdrawal)
        .build();
    withdrawal.withdrawal_nonce = Some(nonce);
    storage
        .insert_db_transaction(&withdrawal)
        .await
        .expect("seed withdrawal");
    signature
}

/// Wait until the escrow's finalized custody of `mint` reads `expected`, which is the
/// reading every reconciliation sweep takes.
async fn wait_for_finalized_custody(
    rpc_url: &str,
    instance: Pubkey,
    mint: Pubkey,
    token_program: Pubkey,
    expected: u64,
) {
    let client = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::finalized());
    let ata = get_associated_token_address_with_program_id(&instance, &mint, &token_program);
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(balance) = client.get_token_account_balance(&ata).await {
            if balance.amount.parse::<u64>().unwrap_or(u64::MAX) == expected {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for finalized custody of {mint} to reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Wait until the indexer has committed a checkpoint at all, which is the first proof its
/// live stream is running. Anything sent before that could be missed entirely.
async fn wait_for_live_indexer(pool: &PgPool, indexer: &mut EscrowIndexer) {
    let deadline = Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    loop {
        if db::get_checkpoint_slot(pool, "escrow")
            .await
            .expect("checkpoint read")
            .is_some()
        {
            return;
        }
        assert!(
            !indexer.task.is_finished(),
            "the indexer exited before committing its first checkpoint"
        );
        assert!(
            Instant::now() < deadline,
            "the indexer never committed a first checkpoint"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Assert the mint was registered by an indexed AllowMint and is allowed there. Without it
/// the ledger has no row for the mint and every comparison below would be vacuous.
async fn assert_mint_registered_and_allowed(pool: &PgPool, mint: Pubkey) {
    let mints: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mints WHERE mint_address = $1")
        .bind(mint.to_string())
        .fetch_one(pool)
        .await
        .expect("mints read");
    assert_eq!(mints, 1, "the AllowMint for {mint} was never indexed");

    let allowed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mint_status_history WHERE mint_address = $1 AND status = 'allowed'",
    )
    .bind(mint.to_string())
    .fetch_one(pool)
    .await
    .expect("mint status read");
    assert_eq!(allowed, 1, "{mint} was never indexed as allowed");
}

/// Wait until the deposit is both indexed and covered by a committed checkpoint, which is
/// what makes the ledger exact at that slot for every later reader.
async fn wait_for_indexed_and_checkpointed(
    pool: &PgPool,
    indexer: &mut EscrowIndexer,
    signature: &str,
) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    let slot = loop {
        if let Some(row) = db::get_transaction(pool, signature).await.expect("tx read") {
            break row.slot;
        }
        assert!(
            !indexer.task.is_finished(),
            "the indexer exited before indexing {signature}"
        );
        assert!(Instant::now() < deadline, "{signature} was never indexed");
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    wait_for_checkpoint_to_cover(pool, indexer, slot as u64).await;
    slot
}

/// Wait until the committed checkpoint reaches `slot`, which is what makes the ledger exact
/// there: every deposit and every release up to it is written.
async fn wait_for_checkpoint_to_cover(pool: &PgPool, indexer: &mut EscrowIndexer, slot: u64) {
    let deadline = Instant::now() + Duration::from_secs(STARTUP_TIMEOUT_SECS);
    loop {
        if db::get_checkpoint_slot(pool, "escrow")
            .await
            .expect("checkpoint read")
            .is_some_and(|committed| committed >= slot)
        {
            return;
        }
        assert!(
            !indexer.task.is_finished(),
            "the indexer exited before committing a checkpoint covering slot {slot}: {:?}",
            (&mut indexer.task).await
        );
        assert!(
            Instant::now() < deadline,
            "the checkpoint never reached slot {slot}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The real startup comparison at the strictest threshold, against a snapshot the running
/// indexer has caught its checkpoint up to. A boot reaches the same call through a block
/// fill, which this validator serves too unreliably to depend on.
async fn startup_comparison_at_threshold_zero(
    rpc_url: &str,
    channel_rpc_url: &str,
    pool: &PgPool,
    storage: &Storage,
    indexer: &mut EscrowIndexer,
    instance: Pubkey,
) -> Result<(), IndexerError> {
    let snapshot = capture_custody_snapshot(rpc_url, &instance)
        .await
        .expect("custody snapshot");
    wait_for_checkpoint_to_cover(pool, indexer, snapshot.slot).await;
    reconcile_against_snapshot(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
        ProgramType::Escrow,
        storage,
        rpc_url,
        Some(channel_rpc_url),
        &instance,
        &snapshot,
    )
    .await
}

/// The same comparison with nothing waiting for the ledger to catch up, which is what a boot
/// gets while the escrow indexer is behind the custody reading.
async fn startup_comparison_over_a_lagging_ledger(
    rpc_url: &str,
    channel_rpc_url: &str,
    storage: &Storage,
    instance: Pubkey,
) -> Result<(), IndexerError> {
    let snapshot = capture_custody_snapshot(rpc_url, &instance)
        .await
        .expect("custody snapshot");
    reconcile_against_snapshot(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
        ProgramType::Escrow,
        storage,
        rpc_url,
        Some(channel_rpc_url),
        &instance,
        &snapshot,
    )
    .await
}

/// The committed escrow checkpoint, which a stopped indexer must have left behind.
async fn committed_checkpoint(pool: &PgPool) -> u64 {
    db::get_checkpoint_slot(pool, "escrow")
        .await
        .expect("checkpoint read")
        .expect("a stopped indexer must leave a checkpoint behind")
}

/// Read the committed checkpoint until it stops moving, and return where it stopped. A
/// stopped indexer still has buffered slots to write, so the frozen value is the only one
/// the staging below can be measured against.
async fn wait_for_checkpoint_to_freeze(pool: &PgPool) -> u64 {
    const STILL_FOR: Duration = Duration::from_secs(8);

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last = committed_checkpoint(pool).await;
    let mut unchanged_since = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let current = committed_checkpoint(pool).await;
        if current == last {
            if unchanged_since.elapsed() >= STILL_FOR {
                println!("  Ledger frozen at checkpoint {last}");
                return last;
            }
        } else {
            last = current;
            unchanged_since = Instant::now();
        }
        assert!(
            Instant::now() < deadline,
            "the checkpoint never stopped moving after the indexer was stopped"
        );
    }
}

/// Wait until a real custody sweep reports a slot the stopped ledger does not cover. Only
/// the sweep can settle this: its slot is the one a tick pins its ledger read to, and it
/// trails the chain's head. The margin keeps a slightly later tick from landing back on it.
async fn wait_for_custody_to_outrun_the_ledger(rpc_url: &str, instance: Pubkey, committed: u64) {
    const MARGIN: u64 = 4;

    let rpc = RpcClientWithRetry::with_retry_config(
        rpc_url.to_string(),
        RetryConfig::default(),
        CommitmentConfig::confirmed(),
    );
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let snapshot = fetch_escrow_balances_by_mint(&rpc, instance)
            .await
            .expect("custody sweep");
        if snapshot.slot > committed + MARGIN {
            println!(
                "  Custody reads at slot {} over checkpoint {committed}",
                snapshot.slot
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "custody never read above the stopped checkpoint {committed}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Poll the durable halt flag until it is set, failing if it never is. The indexer is
/// watched alongside it, so an indexer that died reports itself instead of a bare timeout.
async fn wait_for_halt(
    storage: &Storage,
    indexer: &mut EscrowIndexer,
    timeout: Duration,
) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(halt) = storage.is_reconciliation_halted().await.expect("halt read") {
            return halt.reason;
        }
        assert!(
            !indexer.task.is_finished(),
            "the indexer exited before its checkpoint could catch up: {:?}",
            (&mut indexer.task).await
        );
        assert!(
            Instant::now() < deadline,
            "reconciliation never halted within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Assert the halt flag stays clear for `secs`, failing the moment it is set.
async fn assert_stays_unhalted(storage: &Storage, secs: u64, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if let Some(halt) = storage.is_reconciliation_halted().await.expect("halt read") {
            panic!(
                "{context}: reconciliation halted when it must not have: {}",
                halt.reason
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn observed_release_count(pool: &PgPool, nonce: i64) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM observed_releases WHERE withdrawal_nonce = $1")
        .bind(nonce)
        .fetch_one(pool)
        .await
        .expect("observed_releases read")
}

async fn transaction_status(pool: &PgPool, signature: &str) -> String {
    sqlx::query_scalar("SELECT status::text FROM transactions WHERE signature = $1")
        .bind(signature)
        .fetch_one(pool)
        .await
        .expect("status read")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A drain the supply invariant cannot see, in three phases: the halt it earns, the startup
/// comparison that refuses the same shortfall, and the silence once the indexer is stopped
/// and the ledger can no longer be pinned to the custody slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_drain_halts_the_runtime_refuses_the_next_startup_and_holds_while_unpinnable() {
    println!("=== Liability Reconciliation: Real Drain ===");

    let (validator, faucet, geyser_port) = start_test_validator().await;
    let geyser_endpoint = format!("http://127.0.0.1:{geyser_port}");
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let (_pg, pool, db_url, storage) = start_postgres("liability_drain").await;

    let channel = MockRpcServer::start().await;
    mock_absent_channel_mint(&channel);

    // The escrow has nothing yet, so the indexer starts against an empty ledger and every
    // later row reaches it through the live stream.
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).expect("admin keypair");
    let (_, instance) = TestEnvironment::setup_instance(&client, &faucet, None)
        .await
        .expect("instance");
    let mut indexer = EscrowIndexer::start(
        &db_url,
        &geyser_endpoint,
        &rpc_url,
        &channel.url(),
        instance,
    );
    wait_for_live_indexer(&pool, &mut indexer).await;

    // A Token-2022 mint whose permanent delegate can empty any account for it.
    let delegate = Keypair::new();
    let drainer = Keypair::new();
    let user = Keypair::new();
    helpers::setup_wallets(&client, &faucet, &[&admin, &delegate, &user])
        .await
        .expect("fund wallets");
    let mint = generate_permanent_delegate_mint_2022(
        &client,
        &admin,
        &admin,
        &delegate.pubkey(),
        &Keypair::new(),
        MINT_DECIMALS,
    )
    .await
    .expect("permanent-delegate mint");
    allow_mint_for_program(&client, &admin, instance, mint, TOKEN_2022_PROGRAM_ID)
        .await
        .expect("allow mint");

    // A real deposit: custody rises and the ledger records what the escrow now owes.
    mint_2022_to_owner(&client, &admin, mint, user.pubkey(), &admin, USER_BALANCE)
        .await
        .expect("fund user");
    let deposit_sig = deposit(
        &client,
        &user,
        instance,
        mint,
        TOKEN_2022_PROGRAM_ID,
        DEPOSIT_AMOUNT,
    )
    .await
    .expect("deposit")
    .to_string();
    wait_for_indexed_and_checkpointed(&pool, &mut indexer, &deposit_sig).await;
    assert_mint_registered_and_allowed(&pool, mint).await;
    println!("  Deposit {DEPOSIT_AMOUNT} indexed and checkpointed");

    // A withdrawal the operator has not settled. No payout has been observed for it, so it
    // is still owed and must not be subtracted from what the escrow has to cover.
    let withdrawal_sig = seed_pending_withdrawal(&storage, mint, WITHDRAWAL_AMOUNT, 0).await;

    let escrow_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_2022_PROGRAM_ID);
    drain_via_permanent_delegate(
        &client,
        &admin,
        mint,
        escrow_ata,
        &delegate,
        drainer.pubkey(),
        DRAIN_AMOUNT,
        MINT_DECIMALS,
    )
    .await
    .expect("drain");
    wait_for_finalized_custody(
        &rpc_url,
        instance,
        mint,
        TOKEN_2022_PROGRAM_ID,
        DEPOSIT_AMOUNT - DRAIN_AMOUNT,
    )
    .await;
    println!("  Drained {DRAIN_AMOUNT} with no escrow instruction to explain it");

    let mut webhook = mockito::Server::new_async().await;
    let halt_alert = webhook
        .mock("POST", "/")
        .with_status(200)
        .expect_at_least(1)
        .create_async()
        .await;

    let health = HealthState::new(HealthConfig::operator());
    let cancel = CancellationToken::new();
    let ticks = spawn_reconciliation(
        &db_url,
        &rpc_url,
        &channel.url(),
        instance,
        tick_config(webhook.url()),
        health.clone(),
        cancel.clone(),
    );
    let reason = wait_for_halt(
        &storage,
        &mut indexer,
        Duration::from_secs(STARTUP_TIMEOUT_SECS),
    )
    .await;
    cancel.cancel();
    let _ = ticks.await;

    assert!(
        reason.contains(&mint.to_string()),
        "the halt reason must name the mint: {reason}"
    );
    assert!(
        reason.contains("ledger liabilities"),
        "the halt must be the liability arm, not the supply arm: {reason}"
    );
    assert!(
        reason.contains(&DEPOSIT_AMOUNT.to_string()),
        "liabilities must still carry the unsettled withdrawal's amount: {reason}"
    );
    halt_alert.assert_async().await;
    assert!(
        !health.is_healthy(),
        "a halt must force the operator unhealthy"
    );
    assert_eq!(
        transaction_status(&pool, &withdrawal_sig).await,
        "manual_review",
        "the halt must quarantine the active withdrawal"
    );
    println!("  Halted on the liability arm: {reason}");

    // The same shortfall, seen by the startup comparison rather than by a tick.
    let result = startup_comparison_at_threshold_zero(
        &rpc_url,
        &channel.url(),
        &pool,
        &storage,
        &mut indexer,
        instance,
    )
    .await;
    match result {
        Err(IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
            threshold,
            ..
        })) => assert_eq!(
            threshold, 0,
            "the strict threshold must be the one enforced"
        ),
        other => panic!("expected a startup mismatch halt, got {other:?}"),
    }
    println!("  Startup comparison refused the same shortfall");

    // Now take the ledger away. The shortfall is unchanged and permanent, so an arm that
    // judged an unpinnable ledger would halt again within three ticks; it must not.
    storage
        .clear_reconciliation_halt()
        .await
        .expect("clear halt");
    indexer.stop();
    let frozen = wait_for_checkpoint_to_freeze(&pool).await;
    wait_for_custody_to_outrun_the_ledger(&rpc_url, instance, frozen).await;

    let cancel = CancellationToken::new();
    let ticks = spawn_reconciliation(
        &db_url,
        &rpc_url,
        &channel.url(),
        instance,
        tick_config(webhook.url()),
        HealthState::new(HealthConfig::operator()),
        cancel.clone(),
    );
    assert_stays_unhalted(
        &storage,
        UNPINNABLE_QUIET_SECS,
        "ledger behind the custody slot",
    )
    .await;
    cancel.cancel();
    let _ = ticks.await;
    println!("  Held for {UNPINNABLE_QUIET_SECS}s with the ledger behind the custody slot");

    // A tick may hold on a ledger it cannot pin, but a boot must not: nothing in the ledger
    // accounts for the missing custody, by release or by status, so startup still refuses.
    match startup_comparison_over_a_lagging_ledger(&rpc_url, &channel.url(), &storage, instance)
        .await
    {
        Err(IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
            threshold,
            ..
        })) => assert_eq!(
            threshold, 0,
            "the strict threshold must be the one enforced"
        ),
        other => panic!("a boot over a lagging ledger must still refuse a drain, got {other:?}"),
    }
    println!("  Startup comparison refused the drain with the ledger behind custody");

    channel.shutdown().await;
}

/// A real payout under running ticks and then through the startup comparison. Custody drops
/// the instant the release lands while the row stays unsettled, so only the observed payout,
/// never the row's status, can account for what left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_release_neither_halts_the_runtime_nor_the_next_startup() {
    println!("=== Liability Reconciliation: Real Release ===");

    const NONCE: u64 = 0;

    let (validator, faucet, geyser_port) = start_test_validator().await;
    let geyser_endpoint = format!("http://127.0.0.1:{geyser_port}");
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let (_pg, pool, db_url, storage) = start_postgres("liability_release").await;

    let channel = MockRpcServer::start().await;
    mock_absent_channel_mint(&channel);

    // The instance address is derived before anything exists on-chain, so the indexer can be
    // watching it from empty and every row below arrives through the live stream.
    let instance_seed = Keypair::new();
    let (instance, _) = find_instance_pda(&instance_seed.pubkey());
    let mut indexer = EscrowIndexer::start(
        &db_url,
        &geyser_endpoint,
        &rpc_url,
        &channel.url(),
        instance,
    );
    wait_for_live_indexer(&pool, &mut indexer).await;

    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).expect("admin keypair");
    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, Some(instance_seed))
        .await
        .expect("environment");
    assert_eq!(
        env.instance, instance,
        "the derived instance must be the one created"
    );
    TestEnvironment::setup_operator(&client, &faucet, instance)
        .await
        .expect("operator");
    let mint = env.mint;
    let user = &env.users[0];

    let deposit_sig = deposit(
        &client,
        user,
        instance,
        mint,
        spl_token::id(),
        DEPOSIT_AMOUNT,
    )
    .await
    .expect("deposit")
    .to_string();
    wait_for_indexed_and_checkpointed(&pool, &mut indexer, &deposit_sig).await;
    assert_mint_registered_and_allowed(&pool, mint).await;
    println!("  Deposit {DEPOSIT_AMOUNT} indexed and checkpointed");

    // The row the payout below will discharge. It never reaches `completed`, because no
    // operator runs here, so only the observed release can account for it.
    let withdrawal_sig =
        seed_pending_withdrawal(&storage, mint, WITHDRAWAL_AMOUNT, NONCE as i64).await;

    // Any webhook at all would mean something alerted; nothing here should.
    let mut webhook = mockito::Server::new_async().await;
    let no_alert = webhook
        .mock("POST", "/")
        .with_status(200)
        .expect(0)
        .create_async()
        .await;

    let health = HealthState::new(HealthConfig::operator());
    let cancel = CancellationToken::new();
    let ticks = spawn_reconciliation(
        &db_url,
        &rpc_url,
        &channel.url(),
        instance,
        tick_config(webhook.url()),
        health.clone(),
        cancel.clone(),
    );

    // Let a few ticks run against the undisturbed ledger, then pay out underneath them.
    assert_stays_unhalted(&storage, 5, "before the payout").await;
    helpers::release_funds_on_chain(
        &client,
        &admin,
        instance,
        mint,
        user.pubkey(),
        WITHDRAWAL_AMOUNT,
        NONCE,
    )
    .await
    .expect("release funds");
    println!("  Released {WITHDRAWAL_AMOUNT} while the ticks were running");

    wait_for_finalized_custody(
        &rpc_url,
        instance,
        mint,
        spl_token::id(),
        DEPOSIT_AMOUNT - WITHDRAWAL_AMOUNT,
    )
    .await;
    // Ticks keep running across the drop in custody and well past it.
    assert_stays_unhalted(&storage, 30, "across the payout").await;
    cancel.cancel();
    let _ = ticks.await;

    no_alert.assert_async().await;
    assert!(
        health.is_healthy(),
        "an ordinary payout must leave the operator healthy"
    );
    assert_eq!(
        observed_release_count(&pool, NONCE as i64).await,
        1,
        "the payout must have been indexed, or the ticks proved nothing"
    );
    assert_ne!(
        transaction_status(&pool, &withdrawal_sig).await,
        "completed",
        "the row must still be unsettled, so only the observed release can explain custody"
    );

    // The same subtraction, seen by the startup comparison rather than by a tick.
    startup_comparison_at_threshold_zero(
        &rpc_url,
        &channel.url(),
        &pool,
        &storage,
        &mut indexer,
        instance,
    )
    .await
    .expect("a released withdrawal must not stop startup");
    println!("  Startup comparison accepted the released withdrawal");

    indexer.stop();
    channel.shutdown().await;
}
