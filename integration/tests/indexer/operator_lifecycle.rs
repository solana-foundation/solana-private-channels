//! Integration tests for the operator lifecycle.
//!
//! Each test starts an isolated Postgres container and a Solana test validator
//! (without Geyser), inserts pending transactions directly via the storage
//! layer, and asserts that the operator pipeline processes them correctly.
//!
//! Scenarios covered:
//! 1. Single deposit mint: operator mints channel tokens for one pending deposit.
//! 2. Issuance idempotency: duplicate deposit row does not trigger a double-mint.
//! 3. Withdrawal nonce idempotency: duplicate withdrawal row releases funds only once.
//! 4. Failure alerts: a failed withdrawal fires a webhook POST; a preflight-failed
//!    mint is left Processing for recovery rather than terminalized.
//! 5. Batch deposits: operator processes 5 deposits for distinct recipients in one sweep.
//! 6. Idle operator: no phantom records created when the DB has no pending work.
//! 7. Runtime reconciliation halt: channel supply over-issued beyond escrow custody
//!    trips the durable halt, forced-unhealthy latch, quarantine, and webhook alert.
//! 8. Sequential withdrawals: two consecutive withdrawal nonces both complete correctly.
//! 9. Boot bitmap diff: a database that claims a release the chain never made must
//!    refuse to start, while a release the chain made but the database never
//!    recorded must be reconciled in place and let the operator start.
//! 10. Double release: the same nonce submitted under two different rows must move
//!     tokens exactly once, and the loser must not be reminted.

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

use chrono::Utc;
use helpers::private_channel_node::start_private_channel_node;
use helpers::test_types::WAIT_TIMEOUT_SECS;
use helpers::{db, generate_mint, get_token_balance, mint_to_owner, operator_util};
use mockito::Server;
use private_channel_indexer::config::{
    OperatorConfig, PrivateChannelIndexerConfig, ProgramType, StorageType,
};
use private_channel_indexer::operator;
use private_channel_indexer::operator::reconciliation::run_reconciliation;
use private_channel_indexer::operator::{RetryConfig, RpcClientWithRetry};
use private_channel_indexer::storage::common::amount::TokenAmount;
use private_channel_indexer::storage::common::models::{
    DbMint, DbMintStatus, DbTransaction, DbTransactionBuilder, TransactionStatus,
};
use private_channel_indexer::storage::{PostgresDb, Storage, TransactionType};
use private_channel_indexer::PostgresConfig;
use private_channel_metrics::{HealthConfig, HealthState};
use setup::{TestEnvironment, TEST_ADMIN_KEYPAIR};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use std::sync::Arc;
use std::time::Duration;
use test_utils::operator_helper::same_host_fallback_url;
use test_utils::operator_helper::start_solana_to_private_channel_operator;
use test_utils::operator_helper::OperatorHandle;
use test_utils::validator_helper::start_test_validator_no_geyser;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn seed_mint_status_allowed(
    storage: &Storage,
    mint_address: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            mint_address: mint_address.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{mint_address}"),
            created_at: Utc::now(),
        }])
        .await?;
    Ok(())
}

fn default_operator_config(alert_url: Option<String>) -> OperatorConfig {
    OperatorConfig {
        db_poll_interval: Duration::from_millis(500),
        batch_size: 10,
        retry_max_attempts: 15,
        retry_base_delay: Duration::from_millis(500),
        channel_buffer_size: 100,
        rpc_commitment: solana_sdk::commitment_config::CommitmentLevel::Confirmed,
        alert_webhook_url: alert_url,
        reconciliation_interval: Duration::from_secs(5 * 60),
        reconciliation_tolerance_bps: 10,
        // Escrow operators refuse to start without a reconciliation webhook; a
        // placeholder satisfies the startup gate for harnesses that don't assert
        // delivery. Tests that check delivery override this with a mock URL.
        reconciliation_webhook_url: Some("http://127.0.0.1:0/recon-test".to_string()),
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: 400,
    }
}

fn set_operator_env_vars(keypair: &Keypair) {
    let private_key_base58 = bs58::encode(keypair.to_bytes()).into_string();
    std::env::set_var("ADMIN_SIGNER", "memory");
    std::env::set_var("ADMIN_PRIVATE_KEY", &private_key_base58);
    std::env::set_var("OPERATOR_SIGNER", "memory");
    std::env::set_var("OPERATOR_PRIVATE_KEY", &private_key_base58);
}

async fn start_operator_with_alert(
    program_type: ProgramType,
    rpc_url: String,
    db_url: String,
    operator_keypair: Keypair,
    instance: solana_sdk::pubkey::Pubkey,
    alert_url: Option<String>,
) -> Result<OperatorHandle, Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url: db_url.clone(),
        max_connections: 10,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));

    let common_config = PrivateChannelIndexerConfig {
        program_type,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.clone(),
        // Withdraw operator requires a source chain for remints; single-validator
        // test, so point it at the same RPC. Harmless for Escrow callers.
        source_rpc_url: Some(rpc_url.clone()),
        // The withdraw operator now requires an independent, same-cluster fallback.
        // Reach the same node via a distinct host string so the URL differs while
        // the genesis hash matches. Harmless for Escrow callers (never validated).
        fallback_rpc_url: Some(same_host_fallback_url(&rpc_url)),
        postgres: postgres_config,
        escrow_instance_id: Some(instance),
    };

    let operator_config = default_operator_config(alert_url);

    set_operator_env_vars(&operator_keypair);

    let task_handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = operator::run(storage, common_config, operator_config, None).await {
            tracing::error!("Operator error: {}", e);
        }
    });

    Ok(OperatorHandle {
        _handle: task_handle,
    })
}

/// Start an operator task with a fully custom [`OperatorConfig`].
///
/// Use this when a test needs to override reconciliation intervals,
/// tolerance settings, or other config values beyond the alert-only defaults
/// provided by [`start_operator_with_alert`].
#[allow(dead_code)]
async fn start_operator_with_config(
    program_type: ProgramType,
    rpc_url: String,
    db_url: String,
    operator_keypair: Keypair,
    instance: solana_sdk::pubkey::Pubkey,
    operator_config: OperatorConfig,
) -> Result<OperatorHandle, Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url: db_url.clone(),
        max_connections: 10,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));

    let common_config = PrivateChannelIndexerConfig {
        program_type,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.clone(),
        // Withdraw operator requires a source chain for remints; single-validator
        // test, so point it at the same RPC. Harmless for Escrow callers.
        source_rpc_url: Some(rpc_url.clone()),
        // The withdraw operator now requires an independent, same-cluster fallback.
        // Reach the same node via a distinct host string so the URL differs while
        // the genesis hash matches. Harmless for Escrow callers (never validated).
        fallback_rpc_url: Some(same_host_fallback_url(&rpc_url)),
        postgres: postgres_config,
        escrow_instance_id: Some(instance),
    };

    set_operator_env_vars(&operator_keypair);

    let task_handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = operator::run(storage, common_config, operator_config, None).await {
            tracing::error!("Operator error: {}", e);
        }
    });

    Ok(OperatorHandle {
        _handle: task_handle,
    })
}

/// Poll until the row reaches one of `expected_statuses`, returning the one it
/// reached. A permanently-failed withdrawal is resolved by the deferred-remint
/// gate, which can land on either a completed remint or an escalation, so the
/// caller must accept both.
async fn wait_for_any_transaction_status(
    pool: &sqlx::PgPool,
    signature: &str,
    expected_statuses: &[&str],
    timeout_secs: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if let Some(tx) = db::get_transaction(pool, signature).await? {
            if expected_statuses.contains(&tx.status.as_str()) {
                return Ok(tx.status);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "Transaction {signature} did not reach any of {expected_statuses:?} within {timeout_secs}s"
    )
    .into())
}

/// Poll until the deposit's write-ahead mint signature is journaled in
/// `pending_release_signatures`. The journal is written right before broadcast,
/// so a non-empty journal proves the mint reached the send step.
///
/// Unused until the ownership-checked write-ahead persist lands; the assertion
/// it serves currently expects the send error to terminalize the mint instead.
#[allow(dead_code)]
async fn wait_for_release_signature_journaled(
    pool: &sqlx::PgPool,
    signature: &str,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM pending_release_signatures prs \
             JOIN transactions t ON t.id = prs.transaction_id \
             WHERE t.signature = $1",
        )
        .bind(signature)
        .fetch_one(pool)
        .await?;
        if count > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "deposit {} did not journal a write-ahead mint signature within {}s",
        signature, timeout_secs
    )
    .into())
}

/// Consume a withdrawal nonce on-chain directly, without going through the
/// operator.
///
/// Staging a boot-time divergence needs a bit that is genuinely set while the
/// database knows nothing about it. Letting an operator do it and then deleting
/// the row does not work: `OperatorHandle::shutdown` only detaches the task, so
/// the first operator keeps running and keeps the sender's advisory lock, and a
/// second one started against the same database exits immediately. Sending the
/// release from the test sidesteps operator lifecycle entirely.
async fn release_nonce_on_chain(
    client: &RpcClient,
    admin: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    user: Pubkey,
    amount: u64,
    nonce: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use private_channel_escrow_program_client::instructions::ReleaseFundsBuilder;
    use private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use private_channel_indexer::operator::{
        find_allowed_mint_pda, find_event_authority_pda, find_operator_pda,
        find_withdrawal_bitmap_pda,
    };
    use spl_associated_token_account::get_associated_token_address_with_program_id;

    let token_program = spl_token::id();
    let release_ix = ReleaseFundsBuilder::new()
        .payer(admin.pubkey())
        .operator(admin.pubkey())
        .instance(instance)
        .withdrawal_bitmap(find_withdrawal_bitmap_pda(&instance))
        .operator_pda(find_operator_pda(&instance, &admin.pubkey()))
        .mint(mint)
        .allowed_mint(find_allowed_mint_pda(&instance, &mint))
        .user_ata(get_associated_token_address_with_program_id(
            &user,
            &mint,
            &token_program,
        ))
        .instance_ata(get_associated_token_address_with_program_id(
            &instance,
            &mint,
            &token_program,
        ))
        .token_program(token_program)
        .associated_token_program(spl_associated_token_account::id())
        .event_authority(find_event_authority_pda())
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(amount)
        .user(user)
        .transaction_nonce(nonce)
        .instruction();

    helpers::send_and_confirm_instructions(
        client,
        &[release_ix],
        admin,
        &[admin],
        "Release Funds (direct)",
    )
    .await?;
    Ok(())
}

fn make_withdrawal_transaction(
    signature: String,
    mint: String,
    recipient: String,
    amount: u64,
    nonce: i64,
) -> DbTransaction {
    let now = Utc::now();
    DbTransaction {
        id: 0,
        signature,
        trace_id: Uuid::new_v4().to_string(),
        slot: 1,
        initiator: recipient.clone(),
        recipient,
        mint,
        amount: TokenAmount(amount),
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(nonce),
        status: TransactionStatus::Pending,
        created_at: now,
        updated_at: now,
        processed_at: None,
        counterpart_signature: None,
        remint_signatures: None,
        remint_last_valid_block_heights: None,
        pending_remint_deadline_at: None,
        finality_check_attempts: 0,
        recovery_requeue_attempts: 0,
        instruction_index: 0,
        inner_index: None,
        landed_remint_signature: None,
        release_refused_on_chain: false,
    }
}

/// Happy path: a single pending deposit is picked up by the Solana→PrivateChannel operator,
/// minted on the channel, and the DB row transitions to `completed` with a
/// non-null `counterpart_signature`.
///
/// Inserts one deposit directly via `storage.insert_db_transaction()` rather
/// than going through the on-chain indexer, keeping the test self-contained.
#[tokio::test(flavor = "multi_thread")]
async fn test_deposit_operator_processes_single_mint() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Single Deposit Mint ===");

    // 1. Start validator + Postgres, TestEnvironment::setup() (1 user)
    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_lifecycle")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_lifecycle",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;

    // deposit gate refuses to mint unless the mint was in `allowed`
    // status at the deposit's slot. This test bypasses the indexer, so no
    // `AllowMint` event is ingested, seed both rows manually to mirror
    // what `convert_to_db_models` + `finalize_and_checkpoint` would have
    // produced in production.
    storage
        .upsert_mints_batch(&[DbMint::new(
            env.mint.to_string(),
            6,
            spl_token::id().to_string(),
        )])
        .await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    // 2. Insert 1 pending deposit directly via storage.insert_db_transaction()
    let signature = Signature::new_unique().to_string();
    let recipient = env.users[0].pubkey().to_string();
    let amount = 50_000u64;

    let deposit_txn = DbTransactionBuilder::new(signature.clone(), 1, env.mint.to_string(), amount)
        .initiator(recipient.clone())
        .recipient(recipient)
        .transaction_type(TransactionType::Deposit)
        .build();

    storage.insert_db_transaction(&deposit_txn).await?;

    // 3. Start a PrivateChannel core node as the mint target. It reports every
    // found transaction as `finalized` instantly, so the operator's finalized
    // mint gate completes on the first poll - mirroring production, where mints
    // land on the PC node rather than slow Solana.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let pc_node = start_private_channel_node(operator_keypair.pubkey()).await?;

    // rpc_url (mint target) = PC node; the escrow instance stays on Solana.
    let operator_handle = start_solana_to_private_channel_operator(
        pc_node.url.clone(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    // 4. wait_for_transaction_completion(pool, sig, 180s)
    operator_util::wait_for_transaction_completion(&pool, &signature).await?;

    // 5. Assert status = "completed", counterpart_signature.is_some()
    let db_tx = db::get_transaction(&pool, &signature)
        .await?
        .expect("Transaction not found in DB");
    assert_eq!(db_tx.status, "completed");
    assert!(db_tx.counterpart_signature.is_some());

    operator_handle.shutdown().await;
    pc_node.shutdown().await;

    Ok(())
}

/// Inserts the same deposit row twice (same signature), starts the operator,
/// and asserts that the recipient receives exactly `amount` tokens — not `2 ×
/// amount`.  Verifies that the idempotency memo mechanism in `find_existing_
/// mint_signature` prevents a second on-chain mint for an already-processed
/// deposit.
#[tokio::test(flavor = "multi_thread")]
async fn test_issuance_operator_idempotent_no_double_mint() -> Result<(), Box<dyn std::error::Error>>
{
    println!("=== Operator Lifecycle: Issuance Idempotency ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_idempotent")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_idempotent",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    let user_pubkey = env.users[0].pubkey();

    // deposit gate requires an allowed status row for any mint we issue
    // private channel tokens for; the test bypasses the indexer so seed
    // the rows directly. See `test_deposit_operator_processes_single_mint`.
    storage
        .upsert_mints_batch(&[DbMint::new(
            env.mint.to_string(),
            6,
            spl_token::id().to_string(),
        )])
        .await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    let signature = Signature::new_unique().to_string();
    let recipient = user_pubkey.to_string();
    let amount = 50_000u64;

    let deposit_txn = DbTransactionBuilder::new(signature.clone(), 1, env.mint.to_string(), amount)
        .initiator(recipient.clone())
        .recipient(recipient)
        .transaction_type(TransactionType::Deposit)
        .build();
    storage.insert_db_transaction(&deposit_txn).await?;

    // Duplicate insert with same signature should not create a second mint.
    storage.insert_db_transaction(&deposit_txn).await?;

    // Mint target is the PrivateChannel node (instant finality), so balances are
    // checked there, not on the Solana validator. The recipient ATA does not
    // exist on the PC node until the first mint, so treat "before" as 0.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let pc_node = start_private_channel_node(operator_keypair.pubkey()).await?;
    let pc_client = RpcClient::new(pc_node.url.clone());
    let balance_before = get_token_balance(&pc_client, &user_pubkey, &env.mint)
        .await
        .unwrap_or(0);

    let operator_handle = start_solana_to_private_channel_operator(
        pc_node.url.clone(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    operator_util::wait_for_transaction_completion(&pool, &signature).await?;

    let balance_after = get_token_balance(&pc_client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after,
        balance_before + amount,
        "Duplicate deposit should mint only once"
    );

    operator_handle.shutdown().await;
    pc_node.shutdown().await;
    Ok(())
}

/// Inserts a withdrawal row twice (same signature / nonce 0), starts the
/// PrivateChannel→Solana operator, and asserts the user receives `50_000` tokens — not
/// `100_000`.  Confirms that the duplicate DB row does not result in two
/// `ReleaseFunds` instructions being sent to the escrow program.
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_operator_prevents_double_withdrawal(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Withdrawal Nonce Idempotency ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_withdrawal")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_withdrawal",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;

    // Seed mint metadata so the withdrawal operator can build the instruction.
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    // Ensure escrow ATA has funds to withdraw.
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    mint_to_owner(&client, &admin, env.mint, env.instance, &admin, 200_000).await?;

    let user_pubkey = env.users[0].pubkey();
    let initial_balance = get_token_balance(&client, &user_pubkey, &env.mint).await?;

    let withdrawal_sig = Signature::new_unique().to_string();
    let withdrawal_tx = make_withdrawal_transaction(
        withdrawal_sig.clone(),
        env.mint.to_string(),
        user_pubkey.to_string(),
        50_000,
        0,
    );
    // Duplicate insert with same signature must not create a second withdrawal.
    storage.insert_db_transaction(&withdrawal_tx).await?;
    storage.insert_db_transaction(&withdrawal_tx).await?;

    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let operator_handle = start_operator_with_alert(
        ProgramType::Withdraw,
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
        None,
    )
    .await?;

    // Use the env-aware timeout so coverage-instrumented runs (which set
    // PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS=600) don't hit the 180 s ceiling that was
    // tuned for uninstrumented nextest.
    operator_util::wait_for_transaction_completion(&pool, &withdrawal_sig).await?;

    let balance_after = get_token_balance(&client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after,
        initial_balance + 50_000,
        "Duplicate withdrawal must not release funds again"
    );

    operator_handle.shutdown().await;
    Ok(())
}

/// Triggers one preflight-failing mint (wrong-authority `mint_to`) and one bad
/// withdrawal (mint never allowlisted on the escrow).
///
/// Neither terminalizes the operator: the mint journals its signature write-ahead
/// and is left Processing for recovery with no alert, while the withdrawal is
/// parked by the mint gate before a release is built. Only that parking fires a
/// webhook, so the configured `alert_webhook_url` receives exactly one POST via
/// `db_transaction_writer::send_webhook_alert`.
///
/// Uses a `mockito` HTTP server as the webhook endpoint so no external service
/// is required.
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_withdrawal_alerts_and_preflight_mint_defers_to_recovery(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Alerts on Failure ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_alerts")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_alerts",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;

    let mut server = Server::new_async().await;
    // Only the withdrawal's ManualReview disposition fires a webhook; the
    // preflight-failed mint is deferred to recovery and writes no status.
    let alert_mock = server
        .mock("POST", "/")
        .match_header("content-type", "application/json")
        .with_status(200)
        .expect(1)
        .create_async()
        .await;

    // Start Solana -> PrivateChannel operator with alert URL and low retry count so bad
    // transactions fail quickly without exhausting a long wait window.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let fast_fail_config = OperatorConfig {
        retry_max_attempts: 3,
        ..default_operator_config(Some(server.url()))
    };
    let solana_to_private_channel = start_operator_with_config(
        ProgramType::Escrow,
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
        fast_fail_config,
    )
    .await?;

    // Create a valid SPL mint with a *different* mint authority than the operator's
    // admin key. When the operator calls mint_to using the admin key, the SPL token
    // program rejects it (wrong authority), so the mint fails at preflight after the
    // write-ahead signature is persisted.
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let bad_authority = Keypair::new(); // NOT the operator admin — intentionally wrong
    let bad_mint = Keypair::new();
    generate_mint(&client, &admin, &bad_authority, &bad_mint).await?;

    // Register the bad mint in the mints table so the operator's pending-deposit
    // query (which joins with mints) can find and attempt to process this deposit.
    // Without this, the deposit sits in "pending" forever → test hangs.
    let bad_mint_meta = DbMint::new(
        bad_mint.pubkey().to_string(),
        6,
        spl_token::id().to_string(),
    );
    storage.upsert_mints_batch(&[bad_mint_meta]).await?;
    seed_mint_status_allowed(&storage, &bad_mint.pubkey().to_string()).await?;

    let mint_fail_sig = Signature::new_unique().to_string();
    let recipient = env.users[0].pubkey().to_string();
    let bad_deposit = DbTransactionBuilder::new(
        mint_fail_sig.clone(),
        1,
        bad_mint.pubkey().to_string(),
        10_000,
    )
    .initiator(recipient.clone())
    .recipient(recipient)
    .transaction_type(TransactionType::Deposit)
    .build();
    storage.insert_db_transaction(&bad_deposit).await?;

    wait_for_any_transaction_status(&pool, &mint_fail_sig, &["failed"], *WAIT_TIMEOUT_SECS).await?;

    // Seed a separate mint that is NOT allowed on the instance to force withdrawal failure.
    let bad_withdraw_mint = Keypair::new();
    let bad_mint_pubkey = generate_mint(&client, &admin, &admin, &bad_withdraw_mint).await?;
    let mint_meta = DbMint::new(bad_mint_pubkey.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &bad_mint_pubkey.to_string()).await?;
    mint_to_owner(
        &client,
        &admin,
        bad_mint_pubkey,
        env.instance,
        &admin,
        100_000,
    )
    .await?;

    let withdrawal_sig = Signature::new_unique().to_string();
    let withdrawal_tx = make_withdrawal_transaction(
        withdrawal_sig.clone(),
        bad_mint_pubkey.to_string(),
        env.users[0].pubkey().to_string(),
        25_000,
        0,
    );
    storage.insert_db_transaction(&withdrawal_tx).await?;

    // Start PrivateChannel -> Solana operator with same alert URL and low retry count.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let fast_fail_config = OperatorConfig {
        retry_max_attempts: 3,
        ..default_operator_config(Some(server.url()))
    };
    let private_channel_to_solana = start_operator_with_config(
        ProgramType::Withdraw,
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
        fast_fail_config,
    )
    .await?;

    // The bad withdrawal preflights with `invalid account data for instruction`
    // from the escrow program (the mint isn't whitelisted on the instance), so
    // `sign_and_send` errors before any signature is broadcast. With no
    // signatures to verify, the sender's "cannot safely remint" branch
    // (`indexer/src/operator/sender/transaction.rs`) routes the row to
    // `ManualReview`, NOT `Failed` — reverting that to `Failed` would risk
    // double-reminting if the broadcast had succeeded silently.
    wait_for_any_transaction_status(
        &pool,
        &withdrawal_sig,
        &["manual_review"],
        *WAIT_TIMEOUT_SECS,
    )
    .await?;

    alert_mock.assert();

    solana_to_private_channel.shutdown().await;
    private_channel_to_solana.shutdown().await;
    Ok(())
}

/// operator fetches and processes deposits for multiple distinct
/// recipients in a single sweep.
///
/// Seeds 5 pending deposits, one per user, then asserts that every deposit
/// reaches "completed" status and each recipient's token balance increases by
/// exactly the deposited amount.
#[tokio::test(flavor = "multi_thread")]
async fn test_batch_deposits_multiple_recipients() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Batch Deposits ===");

    const NUM_USERS: usize = 5;
    const DEPOSIT_AMOUNT: u64 = 30_000;

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_batch")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_batch",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    // Create 5 users with 0 initial balance so we can verify the exact deposit amount.
    let env = TestEnvironment::setup(&client, &faucet_keypair, NUM_USERS, 0, None).await?;

    // deposit gate requires an allowed status row for any mint we issue
    // private channel tokens for; the test bypasses the indexer so seed
    // the rows directly. See `test_deposit_operator_processes_single_mint`.
    storage
        .upsert_mints_batch(&[DbMint::new(
            env.mint.to_string(),
            6,
            spl_token::id().to_string(),
        )])
        .await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    // Insert one pending deposit per user, each with a unique on-chain signature.
    let mut signatures = Vec::with_capacity(NUM_USERS);
    for user in &env.users {
        let sig = Signature::new_unique().to_string();
        let recipient = user.pubkey().to_string();
        let txn = DbTransactionBuilder::new(sig.clone(), 1, env.mint.to_string(), DEPOSIT_AMOUNT)
            .initiator(recipient.clone())
            .recipient(recipient)
            .transaction_type(TransactionType::Deposit)
            .build();
        storage.insert_db_transaction(&txn).await?;
        signatures.push(sig);
    }

    // Start the Solana -> PrivateChannel operator and wait for all deposits to be processed.
    // The mint target is a PrivateChannel node (instant finality), so balances
    // are verified there rather than on the Solana validator.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let pc_node = start_private_channel_node(operator_keypair.pubkey()).await?;
    let pc_client = RpcClient::new(pc_node.url.clone());
    let operator_handle = start_solana_to_private_channel_operator(
        pc_node.url.clone(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    operator_util::wait_for_operator_completion(&pool, NUM_USERS, "batch deposits").await?;

    // Every deposit must be completed with a counterpart signature, and each
    // user must hold exactly DEPOSIT_AMOUNT tokens (one mint per deposit).
    for (i, sig) in signatures.iter().enumerate() {
        let db_tx = db::get_transaction(&pool, sig)
            .await?
            .expect("Transaction not found in DB");
        assert_eq!(db_tx.status, "completed", "Deposit {i} not completed");
        assert!(
            db_tx.counterpart_signature.is_some(),
            "Deposit {i} missing counterpart signature"
        );

        let balance = get_token_balance(&pc_client, &env.users[i].pubkey(), &env.mint).await?;
        assert_eq!(
            balance, DEPOSIT_AMOUNT,
            "User {i} balance mismatch after deposit"
        );
    }

    operator_handle.shutdown().await;
    pc_node.shutdown().await;
    Ok(())
}

/// Edge case: operator must remain alive and produce no spurious records when
/// the database contains zero pending transactions.
///
/// Lets the operator idle through several polling cycles, then asserts that
/// neither completed nor failed transactions appeared (no phantom processing).
#[tokio::test(flavor = "multi_thread")]
async fn test_operator_idle_no_pending_transactions() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Idle with No Pending Transactions ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_idle")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_idle",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    // Set up the on-chain instance with no users and no pending transactions.
    let env = TestEnvironment::setup(&client, &faucet_keypair, 0, 0, None).await?;

    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let operator_handle = start_solana_to_private_channel_operator(
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    // Run through multiple polling cycles (db_poll_interval = 500 ms default → ~6 cycles).
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The operator must not have created or mutated any records.
    let completed = db::count_transactions_by_status(&pool, "completed").await?;
    let failed = db::count_transactions_by_status(&pool, "failed").await?;
    assert_eq!(
        completed, 0,
        "Expected no completed transactions in idle run"
    );
    assert_eq!(failed, 0, "Expected no failed transactions in idle run");

    operator_handle.shutdown().await;
    Ok(())
}

/// Runtime reconciliation must fail closed when on-chain channel-token supply
/// exceeds escrow custody beyond the in-flight envelope (the custody-vs-supply
/// invariant, which no other end-to-end test exercises). Supply is over-issued to a
/// non-escrow holder so custody stays 0, then the durable halt flag, the
/// forced-unhealthy latch, and quarantine of an active withdrawal are all
/// asserted. The halt flag is polled with a timeout rather than slept on: it is
/// monotonic (once set it stays set) and the mismatch is persistent, so every
/// tick breaches and the 3-tick confirmation can never race.
#[tokio::test(flavor = "multi_thread")]
async fn test_runtime_reconciliation_halts_on_supply_over_issuance(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Reconciliation Halts on Supply Over-Issuance ===");

    const OVER_ISSUED: u64 = 100_000;
    const WITHDRAWAL_AMOUNT: u64 = 1_000;

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_reconciliation")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_reconciliation",
        pg_host, pg_port
    );

    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    // Escrow instance + allowed mint, escrow ATA custody 0, mint supply 0.
    let env = TestEnvironment::setup(&client, &faucet_keypair, 0, 0, None).await?;
    let mint = env.mint;
    let instance = env.instance;

    let mint_meta = DbMint::new(mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &mint.to_string()).await?;

    // Over-issue: mint supply to a NON-escrow holder so Mint.supply rises while
    // the escrow custody stays 0. This is supply exceeding custody with no
    // backing deposit, the operator-key over-issuance the invariant must catch.
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])
        .map_err(|e| format!("Failed to create admin keypair: {}", e))?;
    let holder = Keypair::new();
    mint_to_owner(&client, &admin, mint, holder.pubkey(), &admin, OVER_ISSUED).await?;

    // One active (pending) withdrawal so the halt's quarantine has a row to flip.
    // Its amount joins the mint's in-flight envelope but stays well under the gap.
    let wd_sig = Signature::new_unique().to_string();
    let withdrawal =
        DbTransactionBuilder::new(wd_sig.clone(), 1, mint.to_string(), WITHDRAWAL_AMOUNT)
            .initiator(Pubkey::new_unique().to_string())
            .recipient(Pubkey::new_unique().to_string())
            .transaction_type(TransactionType::Withdrawal)
            .build();
    storage.insert_db_transaction(&withdrawal).await?;

    // The halt's alert POST is the only webhook under the single-invariant design;
    // assert it fired once the halt is observed.
    let mut mock_server = Server::new_async().await;
    let recon_mock = mock_server
        .mock("POST", "/")
        .with_status(200)
        .expect_at_least(1)
        .create_async()
        .await;

    let recon_config = OperatorConfig {
        reconciliation_interval: Duration::from_millis(200),
        reconciliation_tolerance_bps: 0,
        reconciliation_webhook_url: Some(mock_server.url()),
        ..default_operator_config(None)
    };

    let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
        test_validator.rpc_url(),
        RetryConfig::default(),
        CommitmentConfig::confirmed(),
    ));
    // Single-validator test: custody and channel-supply reads both hit the same RPC.
    let channel_rpc_client = rpc_client.clone();

    let health = HealthState::new(HealthConfig::operator());
    let health_for_task = Some(health.clone());

    let recon_storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 5,
        })
        .await?,
    ));

    let cancellation_token = CancellationToken::new();
    let recon_token_clone = cancellation_token.clone();
    let recon_handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = run_reconciliation(
            recon_storage,
            recon_config,
            rpc_client,
            channel_rpc_client,
            instance,
            health_for_task,
            recon_token_clone,
        )
        .await
        {
            tracing::error!("Reconciliation task error: {}", e);
        }
    });

    // Poll the monotonic halt flag with a deadline rather than sleeping a fixed
    // window, so a slow CI just takes a few more ticks instead of going flaky.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let halt = loop {
        if let Some(info) = storage.is_reconciliation_halted().await? {
            break info;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "reconciliation did not halt within the timeout"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    cancellation_token.cancel();
    let _ = recon_handle.await;

    // The reason names the mint and cites the supply gap, proving the custody-vs-supply
    // invariant tripped the halt.
    assert!(
        halt.reason.contains(&mint.to_string()),
        "halt reason should name the mint: {}",
        halt.reason
    );
    assert!(
        halt.reason.contains("supply by"),
        "halt reason should cite the supply gap: {}",
        halt.reason
    );

    // The halt fired the webhook alert, the only alert under the single invariant.
    recon_mock.assert_async().await;

    // The forced-unhealthy latch is set so /health reports 503 for orchestration.
    assert!(
        !health.is_healthy(),
        "operator health must be forced unhealthy after a halt"
    );

    // The active withdrawal was quarantined to manual_review by the halt.
    let pool = db::connect(&db_url).await?;
    let status: String =
        sqlx::query_scalar("SELECT status::text FROM transactions WHERE signature = $1")
            .bind(&wd_sig)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        status, "manual_review",
        "active withdrawal must be quarantined when the pipeline halts"
    );

    Ok(())
}

/// Sequential withdrawals: the operator releases two consecutive nonces, each
/// consuming its own bit.
///
/// The sender processes transactions one at a time, so nonce 0's bit is set
/// before nonce 1 is even built. Neighbouring nonces share a byte in the
/// bitmap, which is exactly where a wrong mask would make one release clear or
/// block the other. Both withdrawals must complete
/// The database records a `completed` withdrawal at nonce 0, but the freshly
/// created instance's bitmap has every bit clear. That is the database claiming
/// a release the chain never made, which invalidates the operator's whole view
/// of its own history, so it must refuse to start rather than consume more
/// nonces against a history it cannot trust.
#[tokio::test(flavor = "multi_thread")]
async fn test_operator_refuses_to_start_when_db_is_ahead_of_bitmap(
) -> Result<(), Box<dyn std::error::Error>> {
    use test_utils::operator_helper::start_private_channel_to_solana_operator;

    println!("=== Operator Lifecycle: Boot Halts When DB Is Ahead Of The Bitmap ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_bitmap_db_ahead")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_bitmap_db_ahead",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    // Fresh instance: every bit in its bitmap is clear.
    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 0, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    // Step 1: claim nonce 0 as completed. Nothing on-chain backs that claim.
    let poison_sig = Signature::new_unique().to_string();
    let fake_completed = make_withdrawal_transaction(
        poison_sig.clone(),
        env.mint.to_string(),
        env.users[0].pubkey().to_string(),
        10_000,
        0, // nonce
    );
    storage.insert_db_transaction(&fake_completed).await?;
    sqlx::query(
        "UPDATE transactions SET status = 'completed'::transaction_status WHERE signature = $1",
    )
    .bind(&poison_sig)
    .execute(&pool)
    .await?;

    // Step 2: a PENDING withdrawal the halt must leave untouched at nonce 1.
    let trigger_sig = Signature::new_unique().to_string();
    let trigger_withdrawal = make_withdrawal_transaction(
        trigger_sig.clone(),
        env.mint.to_string(),
        env.users[0].pubkey().to_string(),
        5_000,
        1, // nonce
    );
    storage.insert_db_transaction(&trigger_withdrawal).await?;

    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let operator_handle = start_private_channel_to_solana_operator(
        test_validator.rpc_url(),
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    // The operator must refuse to start: the task exits without processing any row.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut exited = false;
    while std::time::Instant::now() < deadline {
        if operator_handle._handle.is_finished() {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        exited,
        "operator must refuse to start (the task exits) when the DB is ahead of the bitmap"
    );

    // The innocent trigger withdrawal never leaves `pending`: the diff gates first.
    let trigger = db::get_transaction(&pool, &trigger_sig)
        .await?
        .expect("trigger withdrawal row must exist");
    assert_eq!(
        trigger.status, "pending",
        "trigger withdrawal must stay pending on refuse-to-start; got {}",
        trigger.status
    );

    operator_handle.shutdown().await;
    Ok(())
}

/// The mirror image, and the direction that must NOT halt. A release lands
/// on-chain and its `completed` write is lost, so a bit is set with no matching
/// row. The money already moved correctly; halting the whole pipeline over a
/// bookkeeping gap would be the wrong trade. The operator starts, reports the
/// orphan nonce, and keeps processing new withdrawals.
#[tokio::test(flavor = "multi_thread")]
async fn test_operator_starts_when_chain_is_ahead_of_db() -> Result<(), Box<dyn std::error::Error>>
{
    println!("=== Operator Lifecycle: Boot Continues When The Chain Is Ahead ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_bitmap_chain_ahead")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_bitmap_chain_ahead",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    mint_to_owner(&client, &admin, env.mint, env.instance, &admin, 200_000).await?;

    let user_pubkey = env.users[0].pubkey();

    // Consume nonce 0 on-chain with no row to match: a release whose write was lost.
    release_nonce_on_chain(
        &client,
        &admin,
        env.instance,
        env.mint,
        user_pubkey,
        50_000,
        0,
    )
    .await?;
    let balance_after_orphan = get_token_balance(&client, &user_pubkey, &env.mint).await?;

    // Advance the sequence past the nonce the chain already consumed.
    sqlx::query("SELECT setval('withdrawal_nonce_seq', 0, true)")
        .execute(&pool)
        .await?;

    let next_sig = Signature::new_unique().to_string();
    storage
        .insert_db_transaction(&make_withdrawal_transaction(
            next_sig.clone(),
            env.mint.to_string(),
            user_pubkey.to_string(),
            25_000,
            1,
        ))
        .await?;

    let operator_handle = start_operator_with_alert(
        ProgramType::Withdraw,
        test_validator.rpc_url(),
        db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance,
        None,
    )
    .await?;

    operator_util::wait_for_transaction_completion(&pool, &next_sig).await?;

    assert!(
        !operator_handle._handle.is_finished(),
        "a chain-ahead divergence must not halt the operator"
    );
    let balance_after = get_token_balance(&client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after,
        balance_after_orphan + 25_000,
        "the post-boot withdrawal must release exactly once"
    );

    operator_handle.shutdown().await;
    Ok(())
}

/// The invariant the bitmap exists to enforce: a nonce that already released
/// must never release again.
///
/// The database cannot produce two rows on one nonce (a trigger assigns them
/// from a sequence behind a unique index), so the second attempt is staged the
/// way it actually happens in production: a row whose release landed is re-armed
/// to `pending`, and the operator sends that same nonce a second time. The
/// program refuses it with `NonceAlreadyUsed` and no tokens move.
///
/// Without any surviving signature to attribute the release to, the re-armed row
/// ends in `manual_review` rather than `completed`. Both are correct terminal
/// states for that arm; what must never happen is a second payout or a remint.
#[tokio::test(flavor = "multi_thread")]
async fn test_second_release_of_same_nonce_moves_no_tokens(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Operator Lifecycle: Double Release Of One Nonce ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_double_release")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_double_release",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    mint_to_owner(&client, &admin, env.mint, env.instance, &admin, 200_000).await?;

    let user_pubkey = env.users[0].pubkey();
    let initial_balance = get_token_balance(&client, &user_pubkey, &env.mint).await?;

    let withdrawal_sig = Signature::new_unique().to_string();
    storage
        .insert_db_transaction(&make_withdrawal_transaction(
            withdrawal_sig.clone(),
            env.mint.to_string(),
            user_pubkey.to_string(),
            50_000,
            0,
        ))
        .await?;

    let operator_handle = start_operator_with_alert(
        ProgramType::Withdraw,
        test_validator.rpc_url(),
        db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance,
        None,
    )
    .await?;

    operator_util::wait_for_transaction_completion(&pool, &withdrawal_sig).await?;

    let balance_after_first = get_token_balance(&client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after_first,
        initial_balance + 50_000,
        "the first release must pay out once"
    );

    // Re-arm the settled row and drop the evidence of its broadcast, so the
    // operator has no local way to know this nonce already released.
    //
    // That is the point of the setup: with the signatures gone, nothing in the
    // operator's own state can stop a second payout, and only the on-chain bit
    // is left to refuse it.
    sqlx::query(
        "DELETE FROM pending_release_signatures WHERE transaction_id IN
           (SELECT id FROM transactions WHERE signature = $1)",
    )
    .bind(&withdrawal_sig)
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE transactions
            SET status = 'pending'::transaction_status,
                counterpart_signature = NULL,
                processed_at = NULL
          WHERE signature = $1",
    )
    .bind(&withdrawal_sig)
    .execute(&pool)
    .await?;

    // Wait for the re-armed row to settle again, whichever terminal state it takes.
    let deadline = std::time::Instant::now() + Duration::from_secs(*WAIT_TIMEOUT_SECS);
    let mut status = String::new();
    while std::time::Instant::now() < deadline {
        status = db::get_transaction(&pool, &withdrawal_sig)
            .await?
            .map(|row| row.status)
            .unwrap_or_default();
        if matches!(
            status.as_str(),
            "completed" | "failed" | "failed_reminted" | "manual_review"
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let balance_after_second = get_token_balance(&client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after_second, balance_after_first,
        "a second release of a consumed nonce must move no tokens (status {status})"
    );
    assert_ne!(
        status, "failed_reminted",
        "a consumed nonce must never be reminted"
    );

    // The bit is set exactly once, whatever the row's bookkeeping says.
    let bitmap_pda = private_channel_indexer::operator::find_withdrawal_bitmap_pda(&env.instance);
    let bitmap = private_channel_indexer::operator::parse_withdrawal_bitmap(
        &client.get_account_data(&bitmap_pda).await?,
    )?;
    assert_eq!(
        bitmap.consumed,
        vec![0],
        "exactly one nonce may be consumed on-chain"
    );

    operator_handle.shutdown().await;
    Ok(())
}

/// A withdrawal whose nonce belongs to the next generation is held back, either
/// by the pre-send generation check or by the program's own
/// `NonceOutsideCurrentGeneration` refusal, depending on which of the two sees
/// it first. Whichever catches it, the row must be requeued rather than failed,
/// and must succeed once the rotation lands.
///
/// Requires the eight-nonce test window, since the production one would need
/// 65,536 withdrawals to reach a boundary.
#[cfg(feature = "test-tree")]
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_one_generation_early_succeeds_after_rotation(
) -> Result<(), Box<dyn std::error::Error>> {
    use private_channel_indexer::operator::bitmap_constants::NONCES_PER_GENERATION;

    println!("=== Operator Lifecycle: Withdrawal One Generation Early ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_early_generation")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_early_generation",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    mint_to_owner(&client, &admin, env.mint, env.instance, &admin, 500_000).await?;

    let user_pubkey = env.users[0].pubkey();
    let initial_balance = get_token_balance(&client, &user_pubkey, &env.mint).await?;

    // The first nonce of generation 1: refused, requeued, valid after rotation.
    let early_sig = Signature::new_unique().to_string();
    storage
        .insert_db_transaction(&make_withdrawal_transaction(
            early_sig.clone(),
            env.mint.to_string(),
            user_pubkey.to_string(),
            10_000,
            NONCES_PER_GENERATION as i64,
        ))
        .await?;

    let operator_handle = start_operator_with_alert(
        ProgramType::Withdraw,
        test_validator.rpc_url(),
        db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance,
        None,
    )
    .await?;

    operator_util::wait_for_transaction_completion(&pool, &early_sig).await?;

    let balance_after = get_token_balance(&client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after,
        initial_balance + 10_000,
        "the early withdrawal must release once the rotation lands"
    );

    operator_handle.shutdown().await;
    Ok(())
}

/// The money-safety case the bitmap gate exists for. A release genuinely landed
/// (its bit is set), but the signature the row carries classifies as dead, so
/// signature evidence alone would say "never landed, remint it". The bitmap
/// overrules that: the entry escalates instead of paying the user twice.
///
/// Driven through `test_hooks::process_pending_remints` rather than a running
/// operator. The gate only fires for an entry that matured while the process was
/// up, which cannot be staged by restarting an operator: `OperatorHandle` cannot
/// actually stop one. Storage and the bitmap are both real here, so the only
/// thing simulated is the scheduling.
#[tokio::test(flavor = "multi_thread")]
async fn test_landed_release_with_dead_signatures_is_not_reminted(
) -> Result<(), Box<dyn std::error::Error>> {
    use private_channel_indexer::operator::sender::test_hooks;
    use private_channel_indexer::operator::sender::types::{PendingRemint, PendingSig};
    use private_channel_indexer::operator::sender::types::{
        TransactionContext, TransactionStatusUpdate,
    };
    use private_channel_indexer::operator::{TransactionKind, WithdrawalRemintInfo};
    use solana_sdk::commitment_config::CommitmentLevel;

    println!("=== Operator Lifecycle: Landed Release With Dead Signatures ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("operator_dead_sig_remint")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/operator_dead_sig_remint",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    ));
    storage.init_schema().await?;

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    seed_mint_status_allowed(&storage, &env.mint.to_string()).await?;

    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    mint_to_owner(&client, &admin, env.mint, env.instance, &admin, 200_000).await?;

    let user_pubkey = env.users[0].pubkey();

    // The release really happens, so nonce 0's bit is genuinely set on-chain.
    release_nonce_on_chain(
        &client,
        &admin,
        env.instance,
        env.mint,
        user_pubkey,
        50_000,
        0,
    )
    .await?;
    let balance_after_release = get_token_balance(&client, &user_pubkey, &env.mint).await?;

    // The withdrawal row this release belongs to, about to be queued for a remint
    // with a signature that was never broadcast and whose blockhash is long
    // expired.
    //
    // That is the exact shape signature-only classification calls dead, so from
    // here the bitmap is the only thing standing between the user and a second
    // credit.
    let withdrawal_sig = Signature::new_unique().to_string();
    storage
        .insert_db_transaction(&make_withdrawal_transaction(
            withdrawal_sig.clone(),
            env.mint.to_string(),
            user_pubkey.to_string(),
            50_000,
            0,
        ))
        .await?;
    let row = db::get_transaction(&pool, &withdrawal_sig)
        .await?
        .expect("withdrawal row must exist");
    let (transaction_id, trace_id): (i64, String) =
        sqlx::query_as("SELECT id, trace_id FROM transactions WHERE signature = $1")
            .bind(&withdrawal_sig)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        row.withdrawal_nonce,
        Some(0),
        "row must own the consumed nonce"
    );

    let config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url: test_validator.rpc_url(),
        fallback_rpc_url: None,
        source_rpc_url: None,
        postgres: PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        },
        escrow_instance_id: Some(env.instance),
    };
    let mut state = test_hooks::new_sender_state(
        &config,
        CommitmentLevel::Confirmed,
        Some(env.instance),
        storage.clone(),
        3,
        400,
        None,
    )?;

    state.pending_remints.push(PendingRemint {
        ctx: TransactionContext {
            kind: TransactionKind::ReleaseFunds,
            transaction_id: Some(transaction_id),
            withdrawal_nonce: Some(0),
            trace_id: Some(trace_id.clone()),
        },
        remint_info: WithdrawalRemintInfo {
            transaction_id,
            trace_id: trace_id.clone(),
            mint: env.mint,
            user: user_pubkey,
            user_ata: spl_associated_token_account::get_associated_token_address(
                &user_pubkey,
                &env.mint,
            ),
            token_program: spl_token::id(),
            amount: 50_000,
        },
        signatures: vec![PendingSig {
            signature: Signature::new_unique(),
            last_valid_block_height: 1,
        }],
        original_error: "release_funds failed".to_string(),
        deadline: Utc::now() - chrono::Duration::seconds(1),
        finality_check_attempts: 0,
        release_refused_on_chain: false,
        coverage_slot: None,
    });

    let (storage_tx, mut storage_rx) = tokio::sync::mpsc::channel::<TransactionStatusUpdate>(10);
    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    let update = storage_rx
        .try_recv()
        .expect("a blocked remint must still report the row");
    assert_eq!(
        update.status,
        private_channel_indexer::storage::common::models::TransactionStatus::ManualReview,
        "a consumed nonce must escalate rather than remint"
    );
    assert!(
        !update.remint_attempted,
        "no mint may be attempted once the bit proves the release landed"
    );
    assert!(
        state.pending_remints.is_empty(),
        "the entry must be consumed"
    );

    let balance_after_gate = get_token_balance(&client, &user_pubkey, &env.mint).await?;
    assert_eq!(
        balance_after_gate, balance_after_release,
        "the gate must prevent any second credit"
    );

    Ok(())
}

/// Instance creation must allocate the withdrawal bitmap in the same
/// instruction, on generation 0 with nothing consumed. Anything else and the
/// first withdrawal against a new instance fails.
///
/// This exercises the same `CreateInstance` shape the devnet
/// `create_instance` binary builds; that binary needs a live cluster, so its
/// PDA derivation is covered by the operator's unit tests instead.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_instance_allocates_a_fresh_bitmap() -> Result<(), Box<dyn std::error::Error>> {
    use private_channel_indexer::operator::{find_withdrawal_bitmap_pda, parse_withdrawal_bitmap};

    println!("=== Operator Lifecycle: CreateInstance Allocates The Bitmap ===");

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 0, None).await?;

    // The instance itself must exist.
    client.get_account(&env.instance).await?;

    let bitmap_pda = find_withdrawal_bitmap_pda(&env.instance);
    let data = client.get_account_data(&bitmap_pda).await?;
    let bitmap = parse_withdrawal_bitmap(&data)?;

    assert_eq!(bitmap.generation, 0, "a new bitmap starts on generation 0");
    assert!(
        bitmap.consumed.is_empty(),
        "a new bitmap has no consumed nonces, found {:?}",
        bitmap.consumed
    );

    Ok(())
}
