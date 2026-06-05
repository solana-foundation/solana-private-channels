//! Integration test for the NULL-nonce withdrawal guard in the processor's
//! withdrawal-dispatch path (`indexer/src/operator/processor.rs`).
//!
//! The DB trigger `trigger_assign_withdrawal_nonce` normally guarantees that
//! every withdrawal row has a non-NULL `withdrawal_nonce`. If something
//! bypasses that trigger (manual SQL, schema drift, a legacy migration),
//! the processor MUST detect the NULL and quarantine the row to
//! `ManualReview` rather than panicking or silently stranding it.
//!
//! This test seeds the invariant violation directly via `sqlx`:
//!   1. Spin up Postgres + Solana test validator
//!   2. Set up the escrow instance + operator + whitelisted mint
//!   3. Insert a well-formed withdrawal row (trigger assigns a nonce)
//!   4. UPDATE the row to set `withdrawal_nonce = NULL`
//!   5. Start the withdrawal operator
//!   6. Assert the row transitions to `manual_review` within 30 s
//!
//! References the `OperatorError::Program(ProgramError::InvalidBuilder)` →
//! `ErrorDisposition::Quarantine("invalid_builder")` classification in
//! `processor.rs::classify_processor_error`.

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

use {
    chrono::Utc,
    helpers::{db, mint_to_owner},
    private_channel_indexer::{
        config::{OperatorConfig, PrivateChannelIndexerConfig, ProgramType, StorageType},
        operator,
        storage::common::models::{DbMint, DbMintStatus, DbTransaction, TransactionStatus},
        storage::{PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    setup::{TestEnvironment, TEST_ADMIN_KEYPAIR},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::{Keypair, Signature, Signer},
    },
    std::{sync::Arc, time::Duration},
    test_utils::operator_helper::OperatorHandle,
    test_utils::validator_helper::start_test_validator_no_geyser,
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::task::JoinHandle,
    uuid::Uuid,
};

fn default_operator_config() -> OperatorConfig {
    OperatorConfig {
        db_poll_interval: Duration::from_millis(500),
        batch_size: 10,
        retry_max_attempts: 15,
        retry_base_delay: Duration::from_millis(500),
        channel_buffer_size: 100,
        rpc_commitment: solana_sdk::commitment_config::CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(5 * 60),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: None,
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

async fn start_withdraw_operator(
    rpc_url: String,
    db_url: String,
    operator_keypair: Keypair,
    instance: solana_sdk::pubkey::Pubkey,
) -> Result<OperatorHandle, Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url: db_url.clone(),
        max_connections: 10,
    };
    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));
    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url,
        source_rpc_url: None,
        postgres: postgres_config,
        escrow_instance_id: Some(instance),
    };
    let operator_config = default_operator_config();
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

/// Poll the DB for up to `timeout_secs` waiting for a row to reach
/// `expected_status`. Returns Ok(()) on success; err otherwise.
async fn wait_for_status(
    pool: &sqlx::PgPool,
    signature: &str,
    expected_status: &str,
    timeout_secs: u64,
) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut last_seen = String::new();
    while start.elapsed().as_secs() < timeout_secs {
        match db::get_transaction(pool, signature).await {
            Ok(Some(tx)) => {
                last_seen = tx.status.clone();
                if tx.status == expected_status {
                    return Ok(());
                }
            }
            Ok(None) => {}
            Err(e) => return Err(format!("DB read failed: {e}")),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    Err(format!(
        "Timed out waiting for {signature} to reach {expected_status}; last seen: {last_seen}"
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn null_withdrawal_nonce_is_quarantined_to_manual_review(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Withdrawal NULL-nonce guard → ManualReview ===");

    // 1. Postgres + test validator.
    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("null_nonce_guard")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/null_nonce_guard",
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

    // 2. Instance + operator + whitelisted mint.
    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    let mint_meta = DbMint::new(env.mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_meta]).await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            mint_address: env.mint.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{}", env.mint),
            created_at: Utc::now(),
        }])
        .await?;

    // Escrow ATA needs funds (not that a successful withdrawal is expected — we
    // just want the non-NULL-nonce path to not kick in first).
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    mint_to_owner(&client, &admin, env.mint, env.instance, &admin, 200_000).await?;

    // 3. Insert a withdrawal row; the DB trigger assigns a fresh nonce.
    let signature = Signature::new_unique().to_string();
    let recipient = env.users[0].pubkey();
    let now = Utc::now();
    let withdrawal = DbTransaction {
        id: 0,
        signature: signature.clone(),
        trace_id: Uuid::new_v4().to_string(),
        slot: 1,
        initiator: recipient.to_string(),
        recipient: recipient.to_string(),
        mint: env.mint.to_string(),
        amount: 10_000,
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(0), // trigger will overwrite with NEXTVAL
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
    };
    storage.insert_db_transaction(&withdrawal).await?;

    // 4. Bypass the trigger: UPDATE the row to NULL out the nonce.
    // The trigger only fires on INSERT (BEFORE INSERT FOR EACH ROW), so a
    // subsequent UPDATE can set the column to NULL.
    let updated =
        sqlx::query("UPDATE transactions SET withdrawal_nonce = NULL WHERE signature = $1")
            .bind(&signature)
            .execute(&pool)
            .await?;
    assert_eq!(
        updated.rows_affected(),
        1,
        "NULL-nonce update must affect 1 row"
    );

    // Confirm the NULL actually landed (defensive — catches schema drift).
    let check: (Option<i64>,) =
        sqlx::query_as("SELECT withdrawal_nonce FROM transactions WHERE signature = $1")
            .bind(&signature)
            .fetch_one(&pool)
            .await?;
    assert!(
        check.0.is_none(),
        "withdrawal_nonce must be NULL before operator starts"
    );

    // 5. Start the operator.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let operator_handle = start_withdraw_operator(
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    // 6. Assert ManualReview within 30s.
    wait_for_status(&pool, &signature, "manual_review", 30)
        .await
        .expect("NULL-nonce row must be quarantined to manual_review");

    operator_handle.shutdown().await;
    Ok(())
}
