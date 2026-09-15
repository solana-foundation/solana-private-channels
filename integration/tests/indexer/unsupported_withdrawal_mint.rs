//! Integration tests for the withdrawal gates in the processor's
//! withdrawal-dispatch path: the allowlist gate, and the two `BlockMint` gates
//! the admin moves independently.
//!
//! A withdrawal can name any mint that exists on the private channel, but only a
//! mint the escrow allowlisted has funds behind it on the target chain. Before the
//! gate, such a row reached a target-chain metadata lookup, came back as a missing
//! account, was classified as an infrastructure failure, and took the whole
//! operator down so a supervisor would restart it. The row was only parked after
//! the restart budget ran out, so one unsupported mint cost several restarts.
//!
//! This test seeds a withdrawal for a mint the escrow never allowlisted, so no
//! `AllowedMint` account exists for it, and asserts the row is parked
//! deterministically while the operator keeps running:
//!   1. Spin up Postgres + Solana test validator
//!   2. Set up the escrow instance + operator
//!   3. Insert a withdrawal whose mint was never allowlisted
//!   4. Start the withdrawal operator
//!   5. Assert the row reaches `manual_review` within 30 s
//!   6. Assert the operator task never exited
//!
//! Step 6 is the regression: a critical task exit is what ends `operator::run`,
//! so a finished handle is exactly the symptom the gate removes.
//!
//! The other two tests allowlist the mint and then call `BlockMint` on it, once
//! per gate. `withdrawals_blocked` must park the row, because `release_funds`
//! rejects it on-chain and that rejection is permanent; `deposits_blocked` must
//! not, because the gates are independent and a mint closed to new deposits still
//! owes its depositors their balance.

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

use {
    chrono::Utc,
    helpers::db,
    helpers::tokens::{get_token_balance, mint_to_owner},
    private_channel_escrow_program_client::{
        instructions::BlockMintBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
    private_channel_indexer::{
        config::{OperatorConfig, PrivateChannelIndexerConfig, ProgramType, StorageType},
        operator,
        storage::common::amount::TokenAmount,
        storage::common::models::{DbMint, DbMintStatus, DbTransaction, TransactionStatus},
        storage::{PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment, TEST_ADMIN_KEYPAIR},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    std::{sync::Arc, time::Duration},
    test_utils::operator_helper::{same_host_fallback_url, OperatorHandle},
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
        rpc_commitment: solana_commitment_config::CommitmentLevel::Confirmed,
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
    instance: Pubkey,
) -> Result<OperatorHandle, Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url: db_url.clone(),
        max_connections: 10,
    };
    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));
    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.clone(),
        // Withdraw operator requires a source chain for remints; single-validator
        // test, so point it at the same RPC.
        source_rpc_url: Some(rpc_url.clone()),
        // The withdraw operator requires an independent, same-cluster fallback.
        fallback_rpc_url: Some(same_host_fallback_url(&rpc_url)),
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
async fn unsupported_withdrawal_mint_is_parked_without_stopping_the_operator(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Withdrawal allowlist gate -> ManualReview ===");

    // 1. Postgres + test validator.
    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("unsupported_mint_gate")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/unsupported_mint_gate",
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

    // 2. Instance + operator, with the environment's own mint allowlisted so the
    // gate has something to accept. The withdrawal below names a different mint
    // that is deliberately never allowlisted, which is the condition under test.
    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 1_000_000, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;
    storage
        .upsert_mints_batch(&[DbMint::new(
            env.mint.to_string(),
            6,
            spl_token::id().to_string(),
        )])
        .await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            withdrawals_blocked: false,
            mint_address: env.mint.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{}", env.mint),
            created_at: Utc::now(),
        }])
        .await?;

    // 3. A withdrawal naming a mint the escrow never allowlisted. The DB trigger
    // assigns its nonce on insert, so it holds a real place in the queue.
    let unsupported_mint = Pubkey::new_unique();
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
        mint: unsupported_mint.to_string(),
        amount: TokenAmount(10_000),
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(0), // trigger overwrites with NEXTVAL
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
    };
    storage.insert_db_transaction(&withdrawal).await?;

    // Defensive: the row must be unsupported for the right reason, and the
    // allowlist must not be empty, or the test would pass for the wrong one.
    let seeded: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mints WHERE mint_address = $1")
        .bind(unsupported_mint.to_string())
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        seeded.0, 0,
        "the withdrawal's mint must have no allowlist row"
    );
    let allowed: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mints WHERE mint_address = $1")
        .bind(env.mint.to_string())
        .fetch_one(&pool)
        .await?;
    assert_eq!(allowed.0, 1, "a different mint must be allowlisted");

    // 4. Start the operator.
    let operator_keypair = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let operator_handle = start_withdraw_operator(
        test_validator.rpc_url(),
        db_url.clone(),
        operator_keypair,
        env.instance,
    )
    .await?;

    // 5. The row is parked rather than retried forever.
    wait_for_status(&pool, &signature, "manual_review", 30)
        .await
        .expect("unsupported-mint row must be quarantined to manual_review");

    // 6. The operator is still running. Before the gate, the same row ended the
    // processor task, which ends `operator::run` and finishes this handle.
    assert!(
        !operator_handle._handle.is_finished(),
        "the operator must survive an unsupported withdrawal mint"
    );

    // The reason recorded on the row is the one the runbook dispatches on.
    let row = db::get_transaction(&pool, &signature)
        .await?
        .expect("row must still exist");
    assert_eq!(row.status, "manual_review");

    operator_handle.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// BlockMint gates
// ---------------------------------------------------------------------------

const WITHDRAW_AMOUNT: u64 = 50_000;

/// Everything the two gate scenarios share: validator, Postgres, escrow instance
/// and operator, an allowlisted SPL mint with a funded escrow ATA, and a
/// recipient whose ATA already exists, since `release_funds` rejects an
/// uninitialized one. They differ only in how `BlockMint` is called afterwards.
struct GateEnv {
    test_validator: solana_test_validator::TestValidator,
    client: RpcClient,
    pool: sqlx::PgPool,
    db_url: String,
    storage: Storage,
    instance: Pubkey,
    mint: Pubkey,
    recipient: Pubkey,
    _pg_container: testcontainers::ContainerAsync<Postgres>,
}

async fn setup_gate_env(db_name: &str) -> Result<GateEnv, Box<dyn std::error::Error>> {
    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{pg_host}:{pg_port}/{db_name}");

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    // One user, zero balance: the ATA is created either way, so the recipient's
    // balance after a release is the released amount and nothing else.
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let env = TestEnvironment::setup(&client, &faucet_keypair, 1, 0, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, env.instance).await?;

    // Fund the escrow ATA directly. Sidesteps the deposit path — only the escrow
    // balance matters to release_funds, and the deposit gate is under test below.
    mint_to_owner(
        &client,
        &admin,
        env.mint,
        env.instance,
        &admin,
        WITHDRAW_AMOUNT * 2,
    )
    .await?;

    storage
        .upsert_mints_batch(&[DbMint::new(
            env.mint.to_string(),
            6,
            spl_token::id().to_string(),
        )])
        .await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            withdrawals_blocked: false,
            mint_address: env.mint.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{}", env.mint),
            created_at: Utc::now(),
        }])
        .await?;

    Ok(GateEnv {
        test_validator,
        client,
        pool,
        db_url,
        storage,
        instance: env.instance,
        mint: env.mint,
        recipient: env.users[0].pubkey(),
        _pg_container: pg_container,
    })
}

/// Set both gates on an allowlisted mint. The PDA stays open either way, so the
/// operator reads the flags rather than an absent account, which is the whole
/// difference between this and the unsupported-mint case above.
async fn set_mint_gates(
    client: &RpcClient,
    admin: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    block_deposits: bool,
    block_withdrawals: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    let ix = BlockMintBuilder::new()
        .payer(admin.pubkey())
        .admin(admin.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .block_deposits(block_deposits)
        .block_withdrawals(block_withdrawals)
        .instruction();

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&admin.pubkey()),
        &[admin],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;
    Ok(())
}

fn make_withdrawal(
    signature: String,
    mint: Pubkey,
    recipient: Pubkey,
    amount: u64,
) -> DbTransaction {
    let now = Utc::now();
    DbTransaction {
        id: 0,
        signature,
        trace_id: Uuid::new_v4().to_string(),
        slot: 1,
        initiator: recipient.to_string(),
        recipient: recipient.to_string(),
        mint: mint.to_string(),
        amount: TokenAmount(amount),
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(0), // trigger overwrites with NEXTVAL
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

/// A withdrawal-blocked mint parks instead of broadcasting. `release_funds`
/// rejects it on-chain, and that failure is permanent: letting it broadcast
/// would terminalize the row through the remint path, where re-opening the gate
/// could no longer rescue it.
#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_blocked_mint_is_parked() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== BlockMint withdrawal gate -> ManualReview ===");

    let env = setup_gate_env("withdrawals_blocked_gate").await?;
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;

    // Withdrawals shut, deposits left open: the row must park on the gate under
    // test and not on the other one.
    set_mint_gates(&env.client, &admin, env.instance, env.mint, false, true).await?;

    let signature = Signature::new_unique().to_string();
    env.storage
        .insert_db_transaction(&make_withdrawal(
            signature.clone(),
            env.mint,
            env.recipient,
            WITHDRAW_AMOUNT,
        ))
        .await?;

    let operator_handle = start_withdraw_operator(
        env.test_validator.rpc_url(),
        env.db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance,
    )
    .await?;

    wait_for_status(&env.pool, &signature, "manual_review", 60)
        .await
        .expect("a withdrawal-blocked mint must park the row");

    assert_eq!(
        get_token_balance(&env.client, &env.recipient, &env.mint).await?,
        0,
        "nothing may be released while the withdrawal gate is shut",
    );
    assert!(
        !operator_handle._handle.is_finished(),
        "the operator must survive a blocked withdrawal mint"
    );

    operator_handle.shutdown().await;
    Ok(())
}

/// The gates are independent: a mint closed to new deposits still owes its
/// depositors their balance, so the withdrawal must release. Before the split,
/// blocking deposits closed the `AllowedMint` PDA and stranded them.
#[tokio::test(flavor = "multi_thread")]
async fn deposit_blocked_mint_still_releases() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== BlockMint deposit gate -> withdrawal still releases ===");

    let env = setup_gate_env("deposits_blocked_gate").await?;
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;

    set_mint_gates(&env.client, &admin, env.instance, env.mint, true, false).await?;

    let signature = Signature::new_unique().to_string();
    env.storage
        .insert_db_transaction(&make_withdrawal(
            signature.clone(),
            env.mint,
            env.recipient,
            WITHDRAW_AMOUNT,
        ))
        .await?;

    let operator_handle = start_withdraw_operator(
        env.test_validator.rpc_url(),
        env.db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance,
    )
    .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if get_token_balance(&env.client, &env.recipient, &env.mint).await? == WITHDRAW_AMOUNT {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let status = db::get_transaction(&env.pool, &signature)
                .await?
                .map(|row| row.status)
                .unwrap_or_else(|| "missing".to_string());
            return Err(format!(
                "deposit-blocked mint did not release within 60s (row status: {status})"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    operator_handle.shutdown().await;
    Ok(())
}
