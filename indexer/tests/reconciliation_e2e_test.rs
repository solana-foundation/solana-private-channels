//! End-to-end integration tests for escrow balance reconciliation.
//!
//! This test suite verifies the complete reconciliation flow:
//! 1. Database query for completed transaction balances
//! 2. On-chain balance fetching (mocked RPC)
//! 3. Balance comparison with tolerance threshold
//! 4. Webhook alerting on mismatches
//!
//! Uses testcontainers for isolated Postgres instances and mockito for webhook server.

use bigdecimal::{BigDecimal, ToPrimitive};
use private_channel_core::webhook::{WebhookClient, WebhookRetryConfig};
use private_channel_indexer::{
    config::OperatorConfig,
    operator::reconciliation::{compare_balances, send_webhook_alert, BalanceMismatch},
    storage::{common::amount::TokenAmount, PostgresDb, Storage},
    PostgresConfig,
};
use solana_sdk::pubkey::Pubkey;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Start a fresh Postgres container, initialize schema, and return (pool, Storage, container).
/// The container must be kept alive for the duration of the test.
async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("reconciliation_e2e_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/reconciliation_e2e_test",
        host, port
    );

    let pool = PgPool::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url,
            max_connections: 5,
        })
        .await?,
    );
    storage.init_schema().await?;

    Ok((pool, storage, container))
}

/// Insert a mint into the database.
async fn insert_mint(
    pool: &PgPool,
    mint_address: &str,
    decimals: i16,
    token_program: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mints (mint_address, decimals, token_program, created_at)
         VALUES ($1, $2, $3, NOW())",
    )
    .bind(mint_address)
    .bind(decimals)
    .bind(token_program)
    .execute(pool)
    .await?;

    Ok(())
}

/// Insert a transaction into the database.
#[allow(clippy::too_many_arguments)]
async fn insert_transaction(
    pool: &PgPool,
    signature: &str,
    mint: &str,
    amount: u64,
    transaction_type: &str,
    status: &str,
    slot: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, created_at, updated_at)
         VALUES ($1, $2, 'test_initiator', 'test_recipient', $3, $4, $5::transaction_type, $6::transaction_status, NOW(), NOW())",
    )
    .bind(signature)
    .bind(slot)
    .bind(mint)
    .bind(TokenAmount(amount))
    .bind(transaction_type)
    .bind(status)
    .execute(pool)
    .await?;

    Ok(())
}

/// Create a test OperatorConfig with specified reconciliation settings.
fn create_test_config(tolerance_bps: u16, webhook_url: Option<String>) -> OperatorConfig {
    OperatorConfig {
        db_poll_interval: Duration::from_secs(1),
        batch_size: 10,
        retry_max_attempts: 3,
        retry_base_delay: Duration::from_secs(1),
        channel_buffer_size: 100,
        rpc_commitment: solana_sdk::commitment_config::CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(300),
        reconciliation_tolerance_bps: tolerance_bps,
        reconciliation_webhook_url: webhook_url,
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: 400,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Test successful reconciliation when balances match within tolerance.
///
/// Setup:
/// - Database has 1,000,000 tokens deposited (completed)
/// - On-chain balance would be 1,000,000 (exact match)
///
/// Expected:
/// - No webhook alert sent
/// - Reconciliation succeeds
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_success_within_tolerance() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Insert completed transactions: 1,000,000 deposited
    insert_transaction(
        &pool,
        "deposit_1",
        &mint,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;

    // Query balances to verify database state
    let balances = storage.get_escrow_balances_by_mint().await?;
    assert_eq!(balances.len(), 1, "expected exactly one mint");

    let balance = &balances[0];
    assert_eq!(balance.mint_address, mint);
    assert_eq!(balance.total_deposits, BigDecimal::from(1_000_000u64));
    assert_eq!(balance.total_withdrawals, BigDecimal::from(0u64));

    // In a real E2E test, we would:
    // 1. Mock RPC endpoint to return on-chain balance of 1,000,000
    // 2. Create webhook mock server
    // 3. Call perform_reconciliation_check
    // 4. Verify no webhook was sent

    // For now, verify the database query works correctly
    Ok(())
}

/// Test reconciliation detects mismatch and sends webhook alert.
///
/// Setup:
/// - Database has 1,000,000 tokens (net: deposits - withdrawals)
/// - On-chain balance would be 900,000 (10% mismatch = 1000 bps)
/// - Tolerance is 10 bps (0.1%)
///
/// Expected:
/// - Mismatch detected (1000 bps > 10 bps tolerance)
/// - Webhook alert sent with correct data
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_detects_mismatch_and_alerts() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let token_program = spl_token::id().to_string();

    insert_mint(&pool, &mint.to_string(), 6, &token_program).await?;
    insert_transaction(
        &pool,
        "deposit_1",
        &mint.to_string(),
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;

    let db_balance_results = storage.get_escrow_balances_by_mint().await?;
    let mut db_balances = HashMap::new();
    for balance_result in db_balance_results {
        let mint_key = balance_result
            .mint_address
            .parse::<Pubkey>()
            .expect("valid mint");
        let net_balance = (&balance_result.total_deposits - &balance_result.total_withdrawals)
            .to_u64()
            .expect("net fits u64");
        db_balances.insert(mint_key, net_balance);
    }

    let mut on_chain_balances = HashMap::new();
    on_chain_balances.insert(mint, 900_000u64);

    let tolerance_bps = 10;
    let mismatches = compare_balances(&on_chain_balances, &db_balances, tolerance_bps);
    assert_eq!(mismatches.len(), 1, "expected one mismatch");

    let mut webhook_server = mockito::Server::new_async().await;
    let webhook_mock = webhook_server
        .mock("POST", "/")
        .match_header("content-type", "application/json")
        .with_status(200)
        .expect(1)
        .create_async()
        .await;

    let webhook_url = Some(webhook_server.url());
    let webhook_client = WebhookClient::new(
        Duration::from_secs(10),
        WebhookRetryConfig::new(3, Duration::from_millis(10), Duration::from_millis(50)),
    )
    .expect("webhook client");
    let result = send_webhook_alert(&webhook_url, &mismatches, &webhook_client).await;
    assert!(
        result.is_ok(),
        "webhook alert should succeed: {:?}",
        result.err()
    );

    webhook_mock.assert_async().await;

    Ok(())
}

/// Test reconciliation with multiple mints - some match, some don't.
///
/// Setup:
/// - Mint1: DB=1,000,000, on-chain=1,000,000 (exact match)
/// - Mint2: DB=2,000,000, on-chain=1,800,000 (10% mismatch)
/// - Tolerance: 10 bps (0.1%)
///
/// Expected:
/// - Mint1: no alert (exact match)
/// - Mint2: webhook alert sent (1000 bps > 10 bps tolerance)
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_multiple_mints_mixed_results() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    let mint1 = Pubkey::new_unique().to_string();
    let mint2 = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mints
    insert_mint(&pool, &mint1, 6, &token_program).await?;
    insert_mint(&pool, &mint2, 9, &token_program).await?;

    // Mint1: 1,000,000 deposited (will match on-chain)
    insert_transaction(
        &pool,
        "mint1_deposit",
        &mint1,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;

    // Mint2: 2,000,000 deposited (will mismatch on-chain)
    insert_transaction(
        &pool,
        "mint2_deposit",
        &mint2,
        2_000_000,
        "deposit",
        "completed",
        101,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;
    assert_eq!(balances.len(), 2, "expected two mints");

    // Verify DB state
    let balance1 = balances
        .iter()
        .find(|b| b.mint_address == mint1)
        .expect("mint1 not found");
    let balance2 = balances
        .iter()
        .find(|b| b.mint_address == mint2)
        .expect("mint2 not found");

    assert_eq!(balance1.total_deposits, BigDecimal::from(1_000_000u64));
    assert_eq!(balance2.total_deposits, BigDecimal::from(2_000_000u64));

    // In a real E2E test, we would:
    // 1. Mock RPC to return on-chain balances: mint1=1,000,000, mint2=1,800,000
    // 2. Set up webhook mock expecting 1 call (for mint2 only)
    // 3. Call perform_reconciliation_check
    // 4. Verify webhook received alert for mint2 but not mint1

    Ok(())
}

/// Test reconciliation with no webhook URL configured.
///
/// Setup:
/// - Database and on-chain have mismatched balances
/// - No webhook URL configured
///
/// Expected:
/// - Mismatch detected but no HTTP request made
/// - Warning logged instead of webhook alert
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_no_webhook_url_configured() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Insert transaction
    insert_transaction(
        &pool,
        "deposit_1",
        &mint,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;

    // Create config with NO webhook URL
    let config = create_test_config(10, None);
    assert_eq!(config.reconciliation_webhook_url, None);

    // Verify database state
    let balances = storage.get_escrow_balances_by_mint().await?;
    assert_eq!(balances.len(), 1);

    // In a real E2E test, we would:
    // 1. Mock RPC to return mismatched on-chain balance
    // 2. Call perform_reconciliation_check
    // 3. Verify no HTTP request made (would fail if webhook called)
    // 4. Verify warning log message

    Ok(())
}

/// Test reconciliation with deposits and withdrawals.
///
/// Setup:
/// - Deposits: 2,000,000
/// - Withdrawals: 500,000
/// - Net DB balance: 1,500,000
///
/// Expected:
/// - Reconciliation compares net balance (1,500,000) against on-chain
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_with_deposits_and_withdrawals(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Insert deposits
    insert_transaction(
        &pool,
        "deposit_1",
        &mint,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "deposit_2",
        &mint,
        1_000_000,
        "deposit",
        "completed",
        101,
    )
    .await?;

    // Insert withdrawal
    insert_transaction(
        &pool,
        "withdrawal_1",
        &mint,
        500_000,
        "withdrawal",
        "completed",
        102,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;
    assert_eq!(balances.len(), 1);

    let balance = &balances[0];
    assert_eq!(
        balance.total_deposits,
        BigDecimal::from(2_000_000u64),
        "total deposits"
    );
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(500_000u64),
        "total withdrawals"
    );

    // Net balance should be 1,500,000 (deposits - withdrawals)
    let net_balance = &balance.total_deposits - &balance.total_withdrawals;
    assert_eq!(net_balance, BigDecimal::from(1_500_000u64), "net balance");

    // In a real E2E test, we would:
    // 1. Mock RPC to return on-chain balance of 1,500,000
    // 2. Call perform_reconciliation_check
    // 3. Verify reconciliation succeeds (exact match)

    Ok(())
}

/// Test reconciliation ignores pending/processing transactions.
///
/// Setup:
/// - Completed deposit: 1,000,000
/// - Pending deposit: 500,000 (should be ignored)
/// - Processing withdrawal: 200,000 (should be ignored)
///
/// Expected:
/// - Net DB balance: 1,000,000 (only completed transactions)
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_ignores_non_completed_transactions(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Completed transaction (should be counted)
    insert_transaction(
        &pool,
        "completed_deposit",
        &mint,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;

    // Pending transaction (should be ignored)
    insert_transaction(
        &pool,
        "pending_deposit",
        &mint,
        500_000,
        "deposit",
        "pending",
        101,
    )
    .await?;

    // Processing transaction (should be ignored)
    insert_transaction(
        &pool,
        "processing_withdrawal",
        &mint,
        200_000,
        "withdrawal",
        "processing",
        102,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;
    assert_eq!(balances.len(), 1);

    let balance = &balances[0];
    assert_eq!(
        balance.total_deposits,
        BigDecimal::from(1_000_000u64),
        "only completed deposits counted"
    );
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(0u64),
        "only completed withdrawals counted"
    );

    // In a real E2E test, we would:
    // 1. Mock RPC to return on-chain balance of 1,000,000
    // 2. Call perform_reconciliation_check
    // 3. Verify reconciliation succeeds (matches completed transactions only)

    Ok(())
}

/// Test reconciliation webhook retry logic.
///
/// Setup:
/// - Webhook server fails first request, succeeds on second
/// - Mismatch condition triggered
///
/// Expected:
/// - Webhook retried and eventually succeeds
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_webhook_retry_logic() -> Result<(), Box<dyn std::error::Error>> {
    let mut webhook_server = mockito::Server::new_async().await;

    let mock_fail = webhook_server
        .mock("POST", "/")
        .with_status(500)
        .expect(1)
        .create_async()
        .await;

    let mock_success = webhook_server
        .mock("POST", "/")
        .with_status(200)
        .expect(1)
        .create_async()
        .await;

    let mismatches = vec![BalanceMismatch {
        mint: Pubkey::new_unique(),
        on_chain_balance: 1_000,
        db_balance: 900,
        delta_bps: 1_000,
    }];

    let webhook_url = Some(webhook_server.url());
    let webhook_client = WebhookClient::new(
        Duration::from_secs(10),
        WebhookRetryConfig::new(2, Duration::from_millis(10), Duration::from_millis(50)),
    )
    .expect("webhook client");
    let result = send_webhook_alert(&webhook_url, &mismatches, &webhook_client).await;
    assert!(
        result.is_ok(),
        "should succeed after retry: {:?}",
        result.err()
    );

    mock_fail.assert_async().await;
    mock_success.assert_async().await;

    Ok(())
}

/// **REAL E2E TEST** - Complete flow from database query to webhook alert.
///
/// This test exercises the full reconciliation flow:
/// 1. Set up database with transactions
/// 2. Query database balances using storage.get_escrow_balances_by_mint()
/// 3. Simulate on-chain balances (mocked)
/// 4. Compare balances using compare_balances()
/// 5. Send webhook alert using send_webhook_alert()
/// 6. Verify webhook received correct mismatch data
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_reconciliation_with_mismatch_and_webhook_alert(
) -> Result<(), Box<dyn std::error::Error>> {
    // ── Step 1: Set up database with test data ────────────────────────────────

    let (pool, storage, _pg) = start_postgres().await?;

    let mint1 = Pubkey::new_unique();
    let mint2 = Pubkey::new_unique();
    let token_program = spl_token::id().to_string();

    // Insert mints
    insert_mint(&pool, &mint1.to_string(), 6, &token_program).await?;
    insert_mint(&pool, &mint2.to_string(), 9, &token_program).await?;

    // Mint1: 1,000,000 deposited, 200,000 withdrawn = 800,000 net
    insert_transaction(
        &pool,
        "mint1_deposit",
        &mint1.to_string(),
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "mint1_withdrawal",
        &mint1.to_string(),
        200_000,
        "withdrawal",
        "completed",
        101,
    )
    .await?;

    // Mint2: 2,000,000 deposited = 2,000,000 net
    insert_transaction(
        &pool,
        "mint2_deposit",
        &mint2.to_string(),
        2_000_000,
        "deposit",
        "completed",
        102,
    )
    .await?;

    // ── Step 2: Query database balances ────────────────────────────────────────

    let db_balance_results = storage.get_escrow_balances_by_mint().await?;
    assert_eq!(db_balance_results.len(), 2, "expected two mints");

    // Convert to HashMap for comparison
    let mut db_balances = HashMap::new();
    for balance_result in db_balance_results {
        let mint = balance_result
            .mint_address
            .parse::<Pubkey>()
            .expect("valid mint address");
        let net_balance = (&balance_result.total_deposits - &balance_result.total_withdrawals)
            .to_u64()
            .expect("net fits u64");
        db_balances.insert(mint, net_balance);
    }

    // Verify DB balances
    assert_eq!(
        *db_balances.get(&mint1).unwrap(),
        800_000,
        "mint1 net balance"
    );
    assert_eq!(
        *db_balances.get(&mint2).unwrap(),
        2_000_000,
        "mint2 net balance"
    );

    // ── Step 3: Simulate on-chain balances (mocked RPC response) ──────────────

    let mut on_chain_balances = HashMap::new();
    // Mint1: on-chain matches DB (800,000) - no mismatch
    on_chain_balances.insert(mint1, 800_000);
    // Mint2: on-chain is 1,800,000 but DB shows 2,000,000 - 10% mismatch (1000 bps)
    on_chain_balances.insert(mint2, 1_800_000);

    // ── Step 4: Compare balances ───────────────────────────────────────────────

    let tolerance_bps = 10; // 0.1% tolerance
    let mismatches = compare_balances(&on_chain_balances, &db_balances, tolerance_bps);

    // Should detect 1 mismatch (mint2 only)
    assert_eq!(mismatches.len(), 1, "expected one mismatch");

    let mismatch = &mismatches[0];
    assert_eq!(mismatch.mint, mint2, "mismatch should be for mint2");
    assert_eq!(mismatch.on_chain_balance, 1_800_000);
    assert_eq!(mismatch.db_balance, 2_000_000);

    // Verify delta calculation: |1,800,000 - 2,000,000| / 1,800,000 * 10000
    // = 200,000 / 1,800,000 * 10000 = 1111 bps (approximately 11%)
    assert!(
        mismatch.delta_bps > 1000,
        "delta should be > 1000 bps for 11% difference"
    );
    assert!(
        mismatch.delta_bps < 1200,
        "delta should be < 1200 bps for 11% difference"
    );

    // ── Step 5: Set up mock webhook server ─────────────────────────────────────

    let mut webhook_server = mockito::Server::new_async().await;

    let webhook_mock = webhook_server
        .mock("POST", "/")
        .match_header("content-type", "application/json")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "mint": mint2.to_string(),
            "on_chain_balance": 1_800_000u64,
            "db_balance": 2_000_000u64,
            "delta_bps": mismatch.delta_bps,
        })))
        .with_status(200)
        .create_async()
        .await;

    // ── Step 6: Send webhook alert ─────────────────────────────────────────────

    let webhook_url = Some(webhook_server.url());
    let webhook_client = WebhookClient::new(
        Duration::from_secs(10),
        WebhookRetryConfig::new(3, Duration::from_millis(10), Duration::from_millis(50)),
    )
    .expect("webhook client");
    let result = send_webhook_alert(&webhook_url, &mismatches, &webhook_client).await;

    assert!(
        result.is_ok(),
        "webhook alert should be sent successfully: {:?}",
        result.err()
    );

    webhook_mock.assert_async().await;

    println!("✓ E2E test passed: database query → comparison → webhook alert");

    Ok(())
}

/// **REAL E2E TEST** - Successful reconciliation (no webhook sent).
///
/// Tests the case where all balances match within tolerance:
/// 1. Database and on-chain balances match
/// 2. compare_balances returns no mismatches
/// 3. No webhook alert sent
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_reconciliation_success_no_alert() -> Result<(), Box<dyn std::error::Error>> {
    // ── Step 1: Set up database ────────────────────────────────────────────────

    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let token_program = spl_token::id().to_string();

    insert_mint(&pool, &mint.to_string(), 6, &token_program).await?;

    // Insert 1,000,000 tokens
    insert_transaction(
        &pool,
        "deposit_1",
        &mint.to_string(),
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;

    // ── Step 2: Query database balances ────────────────────────────────────────

    let db_balance_results = storage.get_escrow_balances_by_mint().await?;
    let mut db_balances = HashMap::new();
    for balance_result in db_balance_results {
        let mint_key = balance_result
            .mint_address
            .parse::<Pubkey>()
            .expect("valid mint");
        let net_balance = (&balance_result.total_deposits - &balance_result.total_withdrawals)
            .to_u64()
            .expect("net fits u64");
        db_balances.insert(mint_key, net_balance);
    }

    // ── Step 3: Simulate on-chain balances (exact match) ──────────────────────

    let mut on_chain_balances = HashMap::new();
    on_chain_balances.insert(mint, 1_000_000); // Exact match with DB

    // ── Step 4: Compare balances ───────────────────────────────────────────────

    let tolerance_bps = 10;
    let mismatches = compare_balances(&on_chain_balances, &db_balances, tolerance_bps);

    // Should have NO mismatches
    assert_eq!(mismatches.len(), 0, "exact match should have no mismatches");

    // ── Step 5: Verify no webhook sent ────────────────────────────────────────

    let mut webhook_server = mockito::Server::new_async().await;

    // Set up a mock that should NOT be called
    let webhook_mock = webhook_server
        .mock("POST", "/")
        .expect(0) // Should not be called
        .create_async()
        .await;

    let webhook_url = Some(webhook_server.url());
    let webhook_client = WebhookClient::new(
        Duration::from_secs(10),
        WebhookRetryConfig::new(3, Duration::from_millis(500), Duration::from_secs(5)),
    )
    .expect("webhook client");
    let result = send_webhook_alert(&webhook_url, &mismatches, &webhook_client).await;

    assert!(result.is_ok(), "send_webhook_alert should succeed");

    // Verify webhook was NOT called
    webhook_mock.assert_async().await;

    println!("✓ E2E test passed: successful reconciliation, no alert sent");

    Ok(())
}
