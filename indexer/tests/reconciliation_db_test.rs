//! Integration tests for get_escrow_balances_by_mint database query.
//!
//! This test suite verifies that the reconciliation database query:
//! 1. Only counts completed transactions (deposits and withdrawals)
//! 2. Correctly sums deposits and withdrawals per mint
//! 3. Handles multiple mints independently
//! 4. Returns correct token_program metadata
//!
//! Uses testcontainers to spin up an isolated Postgres instance for each test.

use bigdecimal::BigDecimal;
use private_channel_indexer::{
    storage::{common::amount::TokenAmount, PostgresDb, Storage},
    PostgresConfig,
};
use solana_sdk::pubkey::Pubkey;
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Start a fresh Postgres container, initialize schema, and return (pool, Storage, container).
/// The container must be kept alive for the duration of the test.
async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("reconciliation_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/reconciliation_test",
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

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Test that only completed transactions are counted in the balance query.
#[tokio::test(flavor = "multi_thread")]
async fn test_only_completed_transactions_counted() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Insert transactions with different statuses
    // Completed transactions: should be counted
    insert_transaction(
        &pool,
        "completed_deposit_1",
        &mint,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "completed_deposit_2",
        &mint,
        500_000,
        "deposit",
        "completed",
        101,
    )
    .await?;
    insert_transaction(
        &pool,
        "completed_withdrawal_1",
        &mint,
        300_000,
        "withdrawal",
        "completed",
        102,
    )
    .await?;

    // Non-completed transactions: should NOT be counted
    insert_transaction(
        &pool,
        "pending_deposit",
        &mint,
        2_000_000,
        "deposit",
        "pending",
        103,
    )
    .await?;
    insert_transaction(
        &pool,
        "processing_deposit",
        &mint,
        1_000_000,
        "deposit",
        "processing",
        104,
    )
    .await?;
    insert_transaction(
        &pool,
        "failed_deposit",
        &mint,
        500_000,
        "deposit",
        "failed",
        105,
    )
    .await?;
    insert_transaction(
        &pool,
        "pending_withdrawal",
        &mint,
        100_000,
        "withdrawal",
        "pending",
        106,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;

    // Verify only one mint
    assert_eq!(balances.len(), 1, "expected exactly one mint");

    let balance = &balances[0];
    assert_eq!(balance.mint_address, mint);
    assert_eq!(balance.token_program, token_program);

    // Expected: completed deposits = 1,000,000 + 500,000 = 1,500,000
    assert_eq!(
        balance.total_deposits,
        BigDecimal::from(1_500_000u64),
        "only completed deposits should be counted"
    );

    // Expected: completed withdrawals = 300,000
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(300_000u64),
        "only completed withdrawals should be counted"
    );

    Ok(())
}

/// Test that balances are correctly aggregated for multiple mints.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_mints_aggregated_independently() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint1 = Pubkey::new_unique().to_string();
    let mint2 = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mints
    insert_mint(&pool, &mint1, 6, &token_program).await?;
    insert_mint(&pool, &mint2, 9, &token_program).await?;

    // Mint 1 transactions
    insert_transaction(
        &pool,
        "mint1_deposit_1",
        &mint1,
        1_000_000,
        "deposit",
        "completed",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "mint1_deposit_2",
        &mint1,
        2_000_000,
        "deposit",
        "completed",
        101,
    )
    .await?;
    insert_transaction(
        &pool,
        "mint1_withdrawal_1",
        &mint1,
        500_000,
        "withdrawal",
        "completed",
        102,
    )
    .await?;

    // Mint 2 transactions
    insert_transaction(
        &pool,
        "mint2_deposit_1",
        &mint2,
        5_000_000,
        "deposit",
        "completed",
        103,
    )
    .await?;
    insert_transaction(
        &pool,
        "mint2_withdrawal_1",
        &mint2,
        1_000_000,
        "withdrawal",
        "completed",
        104,
    )
    .await?;
    insert_transaction(
        &pool,
        "mint2_withdrawal_2",
        &mint2,
        500_000,
        "withdrawal",
        "completed",
        105,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;

    // Should have 2 mints
    assert_eq!(balances.len(), 2, "expected two mints");

    // Find each mint's balance
    let balance1 = balances
        .iter()
        .find(|b| b.mint_address == mint1)
        .expect("mint1 not found");
    let balance2 = balances
        .iter()
        .find(|b| b.mint_address == mint2)
        .expect("mint2 not found");

    // Verify mint1
    assert_eq!(
        balance1.total_deposits,
        BigDecimal::from(3_000_000u64),
        "mint1: 1M + 2M deposits"
    );
    assert_eq!(
        balance1.total_withdrawals,
        BigDecimal::from(500_000u64),
        "mint1: 500K withdrawals"
    );

    // Verify mint2
    assert_eq!(
        balance2.total_deposits,
        BigDecimal::from(5_000_000u64),
        "mint2: 5M deposits"
    );
    assert_eq!(
        balance2.total_withdrawals,
        BigDecimal::from(1_500_000u64),
        "mint2: 1M + 500K withdrawals"
    );

    Ok(())
}

/// Test that mints with no transactions return zero balances.
#[tokio::test(flavor = "multi_thread")]
async fn test_mint_with_no_transactions() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint with no transactions
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;

    // Should have 1 mint with zero balances
    assert_eq!(balances.len(), 1, "expected one mint");

    let balance = &balances[0];
    assert_eq!(balance.mint_address, mint);
    assert_eq!(balance.token_program, token_program);
    assert_eq!(
        balance.total_deposits,
        BigDecimal::from(0u64),
        "no deposits"
    );
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(0u64),
        "no withdrawals"
    );

    Ok(())
}

/// Test empty database returns no balances.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_database() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    // Query balances with no mints
    let balances = storage.get_escrow_balances_by_mint().await?;

    assert_eq!(
        balances.len(),
        0,
        "empty database should return no balances"
    );

    Ok(())
}

/// Test that large amounts don't overflow and are correctly summed.
#[tokio::test(flavor = "multi_thread")]
async fn test_large_amounts() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Each deposit exceeds i64::MAX, so two of them gross-sum past i64::MAX while
    // staying within u64 - the case BIGINT could not store and the ::BIGINT SUM
    // cast would have overflowed. NUMERIC(20,0) must round-trip both exactly.
    let large_amount: u64 = i64::MAX as u64 + 1;

    insert_transaction(
        &pool,
        "large_deposit_1",
        &mint,
        large_amount,
        "deposit",
        "completed",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "large_deposit_2",
        &mint,
        large_amount,
        "deposit",
        "completed",
        101,
    )
    .await?;
    insert_transaction(
        &pool,
        "large_withdrawal",
        &mint,
        large_amount / 2,
        "withdrawal",
        "completed",
        102,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;

    assert_eq!(balances.len(), 1, "expected one mint");

    let balance = &balances[0];
    // Computed in BigDecimal because the gross deposit sum exceeds i64::MAX and
    // 2 * large_amount would overflow u64 in plain arithmetic.
    let expected_deposits = BigDecimal::from(large_amount) * BigDecimal::from(2u64);
    assert_eq!(
        balance.total_deposits, expected_deposits,
        "large deposits summed correctly past i64::MAX"
    );
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(large_amount / 2),
        "large withdrawal counted correctly"
    );

    Ok(())
}

/// Test correct handling of different token programs (SPL Token vs Token-2022).
#[tokio::test(flavor = "multi_thread")]
async fn test_different_token_programs() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint1 = Pubkey::new_unique().to_string();
    let mint2 = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();
    let token_2022_program = spl_token_2022::id().to_string();

    // Insert mints with different token programs
    insert_mint(&pool, &mint1, 6, &token_program).await?;
    insert_mint(&pool, &mint2, 9, &token_2022_program).await?;

    // Add transactions
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

    // Verify token programs are correctly returned
    let balance1 = balances
        .iter()
        .find(|b| b.mint_address == mint1)
        .expect("mint1 not found");
    let balance2 = balances
        .iter()
        .find(|b| b.mint_address == mint2)
        .expect("mint2 not found");

    assert_eq!(
        balance1.token_program, token_program,
        "mint1 should use SPL Token"
    );
    assert_eq!(
        balance2.token_program, token_2022_program,
        "mint2 should use Token-2022"
    );

    Ok(())
}

/// Test that the query correctly handles withdrawals exceeding deposits (net negative balance).
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawals_exceed_deposits() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    // Insert mint
    insert_mint(&pool, &mint, 6, &token_program).await?;

    // Deposits less than withdrawals (shouldn't happen in practice, but query should handle it)
    insert_transaction(
        &pool,
        "deposit",
        &mint,
        500_000,
        "deposit",
        "completed",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "withdrawal_1",
        &mint,
        300_000,
        "withdrawal",
        "completed",
        101,
    )
    .await?;
    insert_transaction(
        &pool,
        "withdrawal_2",
        &mint,
        400_000,
        "withdrawal",
        "completed",
        102,
    )
    .await?;

    // Query balances
    let balances = storage.get_escrow_balances_by_mint().await?;

    assert_eq!(balances.len(), 1, "expected one mint");

    let balance = &balances[0];
    assert_eq!(
        balance.total_deposits,
        BigDecimal::from(500_000u64),
        "deposits counted correctly"
    );
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(700_000u64),
        "withdrawals counted correctly"
    );
    // Net balance would be -200_000 (deposits - withdrawals), but we just store the raw totals

    Ok(())
}
