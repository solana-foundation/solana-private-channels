//! Integration tests for the reconciliation storage queries.
//!
//! Covers the runtime mint enumeration, the startup balance aggregate, the
//! durable halt flag and the in-flight envelope.
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

/// Runtime enumeration is driven by the `mints` table alone: one row per mint,
/// whatever transactions exist, and nothing for an address that only ever
/// appears in `transactions`.
#[tokio::test(flavor = "multi_thread")]
async fn mint_addresses_enumerates_mints_table_only() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    assert!(
        storage.get_mint_addresses().await?.is_empty(),
        "a fresh schema has no mints to enumerate"
    );

    let allowed_no_txns = Pubkey::new_unique().to_string();
    let allowed_with_txns = Pubkey::new_unique().to_string();
    let orphan_only = Pubkey::new_unique().to_string();
    let token_program = spl_token::id().to_string();

    insert_mint(&pool, &allowed_no_txns, 6, &token_program).await?;
    insert_mint(&pool, &allowed_with_txns, 9, &token_program).await?;

    // Two rows on one mint, neither status completed, plus a mint that has no `mints` row.
    insert_transaction(
        &pool,
        "pending_deposit",
        &allowed_with_txns,
        1_000,
        "deposit",
        "pending",
        100,
    )
    .await?;
    insert_transaction(
        &pool,
        "completed_withdrawal",
        &allowed_with_txns,
        400,
        "withdrawal",
        "completed",
        101,
    )
    .await?;
    insert_transaction(
        &pool,
        "orphan_deposit",
        &orphan_only,
        700,
        "deposit",
        "completed",
        102,
    )
    .await?;

    let mut addresses = storage.get_mint_addresses().await?;
    addresses.sort();
    let mut expected = vec![allowed_no_txns, allowed_with_txns];
    expected.sort();

    assert_eq!(
        addresses, expected,
        "one row per mints row; transaction count and status are irrelevant and an orphan mint is excluded"
    );

    Ok(())
}

/// The startup query sums with `SUM(...)::NUMERIC`, so a gross deposit total
/// above `i64::MAX` must round-trip exactly rather than overflow.
#[tokio::test(flavor = "multi_thread")]
async fn startup_balances_sum_past_i64_max_exactly() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    // Each deposit alone exceeds i64::MAX, so the two of them gross-sum past it
    // while each stays inside u64. BIGINT could store neither.
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

    let balances = storage
        .get_mint_balances_for_reconciliation(u64::MAX)
        .await?;
    assert_eq!(balances.len(), 1, "expected one mint");

    // Computed in BigDecimal because 2 * large_amount would overflow u64.
    let expected_deposits = BigDecimal::from(large_amount) * BigDecimal::from(2u64);
    assert_eq!(
        balances[0].total_deposits, expected_deposits,
        "gross deposits must sum exactly past i64::MAX"
    );
    assert_eq!(
        balances[0].total_withdrawals,
        BigDecimal::from(large_amount / 2),
        "completed withdrawal counted exactly"
    );

    Ok(())
}

// ── reconciliation halt flag + in-flight envelope ───────────────────────────

/// Round-trip the durable halt flag: absent -> set -> re-set (idempotent) ->
/// clear -> absent again.
#[tokio::test(flavor = "multi_thread")]
async fn reconciliation_halt_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    assert!(
        storage.is_reconciliation_halted().await?.is_none(),
        "fresh schema must read as not halted"
    );

    storage.set_reconciliation_halt("mint X insolvent").await?;
    let info = storage.is_reconciliation_halted().await?.expect("halt set");
    assert_eq!(info.reason, "mint X insolvent");

    // Idempotent re-set overwrites the reason on the single row.
    storage.set_reconciliation_halt("mint Y insolvent").await?;
    let info = storage
        .is_reconciliation_halted()
        .await?
        .expect("halt still set");
    assert_eq!(info.reason, "mint Y insolvent");

    storage.clear_reconciliation_halt().await?;
    assert!(
        storage.is_reconciliation_halted().await?.is_none(),
        "cleared halt must read as not halted"
    );

    Ok(())
}

/// The envelope query sums only in-flight statuses, grouped per mint; terminal
/// rows are excluded.
#[tokio::test(flavor = "multi_thread")]
async fn in_flight_envelope_sums_unsettled_per_mint() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint_a = Pubkey::new_unique().to_string();
    let mint_b = Pubkey::new_unique().to_string();

    // mint_a: pending 100 + processing 200 + parked 400 + pending_remint 800 = 1500.
    insert_transaction(&pool, "a_pend", &mint_a, 100, "deposit", "pending", 1).await?;
    insert_transaction(&pool, "a_proc", &mint_a, 200, "deposit", "processing", 2).await?;
    insert_transaction(&pool, "a_park", &mint_a, 400, "withdrawal", "parked", 3).await?;
    insert_transaction(
        &pool,
        "a_remint",
        &mint_a,
        800,
        "withdrawal",
        "pending_remint",
        4,
    )
    .await?;
    // Terminal rows on mint_a must be excluded.
    insert_transaction(&pool, "a_done", &mint_a, 1, "deposit", "completed", 5).await?;
    insert_transaction(&pool, "a_fail", &mint_a, 2, "withdrawal", "failed", 6).await?;

    // mint_b: a single pending 250.
    insert_transaction(&pool, "b_pend", &mint_b, 250, "deposit", "pending", 7).await?;

    let mut rows = storage.get_in_flight_amounts_by_mint().await?;
    rows.sort_by(|a, b| a.mint_address.cmp(&b.mint_address));

    let mut by_mint: std::collections::HashMap<String, BigDecimal> = rows
        .into_iter()
        .map(|r| (r.mint_address, r.in_flight_amount))
        .collect();
    assert_eq!(by_mint.remove(&mint_a), Some(BigDecimal::from(1500u64)));
    assert_eq!(by_mint.remove(&mint_b), Some(BigDecimal::from(250u64)));
    assert!(by_mint.is_empty(), "no unexpected mints in the envelope");

    Ok(())
}
