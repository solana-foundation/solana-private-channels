//! Integration tests for the reconciliation storage queries.
//!
//! Covers the ledger balance aggregate shared by startup and runtime
//! reconciliation, the durable halt flag and the in-flight envelope.
//!
//! Uses testcontainers to spin up an isolated Postgres instance for each test.

use bigdecimal::BigDecimal;
use private_channel_indexer::{
    storage::{
        common::{amount::TokenAmount, models::DbObservedRelease},
        PostgresDb, Storage,
    },
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

/// Insert a withdrawal row and return the nonce the trigger assigned to it.
async fn insert_withdrawal(
    pool: &PgPool,
    signature: &str,
    mint: &str,
    amount: u64,
    status: &str,
    slot: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, created_at, updated_at)
         VALUES ($1, $2, 'test_initiator', 'test_recipient', $3, $4, 'withdrawal', $5::transaction_status, NOW(), NOW())
         RETURNING withdrawal_nonce",
    )
    .bind(signature)
    .bind(slot)
    .bind(mint)
    .bind(TokenAmount(amount))
    .bind(status)
    .fetch_one(pool)
    .await
}

/// Record that the escrow indexer saw the release for `nonce` land at `slot`, with no
/// amount of its own: the shape of a row written before the column existed.
async fn observe_release(
    storage: &Storage,
    nonce: i64,
    slot: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    storage
        .insert_observed_releases_batch(&[DbObservedRelease {
            withdrawal_nonce: nonce,
            signature: format!("release_{nonce}"),
            slot,
            amount: None,
        }])
        .await?;
    Ok(())
}

/// Record a release for `nonce` that moved `amount`, which can differ from what the row owes.
async fn observe_release_of(
    storage: &Storage,
    nonce: i64,
    slot: i64,
    amount: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    storage
        .insert_observed_releases_batch(&[DbObservedRelease {
            withdrawal_nonce: nonce,
            signature: format!("release_{nonce}"),
            slot,
            amount: Some(TokenAmount(amount)),
        }])
        .await?;
    Ok(())
}

/// Total withdrawals the aggregate reports for `mint` at `slot`.
async fn withdrawals_at(
    storage: &Storage,
    mint: &str,
    slot: u64,
) -> Result<BigDecimal, Box<dyn std::error::Error>> {
    let rows = storage.get_mint_balances_for_reconciliation(slot).await?;
    let row = rows
        .into_iter()
        .find(|r| r.mint_address == mint)
        .ok_or("mint missing from the aggregate")?;
    Ok(row.total_withdrawals)
}

/// Total withdrawals the unpinned aggregate reports for `mint` at `slot`. This is the read
/// startup falls back to when the escrow checkpoint sits below the custody snapshot.
async fn unpinned_withdrawals_at(
    storage: &Storage,
    mint: &str,
    slot: u64,
) -> Result<BigDecimal, Box<dyn std::error::Error>> {
    let rows = storage
        .get_mint_balances_for_unpinned_reconciliation(slot)
        .await?;
    let row = rows
        .into_iter()
        .find(|r| r.mint_address == mint)
        .ok_or("mint missing from the aggregate")?;
    Ok(row.total_withdrawals)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Deposits of every status count, bounded at the slot.
#[tokio::test(flavor = "multi_thread")]
async fn deposits_of_every_status_count_up_to_the_bound() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let statuses = [
        "pending",
        "processing",
        "completed",
        "failed",
        "manual_review",
    ];
    for (i, status) in statuses.iter().enumerate() {
        insert_transaction(
            &pool,
            &format!("d_{status}"),
            &mint,
            1 << i,
            "deposit",
            status,
            100,
        )
        .await?;
    }
    insert_transaction(&pool, "d_late", &mint, 1_000, "deposit", "completed", 201).await?;

    let at_200 = storage.get_mint_balances_for_reconciliation(200).await?;
    assert_eq!(
        at_200[0].total_deposits,
        BigDecimal::from(31u64),
        "every status counts"
    );
    let at_max = storage
        .get_mint_balances_for_reconciliation(u64::MAX)
        .await?;
    assert_eq!(
        at_max[0].total_deposits,
        BigDecimal::from(1_031u64),
        "the later deposit counts once in range"
    );
    Ok(())
}

/// One row per `mints` row: a mint with nothing in range still reports zero, withdrawal
/// rows never duplicate it, and an address with no `mints` row never appears.
#[tokio::test(flavor = "multi_thread")]
async fn every_mint_row_appears_once_even_without_qualifying_rows(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let token_program = spl_token::id().to_string();
    let empty = Pubkey::new_unique().to_string();
    let busy = Pubkey::new_unique().to_string();
    let orphan = Pubkey::new_unique().to_string();
    insert_mint(&pool, &empty, 6, &token_program).await?;
    insert_mint(&pool, &busy, 6, &token_program).await?;

    insert_transaction(
        &pool,
        "busy_late_deposit",
        &busy,
        500,
        "deposit",
        "completed",
        300,
    )
    .await?;
    insert_withdrawal(&pool, "busy_w1", &busy, 40, "processing", 10).await?;
    insert_withdrawal(&pool, "busy_w2", &busy, 60, "pending", 20).await?;
    insert_transaction(
        &pool,
        "orphan_deposit",
        &orphan,
        700,
        "deposit",
        "completed",
        5,
    )
    .await?;

    let mut rows = storage.get_mint_balances_for_reconciliation(200).await?;
    rows.sort_by(|a, b| a.mint_address.cmp(&b.mint_address));
    let mut expected = vec![empty, busy];
    expected.sort();
    let addresses: Vec<String> = rows.iter().map(|r| r.mint_address.clone()).collect();
    assert_eq!(
        addresses, expected,
        "exactly one row per mints row, orphan excluded"
    );
    for row in &rows {
        assert_eq!(row.total_deposits, BigDecimal::from(0u64));
        assert_eq!(row.total_withdrawals, BigDecimal::from(0u64));
    }
    Ok(())
}

/// Status never subtracts a withdrawal: without an observed release nothing is released.
#[tokio::test(flavor = "multi_thread")]
async fn unreleased_withdrawals_are_never_subtracted() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let statuses = [
        "pending",
        "processing",
        "parked",
        "pending_remint",
        "failed_reminted",
        "manual_review",
        "failed",
        "completed",
    ];
    for status in statuses {
        insert_withdrawal(&pool, &format!("w_{status}"), &mint, 100, status, 100).await?;
    }
    insert_withdrawal(&pool, "w_refused", &mint, 100, "failed_reminted", 100).await?;
    sqlx::query(
        "UPDATE transactions SET release_refused_on_chain = true WHERE signature = 'w_refused'",
    )
    .execute(&pool)
    .await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(0u64)
    );
    Ok(())
}

/// The tick enumerates mints from `get_mint_addresses` and values them from the aggregate.
/// The two read the same `mints` table, so they have to answer with the same universe or a
/// mint gets valued without being checked, or checked without being valued.
#[tokio::test(flavor = "multi_thread")]
async fn mints_enumeration_matches_the_aggregate_universe() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;
    let token_program = spl_token::id().to_string();

    // A mint with nothing against it, one with only a withdrawal, and one with a deposit
    // above the bound: the three shapes that could fall out of one read but not the other.
    let bare = Pubkey::new_unique().to_string();
    let withdrawal_only = Pubkey::new_unique().to_string();
    let late_deposit = Pubkey::new_unique().to_string();
    for mint in [&bare, &withdrawal_only, &late_deposit] {
        insert_mint(&pool, mint, 6, &token_program).await?;
    }
    insert_withdrawal(&pool, "w_enum", &withdrawal_only, 100, "processing", 100).await?;
    insert_transaction(
        &pool,
        "d_enum",
        &late_deposit,
        100,
        "deposit",
        "completed",
        900,
    )
    .await?;

    let mut enumerated = storage.get_mint_addresses().await?;
    let mut aggregated: Vec<String> = storage
        .get_mint_balances_for_reconciliation(10)
        .await?
        .into_iter()
        .map(|r| r.mint_address)
        .collect();
    enumerated.sort();
    aggregated.sort();

    assert_eq!(
        enumerated, aggregated,
        "enumeration and valuation must cover the same mints"
    );
    Ok(())
}

/// A release that moves less than its row owes discharges only what moved. The rest is
/// still custody the escrow should hold, so it stays in the liabilities.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_release_subtracts_only_what_moved() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let nonce = insert_withdrawal(&pool, "w_partial", &mint, 100_000, "processing", 100).await?;
    observe_release_of(&storage, nonce, 100, 1).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(1u64)
    );
    Ok(())
}

/// A release that moves more than its row owes discharges only the row. Custody dropped by
/// the larger figure, so the excess has to stand as a shortfall rather than net itself out.
#[tokio::test(flavor = "multi_thread")]
async fn an_over_release_discharges_no_more_than_the_row_owed(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let nonce = insert_withdrawal(&pool, "w_over", &mint, 100, "processing", 100).await?;
    observe_release_of(&storage, nonce, 100, 200).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(100u64)
    );
    Ok(())
}

/// A release past `i64::MAX` is still a valid `u64` payout. Recording less than it moved
/// would leave phantom liability behind and trip a false insolvency halt.
#[tokio::test(flavor = "multi_thread")]
async fn a_release_past_i64_max_subtracts_its_full_amount() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    for amount in [i64::MAX as u64 + 1, u64::MAX] {
        let mint = Pubkey::new_unique().to_string();
        insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

        let signature = format!("w_large_{amount}");
        let nonce = insert_withdrawal(&pool, &signature, &mint, amount, "processing", 100).await?;
        observe_release_of(&storage, nonce, 100, amount).await?;

        assert_eq!(
            withdrawals_at(&storage, &mint, u64::MAX).await?,
            BigDecimal::from(amount),
            "release of {amount} must subtract in full"
        );
    }
    Ok(())
}

/// Rows written before the column existed carry no amount. They have to keep subtracting
/// the row figure, or the first read after an upgrade calls every past payout still owed.
#[tokio::test(flavor = "multi_thread")]
async fn a_release_with_no_recorded_amount_subtracts_the_row_amount(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let nonce = insert_withdrawal(&pool, "w_legacy", &mint, 100, "processing", 100).await?;
    observe_release(&storage, nonce, 100).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(100u64)
    );
    Ok(())
}

/// The status fallback has no observed release to read an amount from, but a row that has
/// one must use it there too, or the fallback would be looser than the pinned read.
#[tokio::test(flavor = "multi_thread")]
async fn the_unpinned_read_uses_the_observed_amount_when_it_has_one(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let nonce = insert_withdrawal(&pool, "w_unpinned", &mint, 100_000, "completed", 100).await?;
    observe_release_of(&storage, nonce, 100, 1).await?;

    assert_eq!(
        unpinned_withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(1u64)
    );
    Ok(())
}

/// The operator marks a row completed once its release confirms, so an unpinned ledger can
/// use that where it has no observed release of its own. The pinned read still ignores it.
#[tokio::test(flavor = "multi_thread")]
async fn completed_withdrawal_is_released_only_in_the_unpinned_read(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;
    insert_withdrawal(&pool, "w_done", &mint, 300, "completed", 100).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(0u64),
        "the pinned read subtracts only an observed release"
    );
    assert_eq!(
        unpinned_withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(300u64),
        "the unpinned read stands the row status in for the missing release"
    );
    Ok(())
}

/// Only a completed row stands in for a release. Every other state is still owed, so the
/// fallback cannot become an allowance for withdrawals that have not paid out.
#[tokio::test(flavor = "multi_thread")]
async fn unpinned_read_subtracts_no_other_status() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    for status in [
        "pending",
        "processing",
        "parked",
        "pending_remint",
        "failed_reminted",
        "manual_review",
        "failed",
    ] {
        insert_withdrawal(&pool, &format!("u_{status}"), &mint, 100, status, 100).await?;
    }

    assert_eq!(
        unpinned_withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(0u64)
    );
    Ok(())
}

/// Both arms still count in the unpinned read, and a payout carrying both an observed
/// release and a completed status is subtracted once.
#[tokio::test(flavor = "multi_thread")]
async fn unpinned_read_counts_each_payout_once() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;

    let both = insert_withdrawal(&pool, "u_both", &mint, 200, "completed", 100).await?;
    observe_release(&storage, both, 120).await?;
    let observed_only =
        insert_withdrawal(&pool, "u_observed", &mint, 50, "processing", 100).await?;
    observe_release(&storage, observed_only, 120).await?;

    assert_eq!(
        unpinned_withdrawals_at(&storage, &mint, 150).await?,
        BigDecimal::from(250u64)
    );
    Ok(())
}

/// A release whose nonce names no transaction row subtracts nothing.
#[tokio::test(flavor = "multi_thread")]
async fn observed_release_without_matching_row_changes_nothing(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;
    insert_transaction(&pool, "d", &mint, 1_000, "deposit", "completed", 100).await?;
    let nonce = insert_withdrawal(&pool, "w", &mint, 300, "processing", 100).await?;
    observe_release(&storage, nonce + 1_000, 100).await?;

    let rows = storage
        .get_mint_balances_for_reconciliation(u64::MAX)
        .await?;
    assert_eq!(rows[0].total_deposits, BigDecimal::from(1_000u64));
    assert_eq!(rows[0].total_withdrawals, BigDecimal::from(0u64));
    Ok(())
}

/// A release subtracts only under the mint of the row that carries its nonce.
#[tokio::test(flavor = "multi_thread")]
async fn observed_release_subtracts_only_under_its_rows_mint(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let token_program = spl_token::id().to_string();
    let a = Pubkey::new_unique().to_string();
    let b = Pubkey::new_unique().to_string();
    insert_mint(&pool, &a, 6, &token_program).await?;
    insert_mint(&pool, &b, 6, &token_program).await?;
    let nonce_a = insert_withdrawal(&pool, "w_a", &a, 300, "processing", 100).await?;
    insert_withdrawal(&pool, "w_b", &b, 500, "processing", 100).await?;
    observe_release(&storage, nonce_a, 100).await?;

    assert_eq!(
        withdrawals_at(&storage, &a, u64::MAX).await?,
        BigDecimal::from(300u64)
    );
    assert_eq!(
        withdrawals_at(&storage, &b, u64::MAX).await?,
        BigDecimal::from(0u64)
    );
    Ok(())
}

/// A release counts from its own slot on, whatever the row's status.
#[tokio::test(flavor = "multi_thread")]
async fn release_counts_at_the_bound_not_above() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;
    insert_transaction(&pool, "d", &mint, 1_000, "deposit", "completed", 100).await?;
    let nonce = insert_withdrawal(&pool, "w", &mint, 300, "processing", 100).await?;
    observe_release(&storage, nonce, 150).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, 149).await?,
        BigDecimal::from(0u64)
    );
    assert_eq!(
        withdrawals_at(&storage, &mint, 150).await?,
        BigDecimal::from(300u64)
    );
    assert_eq!(
        withdrawals_at(&storage, &mint, u64::MAX).await?,
        BigDecimal::from(300u64)
    );
    let at_150 = storage.get_mint_balances_for_reconciliation(150).await?;
    assert_eq!(at_150[0].total_deposits, BigDecimal::from(1_000u64));
    Ok(())
}

/// Withdrawal rows carry a channel slot, so a row far above the Solana bound still
/// subtracts once its release is observed below it.
#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_channel_slot_above_bound_still_subtracts(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;
    let nonce = insert_withdrawal(&pool, "w", &mint, 70, "completed", 10_000).await?;
    observe_release(&storage, nonce, 120).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, 150).await?,
        BigDecimal::from(70u64)
    );
    Ok(())
}

/// A release that lands after the bound is not subtracted, even once the row reads completed.
#[tokio::test(flavor = "multi_thread")]
async fn completed_withdrawal_released_above_bound_is_not_subtracted(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();
    insert_mint(&pool, &mint, 6, &spl_token::id().to_string()).await?;
    let nonce = insert_withdrawal(&pool, "w", &mint, 200, "completed", 100).await?;
    observe_release(&storage, nonce, 160).await?;

    assert_eq!(
        withdrawals_at(&storage, &mint, 150).await?,
        BigDecimal::from(0u64)
    );
    assert_eq!(
        withdrawals_at(&storage, &mint, 160).await?,
        BigDecimal::from(200u64)
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
    let nonce = insert_withdrawal(
        &pool,
        "large_withdrawal",
        &mint,
        large_amount / 2,
        "completed",
        102,
    )
    .await?;
    observe_release(&storage, nonce, 102).await?;

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
        "released withdrawal counted exactly"
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
