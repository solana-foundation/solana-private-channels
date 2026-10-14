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
        "INSERT INTO mints (mint_address, decimals, token_program, withdraw_fee, created_at)
         VALUES ($1, $2, $3, 1, NOW())",
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
          transaction_type, status, created_at, updated_at, landed_remint_signature)
         VALUES ($1, $2, 'test_initiator', 'test_recipient', $3, $4, $5::transaction_type, $6::transaction_status, NOW(), NOW(), $7)",
    )
    .bind(signature)
    .bind(slot)
    .bind(mint)
    .bind(TokenAmount(amount))
    .bind(transaction_type)
    .bind(status)
    .bind(landed_remint_for(status))
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
          transaction_type, status, created_at, updated_at, landed_remint_signature)
         VALUES ($1, $2, 'test_initiator', 'test_recipient', $3, $4, 'withdrawal', $5::transaction_status, NOW(), NOW(), $6)
         RETURNING withdrawal_nonce",
    )
    .bind(signature)
    .bind(slot)
    .bind(mint)
    .bind(TokenAmount(amount))
    .bind(status)
    .bind(landed_remint_for(status))
    .fetch_one(pool)
    .await
}

/// A reminted row must carry its landed remint (table CHECK); no other status does.
fn landed_remint_for(status: &str) -> Option<String> {
    (status == "failed_reminted").then(|| "remint-landed".to_string())
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

    let mut enumerated: Vec<String> = storage
        .get_mint_addresses()
        .await?
        .into_iter()
        .map(|(mint, _)| mint)
        .collect();
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

/// An outage halt never replaces an insolvency halt, while an insolvency replaces an outage.
/// A row written without the kind column reads as an insolvency, as every older halt was.
#[tokio::test(flavor = "multi_thread")]
async fn outage_halt_never_replaces_an_insolvency_halt() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    storage.set_reconciliation_halt("mint X insolvent").await?;
    assert!(
        !storage.set_outage_halt("inputs unavailable").await?,
        "the outage write must report that an insolvency holds"
    );
    let info = storage
        .is_reconciliation_halted()
        .await?
        .expect("still halted");
    assert_eq!(info.reason, "mint X insolvent");
    assert!(info.insolvency);

    storage.clear_reconciliation_halt().await?;
    assert!(storage.set_outage_halt("inputs unavailable").await?);
    let info = storage
        .is_reconciliation_halted()
        .await?
        .expect("outage set");
    assert_eq!(info.reason, "inputs unavailable");
    assert!(!info.insolvency);

    storage.set_reconciliation_halt("mint Y insolvent").await?;
    let info = storage.is_reconciliation_halted().await?.expect("upgraded");
    assert_eq!(info.reason, "mint Y insolvent");
    assert!(info.insolvency, "an insolvency replaces an outage halt");

    sqlx::query("DELETE FROM reconciliation_halt")
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO reconciliation_halt (id, halted, reason) VALUES (TRUE, TRUE, 'legacy')",
    )
    .execute(&pool)
    .await?;
    let info = storage
        .is_reconciliation_halted()
        .await?
        .expect("legacy row");
    assert!(info.insolvency, "an older halt row defaults to insolvency");

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

    let mut rows = storage.get_in_flight_amounts_by_mint(None, None).await?;
    rows.sort_by(|a, b| a.mint_address.cmp(&b.mint_address));
    assert!(
        rows.iter().all(|r| r.adjustment_amount == 0u64),
        "no bounds, no adjustment"
    );

    let mut by_mint: std::collections::HashMap<String, BigDecimal> = rows
        .into_iter()
        .map(|r| (r.mint_address, r.in_flight_amount))
        .collect();
    assert_eq!(by_mint.remove(&mint_a), Some(BigDecimal::from(1500u64)));
    assert_eq!(by_mint.remove(&mint_b), Some(BigDecimal::from(250u64)));
    assert!(by_mint.is_empty(), "no unexpected mints in the envelope");

    Ok(())
}

/// (in_flight, adjustment) for `mint` under the given bounds.
async fn envelope_parts(
    storage: &Storage,
    mint: &str,
    deposits_after: Option<u64>,
    withdrawals_after: Option<u64>,
) -> Result<(BigDecimal, BigDecimal), Box<dyn std::error::Error>> {
    let rows = storage
        .get_in_flight_amounts_by_mint(deposits_after, withdrawals_after)
        .await?;
    Ok(rows
        .into_iter()
        .find(|r| r.mint_address == mint)
        .map(|r| (r.in_flight_amount, r.adjustment_amount))
        .unwrap_or_default())
}

/// A settled row counts only strictly above its own type's bound: `slot = bound` is excluded,
/// `bound + 1` included, and a deposit is never judged against the withdrawal bound or back.
#[tokio::test(flavor = "multi_thread")]
async fn envelope_adjustment_boundary_per_type() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();

    insert_transaction(&pool, "d_at", &mint, 1, "deposit", "completed", 1_000).await?;
    insert_transaction(&pool, "d_above", &mint, 2, "deposit", "completed", 1_001).await?;
    insert_transaction(&pool, "w_at", &mint, 4, "withdrawal", "completed", 10).await?;
    insert_transaction(&pool, "w_above", &mint, 8, "withdrawal", "completed", 11).await?;
    // Between the two bounds: above the withdrawal bound only.
    insert_transaction(&pool, "d_mid", &mint, 16, "deposit", "completed", 500).await?;
    insert_transaction(&pool, "w_mid", &mint, 32, "withdrawal", "completed", 500).await?;

    let (in_flight, adjustment) = envelope_parts(&storage, &mint, Some(1_000), Some(10)).await?;
    assert_eq!(in_flight, BigDecimal::from(0u64));
    assert_eq!(adjustment, BigDecimal::from(2u64 + 8 + 32));

    // One arm at a time.
    let (_, deposits) = envelope_parts(&storage, &mint, Some(1_000), None).await?;
    assert_eq!(deposits, BigDecimal::from(2u64));
    let (_, withdrawals) = envelope_parts(&storage, &mint, None, Some(10)).await?;
    assert_eq!(withdrawals, BigDecimal::from(8u64 + 32));

    // u64::MAX clamps to i64::MAX and matches nothing rather than wrapping negative.
    let (_, none) = envelope_parts(&storage, &mint, Some(u64::MAX), Some(u64::MAX)).await?;
    assert_eq!(none, BigDecimal::from(0u64));
    Ok(())
}

/// The same rows and bounds as the Mock storage test give the same sums.
#[tokio::test(flavor = "multi_thread")]
async fn envelope_adjustment_matches_the_mock() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mint = Pubkey::new_unique().to_string();

    insert_transaction(&pool, "r1", &mint, 1, "deposit", "completed", 1_000).await?;
    insert_transaction(&pool, "r2", &mint, 2, "deposit", "completed", 1_001).await?;
    insert_transaction(&pool, "r3", &mint, 4, "deposit", "failed", 1_002).await?;
    insert_transaction(&pool, "r4", &mint, 8, "withdrawal", "completed", 10).await?;
    insert_transaction(&pool, "r5", &mint, 16, "withdrawal", "manual_review", 11).await?;
    insert_transaction(&pool, "r6", &mint, 32, "withdrawal", "completed", 5_000).await?;
    insert_transaction(&pool, "r7", &mint, 64, "deposit", "processing", 2_000).await?;

    let parts = envelope_parts(&storage, &mint, Some(1_000), Some(10)).await?;
    assert_eq!(
        parts,
        (
            BigDecimal::from(64u64),
            BigDecimal::from(2u64 + 4 + 16 + 32)
        )
    );
    let (_, adjustment) = envelope_parts(&storage, &mint, Some(1_000), Some(5_000)).await?;
    assert_eq!(adjustment, BigDecimal::from(2u64 + 4));
    Ok(())
}

/// Every status is either in the envelope or counted by its slot arm, never both; a new
/// status must be classified here before this compiles.
#[tokio::test(flavor = "multi_thread")]
async fn every_status_is_counted_exactly_once_above_the_bound(
) -> Result<(), Box<dyn std::error::Error>> {
    use private_channel_indexer::storage::common::models::TransactionStatus::{self, *};
    fn in_envelope(status: TransactionStatus) -> bool {
        match status {
            Pending | Processing | Parked | PendingRemint => true,
            Completed | Failed | FailedReminted | ManualReview => false,
        }
    }
    let all = [
        (Pending, "pending"),
        (Processing, "processing"),
        (Parked, "parked"),
        (PendingRemint, "pending_remint"),
        (Completed, "completed"),
        (Failed, "failed"),
        (FailedReminted, "failed_reminted"),
        (ManualReview, "manual_review"),
    ];
    let (pool, storage, _pg) = start_postgres().await?;
    for (status, label) in all {
        for kind in ["deposit", "withdrawal"] {
            let mint = Pubkey::new_unique().to_string();
            let sig = format!("{label}_{kind}");
            insert_transaction(&pool, &sig, &mint, 7, kind, label, 50).await?;
            let parts = envelope_parts(&storage, &mint, Some(10), Some(10)).await?;
            let (seven, zero) = (BigDecimal::from(7u64), BigDecimal::from(0u64));
            let expected = if in_envelope(status) {
                (seven, zero)
            } else {
                (zero, seven)
            };
            assert_eq!(parts, expected, "{label} {kind}");
        }
    }
    Ok(())
}

/// Schema init runs at every boot, so the withdrawal slot index must survive a second run.
#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_slot_index_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    storage.init_schema().await?;
    let def: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_transactions_withdrawal_slot'",
    )
    .fetch_one(&pool)
    .await?;
    assert!(def.contains("(slot)"), "{def}");
    assert!(def.contains("'withdrawal'"), "{def}");
    Ok(())
}
