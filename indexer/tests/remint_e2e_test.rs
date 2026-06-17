//! End-to-end integration tests for the remint recovery flow.
//!
//! These tests exercise the full Postgres storage layer for PendingRemint
//! state: persistence, retrieval, and status transitions. The parsing and
//! in-memory recovery logic is covered by unit tests in state.rs; here we
//! verify the underlying SQL queries and schema migrations work correctly
//! against a real database.
//!
//! Uses testcontainers for isolated Postgres instances.

use bigdecimal::BigDecimal;
use chrono::Utc;
use private_channel_indexer::{
    storage::{common::amount::TokenAmount, PostgresDb, Storage, TransactionStatus},
    PostgresConfig,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Insert a minimal mint row so the balance queries have something to aggregate.
async fn insert_mint(pool: &PgPool, mint: &Pubkey) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mints (mint_address, decimals, token_program)
         VALUES ($1, 6, $2)",
    )
    .bind(mint.to_string())
    .bind(spl_token::id().to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a completed deposit so escrow has a baseline balance.
async fn insert_deposit(pool: &PgPool, mint: &Pubkey, amount: u64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, trace_id, created_at, updated_at)
         VALUES ($1, 100, $2, $3, $4, $5,
                 'deposit'::transaction_type,
                 'completed'::transaction_status,
                 $6, NOW(), NOW())",
    )
    .bind(Signature::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(mint.to_string())
    .bind(TokenAmount(amount))
    .bind(uuid::Uuid::new_v4().to_string())
    .execute(pool)
    .await?;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Start a fresh Postgres container, initialize the full schema, and return
/// (pool, Storage, container). The container must be kept alive for the
/// duration of the test.
async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("remint_e2e_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/remint_e2e_test",
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

/// Insert a minimal withdrawal transaction directly via SQL and return its
/// generated id. Uses a valid base58 pubkey for mint and recipient so the
/// recovery parsing logic can reconstruct them without error.
async fn insert_withdrawal(
    pool: &PgPool,
    mint: &Pubkey,
    recipient: &Pubkey,
    amount: u64,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query_scalar::<_, i64>(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, trace_id, created_at, updated_at)
         VALUES ($1, 100, $2, $3, $4, $5,
                 'withdrawal'::transaction_type,
                 'processing'::transaction_status,
                 $6, NOW(), NOW())
         RETURNING id",
    )
    .bind(Signature::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string()) // initiator
    .bind(recipient.to_string())
    .bind(mint.to_string())
    .bind(TokenAmount(amount))
    .bind(uuid::Uuid::new_v4().to_string())
    .fetch_one(pool)
    .await?;

    Ok(row)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Verifies the full Postgres roundtrip for PendingRemint state:
///
/// 1. A withdrawal transaction is inserted in Processing status.
/// 2. `set_pending_remint` transitions it to PendingRemint, storing the
///    withdrawal signatures and deadline in the new columns.
/// 3. `get_pending_remint_transactions` retrieves exactly that row with all
///    fields intact — including the signature strings and deadline timestamp.
///
/// This proves the schema migrations ran correctly and the SQL queries for
/// both writes and reads work against a real Postgres instance. The in-memory
/// reconstruction from those rows is already covered by unit tests in state.rs.
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_remint_persisted_and_recovered() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let sig1 = Signature::new_unique();
    let sig2 = Signature::new_unique();
    let amount: u64 = 10_000;

    // Insert the transaction in Processing status (normal pre-failure state).
    let tx_id = insert_withdrawal(&pool, &mint, &recipient, amount).await?;

    // Simulate handle_permanent_failure persisting the remint state.
    // Deadline is 32s from now — the finality safety window.
    // Distinct lvbh values per sig so we can verify index-aligned roundtrip.
    let lvbh_sig1: i64 = 1_000;
    let lvbh_sig2: i64 = 2_000;
    let deadline = Utc::now() + chrono::Duration::seconds(32);
    storage
        .set_pending_remint(
            tx_id,
            vec![sig1.to_string(), sig2.to_string()],
            vec![lvbh_sig1, lvbh_sig2],
            deadline,
        )
        .await?;

    // Simulate restart: fetch all PendingRemint rows as recover_pending_remints would.
    let pending = storage.get_pending_remint_transactions().await?;

    // Exactly one row should come back.
    assert_eq!(pending.len(), 1, "exactly one PendingRemint row expected");
    let row = &pending[0];

    // Identity and amount.
    assert_eq!(row.id, tx_id);
    assert_eq!(row.amount, TokenAmount(amount));

    // Pubkeys stored as strings and must round-trip correctly.
    assert_eq!(row.mint, mint.to_string());
    assert_eq!(row.recipient, recipient.to_string());

    // Both withdrawal signatures must be persisted — the finality check
    // queries all of them to ensure none of the retry attempts finalized.
    let stored_sigs = row
        .remint_signatures
        .as_ref()
        .expect("signatures must be stored");
    assert_eq!(stored_sigs.len(), 2);
    assert!(
        stored_sigs.contains(&sig1.to_string()),
        "sig1 must be stored"
    );
    assert!(
        stored_sigs.contains(&sig2.to_string()),
        "sig2 must be stored"
    );

    // last_valid_block_height per sig must be stored index-aligned with the
    // sig array. The gate uses this to determine whether a broadcast can
    // still land, so reordering or losing it would re-open the audit hole.
    let stored_lvbhs = row
        .remint_last_valid_block_heights
        .as_ref()
        .expect("last_valid_block_heights must be stored");
    assert_eq!(
        stored_lvbhs.len(),
        stored_sigs.len(),
        "lvbh array must be the same length as the signature array"
    );
    let sig1_index = stored_sigs
        .iter()
        .position(|stored_sig| stored_sig == &sig1.to_string())
        .expect("sig1 must be in stored array");
    let sig2_index = stored_sigs
        .iter()
        .position(|stored_sig| stored_sig == &sig2.to_string())
        .expect("sig2 must be in stored array");
    assert_eq!(
        stored_lvbhs[sig1_index], lvbh_sig1,
        "sig1's lvbh must round-trip in the same slot as sig1"
    );
    assert_eq!(
        stored_lvbhs[sig2_index], lvbh_sig2,
        "sig2's lvbh must round-trip in the same slot as sig2"
    );

    // Deadline must be stored and within 2s of what we wrote (Postgres
    // TIMESTAMPTZ has microsecond precision; clock skew between write and
    // read is negligible in practice).
    let stored_deadline = row
        .pending_remint_deadline_at
        .expect("deadline must be stored");
    assert!(
        (stored_deadline - deadline).num_milliseconds().abs() < 2_000,
        "stored deadline should match written deadline, got {stored_deadline}"
    );

    Ok(())
}

/// Verifies that a resolved PendingRemint row is never re-queued on restart.
///
/// The recovery query (`get_pending_remint_transactions`) filters strictly on
/// `status = 'pending_remint'`. Once the operator resolves a row — either by
/// completing a successful remint (FailedReminted), finding the original
/// withdrawal finalized (Completed), or escalating to ManualReview — that row
/// must no longer appear in recovery results.
///
/// Without this guarantee, a crash immediately after resolving a row but
/// before the in-memory queue is drained could cause the row to be re-queued
/// on restart, triggering a duplicate remint.
#[tokio::test(flavor = "multi_thread")]
async fn test_resolved_remint_not_returned_by_recovery_query(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let sig = Signature::new_unique();
    let deadline = Utc::now() + chrono::Duration::seconds(32);

    // Insert and transition to PendingRemint.
    let tx_id = insert_withdrawal(&pool, &mint, &recipient, 5_000).await?;
    storage
        .set_pending_remint(tx_id, vec![sig.to_string()], vec![0], deadline)
        .await?;

    // Confirm it shows up in recovery before resolution.
    let before = storage.get_pending_remint_transactions().await?;
    assert_eq!(before.len(), 1, "row should appear before resolution");

    // Simulate the operator resolving the row (e.g. withdrawal was finalized).
    storage
        .update_transaction_status(
            tx_id,
            TransactionStatus::Completed,
            Some(sig.to_string()),
            Utc::now(),
        )
        .await?;

    // After resolution the row must be invisible to the recovery query.
    // A subsequent restart must not re-queue this entry.
    let after = storage.get_pending_remint_transactions().await?;
    assert!(
        after.is_empty(),
        "resolved row must not appear in recovery query — would cause duplicate remint"
    );

    Ok(())
}

/// Verifies that a PendingRemint row resolved via FailedReminted is not re-queued.     
///                                                                                     
/// After a successful remint, update_transaction_status transitions the row to
/// FailedReminted. The recovery query must not return it on the next startup —         
/// otherwise the remint would fire again even though tokens were already re-minted.    
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_reminted_row_not_returned_by_recovery_query(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let sig = Signature::new_unique();
    let deadline = Utc::now() + chrono::Duration::seconds(32);

    // 1. Insert withdrawal and transition to PendingRemint.
    let tx_id = insert_withdrawal(&pool, &mint, &recipient, 5_000).await?;
    storage
        .set_pending_remint(tx_id, vec![sig.to_string()], vec![0], deadline)
        .await?;

    // Confirm it appears in recovery before resolution.
    let before = storage.get_pending_remint_transactions().await?;
    assert_eq!(before.len(), 1);

    // 2. Simulate a successful remint: status transitions to FailedReminted.
    //    (counterpart_signature is None for FailedReminted — the withdrawal never
    //    landed, so there is no release_funds sig to record.)
    storage
        .update_transaction_status(tx_id, TransactionStatus::FailedReminted, None, Utc::now())
        .await?;

    // 3. Row must not appear in recovery — a second remint would double-credit the user.
    let after = storage.get_pending_remint_transactions().await?;
    assert!(
        after.is_empty(),
        "FailedReminted row must not appear in recovery query — would cause duplicateremint"
    );

    Ok(())
}

/// Full lifecycle: withdrawal failure → PendingRemint → remint → balance restored.
///
/// This test walks the complete happy-path of the remint recovery flow from the
/// storage layer's perspective:
///
/// 1. A deposit lands — escrow holds 10,000 tokens.
/// 2. A withdrawal of 10,000 is attempted but fails permanently after max retries.
///    `handle_permanent_failure` calls `set_pending_remint`, transitioning the row
///    to PendingRemint and storing the withdrawal signatures for the finality check.
/// 3. While the row is in PendingRemint the balance query must NOT count the
///    withdrawal against escrow — the release_funds never landed on-chain, so the
///    tokens are still there.
/// 4. The finality window passes, no withdrawal sig is found finalized, the remint
///    succeeds, and `update_transaction_status(FailedReminted)` is called.
/// 5. After resolution:
///    - The recovery query returns no rows (no duplicate remint on restart).
///    - The balance query still shows the full deposit with zero withdrawals —
///      FailedReminted is not counted in total_withdrawals, correctly reflecting
///      that the escrow still holds the original deposit.
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_failure_remint_restores_balance() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let withdrawal_sig = Signature::new_unique();
    let deposit_amount: u64 = 10_000;
    let withdrawal_amount: u64 = 10_000;

    // Step 1: a deposit lands — tokens enter the escrow.
    insert_mint(&pool, &mint).await?;
    insert_deposit(&pool, &mint, deposit_amount).await?;

    // Step 2: withdrawal is attempted but fails permanently.
    // `set_pending_remint` atomically transitions to PendingRemint, storing the
    // withdrawal signature needed for the finality check after the delay.
    let deadline = Utc::now() + chrono::Duration::seconds(32);
    let tx_id = insert_withdrawal(&pool, &mint, &recipient, withdrawal_amount).await?;
    storage
        .set_pending_remint(tx_id, vec![withdrawal_sig.to_string()], vec![0], deadline)
        .await?;

    // Step 3: while the row is in PendingRemint the balance must reflect the
    // on-chain reality — the withdrawal never landed, so the escrow still holds
    // the full deposit. Counting a PendingRemint withdrawal would undercount
    // the escrow balance and produce a false reconciliation mismatch.
    let pending = storage.get_mint_balances_for_reconciliation().await?;
    let balance = pending
        .iter()
        .find(|b| b.mint_address == mint.to_string())
        .expect("mint must appear in balance query");
    assert_eq!(
        balance.total_deposits,
        BigDecimal::from(deposit_amount),
        "full deposit must be counted"
    );
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(0u64),
        "PendingRemint withdrawal must NOT be counted — it never landed on-chain"
    );

    // Step 4: finality check passes (no sig finalized), remint succeeds.
    storage
        .update_transaction_status(tx_id, TransactionStatus::FailedReminted, None, Utc::now())
        .await?;

    // Step 5a: resolved row must not surface in recovery — prevents duplicate remint.
    let after_recovery = storage.get_pending_remint_transactions().await?;
    assert!(
        after_recovery.is_empty(),
        "FailedReminted row must not appear in recovery query"
    );

    // Step 5b: balance query must remain unchanged after the remint.
    // The failed withdrawal is still NOT in total_withdrawals — FailedReminted
    // means the withdrawal never succeeded on-chain. The escrow still holds the
    // deposit. The user received their tokens back on PrivateChannel via remint.
    let after_balances = storage.get_mint_balances_for_reconciliation().await?;
    let after_balance = after_balances
        .iter()
        .find(|b| b.mint_address == mint.to_string())
        .expect("mint must still appear in balance query");
    assert_eq!(
        after_balance.total_deposits,
        BigDecimal::from(deposit_amount),
        "deposit must still be counted after remint"
    );
    assert_eq!(
        after_balance.total_withdrawals,
        BigDecimal::from(0u64),
        "FailedReminted withdrawal must NOT be counted in total_withdrawals"
    );

    Ok(())
}

/// Multiple concurrent PendingRemint rows must all be excluded from the balance
/// query. The query must show zero withdrawals regardless of how many
/// PendingRemint rows are active at once.
///
/// A single-row test already covers the basic case; this test confirms there
/// is no per-row aggregation bug — e.g. a SUM that accidentally includes
/// some but not all PendingRemint rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_pending_remints_excluded_from_balance(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let deposit_amount: u64 = 30_000;

    insert_mint(&pool, &mint).await?;
    insert_deposit(&pool, &mint, deposit_amount).await?;

    let deadline = Utc::now() + chrono::Duration::seconds(32);

    // Three concurrent withdrawal failures — all in PendingRemint simultaneously.
    for amount in [10_000u64, 8_000, 12_000] {
        let tx_id = insert_withdrawal(&pool, &mint, &Pubkey::new_unique(), amount).await?;
        storage
            .set_pending_remint(
                tx_id,
                vec![Signature::new_unique().to_string()],
                vec![0],
                deadline,
            )
            .await?;
    }

    // All three PendingRemint withdrawals must be invisible to the balance query.
    let balances = storage.get_mint_balances_for_reconciliation().await?;
    let balance = balances
        .iter()
        .find(|b| b.mint_address == mint.to_string())
        .expect("mint must appear in balance query");

    assert_eq!(balance.total_deposits, BigDecimal::from(deposit_amount));
    assert_eq!(
        balance.total_withdrawals,
        BigDecimal::from(0u64),
        "all three PendingRemint withdrawals must be excluded from total_withdrawals"
    );

    // Recovery query must return all three rows for the next startup.
    let pending = storage.get_pending_remint_transactions().await?;
    assert_eq!(
        pending.len(),
        3,
        "all three PendingRemint rows must be recoverable"
    );

    Ok(())
}

/// A PendingRemint row escalated to ManualReview must not resurface in the
/// recovery query on subsequent restarts. The same invariant as Completed and
/// FailedReminted: once resolved, the row must be invisible.
///
/// Without this, every restart would re-queue the row and trigger another
/// ManualReview alert — and if the alert is mis-handled, could lead to
/// duplicate remints after manual intervention.
#[tokio::test(flavor = "multi_thread")]
async fn test_manual_review_not_returned_by_recovery_query(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let sig = Signature::new_unique();
    let deadline = Utc::now() + chrono::Duration::seconds(32);

    let tx_id = insert_withdrawal(&pool, &mint, &recipient, 5_000).await?;
    storage
        .set_pending_remint(tx_id, vec![sig.to_string()], vec![0], deadline)
        .await?;

    // Confirm it appears before resolution.
    let before = storage.get_pending_remint_transactions().await?;
    assert_eq!(before.len(), 1, "row should appear before resolution");

    // Simulate exhausted finality check retries escalating to ManualReview.
    storage
        .update_transaction_status(tx_id, TransactionStatus::ManualReview, None, Utc::now())
        .await?;

    // Must not appear in recovery — would loop re-queueing on every restart.
    let after = storage.get_pending_remint_transactions().await?;
    assert!(
        after.is_empty(),
        "ManualReview row must not appear in recovery query — would cause looping re-queue"
    );

    Ok(())
}
