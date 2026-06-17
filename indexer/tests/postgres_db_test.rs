//! Integration tests for `PostgresDb` methods against a real Postgres via testcontainers.
//!
//! Covers: schema lifecycle, single/batch inserts, pending/lock queries,
//! status updates, checkpoints, mints, reconciliation balances, and withdrawal nonces.
//!
//! Run with: `cd indexer && cargo test --test postgres_db_test -- --test-threads=1`

use bigdecimal::BigDecimal;
use chrono::Utc;
use private_channel_indexer::{
    storage::{
        common::amount::TokenAmount,
        common::models::{DbMint, DbMintStatus, MintStatusAtSlot},
        DbTransaction, PostgresDb, Storage, TransactionStatus, TransactionType,
    },
    PostgresConfig,
};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("db_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{}:{}/db_test", host, port);

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

fn make_db_transaction(sig: &str, txn_type: TransactionType) -> DbTransaction {
    DbTransaction {
        id: 0,
        signature: sig.to_string(),
        trace_id: format!("trace-{sig}"),
        slot: 100,
        initiator: "initiator".to_string(),
        recipient: "recipient".to_string(),
        mint: "mint_addr".to_string(),
        amount: TokenAmount(1_000),
        memo: None,
        transaction_type: txn_type,
        withdrawal_nonce: None,
        status: TransactionStatus::Pending,
        created_at: Utc::now(),
        updated_at: Utc::now(),
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
    }
}

// ── 1. Schema lifecycle ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn init_schema_tables_exist() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, _storage, _pg) = start_postgres().await?;

    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name::text FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name IN ('transactions', 'indexer_state', 'mints')
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await?;

    let names: Vec<&str> = tables.iter().map(|(n,)| n.as_str()).collect();
    assert_eq!(names, vec!["indexer_state", "mints", "transactions"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn init_schema_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    // Second call should not error
    storage.init_schema().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_tables_then_reinit() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    storage.drop_tables().await?;

    // Tables gone
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::bigint FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name IN ('transactions', 'indexer_state', 'mints')",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(count.0, 0);

    // Re-init works
    storage.init_schema().await?;

    let count2: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::bigint FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name IN ('transactions', 'indexer_state', 'mints')",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(count2.0, 3);
    Ok(())
}

/// A database created before this change has `amount BIGINT`. init_schema must
/// widen it to NUMERIC(20,0) in place, after which a value above i64::MAX (which
/// BIGINT could not hold) round-trips through the TokenAmount decode path.
#[tokio::test(flavor = "multi_thread")]
async fn init_schema_widens_legacy_bigint_amount_column() -> Result<(), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("db_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{}:{}/db_test", host, port);
    let pool = PgPool::connect(&db_url).await?;

    // Stand up the pre-change shape: the base transactions table with a BIGINT
    // amount (newer columns are added by init_schema's ALTER migrations).
    sqlx::query(
        "CREATE TYPE transaction_status AS ENUM ('pending', 'processing', 'completed', 'failed')",
    )
    .execute(&pool)
    .await?;
    sqlx::query("CREATE TYPE transaction_type AS ENUM ('deposit', 'withdrawal')")
        .execute(&pool)
        .await?;
    sqlx::query(
        r#"
        CREATE TABLE transactions (
            id BIGSERIAL PRIMARY KEY,
            signature TEXT NOT NULL UNIQUE,
            slot BIGINT NOT NULL,
            initiator TEXT NOT NULL,
            recipient TEXT NOT NULL,
            mint TEXT NOT NULL,
            amount BIGINT NOT NULL,
            memo TEXT,
            status transaction_status NOT NULL DEFAULT 'pending',
            transaction_type transaction_type NOT NULL,
            withdrawal_nonce BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ,
            counterpart_signature TEXT
        );
        "#,
    )
    .execute(&pool)
    .await?;

    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url,
            max_connections: 5,
        })
        .await?,
    );
    storage.init_schema().await?;

    // The column type must now be numeric, not bigint.
    let (data_type,): (String,) = sqlx::query_as(
        "SELECT data_type::text FROM information_schema.columns
         WHERE table_name = 'transactions' AND column_name = 'amount'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(data_type, "numeric", "amount must be widened to NUMERIC");

    // A value BIGINT could never have stored must now round-trip exactly.
    let big = TokenAmount(i64::MAX as u64 + 1);
    let mut txn = make_db_transaction("legacy_big", TransactionType::Deposit);
    txn.amount = big;
    let id = storage.insert_db_transaction(&txn).await?;
    let (got,): (TokenAmount,) = sqlx::query_as("SELECT amount FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(got, big);
    Ok(())
}

// ── 2. Single transaction insert ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn insert_transaction_returns_id() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("sig_1", TransactionType::Deposit);

    let id = storage.insert_db_transaction(&txn).await?;
    assert!(id > 0);

    // Readable back; amount is NUMERIC and decodes through the TokenAmount seam.
    let row: (String, TokenAmount) =
        sqlx::query_as("SELECT signature, amount FROM transactions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(row.0, "sig_1");
    assert_eq!(row.1, TokenAmount(1_000));
    Ok(())
}

/// A deposit and a withdrawal of `i64::MAX + 1` (a value BIGINT would have
/// wrapped to a negative i64) must round-trip through NUMERIC bit-for-bit, both
/// when read back as the raw column and through the DbTransaction FromRow path.
#[tokio::test(flavor = "multi_thread")]
async fn amount_above_i64_max_round_trips_exactly() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let big = i64::MAX as u64 + 1;

    let mut deposit = make_db_transaction("big_deposit", TransactionType::Deposit);
    deposit.amount = TokenAmount(big);
    let mut withdrawal = make_db_transaction("big_withdrawal", TransactionType::Withdrawal);
    withdrawal.amount = TokenAmount(big);

    let deposit_id = storage.insert_db_transaction(&deposit).await?;
    let withdrawal_id = storage.insert_db_transaction(&withdrawal).await?;

    for id in [deposit_id, withdrawal_id] {
        let row: (TokenAmount,) = sqlx::query_as("SELECT amount FROM transactions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            row.0,
            TokenAmount(big),
            "raw column must preserve the full u64"
        );
    }

    let fetched = storage
        .get_pending_db_transactions(TransactionType::Deposit, 10)
        .await?;
    let got = fetched
        .iter()
        .find(|t| t.signature == "big_deposit")
        .expect("deposit row");
    assert_eq!(
        got.amount,
        TokenAmount(big),
        "FromRow must preserve the full u64"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_transaction_duplicate_returns_same_id() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("dup_sig", TransactionType::Deposit);

    let id1 = storage.insert_db_transaction(&txn).await?;
    let id2 = storage.insert_db_transaction(&txn).await?;
    assert_eq!(id1, id2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_withdrawal_auto_assigns_nonce() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("withdrawal_1", TransactionType::Withdrawal);

    let id = storage.insert_db_transaction(&txn).await?;

    let nonce: (Option<i64>,) =
        sqlx::query_as("SELECT withdrawal_nonce FROM transactions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
    assert!(nonce.0.is_some(), "withdrawal should have a nonce assigned");
    Ok(())
}

// ── 3. Batch insert ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn batch_insert_empty() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let ids = storage.insert_db_transactions_batch(&[]).await?;
    assert!(ids.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_insert_three() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let txns = vec![
        make_db_transaction("b1", TransactionType::Deposit),
        make_db_transaction("b2", TransactionType::Deposit),
        make_db_transaction("b3", TransactionType::Deposit),
    ];
    let ids = storage.insert_db_transactions_batch(&txns).await?;
    assert_eq!(ids.len(), 3);
    // All unique
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_insert_with_duplicate() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    // Pre-insert one
    let existing = make_db_transaction("dup_batch", TransactionType::Deposit);
    let pre_id = storage.insert_db_transaction(&existing).await?;

    let txns = vec![
        make_db_transaction("dup_batch", TransactionType::Deposit), // duplicate
        make_db_transaction("new_batch", TransactionType::Deposit),
    ];
    let ids = storage.insert_db_transactions_batch(&txns).await?;
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], pre_id, "duplicate should return existing id");
    assert_ne!(ids[1], pre_id, "new should get a different id");
    Ok(())
}

// ── 4. Get pending / lock ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn get_pending_withdrawals_filters_correctly() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    // Insert pending withdrawal
    let w = make_db_transaction("pending_w", TransactionType::Withdrawal);
    storage.insert_db_transaction(&w).await?;

    // Insert pending deposit (should not appear)
    let d = make_db_transaction("pending_d", TransactionType::Deposit);
    storage.insert_db_transaction(&d).await?;

    // Insert completed withdrawal (should not appear)
    let cw = make_db_transaction("completed_w", TransactionType::Withdrawal);
    let cw_id = storage.insert_db_transaction(&cw).await?;
    sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
        .bind(cw_id)
        .execute(&pool)
        .await?;

    let pending = storage
        .get_pending_db_transactions(TransactionType::Withdrawal, 100)
        .await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].signature, "pending_w");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_pending_empty() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let pending = storage
        .get_pending_db_transactions(TransactionType::Withdrawal, 100)
        .await?;
    assert!(pending.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lock_pending_sets_processing() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let txn = make_db_transaction("lock_me", TransactionType::Withdrawal);
    storage.insert_db_transaction(&txn).await?;

    let locked = storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;
    assert_eq!(locked.len(), 1);
    assert_eq!(locked[0].signature, "lock_me");
    // Status returned is the pre-update value (Pending) but DB has Processing

    // Second lock call should be empty (already Processing)
    let locked2 = storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;
    assert!(locked2.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lock_pending_second_call_empty() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let txn = make_db_transaction("lock2", TransactionType::Deposit);
    storage.insert_db_transaction(&txn).await?;

    let _ = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;
    let second = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;
    assert!(second.is_empty());
    Ok(())
}

// ── 5. Get all transactions ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn get_all_transactions_returns_all_statuses() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    // Insert two deposits with different statuses
    let d1 = make_db_transaction("all_d1", TransactionType::Deposit);
    let d1_id = storage.insert_db_transaction(&d1).await?;
    sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
        .bind(d1_id)
        .execute(&pool)
        .await?;

    let d2 = make_db_transaction("all_d2", TransactionType::Deposit);
    storage.insert_db_transaction(&d2).await?; // stays pending

    let all = storage
        .get_all_db_transactions(TransactionType::Deposit, 100)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    assert_eq!(all.len(), 2);
    Ok(())
}

// ── 6. Update status ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn update_transaction_status_updates_fields() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let txn = make_db_transaction("upd_status", TransactionType::Deposit);
    let id = storage.insert_db_transaction(&txn).await?;

    // Production lifecycle: fetcher must flip to `processing` first.
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;

    let now = Utc::now();
    let written = storage
        .update_transaction_status(
            id,
            TransactionStatus::Completed,
            Some("counter_sig".to_string()),
            now,
        )
        .await?;
    assert!(
        written,
        "row was in Processing, terminal write should report Ok(true)"
    );

    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT status::text, counterpart_signature FROM transactions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "completed");
    assert_eq!(row.1.as_deref(), Some("counter_sig"));
    Ok(())
}

// ── 7. Checkpoints ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_no_row_returns_none() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let cp = storage.get_committed_checkpoint("test_program").await?;
    assert!(cp.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_upsert_and_get() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    storage
        .update_committed_checkpoint("test_program", 42)
        .await?;
    let cp = storage.get_committed_checkpoint("test_program").await?;
    assert_eq!(cp, Some(42));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_update_higher_slot() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    storage.update_committed_checkpoint("prog", 10).await?;
    storage.update_committed_checkpoint("prog", 99).await?;

    let cp = storage.get_committed_checkpoint("prog").await?;
    assert_eq!(cp, Some(99));
    Ok(())
}

/// Monotonic guard: lower slot never overwrites a higher one.
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_update_lower_slot_is_noop() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    storage.update_committed_checkpoint("prog", 500).await?;
    storage.update_committed_checkpoint("prog", 100).await?;

    let cp = storage.get_committed_checkpoint("prog").await?;
    assert_eq!(
        cp,
        Some(500),
        "lower-slot write must not regress the persisted checkpoint"
    );
    Ok(())
}

// ── 8. Mint operations ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn upsert_mints_empty_ok() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    storage.upsert_mints_batch(&[]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_and_get_mints() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let m1 = DbMint::new("mint_a".to_string(), 6, "TokenkegQ".to_string());
    let m2 = DbMint::new("mint_b".to_string(), 9, "TokenzQdB".to_string());
    storage.upsert_mints_batch(&[m1, m2]).await?;

    let got_a = storage.get_mint("mint_a").await?;
    assert!(got_a.is_some());
    assert_eq!(got_a.unwrap().decimals, 6);

    let got_b = storage.get_mint("mint_b").await?;
    assert!(got_b.is_some());
    assert_eq!(got_b.unwrap().decimals, 9);

    // Missing mint
    let got_c = storage.get_mint("mint_c").await?;
    assert!(got_c.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_mint_updates_decimals() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let m = DbMint::new("mint_upd".to_string(), 6, "TokenkegQ".to_string());
    storage.upsert_mints_batch(&[m]).await?;

    // Upsert with new decimals
    let m2 = DbMint::new("mint_upd".to_string(), 9, "TokenkegQ".to_string());
    storage.upsert_mints_batch(&[m2]).await?;

    let got = storage.get_mint("mint_upd").await?.unwrap();
    assert_eq!(got.decimals, 9);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_mint_status_mirrors_history_against_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    storage
        .upsert_mints_batch(&[DbMint::new("sm".to_string(), 6, "TokenkegQ".to_string())])
        .await?;
    assert_eq!(storage.get_mint("sm").await?.unwrap().status, "allowed");

    // allowed@10 then blocked@20 → mirror resolves to the latest (blocked),
    // metadata untouched.
    storage
        .insert_mint_statuses_batch(&[
            mk_status("sm", "allowed", 10, "sig-a"),
            mk_status("sm", "blocked", 20, "sig-b"),
        ])
        .await?;
    storage.sync_mint_status(&["sm".to_string()]).await?;
    let got = storage.get_mint("sm").await?.unwrap();
    assert_eq!(got.status, "blocked");
    assert_eq!(got.decimals, 6);
    assert_eq!(got.token_program, "TokenkegQ");

    // Re-allow at a later slot → mirror flips back.
    storage
        .insert_mint_statuses_batch(&[mk_status("sm", "allowed", 30, "sig-c")])
        .await?;
    storage.sync_mint_status(&["sm".to_string()]).await?;
    assert_eq!(storage.get_mint("sm").await?.unwrap().status, "allowed");

    // Re-running the upsert (slot replay) must not clobber a later block: block
    // it again, re-upsert, and confirm the mirror still reflects history.
    storage
        .insert_mint_statuses_batch(&[mk_status("sm", "blocked", 40, "sig-d")])
        .await?;
    storage.sync_mint_status(&["sm".to_string()]).await?;
    storage
        .upsert_mints_batch(&[DbMint::new("sm".to_string(), 6, "TokenkegQ".to_string())])
        .await?;
    assert_eq!(
        storage.get_mint("sm").await?.unwrap().status,
        "blocked",
        "upsert (re-allow ingest / replay) must not touch status"
    );

    // Syncing a mint with no row is a no-op (no error).
    storage
        .sync_mint_status(&["no_such_mint".to_string()])
        .await?;
    Ok(())
}

// ── 9. Reconciliation balance ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn reconciliation_balance_counts_correctly() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = "recon_mint";
    let tp = "TokenkegQ";
    storage
        .upsert_mints_batch(&[DbMint::new(mint.to_string(), 6, tp.to_string())])
        .await?;

    // Pending deposit (ALL deposits count for reconciliation)
    let mut d1 = make_db_transaction("recon_d1", TransactionType::Deposit);
    d1.mint = mint.to_string();
    d1.amount = TokenAmount(500);
    storage.insert_db_transaction(&d1).await?;

    // Completed deposit
    let mut d2 = make_db_transaction("recon_d2", TransactionType::Deposit);
    d2.mint = mint.to_string();
    d2.amount = TokenAmount(300);
    let d2_id = storage.insert_db_transaction(&d2).await?;
    sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
        .bind(d2_id)
        .execute(&pool)
        .await?;

    // Completed withdrawal (only completed withdrawals count)
    let mut w1 = make_db_transaction("recon_w1", TransactionType::Withdrawal);
    w1.mint = mint.to_string();
    w1.amount = TokenAmount(100);
    let w1_id = storage.insert_db_transaction(&w1).await?;
    sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
        .bind(w1_id)
        .execute(&pool)
        .await?;

    // Pending withdrawal (should NOT count)
    let mut w2 = make_db_transaction("recon_w2", TransactionType::Withdrawal);
    w2.mint = mint.to_string();
    w2.amount = TokenAmount(9999);
    storage.insert_db_transaction(&w2).await?;

    let balances = storage.get_mint_balances_for_reconciliation().await?;
    assert_eq!(balances.len(), 1);
    // Deposits: 500 (pending) + 300 (completed) = 800  (all statuses)
    assert_eq!(balances[0].total_deposits, BigDecimal::from(800u64));
    // Withdrawals: only completed = 100
    assert_eq!(balances[0].total_withdrawals, BigDecimal::from(100u64));
    Ok(())
}

// ── 10. Withdrawal nonces ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn completed_withdrawal_nonces_in_range() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    // Insert 3 withdrawals (auto-nonces: 0, 1, 2)
    for i in 0..3 {
        let w = make_db_transaction(&format!("wnonce_{i}"), TransactionType::Withdrawal);
        let wid = storage.insert_db_transaction(&w).await?;
        if i < 2 {
            // Complete first two
            sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
                .bind(wid)
                .execute(&pool)
                .await?;
        }
    }

    let nonces = storage.get_completed_withdrawal_nonces(0, 10).await?;
    assert_eq!(nonces.len(), 2);
    // third withdrawal (nonce=2) is still pending, should not appear
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn completed_withdrawal_nonces_empty() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let nonces = storage.get_completed_withdrawal_nonces(0, 100).await?;
    assert!(nonces.is_empty());
    Ok(())
}

// ── set_pending_remint status guard ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn set_pending_remint_succeeds_when_processing() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let txn = make_db_transaction("remint_processing", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    // Lock to transition to Processing
    storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;

    let deadline = Utc::now() + chrono::Duration::seconds(32);
    storage
        .set_pending_remint(id, vec!["sig1".to_string()], vec![0], deadline)
        .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn set_pending_remint_fails_when_not_processing() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let txn = make_db_transaction("remint_completed", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    sqlx::query("UPDATE transactions SET status = 'completed' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;

    let deadline = Utc::now() + chrono::Duration::seconds(32);
    let result = storage
        .set_pending_remint(id, vec!["sig1".to_string()], vec![0], deadline)
        .await;

    assert!(result.is_err(), "should fail when status is not processing");
    Ok(())
}

// ── set_mint_extension_flags row-exists guard ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn set_mint_extension_flags_updates_existing_row() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let m = DbMint::new("mint_ext".to_string(), 6, "TokenkegQ".to_string());
    storage.upsert_mints_batch(&[m]).await?;
    let row = storage.get_mint("mint_ext").await?.unwrap();
    assert_eq!(row.is_pausable, None, "upsert should not set is_pausable");
    assert_eq!(
        row.has_permanent_delegate, None,
        "upsert should not set has_permanent_delegate",
    );

    storage
        .set_mint_extension_flags("mint_ext", true, false)
        .await?;
    let row = storage.get_mint("mint_ext").await?.unwrap();
    assert_eq!(row.is_pausable, Some(true));
    assert_eq!(row.has_permanent_delegate, Some(false));

    // Idempotent — writing the same values again is fine.
    storage
        .set_mint_extension_flags("mint_ext", true, false)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn set_mint_extension_flags_fails_when_no_row() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;

    let result = storage
        .set_mint_extension_flags("mint_never_upserted", true, false)
        .await;

    assert!(result.is_err(), "should fail when mints row doesn't exist");
    Ok(())
}

// ── mint_status_history ──────────────────────────────────────────────────────

fn mk_status(mint: &str, status: &str, slot: i64, sig: &str) -> DbMintStatus {
    DbMintStatus {
        mint_address: mint.to_string(),
        status: status.to_string(),
        effective_slot: slot,
        signature: sig.to_string(),
        created_at: Utc::now(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_mint_statuses_batch_persists_rows_pg() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    storage
        .insert_mint_statuses_batch(&[mk_status("mint_pg1", "allowed", 100, "sig-1")])
        .await?;
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::BIGINT FROM mint_status_history WHERE mint_address = $1")
            .bind("mint_pg1")
            .fetch_one(&pool)
            .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_mint_statuses_batch_idempotent_on_pk_conflict_pg(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let row = mk_status("mint_pg2", "allowed", 100, "sig-1");
    storage
        .insert_mint_statuses_batch(std::slice::from_ref(&row))
        .await?;
    storage.insert_mint_statuses_batch(&[row]).await?;
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*)::BIGINT FROM mint_status_history WHERE mint_address = $1")
            .bind("mint_pg2")
            .fetch_one(&pool)
            .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_mint_statuses_batch_empty_input_is_noop_pg(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    storage.insert_mint_statuses_batch(&[]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mint_status_at_slot_returns_blocked_in_window_between_block_and_reallow(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    storage
        .insert_mint_statuses_batch(&[
            mk_status("mint_cycle1", "allowed", 10, "sig-a"),
            mk_status("mint_cycle1", "blocked", 20, "sig-b"),
            mk_status("mint_cycle1", "allowed", 30, "sig-c"),
        ])
        .await?;

    let res = storage.get_mint_status_at_slot("mint_cycle1", 25).await?;
    assert_eq!(res, MintStatusAtSlot::Blocked);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mint_status_at_slot_returns_allowed_after_reallow_in_cycle(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    storage
        .insert_mint_statuses_batch(&[
            mk_status("mint_cycle2", "allowed", 10, "sig-a"),
            mk_status("mint_cycle2", "blocked", 20, "sig-b"),
            mk_status("mint_cycle2", "allowed", 30, "sig-c"),
        ])
        .await?;

    let res = storage.get_mint_status_at_slot("mint_cycle2", 35).await?;
    assert_eq!(res, MintStatusAtSlot::Allowed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mint_status_at_slot_returns_never_allowed_when_no_history(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let res = storage.get_mint_status_at_slot("mint_absent", 100).await?;
    assert_eq!(res, MintStatusAtSlot::NeverAllowed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_mint_status_at_slot_returns_blocked_after_block_entry(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    storage
        .insert_mint_statuses_batch(&[
            mk_status("mint_blk", "allowed", 10, "sig-a"),
            mk_status("mint_blk", "blocked", 20, "sig-b"),
        ])
        .await?;
    let res = storage.get_mint_status_at_slot("mint_blk", 25).await?;
    assert_eq!(res, MintStatusAtSlot::Blocked);
    Ok(())
}

// ── pending_release_signatures (verify-before-demote) ─────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_insert_get_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_roundtrip", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    storage
        .insert_release_signature(id, "sig-a".to_string(), 100)
        .await?;
    storage
        .insert_release_signature(id, "sig-b".to_string(), 200)
        .await?;

    let rows = storage.get_release_signatures(id).await?;
    assert_eq!(
        rows,
        vec![("sig-a".to_string(), 100), ("sig-b".to_string(), 200)]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_insert_is_idempotent_on_signature(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_idem", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    storage
        .insert_release_signature(id, "dup-sig".to_string(), 100)
        .await?;
    // Same signature again is a no-op (ON CONFLICT DO NOTHING).
    storage
        .insert_release_signature(id, "dup-sig".to_string(), 999)
        .await?;

    let rows = storage.get_release_signatures(id).await?;
    assert_eq!(rows.len(), 1, "duplicate signature must not double-insert");
    assert_eq!(rows[0], ("dup-sig".to_string(), 100), "first write wins");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_delete_removes_all_for_txn() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_delete", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    storage
        .insert_release_signature(id, "sig-x".to_string(), 1)
        .await?;
    storage
        .insert_release_signature(id, "sig-y".to_string(), 2)
        .await?;
    storage.delete_release_signatures(id).await?;
    assert!(storage.get_release_signatures(id).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_gc_only_drops_non_processing() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    let proc_txn = make_db_transaction("rel_gc_proc", TransactionType::Withdrawal);
    let proc_id = storage.insert_db_transaction(&proc_txn).await?;
    let done_txn = make_db_transaction("rel_gc_done", TransactionType::Withdrawal);
    let done_id = storage.insert_db_transaction(&done_txn).await?;

    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(proc_id)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE transactions SET status = 'completed'::transaction_status WHERE id = $1")
        .bind(done_id)
        .execute(&pool)
        .await?;

    storage
        .insert_release_signature(proc_id, "sig-proc".to_string(), 1)
        .await?;
    storage
        .insert_release_signature(done_id, "sig-done".to_string(), 2)
        .await?;

    let removed = storage.gc_stale_release_signatures().await?;
    assert_eq!(
        removed, 1,
        "GC must drop exactly the non-processing row's sig"
    );
    assert_eq!(storage.get_release_signatures(proc_id).await?.len(), 1);
    assert!(storage.get_release_signatures(done_id).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_cascade_on_transaction_delete() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_cascade", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;
    storage
        .insert_release_signature(id, "sig-cascade".to_string(), 1)
        .await?;

    sqlx::query("DELETE FROM transactions WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pending_release_signatures WHERE transaction_id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 0, "ON DELETE CASCADE must remove orphaned sigs");
    Ok(())
}

// ── recovery requeue counter ─────────────────────────────────────────────────

async fn status_of(pool: &PgPool, id: i64) -> String {
    sqlx::query_scalar::<_, String>("SELECT status::text FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn updated_at_of(pool: &PgPool, id: i64) -> chrono::DateTime<Utc> {
    sqlx::query_scalar("SELECT updated_at FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn requeue_attempts_of(pool: &PgPool, id: i64) -> i32 {
    sqlx::query_scalar("SELECT recovery_requeue_attempts FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn try_requeue_processing_increments_recovery_counter(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("requeue_counter", TransactionType::Deposit);
    let id = storage.insert_db_transaction(&txn).await?;
    // Lock flips Pending → Processing (and bumps updated_at via trigger).
    storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;
    assert_eq!(
        requeue_attempts_of(&pool, id).await,
        0,
        "starts at default 0"
    );

    let captured = updated_at_of(&pool, id).await;
    let requeued = storage.try_requeue_processing(id, captured).await?;
    assert!(
        requeued,
        "CAS requeue must succeed for the captured timestamp"
    );

    assert_eq!(
        status_of(&pool, id).await,
        "pending",
        "requeue must demote back to pending"
    );
    assert_eq!(
        requeue_attempts_of(&pool, id).await,
        1,
        "successful requeue must increment the durable counter by exactly 1"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn try_requeue_processing_stale_cas_leaves_counter_unchanged(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("requeue_stale", TransactionType::Deposit);
    let id = storage.insert_db_transaction(&txn).await?;
    storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;

    // A timestamp that does NOT match the row's updated_at → CAS no-op.
    let stale = updated_at_of(&pool, id).await - chrono::Duration::seconds(60);
    let requeued = storage.try_requeue_processing(id, stale).await?;
    assert!(!requeued, "stale CAS must no-op");

    assert_eq!(
        status_of(&pool, id).await,
        "processing",
        "no-op CAS must leave status untouched"
    );
    assert_eq!(
        requeue_attempts_of(&pool, id).await,
        0,
        "no-op CAS must NOT bump the counter"
    );
    Ok(())
}
