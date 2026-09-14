//! Integration tests for `PostgresDb` methods against a real Postgres via testcontainers.
//!
//! Covers: schema lifecycle, single/batch inserts, pending/lock queries,
//! status updates, checkpoints, mints, reconciliation balances, and withdrawal nonces.
//!
//! Run with: `cd indexer && cargo test --test postgres_db_test -- --test-threads=1`

use bigdecimal::BigDecimal;
use chrono::Utc;
use private_channel_indexer::{
    config::ProgramType,
    metrics::LIVE_STATE_LOCK_LOST,
    operator::sender_lock_key,
    storage::{
        common::amount::TokenAmount,
        common::models::{DbMint, DbMintStatus, MintStatusAtSlot, StoredSig},
        common::storage::live_lock::{LiveLockGuard, LiveLockMode, LIVE_STATE_LOCK_KEY},
        common::storage::sender_lock::SenderLockGuard,
        postgres::db::{
            apply_lock_session_keepalives, apply_lock_session_lock_timeout,
            probe_advisory_lock_held, release_advisory_lock,
        },
        DbTransaction, PostgresDb, RequeueOutcome, Storage, TransactionStatus, TransactionType,
    },
    PostgresConfig,
};
use sqlx::PgPool;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_util::sync::CancellationToken;

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
        release_refused_on_chain: false,
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

/// Bitmap bits are independent, so an unresolved lower nonce must not withhold
/// higher ones. This pins the rejection of the SMT-era dequeue frontier: a
/// frontier would be a liveness cost here, not a safety property.
#[tokio::test(flavor = "multi_thread")]
async fn withdrawal_dequeue_ignores_lower_active_nonces() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    // Sequential inserts get sequential nonces (0, 1, 2) from the trigger.
    let w0 = storage
        .insert_db_transaction(&make_db_transaction("w0", TransactionType::Withdrawal))
        .await?;
    let w1 = storage
        .insert_db_transaction(&make_db_transaction("w1", TransactionType::Withdrawal))
        .await?;
    let w2 = storage
        .insert_db_transaction(&make_db_transaction("w2", TransactionType::Withdrawal))
        .await?;

    // Park the middle nonce: an active, non-Pending lower nonce.
    sqlx::query("UPDATE transactions SET status = 'parked' WHERE id = $1")
        .bind(w1)
        .execute(&pool)
        .await?;

    let locked = storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;
    let ids: Vec<i64> = locked.iter().map(|txn| txn.id).collect();
    assert_eq!(
        ids,
        vec![w0, w2],
        "a parked lower nonce must not hold back a higher pending one"
    );

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
            None,
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

    // A later deposit, above the bound the assertions below use.
    let mut d3 = make_db_transaction("recon_d3", TransactionType::Deposit);
    d3.mint = mint.to_string();
    d3.amount = TokenAmount(700);
    d3.slot = 200;
    storage.insert_db_transaction(&d3).await?;

    let balances = storage
        .get_mint_balances_for_reconciliation(u64::MAX)
        .await?;
    assert_eq!(balances.len(), 1);
    // Deposits: 500 (pending) + 300 (completed) + 700 (later slot) = 1500 (all statuses)
    assert_eq!(balances[0].total_deposits, BigDecimal::from(1500u64));
    // Withdrawals: only completed = 100
    assert_eq!(balances[0].total_withdrawals, BigDecimal::from(100u64));

    // Bounded at the earlier rows' slot: the later deposit is excluded.
    let at_100 = storage.get_mint_balances_for_reconciliation(100).await?;
    assert_eq!(at_100[0].total_deposits, BigDecimal::from(800u64));
    assert_eq!(at_100[0].total_withdrawals, BigDecimal::from(100u64));

    // Below every row: the mint must still be reported, at zero. If the bound moved to a
    // WHERE clause the mint would vanish here and drop out of the comparison entirely.
    let at_99 = storage.get_mint_balances_for_reconciliation(99).await?;
    assert_eq!(
        at_99.len(),
        1,
        "a mint with no rows in range must still appear"
    );
    assert_eq!(at_99[0].total_deposits, BigDecimal::from(0u64));
    assert_eq!(at_99[0].total_withdrawals, BigDecimal::from(0u64));
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

// ── unreleased withdrawal nonce bounds ───────────────────────────────────────

/// Insert a withdrawal, then force its status and nonce to exactly what the
/// case under test needs. Both are set directly because the insert trigger picks
/// the nonce and every row starts `pending`.
async fn seed_withdrawal(
    pool: &PgPool,
    storage: &Storage,
    tag: &str,
    nonce: i64,
    status: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = make_db_transaction(tag, TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&row).await?;
    sqlx::query("UPDATE transactions SET status = $2::transaction_status, withdrawal_nonce = $3 WHERE id = $1")
        .bind(id)
        .bind(status)
        .bind(nonce)
        .execute(pool)
        .await?;
    Ok(())
}

/// The split the rotation gate depends on: a released or written-off nonce can
/// never need its window again, everything else still might.
#[tokio::test(flavor = "multi_thread")]
async fn unreleased_nonce_bounds_counts_live_and_ignores_terminal(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let live = [
        "pending",
        "processing",
        "parked",
        "pending_remint",
        "manual_review",
    ];
    // Nonces are chosen well above whatever the insert trigger hands out, since
    // the column is uniquely indexed and the trigger numbers rows from zero.
    for (offset, status) in live.iter().enumerate() {
        seed_withdrawal(&pool, &storage, status, 1_010 + offset as i64, status).await?;
    }
    // Terminal rows sit on both sides of the live range, so including any of
    // them by mistake would move a bound and fail this.
    for (offset, status) in ["completed", "failed", "failed_reminted"]
        .iter()
        .enumerate()
    {
        seed_withdrawal(&pool, &storage, status, 1_000 + offset as i64, status).await?;
        seed_withdrawal(
            &pool,
            &storage,
            &format!("{status}_high"),
            1_100 + offset as i64,
            status,
        )
        .await?;
    }

    assert_eq!(
        storage.unreleased_withdrawal_nonce_bounds(0).await?,
        Some((1_010, 1_014)),
        "bounds must span the live withdrawals and nothing else"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unreleased_nonce_bounds_ignores_deposits_and_null_nonces(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let deposit = make_db_transaction("dep", TransactionType::Deposit);
    storage.insert_db_transaction(&deposit).await?;

    let orphan = make_db_transaction("null_nonce", TransactionType::Withdrawal);
    let orphan_id = storage.insert_db_transaction(&orphan).await?;
    sqlx::query("UPDATE transactions SET withdrawal_nonce = NULL WHERE id = $1")
        .bind(orphan_id)
        .execute(&pool)
        .await?;

    assert_eq!(
        storage.unreleased_withdrawal_nonce_bounds(0).await?,
        None,
        "neither a deposit nor a NULL nonce may set a bound"
    );

    seed_withdrawal(&pool, &storage, "live", 1_007, "pending").await?;
    assert_eq!(
        storage.unreleased_withdrawal_nonce_bounds(0).await?,
        Some((1_007, 1_007)),
        "only the live withdrawal that carries a nonce counts"
    );
    Ok(())
}

/// The floor is what lets the rotation gate ignore nonces whose window already
/// closed, so the SQL has to apply it to both aggregates.
#[tokio::test(flavor = "multi_thread")]
async fn unreleased_nonce_bounds_honours_the_floor() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    seed_withdrawal(&pool, &storage, "stranded", 1_003, "manual_review").await?;
    seed_withdrawal(&pool, &storage, "waiting", 1_011, "pending").await?;
    seed_withdrawal(&pool, &storage, "later", 1_020, "parked").await?;

    assert_eq!(
        storage.unreleased_withdrawal_nonce_bounds(1_010).await?,
        Some((1_011, 1_020)),
        "a nonce below the floor sets neither bound"
    );
    assert_eq!(
        storage.unreleased_withdrawal_nonce_bounds(1_021).await?,
        None,
        "a floor above every live nonce leaves nothing"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unreleased_nonce_bounds_none_when_no_live_rows() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;

    assert_eq!(storage.unreleased_withdrawal_nonce_bounds(0).await?, None);

    seed_withdrawal(&pool, &storage, "done", 1_001, "completed").await?;
    assert_eq!(
        storage.unreleased_withdrawal_nonce_bounds(0).await?,
        None,
        "an all-terminal table owes nothing"
    );
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
        .set_pending_remint(id, vec!["sig1".to_string()], vec![0], deadline, false)
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
        .set_pending_remint(id, vec!["sig1".to_string()], vec![0], deadline, false)
        .await;

    assert!(result.is_err(), "should fail when status is not processing");
    Ok(())
}

/// A retry whose first attempt committed but whose acknowledgement was lost must
/// succeed, so the sender can tell a durable handoff from a failed one. Replaying
/// the same payload is accepted; a different payload is not, so a second caller
/// can never silently overwrite the signatures a live remint depends on.
#[tokio::test(flavor = "multi_thread")]
async fn set_pending_remint_replays_identical_payload_only(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let txn = make_db_transaction("remint_replay", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;
    storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;

    let deadline = Utc::now() + chrono::Duration::seconds(32);
    let signatures = vec!["sig1".to_string(), "sig2".to_string()];
    storage
        .set_pending_remint(id, signatures.clone(), vec![10, 20], deadline, false)
        .await?;

    // The row is already PendingRemint; the same payload must still be accepted.
    storage
        .set_pending_remint(id, signatures.clone(), vec![10, 20], deadline, false)
        .await?;

    let stored: Vec<String> =
        sqlx::query_scalar("SELECT remint_signatures FROM transactions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(stored, signatures, "replay must preserve the signatures");

    let other = storage
        .set_pending_remint(id, vec!["different".to_string()], vec![30], deadline, false)
        .await;
    assert!(
        other.is_err(),
        "a different payload must not overwrite a live PendingRemint"
    );

    let stored: Vec<String> =
        sqlx::query_scalar("SELECT remint_signatures FROM transactions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        stored, signatures,
        "the rejected payload must leave the signatures untouched"
    );
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
        .insert_release_signature(id, "sig-a".to_string(), 100, None)
        .await?;
    storage
        .insert_release_signature(id, "sig-b".to_string(), 200, None)
        .await?;

    let rows = storage.get_release_signatures(id).await?;
    assert_eq!(
        rows,
        vec![
            StoredSig {
                signature: "sig-a".to_string(),
                last_valid_block_height: 100,
                blockhash_slot: None
            },
            StoredSig {
                signature: "sig-b".to_string(),
                last_valid_block_height: 200,
                blockhash_slot: None
            },
        ]
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
        .insert_release_signature(id, "dup-sig".to_string(), 100, None)
        .await?;
    // Same signature again is a no-op (ON CONFLICT DO NOTHING).
    storage
        .insert_release_signature(id, "dup-sig".to_string(), 999, None)
        .await?;

    let rows = storage.get_release_signatures(id).await?;
    assert_eq!(rows.len(), 1, "duplicate signature must not double-insert");
    assert_eq!(
        rows[0],
        StoredSig {
            signature: "dup-sig".to_string(),
            last_valid_block_height: 100,
            blockhash_slot: None
        },
        "first write wins"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_delete_removes_all_for_txn() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_delete", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    storage
        .insert_release_signature(id, "sig-x".to_string(), 1, None)
        .await?;
    storage
        .insert_release_signature(id, "sig-y".to_string(), 2, None)
        .await?;
    storage.delete_release_signatures(id).await?;
    assert!(storage.get_release_signatures(id).await?.is_empty());
    Ok(())
}

/// The GC may only reclaim signatures of genuinely terminal rows
/// (completed, failed, failed_reminted). Every non-terminal status retains its
/// write-ahead journal: a row can still be picked up, demoted, re-armed from
/// manual review, or reminted, and the pre-mint gate re-verifies those
/// signatures before it would mint again.
#[tokio::test(flavor = "multi_thread")]
async fn release_signature_gc_retains_non_terminal() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    // (label, status, must_survive)
    let cases = [
        ("processing", true),
        ("pending", true),
        ("manual_review", true),
        ("parked", true),
        ("pending_remint", true),
        ("completed", false),
        ("failed", false),
        ("failed_reminted", false),
    ];

    let mut ids = Vec::new();
    for (status, survive) in cases {
        let ty = if status == "pending_remint" || status == "parked" {
            TransactionType::Withdrawal
        } else {
            TransactionType::Deposit
        };
        let txn = make_db_transaction(&format!("rel_gc_{status}"), ty);
        let id = storage.insert_db_transaction(&txn).await?;
        sqlx::query(&format!(
            "UPDATE transactions SET status = '{status}'::transaction_status WHERE id = $1"
        ))
        .bind(id)
        .execute(&pool)
        .await?;
        storage
            .insert_release_signature(id, format!("sig-{status}"), 1, None)
            .await?;
        ids.push((status, id, survive));
    }

    let removed = storage.gc_stale_release_signatures().await?;
    let expected_removed = ids.iter().filter(|(_, _, s)| !s).count() as u64;
    assert_eq!(
        removed, expected_removed,
        "GC must drop only the terminal rows' signatures"
    );
    for (status, id, survive) in ids {
        let present = !storage.get_release_signatures(id).await?.is_empty();
        assert_eq!(
            present, survive,
            "status '{status}' signature retention mismatch (survive={survive})"
        );
    }
    Ok(())
}

// ── type-scoped stale queries ────────────────────────────────────────────────

async fn backdate_updated_at(
    pool: &PgPool,
    id: i64,
    age: chrono::Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await?;
    sqlx::query("UPDATE transactions SET updated_at = $1 WHERE id = $2")
        .bind(Utc::now() - age)
        .bind(id)
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_processing_query_is_type_exclusive() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let dep = make_db_transaction("stale_type_dep", TransactionType::Deposit);
    let dep_id = storage.insert_db_transaction(&dep).await?;
    let wd = make_db_transaction("stale_type_wd", TransactionType::Withdrawal);
    let wd_id = storage.insert_db_transaction(&wd).await?;
    for id in [dep_id, wd_id] {
        sqlx::query(
            "UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1",
        )
        .bind(id)
        .execute(&pool)
        .await?;
        backdate_updated_at(&pool, id, chrono::Duration::minutes(10)).await?;
    }

    let threshold = std::time::Duration::from_secs(5 * 60);
    let deposits = storage
        .get_stale_processing_transactions(threshold, 100, TransactionType::Deposit)
        .await?;
    assert_eq!(
        deposits.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![dep_id],
        "deposit scope must return exactly the deposit row"
    );
    let withdrawals = storage
        .get_stale_processing_transactions(threshold, 100, TransactionType::Withdrawal)
        .await?;
    assert_eq!(
        withdrawals.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![wd_id],
        "withdrawal scope must return exactly the withdrawal row"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_parked_query_is_type_exclusive() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let dep = make_db_transaction("parked_type_dep", TransactionType::Deposit);
    let dep_id = storage.insert_db_transaction(&dep).await?;
    let wd = make_db_transaction("parked_type_wd", TransactionType::Withdrawal);
    let wd_id = storage.insert_db_transaction(&wd).await?;
    for id in [dep_id, wd_id] {
        sqlx::query("UPDATE transactions SET status = 'parked'::transaction_status WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await?;
        backdate_updated_at(&pool, id, chrono::Duration::minutes(10)).await?;
    }

    let threshold = std::time::Duration::from_secs(5 * 60);
    let withdrawals = storage
        .get_stale_parked_transactions(threshold, 100, TransactionType::Withdrawal)
        .await?;
    assert_eq!(
        withdrawals.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![wd_id],
        "withdrawal scope must return exactly the withdrawal row"
    );
    let deposits = storage
        .get_stale_parked_transactions(threshold, 100, TransactionType::Deposit)
        .await?;
    assert_eq!(
        deposits.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![dep_id],
        "deposit scope must return exactly the deposit row"
    );
    Ok(())
}

/// `pending_release_signatures` is keyed only by `transaction_id`, so a deposit row
/// stores, fetches, and GCs its broadcast signature identically to a withdrawal. This
/// proves deposits reuse the table with no schema change (the pre-broadcast persist).
#[tokio::test(flavor = "multi_thread")]
async fn release_signature_reuses_table_for_deposit() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_deposit", TransactionType::Deposit);
    let id = storage.insert_db_transaction(&txn).await?;

    storage
        .insert_release_signature(id, "sig-deposit".to_string(), 100, None)
        .await?;
    assert_eq!(
        storage.get_release_signatures(id).await?,
        vec![StoredSig {
            signature: "sig-deposit".to_string(),
            last_valid_block_height: 100,
            blockhash_slot: None
        }],
        "deposit signature round-trips like a withdrawal"
    );

    // Leaving Processing makes the row GC-eligible, same as a withdrawal.
    sqlx::query("UPDATE transactions SET status = 'completed'::transaction_status WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;
    let removed = storage.gc_stale_release_signatures().await?;
    assert_eq!(
        removed, 1,
        "GC reclaims the deposit's sig once non-processing"
    );
    assert!(storage.get_release_signatures(id).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn release_signature_cascade_on_transaction_delete() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;
    let txn = make_db_transaction("rel_cascade", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;
    storage
        .insert_release_signature(id, "sig-cascade".to_string(), 1, None)
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

// ── stalled-withdrawal reconciliation ────────────────────────────────────────

/// Insert a row and force it into `status` without going through the operator.
async fn seed_with_status(
    pool: &PgPool,
    storage: &Storage,
    sig: &str,
    txn_type: TransactionType,
    status: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let id = storage
        .insert_db_transaction(&make_db_transaction(sig, txn_type))
        .await?;
    sqlx::query("UPDATE transactions SET status = $2::transaction_status WHERE id = $1")
        .bind(id)
        .bind(status)
        .execute(pool)
        .await?;
    Ok(id)
}

/// I1: the promote CAS refuses every source status but `manual_review` and
/// `pending_remint`, refuses deposits, and honours the `updated_at` compare.
#[tokio::test(flavor = "multi_thread")]
async fn try_complete_stalled_withdrawal_guard_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    // (label, seeded status, bound from-status, transaction type, use a stale CAS, expected)
    let cases: &[(&str, &str, TransactionStatus, TransactionType, bool, bool)] = &[
        (
            "manual_review fresh",
            "manual_review",
            TransactionStatus::ManualReview,
            TransactionType::Withdrawal,
            false,
            true,
        ),
        (
            "pending_remint fresh",
            "pending_remint",
            TransactionStatus::PendingRemint,
            TransactionType::Withdrawal,
            false,
            true,
        ),
        (
            "processing fresh",
            "processing",
            TransactionStatus::Processing,
            TransactionType::Withdrawal,
            false,
            false,
        ),
        (
            "completed fresh",
            "completed",
            TransactionStatus::Completed,
            TransactionType::Withdrawal,
            false,
            false,
        ),
        (
            "manual_review stale cas",
            "manual_review",
            TransactionStatus::ManualReview,
            TransactionType::Withdrawal,
            true,
            false,
        ),
        (
            "deposit manual_review",
            "manual_review",
            TransactionStatus::ManualReview,
            TransactionType::Deposit,
            false,
            false,
        ),
    ];

    for (i, (label, seeded, from_status, txn_type, stale, expected)) in cases.iter().enumerate() {
        let id = seed_with_status(&pool, &storage, &format!("cas_{i}"), *txn_type, seeded).await?;
        let mut captured = updated_at_of(&pool, id).await;
        if *stale {
            captured -= chrono::Duration::seconds(60);
        }

        let promoted = storage
            .try_complete_stalled_withdrawal(id, captured, *from_status, Some(format!("sig-{i}")))
            .await?;
        assert_eq!(promoted, *expected, "{label}: unexpected CAS result");

        if *expected {
            assert_eq!(status_of(&pool, id).await, "completed", "{label}");
            let sig: Option<String> =
                sqlx::query_scalar("SELECT counterpart_signature FROM transactions WHERE id = $1")
                    .bind(id)
                    .fetch_one(&pool)
                    .await?;
            assert_eq!(sig.as_deref(), Some(format!("sig-{i}").as_str()), "{label}");
        } else {
            assert_eq!(
                status_of(&pool, id).await,
                *seeded,
                "{label}: a refused CAS must leave the row exactly as it was"
            );
        }
    }
    Ok(())
}

/// Backdate `updated_at` past the trigger that would otherwise stamp NOW().
async fn force_updated_at(
    pool: &PgPool,
    id: i64,
    ts: chrono::DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query("ALTER TABLE transactions DISABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await?;
    sqlx::query("UPDATE transactions SET updated_at = $2 WHERE id = $1")
        .bind(id)
        .bind(ts)
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE transactions ENABLE TRIGGER update_transactions_updated_at")
        .execute(pool)
        .await?;
    Ok(())
}

async fn set_remint_signatures(
    pool: &PgPool,
    id: i64,
    sigs: Option<Vec<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let heights: Option<Vec<i64>> = sigs.as_ref().map(|s| vec![0i64; s.len()]);
    sqlx::query(
        "UPDATE transactions SET remint_signatures = $2, remint_last_valid_block_heights = $3
         WHERE id = $1",
    )
    .bind(id)
    .bind(sigs)
    .bind(heights)
    .execute(pool)
    .await?;
    Ok(())
}

/// I2: the fetch predicates and ordering, which the mock cannot verify
/// (`array_length` and the NULL-nonce filter are SQL-only).
#[tokio::test(flavor = "multi_thread")]
async fn stalled_withdrawal_query_predicates_and_ordering() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;
    let base = Utc::now() - chrono::Duration::hours(1);
    let sigs = || Some(vec!["sig-a".to_string()]);

    // Three matching manual_review rows, seeded out of `updated_at` order.
    let newest = seed_with_status(
        &pool,
        &storage,
        "q_new",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    let oldest = seed_with_status(
        &pool,
        &storage,
        "q_old",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    let middle = seed_with_status(
        &pool,
        &storage,
        "q_mid",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    for (id, offset) in [(newest, 30i64), (oldest, 10), (middle, 20)] {
        set_remint_signatures(&pool, id, sigs()).await?;
        force_updated_at(&pool, id, base + chrono::Duration::seconds(offset)).await?;
    }

    // One pending_remint row: visible only to the PendingRemint query.
    let remint = seed_with_status(
        &pool,
        &storage,
        "q_pr",
        TransactionType::Withdrawal,
        "pending_remint",
    )
    .await?;
    set_remint_signatures(&pool, remint, sigs()).await?;

    // Every shape the predicates must exclude.
    let wrong_status = seed_with_status(
        &pool,
        &storage,
        "q_done",
        TransactionType::Withdrawal,
        "completed",
    )
    .await?;
    set_remint_signatures(&pool, wrong_status, sigs()).await?;
    let deposit = seed_with_status(
        &pool,
        &storage,
        "q_dep",
        TransactionType::Deposit,
        "manual_review",
    )
    .await?;
    set_remint_signatures(&pool, deposit, sigs()).await?;
    let null_nonce = seed_with_status(
        &pool,
        &storage,
        "q_nonce",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    set_remint_signatures(&pool, null_nonce, sigs()).await?;
    sqlx::query("UPDATE transactions SET withdrawal_nonce = NULL WHERE id = $1")
        .bind(null_nonce)
        .execute(&pool)
        .await?;
    let empty_sigs = seed_with_status(
        &pool,
        &storage,
        "q_empty",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    set_remint_signatures(&pool, empty_sigs, Some(Vec::new())).await?;
    let no_sigs = seed_with_status(
        &pool,
        &storage,
        "q_null",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    set_remint_signatures(&pool, no_sigs, None).await?;
    // Reachable on a database upgraded between the two column migrations:
    // signatures present, the parallel heights array never backfilled.
    let no_heights = seed_with_status(
        &pool,
        &storage,
        "q_heights",
        TransactionType::Withdrawal,
        "manual_review",
    )
    .await?;
    sqlx::query("UPDATE transactions SET remint_signatures = $2 WHERE id = $1")
        .bind(no_heights)
        .bind(vec!["sig-orphan".to_string()])
        .execute(&pool)
        .await?;

    // Ids ascend in insertion order, which is deliberately not `updated_at`
    // order here: a row left untouched keeps its `updated_at` forever, so the
    // sweep pages on the one key that always advances.
    let by_id = vec![newest, oldest, middle];

    let found = storage
        .get_stalled_withdrawals_with_signatures(TransactionStatus::ManualReview, 0, 100)
        .await?;
    assert_eq!(
        found.iter().map(|t| t.id).collect::<Vec<_>>(),
        by_id,
        "only rows with usable evidence, ascending id"
    );

    let found_remint = storage
        .get_stalled_withdrawals_with_signatures(TransactionStatus::PendingRemint, 0, 100)
        .await?;
    assert_eq!(
        found_remint.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![remint],
        "the status bind must not leak rows from the other stalled status"
    );

    // Paging must cover every row exactly once: the second page starts strictly
    // after the last id of the first, so nothing repeats and nothing is skipped.
    let page_one = storage
        .get_stalled_withdrawals_with_signatures(TransactionStatus::ManualReview, 0, 2)
        .await?;
    assert_eq!(
        page_one.iter().map(|t| t.id).collect::<Vec<_>>(),
        by_id[..2].to_vec(),
        "first page is the two lowest ids"
    );
    let page_two = storage
        .get_stalled_withdrawals_with_signatures(TransactionStatus::ManualReview, by_id[1], 2)
        .await?;
    assert_eq!(
        page_two.iter().map(|t| t.id).collect::<Vec<_>>(),
        by_id[2..].to_vec(),
        "the cursor must resume after the previous page, not repeat it"
    );
    Ok(())
}

// ── try_requeue_prebroadcast: cap-gated requeue ──────────────────────────────

/// Under the cap the write flips Processing → Pending and increments the counter
/// (Requeued); at the cap it leaves the row Processing (AtCap). The cap is enforced
/// inside the single write, so no separate counter read is involved.
#[tokio::test(flavor = "multi_thread")]
async fn try_requeue_prebroadcast_requeues_under_cap_then_caps(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let mut txn = make_db_transaction("prebroadcast_cap", TransactionType::Withdrawal);
    txn.withdrawal_nonce = Some(0);
    let id = storage.insert_db_transaction(&txn).await?;

    // Under the cap (max 1, attempts 0): requeue to Pending, counter → 1.
    storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;
    assert_eq!(
        storage.try_requeue_prebroadcast(id, 1).await?,
        RequeueOutcome::Requeued { attempts: 1 }
    );
    assert_eq!(status_of(&pool, id).await, "pending");
    assert_eq!(requeue_attempts_of(&pool, id).await, 1);

    // At the cap (max 1, attempts 1): leave Processing, counter unchanged.
    storage
        .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
        .await?;
    assert_eq!(
        storage.try_requeue_prebroadcast(id, 1).await?,
        RequeueOutcome::AtCap
    );
    assert_eq!(
        status_of(&pool, id).await,
        "processing",
        "at cap must not requeue"
    );
    assert_eq!(
        requeue_attempts_of(&pool, id).await,
        1,
        "at cap must not increment"
    );
    Ok(())
}

/// A row that is not Processing (still Pending) yields NotProcessing and is untouched.
#[tokio::test(flavor = "multi_thread")]
async fn try_requeue_prebroadcast_not_processing_is_noop() -> Result<(), Box<dyn std::error::Error>>
{
    let (pool, storage, _pg) = start_postgres().await?;
    let mut txn = make_db_transaction("prebroadcast_pending", TransactionType::Withdrawal);
    txn.withdrawal_nonce = Some(0);
    let id = storage.insert_db_transaction(&txn).await?;

    // Never locked, so still Pending: the WHERE status = 'processing' finds no row.
    assert_eq!(
        storage.try_requeue_prebroadcast(id, 3).await?,
        RequeueOutcome::NotProcessing
    );
    assert_eq!(status_of(&pool, id).await, "pending");
    assert_eq!(requeue_attempts_of(&pool, id).await, 0);
    Ok(())
}

// ── claim_and_persist_signature: ownership CAS + write-ahead ─────────────────

/// The lock must hand back the post-lock token: the returned row's `status` is
/// `Processing` and its `updated_at` is the trigger-bumped value equal to the
/// DB's current value, not the stale Pending-era timestamp. Without this the
/// deposit claim would CAS against a timestamp that never matches.
#[tokio::test(flavor = "multi_thread")]
async fn lock_returns_processing_era_updated_at() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let id = storage
        .insert_db_transaction(&make_db_transaction("lock_token", TransactionType::Deposit))
        .await?;
    let pending_era = updated_at_of(&pool, id).await;

    let locked = storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;
    let row = locked.iter().find(|t| t.id == id).expect("row was locked");
    assert_eq!(
        row.status,
        TransactionStatus::Processing,
        "the returned row must carry the post-lock Processing status"
    );
    assert_ne!(
        row.updated_at, pending_era,
        "the returned updated_at must be the post-lock trigger-bumped value"
    );
    assert_eq!(
        row.updated_at,
        updated_at_of(&pool, id).await,
        "the returned updated_at must equal the DB's current value"
    );
    Ok(())
}

/// A successful claim leaves exactly one signature and a changed `updated_at`;
/// a failed claim (wrong token) leaves zero new signatures and an unchanged
/// `updated_at` (proving there is no bumped-but-no-signature partial state).
#[tokio::test(flavor = "multi_thread")]
async fn claim_is_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let id = storage
        .insert_db_transaction(&make_db_transaction(
            "claim_atomic",
            TransactionType::Deposit,
        ))
        .await?;
    storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;
    let token = updated_at_of(&pool, id).await;

    let epoch = storage
        .claim_and_persist_signature(id, token, "sig-claim".to_string(), 555, None)
        .await?
        .expect("owning the Processing incarnation must claim");
    let sigs = storage.get_release_signatures(id).await?;
    assert_eq!(sigs.len(), 1, "a successful claim persists exactly one sig");
    assert_eq!(
        sigs[0],
        StoredSig {
            signature: "sig-claim".to_string(),
            last_valid_block_height: 555,
            blockhash_slot: None
        }
    );
    let bumped = updated_at_of(&pool, id).await;
    assert_ne!(bumped, token, "a successful claim bumps updated_at");
    assert_eq!(
        epoch, bumped,
        "the returned epoch must equal the committed post-claim updated_at"
    );

    // A stale token must abort atomically: no new sig, no timestamp change.
    let stale = bumped - chrono::Duration::seconds(60);
    let failed = storage
        .claim_and_persist_signature(id, stale, "sig-fail".to_string(), 1, None)
        .await?;
    assert!(failed.is_none(), "a stale token must not claim");
    assert_eq!(
        storage.get_release_signatures(id).await?.len(),
        1,
        "a failed claim persists no additional signature"
    );
    assert_eq!(
        updated_at_of(&pool, id).await,
        bumped,
        "a failed claim leaves updated_at unchanged"
    );
    Ok(())
}

/// Claiming the same signature twice yields a single row (ON CONFLICT
/// (signature) DO NOTHING), even though each claim owns the current token.
#[tokio::test(flavor = "multi_thread")]
async fn claim_dedups_signature_on_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let id = storage
        .insert_db_transaction(&make_db_transaction(
            "claim_dedup",
            TransactionType::Deposit,
        ))
        .await?;
    storage
        .get_and_lock_pending_transactions(TransactionType::Deposit, 100)
        .await?;

    let token1 = updated_at_of(&pool, id).await;
    let token2 = storage
        .claim_and_persist_signature(id, token1, "dup-sig".to_string(), 1, None)
        .await?
        .expect("the first claim must own the fetch-time token");
    // The first claim's returned epoch is presented directly as the second
    // claim's token, pinning that a returned epoch is a valid next CAS token.
    // The re-inserted duplicate signature must be deduped.
    assert!(storage
        .claim_and_persist_signature(id, token2, "dup-sig".to_string(), 999, None)
        .await?
        .is_some());
    assert_eq!(
        storage.get_release_signatures(id).await?.len(),
        1,
        "a duplicate signature must persist only once"
    );
    Ok(())
}

// ── Sender lock: ownership probe and the connection fence ────────────────────
//
// The heartbeat and the write fence both rest on one claim: for a sender key,
// the session is alive if and only if the lock is held. These tests exercise
// that against a real backend, including the case the whole design exists for,
// a session terminated underneath a running sender.

/// Mirrors the truncate lock id, used only to prove the probe cannot confuse it for ours.
const TRUNCATE_ADVISORY_LOCK_ID: i64 = 0x0043_4F4E_5452_5543;

async fn container_url(container: &testcontainers::ContainerAsync<Postgres>) -> String {
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    format!("postgres://postgres:password@{host}:{port}/db_test")
}

async fn pg_connect(url: &str) -> sqlx::PgConnection {
    use sqlx::Connection;
    sqlx::PgConnection::connect(url).await.unwrap()
}

/// I1. The `(classid << 32) | objid` reassembly plus `objsubid = 1` plus the
/// `pid` filter is the entire correctness of the heartbeat, and the two sender
/// keys share a `classid`, so a `classid`-only filter would pass hand review and
/// fail here. The final clause records executably why `pg_try_advisory_lock`
/// cannot be substituted, so a future simplification cannot land silently.
#[tokio::test]
async fn probe_distinguishes_held_from_free_and_from_other_keys(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;

    let escrow = sender_lock_key(ProgramType::Escrow);
    let withdraw = sender_lock_key(ProgramType::Withdraw);
    assert_ne!(escrow, withdraw);

    let mut holder = pg_connect(&url).await;
    let mut bystander = pg_connect(&url).await;

    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(escrow)
        .fetch_one(&mut holder)
        .await?;
    assert!(acquired);

    assert!(probe_advisory_lock_held(&mut holder, escrow).await?);
    assert!(
        !probe_advisory_lock_held(&mut holder, withdraw).await?,
        "the two sender keys share a classid, so objid must be part of the match"
    );
    assert!(!probe_advisory_lock_held(&mut holder, TRUNCATE_ADVISORY_LOCK_ID).await?);

    for key in [escrow, withdraw, TRUNCATE_ADVISORY_LOCK_ID] {
        assert!(
            !probe_advisory_lock_held(&mut bystander, key).await?,
            "a session holding nothing must see nothing, even for a key another session holds"
        );
    }

    release_advisory_lock(&mut holder, escrow).await?;
    assert!(
        !probe_advisory_lock_held(&mut holder, escrow).await?,
        "the probe must observe the release"
    );
    let reacquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(escrow)
        .fetch_one(&mut holder)
        .await?;
    assert!(
        reacquired,
        "pg_try_advisory_lock returns true on a free lock, which is exactly why it \
         cannot be used as the ownership probe"
    );
    Ok(())
}

async fn remint_columns_of(
    pool: &PgPool,
    id: i64,
) -> Result<(Option<Vec<String>>, Option<Vec<i64>>), Box<dyn std::error::Error>> {
    let row: (Option<Vec<String>>, Option<Vec<i64>>) = sqlx::query_as(
        "SELECT remint_signatures, remint_last_valid_block_heights FROM transactions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// I3: the quarantine CAS stores the evidence in the same statement that flips
/// the status, and a `None` call leaves the columns exactly as they were.
#[tokio::test(flavor = "multi_thread")]
async fn quarantine_cas_writes_signatures_and_coalesces_none(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    let sigs = vec!["sig-quarantine".to_string()];

    let with_sigs = seed_with_status(
        &pool,
        &storage,
        "q3_write",
        TransactionType::Withdrawal,
        "processing",
    )
    .await?;
    let captured = updated_at_of(&pool, with_sigs).await;
    let quarantined = storage
        .try_quarantine_processing(with_sigs, captured, Some(sigs.clone()), Some(vec![777i64]))
        .await?;
    assert!(quarantined, "fresh CAS must succeed");
    assert_eq!(status_of(&pool, with_sigs).await, "manual_review");
    let (stored_sigs, stored_heights) = remint_columns_of(&pool, with_sigs).await?;
    assert_eq!(stored_sigs, Some(sigs));
    assert_eq!(stored_heights, Some(vec![777i64]));

    // NOW() is the transaction timestamp, so these two agree only if the status
    // flip and the column write happened in one statement. A "write columns,
    // then CAS" pair would leave two distinct timestamps (and the CAS could
    // never match, since the first write already bumped updated_at).
    let (updated, processed): (chrono::DateTime<Utc>, Option<chrono::DateTime<Utc>>) =
        sqlx::query_as("SELECT updated_at, processed_at FROM transactions WHERE id = $1")
            .bind(with_sigs)
            .fetch_one(&pool)
            .await?;
    assert!(updated > captured, "the trigger must bump updated_at");
    assert_eq!(
        Some(updated),
        processed,
        "one statement means one NOW(); a second write would desync these"
    );

    // A None call must not erase columns an earlier transition already wrote.
    let preloaded = seed_with_status(
        &pool,
        &storage,
        "q3_keep",
        TransactionType::Withdrawal,
        "processing",
    )
    .await?;
    set_remint_signatures(&pool, preloaded, Some(vec!["sig-existing".to_string()])).await?;
    let captured = updated_at_of(&pool, preloaded).await;
    assert!(
        storage
            .try_quarantine_processing(preloaded, captured, None, None)
            .await?
    );
    let (kept_sigs, kept_heights) = remint_columns_of(&pool, preloaded).await?;
    assert_eq!(kept_sigs, Some(vec!["sig-existing".to_string()]));
    assert_eq!(kept_heights, Some(vec![0i64]));

    // A losing racer writes nothing at all, columns included.
    let stale_row = seed_with_status(
        &pool,
        &storage,
        "q3_stale",
        TransactionType::Withdrawal,
        "processing",
    )
    .await?;
    let stale = updated_at_of(&pool, stale_row).await - chrono::Duration::seconds(60);
    assert!(
        !storage
            .try_quarantine_processing(
                stale_row,
                stale,
                Some(vec!["sig-race".to_string()]),
                Some(vec![1])
            )
            .await?
    );
    assert_eq!(status_of(&pool, stale_row).await, "processing");
    assert_eq!(remint_columns_of(&pool, stale_row).await?, (None, None));
    Ok(())
}

/// A fenced write on a killed session must fail, not apply, and must cancel the operator.
#[tokio::test]
async fn fenced_write_on_a_killed_session_fails_and_does_not_apply(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    let operator = CancellationToken::new();

    let id = storage
        .insert_db_transaction(&make_db_transaction(
            "fence-kill",
            TransactionType::Withdrawal,
        ))
        .await?;
    sqlx::query("UPDATE transactions SET status = 'processing' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;

    let key = sender_lock_key(ProgramType::Escrow);
    let _guard = storage
        .try_acquire_sender_lock(key, "escrow", operator.clone(), Duration::from_secs(3600))
        .await?
        .expect("the lock must be free");

    // Kill the backend that holds the lock, from a different session.
    let mut killer = pg_connect(&url).await;
    let pid: i32 = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
         AND granted AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(key)
    .fetch_one(&mut killer)
    .await?;
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut killer)
        .await?;

    let result = storage.try_park_processing(id).await;
    assert!(
        result.is_err(),
        "a fenced write on a dead session must fail, got {result:?}"
    );
    assert!(
        operator.is_cancelled(),
        "an unprovable fenced write must cancel the operator"
    );

    let status: String = sqlx::query_scalar("SELECT status::text FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        status, "processing",
        "the refused write must not have applied"
    );
    Ok(())
}

/// A constraint violation means the server answered, so it must not read as lock loss.
#[tokio::test]
async fn database_error_on_a_fenced_write_does_not_cancel_the_operator(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _container) = start_postgres().await?;
    let operator = CancellationToken::new();

    let first = storage
        .insert_db_transaction(&make_db_transaction(
            "fence-dup-a",
            TransactionType::Withdrawal,
        ))
        .await?;
    let second = storage
        .insert_db_transaction(&make_db_transaction(
            "fence-dup-b",
            TransactionType::Withdrawal,
        ))
        .await?;
    for id in [first, second] {
        sqlx::query("UPDATE transactions SET status = 'pending_remint' WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await?;
    }

    let _guard = storage
        .try_acquire_sender_lock(
            sender_lock_key(ProgramType::Escrow),
            "escrow",
            operator.clone(),
            Duration::from_secs(3600),
        )
        .await?
        .expect("the lock must be free");

    // The signature column is globally unique, so re-using one is a plain 23505 from a live backend.
    assert!(
        storage
            .claim_remint_attempt(first, "shared-signature".to_string(), 10, None, &[])
            .await?
    );
    let err = storage
        .claim_remint_attempt(second, "shared-signature".to_string(), 20, None, &[])
        .await
        .expect_err("the duplicate signature must surface the constraint violation");

    assert!(
        !operator.is_cancelled(),
        "an application error must not cancel the operator; got {err}"
    );
    // The session is still healthy, so the next fenced write still works.
    assert!(
        storage
            .claim_remint_attempt(second, "distinct-signature".to_string(), 20, None, &[])
            .await?
    );
    Ok(())
}

/// Graceful release must really unlock, and only a separate pool taking the key proves it.
#[tokio::test]
async fn graceful_release_frees_the_lock_for_a_different_pool(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    let operator = CancellationToken::new();
    let key = sender_lock_key(ProgramType::Escrow);

    let guard = storage
        .try_acquire_sender_lock(key, "escrow", operator.clone(), Duration::from_secs(3600))
        .await?
        .expect("the lock must be free");

    let other = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: url.clone(),
            max_connections: 2,
        })
        .await?,
    );
    assert!(
        other
            .try_acquire_sender_lock(key, "escrow", CancellationToken::new(), Duration::ZERO)
            .await?
            .is_none(),
        "a second holder must be refused while the first holds the lock"
    );

    // The Noop arm only exists under the mock-storage feature.
    #[allow(unreachable_patterns)]
    match guard {
        SenderLockGuard::Postgres(handle) => handle.stop_and_wait().await,
        _ => panic!("postgres storage must yield the Postgres guard"),
    }

    assert!(
        other
            .try_acquire_sender_lock(key, "escrow", CancellationToken::new(), Duration::ZERO)
            .await?
            .is_some(),
        "an explicit release must hand the lock to a different pool"
    );
    assert!(
        !operator.is_cancelled(),
        "a graceful release must never look like a lock loss"
    );
    Ok(())
}

/// A fenced write queued behind a row lock a pool connection holds must lose to
/// the server-side `lock_timeout`, not to the client-side write timeout. The
/// server error is re-probed, found to be an ordinary application error on a
/// live session, and must leave both the operator and the connection intact.
/// Losing this race the other way would discard the connection and shut the
/// operator down for nothing more than contention.
#[tokio::test]
async fn fenced_write_blocked_on_a_row_lock_does_not_cancel_the_operator(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, container) = start_postgres().await?;
    let operator = CancellationToken::new();

    let id = storage
        .insert_db_transaction(&make_db_transaction(
            "fence-rowlock",
            TransactionType::Withdrawal,
        ))
        .await?;
    sqlx::query("UPDATE transactions SET status = 'processing' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;

    let _guard = storage
        .try_acquire_sender_lock(
            sender_lock_key(ProgramType::Escrow),
            "escrow",
            operator.clone(),
            Duration::from_secs(3600),
        )
        .await?
        .expect("the lock must be free");

    // Hold the row from another session for longer than lock_timeout but under the write timeout.
    let mut blocker = pg_connect(&container_url(&container).await).await;
    sqlx::query("BEGIN").execute(&mut blocker).await?;
    sqlx::query("SELECT 1 FROM transactions WHERE id = $1 FOR UPDATE")
        .bind(id)
        .execute(&mut blocker)
        .await?;

    let started = std::time::Instant::now();
    let blocked = storage.try_park_processing(id).await;
    let elapsed = started.elapsed();

    sqlx::query("ROLLBACK").execute(&mut blocker).await?;

    assert!(blocked.is_err(), "the blocked write must fail, not hang");
    // A margin, not just "under 5s". At 5s the client timeout fires instead and
    // the operator dies, so any lock_timeout raised into the gap below it has to
    // fail here rather than pass by a tenth of a second. This bound plus the
    // no-cancel assertion is what pins the two settings in the right order.
    assert!(
        elapsed < Duration::from_secs(4),
        "lock_timeout must fire well before the client write timeout; took {elapsed:?}"
    );
    assert!(
        !operator.is_cancelled(),
        "row-lock contention is not lock loss and must not cancel the operator"
    );
    // The connection was never discarded, so the fence still works afterwards.
    assert!(
        storage.try_park_processing(id).await?,
        "the lock connection must survive a contended write"
    );
    Ok(())
}

// ── journaled blockhash slot ──────────────────────────────────────────────────

/// The slot the broadcast blockhash was read at round-trips through both
/// journals, and a write that has none reads back as NULL rather than 0, which
/// would claim the transaction could not predate genesis.
#[tokio::test]
async fn blockhash_slot_round_trips_and_stays_null_when_absent(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, _container) = start_postgres().await?;

    let txn = make_db_transaction("slot_round_trip", TransactionType::Withdrawal);
    let id = storage.insert_db_transaction(&txn).await?;

    storage
        .insert_release_signature(id, "sig-with-slot".to_string(), 1_000, Some(400))
        .await?;
    storage
        .insert_release_signature(id, "sig-without-slot".to_string(), 1_000, None)
        .await?;

    let rows = storage.get_release_signatures(id).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].blockhash_slot,
        Some(400),
        "the slot must round-trip"
    );
    assert_eq!(
        rows[1].blockhash_slot, None,
        "a write with no slot must stay NULL, never default to 0"
    );

    assert!(
        storage
            .claim_remint_attempt(id, "remint-sig".to_string(), 2_000, Some(1_500), &[])
            .await?
    );
    let remints = storage.get_remint_signatures(id).await?;
    assert_eq!(remints.len(), 1);
    assert_eq!(
        remints[0].blockhash_slot,
        Some(1_500),
        "the remint journal must carry the slot too"
    );

    Ok(())
}

// ── Live-state lock ───────────────────────────────────────────────────────────
//
// The lock that keeps live indexer/operator workers and a destructive resync off
// the same database at once. Workers take it shared, resync takes it exclusive.

/// A storage handle on its own pool, standing in for a separate process.
async fn connect_storage(url: &str) -> Storage {
    Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: url.to_string(),
            max_connections: 5,
        })
        .await
        .expect("connect"),
    )
}

/// Kill whichever backend holds `key`, standing in for a failover or an idle-session reap.
async fn terminate_advisory_lock_holder(url: &str, key: i64) {
    let mut conn = pg_connect(url).await;
    let pid: i32 = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
         AND granted AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(key)
    .fetch_one(&mut conn)
    .await
    .expect("exactly one backend must hold the key");
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut conn)
        .await
        .expect("terminate");
}

/// Count every loss reason for one role. The counter is process-global, so a test
/// asserting an exact value must own its role label.
fn live_lock_lost_total(role: &str) -> f64 {
    ["not_held", "probe_error", "probe_timeout"]
        .iter()
        .map(|reason| {
            LIVE_STATE_LOCK_LOST
                .with_label_values(&[role, reason])
                .get()
        })
        .sum()
}

/// Every backend holding the live-state key, with the mode it holds it in. Read
/// from a bystander session so it reports the server's view, not ours.
async fn live_lock_holders(url: &str) -> Vec<String> {
    let mut conn = pg_connect(url).await;
    sqlx::query_scalar::<_, String>(
        "SELECT mode FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
         AND granted AND ((classid::bigint << 32) | objid::bigint) = $1",
    )
    .bind(LIVE_STATE_LOCK_KEY)
    .fetch_all(&mut conn)
    .await
    .expect("holder query")
}

/// Take the lock through the public storage API, as every production caller does.
async fn take_live_lock(
    storage: &Storage,
    mode: LiveLockMode,
    role: &'static str,
    heartbeat: Duration,
) -> Result<LiveLockGuard, private_channel_indexer::error::StorageError> {
    storage
        .try_acquire_live_lock(mode, role, CancellationToken::new(), heartbeat)
        .await
}

/// I1. The whole guarantee in one test: workers coexist, resync is refused while
/// any of them is up, and once resync holds the lock no worker can start. Both
/// directions matter, since only one of them is the destructive case.
#[tokio::test]
async fn shared_holders_coexist_and_exclude_exclusive() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;

    let worker_a = connect_storage(&url).await;
    let worker_b = connect_storage(&url).await;

    let a = take_live_lock(&worker_a, LiveLockMode::Shared, "i1_a", Duration::ZERO).await?;
    let b = take_live_lock(&worker_b, LiveLockMode::Shared, "i1_b", Duration::ZERO).await?;
    assert_eq!(
        live_lock_holders(&url).await.len(),
        2,
        "two shared holders must both hold the key"
    );

    // Resync is refused while either worker is live.
    let resync_storage = connect_storage(&url).await;
    let refused = take_live_lock(
        &resync_storage,
        LiveLockMode::Exclusive,
        "i1_resync",
        Duration::ZERO,
    )
    .await;
    assert!(
        matches!(
            refused,
            Err(
                private_channel_indexer::error::StorageError::LiveStateLockHeld {
                    requested: LiveLockMode::Exclusive
                }
            )
        ),
        "resync must be refused while workers hold the lock, got {refused:?}"
    );

    a.stop_and_wait().await;
    b.stop_and_wait().await;
    assert!(
        live_lock_holders(&url).await.is_empty(),
        "stopping every worker must free the key"
    );

    // With the workers gone resync gets in, and now it locks them out.
    let resync = take_live_lock(
        &resync_storage,
        LiveLockMode::Exclusive,
        "i1_resync",
        Duration::ZERO,
    )
    .await?;
    let late_worker = connect_storage(&url).await;
    let refused = take_live_lock(
        &late_worker,
        LiveLockMode::Shared,
        "i1_late",
        Duration::ZERO,
    )
    .await;
    assert!(
        matches!(
            refused,
            Err(
                private_channel_indexer::error::StorageError::LiveStateLockHeld {
                    requested: LiveLockMode::Shared
                }
            )
        ),
        "a worker starting during a resync must be refused, got {refused:?}"
    );

    resync.stop_and_wait().await;
    Ok(())
}

/// I2. The lock session must not be a pooled connection. Shutdown closes the pool
/// and waits for anything checked out, so a pooled holder would stall every
/// shutdown of every role until that wait timed out.
#[tokio::test]
async fn pool_close_does_not_wait_on_the_live_lock() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;

    let storage = connect_storage(&url).await;
    let guard = take_live_lock(&storage, LiveLockMode::Shared, "i2", Duration::ZERO).await?;

    let started = tokio::time::Instant::now();
    storage.close().await?;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "closing the pool must not wait on the lock session, took {elapsed:?}"
    );
    assert_eq!(
        live_lock_holders(&url).await.len(),
        1,
        "the lock must survive its pool being closed, since the caller still holds it"
    );

    guard.stop_and_wait().await;
    Ok(())
}

/// I3. A killed backend frees the lock server-side while the process keeps running
/// on its other connections. Only the heartbeat notices, and it must stop the role.
#[tokio::test]
async fn terminated_backend_cancels_the_role_token() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;

    let role = "i3_terminated";
    let before = live_lock_lost_total(role);
    let storage = connect_storage(&url).await;
    let token = CancellationToken::new();
    let _guard = storage
        .try_acquire_live_lock(
            LiveLockMode::Shared,
            role,
            token.clone(),
            Duration::from_secs(1),
        )
        .await?;

    terminate_advisory_lock_holder(&url, LIVE_STATE_LOCK_KEY).await;

    assert!(
        tokio::time::timeout(Duration::from_secs(20), token.cancelled())
            .await
            .is_ok(),
        "losing the live-state lock must stop the role"
    );
    assert!(
        live_lock_lost_total(role) >= before + 1.0,
        "losing the live-state lock must be counted"
    );
    Ok(())
}

/// I4. Dropping the guard must end the session, or the lock would linger and keep
/// refusing the next resync (or the next worker) with nothing actually holding it.
#[tokio::test]
async fn guard_drop_frees_the_lock() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;

    let storage = connect_storage(&url).await;
    let guard = take_live_lock(&storage, LiveLockMode::Exclusive, "i4", Duration::ZERO).await?;
    assert_eq!(live_lock_holders(&url).await.len(), 1);
    drop(guard);

    // Drop only signals the task, so poll until the close lands.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let taker = connect_storage(&url).await;
    loop {
        if take_live_lock(&taker, LiveLockMode::Exclusive, "i4_next", Duration::ZERO)
            .await
            .is_ok()
        {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dropping the guard must free the lock"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The runbook tells an operator to find the lock by its decimal key and to read
/// the mode column to tell a worker from a resync. Both are copied by hand into
/// SQL there, so pin them here rather than let the runbook rot into a query that
/// silently returns nothing during an incident.
#[tokio::test]
async fn runbook_query_finds_each_mode_by_its_documented_key(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    assert_eq!(
        LIVE_STATE_LOCK_KEY, 5_497_019_676_134_429_780,
        "the key in live_state_lock_runbook.md must match the code"
    );

    let worker = connect_storage(&url).await;
    let worker_lock =
        take_live_lock(&worker, LiveLockMode::Shared, "rb_worker", Duration::ZERO).await?;
    assert_eq!(
        live_lock_holders(&url).await,
        vec!["ShareLock".to_string()],
        "the runbook reads a worker as ShareLock"
    );
    worker_lock.stop_and_wait().await;

    let resync = connect_storage(&url).await;
    let resync_lock = take_live_lock(
        &resync,
        LiveLockMode::Exclusive,
        "rb_resync",
        Duration::ZERO,
    )
    .await?;
    assert_eq!(
        live_lock_holders(&url).await,
        vec!["ExclusiveLock".to_string()],
        "the runbook reads a resync as ExclusiveLock"
    );
    resync_lock.stop_and_wait().await;
    Ok(())
}

/// I5. The synchronous check resync runs immediately before it drops tables. It
/// must answer from the server, not from a cached belief, so a lock lost between
/// two heartbeats cannot let the destruction through.
#[tokio::test]
async fn ensure_held_reports_loss() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;

    let storage = connect_storage(&url).await;
    let guard = take_live_lock(&storage, LiveLockMode::Exclusive, "i5", Duration::ZERO).await?;
    assert!(
        guard.ensure_held().await.is_ok(),
        "a held lock must prove itself"
    );

    terminate_advisory_lock_holder(&url, LIVE_STATE_LOCK_KEY).await;
    assert!(
        guard.ensure_held().await.is_err(),
        "a lost lock must fail the check that guards the drop"
    );
    Ok(())
}

/// I11. A host that vanishes sends no FIN, so without these the lock backend sits
/// in recv() holding the key until the OS default expires, about two hours. That
/// blocks every resync while nothing is actually running. Postgres reports 0 over
/// a unix socket; the test container speaks TCP.
#[tokio::test]
async fn the_lock_session_sets_its_own_tcp_keepalives() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, _storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    let mut conn = pg_connect(&url).await;

    apply_lock_session_keepalives(&mut conn).await;

    let settings: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, setting FROM pg_settings
         WHERE name LIKE 'tcp_keepalives%' ORDER BY name",
    )
    .fetch_all(&mut conn)
    .await?;

    assert_eq!(
        settings,
        vec![
            ("tcp_keepalives_count".to_string(), "3".to_string()),
            ("tcp_keepalives_idle".to_string(), "60".to_string()),
            ("tcp_keepalives_interval".to_string(), "15".to_string()),
        ],
        "the lock session must carry its own keepalives"
    );
    Ok(())
}

// ── merged-lineage schema ─────────────────────────────────────────────────────

/// The merged schema is the union of two lineages, so it has to be checked as a
/// whole: the bitmap redesign's `observed_releases` and `release_refused_on_chain`
/// must be present, the SMT lineage's `owed_rotation_target` must be gone, and
/// re-running `init_schema` over a database the pre-merge code created must be a
/// no-op rather than a failed migration.
#[tokio::test(flavor = "multi_thread")]
async fn merged_schema_drops_owed_rotation_target_and_is_idempotent(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;

    let owed: Option<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = 'indexer_state' AND column_name = 'owed_rotation_target'",
    )
    .fetch_optional(&pool)
    .await?;
    assert!(
        owed.is_none(),
        "the SMT rotation-arming column must not exist in the merged schema"
    );

    for (table, column) in [
        ("transactions", "release_refused_on_chain"),
        ("indexer_state", "last_committed_slot"),
        ("pending_release_signatures", "blockhash_slot"),
    ] {
        let found: Option<String> = sqlx::query_scalar(
            "SELECT column_name FROM information_schema.columns
             WHERE table_name = $1 AND column_name = $2",
        )
        .bind(table)
        .bind(column)
        .fetch_optional(&pool)
        .await?;
        assert!(
            found.is_some(),
            "{table}.{column} must exist after the merge"
        );
    }

    for table in [
        "observed_releases",
        "pending_remint_signatures",
        "reconciliation_halt",
    ] {
        let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(table)
            .fetch_one(&pool)
            .await?;
        assert!(found.is_some(), "{table} must exist after the merge");
    }

    // Re-running the migration over the schema it just built is what an
    // already-deployed database does on the next boot.
    storage.init_schema().await?;
    storage.init_schema().await?;

    Ok(())
}

// ── Fenced drop ───────────────────────────────────────────────────────────────
//
// The drop has to run on the session that holds the lock. Issued through the pool
// it can outlive the lock: Postgres frees a session lock the moment its backend
// dies, so a worker can start while the drop is still running.

/// Does `transactions` still exist? Stands in for the whole schema, since the drop
/// is all-or-nothing.
async fn transactions_table_exists(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('transactions')::text")
        .fetch_one(pool)
        .await
        .expect("regclass lookup")
        .is_some()
}

/// I12. The fenced drop must still do its job while the lock is genuinely held.
#[tokio::test(flavor = "multi_thread")]
async fn fenced_drop_removes_the_tables_while_the_lock_is_held(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, _pg) = start_postgres().await?;
    storage.init_schema().await?;
    assert!(
        transactions_table_exists(&pool).await,
        "the schema must exist before the drop"
    );

    let guard = take_live_lock(
        &storage,
        LiveLockMode::Exclusive,
        "i12_resync",
        Duration::ZERO,
    )
    .await?;
    storage.drop_tables_fenced(&guard).await?;

    assert!(
        !transactions_table_exists(&pool).await,
        "a fenced drop under a held lock must remove the schema"
    );
    Ok(())
}

/// I13. Once the lock session is gone the lock is free, so a resync must not be able
/// to keep dropping: any worker may now be starting.
#[tokio::test(flavor = "multi_thread")]
async fn fenced_drop_refuses_once_the_lock_session_is_gone(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    storage.init_schema().await?;

    let guard = take_live_lock(
        &storage,
        LiveLockMode::Exclusive,
        "i13_resync",
        Duration::ZERO,
    )
    .await?;
    terminate_advisory_lock_holder(&url, LIVE_STATE_LOCK_KEY).await;

    assert!(
        storage.drop_tables_fenced(&guard).await.is_err(),
        "a drop must not run on a session that no longer holds the lock"
    );
    assert!(
        transactions_table_exists(&pool).await,
        "the refused drop must leave the schema standing"
    );
    Ok(())
}

/// I14. The finding this test exists for: a loss verdict can be a false positive, and the
/// heartbeat deliberately parks the session open afterwards. So the server still reports
/// the lock as held and a probe alone says "go ahead". Acting on that drops every table
/// and then refuses to rebuild, because the rebuild watches the same token. The guard has
/// to refuse on our own verdict, not just on the server's answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_false_loss_verdict_refuses_the_drop() -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    storage.init_schema().await?;

    let lost = CancellationToken::new();
    let guard = storage
        .try_acquire_live_lock(
            LiveLockMode::Exclusive,
            "i14_resync",
            lost.clone(),
            Duration::ZERO,
        )
        .await?;

    // The session is untouched, so this stands in for a verdict that was wrong.
    lost.cancel();

    assert_eq!(
        live_lock_holders(&url).await.len(),
        1,
        "the session must still hold the lock, or this is not the false-positive case"
    );
    assert!(
        guard.ensure_held().await.is_err(),
        "a lock we can no longer vouch for must fail the check that guards the drop"
    );
    assert!(
        storage.drop_tables_fenced(&guard).await.is_err(),
        "the drop must be refused even though the server still reports the lock held"
    );
    assert!(
        transactions_table_exists(&pool).await,
        "a refused drop must leave the database intact"
    );
    Ok(())
}

/// I15. The drop needs ACCESS EXCLUSIVE on every table, so one stray reader is enough to
/// queue it. Unbounded, that wait wedges the resync while it holds the exclusive lock and
/// every worker stays refused, and the heartbeat cannot see it because a busy connection
/// reads as alive. The session's own lock_timeout is what bounds it.
#[tokio::test(flavor = "multi_thread")]
async fn the_lock_session_bounds_its_lock_waits() -> Result<(), Box<dyn std::error::Error>> {
    let (_pool, storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    storage.init_schema().await?;

    let mut conn = pg_connect(&url).await;
    apply_lock_session_lock_timeout(&mut conn).await;

    let lock_timeout: String = sqlx::query_scalar("SELECT current_setting('lock_timeout')")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(
        lock_timeout, "10s",
        "the lock session must bound how long it waits for another session's lock"
    );
    Ok(())
}

/// I16. The same bound, observed through the fenced drop itself: a competing table lock
/// must make it fail rather than hang. The elapsed time is what says which bound fired,
/// since the client-side cap is minutes away.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_drop_blocked_on_a_table_lock_fails_rather_than_hanging(
) -> Result<(), Box<dyn std::error::Error>> {
    let (pool, storage, container) = start_postgres().await?;
    let url = container_url(&container).await;
    storage.init_schema().await?;

    let guard = take_live_lock(
        &storage,
        LiveLockMode::Exclusive,
        "i16_resync",
        Duration::ZERO,
    )
    .await?;

    // A plain read is enough: it takes ACCESS SHARE, which the drop's ACCESS EXCLUSIVE
    // has to wait out.
    let mut blocker = pg_connect(&url).await;
    sqlx::query("BEGIN").execute(&mut blocker).await?;
    sqlx::query("SELECT 1 FROM transactions LIMIT 1")
        .execute(&mut blocker)
        .await?;

    let started = std::time::Instant::now();
    let blocked = storage.drop_tables_fenced(&guard).await;
    let elapsed = started.elapsed();

    sqlx::query("ROLLBACK").execute(&mut blocker).await?;

    assert!(
        blocked.is_err(),
        "a drop queued behind another session's lock must fail, got {blocked:?}"
    );
    assert!(
        elapsed < Duration::from_secs(60),
        "it must be the session's lock_timeout that fired, not the client cap; took {elapsed:?}"
    );
    assert!(
        transactions_table_exists(&pool).await,
        "a drop that never ran must leave the schema standing"
    );
    Ok(())
}
