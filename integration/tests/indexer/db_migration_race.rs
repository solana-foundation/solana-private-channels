//! Database migration idempotency + insert-race safety
//!
//! Target file: `indexer/src/storage/postgres/db.rs`.
//! Binary: `reconciliation_integration` (existing — attached via `#[path]`
//! mod from `tests/indexer/reconciliation.rs`).
//!
//! Two tests:
//!
//!   1. **`test_init_schema_is_idempotent_across_runs`** — calls
//!      `init_schema()` twice on the same pool and asserts no error, no
//!      orphan rows, and that every `CREATE TABLE IF NOT EXISTS` /
//!      `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` / `ADD VALUE IF NOT
//!      EXISTS` branch is exercised. Captures the forward-compatibility
//!      guarantee we rely on when pg_restore-ing from a previous version.
//!
//!   2. **`test_insert_transaction_duplicate_signature_returns_existing_id`**
//!      — inserts the same `DbTransaction` twice concurrently and asserts
//!      both calls return the same `id` without raising an error. Exercises
//!      the `SELECT ... WHERE signature = $1` early-return AND the
//!      `ON CONFLICT DO NOTHING + fallback SELECT` branch (the one that
//!      fires when two writers race past the initial existence check).

use {
    private_channel_indexer::storage::{
        common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType,
    },
    private_channel_indexer::PostgresConfig,
    solana_sdk::signature::Signature,
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
};

async fn start_postgres(
    db_name: &str,
) -> (PostgresDb, String, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);
    let db = PostgresDb::new(&PostgresConfig {
        database_url: url.clone(),
        max_connections: 10,
    })
    .await
    .unwrap();
    (db, url, container)
}

// ── 1. Schema-init idempotency ─────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn test_init_schema_is_idempotent_across_runs() {
    let (db, url, _container) = start_postgres("c1_schema_idempotent").await;
    let storage = Storage::Postgres(db);

    // First run creates every table/enum/column.
    storage.init_schema().await.expect("first init_schema");
    // Second run must be a no-op: all IF NOT EXISTS branches taken.
    storage
        .init_schema()
        .await
        .expect("init_schema must be idempotent; the second run took ALTER/CREATE branches");

    // Sanity: transactions table still exists and is queryable. Connect a
    // separate pool because PostgresDb.pool is private.
    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(&pool)
        .await
        .expect("transactions table must be queryable after double-init");
    assert_eq!(count, 0, "no rows yet; table exists and is reachable");

    // The single-signature uniqueness must have been swapped for the composite
    // triple index across both runs; the old single-signature and
    // (signature, instruction_index) indexes must be gone.
    let triple_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_transactions_signature_ix_inner'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        triple_index, 1,
        "composite triple signature index must exist"
    );

    let prev_composite_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_transactions_signature_ix'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        prev_composite_index, 0,
        "the (signature, instruction_index) index must be dropped after the inner_index migration"
    );

    let old_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_transactions_signature'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(old_index, 0, "old single-signature index must be dropped");

    let old_constraint: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'transactions_signature_key'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        old_constraint, 0,
        "old signature unique constraint must be dropped"
    );
}

// ── 1b. Upgrade from a pre-instruction_index (legacy) schema ────────────────
// Reproduces an in-place upgrade: a database whose `transactions` table predates
// instruction_index and still carries the old single-signature uniqueness.
// init_schema must add the column, build the composite index, and drop the old
// uniqueness without failing on the not-yet-present column at startup.
#[tokio::test(flavor = "multi_thread")]
async fn test_init_schema_upgrades_legacy_signature_only_schema() {
    let (db, url, _container) = start_postgres("c1_legacy_upgrade").await;
    let storage = Storage::Postgres(db);
    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");

    // Seed the current schema, then rewind it to the legacy shape: drop the
    // triple index, the inner_index and instruction_index columns, and restore
    // the old single-signature unique constraint plus its standalone index.
    storage.init_schema().await.expect("seed current schema");
    sqlx::query("DROP INDEX IF EXISTS idx_transactions_signature_ix_inner")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP INDEX IF EXISTS idx_transactions_signature_ix")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions DROP COLUMN IF EXISTS inner_index")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions DROP COLUMN instruction_index")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "ALTER TABLE transactions ADD CONSTRAINT transactions_signature_key UNIQUE (signature)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("CREATE UNIQUE INDEX idx_transactions_signature ON transactions (signature)")
        .execute(&pool)
        .await
        .unwrap();

    // The upgrade run must not crash on the missing column and must converge to
    // the composite identity.
    storage
        .init_schema()
        .await
        .expect("init_schema must upgrade a legacy signature-only schema");

    let has_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_name = 'transactions' AND column_name = 'instruction_index'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(has_column, 1, "instruction_index column must be added");

    let triple_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_transactions_signature_ix_inner'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        triple_index, 1,
        "composite triple signature index must exist after upgrade"
    );

    let old_constraint: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'transactions_signature_key'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        old_constraint, 0,
        "old signature unique constraint must be dropped"
    );

    let old_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_transactions_signature'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(old_index, 0, "old single-signature index must be dropped");
}

// ── 1c. Upgrade from a (signature, instruction_index) schema to the triple ──
// A database that predates inner_index carries the
// idx_transactions_signature_ix composite but no inner_index column. init_schema
// must add the column, build the triple unique index, and drop the old one
// without failing on the not-yet-present column.
#[tokio::test(flavor = "multi_thread")]
async fn test_init_schema_upgrades_instruction_index_schema_to_triple() {
    let (db, url, _container) = start_postgres("c1_triple_upgrade").await;
    let storage = Storage::Postgres(db);
    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");

    // Seed the current schema, then rewind to the pre-inner_index shape: drop the
    // triple index and inner_index column and restore the (signature,
    // instruction_index) composite index.
    storage.init_schema().await.expect("seed current schema");
    sqlx::query("DROP INDEX IF EXISTS idx_transactions_signature_ix_inner")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE transactions DROP COLUMN inner_index")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE UNIQUE INDEX idx_transactions_signature_ix \
         ON transactions (signature, instruction_index)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // The upgrade run must not crash on the missing column and must converge.
    storage
        .init_schema()
        .await
        .expect("init_schema must upgrade a (signature, instruction_index) schema");

    let has_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_name = 'transactions' AND column_name = 'inner_index'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(has_column, 1, "inner_index column must be added");

    let triple_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes \
         WHERE indexname = 'idx_transactions_signature_ix_inner'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        triple_index, 1,
        "triple unique index must exist after upgrade"
    );

    let old_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_transactions_signature_ix'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        old_index, 0,
        "old (signature, instruction_index) index must be dropped"
    );
}

// ── 1d. Top-level + inner rows under one signature persist distinctly ───────
// A top-level deposit (inner_index NULL) and an inner CPI deposit (inner_index 0)
// at the SAME (signature, instruction_index) must persist as two rows, and the
// COALESCE(-1) sentinel must still collapse a duplicate top-level row.
#[tokio::test(flavor = "multi_thread")]
async fn cpi_inner_and_top_level_persist_as_distinct_rows() {
    let (db, url, _container) = start_postgres("c1_cpi_identity").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();

    let signature = Signature::new_unique().to_string();
    let mint = solana_sdk::pubkey::Pubkey::new_unique().to_string();
    let recipient = solana_sdk::pubkey::Pubkey::new_unique().to_string();

    let build = |inner_index: Option<i32>, amount: u64| {
        DbTransactionBuilder::new(signature.clone(), 1, mint.clone(), amount)
            .initiator(recipient.clone())
            .recipient(recipient.clone())
            .transaction_type(TransactionType::Deposit)
            .instruction_index(0)
            .inner_index(inner_index)
            .build()
    };

    // Top-level (NULL) and inner (0) sharing (signature, instruction_index=0).
    let batch = vec![build(None, 100), build(Some(0), 200)];
    let ids = storage
        .insert_db_transactions_batch(&batch)
        .await
        .expect("batch insert ok");
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1], "top-level and inner rows are distinct");

    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
        .bind(&signature)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "two distinct rows persist; got {count}");

    // Re-inserting both is idempotent, including the NULL-inner_index top-level
    // row (COALESCE(-1) sentinel keeps it collision-detecting).
    let ids_again = storage
        .insert_db_transactions_batch(&batch)
        .await
        .expect("re-insert ok");
    assert_eq!(ids_again, ids, "re-insert returns the same ids");

    let count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
            .bind(&signature)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_after, 2,
        "re-insert creates no new rows; got {count_after}"
    );
}

// ── 2. Duplicate-key race in insert_transaction ────────────────────────────
// Both rows default to instruction_index 0, so this also covers the
// same-signature SAME-index collision: (sig, 0) is still unique and the race
// resolves to one shared id.
#[tokio::test(flavor = "multi_thread")]
async fn test_insert_transaction_duplicate_signature_returns_existing_id() {
    let (db, url, _container) = start_postgres("c1_dup_insert").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();

    let signature = Signature::new_unique().to_string();
    let mint = solana_sdk::pubkey::Pubkey::new_unique().to_string();
    let recipient = solana_sdk::pubkey::Pubkey::new_unique().to_string();

    let build = || {
        DbTransactionBuilder::new(signature.clone(), 1, mint.clone(), 100u64)
            .initiator(recipient.clone())
            .recipient(recipient.clone())
            .transaction_type(TransactionType::Deposit)
            .build()
    };

    // Spawn two concurrent inserts of the *same* signature. Both must
    // succeed and must return the same row id (from whichever path ended
    // up producing the row — either the first INSERT or the race-fallback
    // SELECT after ON CONFLICT DO NOTHING).
    let tx1 = build();
    let tx2 = build();
    let db1 = db.clone();
    let db2 = db.clone();
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { db1.insert_transaction_internal(&tx1).await }),
        tokio::spawn(async move { db2.insert_transaction_internal(&tx2).await }),
    );
    let id1 = r1.expect("t1 not panic").expect("insert 1 ok");
    let id2 = r2.expect("t2 not panic").expect("insert 2 ok");
    assert_eq!(
        id1, id2,
        "duplicate-signature inserts must resolve to the same id (id1={id1} id2={id2})"
    );

    // Row exists exactly once.
    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
        .bind(&signature)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "exactly one row survives the race; got {count}");
}

// ── 3. Same signature, distinct instruction_index persists every event ──────
#[tokio::test(flavor = "multi_thread")]
async fn insert_batch_same_signature_distinct_index_persists_all_rows() {
    let (db, url, _container) = start_postgres("c1_composite_identity").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();

    let signature = Signature::new_unique().to_string();
    let mint = solana_sdk::pubkey::Pubkey::new_unique().to_string();
    let recipient = solana_sdk::pubkey::Pubkey::new_unique().to_string();

    // One deposit and one withdrawal sharing the signature, at indices 0 and 1.
    // The withdrawal exercises the per-INSERT nonce trigger as well.
    let build_batch = || {
        let deposit = DbTransactionBuilder::new(signature.clone(), 1, mint.clone(), 100u64)
            .initiator(recipient.clone())
            .recipient(recipient.clone())
            .transaction_type(TransactionType::Deposit)
            .instruction_index(0)
            .build();
        let withdrawal = DbTransactionBuilder::new(signature.clone(), 1, mint.clone(), 50u64)
            .initiator(recipient.clone())
            .recipient(recipient.clone())
            .transaction_type(TransactionType::Withdrawal)
            .instruction_index(1)
            .build();
        vec![deposit, withdrawal]
    };

    let ids = storage
        .insert_db_transactions_batch(&build_batch())
        .await
        .expect("batch insert ok");
    assert_eq!(ids.len(), 2, "two instructions => two returned ids");
    assert_ne!(ids[0], ids[1], "each instruction gets its own row id");

    let pool = sqlx::PgPool::connect(&url).await.expect("sqlx connect");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
        .bind(&signature)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "both same-signature instructions persist; got {count}"
    );

    let indices: Vec<i32> = sqlx::query_scalar(
        "SELECT instruction_index FROM transactions WHERE signature = $1 ORDER BY instruction_index",
    )
    .bind(&signature)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        indices,
        vec![0, 1],
        "both absolute indices persist distinctly"
    );

    let nonces: Vec<Option<i64>> = sqlx::query_scalar(
        "SELECT withdrawal_nonce FROM transactions \
         WHERE signature = $1 AND transaction_type = 'withdrawal'",
    )
    .bind(&signature)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(nonces.len(), 1, "one withdrawal row");
    assert!(
        nonces[0].is_some(),
        "the withdrawal INSERT must receive a non-null nonce from the trigger"
    );

    // Re-inserting the same batch is idempotent on (signature, instruction_index).
    let ids_again = storage
        .insert_db_transactions_batch(&build_batch())
        .await
        .expect("re-insert ok");
    assert_eq!(ids_again, ids, "re-insert returns the same two ids");

    let count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
            .bind(&signature)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        count_after, 2,
        "re-insert does not create new rows; got {count_after}"
    );
}
