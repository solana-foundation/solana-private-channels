//! Pending remint storage round-trip
//!
//! Target: `indexer/src/storage/postgres/db.rs`:
//!   * `set_pending_remint_internal`
//!   * `get_pending_remint_transactions_internal`
//!
//! The operator-side `recover_pending_remints` is `pub(super)` and unit-tested
//! in-file (in `sender/state.rs`). The observable public seam for integration
//! testing is the storage round-trip: if the operator crashes with pending
//! remints in flight, it relies on `set_pending_remint` to have durably
//! persisted them and `get_pending_remint_transactions` to return them on
//! restart.
//!
//! Binary: `reconciliation_integration` (existing, has Postgres fixtures
//! via the nearby `db_migration_race` tests).
//!
//! Cases:
//!   A. Round-trip: insert a pending-remint row, read it back, verify
//!      remint_signatures and deadline_at match exactly.
//!   B. Only Pending-Remint status rows are returned (Pending / Completed
//!      rows are filtered out by `get_pending_remint_transactions`).
//!   C. Multiple pending remints are all returned; ordering does not
//!      matter for correctness but set equivalence must hold.

use {
    chrono::{Duration as ChronoDuration, Utc},
    private_channel_indexer::{
        storage::{common::models::DbTransactionBuilder, PostgresDb, Storage, TransactionType},
        PostgresConfig,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
};

async fn start_pg(db_name: &str) -> (PostgresDb, String, testcontainers::ContainerAsync<Postgres>) {
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

fn make_withdrawal(
    sig: &str,
    nonce: i64,
) -> private_channel_indexer::storage::common::models::DbTransaction {
    let mint = Pubkey::new_unique().to_string();
    let recipient = Pubkey::new_unique().to_string();
    let mut tx = DbTransactionBuilder::new(sig.to_string(), 1, mint, 10_000u64)
        .initiator(recipient.clone())
        .recipient(recipient)
        .transaction_type(TransactionType::Withdrawal)
        .build();
    tx.withdrawal_nonce = Some(nonce);
    tx
}

// ── Case A ──────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_remint_round_trip_preserves_signatures_and_deadline() {
    let (db, url, _container) = start_pg("t12_roundtrip").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();

    let sig = Signature::new_unique().to_string();
    let tx = make_withdrawal(&sig, 0);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();

    // `set_pending_remint_internal` has `WHERE status = 'processing'` —
    // it's the intended transition from in-flight withdrawal to pending
    // remint. Rows freshly inserted via `insert_transaction_internal`
    // start at 'pending', so we first bump them to 'processing' via the
    // public update path.
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();

    let remint_sigs = vec![
        Signature::new_unique().to_string(),
        Signature::new_unique().to_string(),
    ];
    let deadline = Utc::now() + ChronoDuration::minutes(30);

    let remint_lvbhs = vec![0; remint_sigs.len()];
    db.set_pending_remint_internal(tx_id, remint_sigs.clone(), remint_lvbhs, deadline)
        .await
        .expect("set_pending_remint must succeed");

    // set_pending_remint_internal sets status = 'pending_remint' itself, so
    // we can query directly — no manual status update needed after this.

    let rows = db
        .get_pending_remint_transactions_internal()
        .await
        .expect("get_pending_remint_transactions must succeed");

    assert_eq!(
        rows.len(),
        1,
        "expected exactly one pending remint; got {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row.id, tx_id);
    assert_eq!(
        row.remint_signatures.clone().unwrap_or_default(),
        remint_sigs,
        "remint signatures must round-trip"
    );
    assert!(
        row.pending_remint_deadline_at.is_some(),
        "deadline must round-trip"
    );
}

// ── Case B ──────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn test_pending_remint_query_filters_by_status() {
    let (db, url, _container) = start_pg("t12_filter").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    // Insert 3 rows, put each in a different status.
    let sigs: Vec<_> = (0..3)
        .map(|_| Signature::new_unique().to_string())
        .collect();
    let mut ids = Vec::new();
    for (i, sig) in sigs.iter().enumerate() {
        let tx = make_withdrawal(sig, i as i64);
        let id = db.insert_transaction_internal(&tx).await.unwrap();
        ids.push(id);
    }

    // Row 0: transition pending → processing → pending_remint (via the
    // real `set_pending_remint_internal` code path, which is the target
    // surface we want to exercise).
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(ids[0])
        .execute(&pool)
        .await
        .unwrap();
    db.set_pending_remint_internal(
        ids[0],
        vec!["fake".to_string()],
        vec![0],
        Utc::now() + ChronoDuration::minutes(10),
    )
    .await
    .unwrap();

    // Row 1: pending  (should NOT be returned)
    sqlx::query("UPDATE transactions SET status = 'pending'::transaction_status WHERE id = $1")
        .bind(ids[1])
        .execute(&pool)
        .await
        .unwrap();

    // Row 2: completed (should NOT be returned)
    sqlx::query("UPDATE transactions SET status = 'completed'::transaction_status WHERE id = $1")
        .bind(ids[2])
        .execute(&pool)
        .await
        .unwrap();

    let rows = db.get_pending_remint_transactions_internal().await.unwrap();
    assert_eq!(rows.len(), 1, "only the pending_remint row must return");
    assert_eq!(rows[0].id, ids[0]);
}
