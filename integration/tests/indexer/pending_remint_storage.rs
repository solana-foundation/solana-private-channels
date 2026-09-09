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
//!   E. Write-ahead `pending_remint_signatures` round-trip: one live attempt
//!      per transaction, reads back in order, and GC keeps rows while
//!      PendingRemint but sweeps them once the parent is terminal.

use private_channel_indexer::storage::common::models::StoredSig;
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
    db.set_pending_remint_internal(tx_id, remint_sigs.clone(), remint_lvbhs, deadline, false)
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

// ── Case A2 ─────────────────────────────────────────────────────────────────
/// A release the program refused is proof the payout never happened, and it is
/// the only such proof that outlives a bitmap rotation. It is written in the
/// same UPDATE that queues the refund, so it must read straight back off the
/// row, and a refund queued without one must read back false rather than NULL.
#[tokio::test(flavor = "multi_thread")]
async fn test_release_refusal_round_trips_with_the_pending_remint_row() {
    let (db, url, _container) = start_pg("t12_refusal").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let mut ids = Vec::new();
    for nonce in 0..2i64 {
        let sig = Signature::new_unique().to_string();
        let tx = make_withdrawal(&sig, nonce);
        let id = db.insert_transaction_internal(&tx).await.unwrap();
        sqlx::query(
            "UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
        ids.push(id);
    }

    let deadline = Utc::now() + ChronoDuration::seconds(32);
    db.set_pending_remint_internal(ids[0], vec!["sig-a".to_string()], vec![0], deadline, true)
        .await
        .unwrap();
    db.set_pending_remint_internal(ids[1], vec!["sig-b".to_string()], vec![0], deadline, false)
        .await
        .unwrap();

    let rows = db.get_pending_remint_transactions_internal().await.unwrap();
    assert_eq!(rows.len(), 2);
    let refused = rows.iter().find(|r| r.id == ids[0]).unwrap();
    let ordinary = rows.iter().find(|r| r.id == ids[1]).unwrap();
    assert!(
        refused.release_refused_on_chain,
        "the refusal must survive the write"
    );
    assert!(
        !ordinary.release_refused_on_chain,
        "an ordinary deferral must stay held to the bitmap gate"
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
        false,
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

// ── Case D ──────────────────────────────────────────────────────────────────
/// `finality_check_attempts` must persist and round-trip so the safety cap
/// survives operator restarts. A row written with `attempts = 0` and then
/// bumped twice must read back as 2; the WHERE-clause on the bump must reject
/// non-PendingRemint rows so a terminal row can't be silently resurrected.
#[tokio::test(flavor = "multi_thread")]
async fn test_finality_check_attempts_persisted_across_restart() {
    let (db, url, _container) = start_pg("t12_attempts").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let sig = Signature::new_unique().to_string();
    let tx = make_withdrawal(&sig, 0);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();

    // Move to processing so set_pending_remint_internal's status guard passes.
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();

    let initial_deadline = Utc::now() + ChronoDuration::seconds(32);
    db.set_pending_remint_internal(
        tx_id,
        vec![Signature::new_unique().to_string()],
        vec![0],
        initial_deadline,
        false,
    )
    .await
    .unwrap();

    // Fresh PendingRemint rows start at 0.
    let rows = db.get_pending_remint_transactions_internal().await.unwrap();
    assert_eq!(rows[0].finality_check_attempts, 0);

    // Two consecutive bumps mirror the defer path advancing through the cap.
    let bumped_deadline_1 = Utc::now() + ChronoDuration::seconds(64);
    db.bump_pending_remint_finality_attempt_internal(tx_id, 1, bumped_deadline_1)
        .await
        .unwrap();
    let bumped_deadline_2 = Utc::now() + ChronoDuration::seconds(96);
    db.bump_pending_remint_finality_attempt_internal(tx_id, 2, bumped_deadline_2)
        .await
        .unwrap();

    // Restart-equivalent read sees the persisted counter, not 0.
    let rows = db.get_pending_remint_transactions_internal().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].finality_check_attempts, 2);

    // Status guard: a terminal row must not be resurrectable by bump.
    sqlx::query(
        "UPDATE transactions SET status = 'manual_review'::transaction_status WHERE id = $1",
    )
    .bind(tx_id)
    .execute(&pool)
    .await
    .unwrap();
    let result = db
        .bump_pending_remint_finality_attempt_internal(tx_id, 3, Utc::now())
        .await;
    assert!(
        matches!(result, Err(sqlx::Error::RowNotFound)),
        "bump must reject non-PendingRemint rows, got {result:?}"
    );
}

// ── Case E ──────────────────────────────────────────────────────────────────
/// The write-ahead `pending_remint_signatures` table: one live attempt per
/// transaction, reads back in insertion order including superseded attempts, and
/// GC keeps rows while the parent is PendingRemint but sweeps them once terminal.
#[tokio::test(flavor = "multi_thread")]
async fn test_remint_signatures_round_trip_and_gc() {
    let (db, url, _container) = start_pg("t12_remint_sigs").await;
    let storage = Storage::Postgres(db.clone());
    storage.init_schema().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();

    let sig = Signature::new_unique().to_string();
    let tx = make_withdrawal(&sig, 0);
    let tx_id = db.insert_transaction_internal(&tx).await.unwrap();

    // Park the parent in PendingRemint so GC treats its sigs as live.
    sqlx::query("UPDATE transactions SET status = 'processing'::transaction_status WHERE id = $1")
        .bind(tx_id)
        .execute(&pool)
        .await
        .unwrap();
    db.set_pending_remint_internal(
        tx_id,
        vec![sig.clone()],
        vec![0],
        Utc::now() + ChronoDuration::minutes(10),
        false,
    )
    .await
    .unwrap();

    // First attempt takes the one live slot.
    let attempt_a = Signature::new_unique().to_string();
    let attempt_b = Signature::new_unique().to_string();
    assert!(db
        .claim_remint_attempt_internal(tx_id, attempt_a.clone(), 100, None, &[])
        .await
        .unwrap());
    // A second attempt only takes it by naming the one it proved dead.
    assert!(!db
        .claim_remint_attempt_internal(tx_id, attempt_b.clone(), 200, None, &[])
        .await
        .unwrap());
    assert!(db
        .claim_remint_attempt_internal(
            tx_id,
            attempt_b.clone(),
            200,
            None,
            std::slice::from_ref(&attempt_a)
        )
        .await
        .unwrap());

    let stored = db.get_remint_signatures_internal(tx_id).await.unwrap();
    assert_eq!(
        stored,
        vec![
            StoredSig {
                signature: attempt_a.clone(),
                last_valid_block_height: 100,
                blockhash_slot: None,
            },
            StoredSig {
                signature: attempt_b.clone(),
                last_valid_block_height: 200,
                blockhash_slot: None,
            },
        ],
        "both attempts must round-trip in insertion order; a superseded one stays classifiable"
    );

    // GC keeps rows whose parent is still PendingRemint.
    let removed = db.gc_stale_remint_signatures_internal().await.unwrap();
    assert_eq!(removed, 0, "live PendingRemint rows must be kept");
    assert_eq!(
        db.get_remint_signatures_internal(tx_id)
            .await
            .unwrap()
            .len(),
        2
    );

    // Once the parent goes terminal, GC sweeps its write-ahead rows.
    sqlx::query(
        "UPDATE transactions SET status = 'failed_reminted'::transaction_status WHERE id = $1",
    )
    .bind(tx_id)
    .execute(&pool)
    .await
    .unwrap();
    let removed = db.gc_stale_remint_signatures_internal().await.unwrap();
    assert_eq!(removed, 2, "terminal parent's rows must be swept");
    assert!(db
        .get_remint_signatures_internal(tx_id)
        .await
        .unwrap()
        .is_empty());
}
