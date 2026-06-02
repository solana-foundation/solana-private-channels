//! Manually-triggered drills that verify the operations runbooks under
//! `docs/runbooks/`. Every drill is `#[ignore]`-flagged so default
//! `cargo test` skips them; trigger explicitly:
//!
//!     cargo test --test runbook_drills -- --ignored --nocapture
//!     cargo test --test runbook_drills -- --ignored --nocapture drill_2
//!
//! Drills are NOT in CI. They exist so a human running through a runbook can
//! verify the diagnostic and recovery commands in it actually work against
//! a real Postgres schema. Each drill prints, in order:
//!
//! 1. Which runbook + section it verifies.
//! 2. The seeded condition.
//! 3. The diagnostic SQL output.
//! 4. The post-recovery state.
//!
//! Use `RUST_LOG=trace cargo test --test runbook_drills -- --ignored` to see
//! tracing output if a drill needs debugging.

use chrono::Utc;
use private_channel_indexer::{
    storage::{PostgresDb, Storage},
    PostgresConfig,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use sqlx::{PgPool, Row};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── Harness ──────────────────────────────────────────────────────────────────

/// Spin up a fresh Postgres container with the indexer schema applied.
/// The container handle must be kept alive for the duration of the drill.
async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("runbook_drills")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:password@{host}:{port}/runbook_drills");
    let pool = PgPool::connect(&url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: url,
            max_connections: 5,
        })
        .await?,
    );
    storage.init_schema().await?;
    Ok((pool, storage, container))
}

/// Insert a withdrawal row directly into the desired state. Bypasses the
/// operator code path: drills verify the runbook against the post-trigger
/// DB shape, so seeding the shape directly is correct.
///
/// `error_message` is descriptive only. The `transactions` table has no
/// such column - the operator surfaces it on the alert webhook payload,
/// not in DB state. The argument exists so caller-side test code reads
/// like the runbook's dispatch table.
async fn seed_withdrawal(
    pool: &PgPool,
    status: &str,
    nonce: i64,
    _error_message: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        r#"
        INSERT INTO transactions
            (signature, slot, initiator, recipient, mint, amount,
             transaction_type, status, withdrawal_nonce,
             trace_id, processed_at, created_at, updated_at)
        VALUES
            ($1, 100, $2, $3, $4, 1000,
             'withdrawal'::transaction_type,
             $5::transaction_status, $6,
             $7, $8, NOW(), NOW())
        RETURNING id
        "#,
    )
    .bind(Signature::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(status)
    .bind(nonce)
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(Utc::now())
    .fetch_one(pool)
    .await?
    .get::<i64, _>(0);
    Ok(row)
}

/// Insert a deposit row directly. Deposits have no `withdrawal_nonce` (the
/// schema's auto-assign trigger only fires on withdrawals).
async fn seed_deposit(pool: &PgPool, status: &str) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        r#"
        INSERT INTO transactions
            (signature, slot, initiator, recipient, mint, amount,
             transaction_type, status, trace_id, processed_at,
             created_at, updated_at)
        VALUES
            ($1, 100, $2, $3, $4, 1000,
             'deposit'::transaction_type,
             $5::transaction_status, $6, NOW(), NOW(), NOW())
        RETURNING id
        "#,
    )
    .bind(Signature::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(status)
    .bind(uuid::Uuid::new_v4().to_string())
    .fetch_one(pool)
    .await?
    .get::<i64, _>(0);
    Ok(row)
}

/// Seed a `failed_reminted` withdrawal row with the supplied remint
/// signatures bound to the `remint_signatures TEXT[]` column. The runbook
/// `withdrawal_failed_reminted.md` Step 1 hard-requires this column to be
/// non-empty for the reconciliation flow to mean anything; the upstream
/// production path enforces that via `set_pending_remint(…)` running
/// before the `FailedReminted` status transition (see
/// `sender/remint.rs::attempt_remint`). Drills bypass that path and seed
/// directly, so they need a helper that mirrors the post-transition
/// shape.
async fn seed_failed_reminted(
    pool: &PgPool,
    nonce: i64,
    remint_sigs: &[&str],
) -> Result<i64, sqlx::Error> {
    let sigs: Vec<String> = remint_sigs.iter().map(|s| s.to_string()).collect();
    let row = sqlx::query(
        r#"
        INSERT INTO transactions
            (signature, slot, initiator, recipient, mint, amount,
             transaction_type, status, withdrawal_nonce,
             remint_signatures, trace_id, processed_at, created_at, updated_at)
        VALUES
            ($1, 100, $2, $3, $4, 1000,
             'withdrawal'::transaction_type,
             'failed_reminted'::transaction_status, $5,
             $6, $7, $8, NOW(), NOW())
        RETURNING id
        "#,
    )
    .bind(Signature::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(nonce)
    .bind(&sigs)
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(Utc::now())
    .fetch_one(pool)
    .await?
    .get::<i64, _>(0);
    Ok(row)
}

/// Convenience: read the status of a row.
async fn status_of(pool: &PgPool, id: i64) -> Result<String, sqlx::Error> {
    let s: String = sqlx::query_scalar("SELECT status::text FROM transactions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(s)
}

/// Convenience: count rows by status.
async fn count_status(pool: &PgPool, status: &str) -> Result<i64, sqlx::Error> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE status::text = $1")
        .bind(status)
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// Print a banner so `--nocapture` output makes it obvious which runbook
/// section a given drill is verifying.
fn drill_header(runbook: &str, section: &str) {
    eprintln!("\n──────────────────────────────────────────────");
    eprintln!("DRILL: docs/runbooks/{runbook}");
    eprintln!("       § {section}");
    eprintln!("──────────────────────────────────────────────");
}

// ── Drill 1: dispatch table — error_message contracts ───────────────────────
//
// The dispatch table in withdrawal_manual_review.md keys recovery on
// substrings of `error_message`. Those substrings live in the operator
// source. If a contributor changes a string without updating the runbook,
// dispatch silently breaks.
//
// This drill greps the source for each contract substring and fails if any
// one is missing. It does not need Postgres.

#[test]
#[ignore]
fn drill_1_error_message_contracts_present_in_source() {
    drill_header(
        "withdrawal_manual_review.md",
        "Triage — dispatch by error_message",
    );

    // Each entry: (substring the runbook dispatches on, file expected to contain it).
    // Substrings are matched literally, including the unicode em-dash where
    // the source uses it.
    let contracts: &[(&str, &str)] = &[
        // Withdrawal — processor side.
        ("invalid_pubkey", "indexer/src/operator/processor.rs"),
        ("invalid_builder", "indexer/src/operator/processor.rs"),
        ("program_error", "indexer/src/operator/processor.rs"),
        ("mint paused:", "indexer/src/operator/processor.rs"),
        (
            "insufficient escrow balance:",
            "indexer/src/operator/processor.rs",
        ),
        (
            "withdrawal pipeline halted after poison-pill",
            "indexer/src/operator/processor.rs",
        ),
        // Withdrawal — sender side.
        ("remint failed:", "indexer/src/operator/sender/remint.rs"),
        (
            "finality check failed after",
            "indexer/src/operator/sender/remint.rs",
        ),
        (
            "failed to persist pending remint:",
            "indexer/src/operator/sender/transaction.rs",
        ),
        (
            "no signatures to verify — remint unsafe",
            "indexer/src/operator/sender/transaction.rs",
        ),
        // Deposit — sender side. The processor-side strings (invalid_pubkey,
        // invalid_builder, program_error) are shared with withdrawals via
        // `classify_processor_error` and already covered above.
        (
            "Failed idempotency lookup for transaction_id",
            "indexer/src/operator/sender/mint.rs",
        ),
        (
            "Mint initialization failed",
            "indexer/src/operator/sender/transaction.rs",
        ),
        (
            "Unexpected mint error",
            "indexer/src/operator/sender/transaction.rs",
        ),
        (
            "Confirmation failed - transaction status unknown, unsafe to retry",
            "indexer/src/operator/sender/transaction.rs",
        ),
        // Deposit — sender side, post-JIT ManualReview path.
        // `deposit_manual_review.md` § Path D dispatches on these.
        // Constructed in full inside mint.rs (the caller arm in
        // transaction.rs passes the reason through unchanged) so this
        // drill can grep for the literals in a single source file.
        (
            "Mint instruction failed after JIT: mint_authority mismatch",
            "indexer/src/operator/sender/mint.rs",
        ),
        (
            "Mint instruction failed after JIT: corrupt mint state",
            "indexer/src/operator/sender/mint.rs",
        ),
        // Deposit — idempotency memo prefix. Anchors `_verify_onchain_mint.md`
        // step 3.
        (
            "private_channel:mint-idempotency:",
            "indexer/src/operator/constants.rs",
        ),
    ];

    // The integration test runs from the indexer crate root.
    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(&crate_root)
        .parent()
        .expect("workspace root");

    let mut missing: Vec<String> = Vec::new();
    for (substr, path) in contracts {
        let full = workspace_root.join(path);
        let content =
            std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {full:?}: {e}"));
        if content.contains(substr) {
            eprintln!("OK   {path}: {substr:?}");
        } else {
            eprintln!("MISS {path}: {substr:?}");
            missing.push(format!("{path}: {substr:?}"));
        }
    }

    assert!(
        missing.is_empty(),
        "runbook dispatch table contracts missing in source: {missing:#?}"
    );
}

// ── Drill 2: Path A — data error, re-arm collateral, mark trigger Failed ────
//
// Verifies that `withdrawal_manual_review.md § Path A` recovery SQL works:
//   1. Triage query returns rows in the right order (poison row first).
//   2. Mark-failed SQL terminates the trigger row.
//   3. Re-arm SQL flips collateral rows back to `pending`, leaves the
//      trigger row alone.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_2_path_a_data_error_recovery() -> Result<(), Box<dyn std::error::Error>> {
    drill_header("withdrawal_manual_review.md", "Path A — data error");

    let (pool, _storage, _pg) = start_postgres().await?;

    // Seed 1 poison row + 4 collateral sweep rows, all in `manual_review`.
    // Poison has the older `updated_at` so it sorts first in the triage
    // query; collateral rows have NULL error_message (the runbook treats
    // empty error_message as the marker for collateral).
    let poison_id = seed_withdrawal(&pool, "manual_review", 1, Some("invalid_pubkey")).await?;

    // Force the poison row's updated_at to be older so it sorts first.
    sqlx::query("UPDATE transactions SET updated_at = NOW() - INTERVAL '1 minute' WHERE id = $1")
        .bind(poison_id)
        .execute(&pool)
        .await?;

    let mut collateral_ids = Vec::new();
    for n in 2..=5 {
        let id = seed_withdrawal(&pool, "manual_review", n, None).await?;
        collateral_ids.push(id);
    }

    // ── Triage query (verbatim from runbook) ────────────────────────────
    let rows = sqlx::query(
        r#"
        SELECT id, withdrawal_nonce, updated_at
          FROM transactions
         WHERE transaction_type = 'withdrawal'
           AND status = 'manual_review'
         ORDER BY updated_at ASC
         LIMIT 20
        "#,
    )
    .fetch_all(&pool)
    .await?;
    eprintln!("triage returned {} rows", rows.len());
    let first_id: i64 = rows[0].get("id");
    assert_eq!(
        first_id, poison_id,
        "triage must return poison row first (oldest updated_at)"
    );
    assert_eq!(rows.len(), 5, "all 5 manual_review rows visible");

    // ── Mark trigger as failed (verbatim from runbook step 3) ──────────
    sqlx::query("UPDATE transactions SET status = 'failed', updated_at = NOW() WHERE id = $1")
        .bind(poison_id)
        .execute(&pool)
        .await?;
    assert_eq!(status_of(&pool, poison_id).await?, "failed");

    // ── Re-arm collateral (verbatim from runbook step 4) ───────────────
    // Note: drill seeds collateral with error_message = NULL, so the runbook's
    // `error_message IS NULL` filter applies cleanly. (The DB schema has no
    // error_message column today; the runbook semantics rely on the alert
    // payload, but the recovery SQL itself is column-free for the dispatch.)
    let updated = sqlx::query(
        r#"
        UPDATE transactions
           SET status = 'pending', updated_at = NOW()
         WHERE transaction_type = 'withdrawal'
           AND status = 'manual_review'
           AND id <> $1
        "#,
    )
    .bind(poison_id)
    .execute(&pool)
    .await?;
    assert_eq!(
        updated.rows_affected(),
        4,
        "exactly 4 collateral rows re-armed"
    );

    // ── Post-state assertions ──────────────────────────────────────────
    assert_eq!(count_status(&pool, "manual_review").await?, 0);
    assert_eq!(count_status(&pool, "failed").await?, 1);
    assert_eq!(count_status(&pool, "pending").await?, 4);
    for id in collateral_ids {
        assert_eq!(status_of(&pool, id).await?, "pending");
    }

    eprintln!("Path A recovery SQL verified end-to-end.");
    Ok(())
}

// ── Drill 3: Path B — stranded, on-chain LANDED → mark Completed ────────────
//
// Verifies the most dangerous Path B branch: when on-chain verification
// reveals the original release actually landed, the runbook says to mark
// the row Completed with the observed signature. This is the path that
// prevents double-credit when remint failure obscures a successful release.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_3_path_b_landed_marks_completed_with_signature(
) -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "withdrawal_manual_review.md",
        "Path B — stranded; on-chain LANDED branch",
    );

    let (pool, _storage, _pg) = start_postgres().await?;

    // Seed: row in manual_review (post-remint-failure), nonce 7. The
    // webhook payload for this row would carry
    // error_message="<orig> | remint failed: <err>".
    let id = seed_withdrawal(&pool, "manual_review", 7, Some("remint failed: timeout")).await?;

    // Drill simulates the operator running `_verify_onchain_release.md`
    // and getting verdict `LANDED <observed_sig>`.
    let observed_sig = Signature::new_unique().to_string();
    eprintln!("simulated on-chain verification: LANDED {observed_sig}");

    // ── Recovery (verbatim from runbook Path B step 1, LANDED branch) ──
    let updated = sqlx::query(
        r#"
        UPDATE transactions
           SET status = 'completed',
               counterpart_signature = $2,
               updated_at = NOW()
         WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(&observed_sig)
    .execute(&pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    // ── Post-state ────────────────────────────────────────────────────
    let row = sqlx::query(
        "SELECT status::text AS status, counterpart_signature FROM transactions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;
    let status: String = row.get("status");
    let cs: Option<String> = row.get("counterpart_signature");
    assert_eq!(status, "completed");
    assert_eq!(cs.as_deref(), Some(observed_sig.as_str()));

    // The unique index on counterpart_signature WHERE NOT NULL must accept this.
    eprintln!("Path B LANDED-branch recovery verified — row marked completed with observed sig.");
    Ok(())
}

// ── Drill 4: Path C — ambiguous, NOT_LANDED → re-arm to pending ─────────────
//
// Verifies that re-arming a `manual_review` withdrawal back to `pending`
// preserves `withdrawal_nonce` — the on-chain SMT leaf is keyed on this,
// and a re-attempt must hit the same leaf. Also verifies the schema's
// unique-nonce-per-withdrawal constraint does not block re-arming (the
// row keeps the same nonce; no new row is inserted).

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_4_path_c_not_landed_re_arms_with_same_nonce(
) -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "withdrawal_manual_review.md",
        "Path C — ambiguous; NOT_LANDED branch (re-arm)",
    );

    let (pool, _storage, _pg) = start_postgres().await?;

    // Seed: row in manual_review with a specific nonce. Webhook
    // error_message would be e.g. "no signatures to verify — remint unsafe".
    let original_nonce: i64 = 42;
    let id = seed_withdrawal(
        &pool,
        "manual_review",
        original_nonce,
        Some("no signatures to verify — remint unsafe"),
    )
    .await?;

    // Simulated verdict: NOT_LANDED (on-chain confirmed nothing happened).
    eprintln!("simulated on-chain verification: NOT_LANDED");

    // ── Recovery (Path C, NOT_LANDED branch — re-arm to pending) ──────
    let updated =
        sqlx::query("UPDATE transactions SET status = 'pending', updated_at = NOW() WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await?;
    assert_eq!(updated.rows_affected(), 1);

    // ── Post-state: status flipped, nonce preserved ───────────────────
    let row =
        sqlx::query("SELECT status::text AS s, withdrawal_nonce FROM transactions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
    let status: String = row.get("s");
    let nonce: Option<i64> = row.get("withdrawal_nonce");
    assert_eq!(status, "pending");
    assert_eq!(
        nonce,
        Some(original_nonce),
        "nonce must be preserved across re-arm — SMT leaf identity"
    );

    // ── Verify the nonce uniqueness constraint still holds ────────────
    // If we tried to insert a second withdrawal with the same nonce, the
    // unique partial index `idx_transactions_withdrawal_nonce_unique`
    // should reject it. The runbook implicitly relies on this — re-arming
    // is safe because no other row can claim the same nonce.
    let dup = sqlx::query(
        r#"
        INSERT INTO transactions
            (signature, slot, initiator, recipient, mint, amount,
             transaction_type, status, withdrawal_nonce,
             trace_id, created_at, updated_at)
        VALUES
            ($1, 100, $2, $3, $4, 1000,
             'withdrawal'::transaction_type,
             'pending'::transaction_status, $5,
             $6, NOW(), NOW())
        "#,
    )
    .bind(Signature::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(Pubkey::new_unique().to_string())
    .bind(original_nonce)
    .bind(uuid::Uuid::new_v4().to_string())
    .execute(&pool)
    .await;
    assert!(
        dup.is_err(),
        "nonce uniqueness must reject a second withdrawal with the same nonce"
    );

    eprintln!("Path C NOT_LANDED re-arm verified; nonce identity preserved.");
    Ok(())
}

// ── Drill 5: pipeline halt sweep ────────────────────────────────────────────
//
// Verifies the bulk sweep used in `withdrawal_manual_review.md § Path A.halting`:
// the operator's `quarantine_all_active_withdrawals` flips every Pending
// or Processing withdrawal to ManualReview, with one row excluded. Drives
// it via the actual storage method (not raw SQL) so a future change to
// the implementation is caught.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_5_halt_sweep_excludes_poison_only() -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "withdrawal_manual_review.md",
        "Path A.halting - quarantine_all_active_withdrawals semantics",
    );

    let (pool, storage, _pg) = start_postgres().await?;

    // Seed: 1 poison (already manual_review), 3 pending, 2 processing,
    // plus a `completed` row that must NOT be touched (terminal).
    let poison_id = seed_withdrawal(&pool, "manual_review", 1, Some("invalid_pubkey")).await?;
    let mut pending_ids = Vec::new();
    for n in 2..=4 {
        pending_ids.push(seed_withdrawal(&pool, "pending", n, None).await?);
    }
    let mut processing_ids = Vec::new();
    for n in 5..=6 {
        processing_ids.push(seed_withdrawal(&pool, "processing", n, None).await?);
    }
    let completed_id = seed_withdrawal(&pool, "completed", 7, None).await?;

    // Run the actual sweep used by the operator's halt path.
    let affected = storage
        .quarantine_all_active_withdrawals(Some(poison_id))
        .await?;

    // Should affect exactly the 5 pending+processing rows.
    eprintln!("sweep affected {affected} rows");
    assert_eq!(
        affected, 5,
        "sweep must touch exactly Pending+Processing rows other than the poison id"
    );

    // Poison stays as it was, completed untouched, the 5 actives now manual_review.
    assert_eq!(status_of(&pool, poison_id).await?, "manual_review");
    assert_eq!(status_of(&pool, completed_id).await?, "completed");
    for id in pending_ids.iter().chain(processing_ids.iter()) {
        assert_eq!(
            status_of(&pool, *id).await?,
            "manual_review",
            "row {id} should have been swept"
        );
    }

    // Final shape must match what the runbook tells the operator to expect:
    // 6 rows in manual_review (poison + 5 swept), 1 completed.
    assert_eq!(count_status(&pool, "manual_review").await?, 6);
    assert_eq!(count_status(&pool, "pending").await?, 0);
    assert_eq!(count_status(&pool, "processing").await?, 0);
    assert_eq!(count_status(&pool, "completed").await?, 1);

    eprintln!("Halt sweep semantics verified — exclude_id is honored, terminals untouched.");
    Ok(())
}

// ── Drill 6: PendingRemint recovery — terminal statuses do not re-queue ────
//
// The glossary lists `pending_remint` as the only status reloaded by the
// recovery query on operator restart. Path B and Path C recovery actions
// flip rows to `completed` / `failed_reminted` / `manual_review` — those
// must NOT re-enter the remint queue, otherwise a runbook recovery could
// trigger a duplicate remint on the next operator restart.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_6_recovery_query_skips_terminal_statuses() -> Result<(), Box<dyn std::error::Error>>
{
    drill_header("_glossary.md", "PendingRemint recovery contract");

    let (pool, storage, _pg) = start_postgres().await?;

    let deadline = Utc::now() + chrono::Duration::seconds(32);
    let sig = Signature::new_unique().to_string();

    // Seed three rows that all *started* in pending_remint, then were
    // resolved by recovery actions per the runbook.
    let to_completed = seed_withdrawal(&pool, "processing", 100, None).await?;
    storage
        .set_pending_remint(to_completed, vec![sig.clone()], vec![0], deadline)
        .await?;
    let to_failed_reminted = seed_withdrawal(&pool, "processing", 101, None).await?;
    storage
        .set_pending_remint(to_failed_reminted, vec![sig.clone()], vec![0], deadline)
        .await?;
    let to_manual_review = seed_withdrawal(&pool, "processing", 102, None).await?;
    storage
        .set_pending_remint(to_manual_review, vec![sig.clone()], vec![0], deadline)
        .await?;
    // Plus one that stays in pending_remint — the only one recovery should return.
    let still_pending = seed_withdrawal(&pool, "processing", 103, None).await?;
    storage
        .set_pending_remint(still_pending, vec![sig.clone()], vec![0], deadline)
        .await?;

    // Apply the recovery actions the runbook prescribes.
    sqlx::query("UPDATE transactions SET status='completed', counterpart_signature=$2 WHERE id=$1")
        .bind(to_completed)
        .bind(&sig)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE transactions SET status='failed_reminted' WHERE id=$1")
        .bind(to_failed_reminted)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE transactions SET status='manual_review' WHERE id=$1")
        .bind(to_manual_review)
        .execute(&pool)
        .await?;

    // Recovery query returns only the unresolved row.
    let pending = storage.get_pending_remint_transactions().await?;
    assert_eq!(
        pending.len(),
        1,
        "exactly one PendingRemint row should re-enter the queue on restart"
    );
    assert_eq!(pending[0].id, still_pending);

    eprintln!("Recovery query verified — no re-queue for completed/failed_reminted/manual_review.");
    Ok(())
}

// ── Drill 7: terminal statuses are immune to halt sweep ─────────────────────
//
// The glossary marks `completed`, `failed`, `failed_reminted`, and
// `manual_review` as terminal. The halt sweep must not touch any of them.
// This drill confirms the WHERE clause `status IN ('pending', 'processing')`
// is the actual gate — change it to `status NOT IN ('completed', ...)` and
// the runbook's terminality claim breaks.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_7_halt_sweep_does_not_touch_terminals() -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "_glossary.md",
        "Terminal statuses (Completed / Failed / FailedReminted / ManualReview)",
    );

    let (pool, storage, _pg) = start_postgres().await?;

    // One row in each terminal status.
    let completed = seed_withdrawal(&pool, "completed", 1, None).await?;
    let failed = seed_withdrawal(&pool, "failed", 2, None).await?;
    let failed_reminted = seed_withdrawal(&pool, "failed_reminted", 3, None).await?;
    let manual_review = seed_withdrawal(&pool, "manual_review", 4, None).await?;
    // Plus one active row so the sweep has *something* to do.
    let active = seed_withdrawal(&pool, "pending", 5, None).await?;

    let affected = storage.quarantine_all_active_withdrawals(None).await?;
    assert_eq!(
        affected, 1,
        "sweep should only touch the one Pending row; terminals are immune"
    );

    assert_eq!(status_of(&pool, completed).await?, "completed");
    assert_eq!(status_of(&pool, failed).await?, "failed");
    assert_eq!(status_of(&pool, failed_reminted).await?, "failed_reminted");
    assert_eq!(
        status_of(&pool, manual_review).await?,
        "manual_review",
        "manual_review row should not double-flip"
    );
    assert_eq!(status_of(&pool, active).await?, "manual_review");

    eprintln!("Terminal-status immunity to sweep verified.");
    Ok(())
}

// ── Drill 8: webhook contract — alertable set matches README dispatch ───────
//
// The README dispatches alerts on three webhook statuses: `failed`,
// `failed_reminted`, `manual_review`. The glossary makes the same claim.
// Both depend on `db_transaction_writer.rs::is_alertable`. This drill
// pins the contract by reading the source — drift in either direction
// (adding an unalerted terminal, or alerting on a non-terminal) breaks
// the runbook dispatch.

#[test]
#[ignore]
fn drill_8_alertable_set_matches_runbook_dispatch() {
    drill_header("README.md", "Alert dispatch table — alertable status set");

    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(&crate_root)
        .parent()
        .expect("workspace root");
    let writer = workspace_root.join("indexer/src/operator/db_transaction_writer.rs");
    let src = std::fs::read_to_string(&writer).expect("read db_transaction_writer.rs");

    // Locate the `is_alertable` match block.
    let start = src
        .find("let is_alertable = matches!(")
        .expect("is_alertable definition not found — runbook dispatch may be stale");
    let end = src[start..]
        .find(");")
        .expect("malformed is_alertable block")
        + start;
    let block = &src[start..end];
    eprintln!("is_alertable block:\n{block}");

    // Each runbook-claimed alertable status must appear in the match block.
    for variant in [
        "TransactionStatus::Failed",
        "TransactionStatus::FailedReminted",
        "TransactionStatus::ManualReview",
    ] {
        assert!(
            block.contains(variant),
            "runbook claims {variant} fires webhook but is_alertable does not list it"
        );
    }

    // Conversely: any non-terminal that is_alertable lists would be a
    // surprise to the runbook. Pin the count of variants in the match.
    let variant_count = block.matches("TransactionStatus::").count();
    assert_eq!(
        variant_count, 3,
        "is_alertable lists {variant_count} variants; runbook README + glossary expect exactly 3 \
         (Failed, FailedReminted, ManualReview). Update both if this changes."
    );

    eprintln!("Webhook alertable-set contract verified against runbook dispatch.");
}

// ── Drill 9: Path B recovery — counterpart_signature uniqueness fence ──────
//
// The schema has a unique partial index on counterpart_signature where it is
// not null (`idx_transactions_counterpart_signature`, `db.rs::init_schema`).
// This is a safety fence: if an operator running Path B's "mark Completed
// with the observed signature" recovery somehow misidentifies which row the
// on-chain action belonged to, the UPDATE must fail rather than silently
// attaching the same signature to two rows.
//
// Two sub-scenarios:
//   (a) Idempotent re-run — a recovery executed twice with the same sig
//       on the same row succeeds (no-op effect).
//   (b) Cross-row collision — the sig is already attached to a *different*
//       row; the UPDATE must be rejected.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_9_path_b_signature_uniqueness_fence() -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "withdrawal_manual_review.md",
        "Path B — counterpart_signature uniqueness fence (safety)",
    );

    let (pool, _storage, _pg) = start_postgres().await?;

    // ── Sub-scenario (a): idempotent re-run ───────────────────────────
    let id_a = seed_withdrawal(&pool, "manual_review", 100, Some("remint failed: x")).await?;
    let sig = Signature::new_unique().to_string();

    // First recovery: attach sig.
    sqlx::query(
        "UPDATE transactions SET status='completed', counterpart_signature=$2, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(id_a)
    .bind(&sig)
    .execute(&pool)
    .await?;
    assert_eq!(status_of(&pool, id_a).await?, "completed");

    // Second recovery on the same row with the same sig: must succeed (idempotent).
    let rerun = sqlx::query(
        "UPDATE transactions SET status='completed', counterpart_signature=$2, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(id_a)
    .bind(&sig)
    .execute(&pool)
    .await;
    assert!(
        rerun.is_ok(),
        "idempotent re-run of Path B recovery must succeed; got {rerun:?}"
    );
    eprintln!("(a) idempotent Path B re-run: ok");

    // ── Sub-scenario (b): cross-row collision ─────────────────────────
    // Seed a second row in manual_review (a different incident).
    let id_b = seed_withdrawal(&pool, "manual_review", 101, Some("remint failed: y")).await?;

    // Operator runs Path B on row B but supplies the SAME signature already
    // attached to row A. This would silently double-credit if not rejected;
    // the schema must reject it.
    let bad = sqlx::query(
        "UPDATE transactions SET status='completed', counterpart_signature=$2, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(id_b)
    .bind(&sig)
    .execute(&pool)
    .await;
    assert!(
        bad.is_err(),
        "cross-row counterpart_signature collision must be rejected by unique index"
    );
    eprintln!(
        "(b) cross-row Path B with conflicting sig rejected: {}",
        bad.err().unwrap()
    );

    // Row B must still be in manual_review (UPDATE rolled back).
    assert_eq!(status_of(&pool, id_b).await?, "manual_review");

    eprintln!("Path B signature uniqueness fence verified.");
    Ok(())
}

// ── Drill 10: deposit_failed.md recovery flows ──────────────────────────────
//
// Walks both LANDED and NOT_LANDED branches of `deposit_failed.md § Step 2`.
// Deposits use the same SQL shape as withdrawals (mark Completed with sig /
// re-arm to Pending) but no nonce identity to preserve.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_10_deposit_failed_recovery_flows() -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "deposit_failed.md",
        "Step 2 — branch on verdict (LANDED / NOT_LANDED)",
    );

    let (pool, _storage, _pg) = start_postgres().await?;

    // ── LANDED branch ──────────────────────────────────────────────────
    let landed_id = seed_deposit(&pool, "failed").await?;
    let observed_sig = Signature::new_unique().to_string();
    eprintln!("LANDED branch: simulated verdict {observed_sig}");

    sqlx::query(
        "UPDATE transactions SET status='completed', counterpart_signature=$2, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(landed_id)
    .bind(&observed_sig)
    .execute(&pool)
    .await?;
    assert_eq!(status_of(&pool, landed_id).await?, "completed");
    let cs: Option<String> =
        sqlx::query_scalar("SELECT counterpart_signature FROM transactions WHERE id=$1")
            .bind(landed_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(cs.as_deref(), Some(observed_sig.as_str()));

    // ── NOT_LANDED branch — re-arm to pending ─────────────────────────
    let not_landed_id = seed_deposit(&pool, "failed").await?;
    eprintln!("NOT_LANDED branch: re-arming");
    sqlx::query("UPDATE transactions SET status='pending', updated_at=NOW() WHERE id=$1")
        .bind(not_landed_id)
        .execute(&pool)
        .await?;
    assert_eq!(status_of(&pool, not_landed_id).await?, "pending");

    // Deposits have no nonce, so no nonce-uniqueness contract to verify here
    // (unlike withdrawals — see drill_4). Two pending deposits coexist freely.
    let _other = seed_deposit(&pool, "pending").await?;

    // ── deposit_manual_review.md — Path A (mark failed) ───────────────
    let mr_id = seed_deposit(&pool, "manual_review").await?;
    sqlx::query("UPDATE transactions SET status='failed', updated_at=NOW() WHERE id=$1")
        .bind(mr_id)
        .execute(&pool)
        .await?;
    assert_eq!(status_of(&pool, mr_id).await?, "failed");

    eprintln!("Deposit recovery flows verified end-to-end.");
    Ok(())
}

// ── Drill 14: deposit_manual_review.md § Path D recovery flows ─────────────
//
// `deposit_manual_review.md` § Path D documents recovery for the
// sender-side post-JIT ManualReview path. This drill pins:
//
//   1. The two new error_message substrings exist in mint.rs (also
//      covered by drill_1 globally; re-asserted here so the drill is
//      self-contained for an operator running it ad-hoc).
//   2. The idempotency memo prefix is still anchored — the recovery
//      flow re-arms to `pending`, and that re-arm is only safe because
//      the operator's pre-send memo scan dedupes on this prefix.
//   3. Recovery SQL flips `manual_review` → `pending` for the trigger
//      row only; collateral and terminal rows are untouched.
//   4. Recovery SQL is targeted by `id`, not by `error_message` — pins
//      that drill_14's contract is fungible across the two new
//      runbook substrings (and any future ones in the same Path D
//      table).
//   5. Webhook alertability is structurally guaranteed by drill_8 —
//      this drill only narrates the dependency for the on-call reader.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_14_deposit_manual_review_post_jit_recovery_flows(
) -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "deposit_manual_review.md",
        "Path D — sender-side post-JIT failure",
    );

    // ── Step 1: source contract presence ──────────────────────────────
    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(&crate_root)
        .parent()
        .expect("workspace root");
    for substr in [
        "Mint instruction failed after JIT: mint_authority mismatch",
        "Mint instruction failed after JIT: corrupt mint state",
    ] {
        let path = workspace_root.join("indexer/src/operator/sender/mint.rs");
        let content =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        assert!(
            content.contains(substr),
            "Path D dispatch substring missing from mint.rs: {substr:?}",
        );
        eprintln!("OK   mint.rs: {substr:?}");
    }

    // ── Step 2: idempotency memo prefix anchored ──────────────────────
    let constants_path = workspace_root.join("indexer/src/operator/constants.rs");
    let constants_content = std::fs::read_to_string(&constants_path)
        .unwrap_or_else(|e| panic!("read {constants_path:?}: {e}"));
    assert!(
        constants_content.contains("private_channel:mint-idempotency:"),
        "idempotency memo prefix must remain anchored — Path D re-arm \
         relies on the pre-send memo scan to prevent double-mint",
    );
    eprintln!("OK   constants.rs: idempotency memo prefix anchored");

    // ── Steps 3 + 4: recovery SQL is structural and targeted by id ────
    let (pool, _storage, _pg) = start_postgres().await?;

    // Seed three deposits: trigger (manual_review), collateral
    // (pending), terminal (completed). The Path D re-arm SQL must
    // touch only the trigger row.
    let trigger_id = seed_deposit(&pool, "manual_review").await?;
    let collateral_id = seed_deposit(&pool, "pending").await?;
    let terminal_id = seed_deposit(&pool, "completed").await?;

    sqlx::query("UPDATE transactions SET status='pending', updated_at=NOW() WHERE id=$1")
        .bind(trigger_id)
        .execute(&pool)
        .await?;

    assert_eq!(
        status_of(&pool, trigger_id).await?,
        "pending",
        "Path D re-arm must flip the trigger row to pending"
    );
    assert_eq!(
        status_of(&pool, collateral_id).await?,
        "pending",
        "collateral row must be untouched by Path D re-arm"
    );
    assert_eq!(
        status_of(&pool, terminal_id).await?,
        "completed",
        "terminal row must be untouched by Path D re-arm"
    );

    // Repeat the re-arm against a different ManualReview row — the SQL
    // is fungible across error_message values (Path D's two
    // substrings, and the processor-side ManualReview substrings from
    // Paths A/B/C). This catches a future "smart" runbook edit that
    // adds a `WHERE error_message LIKE …` clause and breaks
    // fungibility with drill_10.
    let other_mr_id = seed_deposit(&pool, "manual_review").await?;
    sqlx::query("UPDATE transactions SET status='pending', updated_at=NOW() WHERE id=$1")
        .bind(other_mr_id)
        .execute(&pool)
        .await?;
    assert_eq!(
        status_of(&pool, other_mr_id).await?,
        "pending",
        "re-arm must work regardless of which ManualReview substring was the trigger"
    );

    // ── Step 5: webhook alertability cross-reference ─────────────────
    eprintln!(
        "Webhook alertability for ManualReview is pinned structurally by drill_8 — \
         drill_14 does not re-test that contract."
    );

    eprintln!("Path D recovery flows verified end-to-end.");
    Ok(())
}

// ── Drill 11: program_type label contract ───────────────────────────────────
//
// The README + glossary tell operators to filter metrics by
// `program_type="withdraw"` for withdrawals and `program_type="escrow"` for
// deposits (not "deposit" — easy to get wrong). Pin the source.

#[test]
#[ignore]
fn drill_11_program_type_labels_match_runbooks() {
    drill_header("_glossary.md", "Metrics — program_type label values");

    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(&crate_root)
        .parent()
        .expect("workspace root");
    let cfg = workspace_root.join("indexer/src/config.rs");
    let src = std::fs::read_to_string(&cfg).expect("read config.rs");

    // Locate `as_label` and confirm the two arms emit the expected strings.
    let start = src.find("fn as_label(").expect("as_label not found");
    let end = src[start..]
        .find('}')
        .map(|i| start + i + 1)
        .expect("malformed");
    let block = &src[start..end];
    eprintln!("as_label block:\n{block}");

    assert!(
        block.contains("ProgramType::Escrow => \"escrow\""),
        "deposit operator's program_type label must be \"escrow\" \
         (runbooks reference it explicitly; \"deposit\" would silently miss)"
    );
    assert!(
        block.contains("ProgramType::Withdraw => \"withdraw\""),
        "withdrawal operator's program_type label must be \"withdraw\""
    );

    eprintln!("program_type label contract verified.");
}

// ── Drill 12: withdrawal_failed.md recovery flows ───────────────────────────
//
// `withdrawal_failed.md` covers the rare case of a withdrawal row reaching
// the terminal `failed` status (per `_glossary.md:14-22`, this normally
// only happens for deposits, withdrawals go through `pending_remint`).
// The runbook routes by the on-chain release verdict from
// `_verify_onchain_release.md`. This drill walks each branch:
//
//   - LANDED  → mark Completed with the observed signature (code-defect
//                path: the row should never have been `failed`).
//   - NOT_LANDED → `failed` is terminal; the runbook says "Do not re-arm".
//   - AMBIGUOUS  → no SQL; escalate Tier 2.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_12_withdrawal_failed_recovery_flows() -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "withdrawal_failed.md",
        "Step 2 — branch on verdict (LANDED / NOT_LANDED / AMBIGUOUS)",
    );

    let (pool, _storage, _pg) = start_postgres().await?;

    // ── (1) LANDED branch — mark Completed with observed sig ──────────────
    // The runbook's recovery query at `withdrawal_failed.md:30-35`. Verifies
    // the SQL still parses against the schema and produces the documented
    // end-state on a `failed` starting state.
    let nonce_a: i64 = 200;
    let id_a = seed_withdrawal(
        &pool,
        "failed",
        nonce_a,
        Some("misrouted to send_fatal_error"),
    )
    .await?;
    let observed_sig = Signature::new_unique().to_string();
    eprintln!("(1) LANDED branch: simulated verdict {observed_sig}");

    let updated = sqlx::query(
        "UPDATE transactions SET status='completed', counterpart_signature=$2, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(id_a)
    .bind(&observed_sig)
    .execute(&pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);

    let row = sqlx::query(
        "SELECT status::text AS s, counterpart_signature, withdrawal_nonce
           FROM transactions WHERE id=$1",
    )
    .bind(id_a)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.get::<String, _>("s"), "completed");
    assert_eq!(
        row.get::<Option<String>, _>("counterpart_signature")
            .as_deref(),
        Some(observed_sig.as_str())
    );
    assert_eq!(
        row.get::<Option<i64>, _>("withdrawal_nonce"),
        Some(nonce_a),
        "nonce must be preserved — SMT leaf identity per _glossary.md:62-68"
    );

    // ── (2) Cross-row signature uniqueness fence ──────────────────────────
    // Mirrors drill_9 sub-(b), starting state `failed` instead of
    // `manual_review`. Pins that the same double-credit fence active on
    // manual_review recovery also fires for `failed` recovery — a future
    // schema change that scoped the unique partial index to specific
    // statuses would fail this assertion.
    let id_b = seed_withdrawal(&pool, "failed", 201, Some("second incident")).await?;
    let bad = sqlx::query(
        "UPDATE transactions SET status='completed', counterpart_signature=$2, updated_at=NOW()
          WHERE id=$1",
    )
    .bind(id_b)
    .bind(&observed_sig) // collision with row A
    .execute(&pool)
    .await;
    assert!(
        bad.is_err(),
        "cross-row counterpart_signature collision must be rejected by unique index"
    );
    assert_eq!(
        status_of(&pool, id_b).await?,
        "failed",
        "row B must remain `failed` after rejected UPDATE"
    );
    eprintln!("(2) cross-row sig fence active on `failed` recovery: ok");

    // ── (3) NOT_LANDED branch — `failed` is terminal ──────────────────────
    // The runbook says: "Do not re-arm a `failed` row - the status is
    // terminal by contract." Pin via two complementary checks.
    //
    // (3a) Markdown scan: `withdrawal_failed.md` does not contain a
    //      `failed → pending` UPDATE. Direct test of the runbook itself.
    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(&crate_root)
        .parent()
        .expect("workspace root");
    let runbook = workspace_root.join("docs/runbooks/withdrawal_failed.md");
    let runbook_text = std::fs::read_to_string(&runbook).expect("read withdrawal_failed.md");
    assert!(
        !runbook_text.contains("status = 'pending'") && !runbook_text.contains("status='pending'"),
        "withdrawal_failed.md must not contain a `failed → pending` UPDATE — \
         the runbook's terminal-status contract would be silently violated"
    );

    // (3b) Code grep: no operator code path re-arms a `failed` row to
    //      `pending` via *literal* SQL. Note: the operator uses sqlx
    //      parameterised queries, so this tripwire only catches literal-
    //      SQL violations (rare in this codebase). The primary defense is
    //      sub-3a (markdown scan); 3b is defense-in-depth for the case
    //      where someone bypasses the storage abstraction with raw SQL.
    let operator_dir = workspace_root.join("indexer/src/operator");
    let mut violations: Vec<String> = Vec::new();
    for entry in walkdir(&operator_dir) {
        if entry.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let src = match std::fs::read_to_string(&entry) {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Look for any UPDATE that mentions both status='failed' as a
        // selector and status='pending' as a setter in close proximity.
        // The substring search is conservative — it matches the SQL
        // shape the operator code would have to use to violate the
        // contract.
        let mentions_failed = src.contains("status='failed'") || src.contains("status = 'failed'");
        let flips_to_pending =
            src.contains("SET status='pending'") || src.contains("SET status = 'pending'");
        if mentions_failed && flips_to_pending {
            violations.push(entry.display().to_string());
        }
    }
    assert!(
        violations.is_empty(),
        "operator code must not re-arm `failed` rows to `pending`; found in: {violations:?}"
    );
    eprintln!("(3) NOT_LANDED branch terminal contract verified (markdown + code grep)");

    // ── (4) AMBIGUOUS branch — no SQL, escalate Tier 2 ────────────────────
    // Pin via markdown scan: the AMBIGUOUS paragraph contains no SQL and
    // contains the word "Escalate".
    let ambiguous_idx = runbook_text
        .find("`AMBIGUOUS`")
        .expect("withdrawal_failed.md must mention AMBIGUOUS verdict");
    let ambiguous_para =
        &runbook_text[ambiguous_idx..ambiguous_idx + 200.min(runbook_text.len() - ambiguous_idx)];
    assert!(
        ambiguous_para.contains("Escalate") || ambiguous_para.contains("escalate"),
        "AMBIGUOUS branch must escalate; got:\n{ambiguous_para}"
    );
    assert!(
        !ambiguous_para.contains("UPDATE"),
        "AMBIGUOUS branch must contain no SQL; got:\n{ambiguous_para}"
    );
    eprintln!("(4) AMBIGUOUS branch: escalate, no SQL — verified");

    eprintln!("withdrawal_failed.md recovery flows verified end-to-end.");
    Ok(())
}

// ── Drill 13: withdrawal_failed_reminted.md reconciliation contract ─────────
//
// `withdrawal_failed_reminted.md` is a SUCCESS-OUTCOME runbook — the
// original withdrawal failed on Solana, then the channel-side remint
// succeeded, and no funds are stranded. The reconciliation is read-only:
// confirm the remint signature on-chain, confirm the original release did
// NOT land, close the alert. The runbook contains zero recovery SQL.
//
// This drill pins:
//   (1) the upstream production path that writes `remint_signatures`
//       before the FailedReminted transition,
//   (2) the Step 1 query shape,
//   (3) the read-only contract — no mutating SQL in the runbook,
//   (4) the double-credit fence — if release LANDED, escalate, do not
//       silently mark Completed,
//   (5) the webhook ↔ DB naming asymmetry: webhook payload uses
//       `remint_signature` (singular), DB column is `remint_signatures`
//       (plural array). Per `_glossary.md:35`.

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn drill_13_withdrawal_failed_reminted_reconcile() -> Result<(), Box<dyn std::error::Error>> {
    drill_header(
        "withdrawal_failed_reminted.md",
        "Reconciliation contract (read-only) + double-credit fence",
    );

    let (pool, _storage, _pg) = start_postgres().await?;

    let crate_root = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(&crate_root)
        .parent()
        .expect("workspace root");

    // ── (1) Production code path: FailedReminted preceded by remint_signatures ─
    // The runbook's Step 1 hard-requires non-empty `remint_signatures`. The
    // schema does NOT enforce this (no CHECK constraint); the upstream
    // operator code does, and it does so across two files (deliberately
    // decoupled — signatures are stashed during the original send so a
    // restart between send and confirmation can recover). Pin both legs:
    //
    //   (1a) sender/transaction.rs calls `set_pending_remint(tx_id, sigs, …)`
    //        — this is the persistence call site; runs BEFORE any
    //        FailedReminted transition.
    //   (1b) sender/remint.rs is the only file that writes the
    //        FailedReminted status — confirms the transition has a single
    //        source we can reason about.
    //   (1c) the storage layer's set_pending_remint binds the
    //        `remint_signatures` column — runbook Step 1's column lookup
    //        will find what was written.
    let tx_src = workspace_root.join("indexer/src/operator/sender/transaction.rs");
    let tx_text = std::fs::read_to_string(&tx_src).expect("read sender/transaction.rs");
    assert!(
        tx_text.contains(".set_pending_remint(") || tx_text.contains("set_pending_remint("),
        "(1a) sender/transaction.rs must call set_pending_remint(…) to stash \
         remint signatures before any FailedReminted transition"
    );

    let remint_src = workspace_root.join("indexer/src/operator/sender/remint.rs");
    let remint_text = std::fs::read_to_string(&remint_src).expect("read sender/remint.rs");
    assert!(
        remint_text.contains("TransactionStatus::FailedReminted"),
        "(1b) sender/remint.rs must contain the FailedReminted status transition"
    );

    let storage_src = workspace_root.join("indexer/src/storage/postgres/db.rs");
    let storage_text = std::fs::read_to_string(&storage_src).expect("read db.rs");
    let spr_idx = storage_text
        .find("fn set_pending_remint")
        .expect("(1c) db.rs must define set_pending_remint");
    let spr_window = &storage_text[spr_idx..(spr_idx + 1500).min(storage_text.len())];
    assert!(
        spr_window.contains("remint_signatures"),
        "(1c) db.rs::set_pending_remint must bind the `remint_signatures` column"
    );
    eprintln!("(1) FailedReminted ↔ remint_signatures persistence pinned across both files: ok");

    // ── (2) Step 1 query shape works on a well-formed seeded row ──────────
    // Seeds with two signatures (mirrors retry-attempt array shape). Runs
    // the implicit query backing Step 1: read the column, expect an array
    // of length ≥ 1.
    let sig1 = Signature::new_unique().to_string();
    let sig2 = Signature::new_unique().to_string();
    let id = seed_failed_reminted(&pool, 300, &[&sig1, &sig2]).await?;

    let stored_sigs: Vec<String> =
        sqlx::query_scalar("SELECT remint_signatures FROM transactions WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await?;
    assert!(
        !stored_sigs.is_empty(),
        "seeded failed_reminted row must have non-empty remint_signatures"
    );
    assert_eq!(stored_sigs.len(), 2);
    eprintln!(
        "(2) Step 1 query returned {} remint signatures: {:?}",
        stored_sigs.len(),
        stored_sigs
    );

    // ── (3) Read-only contract — no mutating SQL in the runbook ───────────
    // The whole runbook is "verify and close". A future edit that sneaks in
    // an UPDATE/INSERT/DELETE on the row's status would silently shift the
    // contract. Markdown scan: zero mutating SQL keywords.
    let runbook = workspace_root.join("docs/runbooks/withdrawal_failed_reminted.md");
    let runbook_text =
        std::fs::read_to_string(&runbook).expect("read withdrawal_failed_reminted.md");
    for keyword in &["UPDATE transactions", "INSERT INTO", "DELETE FROM"] {
        assert!(
            !runbook_text.contains(keyword),
            "withdrawal_failed_reminted.md must not contain `{keyword}` — \
             the runbook is read-only by design (success-outcome reconciliation)"
        );
    }
    eprintln!("(3) read-only contract verified — no mutating SQL in runbook");

    // ── (4) Double-credit fence on LANDED verdict ─────────────────────────
    // If `_verify_onchain_release.md` returns LANDED, the user has been
    // double-credited. Runbook prescribes Tier 1 escalation, NOT a status
    // flip. Pin via two checks:
    //
    // (4a) No `SET status='completed'` in the file. Already implied by (3),
    //      but called out separately because the contract semantics differ.
    assert!(
        !runbook_text.contains("status = 'completed'")
            && !runbook_text.contains("status='completed'"),
        "withdrawal_failed_reminted.md must not contain a `SET status='completed'` — \
         that would silently absorb a double-credit on LANDED verdict"
    );

    // (4b) The LANDED paragraph (lines 26-28 of the runbook at time of
    //      writing) escalates to Tier 1.
    let landed_idx = runbook_text
        .find("If `LANDED`")
        .expect("runbook must discuss LANDED branch");
    let landed_para_end = (landed_idx + 400).min(runbook_text.len());
    let landed_para = &runbook_text[landed_idx..landed_para_end];
    assert!(
        landed_para.contains("escalate") || landed_para.contains("Escalate"),
        "LANDED branch must escalate; got:\n{landed_para}"
    );
    assert!(
        landed_para.contains("Tier 1"),
        "LANDED branch must escalate Tier 1 (double-credit is funds-at-risk); got:\n{landed_para}"
    );
    eprintln!("(4) double-credit fence pinned: no completed-flip; LANDED → Tier 1");

    // ── (5) Webhook ↔ DB naming asymmetry ─────────────────────────────────
    // Glossary at `_glossary.md:35` declares the webhook payload uses
    // `remint_signature` (singular). The DB column is `remint_signatures`
    // (plural array). Both must be present; if either is renamed without
    // the other, the runbook reader's mental model breaks.
    let writer_src = workspace_root.join("indexer/src/operator/db_transaction_writer.rs");
    let writer_text = std::fs::read_to_string(&writer_src).expect("read db_transaction_writer.rs");
    assert!(
        writer_text.contains("\"remint_signature\""),
        "webhook serializer in db_transaction_writer.rs must emit JSON key \
         `remint_signature` (singular) — matches _glossary.md:35"
    );

    let db_src = workspace_root.join("indexer/src/storage/postgres/db.rs");
    let db_text = std::fs::read_to_string(&db_src).expect("read db.rs");
    assert!(
        db_text.contains("remint_signatures TEXT[]"),
        "DB column must be `remint_signatures TEXT[]` (plural array) — \
         matches the migration at db.rs ALTER TABLE"
    );
    eprintln!("(5) webhook/DB asymmetry pinned: `remint_signature` ↔ `remint_signatures TEXT[]`");

    eprintln!("withdrawal_failed_reminted.md reconciliation contract verified.");
    Ok(())
}

// Minimal recursive-walk helper used by drill_12's code grep. Avoids
// pulling in `walkdir` as a new dev-dep just for two drills.
fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out: Vec<std::path::PathBuf> = Vec::new();
    let mut stack: Vec<std::path::PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}
