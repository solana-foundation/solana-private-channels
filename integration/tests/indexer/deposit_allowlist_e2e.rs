//! Integration tests for the deposit-allowlist enforcement, against a
//! real Postgres
//!
//! Two changes are exercised here:
//!
//! 1. **Deposit-operator allowlist gate** — `MintCache::assert_mint_allowed_at_slot`
//!    must reject mints with no row in `mints` and accept those that do,
//!    using the public `Storage::Postgres` backend.
//!
//! 2. **Reconciliation orphan-row detection** — `Storage::get_orphan_deposit_ids`
//!    must return exactly the deposit rows whose mint has no allowlist
//!    entry, with realistic mixed-state inputs.
//!
//! Full operator-pipeline e2e (indexer → operator → on-chain submission)
//! is covered separately by the validator-driven tests in this crate.
//! What we get from this file is the load-bearing SQL behavior and the
//! gate behavior verified against the same Postgres engine that runs in
//! production, without the cost of bringing up a validator.

use private_channel_indexer::{
    operator::MintCache,
    storage::{
        common::models::{DbMint, DbTransactionBuilder, TransactionType},
        PostgresDb, Storage,
    },
    PostgresConfig,
};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── Harness ───────────────────────────────────────────────────────────────────

/// Start a fresh Postgres container, init schema, return the public
/// `Storage` (the SUT) and the container guard (kept alive for the
/// test's lifetime).
async fn start_postgres(
) -> Result<(Arc<Storage>, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>> {
    let container = Postgres::default()
        .with_db_name("deposit_allowlist_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/deposit_allowlist_test",
        host, port
    );

    let storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url,
            max_connections: 5,
        })
        .await?,
    ));
    storage.init_schema().await?;

    Ok((storage, container))
}

/// Land a single allowlist entry via the same upsert path the indexer
/// uses for `AllowMint` events. Keeping fixtures on the production API
/// means any future schema migration breaks here in the same way it
/// would break the real ingest path.
async fn insert_mint_row(storage: &Storage, mint: &str) -> Result<(), Box<dyn std::error::Error>> {
    insert_mint_row_at_slot(storage, mint, 0).await
}

/// Like [`insert_mint_row`] but records the `allowed` status at an explicit
/// `effective_slot` (the slot the AllowMint took effect on-chain).
async fn insert_mint_row_at_slot(
    storage: &Storage,
    mint: &str,
    effective_slot: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mint_row = DbMint::new(mint.to_string(), 6, spl_token::id().to_string());
    storage.upsert_mints_batch(&[mint_row]).await?;
    storage
        .insert_mint_statuses_batch(&[
            private_channel_indexer::storage::common::models::DbMintStatus {
                mint_address: mint.to_string(),
                status: "allowed".to_string(),
                effective_slot,
                signature: format!("test-seed-{mint}-{effective_slot}"),
                created_at: chrono::Utc::now(),
            },
        ])
        .await?;
    Ok(())
}

/// Insert a deposit transaction row via the production insert path
/// (`Storage::insert_db_transaction` → `insert_transaction_internal`).
/// Returns the assigned row id. Status defaults to `Pending` —
/// orphan detection is status-agnostic, so this matches the worst-case
/// shape where the operator has not yet processed the row.
async fn insert_deposit_row(
    storage: &Storage,
    signature: &str,
    mint: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    insert_deposit_row_at_slot(storage, signature, mint, 1).await
}

/// Like [`insert_deposit_row`] but at an explicit slot (the orphan query
/// compares deposit slot against each mint-status `effective_slot`).
async fn insert_deposit_row_at_slot(
    storage: &Storage,
    signature: &str,
    mint: &str,
    slot: u64,
) -> Result<i64, Box<dyn std::error::Error>> {
    let tx = DbTransactionBuilder::new(signature.to_string(), slot, mint.to_string(), 100)
        .initiator("init".to_string())
        .recipient("recip".to_string())
        .transaction_type(TransactionType::Deposit)
        .build();
    let id = storage.insert_db_transaction(&tx).await?;
    Ok(id)
}

/// Insert a withdrawal transaction row via the production insert path.
/// Used to prove the orphan query scopes to deposits — withdrawals are
/// not part of this surface.
async fn insert_withdrawal_row(
    storage: &Storage,
    signature: &str,
    mint: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut tx = DbTransactionBuilder::new(signature.to_string(), 1, mint.to_string(), 100)
        .initiator("init".to_string())
        .recipient("recip".to_string())
        .transaction_type(TransactionType::Withdrawal)
        .build();
    // The builder doesn't take `withdrawal_nonce`; production code path
    // assigns it directly on the struct (see `pending_remint_storage.rs`).
    tx.withdrawal_nonce = Some(0);
    let id = storage.insert_db_transaction(&tx).await?;
    Ok(id)
}

// ── Orphan query: positive case ──────────────────────────────────────────────

/// A deposit row whose mint has no `mints` entry must surface as an
/// orphan. This is the core signal the reconciliation log is built on —
/// without it, foreign-mint deposits accumulate invisibly.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_returns_deposit_with_no_allowlist_entry(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let orphan_mint = Pubkey::new_unique().to_string();
    let orphan_id = insert_deposit_row(&storage, "sig_orphan", &orphan_mint).await?;

    let ids = storage.get_orphan_deposit_ids().await?;
    assert_eq!(
        ids,
        vec![orphan_id],
        "deposit with no AllowMint must surface as orphan",
    );

    Ok(())
}

// ── Orphan query: false-positive bounds ──────────────────────────────────────

/// Allowlisted-mint deposits must NOT appear in the orphan set. Without
/// this bound, the dedup loop would log on every healthy deposit.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_excludes_allowlisted_deposits() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    insert_mint_row(&storage, &mint).await?;
    insert_deposit_row(&storage, "sig_allowed", &mint).await?;

    let ids = storage.get_orphan_deposit_ids().await?;
    assert!(
        ids.is_empty(),
        "allowlisted deposit must not appear as orphan, got {:?}",
        ids,
    );

    Ok(())
}

/// Withdrawals are intentionally outside the orphan-detection scope.
/// Burning channel tokens on-chain already bounds the withdrawal side
/// (you can't burn what you don't have), and conflating withdrawal rows
/// would dilute the deposit-side signal.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_excludes_withdrawals() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    // No mints row; transaction_type = withdrawal.
    let mint = Pubkey::new_unique().to_string();
    insert_withdrawal_row(&storage, "sig_w", &mint).await?;

    let ids = storage.get_orphan_deposit_ids().await?;
    assert!(
        ids.is_empty(),
        "orphan query must scope to deposits, got {:?}",
        ids,
    );

    Ok(())
}

// ── Orphan query: cardinality + mixed state ──────────────────────────────────

/// Multiple deposit rows for the same orphan mint must all be returned.
/// If the query accidentally collapsed by mint, a single foreign mint
/// with 100 rows would report as "1" — losing the magnitude signal
/// on-callers need to triage attack vs. ordering race.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_returns_every_orphan_row() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let id1 = insert_deposit_row(&storage, "sig_1", &mint).await?;
    let id2 = insert_deposit_row(&storage, "sig_2", &mint).await?;
    let id3 = insert_deposit_row(&storage, "sig_3", &mint).await?;

    let mut ids = storage.get_orphan_deposit_ids().await?;
    ids.sort();
    let mut expected = vec![id1, id2, id3];
    expected.sort();
    assert_eq!(
        ids, expected,
        "every orphan row must be returned (no implicit DISTINCT mint)",
    );

    Ok(())
}

/// Realistic shape: some mints allowlisted, some not. A single query
/// must isolate exactly the orphan rows and leave allowlisted-mint
/// deposits alone.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_isolates_orphans_in_mixed_state() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let allowed_mint = Pubkey::new_unique().to_string();
    let orphan_mint = Pubkey::new_unique().to_string();
    insert_mint_row(&storage, &allowed_mint).await?;

    insert_deposit_row(&storage, "sig_allowed", &allowed_mint).await?;
    let orphan_id = insert_deposit_row(&storage, "sig_orphan", &orphan_mint).await?;

    let ids = storage.get_orphan_deposit_ids().await?;
    assert_eq!(
        ids,
        vec![orphan_id],
        "only the foreign-mint row should be flagged",
    );

    Ok(())
}

// ── Orphan query: late-INGESTED AllowMint (effective at/before deposit) ───────

/// Benign slot-ordering race: an `AllowMint` effective at/before the deposit's
/// slot (3 <= 5) but ingested after it. The deposit is orphaned on the first
/// tick, then clears once the AllowMint lands. ("Late" = ingested late, not
/// effective late; the opposite case is the `_effective_after_deposit` test.)
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_clears_when_allowmint_ingested_late() -> Result<(), Box<dyn std::error::Error>>
{
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let orphan_id = insert_deposit_row_at_slot(&storage, "sig_late", &mint, 5).await?;

    // Tick 1: row is orphaned because no status row exists yet.
    let ids_before = storage.get_orphan_deposit_ids().await?;
    assert_eq!(
        ids_before,
        vec![orphan_id],
        "row must be orphaned before AllowMint lands",
    );

    // AllowMint lands, effective at slot 3 (<= the deposit slot 5).
    insert_mint_row_at_slot(&storage, &mint, 3).await?;

    // Tick 2: same row is no longer orphaned.
    let ids_after = storage.get_orphan_deposit_ids().await?;
    assert!(
        ids_after.is_empty(),
        "AllowMint effective before the deposit must clear the orphan, got {:?}",
        ids_after,
    );

    Ok(())
}

/// An `AllowMint` effective *after* the deposit (5 > 3) must NOT clear the
/// orphan — the deposit landed before the mint was allowed. Pins the
/// `effective_slot <= deposit.slot` gating against retroactive legitimization.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_query_persists_when_allowmint_effective_after_deposit(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique().to_string();
    let orphan_id = insert_deposit_row_at_slot(&storage, "sig_pre_allow", &mint, 3).await?;

    // AllowMint becomes effective at slot 5 — strictly after the deposit.
    insert_mint_row_at_slot(&storage, &mint, 5).await?;

    let ids = storage.get_orphan_deposit_ids().await?;
    assert_eq!(
        ids,
        vec![orphan_id],
        "deposit before the mint's allowed slot must remain an orphan, got {:?}",
        ids,
    );

    Ok(())
}

// ── Deposit gate against real Postgres ───────────────────────────────────────

/// Deposit gate end-to-end against real Postgres:
///   • Unknown mint → gate errors (operator quarantines the row in
///     production).
///   • Same mint after AllowMint lands → gate passes.
#[tokio::test(flavor = "multi_thread")]
async fn assert_mint_allowed_at_slot_against_real_postgres(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let cache = MintCache::new(storage.clone());

    // Pre-AllowMint: the gate must refuse. The error variant itself is
    // covered by the unit tests; here we only need to confirm the call
    // errors when the row is absent.
    let err = cache
        .assert_mint_allowed_at_slot(&mint, 100, 42)
        .await
        .expect_err("unknown mint must not pass the gate");
    let _ = err;

    // AllowMint lands.
    insert_mint_row(&storage, &mint.to_string()).await?;

    // Post-AllowMint: the gate accepts. A fresh cache is used to rule
    // out any in-memory shortcut — every call must consult the DB.
    let cache = MintCache::new(storage.clone());
    cache
        .assert_mint_allowed_at_slot(&mint, 100, 42)
        .await
        .expect("allowlisted mint must pass the gate");

    Ok(())
}

/// The gate must NOT cache a negative result. If the first call (mint
/// absent) cached `not allowlisted` and the second call (mint present,
/// after AllowMint indexing) used the cache, the operator would
/// permanently refuse a legitimate mint until restart.
///
/// Regression-guard for the cache-bypass property documented on
/// `assert_mint_allowed_at_slot` — unit tests prove the in-memory layer is
/// bypassed; this proves the property holds when the underlying store
/// is Postgres rather than the mock.
#[tokio::test(flavor = "multi_thread")]
async fn assert_mint_allowed_at_slot_does_not_cache_negative_result(
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    // Same cache instance across both calls — the second call would
    // be served from cache if the gate ever decided to memoize.
    let cache = MintCache::new(storage.clone());

    // First call: mint absent → gate refuses.
    cache
        .assert_mint_allowed_at_slot(&mint, 100, 1)
        .await
        .expect_err("first call must refuse the unknown mint");

    // AllowMint lands between the two calls.
    insert_mint_row(&storage, &mint.to_string()).await?;

    // Second call on the same cache: gate must consult the DB again
    // and accept. A cached "not allowlisted" answer here would be a
    // bug — the deposit operator would never recover.
    cache
        .assert_mint_allowed_at_slot(&mint, 100, 1)
        .await
        .expect("second call must consult the DB and accept the newly-allowlisted mint");

    Ok(())
}

// ── Combined: gate + orphan-query agree on the same row ──────────────────────

/// Cross-check of the two changes on a single shared row:
///   • The deposit gate refuses the mint (production: row gets
///     quarantined to ManualReview, no PrivateChannel mint issued).
///   • The same row surfaces in the orphan query (production: row is
///     surfaced in the reconciliation log for triage).
///
/// And the inverse path: once `AllowMint` lands, the gate accepts AND
/// the orphan query clears — both signals flip together.
///
/// This is the integration-level proof that the two halves of the
/// mitigation agree. If a future schema change broke the `mints`
/// lookup for one path but not the other (e.g. gate started reading
/// from a different column than the orphan query), the operator and
/// the reconciliation signal would disagree about which mints are
/// allowed — a more dangerous state than either failure alone, since
/// it would silently degrade either detection (orphans missed) or
/// safety (gate too permissive). This test guarantees they stay in
/// lock-step.
///
/// Note: this verifies the DB-layer agreement only. The actual
/// quarantine-to-ManualReview transition lives in the operator's
/// `process_deposit_funds` loop and requires a running fetcher +
/// sender, which is the validator-level concern covered separately.
#[tokio::test(flavor = "multi_thread")]
async fn gate_and_orphan_query_agree_on_same_row() -> Result<(), Box<dyn std::error::Error>> {
    let (storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    let orphan_id = insert_deposit_row(&storage, "sig_shared", &mint.to_string()).await?;
    let cache = MintCache::new(storage.clone());

    // ── Pre-AllowMint state ──────────────────────────────────────────────
    cache
        .assert_mint_allowed_at_slot(&mint, 1, orphan_id)
        .await
        .expect_err("gate must refuse the mint before AllowMint lands");

    let orphans_before = storage.get_orphan_deposit_ids().await?;
    assert_eq!(
        orphans_before,
        vec![orphan_id],
        "the same row the gate refused must also show up as orphan",
    );

    // ── AllowMint lands ──────────────────────────────────────────────────
    insert_mint_row(&storage, &mint.to_string()).await?;

    // ── Post-AllowMint state ─────────────────────────────────────────────
    cache
        .assert_mint_allowed_at_slot(&mint, 1, orphan_id)
        .await
        .expect("gate must accept the mint once AllowMint lands");

    let orphans_after = storage.get_orphan_deposit_ids().await?;
    assert!(
        orphans_after.is_empty(),
        "the orphan query must clear once AllowMint lands, got {:?}",
        orphans_after,
    );

    Ok(())
}
