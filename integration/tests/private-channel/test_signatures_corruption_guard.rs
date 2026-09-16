//! Data-corruption guards in `get_signatures_for_address`.
//!
//! Three defensive arms fire when a row in `address_signatures` /
//! `transactions` is inconsistent with the indexer's invariants. In
//! production every row is written through the settle-stage helper
//! which validates inputs, so these arms are unreachable from a clean
//! write path. This test fires each one by direct `sqlx::query(...)`
//! inserts against a testcontainers-provided Postgres, then calls
//! `get_signatures_for_address` and asserts the expected error
//! surfaces.
//!
//! Three arms, three sub-tests:
//!   1. `sig_bytes` that's not a valid 64-byte ed25519 signature
//!      (`Signature::try_from` guard).
//!   2. `transactions.data` missing (LEFT JOIN NULL).
//!   3. `transactions.data` set to bytes that don't deserialize as
//!      `StoredTransaction`.
//!
//! The test uses `PostgresAccountsDB::new(... read_only=false)` to
//! create the schema, then manipulates the tables directly. It does
//! not start a full PrivateChannel node — only the accounts layer is needed.

use {
    private_channel_core::accounts::{
        get_signatures_for_address::get_signatures_for_address, postgres::PostgresAccountsDB,
        traits::AccountsDB,
    },
    solana_sdk::pubkey::Pubkey,
    sqlx::Executor,
    testcontainers::{runners::AsyncRunner, ImageExt},
    testcontainers_modules::postgres::Postgres,
};

async fn start_postgres() -> (
    testcontainers::ContainerAsync<Postgres>,
    String,
    PostgresAccountsDB,
) {
    let pg = Postgres::default()
        .with_db_name("private_channel_accountsdb")
        .with_user("postgres")
        .with_password("password")
        .with_tag("16")
        .start()
        .await
        .expect("start postgres");
    let host = pg.get_host().await.expect("pg host");
    let port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/private_channel_accountsdb");
    let accounts = PostgresAccountsDB::new(&url, false)
        .await
        .expect("PostgresAccountsDB::new");
    (pg, url, accounts)
}

/// Arm 1: malformed `signature` bytes in `address_signatures`.
/// `Signature::try_from` returns `Err`, surfacing as
/// "Failed to deserialize signature from address_signatures".
#[tokio::test(flavor = "multi_thread")]
async fn malformed_signature_bytes_surface_as_deserialize_error() {
    let (_pg, _url, accounts) = start_postgres().await;
    let addr = Pubkey::new_unique();

    // Insert an `address_signatures` row with a 3-byte signature (needs to
    // be exactly 64 bytes). This triggers the `Signature::try_from` guard.
    let bogus_sig: Vec<u8> = vec![0x11, 0x22, 0x33];
    accounts
        .pool
        .execute(
            sqlx::query(
                "INSERT INTO address_signatures (address, slot, signature) VALUES ($1, $2, $3)",
            )
            .bind(addr.to_bytes().as_slice())
            .bind(1i64)
            .bind(bogus_sig.as_slice()),
        )
        .await
        .expect("insert malformed sig row");

    let db = AccountsDB::Postgres(accounts.clone());
    let err = get_signatures_for_address(&db, &addr, 10, None, None, None)
        .await
        .expect_err("malformed signature bytes must surface as Err");
    let msg = format!("{err}");
    assert!(
        msg.contains("Failed to deserialize signature"),
        "error must point at signature deserialize guard: {msg}"
    );
}

/// Arm 2: missing `transactions.data` — the LEFT JOIN returns NULL
/// for the blob column. Surfaces as "Transaction data missing".
#[tokio::test(flavor = "multi_thread")]
async fn missing_transaction_row_surfaces_as_corruption_error() {
    let (_pg, _url, accounts) = start_postgres().await;
    let addr = Pubkey::new_unique();

    // Build a well-formed 64-byte signature so Arm 1 doesn't fire first.
    let good_sig: Vec<u8> = (0..64u8).collect();
    accounts
        .pool
        .execute(
            sqlx::query(
                "INSERT INTO address_signatures (address, slot, signature) VALUES ($1, $2, $3)",
            )
            .bind(addr.to_bytes().as_slice())
            .bind(2i64)
            .bind(good_sig.as_slice()),
        )
        .await
        .expect("insert orphaned address_signatures row");
    // Intentionally skip inserting the matching `transactions` row so the
    // LEFT JOIN surfaces NULL data.

    let db = AccountsDB::Postgres(accounts.clone());
    let err = get_signatures_for_address(&db, &addr, 10, None, None, None)
        .await
        .expect_err("missing transaction row must surface as Err");
    let msg = format!("{err}");
    assert!(
        msg.contains("Transaction data missing"),
        "error must point at the missing-transactions guard: {msg}"
    );
}

/// Arm 3: `transactions.data` present but not a valid bincode-encoded
/// `StoredTransaction`. Surfaces as "Failed to deserialize transaction".
#[tokio::test(flavor = "multi_thread")]
async fn garbage_transaction_blob_surfaces_as_bincode_error() {
    let (_pg, _url, accounts) = start_postgres().await;
    let addr = Pubkey::new_unique();

    let good_sig: Vec<u8> = (1..=64u8).collect();
    accounts
        .pool
        .execute(
            sqlx::query(
                "INSERT INTO address_signatures (address, slot, signature) VALUES ($1, $2, $3)",
            )
            .bind(addr.to_bytes().as_slice())
            .bind(3i64)
            .bind(good_sig.as_slice()),
        )
        .await
        .expect("insert address_signatures row");

    let garbage_blob: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11];
    accounts
        .pool
        .execute(
            sqlx::query("INSERT INTO transactions (signature, data) VALUES ($1, $2)")
                .bind(good_sig.as_slice())
                .bind(garbage_blob.as_slice()),
        )
        .await
        .expect("insert garbage transactions row");

    let db = AccountsDB::Postgres(accounts.clone());
    let err = get_signatures_for_address(&db, &addr, 10, None, None, None)
        .await
        .expect_err("garbage transaction blob must surface as Err");
    let msg = format!("{err}");
    assert!(
        msg.contains("Failed to deserialize transaction"),
        "error must point at the bincode-deserialize guard: {msg}"
    );
}
