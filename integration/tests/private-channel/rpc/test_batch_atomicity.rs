//! Integration tests for batch atomicity:
//! A slot and all its account changes, transactions, and metadata MUST be written
//! as a single DB transaction. Either the whole slot commits or nothing does.
//!
//! Four tests verify this:
//!
//! 1. `test_write_batch_constraint_injection` — adds a CHECK constraint that forces
//!    `write_batch` to fail after accounts are written but before the block row is
//!    inserted, then asserts all prior writes in that batch were rolled back.
//!    This proves `write_batch` uses a real transaction.
//!
//! 2. `test_write_batch_process_kill_simulation` — opens a raw Postgres connection,
//!    manually BEGINs a transaction and writes partial slot data, then uses
//!    `pg_terminate_backend()` to forcibly kill that connection (identical to what
//!    Postgres sees when the OS sends SIGKILL to the PrivateChannel process), and asserts
//!    the partial data is gone.
//!    This proves the underlying mechanism works under real connection-kill conditions.
//!
//! 3. `test_store_block_atomicity` — adds a CHECK constraint that forces
//!    `store_block` to fail after the block row is written but before the
//!    `latest_blockhash` metadata is updated, then asserts the block row was
//!    rolled back with it. This proves the fix to `store_block_postgres` is correct.
//!
//! 4. `a_rolled_back_block_leaves_the_live_slot_ahead`: the live slot is
//!    published by idle ticks outside the block transaction, so it stays where
//!    those ticks left it when a block rolls back, ahead of the last stored
//!    block. That is the ordinary idle state, not a partial write.

use {
    private_channel_core::{
        accounts::{traits::BlockInfo, AccountsDB},
        stages::AccountSettlement,
    },
    solana_sdk::{account::AccountSharedData, hash::Hash, pubkey::Pubkey},
    sqlx::{postgres::PgConnection, Connection},
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
    tokio::sync::OnceCell,
};

struct SharedPostgres {
    host: String,
    port: u16,
    _container: ContainerAsync<Postgres>,
}

static SHARED_POSTGRES: OnceCell<SharedPostgres> = OnceCell::const_new();
static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn sanitize_db_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        out.push_str("test");
    }
    if out.len() > 30 {
        out.truncate(30);
    }
    if out.as_bytes()[0].is_ascii_digit() {
        out.insert(0, 't');
    }

    out
}

async fn shared_postgres() -> &'static SharedPostgres {
    SHARED_POSTGRES
        .get_or_init(|| async {
            let container = Postgres::default()
                .with_db_name("postgres")
                .with_user("postgres")
                .with_password("password")
                .start()
                .await
                .expect("Failed to start shared PostgreSQL test container");

            let host = container
                .get_host()
                .await
                .expect("Failed to resolve shared PostgreSQL host")
                .to_string();
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("Failed to resolve shared PostgreSQL port");

            SharedPostgres {
                host,
                port,
                _container: container,
            }
        })
        .await
}

async fn create_isolated_db_url(test_name: &str) -> String {
    let shared = shared_postgres().await;
    let suffix = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let db_name = format!(
        "private_channel_{}_{}",
        sanitize_db_component(test_name),
        suffix
    );

    let admin_url = format!(
        "postgres://postgres:password@{}:{}/postgres",
        shared.host, shared.port
    );
    let mut admin = PgConnection::connect(&admin_url)
        .await
        .expect("Failed to connect to shared PostgreSQL admin database");

    let create_stmt = format!("CREATE DATABASE \"{}\"", db_name);
    sqlx::query(&create_stmt)
        .execute(&mut admin)
        .await
        .expect("Failed to create isolated integration-test database");

    format!(
        "postgres://postgres:password@{}:{}/{}",
        shared.host, shared.port, db_name
    )
}

fn slot_block_info(slot: u64) -> BlockInfo {
    BlockInfo {
        slot,
        blockhash: Hash::default(),
        previous_blockhash: Hash::default(),
        parent_slot: slot.saturating_sub(1),
        block_height: Some(slot),
        block_time: Some(0),
        transaction_signatures: vec![],
        transaction_recent_blockhashes: vec![],
        transaction_message_hashes: vec![],
    }
}

fn bare_account(lamports: u64) -> AccountSharedData {
    AccountSharedData::new(lamports, 0, &Pubkey::default())
}

/// Test 1: constraint injection
///
/// Forces `write_batch` to fail mid-transaction by temporarily making block inserts
/// for slot 2 violate a CHECK constraint. Asserts that the accounts written earlier
/// in the same transaction were rolled back — no partial slot 2 data remains.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_constraint_injection() {
    let url = create_isolated_db_url("write_batch_constraint_injection").await;

    let mut db = AccountsDB::new(&url, false)
        .await
        .expect("Failed to create AccountsDB");

    // Write slot 1 as a clean baseline.
    let pubkey_slot1 = Pubkey::new_unique();
    db.write_batch(
        &[(
            pubkey_slot1,
            AccountSettlement {
                account: bare_account(1_000_000),
                deleted: false,
            },
        )],
        vec![],
        Some(slot_block_info(1)),
    )
    .await
    .expect("slot 1 write_batch must succeed");

    assert_eq!(db.get_latest_slot().await.unwrap(), Some(1));

    // Inject fault: any INSERT into blocks with slot = 2 will fail.
    // This simulates a mid-transaction failure after accounts have been written
    // but before the block row (which comes later in write_batch) is inserted.
    let pool = match &db {
        AccountsDB::Postgres(pg) => Arc::clone(&pg.pool),
        _ => panic!("Expected Postgres backend"),
    };
    sqlx::query("ALTER TABLE blocks ADD CONSTRAINT test_no_slot_2 CHECK (slot <> 2)")
        .execute(&*pool)
        .await
        .expect("Failed to add test constraint");

    // Attempt write_batch for slot 2 — the block insert will hit the constraint,
    // the error propagates out of the transaction, sqlx rolls everything back.
    let pubkey_slot2 = Pubkey::new_unique();
    let result = db
        .write_batch(
            &[(
                pubkey_slot2,
                AccountSettlement {
                    account: bare_account(2_000_000),
                    deleted: false,
                },
            )],
            vec![],
            Some(slot_block_info(2)),
        )
        .await;

    assert!(
        result.is_err(),
        "write_batch must fail when the block insert violates the constraint"
    );

    // latest_slot is derived from MAX(slot) in blocks — slot 2 block was rolled back.
    assert_eq!(
        db.get_latest_slot().await.unwrap(),
        Some(1),
        "latest_slot must still be 1; slot 2 block was never committed"
    );

    // The block row itself must not exist.
    assert!(
        db.get_block(2).await.unwrap().is_none(),
        "slot 2 block must not exist after the rolled-back write_batch"
    );

    // Both chain counters ride the same transaction as the block row.
    assert_eq!(
        db.get_current_slot().await.unwrap(),
        Some(1),
        "the live slot must not move for a block that never committed"
    );
    assert_eq!(
        db.get_block_height().await.unwrap(),
        Some(1),
        "the height must not count a block that never committed"
    );

    // pubkey_slot2's account was written to the accounts table BEFORE the block
    // insert failed. It must have been rolled back with the rest of the transaction.
    let accounts = db.get_accounts(&[pubkey_slot2]).await.unwrap();
    assert!(
        accounts[0].is_none(),
        "pubkey_slot2 account must not exist — it was rolled back with the transaction"
    );

    // Slot 1 data must be completely intact.
    let accounts = db.get_accounts(&[pubkey_slot1]).await.unwrap();
    assert!(
        accounts[0].is_some(),
        "pubkey_slot1 (slot 1 baseline) must still exist"
    );

    // Remove the constraint and confirm slot 2 can now be written cleanly,
    // proving the DB was left in a fully usable state.
    sqlx::query("ALTER TABLE blocks DROP CONSTRAINT test_no_slot_2")
        .execute(&*pool)
        .await
        .expect("Failed to drop test constraint");

    db.write_batch(
        &[(
            pubkey_slot2,
            AccountSettlement {
                account: bare_account(2_000_000),
                deleted: false,
            },
        )],
        vec![],
        Some(slot_block_info(2)),
    )
    .await
    .expect("slot 2 write_batch must succeed after constraint is removed");

    assert_eq!(
        db.get_latest_slot().await.unwrap(),
        Some(2),
        "DB must be at slot 2 after the clean write"
    );
    assert_eq!(
        db.get_current_slot().await.unwrap(),
        Some(2),
        "the live slot must follow the committed block"
    );
}

/// Test 4: the live slot is published by idle ticks, outside the block
/// transaction, so a rolled-back block leaves it ahead of the last stored block.
/// That is the ordinary idle state: nothing requires a block to back a slot.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_block_leaves_the_live_slot_ahead() {
    let url = create_isolated_db_url("rolled_back_block_live_slot").await;

    let mut db = AccountsDB::new(&url, false)
        .await
        .expect("Failed to create AccountsDB");
    let pool = match &db {
        AccountsDB::Postgres(pg) => Arc::clone(&pg.pool),
        _ => panic!("Expected Postgres backend"),
    };
    let postgres_db = match &db {
        AccountsDB::Postgres(pg) => pg.clone(),
        _ => panic!("Expected Postgres backend"),
    };

    db.write_batch(&[], vec![], Some(slot_block_info(1)))
        .await
        .expect("slot 1 write_batch must succeed");

    // Four idle ticks past the last block, which is what the settler publishes
    // while nothing is landing.
    for slot in 2..=5u64 {
        private_channel_core::accounts::current_slot::set_current_slot(&postgres_db, slot)
            .await
            .expect("publishing an idle slot must succeed");
    }

    sqlx::query("ALTER TABLE blocks ADD CONSTRAINT test_no_slot_6 CHECK (slot <> 6)")
        .execute(&*pool)
        .await
        .expect("Failed to add test constraint");

    let result = db.write_batch(&[], vec![], Some(slot_block_info(6))).await;
    assert!(result.is_err(), "the block insert must hit the constraint");

    assert_eq!(
        db.get_current_slot().await.unwrap(),
        Some(5),
        "the rolled-back block must not move the live slot"
    );
    assert_eq!(
        db.get_latest_slot().await.unwrap(),
        Some(1),
        "the block tip must still name the last committed block"
    );
    assert_eq!(
        db.get_block_height().await.unwrap(),
        Some(1),
        "the height must still count only committed blocks"
    );
    assert!(
        db.get_block(5).await.unwrap().is_none(),
        "a slot the ticks passed through carries no block"
    );

    sqlx::query("ALTER TABLE blocks DROP CONSTRAINT test_no_slot_6")
        .execute(&*pool)
        .await
        .expect("Failed to drop test constraint");

    db.write_batch(&[], vec![], Some(slot_block_info(6)))
        .await
        .expect("slot 6 write_batch must succeed after the constraint is removed");
    assert_eq!(db.get_current_slot().await.unwrap(), Some(6));
    assert_eq!(db.get_latest_slot().await.unwrap(), Some(6));
}

/// Test 2: process kill simulation via `pg_terminate_backend`
///
/// Opens a raw Postgres connection (simulating the PrivateChannel process's DB connection),
/// manually BEGINs a transaction, and writes partial slot data directly — exactly
/// the state the DB is in when a settle is mid-flight. Then calls
/// `pg_terminate_backend(pid)` to forcibly kill that connection, which is what
/// Postgres sees when the OS sends SIGKILL to the application process. Asserts that
/// Postgres rolled back the in-flight transaction and no partial data remains.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_process_kill_simulation() {
    let url = create_isolated_db_url("write_batch_process_kill_simulation").await;

    // Initialize the schema via AccountsDB (creates all tables).
    let db = AccountsDB::new(&url, false)
        .await
        .expect("Failed to create AccountsDB");

    // Write slot 1 as a clean baseline using write_batch.
    let pubkey_slot1 = Pubkey::new_unique();
    {
        let mut db_write = db.clone();
        db_write
            .write_batch(
                &[(
                    pubkey_slot1,
                    AccountSettlement {
                        account: bare_account(1_000_000),
                        deleted: false,
                    },
                )],
                vec![],
                Some(slot_block_info(1)),
            )
            .await
            .expect("slot 1 write_batch must succeed");
    }

    assert_eq!(db.get_latest_slot().await.unwrap(), Some(1));

    // Open a raw connection — this represents the PrivateChannel process's DB connection
    // that is in the middle of a write_batch for slot 2.
    let mut victim = PgConnection::connect(&url)
        .await
        .expect("Failed to open victim connection");

    // Manually begin the transaction (as write_batch would via pool.begin()).
    sqlx::query("BEGIN")
        .execute(&mut victim)
        .await
        .expect("BEGIN must succeed");

    // Write partial slot 2 data: an account and a block row, but do NOT commit.
    // This mirrors the mid-flight state of write_batch when the process is killed.
    // The bytes content doesn't matter here — we're only asserting these rows
    // are absent after the connection is killed, not reading their values back.
    let pubkey_slot2 = Pubkey::new_unique();
    let dummy_bytes = [0u8; 32];

    sqlx::query("INSERT INTO accounts (pubkey, data) VALUES ($1, $2)")
        .bind(&pubkey_slot2.to_bytes()[..])
        .bind(&dummy_bytes[..])
        .execute(&mut victim)
        .await
        .expect("accounts INSERT must succeed inside the open transaction");

    sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
        .bind(2i64)
        .bind(&dummy_bytes[..])
        .execute(&mut victim)
        .await
        .expect("blocks INSERT must succeed inside the open transaction");

    // Get the victim connection's backend PID so we can kill it from outside.
    let victim_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut victim)
        .await
        .expect("Failed to get pg_backend_pid");

    // Open a second connection (the "executioner") to terminate the victim.
    // This is equivalent to the OS sending SIGKILL to the PrivateChannel process:
    // Postgres detects the backend termination and rolls back the open transaction.
    let mut executioner = PgConnection::connect(&url)
        .await
        .expect("Failed to open executioner connection");

    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(victim_pid)
        .execute(&mut executioner)
        .await
        .expect("pg_terminate_backend must succeed");

    // The victim connection is now dead server-side. Drop it.
    drop(victim);

    // Give Postgres a moment to process the termination and roll back.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify: Postgres rolled back the in-flight transaction, leaving no partial data.

    // latest_slot is MAX(slot) from blocks — slot 2 block was rolled back.
    assert_eq!(
        db.get_latest_slot().await.unwrap(),
        Some(1),
        "latest_slot must still be 1 after the simulated process kill"
    );

    // The block row must not exist.
    assert!(
        db.get_block(2).await.unwrap().is_none(),
        "slot 2 block must not exist — Postgres rolled back on connection kill"
    );

    // The account written inside the killed transaction must not exist.
    let accounts = db.get_accounts(&[pubkey_slot2]).await.unwrap();
    assert!(
        accounts[0].is_none(),
        "pubkey_slot2 must not exist — rolled back with the killed transaction"
    );

    // Slot 1 baseline must be fully intact.
    let accounts = db.get_accounts(&[pubkey_slot1]).await.unwrap();
    assert!(
        accounts[0].is_some(),
        "pubkey_slot1 (slot 1 baseline) must still exist"
    );
}

/// Test 3: `store_block_postgres` atomicity
///
/// `store_block` performs two writes inside a transaction: a block row insert and
/// a `latest_blockhash` metadata update. This test injects a CHECK constraint that
/// blocks the metadata update, forcing the transaction to fail after the block row
/// has been written. Asserts that the block row is rolled back with it — proving
/// the two writes are atomic and cannot be split by a crash.
#[tokio::test(flavor = "multi_thread")]
async fn test_store_block_atomicity() {
    let url = create_isolated_db_url("store_block_atomicity").await;

    let mut db = AccountsDB::new(&url, false)
        .await
        .expect("Failed to create AccountsDB");

    let pool = match &db {
        AccountsDB::Postgres(pg) => Arc::clone(&pg.pool),
        _ => panic!("Expected Postgres backend"),
    };

    // Inject fault: block any insert of `latest_blockhash` into the metadata table.
    // store_block writes the block row first, then updates latest_blockhash.
    // With this constraint the second write fails, proving both writes roll back.
    sqlx::query(
        "ALTER TABLE metadata ADD CONSTRAINT test_no_blockhash_key CHECK (key != 'latest_blockhash')",
    )
    .execute(&*pool)
    .await
    .expect("Failed to add test constraint");

    // store_block for slot 1 must fail — the latest_blockhash update is blocked.
    let result = db.store_block(slot_block_info(1)).await;
    assert!(
        result.is_err(),
        "store_block must fail when the latest_blockhash update is blocked"
    );

    // The block row that was written before the failure must have been rolled back.
    assert!(
        db.get_block(1).await.unwrap().is_none(),
        "slot 1 block must not exist — the blocks insert was rolled back with the failed metadata update"
    );

    // Drop the constraint and confirm store_block now succeeds end-to-end.
    sqlx::query("ALTER TABLE metadata DROP CONSTRAINT test_no_blockhash_key")
        .execute(&*pool)
        .await
        .expect("Failed to drop test constraint");

    db.store_block(slot_block_info(1))
        .await
        .expect("store_block must succeed after constraint is removed");

    assert!(
        db.get_block(1).await.unwrap().is_some(),
        "slot 1 block must exist after the clean store_block"
    );
}
