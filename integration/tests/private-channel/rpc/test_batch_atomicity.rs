//! Integration tests for how a slot reaches the database:
//! it must be written as a single DB transaction, and it must be written by one
//! writer. Either the whole slot commits or nothing does.
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
//! 4. `two_writers_racing_one_slot_leave_a_single_ledger` forces two independent
//!    writers to contend for the same slot and asserts exactly one commits, with
//!    the loser leaving no accounts, block or blockhash behind.
//!
//! 5. `a_writer_whose_slot_was_truncated_away_is_still_rejected` deletes old
//!    blocks the way retention truncation does, then asserts a writer holding one
//!    of those slots still cannot commit it back over current state.

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

/// Same as `slot_block_info`, with a caller-chosen blockhash so two writers
/// racing one slot can be told apart by whichever block ends up stored.
fn slot_block_info_with_hash(slot: u64, blockhash: Hash) -> BlockInfo {
    BlockInfo {
        blockhash,
        ..slot_block_info(slot)
    }
}

/// Number of backends on this database currently blocked on a lock.
async fn backends_waiting_on_a_lock(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock'",
    )
    .fetch_one(conn)
    .await
    .expect("failed to read pg_stat_activity")
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

/// Test 4: two writers racing one slot
///
/// Two write-capable nodes at the same tip both produce slot 2. Exactly one may
/// win: a ledger holding one writer's block alongside the other's accounts was
/// never produced by a single serial execution.
///
/// The overlap is forced, not timed. A third connection holds an open transaction
/// that already inserted slot 2, so both writers clear the extend-the-ledger guard
/// and park on the slot primary key; rolling it back releases them into contention.
#[tokio::test(flavor = "multi_thread")]
async fn two_writers_racing_one_slot_leave_a_single_ledger() {
    let url = create_isolated_db_url("two_writers_racing_one_slot").await;

    let mut db = AccountsDB::new(&url, false)
        .await
        .expect("Failed to create AccountsDB");
    db.write_batch(&[], vec![], Some(slot_block_info(1)))
        .await
        .expect("slot 1 write_batch must succeed");

    // Blocks slot 2 without committing, so both writers stall on the same row.
    let mut blocker = PgConnection::connect(&url)
        .await
        .expect("Failed to open blocker connection");
    sqlx::query("BEGIN")
        .execute(&mut blocker)
        .await
        .expect("BEGIN must succeed");
    sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
        .bind(2i64)
        .bind(&[0u8; 32][..])
        .execute(&mut blocker)
        .await
        .expect("placeholder blocks INSERT must succeed");

    let account_a = Pubkey::new_unique();
    let account_b = Pubkey::new_unique();
    let blockhash_a = Hash::new_unique();
    let blockhash_b = Hash::new_unique();

    let mut writer_a = AccountsDB::new(&url, false).await.expect("writer A");
    let mut writer_b = AccountsDB::new(&url, false).await.expect("writer B");

    let task_a = tokio::spawn(async move {
        writer_a
            .write_batch(
                &[(
                    account_a,
                    AccountSettlement {
                        account: bare_account(1_000_000),
                        deleted: false,
                    },
                )],
                vec![],
                Some(slot_block_info_with_hash(2, blockhash_a)),
            )
            .await
            .map(|_| ())
    });
    let task_b = tokio::spawn(async move {
        writer_b
            .write_batch(
                &[(
                    account_b,
                    AccountSettlement {
                        account: bare_account(2_000_000),
                        deleted: false,
                    },
                )],
                vec![],
                Some(slot_block_info_with_hash(2, blockhash_b)),
            )
            .await
            .map(|_| ())
    });

    // Wait for both to reach the contended row rather than sleeping a guessed amount.
    let mut observer = PgConnection::connect(&url)
        .await
        .expect("Failed to open observer connection");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while backends_waiting_on_a_lock(&mut observer).await < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "both writers should have parked on the contended slot"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    sqlx::query("ROLLBACK")
        .execute(&mut blocker)
        .await
        .expect("ROLLBACK must succeed");

    let result_a = task_a.await.expect("writer A task panicked");
    let result_b = task_b.await.expect("writer B task panicked");

    assert_eq!(
        result_a.is_ok() as u8 + result_b.is_ok() as u8,
        1,
        "exactly one writer may commit slot 2, got a={result_a:?} b={result_b:?}"
    );

    // Everything stored for slot 2 must come from the writer that won.
    let (winner_account, winner_blockhash, loser_account) = if result_a.is_ok() {
        (account_a, blockhash_a, account_b)
    } else {
        (account_b, blockhash_b, account_a)
    };

    assert_eq!(db.get_latest_slot().await.unwrap(), Some(2));
    assert_eq!(
        db.get_block(2).await.unwrap().unwrap().blockhash,
        winner_blockhash
    );
    assert_eq!(db.get_latest_blockhash().await.unwrap(), winner_blockhash);
    assert!(
        db.get_accounts(&[winner_account]).await.unwrap()[0].is_some(),
        "the winning writer's account must be stored"
    );
    assert!(
        db.get_accounts(&[loser_account]).await.unwrap()[0].is_none(),
        "the losing writer's account must have rolled back with its batch"
    );
}

/// Test 5: a stale writer whose slot no longer exists
///
/// Retention truncation deletes old blocks, so a writer far enough behind finds
/// its target slot free. A plain uniqueness check would let that batch commit and
/// overwrite current accounts with ancient values; only comparing against the tip
/// catches it.
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_whose_slot_was_truncated_away_is_still_rejected() {
    let url = create_isolated_db_url("truncated_away_slot").await;
    let mut db = AccountsDB::new(&url, false)
        .await
        .expect("Failed to create AccountsDB");

    for slot in 1..=5 {
        db.write_batch(&[], vec![], Some(slot_block_info(slot)))
            .await
            .unwrap_or_else(|e| panic!("slot {slot} must commit: {e}"));
    }

    // What truncation leaves behind: the old blocks are gone, the tip is not.
    let mut conn = PgConnection::connect(&url)
        .await
        .expect("Failed to open the truncation connection");
    sqlx::query("DELETE FROM blocks WHERE slot <= 3")
        .execute(&mut conn)
        .await
        .expect("the truncation must succeed");

    let stale_account = Pubkey::new_unique();
    let result = db
        .write_batch(
            &[(
                stale_account,
                AccountSettlement {
                    account: bare_account(9_000_000),
                    deleted: false,
                },
            )],
            vec![],
            Some(slot_block_info(2)),
        )
        .await;

    assert!(
        result.is_err(),
        "a writer holding a truncated slot must still be rejected"
    );
    assert_eq!(
        db.get_latest_slot().await.unwrap(),
        Some(5),
        "the tip must not move"
    );
    assert!(
        db.get_block(2).await.unwrap().is_none(),
        "a truncated slot must not be resurrected"
    );
    assert!(
        db.get_accounts(&[stale_account]).await.unwrap()[0].is_none(),
        "the stale writer's account must have rolled back with its batch"
    );
}
