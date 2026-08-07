use crate::accounts::traits::BlockInfo;
use solana_sdk::{
    hash::Hash,
    instruction::CompiledInstruction,
    message::{Message, MessageHeader},
    signature::{Keypair, Signer},
    transaction::{SanitizedTransaction, Transaction},
};
use solana_system_interface::instruction as system_instruction;
use std::collections::HashSet;

/// Create a SanitizedTransaction transferring SOL between two keypairs.
pub fn create_test_sanitized_transaction(
    from: &Keypair,
    to: &solana_sdk::pubkey::Pubkey,
    amount: u64,
) -> SanitizedTransaction {
    let instruction = system_instruction::transfer(&from.pubkey(), to, amount);
    let message = Message::new(&[instruction], Some(&from.pubkey()));
    let transaction = Transaction::new(&[from], message, Hash::default());
    SanitizedTransaction::try_from_legacy_transaction(transaction, &HashSet::new())
        .expect("failed to create SanitizedTransaction from test legacy transaction")
}

/// Legacy transaction with a repeated account key, built field by field because the SDK dedupes.
pub fn duplicate_account_keys_transaction(payer: &Keypair, recent_blockhash: Hash) -> Transaction {
    // Both copies land in the readonly tail, which is the cheapest shape to submit.
    duplicate_keys_transaction(
        payer,
        recent_blockhash,
        vec![payer.pubkey(), spl_memo::id(), spl_memo::id()],
        2,
        1,
    )
}

/// Same idea, but the repeated key is writable, which is the shape that stresses the write locks.
pub fn duplicate_writable_account_keys_transaction(
    payer: &Keypair,
    recent_blockhash: Hash,
) -> Transaction {
    // Only the trailing program stays readonly, so the payer is locked writable twice.
    duplicate_keys_transaction(
        payer,
        recent_blockhash,
        vec![payer.pubkey(), payer.pubkey(), spl_memo::id()],
        1,
        2,
    )
}

fn duplicate_keys_transaction(
    payer: &Keypair,
    recent_blockhash: Hash,
    account_keys: Vec<solana_sdk::pubkey::Pubkey>,
    num_readonly_unsigned_accounts: u8,
    program_id_index: u8,
) -> Transaction {
    let message = Message {
        header: MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts,
        },
        account_keys,
        recent_blockhash,
        instructions: vec![CompiledInstruction {
            program_id_index,
            accounts: vec![],
            data: b"dup".to_vec(),
        }],
    };
    let mut transaction = Transaction::new_unsigned(message);
    transaction.sign(&[payer], recent_blockhash);
    transaction
}

/// Create a BlockInfo with sensible defaults for a given slot.
pub fn create_test_block_info(slot: u64, blockhash: Hash) -> BlockInfo {
    BlockInfo {
        slot,
        blockhash,
        previous_blockhash: Hash::default(),
        parent_slot: slot.saturating_sub(1),
        block_height: Some(slot),
        block_time: Some(1_700_000_000 + slot as i64),
        transaction_signatures: vec![],
        transaction_recent_blockhashes: vec![],
    }
}

/// Spin up a throwaway Postgres container and return a write-mode AccountsDB.
/// The container handle is returned so the caller keeps it alive for the test duration.
#[cfg(test)]
pub(crate) async fn start_test_postgres() -> (
    crate::accounts::AccountsDB,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_db_name("test_db")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{}:{}/test_db", host, port);
    let db = crate::accounts::AccountsDB::new(&url, false).await.unwrap();
    (db, container)
}

/// Spin up a throwaway Postgres container and return a `PostgresAccountsDB` directly.
/// Use this when testing `PostgresAccountsDB`-specific methods (e.g. `TransactionProcessingCallback`).
#[cfg(test)]
pub(crate) async fn start_test_postgres_raw() -> (
    crate::accounts::PostgresAccountsDB,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_db_name("pg_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:password@{}:{}/pg_test", host, port);
    let db = crate::accounts::PostgresAccountsDB::new(&url, false)
        .await
        .unwrap();
    (db, container)
}

/// Synchronously insert `address_signatures` rows that `write_batch` would
/// otherwise emit asynchronously through the `address_index_writer` worker.
///
/// In production `write_batch` returns the (address, slot, signature) tuples
/// and the settler sends them on the bounded mpsc channel — the writer task
/// flushes them to `address_signatures` shortly after the atomic commit. In
/// tests that don't start the writer, callers must perform this flush inline
/// before any `get_signatures_for_address` read, otherwise the read sees an
/// empty index for rows that were "written" by `write_batch`.
#[cfg(test)]
pub(crate) async fn flush_address_signatures_sync(
    db: &crate::accounts::AccountsDB,
    rows: &[crate::accounts::write_batch::AddressSignatureRow],
) {
    if rows.is_empty() {
        return;
    }
    if let crate::accounts::AccountsDB::Postgres(pg) = db {
        let addresses: Vec<&[u8]> = rows.iter().map(|r| r.address.as_slice()).collect();
        let slots: Vec<i64> = rows.iter().map(|r| r.slot).collect();
        let signatures: Vec<&[u8]> = rows.iter().map(|r| r.signature.as_slice()).collect();
        sqlx::query(
            "INSERT INTO address_signatures (address, slot, signature)
             SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::bytea[])
             ON CONFLICT DO NOTHING",
        )
        .bind(&addresses)
        .bind(&slots)
        .bind(&signatures)
        .execute(pg.pool.as_ref())
        .await
        .expect("test helper: flush_address_signatures_sync failed");
    }
}

/// Return the connection URL for an already-running testcontainers Postgres instance.
/// Useful when a test needs the raw URL (e.g. to pass to `run_node` or a worker).
#[cfg(test)]
pub(crate) async fn postgres_container_url(
    container: &testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    db_name: &str,
) -> String {
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    format!("postgres://postgres:password@{}:{}/{}", host, port, db_name)
}

/// Start a Postgres container and create two `PostgresAccountsDB` instances
/// pointing to the same container (second one via .new() for idempotency testing).
/// Useful for tests that verify database operations are idempotent.
#[cfg(test)]
pub(crate) async fn start_test_postgres_with_new_instance() -> (
    crate::accounts::PostgresAccountsDB,
    crate::accounts::PostgresAccountsDB,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    let (db, container) = start_test_postgres_raw().await;
    let url = postgres_container_url(&container, "pg_test").await;
    let second = crate::accounts::PostgresAccountsDB::new(&url, false)
        .await
        .unwrap();
    (db, second, container)
}

/// Spin up a throwaway Redis container and return a `RedisAccountsDB` directly.
/// Use this when testing `RedisAccountsDB`-specific methods or warm_redis_cache.
#[cfg(test)]
pub(crate) async fn start_test_redis() -> (
    crate::accounts::RedisAccountsDB,
    testcontainers::ContainerAsync<testcontainers_modules::redis::Redis>,
) {
    use testcontainers::{runners::AsyncRunner, ImageExt};

    let container = testcontainers_modules::redis::Redis::default()
        .with_tag("7.0")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://{}:{}", host, port);
    let db = crate::accounts::RedisAccountsDB::new(&url).await.unwrap();
    (db, container)
}

/// A Postgres-backed AccountsDB whose pool points at a closed port with a short
/// acquire timeout, so any query fails fast. Used to exercise the transient
/// (fatal) load path without a 30s wait.
#[cfg(test)]
pub(crate) fn dead_postgres_db() -> crate::accounts::AccountsDB {
    use crate::accounts::{AccountsDB, PostgresAccountsDB};
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Arc;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://user:pass@127.0.0.1:1/db")
        .expect("connect_lazy should not fail");
    AccountsDB::Postgres(PostgresAccountsDB {
        pool: Arc::new(pool),
        read_only: true,
    })
}

/// A Redis-backed AccountsDB whose server has been stopped. Built with a
/// low-retry connection manager so a command fails fast rather than spending
/// seconds on reconnect backoff.
#[cfg(test)]
pub(crate) async fn dead_redis_db() -> crate::accounts::RedisAccountsDB {
    use redis::aio::{ConnectionManager, ConnectionManagerConfig};

    let (_raw, container) = start_test_redis().await;
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://{}:{}", host, port);

    let client = redis::Client::open(url).unwrap();
    let config = ConnectionManagerConfig::new()
        .set_number_of_retries(1)
        .set_factor(1)
        .set_max_delay(5);
    let connection = ConnectionManager::new_with_config(client, config)
        .await
        .unwrap();
    let db = crate::accounts::RedisAccountsDB { connection };

    // Stop the server; subsequent commands fail after a single short reconnect.
    drop(container);
    db
}

/// Create a BOB with empty state and a dummy (non-connecting) Postgres pool.
/// The pool uses a bogus URL — any accidental DB call will fail with a
/// connection timeout. Only for unit tests that stay in-memory.
#[cfg(test)]
pub(crate) fn create_test_bob() -> (
    crate::accounts::bob::BOB,
    tokio::sync::mpsc::UnboundedSender<
        Vec<(solana_sdk::pubkey::Pubkey, crate::stages::AccountSettlement)>,
    >,
) {
    use crate::accounts::{AccountsDB, PostgresAccountsDB};
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Arc;

    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://test@localhost:1/test")
        .expect("connect_lazy should not fail");
    let db = AccountsDB::Postgres(PostgresAccountsDB {
        pool: Arc::new(pool),
        read_only: true,
    });
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let bob = crate::accounts::bob::BOB::new_test(rx, db);
    (bob, tx)
}
