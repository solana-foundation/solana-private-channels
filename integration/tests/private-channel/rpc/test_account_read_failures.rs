//! Over-the-wire coverage for the account-read error split on `getAccountInfo`.
//!
//! The handler's own unit tests pin the RpcResult it returns. These start a real
//! read-only RPC service over HTTP so the JSON-RPC serialization is exercised
//! too, proving a client sees an error object rather than a null account when
//! the store cannot answer, and still sees a null for an account that is simply
//! not there.

use {
    private_channel_core::{
        accounts::AccountsDB,
        health::HeartbeatRegistry,
        rpc::{
            server::{start_rpc_service, RpcServiceConfig},
            ReadDeps,
        },
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{account::AccountSharedData, commitment_config::CommitmentConfig, pubkey::Pubkey},
    std::{
        collections::LinkedList,
        sync::{Arc, RwLock},
    },
    testcontainers::{runners::AsyncRunner, ContainerAsync},
    testcontainers_modules::postgres::Postgres,
    tokio_util::sync::CancellationToken,
};

/// Ask the OS for a free port, then release it so the RPC service can claim it.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// A throwaway Postgres plus a read-only RPC service in front of it. The
/// container and token are returned so the caller keeps them alive.
async fn start_read_only_rpc() -> (
    AccountsDB,
    RpcClient,
    ContainerAsync<Postgres>,
    CancellationToken,
) {
    let container = Postgres::default()
        .with_db_name("acct_read_failures")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!(
        "postgres://postgres:password@{}:{}/acct_read_failures",
        host, port
    );
    let db = AccountsDB::new(&url, false).await.unwrap();

    let rpc_port = free_port().await;
    let shutdown_token = CancellationToken::new();
    start_rpc_service(RpcServiceConfig {
        port: rpc_port,
        max_connections: 8,
        read_deps: Some(ReadDeps {
            accounts_db: db.clone(),
            admin_keys: vec![],
            live_blockhashes: Arc::new(RwLock::new(LinkedList::new())),
            max_blockhashes: 150,
        }),
        write_deps: None,
        heartbeats: HeartbeatRegistry::new(),
        shutdown_token: shutdown_token.clone(),
    })
    .await
    .expect("read-only RPC service must start");

    let client = RpcClient::new_with_commitment(
        format!("http://127.0.0.1:{}", rpc_port),
        CommitmentConfig::processed(),
    );
    (db, client, container, shutdown_token)
}

/// An unreadable account store must reach the client as a JSON-RPC error. Before
/// this split it arrived as a null account, which reads as "no such account".
#[tokio::test(flavor = "multi_thread")]
async fn get_account_info_unreadable_store_is_an_error_over_the_wire() {
    let (mut db, client, _pg, shutdown) = start_read_only_rpc().await;

    let pubkey = Pubkey::new_unique();
    db.set_account(
        pubkey,
        AccountSharedData::new(1_000_000, 0, &Pubkey::new_unique()),
    )
    .await;

    // The account is readable first, so the failure below cannot be confused
    // with the account never having been stored.
    let before = client.get_account(&pubkey).await;
    assert!(
        before.is_ok(),
        "seeded account must read back: {:?}",
        before
    );

    let AccountsDB::Postgres(pg) = &db else {
        panic!("test harness is Postgres-backed");
    };
    sqlx::query("DROP TABLE accounts")
        .execute(pg.pool.as_ref())
        .await
        .unwrap();

    let after = client
        .get_account_with_commitment(&pubkey, CommitmentConfig::processed())
        .await;
    assert!(
        after.is_err(),
        "an unreadable store must be an error, not a null account: {:?}",
        after
    );

    shutdown.cancel();
}

/// The error path must not swallow genuine absence: a pubkey that was never
/// stored still comes back as a null account with a successful response.
#[tokio::test(flavor = "multi_thread")]
async fn get_account_info_genuine_miss_is_null_over_the_wire() {
    let (_db, client, _pg, shutdown) = start_read_only_rpc().await;

    let response = client
        .get_account_with_commitment(&Pubkey::new_unique(), CommitmentConfig::processed())
        .await
        .expect("a healthy store must answer a miss successfully");
    assert!(
        response.value.is_none(),
        "an account that was never stored must read as absent"
    );

    shutdown.cancel();
}
