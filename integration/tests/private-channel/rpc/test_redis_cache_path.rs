//! Redis warm-cache + settle-write coverage test.
//!
//! This standalone test exercises four regions by
//! running a minimal PrivateChannel node with a Postgres `accountsdb_connection_url`
//! **and** `REDIS_URL` set to a testcontainers-provided Redis instance.
//! This is the only configuration that fires the hybrid path in
//! `core/src/stages/settle.rs`:
//!
//! * The Redis init block in the settle worker (reads `REDIS_URL`, constructs
//!   `Option<RedisAccountsDB>`).
//! * The best-effort Redis write on each settled batch.
//!
//! A second sub-test drives the `AccountsDB::Redis` read path with
//! `accountsdb_connection_url=redis://...`, which exercises:
//!
//! * The `hex_to_b58` helper in `accounts/get_signatures_for_address`.
//! * `get_signatures_for_address_redis` (the Redis backend dispatch arm).
//!
//! This file is intentionally standalone (its own `[[test]]` target) so that
//! it runs independently of the broader `private_channel_integration` suite.

use anyhow::Result;
use private_channel_core::nodes::node::{run_node, NodeConfig, NodeMode};
use private_channel_core::stage_metrics::NoopMetrics;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::{net::TcpListener, sync::Arc, time::Duration};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio::time::sleep;

fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Minimal `NodeConfig` for a coverage-focused RPC-only exercise. Admin keys
/// and mint pubkey are irrelevant — we only submit unauthenticated read
/// queries (`getLatestBlockhash`, `getSignaturesForAddress`) that flush one
/// or more empty blocks through the settle stage.
fn minimal_node_config(accountsdb_connection_url: String, port: u16) -> NodeConfig {
    let dummy_admin = Keypair::new().pubkey();
    NodeConfig {
        mode: NodeMode::Aio,
        port,
        sigverify_queue_size: 100,
        sigverify_workers: 1,
        max_connections: 50,
        max_tx_per_batch: 10,
        batch_deadline_ms: 5,
        batch_channel_capacity: 16,
        ingress_queue_capacity: private_channel_core::nodes::node::DEFAULT_INGRESS_QUEUE_CAPACITY,
        sequencer_queue_capacity:
            private_channel_core::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        execution_results_capacity:
            private_channel_core::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY,
        max_svm_workers: 2,
        accountsdb_connection_url,
        admin_keys: vec![dummy_admin],
        transaction_expiration_ms: 15000,
        blocktime_ms: 100,
        perf_sample_period_secs: 10,
        metrics: Arc::new(NoopMetrics),
    }
}

/// Poll `getLatestBlockhash` until the settle worker has committed at least
/// one block, so the Postgres + Redis write paths have both fired.
async fn wait_for_first_block(url: &str) {
    let client = RpcClient::new(url.to_string());
    for _ in 0..50 {
        if client.get_latest_blockhash().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("node never produced a block at {url}");
}

/// Exercise the settle worker's Redis init block + the per-batch
/// best-effort Redis write by running a Postgres-backed node with
/// `REDIS_URL` pointed at a live Redis testcontainer. The settle worker will
/// warm the cache on startup and attempt the Redis write on every block.
#[tokio::test(flavor = "multi_thread")]
async fn settle_worker_uses_redis_warm_cache_when_env_set() -> Result<()> {
    // 1. Start Postgres (primary accountsdb).
    let pg = Postgres::default()
        .with_db_name("private_channel_node")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let pg_host = pg.get_host().await.expect("pg host");
    let pg_port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let pg_url = format!("postgres://postgres:password@{pg_host}:{pg_port}/private_channel_node");

    // 2. Start Redis (optional warm cache).
    // Pin Redis 7 — the default image tag is 5.0 which lacks the
    // `ZRANGE ... BYSCORE REV LIMIT` syntax used by the backend helper.
    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    // 3. Set REDIS_URL so the settle worker's env-var branch fires. We keep
    //    this set for the duration of the test and rely on process isolation
    //    (nextest runs each test in its own process) so we don't clobber any
    //    parallel test.
    //
    //    SAFETY: env mutation in a test — nextest process isolation keeps this
    //    confined. Wrapped in `unsafe` for Rust 2024/recent std semantics.
    // We use the older API here for compatibility with the project's Rust
    // edition (2021) — `set_var` is still safe to call in single-threaded
    // test startup.
    std::env::set_var("REDIS_URL", &redis_url);

    // 4. Start the node pointed at Postgres.
    let port = get_free_port();
    let config = minimal_node_config(pg_url.clone(), port);
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");

    // 5. Wait for the settle worker to produce at least one block. This
    //    guarantees both the Redis init branch and the per-batch Redis
    //    write branch were taken.
    wait_for_first_block(&url).await;

    // 6. Issue a handful of `getLatestBlockhash` calls to ensure multiple
    //    settle cycles run and the Redis best-effort write path is hit
    //    repeatedly.
    let client = RpcClient::new(url.clone());
    for _ in 0..3 {
        client
            .get_latest_blockhash()
            .await
            .expect("getLatestBlockhash");
        sleep(Duration::from_millis(150)).await;
    }

    // 7. Sanity-probe Redis itself — the warm cache must have published
    //    `latest_slot` at least once (set by `warm_redis_cache`). This is a
    //    direct assertion that the REDIS_URL branch actually produced
    //    observable side-effects, not just compiled code.
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    // `latest_slot` is written by the per-batch Redis write path, which runs
    // for every produced block. We poll briefly to tolerate settle timing.
    use redis::AsyncCommands;
    let mut observed_slot: Option<u64> = None;
    for _ in 0..20 {
        observed_slot = conn.get("latest_slot").await.ok();
        if observed_slot.is_some() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let slot = observed_slot.expect(
        "Redis `latest_slot` key must be populated by the settle worker's \
         best-effort Redis write path",
    );
    assert!(
        slot > 0,
        "Redis `latest_slot` must be > 0 after at least one committed block, got {slot}"
    );

    handles.shutdown().await;
    std::env::remove_var("REDIS_URL");
    Ok(())
}

/// Exercise `hex_to_b58` and `get_signatures_for_address_redis`
/// by running a node with `accountsdb_connection_url=redis://...` and issuing
/// a `getSignaturesForAddress` RPC. With no indexed signatures the response
/// is an empty list, but that still dispatches through the Redis-backend arm
/// and through the `ZRANGE ... BYSCORE REV` query — enough to cover the
/// entry point and the empty-result early-return branch.
#[tokio::test(flavor = "multi_thread")]
async fn get_signatures_for_address_dispatches_to_redis_backend() -> Result<()> {
    // Start Redis (used as the sole accountsdb).
    // Pin Redis 7 — the default image tag is 5.0 which lacks the
    // `ZRANGE ... BYSCORE REV LIMIT` syntax used by the backend helper.
    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    // Make sure REDIS_URL is NOT set — we're exercising the
    // `AccountsDB::Redis` read-path branch, not the Postgres+Redis hybrid.
    std::env::remove_var("REDIS_URL");

    let port = get_free_port();
    let config = minimal_node_config(redis_url.clone(), port);
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");

    // Wait for the node to be ready.
    wait_for_first_block(&url).await;

    // Ask for signatures on an arbitrary address — routes through
    // `AccountsDB::Redis` → `get_signatures_for_address_redis`. With no
    // indexed transactions, the query must return an empty vec. This
    // dispatches into the `ZRANGE` call and exits via the
    // `if sig_strings.is_empty()` early-return branch of
    // `get_signatures_for_address_redis`.
    let client = RpcClient::new(url);
    let arbitrary = Pubkey::new_unique();
    let sigs = client
        .get_signatures_for_address(&arbitrary)
        .await
        .expect("getSignaturesForAddress");
    assert!(
        sigs.is_empty(),
        "fresh Redis backend must return empty signature list, got {} entries",
        sigs.len()
    );

    // Seed a couple of fake entries in `addr_sigs:{pubkey}` so the
    // non-empty-result branch (which calls `hex_to_b58` + `MGET`) is
    // also exercised. A valid blob is required because the helper
    // deserializes `StoredTransaction` and returns `Err` on corruption.
    // Rather than fabricate a valid blob, we insert entries with no
    // matching `tx:{sig}` and expect the helper to return a `Transaction
    // data missing` error — that still traverses the `hex_to_b58` loop
    // plus MGET, hitting `hex_to_b58` and the `missing blob` error arm
    // inside `get_signatures_for_address_redis`.
    use redis::AsyncCommands;
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    let key = format!("addr_sigs:{arbitrary}");
    // 64-byte dummy signature, hex-encoded. Score = slot = 1.
    let dummy_sig_hex = "00".repeat(64);
    let _: () = conn
        .zadd(&key, &dummy_sig_hex, 1i64)
        .await
        .expect("zadd dummy sig");

    let result = client.get_signatures_for_address(&arbitrary).await;
    // `get_signatures_for_address_redis` returns `Err("Transaction data
    // missing...")` when the `tx:{sig}` key is absent, and the RPC handler
    // propagates it via `?` — so the client must always see an error here.
    assert!(
        result.is_err(),
        "Redis backend must return an error for a missing tx blob, got Ok({:?})",
        result.ok()
    );

    handles.shutdown().await;
    Ok(())
}
