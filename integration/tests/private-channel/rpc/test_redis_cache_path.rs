//! Redis cache-alignment + settle-write coverage test.
//!
//! Runs a minimal PrivateChannel node with a Postgres `accountsdb_connection_url`
//! **and** `redis_cache_url` pointed at a testcontainers-provided Redis instance.
//! That is the configuration which fires the cache path in
//! `core/src/stages/settle.rs`:
//!
//! * The Redis init block in the settle worker.
//! * The startup cache alignment against Postgres.
//! * The best-effort Redis write on each settled batch.
//!
//! The other two tests cover the read side end to end: a read node refusing to
//! start against a cache stamped for another deployment, and a node answering
//! reads whose key families are never mirrored, which only the Postgres fallback
//! can do.
//!
//! The last one covers the writer giving up on a cache that keeps failing.
//!
//! Redis as the accounts database itself is rejected at startup, so there is no
//! Redis-only variant to cover here; see `accounts::traits` for that.
//!
//! This file is intentionally standalone (its own `[[test]]` target) so that
//! it runs independently of the broader `private_channel_integration` suite.

use anyhow::Result;
use private_channel_core::accounts::postgres::PostgresAccountsDB;
use private_channel_core::nodes::node::{run_node, NodeConfig, NodeMode};
use private_channel_core::stage_metrics::NoopMetrics;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer};
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
        redis_cache_url: None,
        redis_block_ttl_secs: 3_600,
        admin_keys: vec![dummy_admin],
        max_blockhashes: 150,
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

/// Poll `getBlockHeight` until the node has produced up to `target`. Blocks, not
/// slots: an idle node ticks the slot every blocktime but only produces a block
/// on the heartbeat.
async fn wait_for_block_height(client: &RpcClient, target: u64) {
    for _ in 0..150 {
        if client.get_block_height().await.unwrap_or(0) >= target {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("node never reached block height {target}");
}

/// The slot of the last block actually produced, which is the tip the settler
/// checks the cache against. `getSlot` is ahead of it across an idle stretch.
async fn last_block_slot(client: &RpcClient) -> u64 {
    let current_slot = client.get_slot().await.expect("getSlot");
    *client
        .get_blocks(0, Some(current_slot))
        .await
        .expect("getBlocks")
        .last()
        .expect("the node must have produced a block")
}

/// Exercise the settle worker's Redis init block + the per-batch best-effort
/// Redis write by running a Postgres-backed node with `redis_cache_url` pointed
/// at a live Redis testcontainer. The settle worker aligns the cache on startup
/// and attempts the Redis write on every block.
#[tokio::test(flavor = "multi_thread")]
async fn settle_worker_mirrors_to_the_configured_redis_cache() -> Result<()> {
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

    // 2. Start Redis (optional cache). Pin Redis 7, the default image tag is
    // 5.0 and predates commands this path relies on.
    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    // 3. Start the node pointed at Postgres, with the cache configured.
    let port = get_free_port();
    let mut config = minimal_node_config(pg_url.clone(), port);
    config.redis_cache_url = Some(redis_url.clone());
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");

    // 4. Wait for the settle worker to produce at least one block. This
    //    guarantees both the Redis init branch and the per-batch Redis
    //    write branch were taken.
    wait_for_first_block(&url).await;

    // 5. Issue a handful of `getLatestBlockhash` calls to ensure multiple
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

    // 6. Sanity-probe Redis itself: the cache must have published `latest_slot`
    //    at least once. Direct evidence the configured cache produced
    //    observable side effects, not just compiled code.
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    // `latest_slot` is written by the per-batch Redis write path, which runs
    // for every produced block. Genesis writes a zero, and an idle node's next
    // block is a heartbeat away, so poll past it rather than for the first write.
    use redis::AsyncCommands;
    let mut observed_slot: Option<u64> = None;
    for _ in 0..40 {
        observed_slot = conn.get("latest_slot").await.ok();
        if observed_slot.is_some_and(|slot| slot > 0) {
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
    Ok(())
}

/// A read node must refuse to start against a cache carrying another ledger's
/// deployment stamp. Only the write node aligns and purges the cache; a read node
/// that finds foreign state can only decline to serve it, because serving it is
/// exactly the harm the stamp exists to prevent.
///
/// Read mode on purpose. An Aio node runs a settler, which now shares the cache
/// URL and would purge the foreign stamp before the read path ever inspected it.
#[tokio::test(flavor = "multi_thread")]
async fn read_node_refuses_a_cache_from_another_deployment() -> Result<()> {
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

    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    // A read node never creates the schema, so mint it here as a write node
    // would. Without it the node fails on a missing deployment id instead of on
    // the foreign stamp this test is about.
    PostgresAccountsDB::new(&pg_url, false)
        .await
        .expect("initialize schema");

    use redis::AsyncCommands;
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    let _: () = conn
        .set("deployment_id", &[9u8; 16][..])
        .await
        .expect("stamp a foreign deployment");
    let _: () = conn
        .set(
            format!("account:{}", Keypair::new().pubkey()),
            vec![1u8, 2, 3],
        )
        .await
        .expect("seed foreign ledger state");

    let port = get_free_port();
    let mut config = minimal_node_config(pg_url.clone(), port);
    config.mode = NodeMode::Read;
    config.redis_cache_url = Some(redis_url.clone());
    let result = run_node(config).await;

    let error = result.err().expect("node must refuse the foreign cache");
    let message = format!("{error:#}");
    // The exact mismatch, not just the word "deployment", which a missing
    // deployment id would also produce.
    assert!(
        message.contains("belongs to deployment"),
        "error should name the deployment mismatch, got: {message}"
    );
    Ok(())
}

/// Drive the read path through a configured Redis cache.
///
/// Every read below is from a family the cache never holds: ranges, address
/// history and counters are not mirrored, because a short answer from a partial
/// mirror cannot be told from a complete one. They can only be answered by
/// Postgres, which makes this the end-to-end proof that the cache sits in front
/// of the source of truth rather than in place of it.
///
/// One `redis_cache_url` drives both paths, so the tail also asserts the settler
/// mirrored to the same instance the read path attached to.
#[tokio::test(flavor = "multi_thread")]
async fn read_path_serves_through_a_redis_cache() -> Result<()> {
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

    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    let port = get_free_port();
    let mut config = minimal_node_config(pg_url.clone(), port);
    config.redis_cache_url = Some(redis_url.clone());
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");

    wait_for_first_block(&url).await;

    // Each of these reads goes through AccountsDB::Redis. None of their key
    // families is mirrored, so every one is resolved by the Postgres fallback.
    let client = RpcClient::new(url.clone());
    client
        .get_latest_blockhash()
        .await
        .expect("getLatestBlockhash through the cache");
    let slot = client.get_slot().await.expect("getSlot");
    client
        .get_blocks(0, Some(slot))
        .await
        .expect("getBlocks through the cache");
    client
        .get_transaction_count()
        .await
        .expect("getTransactionCount through the cache");
    client
        .get_signatures_for_address(&Keypair::new().pubkey())
        .await
        .expect("getSignaturesForAddress through the cache");

    // One knob drives both paths, so the settler must have mirrored to the same
    // instance the read path attached to.
    use redis::AsyncCommands;
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    let cached_slot: Option<u64> = conn.get("latest_slot").await.expect("redis get");
    assert!(
        cached_slot.is_some(),
        "the settler must mirror to the cache the read path uses"
    );

    handles.shutdown().await;
    Ok(())
}

/// Once the cache has been given up on, the settler must stop touching Redis
/// altogether until its cooldown is up. The tip is corrupted rather than the
/// server stopped, so the marker below can still be read back.
///
/// The pause half only: the twenty blocks waited out below are a couple of
/// seconds against a `CACHE_MIRROR_COOLDOWN` of a minute, so no probe runs here.
/// Recovery is covered by `a_given_up_cache_is_mirrored_to_again_without_a_restart`
/// in core, which drives the settle worker with a cooldown short enough to watch.
///
/// That leaves this test asserting the cooldown is worth its name: drop it to
/// under a second and a probe lands inside this window, purges the keyspace and
/// takes the marker with it.
#[tokio::test(flavor = "multi_thread")]
async fn settler_stops_touching_a_cache_it_has_given_up_on() -> Result<()> {
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

    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    let port = get_free_port();
    let mut config = minimal_node_config(pg_url.clone(), port);
    config.redis_cache_url = Some(redis_url.clone());
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");

    // Mirror normally first, so the corruption below lands on a running settler
    // rather than on its startup alignment.
    wait_for_first_block(&url).await;

    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");

    // The tip is read as a u64, so a value that is not one fails every
    // continuity check from here on without Redis being down.
    let _: () = redis::cmd("SET")
        .arg("latest_slot")
        .arg("not-a-slot")
        .query_async(&mut conn)
        .await
        .expect("corrupt the cached tip");

    // Failures are counted per block, so wait out more blocks than the limit of
    // three. An idle node produces one per heartbeat, so this is not ten slots.
    let client = RpcClient::new(url.clone());
    let corrupted_at = client.get_block_height().await.expect("getBlockHeight");
    wait_for_block_height(&client, corrupted_at + 5).await;

    // A tip a settler still holding the cache would mirror against, and a marker
    // on the key that mirror would overwrite. Reads never write either.
    let repaired_at = last_block_slot(&client).await;
    let _: () = redis::cmd("SET")
        .arg("latest_slot")
        .arg(repaired_at)
        .query_async(&mut conn)
        .await
        .expect("repair the cached tip");
    let _: () = redis::cmd("SET")
        .arg("latest_blockhash")
        .arg("untouched")
        .query_async(&mut conn)
        .await
        .expect("mark the cached tip");

    let repaired_height = client.get_block_height().await.expect("getBlockHeight");
    wait_for_block_height(&client, repaired_height + 3).await;

    // A settler still attached would have overwritten this mirroring the next
    // batch, or purged it condemning a cache it could not line up with.
    let cached_blockhash: Option<String> = redis::cmd("GET")
        .arg("latest_blockhash")
        .query_async(&mut conn)
        .await
        .expect("redis get");
    assert_eq!(
        cached_blockhash.as_deref(),
        Some("untouched"),
        "a cache the settler has given up on must not be written to inside its cooldown"
    );

    handles.shutdown().await;
    Ok(())
}

/// Cached block entries expire while the mirror keeps following block
/// production. The expiry must never look like a cache that missed batches: that
/// would purge the keyspace and start a rebuild once per TTL, forever.
#[tokio::test(flavor = "multi_thread")]
async fn ttl_does_not_condemn_the_cache() -> Result<()> {
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

    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    let port = get_free_port();
    let mut config = minimal_node_config(pg_url.clone(), port);
    config.redis_cache_url = Some(redis_url.clone());
    // Short enough that entries expire while the node is still producing blocks
    // past them, which is the interleaving a condemnation would show up in.
    config.redis_block_ttl_secs = 1;
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");
    wait_for_first_block(&url).await;

    use redis::AsyncCommands;
    let client = RpcClient::new(url.clone());
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");

    // A key a rebuild purges but the mirror never writes. If the cache were
    // condemned and rebuilt, the keyspace was emptied and this is gone.
    let canary = format!("account:{}", Keypair::new().pubkey());
    let _: () = conn.set(&canary, vec![7u8]).await.expect("seed the canary");
    let stamp: Option<Vec<u8>> = conn.get("deployment_id").await.expect("redis get");
    assert!(
        stamp.is_some(),
        "the cache must be in service before the expiry is worth testing"
    );

    // Take a mirrored block and confirm the expiry is actually stamped on it,
    // so what follows measures an expiry rather than a key that never existed.
    let mut mirrored_slot: Option<u64> = None;
    for _ in 0..40 {
        mirrored_slot = conn.get("latest_slot").await.ok().flatten();
        if mirrored_slot.is_some_and(|slot: u64| slot > 0) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let expiring_slot = mirrored_slot.expect("the mirror must publish a tip");
    let ttl: i64 = conn
        .ttl(format!("block:{expiring_slot}"))
        .await
        .expect("redis ttl");
    assert!(
        ttl > 0 && ttl <= 1,
        "the cached block must carry the configured expiry, got {ttl}"
    );

    // Several TTL cycles, so the mirror keeps writing blocks past the expired
    // ones and every one of those writes runs the continuity check.
    sleep(Duration::from_millis(3_500)).await;

    assert!(
        !conn
            .exists::<_, bool>(format!("block:{expiring_slot}"))
            .await
            .expect("redis exists"),
        "the cached block entry must have expired"
    );
    assert!(
        conn.exists::<_, bool>(&canary).await.expect("redis exists"),
        "expiring block entries must not purge the keyspace"
    );
    assert_eq!(
        conn.get::<_, Option<Vec<u8>>>("deployment_id")
            .await
            .expect("redis get"),
        stamp,
        "expiring block entries must leave the cache in service"
    );

    // The tip keys never expire, so the mirror is still following blocks and the
    // node's own tip is still consistent with what the cache publishes. Read the
    // cache first: it is written after the Postgres commit, so it never leads.
    let cached_slot: Option<u64> = conn.get("latest_slot").await.expect("redis get");
    let cached_height: Option<u64> = conn.get("block_height").await.expect("redis get");
    let node_slot = client.get_slot().await.expect("getSlot");
    let node_height = client.get_block_height().await.expect("getBlockHeight");
    let cached_slot = cached_slot.expect("the cached tip must survive the expiry");
    let cached_height = cached_height.expect("the cached height must survive the expiry");
    assert!(
        cached_slot > expiring_slot && cached_slot <= node_slot,
        "the mirror must have followed blocks past the expired one: {expiring_slot} -> {cached_slot}, node at {node_slot}"
    );
    // At most one block can have been mirrored between the two reads.
    assert!(
        cached_height <= node_height && node_height - cached_height <= 1,
        "the cached height must be the height the node serves: cached {cached_height}, node {node_height}"
    );

    // An expired entry is a miss that falls through, never lost history.
    client
        .get_block(expiring_slot)
        .await
        .expect("an expired entry must be resolved against Postgres");

    handles.shutdown().await;
    Ok(())
}

/// The live slot is mirrored on every idle tick but is not the cached tip, so
/// the continuity check must ignore it. A cache condemned once per tick would
/// purge and rebuild the whole keyspace forever.
#[tokio::test(flavor = "multi_thread")]
async fn idle_ticks_do_not_condemn_the_cache() -> Result<()> {
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

    let redis = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis");
    let redis_host = redis.get_host().await.expect("redis host");
    let redis_port = redis.get_host_port_ipv4(6379).await.expect("redis port");
    let redis_url = format!("redis://{redis_host}:{redis_port}");

    let port = get_free_port();
    let mut config = minimal_node_config(pg_url.clone(), port);
    config.redis_cache_url = Some(redis_url.clone());
    // No expiry, so anything missing later is a purge rather than a TTL.
    config.redis_block_ttl_secs = 0;
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");
    wait_for_first_block(&url).await;

    use redis::AsyncCommands;
    let client = RpcClient::new(url.clone());
    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");

    // A key a rebuild purges but the mirror never writes.
    let canary = format!("account:{}", Keypair::new().pubkey());
    let _: () = conn.set(&canary, vec![7u8]).await.expect("seed the canary");
    let stamp: Option<Vec<u8>> = conn.get("deployment_id").await.expect("redis get");
    assert!(
        stamp.is_some(),
        "the cache must be in service before the idle stretch is worth testing"
    );

    // Several heartbeats of idle ticks, each one mirroring a slot the cached tip
    // does not follow.
    sleep(Duration::from_millis(3_500)).await;

    // The gap has to be real: the mirrored tip names a block, the mirrored slot
    // names a tick well past it.
    let cached_tip: u64 = conn
        .get::<_, Option<u64>>("latest_slot")
        .await
        .expect("redis get")
        .expect("the mirror must publish a tip");
    let cached_slot: u64 = conn
        .get::<_, Option<u64>>("current_slot")
        .await
        .expect("redis get")
        .expect("the mirror must publish the live slot");
    assert!(
        cached_slot > cached_tip,
        "idle ticks must have moved the slot past the cached tip: tip {cached_tip}, slot {cached_slot}"
    );

    assert!(
        conn.exists::<_, bool>(&canary).await.expect("redis exists"),
        "a moving slot must not purge the keyspace"
    );
    assert_eq!(
        conn.get::<_, Option<Vec<u8>>>("deployment_id")
            .await
            .expect("redis get"),
        stamp,
        "a moving slot must leave the cache in service"
    );

    // The read path goes through the cache, so this is the slot a replica sees.
    let node_slot = client.get_slot().await.expect("getSlot");
    assert!(
        node_slot >= cached_slot,
        "the served slot must be the mirrored one: cached {cached_slot}, served {node_slot}"
    );

    handles.shutdown().await;
    Ok(())
}
