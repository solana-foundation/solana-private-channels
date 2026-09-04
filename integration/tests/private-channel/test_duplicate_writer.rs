//! Real-node guards for the single-writer model, driving full in-process nodes
//! against a Postgres testcontainer: the startup lease refusing a second
//! write-capable node, the heartbeat stopping a node whose lease was pulled, the
//! database stopping a passed-by writer, and read nodes staying ungated.

use {
    private_channel_core::{
        accounts::writer_lease::WriterLease,
        nodes::node::{run_node, NodeConfig, NodeHandles, NodeMode},
        stage_metrics::NoopMetrics,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    sqlx::{postgres::PgConnection, Connection},
    std::{sync::Arc, time::Duration},
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::time::{sleep, timeout},
    tokio_util::sync::CancellationToken,
};

#[path = "../helpers.rs"]
mod helpers;
use helpers::get_free_port;

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_db_name("duplicate_writer")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/duplicate_writer");
    (container, url)
}

fn write_node_config(db_url: String, port: u16) -> NodeConfig {
    NodeConfig {
        mode: NodeMode::Aio,
        port,
        sigverify_queue_size: 64,
        sigverify_workers: 2,
        max_connections: 100,
        max_tx_per_batch: 16,
        batch_deadline_ms: 5,
        batch_channel_capacity: 8,
        ingress_queue_capacity: 64,
        sequencer_queue_capacity: 64,
        execution_results_capacity: 64,
        max_svm_workers: 2,
        accountsdb_connection_url: db_url,
        redis_cache_url: None,
        admin_keys: vec![],
        transaction_expiration_ms: 15_000,
        blocktime_ms: 100,
        perf_sample_period_secs: 3600,
        metrics: Arc::new(NoopMetrics),
    }
}

/// Block until the node has committed at least one slot, so the settler is
/// provably running before the test does anything to it.
async fn await_first_block(port: u16) {
    let client = RpcClient::new(format!("http://127.0.0.1:{port}"));
    for _ in 0..50 {
        if client.get_latest_blockhash().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("node never produced its first block");
}

/// Block until a block is committed, for nodes that serve no read RPC.
async fn await_first_block_in_db(conn: &mut PgConnection) -> i64 {
    for _ in 0..100 {
        let slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
            .fetch_one(&mut *conn)
            .await
            .expect("failed to read the tip");
        if let Some(slot) = slot {
            return slot;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("node never committed a block");
}

/// Block until a slot above `floor` is committed. A replacement node serves the
/// old tip the moment it boots, so only a new slot proves it is producing.
async fn await_slot_above(conn: &mut PgConnection, floor: i64) -> i64 {
    for _ in 0..100 {
        let slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
            .fetch_one(&mut *conn)
            .await
            .expect("failed to read the tip");
        if let Some(slot) = slot {
            if slot > floor {
                return slot;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("no slot above {floor} was committed");
}

/// The tip, or `None` on a ledger with no blocks.
async fn latest_slot_opt(conn: &mut PgConnection) -> Option<i64> {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(conn)
        .await
        .expect("failed to read the tip")
}

async fn latest_slot(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(conn)
        .await
        .expect("failed to read the tip")
        .expect("the node should have committed at least one block")
}

/// Only write-capable modes take the lease. A read node must still come up
/// against a database a write node already owns, or the documented topology of
/// one writer beside one reader would stop working.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_node_starts_alongside_a_write_node() {
    let (_pg, url) = start_postgres().await;

    let write_port = get_free_port();
    let writer = run_node(write_node_config(url.clone(), write_port))
        .await
        .expect("the write node must start");
    await_first_block(write_port).await;

    let read_port = get_free_port();
    let mut read_config = write_node_config(url, read_port);
    read_config.mode = NodeMode::Read;
    let reader = run_node(read_config)
        .await
        .expect("a read node must start while a write node holds the lease");

    // Serving a blockhash proves it reached the database rather than just booting.
    await_first_block(read_port).await;

    reader.shutdown().await;
    writer.shutdown().await;
}

/// A second write-capable node against one primary must be refused at startup,
/// and the refusal must not outlive the node that caused it: an operator
/// restarting a write node needs the replacement to come straight up.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_write_node_is_refused_until_the_first_releases_the_lease() {
    let (_pg, url) = start_postgres().await;

    // Write mode holds the lease here and Aio is refused, so both arms of the
    // mode gate are exercised.
    let first_port = get_free_port();
    let mut first_config = write_node_config(url.clone(), first_port);
    first_config.mode = NodeMode::Write;
    let first = run_node(first_config)
        .await
        .expect("the first write node must start");
    let mut probe = PgConnection::connect(&url)
        .await
        .expect("failed to open the probe connection");
    await_first_block_in_db(&mut probe).await;

    let second_port = get_free_port();
    let refusal = run_node(write_node_config(url.clone(), second_port))
        .await
        .err()
        .expect("a second write node must be refused while the first holds the lease");
    assert!(
        refusal.to_string().contains("writer lease"),
        "the refusal must name the writer lease, got: {refusal}"
    );

    first.shutdown().await;
    let tip_at_handover = latest_slot(&mut probe).await;

    let replacement_port = get_free_port();
    let replacement: NodeHandles = run_node(write_node_config(url, replacement_port))
        .await
        .expect("a replacement write node must start once the lease is released");
    await_slot_above(&mut probe, tip_at_handover).await;

    // The guard tolerates gaps, so nothing else would catch a restart that
    // resumed from the wrong slot.
    let first_new_slot =
        sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks WHERE slot > $1")
            .bind(tip_at_handover)
            .fetch_one(&mut probe)
            .await
            .expect("failed to read the replacement's first slot")
            .expect("the replacement must have committed a block");
    assert_eq!(
        first_new_slot,
        tip_at_handover + 1,
        "a restart must continue the chain without leaving a gap"
    );

    replacement.shutdown().await;
}

/// A lease can vanish under a running node: a failover, a proxy reaper or a
/// terminated backend all drop the session lock. The heartbeat has to stop the
/// node, or it keeps writing lease-less while a replacement starts beside it.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_loses_its_lease_stops_and_a_replacement_takes_over() {
    let (_pg, url) = start_postgres().await;

    let port = get_free_port();
    let mut handles = run_node(write_node_config(url.clone(), port))
        .await
        .expect("the write node must start");
    await_first_block(port).await;

    // The lease session is the only advisory lock holder on this database.
    let mut killer = PgConnection::connect(&url)
        .await
        .expect("failed to open the killer connection");
    let terminated = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM (
           SELECT pg_terminate_backend(pid) FROM pg_locks
           WHERE locktype = 'advisory' AND granted
         ) AS killed",
    )
    .fetch_one(&mut killer)
    .await
    .expect("failed to terminate the lease session");
    assert_eq!(terminated, 1, "exactly one lease holder was expected");

    // Bounded by the probe interval; the slack is for a loaded machine.
    timeout(Duration::from_secs(60), handles.wait_for_any_worker_quit())
        .await
        .expect("a node that lost its lease must stop");
    handles.shutdown().await;

    let tip_at_handover = latest_slot(&mut killer).await;
    let replacement_port = get_free_port();
    let replacement = run_node(write_node_config(url, replacement_port))
        .await
        .expect("a replacement must start once the stopped node has gone");
    await_slot_above(&mut killer, tip_at_handover).await;
    replacement.shutdown().await;
}

/// The heartbeat only catches a lease this node can no longer prove it holds, so
/// the database still has to fail closed on its own. Move the tip out from under
/// a live settler and assert it stops rather than committing over the newer block.
#[tokio::test(flavor = "multi_thread")]
async fn a_settler_whose_tip_moves_underneath_it_stops_instead_of_forking() {
    let (_pg, url) = start_postgres().await;

    let port = get_free_port();
    let mut handles = run_node(write_node_config(url.clone(), port))
        .await
        .expect("the write node must start");
    await_first_block(port).await;

    // Stands in for a block another writer committed while this settler was busy.
    // Placed well above the tip so the live settler cannot reach that slot first
    // and turn this into a race, and so its very next commit is already behind.
    let mut intruder = PgConnection::connect(&url)
        .await
        .expect("failed to open the intruder connection");
    let stolen_slot = latest_slot(&mut intruder).await + 1_000;
    sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
        .bind(stolen_slot)
        .bind(&[0u8; 32][..])
        .execute(&mut intruder)
        .await
        .expect("the intruder block must be stored");

    // The settler stopping tears the dedup stage down with it, so whichever of the
    // two the join observes first is a scheduling detail. What matters is that the
    // write pipeline goes down, which is what makes the node exit.
    let quit = timeout(Duration::from_secs(30), handles.wait_for_any_worker_quit())
        .await
        .expect("the write pipeline must stop once the tip has been passed");
    assert!(
        quit == "Settle" || quit == "Dedup",
        "a write-pipeline worker must be the one that stopped, got: {quit}"
    );

    // The node must not have written over the block it lost the race for.
    let mut checker = PgConnection::connect(&url)
        .await
        .expect("failed to open the checker connection");
    let stored: Vec<u8> = sqlx::query_scalar("SELECT data FROM blocks WHERE slot = $1")
        .bind(stolen_slot)
        .fetch_one(&mut checker)
        .await
        .expect("the intruder block must still be there");
    assert_eq!(
        stored,
        vec![0u8; 32],
        "the settler must not have overwritten the block that passed it"
    );
    assert_eq!(
        latest_slot(&mut checker).await,
        stolen_slot,
        "the settler must not have committed any slot above the intruder block"
    );

    // The node binary shuts down right after a worker quits, so that path has to
    // survive an already-finished worker and still hand the lease back, or an
    // operator restarting the crashed node would be locked out by its corpse.
    handles.shutdown().await;
    WriterLease::acquire(&url, CancellationToken::new(), Arc::new(NoopMetrics))
        .await
        .expect("the lease must be free once a stopped node has shut down");
}

/// A startup that fails after the lease is taken must hand it back before the
/// error reaches the caller, or a caller retrying at once is refused by a lock
/// nothing means to hold. Occupying the RPC port fails the node late, with the
/// write pipeline already running, so those workers must be stopped as well.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_startup_frees_the_lease_before_returning() {
    let (_pg, url) = start_postgres().await;

    // Opened up front so the assertion below costs one round trip, not a connect.
    let mut probe = PgConnection::connect(&url)
        .await
        .expect("failed to open the probe connection");

    let port = get_free_port();
    let _occupied =
        std::net::TcpListener::bind(("0.0.0.0", port)).expect("failed to take the port");

    let failure = run_node(write_node_config(url.clone(), port))
        .await
        .err()
        .expect("a node whose RPC port is taken must fail to start");
    assert!(
        failure.to_string().to_lowercase().contains("address"),
        "the failure must be the port bind, got: {failure}"
    );

    // No polling: the lock being gone the moment the error returns is the point,
    // and a retry loop would hide a release that only lands later.
    let still_held: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted)",
    )
    .fetch_one(&mut probe)
    .await
    .expect("failed to read pg_locks");
    assert!(
        !still_held,
        "a failed startup must not return while the writer lease is still held"
    );

    WriterLease::acquire(&url, CancellationToken::new(), Arc::new(NoopMetrics))
        .await
        .expect("a failed startup must leave the writer lease available");

    // The workers must be stopped too, not just detached: a pipeline left running
    // would keep committing slots with no lease and no way to shut it down.
    let tip_after_failure = latest_slot_opt(&mut probe).await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(
        latest_slot_opt(&mut probe).await,
        tip_after_failure,
        "a failed startup must leave no worker still committing blocks"
    );
}
