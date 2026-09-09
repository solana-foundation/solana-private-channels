//! Gap gating for `YellowstoneSource`. The first live block of every connection, the very first
//! included, holds the checkpoint until a backfill closes the window below it. Fail-closed.

use mockito::{Matcher, Server as MockitoServer};
use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::checkpoint::CheckpointWriter;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::types::ProcessorMessage;
use private_channel_indexer::indexer::datasource::rpc_polling::rpc::RpcPoller;
use private_channel_indexer::indexer::datasource::yellowstone::YellowstoneSource;
use private_channel_indexer::indexer::transaction_processor::TransactionProcessor;
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::PostgresConfig;
use serde_json::json;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use std::sync::Arc;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[path = "yellowstone_helpers.rs"]
mod yellowstone_helpers;
use yellowstone_helpers::empty_block;

const CHECKPOINT: u64 = 100;
const TIP: u64 = 103;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();
}

/// Real Postgres via testcontainers; the container must stay alive for the test.
async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, Arc<Storage>) {
    let pg = Postgres::default()
        .with_db_name("reconnect_gap")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = pg.get_host().await.expect("pg host");
    let port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let db_url = format!("postgres://postgres:password@{host}:{port}/reconnect_gap");

    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url,
            max_connections: 10,
        })
        .await
        .expect("postgres storage"),
    );
    storage.init_schema().await.expect("init schema");
    (pg, Arc::new(storage))
}

fn empty_block_json() -> serde_json::Value {
    json!({
        "blockhash": "TestBlockHash11111111111111111111111111111",
        "parentSlot": 0,
        "transactions": []
    })
}

async fn mock_block_ok(server: &mut MockitoServer, slot: u64) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [slot]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": empty_block_json(), "id": 1}).to_string())
        .create_async()
        .await
}

/// The enumeration lists this slot as a producer but getBlock will not serve it,
/// so the poller reports it Unavailable and the fill aborts before any SlotComplete.
async fn mock_block_pruned(
    server: &mut MockitoServer,
    slot: u64,
    min_hits: usize,
) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [slot]}),
        ))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "error": { "code": -32009, "message": "Slot was skipped" },
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(min_hits)
        .create_async()
        .await
}

/// The `getBlocks` enumeration over the gap-fill range. Every slot in it produced
/// a block, so the chain proof turns entirely on whether each one can be fetched.
async fn mock_produced_slots(
    server: &mut MockitoServer,
    start: u64,
    end: u64,
    produced: &[u64],
) -> mockito::Mock {
    server
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [start, end]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": produced, "id": 1}).to_string())
        .create_async()
        .await
}

/// The range the reconnect gap-fill covers in every test here: the checkpoint is
/// replayed as the batch's first slot, so the batch is `[CHECKPOINT, TIP]`.
const GAP_FILL_SLOTS: [u64; 4] = [CHECKPOINT, CHECKPOINT + 1, CHECKPOINT + 2, TIP];

async fn wait_for_subscribes(ys: &MockYellowstoneServer, n: usize, secs: u64) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        while ys.call_count("subscribe") < n {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "expected {} subscribe handshakes, got {}",
            n,
            ys.call_count("subscribe")
        )
    });
}

async fn wait_until_matched(mock: &mockito::Mock, secs: u64, what: &str) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        while !mock.matched_async().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for: {what}"));
}

/// Poll the durable checkpoint until it reaches `want`, so a test can await a
/// gate hand-off without racing the writer's batch timer.
async fn wait_for_checkpoint(storage: &Arc<Storage>, program: &str, want: u64, secs: u64) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            if storage
                .get_committed_checkpoint(program)
                .await
                .expect("read checkpoint")
                == Some(want)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("checkpoint for {program} never reached {want}"));
}

fn poller(server: &MockitoServer) -> Arc<RpcPoller> {
    Arc::new(RpcPoller::new(
        server.url(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Confirmed,
    ))
}

/// Wire the real processor + checkpoint writer over `storage`. The durable
/// checkpoint is the observable: the gate holding the frontier is what keeps it
/// from advancing over an unfilled window.
fn spawn_pipeline(
    storage: Arc<Storage>,
) -> (
    mpsc::Sender<ProcessorMessage>,
    JoinHandle<()>,
    JoinHandle<()>,
) {
    let (checkpoint_tx, checkpoint_rx) = mpsc::channel(256);
    let writer_handle = CheckpointWriter::new(storage.clone())
        .with_batch_interval(1)
        .start(checkpoint_rx);
    let (tx, instruction_rx) = mpsc::channel::<ProcessorMessage>(256);
    let processor = TransactionProcessor::new(storage, checkpoint_tx);
    let processor_handle = tokio::spawn(async move {
        let _ = processor.start(instruction_rx).await;
    });
    (tx, processor_handle, writer_handle)
}

fn make_source(ys_url: String, rpc: &MockitoServer, storage: Arc<Storage>) -> YellowstoneSource {
    YellowstoneSource::new(
        ys_url,
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    )
    .with_gap_detection(poller(rpc), 1_000, 16)
    .with_storage(storage)
    // What `run` passes: startup accounted for everything up to the seeded anchor.
    .with_startup_floor(CHECKPOINT)
}

/// While the boundary slot stays pruned the gate holds the durable checkpoint at the
/// seed even though the source resubscribed and delivered the live resume slot; once
/// the RPC heals, the residual window fills and the checkpoint advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_gap_fill_gates_checkpoint_then_recovers() {
    init_tracing();
    let (_pg, storage) = start_postgres().await;
    // The anchor `run` writes before the stream opens; without it nothing is forwarded.
    storage
        .update_committed_checkpoint("escrow", CHECKPOINT)
        .await
        .expect("seed checkpoint");

    let mut rpc = MockitoServer::new_async().await;
    for s in (CHECKPOINT + 1)..=TIP {
        let _m = mock_block_ok(&mut rpc, s).await;
    }
    // The boundary slot blocks: expect the initial attempt plus at least one retry,
    // proving the fill loops instead of resolving over the gap.
    let _produced = mock_produced_slots(&mut rpc, CHECKPOINT, TIP, &GAP_FILL_SLOTS).await;
    let pruned = mock_block_pruned(&mut rpc, CHECKPOINT, 2).await;

    let ys = MockYellowstoneServer::start().await;
    let (tx, processor_handle, writer_handle) = spawn_pipeline(storage.clone());
    let cancel = CancellationToken::new();
    let mut source = make_source(ys.url(), &rpc, storage.clone());
    let handle = source
        .start(tx.clone(), cancel.clone())
        .await
        .expect("yellowstone source start");

    // Steady state: a live slot brings the writer frontier and durable checkpoint to
    // CHECKPOINT before the drop, so the gate has a real anchor to hold.
    wait_for_subscribes(&ys, 1, 10).await;
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(CHECKPOINT)));
    wait_for_checkpoint(&storage, "escrow", CHECKPOINT, 15).await;

    ys.drop_stream();
    // Resume slot delivered on subscribe #2 becomes the gate target; the fill of
    // (CHECKPOINT, TIP] stalls on the pruned boundary.
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(TIP)));

    wait_until_matched(&pruned, 20, "gap-fill retries on the pruned boundary").await;
    assert!(
        ys.call_count("subscribe") >= 2,
        "the source resubscribes immediately under the gate; got {}",
        ys.call_count("subscribe")
    );

    // The gate holds the durable checkpoint at the seed while the window is unfilled,
    // even though the live resume slot TIP was delivered and processed.
    for _ in 0..6 {
        assert_eq!(
            storage
                .get_committed_checkpoint("escrow")
                .await
                .expect("read checkpoint"),
            Some(CHECKPOINT),
            "gate must hold the checkpoint while the residual window is unfilled"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Heal the boundary; the fill closes (CHECKPOINT, TIP] and the checkpoint advances.
    let _healed = mock_block_ok(&mut rpc, CHECKPOINT).await;
    wait_for_checkpoint(&storage, "escrow", TIP, 30).await;

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(3), processor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), writer_handle).await;
    ys.shutdown().await;
}

/// Cancellation during a stalled gap-fill joins the source task promptly and stops
/// all RPC traffic, even though the fill runs as a spawned task under the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_while_gap_fill_stalled_joins_promptly() {
    init_tracing();
    let (_pg, storage) = start_postgres().await;
    storage
        .update_committed_checkpoint("escrow", CHECKPOINT)
        .await
        .expect("seed checkpoint");

    let mut rpc = MockitoServer::new_async().await;
    for s in (CHECKPOINT + 1)..=TIP {
        let _m = mock_block_ok(&mut rpc, s).await;
    }
    let _produced = mock_produced_slots(&mut rpc, CHECKPOINT, TIP, &GAP_FILL_SLOTS).await;
    let pruned = mock_block_pruned(&mut rpc, CHECKPOINT, 1).await;

    let ys = MockYellowstoneServer::start().await;
    // Keep the receiver alive so the source's sends never fail on a closed channel.
    let (tx, _rx) = mpsc::channel::<ProcessorMessage>(256);
    let cancel = CancellationToken::new();
    let mut source = make_source(ys.url(), &rpc, storage.clone());
    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    wait_for_subscribes(&ys, 1, 10).await;
    ys.drop_stream();
    // The first live block on subscribe #2 arms the gate and spawns the fill, which
    // then stalls on the pruned boundary.
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(TIP)));
    wait_until_matched(&pruned, 20, "a failed gap-fill attempt").await;

    // Cancel mid-stall; the select! on the backoff must wake immediately, far inside
    // the 5s production backoff.
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("source must join promptly when cancelled during a stall")
        .expect("source task must not panic");

    // Newest-registered mock matches first: any RPC call after the join would land
    // here and fail the zero-hit expectation.
    let quiet = rpc.mock("POST", "/").expect(0).create_async().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    quiet.assert_async().await;

    ys.shutdown().await;
}

/// The exact bug end-to-end: a live tip beyond the gap must never advance the durable
/// checkpoint while the gap is unfilled. This wires the real processor and writer, the
/// component that corrupted the checkpoint in the bug. The boundary stays pruned for
/// the whole test, so the fill can never resolve; the source resubscribes and delivers
/// the live resume slot and a tip beyond it, yet the gate keeps the checkpoint frozen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_gap_fill_never_advances_checkpoint_over_live_tip() {
    init_tracing();
    let (_pg, storage) = start_postgres().await;
    // The anchor `run` writes before the stream opens; without it nothing is forwarded.
    storage
        .update_committed_checkpoint("escrow", CHECKPOINT)
        .await
        .expect("seed checkpoint");

    let mut rpc = MockitoServer::new_async().await;
    for s in (CHECKPOINT + 1)..=TIP {
        let _m = mock_block_ok(&mut rpc, s).await;
    }
    // Never healed: the gap stays open for the whole test.
    let _produced = mock_produced_slots(&mut rpc, CHECKPOINT, TIP, &GAP_FILL_SLOTS).await;
    let pruned = mock_block_pruned(&mut rpc, CHECKPOINT, 1).await;

    let ys = MockYellowstoneServer::start().await;
    let (tx, processor_handle, writer_handle) = spawn_pipeline(storage.clone());
    let cancel = CancellationToken::new();
    let mut source = make_source(ys.url(), &rpc, storage.clone());
    let handle = source
        .start(tx.clone(), cancel.clone())
        .await
        .expect("yellowstone source start");

    // Bring the frontier and durable checkpoint to CHECKPOINT before the drop.
    wait_for_subscribes(&ys, 1, 10).await;
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(CHECKPOINT)));
    wait_for_checkpoint(&storage, "escrow", CHECKPOINT, 15).await;

    ys.drop_stream();
    // Subscribe #2 resumes at TIP (the gate target); a later live tip beyond the
    // target is delivered too. Under the fix both are gated; a fail-open regression
    // would plain-max the checkpoint to one of them.
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(TIP)));
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(TIP + 1)));
    wait_until_matched(&pruned, 20, "a gap-fill attempt on the pruned slot").await;

    // Hold well past the 5s backoff so a regressed fail-open would have advanced the
    // checkpoint by now; the gate must keep it at the seed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        assert_eq!(
            storage
                .get_committed_checkpoint("escrow")
                .await
                .expect("read checkpoint"),
            Some(CHECKPOINT),
            "checkpoint must never advance past the unfilled gap"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        ys.call_count("subscribe") >= 2,
        "the source resubscribes; the gate, not a blocked resubscribe, is the guard"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(3), processor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), writer_handle).await;
    ys.shutdown().await;
}

/// A cold start is a reconnect as far as the checkpoint is concerned, so the very first block
/// must gate `(checkpoint, resume]` first, or it carries the checkpoint past unread slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_start_gap_gates_checkpoint_on_first_connection() {
    init_tracing();
    let (_pg, storage) = start_postgres().await;
    // The startup anchor, exactly as `ensure_startup_anchor` leaves it before `start()`.
    storage
        .update_committed_checkpoint("escrow", CHECKPOINT)
        .await
        .expect("seed checkpoint");

    let mut rpc = MockitoServer::new_async().await;
    for s in (CHECKPOINT + 1)..=TIP {
        let _m = mock_block_ok(&mut rpc, s).await;
    }
    let _produced = mock_produced_slots(&mut rpc, CHECKPOINT, TIP, &GAP_FILL_SLOTS).await;
    let pruned = mock_block_pruned(&mut rpc, CHECKPOINT, 1).await;

    let ys = MockYellowstoneServer::start().await;
    let (tx, processor_handle, writer_handle) = spawn_pipeline(storage.clone());
    let cancel = CancellationToken::new();
    let mut source = make_source(ys.url(), &rpc, storage.clone());
    let handle = source
        .start(tx.clone(), cancel.clone())
        .await
        .expect("yellowstone source start");

    // No drop_stream anywhere: every assertion below is about connection #1.
    wait_for_subscribes(&ys, 1, 10).await;
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(TIP)));
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(TIP + 1)));

    // A fill was attempted, so the cold start armed rather than streaming straight through.
    wait_until_matched(
        &pruned,
        20,
        "a cold-start gap-fill attempt on the pruned slot",
    )
    .await;
    assert_eq!(
        ys.call_count("subscribe"),
        1,
        "the gate must arm on the first connection, with no reconnect involved"
    );

    // The gate holds the anchor while the window is unfilled, though both live slots landed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        assert_eq!(
            storage
                .get_committed_checkpoint("escrow")
                .await
                .expect("read checkpoint"),
            Some(CHECKPOINT),
            "checkpoint must never advance past the unfilled cold-start window"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Heal the boundary; the fill closes (CHECKPOINT, TIP] and the gate hands off.
    let _healed = mock_block_ok(&mut rpc, CHECKPOINT).await;
    wait_for_checkpoint(&storage, "escrow", TIP, 30).await;

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(3), processor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), writer_handle).await;
    ys.shutdown().await;
}
