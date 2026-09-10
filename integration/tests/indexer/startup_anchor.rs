//! Startup anchor wiring for `private_channel_indexer::indexer::run`.
//!
//! Reconnect gap repair replays from the durable checkpoint, so `run` must persist one
//! before the Yellowstone stream can deliver anything. These tests pin the value it picks:
//! the chain tip when no startup backfill resolved a boundary, and the boundary's floor
//! when one did. Picking the top of the resolved range instead would durably claim the
//! slots backfill has not fetched yet, which is the loss the anchor exists to prevent.

use mockito::{Matcher, Server as MockitoServer};
use private_channel_indexer::{
    config::{BackfillConfig, ReconciliationConfig, RpcPollingConfig, YellowstoneConfig},
    indexer::run,
    storage::{PostgresDb, Storage},
    DatasourceType, IndexerConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType,
    StorageType,
};
use serde_json::json;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use sqlx::PgPool;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

#[path = "yellowstone_helpers.rs"]
mod yellowstone_helpers;
use yellowstone_helpers::empty_block;

/// Chain tip both tests' mock RPC reports.
const TIP: u64 = 900;
/// First slot to process when backfill is on, so the floor 897 stays distinct from the tip.
const START_SLOT: u64 = 898;
/// Slot the cold-start stream opens at, well above the tip so a real window sits under it.
const COLD_START_TIP: u64 = 910;
/// A durable checkpoint the mock RPC node has not caught up to, so the floor lands above it.
const LAGGING_CHECKPOINT: u64 = 1_000;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();
}

/// Real Postgres via testcontainers; the container must stay alive for the test.
async fn start_postgres(
    db: &str,
) -> (
    testcontainers::ContainerAsync<Postgres>,
    PgPool,
    PostgresConfig,
) {
    let pg = Postgres::default()
        .with_db_name(db)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = pg.get_host().await.expect("pg host");
    let port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/{db}");

    let pool = PgPool::connect(&url).await.expect("pg pool");
    let config = PostgresConfig {
        database_url: url,
        max_connections: 10,
    };
    (pg, pool, config)
}

/// Read the anchor row. `run` creates the schema, so an early query is a "not yet".
async fn anchor_of(pool: &PgPool, program: &str) -> Option<u64> {
    // The column is nullable, so a row can exist before any slot has been committed.
    let row: Option<(Option<i64>,)> =
        sqlx::query_as("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
            .bind(program)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.and_then(|(slot,)| slot).map(|slot| slot as u64)
}

/// Poll until the anchor row appears, so the assertion does not race startup.
async fn wait_for_anchor(pool: &PgPool, program: &str, secs: u64) -> u64 {
    tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            if let Some(slot) = anchor_of(pool, program).await {
                return slot;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no startup anchor was ever persisted for {program}"))
}

/// Yellowstone config pointed at a mock that never sends a block, plus the given backfill
/// settings. Withdraw skips escrow reconciliation, which needs an on-chain instance.
/// The rpc_polling block is unused by the Yellowstone datasource but is required whenever
/// backfill is enabled, so it is always supplied.
fn indexer_config(geyser_endpoint: String, backfill: BackfillConfig) -> IndexerConfig {
    IndexerConfig {
        datasource_type: DatasourceType::Yellowstone,
        rpc_polling: Some(RpcPollingConfig {
            from_slot: None,
            poll_interval_ms: 1_000,
            error_retry_interval_ms: 1_000,
            batch_size: 10,
            encoding: UiTransactionEncoding::Json,
            commitment: CommitmentLevel::Finalized,
        }),
        yellowstone: Some(YellowstoneConfig {
            endpoint: geyser_endpoint,
            x_token: None,
            commitment: "confirmed".to_string(),
        }),
        backfill,
        reconciliation: ReconciliationConfig::default(),
    }
}

fn common_config(postgres: PostgresConfig, rpc_url: String) -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url,
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres,
        escrow_instance_id: None,
    }
}

async fn mock_get_slot(rpc: &mut MockitoServer, slot: u64) -> mockito::Mock {
    rpc.mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": slot, "id": 1}).to_string())
        .create_async()
        .await
}

fn empty_block_json() -> serde_json::Value {
    json!({
        "blockhash": "TestBlockHash11111111111111111111111111111",
        "parentSlot": 0,
        "transactions": []
    })
}

async fn mock_block_ok(rpc: &mut MockitoServer, slot: u64) -> mockito::Mock {
    rpc.mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [slot]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": empty_block_json(), "id": 1}).to_string())
        .create_async()
        .await
}

/// The enumeration lists this slot as a producer but getBlock will not serve it, so the
/// poller reports it unavailable and the fill stalls with the gate still closed.
async fn mock_block_pruned(rpc: &mut MockitoServer, slot: u64) -> mockito::Mock {
    rpc.mock("POST", "/")
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
        .expect_at_least(1)
        .create_async()
        .await
}

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
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn wait_for_checkpoint(pool: &PgPool, program: &str, want: u64, secs: u64) {
    let mut last = None;
    let reached = tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            last = anchor_of(pool, program).await;
            if last == Some(want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "checkpoint never reached {want}; last was {last:?}"
    );
}

/// The shipped Yellowstone deployment runs with backfill disabled. Nothing resolves a
/// boundary there, so the tip the stream is about to start from is the anchor, and it has
/// to be durable before the first block rather than after the checkpoint writer flushes.
/// The mock never sends a block, so a row appearing at all can only have come from startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_persists_tip_anchor_before_streaming() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_anchor_tip").await;

    let mut rpc = MockitoServer::new_async().await;
    let _slot = mock_get_slot(&mut rpc, TIP).await;
    // No block mocks: any fetch would mean a backfill ran where none was configured.
    let no_blocks = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getBlock"})))
        .expect(0)
        .create_async()
        .await;

    let ys = MockYellowstoneServer::start().await;

    let backfill = BackfillConfig {
        enabled: false,
        exit_after_backfill: false,
        rpc_url: rpc.url(),
        batch_size: 100,
        max_gap_slots: 1_000,
        start_slot: None,
    };
    let indexer = indexer_config(ys.url(), backfill);
    let common = common_config(postgres, rpc.url());

    // Surface a startup failure instead of letting it look like a missing anchor.
    let handle = tokio::spawn(async move {
        if let Err(e) = run(common, indexer, None).await {
            eprintln!("indexer run exited: {e}");
        }
    });

    // The mock never sends a block, so this anchor can only have come from startup.
    let anchor = wait_for_anchor(&pool, "withdraw", 60).await;
    assert_eq!(
        anchor, TIP,
        "a backfill-disabled deployment must anchor at the tip it starts streaming from"
    );
    no_blocks.assert_async().await;

    handle.abort();
    ys.shutdown().await;
}

/// With startup backfill resolving a range, the anchor is that range's floor. The tip and
/// the live start slot both sit above slots backfill has yet to fetch, so recording either
/// would mark them handled and put them permanently out of reach. All three are plain u64,
/// so nothing but this test stops the wrong one being passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_anchors_at_resolved_from_slot_not_the_tip() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_anchor_floor").await;

    let floor = START_SLOT - 1;

    let mut rpc = MockitoServer::new_async().await;
    let _slot = mock_get_slot(&mut rpc, TIP).await;
    // The floor is exclusive, so the anchor lookup lists from one slot below it.
    let _anchor = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [floor, TIP]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": [898, 899, 900], "id": 1}).to_string())
        .expect_at_least(0)
        .create_async()
        .await;
    let _enumeration = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [START_SLOT, TIP]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": [898, 899, 900], "id": 1}).to_string())
        .expect_at_least(0)
        .create_async()
        .await;
    for slot in START_SLOT..=TIP {
        let _block = rpc
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({"method": "getBlock", "params": [slot]}),
            ))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "blockhash": "TestBlockHash11111111111111111111111111111",
                        "parentSlot": slot - 1,
                        "transactions": []
                    },
                    "id": 1
                })
                .to_string(),
            )
            .expect_at_least(0)
            .create_async()
            .await;
    }

    let ys = MockYellowstoneServer::start().await;

    let backfill = BackfillConfig {
        enabled: true,
        exit_after_backfill: false,
        rpc_url: rpc.url(),
        batch_size: 100,
        max_gap_slots: 1_000,
        start_slot: Some(START_SLOT),
    };
    let indexer = indexer_config(ys.url(), backfill);
    let common = common_config(postgres, rpc.url());

    // Surface a startup failure instead of letting it look like a missing anchor.
    let handle = tokio::spawn(async move {
        if let Err(e) = run(common, indexer, None).await {
            eprintln!("indexer run exited: {e}");
        }
    });

    let anchor = wait_for_anchor(&pool, "withdraw", 60).await;
    assert_eq!(
        anchor,
        floor,
        "the anchor must be the resolved range floor, not the tip ({TIP}) \
         or the live start slot ({})",
        TIP + 1
    );

    handle.abort();
    ys.shutdown().await;
}

/// The withdraw program has no custody comparison to fall back on, so nothing else would
/// notice a configured start slot that skips real burns. A skipped burn is a release that
/// is never queued, so startup has to refuse outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn withdraw_start_slot_ahead_of_checkpoint_refuses_startup() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_anchor_refuse").await;

    // Create the schema the way startup would, then stand in for an earlier run that
    // stopped well below the slice the configured start would begin at.
    let storage = private_channel_indexer::storage::Storage::Postgres(
        private_channel_indexer::storage::PostgresDb::new(&postgres)
            .await
            .expect("storage"),
    );
    storage.init_schema().await.expect("schema");
    let stale_checkpoint = START_SLOT - 50;
    storage
        .update_committed_checkpoint("withdraw", stale_checkpoint)
        .await
        .expect("seed checkpoint");

    let mut rpc = MockitoServer::new_async().await;
    let _slot = mock_get_slot(&mut rpc, TIP).await;

    let ys = MockYellowstoneServer::start().await;

    let backfill = BackfillConfig {
        enabled: true,
        exit_after_backfill: false,
        rpc_url: rpc.url(),
        batch_size: 100,
        max_gap_slots: 1_000,
        start_slot: Some(START_SLOT),
    };
    let indexer = indexer_config(ys.url(), backfill);
    let common = common_config(postgres, rpc.url());

    let err = tokio::time::timeout(Duration::from_secs(60), run(common, indexer, None))
        .await
        .expect("startup must not hang")
        .expect_err("a start slot past the checkpoint must refuse to boot");

    let rendered = err.to_string();
    assert!(
        rendered.contains("indexer.backfill.start_slot"),
        "the refusal must name the offending key, got: {rendered}"
    );
    assert_eq!(
        anchor_of(&pool, "withdraw").await,
        Some(stale_checkpoint),
        "a refused boot must not move or overwrite the durable checkpoint"
    );

    ys.shutdown().await;
}

/// With backfill disabled nothing fills the window between the anchor and the slot the stream
/// opens at, so without cold-start arming the first live block checkpoints straight over it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_gates_the_first_block_when_backfill_is_disabled() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("cold_start_no_backfill").await;

    let mut rpc = MockitoServer::new_async().await;
    // No backfill resolves a boundary, so the anchor and the floor are both the tip.
    let _slot = mock_get_slot(&mut rpc, TIP).await;
    // The fill replays the anchor itself, so its range starts one slot below.
    let produced: Vec<u64> = (TIP - 1..=COLD_START_TIP).collect();
    let _enumeration = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getBlocks"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": produced, "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;
    for slot in TIP + 1..=COLD_START_TIP {
        let _block = mock_block_ok(&mut rpc, slot).await;
    }
    // Holds the window open so the gate can be observed doing its job.
    let pruned = mock_block_pruned(&mut rpc, TIP).await;

    let ys = MockYellowstoneServer::start().await;

    let backfill = BackfillConfig {
        enabled: false,
        exit_after_backfill: false,
        rpc_url: rpc.url(),
        batch_size: 100,
        max_gap_slots: 1_000,
        start_slot: None,
    };
    let indexer = indexer_config(ys.url(), backfill);
    let common = common_config(postgres, rpc.url());

    // Surface a startup failure instead of letting it look like a missing anchor.
    let handle = tokio::spawn(async move {
        if let Err(e) = run(common, indexer, None).await {
            eprintln!("indexer run exited: {e}");
        }
    });

    assert_eq!(wait_for_anchor(&pool, "withdraw", 60).await, TIP);

    // One connection, no drop: everything below is about the very first stream.
    wait_for_subscribes(&ys, 1, 30).await;
    ys.enqueue(UpdateMatcher, Update::ok(empty_block(COLD_START_TIP)));

    // A fill was attempted, so the cold start armed even with backfill switched off.
    wait_until_matched(&pruned, 30, "a cold-start gap-fill attempt").await;
    assert_eq!(
        ys.call_count("subscribe"),
        1,
        "the gate must arm on the first connection, with no reconnect involved"
    );

    // The live block landed, but the checkpoint may not pass the window under it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        assert_eq!(
            anchor_of(&pool, "withdraw").await,
            Some(TIP),
            "checkpoint must never advance past the unfilled cold-start window"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Heal the boundary; the fill closes the window and the gate hands off.
    let _healed = mock_block_ok(&mut rpc, TIP).await;
    wait_for_checkpoint(&pool, "withdraw", COLD_START_TIP, 60).await;

    handle.abort();
    ys.shutdown().await;
}

/// An RPC node trailing the Geyser provider that wrote the checkpoint is routine, so the
/// floor sitting above the tip it reports must not stop the boot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_starts_when_the_rpc_node_lags_the_durable_checkpoint() {
    init_tracing();
    let (_pg, pool, postgres) = start_postgres("startup_anchor_lagging_rpc").await;

    // Seed a checkpoint the endpoint below has not caught up to yet.
    let seeded = Storage::Postgres(PostgresDb::new(&postgres).await.expect("postgres storage"));
    seeded.init_schema().await.expect("init schema");
    seeded
        .update_committed_checkpoint("withdraw", LAGGING_CHECKPOINT)
        .await
        .expect("seed checkpoint");
    drop(seeded);

    let mut rpc = MockitoServer::new_async().await;
    let _slot = mock_get_slot(&mut rpc, LAGGING_CHECKPOINT - 2).await;

    let ys = MockYellowstoneServer::start().await;
    let backfill = BackfillConfig {
        enabled: false,
        exit_after_backfill: false,
        rpc_url: rpc.url(),
        batch_size: 100,
        max_gap_slots: 1_000,
        start_slot: None,
    };
    let indexer = indexer_config(ys.url(), backfill);
    let common = common_config(postgres, rpc.url());

    let (err_tx, mut err_rx) = tokio::sync::mpsc::channel::<String>(1);
    let handle = tokio::spawn(async move {
        if let Err(e) = run(common, indexer, None).await {
            let _ = err_tx.send(e.to_string()).await;
        }
    });

    // A startup that refuses the lag would land here well inside the window.
    if let Ok(Some(e)) = tokio::time::timeout(Duration::from_secs(20), err_rx.recv()).await {
        panic!("a lagging RPC node must not stop startup: {e}");
    }
    assert_eq!(
        anchor_of(&pool, "withdraw").await,
        Some(LAGGING_CHECKPOINT),
        "the durable checkpoint must survive a tip read below it"
    );

    handle.abort();
    ys.shutdown().await;
}
