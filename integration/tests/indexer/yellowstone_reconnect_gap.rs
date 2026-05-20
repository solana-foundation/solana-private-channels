//! Reconnect-gap recovery for `YellowstoneSource`.
//!
//! After a Yellowstone disconnect, the source reads the durable checkpoint
//! from storage and passes `checkpoint - 1`
//! to `fill_slot_range` so the boundary slot is replayed via RPC. Tx/mint
//! inserts are idempotent, so replaying is safe.

use mockito::{Matcher, Server as MockitoServer};
use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::types::ProcessorMessage;
use private_channel_indexer::indexer::datasource::rpc_polling::rpc::RpcPoller;
use private_channel_indexer::indexer::datasource::yellowstone::YellowstoneSource;
use private_channel_indexer::storage::common::storage::mock::MockStorage;
use private_channel_indexer::storage::Storage;
use serde_json::json;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlockMeta,
};

fn block_meta(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["all_blocks_meta".to_string()],
        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: format!("hash-{slot}"),
            ..Default::default()
        })),
        created_at: None,
    }
}

fn empty_block_json() -> serde_json::Value {
    json!({
        "blockhash": "TestBlockHash11111111111111111111111111111",
        "parentSlot": 0,
        "transactions": []
    })
}

/// Happy-path: checkpoint=101 → stream 100,101 → drop → backfill 101..=106
/// inclusive (anchor = checkpoint-1 = 100) → resume streaming 107,108.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gap_fill_runs_after_drop_stream() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    // In-process mockito RPC backend for the RpcPoller backfill path.
    let mut rpc_mock = MockitoServer::new_async().await;

    // Chain tip = 106. Anchor = checkpoint-1 = 100 → backfill 101..=106.
    let _slot_mock = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": 106, "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;

    // Empty blocks → only SlotComplete markers. Slot 101 was also streamed;
    // replay is harmless thanks to idempotent inserts in prod.
    let mut block_mocks = Vec::new();
    for slot in 101u64..=106u64 {
        let m = rpc_mock
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({"method": "getBlock", "params": [slot]}),
            ))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "result": empty_block_json(),
                    "id": 1,
                })
                .to_string(),
            )
            .expect_at_least(1)
            .create_async()
            .await;
        block_mocks.push(m);
    }

    let server = MockYellowstoneServer::start().await;

    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_mock.url(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Confirmed,
    ));

    // Pre-seed durable checkpoint = 101. In prod the processor advances it.
    let mock_storage = MockStorage::new();
    mock_storage.set_checkpoint("escrow", 101);
    let storage: Arc<Storage> = Arc::new(Storage::Mock(mock_storage));

    let (tx, mut rx) = mpsc::channel::<ProcessorMessage>(256);
    let cancel = CancellationToken::new();

    let mut source = YellowstoneSource::new(
        server.url(),
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    )
    .with_gap_detection(rpc_poller, 1_000, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Phase 1: deliver slots 100, 101 pre-disconnect.
    server.enqueue(UpdateMatcher, Update::ok(block_meta(100)));
    server.enqueue(UpdateMatcher, Update::ok(block_meta(101)));

    // Collect both initial slots.
    let mut seen: HashSet<u64> = HashSet::new();
    let deadline_phase1 = tokio::time::Instant::now() + Duration::from_secs(5);
    while !(seen.contains(&100) && seen.contains(&101)) {
        let remaining = deadline_phase1.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("phase 1 timed out; seen: {:?}", seen);
        }
        if let Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) =
            tokio::time::timeout(remaining, rx.recv()).await
        {
            seen.insert(slot);
        }
    }

    // Phase 2: drop the stream → source reads checkpoint and backfills 101..=106.
    server.drop_stream();

    // Phase 3: queue 107,108 to prove streaming resumes post-backfill.
    server.enqueue(UpdateMatcher, Update::ok(block_meta(107)));
    server.enqueue(UpdateMatcher, Update::ok(block_meta(108)));

    // Expect 101..=106 from inclusive backfill + 107,108 from resumed stream.
    let deadline_phase2 = tokio::time::Instant::now() + Duration::from_secs(20);
    let wanted: HashSet<u64> = (101u64..=108u64).collect();
    while !wanted.is_subset(&seen) {
        let remaining = deadline_phase2.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "phase 2 timed out waiting for backfill + resumed stream; \
                 seen so far: {:?}, missing: {:?}",
                seen,
                wanted.difference(&seen).collect::<Vec<_>>()
            );
        }
        if let Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) =
            tokio::time::timeout(remaining, rx.recv()).await
        {
            seen.insert(slot);
        }
    }

    assert!(
        wanted.is_subset(&seen),
        "expected all gap + post-reconnect slots in processor channel; \
         seen: {:?}",
        seen
    );
    assert!(
        server.call_count("subscribe") >= 2,
        "drop_stream + resume should produce ≥2 subscribe handshakes; got {}",
        server.call_count("subscribe")
    );

    // Teardown.
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}

/// Error path: fresh system (checkpoint=0) must skip reconnect gap-fill —
/// no RPC backfill calls, no spurious SlotCompletes. Startup backfill (when
/// configured) is responsible for initial catch-up, not the reconnect path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_system_reconnect_does_not_gap_fill() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let mut rpc_mock = MockitoServer::new_async().await;

    // No getBlock mocks — any call would panic mockito's matcher. getSlot is
    // tolerated (the reconnect path may probe it before checking checkpoint).
    let _slot_mock = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": 200, "id": 1}).to_string())
        .expect_at_most(1)
        .create_async()
        .await;

    let server = MockYellowstoneServer::start().await;

    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_mock.url(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Confirmed,
    ));

    // No checkpoint seeded → get_last_checkpoint returns 0 → skip path.
    let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));

    let (tx, mut rx) = mpsc::channel::<ProcessorMessage>(64);
    let cancel = CancellationToken::new();

    let mut source = YellowstoneSource::new(
        server.url(),
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    )
    .with_gap_detection(rpc_poller, 1_000, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Stream one slot, drop, give the reconnect path time to run.
    server.enqueue(UpdateMatcher, Update::ok(block_meta(100)));
    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    server.drop_stream();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drain the channel — only the streamed slot 100 should appear, never
    // any backfill SlotCompletes (which would mean we contacted the RPC).
    let mut all_slots = vec![];
    while let Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) =
        tokio::time::timeout(Duration::from_millis(50), rx.recv()).await
    {
        all_slots.push(slot);
    }

    let unexpected: Vec<_> = all_slots.iter().filter(|&&s| s != 100).collect();
    assert!(
        unexpected.is_empty(),
        "fresh system must NOT trigger gap-fill; unexpected slots: {:?}",
        unexpected
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}
