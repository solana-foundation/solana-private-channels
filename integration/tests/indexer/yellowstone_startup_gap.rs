//! Startup gap-fill for `YellowstoneSource`.
//!
//! The gRPC stream begins at the live tip, so on the very first connection the
//! slots between the backfill target (the durable frontier) and the first
//! streamed slot are covered by neither backfill nor the live stream. With an
//! initial gap floor set, the source runs the RPC gap-fill once before the
//! first subscription to close that window. A fresh node (floor `None`) runs no
//! initial fill. Mirrors the harness of `yellowstone_reconnect_gap.rs`.

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

// Durable frontier the startup path established (inclusive backfill target).
const FLOOR: u64 = 110;
// RPC chain tip at startup. Gap-fill replays FLOOR..=TIP (anchor = FLOOR-1).
const TIP: u64 = 116;
// First live slot streamed after the fill.
const LIVE_SLOT: u64 = 117;

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

/// With an initial gap floor, the source backfills FLOOR..=TIP via RPC before
/// the first subscription and then streams the live slot - no stream drop, so
/// the gap slots can only have come from the startup fill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_gap_fill_runs_before_first_stream() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let mut rpc_mock = MockitoServer::new_async().await;

    let _slot_mock = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": TIP, "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;

    // Fill replays the boundary slot FLOOR (anchor = FLOOR-1) through TIP.
    let mut block_mocks = Vec::new();
    for slot in FLOOR..=TIP {
        let m = rpc_mock
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({"method": "getBlock", "params": [slot]}),
            ))
            .with_status(200)
            .with_body(json!({"jsonrpc": "2.0", "result": empty_block_json(), "id": 1}).to_string())
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

    // Frontier carries the checkpoint; storage is only the presence guard.
    let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));

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
    .with_storage(storage)
    .with_initial_gap_floor(Some(FLOOR));

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Live slot after the fill - buffered by the mock until subscribe consumes it.
    server.enqueue(UpdateMatcher, Update::ok(block_meta(LIVE_SLOT)));

    // The gap (FLOOR+1..=TIP) plus the live slot must all reach the channel.
    let wanted: HashSet<u64> = ((FLOOR + 1)..=TIP)
        .chain(std::iter::once(LIVE_SLOT))
        .collect();
    let mut seen: HashSet<u64> = HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !wanted.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out; seen: {:?}, missing: {:?}",
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
        "startup fill (no drop) must deliver gap + live slots; seen: {:?}",
        seen
    );
    assert!(
        server.call_count("subscribe") >= 1,
        "expected at least one subscribe handshake; got {}",
        server.call_count("subscribe")
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}

/// A fresh node (floor `None`) runs no startup fill: only the streamed slot
/// appears, never a backfill SlotComplete - the Yellowstone-path Downside A
/// guard (no genesis replay on a fresh node).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_startup_no_initial_fill() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let mut rpc_mock = MockitoServer::new_async().await;

    // No getBlock mocks; getSlot tolerated but must not be needed for a fresh node.
    let _slot_mock = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": 200, "id": 1}).to_string())
        .expect_at_most(0)
        .create_async()
        .await;

    let server = MockYellowstoneServer::start().await;

    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_mock.url(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Confirmed,
    ));

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
    .with_storage(storage)
    .with_initial_gap_floor(None);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    server.enqueue(UpdateMatcher, Update::ok(block_meta(100)));
    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    // Settle any spurious fill activity (there should be none).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut all_slots = vec![];
    while let Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) =
        tokio::time::timeout(Duration::from_millis(50), rx.recv()).await
    {
        all_slots.push(slot);
    }

    let unexpected: Vec<_> = all_slots.iter().filter(|&&s| s != 100).collect();
    assert!(
        unexpected.is_empty(),
        "fresh node must NOT run a startup fill; unexpected slots: {:?}",
        unexpected
    );
    // No RPC at all: the floor-None guard skips the fill before any getSlot probe.
    _slot_mock.assert_async().await;

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}
