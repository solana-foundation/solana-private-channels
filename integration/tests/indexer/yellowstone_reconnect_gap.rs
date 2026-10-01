//! Gap recovery for `YellowstoneSource`. Every connection, cold start or reconnect, arms on the
//! slot it resumed at and replays from `checkpoint - 1`; inserts are idempotent, so that is safe.

use mockito::{Matcher, Server as MockitoServer};
use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::types::ProcessorMessage;
use private_channel_indexer::indexer::datasource::rpc_polling::rpc::RpcPoller;
use private_channel_indexer::indexer::datasource::yellowstone::YellowstoneSource;
use private_channel_indexer::storage::common::storage::mock::MockStorage;
use private_channel_indexer::storage::Storage;
use serde_json::json;
use solana_commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[path = "yellowstone_helpers.rs"]
mod yellowstone_helpers;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use yellowstone_helpers::{block_after, empty_block, ping_update, slot_update};

/// An empty block chained onto `slot - 1`. It also answers the signatures view, so the
/// escrow gap-fill can confirm it empty.
fn empty_block_json(slot: u64) -> serde_json::Value {
    json!({
        "blockhash": "TestBlockHash11111111111111111111111111111",
        "parentSlot": slot - 1,
        "transactions": [],
        "signatures": []
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

    // The fill targets the observed resume slot (106 below), not a tip probe, so
    // no getSlot mock is needed. Anchor = checkpoint-1 = 100 => backfill 101..=106.
    // Empty blocks => only SlotComplete markers. Slot 101 was also streamed;
    // replay is harmless thanks to idempotent inserts in prod.
    // v2 enumerates the batch before fetching it, so this mock is required even
    // though nothing here is absent. Unmocked, mockito answers 501 with an empty
    // body, which becomes a decode error the gap-fill retry loop swallows.
    let _enumeration = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [101, 106]}),
        ))
        .with_status(200)
        .with_body(
            json!({"jsonrpc": "2.0", "result": [101, 102, 103, 104, 105, 106], "id": 1})
                .to_string(),
        )
        .expect_at_least(1)
        .create_async()
        .await;

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
                    "result": empty_block_json(slot),
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
    .with_gap_detection(rpc_poller, None, 1_000, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Phase 1: deliver slots 100, 101 pre-disconnect.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(100)));
    server.enqueue(UpdateMatcher, Update::ok(empty_block(101)));

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

    // Phase 2: drop the stream. On resubscribe the first live block (106) becomes the
    // gate target, and the concurrent backfill fills 101..=106.
    server.drop_stream();

    // Phase 3: queue 106,107,108. The 106 resume slot sets the fill target; 107,108
    // prove streaming continues past the backfilled window.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(106)));
    server.enqueue(UpdateMatcher, Update::ok(empty_block(107)));
    server.enqueue(UpdateMatcher, Update::ok(empty_block(108)));

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

/// The bug, end to end. With no anchor there is no lower bound, so the resuming slot must be
/// withheld; forwarding it is what carried the checkpoint over slots nothing was listening for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_start_without_anchor_withholds_live_slots() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let mut rpc_mock = MockitoServer::new_async().await;

    // Any repair attempt would land here; without an anchor none may be made.
    let no_blocks = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getBlock"})))
        .expect(0)
        .create_async()
        .await;
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

    // No checkpoint seeded, so the source has never recorded a recovery anchor.
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
    .with_gap_detection(rpc_poller, None, 1_000, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Phase 1: the cold start must withhold too; hold well past the 5s arm backoff.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(100)));
    let cold_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut cold_leaked = vec![];
    while tokio::time::Instant::now() < cold_deadline {
        if let Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) =
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
        {
            cold_leaked.push(slot);
        }
    }
    assert!(
        cold_leaked.is_empty(),
        "the first connection may not forward without a durable anchor; leaked: {cold_leaked:?}"
    );
    assert_eq!(
        server.remaining_scripted(),
        0,
        "slot 100 must have been delivered to the source and withheld, not left unsent"
    );

    // Phase 2: drop and resume at 102, the slot that would carry the checkpoint over 101.
    server.drop_stream();
    server.enqueue(UpdateMatcher, Update::ok(empty_block(102)));

    // Hold well past the 5s retry backoff: a regression forwards 102 almost immediately.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let mut leaked = vec![];
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) =
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
        {
            leaked.push(slot);
        }
    }

    assert!(
        leaked.is_empty(),
        "no slot may be forwarded without a durable anchor; leaked: {leaked:?}"
    );
    no_blocks.assert_async().await;
    // Waiting for an anchor parks inside the live connection, so the drop is never seen and
    // nothing resubscribes. Phase 1 already proved the block reached the source and was held.
    assert_eq!(
        server.call_count("subscribe"),
        1,
        "the anchor wait holds the connection open instead of cycling it"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}

/// Silent-stall watchdog: a stream that stays open but stops emitting (no FIN, no
/// error, no server ping) must be force-reconnected. This is the prod failure that
/// wedged the escrow indexer for ~12h: `stream.next()` blocked forever because
/// nothing tripped the reconnect path. The mock holds the stream open once its
/// queue drains, so an empty queue past the stall window reproduces it exactly.
/// No gap detection here — this isolates the watchdog from the backfill path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stall_watchdog_forces_reconnect_on_silent_stream() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let server = MockYellowstoneServer::start().await;

    let (tx, mut rx) = mpsc::channel::<ProcessorMessage>(64);
    let cancel = CancellationToken::new();

    // Short watchdog so the test drives the reconnect without the 60s prod wait.
    let mut source = YellowstoneSource::new(
        server.url(),
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    )
    .with_stall_timeout(Duration::from_millis(300));

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Deliver one slot so the first connection is streaming, then stop: the mock
    // holds the stream open and silent, arming the watchdog.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(100)));
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(
        matches!(
            first,
            Ok(Some(ProcessorMessage::SlotComplete { slot: 100, .. }))
        ),
        "expected slot 100 before the stall; got {first:?}"
    );

    // Watchdog (300ms) fires, then the Err path backs off 5s before resubscribing,
    // so allow generous headroom for the second handshake.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while server.call_count("subscribe") < 2 {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "silent stream must force a reconnect; subscribe count stuck at {}",
                server.call_count("subscribe")
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        server.call_count("subscribe") >= 2,
        "watchdog should resubscribe after a silent stall; got {}",
        server.call_count("subscribe")
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}

/// Startup catch-up that finishes before the stream starts leaves a window between its
/// target and the first streamed slot. Nothing covers that window: the catch-up is done
/// and the stream begins above it, so the first streamed slot would carry the checkpoint
/// straight over it. With first-connection arming the source replays it instead.
///
/// Without the flag the source treats connection one as a cold start, emits no Regate,
/// and slots 102 to 109 are lost while the checkpoint jumps to 110.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_connection_arms_when_startup_backfill_anchored() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let mut rpc_mock = MockitoServer::new_async().await;

    // The window the startup fill did not reach: anchor 101, first streamed slot 110.
    let _enumeration = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [101, 110]}),
        ))
        .with_status(200)
        .with_body(
            json!({
                "jsonrpc": "2.0",
                "result": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
                "id": 1
            })
            .to_string(),
        )
        .expect_at_least(1)
        .create_async()
        .await;

    let mut block_mocks = Vec::new();
    for slot in 101u64..=110u64 {
        let m = rpc_mock
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({"method": "getBlock", "params": [slot]}),
            ))
            .with_status(200)
            .with_body(
                json!({"jsonrpc": "2.0", "result": empty_block_json(slot), "id": 1}).to_string(),
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

    // The checkpoint a completed startup fill would have committed.
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
    .with_gap_detection(rpc_poller, None, 1_000, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // One connection only, no drop: the very first streamed slot must trigger the repair.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(110)));

    let mut regate: Option<(u64, u64)> = None;
    let mut seen: HashSet<u64> = HashSet::new();
    let wanted: HashSet<u64> = (102u64..=110u64).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while regate.is_none() || !wanted.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out; regate: {:?}, seen: {:?}, missing: {:?}",
                regate,
                seen,
                wanted.difference(&seen).collect::<Vec<_>>()
            );
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) => {
                seen.insert(slot);
            }
            Ok(Some(ProcessorMessage::Regate { from, target, .. })) => {
                regate = Some((from, target));
            }
            Ok(Some(_)) => {}
            _ => {}
        }
    }

    assert_eq!(
        regate,
        Some((101, 110)),
        "the gate must be armed from the durable anchor up to the first streamed slot"
    );
    assert!(
        wanted.is_subset(&seen),
        "every slot in the uncovered window must be replayed; seen: {seen:?}"
    );
    assert_eq!(
        server.call_count("subscribe"),
        1,
        "the repair must happen on the first connection, without a reconnect"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}

/// A long run of skipped slots can separate the resume slot from the next block. Arming on
/// the block would count those skipped slots as a gap and trip the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quiet_program_arms_on_the_resume_slot_not_the_first_block() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    const CHECKPOINT: u64 = 100;
    const RESUME: u64 = 103;
    // Far above the resume slot; its parent is RESUME, so every slot between was skipped.
    const FIRST_BLOCK: u64 = 5_000;
    // Comfortably smaller than FIRST_BLOCK - CHECKPOINT, so arming there would fail closed.
    const MAX_GAP: u64 = 100;

    let mut rpc_mock = MockitoServer::new_async().await;
    let _enumeration = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [CHECKPOINT, RESUME]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": [100, 101, 102, 103], "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;
    let mut block_mocks = Vec::new();
    for slot in CHECKPOINT..=RESUME {
        block_mocks.push(
            rpc_mock
                .mock("POST", "/")
                .match_body(Matcher::PartialJson(
                    json!({"method": "getBlock", "params": [slot]}),
                ))
                .with_status(200)
                .with_body(
                    json!({"jsonrpc": "2.0", "result": empty_block_json(slot), "id": 1})
                        .to_string(),
                )
                .expect_at_least(1)
                .create_async()
                .await,
        );
    }
    // Nothing may be fetched from the idle stretch above the resume slot.
    let idle_stretch = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [FIRST_BLOCK]}),
        ))
        .expect(0)
        .create_async()
        .await;

    let server = MockYellowstoneServer::start().await;
    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_mock.url(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Confirmed,
    ));
    let mock_storage = MockStorage::new();
    mock_storage.set_checkpoint("escrow", CHECKPOINT);
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
    .with_gap_detection(rpc_poller, None, MAX_GAP, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // The stream resumes at RESUME, and the next block is FIRST_BLOCK.
    server.enqueue(UpdateMatcher, Update::ok(slot_update(RESUME)));
    server.enqueue(
        UpdateMatcher,
        Update::ok(block_after(FIRST_BLOCK, RESUME, vec![])),
    );

    let mut regate: Option<(u64, u64)> = None;
    let mut seen: HashSet<u64> = HashSet::new();
    let wanted: HashSet<u64> = (CHECKPOINT..=RESUME).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while regate.is_none() || !wanted.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out; regate: {regate:?}, seen: {seen:?}");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) => {
                seen.insert(slot);
            }
            Ok(Some(ProcessorMessage::Regate { from, target, .. })) => {
                regate = Some((from, target));
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("channel closed early"),
            Err(_) => panic!("timed out; regate: {regate:?}, seen: {seen:?}"),
        }
    }

    assert_eq!(
        regate,
        Some((CHECKPOINT, RESUME)),
        "the gate must target the resume slot, not the first program block"
    );
    idle_stretch.assert_async().await;

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    server.shutdown().await;
}

/// A provider that sends no slot updates must still gate, falling back to the first block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_block_still_arms_when_no_slot_update_arrives() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    const CHECKPOINT: u64 = 100;
    const TIP: u64 = 103;

    let mut rpc_mock = MockitoServer::new_async().await;
    let _enumeration = rpc_mock
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlocks", "params": [CHECKPOINT, TIP]}),
        ))
        .with_status(200)
        .with_body(json!({"jsonrpc": "2.0", "result": [100, 101, 102, 103], "id": 1}).to_string())
        .expect_at_least(1)
        .create_async()
        .await;
    let mut block_mocks = Vec::new();
    for slot in CHECKPOINT..=TIP {
        block_mocks.push(
            rpc_mock
                .mock("POST", "/")
                .match_body(Matcher::PartialJson(
                    json!({"method": "getBlock", "params": [slot]}),
                ))
                .with_status(200)
                .with_body(
                    json!({"jsonrpc": "2.0", "result": empty_block_json(slot), "id": 1})
                        .to_string(),
                )
                .expect_at_least(1)
                .create_async()
                .await,
        );
    }

    let server = MockYellowstoneServer::start().await;
    let rpc_poller = Arc::new(RpcPoller::new(
        rpc_mock.url(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Confirmed,
    ));
    let mock_storage = MockStorage::new();
    mock_storage.set_checkpoint("escrow", CHECKPOINT);
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
    .with_gap_detection(rpc_poller, None, 1_000, 16)
    .with_storage(storage);

    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    // Blocks only: no slot update is ever delivered on this stream.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(TIP)));

    let mut regate: Option<(u64, u64)> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while regate.is_none() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::Regate { from, target, .. })) => {
                regate = Some((from, target));
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("channel closed early"),
            Err(_) => panic!("no gate was armed without a slot update"),
        }
    }
    assert_eq!(regate, Some((CHECKPOINT, TIP)));

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    server.shutdown().await;
}

// Progress watchdog: only a Slot or Block at a new highest slot proves the stream is live.

/// Wide enough that CI scheduling jitter cannot fake or hide a stall.
const WATCHDOG_WINDOW: Duration = Duration::from_secs(1);

/// A source with no gap repair and a short watchdog, sending into a channel of capacity `cap`.
async fn start_watchdog_source(
    server: &MockYellowstoneServer,
    cap: usize,
) -> (
    mpsc::Receiver<ProcessorMessage>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel::<ProcessorMessage>(cap);
    let cancel = CancellationToken::new();
    let mut source = YellowstoneSource::new(
        server.url(),
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    )
    .with_stall_timeout(WATCHDOG_WINDOW);
    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");
    (rx, cancel, handle)
}

/// Keeps the processor channel empty so the source never waits on it.
fn spawn_drain(mut rx: mpsc::Receiver<ProcessorMessage>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { while rx.recv().await.is_some() {} })
}

/// True once the source has opened a second subscription, i.e. it reconnected.
fn reconnected(server: &MockYellowstoneServer) -> bool {
    server.call_count("subscribe") > 1
}

/// Enqueues `make(i)` every `every` for up to `budget` and returns whether the source reconnected.
async fn feed_until_reconnect(
    server: &MockYellowstoneServer,
    every: Duration,
    budget: Duration,
    make: impl Fn(u64) -> SubscribeUpdate,
) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    let mut i = 0;
    while tokio::time::Instant::now() < deadline {
        if reconnected(server) {
            return true;
        }
        server.enqueue(UpdateMatcher, Update::ok(make(i)));
        i += 1;
        tokio::time::sleep(every).await;
    }
    reconnected(server)
}

async fn stop_watchdog_source(
    server: MockYellowstoneServer,
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
) {
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    server.shutdown().await;
}

/// The finding: a stream that keeps pinging but sends no chain data must still be torn down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_only_stream_forces_reconnect() {
    let server = MockYellowstoneServer::start().await;
    let (mut rx, cancel, handle) = start_watchdog_source(&server, 64).await;

    server.enqueue(UpdateMatcher, Update::ok(empty_block(100)));
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(
        matches!(
            first,
            Ok(Some(ProcessorMessage::SlotComplete { slot: 100, .. }))
        ),
        "expected slot 100 before the pings; got {first:?}"
    );

    // Pings keep flowing until the assert, so a silent tail cannot be what trips the watchdog.
    let did_reconnect = feed_until_reconnect(
        &server,
        Duration::from_millis(50),
        Duration::from_secs(15),
        |_| ping_update(),
    )
    .await;
    assert!(
        did_reconnect,
        "pings alone must not keep the stream alive; subscribe count stuck at {}",
        server.call_count("subscribe")
    );

    stop_watchdog_source(server, cancel, handle).await;
}

/// Slot updates alone are chain progress, so a stream carrying only them is never torn down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advancing_slots_keep_the_stream_alive() {
    let server = MockYellowstoneServer::start().await;
    let (rx, cancel, handle) = start_watchdog_source(&server, 64).await;
    let drain = spawn_drain(rx);

    // Longer than the window plus the 5s reconnect backoff, so any stall would show.
    let did_reconnect = feed_until_reconnect(
        &server,
        Duration::from_millis(100),
        Duration::from_secs(7),
        |i| slot_update(100 + i),
    )
    .await;
    assert!(
        !did_reconnect,
        "advancing slots must keep the stream alive; subscribe count {}",
        server.call_count("subscribe")
    );

    stop_watchdog_source(server, cancel, handle).await;
    drain.abort();
}

/// Waiting on a slow processor is not stream silence, even for a block whose slot was already seen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_processor_does_not_trip_the_watchdog() {
    let server = MockYellowstoneServer::start().await;
    // Capacity 1: block 100 fills the channel, so block 101 waits until the test drains it.
    let (rx, cancel, handle) = start_watchdog_source(&server, 1).await;

    server.enqueue_sequence([
        Update::ok(empty_block(100)),
        Update::ok(slot_update(101)),
        Update::ok(empty_block(101)),
    ]);
    tokio::time::sleep(WATCHDOG_WINDOW * 3).await;
    let drain = spawn_drain(rx);

    // Nothing is queued yet, so right after block 101 unblocks only the watchdog can fire.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The block tail also shows blocks alone keep the stream alive.
    let did_reconnect = feed_until_reconnect(
        &server,
        Duration::from_millis(100),
        Duration::from_secs(7),
        |i| empty_block(102 + i),
    )
    .await;
    assert!(
        !did_reconnect,
        "a slow processor must not trip the watchdog; subscribe count {}",
        server.call_count("subscribe")
    );

    stop_watchdog_source(server, cancel, handle).await;
    drain.abort();
}
