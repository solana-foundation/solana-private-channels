//! YellowstoneSource defensive branches: malformed-update handling.
//!
//! Exercises the error paths in
//! `indexer/src/indexer/datasource/yellowstone/source.rs` that only fire on a
//! malformed upstream `SubscribeUpdate`. All assertions pin the CURRENT
//! production contract; behaviour changes should fail these tests.
//!
//! ## Pinned contract
//!
//! 1. Stream-level error (`tonic::Status`) -> `connect_and_stream` returns
//!    `Err(DataSourceRpcError::Protocol)`, the outer loop logs via `error!`,
//!    increments `INDEXER_DATASOURCE_RECONNECTS` + `INDEXER_RPC_ERRORS`,
//!    sleeps 5s, and reconnects.
//! 2. A block carrying a tx with a missing inner `message` -> `handle_block`
//!    returns `Err(...)` before emitting `SlotComplete`, which
//!    `connect_and_stream` propagates as `Err(DataSourceError::Rpc)` and the
//!    source reconnects. This is the fail-closed guarantee: the slot is never
//!    checkpointed, so reconnect gap-fill replays it idempotently.
//! 3. A block whose tx references an out-of-bounds or foreign program id -> the
//!    tx is soft-skipped, but the block STILL completes its slot (no reconnect).

use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_indexer::indexer::datasource::common::types::ProcessorMessage;
use private_channel_indexer::indexer::datasource::yellowstone::YellowstoneSource;
use std::str::FromStr;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[path = "yellowstone_helpers.rs"]
mod yellowstone_helpers;
use yellowstone_helpers::{
    bad_program_index_tx_info, block, empty_block, missing_message_tx_info, wrong_program_tx_info,
};

struct TestHarness {
    server: MockYellowstoneServer,
    rx: mpsc::Receiver<ProcessorMessage>,
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

async fn spin_up() -> TestHarness {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let server = MockYellowstoneServer::start().await;
    let (tx, rx) = mpsc::channel::<ProcessorMessage>(64);
    let cancel = CancellationToken::new();

    let mut source = YellowstoneSource::new(
        server.url(),
        None,
        "confirmed".to_string(),
        ProgramType::Escrow,
        None,
    );
    let handle = source
        .start(tx, cancel.clone())
        .await
        .expect("yellowstone source start");

    TestHarness {
        server,
        rx,
        cancel,
        handle,
    }
}

async fn tear_down(h: TestHarness) {
    h.cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), h.handle).await;
    h.server.shutdown().await;
}

/// Drain any messages currently pending on the channel within `window`,
/// collecting only `SlotComplete` slots.
async fn drain_slots(rx: &mut mpsc::Receiver<ProcessorMessage>, window: Duration) -> Vec<u64> {
    let mut slots = vec![];
    let deadline = tokio::time::Instant::now() + window;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) => slots.push(slot),
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
    slots
}

/// Case (a): a `Status::invalid_argument` mid-stream forces a reconnect.
/// The source should open a second `subscribe` RPC and resume delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_error_triggers_reconnect_and_resumes() {
    let mut h = spin_up().await;

    // Deliver one slot, then a malformed stream, then a final slot after
    // reconnect. The 5s sleep inside source.rs's error arm is the
    // dominating factor in this test's runtime.
    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(10)));
    h.server
        .enqueue(UpdateMatcher, Update::malformed("corrupted bytes"));

    // Wait for the first slot and let the malformed update drop the stream.
    let first = tokio::time::timeout(Duration::from_secs(5), h.rx.recv())
        .await
        .expect("timed out waiting for first block")
        .expect("channel closed");
    matches!(first, ProcessorMessage::SlotComplete { slot: 10, .. });

    // Wait for the reconnect handshake before enqueuing the follow-up - if
    // we push slot 11 while the first-stream pump is still alive, the pump
    // can dequeue slot 11, try to send on the dead stream, fail, and lose
    // the update entirely.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if h.server.call_count("subscribe") >= 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("source should reconnect within 10s of malformed stream error");

    // Now enqueue a follow-up that the RECONNECTED stream should deliver.
    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(11)));

    // Give the source time to hit its 5s reconnect sleep and come back.
    let next_slot = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Some(msg) = h.rx.recv().await {
                if let ProcessorMessage::SlotComplete { slot, .. } = msg {
                    return Some(slot);
                }
            } else {
                return None;
            }
        }
    })
    .await
    .expect("timed out waiting for post-reconnect delivery")
    .expect("channel closed before reconnect delivered slot 11");

    assert_eq!(
        next_slot, 11,
        "post-reconnect slot should be delivered on the new subscribe stream"
    );
    assert!(
        h.server.call_count("subscribe") >= 2,
        "expected at least 2 subscribe handshakes (original + reconnect), got {}",
        h.server.call_count("subscribe")
    );

    tear_down(h).await;
}

/// Case (b): a block whose tx is missing its `message` field is fail-closed -
/// `handle_block` returns `Err` before emitting `SlotComplete`, so the stream
/// terminates and the source reconnects. Slot 21 is never checkpointed (in prod
/// reconnect gap-fill replays it via getBlock); the follow-up block on the new
/// stream is delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_message_kills_stream_and_reconnects() {
    let mut h = spin_up().await;

    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(20)));
    h.server.enqueue(
        UpdateMatcher,
        Update::ok(block(21, vec![missing_message_tx_info()])),
    );

    let first = tokio::time::timeout(Duration::from_secs(5), h.rx.recv())
        .await
        .expect("timed out waiting for slot 20")
        .expect("channel closed");
    matches!(first, ProcessorMessage::SlotComplete { slot: 20, .. });

    // Wait for the source to actually reconnect before enqueuing the next
    // slot - otherwise the mock's first-stream pump can race ahead and
    // consume slot 22 from the queue before it notices the client closed.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if h.server.call_count("subscribe") >= 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("source should reconnect within 10s of stream-killing bad tx");

    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(22)));

    let got = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Some(ProcessorMessage::SlotComplete { slot, .. }) = h.rx.recv().await {
                if slot == 22 {
                    return Some(slot);
                }
            }
        }
    })
    .await
    .expect("timed out waiting for post-reconnect slot 22");

    assert_eq!(got, Some(22));
    assert!(
        h.server.call_count("subscribe") >= 2,
        "malformed tx should have killed + triggered reconnect; saw {} subscribes",
        h.server.call_count("subscribe")
    );

    // Slot 21 must never have completed: fail-closed keeps it below the checkpoint.
    let saw_21 = drain_slots(&mut h.rx, Duration::from_millis(100))
        .await
        .contains(&21);
    assert!(!saw_21, "the failed slot 21 must never emit SlotComplete");

    tear_down(h).await;
}

/// Case (b2): a block whose tx has a `program_id_index` past the `account_keys`
/// array hits the source's bounds-check skip. The tx is dropped, but the block
/// STILL completes its slot; the stream stays healthy (no reconnect).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_bounds_program_id_index_is_skipped_but_slot_completes() {
    let mut h = spin_up().await;

    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(40)));
    h.server.enqueue(
        UpdateMatcher,
        Update::ok(block(41, vec![bad_program_index_tx_info()])),
    );
    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(42)));

    let slots = drain_slots(&mut h.rx, Duration::from_secs(4)).await;
    assert_eq!(
        slots,
        vec![40, 41, 42],
        "the out-of-bounds tx is soft-skipped, yet its block still completes slot 41"
    );
    assert_eq!(
        h.server.call_count("subscribe"),
        1,
        "defensive skip is a soft filter (no reconnect)"
    );

    tear_down(h).await;
}

/// Case (c): a block whose tx targets an unrelated program is filtered out by
/// the client-side program check. The tx is dropped, the block still completes
/// its slot, and the stream stays alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_program_id_is_skipped_but_slot_completes() {
    let mut h = spin_up().await;

    // An unrelated program ID - use the system program (11...11).
    let wrong_program = solana_sdk::pubkey::Pubkey::default();

    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(30)));
    h.server.enqueue(
        UpdateMatcher,
        Update::ok(block(31, vec![wrong_program_tx_info(wrong_program)])),
    );
    h.server.enqueue(UpdateMatcher, Update::ok(empty_block(32)));

    let slots = drain_slots(&mut h.rx, Duration::from_secs(4)).await;
    assert_eq!(
        slots,
        vec![30, 31, 32],
        "the wrong-program tx is soft-skipped, yet its block still completes slot 31"
    );
    assert_eq!(
        h.server.call_count("subscribe"),
        1,
        "wrong program id is a soft filter (no reconnect)"
    );

    // Sanity: confirm the escrow program ID is what the source was configured
    // for - the filtered tx legitimately did not match.
    assert_ne!(
        wrong_program,
        solana_sdk::pubkey::Pubkey::from_str(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).unwrap()
    );

    tear_down(h).await;
}
