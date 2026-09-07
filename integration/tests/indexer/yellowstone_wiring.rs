//! End-to-end wiring test for `MockYellowstoneServer` + `YellowstoneSource`.
//!
//! Analogue of `mock_rpc_retry` for the Yellowstone gRPC datasource.
//! Validates the full pipe from a scripted `blocks` stream -> `YellowstoneSource`
//! -> `ProcessorMessage` channel. Confirms the mock is a drop-in substitute
//! for a real Yellowstone node and that the production source decodes a
//! scripted `SubscribeUpdate` block stream end-to-end.
//!
//! What's exercised here:
//!   - `MockYellowstoneServer::start` + `enqueue(Update::ok(...))`
//!   - `YellowstoneSource::start` -> connects over plain HTTP to the mock
//!   - `Block` path: an empty block and a block carrying an escrow `Deposit`
//!     surface as `SlotComplete` and `Instruction(Escrow(Deposit))`, with the
//!     deposit and its slot completion arriving in the same message, so there
//!     is no separate tx stream that can be late (the issue-#22 regression).
//!   - `call_count("subscribe") == 1` and `remaining_scripted == 0`

use private_channel_indexer::config::ProgramType;
use private_channel_indexer::indexer::datasource::common::datasource::DataSource;
use private_channel_indexer::indexer::datasource::common::types::{
    ProcessorMessage, ProgramInstruction,
};
use private_channel_indexer::indexer::datasource::yellowstone::YellowstoneSource;
use std::time::Duration;
use test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[path = "yellowstone_helpers.rs"]
mod yellowstone_helpers;
use yellowstone_helpers::{block, empty_block, escrow_deposit_tx_info};

/// End-to-end wiring: an empty block then a block carrying an escrow Deposit
/// land in the processor channel via YellowstoneSource, in order, with exactly
/// one subscribe handshake. The deposit and its slot completion arrive together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_stream_delivers_deposit_and_completes() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let server = MockYellowstoneServer::start().await;

    // Script an empty block then a block whose single tx is an escrow Deposit.
    server.enqueue(UpdateMatcher, Update::ok(empty_block(100)));
    server.enqueue(
        UpdateMatcher,
        Update::ok(block(101, vec![escrow_deposit_tx_info()])),
    );

    let (tx, mut rx) = mpsc::channel::<ProcessorMessage>(64);
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

    // Collect the slot completions and the deposit within a generous deadline.
    let mut slot_completes: Vec<u64> = vec![];
    let mut deposits_seen = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while slot_completes.len() < 2 || deposits_seen < 1 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let msg = match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(m)) => m,
            Ok(None) => break,
            Err(_) => break,
        };
        match msg {
            ProcessorMessage::SlotComplete { slot, .. } => slot_completes.push(slot),
            ProcessorMessage::Instruction(meta) => {
                if matches!(meta.instruction, ProgramInstruction::Escrow(ref b) if matches!(
                    **b,
                    private_channel_indexer::indexer::datasource::common::parser::EscrowInstruction::Deposit { .. }
                )) {
                    deposits_seen += 1;
                    assert_eq!(meta.slot, 101);
                }
            }
            // No reconnect in this test, so no gate re-arm is expected.
            ProcessorMessage::Regate { .. } => {}
        }
    }

    assert_eq!(
        slot_completes,
        vec![100, 101],
        "each produced block should complete its slot in FIFO order"
    );
    assert_eq!(
        deposits_seen, 1,
        "the scripted Deposit instruction should be parsed and forwarded"
    );
    assert_eq!(
        server.remaining_scripted(),
        0,
        "all scripted updates should have been consumed"
    );
    assert_eq!(
        server.call_count("subscribe"),
        1,
        "exactly one subscribe handshake expected on a clean stream"
    );

    // Shut down - cancel token tells the source to close its stream; the
    // server shutdown drains the gRPC endpoint.
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}
