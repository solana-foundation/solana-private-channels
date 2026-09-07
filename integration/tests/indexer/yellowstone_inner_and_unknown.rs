//! Yellowstone block-parse defensive arms.
//!
//! Covers two branches in
//! `indexer/src/indexer/datasource/yellowstone/source.rs`:
//!
//!   * inner_instructions branch - exercised by a block whose deposit tx carries
//!     one nested DepositEvent CPI in `meta.inner_instructions`. The branch
//!     walks the outer and inner vecs and builds the typed accumulator.
//!
//!   * unsupported/invalid escrow instruction arm - fed by a tx whose top-level
//!     discriminator the parser does not recognise (`Ok(None)`). The indexer
//!     filters the frame silently rather than erroring the stream, and the
//!     block carrying it still completes its slot.
//!
//! Same wiring as `yellowstone_wiring.rs`.

use {
    private_channel_indexer::{
        config::ProgramType,
        indexer::datasource::{
            common::{
                datasource::DataSource,
                types::{ProcessorMessage, ProgramInstruction},
            },
            yellowstone::YellowstoneSource,
        },
    },
    std::time::Duration,
    test_utils::mock_yellowstone::{MockYellowstoneServer, Update, UpdateMatcher},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

#[path = "yellowstone_helpers.rs"]
mod yellowstone_helpers;
use yellowstone_helpers::{block, escrow_deposit_tx_info, unknown_discriminator_tx_info};

/// Feeds:
///   1. block(200, [deposit with meta.inner_instructions])
///   2. block(202, [unknown-discriminator escrow tx])
///
/// Asserts:
///   - The deposit instruction surfaces on the processor channel (inner
///     parsing succeeds without breaking the outer frame).
///   - The unknown-discriminator tx is silently dropped - no
///     `ProgramInstruction` message, no error on the channel.
///   - Both slots emit `SlotComplete` ([200, 202]) - under the blocks model the
///     produced block carrying the skipped tx still completes its slot, which
///     the old two-stream test could not express.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yellowstone_handles_inner_instructions_and_unknown_discriminator() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,private_channel_indexer=debug")
        .with_test_writer()
        .try_init();

    let server = MockYellowstoneServer::start().await;
    server.enqueue(
        UpdateMatcher,
        Update::ok(block(200, vec![escrow_deposit_tx_info()])),
    );
    server.enqueue(
        UpdateMatcher,
        Update::ok(block(202, vec![unknown_discriminator_tx_info()])),
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

    let mut slot_completes: Vec<u64> = vec![];
    let mut deposits = 0usize;
    let mut other_instructions = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while slot_completes.len() < 2 || deposits < 1 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ProcessorMessage::SlotComplete { slot, .. })) => slot_completes.push(slot),
            Ok(Some(ProcessorMessage::Instruction(meta))) => match meta.instruction {
                ProgramInstruction::Escrow(ref b)
                    if matches!(
                        **b,
                        private_channel_indexer::indexer::datasource::common::parser::EscrowInstruction::Deposit { .. }
                    ) =>
                {
                    deposits += 1;
                }
                _ => {
                    other_instructions += 1;
                }
            },
            // No reconnect in this test, so no gate re-arm is expected.
            Ok(Some(ProcessorMessage::Regate { .. })) => {}
            Ok(None) | Err(_) => break,
        }
    }

    assert_eq!(
        slot_completes,
        vec![200, 202],
        "each produced block completes its slot, including the one carrying the skipped tx"
    );
    assert_eq!(
        deposits, 1,
        "the deposit-with-inner-instructions tx must surface (inner_instructions parsing succeeded)"
    );
    assert_eq!(
        other_instructions, 0,
        "unknown-discriminator tx must be silently filtered, not forwarded"
    );
    assert_eq!(
        server.remaining_scripted(),
        0,
        "every scripted update must be consumed"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(3), handle).await;
    server.shutdown().await;
}
