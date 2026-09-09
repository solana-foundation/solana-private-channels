//! `run_sender` cancellation drain arm.
//!
//! Covers the `cancellation_token.cancelled()` arm of `run_sender`'s
//! `tokio::select!` (`indexer/src/operator/sender/mod.rs`) — the path
//! that fires when the caller cancels the shared `CancellationToken`.
//! Behavior:
//!
//!   1. Log "Sender received cancellation signal, draining pipeline...".
//!   2. Drain the processor channel via `while let Some(tx) = recv()` —
//!      submitting every remaining builder through
//!      `handle_transaction_submission`.
//!   3. Cancel the poll-task shutdown token and `drop(poll_result_rx)`.
//!   4. Await the poll-task handle.
//!   5. Call `drain_in_flight` to resolve any still-pending fire-and-
//!      forget mints.
//!   6. Break the `select!` loop and return `Ok(())`.
//!
//! Strategy: mirror the channel-close test — call `pub run_sender`
//! directly with a pre-cancelled token and an empty processor channel
//! (sender dropped so the drain loop completes immediately). This
//! exercises the cancellation arm's exit path without needing scripted
//! RPC traffic or storage fixtures, complementing the `OperatorMockHarness`
//! tests which exit via handle drop rather than cooperative cancellation.

use {
    private_channel_indexer::{
        config::{
            PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
        },
        operator::run_sender,
        storage::{common::storage::mock::MockStorage, Storage},
    },
    solana_sdk::commitment_config::CommitmentLevel,
    std::{sync::Arc, time::Duration},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

fn mock_config() -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres: PostgresConfig {
            database_url: "mock://unused".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: None,
    }
}

/// Pre-cancelled token + empty-and-closed processor channel must drive
/// `run_sender` into the cancellation arm on the first `select!`
/// iteration, walk through the drain + poll-task shutdown +
/// `drain_in_flight` block, and return `Ok(())`.
///
/// What the arm does under these inputs:
///   - `while let Some(tx) = processor_rx.recv().await` — the sender half
///     is dropped so recv() yields None immediately; drained_count = 0.
///   - `poll_shutdown.cancel()` + `drop(poll_result_rx)` + `await`
///     poll_task_handle — exercises graceful poll-task teardown.
///   - `drain_in_flight(&mut state, &storage_tx)` — short-circuits on
///     empty queue.
///   - `break` — exits the outer loop.
///
/// Timeout of 10s catches any regression where the arm's break fails to
/// fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_sender_exits_on_cancellation_with_empty_channel() {
    let config = mock_config();
    let storage = Arc::new(Storage::Mock(MockStorage::new()));

    let (processor_tx, processor_rx) = mpsc::channel(10);
    // Drop the sender so the drain loop's recv() resolves immediately.
    drop(processor_tx);

    let (storage_tx, _storage_rx) = mpsc::channel(10);

    let cancellation_token = CancellationToken::new();
    // Cancel before invoking run_sender so the cancellation arm of the
    // select! fires on entry. Without this the test would rely on the
    // empty processor channel driving the other arm instead, conflating
    // two distinct code paths.
    cancellation_token.cancel();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_sender(
            &config,
            CommitmentLevel::Confirmed,
            processor_rx,
            storage_tx,
            cancellation_token,
            storage,
            /* retry_max_attempts */ 3,
            DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
            /* source_rpc_client */ None,
            /* sender_lock_heartbeat_interval */ Duration::from_secs(5),
        ),
    )
    .await
    .expect("run_sender must exit within 10s when cancellation token fires");

    assert!(
        result.is_ok(),
        "cancellation drain must return Ok(()); got {:?}",
        result.err()
    );
}
