//! `run_sender` channel-close shutdown path.
//!
//! Covers the `processor_rx.recv() == None` arm of `run_sender`'s
//! `tokio::select!` (`indexer/src/operator/sender/mod.rs`) — the path
//! that fires when the processor drops its `mpsc::Sender`. Behavior:
//!
//!   1. Log "Sender channel closed".
//!   2. Cancel the poll-task shutdown token and `drop(poll_result_rx)`.
//!   3. Await the poll-task handle.
//!   4. Drain any lingering in-flight transactions.
//!   5. Break the `select!` loop and return `Ok(())`.
//!
//! Strategy: call `run_sender` directly from the integration test with a
//! processor `mpsc::Receiver` whose sender half is already dropped. The
//! task must reach the `None` arm on its very first `select!` iteration
//! and exit without requiring a cancellation token, storage traffic, or
//! RPC scripting.
//!
//! This deliberately bypasses `OperatorMockHarness` because the harness
//! owns the processor channel inside `operator::run` and can't expose a
//! closed-sender state.

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

/// Pre-closed processor channel must drive `run_sender` into the
/// channel-close arm on the first `select!` iteration and return
/// `Ok(())` within a few milliseconds. The test asserts two guarantees:
///
///   a. The future resolves — proves the arm's `break` fires rather than
///      the loop stalling waiting for a cancellation token that will
///      never arrive.
///   b. The return value is `Ok(())` — proves the shutdown path is
///      treated as a normal termination (contrast: the `?` on
///      `recover_pending_remints` would have propagated an Err if that
///      recovery had failed, so reaching Ok proves the select loop was
///      actually entered before exiting).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_sender_exits_on_processor_channel_close() {
    let config = mock_config();
    let storage = Arc::new(Storage::Mock(MockStorage::new()));

    // Build a receiver whose sender half is dropped before `run_sender`
    // ever sees it. `recv()` on this will resolve to `None` immediately.
    let processor_rx = {
        let (tx, rx) = mpsc::channel(10);
        drop(tx);
        rx
    };
    let (storage_tx, _storage_rx) = mpsc::channel(10);

    // Don't cancel — we're testing the *channel-close* arm, not the
    // cancellation arm (which is covered separately).
    let cancellation_token = CancellationToken::new();

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
    .expect("run_sender must exit within 10s when processor channel is closed");

    assert!(
        result.is_ok(),
        "channel-close shutdown must return Ok(()); got {:?}",
        result.err()
    );
}
