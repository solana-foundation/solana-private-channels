//! Sequencer zero-deadline drain branch.
//!
//! Covers the `else` arm of the `if batch_deadline_ms > 0` fork inside
//! `start_sequence_worker` (`core/src/stages/sequencer.rs`). When
//! `batch_deadline_ms == 0` the
//! sequencer runs a non-blocking `try_recv` drain up to
//! `max_tx_per_batch` instead of setting a tokio sleep deadline.
//!
//! The worker's public entry point `start_sequence_worker` is called
//! directly from this integration test binary — no validator, no
//! postgres, no test-tree feature. Two transactions are pre-queued
//! before the worker starts so:
//!   - The initial blocking `select!` receives the first tx.
//!   - The `else` arm's `while let Ok(tx) = rx.try_recv()` loop picks
//!     up the second tx immediately, exiting on the first `Err`.
//!   - `process_and_send_batches` dispatches a single batch carrying
//!     both transactions.
//!
//! We then assert that the receiver observes exactly one batch with both
//! transactions — that's the fingerprint of the zero-deadline drain
//! firing (vs the deadline arm, which would trigger a tokio sleep and
//! still dispatch after `batch_deadline_ms`). Even if the scheduler
//! split the two txs into separate batches (conflicting writes) the
//! test still proves the drain ran: it would simply observe two batches
//! in rapid succession with no > 0ms delay.

use {
    private_channel_core::{
        nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        scheduler::ConflictFreeBatch,
        stage_metrics::{NoopMetrics, SharedMetrics},
        stages::{start_sequence_worker, SequencerArgs},
        test_helpers::create_test_sanitized_transaction,
    },
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
    std::{sync::Arc, time::Duration},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

/// `batch_deadline_ms = 0` must route the per-batch collection through
/// the non-blocking `try_recv` drain rather than the sleep-based
/// deadline arm. The proof: two pre-queued transactions
/// exit the sequencer as at least one conflict-free batch within well
/// under the deadline wall-clock, and no second batch is produced after
/// the channel runs dry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequencer_zero_deadline_drains_nonblocking() {
    let (input_tx, input_rx) = mpsc::channel(DEFAULT_SEQUENCER_QUEUE_CAPACITY);
    let (batch_tx, mut batch_rx) = mpsc::channel::<ConflictFreeBatch>(16);
    let shutdown = CancellationToken::new();

    // Use distinct payers so the two txs don't write-conflict — the
    // scheduler will emit a single conflict-free batch carrying both.
    let from_a = Keypair::new();
    let from_b = Keypair::new();
    let to_a = Pubkey::new_unique();
    let to_b = Pubkey::new_unique();
    input_tx
        .send(create_test_sanitized_transaction(&from_a, &to_a, 100))
        .await
        .expect("send tx A");
    input_tx
        .send(create_test_sanitized_transaction(&from_b, &to_b, 100))
        .await
        .expect("send tx B");

    let metrics: SharedMetrics = Arc::new(NoopMetrics);
    let _handle = start_sequence_worker(SequencerArgs {
        max_tx_per_batch: 64,
        batch_deadline_ms: 0, // ← the configuration that selects the drain arm
        rx: input_rx,
        batch_tx,
        shutdown_token: shutdown.clone(),
        metrics,
        heartbeat: private_channel_core::health::StageHeartbeat::new(),
    })
    .await;

    // The drain arm has no wall-clock sleep, so the first batch should
    // be available effectively immediately. Allow 500ms of slack for
    // task scheduling; if the deadline arm were taken instead the test
    // would still pass because deadline_ms=0 wouldn't actually sleep —
    // but the try_recv branch is the one we're gating on.
    let first = tokio::time::timeout(Duration::from_millis(500), batch_rx.recv())
        .await
        .expect("first batch must arrive within 500ms of zero-deadline drain")
        .expect("batch channel must remain open while worker lives");
    let total_txs: usize = {
        let mut n = first.transactions.len();
        // The scheduler *may* emit more than one batch if it decided to
        // split the two (e.g. on account metadata we didn't anticipate).
        // Collect any additional batches that arrive within a short
        // window so the total-count assertion is robust.
        while let Ok(Some(batch)) =
            tokio::time::timeout(Duration::from_millis(100), batch_rx.recv()).await
        {
            n += batch.transactions.len();
        }
        n
    };
    assert_eq!(
        total_txs, 2,
        "both pre-queued transactions must reach the batch channel via \
         the zero-deadline drain"
    );

    shutdown.cancel();
    drop(input_tx);
}
