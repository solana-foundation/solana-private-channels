//! End-to-end coverage for the sender-level retry counter inside
//! `send_and_confirm` (`indexer/src/operator/sender/transaction.rs`)
//! via the `test_hooks::run_send_and_confirm` wrapper.
//!
//! The retry counter only fires for the
//! `RetryPolicy::Idempotent` + `withdrawal_nonce` combination — the
//! withdrawal path. Each call to `send_and_confirm` increments
//! `state.retry_counts[nonce]`; once that count reaches
//! `state.retry_max_attempts`, the next call short-circuits, increments
//! the `max_retries_exceeded` metric, and routes to
//! `handle_permanent_failure` *without* attempting the wire send.
//!
//! Because the test omits the on-chain machinery a real withdrawal
//! requires (SMT state, instance PDA, etc.), we construct the
//! `TransactionContext` and `InstructionWithSigners` directly and rely
//! on the same `MockRpcServer` plumbing the JIT and sign-and-send tests
//! use. With no `remint_cache` entry seeded in `state`, the
//! `handle_permanent_failure` arm falls through to `send_fatal_error`
//! and emits `TransactionStatus::Failed`.

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    private_channel_indexer::{
        config::ProgramType,
        operator::{
            sender::{
                test_hooks,
                types::{PendingSig, SenderState, TransactionStatusUpdate},
            },
            utils::instruction_util::{ExtraErrorCheckPolicy, RetryPolicy},
        },
        storage::{common::storage::mock::MockStorage, Storage, TransactionStatus},
    },
    sender_fixtures::{
        blockhash_reply, ensure_admin_signer_env, make_config, make_instruction, make_remint_info,
        withdrawal_ctx,
    },
    solana_sdk::{commitment_config::CommitmentLevel, signature::Signature},
    std::sync::Arc,
    test_utils::mock_rpc::{MockRpcServer, Reply},
    tokio::sync::mpsc,
};

/// Build a fresh withdrawal-side `SenderState` with the given
/// `retry_max_attempts`, plus the `(storage_tx, storage_rx)` pair the
/// helper writes status updates to. Also returns the `MockStorage`
/// handle for fault-injection scenarios — the inner `Arc<Mutex<...>>`
/// fields are shared, so `set_should_fail` calls on the returned
/// handle propagate to the storage that `SenderState` holds.
async fn build_fixture(
    retry_max_attempts: u32,
) -> (
    SenderState,
    mpsc::Receiver<TransactionStatusUpdate>,
    mpsc::Sender<TransactionStatusUpdate>,
    MockRpcServer,
    MockStorage,
) {
    ensure_admin_signer_env();
    let mock = MockRpcServer::start().await;
    let mock_storage = MockStorage::new();
    let storage = Arc::new(Storage::Mock(mock_storage.clone()));
    let state = test_hooks::new_sender_state(
        &make_config(mock.url(), ProgramType::Withdraw),
        CommitmentLevel::Confirmed,
        None,
        storage,
        retry_max_attempts,
        1,
        None,
    )
    .expect("SenderState construction must succeed under Mock storage");
    let (storage_tx, storage_rx) = mpsc::channel(16);
    (state, storage_rx, storage_tx, mock, mock_storage)
}

/// Seed `state.remint_cache[nonce]` so `handle_permanent_failure`
/// takes the deferred-remint branch instead of falling through to
/// `send_fatal_error`. The mint/user/ATA fields are not inspected by
/// the deferral path itself — they only matter once
/// `attempt_remint` actually runs (covered by `remint_flow.rs`).
fn seed_remint_cache(state: &mut SenderState, transaction_id: i64, nonce: u64) {
    state
        .remint_cache
        .insert(nonce, make_remint_info(transaction_id));
}

/// Helper: enqueue one (`getLatestBlockhash`, `sendTransaction`-error)
/// pair so a single `send_and_confirm` call exhausts the wire layer
/// quickly. -32601 is classified permanent → no inner retries → exactly
/// one `sendTransaction` per outer call.
fn enqueue_failing_send(mock: &MockRpcServer, label: &str) {
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", Reply::error(-32601, label.to_string()));
}

// ─────────────────────────────────────────────────────────────────────
// Sender retry counter — cap at `retry_max_attempts`, then short-circuit.
// ─────────────────────────────────────────────────────────────────────
//
// With `retry_max_attempts = 3`: the first three `send_and_confirm`
// calls each issue one wire send (which fails) and route to
// `handle_permanent_failure`. The fourth call observes
// `retry_counts[nonce] == 3 >= retry_max_attempts` at the function
// entry, increments the `max_retries_exceeded` metric, and routes to
// `handle_permanent_failure` *without* touching the RPC. The fourth
// `getLatestBlockhash`/`sendTransaction` script (if scripted) would
// stay unconsumed, proving the short-circuit fired.
#[tokio::test]
async fn idempotent_send_loops_capped_by_retry_max_attempts() {
    let (mut state, mut storage_rx, storage_tx, mock, _mock_storage) = build_fixture(3).await;
    let ctx = withdrawal_ctx(404, 7);

    // Three failing wire sends — every call increments the retry counter.
    for i in 0..3 {
        enqueue_failing_send(&mock, &format!("attempt {}", i + 1));
    }
    // A fourth scripted pair that the short-circuit must NOT consume.
    enqueue_failing_send(&mock, "should never be consumed");

    for _ in 0..4 {
        test_hooks::run_send_and_confirm(
            &mut state,
            make_instruction(),
            None,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;
    }

    // The counter must reflect exactly three attempts (the fourth call
    // observed `attempts >= retry_max_attempts` and short-circuited).
    let attempts = state.retry_counts.get(&7).copied().unwrap_or(0);
    assert_eq!(
        attempts, 3,
        "retry_counts[nonce] must equal retry_max_attempts after the cap is hit"
    );

    // Wire layer: exactly three `sendTransaction` calls, not four.
    assert_eq!(
        mock.call_count("sendTransaction"),
        3,
        "the fourth call must short-circuit before issuing any wire send"
    );
    // The fourth scripted pair must remain queued.
    assert_eq!(
        mock.remaining_scripted("sendTransaction"),
        1,
        "the fourth scripted reply must remain unconsumed"
    );

    // Every call routed to `handle_permanent_failure`, which (with no
    // `remint_cache` entry) falls through to `send_fatal_error`. Drain
    // the channel and confirm we got 4 `Failed` updates — three from
    // the wire-error path and one from the max-retries-exceeded short-
    // circuit. The fourth carries the distinct "Max retries exceeded"
    // error message.
    let mut updates = Vec::new();
    while let Ok(u) = storage_rx.try_recv() {
        updates.push(u);
    }
    assert_eq!(
        updates.len(),
        4,
        "every call must emit exactly one status update"
    );
    assert!(
        updates
            .iter()
            .all(|u| u.status == TransactionStatus::Failed),
        "every status update must be Failed; got {:?}",
        updates.iter().map(|u| &u.status).collect::<Vec<_>>()
    );
    assert!(
        updates
            .last()
            .and_then(|u| u.error_message.as_deref())
            .map(|m| m.contains("Max retries"))
            .unwrap_or(false),
        "the final update must surface the Max-retries-exceeded label; got {:?}",
        updates.last().and_then(|u| u.error_message.as_deref())
    );

    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Higher-budget boundary — retry counter still trips on the (n+1)th call.
// ─────────────────────────────────────────────────────────────────────
//
// Same shape with `retry_max_attempts = 4`: four wire sends consumed,
// the fifth call short-circuits. Pins the inclusive boundary of the
// retry-counter check (`attempts >= retry_max_attempts`).
#[tokio::test]
async fn idempotent_send_loops_capped_at_higher_budget() {
    let (mut state, mut storage_rx, storage_tx, mock, _mock_storage) = build_fixture(4).await;
    let ctx = withdrawal_ctx(505, 11);

    for i in 0..4 {
        enqueue_failing_send(&mock, &format!("attempt {}", i + 1));
    }

    for _ in 0..5 {
        test_hooks::run_send_and_confirm(
            &mut state,
            make_instruction(),
            None,
            &ctx,
            RetryPolicy::Idempotent,
            &ExtraErrorCheckPolicy::None,
            &storage_tx,
        )
        .await;
    }

    assert_eq!(state.retry_counts.get(&11).copied().unwrap_or(0), 4);
    assert_eq!(mock.call_count("sendTransaction"), 4);

    let mut updates = Vec::new();
    while let Ok(u) = storage_rx.try_recv() {
        updates.push(u);
    }
    assert_eq!(updates.len(), 5);
    assert!(updates
        .iter()
        .all(|u| u.status == TransactionStatus::Failed));
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Withdrawal deferral — zero stashed signatures → ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// `remint_cache[nonce]` is seeded so `handle_permanent_failure` enters
// the deferred-remint branch instead of falling through to
// `send_fatal_error`. `pending_signatures[nonce]` is left empty: the
// production code treats this as "the RPC may have broadcast the tx
// before erroring — blind remint is unsafe" and routes to
// `ManualReview` with the "no signatures to verify" label.
#[tokio::test]
async fn deferral_with_zero_stashed_signatures_routes_to_manual_review() {
    let (mut state, mut storage_rx, storage_tx, mock, _mock_storage) = build_fixture(3).await;
    let ctx = withdrawal_ctx(601, 21);
    seed_remint_cache(&mut state, 601, 21);
    // pending_signatures intentionally NOT seeded.

    enqueue_failing_send(&mock, "permanent send error");

    test_hooks::run_send_and_confirm(
        &mut state,
        make_instruction(),
        None,
        &ctx,
        RetryPolicy::Idempotent,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;

    let update = storage_rx
        .recv()
        .await
        .expect("zero-sigs deferral arm must emit a ManualReview update");
    assert_eq!(update.transaction_id, 601);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("no signatures to verify"),
        "zero-sigs arm must surface the 'no signatures to verify' label; got {msg:?}"
    );
    assert!(
        !update.remint_attempted,
        "zero-sigs arm must NOT mark remint_attempted (no remint was scheduled)"
    );
    // Entry was NOT pushed to pending_remints — the unsafe-remint guard
    // returned ManualReview directly.
    assert!(state.pending_remints.is_empty());
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Withdrawal deferral — set_pending_remint succeeds → push to queue.
// ─────────────────────────────────────────────────────────────────────
//
// Pre-seed `pending_signatures[nonce]` with one fake signature so the
// non-zero-sigs branch fires, calls `storage.set_pending_remint`
// (succeeds under default `MockStorage`), and pushes a `PendingRemint`
// entry into `state.pending_remints` for the deferred-finality-check
// loop to pick up later. No status update is emitted on this path —
// the row stays Processing until the remint resolves.
#[tokio::test]
async fn deferral_with_stashed_signatures_pushes_pending_remint() {
    let (mut state, mut storage_rx, storage_tx, mock, _mock_storage) = build_fixture(3).await;
    let ctx = withdrawal_ctx(602, 22);
    seed_remint_cache(&mut state, 602, 22);
    state.pending_signatures.insert(
        22,
        vec![PendingSig {
            signature: Signature::new_unique(),
            last_valid_block_height: 0,
        }],
    );

    enqueue_failing_send(&mock, "permanent send error");

    test_hooks::run_send_and_confirm(
        &mut state,
        make_instruction(),
        None,
        &ctx,
        RetryPolicy::Idempotent,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;

    // No status update — the row is paused, not failed.
    assert!(
        storage_rx.try_recv().is_err(),
        "the push-to-pending_remints arm must NOT emit a status update; the row stays Processing until the deferred check runs"
    );
    assert_eq!(
        state.pending_remints.len(),
        1,
        "exactly one PendingRemint must be queued"
    );
    let entry = &state.pending_remints[0];
    assert_eq!(entry.ctx.transaction_id, Some(602));
    assert_eq!(entry.ctx.withdrawal_nonce, Some(22));
    assert_eq!(
        entry.signatures.len(),
        1,
        "the seeded stashed signature must be carried over to the PendingRemint entry"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Withdrawal deferral — set_pending_remint fails → ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// Same setup as the success scenario above, but
// `MockStorage::set_should_fail("set_pending_remint", true)` makes the
// storage call fail. The arm catches the error and routes to
// `ManualReview` with a `"failed to persist pending remint"` label
// instead of leaving the row in a broken half-state.
#[tokio::test]
async fn deferral_set_pending_remint_storage_failure_routes_to_manual_review() {
    let (mut state, mut storage_rx, storage_tx, mock, mock_storage) = build_fixture(3).await;
    let ctx = withdrawal_ctx(603, 23);
    seed_remint_cache(&mut state, 603, 23);
    state.pending_signatures.insert(
        23,
        vec![PendingSig {
            signature: Signature::new_unique(),
            last_valid_block_height: 0,
        }],
    );
    mock_storage.set_should_fail("set_pending_remint", true);

    enqueue_failing_send(&mock, "permanent send error");

    test_hooks::run_send_and_confirm(
        &mut state,
        make_instruction(),
        None,
        &ctx,
        RetryPolicy::Idempotent,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;

    let update = storage_rx
        .recv()
        .await
        .expect("storage-failure arm must emit a ManualReview update");
    assert_eq!(update.transaction_id, 603);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("failed to persist pending remint"),
        "storage-failure arm must surface the persistence-error label; got {msg:?}"
    );
    // The row must NOT have been pushed to pending_remints — the storage
    // write failed, so the in-memory queue cannot be allowed to drift
    // ahead of the durable state.
    assert!(state.pending_remints.is_empty());
    mock.shutdown().await;
}
