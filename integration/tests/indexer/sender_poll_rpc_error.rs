//! End-to-end coverage for `poll_in_flight`
//! (`indexer/src/operator/sender/transaction.rs`) via the
//! `test_hooks::poll_in_flight` wrapper.
//!
//! `poll_in_flight` is the single-cycle drainer used by `drain_in_flight`
//! at shutdown and by the dedicated `run_poll_task` background loop in
//! production. It pulls the entire `state.in_flight` queue, batches one
//! `getSignatureStatuses` call per chunk, and either routes results via
//! `route_poll_results` or — when the RPC errors — puts the batch back
//! unchanged so the next cycle retries.
//!
//! Both scenarios construct a `SenderState` with a single in-flight
//! `Mint` (deposit-side) entry and drive one cycle through the hook:
//!
//!   (a) **RPC error**: the wrapper exhausts `RpcClientWithRetry`'s
//!       retry budget and `poll_in_flight` returns the batch to the
//!       queue unchanged. No storage update is emitted; `poll_attempts`
//!       does not increment.
//!
//!   (b) **Transient error then confirmed success**: one transient error
//!       inside the retry wrapper, then a finalized status. The wrapper
//!       returns Confirmed; `route_poll_results` → `handle_success`
//!       emits a `Completed` status update and removes the entry from
//!       `in_flight`.

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    private_channel_indexer::{
        operator::{
            sender::{
                test_hooks,
                types::{InFlightTx, TransactionContext, MAX_IN_FLIGHT},
            },
            utils::{
                instruction_util::{ExtraErrorCheckPolicy, RetryPolicy},
                transaction_util::MAX_POLL_ATTEMPTS_CONFIRMATION,
            },
        },
        storage::TransactionStatus,
    },
    sender_fixtures::{build_default_sender_state, make_instruction, null_status_reply},
    serde_json::json,
    solana_sdk::signature::Signature,
    std::sync::Arc,
    test_utils::mock_rpc::Reply,
    tokio::sync::Semaphore,
};

/// Construct an `InFlightTx` for a Mint (deposit) transaction. The
/// `permit` field is required to be a live permit drawn from a fresh
/// semaphore; the production drop semantics don't matter for a single
/// poll cycle. `poll_attempts`, `resend_count`, and `retry_policy`
/// are exposed so timeout / resend-limit scenarios can pre-position
/// the entry near the threshold without simulating earlier ticks.
fn make_in_flight_tx(
    sig: Signature,
    txn_id: i64,
    retry_policy: RetryPolicy,
    poll_attempts: u32,
    resend_count: u32,
) -> InFlightTx {
    let permit = Arc::new(Semaphore::new(MAX_IN_FLIGHT))
        .try_acquire_owned()
        .expect("fresh semaphore must yield a permit");
    InFlightTx {
        signature: sig,
        ctx: TransactionContext {
            transaction_id: Some(txn_id),
            withdrawal_nonce: None,
            trace_id: Some(format!("trace-{txn_id}")),
        },
        instruction: make_instruction(),
        compute_unit_price: None,
        retry_policy,
        extra_error_checks_policy: ExtraErrorCheckPolicy::None,
        poll_attempts,
        resend_count,
        persisted: false,
        permit,
    }
}

// ─────────────────────────────────────────────────────────────────────
// RPC error during poll — batch returned to queue unchanged.
// ─────────────────────────────────────────────────────────────────────
//
// Three scripted -32000 errors exhaust `RpcClientWithRetry`'s default
// budget. `poll_in_flight` observes the `Err` from `get_signature_statuses`,
// pushes every InFlightTx back into `state.in_flight`, and returns
// without emitting a status update.
#[tokio::test]
async fn rpc_error_returns_batch_to_queue_without_status_update() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let sig = Signature::new_unique();
    state
        .in_flight
        .push(make_in_flight_tx(sig, 301, RetryPolicy::None, 0, 0));

    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "transient #1"),
            Reply::error(-32000, "transient #2"),
            Reply::error(-32000, "transient #3"),
            Reply::error(-32000, "transient #4"),
            Reply::error(-32000, "transient #5"),
        ],
    );

    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    assert_eq!(
        state.in_flight.len(),
        1,
        "the RPC-error branch must push the batch back into in_flight"
    );
    assert_eq!(
        state.in_flight.entries.lock().unwrap()[0].poll_attempts,
        0,
        "poll_attempts must NOT increment on the RPC-error branch (route_poll_results was not entered)"
    );
    assert!(
        storage_rx.try_recv().is_err(),
        "no status update must be emitted on the RPC-error branch"
    );

    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Transient error + finalized success — entry completes, queue empties.
// ─────────────────────────────────────────────────────────────────────
//
// `RpcClientWithRetry`'s retry wrapper absorbs the first transient
// error and re-issues the call; the second reply is a finalized status
// with `err: null`, which `check_transaction_status` decodes as
// `ConfirmationResult::Confirmed`. `route_poll_results` → `handle_success`
// emits a `Completed` status update and removes the entry from the queue.
#[tokio::test]
async fn transient_error_then_confirmed_completes_entry() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let sig = Signature::new_unique();
    state
        .in_flight
        .push(make_in_flight_tx(sig, 302, RetryPolicy::None, 0, 0));

    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "transient — retry me"),
            Reply::result(json!({
                "context": { "slot": 200 },
                "value": [{
                    "slot": 200,
                    "confirmations": null,
                    "err": null,
                    "status": { "Ok": null },
                    "confirmationStatus": "finalized"
                }]
            })),
        ],
    );

    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    assert_eq!(
        state.in_flight.len(),
        0,
        "confirmed entry must be removed from in_flight"
    );

    let update = storage_rx
        .recv()
        .await
        .expect("handle_success must emit a Completed status update");
    assert_eq!(update.transaction_id, 302);
    assert_eq!(update.status, TransactionStatus::Completed);
    assert_eq!(
        update.counterpart_signature.as_deref(),
        Some(sig.to_string().as_str()),
        "counterpart_signature must be the in-flight tx's signature"
    );

    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Still-pending — null status with poll_attempts < MAX → push back, increment.
// ─────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn null_status_below_max_pushes_back_and_increments_poll_attempts() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let sig = Signature::new_unique();
    state
        .in_flight
        .push(make_in_flight_tx(sig, 303, RetryPolicy::None, 0, 0));

    mock.enqueue("getSignatureStatuses", null_status_reply());

    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    assert_eq!(
        state.in_flight.len(),
        1,
        "the still-pending arm must push the entry back into in_flight"
    );
    assert_eq!(
        state.in_flight.entries.lock().unwrap()[0].poll_attempts,
        1,
        "the still-pending arm must increment poll_attempts by 1"
    );
    assert!(
        storage_rx.try_recv().is_err(),
        "no status update on the still-pending arm"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Timeout + RetryPolicy::None — permanent failure with "unsafe to retry".
// ─────────────────────────────────────────────────────────────────────
//
// `poll_attempts == MAX - 1` going in; after route_poll_results
// increments to MAX it routes to `handle_permanent_failure` with the
// distinct "transaction status unknown, unsafe to retry" label.
#[tokio::test]
async fn null_status_at_max_with_none_policy_routes_to_permanent_failure() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let sig = Signature::new_unique();
    state.in_flight.push(make_in_flight_tx(
        sig,
        304,
        RetryPolicy::None,
        MAX_POLL_ATTEMPTS_CONFIRMATION - 1,
        0,
    ));

    mock.enqueue("getSignatureStatuses", null_status_reply());

    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("None-policy timeout arm must emit a Failed update");
    assert_eq!(update.transaction_id, 304);
    assert_eq!(update.status, TransactionStatus::Failed);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("unsafe to retry"),
        "None-policy timeout must surface the 'unsafe to retry' label; got {msg:?}"
    );
    assert!(
        state.in_flight.is_empty(),
        "the timed-out entry must NOT remain in_flight"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Timeout + RetryPolicy::Idempotent + resend cap reached — permanent failure.
// ─────────────────────────────────────────────────────────────────────
//
// With `retry_max_attempts = 1` and `resend_count = 1`, the next
// resend would be 2 > 1 — the cap. Production routes to
// `handle_permanent_failure` with the "resend limit exceeded" label
// rather than re-sending forever.
#[tokio::test]
async fn null_status_at_max_idempotent_resend_cap_exceeded_routes_to_permanent_failure() {
    // build_fixture default: retry_max_attempts = 1.
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let sig = Signature::new_unique();
    state.in_flight.push(make_in_flight_tx(
        sig,
        305,
        RetryPolicy::Idempotent,
        MAX_POLL_ATTEMPTS_CONFIRMATION - 1,
        // resend_count = retry_max_attempts so next_resend == retry_max_attempts + 1 → cap.
        1,
    ));

    mock.enqueue("getSignatureStatuses", null_status_reply());

    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("resend-limit arm must emit a Failed update");
    assert_eq!(update.transaction_id, 305);
    assert_eq!(update.status, TransactionStatus::Failed);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("resend limit exceeded"),
        "Idempotent + resend-cap must surface the 'resend limit exceeded' label; got {msg:?}"
    );
    assert!(state.in_flight.is_empty());
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Confirmed-with-err — routes via handle_confirmation_result with the on-chain error.
// ─────────────────────────────────────────────────────────────────────
//
// A finalized status with `err` set is the "confirmed but failed
// on-chain" wire shape. `route_poll_results` decodes the err and
// calls `handle_confirmation_result(Ok(Failed(...)))` directly,
// bypassing the timeout machinery. With no specific program-error
// match, it falls through to the generic catch-all arm of
// `handle_confirmation_result` and emits Failed with the debug repr
// of the program error.
#[tokio::test]
async fn confirmed_with_onchain_error_routes_via_handle_confirmation_result() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let sig = Signature::new_unique();
    state
        .in_flight
        .push(make_in_flight_tx(sig, 306, RetryPolicy::None, 0, 0));

    // Finalized + err set (custom code 99, unrecognised by parse_program_error).
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": { "slot": 200 },
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": { "InstructionError": [0, { "Custom": 99 }] },
                "status": { "Err": { "InstructionError": [0, { "Custom": 99 }] } },
                "confirmationStatus": "finalized"
            }]
        })),
    );

    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("confirmed-with-err arm must emit a Failed update");
    assert_eq!(update.transaction_id, 306);
    assert_eq!(update.status, TransactionStatus::Failed);
    assert!(state.in_flight.is_empty());
    mock.shutdown().await;
}
