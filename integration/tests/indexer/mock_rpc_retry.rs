//! End-to-end wiring test for `MockRpcServer` + `RpcClientWithRetry`.
//!
//! Validates that a scripted RPC sequence is consumed in FIFO order over real
//! HTTP by the same client wrapper the operator uses in production
//! (`operator::utils::rpc_util::RpcClientWithRetry`). This anchors the
//! fault-injection tests that need scripted failures at the RPC layer.
//!
//! What's exercised by this file:
//!   - MockRpcServer bind + dispatch loop
//!   - Reply::result / Reply::error scripted FIFO
//!   - RpcClientWithRetry::with_retry (Idempotent path, success-on-second-try)
//!   - RpcClientWithRetry::get_signature_statuses (actual product method)
//!
//! What's intentionally out of scope here (covered by sibling tests):
//!   - DB fault-injection via Storage::Mock
//!   - full operator bootstrap + end-to-end status-flip assertions

use solana_commitment_config::CommitmentConfig;
use {
    private_channel_indexer::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry},
    serde_json::json,
    solana_sdk::signature::Signature,
    std::{str::FromStr, time::Duration},
    test_utils::mock_rpc::{MockRpcServer, Reply},
};

fn dummy_signature() -> Signature {
    // 64-byte all-ones; bs58-roundtrips cleanly. Any fixed value works — we
    // never hit a real chain.
    Signature::from_str(
        "4BxWw1FjwQCHXWkrK4ZehPWauFTPhBafSr9m8Cuht73LG73nUs3wfuJ6gigkhNppP4pYogP5pQDENbE5nQx1Qp4B",
    )
    .expect("valid fixed signature")
}

/// Success-on-first-try: mock returns a well-formed `getSignatureStatuses`
/// payload with one finalized status; `RpcClientWithRetry` passes it through.
#[tokio::test]
async fn get_signature_statuses_success_returns_scripted_payload() {
    let mock = MockRpcServer::start().await;
    let sig = dummy_signature();

    // Matches the RPC wire format of getSignatureStatuses.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": { "slot": 42 },
            "value": [
                {
                    "slot": 42,
                    "confirmations": null,
                    "err": null,
                    "status": { "Ok": null },
                    "confirmationStatus": "finalized"
                }
            ]
        })),
    );

    let client = RpcClientWithRetry::with_retry_config(
        mock.url(),
        RetryConfig {
            max_attempts: 3,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
        },
        CommitmentConfig::confirmed(),
    );

    let resp = client
        .get_signature_statuses(&[sig])
        .await
        .expect("script should yield a successful response");

    assert_eq!(resp.context.slot, 42);
    assert_eq!(resp.value.len(), 1);
    let status = resp.value[0]
        .as_ref()
        .expect("finalized status should be present");
    assert_eq!(status.slot, 42);
    assert!(status.err.is_none());

    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        1,
        "product code should make exactly one call on a successful first attempt"
    );
    assert_eq!(mock.remaining_scripted("getSignatureStatuses"), 0);

    mock.shutdown().await;
}

/// Retry-through-HTTP: mock errors once, then returns a valid payload.
/// `with_retry(Idempotent)` inside `get_signature_statuses` must re-issue
/// the request over HTTP, consume the second scripted reply, and succeed.
/// Asserts that exactly two HTTP round-trips happened and the retry
/// interval is >= the configured base delay (retry timing bound).
#[tokio::test]
async fn get_signature_statuses_retries_on_transient_error() {
    let mock = MockRpcServer::start().await;
    let sig = dummy_signature();

    // First reply: RPC-level error that is NOT classified as permanent by
    // `is_permanent_rpc_error`. Generic server error -32000 qualifies.
    // Second reply: well-formed success payload.
    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "transient server error"),
            Reply::result(json!({
                "context": { "slot": 101 },
                "value": [ serde_json::Value::Null ]
            })),
        ],
    );

    let base_delay = Duration::from_millis(40);
    let client = RpcClientWithRetry::with_retry_config(
        mock.url(),
        RetryConfig {
            max_attempts: 3,
            base_delay,
            max_delay: Duration::from_millis(500),
        },
        CommitmentConfig::confirmed(),
    );

    let resp = client
        .get_signature_statuses(&[sig])
        .await
        .expect("second attempt should succeed");

    assert_eq!(resp.context.slot, 101);
    assert_eq!(resp.value.len(), 1);
    assert!(resp.value[0].is_none(), "null status should decode as None");

    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        2,
        "retry wrapper should re-issue exactly once after the scripted error"
    );
    assert_eq!(mock.remaining_scripted("getSignatureStatuses"), 0);

    // Retry timing: the gap between call 1 and call 2 should be at least
    // the configured base_delay (exponential backoff starts at base_delay).
    // Allow a small scheduler slack below base_delay to keep the bound
    // tight but not flaky.
    let stamps = mock.call_timestamps("getSignatureStatuses");
    assert_eq!(stamps.len(), 2);
    let gap = stamps[1].duration_since(stamps[0]);
    let min_expected = base_delay - Duration::from_millis(10);
    assert!(
        gap >= min_expected,
        "retry gap {gap:?} should respect base_delay {base_delay:?} (min {min_expected:?})"
    );

    mock.shutdown().await;
}
