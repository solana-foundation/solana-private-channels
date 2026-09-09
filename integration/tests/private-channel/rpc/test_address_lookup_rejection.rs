//! `test_address_lookup_rejection`
//!
//! Target files: `core/src/rpc/send_transaction_impl.rs`,
//! `core/src/rpc/simulate_transaction_impl.rs`.
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! A v0 message that declares address table lookups can never be resolved here,
//! because no address lookup table program is admitted. Both admission paths
//! must refuse it instead of building a transaction whose account indices
//! outrun its own key list.
//!
//! The four cases are the cross product of the two admission paths and the two
//! message shapes, so one builder and one keypair cover the whole matrix:
//!
//!   * one declared lookup  -> rejected with `-32602`
//!   * no declared lookups  -> still admitted, which is what ordinary v0
//!     clients send and what a blanket v0 rejection would have broken

use {
    super::test_context::PrivateChannelContext,
    crate::setup,
    base64::{engine::general_purpose::STANDARD, Engine as _},
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
};

const INVALID_PARAMS_CODE: i64 = -32_602;

pub async fn run_address_lookup_rejection_test(ctx: &PrivateChannelContext) {
    println!("\n=== Address Table Lookup Rejection ===");

    case_send_with_lookup_rejected(ctx).await;
    case_send_without_lookup_accepted(ctx).await;
    case_simulate_with_lookup_rejected(ctx).await;
    case_simulate_without_lookup_accepted(ctx).await;

    println!("✓ sendTransaction + simulateTransaction reject declared lookups and admit plain v0");
}

/// Base64-encodes a v0 System transfer carrying `num_lookups` declared lookups.
/// The lookup case also indexes a lookup-supplied key, so without the guard the
/// node would admit a transaction whose account index it never resolved.
async fn encoded_v0_tx(ctx: &PrivateChannelContext, num_lookups: usize) -> String {
    let payer = Keypair::new();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx = setup::v0_system_transfer(
        &payer,
        &Pubkey::new_unique(),
        1_000,
        blockhash,
        num_lookups,
        num_lookups > 0,
    );
    let bytes = bincode::serialize(&tx).unwrap();
    STANDARD.encode(&bytes)
}

/// Both paths must name the lookup as the reason and use the invalid-params code.
fn assert_lookup_rejection(err: &solana_client::client_error::ClientError, method: &str) {
    let msg = err.to_string();
    assert!(
        msg.contains("Address lookup tables are not supported"),
        "{method} must name address lookup tables as the reason; got: {msg}"
    );
    assert!(
        msg.contains(&INVALID_PARAMS_CODE.to_string()),
        "{method} must reject with {INVALID_PARAMS_CODE}; got: {msg}"
    );
}

// ── sendTransaction, one declared lookup ────────────────────────────────────
async fn case_send_with_lookup_rejected(ctx: &PrivateChannelContext) {
    let encoded = encoded_v0_tx(ctx, 1).await;

    let err = ctx
        .write_client
        .send::<serde_json::Value>(
            RpcRequest::SendTransaction,
            json!([encoded, {"skipPreflight": true, "encoding": "base64"}]),
        )
        .await
        .expect_err("a v0 tx declaring address table lookups must be rejected");
    assert_lookup_rejection(&err, "sendTransaction");
}

// ── sendTransaction, no declared lookups ────────────────────────────────────
async fn case_send_without_lookup_accepted(ctx: &PrivateChannelContext) {
    let encoded = encoded_v0_tx(ctx, 0).await;

    let result = ctx
        .write_client
        .send::<serde_json::Value>(
            RpcRequest::SendTransaction,
            json!([encoded, {"skipPreflight": true, "encoding": "base64"}]),
        )
        .await;
    assert!(
        result.is_ok(),
        "a lookup-free v0 tx must still be admitted: {result:?}"
    );
}

// ── simulateTransaction, one declared lookup ────────────────────────────────
async fn case_simulate_with_lookup_rejected(ctx: &PrivateChannelContext) {
    let encoded = encoded_v0_tx(ctx, 1).await;

    let err = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([encoded, {"encoding": "base64"}]),
        )
        .await
        .expect_err("simulate must reject a v0 tx declaring address table lookups");
    assert_lookup_rejection(&err, "simulateTransaction");
}

// ── simulateTransaction, no declared lookups ────────────────────────────────
async fn case_simulate_without_lookup_accepted(ctx: &PrivateChannelContext) {
    let encoded = encoded_v0_tx(ctx, 0).await;

    // The transfer itself may fail for lack of funds; only admission matters.
    let result = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([encoded, {"encoding": "base64"}]),
        )
        .await;
    assert!(
        result.is_ok(),
        "a lookup-free v0 tx must still be simulated: {result:?}"
    );
}
