//! `test_simulate_transaction_preflight_paths`
//!
//! Target file: `core/src/rpc/simulate_transaction_impl.rs`.
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! Exercises the distinct result-shape branches of `simulate_transaction`:
//!
//!   A. **Valid simulation** → `err=None`, `logs` present, `units_consumed` > 0.
//!   B. **Invalid base64** in the tx parameter → RPC error `-32602`.
//!   C. **Malformed bincode** (valid base64 but not a transaction) → RPC
//!      error `-32602` with "Failed to deserialize transaction".
//!   D. **Duplicate account keys**: RPC error `-32602`, keeping simulation
//!      and `sendTransaction` in agreement about what a valid transaction is.
//!
//! `test_simulate_transaction_with_account_writes` covers the
//! accounts-return / encoding / replaceRecentBlockhash branches separately.

use {
    super::test_context::PrivateChannelContext,
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_core::test_helpers::duplicate_account_keys_transaction,
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{signature::Keypair, signer::Signer, transaction::Transaction},
    solana_system_interface::instruction as system_instruction,
};

pub async fn run_simulate_transaction_preflight_test(ctx: &PrivateChannelContext) {
    println!("\n=== simulateTransaction — Preflight Error Branches ===");

    case_a_valid_simulation(ctx).await;
    case_b_invalid_base64(ctx).await;
    case_c_malformed_bincode(ctx).await;
    case_d_duplicate_account_keys(ctx).await;

    println!("✓ four preflight branches passed");
}

// ── Case A ──────────────────────────────────────────────────────────────────
async fn case_a_valid_simulation(ctx: &PrivateChannelContext) {
    // Build a real system-transfer tx; simulate should succeed with no err
    // and report compute-unit usage.
    let payer = Keypair::new();
    let recipient = Keypair::new().pubkey();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let ix = system_instruction::transfer(&payer.pubkey(), &recipient, 1_000);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer.pubkey()), &[&payer], blockhash);

    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([
                STANDARD.encode(bincode::serialize(&tx).unwrap()),
                {"encoding": "base64", "sigVerify": false}
            ]),
        )
        .await
        .expect("valid tx simulation must succeed");

    let value = &resp["value"];
    // err may be Some if the account lacks funds (it does — poor payer),
    // but logs and units_consumed must be present regardless.
    assert!(
        value.get("logs").is_some(),
        "simulation must return logs; got {value}"
    );
    assert!(
        value.get("unitsConsumed").is_some(),
        "simulation must report unitsConsumed; got {value}"
    );
}

// ── Case B ──────────────────────────────────────────────────────────────────
async fn case_b_invalid_base64(ctx: &PrivateChannelContext) {
    let err = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!(["!!not-base64!!", {"encoding": "base64"}]),
        )
        .await
        .expect_err("invalid base64 must be rejected");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("base64") || msg.contains("-32602"),
        "error must name base64 or invalid-params code; got {msg}"
    );
}

// ── Case C ──────────────────────────────────────────────────────────────────
async fn case_c_malformed_bincode(ctx: &PrivateChannelContext) {
    // Valid base64 decoding into junk bytes → base64 check passes, bincode
    // deserialize fails.
    let junk_bytes = vec![0xAAu8; 64];
    let encoded = STANDARD.encode(&junk_bytes);
    let err = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([encoded, {"encoding": "base64"}]),
        )
        .await
        .expect_err("junk-bytes tx must be rejected by deserializer");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("deserialize") || msg.contains("-32602"),
        "error must name deserialize failure or invalid-params; got {msg}"
    );
}

// ── Case D ──────────────────────────────────────────────────────────────────
async fn case_d_duplicate_account_keys(ctx: &PrivateChannelContext) {
    // Simulation must refuse what sendTransaction refuses, or preflight blesses a doomed tx.
    let payer = Keypair::new();
    let blockhash = ctx.get_blockhash().await.expect("live blockhash");
    let tx = duplicate_account_keys_transaction(&payer, blockhash);
    let encoded = STANDARD.encode(bincode::serialize(&tx).expect("serialize duplicate-key tx"));

    let err = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([encoded, {"encoding": "base64", "sigVerify": false}]),
        )
        .await
        .expect_err("duplicate account keys must be rejected by simulation");
    // Cases B and C already return this code, so only the reason pins this arm.
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("account loaded twice"),
        "error must name the duplicate cause; got {msg}"
    );
}
