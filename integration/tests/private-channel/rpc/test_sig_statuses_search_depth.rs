//! `test_get_signature_statuses_search_depth`
//!
//! Target files: `core/src/rpc/rpc_impl.rs` + `core/src/rpc/get_signature_statuses_impl.rs`.
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! The existing `run_get_signature_statuses_test` covers malformed inputs
//! and the 256-signature limit but never exercises the
//! `searchTransactionHistory` parameter. That flag's code path is precisely
//! the uncovered block in `rpc_impl.rs`.
//!
//! Cases covered:
//!   A. Confirmed recent sig, `searchTransactionHistory: false` → status present.
//!   B. Same sig, `searchTransactionHistory: true`            → status present.
//!      Proves the flag's branch doesn't change semantics for recent sigs.
//!   C. Unknown sig, `searchTransactionHistory: true`         → null (the
//!      history-lookup fall-through returns null when nothing is found).
//!   D. `getSignatureStatuses` with an empty array            → empty value
//!      array (covers the zero-length early-return branch).

use {
    super::test_context::PrivateChannelContext,
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{signature::Signature, signer::Signer},
};

pub async fn run_sig_statuses_search_depth_test(ctx: &PrivateChannelContext) {
    println!("\n=== getSignatureStatuses — Search-Depth Branches ===");

    let sig = submit_trivial_tx(ctx).await;
    ctx.check_transaction_exists(sig).await;

    case_a_recent_sig_without_history(ctx, sig).await;
    case_b_recent_sig_with_history(ctx, sig).await;
    case_c_unknown_sig_with_history(ctx).await;
    case_d_empty_sig_array(ctx).await;

    println!("✓ four search-depth branches passed");
}

async fn submit_trivial_tx(ctx: &PrivateChannelContext) -> Signature {
    let from = solana_sdk::signature::Keypair::new();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let ix = solana_sdk::instruction::Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: b"search-depth".to_vec(),
    };
    let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[ix],
        Some(&from.pubkey()),
        &[&from],
        blockhash,
    );
    ctx.send_transaction(&tx).await.unwrap()
}

// ── Case A ──────────────────────────────────────────────────────────────────
async fn case_a_recent_sig_without_history(ctx: &PrivateChannelContext, sig: Signature) {
    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            json!([[sig.to_string()], {"searchTransactionHistory": false}]),
        )
        .await
        .expect("recent sig with history=false must succeed");
    let arr = resp["value"].as_array().expect("value is array");
    assert_eq!(arr.len(), 1);
    assert!(
        arr[0].is_object(),
        "recent sig must return a status object, got {:?}",
        arr[0]
    );
}

// ── Case B ──────────────────────────────────────────────────────────────────
async fn case_b_recent_sig_with_history(ctx: &PrivateChannelContext, sig: Signature) {
    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            json!([[sig.to_string()], {"searchTransactionHistory": true}]),
        )
        .await
        .expect("recent sig with history=true must succeed");
    let arr = resp["value"].as_array().expect("value is array");
    assert_eq!(arr.len(), 1);
    assert!(
        arr[0].is_object(),
        "recent sig with history=true must still return a status object"
    );
}

// ── Case C ──────────────────────────────────────────────────────────────────
async fn case_c_unknown_sig_with_history(ctx: &PrivateChannelContext) {
    let unknown = Signature::new_unique().to_string();
    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            json!([[unknown], {"searchTransactionHistory": true}]),
        )
        .await
        .expect("unknown sig with history=true must succeed (returns null)");
    let arr = resp["value"].as_array().expect("value is array");
    assert_eq!(arr.len(), 1);
    assert!(
        arr[0].is_null(),
        "unknown sig must be null even with history=true; long-term store has nothing"
    );
}

// ── Case D ──────────────────────────────────────────────────────────────────
async fn case_d_empty_sig_array(ctx: &PrivateChannelContext) {
    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            // IMPORTANT: the outer array wraps the params tuple; empty sigs.
            json!([[]]),
        )
        .await
        .expect("empty sig array must succeed");
    let arr = resp["value"].as_array().expect("value is array");
    assert!(arr.is_empty(), "empty input → empty output; got {arr:?}");
}
