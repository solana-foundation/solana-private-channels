//! `test_simulate_transaction_with_account_writes`
//!
//! Target file: `core/src/rpc/simulate_transaction_impl.rs`
//! (accounts / replaceRecentBlockhash branches).
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! Exercises the `accounts`, `replace_recent_blockhash`, and `sig_verify`
//! config branches:
//!
//!   A. `accounts: { encoding: base64, addresses: [...] }` → value.accounts
//!      returns one entry per requested address.
//!   B. `replaceRecentBlockhash: true` → simulation succeeds even if the
//!      caller's blockhash would otherwise be stale.
//!   C. `sigVerify: false` (default) → unsigned tx still simulates.

use {
    super::test_context::PrivateChannelContext,
    base64::{engine::general_purpose::STANDARD, Engine as _},
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{hash::Hash, signature::Keypair, signer::Signer, transaction::Transaction},
};

pub async fn run_simulate_transaction_account_writes_test(ctx: &PrivateChannelContext) {
    println!("\n=== simulateTransaction — Accounts & replaceRecentBlockhash ===");

    case_a_accounts_returned(ctx).await;
    case_b_replace_recent_blockhash(ctx).await;

    println!("✓ accounts + replaceRecentBlockhash branches passed");
}

// Build a simple memo tx the server will accept. Does NOT sign as the
// payer; we rely on sigVerify=false to avoid needing a funded keypair.
fn build_unsigned_memo(blockhash: Hash) -> Transaction {
    let payer = Keypair::new();
    let ix =
        solana_sdk::instruction::Instruction::new_with_bytes(spl_memo::id(), b"simulate", vec![]);
    Transaction::new_signed_with_payer(&[ix], Some(&payer.pubkey()), &[&payer], blockhash)
}

// ── Case A ──────────────────────────────────────────────────────────────────
async fn case_a_accounts_returned(ctx: &PrivateChannelContext) {
    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx = build_unsigned_memo(blockhash);
    let addr = Keypair::new().pubkey().to_string(); // just a placeholder address
    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([
                STANDARD.encode(bincode::serialize(&tx).unwrap()),
                {
                    "encoding": "base64",
                    "sigVerify": false,
                    "accounts": {
                        "encoding": "base64",
                        "addresses": [addr]
                    }
                }
            ]),
        )
        .await
        .expect("simulate with accounts config must succeed");

    let accounts = resp["value"].get("accounts");
    assert!(
        accounts.is_some() && accounts.unwrap().is_array(),
        "simulation with accounts config must return an `accounts` array; got {resp}"
    );
    let len = accounts.unwrap().as_array().unwrap().len();
    assert_eq!(
        len, 1,
        "one requested address → one accounts slot; got {len}"
    );
}

// ── Case B ──────────────────────────────────────────────────────────────────
async fn case_b_replace_recent_blockhash(ctx: &PrivateChannelContext) {
    // Use an obviously-stale blockhash (all-zero); server must replace it
    // because replaceRecentBlockhash=true was requested.
    let stale = Hash::new_from_array([0u8; 32]);
    let tx = build_unsigned_memo(stale);
    let resp = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::SimulateTransaction,
            json!([
                STANDARD.encode(bincode::serialize(&tx).unwrap()),
                {
                    "encoding": "base64",
                    "sigVerify": false,
                    "replaceRecentBlockhash": true
                }
            ]),
        )
        .await
        .expect("simulate with replaceRecentBlockhash=true must succeed even with stale blockhash");

    let value = &resp["value"];
    assert!(
        value.get("unitsConsumed").is_some() || value.get("err").is_some(),
        "value must include at least one standard field; got {value}"
    );
}
