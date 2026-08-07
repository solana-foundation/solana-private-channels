//! `test_send_transaction_error_classification`
//!
//! Target file: `core/src/rpc/send_transaction_impl.rs`.
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! Covers the non-SDK-duplication branches in `send_transaction_impl`:
//!
//!   A. **Base64 decode failure** — SDK `send_transaction` does client-side
//!      pre-encoding, so an entirely-invalid-base64 case doesn't reach the
//!      server. We therefore use the lower-level `send::<T>(RpcRequest::
//!      SendTransaction, ...)` path and pass a string we know base64 cannot
//!      decode. Hits the base64-decode error arm.
//!
//!   B. **Oversized transaction** — constructs a binary blob >
//!      `PACKET_DATA_SIZE` (1232 bytes), base64-encodes it, and sends. The
//!      server must reject with `INVALID_PARAMS_CODE` before the pipeline
//!      is entered. Hits the size-check arm.
//!
//!   C. **Duplicate account keys**: a hand-assembled legacy message whose
//!      `account_keys` repeat a pubkey. It clears sanitization, the allowlist
//!      and sigverify, so without the ingress lock-validation guard it reaches
//!      the sequencer and aborts the write node. The assertion names the
//!      duplicate-key reason specifically, because every other rejection arm
//!      in this handler also returns `INVALID_PARAMS_CODE` and would otherwise
//!      satisfy it. A memo transaction must then land, which only happens if
//!      the sequencer survived.
//!
//! "Program not in allowlist" is out of scope here because the allowlist
//! enforcement lives in a separate later stage and requires configuration
//! plumbing not part of the default `PrivateChannelContext`. That branch can be
//! a follow-up test when the context exposes a runtime allowlist toggle.

use {
    super::test_context::PrivateChannelContext,
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_core::test_helpers::duplicate_account_keys_transaction,
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{
        instruction::Instruction,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::time::Duration,
};

const INVALID_PARAMS_CODE: i64 = -32_602;

/// Generous because it only bounds the failure case; a healthy pipeline returns in well under a second.
const LIVENESS_PROBE_SECONDS: u64 = 15;

pub async fn run_send_transaction_errors_test(ctx: &PrivateChannelContext) {
    println!("\n=== sendTransaction — Error Classification ===");

    case_a_base64_decode_failure(ctx).await;
    case_b_oversized_transaction(ctx).await;
    case_c_duplicate_account_keys(ctx).await;

    println!("✓ base64-decode + oversized + duplicate-key branches passed");
}

// ── Case A ──────────────────────────────────────────────────────────────────
async fn case_a_base64_decode_failure(ctx: &PrivateChannelContext) {
    // A string that STANDARD engine cannot decode (invalid chars + bad padding).
    // Sent as raw `SendTransaction` params to bypass client-side pre-encoding.
    let bad = "!!not-base64!!";
    let err = ctx
        .write_client
        .send::<serde_json::Value>(
            RpcRequest::SendTransaction,
            json!([bad, {"skipPreflight": true, "encoding": "base64"}]),
        )
        .await
        .expect_err("invalid base64 must be rejected by the server");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("base64")
            || msg.contains("invalid")
            || msg.contains(&INVALID_PARAMS_CODE.to_string()),
        "error must name base64/invalid-param as cause; got: {msg}"
    );
}

// ── Case B ──────────────────────────────────────────────────────────────────
async fn case_b_oversized_transaction(ctx: &PrivateChannelContext) {
    // PACKET_DATA_SIZE = 1232; send 1233 bytes of junk — valid base64, but
    // the decoded length exceeds the packet limit so the handler rejects
    // before attempting bincode deserialization.
    let junk = vec![0u8; 1233];
    let encoded = STANDARD.encode(&junk);
    let err = ctx
        .write_client
        .send::<serde_json::Value>(
            RpcRequest::SendTransaction,
            json!([encoded, {"skipPreflight": true, "encoding": "base64"}]),
        )
        .await
        .expect_err("oversized tx must be rejected");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("too large") || msg.contains("1232") || msg.contains("1233"),
        "error must identify size as the cause; got: {msg}"
    );
}

// ── Case C ──────────────────────────────────────────────────────────────────
async fn case_c_duplicate_account_keys(ctx: &PrivateChannelContext) {
    // A live blockhash keeps the transaction on the path the guard protects:
    // without the guard a stale one would be dropped by dedup instead of
    // reaching the sequencer, so the case would no longer describe the bug.
    let blockhash = ctx
        .get_blockhash()
        .await
        .expect("live blockhash for the duplicate-key tx");
    let payer = Keypair::new();
    let tx = duplicate_account_keys_transaction(&payer, blockhash);
    let encoded = STANDARD.encode(bincode::serialize(&tx).expect("serialize duplicate-key tx"));

    let err = ctx
        .write_client
        .send::<serde_json::Value>(
            RpcRequest::SendTransaction,
            json!([encoded, {"skipPreflight": true, "encoding": "base64"}]),
        )
        .await
        .expect_err("duplicate account keys must be rejected");

    // Naming the reason matters: the base64, size, sanitize and allowlist arms
    // all return the same code, so accepting a bare code would let this case
    // stay green even if the guard under test were deleted.
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("account loaded twice"),
        "error must name the duplicate-key cause; got: {msg}"
    );
    assert!(
        msg.contains(&INVALID_PARAMS_CODE.to_string()),
        "duplicate keys are a client error; got: {msg}"
    );

    // The probe has to wait for the memo to land, not merely be accepted.
    // sendTransaction returns as soon as the tx is queued, three stages ahead of
    // the sequencer, and this harness never polls wait_for_any_worker_quit, so
    // an acceptance-only check would still pass with the write pipeline dead.
    // A landed transaction is proof the sequencer survived the rejected one.
    let probe_blockhash = ctx
        .get_blockhash()
        .await
        .expect("live blockhash for the liveness probe");
    let probe = memo_tx(probe_blockhash, "ottersec-14-liveness");
    let landed = ctx
        .send_and_check(&probe, Duration::from_secs(LIVENESS_PROBE_SECONDS))
        .await
        .expect("liveness probe must not error");
    assert!(
        landed.is_some(),
        "sequencer must still land transactions after the duplicate-key rejection"
    );
}

/// Unique allowlisted memo tx; the memo program is loaded in the node VM so this lands.
fn memo_tx(blockhash: solana_sdk::hash::Hash, tag: &str) -> Transaction {
    let payer = Keypair::new();
    let memo = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: tag.as_bytes().to_vec(),
    };
    Transaction::new_signed_with_payer(&[memo], Some(&payer.pubkey()), &[&payer], blockhash)
}
