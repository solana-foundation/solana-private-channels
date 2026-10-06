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
//!
//!   C. **System instruction not in allowlist**: the System program is
//!      refused. A signed `Allocate` must be rejected at ingress (C1) and must
//!      leave no account behind (C2), which is the end-to-end
//!      no-persistent-state invariant.
//!
//!   D. **System Transfer refused**: ten 1-lamport transfers from a fresh
//!      payer are rejected at ingress and create none of the ten recipients.
//!
//!   E. **Token close to a fresh address**: ATA create plus `CloseAccount`
//!      moves a float lamport with no System instruction, so ingress admits it.
//!      Execution burns the fresh destination and fails the tx, so no row lands.

use solana_commitment_config::CommitmentConfig;
use {
    super::test_context::PrivateChannelContext,
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_core::test_helpers::duplicate_account_keys_transaction,
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    solana_system_interface::instruction as system_instruction,
    std::time::Duration,
};

const INVALID_PARAMS_CODE: i64 = -32_602;

/// Generous because it only bounds the failure case; a healthy pipeline returns in well under a second.
const LIVENESS_PROBE_SECONDS: u64 = 60;

pub async fn run_send_transaction_errors_test(ctx: &PrivateChannelContext) {
    println!("\n=== sendTransaction — Error Classification ===");

    case_a_base64_decode_failure(ctx).await;
    case_b_oversized_transaction(ctx).await;
    case_c_duplicate_account_keys(ctx).await;

    case_c_system_allocate_rejected(ctx).await;
    case_d_system_transfer_rejected(ctx).await;
    case_e_token_close_to_fresh_address_creates_nothing(ctx).await;

    println!("✓ base64-decode + oversized + duplicate-key + System-allocate + System-transfer + token-close branches passed");
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
async fn case_c_system_allocate_rejected(ctx: &PrivateChannelContext) {
    let payer = Keypair::new();
    let fresh = Keypair::new();
    let blockhash = ctx
        .get_blockhash()
        .await
        .expect("blockhash for the allocate tx");

    // `allocate` marks its account as a signer, so `fresh` must sign too or the
    // tx fails sanitization and this case would pass for the wrong reason.
    let ix = system_instruction::allocate(&fresh.pubkey(), 10 * 1024 * 1024);
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&payer.pubkey()),
        &[&payer, &fresh],
        blockhash,
    );

    // C1: the RPC surface itself rejects it, not just the unit-level predicate.
    let err = ctx
        .write_client
        .send_transaction(&tx)
        .await
        .expect_err("System Allocate must be rejected at ingress");
    let msg = err.to_string();
    assert!(
        msg.contains("Only SPL token") || msg.contains(&INVALID_PARAMS_CODE.to_string()),
        "error must name the allowlist as the cause; got: {msg}"
    );

    // C2: the invariant the issue is about, no account row is created.
    let account = ctx
        .read_client
        .get_account_with_commitment(&fresh.pubkey(), CommitmentConfig::processed())
        .await
        .expect("get_account_with_commitment must succeed");
    assert!(
        account.value.is_none(),
        "a rejected allocate must leave no account behind"
    );
}

/// Absence check shared by the cases that must leave no account behind.
async fn assert_no_account(ctx: &PrivateChannelContext, pubkey: &Pubkey, what: &str) {
    let account = ctx
        .read_client
        .get_account_with_commitment(pubkey, CommitmentConfig::processed())
        .await
        .expect("get_account_with_commitment must succeed");
    assert!(account.value.is_none(), "{what} {pubkey} must not exist");
}

// ── Case D ──────────────────────────────────────────────────────────────────
async fn case_d_system_transfer_rejected(ctx: &PrivateChannelContext) {
    let payer = Keypair::new();
    let recipients: Vec<Pubkey> = (0..10).map(|_| Pubkey::new_unique()).collect();
    let ixs: Vec<Instruction> = recipients
        .iter()
        .map(|r| system_instruction::transfer(&payer.pubkey(), r, 1))
        .collect();
    let blockhash = ctx
        .get_blockhash()
        .await
        .expect("blockhash for the transfer tx");
    let tx = Transaction::new_signed_with_payer(&ixs, Some(&payer.pubkey()), &[&payer], blockhash);

    let err = ctx
        .write_client
        .send_transaction(&tx)
        .await
        .expect_err("System Transfer must be rejected at ingress");
    let msg = err.to_string();
    assert!(
        msg.contains("Only SPL token"),
        "error must name the allowlist as the cause; got: {msg}"
    );

    for r in &recipients {
        assert_no_account(ctx, r, "a refused transfer's recipient").await;
    }
}

// ── Case E ──────────────────────────────────────────────────────────────────
async fn case_e_token_close_to_fresh_address_creates_nothing(ctx: &PrivateChannelContext) {
    // The admin creates a mint so the ATA create below has a real one to use.
    let mint = Keypair::new();
    let blockhash = ctx.get_blockhash().await.expect("blockhash for the mint");
    let init = crate::setup::create_mint_account_transaction(
        &ctx.operator_key,
        &mint,
        &ctx.operator_key.pubkey(),
        6,
        blockhash,
    );
    ctx.send_and_check(&init, Duration::from_secs(LIVENESS_PROBE_SECONDS))
        .await
        .expect("mint send must not error")
        .expect("the mint must land");

    let p = Keypair::new();
    let d = Pubkey::new_unique();
    let ata =
        spl_associated_token_account::get_associated_token_address(&p.pubkey(), &mint.pubkey());
    let ixs = [
        spl_associated_token_account::instruction::create_associated_token_account(
            &p.pubkey(),
            &p.pubkey(),
            &mint.pubkey(),
            &spl_token::id(),
        ),
        spl_token::instruction::close_account(&spl_token::id(), &ata, &d, &p.pubkey(), &[])
            .expect("close_account ix"),
    ];
    let blockhash = ctx.get_blockhash().await.expect("blockhash for the close");
    let tx = Transaction::new_signed_with_payer(&ixs, Some(&p.pubkey()), &[&p], blockhash);

    let sig = ctx
        .send_and_check(&tx, Duration::from_secs(LIVENESS_PROBE_SECONDS))
        .await
        .expect("ingress must admit the token close")
        .expect("the token close must land");
    let response = ctx
        .get_transaction(&sig)
        .await
        .expect("get_transaction must not error")
        .expect("the landed tx must be retrievable");
    let err = response
        .pointer("/meta/err")
        .unwrap_or(&serde_json::Value::Null);
    assert!(
        err.to_string().contains("UnbalancedTransaction"),
        "a float lamport paid to a fresh address must fail the tx; got err: {err}"
    );

    assert_no_account(ctx, &d, "the close destination").await;
    assert_no_account(ctx, &ata, "the closed ATA").await;
}
