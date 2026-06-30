//! Coverage for the `Ordering::Equal` (tree-matched) branch of
//! `drain_rotation_retry_queue`. The future and stale branches are pure
//! in-memory logic and live in the sender module's unit tests; the matched
//! branch hands off to `handle_transaction_submission`, which needs the admin
//! signer and a live RPC — so it is exercised here, against the mock-RPC
//! harness, rather than in a unit test.

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    private_channel_escrow_program_client::instructions::ReleaseFundsBuilder,
    private_channel_indexer::{
        operator::{
            sender::{
                test_hooks,
                types::{SenderSMTState, TransactionContext},
            },
            utils::smt_util::SmtState,
        },
        storage::TransactionStatus,
    },
    sender_fixtures::{
        blockhash_reply, build_default_sender_state, confirmed_status_reply, make_remint_info,
        send_transaction_echo_reply,
    },
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

/// A `ReleaseFundsBuilder` with every account populated, so the proof build
/// produces a real instruction. Mirrors the sender module's proof tests.
fn make_release_funds_builder(nonce: u64) -> ReleaseFundsBuilder {
    let pk = Pubkey::new_unique();
    let mut builder = ReleaseFundsBuilder::new();
    builder
        .payer(pk)
        .operator(pk)
        .instance(pk)
        .operator_pda(pk)
        .mint(pk)
        .allowed_mint(pk)
        .user_ata(pk)
        .instance_ata(pk)
        .token_program(spl_token::id())
        .user(pk)
        .amount(1000)
        .transaction_nonce(nonce);
    builder
}

/// Matched mismatch: the queued nonce's tree equals the local SMT tree, so the
/// drain rebuilds and submits it rather than requeuing or escalating. The item
/// must leave the queue and land in the SMT, and no ManualReview is emitted.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_drain_matched_submits() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    // Full happy-path wire script so the submission builds, sends, and confirms.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());

    // Local SMT on tree 0; nonce 2 belongs to tree 0 (matched).
    let nonce = 2u64;
    state.smt_state = Some(SenderSMTState {
        smt_state: SmtState::new(0),
        nonce_to_builder: HashMap::new(),
    });
    state.remint_cache.insert(nonce, make_remint_info(20));
    state.rotation_retry_queue.push((
        TransactionContext {
            transaction_id: Some(20),
            withdrawal_nonce: Some(nonce),
            trace_id: Some("trace-2".to_string()),
        },
        make_release_funds_builder(nonce),
    ));

    test_hooks::drain_rotation_retry_queue(&mut state, &storage_tx).await;

    // Routed to submission: gone from the queue and inserted into the tree.
    assert!(state.rotation_retry_queue.is_empty());
    assert!(state
        .smt_state
        .as_ref()
        .unwrap()
        .smt_state
        .contains_nonce(nonce));
    // A matched item is submitted, never escalated.
    if let Ok(update) = storage_rx.try_recv() {
        assert_ne!(update.status, TransactionStatus::ManualReview);
    }

    mock.shutdown().await;
}
