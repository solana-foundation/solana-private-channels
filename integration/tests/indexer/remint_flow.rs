//! End-to-end coverage for the deferred-remint flow
//! (`indexer/src/operator/sender/remint.rs`) via the
//! `test_hooks::{process_pending_remints, execute_deferred_remint}`
//! wrappers.
//!
//! The flow transitions a withdrawal row through Pending → PendingRemint
//! → either Completed (the withdrawal actually finalized after we
//! deferred), FailedReminted (the remint succeeded), or ManualReview
//! (both the withdrawal failed AND the remint failed). The four
//! scenarios pin the four observable terminal arms:
//!
//!   (a) **Idempotency short-circuit** — `attempt_remint`'s opening
//!       `find_existing_mint_signature_with_memo` lookup returns
//!       `Some(prior_signature)`, so the helper reports success without
//!       sending a duplicate remint. Drives the
//!       `execute_deferred_remint` happy-path-via-idempotency arm and
//!       emits `FailedReminted` carrying the prior sig.
//!
//!   (b) **Withdrawal actually finalized** — finality check returns a
//!       finalized success for one of the stashed signatures, so
//!       `process_pending_remints` skips the remint and emits
//!       `Completed` with the finalized sig as the
//!       counterpart_signature.
//!
//!   (c) **Finality-check RPC error, attempts < MAX** — entry is
//!       re-queued with a fresh deadline and `finality_check_attempts`
//!       incremented; no status update.
//!
//!   (d) **Finality-check RPC error, attempts == MAX-1** — next failure
//!       trips the cap and emits `ManualReview` whose error message
//!       names the underlying RPC failure.
//!
//!   (d.1) **Liveness gate, sig dropped + lvbh valid**: gate defers
//!         rather than reminting because the broadcast could still land.
//!
//!   (d.2) **Liveness gate, sig confirmed-not-finalized + lvbh expired**:
//!         regression guard. A confirmed tx will finalize regardless of
//!         lvbh, so the gate must defer (not remint).
//!
//!   (d.3) **Liveness gate at cap**: `ManualReview` whose error message
//!         names the liveness cause (distinct from RPC failure).

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    private_channel_indexer::{
        config::ProgramType,
        operator::{
            sender::{
                test_hooks,
                types::{PendingRemint, PendingSig, SenderState, TransactionStatusUpdate},
            },
            utils::instruction_util::{remint_idempotency_memo, WithdrawalRemintInfo},
            SignerUtil,
        },
        storage::{common::storage::mock::MockStorage, Storage, TransactionStatus},
    },
    sender_fixtures::{
        blockhash_reply, confirmed_status_reply, ensure_admin_signer_env, make_config,
        make_remint_info, send_transaction_echo_reply, withdrawal_ctx,
    },
    serde_json::json,
    solana_keychain::SolanaSigner,
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature},
    std::{str::FromStr, sync::Arc},
    test_utils::mock_rpc::{MockRpcServer, Reply},
    tokio::sync::mpsc,
};

async fn build_state(
    rpc_url: String,
) -> (
    SenderState,
    mpsc::Receiver<TransactionStatusUpdate>,
    mpsc::Sender<TransactionStatusUpdate>,
    MockStorage,
) {
    ensure_admin_signer_env();
    let mock = MockStorage::new();
    let storage = Arc::new(Storage::Mock(mock.clone()));
    let state = test_hooks::new_sender_state(
        &make_config(rpc_url, ProgramType::Withdraw),
        CommitmentLevel::Confirmed,
        None,
        storage,
        1,
        // Tight confirmation poll interval — the remint flow's
        // `check_transaction_status` consumes this for any send path.
        1,
        None,
    )
    .expect("SenderState construction must succeed under Mock storage");
    let (storage_tx, storage_rx) = mpsc::channel(8);
    (state, storage_rx, storage_tx, mock)
}

/// Push a stub PendingRemint row into the mock so
/// `bump_pending_remint_finality_attempt(id, ...)` finds a row to update.
/// Defer-path tests need this; without a row the bump returns RowNotFound
/// and the fail-closed handler escalates to ManualReview.
fn seed_pending_remint_row(mock: &MockStorage, id: i64, attempts: i32) {
    use private_channel_indexer::storage::common::models::{
        DbTransaction, TransactionStatus, TransactionType,
    };
    let now = chrono::Utc::now();
    mock.pending_remint_transactions
        .lock()
        .unwrap()
        .push(DbTransaction {
            id,
            signature: Signature::new_unique().to_string(),
            trace_id: format!("trace-{id}"),
            slot: 0,
            initiator: Pubkey::new_unique().to_string(),
            recipient: Pubkey::new_unique().to_string(),
            mint: Pubkey::new_unique().to_string(),
            amount: 0,
            memo: None,
            transaction_type: TransactionType::Withdrawal,
            withdrawal_nonce: Some(id),
            status: TransactionStatus::PendingRemint,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: Some(now),
            finality_check_attempts: attempts,
            recovery_requeue_attempts: 0,
        });
}

fn make_pending_remint(
    transaction_id: i64,
    nonce: u64,
    signatures: Vec<Signature>,
    finality_check_attempts: u32,
    info: WithdrawalRemintInfo,
) -> PendingRemint {
    let pending_sigs = signatures
        .into_iter()
        .map(|signature| PendingSig {
            signature,
            last_valid_block_height: 0,
        })
        .collect();
    PendingRemint {
        ctx: withdrawal_ctx(transaction_id, nonce),
        remint_info: info,
        signatures: pending_sigs,
        original_error: "release_funds failed".to_string(),
        // Past deadline so `process_pending_remints` treats the entry
        // as matured and processes it on the first tick.
        deadline: chrono::Utc::now() - chrono::Duration::seconds(1),
        finality_check_attempts,
    }
}

/// Build a PendingRemint with one signature whose `last_valid_block_height`
/// the caller controls. Use when the test needs to exercise the gate's
/// liveness comparison (`current_height > lvbh`).
fn make_pending_remint_with_lvbh(
    transaction_id: i64,
    nonce: u64,
    signature: Signature,
    last_valid_block_height: u64,
    finality_check_attempts: u32,
) -> PendingRemint {
    PendingRemint {
        ctx: withdrawal_ctx(transaction_id, nonce),
        remint_info: make_remint_info(transaction_id),
        signatures: vec![PendingSig {
            signature,
            last_valid_block_height,
        }],
        original_error: "release_funds failed".to_string(),
        deadline: chrono::Utc::now() - chrono::Duration::seconds(1),
        finality_check_attempts,
    }
}

// ─────────────────────────────────────────────────────────────────────
// (a) Idempotency short-circuit — attempt_remint finds prior confirmed remint.
// ─────────────────────────────────────────────────────────────────────
//
// Drives `execute_deferred_remint` directly. The first call inside
// `attempt_remint` is `find_existing_mint_signature_with_memo`, which
// scripts (`getSignaturesForAddress` + `getTransaction`) to return a
// prior confirmed remint carrying the matching memo. The helper
// short-circuits before sending a new transaction and routes the
// `Ok(prior_sig)` arm to the `FailedReminted` status emission.
#[tokio::test]
async fn execute_deferred_remint_short_circuits_on_prior_confirmed_remint() {
    let mock = MockRpcServer::start().await;
    let (state, mut storage_rx, storage_tx, _mock) = build_state(mock.url()).await;

    let txn_id: i64 = 7_777;
    let info = make_remint_info(txn_id);
    let memo = remint_idempotency_memo(txn_id);

    let prior_remint_sig = Signature::from_str(
        "4BxWw1FjwQCHXWkrK4ZehPWauFTPhBafSr9m8Cuht73LG73nUs3wfuJ6gigkhNppP4pYogP5pQDENbE5nQx1Qp4B",
    )
    .unwrap();

    // Phase 1 of attempt_remint: getSignaturesForAddress on the recipient ATA.
    mock.enqueue(
        "getSignaturesForAddress",
        Reply::result(json!([
            {
                "signature": prior_remint_sig.to_string(),
                "slot": 100u64,
                "err": null,
                "memo": format!("[5] {}", memo),
                "blockTime": 1_700_000_000i64,
                "confirmationStatus": "finalized",
            }
        ])),
    );

    // Phase 2 of attempt_remint: getTransaction on the matching sig
    // returns a parsed payload whose `mintTo` info matches the remint
    // builder exactly, so the idempotency short-circuit fires.
    let memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
    let admin = SignerUtil::admin_signer().pubkey();
    mock.enqueue(
        "getTransaction",
        Reply::result(json!({
            "slot": 100,
            "blockTime": 1_700_000_000i64,
            "meta": {
                "err": null,
                "status": { "Ok": null },
                "fee": 5000u64,
                "innerInstructions": [],
                "preBalances": [1_000_000u64],
                "postBalances": [999_995u64],
                "logMessages": [],
                "preTokenBalances": [],
                "postTokenBalances": [],
                "rewards": [],
                "computeUnitsConsumed": 0u64,
            },
            "transaction": {
                "signatures": [prior_remint_sig.to_string()],
                "message": {
                    "accountKeys": [
                        { "pubkey": admin.to_string(),               "signer": true,  "writable": true,  "source": "transaction" },
                        { "pubkey": info.user_ata.to_string(),       "signer": false, "writable": true,  "source": "transaction" },
                        { "pubkey": info.mint.to_string(),           "signer": false, "writable": true,  "source": "transaction" },
                        { "pubkey": info.token_program.to_string(),  "signer": false, "writable": false, "source": "transaction" },
                        { "pubkey": memo_program_id,                 "signer": false, "writable": false, "source": "transaction" },
                    ],
                    "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                    "instructions": [
                        { "program": "spl-memo", "programId": memo_program_id, "parsed": memo },
                        {
                            "program": "spl-token",
                            "programId": info.token_program.to_string(),
                            "parsed": {
                                "type": "mintTo",
                                "info": {
                                    "mint": info.mint.to_string(),
                                    "account": info.user_ata.to_string(),
                                    "mintAuthority": admin.to_string(),
                                    "amount": info.amount.to_string(),
                                },
                            },
                        },
                    ],
                },
            },
        })),
    );

    let entry = make_pending_remint(txn_id, 7, vec![Signature::new_unique()], 0, info);
    test_hooks::execute_deferred_remint(&state, &entry, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("idempotency short-circuit must emit a FailedReminted update");
    assert_eq!(update.transaction_id, txn_id);
    assert_eq!(update.status, TransactionStatus::FailedReminted);
    assert_eq!(
        update.remint_signature.as_deref(),
        Some(prior_remint_sig.to_string().as_str()),
        "remint_signature must echo the prior confirmed remint"
    );
    assert!(
        update.remint_attempted,
        "FailedReminted must mark remint_attempted=true"
    );
    // Critically: no `sendTransaction` call. The whole point of the
    // idempotency check is to avoid duplicate on-chain submissions.
    assert_eq!(
        mock.call_count("sendTransaction"),
        0,
        "idempotency match must skip the wire send entirely"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (b) Withdrawal actually finalized — process_pending_remints emits Completed.
// ─────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn process_pending_remints_skips_remint_when_withdrawal_finalized() {
    let mock = MockRpcServer::start().await;
    let (mut state, mut storage_rx, storage_tx, _mock) = build_state(mock.url()).await;

    let withdrawal_sig = Signature::new_unique();
    state.pending_remints.push(make_pending_remint(
        91,
        3,
        vec![withdrawal_sig],
        0,
        make_remint_info(91),
    ));

    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": { "slot": 200 },
            "value": [{
                "slot": 100,
                "confirmations": null,
                "err": null,
                "status": { "Ok": null },
                "confirmationStatus": "finalized"
            }]
        })),
    );

    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("finalized-withdrawal arm must emit a Completed update");
    assert_eq!(update.transaction_id, 91);
    assert_eq!(update.status, TransactionStatus::Completed);
    assert_eq!(
        update.counterpart_signature.as_deref(),
        Some(withdrawal_sig.to_string().as_str())
    );
    assert!(
        state.pending_remints.is_empty(),
        "entry must be consumed once Completed is emitted"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (c) Finality-check RPC error, attempts < MAX → re-queue.
// ─────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn process_pending_remints_requeues_on_finality_check_rpc_error() {
    let mock = MockRpcServer::start().await;
    let (mut state, mut storage_rx, storage_tx, storage_mock) = build_state(mock.url()).await;

    seed_pending_remint_row(&storage_mock, 92, 0);

    state.pending_remints.push(make_pending_remint(
        92,
        4,
        vec![Signature::new_unique()],
        0,
        make_remint_info(92),
    ));

    // The RpcClientWithRetry default retry loop is wired on top of the
    // raw call. Five errors guarantee the wrapper exhausts and surfaces
    // the error to `process_pending_remints` regardless of retry budget.
    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "rpc dead 1"),
            Reply::error(-32000, "rpc dead 2"),
            Reply::error(-32000, "rpc dead 3"),
            Reply::error(-32000, "rpc dead 4"),
            Reply::error(-32000, "rpc dead 5"),
        ],
    );

    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    assert!(
        storage_rx.try_recv().is_err(),
        "no status update on the re-queue branch"
    );
    assert_eq!(
        state.pending_remints.len(),
        1,
        "entry must be re-queued, not consumed"
    );
    assert_eq!(
        state.pending_remints[0].finality_check_attempts, 1,
        "finality_check_attempts must increment by 1 per failed cycle"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (d) Finality-check RPC error at MAX-1 attempts → ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// MAX_FINALITY_CHECK_ATTEMPTS is 3. An entry already at attempts=2
// (one less than MAX) hits the cap on the next failure and routes to
// ManualReview rather than re-queueing.
#[tokio::test]
async fn process_pending_remints_routes_to_manual_review_at_max_attempts() {
    let mock = MockRpcServer::start().await;
    let (mut state, mut storage_rx, storage_tx, _mock) = build_state(mock.url()).await;

    state.pending_remints.push(make_pending_remint(
        93,
        5,
        vec![Signature::new_unique()],
        2, // MAX_FINALITY_CHECK_ATTEMPTS - 1
        make_remint_info(93),
    ));

    mock.enqueue_sequence(
        "getSignatureStatuses",
        vec![
            Reply::error(-32000, "rpc dead 1"),
            Reply::error(-32000, "rpc dead 2"),
            Reply::error(-32000, "rpc dead 3"),
            Reply::error(-32000, "rpc dead 4"),
            Reply::error(-32000, "rpc dead 5"),
        ],
    );

    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("max-attempts arm must emit a ManualReview update");
    assert_eq!(update.transaction_id, 93);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("escalated to ManualReview"),
        "ManualReview at MAX must surface the escalation label; got {msg:?}"
    );
    assert!(
        msg.contains("signature status RPC failed"),
        "ManualReview must surface the underlying RPC failure; got {msg:?}"
    );
    assert!(
        msg.contains("release_funds failed"),
        "ManualReview must preserve the original withdrawal error; got {msg:?}"
    );
    assert!(
        state.pending_remints.is_empty(),
        "entry must NOT be re-queued past the cap"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (d.1) Liveness gate, sig dropped but lvbh still valid → defer.
// ─────────────────────────────────────────────────────────────────────
//
// Cluster has no record of the broadcast (null status). The blockhash
// is still within its validity window, so a re-broadcast could land.
// The gate must defer rather than remint.
#[tokio::test]
async fn process_pending_remints_defers_when_sig_within_blockhash_validity() {
    let mock = MockRpcServer::start().await;
    let (mut state, mut storage_rx, storage_tx, storage_mock) = build_state(mock.url()).await;

    seed_pending_remint_row(&storage_mock, 94, 0);

    let sig = Signature::new_unique();
    state
        .pending_remints
        .push(make_pending_remint_with_lvbh(94, 6, sig, 1_000, 0));

    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": { "slot": 200 },
            "value": [null]
        })),
    );
    // current_height (50) <= lvbh (1_000): sig still within validity.
    mock.enqueue("getBlockHeight", Reply::result(json!(50)));

    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    assert!(
        storage_rx.try_recv().is_err(),
        "row must stay PendingRemint while the broadcast could still land"
    );
    assert_eq!(state.pending_remints.len(), 1, "entry must be re-queued");
    assert_eq!(
        state.pending_remints[0].finality_check_attempts, 1,
        "deferral counter must increment by 1"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (d.2) Liveness gate, sig confirmed-not-finalized past lvbh → defer.
// ─────────────────────────────────────────────────────────────────────
//
// Regression guard for the case where a low-fee broadcast lands late in
// its validity window: status is `confirmed` (already in a block) and
// the cluster has moved past lvbh, but finalization is still pending.
// The tx will finalize regardless of lvbh, so the gate must defer.
// Reminting here would double-pay when the tx finalizes a few slots later.
#[tokio::test]
async fn process_pending_remints_defers_when_sig_confirmed_not_finalized() {
    let mock = MockRpcServer::start().await;
    let (mut state, mut storage_rx, storage_tx, storage_mock) = build_state(mock.url()).await;

    seed_pending_remint_row(&storage_mock, 95, 0);

    let sig = Signature::new_unique();
    state
        .pending_remints
        .push(make_pending_remint_with_lvbh(95, 7, sig, 100, 0));

    // Status: confirmed (in a block) but not yet finalized, no error.
    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": { "slot": 200 },
            "value": [{
                "slot": 100,
                "confirmations": 1,
                "err": null,
                "status": { "Ok": null },
                "confirmationStatus": "confirmed"
            }]
        })),
    );
    // current_height (1_000) > lvbh (100). The old buggy gate would
    // treat the sig as expired here and remint.
    mock.enqueue("getBlockHeight", Reply::result(json!(1_000)));

    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    assert!(
        storage_rx.try_recv().is_err(),
        "a confirmed-but-not-finalized sig must defer the remint"
    );
    assert_eq!(state.pending_remints.len(), 1);
    assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (d.3) Liveness gate at cap → ManualReview with liveness reason.
// ─────────────────────────────────────────────────────────────────────
//
// Entry already at MAX-1 attempts. On this tick the sig is still live,
// so the cap fires. The escalation message must identify the cause as
// liveness, not an RPC failure, so operators can triage correctly.
#[tokio::test]
async fn process_pending_remints_liveness_cap_escalates_with_liveness_reason() {
    let mock = MockRpcServer::start().await;
    let (mut state, mut storage_rx, storage_tx, _mock) = build_state(mock.url()).await;

    let sig = Signature::new_unique();
    state
        .pending_remints
        .push(make_pending_remint_with_lvbh(96, 8, sig, 1_000, 2));

    mock.enqueue(
        "getSignatureStatuses",
        Reply::result(json!({
            "context": { "slot": 200 },
            "value": [null]
        })),
    );
    mock.enqueue("getBlockHeight", Reply::result(json!(50)));

    test_hooks::process_pending_remints(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("cap must emit a ManualReview update");
    assert_eq!(update.transaction_id, 96);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("signatures still within blockhash validity"),
        "escalation must name the liveness cause; got {msg:?}"
    );
    assert!(state.pending_remints.is_empty(), "entry must not re-queue");
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (e) attempt_remint send + confirm path — emits FailedReminted with new sig.
// ─────────────────────────────────────────────────────────────────────
//
// Idempotency lookup returns empty, so `attempt_remint` proceeds to
// build instructions, sign-and-send, and check confirmation. With all
// three RPC calls scripted to succeed, the helper returns
// `Ok(new_sig)` and `execute_deferred_remint` emits FailedReminted
// carrying the freshly-minted remint signature (NOT a prior idempotent
// match).
#[tokio::test]
async fn execute_deferred_remint_emits_failed_reminted_after_successful_send() {
    let mock = MockRpcServer::start().await;
    let (state, mut storage_rx, storage_tx, _mock) = build_state(mock.url()).await;

    let txn_id: i64 = 7_001;
    let info = make_remint_info(txn_id);

    // Idempotency lookup: no prior remint.
    mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    // Send + confirm: full happy path.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());

    let entry = make_pending_remint(txn_id, 31, vec![Signature::new_unique()], 0, info);
    test_hooks::execute_deferred_remint(&state, &entry, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("successful remint must emit a FailedReminted update");
    assert_eq!(update.transaction_id, txn_id);
    assert_eq!(update.status, TransactionStatus::FailedReminted);
    assert!(
        update.remint_signature.is_some(),
        "FailedReminted must carry the new remint signature"
    );
    assert!(
        update.remint_attempted,
        "FailedReminted must mark remint_attempted=true"
    );
    // Critically: every wire step ran. No reply queued unconsumed.
    assert_eq!(mock.call_count("sendTransaction"), 1);
    assert_eq!(mock.call_count("getSignatureStatuses"), 1);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// (f) attempt_remint send fails — ManualReview combined error.
// ─────────────────────────────────────────────────────────────────────
//
// Idempotency lookup returns empty; `sendTransaction` returns a
// permanent error. `attempt_remint` returns `Err`, and
// `execute_deferred_remint` takes the failure arm — emits
// ManualReview with the combined "<original_error> | remint failed:
// <send_error>" message.
#[tokio::test]
async fn execute_deferred_remint_emits_manual_review_when_send_fails() {
    let mock = MockRpcServer::start().await;
    let (state, mut storage_rx, storage_tx, _mock) = build_state(mock.url()).await;

    let txn_id: i64 = 7_002;
    let info = make_remint_info(txn_id);

    mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", Reply::error(-32601, "method not found"));

    let entry = make_pending_remint(txn_id, 32, vec![Signature::new_unique()], 0, info);
    test_hooks::execute_deferred_remint(&state, &entry, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("send-failure arm must emit a ManualReview update");
    assert_eq!(update.transaction_id, txn_id);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("remint failed"),
        "ManualReview message must surface the 'remint failed' label; got {msg:?}"
    );
    assert!(
        msg.contains("release_funds failed"),
        "ManualReview message must preserve the original withdrawal error; got {msg:?}"
    );
    assert!(
        update.remint_attempted,
        "send-failure arm must mark remint_attempted=true (we tried)"
    );
    assert!(
        update.remint_signature.is_none(),
        "no remint signature when the send itself failed"
    );
    mock.shutdown().await;
}
