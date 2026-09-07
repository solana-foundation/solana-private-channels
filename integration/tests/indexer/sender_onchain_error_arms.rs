//! End-to-end coverage for `handle_confirmation_result`
//! (`indexer/src/operator/sender/transaction.rs`) via the
//! `test_hooks::handle_confirmation_result` wrapper.
//!
//! The function is the central error router for confirmed-but-failed
//! transactions. Each scenario synthesises a
//! `Result<ConfirmationResult, TransactionError>` corresponding to one
//! match arm and verifies the arm fires by inspecting the
//! `TransactionStatusUpdate` the production helper emits — every fatal
//! arm passes a distinct `error_msg` string into
//! `handle_permanent_failure → send_fatal_error`, so the per-arm route
//! is identifiable from the status update alone.
//!
//! Out of scope (covered separately):
//!   - `Ok(Confirmed)` arm — already exercised by the
//!     `sender_poll_rpc_error` success scenario via `handle_success`.
//!   - `Ok(Failed(NonceAlreadyUsed))` and
//!     `Ok(Failed(NonceOutsideCurrentGeneration))` arms: both read the
//!     bitmap, so they are pinned by the sender unit tests instead.
//!   - `Ok(MintNotInitialized)` JIT-init arm — covered by `jit_mint_helper`.
//!   - `Ok(Retry) + Idempotent` arm — recursive `send_and_confirm`,
//!     covered transitively by the next iteration's wire scripting.

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError,
    private_channel_indexer::{
        config::ProgramType,
        error::{ProgramError, TransactionError},
        operator::{
            sender::{test_hooks, types::TransactionStatusUpdate},
            utils::{
                instruction_util::{
                    ExtraErrorCheckPolicy, MintToBuilder, RetryPolicy, TransactionKind,
                },
                transaction_util::ConfirmationResult,
            },
            SignerUtil,
        },
        storage::{
            common::{models::DbMint, storage::mock::MockStorage},
            Storage, TransactionStatus,
        },
    },
    sender_fixtures::{
        account_info_reply_bytes, blockhash_reply, build_default_sender_state,
        confirmed_status_reply, deposit_ctx, ensure_admin_signer_env, make_config,
        make_instruction, pack_mint_with_authority, send_transaction_echo_reply,
    },
    solana_keychain::SolanaSigner,
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature},
    spl_token::{
        solana_program::{program_option::COption, program_pack::Pack},
        state::Mint,
    },
    std::sync::Arc,
    test_utils::mock_rpc::MockRpcServer,
};

/// Drive the hook with a synthesised `result` and return the single
/// status update the production code emits.
async fn drive_and_recv(
    result: Result<ConfirmationResult, TransactionError>,
    retry_policy: RetryPolicy,
    txn_id: i64,
) -> TransactionStatusUpdate {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;
    let ctx = deposit_ctx(txn_id);
    test_hooks::handle_confirmation_result(
        &mut state,
        result,
        Signature::new_unique(),
        None,
        &ctx,
        make_instruction(),
        retry_policy,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;
    let update = storage_rx
        .recv()
        .await
        .expect("fatal arm must emit a status update");
    mock.shutdown().await;
    update
}

// ─────────────────────────────────────────────────────────────────────
// NonceOutsideCurrentGeneration with no nonce: fatal arm.
// ─────────────────────────────────────────────────────────────────────
//
// The routing arm needs a nonce to decide which side of the window the
// rejection falls on. Without one there is nothing to place, so it drops
// through to a plain permanent failure.
#[tokio::test]
async fn nonce_outside_generation_without_nonce_routes_to_fatal_arm() {
    let result = Ok(ConfirmationResult::Failed(Some(
        PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration,
    )));
    let update = drive_and_recv(result, RetryPolicy::Idempotent, 401).await;
    assert_eq!(update.transaction_id, 401);
    assert_eq!(update.status, TransactionStatus::Failed);
    assert_eq!(
        update.error_message.as_deref(),
        Some("nonce outside the current bitmap generation"),
        "the generation arm must pass its specific error message; got {:?}",
        update.error_message
    );
}

// ─────────────────────────────────────────────────────────────────────
// Failed(Some(other)) — generic catch-all arm.
// ─────────────────────────────────────────────────────────────────────
//
// Any program error not specifically routed falls into the generic
// `Failed(program_error)` catch-all and is debug-formatted into the
// error message.
#[tokio::test]
async fn unmapped_program_error_routes_to_generic_failed_arm() {
    let result = Ok(ConfirmationResult::Failed(Some(
        PrivateChannelEscrowProgramError::InvalidMint,
    )));
    let update = drive_and_recv(result, RetryPolicy::Idempotent, 402).await;
    assert_eq!(update.status, TransactionStatus::Failed);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("InvalidMint"),
        "generic Failed arm must debug-format the program error; got {msg:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// MintNotInitialized + no cached builder — fatal arm.
// ─────────────────────────────────────────────────────────────────────
//
// `state.mint_builders` is empty for the supplied txn_id, so the helper
// cannot attempt JIT initialization. The arm emits the
// "Unexpected mint error" error message before falling through to
// `send_fatal_error`.
#[tokio::test]
async fn mint_not_initialized_without_builder_routes_to_fatal_arm() {
    let result = Ok(ConfirmationResult::MintNotInitialized);
    let update = drive_and_recv(result, RetryPolicy::None, 403).await;
    assert_eq!(update.status, TransactionStatus::Failed);
    assert_eq!(
        update.error_message.as_deref(),
        Some("Unexpected mint error"),
        "MintNotInitialized without a cached builder must emit the 'Unexpected mint error' label"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Retry + RetryPolicy::None — fatal arm.
// ─────────────────────────────────────────────────────────────────────
//
// Confirmation timeout on a non-idempotent operation: production cannot
// safely retry (status unknown), so the arm routes straight to
// `handle_permanent_failure` with a distinct
// "unsafe to retry" suffix.
#[tokio::test]
async fn retry_under_none_policy_routes_to_fatal_arm() {
    let result = Ok(ConfirmationResult::Retry);
    let update = drive_and_recv(result, RetryPolicy::None, 404).await;
    assert_eq!(update.status, TransactionStatus::Failed);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("unsafe to retry"),
        "Retry+None must surface the 'unsafe to retry' label; got {msg:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Err(TransactionError) — fatal arm.
// ─────────────────────────────────────────────────────────────────────
//
// A failure inside the polling layer (e.g. the bitmap unreadable,
// surfacing as a ProgramError) routes through the catch-all `Err(e)`
// arm. The `error_msg` is the Display of the underlying error.
#[tokio::test]
async fn err_result_routes_to_confirmation_error_arm() {
    let result = Err(TransactionError::Program(ProgramError::BitmapUnavailable {
        reason: "rpc down".to_string(),
    }));
    let update = drive_and_recv(result, RetryPolicy::Idempotent, 405).await;
    assert_eq!(update.status, TransactionStatus::Failed);
    let msg = update.error_message.unwrap_or_default();
    // It must be non-empty and must not collide with another arm's label.
    assert!(
        !msg.is_empty(),
        "confirmation_error arm must surface the underlying error string"
    );
    assert!(
        !msg.contains("nonce outside")
            && !msg.contains("Unexpected mint")
            && !msg.contains("unsafe to retry"),
        "confirmation_error arm must not collide with another arm's error label; got {msg:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Retry + RetryPolicy::Idempotent — recursive send_and_confirm succeeds.
// ─────────────────────────────────────────────────────────────────────
//
// `RetryPolicy::Idempotent` makes the Retry arm safe to re-send (the
// nonce protects against duplicates). `handle_confirmation_result`
// calls `send_and_confirm` recursively; with the wire scripts in
// place, the recursive call confirms successfully and emits a
// `Completed` status update — NOT a Failed one. This pins the
// "retry succeeded" branch as observably distinct from every fatal
// arm above.
#[tokio::test]
async fn retry_under_idempotent_policy_recursively_resends_and_completes() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    // Scripts for the recursive send_and_confirm call.
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());

    let ctx = deposit_ctx(406);
    test_hooks::handle_confirmation_result(
        &mut state,
        Ok(ConfirmationResult::Retry),
        Signature::new_unique(),
        None,
        &ctx,
        // Recursive send_and_confirm consumes its own instruction; the
        // call shape is otherwise identical to a fresh attempt.
        make_instruction(),
        RetryPolicy::Idempotent,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;

    let update = storage_rx
        .recv()
        .await
        .expect("Idempotent retry must drive the recursive send to a Completed status");
    assert_eq!(update.transaction_id, 406);
    assert_eq!(
        update.status,
        TransactionStatus::Completed,
        "Idempotent retry must succeed via recursive send_and_confirm; got {:?}",
        update.status
    );
    // Recursive call hit the wire exactly once.
    assert_eq!(mock.call_count("sendTransaction"), 1);
    assert_eq!(mock.call_count("getSignatureStatuses"), 1);
    mock.shutdown().await;
}

// ============================================================================
// JIT-verdict caller-arm tests
//
// These exercise the rewritten `MintNotInitialized` arm in
// `handle_confirmation_result` (sender/transaction.rs) end-to-end. Each
// drives the JIT body via mocked RPC into one of the three `JitOutcome`
// variants and asserts the resulting `TransactionStatusUpdate` shape.
// ============================================================================

/// Build a `SenderState` with optional pre-seeded mint cache + builder for
/// the JIT-driven caller-arm tests. The default `build_default_sender_state`
/// fixture doesn't seed anything; these tests need both knobs.
async fn build_state_for_jit_caller_arm(
    populate_mint_cache: bool,
) -> (
    private_channel_indexer::operator::sender::types::SenderState,
    tokio::sync::mpsc::Receiver<TransactionStatusUpdate>,
    tokio::sync::mpsc::Sender<TransactionStatusUpdate>,
    MockRpcServer,
    Pubkey,
) {
    ensure_admin_signer_env();
    let mock = MockRpcServer::start().await;
    let mock_storage = MockStorage::new();
    let mint = Pubkey::new_unique();
    if populate_mint_cache {
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint::new(mint.to_string(), 6, spl_token::id().to_string()),
        );
    }
    let storage = Arc::new(Storage::Mock(mock_storage));
    let state = test_hooks::new_sender_state(
        &make_config(mock.url(), ProgramType::Escrow),
        CommitmentLevel::Confirmed,
        None,
        storage,
        1,
        1,
        None,
    )
    .expect("SenderState construction must succeed under Mock storage");
    let (storage_tx, storage_rx) = tokio::sync::mpsc::channel(8);
    (state, storage_rx, storage_tx, mock, mint)
}

/// Seed a Processing deposit row in the mock store so JIT re-fire tests have a
/// target. `requeue_attempts` seeds the durable counter the transient verdict
/// spends, so a test can start the row at the cap.
fn seed_processing_deposit(
    storage: &MockStorage,
    id: i64,
    updated_at: chrono::DateTime<chrono::Utc>,
    requeue_attempts: i32,
) {
    use private_channel_indexer::storage::common::models::DbTransactionBuilder;
    let owner = Pubkey::new_unique().to_string();
    let mut tx =
        DbTransactionBuilder::new(format!("sig-{id}"), 1, Pubkey::new_unique().to_string(), 1)
            .initiator(owner.clone())
            .recipient(owner)
            .transaction_type(private_channel_indexer::storage::TransactionType::Deposit)
            .build();
    tx.id = id;
    tx.status = TransactionStatus::Processing;
    tx.updated_at = updated_at;
    tx.recovery_requeue_attempts = requeue_attempts;
    storage.pending_transactions.lock().unwrap().push(tx);
}

/// Read a seeded deposit row's status and durable requeue counter together.
fn deposit_row_state(storage: &MockStorage, id: i64) -> (TransactionStatus, i32) {
    let rows = storage.pending_transactions.lock().unwrap();
    let row = rows
        .iter()
        .find(|t| t.id == id)
        .expect("row must be seeded");
    (row.status, row.recovery_requeue_attempts)
}

fn make_mint_builder_for_caller_arm(mint: Pubkey) -> MintToBuilder {
    let mut builder = MintToBuilder::new();
    let admin = SignerUtil::admin_signer().pubkey();
    builder
        .mint(mint)
        .recipient(Pubkey::new_unique())
        .recipient_ata(Pubkey::new_unique())
        .payer(admin)
        .mint_authority(admin)
        .token_program(spl_token::id())
        .amount(1_000)
        .idempotency_memo("private_channel:mint-idempotency:1".to_string());
    builder
}

/// Drive the caller arm with `Ok(MintNotInitialized)` after seeding the
/// mint_builders entry. Pulled out so each test reads as a wire-script plus
/// assertions block. `claim_lease` is the ownership lease the JIT re-fire
/// presents; only the Retry outcome needs one.
async fn drive_caller_arm_with_jit_setup(
    state: &mut private_channel_indexer::operator::sender::types::SenderState,
    txn_id: i64,
    mint: Pubkey,
    claim_lease: Option<chrono::DateTime<chrono::Utc>>,
    storage_tx: &tokio::sync::mpsc::Sender<TransactionStatusUpdate>,
) {
    state
        .mint_builders
        .insert(txn_id, make_mint_builder_for_caller_arm(mint));
    // The #197 claim lease is a follow-up port, so every arm uses the plain ctx.
    let _ = claim_lease;
    let ctx = deposit_ctx(txn_id);
    test_hooks::handle_confirmation_result(
        state,
        Ok(ConfirmationResult::MintNotInitialized),
        Signature::new_unique(),
        None,
        &ctx,
        make_instruction(),
        RetryPolicy::None,
        // Synthesised `Ok(MintNotInitialized)` bypasses the classifier so
        // ExtraErrorCheckPolicy::None is fine here.
        &ExtraErrorCheckPolicy::None,
        storage_tx,
    )
    .await
}

/// JitOutcome::Retry path - the retry is re-issued through the write-ahead
/// fire-and-store path (journaled, then stashed in-flight), so it completes
/// via the poll cycle rather than inline. Driving one poll_in_flight confirms
/// it and emits `Completed`.
#[tokio::test]
async fn mint_not_initialized_jit_retry_completes() {
    let (mut state, mut storage_rx, storage_tx, mock, mint) =
        build_state_for_jit_caller_arm(true).await;
    // Seed a Processing row the re-fire's claim owns via the same t0 the ctx carries.
    let t0 = chrono::Utc::now();
    if let Storage::Mock(mock_storage) = &*state.storage {
        seed_processing_deposit(mock_storage, 5001, t0, 0);
    }

    // JIT pre-check sees admin-owned init, so Retry without sending init.
    let admin_bytes = pack_mint_with_authority(COption::Some(SignerUtil::admin_signer().pubkey()));
    mock.enqueue("getAccountInfo", account_info_reply_bytes(&admin_bytes));

    // fire-and-store re-issue: build/sign then broadcast (journaled write-ahead).
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    // The subsequent poll cycle confirms the stashed retry.
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());

    drive_caller_arm_with_jit_setup(&mut state, 5001, mint, Some(t0), &storage_tx).await;

    if let Storage::Mock(mock_storage) = &*state.storage {
        assert_eq!(
            mock_storage
                .get_release_signatures(5001)
                .await
                .unwrap()
                .len(),
            1,
            "the owned JIT re-fire journals exactly one retry signature"
        );
    }

    // The retry is stashed in-flight, not confirmed inline; drive one poll cycle.
    test_hooks::poll_in_flight(&mut state, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("Retry path must drive the stashed mint to a status update");
    assert_eq!(update.transaction_id, 5001);
    assert_eq!(
        update.status,
        TransactionStatus::Completed,
        "JitOutcome::Retry must complete once the poll cycle confirms it; got {:?}",
        update.status
    );
    mock.shutdown().await;
}

/// JitOutcome::ManualReview (authority mismatch) — the caller arm must
/// emit a ManualReview status with the runbook-dispatch substring and
/// release the cached MintToBuilder.
#[tokio::test]
async fn mint_not_initialized_jit_manual_review_authority_mismatch() {
    let (mut state, mut storage_rx, storage_tx, mock, mint) =
        build_state_for_jit_caller_arm(true).await;

    // JIT pre-check sees foreign authority → ManualReview before any send.
    let foreign_bytes = pack_mint_with_authority(COption::Some(Pubkey::new_unique()));
    mock.enqueue("getAccountInfo", account_info_reply_bytes(&foreign_bytes));

    let txn_id: i64 = 5002;
    drive_caller_arm_with_jit_setup(&mut state, txn_id, mint, None, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("ManualReview path must emit a status update");
    assert_eq!(update.transaction_id, txn_id);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update
        .error_message
        .clone()
        .expect("ManualReview must carry an error_message");
    assert!(
        msg.contains("Mint instruction failed after JIT: mint_authority mismatch"),
        "ManualReview error_message must round-trip the runbook-dispatch substring; got {msg:?}"
    );
    assert!(update.processed_at.is_some());
    assert!(
        !state.mint_builders.contains_key(&txn_id),
        "ManualReview branch must release the cached MintToBuilder"
    );
    assert_eq!(
        mock.call_count("sendTransaction"),
        0,
        "authority-mismatch must abort before sending InitializeMint"
    );
    mock.shutdown().await;
}

/// JitOutcome::ManualReview (corrupt mint state) — pins the second
/// runbook-dispatch substring.
#[tokio::test]
async fn mint_not_initialized_jit_manual_review_corrupt_state() {
    let (mut state, mut storage_rx, storage_tx, mock, mint) =
        build_state_for_jit_caller_arm(true).await;

    // Corrupt bytes (invalid COption discriminant) → CorruptData → ManualReview.
    let mut data = vec![0u8; Mint::LEN];
    data[0] = 0xFF;
    mock.enqueue("getAccountInfo", account_info_reply_bytes(&data));

    let txn_id: i64 = 5003;
    drive_caller_arm_with_jit_setup(&mut state, txn_id, mint, None, &storage_tx).await;

    let update = storage_rx
        .recv()
        .await
        .expect("CorruptData ManualReview path must emit a status update");
    assert_eq!(update.transaction_id, txn_id);
    assert_eq!(update.status, TransactionStatus::ManualReview);
    let msg = update.error_message.unwrap_or_default();
    assert!(
        msg.contains("Mint instruction failed after JIT: corrupt mint state"),
        "ManualReview must carry the corrupt-state runbook substring; got {msg:?}"
    );
    mock.shutdown().await;
}

/// JitOutcome::Transient: a real RPC-driven transient verdict must re-arm the
/// funded deposit for the fetcher instead of ending it. A terminal status here
/// is read by no worker, so the deposit would sit with its escrow funded and no
/// private channel mint until someone edited the database by hand.
#[tokio::test]
async fn mint_not_initialized_jit_transient_requeues_deposit() {
    // Mint cache empty, so the JIT body returns Transient("mint not in mint cache").
    let (mut state, mut storage_rx, storage_tx, mock, mint) =
        build_state_for_jit_caller_arm(false).await;

    // Pre-check sees uninit (would normally fall through to init) but
    // get_mint_metadata fails because the cache is empty.
    mock.enqueue(
        "getAccountInfo",
        account_info_reply_bytes(&[0u8; Mint::LEN]),
    );

    let txn_id: i64 = 5004;
    if let Storage::Mock(mock_storage) = &*state.storage {
        seed_processing_deposit(mock_storage, txn_id, chrono::Utc::now(), 0);
    }

    drive_caller_arm_with_jit_setup(&mut state, txn_id, mint, None, &storage_tx).await;

    assert!(
        storage_rx.try_recv().is_err(),
        "a transient JIT verdict must not write any status for a funded deposit"
    );
    if let Storage::Mock(mock_storage) = &*state.storage {
        let (status, attempts) = deposit_row_state(mock_storage, txn_id);
        assert_eq!(
            status,
            TransactionStatus::Pending,
            "the deposit must be re-armed for the fetcher"
        );
        assert_eq!(attempts, 1, "the durable requeue counter must be consumed");
        assert!(
            mock_storage
                .get_release_signatures(txn_id)
                .await
                .unwrap()
                .is_empty(),
            "a transient verdict must broadcast nothing and journal nothing"
        );
    }
    mock.shutdown().await;
}

/// At the durable requeue cap the sender stops re-arming, but it still must not
/// terminalize the deposit. The row is left Processing so the recovery sweep,
/// which verifies the mint on-chain first, owns the escalation.
#[tokio::test]
async fn mint_not_initialized_jit_transient_at_cap_leaves_processing() {
    let (mut state, mut storage_rx, storage_tx, mock, mint) =
        build_state_for_jit_caller_arm(false).await;

    mock.enqueue(
        "getAccountInfo",
        account_info_reply_bytes(&[0u8; Mint::LEN]),
    );

    // The cap constant is crate-private to the indexer, so this mirrors its
    // value rather than importing it.
    let txn_id: i64 = 5005;
    if let Storage::Mock(mock_storage) = &*state.storage {
        seed_processing_deposit(mock_storage, txn_id, chrono::Utc::now(), 3);
    }

    drive_caller_arm_with_jit_setup(&mut state, txn_id, mint, None, &storage_tx).await;

    assert!(
        storage_rx.try_recv().is_err(),
        "the sender must never terminalize a deposit, even at the requeue cap"
    );
    if let Storage::Mock(mock_storage) = &*state.storage {
        let (status, attempts) = deposit_row_state(mock_storage, txn_id);
        assert_eq!(
            status,
            TransactionStatus::Processing,
            "at the cap the row waits for the recovery sweep"
        );
        assert_eq!(attempts, 3, "the cap must not be exceeded");
    }
    mock.shutdown().await;
}

/// `MintNotInitialized` without a `transaction_id` short-circuits to
/// `handle_permanent_failure("Mint initialization failed")` — covers the
/// `let Some(txn_id) = ...` else-branch in the caller arm.
#[tokio::test]
async fn mint_not_initialized_no_txn_id_routes_to_failed() {
    let (mut state, mut storage_rx, storage_tx, mock) = build_default_sender_state().await;

    let ctx = private_channel_indexer::operator::sender::types::TransactionContext {
        kind: TransactionKind::Mint,
        transaction_id: None,
        withdrawal_nonce: None,
        trace_id: None,
        deposit_claim_lease: None,
    };

    test_hooks::handle_confirmation_result(
        &mut state,
        Ok(ConfirmationResult::MintNotInitialized),
        Signature::new_unique(),
        None,
        &ctx,
        make_instruction(),
        RetryPolicy::None,
        &ExtraErrorCheckPolicy::None,
        &storage_tx,
    )
    .await;

    // No transaction_id → no status update emitted (send_fatal_error
    // guards on Some(transaction_id)).
    drop(storage_tx);
    assert!(storage_rx.recv().await.is_none());
    mock.shutdown().await;
}
