//! End-to-end coverage for `try_jit_mint_initialization`
//! (`indexer/src/operator/sender/mint.rs`) via the
//! `test_hooks::jit_mint_init` wrapper.
//!
//! Drives the production helper against a scripted `MockRpcServer`, so the
//! full code path — on-chain probe, `decode_and_check_authority` branching,
//! `InitializeMint` send, confirmation poll, post-init authority recheck,
//! and backoff fallback — is exercised end-to-end rather than replayed by
//! hand at the wire layer.
//!
//! Each test pins one branch of the rewritten `JitOutcome` decision flow
//! described in `mint.rs`.

#[path = "sender_fixtures.rs"]
mod sender_fixtures;

use {
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_indexer::{
        config::ProgramType,
        operator::{
            sender::{test_hooks, types::InstructionWithSigners, JitOutcome},
            utils::instruction_util::MintToBuilder,
            SignerUtil,
        },
        storage::{
            common::{models::DbMint, storage::mock::MockStorage},
            Storage,
        },
    },
    sender_fixtures::{
        blockhash_reply, confirmed_status_reply, ensure_admin_signer_env, make_config,
        make_instruction, null_status_reply, send_transaction_echo_reply,
    },
    serde_json::json,
    solana_keychain::SolanaSigner,
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey},
    spl_token::{
        solana_program::{program_option::COption, program_pack::Pack},
        state::Mint,
    },
    std::sync::Arc,
    test_utils::mock_rpc::{MockRpcServer, Reply},
};

/// Pack an SPL `Mint` into its on-chain bytes so the byte layout the
/// production helper decodes in `decode_and_check_authority` matches what
/// a real Solana validator would return for `getAccountInfo`.
///
/// `authority`:
/// - `COption::Some(admin_pubkey)` → simulates an initialized mint where
///   the operator's admin owns the authority (Match case).
/// - `COption::Some(other)` → simulates a rotated-/foreign-authority mint
///   (Mismatch case).
/// - `COption::None` → simulates a mint whose authority was cleared (also
///   Mismatch in the helper).
fn pack_mint_bytes(is_initialized: bool, authority: COption<Pubkey>) -> Vec<u8> {
    let mint = Mint {
        mint_authority: authority,
        supply: 0,
        decimals: 6,
        is_initialized,
        freeze_authority: COption::None,
    };
    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint, &mut data).expect("pack mint");
    data
}

/// Initialized mint owned by the operator's admin. Match case.
fn admin_owned_initialized_mint_bytes() -> Vec<u8> {
    let admin = SignerUtil::admin_signer().pubkey();
    pack_mint_bytes(true, COption::Some(admin))
}

/// Initialized mint owned by some *other* pubkey. Mismatch case (the
/// rotated-admin scenario the runbook's Path D documents).
fn foreign_authority_initialized_mint_bytes() -> Vec<u8> {
    pack_mint_bytes(true, COption::Some(Pubkey::new_unique()))
}

/// Build `Mint::LEN` bytes that fail to decode. Setting the
/// `mint_authority` `COption` discriminant (offset 0..4) to an invalid
/// value makes `Mint::unpack_unchecked` reject the buffer with
/// `InvalidAccountData`.
fn corrupt_mint_bytes() -> Vec<u8> {
    let mut data = vec![0u8; Mint::LEN];
    // COption discriminants only allow {0, 1}; 0xFF is invalid.
    data[0] = 0xFF;
    data
}

/// Build a `getAccountInfo` success-shaped reply carrying the given
/// account bytes in base64 encoding (Solana JSON-RPC wire shape).
fn account_info_reply(data: &[u8]) -> Reply {
    Reply::result(json!({
        "context": { "slot": 100 },
        "value": {
            "data": [STANDARD.encode(data), "base64"],
            "executable": false,
            "lamports": 1_461_600u64,
            "owner": spl_token::id().to_string(),
            "rentEpoch": 0u64,
            "space": data.len(),
        }
    }))
}

/// Build a `MintToBuilder` with the minimum field set
/// `try_jit_mint_initialization` reads (`get_mint`, plus enough to flow
/// through `handle_transaction_builder` if scenarios reach the
/// InitializeMint build path).
fn make_mint_builder(mint: Pubkey) -> MintToBuilder {
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

/// Test fixture: builds a fresh `SenderState` with a `MockRpcServer`.
///
/// `populate_builder` controls whether `state.mint_builders` is pre-seeded
/// for `txn_id` (when false, the helper hits its no-cached-builder
/// PermanentFailure branch on first lookup).
///
/// `populate_mint_cache` controls whether `mock_storage.mints` carries a
/// `DbMint` for the mint pubkey. The mint-cache-miss test relies on
/// passing `false` here so `get_mint_metadata` returns the
/// `PermanentFailure("mint not in mint cache")` branch.
struct Fixture {
    state: private_channel_indexer::operator::sender::types::SenderState,
    mock: MockRpcServer,
    txn_id: i64,
    instruction: InstructionWithSigners,
}

async fn build_fixture(populate_builder: bool) -> Fixture {
    build_fixture_inner(populate_builder, true).await
}

async fn build_fixture_inner(populate_builder: bool, populate_mint_cache: bool) -> Fixture {
    ensure_admin_signer_env();
    let mock = MockRpcServer::start().await;
    let mock_storage = MockStorage::new();

    let mint = Pubkey::new_unique();
    if populate_mint_cache {
        // Pre-populate the mint cache so `get_mint_metadata` resolves from
        // storage rather than falling back to RPC. This keeps the
        // per-scenario RPC scripts focused on the JIT helper's own calls
        // (account probe, blockhash, send, confirm, backoff).
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint::new(mint.to_string(), 6, spl_token::id().to_string()),
        );
    }

    let storage = Arc::new(Storage::Mock(mock_storage));
    let mut state = test_hooks::new_sender_state(
        &make_config(mock.url(), ProgramType::Escrow),
        CommitmentLevel::Confirmed,
        None,
        storage,
        // retry_max_attempts is unused by JIT itself but feeds RPC retry config.
        1,
        // Tight confirmation poll interval keeps the unconfirmed-then-backoff
        // tests' wall-clock low while still exercising
        // MAX_POLL_ATTEMPTS_CONFIRMATION = 5 retries.
        1,
        None,
    )
    .expect("SenderState construction must succeed under Mock storage");

    let txn_id: i64 = 7;
    if populate_builder {
        state.mint_builders.insert(txn_id, make_mint_builder(mint));
    }

    let instruction = make_instruction();
    Fixture {
        state,
        mock,
        txn_id,
        instruction,
    }
}

// ─────────────────────────────────────────────────────────────────────
// Pre-check: mint already initialized with admin authority — Retry.
// ─────────────────────────────────────────────────────────────────────
//
// One getAccountInfo reply with an initialized mint whose authority is
// the operator's admin. The helper must short-circuit and return
// `JitOutcome::Retry(_)` without sending any transaction.
#[tokio::test]
async fn jit_returns_retry_when_mint_already_initialized_with_admin_authority() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&admin_owned_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    assert!(
        matches!(outcome, JitOutcome::Retry(_)),
        "fast-path on initialized mint with admin authority must return Retry"
    );
    assert_eq!(
        mock.call_count("getAccountInfo"),
        1,
        "exactly one initial probe; no JIT send required"
    );
    assert_eq!(mock.call_count("sendTransaction"), 0);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Pre-check: mint initialized with a *different* authority — ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// The bug-fix path. Pre-fix, the JIT helper saw `is_initialized = true`
// and blindly returned `Some(original_instruction)`, causing the caller
// to retry the doomed `mint_to` and loop forever. The new helper inspects
// `mint_authority` and routes to ManualReview.
#[tokio::test]
async fn jit_returns_manual_review_when_mint_authority_mismatch() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&foreign_authority_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::ManualReview(reason) => {
            assert!(
                reason.contains("Mint instruction failed after JIT: mint_authority mismatch"),
                "ManualReview reason must carry the runbook-dispatch substring; got {reason:?}"
            );
        }
        other => panic!("expected ManualReview, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(mock.call_count("getAccountInfo"), 1);
    assert_eq!(
        mock.call_count("sendTransaction"),
        0,
        "authority mismatch must abort before sending InitializeMint"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Pre-check: mint initialized with `mint_authority = COption::None`
// (authority cleared via SetAuthority) — also ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// Pins the `AuthorityCheck::Mismatch(Pubkey::default())` branch in
// `decode_and_check_authority` (a mint with no authority is treated as
// a mismatch — the operator can't `mint_to` a no-authority mint).
#[tokio::test]
async fn jit_returns_manual_review_when_mint_authority_cleared() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    let no_authority_bytes = pack_mint_bytes(true, COption::None);
    mock.enqueue("getAccountInfo", account_info_reply(&no_authority_bytes));

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::ManualReview(reason) => {
            assert!(
                reason.contains("Mint instruction failed after JIT: mint_authority mismatch"),
                "no-authority mint must surface the mismatch substring; got {reason:?}"
            );
        }
        other => panic!("expected ManualReview, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(mock.call_count("sendTransaction"), 0);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Pre-check: mint bytes do not decode as SPL Mint — ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// Defensive — should not occur in normal operation. Pins the
// `AuthorityCheck::CorruptData` branch.
#[tokio::test]
async fn jit_returns_manual_review_when_mint_data_corrupt() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue("getAccountInfo", account_info_reply(&corrupt_mint_bytes()));

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::ManualReview(reason) => {
            assert!(
                reason.contains("Mint instruction failed after JIT: corrupt mint state"),
                "ManualReview reason must carry the corrupt-state runbook substring; got {reason:?}"
            );
        }
        other => panic!("expected ManualReview, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(mock.call_count("sendTransaction"), 0);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Full happy path — uninit probe → InitializeMint sent → confirmed
// → post-init authority recheck = match → Retry.
// ─────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn jit_completes_full_initialize_then_returns_retry() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    // Pre-check: account exists with right size but is_initialized=false.
    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());
    // Post-confirm authority recheck: now initialized with admin authority.
    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&admin_owned_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    assert!(
        matches!(outcome, JitOutcome::Retry(_)),
        "full happy path must return Retry"
    );
    assert_eq!(
        mock.call_count("getAccountInfo"),
        2,
        "1 initial probe + 1 post-confirm authority recheck"
    );
    assert_eq!(mock.call_count("getLatestBlockhash"), 1);
    assert_eq!(mock.call_count("sendTransaction"), 1);
    assert_eq!(mock.call_count("getSignatureStatuses"), 1);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Post-init authority mismatch — race during InitializeMint → ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// Pre-check sees uninit, send + confirm succeed, but the post-init
// authority recheck reads back a *different* authority — somebody
// initialized the same mint with another key during our send window.
// Routes to ManualReview with the post-init reason string.
#[tokio::test]
async fn jit_returns_manual_review_when_post_init_authority_mismatch() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());
    // Post-confirm sees the racing concurrent-rotation outcome.
    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&foreign_authority_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::ManualReview(reason) => {
            assert!(
                reason.contains("Mint instruction failed after JIT: mint_authority mismatch"),
                "post-init mismatch must carry the mint_authority-mismatch substring; got \
                 {reason:?}"
            );
            assert!(
                reason.contains("race with concurrent admin rotation"),
                "post-init mismatch reason must distinguish itself from the pre-check one; got \
                 {reason:?}"
            );
        }
        other => panic!("expected ManualReview, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(mock.call_count("sendTransaction"), 1);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Mint cache miss — PermanentFailure.
// ─────────────────────────────────────────────────────────────────────
//
// Pre-check sees uninit (would normally fall through to init), but
// `get_mint_metadata` fails because the mint cache is empty. Routes to
// PermanentFailure with the cache-miss reason string.
#[tokio::test]
async fn jit_returns_permanent_failure_when_mint_cache_miss() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture_inner(true, false).await;

    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::PermanentFailure(reason) => {
            assert!(
                reason.contains("mint not in mint cache"),
                "cache-miss must surface the cache-miss reason; got {reason:?}"
            );
        }
        other => panic!("expected PermanentFailure, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(
        mock.call_count("sendTransaction"),
        0,
        "cache miss must abort before sending InitializeMint"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Initial probe RPC error — fail-safe falls through to JIT init.
// ─────────────────────────────────────────────────────────────────────
//
// `-32601` (method-not-found) is a permanent RPC error that
// `RpcClientWithRetry` does NOT retry, so the helper falls into the
// fail-safe branch and proceeds with InitializeMint. After confirm, the
// post-init authority recheck sees admin-owned initialized data → Retry.
#[tokio::test]
async fn jit_falls_through_when_initial_probe_returns_rpc_error() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue(
        "getAccountInfo",
        Reply::error(-32601, "method not found (simulated)"),
    );
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    mock.enqueue("getSignatureStatuses", confirmed_status_reply());
    // Post-confirm authority recheck sees admin-owned init.
    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&admin_owned_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    assert!(
        matches!(outcome, JitOutcome::Retry(_)),
        "RPC error on probe must not abort JIT — fail-safe branch must continue to send"
    );
    // Probe = 1 (no retry on -32601), then full JIT sequence proceeds.
    // 1 failed probe + 1 post-confirm recheck = 2.
    assert_eq!(mock.call_count("getAccountInfo"), 2);
    assert_eq!(mock.call_count("sendTransaction"), 1);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// sendTransaction fails — PermanentFailure without polling.
// ─────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn jit_returns_permanent_failure_when_send_transaction_fails() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue(
        "sendTransaction",
        Reply::error(-32601, "method not found (simulated send failure)"),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::PermanentFailure(reason) => {
            assert!(
                reason.contains("Failed to send InitializeMint transaction"),
                "send failure must surface the send-failure reason; got {reason:?}"
            );
        }
        other => panic!("expected PermanentFailure, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(mock.call_count("sendTransaction"), 1);
    assert_eq!(
        mock.call_count("getSignatureStatuses"),
        0,
        "no confirmation poll should run when send itself failed"
    );
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Send succeeds but stays unconfirmed — backoff probe finds it
// initialized with admin authority → Retry.
// ─────────────────────────────────────────────────────────────────────
//
// After 5 null status responses `check_transaction_status` returns
// `ConfirmationResult::Retry`, which lands in the
// `mint_authority_check_with_backoff` fallback. The first backoff
// `getAccountInfo` returns admin-owned init bytes, so the helper treats
// JIT as a successful race-recovery and returns Retry.
#[tokio::test]
async fn jit_returns_retry_when_backoff_recovers_with_admin_authority() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    for _ in 0..5 {
        mock.enqueue("getSignatureStatuses", null_status_reply());
    }
    // First backoff probe sees the racing InitializeMint settled with
    // admin authority.
    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&admin_owned_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    assert!(
        matches!(outcome, JitOutcome::Retry(_)),
        "race-recovery branch must return Retry when backoff sees admin-authority init"
    );
    assert_eq!(
        mock.call_count("getAccountInfo"),
        2,
        "1 initial probe + 1 backoff probe (succeeded on first attempt)"
    );
    assert_eq!(mock.call_count("sendTransaction"), 1);
    assert_eq!(mock.call_count("getSignatureStatuses"), 5);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Backoff observes mint initialized with foreign authority → ManualReview.
// ─────────────────────────────────────────────────────────────────────
//
// Same scripted prefix as the race-recovery test, but the backoff probe
// reads back a foreign authority. Routes to ManualReview with the
// post-init mismatch reason (`race with concurrent admin rotation`),
// matching the documented contract.
#[tokio::test]
async fn jit_returns_manual_review_when_backoff_recovers_with_authority_mismatch() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    for _ in 0..5 {
        mock.enqueue("getSignatureStatuses", null_status_reply());
    }
    // Backoff sees concurrent rotation with a foreign authority.
    mock.enqueue(
        "getAccountInfo",
        account_info_reply(&foreign_authority_initialized_mint_bytes()),
    );

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::ManualReview(reason) => {
            assert!(
                reason.contains("race with concurrent admin rotation"),
                "backoff mismatch must reuse the post-init mismatch reason; got {reason:?}"
            );
        }
        other => panic!("expected ManualReview, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(mock.call_count("sendTransaction"), 1);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// Backoff exhausts with uninit reads — PermanentFailure.
// ─────────────────────────────────────────────────────────────────────
//
// Wall-clock note: this is the only test that pays the full
// 4 × BACKOFF_MS = ~750 ms for the backoff loop; do not duplicate this
// shape elsewhere.
#[tokio::test]
async fn jit_returns_permanent_failure_when_backoff_exhausts_with_uninit() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(true).await;

    mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    mock.enqueue("getLatestBlockhash", blockhash_reply());
    mock.enqueue("sendTransaction", send_transaction_echo_reply());
    for _ in 0..5 {
        mock.enqueue("getSignatureStatuses", null_status_reply());
    }
    for _ in 0..4 {
        mock.enqueue("getAccountInfo", account_info_reply(&[0u8; Mint::LEN]));
    }

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::PermanentFailure(reason) => {
            assert!(
                reason.contains("InitializeMint transaction could not be confirmed"),
                "exhausted-backoff must surface the could-not-be-confirmed reason; got {reason:?}"
            );
        }
        other => panic!("expected PermanentFailure, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(
        mock.call_count("getAccountInfo"),
        5,
        "1 initial probe + 4 backoff attempts"
    );
    assert_eq!(mock.call_count("getSignatureStatuses"), 5);
    mock.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────
// No cached builder — PermanentFailure, zero RPC calls.
// ─────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn jit_returns_permanent_failure_when_no_cached_builder() {
    let Fixture {
        mut state,
        mock,
        txn_id,
        instruction,
    } = build_fixture(false).await; // do NOT pre-populate.

    let outcome = test_hooks::jit_mint_init(&mut state, txn_id, instruction).await;

    match outcome {
        JitOutcome::PermanentFailure(reason) => {
            assert!(
                reason.contains("no cached MintToBuilder"),
                "missing-builder branch must surface its specific reason; got {reason:?}"
            );
        }
        other => panic!("expected PermanentFailure, got {:?}", debug_outcome(&other)),
    }
    assert_eq!(
        mock.call_count("getAccountInfo"),
        0,
        "early return must skip every RPC call"
    );
    mock.shutdown().await;
}

/// `JitOutcome` doesn't derive `Debug` (the `Retry` variant carries
/// non-Debug `InstructionWithSigners`), so panic messages need a
/// hand-rolled summary.
fn debug_outcome(outcome: &JitOutcome) -> String {
    match outcome {
        JitOutcome::Retry(_) => "Retry(_)".to_string(),
        JitOutcome::ManualReview(reason) => format!("ManualReview({reason:?})"),
        JitOutcome::PermanentFailure(reason) => format!("PermanentFailure({reason:?})"),
    }
}
