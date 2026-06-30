//! Shared fixture helpers for sender / JIT / remint integration tests.
//!
//! Each of the test binaries under `tests/indexer/` that drives the
//! `test_hooks::*` API mounts this file via
//! `#[path = "sender_fixtures.rs"] mod sender_fixtures;` and pulls in
//! the helpers it needs — keeps the per-file boilerplate to a single
//! `use` block.
//!
//! The helpers here cover only the wire-mock + `SenderState`-config
//! shape; per-file `build_fixture` builders stay local because they
//! diverge in the parts that matter (program type, retry budget,
//! mint-cache pre-seeding, MockStorage handle exposure).
//!
//! Because each consuming test binary uses only a subset of the
//! helpers, the file as a whole carries `#![allow(dead_code)]` —
//! every binary that mounts it gets a private copy and clippy would
//! otherwise complain about whichever helpers that binary doesn't
//! happen to call.

#![allow(dead_code)]

use {
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_indexer::{
        config::{PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType},
        operator::{
            sender::{
                test_hooks,
                types::{
                    InstructionWithSigners, SenderState, TransactionContext,
                    TransactionStatusUpdate,
                },
            },
            utils::instruction_util::{SourceEventId, WithdrawalRemintInfo},
            SignerUtil,
        },
        storage::{common::storage::mock::MockStorage, Storage},
    },
    serde_json::{json, Value},
    solana_keychain::SolanaSigner,
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Keypair},
    spl_associated_token_account::get_associated_token_address_with_program_id,
    std::sync::{Arc, Once},
    test_utils::mock_rpc::{MockRpcServer, Reply},
    tokio::sync::mpsc,
};

/// Set `ADMIN_SIGNER` / `OPERATOR_SIGNER` env vars exactly once per
/// test process. `SignerUtil::admin_signer()` is a `Lazy<Signer>` that
/// reads the env on first access — every test binary that touches
/// `make_instruction` must call this first.
pub fn ensure_admin_signer_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let kp = Keypair::new();
        let key = bs58::encode(kp.to_bytes()).into_string();
        std::env::set_var("ADMIN_SIGNER", "memory");
        std::env::set_var("ADMIN_PRIVATE_KEY", &key);
        std::env::set_var("OPERATOR_SIGNER", "memory");
        std::env::set_var("OPERATOR_PRIVATE_KEY", &key);
    });
}

/// Minimal `PrivateChannelIndexerConfig` pointing at the supplied mock RPC URL.
/// `program_type` differs across tests (Escrow vs Withdraw) so it's a
/// parameter; the rest of the fields are placeholders that the in-memory
/// `Storage::Mock` path never inspects.
pub fn make_config(rpc_url: String, program_type: ProgramType) -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type,
        storage_type: StorageType::Postgres,
        rpc_url,
        source_rpc_url: None,
        postgres: PostgresConfig {
            database_url: "postgres://placeholder/none".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: None,
    }
}

/// Empty-instruction `InstructionWithSigners` carrying just the admin
/// signer. Sufficient for any test where `send_and_confirm` /
/// `try_jit_mint_initialization` only inspects the signer set and feeds
/// the bytes through `sign_and_send_transaction`.
pub fn make_instruction() -> InstructionWithSigners {
    let admin = SignerUtil::admin_signer();
    InstructionWithSigners {
        instructions: vec![],
        fee_payer: admin.pubkey(),
        signers: vec![admin],
        compute_unit_price: None,
        compute_budget: None,
    }
}

/// Canonical `getLatestBlockhash` reply — a valid blockhash and a
/// nontrivial `lastValidBlockHeight`.
pub fn blockhash_reply() -> Reply {
    Reply::result(json!({
        "context": { "slot": 1 },
        "value": {
            "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
            "lastValidBlockHeight": 100
        }
    }))
}

/// `sendTransaction` reply that echoes back the signature embedded in
/// the request — `solana-client::send_transaction` self-checks the
/// returned signature against `tx.signatures[0]`, and a hard-coded sig
/// would trip its mismatch-retry loop.
pub fn send_transaction_echo_reply() -> Reply {
    Reply::dynamic(|req| {
        let params = req
            .get("params")
            .and_then(Value::as_array)
            .expect("sendTransaction request must include params");
        let encoded = params
            .first()
            .and_then(Value::as_str)
            .expect("first param must be the encoded transaction");
        let bytes = STANDARD
            .decode(encoded)
            .expect("encoded tx must be valid base64");
        // Solana wire format: shortvec(1) prefix + 64-byte signature.
        let sig = bs58::encode(&bytes[1..65]).into_string();
        json!(sig)
    })
}

/// `getSignatureStatuses` reply for a single finalized success entry.
pub fn confirmed_status_reply() -> Reply {
    Reply::result(json!({
        "context": { "slot": 42 },
        "value": [{
            "slot": 42,
            "confirmations": null,
            "err": null,
            "status": { "Ok": null },
            "confirmationStatus": "finalized"
        }]
    }))
}

/// `getSignatureStatuses` reply for a single still-pending entry —
/// `value: [null]`. Production code reads this as "transaction not yet
/// observed by the network" and either re-pushes or times out.
pub fn null_status_reply() -> Reply {
    Reply::result(json!({
        "context": { "slot": 42 },
        "value": [null]
    }))
}

/// Deposit-side `TransactionContext`: `transaction_id` set so fatal
/// arms emit a status update; no `withdrawal_nonce` so the
/// remint-deferral branch is not taken.
pub fn deposit_ctx(transaction_id: i64) -> TransactionContext {
    TransactionContext {
        transaction_id: Some(transaction_id),
        withdrawal_nonce: None,
        trace_id: Some(format!("trace-{transaction_id}")),
    }
}

/// Withdrawal-side `TransactionContext`: both fields set, drives the
/// remint-deferral branches in `handle_permanent_failure` and the
/// retry-counter logic in `send_and_confirm`.
pub fn withdrawal_ctx(transaction_id: i64, nonce: u64) -> TransactionContext {
    TransactionContext {
        transaction_id: Some(transaction_id),
        withdrawal_nonce: Some(nonce),
        trace_id: Some(format!("trace-{transaction_id}")),
    }
}

/// Plausible `WithdrawalRemintInfo` for tests that exercise the
/// remint-deferral path. The mint/user/ATA pubkeys are unique per
/// call; only `transaction_id` is caller-controlled, since several
/// tests cross-reference it against status-update payloads.
pub fn make_remint_info(transaction_id: i64) -> WithdrawalRemintInfo {
    let mint = Pubkey::new_unique();
    let user = Pubkey::new_unique();
    let token_program = spl_token::id();
    let user_ata = get_associated_token_address_with_program_id(&user, &mint, &token_program);
    WithdrawalRemintInfo {
        transaction_id,
        // Deterministic per transaction_id; these tests exercise deferral, not memo matching.
        source_event_id: SourceEventId::new(&format!("withdraw-sig-{transaction_id}"), 0, None),
        trace_id: format!("trace-{transaction_id}"),
        mint,
        user,
        user_ata,
        token_program,
        amount: 50_000,
    }
}

/// Build a `MockRpcServer` + `SenderState` wired against `MockStorage`,
/// plus the `(storage_tx, storage_rx)` pair tests use to observe
/// `TransactionStatusUpdate`s emitted by the production helpers.
///
/// `retry_max_attempts = 1` and `confirmation_poll_interval_ms = 1`
/// keep the wall-clock low for the per-scenario tick. Tests that need
/// a different shape — e.g. `MockStorage` handle for fault injection,
/// non-default retry budget, withdrawal-side program type — keep their
/// own `build_fixture` local. This default helper covers the
/// deposit-side / Escrow shape that several files share.
pub async fn build_default_sender_state() -> (
    SenderState,
    mpsc::Receiver<TransactionStatusUpdate>,
    mpsc::Sender<TransactionStatusUpdate>,
    MockRpcServer,
) {
    ensure_admin_signer_env();
    let mock = MockRpcServer::start().await;
    let storage = Arc::new(Storage::Mock(MockStorage::new()));
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
    let (storage_tx, storage_rx) = mpsc::channel(8);
    (state, storage_rx, storage_tx, mock)
}
