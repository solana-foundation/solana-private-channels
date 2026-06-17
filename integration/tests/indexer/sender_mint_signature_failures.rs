//! Mint-idempotency lookup error-path coverage.
//!
//! `find_existing_mint_signature_with_memo` (`sender/mint.rs:214`)
//! contains five distinct failure / skip arms that the happy-path tests
//! in `sender_mint_idempotency.rs` and `sender_mint_validator_encodings.rs`
//! don't reach. Each scenario below pins one of those arms:
//!
//!   - `is_method_not_found_error(-32601)` true → early `Ok(None)`
//!     (graceful degradation when the RPC backend doesn't implement
//!     `getSignaturesForAddress`).
//!   - Generic RPC error → bubble up as `Err(String)`.
//!   - `signature_status.err.is_some()` → `continue` past the entry
//!     before consulting `signature_status.memo`.
//!   - Malformed `signature` string in the JSON-RPC reply →
//!     `Signature::from_str` warn-and-continue branch.
//!   - `getTransaction` errors *after* a memo match → bubble up as
//!     `Err(String)`.
//!   - Builder missing required fields → `expected_mint_instruction`
//!     short-circuits `Ok(None)` without any RPC traffic.

use {
    private_channel_indexer::operator::{
        sender::find_existing_mint_signature_with_memo,
        utils::{
            instruction_util::{mint_idempotency_memo, MintToBuilder, MintToBuilderWithTxnId},
            rpc_util::{RetryConfig, RpcClientWithRetry},
        },
    },
    serde_json::json,
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    spl_associated_token_account::get_associated_token_address_with_program_id,
    std::{str::FromStr, time::Duration},
    test_utils::mock_rpc::{MockRpcServer, Reply},
};

const PRIOR_SIG_STR: &str =
    "4BxWw1FjwQCHXWkrK4ZehPWauFTPhBafSr9m8Cuht73LG73nUs3wfuJ6gigkhNppP4pYogP5pQDENbE5nQx1Qp4B";

fn test_client(url: String) -> RpcClientWithRetry {
    RpcClientWithRetry::with_retry_config(
        url,
        RetryConfig {
            max_attempts: 1, // surface RPC errors immediately, no retry storms
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(20),
        },
        CommitmentConfig::confirmed(),
    )
}

fn complete_builder(
    txn_id: i64,
    mint: Pubkey,
    recipient_ata: Pubkey,
    mint_authority: Pubkey,
    token_program: Pubkey,
    amount: u64,
) -> MintToBuilderWithTxnId {
    let mut builder = MintToBuilder::new();
    builder
        .mint(mint)
        .recipient(Pubkey::new_unique())
        .recipient_ata(recipient_ata)
        .payer(mint_authority)
        .mint_authority(mint_authority)
        .token_program(token_program)
        .amount(amount)
        .idempotency_memo(mint_idempotency_memo(txn_id));
    MintToBuilderWithTxnId {
        builder,
        txn_id,
        trace_id: format!("trace-{txn_id}"),
    }
}

/// Backends that don't implement `getSignaturesForAddress` reply with
/// `-32601 Method not found`. The idempotency check fails closed and
/// returns `Err` so callers refuse to blind-mint (sender → fatal,
/// recovery → quarantine) rather than risk a double-mint.
#[tokio::test]
async fn method_not_found_surfaces_as_error() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 8_001;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let memo = mint_idempotency_memo(txn_id);

    mock.enqueue(
        "getSignaturesForAddress",
        Reply::error(-32601, "method not found"),
    );

    let bwt = complete_builder(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        1_000,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo).await;

    assert!(
        result.is_err(),
        "fail-closed contract: -32601 yields Err so callers don't blind-mint"
    );
    mock.shutdown().await;
}

/// A non-method-not-found RPC error (e.g. a transient `-32000` from a
/// flaky backend) bubbles up as `Err(String)` so the caller can log
/// the lookup failure and retry on the next sender pass.
#[tokio::test]
async fn generic_rpc_error_bubbles_up_as_err() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 8_002;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let memo = mint_idempotency_memo(txn_id);

    mock.enqueue(
        "getSignaturesForAddress",
        Reply::error(-32000, "transient — backend overloaded"),
    );

    let bwt = complete_builder(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        1_000,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo).await;

    assert!(
        result.is_err(),
        "non-method-not-found RPC errors must surface as Err(_) so callers retry"
    );
    let msg = result.unwrap_err();
    assert!(
        msg.contains("idempotency lookup") && msg.contains(&recipient_ata.to_string()),
        "error string should identify the lookup target — got: {msg}"
    );
    mock.shutdown().await;
}

/// A signature entry whose own `err` field is `Some(_)` is a *failed*
/// on-chain transaction. The validator must `continue` past it before
/// even examining the memo — otherwise a failed-then-resent mint with
/// a stale memo could fool the idempotency check.
#[tokio::test]
async fn entry_with_failed_status_is_skipped_and_returns_none() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 8_003;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let memo = mint_idempotency_memo(txn_id);

    // err: Some(...) on the only signature → continue → empty → Ok(None).
    // `AccountNotFound` is a TransactionError unit variant — bare-string form
    // deserializes cleanly. Tuple variants (e.g. InstructionError) would
    // need `{ "InstructionError": [0, ...] }` and trigger an unrelated
    // deserialize error before the validator sees the entry.
    mock.enqueue(
        "getSignaturesForAddress",
        Reply::result(json!([{
            "signature": PRIOR_SIG_STR,
            "slot": 100u64,
            "err": "AccountNotFound",
            "memo": memo,
            "blockTime": 1_700_000_000i64,
            "confirmationStatus": "finalized",
        }])),
    );

    let bwt = complete_builder(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        1_000,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("failed-status entry skip must not be a hard error");

    assert!(
        result.is_none(),
        "failed on-chain entries are not idempotent matches"
    );
    assert_eq!(
        mock.call_count("getTransaction"),
        0,
        "skipping entry must avoid a redundant getTransaction"
    );
    mock.shutdown().await;
}

/// Malformed `signature` string in the JSON-RPC reply → the helper
/// logs a warning and `continue`s; with no other entries the result
/// is `Ok(None)`.
#[tokio::test]
async fn invalid_signature_string_is_warned_and_skipped() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 8_004;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let memo = mint_idempotency_memo(txn_id);

    // First entry: invalid base58 signature, but matching memo. The
    // validator must walk the memo filter (it matches), then trip on
    // `Signature::from_str` and `continue`.
    mock.enqueue(
        "getSignaturesForAddress",
        Reply::result(json!([{
            "signature": "this is definitely not a base58 signature",
            "slot": 100u64,
            "err": null,
            "memo": memo,
            "blockTime": 1_700_000_000i64,
            "confirmationStatus": "finalized",
        }])),
    );

    let bwt = complete_builder(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        1_000,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("invalid-signature skip must not be a hard error");

    assert!(
        result.is_none(),
        "no remaining candidates after skip → Ok(None)"
    );
    mock.shutdown().await;
}

/// `getSignaturesForAddress` returns one matching candidate; the
/// follow-up `getTransaction` errors. The error must surface as
/// `Err(String)` so the caller doesn't silently treat a transient
/// backend failure as "no prior mint exists".
#[tokio::test]
async fn get_transaction_failure_after_memo_match_bubbles_up_as_err() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 8_005;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let memo = mint_idempotency_memo(txn_id);

    mock.enqueue(
        "getSignaturesForAddress",
        Reply::result(json!([{
            "signature": PRIOR_SIG_STR,
            "slot": 100u64,
            "err": null,
            "memo": memo,
            "blockTime": 1_700_000_000i64,
            "confirmationStatus": "finalized",
        }])),
    );
    mock.enqueue(
        "getTransaction",
        Reply::error(-32000, "transient — getTransaction unavailable"),
    );

    let bwt = complete_builder(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        1_000,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo).await;

    assert!(
        result.is_err(),
        "post-match getTransaction errors must surface so the caller can retry"
    );
    let msg = result.unwrap_err();
    assert!(
        msg.contains("idempotency confirmation"),
        "error message should mention the confirmation step — got: {msg}"
    );
    mock.shutdown().await;
}

/// Builder is missing `recipient_ata` (and others). `expected_mint_instruction`
/// short-circuits at the top and the helper returns `Ok(None)` without
/// firing any RPC traffic — important so callers can pass through
/// builders that aren't yet fully wired without paying RPC cost.
#[tokio::test]
async fn incomplete_builder_returns_none_without_rpc_traffic() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 8_006;
    let memo = mint_idempotency_memo(txn_id);

    let mut builder = MintToBuilder::new();
    builder.mint(Pubkey::new_unique());
    // Deliberately missing recipient_ata, mint_authority, token_program, amount.
    let bwt = MintToBuilderWithTxnId {
        builder,
        txn_id,
        trace_id: format!("trace-{txn_id}"),
    };

    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("incomplete builder must not be a hard error");

    assert!(
        result.is_none(),
        "incomplete builder short-circuits to Ok(None)"
    );
    assert_eq!(
        mock.call_count("getSignaturesForAddress"),
        0,
        "no expected-mint computed → no RPC issued"
    );
    mock.shutdown().await;
}

/// Sanity: a `Signature` constant we use elsewhere actually parses.
/// Doubles as a smoke-check that this test bin compiles end-to-end.
#[test]
fn prior_signature_constant_parses() {
    assert!(
        Signature::from_str(PRIOR_SIG_STR).is_ok(),
        "PRIOR_SIG_STR must remain a valid base58 signature literal"
    );
}
