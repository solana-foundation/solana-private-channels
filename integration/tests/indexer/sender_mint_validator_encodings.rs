//! Mint-idempotency validator subtree — encoding + memo coverage.
//!
//! `sender_mint_idempotency.rs` already drives the parsed `MintTo` happy
//! path. This file fills in the still-uncovered validator branches that
//! `find_existing_mint_signature_with_memo` walks for the *other*
//! `getTransaction` shapes:
//!
//!   - `UiMessage::Raw` (compiled instructions, base58-encoded data)
//!     → `raw_message_has_signer`, `raw_instruction_has_memo`,
//!     `raw_instruction_has_expected_mint`, `accounts_and_amount_match`,
//!     `is_memo_program_id`, `parse_token_instruction_mint_amount`
//!     (both `spl-token` and `spl-token-2022` arms).
//!   - `UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(_))`
//!     inside a parsed message →
//!     `partially_decoded_instruction_has_expected_mint` plus the
//!     `instruction_has_memo` `PartiallyDecoded` arm.
//!   - Parsed `mintToChecked` →
//!     `parsed_instruction_has_expected_mint` `tokenAmount` branch.
//!   - Bracketed-length-prefix memo (`[27] expected`) and a no-prefix
//!     memo segment → `memo_matches` loop + `strip_memo_length_prefix`
//!     happy path *and* fall-through return.
//!   - `meta.err` populated → `transaction_succeeded` false short-circuit.
//!
//! All tests script the wire-level `getSignaturesForAddress` and
//! `getTransaction` replies through `MockRpcServer`, then call the
//! re-exported `find_existing_mint_signature_with_memo` helper directly.

use {
    private_channel_indexer::operator::{
        sender::find_existing_mint_signature_with_memo,
        utils::{
            instruction_util::{
                mint_idempotency_memo, MintToBuilder, MintToBuilderWithTxnId, SourceEventId,
            },
            rpc_util::{RetryConfig, RpcClientWithRetry},
        },
    },
    serde_json::{json, Value},
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    spl_associated_token_account::get_associated_token_address_with_program_id,
    std::{str::FromStr, time::Duration},
    test_utils::mock_rpc::{MockRpcServer, Reply},
};

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

// Deterministic source-event id per txn_id so built, scripted, and expected memos agree.
fn event_id(txn_id: i64) -> SourceEventId {
    SourceEventId::new(&format!("mint-sig-{txn_id}"), 0, None)
}
/// Real-looking 64-byte signature usable across scenarios — the validators
/// only care that `Signature::from_str` parses, not which signer produced it.
const PRIOR_SIG_STR: &str =
    "4BxWw1FjwQCHXWkrK4ZehPWauFTPhBafSr9m8Cuht73LG73nUs3wfuJ6gigkhNppP4pYogP5pQDENbE5nQx1Qp4B";

fn prior_sig() -> Signature {
    Signature::from_str(PRIOR_SIG_STR).expect("static signature literal must parse")
}

fn test_client(url: String) -> RpcClientWithRetry {
    RpcClientWithRetry::with_retry_config(
        url,
        RetryConfig {
            max_attempts: 2,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
        },
        CommitmentConfig::confirmed(),
    )
}

fn builder_with_txn(
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
        .idempotency_memo(mint_idempotency_memo(&event_id(txn_id)));
    MintToBuilderWithTxnId {
        builder,
        txn_id,
        trace_id: format!("trace-{txn_id}"),
    }
}

/// Single-signature `getSignaturesForAddress` reply. `memo_field` lets a
/// caller inject prefixed (`[27] foo`), unprefixed, or multi-segment
/// memo strings without rebuilding the JSON.
fn signature_reply(memo_field: &str) -> Reply {
    Reply::result(json!([{
        "signature": PRIOR_SIG_STR,
        "slot": 100u64,
        "err": null,
        "memo": memo_field,
        "blockTime": 1_700_000_000i64,
        "confirmationStatus": "finalized",
    }]))
}

/// Build a `getTransaction` reply whose message is a `UiMessage::Raw`
/// carrying a single compiled `mint_to` instruction at index 1 and a
/// memo at index 0. `instruction_data_b58` lets a caller switch between
/// `spl-token` and `spl-token-2022` packings (and corrupt them, for the
/// negative scenarios in the sibling failure suite).
#[allow(clippy::too_many_arguments)]
fn raw_get_transaction_reply(
    signature: &Signature,
    mint_authority: &Pubkey,
    recipient_ata: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    memo: &str,
    instruction_data_b58: String,
    meta_err: Value,
) -> Reply {
    let memo_data_b58 = bs58::encode(memo.as_bytes()).into_string();
    Reply::result(json!({
        "slot": 100u64,
        "blockTime": 1_700_000_000i64,
        "meta": {
            "err": meta_err,
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
            "signatures": [signature.to_string()],
            "message": {
                "header": {
                    "numRequiredSignatures": 1,
                    "numReadonlySignedAccounts": 0,
                    "numReadonlyUnsignedAccounts": 4,
                },
                "accountKeys": [
                    mint_authority.to_string(),
                    recipient_ata.to_string(),
                    mint.to_string(),
                    token_program.to_string(),
                    MEMO_PROGRAM_ID,
                ],
                "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                "instructions": [
                    { "programIdIndex": 4, "accounts": [], "data": memo_data_b58 },
                    {
                        "programIdIndex": 3,
                        "accounts": [2u8, 1u8, 0u8],
                        "data": instruction_data_b58,
                    },
                ],
            },
        },
    }))
}

/// Build a `getTransaction` reply whose message is a `UiMessage::Parsed`
/// containing two `UiParsedInstruction::PartiallyDecoded` instructions
/// (memo + token MintTo). Forces serde down the partially-decoded
/// variant by omitting the `program` and `parsed` fields that the
/// `ParsedInstruction` variant requires.
fn partially_decoded_get_transaction_reply(
    signature: &Signature,
    mint_authority: &Pubkey,
    recipient_ata: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    memo: &str,
    instruction_data_b58: String,
) -> Reply {
    let memo_data_b58 = bs58::encode(memo.as_bytes()).into_string();
    Reply::result(json!({
        "slot": 100u64,
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
            "signatures": [signature.to_string()],
            "message": {
                "accountKeys": [
                    {
                        "pubkey": mint_authority.to_string(),
                        "signer": true,
                        "writable": true,
                        "source": "transaction",
                    },
                    {
                        "pubkey": recipient_ata.to_string(),
                        "signer": false,
                        "writable": true,
                        "source": "transaction",
                    },
                    {
                        "pubkey": mint.to_string(),
                        "signer": false,
                        "writable": true,
                        "source": "transaction",
                    },
                    {
                        "pubkey": token_program.to_string(),
                        "signer": false,
                        "writable": false,
                        "source": "transaction",
                    },
                    {
                        "pubkey": MEMO_PROGRAM_ID,
                        "signer": false,
                        "writable": false,
                        "source": "transaction",
                    },
                ],
                "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                "instructions": [
                    {
                        "programId": MEMO_PROGRAM_ID,
                        "accounts": [],
                        "data": memo_data_b58,
                    },
                    {
                        "programId": token_program.to_string(),
                        "accounts": [
                            mint.to_string(),
                            recipient_ata.to_string(),
                            mint_authority.to_string(),
                        ],
                        "data": instruction_data_b58,
                    },
                ],
            },
        },
    }))
}

/// Parsed `mintToChecked` reply — drives the `tokenAmount` branch in
/// `parsed_instruction_has_expected_mint` that the existing `mintTo`
/// idempotency test never exercises.
fn parsed_mint_to_checked_reply(
    signature: &Signature,
    mint_authority: &Pubkey,
    recipient_ata: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    memo: &str,
    amount: u64,
) -> Reply {
    Reply::result(json!({
        "slot": 100u64,
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
            "signatures": [signature.to_string()],
            "message": {
                "accountKeys": [
                    {
                        "pubkey": mint_authority.to_string(),
                        "signer": true,
                        "writable": true,
                        "source": "transaction",
                    },
                    {
                        "pubkey": recipient_ata.to_string(),
                        "signer": false,
                        "writable": true,
                        "source": "transaction",
                    },
                    {
                        "pubkey": mint.to_string(),
                        "signer": false,
                        "writable": true,
                        "source": "transaction",
                    },
                    {
                        "pubkey": token_program.to_string(),
                        "signer": false,
                        "writable": false,
                        "source": "transaction",
                    },
                    {
                        "pubkey": MEMO_PROGRAM_ID,
                        "signer": false,
                        "writable": false,
                        "source": "transaction",
                    },
                ],
                "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                "instructions": [
                    {
                        "program": "spl-memo",
                        "programId": MEMO_PROGRAM_ID,
                        "parsed": memo,
                    },
                    {
                        "program": "spl-token",
                        "programId": token_program.to_string(),
                        "parsed": {
                            "type": "mintToChecked",
                            "info": {
                                "mint": mint.to_string(),
                                "account": recipient_ata.to_string(),
                                "mintAuthority": mint_authority.to_string(),
                                "tokenAmount": { "amount": amount.to_string() },
                            },
                        },
                    },
                ],
            },
        },
    }))
}

/// `UiMessage::Raw` mint with `spl-token` `MintTo` packing → walks the
/// raw-encoding validator subtree all the way through
/// `parse_token_instruction_mint_amount`'s `spl_token::id()` arm.
#[tokio::test]
async fn raw_message_spl_token_mint_to_returns_signature() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_001;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 1_234;
    let memo = mint_idempotency_memo(&event_id(txn_id));
    let signature = prior_sig();

    let mint_to_data = spl_token::instruction::TokenInstruction::MintTo { amount }.pack();
    let data_b58 = bs58::encode(mint_to_data).into_string();

    mock.enqueue("getSignaturesForAddress", signature_reply(&memo));
    mock.enqueue(
        "getTransaction",
        raw_get_transaction_reply(
            &signature,
            &mint_authority,
            &recipient_ata,
            &mint,
            &token_program,
            &memo,
            data_b58,
            Value::Null,
        ),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("raw-encoding lookup must succeed");

    assert_eq!(
        result,
        Some(signature),
        "raw-encoding mint with matching memo + accounts must short-circuit"
    );
    mock.shutdown().await;
}

/// Same shape as the spl-token raw test, but the on-chain mint was
/// minted via `spl-token-2022` — drives the second arm in
/// `parse_token_instruction_mint_amount`.
#[tokio::test]
async fn raw_message_spl_token_2022_mint_to_returns_signature() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_002;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token_2022::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 9_999;
    let memo = mint_idempotency_memo(&event_id(txn_id));
    let signature = prior_sig();

    let mint_to_data = spl_token_2022::instruction::TokenInstruction::MintTo { amount }.pack();
    let data_b58 = bs58::encode(mint_to_data).into_string();

    mock.enqueue("getSignaturesForAddress", signature_reply(&memo));
    mock.enqueue(
        "getTransaction",
        raw_get_transaction_reply(
            &signature,
            &mint_authority,
            &recipient_ata,
            &mint,
            &token_program,
            &memo,
            data_b58,
            Value::Null,
        ),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("token-2022 raw lookup must succeed");

    assert_eq!(result, Some(signature), "token-2022 MintTo must match");
    mock.shutdown().await;
}

/// Parsed message carrying `UiParsedInstruction::PartiallyDecoded`
/// instructions — dispatches through
/// `partially_decoded_instruction_has_expected_mint` and the
/// `PartiallyDecoded` arm of `instruction_has_memo`.
#[tokio::test]
async fn partially_decoded_instructions_return_signature() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_003;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 4_242;
    let memo = mint_idempotency_memo(&event_id(txn_id));
    let signature = prior_sig();

    let mint_to_data = spl_token::instruction::TokenInstruction::MintTo { amount }.pack();
    let data_b58 = bs58::encode(mint_to_data).into_string();

    mock.enqueue("getSignaturesForAddress", signature_reply(&memo));
    mock.enqueue(
        "getTransaction",
        partially_decoded_get_transaction_reply(
            &signature,
            &mint_authority,
            &recipient_ata,
            &mint,
            &token_program,
            &memo,
            data_b58,
        ),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("partially-decoded lookup must succeed");

    assert_eq!(
        result,
        Some(signature),
        "partially-decoded MintTo with matching accounts must match"
    );
    mock.shutdown().await;
}

/// Parsed `mintToChecked` carrying the expected memo and accounts —
/// drives the `tokenAmount.amount` branch in
/// `parsed_instruction_has_expected_mint`.
#[tokio::test]
async fn parsed_mint_to_checked_returns_signature() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_004;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 5_555;
    let memo = mint_idempotency_memo(&event_id(txn_id));
    let signature = prior_sig();

    mock.enqueue("getSignaturesForAddress", signature_reply(&memo));
    mock.enqueue(
        "getTransaction",
        parsed_mint_to_checked_reply(
            &signature,
            &mint_authority,
            &recipient_ata,
            &mint,
            &token_program,
            &memo,
            amount,
        ),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("mintToChecked lookup must succeed");

    assert_eq!(result, Some(signature), "mintToChecked must match");
    mock.shutdown().await;
}

/// Memo field arrives as a multi-segment string with one bracketed
/// length-prefix segment and one bare segment — exercises both
/// branches of `strip_memo_length_prefix` (the `return memo;` early
/// return on the bare segment, plus the happy `[N] value` parse on the
/// other).
#[tokio::test]
async fn memo_with_mixed_prefix_segments_matches_and_returns_signature() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_005;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 8_192;
    let expected = mint_idempotency_memo(&event_id(txn_id));
    let signature = prior_sig();

    let memo_field = format!("unrelated-bare-memo; [{}] {}", expected.len(), expected);

    mock.enqueue("getSignaturesForAddress", signature_reply(&memo_field));
    mock.enqueue(
        "getTransaction",
        parsed_mint_to_checked_reply(
            &signature,
            &mint_authority,
            &recipient_ata,
            &mint,
            &token_program,
            &expected,
            amount,
        ),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &expected)
        .await
        .expect("mixed-prefix memo lookup must succeed");

    assert_eq!(
        result,
        Some(signature),
        "second segment carries the bracketed expected memo"
    );
    mock.shutdown().await;
}

/// `signature_status.memo` is a foreign memo that does not match — the
/// validator must `continue` past the entry. With no further
/// signatures, the helper returns `Ok(None)` and never issues a
/// `getTransaction` call.
#[tokio::test]
async fn memo_mismatch_skips_entry_and_returns_none() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_006;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let expected = mint_idempotency_memo(&event_id(txn_id));

    mock.enqueue(
        "getSignaturesForAddress",
        signature_reply("private_channel:mint-idempotency:wrong-id"),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        1_000,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &expected)
        .await
        .expect("non-matching memo must not be a hard failure");

    assert!(
        result.is_none(),
        "non-matching memo must yield Ok(None) so submit proceeds"
    );
    assert_eq!(
        mock.call_count("getTransaction"),
        0,
        "no entry passed memo filter, no getTransaction should fire"
    );
    mock.shutdown().await;
}

/// `meta.err` is populated → `transaction_succeeded` returns false →
/// `transaction_matches_expected_mint` short-circuits → loop continues
/// → `Ok(None)`.
#[tokio::test]
async fn transaction_with_meta_error_is_skipped_and_returns_none() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 7_007;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 1_111;
    let memo = mint_idempotency_memo(&event_id(txn_id));
    let signature = prior_sig();

    let mint_to_data = spl_token::instruction::TokenInstruction::MintTo { amount }.pack();
    let data_b58 = bs58::encode(mint_to_data).into_string();

    mock.enqueue("getSignaturesForAddress", signature_reply(&memo));
    mock.enqueue(
        "getTransaction",
        raw_get_transaction_reply(
            &signature,
            &mint_authority,
            &recipient_ata,
            &mint,
            &token_program,
            &memo,
            data_b58,
            // `AccountNotFound` is a TransactionError unit variant — its bare
            // string form deserializes cleanly, unlike tuple variants
            // (`InstructionError`) which require a `{ "Variant": [...] }` shape.
            json!("AccountNotFound"),
        ),
    );

    let bwt = builder_with_txn(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );
    let result = find_existing_mint_signature_with_memo(&client, &bwt, &memo)
        .await
        .expect("meta-error transaction is filtered, lookup is not a hard failure");

    assert!(
        result.is_none(),
        "transaction with meta.err must not count as a confirmed mint"
    );
    mock.shutdown().await;
}
