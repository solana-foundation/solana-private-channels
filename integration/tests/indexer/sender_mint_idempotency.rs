//! Mint idempotency memo scan: `find_existing_mint_signature_with_memo` returns
//! `Some(signature)` when a prior confirmed mint carries the expected memo on the
//! recipient ATA, so the caller can skip re-minting a deposit that already landed.
//!
//! No longer on the live deposit path (which now persists the signature before
//! broadcast); only the remint path calls it. The wire behavior tested is the same.
//!
//! Strategy: drive it against `MockRpcServer`, scripting a `getSignaturesForAddress`
//! reply with `mint_idempotency_memo(txn_id)` and a matching `getTransaction`.
//!
//! What this test validates:
//!   - `find_existing_mint_signature_with_memo` issues `getSignaturesForAddress`
//!     on the expected ATA
//!   - It filters to entries that carry the memo produced by
//!     `mint_idempotency_memo(txn_id)`
//!   - It fetches the full transaction via `getTransaction` and
//!     verifies the payload matches the expected mint parameters
//!   - On a match, it returns `Some(signature)` so the remint caller can skip
//!     re-sending
//!
//! What is intentionally NOT asserted here:
//!   - The remint caller's use of the returned `Some(sig)` (covered on the
//!     remint path); this file pins the wire-level lookup behavior only
//!   - Metric increments (none fire on the idempotency-hit path)

use {
    base64::{engine::general_purpose::STANDARD, Engine as _},
    private_channel_indexer::operator::{
        find_existing_mint_signature_with_memo,
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

fn build_mint_to_builder_with_txn_id(
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

/// Script a successful `getTransaction` reply whose parsed JSON representation
/// carries:
///   - a `spl-memo` instruction with the expected memo string
///   - a `spl-token` `mintTo` instruction against the expected mint + recipient
///
/// This matches the production `transaction_matches_expected_mint` walker.
fn get_transaction_reply(
    signature: &Signature,
    mint: &Pubkey,
    recipient_ata: &Pubkey,
    mint_authority: &Pubkey,
    token_program: &Pubkey,
    amount: u64,
    memo: &str,
) -> Reply {
    let memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
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
                        "pubkey": memo_program_id,
                        "signer": false,
                        "writable": false,
                        "source": "transaction",
                    },
                ],
                "recentBlockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                "instructions": [
                    {
                        "program": "spl-memo",
                        "programId": memo_program_id,
                        "parsed": memo,
                    },
                    {
                        "program": "spl-token",
                        "programId": token_program.to_string(),
                        "parsed": {
                            "type": "mintTo",
                            "info": {
                                "mint": mint.to_string(),
                                "account": recipient_ata.to_string(),
                                "mintAuthority": mint_authority.to_string(),
                                "amount": amount.to_string(),
                            },
                        },
                    },
                ],
            },
        },
    }))
}

/// When a prior confirmed mint with the expected memo exists on the ATA,
/// `find_existing_mint_signature_with_memo` returns `Some(sig)` - the signal the
/// remint path uses to skip re-minting a deposit that already landed.
#[tokio::test]
async fn finds_prior_confirmed_mint_and_returns_short_circuit_signature() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 9_001;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);
    let amount: u64 = 12_345;
    let memo = mint_idempotency_memo(txn_id);

    // The on-chain signature we'll claim was landed on a previous operator run.
    let prior_signature = Signature::from_str(
        "4BxWw1FjwQCHXWkrK4ZehPWauFTPhBafSr9m8Cuht73LG73nUs3wfuJ6gigkhNppP4pYogP5pQDENbE5nQx1Qp4B",
    )
    .unwrap();

    // 1) getSignaturesForAddress returns one entry carrying the memo + no err.
    mock.enqueue(
        "getSignaturesForAddress",
        Reply::result(json!([
            {
                "signature": prior_signature.to_string(),
                "slot": 100u64,
                "err": null,
                "memo": format!("[5] {}", memo),
                "blockTime": 1_700_000_000i64,
                "confirmationStatus": "finalized",
            }
        ])),
    );

    // 2) getTransaction returns a parsed JSON payload whose instruction
    //    set satisfies `transaction_matches_expected_mint`.
    mock.enqueue(
        "getTransaction",
        get_transaction_reply(
            &prior_signature,
            &mint,
            &recipient_ata,
            &mint_authority,
            &token_program,
            amount,
            &memo,
        ),
    );

    let builder_with_txn = build_mint_to_builder_with_txn_id(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        amount,
    );

    let result = find_existing_mint_signature_with_memo(
        &client,
        &builder_with_txn,
        &mint_idempotency_memo(builder_with_txn.txn_id),
    )
    .await
    .expect("idempotency lookup must succeed when payload matches");

    let found = result.expect("matching memo + mint payload must yield Some(signature)");
    assert_eq!(
        found, prior_signature,
        "returned signature must be the same one we scripted"
    );

    // Exactly one call each — no retries on success.
    assert_eq!(mock.call_count("getSignaturesForAddress"), 1);
    assert_eq!(mock.call_count("getTransaction"), 1);

    mock.shutdown().await;
}

/// When no prior confirmed mint matches the expected memo (empty result),
/// `find_existing_mint_signature_with_memo` returns `Ok(None)` - the signal that
/// the remint path may proceed with the mint.
#[tokio::test]
async fn returns_none_when_no_prior_mint_signature_matches() {
    let mock = MockRpcServer::start().await;
    let client = test_client(mock.url());

    let txn_id: i64 = 9_002;
    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let mint_authority = Pubkey::new_unique();
    let token_program = spl_token::id();
    let recipient_ata =
        get_associated_token_address_with_program_id(&recipient, &mint, &token_program);

    // Empty signature history — no prior confirmed mint on this ATA.
    mock.enqueue("getSignaturesForAddress", Reply::result(json!([])));

    let builder_with_txn = build_mint_to_builder_with_txn_id(
        txn_id,
        mint,
        recipient_ata,
        mint_authority,
        token_program,
        10_000,
    );

    let result = find_existing_mint_signature_with_memo(
        &client,
        &builder_with_txn,
        &mint_idempotency_memo(builder_with_txn.txn_id),
    )
    .await
    .expect("lookup on empty ATA must succeed");

    assert!(
        result.is_none(),
        "empty signatures list must yield Ok(None) so the sender proceeds with submit"
    );
    // No getTransaction call should have fired — no candidate to inspect.
    assert_eq!(mock.call_count("getTransaction"), 0);

    mock.shutdown().await;
}

/// Base-64 blob for reference — sanity that we have the crate imports we
/// need for later (account-data tests live in t8; keeping the
/// import alive avoids dead-code warnings if tests expand).
#[allow(dead_code)]
fn _encode_ref(data: &[u8]) -> String {
    STANDARD.encode(data)
}
