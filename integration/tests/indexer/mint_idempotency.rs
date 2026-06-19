//! Integration tests for operator mint idempotency.
//!
//! Verifies that `find_existing_mint_signature_with_memo` correctly detects a previously
//! confirmed mint-to transaction on-chain, preventing the operator from issuing
//! duplicate channel tokens for the same deposit.
//!
//! As of the write-ahead signature-persistence change this memo scan is no longer on
//! the live deposit-mint send path (which now persists its broadcast signature and lets
//! recovery reconcile against it); the function is reached only from the remint path.
//!
//! Scenarios covered:
//! 1. Matching txn_id + amount → existing signature returned (idempotent re-use).
//! 2. Different txn_id (different memo)  → None (treated as a new, unseen deposit).
//! 3. Matching txn_id but wrong amount   → None (partial match is not reused).

#[path = "helpers/mod.rs"]
mod helpers;

use helpers::{generate_mint, send_and_confirm_instructions, setup_wallets};
use private_channel_indexer::operator::{
    find_existing_mint_signature_with_memo, mint_idempotency_memo, MintToBuilder,
    MintToBuilderWithTxnId, RetryConfig, RpcClientWithRetry,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::sync::Arc;
use test_utils::validator_helper::start_test_validator;

/// Submits a real `mint_to` instruction with an idempotency memo, then confirms
/// that `find_existing_mint_signature_with_memo` can locate the transaction by matching
/// both the memo (txn_id) and the token amount.  Also asserts that a mismatched
/// txn_id or a different amount each independently prevent a false positive.
#[tokio::test(flavor = "multi_thread")]
async fn find_existing_mint_signature_with_memo_detects_confirmed_mint() {
    let (validator, faucet_keypair, _geyser_port) = start_test_validator().await;
    let rpc_url = validator.rpc_url();
    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    let payer = Keypair::new();
    let authority = Keypair::new();
    let mint_kp = Keypair::new();
    setup_wallets(&client, &faucet_keypair, &[&payer, &authority])
        .await
        .unwrap();

    generate_mint(&client, &payer, &authority, &mint_kp)
        .await
        .unwrap();

    let recipient = Keypair::new();
    let recipient_ata = get_associated_token_address_with_program_id(
        &recipient.pubkey(),
        &mint_kp.pubkey(),
        &spl_token::id(),
    );

    let txn_id: i64 = 42;
    let amount: u64 = 1000;
    let memo = mint_idempotency_memo(txn_id);

    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &payer.pubkey(),
            &recipient.pubkey(),
            &mint_kp.pubkey(),
            &spl_token::id(),
        );
    let memo_ix = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![AccountMeta::new_readonly(payer.pubkey(), true)],
        data: memo.as_bytes().to_vec(),
    };
    let mint_to_ix = spl_token::instruction::mint_to(
        &spl_token::id(),
        &mint_kp.pubkey(),
        &recipient_ata,
        &authority.pubkey(),
        &[],
        amount,
    )
    .unwrap();

    let sig = send_and_confirm_instructions(
        &client,
        &[create_ata_ix, memo_ix, mint_to_ix],
        &payer,
        &[&payer, &authority],
        "Mint with idempotency memo",
    )
    .await
    .unwrap();

    // The TransactionStatusService processes transactions asynchronously: even
    // after send_and_confirm_transaction returns, the address_signatures,
    // transaction_memos, and transaction_status columns may not yet be populated.
    // Poll until both getSignaturesForAddress and getTransaction succeed for
    // our signature, guaranteeing full indexing before find_existing_mint_signature_with_memo.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
        loop {
            let sigs = client
                .get_signatures_for_address_with_config(
                    &recipient_ata,
                    GetConfirmedSignaturesForAddress2Config {
                        before: None,
                        until: None,
                        limit: Some(5),
                        commitment: Some(CommitmentConfig::confirmed()),
                    },
                )
                .await
                .unwrap_or_default();
            if !sigs.is_empty() {
                // Also verify getTransaction works for our sig so find_existing_mint_signature_with_memo
                // doesn't get a null response when it calls get_transaction internally.
                let tx_ok = client
                    .get_transaction_with_config(
                        &sig,
                        RpcTransactionConfig {
                            encoding: Some(UiTransactionEncoding::JsonParsed),
                            commitment: Some(CommitmentConfig::confirmed()),
                            max_supported_transaction_version: Some(0),
                        },
                    )
                    .await
                    .is_ok();
                if tx_ok {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "Timed out waiting for recipient_ata transaction to be fully indexed"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    }

    let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
        rpc_url.clone(),
        RetryConfig::default(),
        CommitmentConfig::confirmed(),
    ));

    // Matching builder should find the signature
    let mut builder = MintToBuilder::new();
    builder
        .mint(mint_kp.pubkey())
        .recipient_ata(recipient_ata)
        .mint_authority(authority.pubkey())
        .token_program(spl_token::id())
        .amount(amount);
    let builder_with_id = MintToBuilderWithTxnId {
        builder,
        txn_id,
        trace_id: "mint-idempotency-test".to_string(),
    };

    let result = find_existing_mint_signature_with_memo(
        &rpc_client,
        &builder_with_id,
        &mint_idempotency_memo(builder_with_id.txn_id),
    )
    .await
    .unwrap();
    assert_eq!(result, Some(sig));

    // Different txn_id (different memo) should return None
    let mut builder2 = MintToBuilder::new();
    builder2
        .mint(mint_kp.pubkey())
        .recipient_ata(recipient_ata)
        .mint_authority(authority.pubkey())
        .token_program(spl_token::id())
        .amount(amount);
    let builder_with_wrong_id = MintToBuilderWithTxnId {
        builder: builder2,
        txn_id: 999,
        trace_id: "mint-idempotency-test".to_string(),
    };

    let result2 = find_existing_mint_signature_with_memo(
        &rpc_client,
        &builder_with_wrong_id,
        &mint_idempotency_memo(builder_with_wrong_id.txn_id),
    )
    .await
    .unwrap();
    assert_eq!(result2, None);

    // Wrong amount should return None
    let mut builder3 = MintToBuilder::new();
    builder3
        .mint(mint_kp.pubkey())
        .recipient_ata(recipient_ata)
        .mint_authority(authority.pubkey())
        .token_program(spl_token::id())
        .amount(9999);
    let builder_wrong_amount = MintToBuilderWithTxnId {
        builder: builder3,
        txn_id,
        trace_id: "mint-idempotency-test".to_string(),
    };

    let result3 = find_existing_mint_signature_with_memo(
        &rpc_client,
        &builder_wrong_amount,
        &mint_idempotency_memo(builder_wrong_amount.txn_id),
    )
    .await
    .unwrap();
    assert_eq!(result3, None);
}
