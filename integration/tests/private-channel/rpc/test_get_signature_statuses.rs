use {
    super::test_context::PrivateChannelContext,
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_sdk::{
        signature::{Keypair, Signature},
        signer::Signer,
    },
    solana_system_interface::instruction as system_instruction,
};

pub async fn run_get_signature_statuses_test(ctx: &PrivateChannelContext) {
    println!("\n=== Get Signature Statuses Test ===");

    test_signature_statuses_with_malformed_and_unknown_signatures(ctx).await;
    test_signature_statuses_rejects_too_many_signatures(ctx).await;

    println!("\n✓ getSignatureStatuses tests passed!");
}

async fn test_signature_statuses_with_malformed_and_unknown_signatures(
    ctx: &PrivateChannelContext,
) {
    println!("\n  Test 1: unknown signatures return null, malformed ones fail the call");

    let from_keypair = Keypair::new();
    let to_pubkey = Keypair::new().pubkey();

    let blockhash = ctx.get_blockhash().await.unwrap();
    let transfer_ix = system_instruction::transfer(&from_keypair.pubkey(), &to_pubkey, 1_000);

    let transaction = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[transfer_ix],
        Some(&from_keypair.pubkey()),
        &[&from_keypair],
        blockhash,
    );

    let sig = ctx.send_transaction(&transaction).await.unwrap();
    ctx.check_transaction_exists(sig).await;

    let unknown_sig = Signature::new_unique().to_string();
    let invalid_sig = "not-a-valid-base58-signature".to_string();

    let response = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            json!([[sig.to_string(), unknown_sig]]),
        )
        .await
        .expect("getSignatureStatuses should succeed for well-formed signatures");

    let statuses = response
        .get("value")
        .and_then(|value| value.as_array())
        .expect("Response should contain a status array in the value field");

    assert_eq!(statuses.len(), 2, "Expected one status per input signature");
    assert!(
        statuses[0].is_object(),
        "Confirmed signature should return a status object"
    );
    assert!(
        statuses[1].is_null(),
        "Unknown signature should return null"
    );

    // A malformed entry is a client error, so the batch fails rather than
    // nulling that element and letting it read as proof of non-inclusion.
    let error = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            json!([[sig.to_string(), invalid_sig]]),
        )
        .await
        .expect_err("A malformed signature should fail the whole call");

    let error_message = error.to_string();
    assert!(
        error_message.contains("Invalid signature"),
        "Expected an invalid-signature error, got: {}",
        error_message
    );

    println!("  ✓ unknown signatures return null, malformed ones fail the call");
}

async fn test_signature_statuses_rejects_too_many_signatures(ctx: &PrivateChannelContext) {
    println!("\n  Test 2: request larger than max signature limit fails");

    let too_many_signatures = vec![Signature::new_unique().to_string(); 257];

    let error = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::GetSignatureStatuses,
            json!([too_many_signatures]),
        )
        .await
        .expect_err("Request with 257 signatures should fail");

    let error_message = error.to_string();
    assert!(
        error_message.contains("Too many signatures"),
        "Expected too-many-signatures error, got: {}",
        error_message
    );
    assert!(
        error_message.contains("max: 256"),
        "Expected max signatures hint in error, got: {}",
        error_message
    );

    println!("  ✓ requests over 256 signatures are rejected");
}
