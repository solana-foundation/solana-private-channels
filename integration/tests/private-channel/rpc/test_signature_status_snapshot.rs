use {
    super::test_context::PrivateChannelContext,
    private_channel_indexer::operator::{RetryConfig, RpcClientWithRetry},
    serde_json::json,
    solana_client::rpc_request::RpcRequest,
    solana_commitment_config::CommitmentConfig,
    solana_sdk::{
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::time::Duration,
};

pub async fn run_signature_status_snapshot_test(ctx: &PrivateChannelContext) {
    println!("\n=== Signature Status Snapshot Test ===");

    test_snapshot_matches_the_separate_reads(ctx).await;
    test_snapshot_rejects_bad_requests(ctx).await;

    println!("\n✓ getSignatureStatusSnapshot tests passed!");
}

/// The operator's client, so this also proves the core and operator wire formats agree.
fn operator_client(ctx: &PrivateChannelContext) -> RpcClientWithRetry {
    RpcClientWithRetry::with_retry_config(
        ctx.read_client.url(),
        RetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        },
        CommitmentConfig::confirmed(),
    )
}

async fn test_snapshot_matches_the_separate_reads(ctx: &PrivateChannelContext) {
    println!("\n  Test 1: snapshot statuses, height and floor match the separate methods");

    let from_keypair = Keypair::new();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let memo_ix = solana_sdk::instruction::Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: b"signature-status-snapshot".to_vec(),
    };
    let transaction = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[memo_ix],
        Some(&from_keypair.pubkey()),
        &[&from_keypair],
        blockhash,
    );
    let landed = ctx
        .send_transaction(&transaction)
        .await
        .expect("the memo is admitted at ingress");
    ctx.check_transaction_exists(landed).await;
    let unknown = Signature::new_unique();

    let height_before = ctx.read_client.get_block_height().await.unwrap();
    let snapshot = operator_client(ctx)
        .get_signature_status_snapshot(&[landed, unknown])
        .await
        .expect("the snapshot should succeed for well-formed signatures");
    let height_after = ctx.read_client.get_block_height().await.unwrap();
    let floor = ctx.read_client.get_first_available_block().await.unwrap();
    let statuses = ctx
        .read_client
        .get_signature_statuses_with_history(&[landed, unknown])
        .await
        .unwrap()
        .value;

    assert_eq!(snapshot.value.len(), 2, "one status per signature");
    assert_eq!(
        snapshot.value[0], statuses[0],
        "a landed signature reports the same status as getSignatureStatuses"
    );
    assert!(snapshot.value[1].is_none(), "an unknown signature is null");
    assert!(
        (height_before..=height_after).contains(&snapshot.block_height),
        "snapshot height {} outside the getBlockHeight bracket {height_before}..={height_after}",
        snapshot.block_height
    );
    assert_eq!(snapshot.first_available_block, floor);

    println!("  ✓ snapshot matches the separate reads");
}

async fn test_snapshot_rejects_bad_requests(ctx: &PrivateChannelContext) {
    println!("\n  Test 2: oversized and malformed requests fail instead of returning null");

    let too_many = vec![Signature::new_unique().to_string(); 257];
    let error = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::Custom {
                method: "getSignatureStatusSnapshot",
            },
            json!([too_many]),
        )
        .await
        .expect_err("257 signatures should be rejected");
    assert!(
        error.to_string().contains("Too many signatures"),
        "expected a too-many-signatures error, got: {error}"
    );

    let error = ctx
        .read_client
        .send::<serde_json::Value>(
            RpcRequest::Custom {
                method: "getSignatureStatusSnapshot",
            },
            json!([["not-a-valid-base58-signature"]]),
        )
        .await
        .expect_err("a malformed signature should fail the whole call");
    assert!(
        error.to_string().contains("Invalid signature"),
        "expected an invalid-signature error, got: {error}"
    );

    println!("  ✓ bad requests are errors, never nulls");
}
