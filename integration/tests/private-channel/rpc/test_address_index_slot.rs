use {
    super::test_context::PrivateChannelContext,
    private_channel_indexer::operator::{RetryConfig, RpcClientWithRetry},
    solana_commitment_config::CommitmentConfig,
    solana_sdk::{signature::Keypair, signer::Signer},
    std::time::Duration,
};

/// Poll the index progress through the indexer's own client until the
/// watermark covers the newest block, or panic after `budget`.
async fn wait_caught_up(rpc: &RpcClientWithRetry, budget: Duration, phase: &str) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let (watermark, latest_block) = rpc
            .get_address_index_slot()
            .await
            .expect("a running core answers getAddressIndexSlot");
        if watermark >= latest_block {
            return (watermark, latest_block);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{phase}: index never caught up: watermark {watermark}, latest block {latest_block}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The resync gate's view of a real node: after traffic, and again after an idle
/// stretch, the address index reaches the newest committed block.
pub async fn run_address_index_slot_test(ctx: &PrivateChannelContext) {
    println!("\n=== Address Index Slot Test ===");

    let rpc = RpcClientWithRetry::with_retry_config(
        ctx.read_client.url(),
        RetryConfig::default(),
        CommitmentConfig::confirmed(),
    );

    let from = Keypair::new();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let memo_ix = solana_sdk::instruction::Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: b"address-index-slot".to_vec(),
    };
    let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[memo_ix],
        Some(&from.pubkey()),
        &[&from],
        blockhash,
    );
    ctx.send_and_check(&tx, Duration::from_secs(5))
        .await
        .unwrap()
        .expect("the memo lands in a block");

    let (watermark, latest_block) = wait_caught_up(&rpc, Duration::from_secs(10), "busy").await;
    println!("Caught up after traffic: watermark {watermark}, latest block {latest_block}");

    tokio::time::sleep(Duration::from_secs(3)).await;
    let (watermark, latest_block) = wait_caught_up(&rpc, Duration::from_secs(5), "idle").await;
    println!("Caught up while idle: watermark {watermark}, latest block {latest_block}");

    println!("✓ Address index slot test passed!");
}
