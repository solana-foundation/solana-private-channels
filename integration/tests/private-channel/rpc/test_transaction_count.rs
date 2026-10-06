use {
    super::test_context::PrivateChannelContext,
    solana_sdk::{signature::Keypair, signer::Signer},
};

pub async fn run_transaction_count_test(ctx: &PrivateChannelContext) {
    println!("\n=== Transaction Count Test ===");

    // Get initial transaction count
    let initial_count = ctx.get_transaction_count().await.unwrap();
    println!("Initial transaction count: {}", initial_count);

    // Create a simple memo transaction to increment the count
    let from_keypair = Keypair::new();

    let blockhash = ctx.get_blockhash().await.unwrap();
    let memo_ix = solana_sdk::instruction::Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: b"transaction-count".to_vec(),
    };

    let transaction = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[memo_ix],
        Some(&from_keypair.pubkey()),
        &[&from_keypair],
        blockhash,
    );

    // Send the transaction
    let sig = ctx.send_transaction(&transaction).await.unwrap();
    println!("Sent transaction: {}", sig);

    // Wait for confirmation
    ctx.check_transaction_exists(sig).await;
    println!("Transaction confirmed: {}", sig);

    // Check that transaction count has increased
    let new_count = ctx.get_transaction_count().await.unwrap();
    println!("New transaction count: {}", new_count);

    assert_eq!(
        new_count,
        initial_count + 1,
        "Transaction count should have increased by exactly 1"
    );

    println!("✓ Transaction count test passed!");
}
