use {
    crate::setup::get_associated_token_address,
    solana_sdk::{signature::Keypair, signer::Signer, transaction::Transaction},
    std::time::Duration,
    tokio::time::sleep,
};

use super::test_context::PrivateChannelContext;
use crate::setup;

pub async fn run_tx_replay_test(ctx: &PrivateChannelContext) {
    let alice = Keypair::new();
    let bob = Keypair::new();
    let mint = Keypair::new();

    println!("\nUsers:");
    println!("  Admin: {}", ctx.operator_key.pubkey());
    println!("  Mint: {}", mint.pubkey());
    println!("  Alice: {}", alice.pubkey());
    println!("  Bob: {}", bob.pubkey());

    // Create mint on PrivateChannel
    let blockhash = ctx.get_blockhash().await.unwrap();
    let create_mint_tx = setup::create_mint_account_transaction(
        &ctx.operator_key,
        &mint,
        &ctx.operator_key.pubkey(),
        3,
        blockhash,
    );
    let sig = ctx.send_transaction(&create_mint_tx).await.unwrap();
    ctx.check_transaction_exists(sig).await;
    println!("Created mint: {}", sig);

    let alice_token_account = get_associated_token_address(&alice.pubkey(), &mint.pubkey());
    let bob_token_account = get_associated_token_address(&bob.pubkey(), &mint.pubkey());

    let blockhash = ctx.get_blockhash().await.unwrap();
    // Create token accounts for all users
    let mut sigs = Vec::new();
    for keypair in [&alice, &bob] {
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account(
                &keypair.pubkey(),
                &keypair.pubkey(),
                &mint.pubkey(),
                &spl_token_2022::id(),
            );
        let sig = ctx
            .write_client
            .send_transaction(&Transaction::new_signed_with_payer(
                &[create_ata_ix],
                Some(&keypair.pubkey()),
                &[keypair],
                blockhash,
            ))
            .await
            .unwrap();
        sigs.push(sig);
    }
    for sig in sigs {
        ctx.check_transaction_exists(sig).await;
        println!("Create token account transaction sent: {}", sig);
    }

    // Mint initial tokens to Alice
    println!("\n=== Minting Tokens to Alice ===");
    let blockhash = ctx.get_blockhash().await.unwrap();
    let mint_tx = setup::mint_to_transaction(
        &ctx.operator_key,
        &mint.pubkey(),
        &alice_token_account,
        &ctx.operator_key.pubkey(),
        1_000_000,
        blockhash,
    );

    let sig = ctx.send_transaction(&mint_tx).await.unwrap();
    ctx.check_transaction_exists(sig).await;
    println!("Minted 1000 tokens to Alice: {}", sig);

    // Transfer tokens from Alice to Bob using the same blockhash multiple times
    println!("\n=== Transferring 250 tokens from Alice to Bob ===");
    let blockhash = ctx.get_blockhash().await.unwrap();
    for replay_idx in 0..10 {
        let transfer_tx = setup::transfer_tokens_transaction(
            &alice,
            &bob.pubkey(),
            &mint.pubkey(),
            250_000,
            blockhash,
        );

        let sig = ctx.send_transaction(&transfer_tx).await.unwrap();
        ctx.check_transaction_exists(sig).await;
        println!("replay #{}: Transfer transaction sent: {}", replay_idx, sig);

        // Give time for the transfer to be processed
        sleep(Duration::from_millis(500)).await;

        println!(
            "replay #{}: Alice token account {}",
            replay_idx, alice_token_account
        );
        println!(
            "replay #{}: Bob token account {}",
            replay_idx, bob_token_account
        );
        assert_eq!(
            ctx.get_token_balance(&bob_token_account).await.unwrap(),
            250_000,
            "replay #{}: Bob should have 250 tokens",
            replay_idx
        );
        assert_eq!(
            ctx.get_token_balance(&alice_token_account).await.unwrap(),
            750_000,
            "replay #{}: Alice should have 750 tokens",
            replay_idx
        );
    }
}
