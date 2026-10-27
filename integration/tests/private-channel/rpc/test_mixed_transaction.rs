use {
    solana_sdk::{signature::Keypair, signer::Signer},
    std::time::Duration,
};

use super::{test_context::PrivateChannelContext, utils::SEND_AND_CHECK_DURATION_SECONDS};
use crate::setup;

/// Test that mixed transactions (admin + non-admin instructions) are rejected
pub async fn run_mixed_transaction_test(ctx: &PrivateChannelContext) {
    println!("\n=== Testing Mixed Transactions ===");

    let non_admin = Keypair::new();
    let mint = Keypair::new();

    println!("Non-admin user: {}", non_admin.pubkey());
    println!("Mint: {}", mint.pubkey());

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

    let non_admin_token_account =
        setup::get_associated_token_address(&non_admin.pubkey(), &mint.pubkey());

    // Create token account for non-admin
    let blockhash = ctx.get_blockhash().await.unwrap();
    let create_ata_ix = spl_associated_token_account::instruction::create_associated_token_account(
        &non_admin.pubkey(),
        &non_admin.pubkey(),
        &mint.pubkey(),
        &spl_token_2022::id(),
    );
    let sig = ctx
        .write_client
        .send_transaction(
            &solana_sdk::transaction::Transaction::new_signed_with_payer(
                &[create_ata_ix],
                Some(&non_admin.pubkey()),
                &[&non_admin],
                blockhash,
            ),
        )
        .await
        .unwrap();
    ctx.check_transaction_exists(sig).await;
    println!("Created token account: {}", sig);

    // Mint some tokens to non-admin first so they have something to transfer
    println!("Setting up: minting tokens to non-admin...");
    let blockhash = ctx.get_blockhash().await.unwrap();
    let mint_tx = setup::mint_to_transaction(
        &ctx.operator_key,
        &ctx.mint,
        &non_admin_token_account,
        &ctx.operator_key.pubkey(),
        1_000_000,
        blockhash,
    );

    // This should land (admin transaction)
    let setup_result = ctx
        .send_and_check(
            &mint_tx,
            Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS),
        )
        .await
        .unwrap();
    assert!(
        setup_result.is_some(),
        "Setup failed: mint transaction did not land"
    );
    println!("Setup: minted tokens to non-admin");

    // Try to send a mixed transaction (mint + transfer)
    println!("Attempting to send mixed transaction (should fail)...");
    let blockhash = ctx.get_blockhash().await.unwrap();
    let mixed_tx = setup::mixed_transaction(
        &ctx.operator_key,
        &non_admin,
        &ctx.mint,
        &non_admin_token_account,
        &ctx.operator_key.pubkey(),
        500_000,
        blockhash,
    );

    let result = ctx
        .send_and_check(
            &mixed_tx,
            Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS),
        )
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "Mixed transaction {:?} should not have landed but it did!",
        result
    );

    println!("✓ Mixed transaction correctly rejected");
}
