use super::test_context::PrivateChannelContext;
use solana_sdk::{
    hash::Hash,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as system_instruction;
use std::time::Duration;
use tokio::time::sleep;

pub async fn run_blockhash_validation_test(ctx: &PrivateChannelContext) {
    println!("\n=== Blockhash Validation Test ===");

    // Test 1: Valid recent blockhash should be accepted
    println!("\n--- Test 1: Valid recent blockhash ---");
    let recent_blockhash = ctx.get_blockhash().await.unwrap();
    println!("Got recent blockhash: {:?}", recent_blockhash);

    let is_valid = ctx
        .read_client
        .is_blockhash_valid(
            &recent_blockhash,
            solana_commitment_config::CommitmentConfig::confirmed(),
        )
        .await
        .unwrap();

    assert!(is_valid, "Recent blockhash should be valid");
    println!("✓ Recent blockhash is valid");

    // Test 2: Generate more blocks to populate the blockhash window
    println!("\n--- Test 2: Populate blockhash window ---");
    let first_blockhash = ctx.get_blockhash().await.unwrap();
    println!("First blockhash: {:?}", first_blockhash);

    // Send a few transactions to advance the blockchain
    let test_keypair = Keypair::new();
    for i in 0..3 {
        let blockhash = ctx.get_blockhash().await.unwrap();
        let transfer_ix = system_instruction::transfer(
            &ctx.operator_key.pubkey(),
            &test_keypair.pubkey(),
            1000 * (i + 1),
        );

        let tx = Transaction::new_signed_with_payer(
            &[transfer_ix],
            Some(&ctx.operator_key.pubkey()),
            &[&ctx.operator_key],
            blockhash,
        );

        match ctx.send_transaction(&tx).await {
            Ok(sig) => println!("Transaction {} sent: {:?}", i + 1, sig),
            Err(e) => println!(
                "Transaction {} failed (expected in some test setups): {:?}",
                i + 1,
                e
            ),
        }

        // Small delay to allow block progression
        sleep(Duration::from_millis(100)).await;
    }

    // Test 3: Check if the first blockhash is still valid (in window but not latest)
    println!("\n--- Test 3: Older blockhash in window ---");
    let current_blockhash = ctx.get_blockhash().await.unwrap();
    println!("Current blockhash: {:?}", current_blockhash);

    // The first blockhash should still be in the window if the window is large enough
    // Note: This test may pass or fail depending on the blockhash window size
    let first_is_valid = ctx
        .read_client
        .is_blockhash_valid(
            &first_blockhash,
            solana_commitment_config::CommitmentConfig::confirmed(),
        )
        .await
        .unwrap();

    if first_is_valid {
        println!("✓ First blockhash is still valid (in window but not latest)");
        assert!(
            first_blockhash != current_blockhash,
            "First blockhash should be different from current blockhash"
        );
    } else {
        println!("⚠ First blockhash has expired (window is smaller than expected)");
    }

    // Test 4: Expired/invalid blockhash should be rejected
    println!("\n--- Test 4: Invalid blockhash ---");
    // Create a fake blockhash that's definitely not in the window
    let fake_blockhash = Hash::new_unique();
    println!("Fake blockhash: {:?}", fake_blockhash);

    let fake_is_valid = ctx
        .read_client
        .is_blockhash_valid(
            &fake_blockhash,
            solana_commitment_config::CommitmentConfig::confirmed(),
        )
        .await
        .unwrap();

    assert!(!fake_is_valid, "Fake blockhash should be invalid");
    println!("✓ Fake blockhash is correctly rejected");

    // Test 5: Verify RPC-Dedup consistency
    println!("\n--- Test 5: RPC-Dedup consistency ---");
    // Get a fresh blockhash and immediately validate it
    let fresh_blockhash = ctx.get_blockhash().await.unwrap();
    let fresh_is_valid = ctx
        .read_client
        .is_blockhash_valid(
            &fresh_blockhash,
            solana_commitment_config::CommitmentConfig::confirmed(),
        )
        .await
        .unwrap();

    assert!(
        fresh_is_valid,
        "Fresh blockhash from getLatestBlockhash should be valid in isBlockhashValid"
    );
    println!("✓ RPC consistency verified: blockhash from getLatestBlockhash is valid in isBlockhashValid");

    // Test 6: Submit a transaction with a fake blockhash so it is dropped at
    // the dedup stage's unknown-blockhash arm (`core/src/stages/dedup.rs`).
    // The tx must NOT land — getTransaction returns None — and the dedup
    // metric for "dropped: unknown blockhash" fires.
    println!("\n--- Test 6: Tx with unknown blockhash is dropped ---");
    let fake_for_send = Hash::new_unique();
    let dropped_keypair = Keypair::new();
    let drop_tx = Transaction::new_signed_with_payer(
        &[system_instruction::transfer(
            &ctx.operator_key.pubkey(),
            &dropped_keypair.pubkey(),
            1,
        )],
        Some(&ctx.operator_key.pubkey()),
        &[&ctx.operator_key],
        fake_for_send,
    );
    let send_outcome = ctx
        .write_client
        .send_transaction_with_config(
            &drop_tx,
            solana_client::rpc_config::RpcSendTransactionConfig {
                skip_preflight: true,
                ..Default::default()
            },
        )
        .await;
    if let Ok(sig) = send_outcome {
        // Brief poll: the dedup drop is silent (no rpc error), so we just
        // assert the tx never settles within the window we'd expect for a
        // real settlement.
        sleep(Duration::from_millis(400)).await;
        let landed = ctx.get_transaction(&sig).await.unwrap();
        assert!(
            landed.is_none(),
            "Tx with unknown blockhash should not land; got: {landed:?}"
        );
        println!("✓ Tx with unknown blockhash was dropped at dedup");
    } else {
        // Some configurations may reject unknown blockhashes at sendTransaction
        // (preflight or shape validation) before they reach dedup. Either
        // outcome — server-side rejection or silent dedup drop — is correct
        // behaviour; the dedup-drop path is the silent one we care about
        // for coverage.
        println!(
            "ℹ Server rejected tx with unknown blockhash at send time: {:?}",
            send_outcome.err()
        );
    }

    println!("\n=== Blockhash Validation Test Complete ===\n");
}
