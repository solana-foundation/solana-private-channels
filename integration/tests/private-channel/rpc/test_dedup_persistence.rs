use {
    private_channel_core::nodes::node::{NodeConfig, NodeMode},
    private_channel_core::stage_metrics::NoopMetrics,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig, signature::Keypair, signer::Signer,
        transaction::Transaction,
    },
    spl_associated_token_account::get_associated_token_address,
    std::{sync::Arc, time::Duration},
    tokio::time::sleep,
};

use super::utils::{
    restart_private_channel, send_and_confirm, start_private_channel, token_balance,
};
use crate::helpers::get_free_port;
use crate::setup;

/// Verify that dedup state and chain continuity survive a node restart.
///
/// Flow:
///   1. Start a fresh PrivateChannel node backed by the provided Postgres DB.
///   2. Create a mint, token accounts for Alice and Bob, mint 1_000_000 to Alice.
///   3. Transfer 250_000 from Alice to Bob — record balances, tx count, and slot.
///   4. Restart the node (same DB, same port).
///   5. Assert slot height is restored to at least the pre-restart value.
///   6. Re-submit the exact same transfer transaction (same signature + blockhash).
///   7. Assert balances unchanged and tx count did not increase (dedup works).
///   8. Send a new transfer with a fresh blockhash and confirm it succeeds
///      (blockhash window was restored and the node can continue processing).
pub async fn run_dedup_persistence_test(db_url: String) {
    println!("\n=== Dedup Persistence Test ===");

    let operator = Keypair::new();
    let alice = Keypair::new();
    let bob = Keypair::new();
    let mint = Keypair::new();

    let port = get_free_port();
    let node_config = NodeConfig {
        mode: NodeMode::Aio,
        port,
        sigverify_queue_size: 100,
        sigverify_workers: 2,
        max_connections: 50,
        max_tx_per_batch: 10,
        batch_deadline_ms: 5,
        batch_channel_capacity: 16,
        ingress_queue_capacity: private_channel_core::nodes::node::DEFAULT_INGRESS_QUEUE_CAPACITY,
        sequencer_queue_capacity:
            private_channel_core::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        execution_results_capacity:
            private_channel_core::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY,
        max_svm_workers: 4,
        accountsdb_connection_url: db_url,
        admin_keys: vec![operator.pubkey()],
        transaction_expiration_ms: 15_000,
        blocktime_ms: 100,
        perf_sample_period_secs: 10,
        metrics: Arc::new(NoopMetrics),
    };

    let (handles, rpc_url) = start_private_channel(node_config.clone()).await.unwrap();
    let client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    println!("  Operator : {}", operator.pubkey());
    println!("  Mint     : {}", mint.pubkey());
    println!("  Alice    : {}", alice.pubkey());
    println!("  Bob      : {}", bob.pubkey());

    // --- Create mint ---
    let blockhash = client.get_latest_blockhash().await.unwrap();
    let create_mint_tx =
        setup::create_mint_account_transaction(&operator, &mint, &operator.pubkey(), 3, blockhash);
    send_and_confirm(&client, &create_mint_tx).await;
    println!("  Mint created");

    // --- Create ATAs for Alice and Bob ---
    let alice_ata = get_associated_token_address(&alice.pubkey(), &mint.pubkey());
    let bob_ata = get_associated_token_address(&bob.pubkey(), &mint.pubkey());

    let blockhash = client.get_latest_blockhash().await.unwrap();
    for keypair in [&alice, &bob] {
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account(
                &keypair.pubkey(),
                &keypair.pubkey(),
                &mint.pubkey(),
                &spl_token::id(),
            );
        let tx = Transaction::new_signed_with_payer(
            &[create_ata_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            blockhash,
        );
        send_and_confirm(&client, &tx).await;
    }
    println!("  ATAs created");

    // --- Mint 1_000_000 to Alice ---
    let blockhash = client.get_latest_blockhash().await.unwrap();
    let mint_tx = setup::mint_to_transaction(
        &operator,
        &mint.pubkey(),
        &alice_ata,
        &operator.pubkey(),
        1_000_000,
        blockhash,
    );
    send_and_confirm(&client, &mint_tx).await;
    println!("  Minted 1_000_000 to Alice");

    // --- Transfer 250_000 from Alice to Bob (this is the tx we will replay) ---
    let blockhash = client.get_latest_blockhash().await.unwrap();
    let transfer_tx = setup::transfer_tokens_transaction(
        &alice,
        &bob.pubkey(),
        &mint.pubkey(),
        250_000,
        blockhash,
    );
    send_and_confirm(&client, &transfer_tx).await;

    sleep(Duration::from_millis(300)).await;

    let alice_balance_before = token_balance(&client, &alice_ata).await.unwrap();
    let bob_balance_before = token_balance(&client, &bob_ata).await.unwrap();
    let tx_count_before = client.get_transaction_count().await.unwrap();

    assert_eq!(
        alice_balance_before, 750_000,
        "Alice should have 750_000 before restart"
    );
    assert_eq!(
        bob_balance_before, 250_000,
        "Bob should have 250_000 before restart"
    );
    println!(
        "  Pre-restart — Alice: {}, Bob: {}, tx_count: {}",
        alice_balance_before, bob_balance_before, tx_count_before
    );

    // Record pre-restart slot height
    let slot_before = client.get_slot().await.unwrap();
    println!("  Pre-restart slot: {}", slot_before);

    // --- Restart the node with the same DB ---
    let (new_handles, rpc_url) = restart_private_channel(handles, node_config).await.unwrap();
    // Recreate the client so it doesn't reuse a stale HTTP connection to the old process
    let client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    println!("  Node restarted");

    // --- Assert: slot height restored ---
    let slot_after_restart = client.get_slot().await.unwrap();
    assert!(
        slot_after_restart >= slot_before,
        "Slot must be >= pre-restart value ({} vs {})",
        slot_after_restart,
        slot_before
    );
    println!(
        "  Post-restart slot: {} (pre-restart: {})",
        slot_after_restart, slot_before
    );

    // --- Re-submit the exact same transfer transaction ---
    println!("  Re-submitting transfer tx after restart...");
    // The result doesn't matter — dedup should silently drop it.
    let _ = client.send_transaction(&transfer_tx).await;

    sleep(Duration::from_millis(500)).await;

    // --- Assert: no double execution ---
    let alice_balance_after = token_balance(&client, &alice_ata).await.unwrap();
    let bob_balance_after = token_balance(&client, &bob_ata).await.unwrap();
    let tx_count_after = client.get_transaction_count().await.unwrap();

    assert_eq!(
        alice_balance_after, alice_balance_before,
        "Alice's balance must not change — duplicate was rejected by dedup"
    );
    assert_eq!(
        bob_balance_after, bob_balance_before,
        "Bob's balance must not change — duplicate was rejected by dedup"
    );
    assert_eq!(
        tx_count_after, tx_count_before,
        "Transaction count must not increase — duplicate was rejected by dedup"
    );

    println!(
        "  Post-restart — Alice: {}, Bob: {}, tx_count: {}",
        alice_balance_after, bob_balance_after, tx_count_after
    );
    println!("  PASS: dedup state persisted across restart");

    // --- Assert: node can continue processing with fresh blockhash ---
    println!("  Sending new transfer after restart...");
    let fresh_blockhash = client.get_latest_blockhash().await.unwrap();
    let new_transfer_tx = setup::transfer_tokens_transaction(
        &alice,
        &bob.pubkey(),
        &mint.pubkey(),
        100_000,
        fresh_blockhash,
    );
    send_and_confirm(&client, &new_transfer_tx).await;

    sleep(Duration::from_millis(300)).await;

    let alice_final = token_balance(&client, &alice_ata).await.unwrap();
    let bob_final = token_balance(&client, &bob_ata).await.unwrap();
    let tx_count_final = client.get_transaction_count().await.unwrap();

    assert_eq!(
        alice_final, 650_000,
        "Alice should have 650_000 after new post-restart transfer"
    );
    assert_eq!(
        bob_final, 350_000,
        "Bob should have 350_000 after new post-restart transfer"
    );
    assert!(
        tx_count_final > tx_count_before,
        "Transaction count must increase after new post-restart transfer"
    );

    let slot_final = client.get_slot().await.unwrap();
    assert!(
        slot_final > slot_after_restart,
        "Slot must advance after new post-restart transaction ({} vs {})",
        slot_final,
        slot_after_restart
    );

    println!(
        "  Post-new-tx — Alice: {}, Bob: {}, tx_count: {}, slot: {}",
        alice_final, bob_final, tx_count_final, slot_final
    );
    println!("  PASS: node resumed processing with restored blockhash window");

    new_handles.shutdown().await;
}
