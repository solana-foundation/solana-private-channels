use {
    private_channel_core::{
        nodes::node::{NodeConfig, NodeMode},
        stage_metrics::StageMetrics,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    solana_system_interface::instruction as system_instruction,
    spl_associated_token_account::get_associated_token_address,
    std::{sync::Arc, time::Duration},
    tokio::time::sleep,
};

use super::utils::{send_and_confirm, start_private_channel, token_balance, CountingMetrics};
use crate::helpers::get_free_port;
use crate::setup;

/// verify that a tx whose recent_blockhash expires while it sits in
/// an upstream bounded queue is dropped at the execution stage instead of
/// landing on-chain with a stale hash.
///
/// Flow:
///   1. Start a node with tiny queues + short blockhash window.
///   2. Mint tokens to Alice so she has a real balance.
///   3. Capture `blockhash_A`.
///   4. Saturate sigverify + batch queues with a burst of filler txs.
///   5. Submit Alice→Bob transfer against `blockhash_A`; admitted by dedup
///      while the window is still fresh.
///   6. Sleep `2 × transaction_expiration_ms` so `blockhash_A` ages out
///      before the parked target tx reaches execute_batch.
///   7. Drain pipeline and assert the target tx never landed (status none,
///      Bob's balance unchanged).
pub async fn run_blockhash_expiry_after_admission_test(db_url: String) {
    println!("\n=== Blockhash Expiry After Admission Test ===");

    // Coupled: the expiry sleep must outrun the configured window. Single
    // const so config + sleep can never drift apart.
    const EXPIRY_MS: u64 = 500;

    let operator = Keypair::new();
    let alice = Keypair::new();
    let bob = Keypair::new();
    let mint = Keypair::new();
    let burst_payer = Keypair::new();

    let counting_metrics: Arc<CountingMetrics> = Arc::new(CountingMetrics::default());
    let metrics_for_node: Arc<dyn StageMetrics> = counting_metrics.clone();

    let port = get_free_port();
    let node_config = NodeConfig {
        mode: NodeMode::Aio,
        port,
        // Tiny queues + single-tx batches so the burst saturates the path and
        // the target ends up parked on a bounded send().await.
        sigverify_queue_size: 1,
        sigverify_workers: 1,
        // Must exceed BURST_N + target + setup ops; the burst fires concurrently
        // and would otherwise be rejected at the RPC server before reaching dedup.
        max_connections: 1000,
        max_tx_per_batch: 1,
        batch_deadline_ms: 5,
        batch_channel_capacity: 1,
        max_svm_workers: 4,
        accountsdb_connection_url: db_url,
        admin_keys: vec![operator.pubkey()],
        // EXPIRY_MS expiry × 50ms blocktime → 10-slot window. After
        // sleep(2 × EXPIRY_MS) blockhash_A is guaranteed expired.
        transaction_expiration_ms: EXPIRY_MS,
        blocktime_ms: 50,
        perf_sample_period_secs: 10,
        metrics: metrics_for_node,
    };

    let (handles, rpc_url) = start_private_channel(node_config).await.unwrap();
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

    // --- Mint tokens to Alice ---
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

    let bob_balance_before = token_balance(&client, &bob_ata).await.unwrap();
    assert_eq!(bob_balance_before, 0, "Bob starts at 0");

    // --- Capture blockhash_A: the hash the target tx will use ---
    let blockhash_a = client.get_latest_blockhash().await.unwrap();
    println!("  blockhash_A: {:?}", blockhash_a);

    // --- Saturate sigverify + batch queues with a parallel burst ---
    // Filler txs use a distinct fee payer so they don't conflict-block Alice.
    // They sign against blockhash_A so they pass dedup, then queue up behind
    // the bounded sigverify+batch channels.
    const BURST_N: usize = 500;
    let burst_client = Arc::new(client);
    let mut burst_tasks = Vec::with_capacity(BURST_N);
    for i in 0..BURST_N {
        let client = Arc::clone(&burst_client);
        let payer = burst_payer.insecure_clone();
        let task = tokio::spawn(async move {
            let ix = system_instruction::transfer(
                &payer.pubkey(),
                &solana_sdk::pubkey::Pubkey::new_unique(),
                u64::from(i as u32 + 1),
            );
            let tx = Transaction::new_signed_with_payer(
                &[ix],
                Some(&payer.pubkey()),
                &[&payer],
                blockhash_a,
            );
            // Best-effort fire-and-forget; we don't await confirmation.
            let _ = client.send_transaction(&tx).await;
        });
        burst_tasks.push(task);
    }

    // --- Build + submit the target tx against blockhash_A ---
    let target_tx = setup::transfer_tokens_transaction(
        &alice,
        &bob.pubkey(),
        &mint.pubkey(),
        100_000,
        blockhash_a,
    );
    let target_sig = target_tx.signatures[0];
    // The target must reach dedup for the test to exercise the parked-tx race.
    // RPC-server rejection (e.g. max_connections too low) would otherwise
    // surface as a "race not reproduced" failure with no clear cause.
    let send_outcome = burst_client.send_transaction(&target_tx).await;
    println!("  target send outcome: {:?}", send_outcome);
    println!("  target_sig: {}", target_sig);
    assert!(
        send_outcome.is_ok(),
        "target tx submission failed at RPC: {:?} — bump max_connections or reduce BURST_N",
        send_outcome.err()
    );

    // --- Sleep past expiry so blockhash_A ages out of the live window ---
    sleep(Duration::from_millis(2 * EXPIRY_MS)).await;

    // --- Wait for burst to drain (best-effort) and pipeline to catch up ---
    for t in burst_tasks {
        let _ = t.await;
    }
    sleep(Duration::from_secs(3)).await;

    // --- Assertions ---
    let dropped_at_execution = counting_metrics.executor_dropped_expired();
    let dropped_at_dedup = counting_metrics.dedup_dropped_unknown_blockhash();
    println!(
        "  metrics: executor_dropped_expired={} dedup_dropped_unknown_blockhash={}",
        dropped_at_execution, dropped_at_dedup
    );

    // 1. Executor must have dropped at least one tx for blockhash expiry.
    //    Zero here means the pipeline drained the burst faster than expiry
    //    so the target never sat past its blockhash window — not a fix
    //    regression, but a test-tuning failure. Bump BURST_N if this fires.
    assert!(
        dropped_at_execution >= 1,
        "expected ≥1 tx dropped at execution for expired blockhash, got 0 — pipeline drained before expiry, race not reproduced; consider raising BURST_N"
    );

    // 2. Dedup must NOT have rejected the target at admission. blockhash_A
    //    was live when the target was submitted, so dedup must have admitted
    //    it; the only path to non-landing is the execution-stage filter.
    assert_eq!(
        dropped_at_dedup, 0,
        "dedup must not have rejected anything — all txs used blockhash_A which was live at submission"
    );

    // 3. Target tx must not appear in signature_statuses.
    let statuses = burst_client
        .get_signature_statuses(&[target_sig])
        .await
        .unwrap();
    let target_status = statuses.value.first().cloned().flatten();
    assert!(
        target_status.is_none(),
        "target tx must not have landed; got status: {target_status:?}"
    );

    // 4. Bob's ATA balance must not have moved.
    let bob_balance_after = token_balance(&burst_client, &bob_ata).await.unwrap();
    assert_eq!(
        bob_balance_after, bob_balance_before,
        "Bob's balance must not change — target tx was dropped at execution"
    );

    // 5. A fresh tx against a fresh blockhash still works — the node hasn't
    //    deadlocked, only the expired tx was dropped.
    let fresh_blockhash = burst_client.get_latest_blockhash().await.unwrap();
    assert_ne!(fresh_blockhash, blockhash_a, "blockhash must have rotated");
    let fresh_tx = setup::transfer_tokens_transaction(
        &alice,
        &bob.pubkey(),
        &mint.pubkey(),
        1_000,
        fresh_blockhash,
    );
    send_and_confirm(&burst_client, &fresh_tx).await;
    sleep(Duration::from_millis(300)).await;
    let bob_balance_final = token_balance(&burst_client, &bob_ata).await.unwrap();
    assert_eq!(
        bob_balance_final, 1_000,
        "fresh-blockhash tx must land normally"
    );

    println!("  ✓ expired tx was dropped at execution; node still processing");

    handles.shutdown().await;
}
