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
    tokio::{sync::Semaphore, time::sleep},
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

    // Race is deterministic when BURST_N × per-tx pipeline time ≫ EXPIRY_MS.
    const EXPIRY_MS: u64 = 100;

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
        // Big queues let dedup admit the whole burst before expiry; backpressure
        // then accumulates in sigverify_queue and drains slowly enough that the
        // tail txs reach execute_batch with blockhash_A already evicted.
        sigverify_queue_size: 5000,
        sigverify_workers: 1,
        max_connections: 500,
        max_tx_per_batch: 1,
        batch_deadline_ms: 5,
        batch_channel_capacity: 5000,
        max_svm_workers: 4,
        accountsdb_connection_url: db_url,
        admin_keys: vec![operator.pubkey()],
        // 100ms / 50ms blocktime → 2-slot live window.
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

    // Filler txs sign against blockhash_A with a distinct fee payer. BURST_N
    // sized so sigverify (the rate-limiting stage) can't drain in < EXPIRY_MS
    // even on the fastest realistic CI hardware (per-tx ed25519 ≥ 50µs).
    const BURST_N: usize = 3000;
    const BURST_CONCURRENCY: usize = 300;
    let burst_client = Arc::new(client);
    let semaphore = Arc::new(Semaphore::new(BURST_CONCURRENCY));
    let mut burst_tasks = Vec::with_capacity(BURST_N);
    for i in 0..BURST_N {
        let client = Arc::clone(&burst_client);
        let payer = burst_payer.insecure_clone();
        let sem = Arc::clone(&semaphore);
        let task = tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
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

    // Head-start so target lands at the back of dedup's queue, still well
    // inside the live window.
    sleep(Duration::from_millis(EXPIRY_MS / 3)).await;

    // --- Build + submit the target tx against blockhash_A ---
    let target_tx = setup::transfer_tokens_transaction(
        &alice,
        &bob.pubkey(),
        &mint.pubkey(),
        100_000,
        blockhash_a,
    );
    let target_sig = target_tx.signatures[0];
    // Target must reach dedup; otherwise we'd misreport as "race not reproduced".
    let send_outcome = burst_client.send_transaction(&target_tx).await;
    println!("  target send outcome: {:?}", send_outcome);
    println!("  target_sig: {}", target_sig);
    assert!(
        send_outcome.is_ok(),
        "target tx rejected at RPC: {:?}",
        send_outcome.err()
    );

    // Sleep past expiry, then wait for burst HTTP completion and pipeline drain.
    // 8s covers per-tx up to ~2.5ms for BURST_N=3000.
    sleep(Duration::from_millis(2 * EXPIRY_MS)).await;
    for t in burst_tasks {
        let _ = t.await;
    }
    sleep(Duration::from_secs(8)).await;

    // --- Assertions ---
    let dropped_at_execution = counting_metrics.executor_dropped_expired();
    let dropped_at_dedup = counting_metrics.dedup_dropped_unknown_blockhash();
    println!(
        "  metrics: executor_dropped_expired={} dedup_dropped_unknown_blockhash={}",
        dropped_at_execution, dropped_at_dedup
    );

    // 1. Filter fired: at least one tx dropped at execution for expired blockhash.
    assert!(
        dropped_at_execution >= 1,
        "expected ≥1 tx dropped at execution, got 0 — pipeline drained before expiry, race not reproduced"
    );

    // 2. Dedup admitted everything (blockhash_A was live at submission).
    assert_eq!(dropped_at_dedup, 0, "dedup must not have rejected anything");

    // 3. Target never landed on-chain (Solana semantics for expired txs).
    let statuses = burst_client
        .get_signature_statuses(&[target_sig])
        .await
        .unwrap();
    let target_status = statuses.value.first().cloned().flatten();
    assert!(
        target_status.is_none(),
        "target tx must not have landed; got status: {target_status:?}"
    );

    // 4. No state change on Bob's account.
    let bob_balance_after = token_balance(&burst_client, &bob_ata).await.unwrap();
    assert_eq!(
        bob_balance_after, bob_balance_before,
        "Bob's balance must not change"
    );

    // 5. Liveness: a fresh-blockhash tx still lands.
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
