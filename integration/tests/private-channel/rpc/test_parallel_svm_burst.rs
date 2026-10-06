//! `test_parallel_svm_burst`
//!
//! Target file: `core/src/vm/gasless_callback.rs` — drives the
//! `SnapshotCallback` impl that's only reachable through the parallel-SVM
//! execution path.
//!
//! The parallel path in `core/src/stages/execution.rs` is gated by
//!     `regular_txs_in_batch >= max_svm_workers * MIN_PARALLEL_BATCH_FACTOR`
//! With the integration `NodeConfig` set to `max_svm_workers = 4` and
//! `MIN_PARALLEL_BATCH_FACTOR = 4`, that's a 16-tx threshold. The
//! conflict-free scheduler splits a batch into ConflictFreeBatches; two
//! txs that share a writable account (e.g. the same fee-payer)
//! always end up in separate sub-batches. So a naive burst from a single
//! fee-payer produces 20 single-tx ConflictFreeBatches and never crosses
//! the parallel threshold.
//!
//! Strategy: burst 20 memos, each paid by a different fresh fee-payer. They
//! need no funding, since execution is gasless and a memo spends nothing.
//! With no shared writable account across the burst, the scheduler keeps
//! all 20 in a single ConflictFreeBatch → `regular_transactions.len() = 20`
//! → `execute_parallel` → `SnapshotCallback::from_bob` + impl arms fire.

use {
    super::test_context::PrivateChannelContext,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig},
    solana_sdk::{
        instruction::Instruction,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::{sync::Arc, time::Duration},
    tokio::time::sleep,
};

const BURST_SIZE: usize = 20;
const SETTLE_DEADLINE_SECS: u64 = 10;

pub async fn run_parallel_svm_burst_test(ctx: &PrivateChannelContext) {
    println!("\n=== Parallel-SVM Burst ===");

    // Distinct fee-payers so no pair of burst txs shares a writable account.
    let fee_payers: Vec<Keypair> = (0..BURST_SIZE).map(|_| Keypair::new()).collect();
    let blockhash = ctx
        .get_blockhash()
        .await
        .expect("getLatestBlockhash for burst");

    let txs: Vec<Transaction> = fee_payers
        .iter()
        .enumerate()
        .map(|(i, payer)| {
            let memo = Instruction {
                program_id: spl_memo::id(),
                accounts: vec![],
                data: format!("parallel-burst:{i}").into_bytes(),
            };
            Transaction::new_signed_with_payer(&[memo], Some(&payer.pubkey()), &[payer], blockhash)
        })
        .collect();

    // Single shared client — its internal reqwest pool keeps one HTTP
    // connection alive across all 20 send_transaction calls. Wrapped in
    // Arc so we can clone into each spawned task without re-creating the
    // underlying HTTP machinery per send.
    let client = Arc::new(RpcClient::new(ctx.write_client.url()));

    // Spawn all 20 send_transaction futures together. tokio::spawn returns
    // immediately, so the first send_transaction is already in flight before
    // the loop schedules the next.
    let mut set = tokio::task::JoinSet::new();
    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        ..Default::default()
    };
    for tx in txs {
        let client = client.clone();
        set.spawn(async move {
            client
                .send_transaction_with_config(&tx, send_config)
                .await
                .expect("send_transaction should succeed")
        });
    }

    let mut signatures = Vec::with_capacity(BURST_SIZE);
    while let Some(joined) = set.join_next().await {
        signatures.push(joined.expect("join task"));
    }
    assert_eq!(signatures.len(), BURST_SIZE);
    println!("  → submitted {} concurrent memos", signatures.len());

    // Poll until every signature lands. Generous deadline because parallel
    // execution timing is variable.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(SETTLE_DEADLINE_SECS);
    let mut landed = 0usize;
    while tokio::time::Instant::now() < deadline && landed < signatures.len() {
        landed = 0;
        for sig in &signatures {
            if ctx
                .get_transaction(sig)
                .await
                .expect("get_transaction should not error")
                .is_some()
            {
                landed += 1;
            }
        }
        if landed < signatures.len() {
            sleep(Duration::from_millis(100)).await;
        }
    }

    assert_eq!(
        landed,
        signatures.len(),
        "expected all {} burst txs to settle within {SETTLE_DEADLINE_SECS}s; only {landed} did.",
        signatures.len(),
    );
    println!("  ✓ all {} burst memos settled", landed);
    println!("✓ Parallel-SVM burst test passed");
}
