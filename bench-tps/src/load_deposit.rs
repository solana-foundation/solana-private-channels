//! Phase 3 — Deposit load generation (Solana escrow)
//!
//! Mirrors the structure of `load.rs` but targets the Solana escrow program's
//! `Deposit` instruction instead of an SPL token transfer.
//!
//! The generator signs a batch of deposit transactions per cycle, pushing each
//! batch onto a `BatchQueue`.  Sender threads pop batches and call
//! `send_transaction` against the Solana RPC endpoint.

use {
    crate::{
        bench_metrics::{BENCH_SENT_TOTAL, FLOW_DEPOSIT},
        types::{BatchQueue, BenchState, DepositConfig, MAX_QUEUE_DEPTH},
    },
    private_channel_escrow_program_client::instructions::{Deposit, DepositInstructionArgs},
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        hash::Hash,
        instruction::Instruction,
        pubkey,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        transaction::Transaction,
    },
    spl_associated_token_account::get_associated_token_address,
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    tokio_util::sync::CancellationToken,
    tracing::warn,
};

/// Amount of tokens deposited per transaction (1 raw unit).
const DEPOSIT_AMOUNT: u64 = 1;

/// SPL Memo program — accepts any data and always succeeds.
/// Appending a unique nonce prevents the validator from deduplicating
/// transactions that share the same accounts and blockhash.
const MEMO_PROGRAM_ID: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

/// Build a signed Solana deposit transaction.
///
/// `nonce` is encoded as 8 LE bytes in a memo instruction so that every
/// transaction has a unique signature even when the accounts and blockhash
/// are identical across calls.
fn build_deposit_tx(
    depositor: &Keypair,
    config: &DepositConfig,
    blockhash: Hash,
    nonce: u64,
) -> Transaction {
    let depositor_pubkey = depositor.pubkey();
    let user_ata = get_associated_token_address(&depositor_pubkey, &config.mint);

    let accounts = Deposit {
        payer: depositor_pubkey,
        user: depositor_pubkey,
        instance: config.instance_pda,
        mint: config.mint,
        allowed_mint: config.allowed_mint_pda,
        user_ata,
        instance_ata: config.instance_ata,
        system_program: solana_sdk::system_program::id(),
        token_program: spl_token::id(),
        associated_token_program: spl_associated_token_account::id(),
        event_authority: config.event_authority,
        private_channel_escrow_program:
            private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    };

    let deposit_ix = accounts.instruction(DepositInstructionArgs {
        amount: DEPOSIT_AMOUNT,
        recipient: None,
    });

    let memo_ix = Instruction {
        program_id: MEMO_PROGRAM_ID,
        accounts: vec![],
        data: nonce.to_string().into_bytes(),
    };

    Transaction::new_signed_with_payer(
        &[deposit_ix, memo_ix],
        Some(&depositor_pubkey),
        &[depositor],
        blockhash,
    )
}

/// Async generator: signs batches of deposit transactions and enqueues them.
///
/// Exits when `cancel` is triggered.
pub async fn run_deposit_generator(
    config: Arc<DepositConfig>,
    state: Arc<BenchState>,
    queue: BatchQueue,
    batch_size: usize,
    cancel: CancellationToken,
) {
    let mut tx_seq: usize = 0;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        {
            let (lock, _) = queue.as_ref();
            if lock.lock().unwrap().len() >= MAX_QUEUE_DEPTH {
                tokio::task::yield_now().await;
                continue;
            }
        }

        let blockhash = *state.current_blockhash.read().await;

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let depositor = &config.keypairs[tx_seq % config.keypairs.len()];
            let tx = build_deposit_tx(depositor, &config, blockhash, tx_seq as u64);
            batch.push(tx);
            tx_seq = tx_seq.wrapping_add(1);
        }

        let (lock, cvar) = queue.as_ref();
        lock.lock().unwrap().push_back(batch);
        cvar.notify_one();

        tokio::task::yield_now().await;
    }
}

/// Blocking sender thread for deposit transactions.
///
/// Identical in structure to `load::run_sender_thread` but sends to the Solana
/// RPC URL and increments metrics with `flow="deposit"`.
pub fn run_deposit_sender_thread(
    solana_rpc_url: String,
    queue: BatchQueue,
    cancel: CancellationToken,
    sent_count: Arc<AtomicU64>,
    sleep_ms: u64,
) {
    // Use Confirmed commitment for preflight simulation so that transactions
    // pass even if the setup state (ATAs, token balances) is not yet Finalized.
    // The local validator finalises ~32 slots after confirmation (~13 s), which
    // can lag behind the load phase start.
    let rpc = solana_client::rpc_client::RpcClient::new_with_commitment(
        solana_rpc_url,
        CommitmentConfig::confirmed(),
    );

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let batch = {
            let (lock, cvar) = queue.as_ref();
            let mut q = lock.lock().unwrap();
            loop {
                if cancel.is_cancelled() {
                    return;
                }
                if let Some(batch) = q.pop_front() {
                    break batch;
                }
                let (new_q, _) = cvar
                    .wait_timeout(q, std::time::Duration::from_millis(50))
                    .unwrap();
                q = new_q;
            }
        };

        for tx in &batch {
            BENCH_SENT_TOTAL.with_label_values(&[FLOW_DEPOSIT]).inc();
            if let Err(e) = rpc.send_transaction_with_config(
                tx,
                RpcSendTransactionConfig {
                    skip_preflight: false,
                    preflight_commitment: Some(CommitmentLevel::Confirmed),
                    ..Default::default()
                },
            ) {
                warn!(err = %e, "deposit sender: send_transaction failed");
            }
        }

        sent_count.fetch_add(batch.len() as u64, Ordering::Relaxed);

        if sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
        }
    }
}
