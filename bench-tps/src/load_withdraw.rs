//! Phase 3 — Withdraw load generation (PrivateChannel withdraw-burn)
//!
//! Mirrors the structure of `load.rs` but targets the PrivateChannel withdraw program's
//! `WithdrawFunds` instruction instead of an SPL token transfer.
//!
//! The generator signs a batch of withdraw transactions per cycle, pushing each
//! batch onto a `BatchQueue`.  Sender threads pop batches and call
//! `send_transaction` against the PrivateChannel write-node RPC endpoint.

use {
    crate::{
        bench_metrics::{BENCH_SENT_TOTAL, FLOW_WITHDRAW},
        types::{BatchQueue, BenchState, WithdrawConfig, MAX_QUEUE_DEPTH},
    },
    private_channel_withdraw_program_client::instructions::{
        WithdrawFunds, WithdrawFundsInstructionArgs,
    },
    solana_sdk::{
        hash::Hash, instruction::Instruction, pubkey, pubkey::Pubkey, signature::Keypair,
        signer::Signer, transaction::Transaction,
    },
    spl_associated_token_account::get_associated_token_address,
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    tokio_util::sync::CancellationToken,
    tracing::warn,
};

/// Amount of tokens burned per withdraw transaction (1 raw unit).
const WITHDRAW_AMOUNT: u64 = 1;

/// SPL Memo program — accepts any data and always succeeds.
/// Appending a unique nonce prevents duplicate-signature rejection when the
/// same withdrawer account reuses the same blockhash across batches.
const MEMO_PROGRAM_ID: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

/// Build a signed PrivateChannel withdraw-burn transaction.
///
/// `nonce` is encoded as a decimal string in a memo instruction so that every
/// transaction has a unique signature even when the same keypair and blockhash
/// are reused across batches.
fn build_withdraw_tx(
    withdrawer: &Keypair,
    config: &WithdrawConfig,
    blockhash: Hash,
    nonce: u64,
) -> Transaction {
    let withdrawer_pubkey = withdrawer.pubkey();
    let token_account = get_associated_token_address(&withdrawer_pubkey, &config.mint);

    let accounts = WithdrawFunds {
        user: withdrawer_pubkey,
        mint: config.mint,
        token_account,
        token_program: spl_token::id(),
        associated_token_program: spl_associated_token_account::id(),
    };

    let ix = accounts.instruction(WithdrawFundsInstructionArgs {
        amount: WITHDRAW_AMOUNT,
        destination: None,
    });

    let memo_ix = Instruction {
        program_id: MEMO_PROGRAM_ID,
        accounts: vec![],
        data: nonce.to_string().into_bytes(),
    };

    Transaction::new_signed_with_payer(
        &[ix, memo_ix],
        Some(&withdrawer_pubkey),
        &[withdrawer],
        blockhash,
    )
}

/// Async generator: signs batches of withdraw transactions and enqueues them.
///
/// Exits when `cancel` is triggered.
pub async fn run_withdraw_generator(
    config: Arc<WithdrawConfig>,
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
            let withdrawer = &config.keypairs[tx_seq % config.keypairs.len()];
            let tx = build_withdraw_tx(withdrawer, &config, blockhash, tx_seq as u64);
            batch.push(tx);
            tx_seq = tx_seq.wrapping_add(1);
        }

        let (lock, cvar) = queue.as_ref();
        lock.lock().unwrap().push_back(batch);
        cvar.notify_one();

        tokio::task::yield_now().await;
    }
}

/// Blocking sender thread for withdraw transactions.
///
/// Sends to the PrivateChannel write-node and increments metrics with `flow="withdraw"`.
pub fn run_withdraw_sender_thread(
    private_channel_rpc_url: String,
    queue: BatchQueue,
    cancel: CancellationToken,
    sent_count: Arc<AtomicU64>,
    sleep_ms: u64,
) {
    let rpc = solana_client::rpc_client::RpcClient::new(private_channel_rpc_url);

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
            BENCH_SENT_TOTAL.with_label_values(&[FLOW_WITHDRAW]).inc();
            if let Err(e) = rpc.send_transaction(tx) {
                warn!(err = %e, "withdraw sender: send_transaction failed");
            }
        }

        sent_count.fetch_add(batch.len() as u64, Ordering::Relaxed);

        if sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
        }
    }
}
