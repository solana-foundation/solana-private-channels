//! Phase 3 — Load generation
//!
//! This module contains the two active components of the load phase:
//!
//! **Generator task** (`run_generator`)
//! A single async task that runs on the tokio runtime.  It reads the current
//! blockhash from `BenchState`, signs a batch of SPL transfer transactions
//! (cycling through `BenchConfig::accounts` and `BenchConfig::destinations`),
//! then pushes the completed batch onto `BatchQueue` and notifies the condvar.
//! It applies backpressure by yielding when the queue is already at
//! `MAX_QUEUE_DEPTH` to avoid unbounded memory growth.
//!
//! **Sender threads** (`run_sender_thread`)
//! `--threads` OS threads, each running a blocking loop.  A sender waits on
//! the condvar until a batch is available, pops it, and calls the *synchronous*
//! `solana_client::rpc_client::RpcClient::send_transaction` for each transaction
//! in the batch.  Using blocking threads instead of async tasks prevents the
//! RPC calls from monopolising the tokio thread pool and allows straightforward
//! backpressure via the condvar.  After each batch the thread increments
//! `sent_count` (by the number of transactions sent) and sleeps for `sleep_ms`
//! if throttling is enabled.

use {
    crate::{
        bench_metrics::{BENCH_SENT_TOTAL, FLOW_TRANSFER},
        types::{channel_ata, BenchConfig, BenchState, TRANSFER_AMOUNT},
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        hash::Hash, instruction::Instruction, pubkey::Pubkey, signature::Keypair, signer::Signer,
        transaction::Transaction,
    },
    std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    tokio_util::sync::CancellationToken,
    tracing::warn,
};

/// SPL Memo v1 program — accepts arbitrary UTF-8 (or raw bytes) as instruction
/// data and succeeds unconditionally, making it the standard way to embed
/// unique metadata in a Solana transaction without affecting token balances.
const MEMO_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

/// Build a signed SPL token transfer transaction with a memo instruction that
/// encodes `nonce` as a decimal string.
///
/// Appending a unique nonce guarantees that every transaction has distinct
/// bytes — and therefore a distinct signature — regardless of whether the
/// `(src, dst, amount, blockhash)` tuple repeats.  This completely eliminates
/// the duplicate-signature rejections that the dedup stage would otherwise
/// produce when accounts or destinations are few relative to batch size.
fn build_transfer(
    from: &Keypair,
    to: &Pubkey,
    mint: &Pubkey,
    amount: u64,
    blockhash: Hash,
    nonce: u64,
) -> Transaction {
    let from_pubkey = from.pubkey();
    let from_ata = channel_ata(&from_pubkey, mint);
    let to_ata = channel_ata(to, mint);

    // Unchecked Transfer keeps the measured tx shape; Token-2022 honours it for channel mints.
    #[allow(deprecated)]
    let transfer_ix = spl_token_2022::instruction::transfer(
        &spl_token_2022::id(),
        &from_ata,
        &to_ata,
        &from_pubkey,
        &[],
        amount,
    )
    .unwrap();

    let memo_ix = Instruction {
        program_id: MEMO_PROGRAM_ID,
        accounts: vec![],
        data: nonce.to_string().into_bytes(),
    };

    Transaction::new_signed_with_payer(
        &[transfer_ix, memo_ix],
        Some(&from_pubkey),
        &[from],
        blockhash,
    )
}

/// Split funded accounts into sender and receiver pools for the load phase.
///
/// The accounts list is divided in half:
///   - **Senders**   (`accounts[0..n/2]`) — sign and pay for each transaction.
///   - **Receivers** (`accounts[n/2..n]`) — receive tokens; their pubkeys are returned.
///
/// No account appears in both roles, so no two concurrent transactions share
/// an account and sequencer contention is zero at the default setting.
///
/// `num_conflict_groups` controls how many distinct receiver accounts are used
/// (clamped to the size of the receiver pool):
///   - 1         → every sender targets the same receiver (maximum contention)
///   - pool size → each sender has a unique receiver (zero contention)
pub fn build_destinations(
    accounts: &[Arc<solana_sdk::signature::Keypair>],
    num_conflict_groups: usize,
) -> (Vec<Arc<solana_sdk::signature::Keypair>>, Vec<Pubkey>) {
    let mid = accounts.len() / 2;
    let senders = accounts[..mid].to_vec();
    let n = num_conflict_groups.min(mid).max(1);
    let receivers: Vec<Pubkey> = accounts[mid..mid + n]
        .iter()
        .map(|kp| kp.pubkey())
        .collect();

    (senders, receivers)
}

/// Async generator task: signs batches of SPL transfer transactions and pushes
/// them onto the async_channel for sender tasks to consume.
///
/// The generator cycles through `config.accounts` and `config.destinations`
/// using a wrapping sequence counter so that no two consecutive batches use
/// the same (source, destination) pair (assuming accounts > 1).
///
/// Backpressure is provided by the bounded channel — when the channel is
/// full, `batch_tx.send()` awaits until a sender task pops a batch.
///
/// Exits when `cancel` is triggered.
pub async fn run_generator(
    config: Arc<BenchConfig>,
    state: Arc<BenchState>,
    batch_tx: async_channel::Sender<Vec<Transaction>>,
    batch_size: usize,
    cancel: CancellationToken,
) {
    // Monotonically increasing counter used to index into accounts/destinations.
    // Wraps at usize::MAX without panic.
    let mut tx_seq: usize = 0;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        // Read the latest blockhash.  The blockhash poller keeps this fresh
        // so that the signed transactions are not rejected for a stale hash.
        let blockhash = *state.current_blockhash.read().await;

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            let src = &config.accounts[tx_seq % config.accounts.len()];
            let dst = &config.destinations[tx_seq % config.destinations.len()];
            // tx_seq is passed as the memo nonce so every transaction has a
            // unique signature regardless of blockhash or account cycling.
            let tx = build_transfer(
                src,
                dst,
                &config.mint,
                TRANSFER_AMOUNT,
                blockhash,
                tx_seq as u64,
            );
            batch.push(tx);
            tx_seq = tx_seq.wrapping_add(1);
        }

        // Send the batch to the channel.  The bounded channel provides
        // backpressure — this awaits when the channel is full.
        if batch_tx.send(batch).await.is_err() {
            // Receiver dropped — all sender tasks have exited.
            break;
        }

        // Yield after each batch so the blockhash poller and metrics sampler
        // stay responsive on the same tokio thread.
        tokio::task::yield_now().await;
    }
}

/// Async sender task: pops one batch at a time from a cloned receiver and
/// sends all transactions in the batch concurrently via the async `RpcClient`
/// using `futures::future::join_all`.
///
/// Each sender owns its own `async_channel::Receiver` clone; the channel's
/// built-in MPMC fan-out ensures every batch is delivered to exactly one
/// task. No mutex is involved, so a cancellation signal interrupts every
/// task immediately instead of cascading through a shared lock.
///
/// `sent_count` is incremented by the number of transactions attempted
/// (batch length), matching the BENCH_SENT_TOTAL metric.
pub async fn run_sender_task(
    rpc_url: String,
    batch_rx: async_channel::Receiver<Vec<Transaction>>,
    cancel: CancellationToken,
    sent_count: Arc<AtomicU64>,
    sleep_ms: u64,
) {
    // Each sender task owns its own async RpcClient so there is no lock
    // contention between tasks on the connection pool.
    let rpc = RpcClient::new(rpc_url);

    loop {
        let batch = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            msg = batch_rx.recv() => match msg {
                Ok(b) => b,
                Err(_) => break, // channel closed — generator exited
            }
        };

        // Send all transactions in the batch concurrently.  Each send is an
        // independent HTTP POST, so `join_all` fires them all at once and
        // waits for the slowest one — dramatically reducing per-batch latency
        // compared to sequential sends.
        BENCH_SENT_TOTAL
            .with_label_values(&[FLOW_TRANSFER])
            .inc_by(batch.len() as f64);
        let futs: Vec<_> = batch.iter().map(|tx| rpc.send_transaction(tx)).collect();
        let results = futures::future::join_all(futs).await;

        for result in &results {
            if let Err(e) = result {
                warn!(err = %e, "sender: send_transaction failed");
            }
        }

        sent_count.fetch_add(batch.len() as u64, Ordering::Relaxed);

        // Optional throttle: a non-zero sleep_ms limits the peak send rate
        // without reducing the number of sender tasks.
        if sleep_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
        }
    }
}
