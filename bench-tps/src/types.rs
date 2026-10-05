//! Shared types and constants used across all phases of the bench.
//!
//! Keeping these in one place makes it easy to tune the bench without hunting
//! through multiple files.

use {
    solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Keypair, transaction::Transaction},
    std::{
        collections::VecDeque,
        sync::{Arc, Condvar, Mutex},
    },
    tokio::{sync::RwLock, time::Duration},
};

// ---------------------------------------------------------------------------
// Tuning constants
// ---------------------------------------------------------------------------

/// Number of decimal places for the SPL mint created during setup.
pub const MINT_DECIMALS: u8 = 6;

/// Withdraw fee every bench AllowMint sets, and the withdraw setup writes to
/// the channel. Charged on top of each withdrawal, so a withdraw of 1 raw unit
/// spends 2 of a withdrawer's `initial_balance`.
pub const BENCH_WITHDRAW_FEE: u64 = 1;

/// Minimum withdrawal every bench AllowMint sets, and the withdraw setup
/// writes to the channel. At 1 so the bench's 1-raw-unit withdrawals pass.
pub const BENCH_MIN_WITHDRAW_AMOUNT: u64 = 1;

/// Minimum deposit every bench AllowMint sets. 0 so any bench deposit amount
/// lands.
pub const BENCH_MIN_DEPOSIT_AMOUNT: u64 = 0;

/// Maximum time to wait for a batch of on-chain confirmations before giving up.
pub const CONFIRM_TIMEOUT: Duration = Duration::from_secs(120);

/// Delay between successive `getSignatureStatuses` polls during confirmation.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Maximum number of transactions sent concurrently per ATA/mint-to batch
/// during the setup phase.  Keeps the in-flight HTTP connection count bounded.
pub const MAX_CONCURRENT_SENDS: usize = 64;

/// How often the background blockhash poller calls `getLatestBlockhash`.
/// The private_channel node rejects transactions whose blockhash is older than ~15 s,
/// so refreshing at 80 ms gives a comfortable margin.
pub const BLOCKHASH_POLL_INTERVAL: Duration = Duration::from_millis(80);

/// How often the metrics sampler fires to log instantaneous TPS.
pub const METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// How often the blockhash poller emits an average fetch-latency log line.
pub const BLOCKHASH_LOG_INTERVAL: Duration = Duration::from_secs(2);

/// Token units transferred per SPL transfer transaction.
/// 1 raw unit = 0.000001 token (6 decimals).
///
/// Transaction uniqueness is guaranteed by a memo instruction carrying
/// `tx_seq` rather than by varying the amount, so all transfers use the
/// same fixed amount and accounts drain at a predictable rate.
/// With `--initial-balance 1_000_000` each account can make 1_000_000
/// transfers before exhaustion.
pub const TRANSFER_AMOUNT: u64 = 1;

/// Maximum number of pending batches allowed in the queue before the
/// generator yields.  This bounds queue memory and prevents the generator
/// from running too far ahead of the senders.
pub const MAX_QUEUE_DEPTH: usize = 32;

/// Number of accounts processed per setup batch (ATA creation, mint-to).
/// After each batch all transactions are confirmed before the next batch
/// starts, and only the ones that failed to land are retried.
pub const SETUP_BATCH_SIZE: usize = 200;

/// Maximum signatures per `getSignatureStatuses` RPC call.
/// The Solana RPC spec caps this at 256; exceeding it returns HTTP 413.
pub const SIG_STATUS_CHUNK_SIZE: usize = 256;

// ---------------------------------------------------------------------------
// Shared data structures
// ---------------------------------------------------------------------------

/// Immutable configuration for the load phase, shared across the generator
/// task and all sender threads via `Arc`.
pub struct BenchConfig {
    /// The SPL mint created during setup.  All ATAs and transfers use this mint.
    pub mint: Pubkey,

    /// Sender keypairs (first half of funded accounts).
    /// The generator cycles through these round-robin so that each keypair
    /// signs roughly the same number of transactions.
    pub accounts: Vec<Arc<Keypair>>,

    /// Receiver wallet pubkeys (drawn from the second half of funded accounts).
    /// Length equals `num_conflict_groups` (clamped to the receiver pool size).
    ///
    /// Because sender and receiver pools are disjoint, no account appears on
    /// both sides of a transaction — sequencer contention is zero at the
    /// default setting.  Setting `num_conflict_groups = 1` concentrates all
    /// traffic on a single receiver to stress the sequencer.
    pub destinations: Vec<Pubkey>,
}

/// Mutable shared state updated by background tasks and read by the generator.
pub struct BenchState {
    /// The most recently fetched blockhash.  The blockhash poller writes here
    /// every 80 ms; the generator reads it before signing each batch.
    /// Using a `RwLock` allows many concurrent readers with rare writes.
    pub current_blockhash: RwLock<Hash>,
}

/// The shared batch queue between the async generator task and the blocking
/// sender threads.
///
/// Inner type: `(Mutex<VecDeque<Vec<Transaction>>>, Condvar)`
///
/// - The `Mutex` guards the deque.
/// - The `Condvar` lets sender threads block cheaply when the queue is empty
///   instead of spinning, and wakes them when the generator pushes a new batch.
pub type BatchQueue = Arc<(Mutex<VecDeque<Vec<Transaction>>>, Condvar)>;

// ---------------------------------------------------------------------------
// Deposit / Withdraw configuration structs
// ---------------------------------------------------------------------------

/// Configuration for the deposit load phase.
///
/// Holds the on-chain state set up by `setup_deposit::run_setup_deposit_phase`.
pub struct DepositConfig {
    /// The Solana token mint being deposited into the escrow.
    pub mint: Pubkey,
    /// The escrow instance PDA derived from the instance seed.
    pub instance_pda: Pubkey,
    /// The allowed-mint PDA for this (instance, mint) pair.
    pub allowed_mint_pda: Pubkey,
    /// The instance's ATA (escrow account) for this mint.
    pub instance_ata: Pubkey,
    /// Event authority PDA for the escrow program (derived from "event_authority" seed).
    pub event_authority: Pubkey,
    /// Depositor keypairs, one per account.  Each has a Solana ATA funded with tokens.
    pub keypairs: Vec<Arc<Keypair>>,
    /// Shared mutable state seeded with the current Solana blockhash.
    pub state: Arc<BenchState>,
}

/// Configuration for the withdraw load phase.
///
/// Holds the on-chain state set up by the PrivateChannel `setup::run_setup_phase`.
pub struct WithdrawConfig {
    /// The PrivateChannel SPL mint whose tokens will be burned on withdraw.
    pub mint: Pubkey,
    /// Withdrawer keypairs, one per account.  Each has a PrivateChannel ATA funded with tokens.
    pub keypairs: Vec<Arc<Keypair>>,
    /// The mint's withdraw config PDA on PrivateChannel.
    pub withdraw_config: Pubkey,
    /// The admin's PrivateChannel ATA, which each withdrawal's fee is paid to.
    pub treasury_token_account: Pubkey,
    /// Shared mutable state seeded with the current PrivateChannel blockhash.
    pub state: Arc<BenchState>,
}
