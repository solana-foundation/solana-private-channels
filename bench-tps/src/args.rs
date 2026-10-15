//! CLI argument definitions for private-channel-bench-tps.
//!
//! Every argument can also be set via an environment variable (useful when
//! running inside Docker or calling from `run.sh`).  Environment variables
//! follow the `BENCH_` prefix convention.

use {clap::Parser, std::path::PathBuf};

/// Top-level CLI with subcommands.
#[derive(Parser, Debug)]
#[command(
    name = "private-channel-bench-tps",
    about = "Load testing binary for the PrivateChannel pipeline"
)]
pub struct Cli {
    #[command(subcommand)]
    pub subcommand: SubCommand,
}

/// Available bench subcommands.
#[derive(clap::Subcommand, Debug)]
pub enum SubCommand {
    /// SPL token transfer load test (PrivateChannel pipeline, default flow).
    Transfer(TransferArgs),
    /// Escrow deposit load test (Solana → escrow program).
    Deposit(DepositArgs),
    /// Withdraw-burn load test (PrivateChannel withdraw program).
    Withdraw(WithdrawArgs),
    /// Derive and print the escrow instance PDA for a given instance-seed keypair.
    ///
    /// Prints the base58 PDA to stdout (no RPC call needed).  Used by run.sh to
    /// pre-compute the instance PDA before starting Docker services so that
    /// indexer-solana and operator-solana can be configured to watch it.
    DerivePda(DerivePdaArgs),
    /// Create the escrow instance for an instance-seed keypair, unless it already exists.
    ///
    /// Used by run.sh before starting the workers, which refuse a missing instance.
    CreateInstance(CreateInstanceArgs),
}

/// Arguments for the create-instance subcommand.
#[derive(Parser, Debug)]
pub struct CreateInstanceArgs {
    /// Path to the admin keypair JSON file; it pays for and administers the instance.
    #[arg(long, env = "BENCH_ADMIN_KEYPAIR")]
    pub admin_keypair: PathBuf,

    /// JSON-RPC endpoint of the Solana validator.
    #[arg(
        long,
        default_value = "http://localhost:18899",
        env = "BENCH_SOLANA_RPC_URL"
    )]
    pub solana_rpc_url: String,

    /// Path to the instance-seed keypair JSON file.
    #[arg(long, env = "BENCH_INSTANCE_SEED_KEYPAIR")]
    pub instance_seed_keypair: PathBuf,
}

/// Arguments for the derive-pda subcommand.
#[derive(Parser, Debug)]
pub struct DerivePdaArgs {
    /// Path to the instance-seed keypair JSON file.
    ///
    /// If the file does not exist it will be created (a new keypair is
    /// generated and saved), so this command doubles as a keypair generator.
    #[arg(long, env = "BENCH_INSTANCE_SEED_KEYPAIR")]
    pub instance_seed_keypair: PathBuf,
}

/// Arguments for the transfer subcommand (existing behaviour).
#[derive(Parser, Debug)]
pub struct TransferArgs {
    /// Path to the admin keypair JSON file.
    ///
    /// The admin keypair is used to:
    ///   - initialise the SPL mint
    ///   - create ATAs for each generated account
    ///   - mint initial token balances to each ATA
    ///
    /// Generated automatically by `scripts/run.sh`.
    #[arg(long, env = "BENCH_ADMIN_KEYPAIR")]
    pub admin_keypair: PathBuf,

    /// JSON-RPC endpoint of the private_channel write-node (or gateway).
    ///
    /// `run.sh` points this at the gateway (`http://localhost:GATEWAY_PORT`)
    /// so that read requests are automatically routed to the read-node.
    #[arg(long, default_value = "http://localhost:8899", env = "BENCH_RPC_URL")]
    pub rpc_url: String,

    /// Number of funded source accounts to generate.
    ///
    /// Each account gets its own keypair, ATA, and initial token balance.
    /// Must be >= `--threads` to avoid multiple senders sharing a keypair
    /// (which would cause nonce conflicts).
    #[arg(long, default_value_t = 200, env = "BENCH_ACCOUNTS")]
    pub accounts: usize,

    /// Duration of the load phase in seconds.
    #[arg(long, default_value_t = 60, env = "BENCH_DURATION")]
    pub duration: u64,

    /// Number of concurrent sender tasks.
    ///
    /// Each sender task runs an async loop: pop batch → send all txs
    /// concurrently via `join_all` → sleep `--sender-sleep-ms` → repeat.
    /// More tasks = higher throughput up to the point where the node or
    /// network becomes the bottleneck.
    #[arg(long, default_value_t = 16, env = "BENCH_THREADS")]
    pub threads: usize,

    /// Number of transactions per batch produced by the generator.
    ///
    /// Each batch is sent concurrently by a single sender task using
    /// `join_all`, so larger batches increase per-task parallelism.
    /// Decoupled from `--threads` to allow independent tuning.
    #[arg(long, default_value_t = 200, env = "BENCH_BATCH_SIZE")]
    pub batch_size: usize,

    /// Number of distinct receiver accounts.
    ///
    /// Accounts are split into a sender pool (first half) and a receiver pool
    /// (second half).  This flag controls how many receivers are used:
    ///   - 1              → all senders target the same receiver (max contention)
    ///   - accounts / 2   → each sender has a unique receiver (zero contention)
    ///
    /// Defaults to `accounts / 2` (zero contention) when omitted.
    #[arg(long, env = "BENCH_NUM_CONFLICT_GROUPS")]
    pub num_conflict_groups: Option<usize>,

    /// Initial token balance (raw units) minted to each account's ATA.
    ///
    /// Each transfer costs 1 raw unit, so this is effectively the number of
    /// transfers an account can make before its balance is exhausted.
    #[arg(long, default_value_t = 1_000_000, env = "BENCH_INITIAL_BALANCE")]
    pub initial_balance: u64,

    /// Optional port for a Prometheus `/metrics` endpoint.
    ///
    /// When set, the binary exposes real-time bench metrics for scraping.
    /// When absent, metrics are written to stdout via `tracing` only.
    #[arg(long, env = "BENCH_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Milliseconds each sender thread sleeps after dispatching one batch.
    ///
    /// Use this to throttle the send rate without reducing `--threads`.
    /// A value of 0 disables the sleep entirely (maximum throughput mode).
    #[arg(long, default_value_t = 0, env = "BENCH_SENDER_SLEEP_MS")]
    pub sender_sleep_ms: u64,

    /// Tracing log level.  One of: error, warn, info, debug, trace.
    ///
    /// The `RUST_LOG` environment variable takes precedence when set.
    #[arg(long, default_value = "info", env = "BENCH_LOG_LEVEL")]
    pub log_level: String,
}

/// Arguments for the deposit subcommand (Solana escrow deposit load test).
#[derive(Parser, Debug)]
pub struct DepositArgs {
    /// Path to the admin keypair JSON file.
    ///
    /// Used to fund depositor accounts with SOL and mint tokens.
    #[arg(long, env = "BENCH_ADMIN_KEYPAIR")]
    pub admin_keypair: PathBuf,

    /// JSON-RPC endpoint of the Solana validator (for deposit transactions).
    ///
    /// The local validator container exposes port 8899 → 18899 on the host.
    #[arg(
        long,
        default_value = "http://localhost:18899",
        env = "BENCH_SOLANA_RPC_URL"
    )]
    pub solana_rpc_url: String,

    /// Number of depositor accounts to generate.
    #[arg(long, default_value_t = 20, env = "BENCH_DEPOSIT_ACCOUNTS")]
    pub accounts: usize,

    /// Duration of the deposit load phase in seconds.
    #[arg(long, default_value_t = 60, env = "BENCH_DURATION")]
    pub duration: u64,

    /// Number of concurrent sender threads.
    #[arg(long, default_value_t = 4, env = "BENCH_THREADS")]
    pub threads: usize,

    /// Milliseconds each sender thread sleeps after dispatching one batch.
    #[arg(long, default_value_t = 5, env = "BENCH_SENDER_SLEEP_MS")]
    pub sender_sleep_ms: u64,

    /// Initial token balance (raw units) minted to each depositor ATA.
    #[arg(long, default_value_t = 1_000_000, env = "BENCH_INITIAL_BALANCE")]
    pub initial_balance: u64,

    /// Optional port for a Prometheus `/metrics` endpoint.
    #[arg(long, env = "BENCH_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Prometheus /metrics URL of the operator-solana container.
    ///
    /// When set, samples `private_channel_operator_mints_sent_total` every second and
    /// includes e2e minted count and drop rate in the final CLI summary.
    #[arg(long, env = "BENCH_OPERATOR_METRICS_URL")]
    pub operator_metrics_url: Option<String>,

    /// Path to the instance-seed keypair JSON file.
    ///
    /// If provided and the file exists, the same escrow instance PDA is reused
    /// across runs so that indexer-solana and operator-solana (pre-configured
    /// with the matching PDA) can observe the deposits.  If the file does not
    /// exist it is created (a new keypair is generated and saved).  If this
    /// argument is omitted a fresh ephemeral keypair is generated each run
    /// (useful for isolated tests that don't need e2e indexer tracking).
    #[arg(long, env = "BENCH_INSTANCE_SEED_KEYPAIR")]
    pub instance_seed_keypair: Option<PathBuf>,

    /// JSON-RPC endpoint of the PrivateChannel write-node (or gateway).
    ///
    /// Used during setup to initialise the SPL mint on PrivateChannel so the operator
    /// can mint immediately without JIT initialisation.
    #[arg(
        long,
        default_value = "http://localhost:8898",
        env = "BENCH_PRIVATE_CHANNEL_RPC_URL"
    )]
    pub private_channel_rpc_url: String,

    /// Tracing log level.
    #[arg(long, default_value = "info", env = "BENCH_LOG_LEVEL")]
    pub log_level: String,
}

/// Arguments for the withdraw subcommand (PrivateChannel withdraw-burn load test).
#[derive(Parser, Debug)]
pub struct WithdrawArgs {
    /// Path to the admin keypair JSON file.
    ///
    /// Used to initialise the Solana escrow infrastructure and fund PrivateChannel
    /// withdrawer accounts.  The same keypair is registered as the ReleaseFunds operator.
    #[arg(long, env = "BENCH_ADMIN_KEYPAIR")]
    pub admin_keypair: PathBuf,

    /// JSON-RPC endpoint of the Solana validator (for escrow setup).
    ///
    /// The local validator container exposes port 8899 → 18899 on the host.
    #[arg(
        long,
        default_value = "http://localhost:18899",
        env = "BENCH_SOLANA_RPC_URL"
    )]
    pub solana_rpc_url: String,

    /// JSON-RPC endpoint of the PrivateChannel write-node (or gateway).
    #[arg(long, default_value = "http://localhost:8899", env = "BENCH_RPC_URL")]
    pub rpc_url: String,

    /// Path to the instance-seed keypair JSON file.
    ///
    /// Reuse the same file as the deposit bench (deposit-instance-seed.json) so
    /// that operator-private_channel (pre-configured with COMMON_ESCROW_INSTANCE_ID) can
    /// observe the ReleaseFunds calls from this run.  If absent a fresh ephemeral
    /// keypair is used (e2e tracking via operator-private_channel will not work).
    #[arg(long, env = "BENCH_INSTANCE_SEED_KEYPAIR")]
    pub instance_seed_keypair: Option<PathBuf>,

    /// Number of withdrawer accounts to generate.
    #[arg(long, default_value_t = 20, env = "BENCH_WITHDRAW_ACCOUNTS")]
    pub accounts: usize,

    /// Duration of the withdraw load phase in seconds.
    #[arg(long, default_value_t = 60, env = "BENCH_DURATION")]
    pub duration: u64,

    /// Number of concurrent sender threads.
    #[arg(long, default_value_t = 4, env = "BENCH_THREADS")]
    pub threads: usize,

    /// Milliseconds each sender thread sleeps after dispatching one batch.
    #[arg(long, default_value_t = 5, env = "BENCH_SENDER_SLEEP_MS")]
    pub sender_sleep_ms: u64,

    /// Initial token balance (raw units) minted to each withdrawer's PrivateChannel ATA.
    #[arg(long, default_value_t = 1_000_000, env = "BENCH_INITIAL_BALANCE")]
    pub initial_balance: u64,

    /// Optional port for a Prometheus `/metrics` endpoint.
    #[arg(long, env = "BENCH_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Prometheus /metrics URL of the operator-private_channel container.
    ///
    /// When set, samples `private_channel_operator_mints_sent_total` every second and
    /// includes e2e solana_released count and drop rate in the final CLI summary.
    #[arg(long, env = "BENCH_WITHDRAW_OPERATOR_METRICS_URL")]
    pub operator_metrics_url: Option<String>,

    /// Tracing log level.
    #[arg(long, default_value = "info", env = "BENCH_LOG_LEVEL")]
    pub log_level: String,
}
