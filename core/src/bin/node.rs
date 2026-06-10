use {
    clap::Parser,
    private_channel_core::{
        nodes::node::{
            run_node, NodeConfig, NodeMode, DEFAULT_EXECUTION_RESULTS_CAPACITY,
            DEFAULT_INGRESS_QUEUE_CAPACITY, DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        },
        stage_metrics::{init_prometheus_metrics, NoopMetrics, PrometheusMetrics},
    },
    private_channel_metrics::start_metrics_server,
    solana_sdk::pubkey::Pubkey,
    std::{str::FromStr, sync::Arc},
    tokio::signal,
    tracing::{error, info, warn},
};

/// PrivateChannel Node - High-performance Solana transaction processing node
#[derive(Parser, Debug)]
#[command(
    name = "private-channel-node",
    about = "PrivateChannel node that can run in read, write, or all-in-one mode"
)]
struct Args {
    /// Node operation mode
    #[arg(short, long, default_value = "aio", env = "PRIVATE_CHANNEL_MODE")]
    mode: NodeMode,

    /// Port to listen on for RPC requests
    #[arg(short, long, default_value_t = 8899, env = "PRIVATE_CHANNEL_PORT")]
    port: u16,

    /// Size of the signature verification queue
    #[arg(
        long,
        default_value_t = 1000,
        env = "PRIVATE_CHANNEL_SIGVERIFY_QUEUE_SIZE"
    )]
    sigverify_queue_size: usize,

    /// Number of signature verification workers
    #[arg(long, default_value_t = 4, env = "PRIVATE_CHANNEL_SIGVERIFY_WORKERS")]
    sigverify_workers: usize,

    /// Maximum number of concurrent RPC connections
    #[arg(long, default_value_t = 100, env = "PRIVATE_CHANNEL_MAX_CONNECTIONS")]
    max_connections: usize,

    /// Maximum transactions per batch
    #[arg(long, default_value_t = 64, env = "PRIVATE_CHANNEL_MAX_TX_PER_BATCH")]
    max_tx_per_batch: usize,

    /// Batch collection deadline in milliseconds (0 = disable, dispatch immediately)
    #[arg(long, default_value_t = 10, env = "PRIVATE_CHANNEL_BATCH_DEADLINE_MS")]
    batch_deadline_ms: u64,

    /// Sequencer→executor batch channel capacity (bounded for back-pressure).
    #[arg(
        long,
        default_value_t = 16,
        env = "PRIVATE_CHANNEL_BATCH_CHANNEL_CAPACITY"
    )]
    batch_channel_capacity: usize,

    /// RPC→dedup ingress queue capacity; a full queue sheds load at admission.
    #[arg(
        long,
        default_value_t = DEFAULT_INGRESS_QUEUE_CAPACITY,
        env = "PRIVATE_CHANNEL_INGRESS_QUEUE_CAPACITY"
    )]
    ingress_queue_capacity: usize,

    /// sigverify→sequencer queue capacity; full applies upstream backpressure.
    #[arg(
        long,
        default_value_t = DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        env = "PRIVATE_CHANNEL_SEQUENCER_QUEUE_CAPACITY"
    )]
    sequencer_queue_capacity: usize,

    /// executor→settler results queue capacity; full applies upstream backpressure.
    #[arg(
        long,
        default_value_t = DEFAULT_EXECUTION_RESULTS_CAPACITY,
        env = "PRIVATE_CHANNEL_EXECUTION_RESULTS_CAPACITY"
    )]
    execution_results_capacity: usize,

    /// Maximum parallel SVM worker threads per batch (including the calling thread).
    /// Set to 1 to disable intra-batch parallelism. Effective only for batches
    /// large enough to amortize the snapshot + thread spawn overhead.
    #[arg(long, default_value_t = 8, env = "PRIVATE_CHANNEL_MAX_SVM_WORKERS")]
    max_svm_workers: usize,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", env = "PRIVATE_CHANNEL_LOG_LEVEL")]
    log_level: String,

    /// Enable JSON logging format
    #[arg(long, env = "PRIVATE_CHANNEL_JSON_LOGS")]
    json_logs: bool,

    /// Accounts database configuration
    #[arg(long, env = "PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL")]
    accountsdb_connection_url: String,

    /// Admin public keys that can bypass certain restrictions (comma-separated base58 strings)
    /// Example: --admin-keys "11111111111111111111111111111111,22222222222222222222222222222222"
    #[arg(long, env = "PRIVATE_CHANNEL_ADMIN_KEYS", value_delimiter = ',')]
    admin_keys: Vec<String>,

    /// Transaction expiration time in milliseconds
    #[arg(
        long,
        default_value_t = 15000,
        env = "PRIVATE_CHANNEL_TRANSACTION_EXPIRATION_MS"
    )]
    transaction_expiration_ms: u64,

    /// Block time in milliseconds
    #[arg(long, default_value_t = 100, env = "PRIVATE_CHANNEL_BLOCKTIME_MS")]
    blocktime_ms: u64,

    /// Performance sample collection period in seconds
    #[arg(
        long,
        default_value_t = 60,
        env = "PRIVATE_CHANNEL_PERF_SAMPLE_PERIOD_SECS"
    )]
    perf_sample_period_secs: u64,

    /// Enable Prometheus stage metrics server (load testing / profiling only).
    /// Bind port is controlled by the PRIVATE_CHANNEL_METRICS_PORT environment variable (default: 9090).
    #[arg(long, env = "PRIVATE_CHANNEL_METRICS")]
    metrics: bool,
}

async fn run_node_with_args(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Parse admin keys from base58 strings to Pubkey
    let admin_keys: Vec<Pubkey> = args
        .admin_keys
        .iter()
        .filter_map(|key_str| {
            if key_str.is_empty() {
                return None;
            }
            match Pubkey::from_str(key_str) {
                Ok(pubkey) => Some(pubkey),
                Err(e) => {
                    error!("Invalid admin key '{}': {}", key_str, e);
                    None
                }
            }
        })
        .collect();

    if !admin_keys.is_empty() {
        info!("Configured admin keys: {:?}", admin_keys);
    }

    let metrics: Arc<dyn private_channel_core::stage_metrics::StageMetrics> = if args.metrics {
        let metrics_port = match std::env::var("PRIVATE_CHANNEL_METRICS_PORT") {
            Ok(value) => match value.parse::<u16>() {
                Ok(port) => port,
                Err(err) => {
                    warn!(
                        "Invalid PRIVATE_CHANNEL_METRICS_PORT='{}' ({}); falling back to 9090",
                        value, err
                    );
                    9090
                }
            },
            Err(_) => 9090,
        };
        init_prometheus_metrics();
        start_metrics_server(metrics_port);
        info!("Stage metrics enabled on port {}", metrics_port);
        Arc::new(PrometheusMetrics)
    } else {
        Arc::new(NoopMetrics)
    };

    let config = NodeConfig {
        mode: args.mode,
        port: args.port,
        sigverify_queue_size: args.sigverify_queue_size,
        sigverify_workers: args.sigverify_workers,
        max_connections: args.max_connections,
        max_tx_per_batch: args.max_tx_per_batch,
        batch_deadline_ms: args.batch_deadline_ms,
        batch_channel_capacity: args.batch_channel_capacity,
        ingress_queue_capacity: args.ingress_queue_capacity,
        sequencer_queue_capacity: args.sequencer_queue_capacity,
        execution_results_capacity: args.execution_results_capacity,
        max_svm_workers: args.max_svm_workers,
        accountsdb_connection_url: args.accountsdb_connection_url,
        admin_keys,
        transaction_expiration_ms: args.transaction_expiration_ms,
        blocktime_ms: args.blocktime_ms,
        perf_sample_period_secs: args.perf_sample_period_secs,
        metrics,
    };

    let mut handles = run_node(config).await?;

    // Wait for either shutdown signal or any worker to quit
    tokio::select! {
        _ = shutdown_signal() => {
            info!("Received shutdown signal");
        }
        worker_name = handles.wait_for_any_worker_quit() => {
            error!("{} worker quit unexpectedly, shutting down node", worker_name);
            // Trigger shutdown of remaining workers
            handles.shutdown().await;
            return Err(format!("{} worker quit unexpectedly", worker_name).into());
        }
    }

    // Shutdown the node gracefully
    handles.shutdown().await;

    Ok(())
}

fn init_logging(log_level: &str, json_logs: bool) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

    if json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Initialize logging
    init_logging(&args.log_level, args.json_logs);

    info!(
        "Starting PrivateChannel node v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!("Mode: {:?}", args.mode);

    if let Err(e) = run_node_with_args(args).await {
        error!("Node failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
