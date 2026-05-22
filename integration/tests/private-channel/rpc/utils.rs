use {
    anyhow::Result,
    private_channel_core::{
        nodes::node::{run_node, NodeConfig, NodeHandles},
        stage_metrics::StageMetrics,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::Transaction},
    solana_transaction_status::UiTransactionEncoding,
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Once,
        },
        time::Duration,
    },
    tokio::time::sleep,
    tracing::warn,
};

// Ensure tracing is only initialized once across all tests
static INIT: Once = Once::new();

pub const MINT_DECIMALS: u8 = 3;
pub const SEND_AND_CHECK_DURATION_SECONDS: u64 = 1;
pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
pub const AIRDROP_LAMPORTS: u64 = LAMPORTS_PER_SOL;

pub fn init_tracing() {
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
            .add_directive("solana=off".parse().unwrap())
            .add_directive("agave=off".parse().unwrap());

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .init();
    });
}

pub async fn start_private_channel(config: NodeConfig) -> Result<(NodeHandles, String)> {
    let port = config.port;
    let node_handles = run_node(config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start node: {}", e))?;

    let url = format!("http://127.0.0.1:{}", port);
    // Poll until the node has produced its first block (blockhash in DB).
    // get_latest_blockhash requires at least one committed block, so success
    // here means the full pipeline (RPC + settler + DB) is ready for testing.
    // With blocktime_ms = 100 ms this typically takes 200–400 ms.
    let client = RpcClient::new(url.clone());
    for _ in 0..50 {
        if client.get_latest_blockhash().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    println!("\n=== Node Started ===");
    println!("Node endpoint: {}", url);

    Ok((node_handles, url))
}

pub async fn confirm_transaction(client: &RpcClient, sig: Signature) {
    for _ in 0..30 {
        match client
            .get_transaction(&sig, UiTransactionEncoding::Base64)
            .await
        {
            Ok(_) => return,
            Err(e) => warn!("Error getting transaction: {}", e),
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Transaction {} not confirmed within 3 seconds", sig);
}

pub async fn send_and_confirm(client: &RpcClient, tx: &Transaction) {
    let sig = client.send_transaction(tx).await.unwrap();
    confirm_transaction(client, sig).await;
}

pub async fn token_balance(client: &RpcClient, token_account: &Pubkey) -> Result<u64> {
    let balance = client
        .get_token_account_balance(token_account)
        .await
        .map_err(anyhow::Error::from)?;
    balance.amount.parse::<u64>().map_err(anyhow::Error::from)
}

pub async fn restart_private_channel(
    handles: NodeHandles,
    config: NodeConfig,
) -> Result<(NodeHandles, String)> {
    println!("\n=== Restarting Node ===");
    handles.shutdown().await;
    // Brief pause to allow the OS to release the port
    sleep(Duration::from_millis(200)).await;
    start_private_channel(config).await
}

// Test-double `StageMetrics` that tracks only the counters individual tests
// need to assert on. Add new fields here as more tests need coverage; keep
// the unused methods as empty bodies so adding the field is a one-line change.
#[derive(Default)]
pub struct CountingMetrics {
    pub executor_dropped_expired: AtomicUsize,
    pub dedup_dropped_unknown_blockhash: AtomicUsize,
}

impl CountingMetrics {
    pub fn executor_dropped_expired(&self) -> usize {
        self.executor_dropped_expired.load(Ordering::Relaxed)
    }
    pub fn dedup_dropped_unknown_blockhash(&self) -> usize {
        self.dedup_dropped_unknown_blockhash.load(Ordering::Relaxed)
    }
}

impl StageMetrics for CountingMetrics {
    fn executor_dropped_expired_blockhash(&self, count: usize) {
        self.executor_dropped_expired
            .fetch_add(count, Ordering::Relaxed);
    }
    fn dedup_dropped_unknown_blockhash(&self) {
        self.dedup_dropped_unknown_blockhash
            .fetch_add(1, Ordering::Relaxed);
    }
    fn dedup_received(&self) {}
    fn dedup_forwarded(&self) {}
    fn dedup_dropped_duplicate(&self) {}
    fn sigverify_forwarded(&self) {}
    fn sigverify_rejected(&self, _: &'static str) {}
    fn sequencer_collected(&self, _: usize) {}
    fn sequencer_transactions_emitted(&self, _: usize) {}
    fn executor_results_sent(&self, _: usize) {}
    fn executor_results_send_failed(&self, _: &'static str) {}
    fn executor_missing_results(&self, _: &'static str) {}
    fn executor_batch_duration_ms(&self, _: f64) {}
    fn executor_preload_duration_ms(&self, _: f64) {}
    fn executor_svm_duration_ms(&self, _: &'static str, _: f64) {}
    fn executor_bob_update_duration_ms(&self, _: &'static str, _: f64) {}
    fn settler_txs_settled(&self, _: usize) {}
    fn settler_settle_duration_ms(&self, _: f64) {}
    fn settler_db_write_duration_ms(&self, _: f64) {}
    fn settler_processing_duration_ms(&self, _: f64) {}
    fn address_signatures_queue_depth(&self, _: usize) {}
    fn address_signatures_send_blocked_ms(&self, _: f64) {}
    fn address_signatures_flush_duration_ms(&self, _: f64) {}
    fn address_signatures_rows_flushed(&self, _: usize) {}
    fn address_signatures_flush_errors_total(&self) {}
}
