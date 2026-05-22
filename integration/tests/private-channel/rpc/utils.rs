use {
    anyhow::Result,
    private_channel_core::nodes::node::{run_node, NodeConfig, NodeHandles},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::Transaction},
    solana_transaction_status::UiTransactionEncoding,
    std::{sync::Once, time::Duration},
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
