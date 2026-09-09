#![allow(dead_code)]

//! Shared helper that stands up a real PrivateChannel core node (SVM) with its
//! own Postgres accountsdb, for operator-lifecycle tests that need the deposit
//! operator to mint against an instant-finality target.
//!
//! In production a deposit mint lands on the PrivateChannel core node, which
//! reports every found transaction as `finalized` immediately. A
//! `solana-test-validator` finalizes slowly (~37s/tx), so pointing the mint
//! operator at a real PC node here makes the `finalized` mint gate complete on
//! the first poll, exactly as in production.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use private_channel_core::nodes::node::{run_node, NodeConfig, NodeHandles, NodeMode};
use private_channel_core::stage_metrics::NoopMetrics;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

/// A running PrivateChannel core node plus the Postgres container backing its
/// accountsdb. Keep this alive for the duration of the test; drop or call
/// [`PrivateChannelNode::shutdown`] to tear it down.
pub struct PrivateChannelNode {
    pub url: String,
    handles: Option<NodeHandles>,
    _db: ContainerAsync<Postgres>,
}

impl PrivateChannelNode {
    pub async fn shutdown(mut self) {
        if let Some(handles) = self.handles.take() {
            handles.shutdown().await;
        }
    }
}

fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to port 0");
    let port = listener
        .local_addr()
        .expect("Failed to get local address")
        .port();
    drop(listener);
    port
}

/// Start a PrivateChannel core node whose admin/mint authority is `admin_pubkey`
/// (the operator key). The node JIT-initializes mints and creates recipient
/// ATAs when the operator's `MintTo` arrives, so no explicit mint setup is
/// needed here.
pub async fn start_private_channel_node(
    admin_pubkey: Pubkey,
) -> Result<PrivateChannelNode, Box<dyn std::error::Error>> {
    let db_container = Postgres::default()
        .with_db_name("private_channel_node")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let host = db_container.get_host().await?;
    let port = db_container.get_host_port_ipv4(5432).await?;
    let accountsdb_connection_url = format!(
        "postgres://postgres:password@{}:{}/private_channel_node",
        host, port
    );

    let node_port = get_free_port();
    let node_config = NodeConfig {
        mode: NodeMode::Aio,
        port: node_port,
        sigverify_queue_size: 100,
        sigverify_workers: 2,
        max_connections: 50,
        max_tx_per_batch: 32,
        batch_deadline_ms: 50,
        batch_channel_capacity: 16,
        ingress_queue_capacity: private_channel_core::nodes::node::DEFAULT_INGRESS_QUEUE_CAPACITY,
        sequencer_queue_capacity:
            private_channel_core::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        execution_results_capacity:
            private_channel_core::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY,
        max_svm_workers: 4,
        accountsdb_connection_url,
        redis_cache_url: None,
        redis_block_ttl_secs: 3_600,
        admin_keys: vec![admin_pubkey],
        max_blockhashes: 150,
        blocktime_ms: 100,
        perf_sample_period_secs: 10,
        metrics: Arc::new(NoopMetrics),
    };

    let handles = run_node(node_config)
        .await
        .map_err(|e| format!("Failed to start PrivateChannel node: {}", e))?;

    let url = format!("http://127.0.0.1:{}", node_port);

    // Poll until the node has produced its first block (get_latest_blockhash
    // succeeds), meaning the RPC + settler + DB pipeline is ready.
    let client = RpcClient::new(url.clone());
    for _ in 0..50 {
        if client.get_latest_blockhash().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("=== PrivateChannel node started at {} ===", url);

    Ok(PrivateChannelNode {
        url,
        handles: Some(handles),
        _db: db_container,
    })
}
