//! `run_node` config-validation guards in `core/src/nodes/node.rs`.
//!
//! The two guards at the top of `run_node` reject misconfigurations:
//!   - `blocktime_ms == 0` on a write-mode node
//!   - `max_blockhashes == 0` on a write-mode node
//!
//! Both are cheap to exercise by calling `run_node` with a deliberately
//! bad `NodeConfig`. No postgres, no redis — the validation fires
//! before any I/O.

use {
    private_channel_core::{
        nodes::node::{run_node, NodeConfig, NodeMode},
        stage_metrics::NoopMetrics,
    },
    solana_sdk::{signature::Keypair, signer::Signer},
    std::sync::Arc,
};

fn base_config(mode: NodeMode) -> NodeConfig {
    NodeConfig {
        mode,
        port: 0,
        sigverify_queue_size: 16,
        sigverify_workers: 1,
        max_connections: 10,
        max_tx_per_batch: 8,
        batch_deadline_ms: 5,
        batch_channel_capacity: 4,
        ingress_queue_capacity: private_channel_core::nodes::node::DEFAULT_INGRESS_QUEUE_CAPACITY,
        sequencer_queue_capacity:
            private_channel_core::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        execution_results_capacity:
            private_channel_core::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY,
        max_svm_workers: 1,
        accountsdb_connection_url: "postgres://unused/private_channel".to_string(),
        redis_cache_url: None,
        redis_block_ttl_secs: 3_600,
        admin_keys: vec![Keypair::new().pubkey()],
        max_blockhashes: 150,
        blocktime_ms: 100,
        perf_sample_period_secs: 60,
        metrics: Arc::new(NoopMetrics),
    }
}

/// `blocktime_ms = 0` on a write node trips the first validation guard.
/// The error must surface before any accountsdb / network I/O.
#[tokio::test(flavor = "multi_thread")]
async fn zero_blocktime_on_write_mode_fails_validation() {
    let mut config = base_config(NodeMode::Write);
    config.blocktime_ms = 0;

    let err = match run_node(config).await {
        Err(e) => e,
        Ok(_) => panic!("zero blocktime on Write mode must fail validation"),
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("blocktime_ms cannot be 0"),
        "error must name the blocktime guard: {msg}"
    );
}

/// A zero blockhash window trips the second validation guard: a read node would
/// advertise it as `lastValidBlockHeight` and a write node would size the dedup
/// cache with it.
#[tokio::test(flavor = "multi_thread")]
async fn zero_max_blockhashes_on_write_mode_fails_validation() {
    let mut config = base_config(NodeMode::Aio);
    config.max_blockhashes = 0;

    let err = match run_node(config).await {
        Err(e) => e,
        Ok(_) => panic!("zero max_blockhashes on Aio mode must fail validation"),
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("max_blockhashes must be greater than 0")
            && msg.contains("transaction_expiration_ms"),
        "error must name the violated parameter: {msg}"
    );
}
