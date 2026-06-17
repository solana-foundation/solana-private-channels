//! Real-node guards for the bounded write pipeline, driving a full in-process
//! node (`run_node`) against a Postgres testcontainer.
//!
//! `health_ok_under_sustained_backpressure` and `no_deadlock_under_slow_settler`
//! run in CI: they assert deterministic structural properties (a saturated
//! pipeline stays healthy past the heartbeat margin; a slow settler never
//! deadlocks). `bounded_memory_under_burst` stays `#[ignore]` — its RSS-plateau
//! assertion is resource-sensitive and belongs in a staging load run.

use {
    private_channel_core::{
        nodes::node::{run_node, NodeConfig, NodeHandles, NodeMode},
        stage_metrics::PrometheusMetrics,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        instruction::Instruction,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::{sync::Arc, time::Duration},
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::time::{sleep, timeout},
};

// Small capacities so a modest burst saturates the pipeline quickly.
const INGRESS_CAP: usize = 64;
const SEQUENCER_CAP: usize = 64;
const RESULTS_CAP: usize = 64;

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_db_name("backpressure_node")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/backpressure_node");
    (container, url)
}

fn load_config(db_url: String, port: u16, prometheus: bool) -> NodeConfig {
    NodeConfig {
        mode: NodeMode::Aio,
        port,
        sigverify_queue_size: 64,
        sigverify_workers: 2,
        max_connections: 100,
        max_tx_per_batch: 16,
        batch_deadline_ms: 5,
        batch_channel_capacity: 8,
        ingress_queue_capacity: INGRESS_CAP,
        sequencer_queue_capacity: SEQUENCER_CAP,
        execution_results_capacity: RESULTS_CAP,
        max_svm_workers: 2,
        accountsdb_connection_url: db_url,
        admin_keys: vec![],
        transaction_expiration_ms: 15_000,
        blocktime_ms: 100,
        perf_sample_period_secs: 3600,
        metrics: if prometheus {
            Arc::new(PrometheusMetrics)
        } else {
            Arc::new(private_channel_core::stage_metrics::NoopMetrics)
        },
    }
}

// Grab a free port and release it so the node can bind it (standard repo pattern).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn start_node(config: NodeConfig) -> (NodeHandles, String) {
    let port = config.port;
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");
    let client = RpcClient::new(url.clone());
    // Wait until the settler has produced the first block (so getLatestBlockhash works).
    for _ in 0..50 {
        if client.get_latest_blockhash().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    (handles, url)
}

/// A unique, allowlisted, signature-valid memo tx against `blockhash`.
fn memo_tx(blockhash: solana_sdk::hash::Hash, nonce: u64) -> Transaction {
    let payer = Keypair::new();
    let memo = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: format!("backpressure:{nonce}").into_bytes(),
    };
    Transaction::new_signed_with_payer(&[memo], Some(&payer.pubkey()), &[&payer], blockhash)
}

fn rss_kb() -> u64 {
    // VmRSS from /proc/self/status is already in kB, so it's page-size agnostic. Linux-only.
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|kb| kb.parse().ok())
        .unwrap_or(0)
}

fn shed_total() -> f64 {
    private_channel_metrics::prometheus::gather()
        .into_iter()
        .filter(|mf| mf.name() == "private_channel_rpc_ingress_shed_total")
        .flat_map(|mf| mf.get_metric().to_vec())
        .map(|m| m.get_counter().value())
        .sum()
}

/// Under a continuous valid-tx burst the pipeline is backpressured but every
/// stage keeps progressing, so `/health` must stay 200 (no false restart).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_ok_under_sustained_backpressure() {
    let (_pg, db_url) = start_postgres().await;
    let (handles, url) = start_node(load_config(db_url, free_port(), false)).await;
    let client = RpcClient::new(url.clone());

    // Run past the 5s heartbeat margin so a stage that stopped progressing under
    // backpressure would actually surface as unhealthy.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        let bh = client.get_latest_blockhash().await.expect("blockhash");
        for n in 0..256u64 {
            let _ = client.send_transaction(&memo_tx(bh, n)).await; // shed errors are fine
        }
        let resp = http_get(&format!("{url}/health")).await;
        assert_eq!(resp, 200, "/health must stay 200 under backpressure");
    }
    handles.shutdown().await;
}

/// A deliberately slow settler (long blocktime) under burst must not deadlock —
/// the pipeline keeps making forward progress (blocks advance).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_deadlock_under_slow_settler() {
    let (_pg, db_url) = start_postgres().await;
    let mut config = load_config(db_url, free_port(), false);
    config.blocktime_ms = 2000; // slow settler stresses the executor→settler edge
    let (handles, url) = start_node(config).await;
    let client = RpcClient::new(url.clone());

    let slot_start = client.get_slot().await.unwrap_or(0);
    // ~10s spans several 2000ms blocks — enough to prove slots keep advancing.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let bh = match client.get_latest_blockhash().await {
            Ok(b) => b,
            Err(_) => continue,
        };
        for n in 0..256u64 {
            let _ = client.send_transaction(&memo_tx(bh, n)).await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let slot_end = client.get_slot().await.expect("slot still served");
    assert!(
        slot_end > slot_start,
        "settler must keep advancing slots under load (no deadlock): {slot_start} -> {slot_end}"
    );
    handles.shutdown().await;
}

/// The direct regression for the finding: a sustained valid-tx burst above drain
/// rate must leave process RSS bounded (it plateaus instead of climbing to OOM),
/// and the shed counter must climb once ingress saturates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test: needs Docker, run in staging"]
async fn bounded_memory_under_burst() {
    private_channel_core::stage_metrics::init_prometheus_metrics();
    let (_pg, db_url) = start_postgres().await;
    let (handles, url) = start_node(load_config(db_url, free_port(), true)).await;
    let client = RpcClient::new(url.clone());

    // Warm up so the steady-state working set is allocated before we sample.
    let warm_bh = client.get_latest_blockhash().await.expect("blockhash");
    for n in 0..1000u64 {
        let _ = client.send_transaction(&memo_tx(warm_bh, n)).await;
    }
    sleep(Duration::from_secs(2)).await;
    let baseline_rss = rss_kb();
    let shed_before = shed_total();

    // Sustained burst well above drain rate.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut n = 1000u64;
    while std::time::Instant::now() < deadline {
        let bh = client.get_latest_blockhash().await.unwrap_or(warm_bh);
        for _ in 0..512u64 {
            let _ = client.send_transaction(&memo_tx(bh, n)).await;
            n += 1;
        }
    }

    let peak_rss = rss_kb();
    // Ceiling: bounded queues mean RSS may grow modestly but must not balloon.
    // 4× the warmed baseline is a generous plateau bound that still catches an
    // unbounded leak (which would be orders of magnitude over baseline).
    assert!(
        peak_rss < baseline_rss.saturating_mul(4),
        "RSS must plateau under burst: baseline={baseline_rss}KiB peak={peak_rss}KiB"
    );
    assert!(
        shed_total() > shed_before,
        "rpc_ingress_shed_total must climb under sustained overload"
    );
    handles.shutdown().await;
}

/// Minimal HTTP GET returning the status code, using hyper (no reqwest dep).
async fn http_get(url: &str) -> u16 {
    use {
        http_body_util::Empty,
        hyper::{body::Bytes, Request},
        hyper_util::{client::legacy::Client, rt::TokioExecutor},
    };
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::get(url).body(Empty::<Bytes>::new()).expect("req");
    match timeout(Duration::from_secs(5), client.request(req)).await {
        Ok(Ok(resp)) => resp.status().as_u16(),
        _ => 0,
    }
}
