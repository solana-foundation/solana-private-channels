//! End-to-end guards for the ordered pipeline drain.
//!
//! A shutdown drains the stages in order instead of stopping them all at once,
//! so no transaction that the RPC already acknowledged is discarded. These tests
//! drive a real in-process node through `run_node` against a Postgres
//! testcontainer, take signatures only from successful `sendTransaction` calls,
//! and check every one of them is queryable after a restart against the same
//! database.

use {
    private_channel_core::{
        nodes::node::{run_node, NodeConfig, NodeHandles, NodeMode},
        stage_metrics::PrometheusMetrics,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        instruction::Instruction,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    solana_transaction_status::UiTransactionEncoding,
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::time::sleep,
};

/// The Prometheus registry is process-global and both tests in this binary read
/// the same counters, so they must not run at the same time.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Docker's default SIGTERM-to-SIGKILL window, which no compose file overrides.
/// The drain has to finish inside it or the process is killed part-way through.
const CONTAINER_STOP_GRACE: Duration = Duration::from_secs(10);

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_db_name("ordered_shutdown")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/ordered_shutdown");
    (container, url)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Small queues so a modest burst leaves work in every stage at shutdown.
fn load_config(db_url: String, port: u16) -> NodeConfig {
    NodeConfig {
        mode: NodeMode::Aio,
        port,
        sigverify_queue_size: 64,
        sigverify_workers: 2,
        max_connections: 100,
        max_tx_per_batch: 8,
        batch_deadline_ms: 5,
        batch_channel_capacity: 8,
        ingress_queue_capacity: 512,
        sequencer_queue_capacity: 128,
        execution_results_capacity: 64,
        max_svm_workers: 2,
        accountsdb_connection_url: db_url,
        redis_cache_url: None,
        admin_keys: vec![],
        max_blockhashes: 150,
        redis_block_ttl_secs: 3_600,
        blocktime_ms: 100,
        perf_sample_period_secs: 3600,
        metrics: Arc::new(PrometheusMetrics),
    }
}

async fn start_node(config: NodeConfig) -> (NodeHandles, String) {
    let port = config.port;
    let handles = run_node(config).await.expect("run_node");
    let url = format!("http://127.0.0.1:{port}");
    let client = RpcClient::new(url.clone());
    for _ in 0..80 {
        if client.get_latest_blockhash().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    (handles, url)
}

/// A unique, allowlisted, signature-valid transaction.
fn memo_tx(blockhash: solana_sdk::hash::Hash, nonce: u64) -> Transaction {
    let payer = Keypair::new();
    let memo = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: format!("drain:{nonce}").into_bytes(),
    };
    Transaction::new_signed_with_payer(&[memo], Some(&payer.pubkey()), &[&payer], blockhash)
}

fn metric_total(name: &str) -> f64 {
    private_channel_metrics::prometheus::gather()
        .into_iter()
        .filter(|mf| mf.name() == name)
        .flat_map(|mf| mf.get_metric().to_vec())
        .map(|m| m.get_counter().value())
        .sum()
}

/// Signatures the node accepted. A shed or an error is not an acceptance, so
/// only successful calls are recorded and only those carry a promise.
///
/// Submitters run concurrently and keep running across the shutdown. Sequential
/// submission would let each transaction settle before the next was sent, so the
/// pipeline would be empty when the drain started and the test would pass
/// without ever exercising it.
struct Loader {
    accepted: Arc<Mutex<Vec<Signature>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Loader {
    async fn spawn(url: &str, workers: u64) -> Self {
        let blockhash = RpcClient::new(url.to_string())
            .get_latest_blockhash()
            .await
            .expect("blockhash before burst");
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for worker in 0..workers {
            let client = RpcClient::new(url.to_string());
            let sink = Arc::clone(&accepted);
            tasks.push(tokio::spawn(async move {
                for nonce in 0..20_000u64 {
                    // Only an Ok is a promise to the client; a shed or a
                    // post-shutdown refusal is not, and must not be recorded.
                    if let Ok(sig) = client
                        .send_transaction(&memo_tx(blockhash, worker * 1_000_000 + nonce))
                        .await
                    {
                        sink.lock().expect("accepted lock").push(sig);
                    }
                }
            }));
        }
        Self { accepted, tasks }
    }

    fn accepted_so_far(&self) -> usize {
        self.accepted.lock().expect("accepted lock").len()
    }

    /// Stops the submitters and returns everything the node acknowledged.
    fn finish(self) -> Vec<Signature> {
        for t in &self.tasks {
            t.abort();
        }
        Arc::try_unwrap(self.accepted)
            .unwrap_or_else(|arc| Mutex::new(arc.lock().expect("accepted lock").clone()))
            .into_inner()
            .expect("accepted lock")
    }
}

/// Transactions admitted but not yet settled. A drain test is only meaningful
/// when this is non-zero at the moment shutdown is called.
///
/// The counters are process-global and cumulative across every node this binary
/// starts, so this is only meaningful as a delta against a baseline taken while
/// the node under test was still idle.
fn in_flight() -> f64 {
    metric_total("private_channel_dedup_received_total")
        - metric_total("private_channel_settler_txs_settled_total")
}

/// Slots must be contiguous: a gap would mean a block was skipped rather than
/// drained, which the ordered shutdown is supposed to make impossible.
async fn assert_chain_contiguous(client: &RpcClient) {
    let tip = client.get_slot().await.expect("slot after restart");
    let blocks = client
        .get_blocks(0, Some(tip))
        .await
        .expect("blocks after restart");
    assert!(!blocks.is_empty(), "restarted node has no blocks");
    for pair in blocks.windows(2) {
        assert_eq!(
            pair[1],
            pair[0] + 1,
            "slot gap between {} and {} after restart",
            pair[0],
            pair[1]
        );
    }
}

/// Every accepted signature must be queryable, allowing time for the restarted
/// node to come up rather than assuming it is instantly ready.
async fn assert_all_queryable(client: &RpcClient, accepted: &[Signature]) {
    let mut missing = Vec::new();
    for sig in accepted {
        let mut found = false;
        for _ in 0..40 {
            if client
                .get_transaction(sig, UiTransactionEncoding::Base64)
                .await
                .is_ok()
            {
                found = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        if !found {
            missing.push(*sig);
        }
    }
    // Only on failure, and worth the lines: loss is always a stage dropping work,
    // and the counters say which one without another twenty-minute run.
    if !missing.is_empty() {
        println!(
            "stage totals: sigverify_fwd={} sigverify_rej={} dedup_recv={} dedup_fwd={} \
             dedup_dup={} dedup_unknown_bh={} seq_emitted={} exec_sent={} exec_expired={} \
             exec_send_failed={} settled={} discarded={}",
            metric_total("private_channel_sigverify_forwarded_total"),
            metric_total("private_channel_sigverify_rejected_total"),
            metric_total("private_channel_dedup_received_total"),
            metric_total("private_channel_dedup_forwarded_total"),
            metric_total("private_channel_dedup_dropped_duplicate_total"),
            metric_total("private_channel_dedup_dropped_unknown_bh_total"),
            metric_total("private_channel_sequencer_transactions_emitted_total"),
            metric_total("private_channel_executor_results_sent_total"),
            metric_total("private_channel_executor_dropped_expired_bh_total"),
            metric_total("private_channel_executor_results_send_failed_total"),
            metric_total("private_channel_settler_txs_settled_total"),
            metric_total("private_channel_discarded_executed_transactions_total"),
        );
    }
    assert!(
        missing.is_empty(),
        "{} of {} accepted transactions were lost across shutdown: {:?}",
        missing.len(),
        accepted.len(),
        missing.iter().take(5).collect::<Vec<_>>()
    );
}

/// The drain end to end. Shutdown fires mid-burst with work in every stage, and
/// after a restart against the same database every signature the node handed out
/// must still resolve. This fails on any of the drop paths the ordered drain
/// replaced: the sequencer's non-blocking flush, the executor abandoning a send,
/// the settler exiting before its buffer was committed, and the address-index
/// writer exiting before the settler finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_transactions_survive_an_ordered_shutdown() {
    let _guard = TEST_LOCK.lock().await;
    let (_pg, db_url) = start_postgres().await;
    let (handles, url) = start_node(load_config(db_url.clone(), free_port())).await;
    let client = RpcClient::new_with_commitment(url.clone(), CommitmentConfig::processed());

    let discarded_before = metric_total("private_channel_discarded_executed_transactions_total");
    let in_flight_before = in_flight();

    let loader = Loader::spawn(&url, 4).await;
    sleep(Duration::from_secs(2)).await;

    // Guards against a vacuous pass: if the pipeline had already settled
    // everything, the drain would never be exercised and this test would prove
    // nothing. Sampled immediately before the shutdown it is about to survive.
    let carried = in_flight() - in_flight_before;
    assert!(
        carried > 0.0,
        "shutdown landed on an empty pipeline, so the drain was never exercised"
    );
    let accepted_at_shutdown = loader.accepted_so_far();

    handles.shutdown().await;

    // Only that it is refused, not how. The accept loop has already exited, so a
    // client opening a fresh connection is turned away at the transport and
    // never reaches admission. Which error admission itself returns is pinned by
    // the send_transaction_impl unit tests instead.
    let err = client
        .send_transaction(&memo_tx(solana_sdk::hash::Hash::new_unique(), 9_999))
        .await
        .expect_err("a shut-down node must not acknowledge new transactions");
    println!("post-shutdown submission refused with: {err}");

    let accepted = loader.finish();
    assert!(
        accepted.len() >= 50,
        "test needs a real burst, only {} were accepted",
        accepted.len()
    );
    println!(
        "accepted {} ({} before shutdown), {carried} in flight at shutdown",
        accepted.len(),
        accepted_at_shutdown
    );

    let discarded =
        metric_total("private_channel_discarded_executed_transactions_total") - discarded_before;
    assert_eq!(
        discarded, 0.0,
        "a clean shutdown must not discard executed transactions"
    );

    // Restart against the same database and confirm nothing was lost.
    sleep(Duration::from_millis(300)).await;
    let (restarted, url) = start_node(load_config(db_url, free_port())).await;
    let client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());
    assert_all_queryable(&client, &accepted).await;
    assert_chain_contiguous(&client).await;

    restarted.shutdown().await;
}

/// The drain must also finish when the pipeline is saturated, which is the load
/// the deadline is sized against. Overrunning it in production means the
/// container kills the process mid-drain and loses what the ordering preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saturated_pipeline_still_drains_within_the_deadline() {
    let _guard = TEST_LOCK.lock().await;
    let (_pg, db_url) = start_postgres().await;
    let (handles, url) = start_node(load_config(db_url.clone(), free_port())).await;

    let discarded_before = metric_total("private_channel_discarded_executed_transactions_total");
    let in_flight_before = in_flight();

    // More submitters than the first test, to hold every queue at capacity.
    let loader = Loader::spawn(&url, 8).await;
    sleep(Duration::from_secs(3)).await;

    let carried = in_flight() - in_flight_before;
    assert!(
        carried > 0.0,
        "pipeline was not saturated, so the deadline was never tested"
    );

    let started = std::time::Instant::now();
    handles.shutdown().await;
    let elapsed = started.elapsed();
    let accepted = loader.finish();
    println!(
        "saturated drain completed in {elapsed:?} with {carried} in flight, {} accepted",
        accepted.len()
    );

    // The container stop grace period is the real constraint: overrunning it
    // means the process is killed mid-drain and loses what ordering preserved.
    assert!(
        elapsed < CONTAINER_STOP_GRACE,
        "saturated drain took {elapsed:?}, which overruns the {CONTAINER_STOP_GRACE:?} stop grace"
    );

    let discarded =
        metric_total("private_channel_discarded_executed_transactions_total") - discarded_before;
    assert_eq!(
        discarded, 0.0,
        "a saturated shutdown must not discard executed transactions either"
    );

    // Loss under saturation is the failure that matters, not just slowness.
    sleep(Duration::from_millis(300)).await;
    let (restarted, url) = start_node(load_config(db_url, free_port())).await;
    let client = RpcClient::new_with_commitment(url, CommitmentConfig::processed());
    assert_all_queryable(&client, &accepted).await;
    assert_chain_contiguous(&client).await;

    restarted.shutdown().await;
}
