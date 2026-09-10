//! Real-node guards for settlement retry and the discard record.
//!
//! A storage failure that clears must cost the node a stall, not a restart, and
//! `/health` must report the stall while it is happening. A failure that never
//! clears must name every executed transaction it drops rather than logging a
//! bare count.
//!
//! Commits are made to fail deterministically with a CHECK constraint on the
//! `blocks` table, the same fault-injection the batch-atomicity tests use.

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
    sqlx::postgres::PgPoolOptions,
    std::{sync::Arc, time::Duration},
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::time::sleep,
};

/// The Prometheus registry is process-global and both tests in this binary read
/// the same counters, so they must not run at the same time.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_db_name("settle_retry")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/settle_retry");
    (container, url)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

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

fn memo_tx(blockhash: solana_sdk::hash::Hash, nonce: u64) -> Transaction {
    let payer = Keypair::new();
    let memo = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: format!("retry:{nonce}").into_bytes(),
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

async fn http_status(url: &str) -> u16 {
    use {
        http_body_util::Empty,
        hyper::{body::Bytes, Request},
        hyper_util::{client::legacy::Client, rt::TokioExecutor},
    };
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::get(url).body(Empty::<Bytes>::new()).expect("req");
    match tokio::time::timeout(Duration::from_secs(5), client.request(req)).await {
        Ok(Ok(resp)) => resp.status().as_u16(),
        _ => 0,
    }
}

/// Block every commit from `slot` onward, so the settler cannot make progress.
async fn block_slots_from(db_url: &str, slot: i64) {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(db_url)
        .await
        .expect("fault-injection pool");
    // NOT VALID so blocks already committed are left alone; only new inserts
    // are checked, which is what makes this independent of how far the node got.
    sqlx::query(&format!(
        "ALTER TABLE blocks ADD CONSTRAINT test_stall CHECK (slot < {slot}) NOT VALID"
    ))
    .execute(&pool)
    .await
    .expect("add stall constraint");
}

async fn unblock_slots(db_url: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(db_url)
        .await
        .expect("fault-injection pool");
    sqlx::query("ALTER TABLE blocks DROP CONSTRAINT test_stall")
        .execute(&pool)
        .await
        .expect("drop stall constraint");
}

async fn current_slot(db_url: &str) -> i64 {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(db_url)
        .await
        .expect("pool");
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(slot) FROM blocks")
        .fetch_one(&pool)
        .await
        .expect("max slot")
        .unwrap_or(0)
}

async fn submit(client: &RpcClient, count: u64) -> Vec<Signature> {
    let blockhash = client.get_latest_blockhash().await.expect("blockhash");
    let mut accepted = Vec::new();
    for nonce in 0..count {
        if let Ok(sig) = client.send_transaction(&memo_tx(blockhash, nonce)).await {
            accepted.push(sig);
        }
    }
    accepted
}

/// A storage failure that clears inside the budget must leave the node running
/// and every accepted transaction queryable. While it is stalled `/health` must
/// stop reporting 200, because a node that cannot commit should be taken out of
/// rotation rather than handed more work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transient_storage_failure_stalls_the_node_but_does_not_kill_it() {
    let _guard = TEST_LOCK.lock().await;
    let (_pg, db_url) = start_postgres().await;
    let (handles, url) = start_node(load_config(db_url.clone(), free_port())).await;
    let client = RpcClient::new_with_commitment(url.clone(), CommitmentConfig::processed());

    let stall_from = current_slot(&db_url).await + 2;
    let accepted = submit(&client, 40).await;
    assert!(!accepted.is_empty(), "burst must be accepted");
    block_slots_from(&db_url, stall_from).await;

    // Past the stage progress margin, so a healthy reading here would mean the
    // stall is invisible to an orchestrator.
    sleep(Duration::from_secs(8)).await;
    let stalled_status = http_status(&format!("{url}/health")).await;
    assert_ne!(
        stalled_status, 200,
        "a settler that cannot commit must not report healthy"
    );

    unblock_slots(&db_url).await;

    // The node must recover on its own rather than needing a restart.
    let mut recovered = false;
    for _ in 0..60 {
        if http_status(&format!("{url}/health")).await == 200 {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert!(recovered, "node must recover once the failure clears");

    for sig in accepted.iter().take(5) {
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
        assert!(
            found,
            "transaction {sig} accepted before the stall was lost"
        );
    }

    handles.shutdown().await;
}

/// A failure that never clears must be recorded, not swallowed. The counter is
/// the operator's only signal that accepted work was dropped, and blocks
/// committed before the failure must still be intact afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrecoverable_failure_records_what_it_discards() {
    let _guard = TEST_LOCK.lock().await;
    let (_pg, db_url) = start_postgres().await;
    let (handles, url) = start_node(load_config(db_url.clone(), free_port())).await;
    let client = RpcClient::new_with_commitment(url.clone(), CommitmentConfig::processed());

    let good_slot = current_slot(&db_url).await;
    let discarded_before = metric_total("private_channel_discarded_executed_transactions_total");

    block_slots_from(&db_url, good_slot + 2).await;
    let accepted = submit(&client, 40).await;
    assert!(!accepted.is_empty(), "burst must be accepted");

    // Long enough for the budget to be spent and the settler to give up.
    sleep(Duration::from_secs(35)).await;

    let discarded =
        metric_total("private_channel_discarded_executed_transactions_total") - discarded_before;
    assert!(
        discarded > 0.0,
        "an unsettleable buffer must be recorded, not dropped silently"
    );

    // Blocks committed before the failure must be untouched by it.
    assert!(
        current_slot(&db_url).await >= good_slot,
        "already-committed blocks must survive a failed settlement"
    );

    unblock_slots(&db_url).await;
    handles.shutdown().await;
}
