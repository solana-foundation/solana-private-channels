//! End-to-end check that a v1 transaction lands in the channel and reads back.
//!
//! A real node runs in-process against a Postgres testcontainer. Transactions go
//! in through `sendTransaction` exactly as a wallet would send them, and come
//! back through `getTransaction`, including after a restart against the same
//! database, which is what proves the stored row format end to end.

use solana_commitment_config::CommitmentConfig;
use {
    private_channel_core::{
        nodes::node::{run_node, NodeConfig, NodeHandles, NodeMode},
        stage_metrics::PrometheusMetrics,
    },
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig},
    solana_sdk::{
        hash::Hash,
        instruction::Instruction,
        message::{compiled_instruction::CompiledInstruction, v1, MessageHeader, VersionedMessage},
        signature::{Keypair, Signature, Signer},
        transaction::{Transaction, TransactionVersion, VersionedTransaction},
    },
    solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding},
    std::{sync::Arc, time::Duration},
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::time::sleep,
};

/// Largest transaction the legacy and v0 formats allow.
const LEGACY_TRANSACTION_CAP: usize = 1232;

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_db_name("v1_transaction")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/v1_transaction");
    (container, url)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn node_config(db_url: String, port: u16) -> NodeConfig {
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

async fn start_node(config: NodeConfig) -> (NodeHandles, RpcClient) {
    let port = config.port;
    let handles = run_node(config).await.expect("run_node");
    let client = RpcClient::new_with_commitment(
        format!("http://127.0.0.1:{port}"),
        CommitmentConfig::processed(),
    );
    for _ in 0..80 {
        if client.get_latest_blockhash().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    (handles, client)
}

fn legacy_memo(blockhash: Hash, memo: &str) -> Transaction {
    let payer = Keypair::new();
    let instruction = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: memo.as_bytes().to_vec(),
    };
    Transaction::new_signed_with_payer(&[instruction], Some(&payer.pubkey()), &[&payer], blockhash)
}

/// A signed v1 transaction carrying one memo, built the way a wallet builds it.
fn v1_memo(blockhash: Hash, memo: &str) -> VersionedTransaction {
    v1_memo_with(blockhash, memo, v1::TransactionConfig::empty())
}

/// The same memo, carrying the compute budget settings a v1 message allows.
fn v1_memo_with(
    blockhash: Hash,
    memo: &str,
    config: v1::TransactionConfig,
) -> VersionedTransaction {
    let payer = Keypair::new();
    let message = v1::Message::new(
        MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        },
        config,
        blockhash,
        vec![payer.pubkey(), spl_memo::id()],
        vec![CompiledInstruction {
            program_id_index: 1,
            accounts: vec![],
            data: memo.as_bytes().to_vec(),
        }],
    );
    VersionedTransaction::try_new(VersionedMessage::V1(message), &[&payer]).expect("sign v1")
}

fn read_config(max_version: Option<u8>) -> RpcTransactionConfig {
    RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::processed()),
        max_supported_transaction_version: max_version,
    }
}

/// Waits for a settled transaction to become readable with the v1 ceiling.
async fn read_back(
    client: &RpcClient,
    signature: &Signature,
) -> EncodedConfirmedTransactionWithStatusMeta {
    for _ in 0..100 {
        if let Ok(tx) = client
            .get_transaction_with_config(signature, read_config(Some(1)))
            .await
        {
            return tx;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("transaction {signature} never became readable");
}

/// The fields a caller relies on, read through the JSON the RPC returns.
fn compute_units(tx: &EncodedConfirmedTransactionWithStatusMeta) -> Option<u64> {
    let meta = serde_json::to_value(&tx.transaction.meta).expect("meta to json");
    meta.get("computeUnitsConsumed").and_then(|v| v.as_u64())
}

fn failed(tx: &EncodedConfirmedTransactionWithStatusMeta) -> bool {
    let meta = serde_json::to_value(&tx.transaction.meta).expect("meta to json");
    !meta.get("err").is_none_or(|err| err.is_null())
}

/// Both formats land and read back correctly, a v1 transaction larger than the
/// legacy cap is accepted, the version ceiling is honoured, and every row still
/// reads after a restart against the same database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_and_legacy_transactions_land_and_read_back_across_a_restart() {
    let (_pg, db_url) = start_postgres().await;
    let (handles, client) = start_node(node_config(db_url.clone(), free_port())).await;
    let blockhash = client.get_latest_blockhash().await.expect("blockhash");

    let legacy = client
        .send_transaction(&legacy_memo(blockhash, "legacy"))
        .await
        .expect("a legacy transaction is accepted");

    let v1 = client
        .send_transaction(&v1_memo(blockhash, "v1"))
        .await
        .expect("a v1 transaction is accepted");

    // Only v1 may exceed the legacy cap, so this proves the raised ceiling end to end.
    let large_memo = "x".repeat(2_000);
    let large_tx = v1_memo(blockhash, &large_memo);
    let wire_len = wincode::serialize(&large_tx).expect("wire bytes").len();
    assert!(
        wire_len > LEGACY_TRANSACTION_CAP,
        "test transaction must exceed the legacy cap"
    );
    let large_v1 = client
        .send_transaction(&large_tx)
        .await
        .expect("a v1 transaction above the legacy cap is accepted");

    let signatures = [legacy, v1, large_v1];
    for signature in &signatures {
        let tx = read_back(&client, signature).await;
        assert!(!failed(&tx), "{signature} must execute successfully");
        assert!(
            compute_units(&tx).is_some_and(|units| units > 0),
            "{signature} must report the compute units it used"
        );
    }
    assert_eq!(
        read_back(&client, &legacy).await.transaction.version,
        Some(TransactionVersion::LEGACY)
    );
    for signature in [&v1, &large_v1] {
        assert_eq!(
            read_back(&client, signature).await.transaction.version,
            Some(TransactionVersion::Number(1)),
            "{signature} must read back as v1"
        );
    }

    // A caller that did not opt in to v1 gets Agave's error, not a dropped connection.
    let refused = client
        .get_transaction_with_config(&v1, read_config(None))
        .await
        .expect_err("a v1 transaction must be refused without a version ceiling");
    assert!(
        refused.to_string().contains("-32015") || refused.to_string().contains("not supported"),
        "expected the unsupported version error, got: {refused}"
    );

    // Restart against the same database: every row must still decode.
    handles.shutdown().await;
    sleep(Duration::from_millis(300)).await;
    let (restarted, client) = start_node(node_config(db_url, free_port())).await;
    for signature in &signatures {
        let tx = read_back(&client, signature).await;
        assert!(
            compute_units(&tx).is_some_and(|units| units > 0),
            "{signature} must still report its compute units after a restart"
        );
    }
    assert_eq!(
        read_back(&client, &large_v1).await.transaction.version,
        Some(TransactionVersion::Number(1))
    );

    restarted.shutdown().await;
}

/// The channel runs every transaction under one gasless budget, so the compute
/// budget a v1 message carries must be ignored. Each setting below would make
/// the transaction fail if it were honoured, so landing it proves all of them are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_v1_transactions_own_compute_budget_is_ignored() {
    let (_pg, db_url) = start_postgres().await;
    let (handles, client) = start_node(node_config(db_url, free_port())).await;
    let blockhash = client.get_latest_blockhash().await.expect("blockhash");

    let hostile = v1::TransactionConfig {
        // Far below what a memo consumes, so honouring it would exhaust compute.
        compute_unit_limit: Some(1),
        // Smaller than any program account, so honouring it would refuse the load.
        loaded_accounts_data_size_limit: Some(1),
        // More than an unfunded fee payer holds, so charging it would fail.
        priority_fee: Some(1_000_000_000),
        heap_size: Some(256 * 1024),
    };
    let signature = client
        .send_transaction(&v1_memo_with(blockhash, "hostile budget", hostile))
        .await
        .expect("a v1 transaction with its own budget is accepted");

    let tx = read_back(&client, &signature).await;
    assert!(
        !failed(&tx),
        "a v1 transaction's budget settings must not affect execution"
    );
    assert!(
        compute_units(&tx).is_some_and(|units| units > 1),
        "the transaction must run past the 1 compute unit it asked for"
    );

    handles.shutdown().await;
}
