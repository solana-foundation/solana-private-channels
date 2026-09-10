//! Real-node guard for the pre-SVM account size gate, driving a full in-process
//! node (`run_node`) against a Postgres testcontainer.
//!
//! A small transaction can name a large amount of account data through readonly
//! keys no instruction uses. The node must refuse it on the strength of the
//! stored sizes alone, without ever reading those accounts out of the store.

use {
    private_channel_core::{
        accounts::AccountsDB,
        nodes::node::{run_node, NodeConfig, NodeHandles, NodeMode},
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        account::AccountSharedData,
        commitment_config::CommitmentConfig,
        instruction::Instruction,
        message::Message,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    std::time::Duration,
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
    tokio::time::sleep,
};

/// The largest an account can be, so seven of them clear the 64 MiB the SVM
/// would load while the transaction naming them still fits in a packet.
const BIG_ACCOUNT_BYTES: usize = 10 * 1024 * 1024;
const BIG_ACCOUNT_COUNT: usize = 7;

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_db_name("oversized_node")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("pg host");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/oversized_node");
    (container, url)
}

fn load_config(db_url: String, port: u16) -> NodeConfig {
    NodeConfig {
        mode: NodeMode::Aio,
        port,
        accountsdb_connection_url: db_url,
        ..NodeConfig::default()
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

async fn start_node(config: NodeConfig) -> (NodeHandles, RpcClient) {
    let port = config.port;
    let handles = run_node(config).await.expect("run_node");
    let client = RpcClient::new_with_commitment(
        format!("http://127.0.0.1:{port}"),
        CommitmentConfig::processed(),
    );
    for _ in 0..50 {
        if client.get_latest_blockhash().await.is_ok() {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    (handles, client)
}

/// A memo tx, optionally carrying `extra` as readonly keys no instruction uses.
///
/// The keys go straight into the message rather than through the instruction,
/// because sanitization only checks header counts and index bounds while the
/// SVM loads and charges for every key it finds.
fn memo_tx(blockhash: solana_sdk::hash::Hash, nonce: u64, extra: &[Pubkey]) -> Transaction {
    let payer = Keypair::new();
    let memo = Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: format!("oversized:{nonce}").into_bytes(),
    };
    let mut msg = Message::new(&[memo], Some(&payer.pubkey()));
    msg.account_keys.extend_from_slice(extra);
    msg.header.num_readonly_unsigned_accounts += extra.len() as u8;
    Transaction::new(&[&payer], msg, blockhash)
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

async fn land_control_tx(client: &RpcClient, nonce: u64) -> Signature {
    let blockhash = client
        .get_latest_blockhash()
        .await
        .expect("node must serve a blockhash");
    let signature = client
        .send_transaction(&memo_tx(blockhash, nonce, &[]))
        .await
        .expect("a plain memo tx must be admitted");
    for _ in 0..100 {
        if client
            .get_signature_statuses(&[signature])
            .await
            .expect("status query must succeed")
            .value[0]
            .is_some()
        {
            return signature;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("control tx {signature} never landed");
}

/// 70 MiB of accounts named by one 1 KiB transaction must not be materialised,
/// and the node must keep executing afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_account_set_is_refused_without_being_loaded() {
    let (_pg, db_url) = start_postgres().await;

    // Seed the large accounts before the node starts, so they are real stored
    // rows the executor would otherwise fetch.
    let mut db = AccountsDB::new(&db_url, false)
        .await
        .expect("accounts db must open");
    let big: Vec<Pubkey> = {
        let mut keys = Vec::with_capacity(BIG_ACCOUNT_COUNT);
        for _ in 0..BIG_ACCOUNT_COUNT {
            let pubkey = Pubkey::new_unique();
            db.set_account(
                pubkey,
                AccountSharedData::new(1, BIG_ACCOUNT_BYTES, &solana_sdk_ids::system_program::ID),
            )
            .await;
            keys.push(pubkey);
        }
        keys
    };
    drop(db);

    let (handles, client) = start_node(load_config(db_url, free_port())).await;

    // Warm the node, then measure, so the baseline excludes start-up growth.
    land_control_tx(&client, 1).await;
    let rss_before = rss_kb();

    let blockhash = client
        .get_latest_blockhash()
        .await
        .expect("node must serve a blockhash");
    let oversized = memo_tx(blockhash, 2, &big);
    let oversized_sig = client
        .send_transaction(&oversized)
        .await
        .expect("admission is per instruction, so a memo tx is accepted");

    // The next control tx landing proves the executor survived the oversized one
    // and is still draining batches.
    land_control_tx(&client, 3).await;

    let status = client
        .get_signature_statuses(&[oversized_sig])
        .await
        .expect("status query must succeed")
        .value[0]
        .clone();
    assert!(
        status.is_none(),
        "an oversized tx is rejected before execution, so it never reaches the transactions table"
    );

    let growth_kb = rss_kb().saturating_sub(rss_before);
    assert!(
        growth_kb < 40 * 1024,
        "RSS grew {growth_kb} kB, so {BIG_ACCOUNT_COUNT} x 10 MiB of accounts were loaded after all"
    );

    handles.shutdown().await;
}
