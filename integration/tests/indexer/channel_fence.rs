//! Channel fence: a channel restored behind the indexer DB re-produces slots with new
//! hashes, so the withdraw indexer must refuse to resume (SOLA13-99) or stop if it runs,
//! and the escrow operator, which reads the same fence, must refuse too (SOLA13-164).

#[path = "helpers/private_channel_node.rs"]
mod private_channel_node;

use mockito::{Matcher, Server as MockitoServer, ServerGuard};
use private_channel_indexer::{
    config::{BackfillConfig, ReconciliationConfig, RpcPollingConfig},
    error::{IndexerError, OperatorError},
    indexer::run,
    storage::{common::models::ChannelFence, PostgresDb, Storage},
    DatasourceType, IndexerConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType,
    StorageType,
};
use serde_json::json;
use solana_commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use sqlx::PgPool;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

const CHECKPOINT: u64 = 100;
const FENCE_HASH: &str = "FenceHash100";

async fn start_postgres(
    db: &str,
) -> (
    testcontainers::ContainerAsync<Postgres>,
    PgPool,
    PostgresConfig,
) {
    let pg = Postgres::default()
        .with_db_name(db)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container");
    let host = pg.get_host().await.expect("pg host");
    let port = pg.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgres://postgres:password@{host}:{port}/{db}");
    let pool = PgPool::connect(&url).await.expect("pg pool");
    (
        pg,
        pool,
        PostgresConfig {
            database_url: url,
            max_connections: 10,
        },
    )
}

/// A withdraw indexer that indexed through `CHECKPOINT` and recorded its block as the fence.
async fn seed_fenced_checkpoint(postgres: &PostgresConfig) {
    let storage = Storage::Postgres(PostgresDb::new(postgres).await.expect("storage"));
    storage.init_schema().await.expect("schema");
    storage
        .update_committed_checkpoint_with_fence(
            "withdraw",
            CHECKPOINT,
            Some(&ChannelFence {
                slot: CHECKPOINT,
                blockhash: FENCE_HASH.to_string(),
            }),
        )
        .await
        .expect("seed checkpoint");
}

fn configs(
    postgres: PostgresConfig,
    rpc_url: String,
) -> (PrivateChannelIndexerConfig, IndexerConfig) {
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.clone(),
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres,
        escrow_instance_id: None,
    };
    let indexer = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: Some(RpcPollingConfig {
            from_slot: None,
            poll_interval_ms: 50,
            error_retry_interval_ms: 50,
            batch_size: 10,
            encoding: UiTransactionEncoding::Json,
            commitment: CommitmentLevel::Finalized,
        }),
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: false,
            exit_after_backfill: false,
            rpc_url,
            batch_size: 10,
            max_gap_slots: 1_000,
            start_slot: None,
        },
        reconciliation: ReconciliationConfig::default(),
    };
    (common, indexer)
}

async fn mock_slot(rpc: &mut ServerGuard, slot: u64) -> mockito::Mock {
    rpc.mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getSlot"})))
        .with_body(json!({"jsonrpc": "2.0", "result": slot, "id": 1}).to_string())
        .create_async()
        .await
}

async fn mock_block(
    rpc: &mut ServerGuard,
    slot: u64,
    hash: &str,
    parent: u64,
    prev: &str,
) -> mockito::Mock {
    rpc.mock("POST", "/")
        .match_body(Matcher::PartialJson(
            json!({"method": "getBlock", "params": [slot]}),
        ))
        .with_body(
            json!({"jsonrpc": "2.0", "id": 1, "result": {
                "blockhash": hash, "previousBlockhash": prev, "parentSlot": parent,
                "transactions": [], "blockHeight": slot, "blockTime": null
            }})
            .to_string(),
        )
        .create_async()
        .await
}

async fn checkpoint_of(pool: &PgPool) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT last_committed_slot FROM indexer_state WHERE program_type = 'withdraw'",
    )
    .fetch_optional(pool)
    .await
    .unwrap()
    .flatten()
}

/// The Apex 99 shape: the channel was restored to before slot 100 and has since produced
/// past it again. The fence block now has another hash, so the indexer refuses to start
/// and never polls a slot above the checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_pitr_below_fence_refuses_boot() {
    let (_pg, pool, postgres) = start_postgres("fence_refuse").await;
    seed_fenced_checkpoint(&postgres).await;

    let mut rpc = MockitoServer::new_async().await;
    // Restored tip below the fence first: a node catching up is waited for.
    let behind = mock_slot(&mut rpc, 90).await;
    let _fence_block = mock_block(&mut rpc, CHECKPOINT, "ReproducedHash100", 99, "x").await;
    let above = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getBlocks"})))
        .expect(0)
        .create_async()
        .await;

    let (common, indexer) = configs(postgres, rpc.url());
    let handle = tokio::spawn(run(common, indexer, None));
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        !handle.is_finished(),
        "a tip below the fence must be waited for"
    );
    behind.remove_async().await;
    let _caught_up = mock_slot(&mut rpc, 105).await;

    let err = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("boot must not hang")
        .unwrap()
        .expect_err("a re-produced fence block must refuse boot");
    assert!(matches!(err, IndexerError::ChannelFence { .. }), "{err}");
    above.assert_async().await;
    assert_eq!(checkpoint_of(&pool).await, Some(CHECKPOINT as i64));
}

/// The channel is restored while the indexer keeps running. The first new block does not
/// link to the fence, so the indexer stops before it writes anything for that block.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_pitr_while_running_exits_on_broken_link() {
    let (_pg, pool, postgres) = start_postgres("fence_link").await;
    seed_fenced_checkpoint(&postgres).await;

    let mut rpc = MockitoServer::new_async().await;
    let _slot = mock_slot(&mut rpc, 105).await;
    // Boot check passes: the fence block is still there.
    let _fence_block = mock_block(&mut rpc, CHECKPOINT, FENCE_HASH, 99, "x").await;
    let _listing = rpc
        .mock("POST", "/")
        .match_body(Matcher::PartialJson(json!({"method": "getBlocks"})))
        .with_body(json!({"jsonrpc": "2.0", "result": [101], "id": 1}).to_string())
        .create_async()
        .await;
    // Then the channel is rewound under it: block 101 names a different parent hash.
    let _forked = mock_block(&mut rpc, 101, "NewHash101", CHECKPOINT, "OtherHash100").await;

    let (common, indexer) = configs(postgres, rpc.url());
    let err = tokio::time::timeout(Duration::from_secs(30), run(common, indexer, None))
        .await
        .expect("the indexer must stop on a broken link")
        .expect_err("a broken link is fatal");
    assert!(matches!(err, IndexerError::ChannelFence { .. }), "{err}");
    assert_eq!(checkpoint_of(&pool).await, Some(CHECKPOINT as i64));
}

/// Apex 164: the channel was restored behind the indexer DB, so completed deposits lost
/// their credit. The escrow operator must refuse to start rather than keep minting there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn escrow_operator_refuses_on_fence_mismatch() {
    let (_pg, _pool, postgres) = start_postgres("fence_escrow_operator").await;
    seed_fenced_checkpoint(&postgres).await;

    let mut rpc = MockitoServer::new_async().await;
    let _slot = mock_slot(&mut rpc, 105).await;
    let _fence_block = mock_block(&mut rpc, CHECKPOINT, "ReproducedHash100", 99, "x").await;

    let storage = std::sync::Arc::new(Storage::Postgres(
        PostgresDb::new(&postgres).await.expect("storage"),
    ));
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: rpc.url(),
        source_rpc_url: Some(rpc.url()),
        fallback_rpc_url: None,
        postgres,
        escrow_instance_id: Some(solana_sdk::pubkey::Pubkey::new_unique()),
    };
    let config = test_utils::operator_helper::default_operator_config();
    // Without the fence the operator would go on to sign mints, so give it a signer.
    let key = bs58::encode(solana_sdk::signature::Keypair::new().to_bytes()).into_string();
    std::env::set_var("ADMIN_SIGNER", "memory");
    std::env::set_var("ADMIN_PRIVATE_KEY", &key);
    let err = tokio::time::timeout(
        Duration::from_secs(30),
        private_channel_indexer::operator::run(storage, common, config, None),
    )
    .await
    .expect("boot must not hang")
    .expect_err("a re-produced fence block must refuse the escrow operator");
    assert!(matches!(err, OperatorError::ChannelFence { .. }), "{err}");
}

/// `None` also while the indexer has not created its schema yet.
async fn fence_slot_of(db_url: &str) -> Option<i64> {
    let pool = PgPool::connect(db_url).await.expect("pg pool");
    sqlx::query_scalar("SELECT fence_slot FROM indexer_state WHERE program_type = 'withdraw'")
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten()
        .flatten()
}

/// One memo transaction, so the channel produces a block with a hash.
async fn channel_block(url: &str) {
    let client = solana_client::nonblocking::rpc_client::RpcClient::new(url.to_string());
    let payer = solana_sdk::signature::Keypair::new();
    let memo = solana_sdk::instruction::Instruction {
        program_id: spl_memo::id(),
        accounts: vec![],
        data: solana_sdk::signature::Signature::new_unique()
            .to_string()
            .into_bytes(),
    };
    let tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[memo],
        Some(&solana_sdk::signer::Signer::pubkey(&payer)),
        &[&payer],
        client.get_latest_blockhash().await.expect("blockhash"),
    );
    client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("memo lands");
}

async fn wait_for_fence_above(db_url: &str, slot: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while fence_slot_of(db_url).await.is_none_or(|f| f <= slot) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the fence never passed slot {slot}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// End to end on a real core node: the channel primary is restored to a backup older
/// than the indexer DB. The withdraw indexer and the escrow operator both refuse to
/// start. Restoring the indexer DB to a backup older than the primary's lets it run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_restore_refuses_everything() {
    let admin = solana_sdk::signature::Keypair::new();
    let mut node = private_channel_node::start_private_channel_node(
        solana_sdk::signer::Signer::pubkey(&admin),
    )
    .await
    .expect("core node");
    let (pg, _pool, postgres) = start_postgres("e2e_primary_restore").await;
    let db_url = postgres.database_url.clone();
    channel_block(&node.url).await;

    let (common, indexer) = configs(postgres.clone(), node.url.clone());
    let first = tokio::spawn(run(common, indexer, None));
    wait_for_fence_above(&db_url, 0).await;

    // The indexer backup is taken first, so its target is earlier than the primary's.
    let indexer_backup = private_channel_node::container_pg_dump(&pg, "e2e_primary_restore");
    let channel_backup = node.dump();
    let client = solana_client::nonblocking::rpc_client::RpcClient::new(node.url.clone());
    let tip_at_backup = client.get_slot().await.expect("tip") as i64;

    // The channel moves on and the indexer's fence follows it past the backup.
    channel_block(&node.url).await;
    wait_for_fence_above(&db_url, tip_at_backup).await;
    first.abort();
    let _ = first.await;

    node.restore_and_restart(&channel_backup)
        .await
        .expect("restored node");

    let (common, indexer) = configs(postgres.clone(), node.url.clone());
    let err = tokio::time::timeout(Duration::from_secs(120), run(common, indexer, None))
        .await
        .expect("the withdraw indexer must not wait forever")
        .expect_err("a restored primary must refuse the withdraw indexer");
    assert!(matches!(err, IndexerError::ChannelFence { .. }), "{err}");

    let storage = std::sync::Arc::new(Storage::Postgres(
        PostgresDb::new(&postgres).await.expect("storage"),
    ));
    let escrow = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: node.url.clone(),
        source_rpc_url: Some(node.url.clone()),
        fallback_rpc_url: None,
        postgres: postgres.clone(),
        escrow_instance_id: Some(solana_sdk::pubkey::Pubkey::new_unique()),
    };
    let err = tokio::time::timeout(
        Duration::from_secs(120),
        private_channel_indexer::operator::run(
            storage,
            escrow,
            test_utils::operator_helper::default_operator_config(),
            None,
        ),
    )
    .await
    .expect("the escrow operator must not wait forever")
    .expect_err("a restored primary must refuse the escrow operator");
    assert!(matches!(err, OperatorError::ChannelFence { .. }), "{err}");

    // The documented order: the indexer restored to before the primary's target.
    private_channel_node::container_pg_restore(&pg, "e2e_primary_restore", &indexer_backup);
    let fence = fence_slot_of(&db_url)
        .await
        .expect("the older backup has a fence");
    assert!(fence <= tip_at_backup);
    channel_block(&node.url).await;
    let (common, indexer) = configs(postgres, node.url.clone());
    let resumed = tokio::spawn(run(common, indexer, None));
    wait_for_fence_above(&db_url, fence).await;
    assert!(
        !resumed.is_finished(),
        "the indexer must keep running on a consistent pair"
    );
    resumed.abort();
    node.shutdown().await;
}
