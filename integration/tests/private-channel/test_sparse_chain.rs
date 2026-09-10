//! End-to-end coverage for a chain whose slots and block heights are separate
//! counters. Each test drives a real node over JSON-RPC, so what is asserted here
//! is the wire contract a stock Solana client sees.

use anyhow::Result;
use private_channel_core::nodes::node::{run_node, NodeConfig, NodeMode};
use private_channel_core::stage_metrics::NoopMetrics;
use private_channel_indexer::indexer::datasource::rpc_polling::rpc::RpcPoller;
use private_channel_indexer::indexer::datasource::rpc_polling::types::BlockFetch;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::{
    hash::Hash, pubkey::Pubkey, signature::Keypair, signature::Signature, signer::Signer,
    transaction::Transaction,
};
use solana_system_interface::instruction as system_instruction;
use solana_transaction_status::UiTransactionEncoding;
use std::{net::TcpListener, sync::Arc, time::Duration};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

/// The settler's idle block cadence. Every timing below is expressed in terms of
/// it, so a change to the constant shows up as a failure rather than as flake.
const HEARTBEAT: Duration = Duration::from_secs(1);
const BLOCKTIME_MS: u64 = 100;
/// Ticks per heartbeat, which is the ratio the slot outruns the height by while
/// the node is idle.
const TICKS_PER_HEARTBEAT: u64 = HEARTBEAT.as_millis() as u64 / BLOCKTIME_MS;

fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// A running node. The Postgres container backing it is held separately so a
/// restart can drop the node without dropping its database.
struct TestNode {
    url: String,
    client: RpcClient,
    handles: Option<private_channel_core::nodes::node::NodeHandles>,
}

impl TestNode {
    /// Stop the node and start a fresh one against the same database, which is
    /// what a rolling restart looks like from the ledger's side.
    async fn restart(mut self, max_blockhashes: usize, pg_url: &str) -> Result<TestNode> {
        if let Some(handles) = self.handles.take() {
            handles.shutdown().await;
        }
        start_node_on(pg_url, max_blockhashes).await
    }
}

/// Start a throwaway Postgres and an idle node in front of it. The container is
/// returned so the caller keeps the database alive for the whole test.
async fn start_node(
    max_blockhashes: usize,
) -> Result<(TestNode, String, testcontainers::ContainerAsync<Postgres>)> {
    let pg = Postgres::default()
        .with_db_name("private_channel_node")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let host = pg.get_host().await?;
    let port = pg.get_host_port_ipv4(5432).await?;
    let pg_url = format!("postgres://postgres:password@{host}:{port}/private_channel_node");

    let node = start_node_on(&pg_url, max_blockhashes).await?;
    Ok((node, pg_url, pg))
}

/// Start a node against an existing database and wait for it to produce a block.
async fn start_node_on(pg_url: &str, max_blockhashes: usize) -> Result<TestNode> {
    let rpc_port = get_free_port();
    let config = NodeConfig {
        mode: NodeMode::Aio,
        port: rpc_port,
        sigverify_queue_size: 100,
        sigverify_workers: 1,
        max_connections: 50,
        max_tx_per_batch: 10,
        batch_deadline_ms: 5,
        batch_channel_capacity: 16,
        ingress_queue_capacity: private_channel_core::nodes::node::DEFAULT_INGRESS_QUEUE_CAPACITY,
        sequencer_queue_capacity:
            private_channel_core::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY,
        execution_results_capacity:
            private_channel_core::nodes::node::DEFAULT_EXECUTION_RESULTS_CAPACITY,
        max_svm_workers: 2,
        accountsdb_connection_url: pg_url.to_string(),
        redis_cache_url: None,
        redis_block_ttl_secs: 3_600,
        admin_keys: vec![Keypair::new().pubkey()],
        max_blockhashes,
        blocktime_ms: BLOCKTIME_MS,
        perf_sample_period_secs: 3_600,
        metrics: Arc::new(NoopMetrics),
    };
    let handles = run_node(config).await.expect("run_node");

    let url = format!("http://127.0.0.1:{rpc_port}");
    let client = RpcClient::new(url.clone());
    for _ in 0..60 {
        if client.get_latest_blockhash().await.is_ok() {
            return Ok(TestNode {
                url,
                client,
                handles: Some(handles),
            });
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("node never produced a block at {url}");
}

/// A distinct System transfer bearing `blockhash`. Execution is gasless, so a
/// fresh payer still settles, which makes each probe observable by signature.
fn client_transaction(blockhash: Hash) -> Transaction {
    let payer = Keypair::new();
    let transfer = system_instruction::transfer(&payer.pubkey(), &Pubkey::new_unique(), 1);
    Transaction::new_signed_with_payer(&[transfer], Some(&payer.pubkey()), &[&payer], blockhash)
}

/// Submit without preflight. Preflight would reject a stale blockhash in the
/// client, and what is under test is what the node does with the transaction.
async fn submit(client: &RpcClient, tx: &Transaction) -> Result<Signature> {
    Ok(client
        .send_transaction_with_config(
            tx,
            RpcSendTransactionConfig {
                skip_preflight: true,
                ..Default::default()
            },
        )
        .await?)
}

/// Poll `getSignatureStatuses` the way a confirmation loop does. False means the
/// transaction was still absent when the budget ran out.
async fn landed(client: &RpcClient, signature: &Signature, within: Duration) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let statuses = client.get_signature_statuses(&[*signature]).await?.value;
        if statuses.first().is_some_and(|status| status.is_some()) {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        sleep(Duration::from_millis(25)).await;
    }
}

/// Slots count ticks and `getBlockHeight` counts blocks, so an idle stretch pulls
/// them apart at the tick-to-heartbeat ratio. They used to be the same number,
/// which is what made a stock client's confirmation loop work only by accident.
#[tokio::test(flavor = "multi_thread")]
async fn slot_and_height_diverge_across_an_idle_stretch() -> Result<()> {
    let (node, _pg_url, _pg) = start_node(150).await?;

    let height_before = node.client.get_block_height().await?;
    sleep(HEARTBEAT * 3).await;
    let slot = node.client.get_slot().await?;
    let height = node.client.get_block_height().await?;

    assert!(
        height > height_before,
        "the heartbeat must keep the height advancing while idle: {height_before} -> {height}"
    );
    assert!(
        slot > height,
        "the slot must outrun the height across an idle stretch: slot {slot}, height {height}"
    );
    // Two full heartbeats of slack absorbs scheduling jitter; the real ratio
    // over three heartbeats is around thirty ticks to three blocks.
    assert!(
        slot - height >= TICKS_PER_HEARTBEAT,
        "the gap must track the tick-to-block ratio: slot {slot}, height {height}"
    );
    Ok(())
}

/// The indexer checks each block's `parentSlot` against the previous block it
/// fetched, demanding exact equality. On a sparse chain `slot - 1` names a slot
/// with no block, so the walk stalls and transactions stop being recorded.
#[tokio::test(flavor = "multi_thread")]
async fn the_indexer_walks_a_sparse_chain() -> Result<()> {
    let (node, _pg_url, _pg) = start_node(150).await?;
    sleep(HEARTBEAT * 3).await;

    // The gap must exist before anything about the walk is worth asserting: on a
    // dense chain this test passes while testing nothing.
    let tip = node.client.get_slot().await?;
    let produced = node.client.get_blocks(0, Some(tip)).await?;
    assert!(
        produced.len() >= 3,
        "need several blocks to walk, got {produced:?}"
    );
    assert!(
        produced.windows(2).any(|w| w[1] - w[0] > 1),
        "the chain must actually be sparse before linkage is worth checking: {produced:?}"
    );

    // End on a producer so the walk needs no witness beyond the batch.
    let last_producer = *produced.last().expect("a produced block");
    let poller = RpcPoller::new(
        node.url.clone(),
        UiTransactionEncoding::Json,
        CommitmentLevel::Finalized,
    );
    let verdicts = poller.get_blocks_batch((0..=last_producer).collect()).await;

    assert_eq!(verdicts.len() as u64, last_producer + 1);
    let mut skipped = 0usize;
    for (slot, verdict) in &verdicts {
        match verdict {
            Ok(BlockFetch::Present(_)) => assert!(
                produced.contains(slot),
                "slot {slot} was served a block but was not listed as a producer"
            ),
            // A slot proven empty by the next block's parent link.
            Ok(BlockFetch::Skipped) => skipped += 1,
            Ok(BlockFetch::Unavailable) => {
                panic!("slot {slot} could not be proven empty, so the walk stops advancing here")
            }
            Err(e) => panic!("slot {slot} was left unproven: {e}"),
        }
    }
    assert!(
        skipped > 0,
        "the sparse slots must be proven empty, not merely absent"
    );
    Ok(())
}

/// Expiry is a block count, so a blockhash survives `max_blockhashes` produced
/// blocks however long they take. A stock client's poll of `getBlockHeight` must
/// terminate on the same event the node uses to evict the hash.
#[tokio::test(flavor = "multi_thread")]
async fn a_blockhash_expires_after_max_blockhashes_blocks() -> Result<()> {
    let window = 3usize;
    let (node, _pg_url, _pg) = start_node(window).await?;

    let response = node
        .client
        .get_latest_blockhash_with_commitment(node.client.commitment())
        .await?;
    let (blockhash, last_valid_block_height) = response;
    let height_at_mint = node.client.get_block_height().await?;
    let slot_at_mint = node.client.get_slot().await?;
    // Inclusive, as Solana's is: a window of W blocks keeps a hash minted at
    // height H live through H + W - 1, which is where the node evicts it.
    assert_eq!(
        last_valid_block_height,
        height_at_mint + window as u64 - 1,
        "the deadline must be the last height at which the hash is still live"
    );
    assert!(
        node.client
            .is_blockhash_valid(&blockhash, node.client.commitment())
            .await?,
        "a freshly minted hash must be inside the window"
    );

    // At the deadline itself the hash must still be usable: the bound is
    // inclusive, and a client that submits exactly there must not be rejected.
    // The idle heartbeat leaves about a second of slack to observe it in.
    let boundary_deadline = tokio::time::Instant::now() + HEARTBEAT * (window as u32 + 10);
    loop {
        let height = node.client.get_block_height().await?;
        if height == last_valid_block_height {
            assert!(
                node.client
                    .is_blockhash_valid(&blockhash, node.client.commitment())
                    .await?,
                "the hash must still be live at its own lastValidBlockHeight"
            );
            break;
        }
        assert!(
            height < last_valid_block_height,
            "the deadline height was skipped, which the heartbeat should make impossible"
        );
        assert!(
            tokio::time::Instant::now() < boundary_deadline,
            "never reached the deadline height {last_valid_block_height}, stuck at {height}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    // The loop a stock client runs. At the idle heartbeat this takes about
    // `window` seconds, so the deadline is generous by an order of magnitude.
    let deadline = tokio::time::Instant::now() + HEARTBEAT * (window as u32 + 10);
    let height = loop {
        let height = node.client.get_block_height().await?;
        if height > last_valid_block_height {
            break height;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the confirmation loop never terminated: height {height}, deadline {last_valid_block_height}"
        );
        sleep(Duration::from_millis(100)).await;
    };

    assert!(
        !node
            .client
            .is_blockhash_valid(&blockhash, node.client.commitment())
            .await?,
        "a hash past its lastValidBlockHeight must have left the window"
    );

    // The same stretch measured in slots: the slot ran far past the deadline
    // long before the hash expired, which is the whole reason the deadline had
    // to stop being a slot.
    let slot = node.client.get_slot().await?;
    assert!(
        slot - slot_at_mint > height - height_at_mint,
        "an idle stretch must consume more slots than blocks: slots {slot_at_mint} -> {slot}, heights {height_at_mint} -> {height}"
    );
    Ok(())
}

/// Both counters live in `metadata` and are seeded on startup from the last
/// stored block. A restart must resume the height, not restart it, or every
/// `lastValidBlockHeight` a client holds is invalidated.
#[tokio::test(flavor = "multi_thread")]
async fn window_survives_a_restart_during_idle() -> Result<()> {
    let (node, pg_url, _pg) = start_node(150).await?;
    sleep(HEARTBEAT * 3).await;

    let slot_before = node.client.get_slot().await?;
    let height_before = node.client.get_block_height().await?;
    let blockhash_before = node.client.get_latest_blockhash().await?;
    assert!(
        slot_before > height_before,
        "the chain must be sparse before the restart is worth checking"
    );

    let node = node.restart(150, &pg_url).await?;

    let height_after = node.client.get_block_height().await?;
    assert!(
        height_after >= height_before,
        "height must continue the old sequence, not restart: {height_before} -> {height_after}"
    );
    let slot_after = node.client.get_slot().await?;
    assert!(
        slot_after >= slot_before,
        "the slot must resume from the last tick, not the last block: {slot_before} -> {slot_after}"
    );
    assert!(
        slot_after > height_after,
        "the restored slot must still lead the height: slot {slot_after}, height {height_after}"
    );
    assert!(
        node.client
            .is_blockhash_valid(&blockhash_before, node.client.commitment())
            .await?,
        "a hash minted before the restart must still be inside the reloaded window"
    );
    Ok(())
}

/// The contract this change exists for. A stock client captures
/// `lastValidBlockHeight`, polls `getBlockHeight`, and must be right about the
/// exact height at which its transaction can no longer land.
#[tokio::test(flavor = "multi_thread")]
async fn stock_client_confirmation_loop_terminates() -> Result<()> {
    const WINDOW: usize = 5;
    let (node, _pg_url, _pg) = start_node(WINDOW).await?;
    let client = &node.client;

    // Re-sample until no block lands between the two height reads, so the
    // deadline is compared against the height it was actually minted at.
    let (blockhash, last_valid_block_height, minted_at) = loop {
        let before = client.get_block_height().await?;
        let (blockhash, last_valid_block_height) = client
            .get_latest_blockhash_with_commitment(client.commitment())
            .await?;
        let after = client.get_block_height().await?;
        if before == after {
            break (blockhash, last_valid_block_height, after);
        }
    };
    assert_eq!(
        last_valid_block_height,
        minted_at + WINDOW as u64 - 1,
        "the deadline must be the last height at which the node still holds the hash"
    );

    // The loop an SDK runs: submit, then poll the height and the status together.
    let signature = submit(client, &client_transaction(blockhash)).await?;
    let confirmed_at = loop {
        let height = client.get_block_height().await?;
        if client.get_signature_statuses(&[signature]).await?.value[0].is_some() {
            break height;
        }
        assert!(
            height <= last_valid_block_height,
            "the loop passed the deadline {last_valid_block_height} with the transaction still absent"
        );
        sleep(Duration::from_millis(25)).await;
    };
    assert!(
        confirmed_at <= last_valid_block_height,
        "the transaction must confirm inside its own deadline: at {confirmed_at}, deadline {last_valid_block_height}"
    );

    // The bound is inclusive, so at the deadline height itself a transaction
    // bearing the hash must still land. A client that gave up here would be
    // abandoning a transaction the node would have accepted.
    let boundary = tokio::time::Instant::now() + HEARTBEAT * (WINDOW as u32 + 10);
    loop {
        let height = client.get_block_height().await?;
        assert!(
            height <= last_valid_block_height,
            "the deadline height was skipped, which the heartbeat should make impossible"
        );
        if height == last_valid_block_height {
            break;
        }
        assert!(
            tokio::time::Instant::now() < boundary,
            "never reached the deadline height {last_valid_block_height}, stuck at {height}"
        );
        sleep(Duration::from_millis(20)).await;
    }
    let at_deadline = submit(client, &client_transaction(blockhash)).await?;
    assert!(
        landed(client, &at_deadline, HEARTBEAT * 3).await?,
        "a transaction submitted at lastValidBlockHeight must still land"
    );

    // Past the deadline the node has evicted the hash, so the client is right to
    // stop waiting: nothing bearing that hash can land any more.
    let deadline = tokio::time::Instant::now() + HEARTBEAT * (WINDOW as u32 + 10);
    let expired_at = loop {
        let height = client.get_block_height().await?;
        if height > last_valid_block_height {
            break height;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the confirmation loop never terminated: height {height}, deadline {last_valid_block_height}"
        );
        sleep(Duration::from_millis(50)).await;
    };
    assert!(
        !client
            .is_blockhash_valid(&blockhash, client.commitment())
            .await?,
        "a hash past its lastValidBlockHeight must have left the window"
    );
    let after_deadline = submit(client, &client_transaction(blockhash)).await?;
    assert!(
        !landed(client, &after_deadline, HEARTBEAT * 3).await?,
        "a transaction bearing an expired blockhash must not land"
    );
    // Blocks kept being produced while it was pending, so its absence is a
    // rejection rather than a stalled chain.
    let height_after = client.get_block_height().await?;
    assert!(
        height_after > expired_at,
        "the chain must have kept producing blocks: {expired_at} -> {height_after}"
    );
    Ok(())
}

/// The accepted trade, measured rather than assumed: the same number of blocks
/// takes materially longer to elapse while the node is idle, so the wall-clock
/// validity window moves with load exactly as Solana's does.
#[tokio::test(flavor = "multi_thread")]
async fn idle_expiry_stretches_with_the_heartbeat() -> Result<()> {
    const BLOCKS: u64 = 5;
    let (node, _pg_url, _pg) = start_node(150).await?;
    let client = &node.client;

    // Under load every tick carries work, so the tick is the cadence.
    let load_from = client.get_block_height().await?;
    let load_from_slot = client.get_slot().await?;
    let load_started = tokio::time::Instant::now();
    loop {
        let height = client.get_block_height().await?;
        if height >= load_from + BLOCKS {
            break;
        }
        let blockhash = client.get_latest_blockhash().await?;
        submit(client, &client_transaction(blockhash)).await?;
        sleep(Duration::from_millis(20)).await;
    }
    let under_load = load_started.elapsed();
    let load_slots = client.get_slot().await? - load_from_slot;

    // Let the pipeline drain so the idle measurement is not shortened by
    // transactions submitted during the load phase.
    sleep(HEARTBEAT).await;

    let idle_from = client.get_block_height().await?;
    let idle_from_slot = client.get_slot().await?;
    let idle_started = tokio::time::Instant::now();
    while client.get_block_height().await? < idle_from + BLOCKS {
        sleep(Duration::from_millis(25)).await;
    }
    let while_idle = idle_started.elapsed();
    let idle_slots = client.get_slot().await? - idle_from_slot;

    // The gap has to be real before the durations mean anything: on a dense
    // chain both phases would be the same and this would measure nothing.
    assert!(
        idle_slots >= BLOCKS * 2,
        "the idle stretch must be sparse: {BLOCKS} blocks spanned only {idle_slots} slots"
    );
    assert!(
        idle_slots > load_slots,
        "idle blocks must cost more slots than loaded ones: {idle_slots} against {load_slots}"
    );
    // The heartbeat is a floor on idle cadence, so this many blocks cannot
    // elapse faster than one heartbeat less than that count.
    assert!(
        while_idle >= HEARTBEAT * (BLOCKS as u32 - 1),
        "the heartbeat must pace idle block production, took {while_idle:?}"
    );
    // A ratio, never an absolute duration: what matters is that the same window
    // is materially longer idle than under load.
    assert!(
        while_idle >= under_load * 2,
        "the same block window must stretch while idle: {under_load:?} under load, {while_idle:?} idle"
    );
    Ok(())
}

/// `getSlot` reports the live tick counter, so it advances every blocktime while
/// the node is idle and blocks are sparse. A client polling it sees a slot that
/// moves with the chain rather than one frozen until the next heartbeat block.
#[tokio::test(flavor = "multi_thread")]
async fn get_slot_advances_every_tick_while_idle() -> Result<()> {
    let (node, _pg_url, _pg) = start_node(150).await?;
    // Skip the startup blocks so the samples cover steady idle cadence.
    sleep(HEARTBEAT).await;

    const HEARTBEATS: u32 = 4;
    let height_before = node.client.get_block_height().await?;
    let started = tokio::time::Instant::now();
    let mut samples: Vec<u64> = Vec::new();
    while started.elapsed() < HEARTBEAT * HEARTBEATS {
        samples.push(node.client.get_slot().await?);
        sleep(Duration::from_millis(25)).await;
    }
    let height_after = node.client.get_block_height().await?;

    // The chain must actually be sparse before the slot's cadence means
    // anything: on a dense chain every tick carries a block and this measures
    // nothing.
    let advanced = samples.last().unwrap() - samples.first().unwrap();
    let blocks = height_after - height_before;
    assert!(
        blocks >= 2,
        "the heartbeat must have produced blocks to compare against, got {blocks}"
    );
    assert!(
        advanced > blocks * 3,
        "slots must outrun blocks before the cadence is worth checking: {advanced} slots, {blocks} blocks"
    );

    assert!(
        samples.windows(2).all(|pair| pair[1] >= pair[0]),
        "getSlot must never go backwards: {samples:?}"
    );
    assert!(
        advanced >= TICKS_PER_HEARTBEAT * (HEARTBEATS as u64 - 2),
        "the slot must track the tick cadence, advanced only {advanced}"
    );

    // Every observed move must be a few ticks at most. A slot published only
    // with a block jumps a whole heartbeat at a time, which is what this
    // inverts.
    let steps: Vec<u64> = samples
        .windows(2)
        .filter(|pair| pair[1] > pair[0])
        .map(|pair| pair[1] - pair[0])
        .collect();
    assert!(
        steps.len() >= HEARTBEATS as usize * 2,
        "the slot must move continuously, not once per block: {samples:?}"
    );
    for step in &steps {
        assert!(
            *step <= 4,
            "an idle step must be a tick or two, got {step} in {samples:?}"
        );
    }

    // The height is the counter that stays flat between blocks, and the slot
    // always leads it.
    let slot = node.client.get_slot().await?;
    assert!(
        slot > height_after,
        "the slot must lead the height: slot {slot}, height {height_after}"
    );
    Ok(())
}

/// A client that recorded a slot, or bound a `minContextSlot` to one, must never
/// see the chain go backwards. The slot is durable before it is served, so a
/// restart resumes from the last tick rather than from the last stored block.
#[tokio::test(flavor = "multi_thread")]
async fn get_slot_does_not_regress_across_a_restart() -> Result<()> {
    let (node, pg_url, _pg) = start_node(150).await?;
    sleep(HEARTBEAT * 2).await;

    // Restart from a slot that carries no block, or the reseed is never asked
    // the question this test exists for.
    let deadline = tokio::time::Instant::now() + HEARTBEAT * 5;
    let (slot_before, last_block) = loop {
        let slot = node.client.get_slot().await?;
        let produced = node.client.get_blocks(0, Some(slot)).await?;
        let last_block = *produced.last().expect("a produced block");
        assert!(
            produced.windows(2).any(|pair| pair[1] - pair[0] > 1),
            "the chain must be sparse before a restart proves anything: {produced:?}"
        );
        // Several ticks past the last block, so a reseed from that block would
        // land visibly below what was already served.
        if slot >= last_block + TICKS_PER_HEARTBEAT / 2 {
            break (slot, last_block);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never observed a slot past the last block, so the chain is not sparse"
        );
        sleep(Duration::from_millis(20)).await;
    };

    let node = node.restart(150, &pg_url).await?;

    // Sample past the first blocks the restarted node produces: a reseed from
    // the last stored block only shows up once it writes one.
    let mut samples: Vec<u64> = Vec::new();
    let started = tokio::time::Instant::now();
    while started.elapsed() < HEARTBEAT * 3 {
        samples.push(node.client.get_slot().await?);
        sleep(Duration::from_millis(25)).await;
    }

    assert!(
        samples.iter().all(|slot| *slot >= slot_before),
        "getSlot went backwards across the restart from {slot_before} (last block {last_block}): {samples:?}"
    );
    assert!(
        samples.windows(2).all(|pair| pair[1] >= pair[0]),
        "getSlot must stay monotonic after the restart: {samples:?}"
    );
    Ok(())
}
