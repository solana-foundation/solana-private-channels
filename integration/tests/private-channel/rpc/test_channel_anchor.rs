use super::test_context::PrivateChannelContext;
use private_channel_indexer::operator::escrow_sweep::{channel_anchor, fetch_channel_supply_at};
use private_channel_indexer::operator::{RetryConfig, RpcClientWithRetry};
use solana_commitment_config::CommitmentConfig;

/// Reconciliation's supply freshness check against a real channel node: the node accepts
/// the finalized commitment on getSlot and getBlocks, its newest block is recent, and a
/// supply read answers at or after that block.
pub async fn run_channel_anchor_test(ctx: &PrivateChannelContext) {
    println!("\n=== Channel Freshness Anchor Test ===");

    let rpc = RpcClientWithRetry::with_retry_config(
        ctx.read_client.url(),
        RetryConfig::default(),
        CommitmentConfig::finalized(),
    );
    let anchor = channel_anchor(&rpc)
        .await
        .expect("a live channel has a recent block");
    let (_, slot) = fetch_channel_supply_at(&rpc, &ctx.mint)
        .await
        .expect("channel supply read");

    assert!(
        slot >= anchor,
        "a supply read taken after the anchor answers at or past it: slot {slot}, anchor {anchor}"
    );
    println!("✓ Anchor block {anchor}, supply read at slot {slot}");
}
