use crate::operator::RpcClientWithRetry;
use crate::storage::common::models::{ChannelFence, FenceRecord};
use std::time::Duration;
use tracing::info;

/// Outcome of checking the channel against the withdraw indexer's fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceVerdict {
    /// The channel still has the fence block, or there is no fence yet.
    Ok,
    /// The channel proved it no longer has the fence block: another hash, a chain that
    /// skips the slot, or a block where the indexer proved slots empty. Nothing else counts,
    /// so this never fires without a restore.
    Mismatch(String),
    /// No verdict yet: a lagging or pruned node, or an RPC error. Never read as a restore.
    Unavailable(String),
}

/// How long boot may wait for the channel to give a verdict.
#[derive(Debug, Clone, Copy)]
pub enum FenceWait {
    UntilTip { poll: Duration, budget: Duration },
}

/// One read of the channel: a verdict, or a reason to look again.
enum Probe {
    Final(FenceVerdict),
    Retry(String),
}

/// Check the channel still holds the fence block. A block hash is used rather than a
/// counter because the channel produces it: a restored channel re-produces the fence slot
/// with a different hash, or skips it, without anyone having to record the restore.
pub async fn verify_channel_fence(
    rpc: &RpcClientWithRetry,
    record: Option<&FenceRecord>,
    wait: FenceWait,
) -> FenceVerdict {
    let Some(FenceRecord { fence, checkpoint }) = record else {
        return FenceVerdict::Ok;
    };
    let FenceWait::UntilTip { poll, budget } = wait;
    let started = tokio::time::Instant::now();
    loop {
        let reason = match probe(rpc, fence, *checkpoint).await {
            Probe::Final(verdict) => return verdict,
            Probe::Retry(reason) => reason,
        };
        if started.elapsed() >= budget {
            return FenceVerdict::Unavailable(format!("{reason}, still after {budget:?}"));
        }
        info!(
            fence_slot = fence.slot,
            "Waiting to check the channel fence: {reason}"
        );
        tokio::time::sleep(poll).await;
    }
}

async fn probe(rpc: &RpcClientWithRetry, fence: &ChannelFence, checkpoint: u64) -> Probe {
    let tip = match rpc.get_slot().await {
        Ok(tip) => tip,
        Err(e) => {
            return Probe::Final(FenceVerdict::Unavailable(format!(
                "channel tip unreadable: {e}"
            )))
        }
    };
    if tip < fence.slot {
        return Probe::Retry(format!(
            "channel tip {tip} is below fence slot {}",
            fence.slot
        ));
    }
    match rpc.get_block_summary(fence.slot).await {
        Ok(Some((hash, _))) if hash == fence.blockhash => probe_gap(rpc, fence, checkpoint).await,
        Ok(Some((hash, _))) => Probe::Final(FenceVerdict::Mismatch(format!(
            "channel block {} has hash {hash}, the indexer recorded {}",
            fence.slot, fence.blockhash
        ))),
        Ok(None) => probe_missing_block(rpc, fence).await,
        Err(e) => Probe::Final(FenceVerdict::Unavailable(format!(
            "channel block {} unreadable: {e}",
            fence.slot
        ))),
    }
}

/// The indexer proved every slot between the fence block and its checkpoint empty, and
/// resumes after the checkpoint. A channel block there now was produced by a restored
/// chain; a later block from the same node proves the range still empty.
async fn probe_gap(rpc: &RpcClientWithRetry, fence: &ChannelFence, checkpoint: u64) -> Probe {
    if checkpoint <= fence.slot {
        return Probe::Final(FenceVerdict::Ok);
    }
    match rpc.get_blocks_with_limit(fence.slot + 1, 1).await {
        Ok(slots) => match slots.first() {
            Some(&next) if next <= checkpoint => Probe::Final(FenceVerdict::Mismatch(format!(
                "the channel has block {next} between fence block {} and checkpoint \
                 {checkpoint}, where the indexer found none",
                fence.slot
            ))),
            Some(_) => Probe::Final(FenceVerdict::Ok),
            None => Probe::Retry(format!(
                "channel serves no block after fence slot {} yet, so slots up to checkpoint \
                 {checkpoint} cannot be checked",
                fence.slot
            )),
        },
        Err(e) => Probe::Final(FenceVerdict::Unavailable(format!(
            "channel block list unreadable: {e}"
        ))),
    }
}

/// A missing fence block is not proof: a read node's cached tip can be ahead of its replica.
/// The proof is a later block on the same node whose parent is below the fence slot. Its
/// block list reads Postgres, and replication is ordered, so it holds every earlier block.
async fn probe_missing_block(rpc: &RpcClientWithRetry, fence: &ChannelFence) -> Probe {
    match rpc.get_first_available_block().await {
        Ok(floor) if fence.slot < floor => {
            return Probe::Final(FenceVerdict::Unavailable(format!(
                "fence block {} is below the channel's first available block {floor} (pruned)",
                fence.slot
            )))
        }
        Ok(_) => {}
        Err(e) => {
            return Probe::Final(FenceVerdict::Unavailable(format!(
                "channel first available block unreadable: {e}"
            )))
        }
    }
    let later = match rpc.get_blocks_with_limit(fence.slot + 1, 1).await {
        Ok(slots) => slots.into_iter().next(),
        Err(e) => {
            return Probe::Final(FenceVerdict::Unavailable(format!(
                "channel block list unreadable: {e}"
            )))
        }
    };
    let Some(later) = later else {
        return Probe::Retry(format!(
            "channel does not serve fence block {} yet",
            fence.slot
        ));
    };
    match rpc.get_block_summary(later).await {
        Ok(Some((_, parent))) if parent < fence.slot => {
            Probe::Final(FenceVerdict::Mismatch(format!(
                "the channel skips fence slot {}: its block {later} follows block {parent}",
                fence.slot
            )))
        }
        Ok(_) => Probe::Retry(format!(
            "channel does not serve fence block {} yet but has block {later}",
            fence.slot
        )),
        Err(e) => Probe::Final(FenceVerdict::Unavailable(format!(
            "channel block {later} unreadable: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::RetryConfig;
    use mockito::{Matcher, Server, ServerGuard};
    use serde_json::json;
    use solana_commitment_config::CommitmentConfig;

    fn fast_rpc(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    /// A fence whose checkpoint sits on the fence block.
    fn fence(slot: u64, hash: &str) -> FenceRecord {
        FenceRecord {
            fence: ChannelFence {
                slot,
                blockhash: hash.to_string(),
            },
            checkpoint: slot,
        }
    }

    fn reply(result: serde_json::Value) -> String {
        json!({ "jsonrpc": "2.0", "id": 1, "result": result }).to_string()
    }

    fn mock_slot(server: &mut ServerGuard, slot: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(json!({ "method": "getSlot" })))
            .with_body(reply(json!(slot)))
            .create()
    }

    fn mock_block(server: &mut ServerGuard, slot: u64, body: serde_json::Value) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({ "method": "getBlock", "params": [slot] }),
            ))
            .with_body(body.to_string())
            .create()
    }

    fn block_body(hash: &str, parent: u64) -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": 1, "result": {
            "blockhash": hash, "previousBlockhash": "prev", "parentSlot": parent,
            "blockHeight": 10, "blockTime": null
        }})
    }

    fn error_body(code: i64) -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": 1, "error": { "code": code, "message": "missing" } })
    }

    fn mock_floor(server: &mut ServerGuard, floor: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({ "method": "getFirstAvailableBlock" }),
            ))
            .with_body(reply(json!(floor)))
            .create()
    }

    fn mock_later_blocks(server: &mut ServerGuard, slots: &[u64]) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({ "method": "getBlocksWithLimit" }),
            ))
            .with_body(reply(json!(slots)))
            .create()
    }

    const BOOT: FenceWait = FenceWait::UntilTip {
        poll: Duration::from_millis(5),
        budget: Duration::from_millis(100),
    };

    fn label(verdict: &FenceVerdict) -> &'static str {
        match verdict {
            FenceVerdict::Ok => "ok",
            FenceVerdict::Mismatch(_) => "mismatch",
            FenceVerdict::Unavailable(_) => "unavailable",
        }
    }

    /// The fence block is slot 100. A served block never changes or disappears without a
    /// restore, so only a different hash, or the node's own later block skipping slot 100,
    /// proves one. Everything else is a node that cannot answer yet.
    #[tokio::test]
    async fn fence_verify_matrix() {
        // (case, getBlock(100), first available block, later blocks, getBlock(later), want)
        type Case = (
            &'static str,
            serde_json::Value,
            u64,
            Vec<u64>,
            Option<serde_json::Value>,
            &'static str,
        );
        let cases: Vec<Case> = vec![
            ("same hash", block_body("H", 99), 0, vec![], None, "ok"),
            (
                "hash differs",
                block_body("other", 99),
                0,
                vec![],
                None,
                "mismatch",
            ),
            // A read node whose cached tip is ahead of its replica: no later block yet.
            (
                "lagging node, skipped",
                error_body(-32007),
                0,
                vec![],
                None,
                "unavailable",
            ),
            (
                "lagging node, not available",
                error_body(-32004),
                0,
                vec![],
                None,
                "unavailable",
            ),
            (
                "lagging node, null",
                json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
                0,
                vec![],
                None,
                "unavailable",
            ),
            (
                "lagging node, -32009",
                error_body(-32009),
                0,
                vec![],
                None,
                "unavailable",
            ),
            (
                "restored chain skips the slot",
                error_body(-32007),
                0,
                vec![105],
                Some(block_body("N105", 98)),
                "mismatch",
            ),
            (
                "later block names the fence slot",
                error_body(-32007),
                0,
                vec![105],
                Some(block_body("N105", 100)),
                "unavailable",
            ),
            (
                "truncated below the fence",
                error_body(-32007),
                120,
                vec![150],
                Some(block_body("N150", 98)),
                "unavailable",
            ),
            (
                "block read fails",
                error_body(-32000),
                0,
                vec![],
                None,
                "unavailable",
            ),
        ];
        for (case, answer, floor, later, later_block, want) in cases {
            let mut server = Server::new_async().await;
            let _slot = mock_slot(&mut server, 150);
            let _block = mock_block(&mut server, 100, answer);
            let _floor = mock_floor(&mut server, floor);
            let _later = mock_later_blocks(&mut server, &later);
            let _later_block = later_block.map(|body| mock_block(&mut server, later[0], body));
            let verdict =
                verify_channel_fence(&fast_rpc(&server.url()), Some(&fence(100, "H")), BOOT).await;
            assert_eq!(label(&verdict), want, "{case}: {verdict:?}");
        }
    }

    /// Slots between the fence block and the checkpoint were proven empty when indexed, so
    /// a channel block there now proves a restore the indexer would resume past. Only a
    /// later block from the same node proves the range still empty.
    #[tokio::test]
    async fn fence_verify_gap_to_checkpoint() {
        // (case, first block after the fence, want)
        let cases: [(&str, Vec<u64>, &str); 3] = [
            ("restored chain fills the gap", vec![103], "mismatch"),
            ("next block is past the checkpoint", vec![110], "ok"),
            ("no later block yet", vec![], "unavailable"),
        ];
        for (case, later, want) in cases {
            let mut server = Server::new_async().await;
            let _slot = mock_slot(&mut server, 150);
            let _block = mock_block(&mut server, 100, block_body("H", 99));
            let _later = mock_later_blocks(&mut server, &later);
            let mut record = fence(100, "H");
            record.checkpoint = 105;
            let verdict = verify_channel_fence(&fast_rpc(&server.url()), Some(&record), BOOT).await;
            assert_eq!(label(&verdict), want, "{case}: {verdict:?}");
        }
    }

    /// A tip below the fence slot is a node catching up: waited for, and never a mismatch.
    #[tokio::test]
    async fn fence_verify_tip_below_is_never_a_mismatch() {
        let mut server = Server::new_async().await;
        let _slot = mock_slot(&mut server, 90);
        let _block = mock_block(&mut server, 100, block_body("H", 99));
        let verdict =
            verify_channel_fence(&fast_rpc(&server.url()), Some(&fence(100, "H")), BOOT).await;
        assert_eq!(label(&verdict), "unavailable", "{verdict:?}");
    }

    /// A node still catching up at boot is waited for, not refused.
    #[tokio::test]
    async fn fence_verify_waits_for_the_tip() {
        let mut server = Server::new_async().await;
        let behind = mock_slot(&mut server, 90).expect(2);
        let _block = mock_block(&mut server, 100, block_body("H", 99));
        let rpc = fast_rpc(&server.url());
        let check =
            tokio::spawn(
                async move { verify_channel_fence(&rpc, Some(&fence(100, "H")), BOOT).await },
            );
        tokio::time::sleep(Duration::from_millis(8)).await;
        behind.remove();
        let _caught_up = mock_slot(&mut server, 120);
        assert_eq!(check.await.unwrap(), FenceVerdict::Ok);
    }

    /// A lagging replica that catches up within the budget is waited for, then verified.
    #[tokio::test]
    async fn fence_verify_waits_for_a_lagging_replica() {
        let mut server = Server::new_async().await;
        let _slot = mock_slot(&mut server, 150);
        let _floor = mock_floor(&mut server, 0);
        let _later = mock_later_blocks(&mut server, &[]);
        let missing = mock_block(&mut server, 100, error_body(-32007)).expect_at_least(1);
        let rpc = fast_rpc(&server.url());
        let check =
            tokio::spawn(
                async move { verify_channel_fence(&rpc, Some(&fence(100, "H")), BOOT).await },
            );
        tokio::time::sleep(Duration::from_millis(20)).await;
        missing.remove();
        let _present = mock_block(&mut server, 100, block_body("H", 99));
        assert_eq!(check.await.unwrap(), FenceVerdict::Ok);
    }

    /// No fence (first boot after upgrade, or after a resync) skips the check entirely.
    #[tokio::test]
    async fn fence_verify_null_fence_makes_no_call() {
        let mut server = Server::new_async().await;
        let none = server.mock("POST", "/").expect(0).create();
        let verdict = verify_channel_fence(&fast_rpc(&server.url()), None, BOOT).await;
        assert_eq!(verdict, FenceVerdict::Ok);
        none.assert();
    }
}
