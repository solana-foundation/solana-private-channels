use crate::metrics::CHANNEL_FENCE_MISMATCH;
use crate::operator::RpcClientWithRetry;
use crate::storage::common::models::ChannelFence;
use crate::storage::Storage;
use std::time::Duration;
use tracing::{error, info, warn};

/// Outcome of checking the channel against the withdraw indexer's fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceVerdict {
    /// The channel still has the fence block, or there is no fence yet.
    Ok,
    /// The channel was restored behind the indexer DB.
    Mismatch(String),
    /// The check could not run; never read as a verdict either way.
    Unavailable(String),
}

/// How long the check may wait for the channel tip to reach the fence slot.
#[derive(Debug, Clone, Copy)]
pub enum FenceWait {
    /// Boot: a tip below the fence may be a node still catching up, so wait for it.
    UntilTip { poll: Duration, budget: Duration },
    /// Recovery tick: wait out a moment of replica lag, then a tip still below is a rewind.
    Brief { poll: Duration, budget: Duration },
}

/// The fence was read from this channel, so a tip still below it after this is a rewind.
const TICK_WAIT: FenceWait = FenceWait::Brief {
    poll: Duration::from_secs(1),
    budget: Duration::from_secs(15),
};

/// Check the channel still holds the fence block. A block hash is used rather than a
/// counter because the channel produces it: a restored channel re-produces the fence slot
/// with a different hash, or skips it, without anyone having to record the restore.
pub async fn verify_channel_fence(
    rpc: &RpcClientWithRetry,
    fence: Option<&ChannelFence>,
    wait: FenceWait,
) -> FenceVerdict {
    let Some(fence) = fence else {
        return FenceVerdict::Ok;
    };
    let started = tokio::time::Instant::now();
    loop {
        let tip = match rpc.get_slot().await {
            Ok(tip) => tip,
            Err(e) => return FenceVerdict::Unavailable(format!("channel tip unreadable: {e}")),
        };
        if tip >= fence.slot {
            break;
        }
        match wait {
            FenceWait::Brief { poll, budget } => {
                if started.elapsed() >= budget {
                    return FenceVerdict::Mismatch(format!(
                        "channel tip {tip} stayed below fence slot {} for {budget:?}",
                        fence.slot
                    ));
                }
                tokio::time::sleep(poll).await;
            }
            FenceWait::UntilTip { poll, budget } => {
                if started.elapsed() >= budget {
                    return FenceVerdict::Unavailable(format!(
                        "channel tip {tip} stayed below fence slot {} for {budget:?}",
                        fence.slot
                    ));
                }
                info!(
                    tip,
                    fence_slot = fence.slot,
                    "Waiting for the channel tip to reach the fence"
                );
                tokio::time::sleep(poll).await;
            }
        }
    }

    match rpc.get_block_hashes(fence.slot).await {
        Ok(Some((hash, _))) if hash == fence.blockhash => FenceVerdict::Ok,
        Ok(Some((hash, _))) => FenceVerdict::Mismatch(format!(
            "channel block {} is {hash}, the indexer recorded {}",
            fence.slot, fence.blockhash
        )),
        // The tip is past the slot, so a missing block is an answer, not lag, unless
        // `truncate` pruned it, which says nothing about a restore.
        Ok(None) => match rpc.get_first_available_block().await {
            Ok(floor) if fence.slot < floor => FenceVerdict::Unavailable(format!(
                "fence block {} is below the channel's first available block {floor}; it was \
                 pruned, so the fence cannot be checked",
                fence.slot
            )),
            Ok(_) => FenceVerdict::Mismatch(format!(
                "channel has no block at fence slot {}, the indexer recorded {}",
                fence.slot, fence.blockhash
            )),
            Err(e) => FenceVerdict::Unavailable(format!(
                "channel has no block at fence slot {} and its first available block is \
                 unreadable: {e}",
                fence.slot
            )),
        },
        Err(e) => {
            FenceVerdict::Unavailable(format!("channel block {} unreadable: {e}", fence.slot))
        }
    }
}

/// Re-check on a running operator. Only a mismatch counts: a check that cannot run is
/// retried on the next tick instead of stopping withdrawals over an RPC blip.
pub async fn fence_broken_on_tick(storage: &Storage, rpc: &RpcClientWithRetry, role: &str) -> bool {
    let fence = match storage.get_channel_fence().await {
        Ok(fence) => fence,
        Err(e) => {
            warn!("Channel fence unreadable on recovery tick, retrying next tick: {e}");
            return false;
        }
    };
    match verify_channel_fence(rpc, fence.as_ref(), TICK_WAIT).await {
        FenceVerdict::Ok => false,
        FenceVerdict::Mismatch(reason) => {
            CHANNEL_FENCE_MISMATCH.with_label_values(&[role]).inc();
            error!("Channel was restored under the running operator, stopping: {reason}");
            true
        }
        FenceVerdict::Unavailable(reason) => {
            warn!("Channel fence unchecked on recovery tick, retrying next tick: {reason}");
            false
        }
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

    fn fence(slot: u64, hash: &str) -> ChannelFence {
        ChannelFence {
            slot,
            blockhash: hash.to_string(),
        }
    }

    fn mock_slot(server: &mut ServerGuard, slot: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(json!({ "method": "getSlot" })))
            .with_body(json!({ "jsonrpc": "2.0", "result": slot, "id": 1 }).to_string())
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

    fn block_body(hash: &str) -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": 1, "result": {
            "blockhash": hash, "previousBlockhash": "prev", "parentSlot": 99,
            "blockHeight": 10, "blockTime": null
        }})
    }

    fn error_body(code: i64) -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": 1, "error": { "code": code, "message": "missing" } })
    }

    const BOOT: FenceWait = FenceWait::UntilTip {
        poll: Duration::from_millis(5),
        budget: Duration::from_millis(300),
    };

    const TICK: FenceWait = FenceWait::Brief {
        poll: Duration::from_millis(5),
        budget: Duration::from_millis(50),
    };

    fn mock_floor(server: &mut ServerGuard, floor: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(
                json!({ "method": "getFirstAvailableBlock" }),
            ))
            .with_body(json!({ "jsonrpc": "2.0", "result": floor, "id": 1 }).to_string())
            .create()
    }

    #[tokio::test]
    async fn fence_verify_matrix() {
        // (tip, getBlock answer, first available block, wait, expected)
        type Case = (u64, serde_json::Value, u64, FenceWait, &'static str);
        let cases: Vec<Case> = vec![
            (100, block_body("H"), 0, BOOT, "ok"),
            (100, block_body("other"), 0, BOOT, "mismatch"),
            (150, error_body(-32007), 0, BOOT, "mismatch"),
            (150, error_body(-32004), 0, BOOT, "mismatch"),
            (
                150,
                json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
                0,
                BOOT,
                "mismatch",
            ),
            (150, error_body(-32000), 0, BOOT, "unavailable"),
            // Pruned by truncate, not restored: the block is gone for another reason.
            (150, error_body(-32007), 120, BOOT, "unavailable"),
            (150, error_body(-32007), 120, TICK, "unavailable"),
            (90, block_body("H"), 0, TICK, "mismatch"),
            (90, block_body("H"), 0, BOOT, "unavailable"),
        ];
        for (tip, answer, floor, wait, want) in cases {
            let mut server = Server::new_async().await;
            let _slot = mock_slot(&mut server, tip);
            let _block = mock_block(&mut server, 100, answer.clone());
            let _floor = mock_floor(&mut server, floor);
            let verdict =
                verify_channel_fence(&fast_rpc(&server.url()), Some(&fence(100, "H")), wait).await;
            let got = match verdict {
                FenceVerdict::Ok => "ok",
                FenceVerdict::Mismatch(_) => "mismatch",
                FenceVerdict::Unavailable(_) => "unavailable",
            };
            assert_eq!(got, want, "tip {tip}, answer {answer}, verdict {verdict:?}");
        }
    }

    /// A node still catching up at boot is waited for, not refused.
    #[tokio::test]
    async fn fence_verify_waits_for_the_tip() {
        let mut server = Server::new_async().await;
        let behind = mock_slot(&mut server, 90).expect(2);
        let _block = mock_block(&mut server, 100, block_body("H"));
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

    /// A replica lagging for a moment on the recovery tick is waited for, not taken as a restore.
    #[tokio::test]
    async fn fence_tick_waits_out_a_brief_lag() {
        let mut server = Server::new_async().await;
        let behind = mock_slot(&mut server, 90).expect(2);
        let _block = mock_block(&mut server, 100, block_body("H"));
        let rpc = fast_rpc(&server.url());
        let check =
            tokio::spawn(
                async move { verify_channel_fence(&rpc, Some(&fence(100, "H")), TICK).await },
            );
        tokio::time::sleep(Duration::from_millis(8)).await;
        behind.remove();
        let _caught_up = mock_slot(&mut server, 120);
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
