use crate::storage::common::models::ChannelFence;
use std::collections::BTreeMap;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Recorded blocks kept for neighbour checks. Only the newest are needed, since a block
/// arriving below all of them would mean the fill fell this far behind the live stream.
const MAX_RECORDED_BLOCKS: usize = 100_000;

/// Hash links between the channel blocks the withdraw indexer reads, seeded with the fence
/// and shared by the startup fill and the live stream, so a block may arrive before its
/// parent. A broken link means the channel was restored behind the indexer DB.
pub struct ChainLink {
    inner: Mutex<Links>,
    broken: CancellationToken,
}

struct Links {
    fence: Option<ChannelFence>,
    // Durable checkpoint at boot. Every slot in (fence, floor] was read as skipped.
    floor: u64,
    blocks: BTreeMap<u64, Block>,
    capacity: usize,
    broken: Option<String>,
}

struct Block {
    hash: String,
    prev: String,
    parent: u64,
}

impl ChainLink {
    pub fn new(fence: Option<ChannelFence>, floor: u64) -> Self {
        Self::with_capacity(fence, floor, MAX_RECORDED_BLOCKS)
    }

    fn with_capacity(fence: Option<ChannelFence>, floor: u64, capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Links {
                fence,
                floor,
                blocks: BTreeMap::new(),
                capacity,
                broken: None,
            }),
            broken: CancellationToken::new(),
        }
    }

    /// Record a present block and check it against the nearest recorded blocks on either
    /// side. `Err` is final: the token returned by `broken` is cancelled.
    pub fn record(
        &self,
        slot: u64,
        parent_slot: u64,
        hash: &str,
        prev: &str,
    ) -> Result<(), String> {
        let mut links = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(reason) = &links.broken {
            return Err(reason.clone());
        }
        let outcome = links.link(slot, parent_slot, hash, prev);
        if let Err(reason) = &outcome {
            error!("Channel chain link broken: {reason}");
            links.broken = Some(reason.clone());
            self.broken.cancel();
        }
        outcome
    }

    /// Cancelled once any link breaks, so the indexer can stop as a whole.
    pub fn broken(&self) -> CancellationToken {
        self.broken.clone()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().blocks.len()
    }
}

impl Links {
    // On one chain a block's parent is the newest present block below it, so every recorded
    // block between the parent and the block, on either side of it, breaks the link.
    fn link(&mut self, slot: u64, parent: u64, hash: &str, prev: &str) -> Result<(), String> {
        if let Some(known) = self.blocks.get(&slot) {
            if known.hash == hash {
                return Ok(());
            }
            return Err(format!(
                "slot {slot} was served as {} and then as {hash}",
                known.hash
            ));
        }

        match self.blocks.range(..slot).next_back() {
            Some((&below, block)) if parent < below => {
                return Err(format!(
                    "block {slot} names parent {parent} but block {below} ({}) lies between; \
                     the channel no longer has the history the indexer read",
                    block.hash
                ));
            }
            Some((&below, block)) if parent == below && block.hash != prev => {
                return Err(format!(
                    "block {slot} names parent {parent} as {prev} but it was served as {}",
                    block.hash
                ));
            }
            Some((&below, _)) if parent == below => {}
            _ if parent <= self.floor => {
                if let Some(fence) = &self.fence {
                    if parent != fence.slot || prev != fence.blockhash {
                        return Err(format!(
                            "block {slot} names parent {parent} ({prev}) but the indexer's fence \
                             is block {} ({}); the channel no longer has the history the indexer read",
                            fence.slot, fence.blockhash
                        ));
                    }
                }
            }
            // The parent is above every recorded block below this one, so not fetched yet.
            _ => {}
        }

        if let Some((&above, block)) = self.blocks.range(slot + 1..).next() {
            if block.parent < slot {
                return Err(format!(
                    "block {above} names parent {} but block {slot} lies between; the channel forked",
                    block.parent
                ));
            }
            if block.parent == slot && block.prev != hash {
                return Err(format!(
                    "block {above} names parent {slot} as {} but it was served as {hash}",
                    block.prev
                ));
            }
        }

        self.blocks.insert(
            slot,
            Block {
                hash: hash.to_string(),
                prev: prev.to_string(),
                parent,
            },
        );
        while self.blocks.len() > self.capacity {
            self.blocks.pop_first();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(slot: u64) -> Option<ChannelFence> {
        Some(ChannelFence {
            slot,
            blockhash: format!("h{slot}"),
        })
    }

    /// Record a block whose hash and parent hash follow the `h{slot}` naming.
    fn link(chain: &ChainLink, slot: u64, parent: u64) -> Result<(), String> {
        chain.record(slot, parent, &format!("h{slot}"), &format!("h{parent}"))
    }

    #[test]
    fn chain_link_matrix() {
        // First live block links to the fence across skipped slots.
        let chain = ChainLink::new(fence(95), 100);
        assert!(link(&chain, 103, 95).is_ok());
        assert!(link(&chain, 104, 103).is_ok());
        assert!(!chain.broken().is_cancelled());

        // A parent hash that differs from the fence is a rewound channel.
        let chain = ChainLink::new(fence(95), 100);
        assert!(chain.record(101, 95, "h101", "other").is_err());
        assert!(chain.broken().is_cancelled());
        // Once broken it stays broken.
        assert!(link(&chain, 102, 101).is_err());

        // A parent below the fence skips the fence block.
        let chain = ChainLink::new(fence(95), 100);
        assert!(link(&chain, 101, 90).is_err());

        // A parent inside the committed range names a block the indexer saw as skipped.
        let chain = ChainLink::new(fence(95), 100);
        assert!(link(&chain, 101, 98).is_err());

        // Child before parent (live ahead of the fill), link holds either order.
        let chain = ChainLink::new(fence(100), 100);
        assert!(link(&chain, 110, 108).is_ok());
        assert!(link(&chain, 105, 100).is_ok());
        assert!(link(&chain, 108, 105).is_ok());
        assert!(!chain.broken().is_cancelled());

        // Child before parent, link broken when the parent arrives.
        let chain = ChainLink::new(fence(100), 100);
        assert!(chain.record(110, 108, "h110", "not-h108").is_ok());
        assert!(link(&chain, 108, 100).is_err());

        // Parent before child, link broken when the child arrives.
        let chain = ChainLink::new(fence(100), 100);
        assert!(link(&chain, 108, 100).is_ok());
        assert!(chain.record(110, 108, "h110", "not-h108").is_err());

        // The same slot under a second hash is a fork switch.
        let chain = ChainLink::new(fence(100), 100);
        assert!(link(&chain, 101, 100).is_ok());
        assert!(chain.record(101, 100, "other", "h100").is_err());

        // Two blocks naming one parent is a fork too.
        let chain = ChainLink::new(fence(100), 100);
        assert!(link(&chain, 110, 105).is_ok());
        assert!(chain.record(111, 105, "h111", "h105").is_err());

        // No fence (first boot after upgrade or after a resync): the first block is accepted.
        let chain = ChainLink::new(None, 100);
        assert!(chain.record(101, 99, "h101", "anything").is_ok());
        assert!(link(&chain, 102, 101).is_ok());
        assert!(chain.record(103, 102, "h103", "wrong").is_err());
    }

    /// A channel restored while the indexer runs re-produces blocks whose parent is an older
    /// block, skipping blocks the indexer already linked. That must break the chain even
    /// when those blocks were linked long ago (SOLA13-212 with services running).
    #[test]
    fn chain_link_rejects_a_restore_behind_linked_blocks() {
        let chain = ChainLink::new(fence(100), 100);
        for slot in 101..=103 {
            link(&chain, slot, slot - 1).unwrap();
        }
        // The restored channel's next block names 101 as its parent, skipping 102 and 103.
        assert!(chain.record(104, 101, "r104", "h101").is_err());

        // Same after a long run, against a parent linked a thousand blocks ago.
        let chain = ChainLink::new(fence(100), 100);
        for slot in 101..=1_100 {
            link(&chain, slot, slot - 1).unwrap();
        }
        assert!(chain.record(1_101, 1_050, "r1101", "r1050").is_err());

        // A late fill block landing between two linked blocks is a fork too.
        let chain = ChainLink::new(fence(100), 100);
        link(&chain, 110, 105).unwrap();
        link(&chain, 105, 100).unwrap();
        assert!(link(&chain, 107, 105).is_err());
    }

    /// The map is capped, keeping the newest blocks, and still catches a restore after that.
    #[test]
    fn chain_link_caps_recorded_blocks() {
        let chain = ChainLink::with_capacity(fence(100), 100, 16);
        for slot in 101..=1_100 {
            link(&chain, slot, slot - 1).unwrap();
        }
        assert_eq!(chain.len(), 16);
        assert!(chain.record(1_101, 1_050, "r1101", "r1050").is_err());
    }
}
