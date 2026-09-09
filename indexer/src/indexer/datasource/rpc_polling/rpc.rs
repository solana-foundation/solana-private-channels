use crate::error::DataSourceRpcError;
use crate::indexer::datasource::rpc_polling::types::{BlockFetch, RpcBlock};
use futures::future::join_all;
use serde_json::json;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use std::collections::HashMap;
use tracing::{debug, warn};

/// The widest run of slots an idle node can pass with no block. It heartbeats one
/// block a second and a slot is one `blocktime_ms` tick, so the run is
/// `1000 / blocktime_ms`: ten by default, and at most this at a 1ms blocktime.
pub(crate) const MAX_IDLE_GAP_SLOTS: u64 = 1_000;

/// How far to search for the next producing slot before calling the distance a
/// hole in the ledger rather than an idle gap. Tenfold the gap above, because the
/// same poller also reads chains whose skipped runs no heartbeat bounds.
pub(crate) const MAX_LOOKAHEAD_SLOTS: u64 = MAX_IDLE_GAP_SLOTS * 10;

pub struct RpcPoller {
    client: reqwest::Client,
    rpc_url: String,
    encoding: UiTransactionEncoding,
    commitment: CommitmentLevel,
}

impl RpcPoller {
    pub fn new(
        rpc_url: String,
        encoding: UiTransactionEncoding,
        commitment: CommitmentLevel,
    ) -> Self {
        let client = reqwest::Client::new();
        Self {
            client,
            rpc_url,
            encoding,
            commitment,
        }
    }

    /// Fetch just the getBlock result: `Some(block)` when present, `None` for a
    /// `-32004/-32007/-32009/null` "skipped or missing" response (which the node
    /// cannot tell apart from unavailable). Other RPC errors surface as `Err`.
    pub async fn get_block_present(
        &self,
        slot: u64,
    ) -> Result<Option<RpcBlock>, DataSourceRpcError> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBlock",
                "params": [
                    slot,
                    {
                        "encoding": self.encoding.to_string(),
                        "transactionDetails": "full",
                        "maxSupportedTransactionVersion": 0,
                        "rewards": false,
                        "commitment": self.commitment.to_string(),
                    }
                ]
            }))
            .send()
            .await?;

        let json: serde_json::Value = response.json().await?;

        if let Some(error) = json.get("error") {
            // -32004: Block not available for slot (skipped or missing)
            // -32007: Slot skipped or missing due to ledger jump to recent snapshot
            // -32009: Slot skipped or missing in long-term storage
            if error["code"] == -32004 || error["code"] == -32007 || error["code"] == -32009 {
                return Ok(None);
            }
            return Err(DataSourceRpcError::Protocol {
                reason: format!("RPC error: {}", error),
            });
        }

        // A null result is the same "skipped or missing" ambiguity as the errors above.
        if json["result"].is_null() {
            return Ok(None);
        }

        let block: RpcBlock =
            serde_json::from_value(json["result"].clone()).map_err(DataSourceRpcError::from)?;
        Ok(Some(block))
    }

    /// Enumerate the slots in `[start, end]` that actually produced a block.
    /// This is what removes the skipped-versus-unavailable ambiguity: a slot the
    /// node does not list is never fetched, so its ambiguous response is never seen.
    pub async fn get_blocks(&self, start: u64, end: u64) -> Result<Vec<u64>, DataSourceRpcError> {
        self.slot_list(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlocks",
            "params": [start, end, { "commitment": self.commitment.to_string() }]
        }))
        .await
    }

    /// List up to `limit` producing slots at or after `start`. Used to find the
    /// first producer past a batch, whose distance is unbounded, in one call.
    pub async fn get_blocks_with_limit(
        &self,
        start: u64,
        limit: u64,
    ) -> Result<Vec<u64>, DataSourceRpcError> {
        self.slot_list(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlocksWithLimit",
            "params": [start, limit, { "commitment": self.commitment.to_string() }]
        }))
        .await
    }

    /// Post a slot-enumeration request and decode its `[u64]` result.
    async fn slot_list(&self, body: serde_json::Value) -> Result<Vec<u64>, DataSourceRpcError> {
        let response = self.client.post(&self.rpc_url).json(&body).send().await?;
        let json: serde_json::Value = response.json().await?;

        if let Some(error) = json.get("error") {
            return Err(DataSourceRpcError::Protocol {
                reason: format!("RPC error: {}", error),
            });
        }

        let slots = json["result"]
            .as_array()
            .ok_or_else(|| DataSourceRpcError::Protocol {
                reason: "Invalid slot list response".to_string(),
            })?;

        slots
            .iter()
            .map(|v| {
                v.as_u64().ok_or_else(|| DataSourceRpcError::Protocol {
                    reason: format!("Invalid slot in list response: {v}"),
                })
            })
            .collect()
    }

    /// Classify a contiguous ascending run of slots by enumerating the slots in
    /// `[start, end]` that produced a block and walking their `parentSlot` links
    /// forward from an anchor of `start - 1`, the last already-proven slot. A link
    /// that names the previous proven slot proves the non-producers between them
    /// empty; anything else means a real block sits in the gap that this endpoint
    /// will not serve, so those slots must never be checkpointed past.
    ///
    /// `parentSlot` is intrinsic block-header data, so a pruned, lagging or
    /// incomplete endpoint can only ever break a link, never fabricate one. Every
    /// way the node can be wrong therefore fails closed.
    ///
    /// The precondition is that `slots` is a contiguous ascending run, which every
    /// caller satisfies; it is documented rather than asserted, and the batch size
    /// must stay well below the node's getBlocks range cap. Results are emitted in
    /// the caller's order because both consumers stop at the first unresolved slot,
    /// which is what makes the ungated max-checkpoint safe.
    pub async fn get_blocks_batch(
        &self,
        slots: Vec<u64>,
    ) -> Vec<(u64, Result<BlockFetch, DataSourceRpcError>)> {
        let (Some(&start), Some(&end)) = (slots.first(), slots.last()) else {
            return vec![];
        };

        let produced = match self.get_blocks(start, end).await {
            Ok(produced) => produced,
            Err(e) => {
                // With no enumeration nothing can be proven, so no block is fetched
                // and every requested slot fails closed.
                let reason = format!("getBlocks({start}, {end}) failed: {e}");
                return slots
                    .into_iter()
                    .map(|slot| (slot, Err(unproven(&reason))))
                    .collect();
            }
        };

        let mut verdicts = self.classify_range(start, end, produced).await;
        slots
            .into_iter()
            .map(|slot| {
                let verdict = verdicts.remove(&slot).unwrap_or_else(|| {
                    Err(unproven(&format!(
                        "slot {slot} was not covered by the classification of [{start}, {end}]"
                    )))
                });
                (slot, verdict)
            })
            .collect()
    }

    /// Walk the producers of `[start, end]` and build one verdict per slot.
    async fn classify_range(
        &self,
        start: u64,
        end: u64,
        produced: Vec<u64>,
    ) -> HashMap<u64, Result<BlockFetch, DataSourceRpcError>> {
        // Slot 0 has no predecessor to anchor on. Saturating to 0 here would put an
        // in-range slot inside the already-proven region and wave genesis through.
        let anchor = match start.checked_sub(1) {
            Some(anchor) => Proven::Anchor(anchor),
            None => Proven::Unanchored,
        };

        // Normalise the listing: the walk needs ascending, in-range, distinct slots.
        // Sorting cannot fabricate a proof, because the parent links are unchanged.
        let mut producers: Vec<u64> = produced
            .into_iter()
            .filter(|s| (start..=end).contains(s))
            .collect();
        producers.sort_unstable();
        producers.dedup();

        // Only producers are fetched, so the ambiguous "skipped or missing" answer
        // is never observed for a slot the node did not list.
        let fetched = join_all(
            producers
                .iter()
                .map(|&slot| async move { (slot, self.get_block_present(slot).await) }),
        )
        .await;

        debug!(
            "Batch [{start}, {end}]: {} of {} slots produced a block",
            producers.len(),
            end.saturating_sub(start) + 1
        );

        let mut verdicts = HashMap::new();
        let mut proven = anchor;
        // First slot of the run that is still waiting for a witness.
        let mut pending = start;

        for (slot, fetch) in fetched {
            match fetch {
                Ok(Some(block)) => {
                    // The link has to be judged even when there is no gap to fill:
                    // between adjacent producers the range is empty, and discarding
                    // the verdict there would let a contradiction re-anchor the walk.
                    let extends = proven.gap_is_empty_below(block.parent_slot);
                    if !extends {
                        debug!(
                            "Slot {slot} names parent {} which does not extend {proven:?}, so slots {pending}..={slot} cannot be proven",
                            block.parent_slot
                        );
                    }
                    let gap = if extends {
                        BlockFetch::Skipped
                    } else {
                        BlockFetch::Unavailable
                    };
                    fill_verdict(&mut verdicts, pending..slot, gap);
                    verdicts.insert(slot, Ok(BlockFetch::Present(block)));
                    // A link that contradicts a block already in hand means the
                    // endpoint is serving two forks, so nothing after it can be
                    // trusted. A link that merely fails to cover a gap still leaves
                    // this block in hand as a sound anchor for what follows.
                    proven = match proven {
                        Proven::Block(_) if !extends => Proven::Broken,
                        _ => Proven::Block(slot),
                    };
                }
                Ok(None) => {
                    debug!("Slot {slot} was listed as a producer but could not be fetched");
                    // Listed as a producer but not served: a block exists here that
                    // this endpoint will not hand over, and the run before it has
                    // lost the witness that would have proven it empty.
                    fill_verdict(&mut verdicts, pending..=slot, BlockFetch::Unavailable);
                    proven = Proven::Broken;
                }
                Err(e) => {
                    let reason = format!("getBlock({slot}) failed: {e}");
                    fill_unproven(&mut verdicts, pending..slot, &reason);
                    verdicts.insert(slot, Err(e));
                    proven = Proven::Broken;
                }
            }
            pending = slot.saturating_add(1);
        }

        // A tail exists exactly when `end` itself produced nothing, so it has no
        // in-batch block to witness it. The bound check also covers an inverted
        // range, where nothing was classified at all.
        let has_tail = producers.last() != Some(&end);
        if has_tail && pending <= end {
            match self.witness_tail(&proven, end).await {
                Ok(verdict) => {
                    debug!("Tail [{pending}, {end}] resolved by witness as {verdict:?}");
                    fill_verdict(&mut verdicts, pending..=end, verdict)
                }
                Err(reason) => {
                    debug!("Tail [{pending}, {end}] is unproven: {reason}");
                    fill_unproven(&mut verdicts, pending..=end, &reason)
                }
            }
        }

        verdicts
    }

    /// Resolve a trailing run of non-producers with the first producer past `end`,
    /// whose distance is unbounded. `Err` means no witness could be obtained, which
    /// is undetermined and must be retried; it is deliberately not a verdict,
    /// because calling an unwitnessed tail skipped is exactly the data loss this
    /// classifier exists to prevent.
    async fn witness_tail(&self, proven: &Proven, end: u64) -> Result<BlockFetch, String> {
        let from = end.saturating_add(1);
        let listed = self
            .get_blocks_with_limit(from, 1)
            .await
            .map_err(|e| format!("tail witness lookup from slot {from} failed: {e}"))?;

        let witness = match listed.first() {
            Some(&slot) if slot >= from => slot,
            Some(&slot) => {
                return Err(format!(
                    "tail witness lookup from slot {from} answered with slot {slot}; endpoint is inconsistent"
                ))
            }
            None => {
                return Err(format!(
                    "no block is listed at or after slot {from}, so the tail of the batch is unwitnessed"
                ))
            }
        };

        let block = self
            .get_block_present(witness)
            .await
            .map_err(|e| format!("tail witness block {witness} failed to fetch: {e}"))?
            .ok_or_else(|| {
                format!("tail witness block {witness} could not be served, so the tail of the batch is unwitnessed")
            })?;

        Ok(if proven.gap_is_empty_below(block.parent_slot) {
            BlockFetch::Skipped
        } else {
            BlockFetch::Unavailable
        })
    }

    /// Get the latest slot
    pub async fn get_latest_slot(&self) -> Result<u64, DataSourceRpcError> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSlot",
                "params": [{
                    "commitment": self.commitment.to_string()
                  }]
            }))
            .send()
            .await
            .map_err(DataSourceRpcError::from)?;

        let json: serde_json::Value = response.json().await.map_err(DataSourceRpcError::from)?;

        if let Some(error) = json.get("error") {
            return Err(DataSourceRpcError::Protocol {
                reason: format!("RPC error: {}", error),
            });
        }

        json["result"]
            .as_u64()
            .ok_or_else(|| DataSourceRpcError::Protocol {
                reason: "Invalid slot response".to_string(),
            })
    }

    /// Get slot range to process, returning (slots, chain_tip). The range ends on
    /// a produced block: a tail is proven by the next block's parent link, and the
    /// tip is a tick that usually carries none. `max_slots` caps the batch, not
    /// the search, so an idle stretch wider than it still advances.
    pub async fn get_slots_to_process(
        &self,
        from_slot: u64,
        max_slots: usize,
    ) -> Result<(Vec<u64>, u64), DataSourceRpcError> {
        let latest_slot = self.get_latest_slot().await?;

        if from_slot >= latest_slot {
            return Ok((vec![], latest_slot));
        }

        // The same window the batch would have covered, so the enumeration here is
        // the one the walk needs rather than an extra range.
        let window_end = std::cmp::min(from_slot + max_slots as u64, latest_slot).saturating_sub(1);
        if window_end < from_slot {
            return Ok((vec![], latest_slot));
        }
        let produced = self.get_blocks(from_slot, window_end).await?;

        // An empty window is an idle stretch wider than the batch, not the end of
        // the chain, so the search continues past it. Bounding it here instead
        // would tie the batch size to the node's idle block cadence.
        let last = match produced.iter().max() {
            Some(&last) => last,
            None => match self
                .next_producer(from_slot, latest_slot.saturating_sub(1))
                .await?
            {
                Some(slot) => slot,
                None => return Ok((vec![], latest_slot)),
            },
        };

        Ok(((from_slot..=last).collect(), latest_slot))
    }

    /// The first slot at or after `from` that produced a block, with `highest` the
    /// last slot the poller may claim. `None` is an idle tip rather than an error,
    /// so the caller waits and asks again.
    async fn next_producer(
        &self,
        from: u64,
        highest: u64,
    ) -> Result<Option<u64>, DataSourceRpcError> {
        let Some(&slot) = self.get_blocks_with_limit(from, 1).await?.first() else {
            return Ok(None);
        };

        if slot < from {
            warn!("lookahead from slot {from} answered with slot {slot}; endpoint is inconsistent");
            return Ok(None);
        }
        // A block on the tip slot is claimed by the next poll, the same way the
        // window has always stopped short of it.
        if slot > highest {
            return Ok(None);
        }
        // This far out is a hole in the ledger, not an idle gap, and claiming it
        // would build a batch carrying an entry for every slot across the hole.
        if slot - from > MAX_LOOKAHEAD_SLOTS {
            warn!(
                "the next block after slot {from} is at slot {slot}, {} slots away and past the \
                 lookahead cap of {MAX_LOOKAHEAD_SLOTS}; the poller is not advancing",
                slot - from
            );
            return Ok(None);
        }

        Ok(Some(slot))
    }
}

/// How far the chain has been proven while walking a batch.
#[derive(Debug)]
enum Proven {
    /// The batch starts at slot 0, so there is no earlier slot to anchor on and
    /// nothing below the first block can be proven empty. A parent naming slot 0
    /// is positive proof the genesis block exists, so it must never be read as
    /// the anchor having covered the gap.
    Unanchored,
    /// The slot before the batch. It is proven complete, but it may itself have
    /// produced no block, so the next block's parent can legitimately name an
    /// earlier slot. Demanding an exact match here would stall the indexer for
    /// good whenever a batch boundary lands just after a skipped slot.
    Anchor(u64),
    /// A block was fetched at this slot, so the next block must name it exactly.
    /// Anything earlier would contradict a block we have in hand.
    Block(u64),
    /// A producer could not be read, or a link contradicted a block already in
    /// hand, so nothing after it can be proven.
    Broken,
}

impl Proven {
    /// Whether a block naming `parent` proves that nothing was produced between
    /// the proven point and that block. A parent at or before the proven point
    /// covers the whole gap; a parent inside it names a block the enumeration
    /// did not list, which is exactly the unavailable case.
    fn gap_is_empty_below(&self, parent: u64) -> bool {
        match self {
            Proven::Unanchored => false,
            Proven::Anchor(anchor) => parent <= *anchor,
            Proven::Block(slot) => parent == *slot,
            Proven::Broken => false,
        }
    }
}

/// A slot whose state could not be determined. `Err` is the "do not know" channel,
/// distinct from an `Ok` verdict; both consumers already treat it as retryable.
fn unproven(reason: &str) -> DataSourceRpcError {
    DataSourceRpcError::Unproven {
        reason: reason.to_string(),
    }
}

fn fill_verdict(
    verdicts: &mut HashMap<u64, Result<BlockFetch, DataSourceRpcError>>,
    slots: impl Iterator<Item = u64>,
    verdict: BlockFetch,
) {
    for slot in slots {
        verdicts.insert(slot, Ok(verdict.clone()));
    }
}

fn fill_unproven(
    verdicts: &mut HashMap<u64, Result<BlockFetch, DataSourceRpcError>>,
    slots: impl Iterator<Item = u64>,
    reason: &str,
) {
    for slot in slots {
        verdicts.insert(slot, Err(unproven(reason)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::rpc_mocks::*;
    use mockito::Server;

    fn poller(server: &Server) -> RpcPoller {
        RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Finalized,
        )
    }

    /// Collapse a verdict to a short tag so the scenarios below can assert a
    /// whole batch as one readable table.
    fn tag(result: &Result<BlockFetch, DataSourceRpcError>) -> &'static str {
        match result {
            Ok(BlockFetch::Present(_)) => "present",
            Ok(BlockFetch::Skipped) => "skipped",
            Ok(BlockFetch::Unavailable) => "unavailable",
            Err(_) => "err",
        }
    }

    fn tags(results: &[(u64, Result<BlockFetch, DataSourceRpcError>)]) -> Vec<(u64, &'static str)> {
        results.iter().map(|(slot, r)| (*slot, tag(r))).collect()
    }

    /// Fails the test loudly if the classifier issues a witness lookup it should
    /// not need. Without this an unexpected call is answered by mockito's 501 and
    /// disappears into a retry loop.
    fn no_witness_lookup(server: &mut Server) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlocksWithLimit"
            })))
            .expect(0)
            .create()
    }

    // ============================================================================
    // get_blocks_batch: the chain proof
    // ============================================================================

    /// U-1. Every producer's parentSlot names the previous proven slot, so the
    /// non-producers between them are proven empty.
    #[tokio::test]
    async fn chain_extends_cleanly_proves_skipped() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 104, &[(100, 99), (102, 100), (104, 102)]);
        let witness = no_witness_lookup(&mut server);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103, 104])
            .await;

        assert_eq!(
            tags(&results),
            vec![
                (100, "present"),
                (101, "skipped"),
                (102, "present"),
                (103, "skipped"),
                (104, "present"),
            ]
        );
        witness.assert();
    }

    /// The anchor is the slot before the batch, and it may itself have produced no
    /// block: a previous batch can legitimately end on a proven-empty slot. The next
    /// block's parent then names an earlier slot, which still proves the gap empty.
    /// Requiring an exact match here would stall the indexer permanently on a batch
    /// boundary that lands after a skip.
    #[tokio::test]
    async fn anchor_that_produced_no_block_still_proves_the_gap() {
        let mut server = Server::new_async().await;
        // Batch [11..=13] anchored at 10. Slots 10 and 11 produced nothing, so 12's
        // parent is 9, the last block before the anchor.
        let _c = chain(&mut server, 11, 13, &[(12, 9), (13, 12)]);

        let results = poller(&server).get_blocks_batch(vec![11, 12, 13]).await;

        assert_eq!(
            tags(&results),
            vec![(11, "skipped"), (12, "present"), (13, "present")]
        );
    }

    /// The other side of the same rule: a parent that lands strictly inside the gap
    /// names a block the enumeration did not list, so the gap fails closed.
    #[tokio::test]
    async fn parent_inside_the_gap_after_the_anchor_is_unavailable() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 11, 13, &[(13, 12)]);

        let results = poller(&server).get_blocks_batch(vec![11, 12, 13]).await;

        assert_eq!(
            tags(&results),
            vec![(11, "unavailable"), (12, "unavailable"), (13, "present")]
        );
    }

    /// Slot 0 has no earlier slot to anchor on, so it is not "already proven".
    /// A witness naming parentSlot 0 is positive proof the genesis block exists,
    /// and waving it through as a skip would checkpoint straight past it.
    #[tokio::test]
    async fn genesis_slot_is_never_proven_skipped_by_its_own_anchor() {
        let mut server = Server::new_async().await;
        // The endpoint lists nothing in [0, 2], but a block past the batch names
        // slot 0 as its parent, which proves a block sits there.
        let _e = mock_get_blocks(&mut server, 0, 2, &[]);
        let _w = mock_get_blocks_with_limit(&mut server, 3, &[3]);
        let _b = mock_get_block_at(&mut server, 3, 0);

        let results = poller(&server).get_blocks_batch(vec![0, 1, 2]).await;

        assert_eq!(
            tags(&results),
            vec![(0, "unavailable"), (1, "unavailable"), (2, "unavailable")]
        );
    }

    /// Two producers back to back still have to chain. A parent link that skips
    /// the block just fetched is a contradiction, and an empty gap between them
    /// must not let it be discarded and the proof silently re-anchored.
    #[tokio::test]
    async fn broken_link_between_adjacent_producers_breaks_the_proof() {
        let mut server = Server::new_async().await;
        // 101 claims parent 55 while 100 was fetched a moment ago.
        let _c = chain(&mut server, 100, 102, &[(100, 99), (101, 55)]);
        let _w = mock_get_blocks_with_limit(&mut server, 103, &[103]);
        let _b = mock_get_block_at(&mut server, 103, 101);

        let results = poller(&server).get_blocks_batch(vec![100, 101, 102]).await;

        // 102 must not be proven empty off a chain that already contradicted itself.
        assert_eq!(
            tags(&results),
            vec![(100, "present"), (101, "present"), (102, "unavailable")]
        );
    }

    /// U-2. The exact case the retention floor got wrong: slot 102 holds a real
    /// block that this endpoint did not list and will not serve, and 104's parent
    /// link proves it. The whole unlisted run must fail closed.
    #[tokio::test]
    async fn parent_naming_unlisted_slot_is_unavailable() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 104, &[(100, 99), (104, 102)]);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103, 104])
            .await;

        assert_eq!(
            tags(&results),
            vec![
                (100, "present"),
                (101, "unavailable"),
                (102, "unavailable"),
                (103, "unavailable"),
                (104, "present"),
            ]
        );
    }

    /// U-3. A load balancer can list a slot from one replica and refuse to serve it
    /// from another. The listed-but-unfetchable slot is Unavailable, and it breaks
    /// the chain, so nothing around it may be called a proven skip.
    #[tokio::test]
    async fn listed_producer_unfetchable_is_unavailable() {
        let mut server = Server::new_async().await;
        let _blocks = mock_get_blocks(&mut server, 100, 104, &[100, 102, 104]);
        let _b100 = mock_get_block_at(&mut server, 100, 99);
        let _b102 = mock_get_block_absent(&mut server, 102, -32009);
        let _b104 = mock_get_block_at(&mut server, 104, 102);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103, 104])
            .await;

        let tagged = tags(&results);
        assert_eq!(tagged[2], (102, "unavailable"));
        assert!(
            tagged.iter().all(|(_, t)| *t != "skipped"),
            "a broken chain must not yield any proven skip: {tagged:?}"
        );
    }

    /// A channel node that cannot read a block answers `-32000`, which is outside
    /// the skipped-or-missing set, so the slot is undetermined and retried. An
    /// internal failure must never be read as a proven skip.
    #[tokio::test]
    async fn server_error_on_listed_producer_is_never_a_skip() {
        let mut server = Server::new_async().await;
        let _blocks = mock_get_blocks(&mut server, 100, 104, &[100, 102, 104]);
        let _b100 = mock_get_block_at(&mut server, 100, 99);
        let _b102 = mock_get_block_error(&mut server, 102, -32000, "Failed to read block");
        let _b104 = mock_get_block_at(&mut server, 104, 102);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103, 104])
            .await;

        let tagged = tags(&results);
        assert_eq!(tagged[2], (102, "err"));
        assert!(
            tagged.iter().all(|(_, t)| *t != "skipped"),
            "an internal read failure must not yield any proven skip: {tagged:?}"
        );
    }

    /// U-4. A batch ending in non-producers is witnessed by the first producer past
    /// the range, whose parent link proves the trailing run empty.
    #[tokio::test]
    async fn trailing_run_proven_by_witness() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 103, &[(100, 99)]);
        let _w = mock_get_blocks_with_limit(&mut server, 104, &[105]);
        let _wb = mock_get_block_at(&mut server, 105, 100);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103])
            .await;

        assert_eq!(
            tags(&results),
            vec![
                (100, "present"),
                (101, "skipped"),
                (102, "skipped"),
                (103, "skipped"),
            ]
        );
    }

    /// U-5. No witness can be listed, so the trailing run is undetermined. It must
    /// be retried, never called a skip: this is the one caveat of the design and
    /// the single place a regression would silently reintroduce the data loss.
    #[tokio::test]
    async fn trailing_run_without_witness_retries() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 103, &[(100, 99)]);
        let _w = mock_get_blocks_with_limit(&mut server, 104, &[]);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103])
            .await;

        assert_eq!(
            tags(&results),
            vec![(100, "present"), (101, "err"), (102, "err"), (103, "err")]
        );
    }

    /// U-6. The witness is listed but cannot be served, which is the same
    /// undetermined tail as having no witness at all.
    #[tokio::test]
    async fn trailing_run_witness_unfetchable_retries() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 103, &[(100, 99)]);
        let _w = mock_get_blocks_with_limit(&mut server, 104, &[105]);
        let _wb = mock_get_block_absent(&mut server, 105, -32009);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103])
            .await;

        assert_eq!(
            tags(&results),
            vec![(100, "present"), (101, "err"), (102, "err"), (103, "err")]
        );
    }

    /// U-7. The witness resolves but its parent lands inside the trailing run, so a
    /// real block lives there. That is a determined verdict (Unavailable), not the
    /// undetermined retry of U-5.
    #[tokio::test]
    async fn trailing_run_witness_parent_breaks_chain_is_unavailable() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 103, &[(100, 99)]);
        let _w = mock_get_blocks_with_limit(&mut server, 104, &[105]);
        let _wb = mock_get_block_at(&mut server, 105, 102);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103])
            .await;

        assert_eq!(
            tags(&results),
            vec![
                (100, "present"),
                (101, "unavailable"),
                (102, "unavailable"),
                (103, "unavailable"),
            ]
        );
    }

    /// U-8. A range with no producers at all is one long trailing run; a witness
    /// linking back to the anchor proves the whole range empty.
    #[tokio::test]
    async fn empty_range_all_skipped_when_witness_links_to_anchor() {
        let mut server = Server::new_async().await;
        let _blocks = mock_get_blocks(&mut server, 100, 104, &[]);
        let _w = mock_get_blocks_with_limit(&mut server, 105, &[106]);
        let _wb = mock_get_block_at(&mut server, 106, 99);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103, 104])
            .await;

        assert!(
            tags(&results).iter().all(|(_, t)| *t == "skipped"),
            "witness parent == anchor proves the whole range empty: {:?}",
            tags(&results)
        );
    }

    /// U-9. The most damaging shape: an entirely pruned range. The witness parent
    /// lands inside it, so nothing here may be checkpointed past.
    #[tokio::test]
    async fn empty_range_all_unavailable_when_chain_breaks() {
        let mut server = Server::new_async().await;
        let _blocks = mock_get_blocks(&mut server, 100, 104, &[]);
        let _w = mock_get_blocks_with_limit(&mut server, 105, &[106]);
        let _wb = mock_get_block_at(&mut server, 106, 102);

        let results = poller(&server)
            .get_blocks_batch(vec![100, 101, 102, 103, 104])
            .await;

        assert!(
            tags(&results).iter().all(|(_, t)| *t == "unavailable"),
            "a broken witness link over an empty range must fail closed: {:?}",
            tags(&results)
        );
    }

    /// U-10. If the enumeration itself fails there is nothing to reason from, so
    /// every slot fails closed and no block is fetched.
    #[tokio::test]
    async fn get_blocks_error_fails_closed_without_fetching() {
        let mut server = Server::new_async().await;
        let _blocks = mock_get_blocks_error(&mut server, -32600, "Invalid request");
        let untouched = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlock"
            })))
            .expect(0)
            .create();

        let results = poller(&server).get_blocks_batch(vec![100, 101, 102]).await;

        assert_eq!(
            tags(&results),
            vec![(100, "err"), (101, "err"), (102, "err")]
        );
        untouched.assert();
    }

    /// U-11. Backfill can be configured to start at slot 0, so the anchor
    /// arithmetic must not wrap. Block 0 names itself as parent on both chains.
    #[tokio::test]
    async fn anchor_at_genesis_does_not_underflow() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 0, 1, &[(0, 0), (1, 0)]);

        let results = poller(&server).get_blocks_batch(vec![0, 1]).await;

        assert_eq!(tags(&results), vec![(0, "present"), (1, "present")]);
    }

    /// U-12. Both consumers stop at the first unresolved slot and rely on ascending
    /// order to do so; that ordering is what makes the ungated max-checkpoint safe.
    #[tokio::test]
    async fn results_preserve_requested_slot_order() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 104, &[(100, 99), (102, 100), (104, 102)]);
        let _w = no_witness_lookup(&mut server);

        let requested = vec![100, 101, 102, 103, 104];
        let results = poller(&server).get_blocks_batch(requested.clone()).await;

        let returned: Vec<u64> = results.iter().map(|(slot, _)| *slot).collect();
        assert_eq!(returned, requested);
    }

    /// U-13. Non-producing slots are never fetched at all. That is what removes the
    /// ambiguity at the source, and it is the basis of the per-batch call budget.
    #[tokio::test]
    async fn non_producers_are_never_fetched() {
        let mut server = Server::new_async().await;
        let never = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlock",
                "params": [101]
            })))
            .expect(0)
            .create();
        let _blocks = mock_get_blocks(&mut server, 100, 102, &[100, 102]);
        let _b100 = mock_get_block_at(&mut server, 100, 99);
        let _b102 = mock_get_block_at(&mut server, 102, 100);

        let results = poller(&server).get_blocks_batch(vec![100, 101, 102]).await;

        assert_eq!(
            tags(&results),
            vec![(100, "present"), (101, "skipped"), (102, "present")]
        );
        never.assert();
    }

    /// U-14. Regression: both the enumeration and the block fetch must carry the
    /// configured commitment. A previous version omitted it on getBlock, so the
    /// server defaulted to finalized and answered -32009 for confirmed-only slots.
    #[tokio::test]
    async fn forwards_configured_commitment() {
        let mut server = Server::new_async().await;
        let enumerate = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlocks",
                "params": [100, 100, { "commitment": "confirmed" }]
            })))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":[100],"id":1}"#)
            .expect(1)
            .create();
        let fetch = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlock",
                "params": [100, { "commitment": "confirmed" }]
            })))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"blockhash":"B100","parentSlot":99,"transactions":[]},"id":1}"#,
            )
            .expect(1)
            .create();

        let poller = RpcPoller::new(
            server.url(),
            UiTransactionEncoding::Json,
            CommitmentLevel::Confirmed,
        );
        let results = poller.get_blocks_batch(vec![100]).await;

        enumerate.assert();
        fetch.assert();
        assert_eq!(tags(&results), vec![(100, "present")]);
    }

    /// U-15. An empty batch is answered without touching the network.
    #[tokio::test]
    async fn empty_batch_issues_no_rpc() {
        let mut server = Server::new_async().await;
        let untouched = server.mock("POST", "/").expect(0).create();

        let results = poller(&server).get_blocks_batch(vec![]).await;

        assert!(results.is_empty());
        untouched.assert();
    }

    /// A mixed batch resolved end to end, with the last slot a producer so no
    /// witness is needed.
    #[tokio::test]
    async fn test_get_blocks_batch_multiple_blocks() {
        let mut server = Server::new_async().await;
        let _c = chain(&mut server, 100, 102, &[(100, 99), (102, 100)]);

        let results = poller(&server).get_blocks_batch(vec![100, 101, 102]).await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 100);
        match results[0].1.as_ref().unwrap() {
            BlockFetch::Present(block) => assert_eq!(block.blockhash, "TestBlockHash100"),
            other => panic!("slot 100 expected Present, got {other:?}"),
        }
        assert!(matches!(
            results[1].1.as_ref().unwrap(),
            BlockFetch::Skipped
        ));
        match results[2].1.as_ref().unwrap() {
            BlockFetch::Present(block) => assert_eq!(block.blockhash, "TestBlockHash102"),
            other => panic!("slot 102 expected Present, got {other:?}"),
        }
    }

    // ============================================================================
    // get_block_present
    // ============================================================================

    /// U-16. The single-slot entry point the fallback re-fetch uses: contents or
    /// nothing, with every "skipped or missing" spelling collapsing to None.
    #[tokio::test]
    async fn get_block_present_maps_absent_to_none() {
        let mut server = Server::new_async().await;
        let _b = mock_get_block_at(&mut server, 100, 99);
        for (slot, code) in [(101, -32004), (102, -32007), (103, -32009)] {
            let _m = mock_get_block_absent(&mut server, slot, code);
        }
        let _null = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlock",
                "params": [104]
            })))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":null,"id":1}"#)
            .create();
        let _err = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getBlock",
                "params": [105]
            })))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid"},"id":1}"#)
            .create();

        let poller = poller(&server);

        let present = poller.get_block_present(100).await.unwrap();
        assert_eq!(present.unwrap().parent_slot, 99);
        for slot in [101, 102, 103, 104] {
            assert!(
                poller.get_block_present(slot).await.unwrap().is_none(),
                "slot {slot} must map to None"
            );
        }
        let err = poller.get_block_present(105).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("RPC error"));
    }

    // ============================================================================
    // get_latest_slot Tests
    // ============================================================================

    #[tokio::test]
    async fn test_get_latest_slot_success() {
        let mut server = Server::new_async().await;
        let _m = mock_rpc_success(&mut server, "12345").await;

        let poller = poller(&server);
        let result = poller.get_latest_slot().await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 12345);
    }

    #[tokio::test]
    async fn test_get_latest_slot_rpc_error() {
        let mut server = Server::new_async().await;
        let _m = mock_rpc_error(&mut server, -32600, "Invalid request").await;

        let poller = poller(&server);
        let result = poller.get_latest_slot().await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("RPC error"));
    }

    // ============================================================================
    // get_slots_to_process Tests
    // ============================================================================

    #[tokio::test]
    async fn test_get_slots_to_process_normal() {
        let mut server = Server::new_async().await;
        let _m_slot = mock_get_slot(&mut server, 150);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[100, 110, 129]);

        let poller = poller(&server);
        let result = poller.get_slots_to_process(100, 30).await;

        assert!(result.is_ok());
        let (slots, chain_tip) = result.unwrap();
        assert_eq!(slots.len(), 30);
        assert_eq!(slots[0], 100);
        assert_eq!(slots[29], 129);
        assert_eq!(chain_tip, 150);
    }

    /// The chain tip is a tick and usually carries no block, so the walk has to
    /// stop at the last produced one. Ending anywhere else leaves a tail that no
    /// parent link can prove and the source refuses to advance past.
    #[tokio::test]
    async fn get_slots_to_process_ends_on_a_produced_block() {
        let mut server = Server::new_async().await;
        let _m_slot = mock_get_slot(&mut server, 150);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[100, 110, 120]);
        // A window that holds a block settles the range on its own, so the
        // loaded path never pays for the lookahead.
        let m_next = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlocksWithLimit"
            })))
            .expect(0)
            .create();

        let poller = poller(&server);
        let (slots, chain_tip) = poller.get_slots_to_process(100, 30).await.unwrap();

        m_next.assert();
        assert_eq!(slots.first(), Some(&100));
        assert_eq!(
            slots.last(),
            Some(&120),
            "the range must end on the last produced block, not on the tip"
        );
        assert_eq!(chain_tip, 150);
    }

    /// An idle stretch wider than the batch leaves the window empty while a block
    /// sits past it. The window caps the batch, it does not bound the search, so
    /// the range has to reach that block. Stopping at the window would park the
    /// poller on a range it can never advance out of.
    #[tokio::test]
    async fn get_slots_to_process_extends_past_an_empty_window_to_the_next_producer() {
        let mut server = Server::new_async().await;
        let _m_slot = mock_get_slot(&mut server, 150);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[]);
        let _m_next = mock_get_blocks_with_limit(&mut server, 100, &[140]);

        let poller = poller(&server);
        let (slots, chain_tip) = poller.get_slots_to_process(100, 30).await.unwrap();

        assert_eq!(slots.first(), Some(&100));
        assert_eq!(
            slots.last(),
            Some(&140),
            "the range must reach the first producer past the window"
        );
        assert_eq!(chain_tip, 150);
    }

    /// Nothing produced anywhere at or after the window is a genuinely idle tip,
    /// not a stall. There is nothing provable to walk, so the poller waits.
    #[tokio::test]
    async fn get_slots_to_process_waits_when_no_block_has_been_produced_yet() {
        let mut server = Server::new_async().await;
        let _m_slot = mock_get_slot(&mut server, 150);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[]);
        let _m_next = mock_get_blocks_with_limit(&mut server, 100, &[]);

        let poller = poller(&server);
        let (slots, chain_tip) = poller.get_slots_to_process(100, 30).await.unwrap();

        assert!(slots.is_empty());
        assert_eq!(chain_tip, 150);
    }

    /// The tip slot is never processed, so a block landing on it waits for the
    /// next poll. The lookahead must respect that ceiling rather than widen it.
    #[tokio::test]
    async fn get_slots_to_process_ignores_a_producer_at_the_tip() {
        let mut server = Server::new_async().await;
        let _m_slot = mock_get_slot(&mut server, 150);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[]);
        let _m_next = mock_get_blocks_with_limit(&mut server, 100, &[150]);

        let poller = poller(&server);
        let (slots, chain_tip) = poller.get_slots_to_process(100, 30).await.unwrap();

        assert!(slots.is_empty(), "the tip slot must not be claimed");
        assert_eq!(chain_tip, 150);
    }

    /// A producer this far out is a hole in the ledger rather than an idle gap.
    /// Claiming it would build a batch per slot across the hole, so the poller
    /// declines and leaves the distance in the log.
    #[tokio::test]
    async fn get_slots_to_process_refuses_a_lookahead_beyond_the_cap() {
        let mut server = Server::new_async().await;
        let far = 100 + MAX_LOOKAHEAD_SLOTS + 1;
        let _m_slot = mock_get_slot(&mut server, far + 10);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[]);
        let _m_next = mock_get_blocks_with_limit(&mut server, 100, &[far]);

        let poller = poller(&server);
        let (slots, _) = poller.get_slots_to_process(100, 30).await.unwrap();

        assert!(
            slots.is_empty(),
            "a producer past the cap must not be claimed"
        );
    }

    /// An answer below the window start contradicts the request. Building the
    /// range anyway yields an empty one, which would read as caught up and stall
    /// the poller silently, so it is refused outright.
    #[tokio::test]
    async fn get_slots_to_process_rejects_a_producer_before_the_window() {
        let mut server = Server::new_async().await;
        let _m_slot = mock_get_slot(&mut server, 150);
        let _m_enum = mock_get_blocks(&mut server, 100, 129, &[]);
        let _m_next = mock_get_blocks_with_limit(&mut server, 100, &[90]);

        let poller = poller(&server);
        let (slots, _) = poller.get_slots_to_process(100, 30).await.unwrap();

        assert!(slots.is_empty());
    }

    #[tokio::test]
    async fn test_get_slots_to_process_already_caught_up() {
        let mut server = Server::new_async().await;
        let _m = mock_rpc_success(&mut server, "100").await;

        let poller = poller(&server);
        let result = poller.get_slots_to_process(100, 30).await;

        assert!(result.is_ok());
        let (slots, chain_tip) = result.unwrap();
        assert!(slots.is_empty());
        assert_eq!(chain_tip, 100);
    }
}
