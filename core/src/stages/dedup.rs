use {
    crate::{
        accounts::traits::AccountsDB, health::StageHeartbeat, nodes::node::WorkerHandle,
        stage_metrics::SharedMetrics,
    },
    anyhow::{ensure, Result},
    solana_sdk::{hash::Hash, transaction::SanitizedTransaction},
    std::{
        collections::{HashMap, HashSet, LinkedList},
        sync::{Arc, RwLock},
    },
    tokio::sync::mpsc,
    tracing::{info, warn},
};

pub struct DedupArgs {
    pub max_blockhashes: usize,
    pub input_rx: mpsc::Receiver<SanitizedTransaction>,
    pub settled_blockhashes_rx: mpsc::UnboundedReceiver<Hash>,
    pub output_tx: mpsc::Sender<SanitizedTransaction>,
    /// Pre-populated from DB on startup; empty on a fresh node.
    pub initial_live_blockhashes: LinkedList<Hash>,
    /// Pre-populated from DB on startup; empty on a fresh node.
    pub initial_dedup_cache: HashMap<Hash, HashSet<Hash>>,
    pub metrics: SharedMetrics,
    pub heartbeat: Arc<StageHeartbeat>,
}

/// Bounded ingress queue from RPC into the pipeline; when full it rejects new
/// transactions instead of blocking. It is MPMC because the first stage is the
/// sigverify worker pool, so many workers receive from this one channel.
pub fn create_ingress_channel(
    capacity: usize,
) -> (
    async_channel::Sender<SanitizedTransaction>,
    async_channel::Receiver<SanitizedTransaction>,
) {
    async_channel::bounded(capacity)
}

/// Load dedup state from the DB to seed the cache on restart.
///
/// Reads the last `max_blockhashes` blocks and reconstructs:
/// - `live_blockhashes`: the ordered list of recent settled blockhashes
/// - `dedup_cache`: blockhash to the set of message hashes that used it as recent_blockhash
///
/// Returns empty state only on a fresh node (no metadata in DB yet).
/// Any DB query failure is propagated as an error — the caller must not
/// start the node with an empty cache when prior state exists, as that
/// could allow duplicate transactions to execute after a restart.
///
/// Age is not consulted: the last `max_blockhashes` blocks are exactly the hashes
/// still inside their published `lastValidBlockHeight`, however old they are.
pub async fn load_dedup_state(
    accounts_db: &AccountsDB,
    max_blockhashes: usize,
) -> Result<DedupState> {
    let live_blockhashes: LinkedList<Hash> = LinkedList::new();
    let dedup_cache: HashMap<Hash, HashSet<Hash>> = HashMap::new();

    let blocks = accounts_db.get_last_blocks(max_blockhashes).await?;
    if blocks.is_empty() {
        info!("Dedup: no prior blocks found, starting with empty state");
        return Ok((live_blockhashes, dedup_cache));
    }

    let loaded = blocks.len();
    if dedup_window_is_short(&blocks, max_blockhashes) {
        warn!(
            "Dedup: only {loaded} blocks survived retention against a window of {max_blockhashes}, \
             so transactions carrying a blockhash from the missing {} blocks will be rejected as \
             unknown inside their published lastValidBlockHeight; retention was cut below the window",
            max_blockhashes - loaded
        );
    }
    let (live_blockhashes, dedup_cache) = build_dedup_state(&blocks)?;

    info!(
        loaded_blocks = loaded,
        live_blockhashes = live_blockhashes.len(),
        cache_entries = dedup_cache.values().map(|s| s.len()).sum::<usize>(),
        "Dedup: restored dedup state from the last {max_blockhashes} blocks",
    );

    Ok((live_blockhashes, dedup_cache))
}

type DedupState = (LinkedList<Hash>, HashMap<Hash, HashSet<Hash>>);

/// A window short of `max_blockhashes` means retention cut it, unless genesis
/// is in it: then the chain has simply not produced that many blocks yet.
/// `blocks` is newest first, so the oldest loaded block is the last.
fn dedup_window_is_short(
    blocks: &[crate::accounts::traits::BlockInfo],
    max_blockhashes: usize,
) -> bool {
    blocks.len() < max_blockhashes && blocks.last().is_some_and(|oldest| oldest.slot != 0)
}

/// Ingest pending blockhash updates into `live_blockhashes`
///
/// If `first` is `Some`, it is the blockhash the caller already pulled
/// from the channel via `.recv()`; it is applied first and then any
/// additional hashes already in the channel are drained. If `first`
/// is `None`, the function peeks with `try_recv` and returns without
/// touching the lock when nothing is pending — so the hot path where
/// no blockhash has arrived does not block RPC readers of
/// `live_blockhashes`.
///
/// Ensures the dedup window is fully up-to-date before any transaction
/// is checked, preventing false "unknown blockhash" rejections caused
/// by stale state under load.
fn ingest_blockhashes(
    first: Option<Hash>,
    settled_blockhashes_rx: &mut mpsc::UnboundedReceiver<Hash>,
    live_blockhashes: &RwLock<LinkedList<Hash>>,
    dedup_cache: &mut HashMap<Hash, HashSet<Hash>>,
    max_blockhashes: usize,
) {
    let first = match first.or_else(|| settled_blockhashes_rx.try_recv().ok()) {
        Some(h) => h,
        None => return,
    };
    let mut bh_list = live_blockhashes.write().expect("blockhash lock poisoned");
    bh_list.push_back(first);
    while let Ok(blockhash) = settled_blockhashes_rx.try_recv() {
        bh_list.push_back(blockhash);
    }
    while bh_list.len() > max_blockhashes {
        if let Some(expired) = bh_list.pop_front() {
            dedup_cache.remove(&expired);
        }
    }
}

/// Pure computation: build `(live_blockhashes, dedup_cache)` from an ordered
/// slice of blocks. Extracted so it can be unit-tested without a live DB.
fn build_dedup_state(blocks: &[crate::accounts::traits::BlockInfo]) -> Result<DedupState> {
    let mut live_blockhashes: LinkedList<Hash> = LinkedList::new();
    let mut dedup_cache: HashMap<Hash, HashSet<Hash>> = HashMap::new();

    let loaded_hashes: HashSet<Hash> = blocks.iter().map(|b| b.blockhash).collect();

    for block in blocks {
        ensure!(
            block.transaction_signatures.len() == block.transaction_recent_blockhashes.len(),
            "Block {} has mismatched transaction_signatures ({}) and transaction_recent_blockhashes ({}) lengths",
            block.slot,
            block.transaction_signatures.len(),
            block.transaction_recent_blockhashes.len(),
        );
        ensure!(
            block.transaction_message_hashes.len() == block.transaction_signatures.len(),
            "Block {} has mismatched transaction_message_hashes ({}) and transaction_signatures ({}) lengths",
            block.slot,
            block.transaction_message_hashes.len(),
            block.transaction_signatures.len(),
        );

        live_blockhashes.push_back(block.blockhash);

        // The message hash is the replay identity, so the restart cache keys on
        // it exactly as the runtime stage does.
        for (message_hash, recent_blockhash) in block
            .transaction_message_hashes
            .iter()
            .zip(block.transaction_recent_blockhashes.iter())
        {
            if loaded_hashes.contains(recent_blockhash) {
                dedup_cache
                    .entry(*recent_blockhash)
                    .or_default()
                    .insert(*message_hash);
            }
        }
    }

    Ok((live_blockhashes, dedup_cache))
}

pub async fn start_dedup(args: DedupArgs) -> (WorkerHandle, Arc<RwLock<LinkedList<Hash>>>) {
    let DedupArgs {
        max_blockhashes,
        mut input_rx,
        mut settled_blockhashes_rx,
        output_tx,
        initial_live_blockhashes,
        initial_dedup_cache,
        metrics,
        heartbeat,
    } = args;

    let live_blockhashes = Arc::new(RwLock::new(initial_live_blockhashes));
    let live_blockhashes_clone = Arc::clone(&live_blockhashes);

    let handle = tokio::spawn(async move {
        info!("Dedup stage started");

        let mut dedup_cache: HashMap<Hash, HashSet<Hash>> = initial_dedup_cache;

        loop {
            // Before blocking on select, drain any already-pending blockhash
            // updates so the live set is current.
            ingest_blockhashes(
                None,
                &mut settled_blockhashes_rx,
                &live_blockhashes_clone,
                &mut dedup_cache,
                max_blockhashes,
            );

            // Exits when the input closes, never on a shutdown signal: waiting on
            // the settled-blockhash channel instead would deadlock, because the
            // settler cannot finish until this stage drains into it.
            tokio::select! {
                biased;

                // Blockhash updates have priority over transaction processing.
                // When both channels are ready, `biased` ensures we ingest new
                // blockhashes before checking transactions.
                result = settled_blockhashes_rx.recv() => {
                    match result {
                        Some(blockhash) => {
                            // Apply the hash we just received along with any
                            // others that arrived in the meantime, under a
                            // single write lock.
                            ingest_blockhashes(
                                Some(blockhash),
                                &mut settled_blockhashes_rx,
                                &live_blockhashes_clone,
                                &mut dedup_cache,
                                max_blockhashes,
                            );
                        }
                        None => {
                            warn!("Dedup settled blockhashes channel closed, shutting down");
                            break;
                        }
                    }
                }

                // Process incoming transactions.
                //
                // The output channel (`output_tx`) is bounded, so `send().await`
                // can block when the sequencer stage is saturated.  While this
                // task is suspended on that await, new blockhash updates pile up
                // in `settled_blockhashes_rx` and the live-hash window falls
                // behind what `getLatestBlockhash` returns to clients.
                //
                // To avoid this, we race the send against incoming blockhash
                // updates using a nested `tokio::select!`.  When a new blockhash
                // arrives while we're waiting to send, we ingest it immediately,
                // then re-check the send.  The transaction is only forwarded once
                // the channel has capacity; blockhashes are never delayed.
                result = input_rx.recv() => {
                    match result {
                        Some(transaction) => {
                            metrics.dedup_received();
                            heartbeat.record_input();
                            // Key replay on the message hash, not the first signature.
                            // One message can have many valid signatures, so keying on
                            // the signature would let a sponsor replay the same signed
                            // message. The message hash is the same across those signature
                            // variants. Dedup is single-threaded and runs after sigverify,
                            // so check-and-insert is atomic and only caches verified txs.
                            let message_hash = *transaction.message_hash();
                            let signature = *transaction.signature();
                            let blockhash = *transaction.message().recent_blockhash();

                            // Drain any blockhash updates that arrived while we
                            // were processing the previous transaction (or while
                            // output_tx.send() was awaiting).
                            ingest_blockhashes(
                                None,
                                &mut settled_blockhashes_rx,
                                &live_blockhashes_clone,
                                &mut dedup_cache,
                                max_blockhashes,
                            );

                            if !live_blockhashes_clone.read()
                                .expect("blockhash lock poisoned")
                                .contains(&blockhash) {
                                metrics.dedup_dropped_unknown_blockhash();
                                warn!("Blockhash {} not found in live blockhashes", blockhash);
                                continue;
                            }

                            // Check if duplicate using two-layer lookup
                            let is_duplicate = dedup_cache
                                .get(&blockhash)
                                .map(|hashes| hashes.contains(&message_hash))
                                .unwrap_or(false);

                            if is_duplicate {
                                metrics.dedup_dropped_duplicate();
                                warn!("Duplicate transaction detected: {} (blockhash: {})", signature, blockhash);
                                continue;
                            }

                            // Add to cache
                            dedup_cache
                                .entry(blockhash)
                                .or_default()
                                .insert(message_hash);

                            metrics.dedup_forwarded();

                            // Forward to the sequencer.  While waiting for capacity
                            // on the bounded output channel, keep draining blockhash
                            // updates so the live set stays current even when
                            // backpressure stalls the pipeline.
                            loop {
                                tokio::select! {
                                    biased;
                                    bh = settled_blockhashes_rx.recv() => {
                                        match bh {
                                            Some(bh) => {
                                                ingest_blockhashes(
                                                    Some(bh),
                                                    &mut settled_blockhashes_rx,
                                                    &live_blockhashes_clone,
                                                    &mut dedup_cache,
                                                    max_blockhashes,
                                                );
                                                // Loop back to retry the send.
                                            }
                                            None => {
                                                warn!("Dedup settled blockhashes channel closed");
                                                // Fall through — the outer loop
                                                // will detect the closed channel.
                                                break;
                                            }
                                        }
                                    }
                                    send_result = output_tx.send(transaction.clone()) => {
                                        if let Err(e) = send_result {
                                            warn!("Failed to forward transaction to sequencer: {}", e);
                                        } else {
                                            heartbeat.record_progress();
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                        None => {
                            warn!("Dedup input channel closed, shutting down");
                            break;
                        }
                    }
                }
            }
        }

        info!("Dedup stopped");
    });

    (
        WorkerHandle::new("Dedup".to_string(), handle),
        live_blockhashes,
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{accounts::traits::BlockInfo, stage_metrics::NoopMetrics},
        solana_sdk::{
            hash::Hash,
            message::Message,
            pubkey::Pubkey,
            signature::{Keypair, Signature, Signer},
            transaction::{SanitizedTransaction, Transaction},
        },
        solana_system_interface::instruction as system_instruction,
        std::{collections::HashSet, time::Duration},
    };

    // --- helpers shared by both suites ---

    fn make_tx(payer: &Keypair, blockhash: Hash) -> SanitizedTransaction {
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&payer.pubkey(), &to, 1);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer], msg, blockhash);
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap()
    }

    /// Build two sanitized transactions that share one signed message but carry
    /// different first signatures. Sanitization derives message_hash from the
    /// message and never verifies signatures, so both share message_hash and
    /// differ only in signatures[0]. This is the dedup-stage stand-in for a
    /// malicious sponsor replaying one victim authorization under varied nonces.
    fn tx_with_same_message_diff_sig(
        payer: &Keypair,
        blockhash: Hash,
    ) -> (SanitizedTransaction, SanitizedTransaction) {
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&payer.pubkey(), &to, 1);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));

        let tx_a = Transaction::new(&[payer], msg.clone(), blockhash);
        let mut tx_b = tx_a.clone();
        // Swap only the first signature for another distinct value; the signed
        // message stays byte-identical so message_hash is unchanged.
        tx_b.signatures[0] = Signature::new_unique();

        let sanitized_a =
            SanitizedTransaction::try_from_legacy_transaction(tx_a, &HashSet::new()).unwrap();
        let sanitized_b =
            SanitizedTransaction::try_from_legacy_transaction(tx_b, &HashSet::new()).unwrap();
        (sanitized_a, sanitized_b)
    }

    // Each tuple is (signature, message_hash, recent_blockhash) so the block
    // carries the parallel arrays the restart rebuild keys on.
    fn make_block(slot: u64, blockhash: Hash, txs: &[(Signature, Hash, Hash)]) -> BlockInfo {
        make_block_at(slot, blockhash, txs, None)
    }

    fn make_block_at(
        slot: u64,
        blockhash: Hash,
        txs: &[(Signature, Hash, Hash)],
        block_time: Option<i64>,
    ) -> BlockInfo {
        BlockInfo {
            slot,
            blockhash,
            previous_blockhash: Hash::default(),
            parent_slot: slot.saturating_sub(1),
            block_height: Some(slot),
            block_time,
            transaction_signatures: txs.iter().map(|(s, _, _)| *s).collect(),
            transaction_message_hashes: txs.iter().map(|(_, m, _)| *m).collect(),
            transaction_recent_blockhashes: txs.iter().map(|(_, _, h)| *h).collect(),
        }
    }

    const TEST_INGRESS_CAP: usize = 64;

    /// Spin up the dedup stage and return the handles needed for driving it.
    fn start_test_dedup() -> (
        mpsc::Sender<SanitizedTransaction>,
        mpsc::UnboundedSender<Hash>,
        mpsc::Receiver<SanitizedTransaction>,
    ) {
        let (input_tx, input_rx) = mpsc::channel(TEST_INGRESS_CAP);
        let (bh_tx, bh_rx) = mpsc::unbounded_channel();
        let (output_tx, output_rx) = mpsc::channel(64);

        let args = DedupArgs {
            max_blockhashes: 8,
            input_rx,
            settled_blockhashes_rx: bh_rx,
            output_tx,
            initial_live_blockhashes: LinkedList::new(),
            initial_dedup_cache: HashMap::new(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        };
        tokio::spawn(async move {
            start_dedup(args).await;
        });

        (input_tx, bh_tx, output_rx)
    }

    // --- live dedup stage tests ---

    #[tokio::test]
    async fn unknown_blockhash_rejected() {
        let (input_tx, bh_tx, mut output_rx) = start_test_dedup();

        let live_bh = Hash::new_unique();
        bh_tx.send(live_bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        let unknown_bh = Hash::new_unique();
        let tx = make_tx(&payer, unknown_bh);
        input_tx.send(tx).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), output_rx.recv()).await;
        assert!(
            result.is_err(),
            "tx with unknown blockhash should not be forwarded"
        );

        drop(input_tx);
    }

    // An identical resubmit (same message, same signature) is still deduped, so
    // the re-key does not regress the original duplicate-drop behavior.
    #[tokio::test]
    async fn identical_resubmit_rejected() {
        let (input_tx, bh_tx, mut output_rx) = start_test_dedup();

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        let tx = make_tx(&payer, bh);

        input_tx.send(tx.clone()).await.unwrap();
        let first = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        assert!(first.is_ok(), "first tx should be forwarded");

        input_tx.send(tx).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(100), output_rx.recv()).await;
        assert!(second.is_err(), "duplicate tx should not be forwarded");

        drop(input_tx);
    }

    // Two distinct transfers under the same live blockhash have different
    // messages, so different message hashes; both must be forwarded. Guards
    // against false-positive dedup of legitimate distinct transactions.
    #[tokio::test]
    async fn distinct_messages_both_forwarded() {
        let (input_tx, bh_tx, mut output_rx) = start_test_dedup();

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        // make_tx sends to a fresh random destination each call, so the two
        // messages differ and hash differently.
        let tx1 = make_tx(&payer, bh);
        let tx2 = make_tx(&payer, bh);
        assert_ne!(
            tx1.message_hash(),
            tx2.message_hash(),
            "distinct transfers must differ in message_hash"
        );

        input_tx.send(tx1).await.unwrap();
        let first = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        assert!(first.is_ok(), "first distinct tx should be forwarded");

        input_tx.send(tx2).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        assert!(
            second.is_ok(),
            "second distinct tx must also be forwarded, not deduped"
        );

        drop(input_tx);
    }

    // Regression for the first-signature replay: a second variant that shares
    // the signed message but carries a different first signature must be
    // dropped as a duplicate. Fails on signature-keyed dedup, which forwards it.
    #[tokio::test]
    async fn varied_signature_same_message_rejected() {
        let (input_tx, bh_tx, mut output_rx) = start_test_dedup();

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        let (tx_a, tx_b) = tx_with_same_message_diff_sig(&payer, bh);
        assert_eq!(
            tx_a.message_hash(),
            tx_b.message_hash(),
            "variants must share message_hash"
        );
        assert_ne!(
            tx_a.signature(),
            tx_b.signature(),
            "variants must differ in first signature"
        );

        input_tx.send(tx_a).await.unwrap();
        let first = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        assert!(first.is_ok(), "first variant should be forwarded");

        input_tx.send(tx_b).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(100), output_rx.recv()).await;
        assert!(
            second.is_err(),
            "second variant sharing the message must be deduped"
        );

        drop(input_tx);
    }

    #[tokio::test]
    async fn valid_transaction_forwarded() {
        let (input_tx, bh_tx, mut output_rx) = start_test_dedup();

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        let tx = make_tx(&payer, bh);
        let expected_sig = *tx.signature();

        input_tx.send(tx).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        match result {
            Ok(Some(forwarded)) => {
                assert_eq!(*forwarded.signature(), expected_sig);
            }
            other => panic!("expected forwarded tx, got {:?}", other),
        }

        drop(input_tx);
    }

    #[tokio::test]
    async fn expired_blockhash_evicted() {
        let (input_tx, bh_tx, mut output_rx) = start_test_dedup();

        let mut hashes = Vec::new();
        for _ in 0..9 {
            let h = Hash::new_unique();
            hashes.push(h);
            bh_tx.send(h).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(30)).await;

        let payer = Keypair::new();
        let tx = make_tx(&payer, hashes[0]);
        input_tx.send(tx).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(100), output_rx.recv()).await;
        assert!(
            result.is_err(),
            "tx using evicted blockhash should not be forwarded"
        );

        let tx2 = make_tx(&payer, hashes[8]);
        input_tx.send(tx2).await.unwrap();
        let result2 = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        assert!(
            result2.is_ok(),
            "tx using latest blockhash should be forwarded"
        );

        drop(input_tx);
    }

    /// The window is block-denominated: one hash arrives per produced block and
    /// each one evicts exactly one older entry, taking its replay-protection
    /// cache entry with it. Validity and replay protection are the same window.
    #[test]
    fn dedup_evicts_one_entry_per_block() {
        use std::sync::RwLock;

        let max_blockhashes = 3usize;
        let live = RwLock::new(LinkedList::new());
        let mut cache: HashMap<Hash, HashSet<Hash>> = HashMap::new();

        let hashes: Vec<Hash> = (0..5).map(|_| Hash::new_unique()).collect();
        for hash in &hashes {
            cache.insert(*hash, HashSet::from([Hash::new_unique()]));
            let (_tx, mut rx) = mpsc::unbounded_channel();
            rx.close();
            ingest_blockhashes(Some(*hash), &mut rx, &live, &mut cache, max_blockhashes);
        }

        let window: Vec<Hash> = live.read().unwrap().iter().copied().collect();
        assert_eq!(window, hashes[2..], "the window holds the newest blocks");
        for evicted in &hashes[..2] {
            assert!(
                !cache.contains_key(evicted),
                "an evicted blockhash takes its message hashes with it"
            );
        }
        for kept in &hashes[2..] {
            assert!(cache.contains_key(kept), "a live blockhash keeps its entry");
        }
    }

    /// The bound is the number of live blocks, and block cadence is not an input
    /// to it. Under load many hashes arrive per ingest and at idle one does; the
    /// retained set is identical either way.
    #[test]
    fn dedup_footprint_is_bounded_by_the_window_not_by_cadence() {
        use std::sync::RwLock;

        let max_blockhashes = 4usize;
        let per_block = 3usize;

        // Same blocks, same message hashes; only how many arrive per ingest
        // differs, which is the whole of the load-to-idle transition.
        let hashes: Vec<Hash> = (0..12).map(|_| Hash::new_unique()).collect();
        let mut retained = Vec::new();
        for burst in [4usize, 1] {
            let live = RwLock::new(LinkedList::new());
            let mut cache: HashMap<Hash, HashSet<Hash>> = HashMap::new();

            for chunk in hashes.chunks(burst) {
                let (tx, mut rx) = mpsc::unbounded_channel();
                for hash in chunk {
                    cache.insert(*hash, (0..per_block).map(|_| Hash::new_unique()).collect());
                    tx.send(*hash).expect("queue the settled blockhash");
                }
                drop(tx);
                ingest_blockhashes(None, &mut rx, &live, &mut cache, max_blockhashes);
            }

            assert_eq!(
                cache.len(),
                max_blockhashes,
                "the cache holds one entry per live block, never more"
            );
            assert_eq!(
                cache.values().map(|set| set.len()).sum::<usize>(),
                max_blockhashes * per_block,
                "an evicted block takes its message hashes with it"
            );
            let window: Vec<Hash> = live.read().unwrap().iter().copied().collect();
            assert_eq!(window, hashes[hashes.len() - max_blockhashes..]);
            retained.push(window);
        }

        assert_eq!(
            retained[0], retained[1],
            "block cadence must not change what the window retains"
        );
    }

    // --- build_dedup_state unit tests ---

    #[test]
    fn test_empty_blocks_returns_empty_state() {
        let (live, cache) = build_dedup_state(&[]).unwrap();
        assert!(live.is_empty());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_single_block_no_transactions() {
        let hash = Hash::new_unique();
        let block = make_block(1, hash, &[]);
        let (live, cache) = build_dedup_state(&[block]).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(*live.front().unwrap(), hash);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_transactions_referencing_in_window_hash_are_cached() {
        let hash1 = Hash::new_unique();
        let hash2 = Hash::new_unique();
        let sig1 = Signature::new_unique();
        let sig2 = Signature::new_unique();
        let mh1 = Hash::new_unique();
        let mh2 = Hash::new_unique();

        let blocks = vec![
            make_block(1, hash1, &[]),
            make_block(2, hash2, &[(sig1, mh1, hash1), (sig2, mh2, hash1)]),
        ];
        let (live, cache) = build_dedup_state(&blocks).unwrap();

        assert_eq!(live.len(), 2);
        // The cache is keyed by message hash, not signature.
        let hashes = cache.get(&hash1).unwrap();
        assert!(hashes.contains(&mh1));
        assert!(hashes.contains(&mh2));
        assert!(!cache.contains_key(&hash2));
    }

    #[test]
    fn test_transactions_referencing_out_of_window_hash_are_filtered() {
        let old_hash = Hash::new_unique();
        let hash1 = Hash::new_unique();
        let sig = Signature::new_unique();
        let mh = Hash::new_unique();

        let blocks = vec![make_block(1, hash1, &[(sig, mh, old_hash)])];
        let (live, cache) = build_dedup_state(&blocks).unwrap();

        assert_eq!(live.len(), 1);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_mismatched_lengths_returns_error() {
        let mut block = make_block(1, Hash::new_unique(), &[]);
        block.transaction_signatures.push(Signature::new_unique());

        let result = build_dedup_state(&[block]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mismatched transaction_signatures"));
    }

    // The parallel-array invariant also covers message hashes: a block whose
    // message-hash count diverges from its signature count must be rejected,
    // so a corrupt row can never seed a wrong or empty replay cache.
    #[test]
    fn test_mismatched_message_hash_length_returns_error() {
        let hash = Hash::new_unique();
        let mut block = make_block(
            1,
            hash,
            &[(Signature::new_unique(), Hash::new_unique(), hash)],
        );
        // Drop the message hash so only that array is short.
        block.transaction_message_hashes.clear();

        let result = build_dedup_state(&[block]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mismatched transaction_message_hashes"));
    }

    #[test]
    fn test_multiple_blocks_all_hashes_in_live_list() {
        let hashes: Vec<Hash> = (0..5).map(|_| Hash::new_unique()).collect();
        let blocks: Vec<BlockInfo> = hashes
            .iter()
            .enumerate()
            .map(|(i, &h)| make_block(i as u64, h, &[]))
            .collect();

        let (live, _) = build_dedup_state(&blocks).unwrap();

        assert_eq!(live.len(), 5);
        for (got, expected) in live.iter().zip(hashes.iter()) {
            assert_eq!(got, expected);
        }
    }

    // --- reorder wiring + anti-poison integration tests ---

    /// Wire ingress (async_channel) -> sigverify -> (mpsc) dedup -> (mpsc)
    /// sequencer, exactly as the node builds the write pipeline. Returns the
    /// ingress sender, the settled-blockhash sender feeding dedup, the sequencer
    /// receiver, and the shutdown token.
    async fn start_test_pipeline() -> (
        async_channel::Sender<SanitizedTransaction>,
        mpsc::UnboundedSender<Hash>,
        mpsc::Receiver<SanitizedTransaction>,
        tokio_util::sync::CancellationToken,
    ) {
        use crate::stages::sigverify::{start_sigverify_workerpool, SigverifyArgs};

        let (ingress_tx, ingress_rx) = async_channel::bounded(64);
        let (dedup_tx, dedup_rx) = mpsc::channel(64);
        let (sequencer_tx, sequencer_rx) = mpsc::channel(64);
        let (bh_tx, bh_rx) = mpsc::unbounded_channel();
        let shutdown = tokio_util::sync::CancellationToken::new();

        start_sigverify_workerpool(SigverifyArgs {
            num_workers: 2,
            admin_keys: vec![],
            rx: ingress_rx,
            output_tx: dedup_tx,
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        tokio::spawn(async move {
            start_dedup(DedupArgs {
                max_blockhashes: 8,
                input_rx: dedup_rx,
                settled_blockhashes_rx: bh_rx,
                output_tx: sequencer_tx,
                initial_live_blockhashes: LinkedList::new(),
                initial_dedup_cache: HashMap::new(),
                metrics: Arc::new(NoopMetrics),
                heartbeat: crate::health::StageHeartbeat::new(),
            })
            .await;
        });

        (ingress_tx, bh_tx, sequencer_rx, shutdown)
    }

    // An invalid-signature transaction carrying message M is dropped by sigverify
    // and never reaches dedup, so a later valid transaction with the same message
    // M is forwarded, not falsely deduped. This fails if the cache is inserted
    // before verification (the pre-verify poisoning DoS).
    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_tx_does_not_poison_dedup() {
        let (ingress_tx, bh_tx, mut sequencer_rx, _shutdown) = start_test_pipeline().await;

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        // tx_a is properly signed (valid); tx_b shares the message but has a bogus
        // first signature, so sigverify rejects it.
        let (tx_a, tx_b) = tx_with_same_message_diff_sig(&payer, bh);
        let expected_sig = *tx_a.signature();

        // Send the invalid variant first; sigverify must drop it.
        ingress_tx.send(tx_b).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The valid variant with the same message must still be forwarded.
        ingress_tx.send(tx_a).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), sequencer_rx.recv()).await;
        match received {
            Ok(Some(tx)) => assert_eq!(
                *tx.signature(),
                expected_sig,
                "the valid tx must be the one forwarded"
            ),
            other => panic!("valid tx must not be deduped by a dropped invalid tx: {other:?}"),
        }

        drop(ingress_tx);
    }

    // A single valid transaction traverses the full reorder: ingress -> sigverify
    // -> dedup -> sequencer. Pins the channel retype and stage wiring.
    #[tokio::test(flavor = "multi_thread")]
    async fn valid_tx_flows_sigverify_to_sequencer() {
        let (ingress_tx, bh_tx, mut sequencer_rx, _shutdown) = start_test_pipeline().await;

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        let tx = make_tx(&payer, bh);
        let expected_sig = *tx.signature();

        ingress_tx.send(tx).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), sequencer_rx.recv()).await;
        match received {
            Ok(Some(tx)) => assert_eq!(*tx.signature(), expected_sig),
            other => panic!("valid tx must reach the sequencer: {other:?}"),
        }

        drop(ingress_tx);
    }

    /// The window counts blocks, not slots. A slot range that wide holds far fewer
    /// blocks on a sparse chain, so restoring by slot range would drop hashes the
    /// node had just published a `lastValidBlockHeight` for.
    #[tokio::test(flavor = "multi_thread")]
    async fn restart_restores_the_window_by_block_count_not_slot_range() {
        let (mut db, _pg) = crate::test_helpers::start_test_postgres().await;

        // One block every ten slots, which is what an idle node produces.
        let hashes: Vec<Hash> = (0..5).map(|_| Hash::new_unique()).collect();
        for (index, hash) in hashes.iter().enumerate() {
            db.store_block(make_block_at(index as u64 * 10, *hash, &[], Some(0)))
                .await
                .unwrap();
        }

        let (live, _cache) = load_dedup_state(&db, hashes.len()).await.unwrap();

        assert_eq!(
            live.len(),
            hashes.len(),
            "the last {} blocks must all be restored, whatever slots they occupy",
            hashes.len()
        );
    }

    /// Expiry is block-counted, so the restored window is the last
    /// `max_blockhashes` blocks whatever their age. An idle node's live hashes are
    /// minutes old, and a clock-based drop would reject hashes clients still hold.
    #[tokio::test(flavor = "multi_thread")]
    async fn restart_restores_the_window_by_block_count_not_age() {
        let (mut db, _pg) = crate::test_helpers::start_test_postgres().await;

        let hour_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 3_600;

        let hashes: Vec<Hash> = (0..3).map(|_| Hash::new_unique()).collect();
        for (slot, hash) in hashes.iter().enumerate() {
            db.store_block(make_block_at(slot as u64, *hash, &[], Some(hour_ago)))
                .await
                .unwrap();
        }

        let (live, _cache) = load_dedup_state(&db, 8).await.unwrap();

        assert_eq!(
            live.len(),
            hashes.len(),
            "every block inside the block-counted window must be restored"
        );
    }

    // --- persistence roundtrip (Postgres-gated) ---

    // Store new-format blocks (with message hashes) through store_block, then
    // load_dedup_state must rebuild a cache keyed by (blockhash, message_hash)
    // and a live_blockhashes list matching the stored blockhashes.
    #[tokio::test(flavor = "multi_thread")]
    async fn load_dedup_state_roundtrip_message_hash() {
        let (mut db, _pg) = crate::test_helpers::start_test_postgres().await;

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let bh0 = Hash::new_unique();
        let bh1 = Hash::new_unique();
        let sig = Signature::new_unique();
        let mh = Hash::new_unique();

        // Slot 0: no transactions, just establishes bh0 as a live blockhash.
        let block0 = BlockInfo {
            slot: 0,
            blockhash: bh0,
            previous_blockhash: Hash::default(),
            parent_slot: 0,
            block_height: Some(0),
            block_time: Some(now_secs),
            transaction_signatures: vec![],
            transaction_recent_blockhashes: vec![],
            transaction_message_hashes: vec![],
        };
        // Slot 1: one tx referencing bh0 as its recent blockhash.
        let block1 = BlockInfo {
            slot: 1,
            blockhash: bh1,
            previous_blockhash: bh0,
            parent_slot: 0,
            block_height: Some(1),
            block_time: Some(now_secs),
            transaction_signatures: vec![sig],
            transaction_recent_blockhashes: vec![bh0],
            transaction_message_hashes: vec![mh],
        };

        db.store_block(block0).await.unwrap();
        db.store_block(block1).await.unwrap();

        let (live, cache) = load_dedup_state(&db, 8).await.unwrap();

        assert!(live.contains(&bh0), "bh0 must be a live blockhash");
        assert!(live.contains(&bh1), "bh1 must be a live blockhash");
        let hashes = cache
            .get(&bh0)
            .expect("bh0 must key a dedup cache entry from the tx that referenced it");
        assert!(
            hashes.contains(&mh),
            "cache must hold the message hash, keyed by the referenced blockhash"
        );
    }

    /// Dedup exits when its input closes even while the settler is still alive
    /// and its blockhash channel is still open. Waiting on that channel instead
    /// would deadlock the drain: the settler cannot finish until dedup has
    /// drained into the sequencer, and dedup would be waiting on the settler.
    #[tokio::test(flavor = "multi_thread")]
    async fn dedup_exits_on_input_close_without_waiting_on_blockhashes() {
        let (input_tx, bh_tx, _output_rx) = start_test_dedup();

        // Held open for the whole test, standing in for a settler still running.
        let live = Hash::new_unique();
        bh_tx.send(live).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (probe_tx, mut probe_rx) = mpsc::channel::<()>(1);
        let watcher = tokio::spawn(async move {
            // Closing dedup's input is the only signal it should need.
            drop(input_tx);
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = probe_tx.send(()).await;
        });

        assert!(
            tokio::time::timeout(Duration::from_secs(5), probe_rx.recv())
                .await
                .is_ok(),
            "dedup must not block the drain on the settled-blockhash channel"
        );
        drop(bh_tx);
        let _ = watcher.await;
    }

    /// A window short of `max_blockhashes` is only a truncation problem when the
    /// blocks below it existed. Genesis in the window means the chain is young.
    #[test]
    fn dedup_window_is_short_only_when_truncation_cut_it() {
        // Newest first, as `get_last_blocks` returns them.
        let chain = |oldest_slot: u64, loaded: u64| -> Vec<BlockInfo> {
            (0..loaded)
                .rev()
                .map(|i| make_block(oldest_slot + i, Hash::new_unique(), &[]))
                .collect()
        };
        for (oldest_slot, loaded, max, short) in [
            (5, 3, 8, true),
            (0, 3, 8, false),
            (5, 8, 8, false),
            (5, 0, 8, false),
        ] {
            assert_eq!(
                dedup_window_is_short(&chain(oldest_slot, loaded), max),
                short,
                "{loaded} blocks from slot {oldest_slot} against a window of {max}"
            );
        }
    }
}
