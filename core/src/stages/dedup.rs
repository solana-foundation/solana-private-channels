use {
    crate::{
        accounts::traits::AccountsDB, accounts::traits::BlockInfo, health::StageHeartbeat,
        nodes::node::WorkerHandle, stage_metrics::SharedMetrics,
    },
    anyhow::{ensure, Result},
    solana_sdk::{hash::Hash, signature::Signature, transaction::SanitizedTransaction},
    std::{
        collections::{HashMap, HashSet, LinkedList},
        sync::{Arc, RwLock},
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
};

pub struct DedupArgs {
    pub max_blockhashes: usize,
    pub input_rx: mpsc::Receiver<SanitizedTransaction>,
    pub settled_blockhashes_rx: mpsc::UnboundedReceiver<Hash>,
    pub output_tx: async_channel::Sender<SanitizedTransaction>,
    pub shutdown_token: CancellationToken,
    /// Pre-populated from DB on startup; empty on a fresh node.
    pub initial_live_blockhashes: LinkedList<Hash>,
    /// Pre-populated from DB on startup; empty on a fresh node.
    pub initial_dedup_cache: HashMap<Hash, HashSet<Signature>>,
    pub metrics: SharedMetrics,
    pub heartbeat: Arc<StageHeartbeat>,
}

/// Create the bounded dedup channel pair; a full queue sheds load at RPC ingress.
pub fn create_dedup_channel(
    capacity: usize,
) -> (
    mpsc::Sender<SanitizedTransaction>,
    mpsc::Receiver<SanitizedTransaction>,
) {
    mpsc::channel(capacity)
}

/// Load dedup state from the DB to seed the cache on restart.
///
/// Reads the last `max_blockhashes` blocks and reconstructs:
/// - `live_blockhashes`: the ordered list of recent settled blockhashes
/// - `dedup_cache`: blockhash → set of signatures that used it as recent_blockhash
///
/// Returns empty state only on a fresh node (no metadata in DB yet).
/// Any DB query failure is propagated as an error — the caller must not
/// start the node with an empty cache when prior state exists, as that
/// could allow duplicate transactions to execute after a restart.
pub async fn load_dedup_state(
    accounts_db: &AccountsDB,
    max_blockhashes: usize,
    expiry_ms: u64,
) -> Result<DedupState> {
    let live_blockhashes: LinkedList<Hash> = LinkedList::new();
    let dedup_cache: HashMap<Hash, HashSet<Signature>> = HashMap::new();

    let latest_slot = match accounts_db.get_latest_slot().await? {
        Some(slot) => slot,
        None => {
            info!("Dedup: no prior blocks found, starting with empty state");
            return Ok((live_blockhashes, dedup_cache));
        }
    };

    let start_slot = latest_slot.saturating_sub((max_blockhashes as u64).saturating_sub(1));

    let blocks = accounts_db
        .get_blocks_in_range(start_slot, latest_slot)
        .await?;

    let loaded = blocks.len();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        // Checked cast; the i64::MAX fallback is unreachable
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let blocks = prune_expired_blocks(blocks, now_secs, expiry_ms);
    let pruned = loaded.saturating_sub(blocks.len());

    let (live_blockhashes, dedup_cache) = build_dedup_state(&blocks)?;

    info!(
        loaded_blocks = loaded,
        pruned_blocks = pruned,
        kept_blocks = blocks.len(),
        live_blockhashes = live_blockhashes.len(),
        cache_entries = dedup_cache.values().map(|s| s.len()).sum::<usize>(),
        "Dedup: restored dedup state; {pruned} restored blocks were pruned as older than the {expiry_ms}ms expiry window",
    );

    Ok((live_blockhashes, dedup_cache))
}

type DedupState = (LinkedList<Hash>, HashMap<Hash, HashSet<Signature>>);

/// Drop restored blocks older than the expiry window.
fn prune_expired_blocks(blocks: Vec<BlockInfo>, now_secs: i64, expiry_ms: u64) -> Vec<BlockInfo> {
    // Checked: an oversized expiry clamps to i64::MAX, never wraps negative (drop all).
    let expiry_secs = i64::try_from(expiry_ms / 1000).unwrap_or(i64::MAX);
    blocks
        .into_iter()
        .filter(|block| match block.block_time {
            Some(block_time) => now_secs.saturating_sub(block_time) <= expiry_secs,
            // Age unknown means we cannot prove expiry, so keep the block.
            None => true,
        })
        .collect()
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
    dedup_cache: &mut HashMap<Hash, HashSet<Signature>>,
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
    let mut dedup_cache: HashMap<Hash, HashSet<Signature>> = HashMap::new();

    let loaded_hashes: HashSet<Hash> = blocks.iter().map(|b| b.blockhash).collect();

    for block in blocks {
        ensure!(
            block.transaction_signatures.len() == block.transaction_recent_blockhashes.len(),
            "Block {} has mismatched transaction_signatures ({}) and transaction_recent_blockhashes ({}) lengths",
            block.slot,
            block.transaction_signatures.len(),
            block.transaction_recent_blockhashes.len(),
        );

        live_blockhashes.push_back(block.blockhash);

        for (signature, recent_blockhash) in block
            .transaction_signatures
            .iter()
            .zip(block.transaction_recent_blockhashes.iter())
        {
            if loaded_hashes.contains(recent_blockhash) {
                dedup_cache
                    .entry(*recent_blockhash)
                    .or_default()
                    .insert(*signature);
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
        shutdown_token,
        initial_live_blockhashes,
        initial_dedup_cache,
        metrics,
        heartbeat,
    } = args;

    let live_blockhashes = Arc::new(RwLock::new(initial_live_blockhashes));
    let live_blockhashes_clone = Arc::clone(&live_blockhashes);

    let handle = tokio::spawn(async move {
        info!("Dedup stage started");

        let mut dedup_cache: HashMap<Hash, HashSet<Signature>> = initial_dedup_cache;

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

            tokio::select! {
                biased;

                // Shutdown signal — checked first so shutdown is prompt.
                _ = shutdown_token.cancelled() => {
                    info!("Dedup received shutdown signal");
                    break;
                }

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
                // can block when the sigverify stage is saturated.  While this
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
                                .map(|sigs| sigs.contains(&signature))
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
                                .insert(signature);

                            metrics.dedup_forwarded();

                            // Forward to sigverify.  While waiting for capacity on
                            // the bounded output channel, keep draining blockhash
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
                                            warn!("Failed to forward transaction to sigverify: {}", e);
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

    fn make_block(slot: u64, blockhash: Hash, sigs: &[(Signature, Hash)]) -> BlockInfo {
        make_block_at(slot, blockhash, sigs, None)
    }

    fn make_block_at(
        slot: u64,
        blockhash: Hash,
        sigs: &[(Signature, Hash)],
        block_time: Option<i64>,
    ) -> BlockInfo {
        BlockInfo {
            slot,
            blockhash,
            previous_blockhash: Hash::default(),
            parent_slot: slot.saturating_sub(1),
            block_height: Some(slot),
            block_time,
            transaction_signatures: sigs.iter().map(|(s, _)| *s).collect(),
            transaction_recent_blockhashes: sigs.iter().map(|(_, h)| *h).collect(),
        }
    }

    // 15s window mirrored by the block ages below; bind both to one const so
    // the prune boundary and the test fixtures can never drift apart.
    const EXPIRY_MS: u64 = 15_000;

    const TEST_INGRESS_CAP: usize = 64;

    /// Spin up the dedup stage and return the handles needed for driving it.
    fn start_test_dedup() -> (
        mpsc::Sender<SanitizedTransaction>,
        mpsc::UnboundedSender<Hash>,
        async_channel::Receiver<SanitizedTransaction>,
        CancellationToken,
    ) {
        let (input_tx, input_rx) = mpsc::channel(TEST_INGRESS_CAP);
        let (bh_tx, bh_rx) = mpsc::unbounded_channel();
        let (output_tx, output_rx) = async_channel::bounded(64);
        let shutdown = CancellationToken::new();

        let args = DedupArgs {
            max_blockhashes: 8,
            input_rx,
            settled_blockhashes_rx: bh_rx,
            output_tx,
            shutdown_token: shutdown.clone(),
            initial_live_blockhashes: LinkedList::new(),
            initial_dedup_cache: HashMap::new(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        };
        tokio::spawn(async move {
            start_dedup(args).await;
        });

        (input_tx, bh_tx, output_rx, shutdown)
    }

    // --- live dedup stage tests ---

    #[tokio::test]
    async fn unknown_blockhash_rejected() {
        let (input_tx, bh_tx, output_rx, shutdown) = start_test_dedup();

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

        shutdown.cancel();
    }

    #[tokio::test]
    async fn duplicate_signature_rejected() {
        let (input_tx, bh_tx, output_rx, shutdown) = start_test_dedup();

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

        shutdown.cancel();
    }

    #[tokio::test]
    async fn valid_transaction_forwarded() {
        let (input_tx, bh_tx, output_rx, shutdown) = start_test_dedup();

        let bh = Hash::new_unique();
        bh_tx.send(bh).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payer = Keypair::new();
        let tx = make_tx(&payer, bh);
        let expected_sig = *tx.signature();

        input_tx.send(tx).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(200), output_rx.recv()).await;
        match result {
            Ok(Ok(forwarded)) => {
                assert_eq!(*forwarded.signature(), expected_sig);
            }
            other => panic!("expected forwarded tx, got {:?}", other),
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn expired_blockhash_evicted() {
        let (input_tx, bh_tx, output_rx, shutdown) = start_test_dedup();

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

        shutdown.cancel();
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

        let blocks = vec![
            make_block(1, hash1, &[]),
            make_block(2, hash2, &[(sig1, hash1), (sig2, hash1)]),
        ];
        let (live, cache) = build_dedup_state(&blocks).unwrap();

        assert_eq!(live.len(), 2);
        let sigs = cache.get(&hash1).unwrap();
        assert!(sigs.contains(&sig1));
        assert!(sigs.contains(&sig2));
        assert!(!cache.contains_key(&hash2));
    }

    #[test]
    fn test_transactions_referencing_out_of_window_hash_are_filtered() {
        let old_hash = Hash::new_unique();
        let hash1 = Hash::new_unique();
        let sig = Signature::new_unique();

        let blocks = vec![make_block(1, hash1, &[(sig, old_hash)])];
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

    // --- prune_expired_blocks unit tests ---

    #[test]
    fn prune_drops_blocks_older_than_expiry() {
        let now = 1_000_000i64;
        let old = make_block_at(1, Hash::new_unique(), &[], Some(now - 20));
        let fresh = make_block_at(2, Hash::new_unique(), &[], Some(now - 5));
        let fresh_hash = fresh.blockhash;

        let kept = prune_expired_blocks(vec![old, fresh], now, EXPIRY_MS);

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].blockhash, fresh_hash);
    }

    #[test]
    fn prune_keeps_all_when_within_expiry() {
        let now = 1_000_000i64;
        let blocks = vec![
            make_block_at(1, Hash::new_unique(), &[], Some(now - 1)),
            make_block_at(2, Hash::new_unique(), &[], Some(now - 1)),
        ];

        let kept = prune_expired_blocks(blocks, now, EXPIRY_MS);

        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn prune_keeps_none_when_all_stale() {
        let now = 1_000_000i64;
        let blocks = vec![
            make_block_at(1, Hash::new_unique(), &[], Some(now - 3600)),
            make_block_at(2, Hash::new_unique(), &[], Some(now - 3600)),
        ];

        let kept = prune_expired_blocks(blocks, now, EXPIRY_MS);

        assert!(kept.is_empty());
    }

    #[test]
    fn prune_keeps_block_time_none() {
        let now = 1_000_000i64;
        let blocks = vec![make_block_at(1, Hash::new_unique(), &[], None)];

        let kept = prune_expired_blocks(blocks, now, EXPIRY_MS);

        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn prune_handles_future_block_time() {
        let now = 1_000_000i64;
        let blocks = vec![make_block_at(1, Hash::new_unique(), &[], Some(now + 3600))];

        let kept = prune_expired_blocks(blocks, now, EXPIRY_MS);

        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn prune_then_build_drops_orphan_signatures() {
        let now = 1_000_000i64;
        let stale_hash = Hash::new_unique();
        let fresh_hash = Hash::new_unique();
        let stale_sig = Signature::new_unique();
        let fresh_sig = Signature::new_unique();

        // Stale block carries a self-referencing signature; fresh block too.
        let stale = make_block_at(1, stale_hash, &[(stale_sig, stale_hash)], Some(now - 3600));
        let fresh = make_block_at(2, fresh_hash, &[(fresh_sig, fresh_hash)], Some(now - 1));

        let kept = prune_expired_blocks(vec![stale, fresh], now, EXPIRY_MS);
        let (live, cache) = build_dedup_state(&kept).unwrap();

        assert_eq!(live.len(), 1);
        assert_eq!(*live.front().unwrap(), fresh_hash);
        assert!(!cache.contains_key(&stale_hash));
        assert!(cache.get(&fresh_hash).unwrap().contains(&fresh_sig));
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
}
