use crate::metrics;
use crate::{config::ProgramType, error::CheckpointError, storage::Storage};
use private_channel_metrics::MetricLabel;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{info, warn};

/// Gated ticks with no frontier advance before a stall warning fires (~15s at 5s/tick).
const STALL_WARN_TICKS: u32 = 3;

/// Checkpoint update message sent by transaction processor
/// Indicates that a slot has been fully processed (transactions saved or confirmed empty)
#[derive(Debug, Clone)]
pub struct CheckpointUpdate {
    pub program_type: ProgramType,
    pub slot: u64,
}

/// Per-program-type checkpoint progress.
///
/// `frontier` is the contiguous, fully-processed prefix and equals the value
/// persisted to storage — it never advances past a slot that is not yet durably
/// processed. While gated to a backfill range `(from_slot, target]`, out-of-range
/// and out-of-order updates are staged in `completed` and only fold into
/// `frontier` once they make it literally contiguous, so a missing slot cannot be
/// leapfrogged.
struct CheckpointState {
    // Highest contiguous fully-processed slot; the value persisted to storage.
    frontier: u64,
    // Processed-but-not-yet-contiguous slots in `(frontier, target]`, awaiting the fold.
    completed: HashSet<u64>,
    // Backfill target `T0` while gated; `None` once handed off / ungated (plain max).
    gate: Option<u64>,
    // True when `frontier` advanced since the last successful flush (so flush has work).
    dirty: bool,
    // Consecutive gated ticks with no frontier advance, for the stall warning.
    stalled_ticks: u32,
}

impl CheckpointState {
    fn ungated() -> Self {
        Self {
            frontier: 0,
            completed: HashSet::new(),
            gate: None,
            dirty: false,
            stalled_ticks: 0,
        }
    }

    /// Gated state seeded at backfill's effective `from_slot`. Seeding the frontier
    /// from `from_slot` (not a bare DB read) is required because a configured
    /// `start_slot` can push `from_slot` above the stored checkpoint; seeding lower
    /// would stall the frontier on slots that backfill will never emit.
    fn gated(from_slot: u64, target: u64) -> Self {
        Self {
            frontier: from_slot,
            completed: HashSet::new(),
            gate: Some(target),
            dirty: false,
            stalled_ticks: 0,
        }
    }

    /// Record that `slot` is fully processed, advance `frontier` (the durable
    /// checkpoint), and return whether it moved.
    ///
    /// When ungated, or after a backfill gap has been filled, `frontier` just tracks
    /// the highest slot seen. While a gap is still open it advances only across
    /// contiguous slots, so a slot that hasn't arrived yet can never be skipped.
    fn apply(&mut self, slot: u64) -> bool {
        let before = self.frontier;

        match self.gate {
            // Still filling a backfill gap — advance only across contiguous slots.
            Some(target) if self.frontier < target => self.fill_gap(slot, target),
            // Ungated, or the gap is filled — track the highest slot seen.
            _ => self.frontier = self.frontier.max(slot),
        }

        let advanced = self.frontier > before;
        // If the frontier moved, mark it so the next flush persists it.
        self.dirty |= advanced;
        advanced
    }

    /// While gated, pull `frontier` up across the contiguous run of processed slots,
    /// parking out-of-order slots in `completed` until the ones before them arrive.
    fn fill_gap(&mut self, slot: u64, target: u64) {
        // Only slots inside the open gap `(frontier, target]` matter; a lower one is
        // already covered, a higher one is a live tip whose row persists regardless.
        let in_gap = self.frontier < slot && slot <= target;
        if !in_gap {
            return;
        }

        // Record the slot, then advance the frontier over each now-contiguous slot.
        self.completed.insert(slot);
        while self.completed.remove(&(self.frontier + 1)) {
            self.frontier += 1;
        }

        // Gap fully closed — drop the staging set; later slots use the plain-max path.
        if self.frontier >= target {
            self.completed.clear();
        }
    }

    /// Slots left to fill while gated (`target - frontier`), saturating to 0 post-handoff.
    fn lag(&self) -> u64 {
        match self.gate {
            Some(t0) => t0.saturating_sub(self.frontier),
            None => 0,
        }
    }
}

/// Checkpoint writer service that batches and persists checkpoint updates
pub struct CheckpointWriter {
    storage: Arc<Storage>,
    batch_interval_secs: u64,
    max_batch_size: usize,
    // Backfill range `(from_slot, target]` each new program state is gated to; `None` runs ungated.
    gate: Option<(u64, u64)>,
}

impl CheckpointWriter {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            batch_interval_secs: 5, // Write every 5 seconds
            max_batch_size: 100,    // Or every 100 updates
            gate: None,
        }
    }

    /// Gate the frontier to the backfill range `(from_slot, target]` (from exclusive, target inclusive) so the checkpoint can't cross the unfilled gap.
    pub fn with_gate(mut self, from_slot: u64, target: u64) -> Self {
        self.gate = Some((from_slot, target));
        self
    }

    pub fn with_batch_interval(mut self, seconds: u64) -> Self {
        self.batch_interval_secs = seconds;
        self
    }

    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    fn new_state(&self) -> CheckpointState {
        match self.gate {
            Some((from_slot, target)) => CheckpointState::gated(from_slot, target),
            None => CheckpointState::ungated(),
        }
    }

    /// Start the checkpoint writer service
    /// Spawns a background task that listens for checkpoint updates and batches writes to DB
    pub fn start(self, mut rx: mpsc::Receiver<CheckpointUpdate>) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "Starting CheckpointWriter service (batch interval: {}s, max batch size: {}, gated: {})",
                self.batch_interval_secs,
                self.max_batch_size,
                self.gate.is_some()
            );

            let mut states: HashMap<ProgramType, CheckpointState> = HashMap::new();
            let mut update_count = 0;

            let mut ticker = interval(Duration::from_secs(self.batch_interval_secs));
            ticker.tick().await; // First tick completes immediately

            loop {
                tokio::select! {
                    update = rx.recv() => {
                        match update {
                            Some(update) => {
                                self.record_update(&mut states, update);

                                update_count += 1;

                                if update_count >= self.max_batch_size {
                                    self.flush_checkpoints(&mut states).await;
                                    update_count = 0;
                                }
                            }
                            None => {
                                info!("Checkpoint channel closed, flushing remaining checkpoints");
                                self.flush_checkpoints(&mut states).await;
                                break;
                            }
                        }
                    }

                    _ = ticker.tick() => {
                        Self::warn_on_stall(&mut states);
                        self.flush_checkpoints(&mut states).await;
                        update_count = 0;
                    }
                }
            }

            info!("CheckpointWriter service stopped");
        })
    }

    fn record_update(
        &self,
        states: &mut HashMap<ProgramType, CheckpointState>,
        update: CheckpointUpdate,
    ) {
        let state = states
            .entry(update.program_type)
            .or_insert_with(|| self.new_state());
        state.apply(update.slot);
        metrics::INDEXER_CHECKPOINT_FRONTIER_LAG
            .with_label_values(&[update.program_type.as_label()])
            .set(state.lag() as f64);
    }

    /// Re-warn every `STALL_WARN_TICKS` ticks that a gated frontier stays frozen, and refresh the lag gauge so it stays live when no updates arrive.
    fn warn_on_stall(states: &mut HashMap<ProgramType, CheckpointState>) {
        for (&program_type, state) in states.iter_mut() {
            metrics::INDEXER_CHECKPOINT_FRONTIER_LAG
                .with_label_values(&[program_type.as_label()])
                .set(state.lag() as f64);
            if state.gate.is_none() || state.lag() == 0 || state.dirty {
                state.stalled_ticks = 0;
                continue;
            }
            state.stalled_ticks += 1;
            // Re-fire periodically (not just once) so a hours-long stall keeps logging.
            if state.stalled_ticks % STALL_WARN_TICKS == 0 {
                warn!(
                    ?program_type,
                    frontier = state.frontier,
                    t0 = state.gate.unwrap_or_default(),
                    lag = state.lag(),
                    "checkpoint frontier stalled while gated; backfill blocked on a missing or unprocessed slot"
                );
            }
        }
    }

    /// Persist each dirty program type's contiguous frontier, clearing `dirty` only on a
    /// successful write. A failed write logs and leaves `dirty` set so the next tick retries.
    async fn flush_checkpoints(&self, states: &mut HashMap<ProgramType, CheckpointState>) {
        for (&program_type, state) in states.iter_mut() {
            if !state.dirty {
                continue;
            }
            let program_type_str = format!("{:?}", program_type).to_lowercase();

            match self
                .storage
                .update_committed_checkpoint(&program_type_str, state.frontier)
                .await
            {
                Ok(_) => {
                    info!(
                        "Checkpoint updated: {:?} -> slot {}",
                        program_type, state.frontier
                    );
                    state.dirty = false;
                }
                Err(e) => {
                    warn!(
                        "Failed to update checkpoint for {:?} at slot {}: {}",
                        program_type, state.frontier, e
                    );
                }
            }
        }
    }
}

/// Helper to get the last checkpoint for a program type
pub async fn get_last_checkpoint(
    storage: &Arc<Storage>,
    program_type: ProgramType,
) -> Result<u64, CheckpointError> {
    let program_type_str = format!("{:?}", program_type).to_lowercase();
    let checkpoint = storage
        .get_committed_checkpoint(&program_type_str)
        .await?
        .unwrap_or(0);

    info!("Last checkpoint for {:?}: {}", program_type, checkpoint);
    Ok(checkpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::storage::mock::MockStorage;

    /// Backfill range boundaries shared across the gated tests: `FROM` is
    /// exclusive (last durable checkpoint), `T0` is inclusive (last slot backfill
    /// must fill), so the gated range is the closed interval `[FROM+1, T0]`.
    const FROM: u64 = 100;
    const T0: u64 = 110;

    /// Apply a sequence of slot updates to a state and return the resulting frontier.
    fn drive(state: &mut CheckpointState, slots: &[u64]) -> u64 {
        for &slot in slots {
            state.apply(slot);
        }
        state.frontier
    }

    /// Build the pending-state map a flush test expects: one dirty ungated state
    /// per (program_type, frontier), matching how the writer stages updates.
    fn pending_states(entries: &[(ProgramType, u64)]) -> HashMap<ProgramType, CheckpointState> {
        let mut states = HashMap::new();
        for &(program_type, slot) in entries {
            let mut state = CheckpointState::ungated();
            state.apply(slot);
            states.insert(program_type, state);
        }
        states
    }

    // ============================================================================
    // Gated-contiguous frontier Tests
    // ============================================================================

    #[test]
    fn gated_drops_live_tip_above_t0() {
        let mut state = CheckpointState::gated(FROM, T0);
        // 101,102 fold contiguously; the live tip 1_000_000 is > T0 → dropped.
        assert_eq!(drive(&mut state, &[101, 102, 1_000_000]), 102);
        assert!(state.dirty);
    }

    /// The whole gated range folds the frontier up to exactly T0, after which it
    /// hands off and a later live-tip slot advances via plain max.
    #[test]
    fn gated_fills_range_then_hands_off_to_max() {
        let mut state = CheckpointState::gated(FROM, T0);
        let full: Vec<u64> = (FROM + 1..=T0).collect();
        assert_eq!(drive(&mut state, &full), T0);
        assert_eq!(drive(&mut state, &[T0 + 50]), T0 + 50);
    }

    /// A hole at 103 must freeze the frontier at 102 even though 104..=110 and a live tip arrive after it.
    #[test]
    fn gated_stalls_on_hole_no_leapfrog() {
        let mut state = CheckpointState::gated(FROM, T0);
        let mut slots = vec![101, 102];
        slots.extend(104..=110);
        slots.push(1_000_000);
        assert_eq!(drive(&mut state, &slots), 102);
    }

    #[test]
    fn gated_out_of_order_within_range() {
        let mut state = CheckpointState::gated(FROM, T0);
        // 103 arrives before 101/102; frontier only reaches 103 once all three are present.
        assert_eq!(drive(&mut state, &[103]), FROM);
        assert_eq!(drive(&mut state, &[101]), 101);
        assert_eq!(drive(&mut state, &[102]), 103);
    }

    /// With start_slot configured, the gate's `from` = max(start_slot-1, checkpoint);
    /// the frontier must seed at that `from`, not the lower DB checkpoint, or it
    /// would stall on slots backfill never emits.
    #[test]
    fn seed_respects_start_slot() {
        const START_SLOT: u64 = 200;
        const DB_CHECKPOINT: u64 = 100;
        let from = (START_SLOT - 1).max(DB_CHECKPOINT);
        let mut state = CheckpointState::gated(from, from + 5);
        assert_eq!(state.frontier, START_SLOT - 1);
        assert_eq!(drive(&mut state, &[START_SLOT]), START_SLOT);
    }

    /// Regression contract: ungated state is byte-for-byte today's max-of-seen.
    #[test]
    fn ungated_is_pure_max() {
        let mut state = CheckpointState::ungated();
        assert_eq!(drive(&mut state, &[300, 100]), 300);
    }

    #[test]
    fn lag_gauge_saturating() {
        let mut state = CheckpointState::gated(FROM, T0);
        let full: Vec<u64> = (FROM + 1..=T0).collect();
        drive(&mut state, &full);
        drive(&mut state, &[T0 + 50]);
        // frontier (T0+50) > target (T0): saturating_sub must report 0, not wrap.
        assert_eq!(state.lag(), 0);
    }

    // ============================================================================
    // Builder Tests
    // ============================================================================

    #[test]
    fn test_builder_with_batch_interval() {
        let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = CheckpointWriter::new(storage).with_batch_interval(10);

        assert_eq!(writer.batch_interval_secs, 10);
    }

    #[test]
    fn test_builder_with_max_batch_size() {
        let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = CheckpointWriter::new(storage).with_max_batch_size(50);

        assert_eq!(writer.max_batch_size, 50);
    }

    #[test]
    fn test_builder_chaining() {
        let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = CheckpointWriter::new(storage)
            .with_batch_interval(15)
            .with_max_batch_size(75);

        assert_eq!(writer.batch_interval_secs, 15);
        assert_eq!(writer.max_batch_size, 75);
    }

    // ============================================================================
    // flush_checkpoints Tests
    // ============================================================================

    #[tokio::test]
    async fn test_flush_checkpoints_success() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let writer = CheckpointWriter::new(storage.clone());

        let mut pending =
            pending_states(&[(ProgramType::Escrow, 100), (ProgramType::Withdraw, 200)]);

        writer.flush_checkpoints(&mut pending).await;

        // Successful writes clear the dirty flag; nothing remains to flush.
        assert!(pending.values().all(|s| !s.dirty));

        // Verify checkpoints were written
        let escrow_checkpoint = storage
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        let withdraw_checkpoint = storage
            .get_committed_checkpoint("withdraw")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(escrow_checkpoint, 100);
        assert_eq!(withdraw_checkpoint, 200);
    }

    #[tokio::test]
    async fn test_flush_checkpoints_partial_failure() {
        let mock = MockStorage::new();
        mock.set_should_fail("escrow", true); // Escrow will fail
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let writer = CheckpointWriter::new(storage.clone());

        let mut pending =
            pending_states(&[(ProgramType::Escrow, 100), (ProgramType::Withdraw, 200)]);

        writer.flush_checkpoints(&mut pending).await;

        // Failed checkpoint stays dirty for retry; the successful one is cleared.
        assert!(pending.get(&ProgramType::Escrow).unwrap().dirty);
        assert_eq!(pending.get(&ProgramType::Escrow).unwrap().frontier, 100);
        assert!(!pending.get(&ProgramType::Withdraw).unwrap().dirty);

        // Successful checkpoint should be written
        let withdraw_checkpoint = storage
            .get_committed_checkpoint("withdraw")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(withdraw_checkpoint, 200);

        // Failed checkpoint should not be written
        let escrow_checkpoint = storage.get_committed_checkpoint("escrow").await.unwrap();
        assert_eq!(escrow_checkpoint, None);
    }

    #[tokio::test]
    async fn test_flush_checkpoints_empty_pending() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = CheckpointWriter::new(storage);

        let mut pending: HashMap<ProgramType, CheckpointState> = HashMap::new();

        writer.flush_checkpoints(&mut pending).await;

        assert!(pending.is_empty());
    }

    // ============================================================================
    // get_last_checkpoint Tests
    // ============================================================================

    #[tokio::test]
    async fn test_get_last_checkpoint_exists() {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 12345);
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock));

        let checkpoint = get_last_checkpoint(&storage, ProgramType::Escrow)
            .await
            .unwrap();

        assert_eq!(checkpoint, 12345);
    }

    #[tokio::test]
    async fn test_get_last_checkpoint_defaults_to_zero() {
        let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));

        let checkpoint = get_last_checkpoint(&storage, ProgramType::Escrow)
            .await
            .unwrap();

        assert_eq!(checkpoint, 0);
    }

    #[tokio::test]
    async fn test_get_last_checkpoint_multiple_program_types() {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 100);
        mock.set_checkpoint("withdraw", 200);
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock));

        let escrow_checkpoint = get_last_checkpoint(&storage, ProgramType::Escrow)
            .await
            .unwrap();
        let withdraw_checkpoint = get_last_checkpoint(&storage, ProgramType::Withdraw)
            .await
            .unwrap();

        assert_eq!(escrow_checkpoint, 100);
        assert_eq!(withdraw_checkpoint, 200);
    }

    // ============================================================================
    // start() integration tests
    // ============================================================================

    #[tokio::test]
    async fn test_start_flushes_on_channel_close() {
        let mock = MockStorage::new();
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock.clone()));
        let writer = CheckpointWriter::new(storage.clone())
            .with_batch_interval(1) // short so the task terminates quickly
            .with_max_batch_size(1000);

        let (tx, rx) = mpsc::channel(16);
        let handle = writer.start(rx);

        tx.send(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 500,
        })
        .await
        .unwrap();

        // Drop sender to close the channel
        drop(tx);

        // Wait for the task to finish (ticker will flush then exit)
        handle.await.unwrap();

        // Verify checkpoint was flushed
        let cp = storage.get_committed_checkpoint("escrow").await.unwrap();
        assert_eq!(cp, Some(500));
    }

    #[tokio::test]
    async fn test_start_flushes_on_max_batch_size() {
        let mock = MockStorage::new();
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock.clone()));
        let writer = CheckpointWriter::new(storage.clone())
            .with_batch_interval(1)
            .with_max_batch_size(2); // flush after 2 updates

        let (tx, rx) = mpsc::channel(16);
        let handle = writer.start(rx);

        // Send 2 updates to trigger batch flush
        tx.send(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 100,
        })
        .await
        .unwrap();
        tx.send(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 200,
        })
        .await
        .unwrap();

        // Give the task a moment to process and flush
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Verify checkpoint was flushed (latest slot wins)
        let cp = storage.get_committed_checkpoint("escrow").await.unwrap();
        assert_eq!(cp, Some(200));

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_start_keeps_highest_slot_per_program_type() {
        let mock = MockStorage::new();
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock.clone()));
        let writer = CheckpointWriter::new(storage.clone())
            .with_batch_interval(1) // short so the task terminates quickly
            .with_max_batch_size(1000);

        let (tx, rx) = mpsc::channel(16);
        let handle = writer.start(rx);

        // Send updates with decreasing slots - highest should win
        tx.send(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 300,
        })
        .await
        .unwrap();
        tx.send(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 100, // lower slot, should be ignored
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap();

        let cp = storage.get_committed_checkpoint("escrow").await.unwrap();
        assert_eq!(cp, Some(300));
    }

    #[tokio::test]
    async fn test_start_flushes_on_timer() {
        let mock = MockStorage::new();
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock.clone()));
        let writer = CheckpointWriter::new(storage.clone())
            .with_batch_interval(1) // 1 second interval
            .with_max_batch_size(1000);

        let (tx, rx) = mpsc::channel(16);
        let handle = writer.start(rx);

        tx.send(CheckpointUpdate {
            program_type: ProgramType::Withdraw,
            slot: 42,
        })
        .await
        .unwrap();

        // Wait for timer to trigger flush
        tokio::time::sleep(Duration::from_secs(2)).await;

        let cp = storage.get_committed_checkpoint("withdraw").await.unwrap();
        assert_eq!(cp, Some(42));

        drop(tx);
        handle.await.unwrap();
    }
}
