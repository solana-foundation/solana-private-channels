use crate::metrics;
use crate::{config::ProgramType, error::CheckpointError, storage::Storage};
use private_channel_metrics::MetricLabel;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{error, info, warn};

/// Gated ticks with no frontier advance before a stall warning fires (~15s at 5s/tick).
const STALL_WARN_TICKS: u32 = 3;

/// Checkpoint update message sent by transaction processor
/// Indicates that a slot has been fully processed (transactions saved or confirmed empty)
#[derive(Debug, Clone)]
pub struct CheckpointUpdate {
    pub program_type: ProgramType,
    pub slot: u64,
}

/// In-band message on the checkpoint channel. `Slot` advances the durable frontier;
/// `Regate` re-arms the per-program gate on reconnect. Both ride one FIFO channel so
/// a re-arm always applies before the slot it precedes (no cross-channel race).
#[derive(Debug, Clone)]
pub enum CheckpointMsg {
    Slot(CheckpointUpdate),
    Regate {
        program_type: ProgramType,
        from: u64,
        target: u64,
    },
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
    // Backfill/reconnect target while gated; `None` only when never gated. After the
    // frontier reaches the target it stays `Some` but is inert (the plain-max path).
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

    /// Gated state seeded at backfill's effective `from_slot`. Seeding from `from_slot`
    /// (not a bare DB read) is required when nothing has ever been indexed, because a
    /// configured start slot puts the floor above zero and seeding lower would stall the
    /// frontier on slots that backfill will never emit.
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

    /// Re-arm the gate to hold the checkpoint until slots `(from, target]` are filled.
    /// `from` is the durable checkpoint: pulling the frontier up to it stops a brand-new
    /// state from gating at 0 and never folding. The frontier never moves backward, and
    /// the newest reconnect sets the target so a lower resume slot can still hand off.
    fn regate(&mut self, from: u64, target: u64) {
        self.frontier = self.frontier.max(from);
        self.gate = Some(target);
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
    pub fn start(self, mut rx: mpsc::Receiver<CheckpointMsg>) -> JoinHandle<()> {
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
                            // A Regate is a control signal, not slot progress: re-arm the
                            // gate and do not count it toward a batch flush.
                            Some(CheckpointMsg::Regate { program_type, from, target }) => {
                                self.record_regate(&mut states, program_type, from, target);
                            }
                            Some(CheckpointMsg::Slot(update)) => {
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

    /// Handle a reconnect Regate for one program: re-arm its gate over `(from, target]`
    /// and refresh the lag gauge. Just in-memory state, no DB write. The gate then holds
    /// the checkpoint until the backfill fills every slot in that window.
    fn record_regate(
        &self,
        states: &mut HashMap<ProgramType, CheckpointState>,
        program_type: ProgramType,
        from: u64,
        target: u64,
    ) {
        let state = states
            .entry(program_type)
            .or_insert_with(|| self.new_state());
        state.regate(from, target);
        metrics::INDEXER_CHECKPOINT_FRONTIER_LAG
            .with_label_values(&[program_type.as_label()])
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
            match self
                .storage
                .update_committed_checkpoint(&program_key(program_type), state.frontier)
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

/// Storage key a program type's checkpoint row is stored under.
pub(crate) fn program_key(program_type: ProgramType) -> String {
    format!("{:?}", program_type).to_lowercase()
}

/// Read a program type's durable checkpoint, or `None` when it has never been written.
///
/// Absence and slot zero are deliberately kept apart. A caller that wants to catch up
/// from the beginning can treat `None` as genesis, but reconnect repair needs to know
/// it has no recovery anchor at all, because advancing past a gap it cannot replay
/// would put the missed slots permanently out of reach.
pub async fn get_last_checkpoint(
    storage: &Arc<Storage>,
    program_type: ProgramType,
) -> Result<Option<u64>, CheckpointError> {
    let checkpoint = storage
        .get_committed_checkpoint(&program_key(program_type))
        .await?;

    info!("Last checkpoint for {:?}: {:?}", program_type, checkpoint);
    Ok(checkpoint)
}

/// Config key naming the first slot startup backfill should fill.
pub const BACKFILL_START_SETTING: &str = "indexer.backfill.start_slot";

/// Config key naming the first slot the RPC polling source should request.
pub const RPC_POLLING_START_SETTING: &str = "indexer.rpc_polling.start_slot";

/// Exclusive floor a configured start slot may set, or an error naming the slots it would
/// skip.
///
/// A durable checkpoint is evidence of what was actually processed; a configured start slot
/// is only intent. Intent may initialise a ledger that has never been indexed, but letting
/// it advance past evidence would drop the slots in between with nothing recording them.
pub fn start_floor(
    setting: &'static str,
    program_type: ProgramType,
    last_checkpoint: Option<u64>,
    configured_start: Option<u64>,
) -> Result<u64, CheckpointError> {
    match (last_checkpoint, configured_start) {
        // A start slot is inclusive and a checkpoint exclusive, so compare one below it.
        // Equal means resuming exactly where we stopped, which skips nothing.
        (Some(checkpoint), Some(start_slot)) if start_slot.saturating_sub(1) > checkpoint => {
            Err(CheckpointError::StartSlotAheadOfCheckpoint {
                setting,
                program_type: program_type.as_label(),
                start_slot,
                checkpoint,
            })
        }
        // Below the checkpoint the configured value is silently dropped, so say so once.
        (Some(checkpoint), Some(start_slot)) if start_slot.saturating_sub(1) < checkpoint => {
            warn!(
                setting,
                program_type = program_type.as_label(),
                start_slot,
                checkpoint,
                "configured start slot is below the durable checkpoint and is ignored; \
                 re-scanning already-indexed slots needs a destructive resync, not a lower \
                 start slot"
            );
            Ok(checkpoint)
        }
        (Some(checkpoint), _) => Ok(checkpoint),
        // Never indexed, so the configured start picks the floor, else catch up from genesis.
        (None, configured) => Ok(configured.unwrap_or(0).saturating_sub(1)),
    }
}

/// Inclusive slot a live stream with no fill behind it must resume from, or `None` to
/// leave the source its own default of the chain tip.
///
/// A checkpoint is exclusive, so the next unprocessed slot sits one above it. A configured
/// start is already inclusive and passes through untouched, keeping it distinct from the
/// slot above it on a ledger that has never been indexed.
///
/// Genesis is floored to slot one because proving a slot empty needs a predecessor to
/// anchor on, and slot zero has none. Starting there would retry forever on any endpoint
/// that no longer serves the genesis block, and no program is deployed that early anyway.
pub fn live_resume_slot(
    last_checkpoint: Option<u64>,
    configured_start: Option<u64>,
) -> Option<u64> {
    last_checkpoint
        .map(|checkpoint| checkpoint.saturating_add(1))
        .or(configured_start)
        .map(|slot| slot.max(1))
}

/// Longest startup will wait for a filled range to become durable before giving up.
///
/// Sized as a fail-closed backstop rather than a tuning knob. By the time the wait starts
/// every message has already been sent, and the instruction channel holds at most a
/// thousand of them, so what remains is the processor draining that buffer plus one flush
/// interval. Two minutes is far above that, and exceeding it means a slot is genuinely
/// stuck rather than slow.
pub const CHECKPOINT_COMMIT_TIMEOUT_SECS: u64 = 120;

/// How often the wait re-reads the durable checkpoint.
const CHECKPOINT_COMMIT_POLL_MS: u64 = 200;

/// Wait until the durable checkpoint for `program_type` covers `target`.
///
/// This is the witness that a backfilled range is both processed and persisted. It is
/// exact rather than approximate because the writer's gate seeds its frontier at the
/// range floor and then advances only across contiguous slots, so the checkpoint cannot
/// reach the target until every slot in between has been fully written.
///
/// A read failure is a database blip and is retried; an absent row means the writer has
/// not flushed for the first time yet, which is normal on a fresh store, so it is also
/// retried. Only the deadline ends this unsuccessfully, and it does so by failing the
/// boot: continuing would compare on-chain custody against a ledger still missing a slot.
pub async fn wait_for_checkpoint_commit(
    storage: &Arc<Storage>,
    program_type: ProgramType,
    target: u64,
    timeout: Duration,
) -> Result<(), CheckpointError> {
    let started = tokio::time::Instant::now();
    let key = program_key(program_type);
    let mut last: Option<u64> = None;
    // Only a successful read tells us anything about the frontier. Without this, a store
    // whose reads all fail would time out reporting "no row was ever written", sending an
    // operator after a stalled writer when the real fault is an unreachable database.
    let mut read_ok = false;

    loop {
        // Read the row directly rather than through get_last_checkpoint, whose per-call
        // info log would emit hundreds of lines across a long wait.
        match storage.get_committed_checkpoint(&key).await {
            Ok(committed) => {
                last = committed;
                read_ok = true;
                if committed.is_some_and(|slot| slot >= target) {
                    info!(
                        "Checkpoint for {:?} committed at {:?}, covering backfill target {}",
                        program_type, committed, target
                    );
                    return Ok(());
                }
            }
            Err(e) => {
                warn!(
                    "Checkpoint read failed while waiting for {:?} to reach {}, retrying: {}",
                    program_type, target, e
                );
            }
        }

        let waited_secs = started.elapsed().as_secs();
        if started.elapsed() >= timeout {
            if !read_ok {
                error!(
                    "Checkpoint for {:?} was never readable while waiting for {}; the store \
                     is unreachable rather than the frontier being stalled",
                    program_type, target
                );
            }
            return Err(CheckpointError::CommitTimeout {
                program_type: key,
                last,
                target,
                waited_secs,
            });
        }

        tokio::time::sleep(Duration::from_millis(CHECKPOINT_COMMIT_POLL_MS)).await;
    }
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
    // Regate (reconnect re-arm) Tests
    // ============================================================================

    /// After a hand-off, a reconnect re-arms the gate to a new target; a hole plus
    /// a live tip must not leapfrog the frontier, and once the residual window fills
    /// contiguously it folds up to the new target and hands off again.
    #[test]
    fn regate_rearms_after_handoff_and_blocks_leapfrog() {
        let mut state = CheckpointState::gated(FROM, T0);
        let full: Vec<u64> = (FROM + 1..=T0).collect();
        assert_eq!(drive(&mut state, &full), T0);
        // Hand off to plain-max with a live slot above T0.
        assert_eq!(drive(&mut state, &[T0 + 50]), T0 + 50);

        // Reconnect observes a later resume slot and re-arms the gate.
        state.regate(T0 + 50, T0 + 100);

        // A hole (T0+51 skipped) plus a live tip cannot move the durable frontier.
        assert_eq!(drive(&mut state, &[T0 + 52, 9_000_000]), T0 + 50);

        // Filling the residual window contiguously folds to the new target.
        let residual: Vec<u64> = (T0 + 51..=T0 + 100).collect();
        assert_eq!(drive(&mut state, &residual), T0 + 100);

        // Handed off again: a later live tip advances via plain max.
        assert_eq!(drive(&mut state, &[9_000_001]), 9_000_001);
    }

    /// The gate target tracks the latest reconnect: a lower resume slot after a higher
    /// one lowers the target so the fold can hand off, instead of stalling above what the
    /// source will emit.
    #[test]
    fn regate_latest_target_wins() {
        let mut state = CheckpointState::gated(FROM, T0);
        // A first reconnect raises the target above T0.
        state.regate(FROM, T0 + 20);
        let upto_t0: Vec<u64> = (FROM + 1..=T0).collect();
        assert_eq!(drive(&mut state, &upto_t0), T0, "still gated below T0+20");
        // A later reconnect resumes LOWER; the target follows it down.
        state.regate(FROM, T0 + 5);
        let rest: Vec<u64> = (T0 + 1..=T0 + 5).collect();
        assert_eq!(
            drive(&mut state, &rest),
            T0 + 5,
            "hands off at the latest target"
        );
        assert_eq!(drive(&mut state, &[T0 + 500]), T0 + 500, "then plain max");
    }

    /// A reconnect on a fresh state seeds the frontier to the durable checkpoint, so a
    /// Regate arriving before any Slot cannot freeze the fold at 0; a later lower `from`
    /// never rewinds it.
    #[test]
    fn regate_seeds_frontier_from_durable_checkpoint() {
        let mut state = CheckpointState::ungated();
        assert_eq!(state.frontier, 0);
        state.regate(5000, 5001);
        assert_eq!(
            state.frontier, 5000,
            "fresh state anchors at the durable checkpoint"
        );
        assert_eq!(
            drive(&mut state, &[5001]),
            5001,
            "folds to the target and hands off"
        );
        state.regate(10, 6000);
        assert_eq!(
            state.frontier, 5001,
            "a lower from never rewinds the frontier"
        );
    }

    /// A target at or below the frontier is inert: the gap is already covered, so
    /// plain-max progress continues with no stall.
    #[test]
    fn regate_below_frontier_is_inert() {
        let mut state = CheckpointState::ungated();
        assert_eq!(drive(&mut state, &[500]), 500);
        state.regate(400, 400);
        assert_eq!(drive(&mut state, &[600]), 600);
        assert_eq!(state.lag(), 0);
    }

    /// A cold-start regate carries a lower `from` and a higher target than the startup gate.
    /// The widened gate must still fold contiguously, so neither end lets a slot through.
    #[test]
    fn regate_over_open_startup_gate_widens_without_skipping() {
        let mut state = CheckpointState::gated(FROM, T0);
        // Startup backfill is mid-flight: the gate to T0 is still open.
        assert_eq!(drive(&mut state, &[FROM + 1, FROM + 2, FROM + 3]), FROM + 3);

        // Cold start arms from the durable checkpoint, which sits below from_slot.
        state.regate(FROM - 10, T0 + 20);
        assert_eq!(
            state.frontier,
            FROM + 3,
            "a lower from never pulls the frontier back over the unfilled startup range"
        );

        // The resume slot alone is a hole away from the frontier, so it cannot move it.
        assert_eq!(drive(&mut state, &[T0 + 20]), FROM + 3);

        // Only the full contiguous run up to the widened target hands off.
        let rest: Vec<u64> = (FROM + 4..=T0 + 20).collect();
        assert_eq!(drive(&mut state, &rest), T0 + 20);
    }

    /// The writer's Regate handling re-gates the correct per-program state: after
    /// record_regate the gated frontier stays put and lag reflects the new target.
    #[test]
    fn record_regate_arms_gate_for_program() {
        let storage: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));
        let writer = CheckpointWriter::new(storage);
        let mut states: HashMap<ProgramType, CheckpointState> = HashMap::new();

        // Seed the program at frontier 100 the way the writer loop would.
        writer.record_update(
            &mut states,
            CheckpointUpdate {
                program_type: ProgramType::Escrow,
                slot: 100,
            },
        );
        writer.record_regate(&mut states, ProgramType::Escrow, 100, 110);

        // A live tip and an in-gap slot must not move the gated frontier.
        writer.record_update(
            &mut states,
            CheckpointUpdate {
                program_type: ProgramType::Escrow,
                slot: 105,
            },
        );
        writer.record_update(
            &mut states,
            CheckpointUpdate {
                program_type: ProgramType::Escrow,
                slot: 2_000_000,
            },
        );

        let state = states.get(&ProgramType::Escrow).unwrap();
        assert_eq!(state.frontier, 100);
        assert_eq!(state.lag(), 10);
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
    // start_floor Tests
    // ============================================================================

    /// Every reachable pairing of a stored checkpoint and a configured start slot.
    ///
    /// One table covers both callers because they ask the same question of different
    /// config keys, so a rule that holds here holds at each call site.
    #[test]
    fn live_resume_slot_matrix() {
        // (checkpoint, configured start, expected inclusive start, why)
        type Case = (Option<u64>, Option<u64>, Option<u64>, &'static str);

        let cases: [Case; 7] = [
            (
                None,
                None,
                None,
                "nothing known, source keeps its own default",
            ),
            (
                None,
                Some(0),
                Some(1),
                "genesis has no predecessor slot to anchor a proof on",
            ),
            (
                None,
                Some(200),
                Some(200),
                "configured start passes through inclusive",
            ),
            (
                Some(100),
                None,
                Some(101),
                "resume one above the exclusive checkpoint",
            ),
            (
                Some(100),
                Some(50),
                Some(101),
                "checkpoint wins over a lower start",
            ),
            (
                Some(100),
                Some(101),
                Some(101),
                "checkpoint and start agree",
            ),
            (Some(0), Some(0), Some(1), "slot zero already processed"),
        ];

        for (checkpoint, configured, want, why) in cases {
            assert_eq!(
                live_resume_slot(checkpoint, configured),
                want,
                "checkpoint {checkpoint:?} start {configured:?} ({why})"
            );
        }
    }

    #[test]
    fn start_floor_matrix() {
        // (checkpoint, configured start, expected floor or None for a refusal, why)
        type Case = (Option<u64>, Option<u64>, Option<u64>, &'static str);

        let cases: [Case; 12] = [
            (Some(100), Some(200), None, "skips 101..=199"),
            (Some(100), Some(102), None, "skips exactly one slot"),
            (
                Some(100),
                Some(101),
                Some(100),
                "resumes exactly, skips nothing",
            ),
            (Some(100), Some(50), Some(100), "checkpoint wins, warns"),
            (
                Some(100),
                Some(100),
                Some(100),
                "one slot below the checkpoint still warns",
            ),
            (
                Some(0),
                Some(0),
                Some(0),
                "genesis start matches genesis checkpoint",
            ),
            (Some(100), None, Some(100), "no configured start"),
            (Some(0), Some(1), Some(0), "genesis checkpoint resumed"),
            (Some(0), Some(2), None, "slot zero is a real checkpoint"),
            (
                None,
                Some(200),
                Some(199),
                "empty ledger, start above the tip",
            ),
            (None, Some(0), Some(0), "no underflow at genesis"),
            (None, None, Some(0), "catch up from genesis"),
        ];

        for (checkpoint, configured, want, why) in cases {
            let got = start_floor(
                BACKFILL_START_SETTING,
                ProgramType::Withdraw,
                checkpoint,
                configured,
            );
            match want {
                Some(floor) => assert_eq!(
                    got.as_ref().ok(),
                    Some(&floor),
                    "checkpoint {checkpoint:?} start {configured:?} ({why})"
                ),
                None => assert!(
                    matches!(got, Err(CheckpointError::StartSlotAheadOfCheckpoint { .. })),
                    "checkpoint {checkpoint:?} start {configured:?} must refuse ({why})"
                ),
            }
        }
    }

    /// The refusal names the offending key, which is how an operator knows which knob
    /// to change and how the runbook routes to the right remedy.
    #[test]
    fn start_floor_error_names_the_setting_and_program() {
        let err = start_floor(
            RPC_POLLING_START_SETTING,
            ProgramType::Escrow,
            Some(100),
            Some(200),
        )
        .expect_err("a start slot above the checkpoint must refuse");

        let CheckpointError::StartSlotAheadOfCheckpoint {
            setting,
            program_type,
            start_slot,
            checkpoint,
        } = err
        else {
            panic!("expected StartSlotAheadOfCheckpoint, got {err:?}");
        };
        assert_eq!(setting, "indexer.rpc_polling.start_slot");
        assert_eq!(program_type, "escrow");
        assert_eq!(start_slot, 200);
        assert_eq!(checkpoint, 100);
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

        assert_eq!(checkpoint, Some(12345));
    }

    /// Both halves belong in one test: the bug was that "never anchored" and
    /// "anchored at genesis" produced the same value, so the assertion that
    /// matters is that these two stores now read back differently. Reconnect
    /// repair keys its fail-closed decision on exactly this distinction.
    #[tokio::test]
    async fn get_last_checkpoint_returns_none_when_absent() {
        let absent: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));
        assert_eq!(
            get_last_checkpoint(&absent, ProgramType::Escrow)
                .await
                .unwrap(),
            None,
            "a store with no row must report absence, not slot zero"
        );

        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", 0);
        let genesis: Arc<Storage> = Arc::new(Storage::Mock(mock));
        assert_eq!(
            get_last_checkpoint(&genesis, ProgramType::Escrow)
                .await
                .unwrap(),
            Some(0),
            "a durable anchor at genesis must be distinguishable from absence"
        );
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

        assert_eq!(escrow_checkpoint, Some(100));
        assert_eq!(withdraw_checkpoint, Some(200));
    }

    // ============================================================================
    // wait_for_checkpoint_commit Tests
    // ============================================================================

    /// Target the wait tests aim at, and a timeout long enough that only a real
    /// stall reaches it.
    const WAIT_TARGET: u64 = 500;
    const GENEROUS: Duration = Duration::from_secs(5);
    const IMPATIENT: Duration = Duration::from_millis(400);

    /// Set the escrow checkpoint after a short delay, standing in for the writer's flush.
    fn commit_after(mock: MockStorage, slot: u64, delay: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            mock.set_checkpoint("escrow", slot);
        })
    }

    /// Both end states belong in one test: a batch flush can land the frontier above the
    /// target, so an equality check would wait forever on a checkpoint that already
    /// covers everything the fill produced.
    #[tokio::test]
    async fn wait_for_checkpoint_commit_returns_once_target_committed() {
        for committed in [WAIT_TARGET, WAIT_TARGET + 5] {
            let mock = MockStorage::new();
            let storage: Arc<Storage> = Arc::new(Storage::Mock(mock.clone()));
            let writer = commit_after(mock, committed, Duration::from_millis(150));

            let result =
                wait_for_checkpoint_commit(&storage, ProgramType::Escrow, WAIT_TARGET, GENEROUS)
                    .await;

            assert!(
                result.is_ok(),
                "a committed checkpoint of {committed} must satisfy target {WAIT_TARGET}"
            );
            writer.await.unwrap();
        }
    }

    /// A frontier one slot short means a slot in the range was never processed. Failing
    /// the boot is the point: continuing would compare on-chain custody against a ledger
    /// that is still missing a slot, which is the bug this wait exists to prevent.
    #[tokio::test]
    async fn wait_for_checkpoint_commit_times_out_below_target() {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", WAIT_TARGET - 1);
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock));

        let err = wait_for_checkpoint_commit(&storage, ProgramType::Escrow, WAIT_TARGET, IMPATIENT)
            .await
            .expect_err("a frontier below the target must not be accepted");

        match err {
            CheckpointError::CommitTimeout { last, target, .. } => {
                assert_eq!(last, Some(WAIT_TARGET - 1));
                assert_eq!(target, WAIT_TARGET);
            }
            other => panic!("expected CommitTimeout, got {other:?}"),
        }
    }

    /// A database blip during the wait must not abort a boot that is one flush away from
    /// succeeding.
    #[tokio::test]
    async fn wait_for_checkpoint_commit_rides_out_transient_read_failures() {
        let mock = MockStorage::new();
        mock.set_checkpoint("escrow", WAIT_TARGET);
        mock.set_fail_times("get_committed_checkpoint", 2);
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock));

        let result =
            wait_for_checkpoint_commit(&storage, ProgramType::Escrow, WAIT_TARGET, GENEROUS).await;

        assert!(
            result.is_ok(),
            "two failed reads must be retried, not treated as a stall: {result:?}"
        );
    }

    /// A store with no row yet is normal for the first moments of a fresh boot, so the
    /// wait polls on. It still has to end: absence forever is a stall like any other, and
    /// reporting it as `None` rather than 0 tells an operator the writer never flushed.
    #[tokio::test]
    async fn wait_for_checkpoint_commit_treats_absent_row_as_not_yet() {
        let absent: Arc<Storage> = Arc::new(Storage::Mock(MockStorage::new()));
        let err = wait_for_checkpoint_commit(&absent, ProgramType::Escrow, WAIT_TARGET, IMPATIENT)
            .await
            .expect_err("an absent row must not be read as a satisfied target");

        match err {
            CheckpointError::CommitTimeout { last, .. } => assert_eq!(last, None),
            other => panic!("expected CommitTimeout, got {other:?}"),
        }

        let mock = MockStorage::new();
        let storage: Arc<Storage> = Arc::new(Storage::Mock(mock.clone()));
        let writer = commit_after(mock, WAIT_TARGET, Duration::from_millis(150));
        assert!(
            wait_for_checkpoint_commit(&storage, ProgramType::Escrow, WAIT_TARGET, GENEROUS)
                .await
                .is_ok(),
            "a row written mid-wait must satisfy the target"
        );
        writer.await.unwrap();
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

        tx.send(CheckpointMsg::Slot(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 500,
        }))
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
        tx.send(CheckpointMsg::Slot(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 100,
        }))
        .await
        .unwrap();
        tx.send(CheckpointMsg::Slot(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 200,
        }))
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
        tx.send(CheckpointMsg::Slot(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 300,
        }))
        .await
        .unwrap();
        tx.send(CheckpointMsg::Slot(CheckpointUpdate {
            program_type: ProgramType::Escrow,
            slot: 100, // lower slot, should be ignored
        }))
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

        tx.send(CheckpointMsg::Slot(CheckpointUpdate {
            program_type: ProgramType::Withdraw,
            slot: 42,
        }))
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
