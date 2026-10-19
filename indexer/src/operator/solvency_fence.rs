//! In-process gate on the escrow operator's value paths. It lives in memory, not in a row, so
//! nothing moves before the first clean reconciliation tick and a confirmed breach stops value
//! at once even when the durable halt write fails.

use crate::config::ProgramType;
use crate::metrics::OPERATOR_SOLVENCY_FENCE_STATE;
use private_channel_metrics::MetricLabel;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;

/// Closed ticks that still count as fetcher progress. By then a breach has tripped the
/// fence or dark inputs have written the outage halt, so a longer wait is a real stall.
pub const CLOSED_TICKS_AS_PROGRESS: u32 = 3;

/// Where the fence stands. The discriminant is the gauge value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FenceState {
    /// No clean reconciliation tick yet in this process.
    Closed = 0,
    Open = 1,
    /// A confirmed insolvency in this process. Only a restart reopens it.
    Tripped = 2,
}

impl FenceState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => FenceState::Closed,
            1 => FenceState::Open,
            _ => FenceState::Tripped,
        }
    }
}

/// Shared handle; clones see the same state.
#[derive(Debug, Clone)]
pub struct SolvencyFence {
    state: Arc<AtomicU8>,
    closed_ticks: Arc<AtomicU32>,
    /// `program_type` label of the state gauge; `None` reports nothing.
    gauge_label: Option<Arc<str>>,
}

impl SolvencyFence {
    /// The operator's fence: closed for the escrow role, which runs reconciliation, and open
    /// for the withdraw role, which runs none. Reports its state on the gauge from now on.
    pub fn for_role(program_type: ProgramType) -> Self {
        let fence = match program_type {
            ProgramType::Escrow => Self::closed(),
            ProgramType::Withdraw => Self::open_from_start(),
        };
        fence.reporting_as(program_type.as_label())
    }

    /// Report every state this fence takes on the gauge under `label`, starting now.
    pub fn reporting_as(mut self, label: &str) -> Self {
        self.gauge_label = Some(Arc::from(label));
        self.publish();
        self
    }

    fn publish(&self) {
        if let Some(label) = &self.gauge_label {
            OPERATOR_SOLVENCY_FENCE_STATE
                .with_label_values(&[label])
                .set(self.state() as u8 as f64);
        }
    }

    /// The escrow operator's start: nothing moves until reconciliation opens it.
    pub fn closed() -> Self {
        Self::with_state(FenceState::Closed)
    }

    /// For roles that run no reconciliation, and for tests of other gates.
    pub fn open_from_start() -> Self {
        Self::with_state(FenceState::Open)
    }

    fn with_state(state: FenceState) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(state as u8)),
            closed_ticks: Arc::new(AtomicU32::new(0)),
            gauge_label: None,
        }
    }

    pub fn state(&self) -> FenceState {
        FenceState::from_u8(self.state.load(Ordering::SeqCst))
    }

    pub fn is_open(&self) -> bool {
        self.state() == FenceState::Open
    }

    /// Open a closed fence. A compare-and-swap, so a late open can never undo a trip.
    pub fn open(&self) -> bool {
        let opened = self
            .state
            .compare_exchange(
                FenceState::Closed as u8,
                FenceState::Open as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok();
        self.publish();
        opened
    }

    /// Close for good in this process.
    pub fn trip(&self) {
        self.state
            .store(FenceState::Tripped as u8, Ordering::SeqCst);
        self.publish();
    }

    /// Reconciliation ticks that ended with the fence still closed.
    pub fn closed_ticks(&self) -> u32 {
        self.closed_ticks.load(Ordering::SeqCst)
    }

    pub fn note_closed_tick(&self) {
        self.closed_ticks.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether a fetcher waiting on this fence still counts as making progress.
    pub fn wait_counts_as_progress(&self) -> bool {
        self.state() == FenceState::Closed && self.closed_ticks() < CLOSED_TICKS_AS_PROGRESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum Op {
        Open,
        Trip,
    }

    #[test]
    fn transitions_follow_the_state_machine() {
        use FenceState::*;
        let cases = [
            (Closed, Op::Open, Open),
            (Closed, Op::Trip, Tripped),
            (Open, Op::Open, Open),
            (Open, Op::Trip, Tripped),
            (Tripped, Op::Open, Tripped),
            (Tripped, Op::Trip, Tripped),
        ];
        for (start, op, expected) in cases {
            let fence = SolvencyFence::with_state(start);
            match op {
                Op::Open => {
                    fence.open();
                }
                Op::Trip => fence.trip(),
            }
            assert_eq!(fence.state(), expected, "{start:?} + {op:?}");
            assert_eq!(fence.is_open(), expected == Open, "{start:?} + {op:?}");
        }
    }

    #[test]
    fn open_reports_whether_it_opened() {
        let fence = SolvencyFence::closed();
        assert!(fence.open());
        assert!(!fence.open(), "already open");
        fence.trip();
        assert!(!fence.open(), "a tripped fence never reopens");
    }

    #[test]
    fn constructors_and_clones_share_one_state() {
        assert_eq!(SolvencyFence::closed().state(), FenceState::Closed);
        assert!(SolvencyFence::open_from_start().is_open());
        let fence = SolvencyFence::closed();
        let clone = fence.clone();
        clone.trip();
        assert_eq!(fence.state(), FenceState::Tripped);
    }

    #[test]
    fn closed_ticks_count_and_bound_the_progress_window() {
        let fence = SolvencyFence::closed();
        for tick in 0..CLOSED_TICKS_AS_PROGRESS {
            assert!(fence.wait_counts_as_progress(), "tick {tick}");
            fence.note_closed_tick();
        }
        assert_eq!(fence.closed_ticks(), CLOSED_TICKS_AS_PROGRESS);
        assert!(!fence.wait_counts_as_progress());

        let opened = SolvencyFence::closed();
        opened.note_closed_tick();
        opened.open();
        assert_eq!(opened.closed_ticks(), 1, "the count stays readable");
        assert!(!opened.wait_counts_as_progress());

        let tripped = SolvencyFence::closed();
        tripped.trip();
        assert!(!tripped.wait_counts_as_progress(), "never while tripped");
    }

    #[test]
    fn the_role_picks_the_starting_state() {
        assert_eq!(
            SolvencyFence::for_role(ProgramType::Escrow).state(),
            FenceState::Closed
        );
        assert!(SolvencyFence::for_role(ProgramType::Withdraw).is_open());
    }

    #[test]
    fn every_transition_is_reported_on_the_gauge() {
        let label = "fence_gauge_test";
        let gauge = || {
            OPERATOR_SOLVENCY_FENCE_STATE
                .with_label_values(&[label])
                .get()
        };
        let fence = SolvencyFence::closed().reporting_as(label);
        assert_eq!(gauge(), 0.0);
        fence.open();
        assert_eq!(gauge(), 1.0);
        fence.trip();
        assert_eq!(gauge(), 2.0);
        fence.open();
        assert_eq!(gauge(), 2.0, "a refused open keeps reporting tripped");
    }

    #[test]
    fn state_discriminants_are_the_gauge_values() {
        assert_eq!(FenceState::Closed as u8, 0);
        assert_eq!(FenceState::Open as u8, 1);
        assert_eq!(FenceState::Tripped as u8, 2);
    }
}
