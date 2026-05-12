//! Per-stage liveness heartbeats consumed by the /health endpoint.
//!
//! Each pipeline stage owns a `StageHeartbeat`; the stage updates `last_input_at`
//! when it receives a unit of work and `last_progress_at` when it produces output.
//! /health declares a stage healthy when the two are close in time (progress is
//! caught up to input within `STAGE_PROGRESS_MARGIN_SECS`) or when no input has
//! ever been received (legitimately idle).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Margin within which last_progress_at must be of last_input_at. 5s absorbs
/// in-flight processing without flagging stages that are just busy.
const STAGE_PROGRESS_MARGIN_SECS: i64 = 5;

#[derive(Debug, Default)]
pub struct StageHeartbeat {
    last_input_at: AtomicI64,
    last_progress_at: AtomicI64,
}

impl StageHeartbeat {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Bump on each successfully received unit of input.
    pub fn record_input(&self) {
        self.last_input_at.store(now_unix(), Ordering::Relaxed);
    }

    /// Bump on each successfully produced output.
    pub fn record_progress(&self) {
        self.last_progress_at.store(now_unix(), Ordering::Relaxed);
    }

    /// Healthy iff never received input (idle) or progress is caught up with input.
    pub fn is_healthy(&self) -> bool {
        let t_input = self.last_input_at.load(Ordering::Relaxed);
        if t_input == 0 {
            return true;
        }
        let t_progress = self.last_progress_at.load(Ordering::Relaxed);
        t_progress >= t_input - STAGE_PROGRESS_MARGIN_SECS
    }
}

/// Top-level registry passed to the /health handler. Each field is `None` when
/// the corresponding stage isn't running (e.g. read-only mode skips all stages).
#[derive(Debug, Default, Clone)]
pub struct HeartbeatRegistry {
    pub dedup: Option<Arc<StageHeartbeat>>,
    pub sigverify: Option<Arc<StageHeartbeat>>,
    pub sequencer: Option<Arc<StageHeartbeat>>,
    pub executor: Option<Arc<StageHeartbeat>>,
    pub settler: Option<Arc<StageHeartbeat>>,
    pub address_index_writer: Option<Arc<StageHeartbeat>>,
}

impl HeartbeatRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the name of the first unhealthy stage, or None if every running stage is healthy.
    pub fn first_unhealthy(&self) -> Option<&'static str> {
        for (name, hb) in [
            ("dedup", &self.dedup),
            ("sigverify", &self.sigverify),
            ("sequencer", &self.sequencer),
            ("executor", &self.executor),
            ("settler", &self.settler),
            ("address_index_writer", &self.address_index_writer),
        ] {
            if let Some(hb) = hb {
                if !hb.is_healthy() {
                    return Some(name);
                }
            }
        }
        None
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hb_with(t_input: i64, t_progress: i64) -> Arc<StageHeartbeat> {
        let hb = StageHeartbeat::new();
        hb.last_input_at.store(t_input, Ordering::Relaxed);
        hb.last_progress_at.store(t_progress, Ordering::Relaxed);
        hb
    }

    #[test]
    fn fresh_stage_with_no_input_is_healthy() {
        let hb = StageHeartbeat::new();
        assert!(hb.is_healthy());
    }

    #[test]
    fn progress_caught_up_with_input_is_healthy() {
        // input at t=1000, progress at t=1000 — caught up
        let hb = hb_with(1000, 1000);
        assert!(hb.is_healthy());
    }

    #[test]
    fn progress_within_margin_is_healthy() {
        // input at t=1000, progress at t=996 — 4s behind, under 5s margin
        let hb = hb_with(1000, 996);
        assert!(hb.is_healthy());
    }

    #[test]
    fn progress_at_margin_boundary_is_healthy() {
        // input at t=1000, progress at t=995 — exactly 5s, equals threshold (>=)
        let hb = hb_with(1000, 995);
        assert!(hb.is_healthy());
    }

    #[test]
    fn progress_beyond_margin_is_unhealthy() {
        // input at t=1000, progress at t=994 — 6s behind, over 5s margin
        let hb = hb_with(1000, 994);
        assert!(!hb.is_healthy());
    }

    #[test]
    fn progress_far_behind_is_unhealthy() {
        // input at t=1000, progress at t=900 — 100s behind
        let hb = hb_with(1000, 900);
        assert!(!hb.is_healthy());
    }

    #[test]
    fn record_input_advances_timestamp() {
        let hb = StageHeartbeat::new();
        let before = hb.last_input_at.load(Ordering::Relaxed);
        hb.record_input();
        let after = hb.last_input_at.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn record_progress_advances_timestamp() {
        let hb = StageHeartbeat::new();
        let before = hb.last_progress_at.load(Ordering::Relaxed);
        hb.record_progress();
        let after = hb.last_progress_at.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn registry_with_no_stages_reports_no_unhealthy() {
        let r = HeartbeatRegistry::new();
        assert_eq!(r.first_unhealthy(), None);
    }

    #[test]
    fn registry_reports_first_unhealthy_stage_in_order() {
        let mut r = HeartbeatRegistry::new();
        r.dedup = Some(StageHeartbeat::new());
        r.sigverify = Some(hb_with(1000, 900)); // unhealthy
        r.executor = Some(hb_with(1000, 900)); // also unhealthy, but reported second
                                               // First unhealthy in ordered iteration (dedup, sigverify, sequencer, executor, settler):
                                               //   dedup is healthy (no input recorded), sigverify is unhealthy
        assert_eq!(r.first_unhealthy(), Some("sigverify"));
    }

    #[test]
    fn registry_with_only_healthy_stages_reports_none() {
        let mut r = HeartbeatRegistry::new();
        r.dedup = Some(StageHeartbeat::new());
        r.sequencer = Some(hb_with(1000, 1000));
        r.settler = Some(hb_with(2000, 1998));
        assert_eq!(r.first_unhealthy(), None);
    }

    #[test]
    fn registry_skips_none_slots() {
        let mut r = HeartbeatRegistry::new();
        // Only sigverify and settler are wired (e.g. minimal pipeline).
        r.sigverify = Some(StageHeartbeat::new());
        r.settler = Some(StageHeartbeat::new());
        // Both healthy (never received input). Other slots None — must be skipped.
        assert_eq!(r.first_unhealthy(), None);
    }

    #[test]
    fn registry_attributes_to_correct_stage() {
        // Only the executor is unhealthy — the registry must name "executor".
        let mut r = HeartbeatRegistry::new();
        r.dedup = Some(StageHeartbeat::new());
        r.sigverify = Some(StageHeartbeat::new());
        r.sequencer = Some(StageHeartbeat::new());
        r.executor = Some(hb_with(1000, 900));
        r.settler = Some(StageHeartbeat::new());
        assert_eq!(r.first_unhealthy(), Some("executor"));
    }
}
