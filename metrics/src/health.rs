//! Per-service health state for the /health endpoint exposed alongside /metrics.
//!
//! Services bump `record_progress()` after each successful unit of work and
//! call `set_pending()` whenever they observe their backlog or lag. /health
//! reports unhealthy when (a) backlog exceeds the configured ceiling, or
//! (b) backlog is non-zero AND no progress has been recorded within the
//! staleness window. Idle (no backlog, no progress) stays healthy.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy)]
pub struct HealthConfig {
    /// /health flips unhealthy if `pending` exceeds this value.
    pub max_pending: u64,
    /// /health flips unhealthy if the most recent `record_progress()`
    /// is older than this many seconds (subject to the rules below).
    pub stale_threshold_secs: i64,
    /// When true, the staleness check applies even when `pending == 0`.
    /// Use for services watching a continuously-active stream (e.g. an
    /// indexer subscribed to live Solana — the chain always produces
    /// slots, so any quiet period beyond the threshold is a wedge).
    /// When false, `pending == 0` is treated as legitimate idle and
    /// remains healthy (e.g. the operator only acts when work arrives).
    pub require_continuous_progress: bool,
}

impl HealthConfig {
    /// Defaults for the Solana / PrivateChannel slot indexer:
    /// - 50-slot lag tolerance (~20s on Solana mainnet)
    /// - 30-second staleness window, applied unconditionally because
    ///   the chain is always producing slots.
    pub const fn indexer() -> Self {
        Self {
            max_pending: 50,
            stale_threshold_secs: 30,
            require_continuous_progress: true,
        }
    }

    /// Defaults for the operator (mint/release submitter):
    /// - no hard backlog cap (operators legitimately accumulate work)
    /// - 60-second staleness window, only enforced when there's
    ///   pending work — operators legitimately idle for hours.
    pub const fn operator() -> Self {
        Self {
            max_pending: u64::MAX,
            stale_threshold_secs: 60,
            require_continuous_progress: false,
        }
    }
}

#[derive(Debug)]
pub struct HealthState {
    /// Unix seconds of the most recent successful unit of work. 0 = never.
    last_progress_at: AtomicI64,
    /// Service-defined backlog metric (slot lag for indexer, queue depth for operator).
    pending: AtomicU64,
    /// Deliberate sticky-unhealthy latch (`Some(reason)`); never self-clears,
    /// recovery is manual so a solvency incident stays visible. Read on /health.
    forced_unhealthy: Mutex<Option<String>>,
    config: HealthConfig,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HealthOutcome {
    Healthy,
    /// A service latched itself unhealthy via `force_unhealthy`.
    ForcedUnhealthy {
        reason: String,
    },
    /// Backlog has exceeded the configured ceiling.
    BacklogExceeded {
        pending: u64,
        ceiling: u64,
    },
    /// Backlog non-zero and no progress within the staleness window.
    Stalled {
        pending: u64,
        age_secs: i64,
    },
}

impl HealthState {
    pub fn new(config: HealthConfig) -> Arc<Self> {
        Arc::new(Self {
            last_progress_at: AtomicI64::new(0),
            pending: AtomicU64::new(0),
            forced_unhealthy: Mutex::new(None),
            config,
        })
    }

    pub fn record_progress(&self) {
        self.last_progress_at.store(now_unix(), Ordering::Relaxed);
    }

    /// Latch the service unhealthy with a durable reason. Idempotent: once
    /// latched it keeps the first reason, so the original incident stays visible
    /// even if a caller re-invokes it every poll.
    pub fn force_unhealthy(&self, reason: impl Into<String>) {
        let mut latched = self.forced_unhealthy.lock().unwrap();
        if latched.is_none() {
            *latched = Some(reason.into());
        }
    }

    pub fn set_pending(&self, value: u64) {
        self.pending.store(value, Ordering::Relaxed);
    }

    /// Test/diagnostics accessor for the last_progress_at atomic. Production
    /// code should call `record_progress()`.
    pub fn last_progress_at(&self) -> &AtomicI64 {
        &self.last_progress_at
    }

    pub fn check(&self) -> HealthOutcome {
        self.check_at(now_unix())
    }

    /// Variant that takes an injected "now" so tests are deterministic. Private:
    /// production calls `check()`, tests reach this directly from the child module.
    fn check_at(&self, now: i64) -> HealthOutcome {
        // The latch wins over every derived signal so a forced-unhealthy state
        // is never masked by an idle/healthy backlog reading.
        if let Some(reason) = self.forced_unhealthy.lock().unwrap().clone() {
            return HealthOutcome::ForcedUnhealthy { reason };
        }
        let pending = self.pending.load(Ordering::Relaxed);
        if pending > self.config.max_pending {
            return HealthOutcome::BacklogExceeded {
                pending,
                ceiling: self.config.max_pending,
            };
        }
        let last_progress = self.last_progress_at.load(Ordering::Relaxed);
        // Never progressed: rely on the container's start_period to absorb the
        // initial window, then trust subsequent calls to set the timestamp.
        if last_progress == 0 {
            return HealthOutcome::Healthy;
        }
        // Without continuous-progress mode, pending == 0 is legitimate idle.
        if !self.config.require_continuous_progress && pending == 0 {
            return HealthOutcome::Healthy;
        }
        let age = now - last_progress;
        if age > self.config.stale_threshold_secs {
            return HealthOutcome::Stalled {
                pending,
                age_secs: age,
            };
        }
        HealthOutcome::Healthy
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.check(), HealthOutcome::Healthy)
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

    fn cfg(max_pending: u64, threshold: i64) -> HealthConfig {
        HealthConfig {
            max_pending,
            stale_threshold_secs: threshold,
            require_continuous_progress: false,
        }
    }

    fn cfg_continuous(max_pending: u64, threshold: i64) -> HealthConfig {
        HealthConfig {
            max_pending,
            stale_threshold_secs: threshold,
            require_continuous_progress: true,
        }
    }

    #[test]
    fn fresh_state_is_healthy() {
        let h = HealthState::new(cfg(10, 30));
        assert_eq!(h.check_at(1000), HealthOutcome::Healthy);
    }

    #[test]
    fn idle_with_no_backlog_is_healthy_even_with_old_progress() {
        let h = HealthState::new(cfg(10, 30));
        h.last_progress_at.store(1, Ordering::Relaxed);
        h.set_pending(0);
        assert_eq!(h.check_at(1_000_000), HealthOutcome::Healthy);
    }

    #[test]
    fn fresh_progress_is_healthy_even_with_backlog() {
        let h = HealthState::new(cfg(10, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(5);
        // 10s after last progress, well under 30s threshold
        assert_eq!(h.check_at(1010), HealthOutcome::Healthy);
    }

    #[test]
    fn backlog_above_ceiling_is_unhealthy() {
        let h = HealthState::new(cfg(10, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(11);
        assert_eq!(
            h.check_at(1001),
            HealthOutcome::BacklogExceeded {
                pending: 11,
                ceiling: 10
            }
        );
    }

    #[test]
    fn stale_progress_with_backlog_is_unhealthy() {
        let h = HealthState::new(cfg(100, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(5);
        // 31s after last progress, past 30s threshold
        assert_eq!(
            h.check_at(1031),
            HealthOutcome::Stalled {
                pending: 5,
                age_secs: 31
            }
        );
    }

    #[test]
    fn never_progressed_with_backlog_is_healthy_during_grace() {
        // start_period is the orchestration-side mechanism; we report healthy
        // until the first record_progress() so the start_period is meaningful.
        let h = HealthState::new(cfg(100, 30));
        h.set_pending(50);
        assert_eq!(h.check_at(1000), HealthOutcome::Healthy);
    }

    #[test]
    fn record_progress_bumps_timestamp() {
        let h = HealthState::new(cfg(10, 30));
        let before = h.last_progress_at.load(Ordering::Relaxed);
        h.record_progress();
        let after = h.last_progress_at.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn set_pending_persists() {
        let h = HealthState::new(cfg(100, 30));
        h.set_pending(42);
        assert_eq!(h.pending.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn boundary_pending_equal_to_ceiling_is_healthy() {
        let h = HealthState::new(cfg(10, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(10);
        assert_eq!(h.check_at(1001), HealthOutcome::Healthy);
    }

    #[test]
    fn boundary_age_equal_to_threshold_is_healthy() {
        let h = HealthState::new(cfg(100, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(5);
        // exactly 30s — strict greater-than means equal is healthy
        assert_eq!(h.check_at(1030), HealthOutcome::Healthy);
    }

    #[test]
    fn indexer_default_config_values() {
        let cfg = HealthConfig::indexer();
        assert_eq!(cfg.max_pending, 50);
        assert_eq!(cfg.stale_threshold_secs, 30);
        assert!(cfg.require_continuous_progress);
    }

    #[test]
    fn operator_default_config_values() {
        let cfg = HealthConfig::operator();
        assert_eq!(cfg.max_pending, u64::MAX);
        assert_eq!(cfg.stale_threshold_secs, 60);
        assert!(!cfg.require_continuous_progress);
    }

    #[test]
    fn continuous_mode_flips_unhealthy_when_idle_goes_stale() {
        // Indexer-style: no pending work but staleness threshold exceeded
        // (e.g. Yellowstone stream died, no new SlotComplete events).
        let h = HealthState::new(cfg_continuous(50, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(0);
        match h.check_at(1031) {
            HealthOutcome::Stalled {
                pending: 0,
                age_secs: 31,
            } => {}
            other => panic!("expected Stalled, got {:?}", other),
        }
    }

    #[test]
    fn continuous_mode_stays_healthy_when_progress_is_fresh() {
        let h = HealthState::new(cfg_continuous(50, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(0);
        assert_eq!(h.check_at(1010), HealthOutcome::Healthy);
    }

    #[test]
    fn force_unhealthy_flips_check_and_is_healthy_false() {
        let h = HealthState::new(cfg(10, 30));
        // A fresh state with no backlog is Healthy until the latch is set.
        assert!(h.is_healthy());
        h.force_unhealthy("proven insolvency");
        assert!(!h.is_healthy());
        assert!(matches!(
            h.check_at(1000),
            HealthOutcome::ForcedUnhealthy { .. }
        ));
    }

    #[test]
    fn forced_reason_surfaced() {
        let h = HealthState::new(cfg(10, 30));
        h.force_unhealthy("mint ABC insolvent");
        match h.check_at(1000) {
            HealthOutcome::ForcedUnhealthy { reason } => {
                assert_eq!(reason, "mint ABC insolvent");
            }
            other => panic!("expected ForcedUnhealthy, got {:?}", other),
        }
    }

    #[test]
    fn force_unhealthy_overrides_otherwise_healthy_idle() {
        // Idle-with-old-progress is normally Healthy; the latch must win.
        let h = HealthState::new(cfg(10, 30));
        h.last_progress_at.store(1, Ordering::Relaxed);
        h.set_pending(0);
        assert_eq!(h.check_at(1_000_000), HealthOutcome::Healthy);
        h.force_unhealthy("halt");
        assert!(matches!(
            h.check_at(1_000_000),
            HealthOutcome::ForcedUnhealthy { .. }
        ));
    }

    #[test]
    fn not_forced_preserves_derived_outcomes() {
        // Without the latch, the derived backlog/staleness outcomes are unchanged.
        let h = HealthState::new(cfg(10, 30));
        h.last_progress_at.store(1000, Ordering::Relaxed);
        h.set_pending(11);
        assert_eq!(
            h.check_at(1001),
            HealthOutcome::BacklogExceeded {
                pending: 11,
                ceiling: 10
            }
        );
    }

    #[test]
    fn continuous_mode_respects_initial_grace_window() {
        // Even in continuous mode, last_progress == 0 stays healthy so that
        // start_period can absorb the boot window.
        let h = HealthState::new(cfg_continuous(50, 30));
        assert_eq!(h.check_at(1_000_000), HealthOutcome::Healthy);
    }
}
