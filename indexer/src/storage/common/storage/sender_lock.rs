use crate::{
    error::StorageError,
    metrics::OPERATOR_SENDER_LOCK_LOST,
    storage::common::storage::Storage,
    storage::postgres::lock_connection::{LockConnection, ProbeOutcome, PROBE_TIMEOUT},
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Probe the lock on a fixed interval and call `fail_closed` once ownership can no
/// longer be proven. Generic over the probe so every verdict is unit-testable
/// without a database, and over the loss report so both the sender lock and the
/// live-state lock share this one loop. An interval of zero disables probing but
/// keeps the task, and therefore the release path, in exactly one shape.
///
/// `budget` is how long a probe may go unanswered before the role stops, and it
/// applies to the timeout arm alone. The other two verdicts are already proof and
/// get no tolerance: "not held" is the server answering about the lock, and a
/// query error on a pinned connection means the session itself is gone, which is
/// the same thing. Only a timeout is genuinely ambiguous, since a healthy server
/// under load can miss the deadline. A budget of zero stops on the first timeout.
pub(crate) async fn run_lock_heartbeat<P, Fut, L>(
    interval: Duration,
    budget: Duration,
    stop: CancellationToken,
    mut fail_closed: L,
    mut probe: P,
) where
    P: FnMut() -> Fut,
    Fut: Future<Output = Result<ProbeOutcome, sqlx::Error>>,
    L: FnMut(&str, &str),
{
    let mut confirmed_at = tokio::time::Instant::now();

    loop {
        if interval.is_zero() {
            stop.cancelled().await;
            return;
        }

        // Not raced against the stop token: dropping a query mid-flight ruins the connection.
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }

        const UNANSWERED: &str = "probe did not answer within the timeout";

        match tokio::time::timeout(PROBE_TIMEOUT, probe()).await {
            Ok(Ok(ProbeOutcome::Held)) => {
                confirmed_at = tokio::time::Instant::now();
                continue;
            }
            // A synchronous check holds the connection, which proves the session is
            // alive on its own, so the last real confirmation still stands.
            Ok(Ok(ProbeOutcome::Busy)) => {
                debug!("Lock probe skipped; connection in use");
                continue;
            }
            // A fenced write already reported and cancelled, so do not count it twice.
            Ok(Ok(ProbeOutcome::Gone)) => return,
            // The server answered and it is not ours. Proof, so no tolerance.
            Ok(Ok(ProbeOutcome::NotHeld)) => {
                fail_closed("not_held", "pg_locks reports the lock is not held");
                return;
            }
            // A checked-out connection never heals, so an error here means the session
            // is gone, and with it the lock. Proof, so no tolerance.
            Ok(Err(e)) => {
                fail_closed("probe_error", &format!("probe query failed: {e}"));
                return;
            }
            // Unanswered. Falls through to the budget below.
            Err(_) => {}
        }

        // A timeout says the server is slow, not that the lock is gone. Retry while the
        // budget lasts: our session is still open, so it still holds the lock, and
        // nothing the lock protects is at risk meanwhile.
        if confirmed_at.elapsed() < budget {
            warn!("Lock probe inconclusive ({UNANSWERED}); retrying");
            continue;
        }
        fail_closed("probe_timeout", UNANSWERED);
        return;
    }
}

/// Report a sender lock we can no longer prove we own, and stop the operator.
fn report_sender_lock_lost(
    program_type: &str,
    operator_token: &CancellationToken,
    reason: &str,
    detail: &str,
) {
    OPERATOR_SENDER_LOCK_LOST
        .with_label_values(&[program_type, reason])
        .inc();
    error!(
        reason,
        "Sender advisory lock ownership could not be proven ({detail}); cancelling the operator"
    );
    operator_token.cancel();
}

/// Held for the sender's lifetime. Dropping it tells the heartbeat to unlock and let go.
pub enum SenderLockGuard {
    Postgres(HeartbeatHandle),
    #[cfg(any(test, feature = "test-mock-storage"))]
    Noop,
}

/// Owns the heartbeat task that owns the pinned connection.
pub struct HeartbeatHandle {
    stop: CancellationToken,
    /// Deliberately never aborted: aborting drops the connection at the next
    /// await point, which returns it to the pool still holding the lock and can
    /// interrupt the release query mid-flight. Cancelling instead lets the task
    /// run its own release path.
    task: Option<tokio::task::JoinHandle<()>>,
}

impl HeartbeatHandle {
    /// Cancel and wait for release, making the handoff deterministic for tests.
    pub async fn stop_and_wait(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        // The task owns the connection and unlocks itself; this only signals it.
        self.stop.cancel();
    }
}

/// Try to become the singleton sender for `key`. `Ok(None)` means someone else holds it.
pub async fn try_acquire_sender_lock(
    storage: &Storage,
    key: i64,
    program_type: &'static str,
    operator_token: CancellationToken,
    heartbeat_interval: Duration,
) -> Result<Option<SenderLockGuard>, StorageError> {
    match storage {
        Storage::Postgres(db) => {
            let Some(lock) = db
                .try_acquire_sender_lock(key, program_type, operator_token.clone())
                .await?
            else {
                return Ok(None);
            };
            if heartbeat_interval.is_zero() {
                warn!(
                    "Sender lock heartbeat is disabled; a lost advisory lock will not be detected"
                );
            }
            Ok(Some(SenderLockGuard::Postgres(spawn_heartbeat(
                lock,
                program_type,
                operator_token,
                heartbeat_interval,
            ))))
        }
        // Mock has no shared store, so nothing to contend on and nothing to spawn.
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(_) => Ok(Some(SenderLockGuard::Noop)),
    }
}

fn spawn_heartbeat(
    lock: Arc<LockConnection>,
    program_type: &'static str,
    operator_token: CancellationToken,
    heartbeat_interval: Duration,
) -> HeartbeatHandle {
    let stop = CancellationToken::new();
    let task_stop = stop.clone();
    let task = tokio::spawn(async move {
        let probe_lock = lock.clone();
        run_lock_heartbeat(
            heartbeat_interval,
            // No tolerance for an unanswered probe, unchanged. The sender's fail-closed
            // timing is the operator's to retune, not this change's.
            Duration::ZERO,
            task_stop.clone(),
            move |reason, detail| {
                report_sender_lock_lost(program_type, &operator_token, reason, detail)
            },
            move || {
                let lock = probe_lock.clone();
                async move { lock.probe().await }
            },
        )
        .await;

        // Only a graceful stop unlocks.
        if task_stop.is_cancelled() {
            lock.release().await;
            return;
        }

        // A lock-loss verdict must not free the lock here. The sender has only
        // just been told to stop and still has a drain to run, during which it
        // broadcasts its queued work. If the verdict were a false positive, and
        // a slow probe on a healthy session can produce one, closing the socket
        // now would hand the lock to a replacement that starts alongside a
        // sender that is still broadcasting. Poison the connection so every
        // fenced write fails closed, then wait for the sender to finish before
        // letting the session go.
        lock.poison();
        task_stop.cancelled().await;
        lock.discard().await;
    });
    HeartbeatHandle {
        stop,
        task: Some(task),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    const ALL_REASONS: [&str; 4] = ["not_held", "probe_error", "probe_timeout", "fenced_write"];

    fn lost_count(program_type: &str, reason: &str) -> f64 {
        crate::metrics::OPERATOR_SENDER_LOCK_LOST
            .with_label_values(&[program_type, reason])
            .get()
    }

    /// The sender's own loss report, so these tests still exercise the exact
    /// metric and cancellation the production heartbeat performs.
    fn sender_on_lost(
        program_type: &'static str,
        operator: CancellationToken,
    ) -> impl FnMut(&str, &str) {
        move |reason, detail| report_sender_lock_lost(program_type, &operator, reason, detail)
    }

    /// U1. Four `run_sender` call sites pass `Storage::Mock`. A `Noop` that
    /// spawned or cancelled anything would break all of them as a mysterious
    /// hang rather than a clear failure, so pin that it does neither, and that
    /// sender-owned writes still work through it.
    #[tokio::test]
    async fn noop_guard_spawns_nothing_and_never_cancels() {
        use crate::storage::{
            common::models::{TransactionStatus, TransactionType},
            common::storage::mock::MockStorage,
            DbTransaction,
        };

        let mock = MockStorage::new();
        let now = chrono::Utc::now();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id: 7,
                signature: "u1-sig".to_string(),
                trace_id: "u1-trace".to_string(),
                slot: 0,
                initiator: String::new(),
                recipient: String::new(),
                mint: String::new(),
                amount: crate::storage::common::amount::TokenAmount(0),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(7),
                status: TransactionStatus::Processing,
                created_at: now,
                updated_at: now,
                processed_at: None,
                counterpart_signature: None,
                remint_signatures: None,
                remint_last_valid_block_heights: None,
                pending_remint_deadline_at: None,
                finality_check_attempts: 0,
                recovery_requeue_attempts: 0,
                instruction_index: 0,
                inner_index: None,
                landed_remint_signature: None,
                release_refused_on_chain: false,
            });
        let storage = Storage::Mock(mock);
        let operator = CancellationToken::new();

        let guard = try_acquire_sender_lock(
            &storage,
            0x53_4E_44_5F_45_53_43_52,
            "u1_mock",
            operator.clone(),
            Duration::from_secs(5),
        )
        .await
        .expect("mock acquisition never fails")
        .expect("mock is always available");

        assert!(
            matches!(guard, SenderLockGuard::Noop),
            "mock storage must yield the Noop guard"
        );
        assert!(!operator.is_cancelled(), "holding Noop must not cancel");

        // Fenced writes must still apply on the mock, which has no pinned connection.
        assert!(
            storage.try_park_processing(7).await.expect("mock park"),
            "fenced ops must execute normally on the mock path"
        );

        drop(guard);
        tokio::task::yield_now().await;
        assert!(
            !operator.is_cancelled(),
            "dropping Noop must not cancel the operator token"
        );
    }

    /// U3. A lost lock must stop the operator on the first observation, not the third.
    #[tokio::test(start_paused = true)]
    async fn false_probe_cancels_the_operator_immediately() {
        let program_type = "u3_not_held";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before = lost_count(program_type, "not_held");

        let probe_calls = calls.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::ZERO,
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ProbeOutcome::NotHeld)
                }
            },
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "loop must exit after the first false probe"
        );
        assert!(
            operator.is_cancelled(),
            "a false probe must cancel the operator token"
        );
        assert_eq!(
            lost_count(program_type, "not_held"),
            before + 1.0,
            "a false probe must increment reason=not_held exactly once"
        );
    }

    /// U2. The happy path must be silent, or the heartbeat gets switched off within a week.
    #[tokio::test(start_paused = true)]
    async fn healthy_probe_never_cancels() {
        let program_type = "u2_healthy";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before: Vec<f64> = ALL_REASONS
            .iter()
            .map(|r| lost_count(program_type, r))
            .collect();

        // Stop from inside the probe after 20 ticks so the loop survives many intervals.
        let probe_calls = calls.clone();
        let probe_stop = stop.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::ZERO,
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                let stop = probe_stop.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) >= 19 {
                        stop.cancel();
                    }
                    Ok(ProbeOutcome::Held)
                }
            },
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 20, "loop must keep probing");
        assert!(
            !operator.is_cancelled(),
            "a healthy probe must never cancel the operator token"
        );
        for (reason, was) in ALL_REASONS.iter().zip(before) {
            assert_eq!(
                lost_count(program_type, reason),
                was,
                "healthy probes must not increment reason={reason}"
            );
        }
    }

    /// U4. `Err` cancels immediately. This is where an N-failures rule would have differed.
    #[tokio::test(start_paused = true)]
    async fn probe_error_cancels_the_operator_immediately() {
        let program_type = "u4_probe_error";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before = lost_count(program_type, "probe_error");

        let probe_calls = calls.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::ZERO,
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(sqlx::Error::PoolClosed)
                }
            },
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "loop must exit on the first probe error, with no retry tolerance"
        );
        assert!(operator.is_cancelled(), "a probe error must cancel");
        assert_eq!(lost_count(program_type, "probe_error"), before + 1.0);
    }

    /// U5. Without the timeout a hang looks exactly like health, which is worse than no check.
    #[tokio::test(start_paused = true)]
    async fn hung_probe_cancels_after_the_timeout() {
        let program_type = "u5_probe_timeout";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();

        let before = lost_count(program_type, "probe_timeout");

        let interval = Duration::from_secs(5);
        let started = tokio::time::Instant::now();
        run_lock_heartbeat(
            interval,
            Duration::ZERO,
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            std::future::pending::<Result<ProbeOutcome, sqlx::Error>>,
        )
        .await;

        assert!(operator.is_cancelled(), "a hung probe must cancel");
        assert_eq!(lost_count(program_type, "probe_timeout"), before + 1.0);
        // The verdict lands one full timeout after the tick, never sooner.
        assert_eq!(started.elapsed(), interval + PROBE_TIMEOUT);
    }

    /// U6. Every deploy takes this path, so it must not look like a lock loss and page.
    #[tokio::test(start_paused = true)]
    async fn stop_token_exits_quietly() {
        let program_type = "u6_graceful";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();

        let before: Vec<f64> = ALL_REASONS
            .iter()
            .map(|r| lost_count(program_type, r))
            .collect();

        stop.cancel();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::ZERO,
            stop,
            sender_on_lost(program_type, operator.clone()),
            || async { panic!("a cancelled stop token must not probe") },
        )
        .await;

        assert!(
            !operator.is_cancelled(),
            "a graceful stop must not cancel the operator token"
        );
        for (reason, was) in ALL_REASONS.iter().zip(before) {
            assert_eq!(lost_count(program_type, reason), was);
        }
    }

    /// A zero interval disables probing but still honours the stop token.
    #[tokio::test(start_paused = true)]
    async fn zero_interval_never_probes_but_still_stops() {
        let program_type = "u7_disabled";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();

        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            stopper.cancel();
        });

        run_lock_heartbeat(
            Duration::ZERO,
            Duration::ZERO,
            stop,
            sender_on_lost(program_type, operator.clone()),
            || async { panic!("a zero interval must never probe") },
        )
        .await;

        assert!(!operator.is_cancelled());
    }

    /// A busy connection means a fenced write is in flight, which is proof of life on its own.
    #[tokio::test(start_paused = true)]
    async fn busy_connection_skips_the_tick_without_cancelling() {
        let program_type = "u8_busy";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before: Vec<f64> = ALL_REASONS
            .iter()
            .map(|r| lost_count(program_type, r))
            .collect();

        let probe_calls = calls.clone();
        let probe_stop = stop.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::ZERO,
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                let stop = probe_stop.clone();
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) >= 4 {
                        stop.cancel();
                    }
                    Ok(ProbeOutcome::Busy)
                }
            },
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(!operator.is_cancelled());
        for (reason, was) in ALL_REASONS.iter().zip(before) {
            assert_eq!(lost_count(program_type, reason), was);
        }
    }
    /// U7. A probe that misses the deadline and then answers says the server was
    /// slow, not that the lock is gone. Stopping the role on it would abort a rebuild
    /// that has already dropped the tables, so retry it while the budget lasts.
    #[tokio::test(start_paused = true)]
    async fn inconclusive_probe_within_budget_keeps_probing() {
        let program_type = "u7_within_budget";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before: Vec<f64> = ALL_REASONS
            .iter()
            .map(|r| lost_count(program_type, r))
            .collect();

        // Hangs past the deadline for the first two ticks, then answers for the rest.
        let probe_calls = calls.clone();
        let probe_stop = stop.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::from_secs(30),
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                let stop = probe_stop.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n >= 5 {
                        stop.cancel();
                    }
                    if n < 2 {
                        std::future::pending::<Result<ProbeOutcome, sqlx::Error>>().await
                    } else {
                        Ok(ProbeOutcome::Held)
                    }
                }
            },
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            6,
            "a tolerated failure must not end the loop"
        );
        assert!(
            !operator.is_cancelled(),
            "a probe that failed and then answered must not stop the role"
        );
        for (reason, was) in ALL_REASONS.iter().zip(before) {
            assert_eq!(
                lost_count(program_type, reason),
                was,
                "a tolerated failure must not count as a loss under reason={reason}"
            );
        }
    }

    /// U8. Tolerance is bounded, or an unreachable database would never stop the
    /// role at all. The verdict must land once the budget is spent and not before.
    /// Every probe here misses the deadline, which is the only arm the budget covers.
    #[tokio::test(start_paused = true)]
    async fn inconclusive_probe_cancels_once_the_budget_is_spent() {
        const INTERVAL: Duration = Duration::from_secs(5);
        const BUDGET: Duration = Duration::from_secs(30);

        let program_type = "u8_budget_spent";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();

        let before = lost_count(program_type, "probe_timeout");
        let started = tokio::time::Instant::now();

        run_lock_heartbeat(
            INTERVAL,
            BUDGET,
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            std::future::pending::<Result<ProbeOutcome, sqlx::Error>>,
        )
        .await;

        assert!(operator.is_cancelled(), "a spent budget must stop the role");
        assert_eq!(lost_count(program_type, "probe_timeout"), before + 1.0);
        assert!(
            started.elapsed() >= BUDGET,
            "the role must not stop before the budget has passed, stopped after {:?}",
            started.elapsed()
        );
    }

    /// U9. `NotHeld` is the server answering, not failing to answer. It is proof, so
    /// the budget must not apply to it: the role stops on the first such probe.
    #[tokio::test(start_paused = true)]
    async fn not_held_ignores_the_budget() {
        let program_type = "u9_not_held";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before = lost_count(program_type, "not_held");

        let probe_calls = calls.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::from_secs(3600),
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(ProbeOutcome::NotHeld)
                }
            },
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "proof of loss must exit on the first probe however large the budget"
        );
        assert!(operator.is_cancelled(), "proof of loss must stop the role");
        assert_eq!(lost_count(program_type, "not_held"), before + 1.0);
    }

    /// U10. A terminated backend surfaces as a probe error, and it means the session
    /// and the lock are already gone. Tolerating it would leave a worker writing
    /// against a database another holder could now be rebuilding, so the budget must
    /// never reach this arm however large it is.
    #[tokio::test(start_paused = true)]
    async fn probe_error_ignores_the_budget() {
        let program_type = "u10_probe_error_budget";
        let stop = CancellationToken::new();
        let operator = CancellationToken::new();
        let calls = Arc::new(AtomicU32::new(0));

        let before = lost_count(program_type, "probe_error");

        let probe_calls = calls.clone();
        run_lock_heartbeat(
            Duration::from_secs(5),
            Duration::from_secs(3600),
            stop.clone(),
            sender_on_lost(program_type, operator.clone()),
            move || {
                let calls = probe_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(sqlx::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "backend terminated",
                    )))
                }
            },
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a dead session must exit on the first probe however large the budget"
        );
        assert!(operator.is_cancelled(), "a dead session must stop the role");
        assert_eq!(lost_count(program_type, "probe_error"), before + 1.0);
    }
}
