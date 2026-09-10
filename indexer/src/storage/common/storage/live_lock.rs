//! Live-state lock: separates running workers from destructive maintenance.
//!
//! Every indexer and operator holds one advisory key in shared mode for its
//! whole life; resync holds the same key in exclusive mode. Postgres then
//! enforces the rule directly: any number of workers coexist, resync is refused
//! while one is up, and a worker starting mid-rebuild is refused until resync
//! exits. There is no window between checking and acting.

use crate::{
    error::StorageError,
    metrics::LIVE_STATE_LOCK_LOST,
    storage::common::storage::Storage,
    storage::postgres::db::probe_advisory_lock_held,
    storage::postgres::lock_connection::{ProbeOutcome, PROBE_TIMEOUT},
};
use futures::future::BoxFuture;
use sqlx::{Connection, PgConnection};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// Advisory key for the live-state lock. Distinct from the sender keys so the two
/// never contend, and stable forever: changing it would silently stop excluding
/// workers that still hold the old value.
pub const LIVE_STATE_LOCK_KEY: i64 = 0x4C_49_56_45_5F_53_54_54; // "LIVE_STT"

/// How often a holder re-proves it still owns the lock.
///
/// A session-scoped advisory lock dies with its session, and a killed backend
/// frees the lock while the process keeps running on its other pool connections.
/// Only a probe notices that. Deliberately not operator-configurable: the value
/// only makes sense alongside the probe timeout it is tuned against.
pub const LIVE_LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// How long a probe may go unanswered before the holder stops.
///
/// A slow server is not a lost lock, and our session still holds the key while we
/// wait, so retrying risks nothing. Only the timeout is tolerated: a dead session
/// or a "not held" answer is proof and stops the role at once. Sized to ride out a
/// stall without leaving a role running against a database it cannot reach.
pub const LIVE_LOCK_UNCONFIRMED_BUDGET: Duration = Duration::from_secs(30);

/// Which side of the lock a caller wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveLockMode {
    /// Taken by every live indexer and operator. Shared holders never block each other.
    Shared,
    /// Taken by resync alone. Excludes every shared holder and any other exclusive one.
    Exclusive,
}

impl LiveLockMode {
    /// The acquire call for this mode. Both are non-blocking: a refusal is an
    /// answer we want to report, not something to wait for.
    pub(crate) fn acquire_sql(self) -> &'static str {
        match self {
            Self::Shared => "SELECT pg_try_advisory_lock_shared($1)",
            Self::Exclusive => "SELECT pg_try_advisory_lock($1)",
        }
    }

    /// What a refusal in this mode tells the operator, phrased as the fix.
    pub fn refusal_detail(self) -> &'static str {
        match self {
            Self::Shared => {
                "the live-state lock is held exclusively, so a resync is rebuilding this \
                 database; refusing to start"
            }
            Self::Exclusive => {
                "the live-state lock is held by live indexer or operator workers (or another \
                 resync); stop them before running resync"
            }
        }
    }
}

/// Every reason a live-state lock can be declared lost. Shared so the emitting code
/// and the pre-registration below read one list and neither can drift.
const LOSS_REASONS: [&str; 3] = ["not_held", "probe_error", "probe_timeout"];

/// Create the zero series for `role` so the alert has something to compare against
/// from the moment this process holds the lock.
fn preregister_loss_reasons(role: &'static str) {
    for reason in LOSS_REASONS {
        LIVE_STATE_LOCK_LOST.with_label_values(&[role, reason]);
    }
}

/// Report a live-state lock we can no longer prove we own, and stop the role.
fn report_lock_lost(role: &'static str, token: &CancellationToken, reason: &str, detail: &str) {
    LIVE_STATE_LOCK_LOST
        .with_label_values(&[role, reason])
        .inc();
    error!(
        reason,
        "Live-state lock ownership could not be proven ({detail}); stopping the {role}"
    );
    token.cancel();
}

/// The session that holds the lock, plus the key it holds.
#[derive(Debug)]
struct LiveLockSession {
    key: i64,
    /// `None` once the session has been closed. The mutex gives the heartbeat and
    /// any synchronous check one owner.
    conn: Mutex<Option<PgConnection>>,
}

impl LiveLockSession {
    /// One heartbeat tick. A busy connection means a synchronous check is in
    /// flight, which proves the session is alive on its own.
    async fn probe(&self) -> Result<ProbeOutcome, sqlx::Error> {
        let Ok(mut guard) = self.conn.try_lock() else {
            return Ok(ProbeOutcome::Busy);
        };
        let Some(conn) = guard.as_mut() else {
            return Ok(ProbeOutcome::Gone);
        };
        let held = probe_advisory_lock_held(conn, self.key).await?;
        Ok(if held {
            ProbeOutcome::Held
        } else {
            ProbeOutcome::NotHeld
        })
    }

    /// End the session, which is what frees a session-scoped advisory lock.
    async fn close(&self) {
        let Some(conn) = self.conn.lock().await.take() else {
            return;
        };
        if tokio::time::timeout(PROBE_TIMEOUT, conn.close())
            .await
            .is_err()
        {
            warn!("Live-state lock session did not close within the timeout");
        }
    }
}

/// Owns the heartbeat task that owns the lock's session.
#[derive(Debug)]
pub struct LiveLockHandle {
    stop: CancellationToken,
    session: Arc<LiveLockSession>,
    /// Never aborted: aborting would drop the connection at an await point and
    /// skip the close, so the lock would linger until the pool noticed. Cancel
    /// instead and let the task run its own close path.
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LiveLockHandle {
    /// Cancel and wait for the session to close, making the handoff deterministic.
    pub async fn stop_and_wait(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LiveLockHandle {
    fn drop(&mut self) {
        // The task owns the session and closes it itself; this only signals it.
        self.stop.cancel();
    }
}

/// Held for as long as the caller needs the lock. Dropping it releases the lock.
#[derive(Debug)]
pub enum LiveLockGuard {
    Postgres(LiveLockHandle),
    #[cfg(any(test, feature = "test-mock-storage"))]
    Noop,
}

impl LiveLockGuard {
    /// Prove, right now, that this session still holds the lock.
    ///
    /// The heartbeat bounds a silent loss to one interval, which is fine for a
    /// worker that only has to stop. A caller about to do something irreversible
    /// needs the stronger answer, so it asks synchronously first.
    pub async fn ensure_held(&self) -> Result<(), StorageError> {
        match self {
            Self::Postgres(handle) => handle.ensure_held().await,
            #[cfg(any(test, feature = "test-mock-storage"))]
            Self::Noop => Ok(()),
        }
    }

    /// Cancel the heartbeat and wait for the lock to be released.
    pub async fn stop_and_wait(self) {
        match self {
            Self::Postgres(handle) => handle.stop_and_wait().await,
            #[cfg(any(test, feature = "test-mock-storage"))]
            Self::Noop => {}
        }
    }

    /// Run `f` on the session that holds the lock.
    ///
    /// The point is that the two cannot be separated: the lock lives exactly as long as
    /// this session, so work issued here can never outlive it. A probe answers only for
    /// the instant it ran, which is not enough for anything destructive.
    pub(crate) async fn run_fenced<T, F>(&self, f: F) -> Result<T, StorageError>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, sqlx::Error>>,
    {
        match self {
            Self::Postgres(handle) => handle.run_fenced(f).await,
            #[cfg(any(test, feature = "test-mock-storage"))]
            Self::Noop => Err(StorageError::LiveStateLockLost),
        }
    }
}

impl LiveLockHandle {
    async fn ensure_held(&self) -> Result<(), StorageError> {
        let mut guard = self.session.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return Err(StorageError::LiveStateLockLost);
        };
        match tokio::time::timeout(
            PROBE_TIMEOUT,
            probe_advisory_lock_held(conn, self.session.key),
        )
        .await
        {
            Ok(Ok(true)) => Ok(()),
            _ => Err(StorageError::LiveStateLockLost),
        }
    }

    async fn run_fenced<T, F>(&self, f: F) -> Result<T, StorageError>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, sqlx::Error>>,
    {
        let mut guard = self.session.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return Err(StorageError::LiveStateLockLost);
        };
        // Any failure here is a lost lock as far as the caller is concerned. The session
        // is the lock, so a statement it could not run is one we could not fence.
        f(conn).await.map_err(|e| {
            error!("Fenced work on the live-state lock session failed: {e}");
            StorageError::LiveStateLockLost
        })
    }
}

/// Take the live-state lock in `mode`, or fail with what is holding it.
///
/// `on_lost` is cancelled if ownership later stops being provable; pass the token
/// the role already shuts down on. A zero `heartbeat_interval` disables probing.
pub async fn try_acquire_live_lock(
    storage: &Storage,
    mode: LiveLockMode,
    role: &'static str,
    on_lost: CancellationToken,
    heartbeat_interval: Duration,
) -> Result<LiveLockGuard, StorageError> {
    match storage {
        Storage::Postgres(db) => {
            let Some(conn) = db.try_acquire_live_lock(mode).await? else {
                return Err(StorageError::LiveStateLockHeld { requested: mode });
            };
            if heartbeat_interval.is_zero() {
                warn!("Live-state lock heartbeat is disabled; a lost lock will not be detected");
            }
            preregister_loss_reasons(role);
            let session = Arc::new(LiveLockSession {
                key: LIVE_STATE_LOCK_KEY,
                conn: Mutex::new(Some(conn)),
            });
            Ok(LiveLockGuard::Postgres(spawn_heartbeat(
                session,
                role,
                on_lost,
                heartbeat_interval,
            )))
        }
        // Mock has no shared store, so there is nothing to contend on.
        #[cfg(any(test, feature = "test-mock-storage"))]
        Storage::Mock(_) => Ok(LiveLockGuard::Noop),
    }
}

fn spawn_heartbeat(
    session: Arc<LiveLockSession>,
    role: &'static str,
    on_lost: CancellationToken,
    heartbeat_interval: Duration,
) -> LiveLockHandle {
    let stop = CancellationToken::new();
    let task_stop = stop.clone();
    let task_session = session.clone();
    let task = tokio::spawn(async move {
        let probe_session = task_session.clone();
        super::sender_lock::run_lock_heartbeat(
            heartbeat_interval,
            LIVE_LOCK_UNCONFIRMED_BUDGET,
            task_stop.clone(),
            move |reason, detail| report_lock_lost(role, &on_lost, reason, detail),
            move || {
                let session = probe_session.clone();
                async move { session.probe().await }
            },
        )
        .await;

        if task_stop.is_cancelled() {
            task_session.close().await;
            return;
        }

        // A loss verdict can be a false positive on a healthy session, and closing
        // now would free the lock while the caller is still winding down. Keep the
        // session until the guard is dropped, then let it go.
        task_stop.cancelled().await;
        task_session.close().await;
    });
    LiveLockHandle {
        stop,
        session,
        task: Some(task),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lost_count(role: &str, reason: &str) -> f64 {
        LIVE_STATE_LOCK_LOST
            .with_label_values(&[role, reason])
            .get()
    }

    /// The lost-lock alert reads an increase over a window, so every reason needs a
    /// zero series before the first loss or the query matches nothing at all. The
    /// role label has to be the one that will actually emit, which is why this is
    /// done where the role is known rather than alongside the program-type labels.
    #[test]
    fn every_loss_reason_is_preregistered_for_the_role() {
        let role = "u1_preregister";
        preregister_loss_reasons(role);

        for reason in LOSS_REASONS {
            assert_eq!(
                lost_count(role, reason),
                0.0,
                "reason {reason} must exist as a zero series"
            );
        }
    }

    /// The loss report is the whole detection mechanism: it must count under the
    /// role it was given and stop that role, exactly once.
    #[tokio::test]
    async fn report_lock_lost_counts_and_cancels() {
        let role = "u1_role";
        let token = CancellationToken::new();
        let before = lost_count(role, "not_held");

        report_lock_lost(
            role,
            &token,
            "not_held",
            "pg_locks reports the lock is not held",
        );

        assert!(token.is_cancelled(), "a loss must stop the role");
        assert_eq!(
            lost_count(role, "not_held"),
            before + 1.0,
            "a loss must be counted once under its own role and reason"
        );
        assert_eq!(
            lost_count(role, "probe_error"),
            0.0,
            "only the observed reason may be counted"
        );
    }

    /// Both modes must map to the non-blocking call for their own side of the lock.
    /// A blocking acquire here would hang a worker behind a running resync instead
    /// of refusing to start, and a shared acquire for resync would let workers in.
    #[test]
    fn acquire_sql_matches_the_mode() {
        assert_eq!(
            LiveLockMode::Shared.acquire_sql(),
            "SELECT pg_try_advisory_lock_shared($1)"
        );
        assert_eq!(
            LiveLockMode::Exclusive.acquire_sql(),
            "SELECT pg_try_advisory_lock($1)"
        );
    }

    /// The key must never collide with another advisory user on the same database.
    #[test]
    fn live_state_key_is_distinct_from_the_sender_keys() {
        use crate::{config::ProgramType, operator::sender_lock_key};

        assert_ne!(LIVE_STATE_LOCK_KEY, sender_lock_key(ProgramType::Escrow));
        assert_ne!(LIVE_STATE_LOCK_KEY, sender_lock_key(ProgramType::Withdraw));
    }
}
