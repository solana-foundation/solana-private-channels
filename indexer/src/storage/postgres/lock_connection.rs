//! Single-owner handle on the Postgres session holding a sender's advisory lock.
//!
//! Nothing here ever unlocks a sender key, and a session-scoped advisory lock
//! ends only when its session does. So the session is alive exactly when the
//! lock is held, and a write that runs on it proves ownership by construction.
//! There is no token to pass and nothing to forget.
//!
//! The heartbeat shares this connection, so it has one owner and every use of
//! it is bounded by a timeout.

use crate::metrics::OPERATOR_SENDER_LOCK_LOST;
use futures::future::BoxFuture;
use sqlx::{pool::PoolConnection, PgConnection, Postgres};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// Cap on one probe. Without it a hung backend is indistinguishable from a healthy one.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Cap on one fenced write. A stuck write would disable the fence and the heartbeat together.
pub(crate) const FENCED_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Row-lock wait on the pinned session, kept under `FENCED_WRITE_TIMEOUT` so a queued write fails cleanly.
const LOCK_TIMEOUT_MS: &str = "3000";

/// Result of asking Postgres whether this session still holds the sender lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    /// The pinned session provably still holds the lock.
    Held,
    /// The query succeeded and proved the lock is gone.
    NotHeld,
    /// A fenced write holds the connection. It proves liveness itself, so skipping this tick is safe.
    Busy,
    /// The connection is gone and a previous verdict already accounted for it.
    Gone,
}

/// Verdict on one fenced write, separate from the caller's result so the connection can be dropped after the borrow ends.
enum WriteVerdict<T> {
    Applied(T),
    /// The server answered and still holds our lock: ordinary application error.
    AppError(sqlx::Error),
    /// Ownership could not be proven: fail closed.
    Lost(sqlx::Error),
}

pub struct LockConnection {
    key: i64,
    program_type: &'static str,
    operator_token: CancellationToken,
    /// `None` once released or discarded. The mutex gives the heartbeat and the writes one owner.
    conn: Mutex<Option<PoolConnection<Postgres>>>,
    /// Set once ownership stops being provable. Refuses further use without closing; see `poison`.
    poisoned: AtomicBool,
}

impl LockConnection {
    pub(crate) fn new(
        conn: PoolConnection<Postgres>,
        key: i64,
        program_type: &'static str,
        operator_token: CancellationToken,
    ) -> Self {
        Self {
            key,
            program_type,
            operator_token,
            conn: Mutex::new(Some(conn)),
            poisoned: AtomicBool::new(false),
        }
    }

    /// Mark ownership unprovable without closing the connection.
    ///
    /// Closing would be worse than useless. A verdict can be a false positive,
    /// and closing the socket hands the lock to a replacement while this process
    /// is still draining and broadcasting. Holding it until the sender stops
    /// keeps a false positive costing one restart. Writes fail closed meanwhile.
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    /// Bound how long a fenced write waits on a row lock. Best effort; the client timeout still applies.
    pub(crate) async fn apply_lock_timeout(&self) {
        let mut guard = self.conn.lock().await;
        let Some(conn) = guard.as_mut() else { return };
        // `SET` takes no bind parameters, which would force the value into the statement text.
        if let Err(e) = sqlx::query("SELECT set_config('lock_timeout', $1, false)")
            .bind(LOCK_TIMEOUT_MS)
            .execute(&mut **conn)
            .await
        {
            warn!("Could not set lock_timeout on the sender lock connection: {e}");
        }
    }

    /// Run one sender-owned write on the lock-holding session. Failure is classified, never assumed.
    pub(crate) async fn run<T, F>(&self, f: F) -> Result<T, sqlx::Error>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, sqlx::Error>>,
    {
        // Whichever verdict got here first already reported, so stay silent and do not cancel twice.
        if self.is_poisoned() {
            return Err(lock_unavailable());
        }

        let mut guard = self.conn.lock().await;
        let verdict = match guard.as_mut() {
            None => return Err(lock_unavailable()),
            Some(conn) => self.classify(conn, f).await,
        };

        match verdict {
            WriteVerdict::Applied(value) => Ok(value),
            WriteVerdict::AppError(e) => Err(e),
            WriteVerdict::Lost(e) => {
                drop(guard);
                self.poison();
                self.fail_closed(&format!("fenced write could not prove ownership: {e}"));
                Err(e)
            }
        }
    }

    async fn classify<T, F>(&self, conn: &mut PgConnection, f: F) -> WriteVerdict<T>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, sqlx::Error>>,
    {
        let error = match tokio::time::timeout(FENCED_WRITE_TIMEOUT, f(conn)).await {
            Ok(Ok(value)) => return WriteVerdict::Applied(value),
            Ok(Err(e)) => e,
            // Dropped mid-flight, so the protocol state is unknown and cannot be trusted again.
            Err(_) => {
                return WriteVerdict::Lost(sqlx::Error::Protocol(
                    "fenced write timed out on the sender lock connection".into(),
                ))
            }
        };

        if is_connection_level(&error) {
            return WriteVerdict::Lost(error);
        }

        // The server answered, which usually means an ordinary application error.
        // It is not proof on its own: a backend killed mid-query reports a FATAL
        // through the same channel and sqlx surfaces that as `Database` too. So
        // re-probe instead of assuming, and soundness needs no error taxonomy.
        match tokio::time::timeout(
            PROBE_TIMEOUT,
            super::db::probe_advisory_lock_held(conn, self.key),
        )
        .await
        {
            Ok(Ok(true)) => WriteVerdict::AppError(error),
            _ => WriteVerdict::Lost(error),
        }
    }

    /// One heartbeat tick against the pinned session.
    pub(crate) async fn probe(&self) -> Result<ProbeOutcome, sqlx::Error> {
        // Never block here: contention means skip the tick, not declare a timeout.
        if self.is_poisoned() {
            return Ok(ProbeOutcome::Gone);
        }
        let Ok(mut guard) = self.conn.try_lock() else {
            return Ok(ProbeOutcome::Busy);
        };
        let Some(conn) = guard.as_mut() else {
            return Ok(ProbeOutcome::Gone);
        };
        let held = super::db::probe_advisory_lock_held(conn, self.key).await?;
        Ok(if held {
            ProbeOutcome::Held
        } else {
            ProbeOutcome::NotHeld
        })
    }

    /// Unlock explicitly, then return the healthy connection to the pool.
    pub(crate) async fn release(&self) {
        let mut guard = self.conn.lock().await;
        let Some(mut conn) = guard.take() else { return };
        match tokio::time::timeout(
            PROBE_TIMEOUT,
            super::db::release_advisory_lock(&mut conn, self.key),
        )
        .await
        {
            // Only a confirmed unlock may go back to the pool. A pooled connection
            // still holding the lock would lock out every future sender, so detach
            // instead and let the socket close. Never a lock-loss verdict either
            // way: we are stopping anyway and process exit frees the locks.
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("Sender advisory lock release failed: {e}; discarding the connection");
                drop(conn.detach());
            }
            Err(_) => {
                warn!("Sender advisory lock release timed out; discarding the connection");
                drop(conn.detach());
            }
        }
    }

    /// Drop without unlocking. Only safe once the sender has stopped.
    pub(crate) async fn discard(&self) {
        if let Some(conn) = self.conn.lock().await.take() {
            drop(conn.detach());
        }
    }

    fn fail_closed(&self, detail: &str) {
        OPERATOR_SENDER_LOCK_LOST
            .with_label_values(&[self.program_type, "fenced_write"])
            .inc();
        error!(
            reason = "fenced_write",
            "Sender advisory lock ownership could not be proven ({detail}); cancelling the operator"
        );
        self.operator_token.cancel();
    }
}

/// Returned when a write outlives the connection. The path that emptied it already reported.
fn lock_unavailable() -> sqlx::Error {
    sqlx::Error::Protocol("sender lock connection is no longer held".into())
}

/// True when the connection itself is unusable. Everything else is inconclusive and must be re-probed.
fn is_connection_level(e: &sqlx::Error) -> bool {
    matches!(
        e,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolClosed
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::WorkerCrashed
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classifier is the whole soundness argument, so pin both directions.
    #[test]
    fn only_unusable_connections_are_connection_level() {
        assert!(is_connection_level(&sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof"
        ))));
        assert!(is_connection_level(&sqlx::Error::Protocol("x".into())));
        assert!(is_connection_level(&sqlx::Error::PoolClosed));
        assert!(is_connection_level(&sqlx::Error::WorkerCrashed));

        assert!(!is_connection_level(&sqlx::Error::RowNotFound));
        assert!(!is_connection_level(&sqlx::Error::ColumnNotFound(
            "c".into()
        )));
    }
}
