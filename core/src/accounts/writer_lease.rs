//! Single-writer gate for the write pipeline. A session advisory lock refuses a
//! second write-capable node at startup, and a heartbeat re-proves ownership so a
//! node that has silently lost the lock stops instead of running lease-less.
//!
//! The lock lives exactly as long as its Postgres session, so the lease session
//! is only ever connected and closed here. Ownership is read from a separate
//! connection: a probe that stalls then costs nothing, which is what lets a slow
//! answer be retried instead of taken as a verdict.

use {
    anyhow::{anyhow, Context, Result},
    sqlx::{Connection, PgConnection},
    std::{future::Future, time::Duration},
    tokio::{task::JoinHandle, time::Instant},
    tokio_util::sync::CancellationToken,
    tracing::{error, info, warn},
};

use crate::stage_metrics::SharedMetrics;

/// Identifies the write-pipeline lease. Distinct from the truncation lock so an
/// admin truncation can still run alongside a live writer.
const WRITER_LEASE_LOCK_ID: i64 = 0x50435F_57524954; // "PC_WRIT" as hex

/// How often the lease is asked to prove it still holds the lock.
pub const LEASE_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// Cap on one probe, connect included. Matches the interval so a probe never
/// overlaps the next tick, and a slow answer is retried rather than believed.
const LEASE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long ownership may go unconfirmed before the node stops. Long enough to
/// ride out a checkpoint or a short stall, short enough that a lease-less node
/// is not left writing.
const LEASE_UNCONFIRMED_BUDGET: Duration = Duration::from_secs(30);

/// Idle seconds before Postgres starts probing the lease socket, then the probe
/// spacing and how many may go unanswered. Same values as the gateway uses, and
/// together they reap a vanished writer in under two minutes.
const LEASE_KEEPALIVE_IDLE_SECS: u32 = 60;
const LEASE_KEEPALIVE_INTERVAL_SECS: u32 = 15;
const LEASE_KEEPALIVE_COUNT: u32 = 3;

/// Owns the Postgres session holding the writer lease, and the heartbeat task
/// that re-proves it. The connection is opened directly rather than pooled: sqlx
/// does not reset a returned connection, so a pooled lock would never free.
///
/// There is deliberately no `Drop`. Dropping the handle leaves the heartbeat and
/// its session alive, so a lease that is forgotten keeps the lock, and only
/// `release` hands it back.
pub struct WriterLease {
    stop: CancellationToken,
    /// Taken by `release`, which is the only path that awaits the heartbeat.
    heartbeat: Option<JoinHandle<()>>,
}

/// Which backend holds the lease. The pid alone can be recycled by a restart, so
/// the start time is carried with it and both must match.
#[derive(Clone, Copy, Debug)]
struct LeaseIdentity {
    pid: i32,
    /// Epoch seconds, so the comparison does not depend on either session's
    /// `TimeZone` the way a rendered timestamp would.
    backend_start: f64,
}

/// Read the lease session's own identity, so a later probe can ask about it from
/// somewhere else.
async fn read_lease_identity(conn: &mut PgConnection) -> Result<LeaseIdentity, sqlx::Error> {
    let (pid, backend_start): (i32, f64) = sqlx::query_as(
        "SELECT pid, EXTRACT(EPOCH FROM backend_start)::float8
         FROM pg_stat_activity WHERE pid = pg_backend_pid()",
    )
    .fetch_one(conn)
    .await?;
    Ok(LeaseIdentity { pid, backend_start })
}

/// Does the lease backend still hold the lock? Asked on a connection of its own,
/// which is then closed.
///
/// Deliberately not `pg_try_advisory_lock`, which would silently retake a lost
/// lock and hide the gap. A bigint key lives in `classid` (high 32 bits) and
/// `objid` (low 32), so both are matched.
async fn lease_is_held_by(
    database_url: &str,
    identity: LeaseIdentity,
) -> Result<bool, sqlx::Error> {
    let mut conn = PgConnection::connect(database_url).await?;
    let held: Result<bool, sqlx::Error> = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1 FROM pg_locks l
          JOIN pg_stat_activity a ON a.pid = l.pid
          WHERE l.pid = $1
            AND EXTRACT(EPOCH FROM a.backend_start)::float8 = $2
            AND l.locktype = 'advisory'
            AND l.objsubid = 1
            AND l.granted
            AND ((l.classid::bigint << 32) | l.objid::bigint) = $3
        )
        "#,
    )
    .bind(identity.pid)
    .bind(identity.backend_start)
    .bind(WRITER_LEASE_LOCK_ID)
    .fetch_one(&mut conn)
    .await;

    // Best effort: the answer is already in hand, and a probe connection left to
    // drop closes just the same.
    let _ = conn.close().await;
    held
}

/// Ask Postgres to reap this session quickly if the node's host disappears. A
/// vanished host sends no FIN, so the default leaves the lock held for about two
/// hours. Best effort: an unsupported platform or a unix socket ignores these.
async fn apply_lease_keepalives(conn: &mut PgConnection) {
    // `SET` takes no bind parameters, which would force the values into the
    // statement text.
    let applied = sqlx::query(
        "SELECT set_config('tcp_keepalives_idle', $1, false),
                set_config('tcp_keepalives_interval', $2, false),
                set_config('tcp_keepalives_count', $3, false)",
    )
    .bind(LEASE_KEEPALIVE_IDLE_SECS.to_string())
    .bind(LEASE_KEEPALIVE_INTERVAL_SECS.to_string())
    .bind(LEASE_KEEPALIVE_COUNT.to_string())
    .execute(conn)
    .await;

    if let Err(e) = applied {
        warn!(
            "Could not set TCP keepalives on the writer lease session: {}",
            e
        );
    }
}

/// Re-prove ownership on `interval` and cancel `node_shutdown` once it can no
/// longer be proven.
///
/// A probe that answers "not held" is proof and stops the node at once. A probe
/// that fails or times out is not: Postgres being slow says nothing about the
/// lock, so those are retried until `budget` has passed with no confirmation.
async fn run_heartbeat<P, F>(
    mut conn: PgConnection,
    interval: Duration,
    budget: Duration,
    probe: P,
    stop: CancellationToken,
    node_shutdown: CancellationToken,
    metrics: SharedMetrics,
) where
    P: Fn() -> F,
    F: Future<Output = Result<bool, sqlx::Error>>,
{
    let mut confirmed_at = Instant::now();

    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }

        let reason = match tokio::time::timeout(LEASE_PROBE_TIMEOUT, probe()).await {
            Ok(Ok(true)) => {
                metrics.writer_lease_probe("held");
                confirmed_at = Instant::now();
                continue;
            }
            Ok(Ok(false)) => {
                metrics.writer_lease_probe("not_held");
                "pg_locks reports the lease is no longer held"
            }
            // Not a verdict on its own: the probe runs on its own connection, so
            // a failure here is about reaching Postgres, not about the lock.
            outcome => {
                let (label, detail) = match outcome {
                    Ok(Err(e)) => ("probe_error", format!("the probe query failed: {e}")),
                    _ => (
                        "probe_timeout",
                        "the probe did not answer in time".to_string(),
                    ),
                };
                metrics.writer_lease_probe(label);

                let unconfirmed = confirmed_at.elapsed();
                if unconfirmed < budget {
                    warn!("Writer lease probe inconclusive ({detail}); retrying");
                    continue;
                }
                metrics.writer_lease_lost("probe_unavailable");
                error!(
                    "Writer lease ownership unconfirmed for {}s ({detail}); stopping the node",
                    unconfirmed.as_secs()
                );
                node_shutdown.cancel();
                stop.cancelled().await;
                return;
            }
        };

        metrics.writer_lease_lost("not_held");
        error!("Writer lease ownership could not be proven ({reason}); stopping the node");
        node_shutdown.cancel();

        // A verdict can be a false positive, and the node keeps committing for
        // the whole drain, so hold the socket open until the node reports its
        // workers stopped and then just drop it.
        stop.cancelled().await;
        return;
    }

    // Only a deliberate release reaches here, so the lock is ours to give back.
    // Bounded because the lease session is idle for the node's whole life: a
    // half-open socket would otherwise hang shutdown, and closing frees the lock
    // anyway.
    match tokio::time::timeout(
        LEASE_PROBE_TIMEOUT,
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(WRITER_LEASE_LOCK_ID)
            .execute(&mut conn),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => warn!("Failed to release the writer lease explicitly: {}", e),
        Err(_) => warn!("Releasing the writer lease timed out; closing the session instead"),
    }
    if let Err(e) = conn.close().await {
        warn!("Failed to close the writer lease connection: {}", e);
    }
    info!("Writer lease released");
}

impl WriterLease {
    /// Claim the lease, or fail if another write-capable node already holds it.
    /// Cancels `node_shutdown` if ownership later stops being provable.
    pub async fn acquire(
        database_url: &str,
        node_shutdown: CancellationToken,
        metrics: SharedMetrics,
    ) -> Result<Self> {
        Self::acquire_with_probe_interval(
            database_url,
            node_shutdown,
            metrics,
            LEASE_PROBE_INTERVAL,
        )
        .await
    }

    /// Same, with the probe interval chosen by the caller so a test can drive a
    /// lease loss without waiting out the production one.
    pub async fn acquire_with_probe_interval(
        database_url: &str,
        node_shutdown: CancellationToken,
        metrics: SharedMetrics,
        probe_interval: Duration,
    ) -> Result<Self> {
        let mut conn = PgConnection::connect(database_url)
            .await
            .context("Failed to open the writer lease connection")?;

        // Before the lock, so a session that takes it is already reapable.
        apply_lease_keepalives(&mut conn).await;

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(WRITER_LEASE_LOCK_ID)
            .fetch_one(&mut conn)
            .await
            .context("Failed to acquire the writer lease")?;

        if !acquired {
            return Err(anyhow!(
                "Another write-capable node already holds the writer lease on this database. \
                 Only one write or aio node may run against a Postgres primary."
            ));
        }

        let identity = read_lease_identity(&mut conn)
            .await
            .context("Failed to identify the writer lease session")?;

        let stop = CancellationToken::new();
        let url = database_url.to_string();
        let heartbeat = tokio::spawn(run_heartbeat(
            conn,
            probe_interval,
            LEASE_UNCONFIRMED_BUDGET,
            move || {
                let url = url.clone();
                async move { lease_is_held_by(&url, identity).await }
            },
            stop.clone(),
            node_shutdown,
            metrics,
        ));

        info!("Writer lease acquired");
        Ok(Self {
            stop,
            heartbeat: Some(heartbeat),
        })
    }

    /// Give the lease up so a replacement node can start immediately. The only
    /// path that frees the lock; anything else keeps it.
    pub async fn release(mut self) {
        self.stop.cancel();
        if let Some(heartbeat) = self.heartbeat.take() {
            if let Err(e) = heartbeat.await {
                warn!("Writer lease heartbeat did not stop cleanly: {}", e);
            }
        }
    }

    /// Keep the lock until this process exits, for a shutdown that could not prove
    /// every worker had stopped. Releasing it there would let a replacement start
    /// while a detached worker can still commit.
    ///
    /// Dropping the lease does the same thing; this only says so out loud.
    pub fn hold(self) {
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        stage_metrics::NoopMetrics,
        test_helpers::{postgres_container_url, start_test_postgres_with_url},
    };
    use std::sync::Arc;

    fn metrics() -> SharedMetrics {
        Arc::new(NoopMetrics)
    }

    /// Kill every other backend on this database, which is what a failover, a
    /// connection reaper or `pg_terminate_backend` does to the lease session.
    async fn terminate_other_backends(pool: &sqlx::PgPool) {
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .execute(pool)
        .await
        .expect("failed to terminate the lease backend");
    }

    /// One holder at a time, and the lease must come back after a release, which
    /// is what lets a restarted node take over from the one it replaces.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_lease_is_exclusive_and_reusable_after_release() {
        let (_db, _pg, url) = start_test_postgres_with_url().await;
        let shutdown = CancellationToken::new();

        let first = WriterLease::acquire(&url, shutdown.clone(), metrics())
            .await
            .expect("the first lease must be granted");

        let err = WriterLease::acquire(&url, shutdown.clone(), metrics())
            .await
            .err()
            .expect("a second lease on the same database must be refused");
        assert!(
            err.to_string().contains("writer lease"),
            "the error must name the writer lease, got: {err}"
        );

        first.release().await;
        assert!(
            !shutdown.is_cancelled(),
            "a deliberate release must not look like a lost lease"
        );

        WriterLease::acquire(&url, shutdown, metrics())
            .await
            .expect("the lease must be available again after a release");
    }

    /// A dropped handle says nothing about the workers, which keep running and
    /// keep committing. Freeing the lock there would let a replacement start
    /// alongside them, so only `release` may hand it back.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_lease_keeps_the_lock() {
        let (_db, _pg, url) = start_test_postgres_with_url().await;
        let shutdown = CancellationToken::new();

        drop(
            WriterLease::acquire(&url, shutdown.clone(), metrics())
                .await
                .expect("the first lease must be granted"),
        );

        // Long enough for an unlock to have landed if one were on its way.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            WriterLease::acquire(&url, shutdown, metrics())
                .await
                .is_err(),
            "a dropped lease must keep the lock until the process exits"
        );
    }

    /// Advisory locks are per database, so two deployments sharing one Postgres
    /// server do not lock each other out.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_lease_does_not_span_databases() {
        let (db, pg, url) = start_test_postgres_with_url().await;
        let shutdown = CancellationToken::new();

        sqlx::query("CREATE DATABASE other_deployment")
            .execute(db.pool.as_ref())
            .await
            .expect("failed to create the second database");
        let other_url = postgres_container_url(&pg, "other_deployment").await;

        let _held = WriterLease::acquire(&url, shutdown.clone(), metrics())
            .await
            .expect("the first lease must be granted");

        WriterLease::acquire(&other_url, shutdown, metrics())
            .await
            .expect("a lease on a different database must not be blocked");
    }

    /// A lease can be lost without the node noticing: a failover, a proxy reaper
    /// or a terminated backend all drop the lock while the node keeps writing.
    /// "Not held" is proof, so unlike an unanswered probe it gets no tolerance.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lost_lock_stops_the_node_on_the_first_probe() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let shutdown = CancellationToken::new();

        let _lease = WriterLease::acquire_with_probe_interval(
            &url,
            shutdown.clone(),
            metrics(),
            Duration::from_millis(50),
        )
        .await
        .expect("the lease must be granted");

        terminate_other_backends(db.pool.as_ref()).await;

        // Well inside LEASE_UNCONFIRMED_BUDGET, so a pass here is the definitive
        // branch firing and not the budget running out.
        tokio::time::timeout(Duration::from_secs(10), shutdown.cancelled())
            .await
            .expect("losing the lease must stop the node");
    }

    /// The lock's truth is server-side, so the probe must be able to read it from
    /// a connection that is not the lease's own.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_probe_reads_the_lease_from_a_separate_session() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let mut conn = PgConnection::connect(&url)
            .await
            .expect("failed to open the lease connection");
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(WRITER_LEASE_LOCK_ID)
            .fetch_one(&mut conn)
            .await
            .expect("failed to take the lease lock");
        assert!(taken, "the lease lock must start free");

        let identity = read_lease_identity(&mut conn)
            .await
            .expect("failed to read the lease identity");

        assert!(
            lease_is_held_by(&url, identity)
                .await
                .expect("the probe must answer"),
            "a held lease must probe as held from another session"
        );

        terminate_other_backends(db.pool.as_ref()).await;

        assert!(
            !lease_is_held_by(&url, identity)
                .await
                .expect("the probe must answer"),
            "a terminated lease backend must probe as not held"
        );
    }

    /// Can a third session take `id` right now? Unlocks again so the answer is
    /// repeatable on a pooled connection, which sqlx never resets.
    async fn lock_is_free(pool: &sqlx::PgPool, id: i64) -> bool {
        let mut conn = pool.acquire().await.expect("failed to check the lock");
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(id)
            .fetch_one(&mut *conn)
            .await
            .expect("failed to probe the lock");
        if taken {
            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(id)
                .execute(&mut *conn)
                .await
                .expect("failed to release the probe lock");
        }
        taken
    }

    /// Stands in for the lease lock in the heartbeat tests. Held on the same
    /// session, so it is held for exactly as long, and a third session can watch
    /// it from outside.
    const WITNESS_LOCK_ID: i64 = 0x50435F_5749544E; // "PC_WITN" as hex

    /// A session holding the witness lock, plus the tokens a heartbeat runs on.
    async fn witness_session(url: &str) -> (PgConnection, CancellationToken, CancellationToken) {
        let mut conn = PgConnection::connect(url)
            .await
            .expect("failed to open the lease connection");
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(WITNESS_LOCK_ID)
            .fetch_one(&mut conn)
            .await
            .expect("failed to take the witness lock");
        assert!(taken, "the witness lock must start free");
        (conn, CancellationToken::new(), CancellationToken::new())
    }

    fn probe_failure() -> sqlx::Error {
        sqlx::Error::Protocol("the probe connection is unavailable".into())
    }

    /// A verdict can be a false positive, and the node keeps committing for the
    /// whole drain. So an unprovable probe must stop the node without ending the
    /// session: the lock lives exactly as long as that session does.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unprovable_probe_keeps_the_lease_session_until_it_is_stopped() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let (conn, stop, node_shutdown) = witness_session(&url).await;

        let heartbeat = tokio::spawn(run_heartbeat(
            conn,
            Duration::from_millis(50),
            Duration::from_secs(30),
            || async { Ok(false) },
            stop.clone(),
            node_shutdown.clone(),
            metrics(),
        ));

        tokio::time::timeout(Duration::from_secs(10), node_shutdown.cancelled())
            .await
            .expect("an unprovable probe must stop the node");

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !heartbeat.is_finished(),
            "the heartbeat must outlive the verdict, or the lock goes with it"
        );
        assert!(
            !lock_is_free(db.pool.as_ref(), WITNESS_LOCK_ID).await,
            "the lease session must still be open while the node drains"
        );

        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), heartbeat)
            .await
            .expect("stopping the lease must end the heartbeat")
            .expect("the heartbeat must not panic");

        // The socket closes as the connection drops, so the lock frees a moment
        // later rather than on the await above.
        for _ in 0..50 {
            if lock_is_free(db.pool.as_ref(), WITNESS_LOCK_ID).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("stopping the lease must close its session");
    }

    /// Postgres being slow says nothing about the lock, and this node is the only
    /// writer in the deployment. So a probe that fails to answer must be retried,
    /// not taken as a verdict, and the lease session must survive the retries.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unanswered_probe_is_tolerated_while_the_budget_lasts() {
        let (db, _pg, url) = start_test_postgres_with_url().await;
        let (conn, stop, node_shutdown) = witness_session(&url).await;

        // Fails for longer than one tick, then answers for the rest of the test.
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&probes);
        let heartbeat = tokio::spawn(run_heartbeat(
            conn,
            Duration::from_millis(20),
            Duration::from_millis(300),
            move || {
                let n = counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if n < 3 {
                        Err(probe_failure())
                    } else {
                        Ok(true)
                    }
                }
            },
            stop.clone(),
            node_shutdown.clone(),
            metrics(),
        ));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !node_shutdown.is_cancelled(),
            "a probe that failed and then answered must not stop the node"
        );
        assert!(
            probes.load(std::sync::atomic::Ordering::SeqCst) > 3,
            "the heartbeat must keep probing after a failure"
        );

        // The lock still frees on request, so the tolerated failures left the
        // lease session usable.
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), heartbeat)
            .await
            .expect("stopping the lease must end the heartbeat")
            .expect("the heartbeat must not panic");
        for _ in 0..50 {
            if lock_is_free(db.pool.as_ref(), WITNESS_LOCK_ID).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("a tolerated failure must leave the lease session releasable");
    }

    /// Tolerance is bounded: once ownership has gone unconfirmed for the budget
    /// the node stops, and it still parks rather than dropping the session, since
    /// an unanswered probe is exactly the case that may be a false positive.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unanswered_probe_stops_the_node_when_the_budget_runs_out() {
        const BUDGET: Duration = Duration::from_millis(300);

        let (db, _pg, url) = start_test_postgres_with_url().await;
        let (conn, stop, node_shutdown) = witness_session(&url).await;

        let started = tokio::time::Instant::now();
        let heartbeat = tokio::spawn(run_heartbeat(
            conn,
            Duration::from_millis(20),
            BUDGET,
            || async { Err(probe_failure()) },
            stop.clone(),
            node_shutdown.clone(),
            metrics(),
        ));

        tokio::time::timeout(Duration::from_secs(10), node_shutdown.cancelled())
            .await
            .expect("an unconfirmed lease must stop the node once the budget runs out");
        assert!(
            started.elapsed() >= BUDGET,
            "the node must not stop before the budget has passed"
        );

        assert!(
            !heartbeat.is_finished(),
            "the heartbeat must outlive the verdict, or the lock goes with it"
        );
        assert!(
            !lock_is_free(db.pool.as_ref(), WITNESS_LOCK_ID).await,
            "the lease session must still be open while the node drains"
        );
        stop.cancel();
    }

    /// A host that vanishes sends no FIN, so without these the lease backend sits
    /// in recv() holding the lock until the OS default expires, about two hours.
    /// Postgres reports 0 over a unix socket; the test container speaks TCP.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_lease_session_sets_its_own_tcp_keepalives() {
        let (_db, _pg, url) = start_test_postgres_with_url().await;
        let mut conn = PgConnection::connect(&url)
            .await
            .expect("failed to open the lease connection");

        apply_lease_keepalives(&mut conn).await;

        let settings: Vec<(String, String)> = sqlx::query_as(
            "SELECT name, setting FROM pg_settings
             WHERE name LIKE 'tcp_keepalives%' ORDER BY name",
        )
        .fetch_all(&mut conn)
        .await
        .expect("failed to read the keepalive settings");

        assert_eq!(
            settings,
            vec![
                (
                    "tcp_keepalives_count".to_string(),
                    LEASE_KEEPALIVE_COUNT.to_string()
                ),
                (
                    "tcp_keepalives_idle".to_string(),
                    LEASE_KEEPALIVE_IDLE_SECS.to_string()
                ),
                (
                    "tcp_keepalives_interval".to_string(),
                    LEASE_KEEPALIVE_INTERVAL_SECS.to_string()
                ),
            ],
            "the lease session must carry its own keepalives"
        );
    }
}
