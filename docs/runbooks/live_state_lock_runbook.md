# Runbook - Live-State Lock

The live-state lock keeps running workers and a destructive resync off the same
database. Every indexer and operator takes one Postgres advisory key in **shared**
mode for its whole life; `resync` takes the same key in **exclusive** mode. Postgres
enforces the rest: workers coexist freely, and the two sides can never overlap.

This runbook covers the refusals it produces, the unfinished-resync marker that backs it
up, and the alert for losing it.

As with every runbook here, any recovery SQL is **bookkeeping, not fund movement** -
see [`README.md`](README.md).

---

## Symptom 1: a worker refuses to start

```
Indexer refusing to start: the live-state lock is held exclusively, so a resync is
rebuilding this database; refusing to start
```

(The operator logs the same line with `Operator refusing to start`.)

**This is correct behaviour, not a fault.** A resync is deleting and rebuilding rows
this process would act on. Starting anyway would write rows the resync is about to
delete, and advance a checkpoint over slots that are not rebuilt yet.

**Action:** wait for the resync to finish, then let orchestration restart the worker.
Confirm a resync really is running:

```sql
SELECT pid, mode, backend_start, state
FROM pg_locks l JOIN pg_stat_activity a USING (pid)
WHERE l.locktype = 'advisory' AND l.objsubid = 1 AND l.granted
  AND ((l.classid::bigint << 32) | l.objid::bigint) = 5497019676134429780;
```

`ExclusiveLock` is the resync. If no row comes back and workers still refuse, the
refusal is stale only if the process holding it died without closing its session,
which cannot happen: the lock dies with the session. Re-check the query.

## Symptom 2: resync refuses to start

```
Refusing to resync: the live-state lock is held by live indexer or operator workers
(or another resync); stop them before running resync
```

**Action:** scale every indexer and operator on this database to zero, confirm with
the query above (`ShareLock` rows are workers), then re-run. Do not try to force it.

> **Deploy-ordering caveat.** Only a worker running a build that takes this lock is
> visible to the refusal. During a rolling upgrade, confirm workers are stopped by
> process or deployment, not by the absence of a refusal.

**If the query shows a holder but no such worker is running**, its host vanished
without closing the socket. The lock session carries TCP keepalives (60s idle, 15s
apart, 3 missed) so Postgres reaps it in under two minutes on its own. Wait that out
and re-run. If a holder outlives it, confirm the pid against
`pg_stat_activity.backend_start` and `client_addr` before terminating it by hand.

## Symptom 3: resync refuses because of a reconciliation halt

```
reconciliation halt is set (<reason>); a rebuild would hide the evidence behind it,
so resolve and clear the halt first
```

A halt means custody and the ledger disagree. Rebuilding the ledger now would bury the
evidence of why, before anyone has worked out what went wrong.

**Action:** work the halt to a conclusion first via
[`reconciliation_halt_runbook.md`](reconciliation_halt_runbook.md), clear the flag,
then resync. There is deliberately no override flag.

## Symptom 4: a worker or resync refuses because a resync did not finish

```
Indexer refusing to start: an unfinished <program> resync owns this database; rerun
resync for <program> before starting workers
```

(The operator logs the same line with `Operator refusing to start`. A resync of the other
program refuses with the same message.)

A resync deletes its program's rows and, in the same transaction, writes a
`resync_state` row and sets the reconciliation halt with the reason `unfinished
<program> resync: rerun resync for <program>; do not clear this halt by hand`. The
halt is what stops operators built before the marker: they still skip fetching
while a halt is set. Only a rebuild that completes clears both, again in one
transaction on the lock session, and it clears the halt only if the reason is
still its own. The row survives the resync
process, so its presence means a resync died after deleting rows: a crash, an OOM, a lost
lock, or a connection that failed at COMMIT. The rows it deleted are not rebuilt, and a
worker starting now would mint or release against a half-built ledger.

**Action:** re-run `resync` for the named program, with the same arguments, until it
completes. The rerun deletes and rebuilds that program's rows again from scratch, and
clears the row and its halt only when it finishes. Do not delete the `resync_state` row
or clear that halt by hand: that is exactly what lets a worker start on the half-built
rows. If the halt reason has been replaced (an older operator's reconciliation tripped
meanwhile), the rerun refuses; work that halt through the reconciliation halt runbook
first, clear it, then rerun.

## Symptom 5: resync refuses because work is still in flight

```
rows this resync would rebuild are still processing, pending remint, in manual review,
or have unsettled broadcast journals; run the operator until they settle, stop it, then
retry
```

A row of the resyncing program has a mint, remint or release that may still land. The
resync deletes the row's broadcast journal, so a transaction landing after the resync
reads the chain would leave the rebuilt row `pending` and be sent again.

**Action:** start the operator for that program and let recovery settle the rows until
none is `processing` or `pending_remint` and every journaled row reaches a terminal
status. Work any `manual_review` row to a conclusion. Then stop every worker and re-run the resync. There
is no override. Rows of the other program never block it: the resync leaves them, and
their journals, untouched.

## Symptom 6: `live-state-lock-lost` alert

`private_channel_live_state_lock_lost_total` increased. A role could not prove it
still owns the lock, so it stopped itself. `role` is `indexer` or `operator`;
`reason` is `not_held`, `probe_error` or `probe_timeout`.

`not_held` and `probe_error` fire on the first bad probe: the server answered that the
lock is gone, or the session itself is dead, and both are proof. `probe_timeout` is
different, and only fires after ownership has gone unconfirmed for 30s, so it means the
database was unreachable or too slow to answer for that whole window rather than a
single slow query. Expect a lost-lock alert to trail the underlying event by up to that
long, and no longer: the 30s is an upper bound, not an approximation.

A role that loses the lock stops without draining and exits non-zero, so its supervisor
restarts it. That is deliberate. Postgres frees the lock the moment the session dies, so
a resync may already be deleting these rows, and a drain would keep writing into them.
Nothing is lost that an abrupt restart would not also lose: the durable checkpoint stands
and the role replays from it once the resync has finished and the lock is free again.

Resync stops on the same verdict but cannot page here: it is a one-shot command with
no metrics server, so a losing resync surfaces as a failed command with the error in
its output. Treat that the same way as this symptom, then re-run it.

A session-scoped advisory lock dies with its session, so this means the session went
away while the process kept running. Establish the database-side cause, in order:

- A Postgres failover or restart in the window (`pg_postmaster_start_time()`).
- A connection reaper: idle-session settings, a proxy or load-balancer idle timeout.
- A `pg_terminate_backend` from maintenance tooling.
- **A pooler in transaction-pooling mode.** Session-level advisory locks do not
  survive it at all. In that topology neither this lock nor the sender lock means
  anything, so rule it out first. Same constraint as
  [`sender_lock_lost_runbook.md`](sender_lock_lost_runbook.md).

**Mitigation:** normally none for a worker; the restart is the mitigation and one
isolated firing after a failover is expected. A resync that reported the same verdict
aborted partway and left the database half rebuilt, with the `resync_state` marker set
(Symptom 4): re-run it to completion. The chain is still the source of truth, so a rerun
recovers it.
