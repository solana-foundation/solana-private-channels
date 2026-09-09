# Runbook - Sender Advisory Lock Lost

This runbook covers the **`sender-lock-lost` Grafana alert**, raised by the
counter `private_channel_operator_sender_lock_lost_total`.

The alert means one thing: an operator's sender could not prove it still owned
its Postgres advisory lock, so it **cancelled the entire operator and shut
itself down**. This is deliberate. The advisory lock is what makes the sender a
singleton, and a sender that has lost it can be running alongside a replacement
that also believes it is the only sender. Two live senders can both sign a
remint for the same withdrawal, which is a double-mint risk.

The shutdown is the mitigation, not the incident. The incident is *why the lock
stopped being provable*.

This is not a transaction-status alert. No row is marked `failed` or
`manual_review` by it, and there is no webhook payload; it pages through Grafana
like [`indexer_block_unavailable.md`](indexer_block_unavailable.md) and
[`reconciliation_halt_runbook.md`](reconciliation_halt_runbook.md).

Every query below is **read-only**. There is no recovery SQL for this condition.

---

## What the operator does automatically

The sender takes a per-role session advisory lock at startup and then re-proves
ownership on a fixed interval (default 5s) by reading `pg_locks` on that same
pinned connection. Sender-owned failure-path writes also execute on that
connection, so each of them is its own ownership proof.

The first time ownership cannot be proven, the sender increments this counter,
cancels the operator's shared cancellation token, drains its in-flight work and
exits. The supervisor then terminates the process, and orchestration restarts
it. Worst-case detection is `interval + 2s`, about 7s on the default.

There is no retry and no consecutive-failure tolerance. That is a deliberate
trade: during a full database outage the sender cannot do useful work anyway and
no replacement can take the lock either, while during a partial outage (our
session dead, the database up) a replacement absolutely can start, and the probe
cannot tell the two apart.

## `reason` label

| `reason` | Meaning |
|---|---|
| `not_held` | The probe succeeded and proved the lock is gone. Definitive loss. |
| `probe_error` | The probe query failed on the pinned session. A checked-out sqlx connection is never healed in place, so this is strong evidence the session ended. A terminated backend usually lands here. |
| `probe_timeout` | The probe did not answer within 2s. A hung backend or a black-holed socket. |
| `fenced_write` | A sender-owned write could not be proven to have executed inside the lock's own session. |

## Symptom

- Grafana alert `sender-lock-lost`, severity critical.
- The operator process restarts, once, shortly after the alert.
- **`OPERATOR_TASK_EXIT` will normally be labelled `fetcher`, not `sender`.**
  The supervision `select!` is `biased` and the sender is the one task that must
  finish draining before it can exit, so another arm resolves first. Do not
  treat a `fetcher` label as evidence that the fetcher was the problem, and do
  not use `OPERATOR_TASK_EXIT{task="sender"}` as a lock-loss signal.

## Triage

### 1. Confirm the operator came back

```
kubectl get pods -l app=<operator> -o wide
```

Expect exactly one running pod per role, with a recent restart. If it is
crash-looping, the cause is not this alert; follow the boot failure instead.

### 2. Confirm exactly one sender holds each role key

Read-only, against the operator database:

```sql
SELECT
  l.pid,
  ((l.classid::bigint << 32) | l.objid::bigint) AS lock_key,
  a.application_name,
  a.client_addr,
  a.backend_start,
  a.state
FROM pg_locks l
JOIN pg_stat_activity a USING (pid)
WHERE l.locktype = 'advisory'
  AND l.objsubid = 1
  AND l.granted
  AND ((l.classid::bigint << 32) | l.objid::bigint) IN (
        6002810529307116370,   -- escrow sender  ("SND_ESCR")
        6002810529608127063    -- withdraw sender ("SND_WDRW")
      );
```

Expect at most one row per key. **Two rows for one key is impossible** and means
the query was run against the wrong database. Zero rows for a key whose operator
is running means that operator has not reached its sender yet, or is refusing to
start.

`client_addr` is the important column: if it does not match the pod you expect,
a second operator exists.

### 3. Check whether a second operator pod exists or existed

The lock only fences processes sharing one database. Look for a second
deployment, a stuck pod from a rolling restart, a manually-run operator, or a
job that points at the same `DATABASE_URL`.

```
kubectl get pods --all-namespaces -o wide | grep -i operator
```

### 4. Correlate with the remint claim-lost counter

```
increase(private_channel_operator_remint_claim_lost_total[1h])
```

Nonzero in the same window means a second sender did not merely exist, it
survived long enough to sign a remint. That is a materially worse diagnosis; see
the `remint-claim-lost` alert. Zero means the shutdown fenced the window before
anything value-bearing happened, which is the intended outcome.

### 5. Establish the database-side cause

The counter says ownership stopped being provable; it does not say why. Check,
in order:

- A Postgres failover or restart in the window (`pg_postmaster_start_time()`,
  provider event log).
- A connection reaper: `pg_stat_activity` idle-session settings, a proxy idle
  timeout, or a load balancer TCP idle timeout.
- A `pg_terminate_backend` from maintenance tooling.
- **A pooler in transaction-pooling mode.** Session-level advisory locks do not
  survive it at all. If a pgbouncer or similar sits in front of Postgres, this
  is the first thing to rule out, because in that topology neither the lock nor
  the heartbeat means anything.

## Mitigation

Normally none: the restart is the mitigation, and one isolated firing after a
database failover is expected behaviour.

If the alert fires repeatedly on a database that is otherwise healthy, the
heartbeat itself is the availability problem. **There is no runtime kill switch.**
The interval is the compile-time constant `SENDER_LOCK_HEARTBEAT_INTERVAL` in
`indexer/src/operator/sender/mod.rs`, so disabling or lengthening the probe needs
a code change and a redeploy. Escalate to the on-call engineer rather than
looking for a config key or an environment variable.

Setting the interval to zero in code stops the probe but **not** the fence:
sender-owned failure-path writes still execute on the lock-holding session, so
`reason=fenced_write` can still fire and still shuts the operator down. That is
deliberate, since those are exactly the writes that must not happen without the
lock. The `reason` label tells you which mechanism is firing, and `fenced_write`
means a real sender write could not be proven rather than a flaky probe.

The pre-send remint claim remains the fund-safety control either way, so a
heartbeat problem is an availability incident, not a custody one.

## Escalate if

- Two operator pods are found running against the same database. Stop the
  duplicate first, then investigate.
- `private_channel_operator_remint_claim_lost_total` also incremented.
- The alert fires more than once in a 30-day window, which means lock loss is a
  recurring condition rather than a one-off.

See [`_escalation.md`](_escalation.md).
