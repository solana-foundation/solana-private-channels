# Runbook - Writer Lease Unavailable

This runbook covers a **write-capable node that refuses to start** with:

```
Another write-capable node already holds the writer lease on this database.
Only one write or aio node may run against a Postgres primary.
```

when, as far as you can tell, **no other write node is running**.

This is a **node** condition, not an operator one. It marks no transaction row
`failed` or `manual_review`, so it is **not routed by the webhook dispatch
table** in [`README.md`](README.md). It follows the same shape as
[`corrupt_account_row.md`](corrupt_account_row.md): recognize it from the
crash-loop plus the log line above, not from an alert.

It matters more than a normal boot failure. The gateway routes
`sendTransaction` to a single `--write-url` ([`../CONFIG.md`](../CONFIG.md)), so
while no write node can start, **the deployment accepts no transactions at all**.
Reads keep serving.

Every query below is read-only except the one in
[Recovery](#recovery-terminate-the-zombie-backend), which is clearly marked.

---

## What the lease is

A write or aio node takes a Postgres **session-scoped advisory lock** at startup
and holds it for its whole life. A second write-capable node against the same
database is refused. That is the mechanism that stops two writers producing
mixed ledger state.

A session advisory lock lives exactly as long as its Postgres backend, and that
backend lives as long as its TCP connection. There is no lease expiry, no TTL and
no fencing token to revoke. **The connection is the lease.**

## Why it is usually not stuck

Almost every way a node dies frees the lock immediately:

| How the node ended | What Postgres sees | Lock freed in |
|---|---|---|
| Clean shutdown | Explicit unlock, then close | Milliseconds |
| Panic, `exit`, OOM kill, `SIGKILL` | Kernel sends FIN, backend sees EOF | Milliseconds |
| Container stopped or rescheduled | Same FIN | Milliseconds |

So if a replacement is refused for more than a second or two, the old node's
**socket is still open from Postgres's point of view**.

## The one case that sticks: a vanished host

If the host disappears without closing its sockets, no FIN is ever sent. The
backend stays blocked holding the lock, and Postgres only notices when TCP
keepalives expire. Causes:

- hypervisor or cloud-provider instance kill,
- kernel panic or hard power loss,
- network partition between node and database,
- a Kubernetes node going `NotReady` with the pod not evicted.

The lease session asks Postgres for its own keepalives (60s idle, then 3 probes
15s apart), so a vanished writer is normally reaped in **under two minutes**.
Two things defeat that:

- **A connection pooler.** Behind pgbouncer or similar, the keepalives protect
  the pooler-to-Postgres hop, not the node-to-pooler hop. The pooler's server
  session can outlive the node that owned it. The indexer's
  [`sender_lock_lost_runbook.md`](sender_lock_lost_runbook.md) carries the same
  caveat for the same reason.
- **A platform that ignores `SO_KEEPALIVE`.** The node logs
  `Could not set TCP keepalives on the writer lease session` if the request was
  rejected outright, but a silently-ignored setting logs nothing.

Without keepalives the fallback is the OS default: on Linux
`net.ipv4.tcp_keepalive_time = 7200` plus 9 probes at 75s, so **about 2h11m**.

## The other case that sticks: a node that stopped without releasing

The lease is handed back only by a clean `shutdown()`. Every other ending keeps
the lock until the process exits, on purpose: a node that cannot prove its
workers stopped can still be committing, and freeing the lock there would let a
replacement start beside it.

So a node that stopped for one of these reasons still holds the lock until its
process is gone:

- **`Writer lease ownership unconfirmed for Ns`** - Postgres stopped answering
  the ownership probe for 30 seconds. The lock is probably still held and still
  ours, which is why the session is kept.
- **`Holding the writer lease: a worker did not stop in time`** - a worker
  ignored its abort during shutdown.

In both cases the fix is to make sure the old process is actually dead. Once it
exits the socket closes and the lock frees in milliseconds. Only reach for
[Recovery](#recovery-terminate-the-zombie-backend) if the process is gone and the
lock is not.

Two counters describe this from the outside:

- `private_channel_writer_lease_probe_total{outcome}` - `held`, `not_held`,
  `probe_error`, `probe_timeout`. A rising `probe_error` or `probe_timeout` with
  no `not_held` is a slow or unreachable database, not a lost lease.
- `private_channel_writer_lease_lost_total{reason}` - `not_held` (proof: another
  session holds it, or the backend is gone) or `probe_unavailable` (the 30s
  budget ran out).

> **Do not set `idle_session_timeout` on the node's database role.** The lease
> session is idle for the node's whole life by design, since ownership is read
> from a separate connection. A timeout would kill it, free the lock under a
> running writer, and stop the node on the next probe.

## Triage

### 1. Confirm the lock is held, and by whom

```sql
SELECT a.pid,
       a.backend_start,
       a.state,
       a.client_addr,
       a.application_name,
       now() - a.state_change AS idle_for
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'advisory'
  AND l.granted
  AND l.objsubid = 1
  AND ((l.classid::bigint << 32) | l.objid::bigint) = 22592074902817108;
```

`22592074902817108` is the writer-lease key, `PC_WRIT` in ASCII.

- **No rows.** The lease is free and this runbook is not your problem. The node
  is failing for another reason; read the actual startup error.
- **One row.** Note `client_addr` and `backend_start`. That is the claimed owner.

### 2. Decide whether that owner is alive

This is the whole decision, and getting it wrong is worse than waiting.

- Does `client_addr` match a host that still exists?
- Is a write or aio node process running on it?
- Is the ledger tip still advancing?

```sql
SELECT MAX(slot) AS tip FROM blocks;
```

Run it twice, a few seconds apart. **A tip that is still moving means a live
writer is still committing.** Do not go to recovery. You are looking at a
genuine duplicate-start attempt, and the refusal is the system working.

### 3. If the tip is frozen and the host is gone

You have a zombie backend. Recovery below.

## Recovery: terminate the zombie backend

> **Only after step 2.** Terminating the backend of a *live* writer mid-batch is
> safe for durability (its transaction rolls back) but it will stop that node,
> and if a replacement then starts from the old tip you have traded a boot
> failure for a restart. Confirm the tip is frozen first.

```sql
SELECT pg_terminate_backend(a.pid)
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'advisory'
  AND l.granted
  AND l.objsubid = 1
  AND ((l.classid::bigint << 32) | l.objid::bigint) = 22592074902817108;
```

Re-run the triage query in step 1. It must return no rows. Then start the write
node; it should acquire the lease immediately.

If the lock reappears held by a new pid, a real writer took it. Stop and find it.

## Follow-up

- **Behind a pooler?** Pin the lease connection to a direct Postgres endpoint, or
  set the pooler's own server-side idle timeout below the operator patience
  threshold. A pooler between a node and its lease is the one configuration that
  can reproduce the multi-hour stall despite the keepalives.
- **Repeat occurrences** point at the platform, not the node: instances being
  killed without draining, or a partition that outlives the keepalive window.

## Escalation

See [`_escalation.md`](_escalation.md). This is a full write outage, so it
escalates on the same footing as a pipeline halt.
