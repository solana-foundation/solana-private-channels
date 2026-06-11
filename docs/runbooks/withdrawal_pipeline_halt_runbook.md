# Runbook - Withdrawal Pipeline Halt

This runbook covers the **SMT-root-mismatch boot pre-flight**. On startup a
withdraw operator first reconciles any in-flight releases, then validates the
local SMT root against the on-chain `withdrawal_transactions_root` **before**
spawning the pipeline. The common cause - a release that landed on-chain whose
`Completed` write was lost - is now **auto-reconciled** at boot from the durable
release signature. The operator only refuses to start on an **unforeseen**
divergence that the reconcile cannot resolve.

This halt has **no dedicated "pipeline halted" alert**. A refuse-to-start
surfaces as the operator process exiting at boot (a crash-loop under the
supervisor) with `SMT root mismatch` in the error logs, and - if the divergence
came from an in-flight row the reconcile quarantined - a `manual_review` webhook
for that row. No withdrawal is ever marked `failed` by this path. Recognize it by
the boot-time crash-loop plus the log markers, not a single halt event, and it is
not routed by the dispatch table in [`README.md`](README.md).

As with every runbook here, the recovery `UPDATE` statements are
**bookkeeping, not fund movement** - see
[`README.md`](README.md) § "Recovery SQL is bookkeeping; fund restoration
is human-in-the-loop".

---

## SMT root mismatch on startup

### What the operator does automatically

On boot, before any withdrawal is fetched, locked, or processed, the operator:

1. **Reconciles in-flight releases.** Every consumed nonce has a release
   signature persisted **write-ahead** (before broadcast), so a release that
   landed but never reached `Completed` is detected by an on-chain finality
   check and promoted to `Completed` - re-recording the nonce. A row with no
   recorded signature, or one the RPC cannot classify, is quarantined to
   `manual_review` (never `failed`).
2. **Validates** the rebuilt local SMT root against the on-chain root.

If validation passes, the pipeline starts normally
(`SMT root verification passed` in the logs). A residual mismatch the reconcile
could not resolve is a **fail-closed refuse-to-start**: the operator returns an
error and exits without consuming nonces against a tree it cannot reason about.
This should never fire under the known cause (the write-ahead signature plus the
boot reconcile close that gap); it guards an unforeseen divergence such as a
program bug or a manual on-chain admin operation.

### Symptom

- The withdraw operator does not stay up: it exits at boot and the supervisor
  restarts it in a loop. New withdrawals never reach `completed`.
- The operator error logs carry `SMT root mismatch` at boot (see Detection).
- **No** withdrawal row is marked `failed`. If the divergence originated from an
  in-flight row, that single row is in `manual_review` with a recovery-worker
  alert; no collateral rows exist.

### Detection

`validate_smt_root` (`indexer/src/operator/sender/state.rs`) compares the local
SMT root against the on-chain `withdrawal_transactions_root` and, on mismatch,
emits an `error!` log carrying the instance, tree index, both roots, and the
DB-derived nonces, e.g.:

```
SMT root mismatch: database out of sync with on-chain state. ...
  instance=<pda> tree_index=<n>
  local_root=[...] onchain_root=[...] nonces=[...]
```

Grep the operator logs for `SMT root mismatch` to confirm, and check that the
process is crash-looping at boot (not running with a halted pipeline).

### Diagnosis - find the consumed-but-unrecorded nonce

The divergence direction is the same as the known cause: a nonce was
**consumed on-chain** (the on-chain root advanced) but is **missing
`Completed` in the DB**. The boot reconcile already tried and failed to resolve
it, so identify the nonce by hand.

1. Read `tree_index` from the log. The current tree window is
   `tree_index * MAX_TREE_LEAVES ..< (tree_index + 1) * MAX_TREE_LEAVES`
   - the same `[min, max)` window `validate_smt_root` rebuilds from
   `get_completed_withdrawal_nonces`. Only nonces in this window matter.
2. Pull the withdrawal rows in that window that are NOT `completed`:

   ```sql
   SELECT id, withdrawal_nonce, status, counterpart_signature, updated_at
     FROM transactions
    WHERE transaction_type = 'withdrawal'
      AND withdrawal_nonce >= :min_nonce
      AND withdrawal_nonce <  :max_nonce
      AND status <> 'completed'
    ORDER BY withdrawal_nonce ASC;
   ```

   A row in `manual_review` whose alert `error_message` was
   `no broadcast signatures recorded; cannot verify release landed`
   (recovery-worker quarantine) is the prime suspect.
3. For each candidate, run
   [`_verify_onchain_release.md`](_verify_onchain_release.md). Exactly
   one verdict resolves the mismatch: a `LANDED <sig>` whose
   `withdrawal_nonce` falls in the tree window is the consumed nonce the
   DB is missing.
   - If verification is `AMBIGUOUS`, **stop** and
     [escalate](_escalation.md) (Tier 2). Do not mark anything
     `Completed` on a guess - that would re-credit the user's view
     incorrectly and mask a real divergence.

### Resolution - record the landed nonce, then restart

Once on-chain verification proves a specific nonce landed, mark that row
`Completed` with the observed signature. This re-inserts the missing nonce into
the set `validate_smt_root` rebuilds from, so the local root re-matches the
on-chain root on the next boot.

```sql
UPDATE transactions
   SET status = 'completed',
       counterpart_signature = :sig,
       updated_at = NOW()
 WHERE id = :transaction_id;
```

Then restart the withdraw operator. On boot, the pre-flight rebuilds the SMT -
now including the recorded nonce - the root verification passes
(`SMT root verification passed`), and the pipeline starts.

This `UPDATE` is bookkeeping only: it does not move funds. The release already
landed on-chain (that is what you verified); this statement only makes the
operator's record agree with the chain so the pipeline can resume. Never run it
without a `LANDED` verdict.

> **No collateral re-arm needed.** This path never marks withdrawals `failed`
> (the SOLA2-21 fix routes SMT-init errors to leave the row `Processing`, never
> `send_fatal_error`). There is no halt-window casualty list to re-arm - the only
> row to act on is the verified-landed nonce above (and any single in-flight row
> the reconcile quarantined to `manual_review`, handled by
> [`withdrawal_manual_review.md`](withdrawal_manual_review.md)).

### Escalation

[`_escalation.md`](_escalation.md). Escalate (Tier 2) if on-chain
verification is `AMBIGUOUS`, if no candidate row's nonce matches the
advanced on-chain root, or if the mismatch persists after recording the
verified-landed nonce and restarting.

### Post-incident artifacts (required)

- Tree index and tree-window nonce range.
- The landed nonce, its `transaction_id`, and the verified signature.
- On-chain verdict and the RPC endpoint used.
- Confirmation that `SMT root verification passed` appeared on the
  post-fix restart.
