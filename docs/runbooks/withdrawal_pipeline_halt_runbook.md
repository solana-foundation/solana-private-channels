# Runbook - Withdrawal Pipeline Halt

This runbook covers the **SMT-root-mismatch startup halt**. It is a
deliberate fail-stop: on boot, the first release dispatch finds the local
SMT root != the on-chain root, and the operator refuses to start the
withdrawal pipeline rather than consume nonces against a tree it cannot
reason about.

This halt has **no dedicated "pipeline halted" alert**. It surfaces as
repeated per-row `failed` webhooks whose `error_message` carries `SMT root
mismatch` (each withdrawal dispatched after boot is marked `Failed` by
`send_fatal_error`), plus the operator error logs. You recognize it by
that pattern, not a single halt event, and it is not routed by the
dispatch table in [`README.md`](README.md).

As with every runbook here, the recovery `UPDATE` statements are
**bookkeeping, not fund movement** - see
[`README.md`](README.md) § "Recovery SQL is bookkeeping; fund restoration
is human-in-the-loop".

---

## SMT root mismatch on startup

The terminal state of the best-effort release-signature persistence
tradeoff at `indexer/src/operator/sender/transaction.rs` (the
`insert_release_signature` site). When a release **lands on-chain** (the
nonce is consumed and the Instance's `withdrawal_transactions_root`
advances) but the operator crashes before writing `Completed` - and the
best-effort signature insert had also failed - recovery correctly
quarantines the row (`no broadcast signatures recorded; cannot verify
release landed`), but the DB now has no record of that consumed nonce.
The next boot rebuilds the local SMT without it and refuses to start the
withdrawal pipeline.

### Symptom

- New withdrawals stop reaching `completed` - the release pipeline is
  stalled. (`completed` itself fires no webhook, so the visible signal is
  the failures below, not a missing success alert.)
- Each withdrawal dispatched after boot is marked `Failed` and fires a
  per-row `failed` webhook whose `error_message` contains `SMT root
  mismatch` (`send_fatal_error` on the lazy `initialize_smt_state`). There
  is **no dedicated halt-level alert** - recognize the halt by the
  repeated `failed` + `SMT root mismatch` pattern, not a single event.
- These rows did **not** actually fail; they are collateral and must be
  re-armed once the mismatch is resolved (see Resolution).

### Detection

`initialize_smt_state` (`indexer/src/operator/sender/state.rs`) compares
the local SMT root against the on-chain `withdrawal_transactions_root`
and, on mismatch, emits these `error!` markers before returning
`Err(SmtRootMismatch)` (state.rs:122-137):

```
SMT root mismatch detected! Database out of sync with on-chain state.
  Instance PDA: <pda>
  Tree Index: <n>
  Nonces from DB: [...]
  Local root:    [...]
  On-chain root: [...]
```

```
This typically means:
  1. A withdrawal was successfully processed on-chain
  2. But the operator crashed before updating the database
  3. The database is now missing transaction records
```

Grep the operator logs for `SMT root mismatch detected` to confirm.

### Diagnosis - find the consumed-but-unrecorded nonce

The divergence direction is always the same here: a nonce was
**consumed on-chain** (the on-chain root advanced) but is **missing
`Completed` in the DB** (the DB is behind the chain). You must identify
which nonce landed.

1. Read `Tree Index` from the log. The current tree window is
   `tree_index * MAX_TREE_LEAVES ..< (tree_index + 1) * MAX_TREE_LEAVES`
   - the same window `initialize_smt_state` rebuilds from
   `get_completed_withdrawal_nonces` (state.rs:94-102). Only nonces in
   this window matter.
2. Pull the withdrawal rows in that window that are NOT `completed`
   (the candidates whose release may have landed without a `Completed`
   write). The recovery quarantine reason is the strongest hint:

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
`Completed` with the observed signature. This re-inserts the missing
nonce into the set `initialize_smt_state` rebuilds from, so the local
root re-matches the on-chain root on the next boot.

```sql
UPDATE transactions
   SET status = 'completed',
       counterpart_signature = :sig,
       updated_at = NOW()
 WHERE id = :transaction_id;
```

Then restart the withdraw operator. On boot, `initialize_smt_state`
rebuilds the SMT - now including the recorded nonce - and the root
verification passes (`SMT root verification passed`).

This `UPDATE` is bookkeeping only: it does not move funds. The release
already landed on-chain (that is what you verified); this statement only
makes the operator's record agree with the chain so the pipeline can
resume. Never run it without a `LANDED` verdict.

**Re-arm the collateral rows.** Withdrawals dispatched during the halt
were marked `Failed` by `send_fatal_error` but did not actually fail
on-chain. Their failure reason is only in the alert webhook payload -
`transactions` has no `error_message` column - so identify them by the
halt window instead: `failed` withdrawals whose `processed_at` falls
between the halting boot (the first `SMT root mismatch detected` log line)
and this fix. Review the list before acting:

```sql
SELECT id, withdrawal_nonce, processed_at
  FROM transactions
 WHERE transaction_type = 'withdrawal'
   AND status = 'failed'
   AND processed_at >= :halt_start_ts
 ORDER BY processed_at ASC;
```

Confirm each is a halt casualty (not a genuine on-chain failure), then
re-arm the reviewed ids:

```sql
UPDATE transactions SET status = 'pending', recovery_requeue_attempts = 0, updated_at = NOW()
 WHERE id = ANY(:reviewed_ids);
```

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
