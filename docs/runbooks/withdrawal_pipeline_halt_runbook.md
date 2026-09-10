# Runbook - Withdrawal Pipeline Halt

This runbook covers the **withdrawal bitmap boot pre-flight**. On startup a
withdraw operator first reconciles any in-flight releases, then diffs the
on-chain withdrawal bitmap against the withdrawals the database records as
`completed` **before** spawning the pipeline.

The diff is **directional**, and the two directions mean opposite things:

| Direction | Meaning | Operator behaviour |
|---|---|---|
| **Chain ahead** - bit set, no `completed` row | A release landed and only its bookkeeping was lost. The money already moved correctly. | Names the nonces, resolves each from its broadcast signatures, and **starts anyway**. |
| **DB ahead** - `completed` row, bit clear | The database believes in a release the chain never made. Every later decision would rest on a false history. | **Refuses to start.** |

Only the DB-ahead direction halts. That is the whole subject of this runbook.

This halt has **no dedicated "pipeline halted" alert**. A refuse-to-start
surfaces as the operator process exiting at boot (a crash-loop under the
supervisor) with `Withdrawal bitmap divergence` in the error logs, and a
`private_channel_operator_transaction_errors_total{error_reason="bitmap_divergence"}`
increment. No withdrawal is ever marked `failed` by this path. Recognize it by
the boot-time crash-loop plus the log markers, not a single halt event, and it is
not routed by the dispatch table in [`README.md`](README.md).

As with every runbook here, the recovery `UPDATE` statements are
**bookkeeping, not fund movement** - see
[`README.md`](README.md) § "Recovery SQL is bookkeeping; fund restoration
is human-in-the-loop".

---

## Withdrawal bitmap divergence on startup

### What the operator does automatically

On boot, before any withdrawal is fetched, locked, or processed, the operator:

1. **Reconciles in-flight releases.** Every consumed nonce has a release
   signature persisted **write-ahead** (before broadcast), so a release that
   landed but never reached `completed` is detected by an on-chain finality
   check and promoted to `completed`. A row with no recorded signature, or one
   the RPC cannot classify, is quarantined to `manual_review` (never `failed`).
   Quarantining is not the end of the story: the signatures are copied onto the
   row, so a row quarantined only because the RPC was unreachable is
   re-classified on every recovery tick and at each boot, and promotes itself
   once the release is proven finalized.
2. **Reconciles stalled `pending_remint` rows.** Withdrawals parked in
   `pending_remint` that still carry release signatures are classified the same
   way and promoted to `completed` on proof, so a landed-but-unrecorded nonce
   held there no longer wedges the diff below. Rows carrying no signatures are
   skipped entirely and stay for a human. This runs at boot only, before the
   sender exists: the sender owns those rows and may have a remint in flight,
   so completing one from underneath it would pay the withdrawal and remint the
   burn. The equivalent sweep over `manual_review` has no owner and runs on
   every recovery tick instead. The boot pass is time-bounded
   (`BOOT_RECONCILE_BUDGET`); if it runs out, the diff below still decides
   whether the operator may start.
3. **Diffs the bitmap** for the generation the bitmap is currently on against
   `completed` withdrawals whose nonce falls in that generation's window.
4. **Repairs chain-ahead nonces in place.** For each nonce whose bit is set with
   no `completed` row, the operator loads that withdrawal's stored broadcast
   signatures and classifies them on-chain. A landed signature marks the row
   `completed` against it; anything else escalates that single row to
   `manual_review`. Startup continues either way.
5. **Re-reads the bitmap once before halting on DB-ahead.** The bitmap and the
   database are read at different instants, so a release landing between them
   looks exactly like DB-ahead. A real divergence survives the second read; a
   race does not.

If the diff is clean, the pipeline starts normally
(`Withdrawal bitmap verification passed` in the logs).

Reaching the rest of this runbook therefore means something stronger than it
used to. The self-clearing sweeps above run first, so any row that could have
resolved itself from stored evidence already has. What is left is the db-ahead
direction alone: a `completed` row whose bit is clear in the current
generation, which survived the confirmatory re-read. Chain-ahead nonces never
get here - they are repaired or escalated in place and boot continues.

### Symptom

- The withdraw operator does not stay up: it exits at boot and the supervisor
  restarts it in a loop. New withdrawals never reach `completed`.
- The operator error logs carry `Withdrawal bitmap divergence` at boot.
- **No** withdrawal row is marked `failed`.

### Detection

`validate_bitmap_consistency` emits an `error!` log naming the exact nonces on
each side of the divergence:

```
Withdrawal bitmap divergence: the database claims releases the chain never made.
Refusing to start; reconcile these nonces before restarting.
  instance=<pda> generation=<n>
  db_only=[<nonces>] chain_only=[<nonces>]
```

`db_only` is the halting set: nonces the database records as `completed` whose
bit is clear on-chain. `chain_only` is informational and does not halt on its
own.

Grep the operator logs for `Withdrawal bitmap divergence` to confirm, and check
that the process is crash-looping at boot (not running with a halted pipeline).

### Diagnosis - the nonces are already named

Unlike the root comparison this replaced, the bitmap diff tells you exactly
which nonces disagree. There is no window to reconstruct by hand.

1. Take the `db_only` list straight from the log line.
2. Pull those rows:

   ```sql
   SELECT id, withdrawal_nonce, status, counterpart_signature, updated_at
     FROM transactions
    WHERE transaction_type = 'withdrawal'
      AND withdrawal_nonce = ANY(:db_only_nonces)
    ORDER BY withdrawal_nonce ASC;
   ```

3. For each one, run
   [`_verify_onchain_release.md`](_verify_onchain_release.md) against its
   `counterpart_signature`. There are three possible verdicts:

   - **`NOT LANDED`** - the row was marked `completed` for a release that never
     happened. This is the expected finding: the database is wrong, and the user
     has not been paid. Continue to Resolution.
   - **`LANDED <sig>`** - the release did happen, yet the bit is clear. That can
     only mean the bitmap rotated past this nonce's generation, or the operator
     is pointed at a different instance than the one that served the release.
     **Stop** and [escalate](_escalation.md) (Tier 2); do not clear the row.
   - **`AMBIGUOUS`** - **stop** and [escalate](_escalation.md) (Tier 2).

### Resolution - correct the wrong row, then restart

Only for a `NOT LANDED` verdict. The row claims a payout that never occurred, so
it must go back to a non-terminal state and be escalated for a human to decide
whether to re-attempt the withdrawal.

```sql
UPDATE transactions
   SET status = 'manual_review',
       counterpart_signature = NULL,
       updated_at = NOW()
 WHERE id = :transaction_id;
```

The `transactions` table does not store `error_message` - it lives in the alert
payload only, so record the reason in the incident notes rather than the row.

Then restart the withdraw operator. On boot the diff no longer sees a
`completed` row without a bit, the verification passes
(`Withdrawal bitmap verification passed`), and the pipeline starts.

This `UPDATE` is bookkeeping only: it does not move funds. It records that the
release the database claimed never happened, so the operator's history agrees
with the chain again.

> **No collateral re-arm needed.** This path never marks withdrawals `failed` -
> a read failure while building a transaction leaves the row `processing` for
> the recovery worker rather than calling `send_fatal_error`. The only rows to
> act on are the named `db_only` nonces above, plus any single row the chain-ahead
> repair escalated to `manual_review`, handled by
> [`withdrawal_manual_review.md`](withdrawal_manual_review.md).

### Escalation

[`_escalation.md`](_escalation.md). Escalate (Tier 2) if on-chain verification is
`AMBIGUOUS`, if a `db_only` nonce verifies as `LANDED`, or if the divergence
persists after correcting the named rows and restarting.

### Post-incident artifacts (required)

- Bitmap generation and both nonce lists from the log line.
- Each `db_only` nonce, its `transaction_id`, and its on-chain verdict.
- The RPC endpoint used for verification.
- Confirmation that `Withdrawal bitmap verification passed` appeared on the
  post-fix restart.
