# Runbook - Withdrawal Pipeline Halt

This runbook covers the **withdrawal bitmap boot pre-flight**. On startup a
withdraw operator first reconciles any in-flight releases, then diffs the
on-chain withdrawal bitmap against the withdrawals the database records as
`completed` **before** spawning the pipeline.

The diff is **directional**, and the two directions mean opposite things:

| Direction | Meaning | Operator behaviour |
|---|---|---|
| **Chain ahead** - bit set, no `completed` row | Either a release landed and only its status write was lost, or the indexer DB was restored to before the release. | Completes the row from a **landed journaled release signature** and starts. Any other chain-ahead nonce **refuses to start**: see [Unexplained consumed nonce on startup](#unexplained-consumed-nonce-on-startup). |
| **DB ahead** - `completed` row, bit clear | The database believes in a release the chain never made. Every later decision would rest on a false history. | **Refuses to start.** |

Both directions can halt. The operator also refuses to start when the diff
cannot be run at all, because nothing runs it again later; see
[Bitmap check could not run on startup](#bitmap-check-could-not-run-on-startup),
and when the channel was restored behind the indexer DB; see
[Channel fence refused](#channel-fence-refused).

These halts have **no dedicated "pipeline halted" alert**. A refuse-to-start
surfaces as the operator process exiting at boot (a crash-loop under the
supervisor) with `Withdraw boot pre-flight failed, refusing to start` in the
error logs. A divergence also logs `Withdrawal bitmap divergence` and increments
`private_channel_operator_transaction_errors_total{error_reason="bitmap_divergence"}`. No withdrawal is ever marked `failed` by this path. Recognize it by
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
   `completed` withdrawals whose nonce falls in that generation's window or any
   later one. Later generations count because the chain cannot release a nonce
   before it rotates into that generation.
4. **Repairs chain-ahead nonces it can prove.** For each nonce whose bit is set
   with no `completed` row, the operator loads that withdrawal's stored broadcast
   signatures and classifies them on-chain. A landed signature marks the row
   `completed` against it. Anything else (no row, no signature, a signature that
   is not proven landed, a row already terminal) writes nothing and refuses boot
   once the diff is done; see
   [Unexplained consumed nonce on startup](#unexplained-consumed-nonce-on-startup).
   A nonce whose row was already reminted still halts as a double payout.
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
generation, which survived the confirmatory re-read. Chain-ahead nonces have
their own section below.

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

If the instance's bitmap account does not exist, the operator diffs against an
empty generation 0, so every `completed` withdrawal, in any generation, shows up in
`db_only`. A log line `Withdrawal bitmap does not exist` before the divergence
almost always means `escrow_instance_id` points at the wrong instance, or `rpc_url`
at the wrong cluster. Check both first, before touching any row.

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
> act on are the named `db_only` nonces above.

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

---

## Bitmap check could not run on startup

### Symptom

- The withdraw operator exits at boot and the supervisor restarts it in a loop.
- The error log reads `Withdraw boot pre-flight failed, refusing to start: Program error:
  Withdrawal bitmap unavailable: ...`, or a `Storage error:` or `Account error:` instead.
- No row is marked `failed` by this check. No `bitmap_divergence` increment.

### Why the operator refuses

The diff is the only check that a `completed` row matches a set bit, and nothing
runs it after boot. Once the bitmap rotates, the old generation's bits are gone,
so starting without the diff could let a false `completed` row go unnoticed for
good. Refusing is the safe default; the supervisor's restart is the retry.

A bitmap account that does not exist is not this case. It is read as an empty
generation 0 and diffed normally, so a fresh deployment with an empty database
still starts.

### Resolution

1. Read the error after `refusing to start:`.
   - `Withdrawal bitmap unavailable` with an RPC error: the Solana `rpc_url` did not
     answer at a finalized slot. Restore the endpoint or point the operator at a
     healthy one.
   - A storage error: the database read of `completed` withdrawals failed. Restore
     the database.
   - `Account error: Failed to deserialize account data for <bitmap>`: the account at the bitmap
     address is not a withdrawal bitmap. Check `escrow_instance_id` and the program
     ID, then [escalate](_escalation.md) (Tier 2) if both are right.
2. Do nothing else. The next restart reruns the check and the operator starts on
   its own once it passes (`Withdrawal bitmap verification passed`).

If the error is `The Withdraw sender lock was lost during the boot pre-flight`,
the sender lock was lost during the pre-flight. Treat it as a sender lock
problem, not a bitmap problem.

---

## Unexplained consumed nonce on startup

### Symptom

- The withdraw operator exits at boot and the supervisor restarts it in a loop.
- The error log reads `Withdrawal nonces [<n>, ...] are consumed on-chain but no row's
  landed release signature explains them; refusing to start`, after one
  `Consumed nonce could not be explained at boot` line per nonce.
- `private_channel_bitmap_unexplained_nonce_total` increases.
- Or: `The database's highest withdrawal nonce is in generation <g> but the chain is on
  generation <h>`. Same cause, for a bitmap that has rotated since the DB's target.
- No row is written by this check.

### After upgrading to this check

Before this version, a chain-ahead nonce that could not be repaired was moved to
`manual_review` and the operator started anyway. Those rows now refuse boot. Before
upgrading, list the `manual_review` withdrawals, check which of their nonces are set in
the bitmap (see `withdrawal_manual_review.md`), and resolve those with the procedure below:

```sql
SELECT id, withdrawal_nonce, status, updated_at FROM transactions
 WHERE transaction_type = 'withdrawal' AND status = 'manual_review';
```

### Why the operator refuses

A `ReleaseFunds` names a nonce, an amount and a recipient, but not the burn it paid. After
an indexer DB restore to before some releases, the withdraw indexer (`indexer-private-channel`)
re-indexes those burns and the nonce sequence numbers them again, usually with different nonces than before (the
sequence has gaps). A set bit then sits next to a `pending` row that may or may not be the
burn it paid, and two burns of the same amount to the same recipient cannot be told apart.
Serving any withdrawal could pay one a second time, so nothing is served until every
consumed nonce is tied to a row by proof.

The generation rule covers what the bitmap cannot show: once the bitmap rotates, the old
generation's bits are gone, and a DB still numbering in that generation would refund burns
that were already paid.

### Resolution

**Preferred: restore the indexer DB to a later target.** The cause is almost always an
indexer restore to a point before releases that the chain has. If a backup or WAL target
exists that is later than the last release but still at least 60 seconds earlier than the
channel primary's restore target (or any time, if the primary was not restored), restore
`postgres-indexer` to it ([`../PITR.md`](../PITR.md)) and restart. The operator then finds
each release's row with its journal and starts.

**Transient: a status the RPC could not answer.** If the row named by the nonce is
`processing` with a stored release signature, the operator may only have failed to read
that signature's status. Check the Solana RPC and restart; the next boot re-reads it. If it
keeps refusing, run [`_verify_onchain_release.md`](_verify_onchain_release.md) for that
row. On `LANDED <sig>` complete the row by hand:

```sql
UPDATE transactions
   SET status = 'completed', counterpart_signature = :landed_signature,
       processed_at = NOW(), updated_at = NOW()
 WHERE id = :transaction_id AND withdrawal_nonce = :nonce;
```

**Otherwise: tie each release to its burn by hand.** Do this only with Tier 2 on the call.

1. Start both indexers (`indexer-solana` and `indexer-private-channel`) and let them catch
   up: the first records every release the chain made in `observed_releases`, the second
   re-creates the withdrawal rows. Keep both operators stopped.
2. For each named nonce:

   ```sql
   SELECT withdrawal_nonce, signature, slot, amount
     FROM observed_releases WHERE withdrawal_nonce = :nonce;
   ```

   Run [`_verify_onchain_release.md`](_verify_onchain_release.md) on that signature and note
   the recipient token account and amount from the transaction. Anything but `LANDED`:
   stop and [escalate](_escalation.md).
3. Find the burn it paid: the withdrawal row with that recipient and amount whose burn slot
   is before the release slot and that no other release has been tied to. If no row fits,
   or more than one fits, **stop and escalate**: guessing pays someone twice.
4. Give the row the released nonce and complete it, in one transaction. Another row may
   hold that nonce after the renumbering, so move it to a fresh nonce first:

   ```sql
   BEGIN;
   -- Past every nonce the chain or the DB has used.
   SELECT setval('withdrawal_nonce_seq',
                 GREATEST(:highest_consumed_nonce,
                          (SELECT MAX(withdrawal_nonce) FROM transactions
                            WHERE transaction_type = 'withdrawal')));
   UPDATE transactions SET withdrawal_nonce = nextval('withdrawal_nonce_seq'), updated_at = NOW()
    WHERE transaction_type = 'withdrawal' AND withdrawal_nonce = :nonce
      AND id <> :transaction_id;
   UPDATE transactions
      SET withdrawal_nonce = :nonce, status = 'completed',
          counterpart_signature = :release_signature,
          processed_at = NOW(), updated_at = NOW()
    WHERE id = :transaction_id;
   COMMIT;
   ```

5. Restart the withdraw operator. It starts once no consumed nonce is left unexplained, or
   names the ones still left.

For the generation rule, the same applies: restore a later indexer target if one exists;
otherwise escalate (Tier 2). Do not move the sequence forward to silence it, because the
burns of the rotated generation may already be paid.

These `UPDATE`s are bookkeeping only. They record releases the chain already made.

### Post-incident artifacts (required)

- The named nonces, and for each the release signature, recipient, amount and the
  `transaction_id` it was tied to, or the indexer restore target used instead.
- The `_verify_onchain_release.md` verdict for each.

---

## Channel fence refused

### Symptom

- The withdraw indexer or an operator exits at boot, or stops while running, with
  `channel fence check failed: ...` (for example `channel block <slot> is <hash>, the
  indexer recorded <hash>` or `channel has no block at fence slot <slot>`), or the withdraw
  indexer logs `Block <slot> does not extend the channel history the indexer read`.
- `private_channel_channel_fence_mismatch_total` increases.

### Why it refuses

The withdraw indexer records the hash of the channel block its checkpoint was built on. A
channel primary restored to an earlier point re-produces the same slots with new hashes, so
the recorded block is gone. Left running, the withdraw indexer would skip burns above the
restored tip, the escrow operator would keep crediting a channel that lost earlier credits,
and the withdraw operator would release pending rows whose burn was rolled back, or a
second time for a burn the user repeats on the restored channel.

### Resolution

1. Stop everything as in [`../PITR.md`](../PITR.md) Step 1.
2. **Before restoring anything**, record what moved after the primary's restore target,
   from the current indexer DB (or a dump of it). The next step replaces this DB.

   ```sql
   SELECT id, transaction_type, status, signature, instruction_index, inner_index, slot,
          initiator, withdrawal_nonce, counterpart_signature, amount, recipient, mint,
          processed_at
     FROM transactions
    WHERE processed_at >= :primary_restore_target
    ORDER BY processed_at;
   ```

   Keep the output with the incident. Funds that moved in this window are not repaired by
   any service:
   - A deposit minted in the window is lost on the restored channel and is minted again
     after the indexer restore. That is the correct single credit; nothing to do.
   - A withdrawal released in the window whose burn `slot` is above the restored channel's
     tip paid, on Solana, a burn that no longer exists on the channel, so the user holds
     both. [Escalate](_escalation.md) (Tier 2) with those rows. A release whose burn is at or
     below that tip was legitimate; its row is re-created by the indexers and tied to the
     release with the [unexplained consumed nonce](#unexplained-consumed-nonce-on-startup)
     procedure.
3. Restore `postgres-indexer` to a target at least 60 seconds earlier than the channel
   primary's restore target, then restart in the PITR order. Do not clear
   `fence_slot`/`fence_blockhash` by hand.
4. Each release from step 2 whose burn is above the restored tip consumed a nonce on Solana, but the restored indexer DB has no
   row for it and never will (its burn is gone from the channel), so the withdraw operator
   refuses with an [unexplained consumed nonce](#unexplained-consumed-nonce-on-startup).
   With Tier 2, once the escalation in step 2 has taken over the money question, record
   each of those releases so the gate can tie its nonce to a row. Use the values from
   step 2 and the release's `_verify_onchain_release.md` verdict (`LANDED` only):

   ```sql
   -- One per release from step 2. The nonce is given explicitly, so the trigger keeps it.
   INSERT INTO transactions
       (signature, instruction_index, inner_index, slot, initiator, recipient, mint, amount,
        transaction_type, withdrawal_nonce, status, counterpart_signature, processed_at)
   VALUES
       (:signature, :instruction_index, :inner_index, :slot, :initiator, :recipient, :mint,
        :amount, 'withdrawal', :withdrawal_nonce, 'completed', :counterpart_signature, NOW());
   ```

   Then make sure the sequence is past every nonce used, as in step 4 of the unexplained
   nonce procedure, and restart the withdraw operator.

If the log reads `channel fence could not be checked`, the channel RPC did not answer, its
tip stayed below the fence slot for 10 minutes, or the fence block was pruned. A tip far
below the fence with a healthy RPC is a primary restore that rolled back more than 10
minutes: treat it as above. For a pruned fence block, follow
[`../PITR.md`](../PITR.md#what-a-refusal-after-a-restore-means). Otherwise fix the RPC
and restart.
