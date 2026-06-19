# Runbook - Withdrawal `ManualReview`

Triggered by webhook payload `status=manual_review` for a withdrawal row.

## Symptom

- Webhook with `status=manual_review` for one or more transaction IDs.
- ERROR-level log line: `Transaction <id> ManualReview`.
- May or may not be paired with a pipeline halt: if the trigger error
  was a build-side deterministic failure (Path A.halting below), the
  operator's `halt_withdrawal_pipeline` ran and bulk-flipped every
  active withdrawal to `manual_review`. Multiple webhooks for the same
  timestamp burst confirm a halt occurred.

## Triage - dispatch by `error_message`

`error_message` is on the **alert webhook payload**, not in the
`transactions` table. Read it from the alert that paged you, the operator
ERROR log line `Transaction <id> error: <message>`, or the upstream alert
store.

Pull the row's DB-side state:

```sql
SELECT id, withdrawal_nonce, status, counterpart_signature,
       remint_signatures, updated_at
  FROM transactions
 WHERE id = :transaction_id;
```

Match the webhook's `error_message` against the table below to pick the
recovery path. Substring match - the messages are concatenations and may
have prefixes.

| `error_message` contains | Path | Halts pipeline? | Source |
|---|---|---|---|
| `invalid_pubkey`, `invalid_builder`, `program_error` | A.halting | yes | `processor.rs` quarantine |
| `withdrawal pipeline halted after poison-pill` | A.halting (collateral row) | yes | halt sweep, channel drain |
| (empty `error_message`, status flipped without a quarantine update) | A.halting (collateral row) | yes | `quarantine_all_active_withdrawals` |
| `mint paused:` | A.non-halting | no | pre-flight |
| `insufficient escrow balance:` | A.non-halting | no | pre-flight |
| `remint failed:` | B - stranded after remint failure | no | `sender/remint.rs` |
| `finality check failed after` | C - ambiguous (RPC unreachable) | no | `sender/remint.rs` |
| `failed to persist pending remint:` | C - ambiguous (DB lost the sig) | no | `sender/transaction.rs` |
| `no signatures to verify` | C - ambiguous (RPC may have broadcast) | no | `sender/transaction.rs` |
| `withdrawal row missing nonce` | F - corrupt withdrawal row | no | recovery worker quarantine |
| `no broadcast signatures recorded; cannot verify release landed` | C - ambiguous (recovery cannot prove outcome) | no | recovery worker quarantine |
| `could not verify release landed (` | C - ambiguous (RPC unreachable during recovery) | no | recovery worker quarantine |
| `recovery requeues without progress` | G - requeue cap exhausted (release never landed) | no | recovery worker quarantine |

## Path A.halting - build error that halted the pipeline

The trigger row's data is bad in a way that would corrupt the SMT (NULL
nonce, malformed pubkey, builder rejection). The processor quarantined the
trigger and ran `halt_withdrawal_pipeline`, which drained the fetcher
channel and bulk-flipped every `pending`/`processing` withdrawal to
`manual_review`. Recovery has to handle both the trigger and the
collateral.

1. **Verify on-chain.** Run [`_verify_onchain_release.md`](_verify_onchain_release.md)
   for the trigger row. Expected verdict: `NOT_LANDED` (build failed before
   any RPC call). If `LANDED` → switch to Path C reconciliation. If
   `AMBIGUOUS` → [escalate](_escalation.md) (Tier 2).
2. **Identify the trigger row** (oldest `manual_review` by `updated_at`):
   ```sql
   SELECT id, withdrawal_nonce, updated_at
     FROM transactions
    WHERE transaction_type = 'withdrawal'
      AND status = 'manual_review'
    ORDER BY updated_at ASC
    LIMIT 20;
   ```
   The first row is the poison-pill (paired with the original webhook).
   Subsequent rows are collateral from the halt sweep - those came in as
   webhooks too, but with no quarantine `error_message` (the sweep doesn't
   send a `TransactionStatusUpdate` per row; status is flipped in bulk via
   `quarantine_all_active_withdrawals`).
3. **Decide the trigger row's fate.**
   - Bad data, unrecoverable (e.g. malformed mint pubkey, NULL nonce):
     ```sql
     UPDATE transactions SET status = 'failed', updated_at = NOW()
      WHERE id = :poison_id;
     ```
     `failed` is terminal; webhook already fired. The user must be refunded
     out-of-band - capture in the incident record.
   - Transient error conservatively quarantined as deterministic
     (rare; see "Conservative classification" below): fix the row data
     and re-arm to `pending`.
4. **Re-arm sweep rows:**
   ```sql
   UPDATE transactions
      SET status = 'pending', recovery_requeue_attempts = 0, updated_at = NOW()
    WHERE transaction_type = 'withdrawal'
      AND status = 'manual_review'
      AND id <> :poison_id;
   ```
   The `transactions` table does not store `error_message` - it lives in the
   alert payload only. Distinguishing trigger from collateral happens in
   triage (Step 2: oldest `updated_at` is the trigger), not in the re-arm
   query. If you have *multiple* unresolved trigger rows from different
   incidents, recover them one at a time and exclude each via `id <> :id`.
5. **Restart the withdraw operator** (Docker, from the repo root: `docker compose
   restart operator-private-channel`; or by container: `docker restart
   private-channel-operator-private-channel`). The fetcher picks up `pending` rows and
   processing resumes.
6. **Confirm recovery** by watching for new `Completed` webhooks for
   the re-armed rows.

## Path A.non-halting - pre-flight bail (paused mint, escrow drain)

The pre-flight check (`processor.rs::check_withdrawal_preflights`) bailed
on a row whose mint is paused or whose target ATA does not have enough
on-chain balance. The processor quarantined **only this row** and
**continued the loop** - there is no halt and no collateral. Other
withdrawals are unaffected.

1. **Verify on-chain.** Run [`_verify_onchain_release.md`](_verify_onchain_release.md)
   for this row. Expected: `NOT_LANDED` (pre-flight aborted before send).
   If `LANDED` → switch to Path C reconciliation. If `AMBIGUOUS` →
   [escalate](_escalation.md) (Tier 2).
2. **Confirm the pre-flight condition still holds.**
   - `mint paused:` - check the mint's PausableConfig extension on Solana
     (`solana account <mint>` and decode the Token-2022 extension). If
     still paused, the row cannot be retried until the mint is unpaused;
     hold or refund.
   - `insufficient escrow balance:` - check the escrow ATA balance vs.
     the row's amount. A permanent delegate may have drained the ATA. If
     the deficit is permanent, refund out-of-band; do not re-arm.
3. **If the condition has cleared, re-arm just this row:**
   ```sql
   UPDATE transactions
      SET status = 'pending', recovery_requeue_attempts = 0, updated_at = NOW()
    WHERE id = :transaction_id;
   ```
4. **No operator restart is needed.** The processor did not halt; the
   fetcher will pick up the re-armed row on its next tick.
5. **If the condition is permanent**, mark `failed` and capture the
   refund obligation in the incident record:
   ```sql
   UPDATE transactions SET status = 'failed', updated_at = NOW()
    WHERE id = :transaction_id;
   ```

### Conservative classification - trigger is safe to re-arm

Cross-cuts both A.halting and A.non-halting.

If `error_message` describes a transient condition (RPC error, DB error
surfaced as an `OperatorError::Program`), the classifier in
`indexer/src/operator/processor.rs::classify_processor_error` quarantined
on the side of caution rather than retrying. This is the intended
behavior: misclassifying a deterministic error as transient could put
the operator into a tight retry loop that consumes nonces against a
broken row and corrupts the SMT. The asymmetric cost favors a noisy
quarantine over a silent retry.

What this means for recovery: the trigger row is safe to retry, not
just the collateral. Re-arm the affected row(s) including the trigger;
for the halting variant, re-arm via Step 4.
[Escalate](_escalation.md) (Tier 3) so the taxonomy can be extended to
classify this error variant explicitly. Do not patch in-place during
incident response.

## Path B - stranded after remint failure

The original withdrawal failed AND the remint also failed. Both the on-chain
release and the channel-side remint may have left partial state.
`error_message` looks like: `<original_error> | remint failed: <remint_error>`.

1. **Verify on-chain release.** Run
   [`_verify_onchain_release.md`](_verify_onchain_release.md).
   - If `LANDED <sig>` → user already received funds on Solana. The remint
     would have been a duplicate; its failure was the right outcome. Mark
     completed:
     ```sql
     UPDATE transactions
        SET status = 'completed',
            counterpart_signature = :sig,
            updated_at = NOW()
      WHERE id = :transaction_id;
     ```
     Done. No further action.
   - If `NOT_LANDED` → continue.
   - If `AMBIGUOUS` → [escalate](_escalation.md) (Tier 2).
2. **Verify the remint signature on the channel side.** The remint targets
   the private channel side, not Solana mainnet. Check the private channel read node for the
   user's ATA balance before/after `processed_at`. If the balance moved, the
   remint actually succeeded and the failure was a confirmation glitch - mark
   `failed_reminted` and capture the remint signature manually.
3. **If both confirmed not-landed,** the user's funds are stuck:
   - Their private channel side tokens were burned for the withdrawal.
   - Solana-side release did not happen.
   - Remint to restore burned tokens did not happen.
   - **[Escalate](_escalation.md) (Tier 1).** Out-of-band restoration
     (manual mint or manual release) is the only path. Do not flip the
     row's status until restoration is reconciled - the alert state
     preserves the trail.

## Path C - ambiguous on-chain state

The withdrawal *may* have landed; the operator could not verify before
committing the row to manual review. Sub-triggers below; same recovery.

> **Recovery now verifies on-chain before demoting.** The crash-recovery
> worker persists every broadcast release signature to
> `pending_release_signatures` at send time and, for a stale `Processing`
> withdrawal, classifies those signatures on-chain (the same finality check
> the remint flow uses) *before* deciding. A finalized-success signature is
> promoted to `Completed` (never re-sent); a dead/expired signature is
> demoted to `Pending`; a still-live signature is left in `Processing` for
> the next sweep. It only quarantines when it cannot prove the outcome:
> either **no broadcast signatures were recorded** (`no broadcast signatures
> recorded; cannot verify release landed`) or **the RPC could not classify
> them** (`could not verify release landed (...)`, with the signature list
> appended). Both land here in Path C — verify on-chain and act on the
> verdict; never blindly re-arm a row whose release may already be on-chain.

1. **Verify on-chain.** Run
   [`_verify_onchain_release.md`](_verify_onchain_release.md). This is
   the entire decision.
2. **If `LANDED <sig>`:** withdrawal succeeded; do NOT remint. Mark completed
   with the observed signature:
   ```sql
   UPDATE transactions
      SET status = 'completed',
          counterpart_signature = :sig,
          updated_at = NOW()
    WHERE id = :transaction_id;
   ```
3. **If `NOT_LANDED`:** withdrawal did not happen. The user's private channel tokens
   may or may not be burned (depends on the trigger sub-site). Confirm burn
   state via channel read node before deciding:
   - Burned, no release → re-arm to `pending` and restart operator. The
     withdrawal will be re-attempted; the channel-side burn is idempotent.
   - Not burned → no user impact; close the alert and re-arm.
4. **If `AMBIGUOUS`:** stop. [Escalate](_escalation.md) (Tier 2). Wait
   for RPC visibility to recover. Do not act.

> **If the quarantined release actually landed** (verdict `LANDED`, but
> the row was quarantined with `no broadcast signatures recorded; cannot
> verify release landed` and never written `Completed`), the consumed
> nonce is missing from the DB. The boot pre-flight normally reconciles
> this from the durable release signature; only if it cannot will the
> operator refuse to start. See
> [`withdrawal_pipeline_halt_runbook.md`](withdrawal_pipeline_halt_runbook.md).
> Marking the row `Completed` per Step 2 above re-records the nonce and
> resolves any such refuse-to-start.

## Path F - corrupt withdrawal row (missing nonce)

`error_message`: `withdrawal row missing nonce`. The recovery worker
found a stale `Processing` withdrawal whose `withdrawal_nonce` is
`NULL`. The indexer always populates this column for withdrawal rows,
so a NULL on this row indicates either a manual DB edit, a partial
schema migration, or a defect in the indexer write path. **Do not
re-arm.** The processor would reject the row identically on every tick.

### Step 1 - confirm the corruption

```sql
SELECT id, signature, withdrawal_nonce, mint, amount, recipient,
       created_at, updated_at
  FROM transactions
 WHERE id = :transaction_id;
```

If `withdrawal_nonce IS NOT NULL`, the row was repaired between
quarantine and triage. Re-arm to `pending`:

```sql
UPDATE transactions SET status = 'pending', recovery_requeue_attempts = 0, updated_at = NOW()
 WHERE id = :transaction_id;
```

Otherwise proceed.

### Step 2 - check whether the burn landed on the PrivateChannel side

`signature` is the originating PrivateChannel burn signature.

```bash
solana confirm -v <signature> --url <private-channel-rpc>
```

### Step 3 - branch on burn verdict

#### Burn landed

The user already burned. Escalate (Tier 1) for refund coordination —
either a manual `release_funds` to the depositor or a manual remint of
the burned tokens. Then mark the row terminal:

```sql
UPDATE transactions SET status = 'failed', updated_at = NOW()
 WHERE id = :transaction_id;
```

#### Burn did not land

The indexer wrote a withdrawal row for an instruction that did not
finalize. [Escalate](_escalation.md) (Tier 3) — the indexer
write-or-classify path has a defect. Capture the row, then delete:

```sql
DELETE FROM transactions WHERE id = :transaction_id;
```

## Path G - requeue cap exhausted (release never landed)

`error_message` contains `recovery requeues without progress`. Each
recovery pass that finds the row's release signatures all
finalized-failed or expired (`SigFinality::Dead`) demotes the stuck
`processing` row back to `pending` for a fresh send. After
`MAX_RECOVERY_REQUEUE_ATTEMPTS` (3) such requeues with no release ever
landing, recovery quarantines instead of looping forever. So the
release-funds transaction was rebroadcast 3 times and every attempt died
on-chain or expired - none finalized. The row data is valid (distinct
from Path A/F) and the signatures are conclusively Dead each cycle
(distinct from Path C's ambiguity).

### Step 1 - verify on-chain

Run [`_verify_onchain_release.md`](_verify_onchain_release.md). Expected
verdict: `NOT_LANDED` (every attempt died). If `LANDED` -> a landed
signature was misclassified as Dead; switch to Path C reconciliation and
[escalate](_escalation.md) (Tier 2) - the classifier has a defect.

### Step 2 - find why every release died

Pull the recorded release signatures (keyed by `transaction_id`) and read
each on-chain failure:

```sql
SELECT signature, last_valid_block_height, created_at
  FROM pending_release_signatures
 WHERE transaction_id = :transaction_id
 ORDER BY created_at;
```

For each, `solana confirm -v <signature> --url <solana-rpc-url>` to read
the `InstructionError`. A repeating deterministic error (escrow
underfunded, SMT/proof rejection, account state) means re-sending will not
help - [escalate](_escalation.md) (Tier 2/3) to engineering. A transient
cause (blockhash expiry under load, RPC outage during the send window)
may already have cleared.

### Step 3 - resolve, then re-arm

Fix the root cause first. Then re-arm to `pending` **and reset the requeue
counter** - re-arming without the reset re-quarantines the row on its next
stall, since the counter is already at the cap:

```sql
UPDATE transactions
   SET status = 'pending', recovery_requeue_attempts = 0, updated_at = NOW()
 WHERE id = :transaction_id;
```

If the release can never land (unrecoverable on-chain rejection), mark the
row terminal and [escalate](_escalation.md) (Tier 1) for refund
coordination:

```sql
UPDATE transactions SET status = 'failed', updated_at = NOW()
 WHERE id = :transaction_id;
```

## Post-incident artifacts (required)

Capture in the incident record:
- Transaction id, withdrawal nonce, `processed_at`.
- Full `error_message` content.
- Trigger site (which row of the dispatch table).
- On-chain verdict (`LANDED <sig>` / `NOT_LANDED` / `AMBIGUOUS`).
- Recovery action taken (SQL run, sig used, escalation path).
- RPC endpoint used for verification.

These feed the audit trail for any user-facing reconciliation.

