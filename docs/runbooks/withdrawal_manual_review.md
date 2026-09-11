# Runbook - Withdrawal `ManualReview`

Triggered by webhook payload `status=manual_review` for a withdrawal row.

## Symptom

- Webhook with `status=manual_review` for one or more transaction IDs.
- ERROR-level log line: `Transaction <id> ManualReview`.
- May or may not be paired with a pipeline halt: if the trigger error
  was a build-side deterministic failure (Path A.halting below), the
  operator's `halt_withdrawal_pipeline` ran and bulk-flipped every
  active withdrawal at or above the poison row's nonce to
  `manual_review`. Multiple webhooks for the same timestamp burst
  confirm a halt occurred.

## Triage - dispatch by `error_message`

`error_message` is on the **alert webhook payload**, not in the
`transactions` table. Read it from the alert that paged you, the operator
ERROR log line `Transaction <id> error: <message>`, or the upstream alert
store.

Pull the row's DB-side state:

```sql
SELECT id, signature, slot, withdrawal_nonce, status, counterpart_signature,
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
| (empty `error_message`, status flipped without a quarantine update) | A.halting (collateral row) | yes | `quarantine_active_withdrawals` |
| `mint paused:` | A.non-halting | no | pre-flight |
| `insufficient escrow balance:` | A.non-halting | no | pre-flight |
| `unsupported withdrawal mint:` | A.non-halting | no | allowlist gate |
| `withdrawal mint absent on target chain:` | A.non-halting | no | pre-flight |
| `transfer-hook validation account missing for mint:` | A.non-halting | no | hook resolution |
| `transfer-hook accounts exceed the per-transfer cap` | A.non-halting | no | hook resolution |
| `escrow ATA frozen for mint:` | A.non-halting | no | pre-flight |
| `withdrawals blocked for mint:` | A.non-halting | no | allowlist gate |
| `remint failed:` | B - stranded after remint failure | no | `sender/remint.rs` |
| `finality check failed after` | C - ambiguous (RPC unreachable) | no | `sender/remint.rs` |
| `no signatures to verify` | C - ambiguous (RPC may have broadcast) | no | `sender/transaction.rs` |
| `withdrawal row missing nonce` | F - corrupt withdrawal row | no | recovery worker quarantine |
| `released on-chain with no recorded broadcast signature` | C - proven landed, journal empty (Step 2 resolves it) | no | recovery worker quarantine |
| `release verification still uncertain after` | C - ambiguous (proof unavailable past the escalation window) | no | recovery worker quarantine |
| `release signature journal still unreadable after` | C - ambiguous (database unreadable past the escalation window; check Postgres first) | no | recovery worker quarantine |
| `malformed stored release signature` | C - ambiguous (journal corrupt, so a signature was recorded) | no | recovery worker quarantine |
| `no escrow instance configured to verify the release against` | C - ambiguous (operator has no escrow instance configured) | no | recovery worker quarantine |
| `could not verify release landed (` | C - ambiguous (RPC unreachable during recovery) | no | recovery worker quarantine |
| `recovery requeues without progress` | G - requeue cap exhausted (release never landed) | no | recovery worker quarantine |

## An unresolved row holds the bitmap rotation

Before triaging, understand the clock you are on. The withdrawal bitmap covers
one generation of nonces at a time, and the operator will not rotate it past a
withdrawal that is not yet terminal. `manual_review` counts as not terminal,
because a human can still resolve one of these rows into a release.

So a row left in `manual_review` inside the generation the chain is currently on
blocks the rotation into the next one, and every withdrawal with a higher nonce
parks and waits. Those withdrawals are not lost and nothing is at risk, but they
do not settle until this row reaches `completed`, `failed`, or `failed_reminted`.

Confirm that is what is happening:

```
private_channel_operator_transaction_errors_total{error_reason="rotation_blocked_by_lower_nonce"}
```

A rising count means the operator wants to rotate and has been held back for
more than five minutes. It stays flat while a boundary is merely being crossed,
so a count that moves is a row someone has to resolve, not ordinary traffic. The
accompanying WARN log names the blocking nonce, which is the row to resolve
first. Resolving it is what releases the block; no rotation command exists and
none is needed.

## Path A.halting - build error that halted the pipeline

The trigger row's data is bad in a way that makes it unreleasable (NULL
nonce, malformed pubkey, builder rejection). The processor quarantined the
trigger and ran `halt_withdrawal_pipeline`, which drained the fetcher
channel and bulk-flipped every active withdrawal **at or above the trigger
row's `withdrawal_nonce`** to `manual_review`. Recovery has to handle both
the trigger and the collateral.

Withdrawals *below* the trigger's nonce are deliberately left in
`processing` or `parked`. Those rows were already signed or handed to the
sender, and terminalizing one discards the `completed` write that lands
when its release confirms, which leaves the next boot's bitmap diff seeing
a set bit with no `completed` row. The stuck-row recovery worker and
the stale-parked sweep own those rows; do not bulk-flip them by hand. If
the trigger row has no `withdrawal_nonce` at all the sweep is unbounded and
every active withdrawal is collateral.

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
   `quarantine_active_withdrawals`).
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
      AND id <> ALL(:excluded_ids);
   ```
   `:excluded_ids` is `:poison_id` plus every row a prior escalation left
   held in `manual_review` (each recorded in its own incident record).
   Re-arming a held row releases escrowed funds against a burn nobody
   proved happened.
   The `transactions` table does not store `error_message` - it lives in the
   alert payload only. Distinguishing trigger from collateral happens in
   triage (Step 2: oldest `updated_at` is the trigger), not in the re-arm
   query.
5. **Restart the withdraw operator** (Docker, from the repo root: `docker compose
   restart operator-private-channel`; or by container: `docker restart
   private-channel-operator-private-channel`). The fetcher picks up `pending` rows and
   processing resumes.
6. **Confirm recovery** by watching for new `Completed` webhooks for
   the re-armed rows.

## Path A.non-halting - row-specific bail (allowlist gate, paused mint, escrow drain)

One of two checks bailed on this row. `processor.rs::check_withdrawal_mint_supported`
runs first and emits `unsupported withdrawal mint:`;
`processor.rs::check_withdrawal_preflights` runs after it and emits
`mint paused:`, `insufficient escrow balance:`, and
`withdrawal mint absent on target chain:`. Either way the processor parked
**only this row** and **continued the loop** - there is no halt and no
collateral. Other withdrawals are unaffected.

Note that parking is not a refund. `WithdrawFunds` burns the user's channel
tokens before the row exists, and a row parked here never reaches the
sender, so the compensating remint that normally restores those tokens
after a permanent release failure never runs. Every disposition below has
to say explicitly what the user is owed.

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
   - `unsupported withdrawal mint:` - the escrow has no `AllowedMint` account for
     this mint, which is the same account `release_funds` requires, so the release
     could not have landed. Two causes, and they need opposite responses. Confirm
     which with `solana account $(allowed-mint-pda <instance> <mint>) --url
     <target-rpc>`, then:
     - **Never allowlisted.** No escrowed funds back the row. Do not re-arm; mark
       `failed`. The user's channel tokens were burned to create the row, so decide
       and record whether to restore them by an admin mint on the channel.
       [Escalate](_escalation.md) (Tier 3): a burn of an unsupported mint means one
       was created or distributed on the channel without a matching `AllowMint`.
     - **Present but undecodable** (message says `allowlist account failed to decode`).
       The account exists and is escrow-owned but its layout is not one this operator
       build knows, so the deployed program and the operator are out of step. Do not
       re-arm until they match. [Escalate](_escalation.md) (Tier 2).

     Either way, if the parked row's `withdrawal_nonce` is a multiple of the tree
     size, marking it `failed` releases later withdrawals onto a tree generation
     that was never rotated, so rotate before you terminalize it. This applies to
     any terminalized boundary row, not just this one.
   - `escrow ATA frozen for mint:` - the mint's `freeze_authority` holder froze the
     pooled escrow ATA, so no release for that mint can settle. The escrow still
     holds the funds and the row is intact. Nothing on our side can thaw it: contact
     the authority holder, and re-arm the row once
     `solana account $(escrow-ata <instance> <mint>) --url <target-rpc>` shows the
     account no longer frozen. Because the ATA is pooled per mint, expect every
     withdrawal for that mint to park here, not just this one. Only the pooled
     ATA is checked: a freeze on one user's own ATA fails on-chain instead and
     reminds their channel balance, so it lands in
     [withdrawal_failed_reminted.md](withdrawal_failed_reminted.md), not here.
     [Escalate](_escalation.md) (Tier 2).
   - `withdrawals blocked for mint:` - an admin set the mint's withdrawal gate, which
     `release_funds` rejects on-chain. The escrow still holds the funds and the row is
     intact, so nothing is lost. Re-open the gate (`BlockMint` with
     `block_withdrawals: false`, or `AllowMint`, which re-opens both), then re-arm the
     row. Blocking withdrawals is deliberate, so confirm with whoever set it before
     re-opening. No operator restart is needed either way: the gate is mirrored onto
     the `mints` row and read on every withdrawal, so a block and a re-open both take
     effect once the indexer has processed that slot. [Escalate](_escalation.md) (Tier 2).
   - `transfer-hook validation account missing for mint:` - the mint's
     `TransferHook` points at a hook program whose `ExtraAccountMetaList` does not
     exist, so Token-2022 can resolve no transfer of it and no release can settle.
     The escrow still holds the funds and the row is intact. Nothing on our side
     creates that account: only the hook program can, so contact the mint issuer.
     Its address is the `["extra-account-metas", mint]` PDA of the hook program the
     mint names; re-arm the row once `solana account <that address>` shows it
     exists. Expect every withdrawal of that mint to park here. Deposits of it fail
     on-chain for the same reason, so consider blocking deposits (`BlockMint` with
     `block_deposits: true`) until it is fixed. [Escalate](_escalation.md) (Tier 2).
   - `withdrawal mint absent on target chain:` - the mint was allowlisted, so its
     account existed then, and the node answered from a slot at or past that allow
     before reporting nothing. A lagging node cannot produce this message; it
     errors and the operator retries instead. So the account was closed, or
     `rpc_url` points at a different cluster from the one the escrow is deployed
     on. Check `solana account <mint> --url <target-rpc>` and confirm the cluster.
     If the cluster is wrong, correct `rpc_url`, restart the operator, and re-arm
     the parked rows. If it is right and the account is gone, do not re-arm;
     refund out-of-band, and note that the channel tokens were burned too.
     [Escalate](_escalation.md) (Tier 2).
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
broken row and burns its nonce. The asymmetric cost favors a noisy
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
> the next sweep.
>
> **A row with no recorded signature is no longer quarantined on sight.**
> The signature is written in the same transaction that claims the row, so
> an empty journal means the release never broadcast. Recovery corroborates
> that against the on-chain withdrawal bitmap - a fresh read of the
> generation and the consumed-nonce bits - and re-arms the row to `pending`
> automatically when the nonce's bit is provably unset; this is the manual
> `NOT_LANDED` decision in Step 3 below, now automated with the same proof.
> Such rows never reach manual review, so a signatureless row that does
> arrive here means a read it depends on stayed broken for 10 minutes of
> sweeps: either the on-chain proof (`... release verification still
> uncertain after ...`) or the signature journal itself (`release signature
> journal still unreadable after ...`). The second points at the database
> rather than the chain, so check Postgres health before triaging the row.
> Verify on-chain and act on the verdict; never blindly re-arm a row whose
> release may already be on-chain.
>
> A journal that reads back *corrupt* is different and arrives immediately
> (`malformed stored release signature ...`): a signature was recorded, so
> the release may have broadcast, and re-reading only returns the same bytes.
>
> The RPC-could-not-classify case for a row that *does* have recorded
> signatures (`could not verify release landed (...)`, with the signature
> list appended) still lands here unchanged.
>
> A sender that could not establish the `PendingRemint` handoff leaves the row
> `Processing` (metric `pending_remint_state_unknown`) rather than quarantining
> it, so it reaches this path through the recovery sweep above.

> **Some Path C rows resolve themselves; check before you act.** The
> quarantine now copies the broadcast signatures onto the row
> (`remint_signatures`), and every recovery tick plus each operator boot
> re-classifies them. A row quarantined with `could not verify release
> landed (...)` was quarantined because the RPC was unreachable, not because
> the release failed, so once the RPC catches up that row promotes itself to
> `completed` with the landed signature. A row quarantined with `no broadcast
> signatures recorded ...` has nothing to re-check and will never self-clear.
> Re-read the row's status before starting the steps below: if it is already
> `completed`, the sweep resolved the bookkeeping and the alert can be closed
> against that signature. The promotion is bookkeeping only and needs no
> operator restart to take effect: the sender holds no local copy of the
> released-nonce set, and every release is authorized against the on-chain
> bitmap itself.

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
   state before deciding - `signature` is the originating PrivateChannel
   burn:
   ```bash
   solana confirm -v <signature> --url <private-channel-rpc>
   ```
   `Finalized` with no error means burned. A `not found` is **not** proof of
   non-inclusion. It counts only if the endpoint observed the row's `slot`,
   which takes both bounds against `<private-channel-rpc>`:
   `solana first-available-block` at or below the row's `slot`, else the slot
   was pruned away, **and** `solana slot --commitment finalized` at or above
   it, else the node never reached it. Either bound alone is worthless: a
   pruned node and a lagging node return the same `not found` for a burn that
   did land. The operator enforces both before calling a signature dead by
   absence, the top via the blockhash-expiry check that produces
   `DeadByAbsence` and the bottom via the ledger floor in
   `sender/remint.rs::coverage_verdict`. Honor both here.
   - Burned, no release → re-arm to `pending` and restart operator. The
     withdrawal will be re-attempted; the channel-side burn is idempotent.
   - Not burned, proven absent → **do not re-arm.** Nothing backs the
     row: a re-arm releases escrowed target-chain funds against a burn
     that never happened, and neither the builder nor the escrow program
     can detect it. Capture the row, its `signature` and the
     `solana confirm` output in the incident record, then mark the row
     terminal:
     ```sql
     UPDATE transactions SET status = 'failed', updated_at = NOW()
      WHERE id = :transaction_id;
     ```
     No refund is owed - the user still holds their channel tokens.
     [Escalate](_escalation.md) (Tier 3): a row with no finalized burn
     means the ingestion or write path has a defect.
   - Burn state unproven (RPC error, or `not found` with either bound
     unmet: floor above the row's `slot`, or finalized tip below it) →
     stop. [Escalate](_escalation.md) (Tier 2).
     Do not terminalize: marking a genuinely burned row `failed` strands
     the user's tokens with no restoration path. Re-run once the endpoint
     covers the slot, or from an archival node. Record the id as held in
     the incident record - it stays in `manual_review`, so a later halt's
     Path A Step 4 must exclude it.
4. **If `AMBIGUOUS`:** stop. [Escalate](_escalation.md) (Tier 2). Wait
   for RPC visibility to recover. Do not act.

> **If the quarantined release actually landed** (verdict `LANDED`, but
> the row was quarantined with `released on-chain with no recorded
> broadcast signature` and never written `Completed`), the consumed
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
SELECT id, signature, slot, withdrawal_nonce, mint, amount, recipient,
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

`not found` proves non-inclusion only if the endpoint observed the row's
`slot`: first-available-block at or below it **and** finalized tip at or
above it. A pruned node and a lagging node both return the same `not found`
for a burn that did land. Same two bounds as Path C Step 3.

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

#### Burn state unproven

The RPC failed, or `not found` came back without ledger coverage of the
row's `slot`. Stop. [Escalate](_escalation.md) (Tier 2). **Do not delete
the row** - the burn may have landed, and this row is the only record of
it. Re-run Step 2 once the endpoint covers the slot, or against an
archival node. Record the id as held in the incident record, as in Path C
Step 3: the row stays in `manual_review`, so a later halt's Path A Step 4
must exclude it.

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
underfunded, nonce already consumed, account state) means re-sending will not
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
- Burn verdict on the source side (`signature` + `solana confirm` output),
  when the path branched on it.
- Recovery action taken (SQL run, sig used, escalation path).
- RPC endpoint used for verification.

These feed the audit trail for any user-facing reconciliation.

