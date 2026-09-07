# Runbook - Deposit `Failed`

Triggered by webhook payload `status=failed` for a row with
`transaction_type='deposit'`. Unlike on the withdrawal side, this is the
**primary** terminal alert for deposits - there is no remint path, so
sender-side failures land here instead of in `ManualReview`.

## Symptom

- Webhook with `status=failed`, `transaction_type=deposit`.
- ERROR log: `Transaction <id> Failed`.

## Triage - dispatch by `error_message`

`error_message` is on the webhook payload (not in the DB). Each value
maps to a specific sender-side site:

| `error_message` | Source | Trigger |
|---|---|---|
| starts with `Failed idempotency lookup for transaction_id` | `sender/mint.rs` | `getSignaturesForAddress` RPC failed during pre-send memo scan. |
| `Mint initialization failed` | `sender/transaction.rs` | Log-only marker, **no webhook**: this message is only built on the defensive `MintNotInitialized`-without-`transaction_id` branch, and with no id there is no row to write a status to. A failing just-in-time `InitializeMint` no longer lands here at all: transient init failures (RPC outage, send error, unconfirmable init) re-arm the deposit to `pending` under the requeue cap instead, and only the recovery worker may escalate one. The *post-JIT* mint failure case (mint exists but unusable) is in [`deposit_manual_review.md`](deposit_manual_review.md) § Path D; the cap-exhausted case is § Path G. |
| `Unexpected mint error` | `sender/transaction.rs` | `MintNotInitialized` confirmation result on a non-Mint tx (defensive; should never fire). |
| `Confirmation failed - transaction status unknown, unsafe to retry` | `sender/transaction.rs` | RPC polling timed out for a non-idempotent send; mint may or may not have landed. |
| free-form (often a program-error debug repr) | `sender/transaction.rs` | On-chain program error during confirmation (e.g. paused mint, bad mint authority — `OwnerMismatch` from SPL Token's `mint_to`) or RPC confirmation error. **The most common rotated-admin-key case lands here, not in `manual_review`** — `OwnerMismatch` is `Custom(3)`, which is not in the JIT-trigger classifier's allow-list. |

## Recovery

The decision shape is the same for every trigger: did the mint land on
the private channel or not?

### Step 1 - verify on-chain

Run [`_verify_onchain_mint.md`](_verify_onchain_mint.md). The verdict
drives every recovery path below.

### Step 2 - branch on verdict

#### `LANDED <signature>` - mint actually succeeded

The user already received private channel side tokens. The `Failed` status is
incorrect; make it `completed` with the observed signature:

```sql
UPDATE transactions
   SET status = 'completed',
       counterpart_signature = :signature,
       updated_at = NOW()
 WHERE id = :transaction_id;
```

If this UPDATE is rejected by the unique partial index on
`counterpart_signature`, **stop**. The signature is already attached to a
different row. [Escalate](_escalation.md) (Tier 3 - operator
misidentified row) before proceeding; running ahead would silently
double-credit.

File via the Tier 3 process: `Failed` was wrong, which means either:
- The confirmation timeout fired but the tx finalized after - common
  case; the runbook fix is enough.
- The classifier or routing has a bug that prevented the success path
  from running.

#### `NOT_LANDED` - mint genuinely did not happen

Re-arm the row to `pending`. The idempotency memo is a safety net even
if a previous attempt did broadcast: the operator's pre-send memo scan
short-circuits to `Completed` if it finds a memo'd signature.

Re-arming is additionally guarded by the pre-mint signature gate: a
non-terminal row keeps its persisted broadcast signatures
(`pending_release_signatures` is only GC'd once the row is terminal), and
the deposit processor re-classifies them on the channel before building a
new mint. A signature that turns out to have landed completes the row
instead of re-minting; an unverifiable one is left `processing` for the
recovery sweep, which re-checks it on the channel and is the sole owner of
the quarantine decision (see
[`deposit_manual_review.md`](deposit_manual_review.md) Path E).

```sql
UPDATE transactions SET status = 'pending', recovery_requeue_attempts = 0, updated_at = NOW()
 WHERE id = :transaction_id;
```

Before re-arming for `Mint initialization failed` specifically: confirm
the underlying mint account and authority are correctly set up on the
private channel chain. Without that, the next attempt fails the same way.

For program-error cases (paused mint, bad authority, etc.), fix the
underlying condition first, then re-arm.

#### `AMBIGUOUS` - RPC unreachable, history rotated, or inconclusive

Stop. [Escalate](_escalation.md) (Tier 2). Do not act.

- If RPC is recovering, retry the verification procedure once visibility
  is back.
- If the original `processed_at` predates the RPC's signature lookback
  window, engineering must do an out-of-band audit via archived block
  history before any recovery action.

Re-arming to `pending` in the `AMBIGUOUS` case relies on the operator's
own idempotency check; that check uses the same lookback window and will
be just as blind. Do not lean on it alone for an old `processed_at`.

## Cross-link — when ManualReview is the right runbook

If you expected `Mint initialization failed` here but the row is in
`manual_review` instead, see
[`deposit_manual_review.md`](deposit_manual_review.md) § Path D —
that's the path for a successful (or unnecessary) JIT followed by a
structural mint problem (wrong authority, corrupt data).

## Special case - `Failed idempotency lookup`

This trigger fires when the operator's pre-send memo scan itself
errored (RPC down at attempt time). The mint was **not** sent
afterwards. On-chain verification will most likely return `NOT_LANDED`
and Step 2's re-arm path applies. The retry is safe because the next
attempt will run the memo scan again (and will detect any prior
landed mint).

## Not an alert - `deposit_ownership_lost` metric

`OPERATOR_TRANSACTION_ERRORS{error_reason="deposit_ownership_lost"}` is
**informational, not an incident**. It counts a deposit mint whose sender
lost ownership of its row before broadcast: while the built mint was queued,
the recovery worker demoted the row (or a re-fetch re-locked it), so the
stale builder was dropped without broadcasting and without writing any
status. The same label also fires when a `MintNotInitialized` JIT re-fire
loses its lease mid-retry (recovery demoted the row during the JIT window);
the semantics are identical. The row's current owner or recovery mints it
instead, so the one-deposit-one-mint invariant holds. No operator action; a
sustained rate only signals the in-flight cap or the JIT window is stranding
builders long enough for recovery to reclaim them (a throughput tuning
signal, not a correctness bug).

`OPERATOR_TRANSACTION_ERRORS{error_reason="jit_missing_claim_lease"}` is a
defensive counter that should never fire: a JIT re-fire arrived without the
ownership epoch its first claim stored. The re-fire is dropped without
broadcast and the row stays Processing for recovery. A sustained rate is a
code bug, not an operational condition.

`OPERATOR_TRANSACTION_ERRORS{error_reason="mint_jit_transient"}` counts a
deposit whose just-in-time `InitializeMint` could not be completed on this
attempt (channel RPC blip, mint metadata lookup failure, unconfirmable init).
The deposit's own `mint_to` did reach the chain and failed there saying the mint
is not initialized, so its signature stays in the journal; no status is written
and the row is requeued to `pending`, paired with `prebroadcast_requeued`. The
re-mint on the next pickup is gated on that stored signature being proven dead,
so this is not a double-mint path. A short burst during a channel RPC wobble is
expected and self-healing. Watch instead for `prebroadcast_requeue_cap` on the
escrow operator: that means a deposit spent all three requeues without the mint
landing and is now waiting on the recovery sweep to quarantine it, which
surfaces as [`deposit_manual_review.md`](deposit_manual_review.md) § Path G.

`OPERATOR_TRANSACTION_ERRORS{error_reason="mint_jit_requeue_raced"}` is
informational: the JIT requeue found the row no longer `processing`, so recovery
or another writer already owns it. Nothing is written and no action is needed.

## Post-incident artifacts

- Transaction id, originating Solana deposit `signature`, `recipient`,
  `mint`, `amount`.
- Full webhook `error_message` and `error_reason` metric label.
- On-chain verdict (`LANDED <sig>` / `NOT_LANDED` / `AMBIGUOUS`).
- Recovery action taken.
- If the trigger pointed to environmental misconfiguration (paused mint,
  bad authority), the remediation step taken on the private channel side.

