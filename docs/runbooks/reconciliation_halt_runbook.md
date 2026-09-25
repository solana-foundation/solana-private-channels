# Runbook - Reconciliation Halt

This runbook covers the **runtime reconciliation halt** in the escrow operator.
Unlike the alert-only behavior of the past, a **proven** insolvency now fails
closed: it sets a durable DB flag that freezes **both** operators' fetchers
(deposits and withdrawals), quarantines active withdrawals, forces the escrow
operator's `/health` to 503, and posts a webhook.

A halt is deliberate: it trades liveness for integrity. It only fires when one of
two per-mint invariants (below) breaches for three consecutive finalized-read
ticks, so a transient (in-flight activity, a one-off bad RPC read) cannot trip it. Recovery
is **manual** - a human must confirm real backing before clearing the flag.

As with every runbook here, the recovery `UPDATE` statements are
**bookkeeping, not fund movement** - see
[`README.md`](README.md) § "Recovery SQL is bookkeeping; fund restoration is
human-in-the-loop".

---

## What the operator does automatically

Runtime reconciliation checks two invariants against **finalized** reads, per
mint, each with its own consecutive-tick counter:

> **Supply: channel `Mint.supply` (PrivateChannel) must not exceed escrow custody
> (Solana) beyond the in-flight envelope.**
>
> **Liability: escrow custody must cover the DB ledger's liabilities at the custody
> slot.**

On-chain supply is already net of burns (the program burns channel tokens at
withdrawal initiation, not at release), so the supply check does **not** trust
the DB ledger; it catches over-issuance. Its halt reason reads
`... custody <C> short of supply by <GAP>, envelope <E> tolerance <T> ...`.

The liability check catches a custody drain that the supply check cannot see
while unminted (`pending`, `failed`, `manual_review`) deposits pad the gap.
Liabilities are every deposit indexed at or below the custody slot, in any
status, minus every withdrawal whose `release_funds` the escrow indexer recorded
in `observed_releases` at or below that slot. A release discharges what it
actually moved, capped at what its row owed, so a payout smaller than its row
leaves the rest owed and one larger leaves the excess standing as a shortfall.
Before comparing, the operator waits (up to 30 s) for the escrow indexer's
committed checkpoint to reach the custody slot; if it does not, liabilities are
unknown for that tick and the counter holds. Its halt reason reads
`... custody <C> short of ledger liabilities <L> by <GAP>, tolerance <T> at slot <S> ...`.

A tick that cannot pin the ledger leaves the liability check unarmed, so
`private_channel_operator_reconciliation_liability_dark_ticks` counts how many
consecutive ticks it has been off (0 when armed) and every third dark tick logs
`reconciliation_alert` and posts a webhook. An indefinitely frozen escrow
checkpoint keeps that firing rather than going quiet. A shortfall too small to
halt is still exported per mint as
`private_channel_operator_reconciliation_liability_shortfall_raw`, because that
is the state that refuses the next boot.

The DB also supplies the mint universe (every `mints` row, so a blocked or
not-held mint is still checked) and the in-flight envelope. Custody is read from
the escrow instance's associated token account (ATA) for each of those mints,
using the mint's recorded token program. The escrow program only deposits into
and releases from that ATA, so it is the whole backing; tokens sent to any other
account the escrow happens to own are not custody. Custody, channel
supply and the envelope are all read before the checkpoint wait, so the supply
invariant compares one instant.

Channel supply is only used when it is fresh. Each tick first finds the channel's
newest block and requires its block time to be under 120 s old, then accepts a
supply read only if it was answered at or after that block. A channel node that
is frozen, a read replica that lags, or a backend behind the others therefore
gives no supply reading for that tick instead of an old one. This needs the
channel write node's clock and the operator's clock to agree to well within
120 s (run NTP on both). Both checks use the same small bps cushion of
custody; startup applies the same formula on top of `mismatch_threshold_raw`,
defaulting to 0 so a boot tolerates nothing it is not configured to.

When either check breaches for three consecutive ticks, the operator **halts**:

1. Sets the durable `reconciliation_halt` flag (reason recorded).
2. Quarantines every active (`pending`/`processing`/`parked`) withdrawal to
   `manual_review`.
3. Forces its `/health` to 503 (pages orchestration).
4. Posts the halt webhook. This webhook is the only alert - there is no separate
   sensitive alert layer; the alert fires together with the halt.

### Inputs-dark halt

The checks are only as good as their inputs. A tick that cannot read a required
input (the DB mint set, custody, the in-flight envelope, a fresh channel supply
for any mint, the escrow checkpoint, or the ledger rows) counts as a dark tick, exported as
`private_channel_operator_reconciliation_input_dark_ticks`. Three dark ticks in
a row (about 10 minutes at the default 5 minute interval) halt with the reason
`reconciliation halt: required inputs unavailable for <N> consecutive ticks (last: <reason>)`.
Any tick that reads everything resets the count. A lagging escrow checkpoint is
not a dark tick; it stays on the liability-dark alert described above. A
checkpoint that cannot be read at all during the wait is a dark tick.

This halt sets the flag, forces `/health` to 503 once the flag has been written, and
posts a webhook with `halt_reason` and `dark_ticks`, but it does **not** quarantine
withdrawals: nothing is proven wrong, and the flag alone already blocks every send. Typical causes are
a Solana RPC or DB outage, a channel node that is down, frozen or more than 120 s
behind, more than 120 s of clock skew either way between the channel write node
and the operator, or an unreadable `mints` row. Restore the input first. Clearing
the flag while the input is still unreadable halts again on the next tick. If the
flag write itself fails, the operator retries it within the tick and again on
later ticks until it lands. `/health` is only forced to 503 once the flag has
landed, because that latch lasts until a restart; until then the webhook, the
`input_dark_ticks` gauge and its alert are what page. The halt webhook fires once
per incident, not on every retry: once for the outage, and once for each mint
whose breach confirms while the flag write is still failing.

An inputs-dark halt never replaces an insolvency halt: if the flag already holds
an insolvency, it is left as it is, reason included. The other way round, a
breach that confirms while an inputs-dark halt holds still trips as an
insolvency: it replaces the reason, quarantines active withdrawals and posts the
insolvency webhook. Before clearing an inputs-dark halt, check that no breach is
building: look for `halt pending confirmation` warnings and a nonzero
`private_channel_operator_reconciliation_liability_shortfall_raw`. A breach still
counting toward its third tick has not quarantined anything yet, so clearing the
flag then would let withdrawals of that mint go out.

When there are more than 100 mints, custody is read in batches of 100 that can
answer at different slots. Each mint is compared with the ledger at the slot its
own batch answered at, and a liability halt reason names that slot. Custody is
held to the same freshness rule as channel supply: each tick finds Solana's
newest finalized block, requires it to be under 120 s old, and a custody batch
answered below it is a failed custody read, so a lagging RPC backend makes the
tick dark instead of checking an old balance.

### Where the halt is enforced

The halt flag is read at the top of the shared fetcher loop, so it freezes
**both** the escrow and withdraw operators, and it **survives restarts** - both
re-read it at first poll and stay frozen until it is cleared. A fetcher that
cannot read the flag skips the poll too, counted as
`private_channel_operator_transaction_errors_total{error_reason="halt_read_error"}`.

The flag is also checked in the same database statement that claims a row right
before its mint or release is broadcast, so work already in the pipeline when the
halt lands is not sent either. Such a claim is counted as
`error_reason="halted_before_broadcast"`. A deposit or withdrawal stopped this way
that never broadcast is put back to `pending` right away, without using a requeue
attempt, and goes out once the flag is cleared. One with an earlier broadcast
attempt stays `processing` for the recovery worker to check on chain. A claim whose
statement ran before the halt committed is still sent; each later attempt is refused.

Remints are not gated by the halt. A remint only returns tokens that were burned
for a withdrawal whose release is proven not to have happened, so it cannot push
supply above custody.

## Symptom

- Deposits stop minting and withdrawals stop releasing across both operators.
  Remints of failed withdrawals continue.
- The escrow operator's `/health` returns 503 with `"reason":"forced"`.
- Logs carry `RECONCILIATION HALT tripped; freezing both pipelines` with the
  reason: `short of supply by` (supply check) or `short of ledger liabilities`
  (liability check), plus the mint, gap, tolerance and tick count.
- Active withdrawals are in `manual_review`.

## Detection

Inspect the flag directly:

```sql
SELECT halted, reason, halted_at FROM reconciliation_halt WHERE id = TRUE;
```

A `halted = TRUE` row is an active halt. `reason` carries the offending mint,
which check tripped, and the exact custody / gap / envelope or liabilities /
tolerance numbers.

## Investigate before clearing

Do **not** clear the flag until you have confirmed real backing. For the mint in
the halt reason:

1. **On-chain Solana custody.** Read the balance of the escrow instance's ATA
   for the mint (derived from the escrow PDA, the mint and its token program), at
   `finalized`. This is authoritative custody. Other token accounts the escrow PDA
   owns (`getTokenAccountsByOwner`) are not custody: the program never moves them. See [`_verify_onchain_release.md`](_verify_onchain_release.md).
2. **On-chain PrivateChannel supply.** Read the channel mint's `Mint.supply`
   (`getAccountInfo` on the mint, decode the SPL Mint). This is the total minted,
   already net of burns. `supply - custody` is the halt gap.
3. **In-flight envelope.** Confirm the gap is not merely un-settled work:

   ```sql
   SELECT mint, COALESCE(SUM(amount),0) AS in_flight
   FROM transactions
   WHERE mint = '<MINT>'
     AND status IN ('pending','processing','parked','pending_remint')
   GROUP BY mint;
   ```

4. **DB ledger liabilities.** The basis of a liability halt, and context for a
   supply halt (e.g. no deposits justify the minted amount). Use the slot from
   the halt reason for `<SLOT>`:

   ```sql
   SELECT
     SUM(CASE WHEN t.transaction_type='deposit' AND t.slot <= <SLOT>
              THEN t.amount ELSE 0 END) AS deposits,
     SUM(CASE WHEN t.transaction_type='withdrawal' AND r.withdrawal_nonce IS NOT NULL
              THEN LEAST(COALESCE(r.amount, t.amount), t.amount) ELSE 0 END) AS released
   FROM transactions t
   LEFT JOIN observed_releases r
     ON r.withdrawal_nonce = t.withdrawal_nonce AND r.slot <= <SLOT>
   WHERE t.mint = '<MINT>';
   ```

   A boot refuses on the same comparison. When the escrow indexer's checkpoint is
   below the custody snapshot, that boot-time read also counts a `completed`
   withdrawal as released, since the operator writes that status only after the
   release confirms. A refusal there is a shortfall neither the chain nor the
   operator's own records explain.

   For a liability halt, custody below `deposits - released` means tokens left
   escrow without a recorded release. Only `ReleaseFunds` is indexed as an
   outflow, so check the escrow token accounts' history for other movements: a
   permanent-delegate transfer (`mints.has_permanent_delegate`) or a Token-2022
   fee effect is a real custody loss to the escrow even if the issuer calls it
   legitimate, and must be resolved before clearing. A liability gap that
   disappears once the escrow indexer catches up was lag, not a drain.

If `supply > custody` persists beyond the in-flight envelope once the in-flight
work settles, this is a **real** solvency incident (operator-key over-issuance or
a custody shortfall). Escalate per [`_escalation.md`](_escalation.md); rotate the
operator/admin key if over-issuance is suspected, and do **not** resume the
pipelines.

Rotating the channel admin means migrating every receipt mint's authority to the
new key with SPL `SetAuthority` first. The old key stays the on-chain
`mint_authority` until you do, so deposits fail with `OwnerMismatch`
(see [`deposit_failed.md`](deposit_failed.md)) and the old key keeps the ability to
mint. This is separate from the escrow `Instance.admin`, which `SetNewAdmin`
rotates on its own.

## Recover (only after backing is confirmed)

Once you have verified that custody genuinely backs the minted supply (e.g. the
gap was a transient the reads have since settled, or the discrepancy has been
reconciled on-chain), clear the flag:

```sql
UPDATE reconciliation_halt SET halted = FALSE, halted_at = NOW() WHERE id = TRUE;
```

Both operators' fetchers resume on their next poll (no restart required). The
escrow operator's forced-unhealthy latch is in-memory, so restart it (or let the
supervisor cycle it) to clear the 503 once the flag is down. Re-queue any rows
left in `manual_review` per the relevant per-row runbook
([`withdrawal_manual_review.md`](withdrawal_manual_review.md)) after confirming
each is safe.
