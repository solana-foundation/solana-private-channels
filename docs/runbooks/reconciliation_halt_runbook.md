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
in `observed_releases` at or below that slot. Before comparing, the operator
waits (up to 30 s) for the escrow indexer's committed checkpoint to reach the
custody slot; if it does not, liabilities are unknown for that tick, the
counter holds, and a `warn!` names the checkpoint and slot. Its halt reason reads
`... custody <C> short of ledger liabilities <L> by <GAP>, tolerance <T> at slot <S> ...`.

The DB also supplies the mint universe (every `mints` row, so a blocked or
not-held mint is still checked) and the in-flight envelope. Both checks use the
same small bps cushion of custody.

When either check breaches for three consecutive ticks, the operator **halts**:

1. Sets the durable `reconciliation_halt` flag (reason recorded).
2. Quarantines every active (`pending`/`processing`/`parked`) withdrawal to
   `manual_review`.
3. Forces its `/health` to 503 (pages orchestration).
4. Posts the halt webhook. This webhook is the only alert - there is no separate
   sensitive alert layer; the alert fires together with the halt.

The halt flag is read at the top of the shared fetcher loop, so it freezes
**both** the escrow and withdraw operators, and it **survives restarts** - both
re-read it at first poll and stay frozen until it is cleared.

## Symptom

- Deposits stop minting and withdrawals stop releasing across both operators.
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

1. **On-chain Solana custody.** Sum the escrow instance's token accounts for the
   mint (`getTokenAccountsByOwner` on the escrow PDA), at `finalized`. This is
   authoritative custody. See [`_verify_onchain_release.md`](_verify_onchain_release.md).
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
     SUM(CASE WHEN t.transaction_type='withdrawal' AND EXISTS (
              SELECT 1 FROM observed_releases r
              WHERE r.withdrawal_nonce = t.withdrawal_nonce AND r.slot <= <SLOT>)
              THEN t.amount ELSE 0 END) AS released
   FROM transactions t WHERE t.mint = '<MINT>';
   ```

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
