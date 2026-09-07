# Runbook - Reconciliation Halt

This runbook covers the **runtime reconciliation halt** in the escrow operator.
Unlike the alert-only behavior of the past, a **proven** insolvency now fails
closed: it sets a durable DB flag that freezes **both** operators' fetchers
(deposits and withdrawals), quarantines active withdrawals, forces the escrow
operator's `/health` to 503, and posts a webhook.

A halt is deliberate: it trades liveness for integrity. It only fires when the
on-chain channel-token supply exceeds on-chain escrow custody by more than the
DB-computed in-flight envelope for three consecutive finalized-read ticks, so a
transient (in-flight activity, a one-off bad RPC read) cannot trip it. Recovery
is **manual** - a human must confirm real backing before clearing the flag.

As with every runbook here, the recovery `UPDATE` statements are
**bookkeeping, not fund movement** - see
[`README.md`](README.md) § "Recovery SQL is bookkeeping; fund restoration is
human-in-the-loop".

---

## What the operator does automatically

Runtime reconciliation checks a single on-chain invariant against **finalized**
reads, per mint:

> **channel `Mint.supply` (PrivateChannel) must not exceed escrow custody (Solana).**

On-chain supply is already net of burns (the program burns channel tokens at
withdrawal initiation, not at release), so `supply <= custody` is a pure
economic-backing check that does **not** trust the DB ledger. The DB is read only
to (a) enumerate the mint universe (the `mints` table, so a blocked or not-held
mint with outstanding supply is still checked) and (b) compute the per-mint
in-flight envelope; neither is compared against custody.

When a mint shows `supply - custody` greater than its in-flight envelope (plus a
small bps cushion) for three consecutive ticks, the operator **halts**:

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
  mint, the supply gap, envelope, tolerance, and tick count.
- Active withdrawals are in `manual_review`.

## Detection

Inspect the flag directly:

```sql
SELECT halted, reason, halted_at FROM reconciliation_halt WHERE id = TRUE;
```

A `halted = TRUE` row is an active halt. `reason` carries the offending mint and
the exact custody / supply-gap / envelope / tolerance numbers.

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

4. **DB ledger (context only, not the halt basis).** The recorded deposits and
   completed withdrawals help explain *why* the supply may be unbacked (e.g. no
   deposits justify the minted amount), but the halt does not compare them:

   ```sql
   SELECT mint,
          SUM(CASE WHEN transaction_type='deposit' AND status='completed'
                   THEN amount ELSE 0 END)   AS deposits_completed,
          SUM(CASE WHEN transaction_type='withdrawal' AND status='completed'
                   THEN amount ELSE 0 END)   AS withdrawals_completed
   FROM transactions WHERE mint = '<MINT>' GROUP BY mint;
   ```

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
