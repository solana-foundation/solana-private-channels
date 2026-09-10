# Escalation - Operator Runbooks

When a runbook says "escalate", come here. The link annotation in the
source runbook tells you which tier you're in (e.g.
`[escalate](_escalation.md) (Tier 2)`). Page the right destination and
send the prescribed payload.

## Tier 1 - Stranded user funds (page immediately)

Funds are demonstrably out of the user's control and not recoverable
by re-arming the row.

Page: the on-call operator.
Also notify: the treasury / refund-coordination owner, and customer
support.

Send:
- `transaction_id`, `transaction_type`, `amount`, `mint`, `recipient`.
- Originating signature (Solana for deposits, private channel side for
  withdrawals).
- On-chain verdict from the verify procedure and every signature
  checked.
- Which leg failed and why funds are stranded.
- Whether the user needs proactive comms before the next operator tick.

## Tier 2 - AMBIGUOUS on-chain state (page, less urgent)

Verification could not produce `LANDED` or `NOT_LANDED`. Cause is
usually RPC unreachable, signature lookback rotated past `processed_at`,
or evidence is contradictory (e.g. sig finalized but the nonce's bit is clear).
Funds may or may not be stranded; no recovery action is safe yet.

Page: the on-call operator.
Do NOT notify treasury or customer support until the verdict resolves -
those are Tier 1 only.

Send:
- `transaction_id`, `transaction_type`, `amount`, `mint`, `recipient`.
- Originating signature (Solana for deposits, private channel side for
  withdrawals).
- On-chain verdict from the verify procedure and every signature
  checked.
- Specific RPC endpoint queried and the exact failure (timeout,
  `MethodNotFound`, error message).
- `processed_at` and the oldest signature timestamp the RPC returned
  (to assess if the lookback window rotated past the row).
- Whether retry is appropriate once RPC visibility recovers, and the
  proposed re-check plan.

## Tier 3 - Code-defect signals (file P1 ticket, no immediate page)

The operator state machine produced an outcome the runbook says
shouldn't happen. Funds are not at immediate risk - the existing fence
(webhook, idempotency memo, unique-index, etc.) held - but the
underlying bug needs fixing before recurrence.

File: a P1 ticket against the engineering team that owns the operator.

Capture:
- `transaction_id`, `transaction_type`, `amount`, `mint`, `recipient`.
- Originating signature (Solana for deposits, private channel side for
  withdrawals).
- On-chain verdict from the verify procedure and every signature
  checked.
- The runbook section that says this shouldn't happen, quoted.
- Reproduction steps if known.
- Whether to halt similar incidents (manually flip active rows to
  `manual_review` so the operator stops processing them) until
  patched, or whether the existing fence is enough to let normal
  operation continue.

## Decision: which tier?

If you arrived here without a tier annotation in the source link, use
this quick test:

1. Are user funds demonstrably stuck and not recoverable by SQL?
   → **Tier 1.**
2. Is the on-chain verdict `AMBIGUOUS` or contradictory?
   → **Tier 2.**
3. Operator did the wrong thing but funds are safe (existing fence held)?
   → **Tier 3.**
4. None of the above?
   → You probably don't need to escalate; finish the runbook recovery.

When in doubt, escalate to the higher tier.
