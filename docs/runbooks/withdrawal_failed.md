# Runbook - Withdrawal `Failed`

Triggered by webhook `status=failed` for a row with `transaction_type='withdrawal'`.

## Why this is rare for withdrawals

The withdrawal failure path normally goes
`processing → pending_remint → {completed, failed_reminted, manual_review}`.
The terminal `failed` status is set by `send_fatal_error` in
`indexer/src/operator/sender/transaction.rs`, which is reached when the
sender's `TransactionContext` carries no remint info - primarily for non-
withdrawal transactions (deposits, mint init, bitmap rotation).

A withdrawal row reaching `failed` therefore means one of:
- The row was misrouted (a non-withdrawal tx that incorrectly carried
  `transaction_type='withdrawal'`). Investigate the indexer.
- A code change moved a withdrawal-specific failure to `send_fatal_error`.
  Investigate recent commits to `sender/transaction.rs`.
- Operator-side bug.

## Procedure

1. **Confirm `transaction_type`.** If `deposit`, this runbook does not apply
   - deposit-side recovery is out of scope here.
2. **Run [`_verify_onchain_release.md`](_verify_onchain_release.md).** Funds
   risk depends on whether the release actually landed:
   - `LANDED <sig>`: critical bug - `failed` was wrong. The user got their
     funds; the row should be `completed`. Fix:
     ```sql
     UPDATE transactions
        SET status = 'completed',
            counterpart_signature = :sig,
            updated_at = NOW()
      WHERE id = :transaction_id;
     ```
     [Escalate](_escalation.md) (Tier 3 - code-defect) on the operator
     code path that produced this; the classifier or routing has a bug.
     Capture `error_message`, the `failed` `processed_at`, and the
     discovered signature.
   - `NOT_LANDED`: no on-chain action; user's private channel side state must be
     reconciled (burn may have completed without release). Treat the same as
     Path B in [`withdrawal_manual_review.md`](withdrawal_manual_review.md):
     check burn state, [escalate](_escalation.md) (Tier 1) for manual
     restoration if needed. Do not re-arm a `failed` row - the status is
     terminal by contract.
   - `AMBIGUOUS`: [escalate](_escalation.md) (Tier 2). Do not act.

## Post-incident

`Failed` on a withdrawal is a code defect signal, not just an ops event.
Always file a follow-up ticket against the operator regardless of recovery
outcome.
