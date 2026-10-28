# Runbook - Withdrawal `FailedReminted`

Triggered by webhook `status=failed_reminted`. **This is a success outcome.**
The original withdrawal failed on Solana; the channel-side remint restored
the user's burned private channel tokens.

## Symptom

- Webhook payload: `status=failed_reminted`, `remint_signature=<sig>`,
  `remint_status=success`.
- ERROR log line `Transaction <id> FailedReminted` (the writer logs every
  alertable status at ERROR for paging visibility - this one is benign).

No funds are stranded. No recovery action needed.

**Exception:** if `error_message` contains `remint landed but the row is no
longer pending_remint`, another writer moved the row while this remint
landed, so the row's status does not show the refund. The user is refunded
by `remint_signature`. Check the row's status, then:

- Cancel any out-of-band refund in progress for this `transaction_id`, and
  record the remint signature in the incident record.
- If the row is `manual_review`, mark it reminted with the Triage SQL in
  [`withdrawal_manual_review.md`](withdrawal_manual_review.md).
- If the row is `completed`, the release landed too:
  [escalate](_escalation.md) (Tier 1).

## Expected cause: escrow whitelist

While withdrawals are whitelisted, the escrow refuses a release whose withdrawer or destination is not allowed to withdraw, with `custom program error: 0x14` (`WalletNotAllowed`). `Some(WalletNotAllowed)` proves it: the operator traced it to the escrow. A bare `0x14` or `recovered from persistent storage` does not, since Token-2022 and hooks can return `0x14` too. For those, confirm that withdrawals are whitelisted, that the withdrawer's or destination's AllowedWallet PDA is missing or has `withdrawals_blocked` set, and that the history (`getSignaturesForAddress`) of every admin the instance has had since the row's `created_at` shows neither changed since then. If so, reconcile as usual; repeats need no ticket. Otherwise ticket it. With the gateway check on, a whitelist refusal that still reaches a remint means the check could not run (look for `Withdraw whitelist not checked` in the gateway log), or the wallet was removed after it sent.

`custom program error: 0x15` or `Some(InvalidAllowedWalletPda)` is never an expected refusal. It most likely means an operator bug that passed the wrong account: [escalate](_escalation.md) (Tier 3).

## Reconciliation steps

1. **Confirm the remint signature on-chain.**
   ```bash
   solana confirm -v <remint_signature> --url <rpc-url>
   ```
   Expected: `Finalized` with no error. The remint targets the private channel side
   token program, so use the channel read node's RPC.
2. **Confirm the original withdrawal did NOT land** by running
   [`_verify_onchain_release.md`](_verify_onchain_release.md). Expected
   verdict: `NOT_LANDED`. If `LANDED`, the user has been double-credited
   (got funds on Solana AND a remint) -
   [escalate immediately](_escalation.md) (Tier 1).
3. **Close the alert** in your tracker. Note the remint signature.

## When to investigate further

- If `error_message` indicates a transient cause (RPC timeout, blockhash
  expiry) the original withdrawal may have been recoverable without remint.
  Repeated occurrences suggest the retry/backoff config or the
  `RetryPolicy::None` decision in `sender/remint.rs::attempt_remint` needs
  revisiting.
- If multiple `failed_reminted` events fire within a short window, file a
  ticket - the underlying RPC or program is unstable. Whitelist refusals
  (above) are the exception.

## Post-incident

Capture: transaction_id, original `error_message`, remint_signature, and the
on-chain verdict for both. These feed any reconciliation reports.
