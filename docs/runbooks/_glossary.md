# Glossary - Operator Status & Alert Surface

Reference for every other runbook in this directory. Not standalone.

Covers both withdrawal and deposit operators. Behavioral differences
between the two are called out inline.

## Status state machine

Defined in `indexer/src/storage/common/models.rs`. Enum `TransactionStatus`,
DB type `transaction_status`.

| Status | Terminal? | Webhook? | Meaning |
|---|---|---|---|
| `pending` | no | no | Inserted by indexer, not yet picked up by operator. |
| `processing` | no | no | Fetcher locked it; processor or sender is acting on it. |
| `pending_remint` | no | no | **Withdrawal-only.** Failed but signatures were stashed; finality check queued. Recovery query (`get_pending_remint_transactions`) re-loads these on restart. Deposits never enter this state. |
| `completed` | yes | no | Withdrawal release or deposit mint confirmed on-chain. |
| `failed` | yes | yes | Terminal failure with no on-chain proof. **Primary alert for deposits** (sender-side failures terminate here since there is no remint path). Rare for withdrawals - those go through `pending_remint`. |
| `failed_reminted` | yes | yes | **Withdrawal-only.** Original withdrawal failed, remint of burned private channel tokens succeeded. Deposits do not have a remint path. |
| `manual_review` | yes | yes | Operator stopped acting on this row. Requires human triage. Withdrawals: six triggers (build error → halt, pre-flight bail → no halt, four sender-side ambiguities). Deposits: build error only (no halt, no sweep). |

Webhook receivers should treat `failed`, `failed_reminted`, `manual_review` as
the alertable set. Source: `indexer/src/operator/db_transaction_writer.rs`,
the `is_alertable` match.

## Webhook payload shape

```json
{
  "transaction_id": 123,
  "trace_id": "uuid",
  "status": "manual_review" | "failed" | "failed_reminted",
  "counterpart_signature": "<sig>" | null,
  "error_message": "<string>" | null,
  "processed_at": "<rfc3339>",
  "timestamp": "<rfc3339>",
  "remint_signature": "<sig>" | null,
  "remint_status": "success" | "failed" | null
}
```

Webhook config: 10s timeout, **single attempt, no retries**
(`db_transaction_writer.rs`). A dropped webhook means a missed alert. The
ERROR-level log line `Transaction <id> <Status>` always fires, so logs are the
backup.

## Pipeline-halt asymmetry

Withdrawals halt the entire pipeline on a deterministic per-row error
(`processor.rs::halt_withdrawal_pipeline`). The reason is on-chain: a
quarantined withdrawal would leave a permanent gap in the SMT that
rejects every subsequent nonce. Halt + sweep is safer than bleeding
errors downstream.

Deposits never halt. The deposit loop (`process_deposit_funds`)
continues after each quarantine. There is no SMT, no nonce, no
sequential dependency between deposits.

This is why the withdrawal runbooks have a dedicated halt runbook and
the deposit ones do not.

## Withdrawal nonce and SMT

- Each withdrawal row has `withdrawal_nonce: BIGINT NOT NULL`.
- The on-chain SMT has `MAX_TREE_LEAVES` slots (see
  `indexer/src/operator/tree_constants.rs`). Leaf position = `nonce % MAX_TREE_LEAVES`.
- A quarantined withdrawal occupies its leaf logically (the next nonce
  expects sequential progression). The pipeline halts because subsequent
  nonces would fail at the program until the tree is rotated.
- Rotation: `ResetSmtRootBuilder` (escrow program). Triggered automatically
  when a nonce hits the `MAX_TREE_LEAVES` boundary; no admin CLI entrypoint
  exists today.

## On-chain references

- Escrow program ID: see `versions.env` and `core/` config.
- Operator account: signer for `release_funds`. Recent signature history is
  the authoritative source for "did this withdrawal land?" - see
  `_verify_onchain_release.md`.

## Idempotency memo (deposit-side)

Every deposit mint carries a deterministic memo:
`private_channel:mint-idempotency:<transaction_id>`
(`indexer/src/operator/constants.rs::MINT_IDEMPOTENCY_MEMO_PREFIX`).
Before sending, the operator scans the recipient ATA's recent signatures
on the private channel chain (`find_existing_mint_signature_with_memo`) and
short-circuits to `Completed` if a memo'd signature is already
finalized.

This is the primary fence against double-minting on retry. It works only
within the RPC's signature lookback window - older history is invisible
to the scan, which is why the verify-on-chain procedure escalates as
`AMBIGUOUS` when `processed_at` predates the window.

Withdrawals have an analogous fence: the `pending_remint` recovery
checks finality of stashed signatures before reminting.

## Roles in this directory

- `_glossary.md` - this file. Reference, no actions.
- `_verify_onchain_release.md` - withdrawal-side verification: did a
  release land on Solana?
- `_verify_onchain_mint.md` - deposit-side verification: did a mint
  land on the private channel?
- `_escalation.md` - escalation tiers and contacts. Every "escalate"
  reference in the recovery runbooks links here.
- `withdrawal_manual_review.md` - withdrawal manual review, dispatches
  by trigger site.
- `withdrawal_failed.md` - narrow runbook for the rare withdrawal
  `Failed`.
- `withdrawal_failed_reminted.md` - withdrawal reconciliation only; not
  a recovery.
- `deposit_manual_review.md` - deposit manual review (build error
  only).
- `deposit_failed.md` - primary deposit alert runbook.
- `README.md` - alert-to-runbook dispatch.
