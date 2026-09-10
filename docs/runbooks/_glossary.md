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
| `manual_review` | conditional | yes | Operator stopped acting on this row. Requires human triage. Withdrawals: seven triggers (build error → halt, allowlist gate → no halt, pre-flight bail → no halt, four sender-side ambiguities). Deposits: build error, sender-side post-JIT mint failure, or processor-side allowlist-gate rejection (no halt, no sweep). **Conditionally terminal:** a withdrawal row that carries release signatures in `remint_signatures` is re-checked every recovery tick and at boot; if those signatures are proven finalized on-chain it self-clears to `completed`. Every other `manual_review` row, including all deposits, is terminal and needs a human. |

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
quarantined withdrawal leaves a permanent hole in the nonce sequence,
and the row is unreleasable once its generation rotates away. Halt +
sweep is safer than bleeding errors downstream.

The sweep is bounded below by the poison row's `withdrawal_nonce`. Active
withdrawals with a lower nonce are left alone for the recovery worker; see
`withdrawal_manual_review.md` § Path A.

Deposits never halt. The deposit loop (`process_deposit_funds`)
continues after each quarantine. There is no nonce and no sequential
dependency between deposits.

This is why the withdrawal runbooks have a dedicated halt runbook and
the deposit ones do not.

## Withdrawal nonce and the bitmap

- Each withdrawal row has `withdrawal_nonce: BIGINT NOT NULL`.
- The escrow instance owns a withdrawal bitmap PDA holding one bit per nonce
  in the current generation. `NONCES_PER_GENERATION` (see
  `indexer/src/operator/constants.rs`) sets the window; bit position =
  `nonce % NONCES_PER_GENERATION`.
- A set bit is the authoritative answer to "did this nonce release?". The
  program refuses a second release of the same nonce with `NonceAlreadyUsed`.
- Generation: `nonce / NONCES_PER_GENERATION`. A nonce outside the bitmap's
  current generation is refused with `NonceOutsideCurrentGeneration`.
- Rotation: `RotateBitmapBuilder` (escrow program) clears every bit and
  advances the generation. The sender arms one on a timer, whenever the lowest
  withdrawal nonce that still owes a release belongs to a later generation than
  the bitmap is on; no admin CLI entrypoint exists today. Nonces from a
  rotated-past generation can never be released.
- A withdrawal that is not yet terminal and sits **inside the generation the
  bitmap is currently on** holds the rotation back, `manual_review` included, so
  an unresolved row stalls every withdrawal in later generations. A row from an
  already-rotated-past generation does not: its window shut and no rotation
  reopens it.
  `private_channel_operator_transaction_errors_total{error_reason="rotation_blocked_by_lower_nonce"}`
  counts a block that has persisted for five minutes, not an ordinary boundary
  crossing, and the log names the blocking nonce.

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
