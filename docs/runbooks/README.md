# Runbooks - Operator

Operator runbooks for the Solana Private Channels payment-channel system. Covers both the
withdrawal operator (private channel → Solana releases) and the deposit / escrow
operator (Solana → private channel mints). Start here when an alert fires.

The two operators have different failure shapes: withdrawals can halt
the pipeline (SMT nonce gap), deposits cannot. The dispatch table below
routes by webhook + `transaction_type`.

> **One halt has no dedicated alert.** The **SMT-root-mismatch startup
> halt** fires no "pipeline halted" event - it surfaces as repeated
> per-row `failed` webhooks whose `error_message` carries `SMT root
> mismatch` (plus `SMT root mismatch detected` in the operator logs).
> Recognize it by that pattern, not a single alert, and not via this
> dispatch table. See
> [`withdrawal_pipeline_halt_runbook.md`](withdrawal_pipeline_halt_runbook.md).

## Alert dispatch

The **alert webhook** in `db_transaction_writer.rs` is the only
configured paging mechanism today. It fires on `Failed`,
`FailedReminted`, and `ManualReview` status transitions (single attempt,
no retries). All dispatch below is keyed on the webhook payload.

| Alert (webhook payload) | `transaction_type` | Symptom | Runbook |
|---|---|---|---|
| `status=manual_review` | `withdrawal` | Single row stopped; pipeline may also be halted. | [`withdrawal_manual_review.md`](withdrawal_manual_review.md) |
| `status=manual_review` | `deposit` | Single row stopped — deterministic build error (processor), sender-side post-JIT mint failure (mint authority mismatch / corrupt state), recovery-worker idempotency lookup failure, or mint not in the `AllowMint` allowlist (processor-side gate). No halt, no collateral. | [`deposit_manual_review.md`](deposit_manual_review.md) |
| `status=failed` | `withdrawal` | Single row terminated without on-chain proof. Rare for withdrawals. | [`withdrawal_failed.md`](withdrawal_failed.md) |
| `status=failed` | `deposit` | **Primary deposit alert.** Sender-side terminal failure (RPC, build, confirmation, on-chain rejection). | [`deposit_failed.md`](deposit_failed.md) |
| `status=failed_reminted` | `withdrawal` | Withdrawal failed; remint succeeded. Reconcile only. | [`withdrawal_failed_reminted.md`](withdrawal_failed_reminted.md) |

## First action regardless of alert

1. Capture the alert payload (transaction_id, error_message, processed_at,
   `transaction_type`).
2. Run the on-chain verification procedure that matches the
   `transaction_type`:
   - Withdrawals → [`_verify_onchain_release.md`](_verify_onchain_release.md)
   - Deposits → [`_verify_onchain_mint.md`](_verify_onchain_mint.md)
3. Do not take recovery action until you have a verdict.

## Recovery SQL is bookkeeping; fund restoration is human-in-the-loop

A core design property of these runbooks: the recovery `UPDATE`
statements only change the operator's view of a row. They do not move
on-chain funds, mint, burn, or refund anything. That separation is
intentional - it lets a single human-readable command resolve the
operator's state without coupling it to any chain action that might
itself fail.

The system has exactly one **automatic** restoration path: a
withdrawal that fails sender-side on Solana auto-remints the user's
burned private channel tokens. That outcome ends with `status=failed_reminted`
and no human action.

Every other terminal outcome - withdrawal `failed` after a build error,
withdrawal where both the on-chain release and the auto-remint failed,
deposit `manual_review` with bad row data, deposit `failed` whose
underlying condition can't be remedied -
**routes to a human via Tier 1 escalation**. The recovery SQL marks
the operator's bookkeeping done; the actual fund restoration (manual
remint, compensating release, off-chain refund) is a separate step
the on-call operator coordinates with treasury and tracks in the
incident record.

When you run a recovery `UPDATE`, you are not "fixing the user." You
are making the operator's state consistent so the pipeline can
resume. The user-side fix lives in the Tier 1 escalation channel.
The runbooks call this out at every relevant site.

## Reference

- [`_glossary.md`](_glossary.md) - status state machine, webhook schema,
  metrics, withdrawal/deposit asymmetries.
- [`_verify_onchain_release.md`](_verify_onchain_release.md) - withdrawal
  on-chain check (Solana mainnet).
- [`_verify_onchain_mint.md`](_verify_onchain_mint.md) - deposit
  on-chain check (private channel chain).
- [`_escalation.md`](_escalation.md) - escalation tiers and contacts.
  Every "escalate" call-site in the recovery runbooks links here.
- [`withdrawal_pipeline_halt_runbook.md`](withdrawal_pipeline_halt_runbook.md) -
  the SMT-root-mismatch startup halt (log-discovered, not paged).

## Drills

[`indexer/tests/runbook_drills.rs`](../../indexer/tests/runbook_drills.rs)
contains seventeen `#[ignore]`-flagged drills that verify these runbooks'
commands actually do what the prose claims. Drills are **manually
triggered, not in CI** - they exist so a human about to use a runbook
(or about to publish an edit) can confirm the diagnostic and recovery
flows still work. Each drill prints the runbook section it verifies and
pins the relevant contract.

| Drill | Side | Verifies |
|---|---|---|
| `drill_1_error_message_contracts_present_in_source` | both | Source contains every `error_message` substring the dispatch tables match on. |
| `drill_2_path_a_data_error_recovery` | withdrawal | Triage SQL orders the trigger row first; recovery SQL reaches the documented end-state. |
| `drill_3_path_b_landed_marks_completed_with_signature` | withdrawal | On `LANDED`, mark `completed` with the observed signature (prevents double-credit). |
| `drill_4_path_c_not_landed_re_arms_with_same_nonce` | withdrawal | Re-arm preserves `withdrawal_nonce`; nonce-uniqueness index still enforces. |
| `drill_5_halt_sweep_excludes_poison_only` | withdrawal | Bulk-quarantine flips every active withdrawal except the excluded poison id. |
| `drill_6_recovery_query_skips_terminal_statuses` | withdrawal | Recovery query skips rows already resolved to a terminal status. |
| `drill_7_halt_sweep_does_not_touch_terminals` | withdrawal | Bulk-quarantine leaves terminal-status rows alone. |
| `drill_8_alertable_set_matches_runbook_dispatch` | both | Webhook fires on exactly `Failed`, `FailedReminted`, `ManualReview`. |
| `drill_9_path_b_signature_uniqueness_fence` | withdrawal | Mark-completed-with-sig is idempotent; unique index rejects cross-row collision. |
| `drill_10_deposit_failed_recovery_flows` | deposit | `LANDED` → completed-with-sig; `NOT_LANDED` → re-arm; bad data → failed. |
| `drill_11_program_type_labels_match_runbooks` | both | Pins `ProgramType::as_label` to `withdraw` / `escrow`. |
| `drill_12_withdrawal_failed_recovery_flows` | withdrawal | `withdrawal_failed.md` LANDED → completed-with-sig; cross-row signature fence still applies on `failed`; NOT_LANDED is terminal (markdown + operator code grep); AMBIGUOUS escalates without SQL. |
| `drill_13_withdrawal_failed_reminted_reconcile` | withdrawal | `failed_reminted` transition writes `remint_signatures`; runbook contains zero mutating SQL; LANDED verdict cannot be silently absorbed via `SET status='completed'`; webhook `remint_signature` (singular) ↔ DB `remint_signatures` (plural) asymmetry pinned. |
| `drill_14_deposit_manual_review_post_jit_recovery_flows` | deposit | `deposit_manual_review.md` § Path D: post-JIT trigger strings present in `mint.rs`; re-arm SQL flips `manual_review` → `pending` and is targeted by id (not error_message); idempotency memo prefix anchored. |
| `drill_15_deposit_manual_review_recovery_idempotency_failure_flow` | deposit | `deposit_manual_review.md` § Path E: recovery-worker `deposit idempotency:` triage substring present in `recovery.rs`; re-arm SQL flips `manual_review` → `pending` and is row-scoped by id. |
| `drill_16_withdrawal_manual_review_recovery_missing_nonce_flow` | withdrawal | `withdrawal_manual_review.md` § Path F: recovery-worker `withdrawal row missing nonce` triage substring present in `recovery.rs`; recovery branch SQL is row-scoped; no re-arm SQL exists for this path. |
| `drill_17_deposit_manual_review_allowlist_gate_recovery_flows` | deposit | Allowlist-gate recovery flow in `deposit_manual_review.md` is in sync with source: triage strings still exist and recovery SQL is row-scoped. |

Trigger (`make` shorthand, runs from repo root):

```bash
make drills                  # all drills
make drill NAME=drill_2      # single drill (substring match)
```

Or directly via cargo:

```bash
cargo test -p private-channel-indexer --test runbook_drills -- --ignored --nocapture

# Single drill, with trace logs for debugging:
RUST_LOG=trace cargo test -p private-channel-indexer --test runbook_drills -- \
    --ignored --nocapture drill_2
```

### When to run drills

- Before merging a runbook edit.
- After changes to: `processor.rs`, `sender/transaction.rs` (and in
  particular `send_fatal_error` — drill_12; or the
  `JitOutcome::ManualReview` caller-arm dispatch which emits the
  `ManualReview` status update inline — drill_14), `sender/mint.rs`
  (the `JitOutcome::ManualReview` reason strings live here — drill_14
  specifically), `sender/remint.rs`, `db_transaction_writer.rs`
  (including its webhook-payload serializer — drill_13 anchors on the
  `"remint_signature"` JSON key string literal), or the indexer
  schema.
