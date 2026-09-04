# Runbooks - Operator

Operator runbooks for the Solana Private Channels payment-channel system. Covers both the
withdrawal operator (private channel → Solana releases) and the deposit / escrow
operator (Solana → private channel mints). Start here when an alert fires.

The two operators have different failure shapes: withdrawals can halt
the pipeline (SMT nonce gap), deposits cannot. The dispatch table below
routes by webhook + `transaction_type`.

> **Six conditions are not webhook-routed.** The indexer's
> **`block_unavailable`** wedge pages through Grafana instead, because it changes
> no transaction row; see
> [`indexer_block_unavailable.md`](indexer_block_unavailable.md).
>
> **A second condition pages through Grafana, not the webhook.** The
> **`sender-lock-lost`** alert fires when a sender cannot prove it still owns
> its Postgres advisory lock and shuts the whole operator down to stop two
> senders running at once. It changes no transaction row, so it is not in the
> dispatch table; see
> [`sender_lock_lost_runbook.md`](sender_lock_lost_runbook.md).
>
> **Two halts have no dedicated alert.** The **SMT-root-mismatch boot
> pre-flight** fires no "pipeline halted" event and marks no row `failed`.
> The common cause is auto-reconciled at boot; an unforeseen divergence the
> reconcile cannot resolve makes the operator **refuse to start**, surfacing
> as a boot-time crash-loop with `SMT root mismatch` in the operator logs.
> Recognize it by that pattern, not a single alert, and not via this
> dispatch table. See
> [`withdrawal_pipeline_halt_runbook.md`](withdrawal_pipeline_halt_runbook.md).
> The **`StartSlotAheadOfCheckpoint`** startup refusal is the other: a
> configured start slot sits above the durable checkpoint, so booting would
> skip slots nothing would ever go back for. It also shows as a boot-time
> crash-loop, recognized by that marker in the indexer logs; see
> [`indexer_start_slot_ahead_of_checkpoint.md`](indexer_start_slot_ahead_of_checkpoint.md).
>
> **One node condition crash-loops instead of alerting.** A row in the node's
> `accounts` table that will not deserialize makes the executor refuse to run any
> batch touching it, so the node exits and is restarted in a loop. It marks no
> transaction row, and is detected from
> `private_channel_executor_corrupt_account_total` plus the pubkey in the
> executor log; see [`corrupt_account_row.md`](corrupt_account_row.md).
>
> **One node condition refuses to start and takes writes down with it.** A write
> or aio node that cannot take the Postgres writer lease exits at startup, and
> the gateway has only one write URL, so no transaction is accepted while it is
> down. The lock is normally freed the instant the old node's socket closes; a
> host that vanishes without closing it is the case that sticks. Recognize it
> from the boot loop plus `already holds the writer lease` in the node log; see
> [`writer_lease_unavailable.md`](writer_lease_unavailable.md).

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
- [`indexer_block_unavailable.md`](indexer_block_unavailable.md) - the indexer
  refusing to checkpoint past a slot whose block the RPC endpoint will not serve.
  Paged by the `indexer-block-unavailable` Grafana alert, not by the webhook
  dispatch table above (no transaction row changes status).

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
| `drill_2_path_a_data_error_recovery` | withdrawal | Triage SQL orders the trigger row first; recovery SQL reaches the documented end-state, and `id <> ALL(:excluded_ids)` leaves held rows quarantined. |
| `drill_3_path_b_landed_marks_completed_with_signature` | withdrawal | On `LANDED`, mark `completed` with the observed signature (prevents double-credit). |
| `drill_4_path_c_not_landed_recovery_flows` | withdrawal | `withdrawal_manual_review.md` § Path C Step 3: burned branch re-arms preserving `withdrawal_nonce` (nonce-uniqueness index still enforces); not-burned branch terminalizes the row and never returns it to the fetcher's pending queue. |
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
