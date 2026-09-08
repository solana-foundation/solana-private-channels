# Must-fix round 2 — verification of `chore/ottersec-merge`

Round scope: the ten open findings in the high-effort review of `origin/main...HEAD`
(`MUST_FIX.md`), fixed in eight commits on top of `backup/pre-mustfix-2026-09-08`.
Finding 0 in that document was already closed before this round (`6231016`) and is
carried here for the audit trail.

Nothing was blocked. No step wrote a `*-BLOCKED.md` file.

---

## 1. Verification gates

| Gate | Result |
|---|---|
| `cargo build --workspace --all-targets` | pass |
| `cargo fmt --all --check` | pass, no diff |
| `cargo clippy --workspace --all-targets -- -D warnings` | pass, no warnings |

### Test results, per crate

| Crate | Target | Passed | Failed | Ignored |
|---|---|---|---|---|
| `private-channel-core` | lib | 656 | 0 | 1 |
| | `bin/node` | 1 | 0 | 0 |
| | `tests/db_tests.rs` | 37 | 0 | 0 |
| | **total** | **694** | **0** | **1** |
| `private-channel-indexer` | lib | 942 | 0 | 0 |
| | `tests/postgres_db_test.rs` | 70 | 0 | 0 |
| | `tests/reconciliation_db_test.rs` | 9 | 0 | 0 |
| | `tests/remint_e2e_test.rs` | 9 | 0 | 0 |
| | `tests/runbook_drills.rs` | 0 | 0 | 18 |
| | doc-tests | 0 | 0 | 1 |
| | **total** | **1030** | **0** | **19** |
| `private-channel-gateway` | lib | 61 | 0 | 0 |
| | `tests/auth_integration.rs` | 39 | 0 | 0 |
| | **total** | **100** | **0** | **0** |
| `auth` (package `auth`, lib `private_channel_auth`) | lib | 27 | 0 | 0 |
| | `bin/admin` | 13 | 0 | 0 |
| | `tests/integration.rs` | 31 | 0 | 0 |
| | **total** | **71** | **0** | **0** |

Grand total: **1895 passed, 0 failed, 20 ignored.** No failure needed a fix-up
commit, so the round is the eight fix commits plus this document.

The 18 ignored `runbook_drills` are the pre-existing `#[ignore]` set described in
`MUST_FIX.md`'s ruled-out list; three of them fail against `main`, `ours` and the
merge base identically and are not this merge's to fix.

---

## 2. The ten findings

Every row was re-checked at the cited file after the fix, and the named test was
confirmed to exist and to be exercised by the green run above.

| # | Finding | Fixed | Commit | Test that pins it |
|---|---|---|---|---|
| 0 | Redaction dead on ungated `getSignatureStatuses` | yes, before this round | `6231016` | `test_get_signature_statuses_anonymous_gets_uniform_errors` (`gateway/tests/auth_integration.rs`) |
| 1 | Both poll paths routed on `confirmed()` | yes | `ee2de92` | `poll_in_flight_confirmed_not_finalized_does_not_settle`, `run_poll_task_confirmed_not_finalized_does_not_settle`, `poll_in_flight_finalized_tx_emits_completed` |
| 2 | `attempt_remint` confirmed at `confirmed()` | yes | `ee2de92` | `execute_deferred_remint_confirmed_not_finalized_does_not_terminalize`, `execute_deferred_remint_finalized_terminalizes_and_clears_the_journal` |
| 3 | `Dead` + no instance re-armed uncorroborated | yes | `f34b772` | `check_withdrawal_dead_signature_quarantines_when_no_instance_configured` |
| 4 | Boot promotion had no remint-claim interlock | yes | `c324bd5` | `reconcile_landed_leaves_a_row_with_an_outstanding_remint_claim` |
| 5 | Halt sweep quarantined without the release journal | yes | `e3529d8` | `quarantine_active_withdrawals_mirrors_release_journal`, `quarantine_active_withdrawals_keeps_existing_signatures_when_journal_empty` |
| 6 | `drop(tx.permit)` missing from the confirmed arm | yes | `fd3604f` | `jit_mint_retry_reuses_slot_of_finalized_entry_when_saturated` |
| 7 | Redaction decided from the raw JWT | yes | `e1a3e9d` | `test_demoted_operator_loses_raw_errors` |
| 8 | `MockStorage` kept the removed nonce frontier | yes | `f730740` | `get_and_lock_withdrawals_ignores_lower_active_nonces_and_orders_by_created_at` (mock side); `withdrawal_dequeue_ignores_lower_active_nonces` in `postgres_db_test.rs` (SQL side, from `786e527`) |
| 9 | Mock `set_pending_remint` wrote the flag to the wrong copy | yes | `f730740` | `set_pending_remint_refusal_is_visible_by_nonce` |
| 10 | `parse_create_instance` not re-indexed for 8 accounts | yes | `12abf5c` | `test_create_instance_maps_bitmap_era_account_layout` |

None of the ten is left open.

### Site-level confirmation

- **1** — `indexer/src/operator/sender/transaction.rs:2060` and `:2356` both read
  `status.satisfies_commitment(CommitmentConfig::finalized())`. The only remaining
  `confirmed()` in a routing position is `transaction.rs:667`, which is `confirmed()`
  on `origin/main` too (line 845 there); it is not a lost fix and was left alone.
- **2** — `remint.rs:237` passes `CommitmentConfig::finalized()`, matching
  `classify_endpoint` at `:656` and `:691`.
- **3** — `recovery.rs:701` returns `Quarantine` for `instance_pda.is_none()`,
  worded like the `pending.is_empty()` branch above it.
- **4** — `get_stalled_withdrawals_with_signatures_internal` adds
  `landed_remint_signature IS NULL` and `NOT EXISTS (SELECT 1 FROM
  pending_remint_signatures …)`; the mock mirrors both.
- **5** — `quarantine_active_withdrawals_internal` now COALESCEs
  `remint_signatures` and `remint_last_valid_block_heights` from
  `pending_release_signatures` inside the same UPDATE.
- **6** — `drop(tx.permit);` is the first statement of the finalized arm at
  `transaction.rs:2061`-`2063`.
- **7** — `enforce_auth` calls `redacts_transaction_errors_for(claims.as_ref(), method)`;
  the header-parsing form survives only on the ungated branch, which has no DB
  check to consult.
- **8** — `mock.rs:212`-`238` filters on `status == Pending`, sorts by `created_at`,
  and computes no barrier.
- **9** — `mock.rs:643` sets `release_refused_on_chain` on the
  `pending_transactions` row and derives the rehydration copy from it at `:647`.
- **10** — `escrow.rs:322`-`338` guards on `CREATE_INSTANCE_ACCOUNTS` (8) and maps
  4 → `withdrawal_bitmap`, 5 → `system_program`, 6 → `event_authority`,
  7 → `private_channel_escrow_program`.

---

## 3. Merge-plan resolution gates, section 7.1

Re-run at HEAD. Every gate holds.

| Gate | Result |
|---|---|
| No hits for `ResetSmtRoot`, `MAX_TREE_LEAVES`, `smt_util`, `tree_index`, `smt_root`, `SparseMerkle`, `has_active_withdrawal_below`, `lowest_unreleased_withdrawal_below`, `quarantine_all_active_withdrawals` | pass, 0 hits each |
| No hits for `owed_rotation_target` | pass in substance: the only three hits are `postgres_db_test.rs:2540`-`2550`, the schema test that asserts the column is *absent*. No production reference. |
| No `load_accounts` in `core/src` | pass; all 18 hits are the substring inside `preload_accounts` |
| One definition each of `EXECUTOR_PRELOAD_FATAL`, `EXECUTOR_CORRUPT_ACCOUNT` | pass, both defined once in `core/src/stage_metrics.rs` (`:358`, `:364`) |
| `get_and_lock_pending_transactions_internal` has `SKIP LOCKED` and no lower-nonce predicate | pass: `WHERE status = $1 AND transaction_type = $2 ORDER BY created_at ASC LIMIT $3 FOR UPDATE SKIP LOCKED` |
| `trip_halt` calls `quarantine_active_withdrawals` | pass, `reconciliation.rs:430` |
| Every `run_startup_reconciliation` call site passes the channel RPC handle; the surplus test mocks a healthy supply | pass; the single production call site is `indexer.rs:122`, and `test_classify_surplus_never_blocks` holds |
| `write_slot` takes the observed releases and returns `Err` on their failure; no `send_checkpoint` flag | pass, `transaction_processor.rs:353` and `:412`-`:437`; zero hits for `send_checkpoint` |
| Every `get_signature_statuses` call site checks response length against the chunk | pass: `fetch_statuses_checked` (`transaction.rs:2227`) and `classify_endpoint` (`remint.rs:645`) both compare lengths; `transaction_util.rs:135` is a single-signature call whose `.first()` on a short response yields `None` and re-polls |
| No catch-all builder-error arm terminalizes a deposit; `SendDurability` exists | pass, `sender/types.rs:95`, routed at `transaction.rs:209`-`212` |
| `source_event_id` present in the mint path | pass, `sender/mint.rs`, `sender/remint.rs`, `utils/instruction_util.rs` |
| Every `Storage` method resolves in `mock.rs` and `db.rs`; no orphan `pub mod` | pass, 57 `pub mod` declarations, 57 files, no orphan either way |
| Escrow `declare_id!` matches `ESCROW_PROGRAM_ID`; no `GokvZqD2` | pass, both `9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU`; 0 hits |
| `cargo build --workspace --all-targets` and clippy `-D warnings` clean | pass |

**No SMT symbol survives anywhere in the tree, and the Postgres dequeue has no
reintroduced nonce frontier.**

---

## 4. New findings from this round

### N1. FIXED — mock `gc_stale_remint_signatures` read the rehydration list, not the live row

**File:** `indexer/src/storage/common/storage/mock.rs:1353`-`1371` (fixed in `c324bd5`)
**Class:** correctness of the test double — same class as findings 8 and 9.

`gc_stale_remint_signatures` built its keep-set from `pending_remint_transactions`
alone. Postgres reads one table, so a row whose live status has moved on is
non-pending and its claim is deleted; the mock still saw the stale rehydration
copy as `PendingRemint` and kept the claim.

**Failure scenario.** Any test that promotes a row out of `PendingRemint` and then
asserts the claim record was GC'd passes vacuously — the mock keeps the claim for
the wrong reason. This surfaced while writing finding 4's test, which asserts the
claim *survives*: it would have been green against a broken fix.

**Fix applied.** Resolve status from `pending_transactions` first, falling back to
the rehydration list only for ids the live mirror does not hold.

### N2. OPEN — the two SQL changes from findings 4 and 5 have no Postgres-level test

**File:** `indexer/tests/postgres_db_test.rs:1843` (the
`get_stalled_withdrawals_with_signatures` case) and, for the halt sweep, nowhere.
**Class:** test coverage — the mock is the sole assertion of a contract that only
exists in raw SQL.

Finding 4 added `landed_remint_signature IS NULL` and a `NOT EXISTS` subquery
against `pending_remint_signatures` to
`get_stalled_withdrawals_with_signatures_internal`. Finding 5 added two COALESCE
subqueries over `pending_release_signatures` to
`quarantine_active_withdrawals_internal`. Both are tested only through
`MockStorage`. The existing Postgres test at `:1843` seeds the unclassifiable-row
cases but never a claimed or landed refund, so it passes with or without the new
predicates. `quarantine_active_withdrawals` has no `postgres_db_test.rs` case at
all; the only SQL-level exercise is `drill_5_halt_sweep_excludes_poison_only`,
which is `#[ignore]`d and, per `MUST_FIX.md`, wired into no CI target.

**Failure scenario.** Someone edits either statement — a column rename, a join
that silently drops rows, a `NOT EXISTS` that binds the wrong id — and the whole
suite stays green, because the mock is hand-written to the intended behaviour
rather than derived from the SQL. That is exactly the divergence findings 8 and 9
were about, now reintroduced one level down. The consequence in production is the
double payout of finding 4 or the unrepairable row of finding 5.

**Suggested fix.** Two cases in `indexer/tests/postgres_db_test.rs`: extend the
existing stalled-withdrawal test with a row carrying a `pending_remint_signatures`
claim and a row with a non-NULL `landed_remint_signature`, asserting neither is
returned; and add a halt-sweep test that seeds `pending_release_signatures` for a
`Processing` withdrawal, calls `quarantine_active_withdrawals`, and asserts the
row comes back from `get_stalled_withdrawals_with_signatures`.

---

## 5. Deliberately not changed

- **`transaction.rs:667` still uses `CommitmentConfig::confirmed()`.** This is the
  synchronous send-and-confirm path, and it is `confirmed()` on `origin/main` as
  well (`:845` there). It is not a lost Cantina fix, so raising it is a behaviour
  change outside this round's scope.
- **The dequeue nonce frontier stays out of `db.rs`.** Plan D5. Finding 8 aligns
  the *mock* with that decision; it does not revisit it.
- **A custody surplus still does not block startup.** Plan D6, OtterSec Off-chain
  #5. Ruled out by the review as deliberate.
- **The three failing runbook drills** (`drill_1`, `drill_15`, `drill_17`) are
  untouched. Each asserted string is absent, or each violation present,
  identically at `977327b9`, `d91ed1e0` and `origin/main`; they predate the
  divergence and this merge neither caused nor can fix them.
- **`rotation_driver_e2e` is still wired into no Makefile or CI target**, and the
  `tree-rotation-alerts` replacement rules are still missing. Both are follow-up
  tickets the merge plan puts out of scope.
- **Everything else in `MUST_FIX.md`'s "reviewed and ruled out" list** — the
  missing `transactions(withdrawal_nonce)` index, the boot reconcile's startup
  budget, the "Completed status write LOST" log noise, `hold_queued_release`
  ignoring `Ok(false)`, the `release_leases` leak, `cleanup_mint_builder`'s key
  mismatch, the stale doc comments, mock `get_withdrawal_by_nonce` ordering, and
  the dead `transactions.release_signatures` column — is unchanged. Each is style,
  performance, documentation, or an outcome the plan chose.
- **N2 above is left open rather than fixed here.** Writing the two Postgres cases
  is new test work, not a repair of a finding this round was scoped to close, and
  it belongs in a commit of its own so its failure-before-fix can be demonstrated
  against the pre-`c324bd5`/`e3529d8` SQL.

---

## 6. Commits in this round

```
12abf5c fix(indexer): re-index parse_create_instance for the 8-account layout (must-fix 10)
e1a3e9d fix(gateway): derive transaction-error redaction from the DB-resolved role (must-fix 7)
fd3604f fix(indexer): free the finalized entry's in-flight permit before the JIT retry (must-fix 6)
e3529d8 fix(indexer): mirror release-signature columns in the halt sweep update (must-fix 5)
c324bd5 fix(indexer): exclude claimed or landed refunds from boot promotion (must-fix 4)
f34b772 fix(indexer): quarantine dead-signature withdrawals with no configured instance (must-fix 3)
ee2de92 fix(indexer): route confirmation polls and remint confirmation on finalized (must-fix 1, 2)
f730740 fix(indexer): align MockStorage dequeue and remint refusal flag with Postgres (must-fix 8, 9)
```
