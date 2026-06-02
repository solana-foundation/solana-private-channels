# Runbook - Deposit `ManualReview`

Triggered by webhook payload `status=manual_review` for a row with
`transaction_type='deposit'`.

## Scope and key differences vs. withdrawals

Deposits never halt the pipeline. The processor's classifier
(`processor.rs::classify_processor_error`) is shared with the withdrawal
side, but the deposit loop continues after each quarantine
(`process_deposit_funds`, `processor.rs:704-722` - note the absence of
`halt_withdrawal_pipeline` and the loop `continue` semantics). There is
no SMT, no nonce, no remint.

Practically: a single deposit `manual_review` is a single row in trouble.
Other deposits keep flowing. There is no collateral, no sweep, no halt to
recover from.

There are **three trigger surfaces** that can land a deposit in
`manual_review`:

1. **Processor-side row-data validation** — deterministic per-row error
   raised before any RPC call (Paths A / B / C below).
2. **Sender-side post-JIT mint failure** — the operator's just-in-time
   mint-init helper found the on-chain mint in a state it cannot fix by
   re-issuing `mint_to` (wrong authority on the private channel mint, or
   corrupt mint data). See **Path D** for recovery.
3. **Processor-side allowlist gate** — the deposit's `mint` has no row
   in the `mints` allowlist (`MintCache::assert_mint_allowlisted`). Row
   data is fine; no `MintTo` was attempted. See **Path E**.

## Symptom

- Webhook with `status=manual_review`, `transaction_type=deposit`.
- ERROR-level log line `Transaction <id> ManualReview`.

## Triage

`error_message` distinguishes the three trigger surfaces and dispatches
to the right Path below.

### Processor-side surface (`processor.rs::process_deposit_funds`)

| `error_message` contains | Cause |
|---|---|
| `invalid_pubkey` | `mint` or `recipient` field is not a valid base58 pubkey. |
| `invalid_builder` | Builder rejected the row's data (e.g. negative amount). |
| `program_error` | Generic builder error not covered by the specific variants. |

### Sender-side post-JIT surface (`sender/mint.rs`)

| `error_message` contains | Cause |
|---|---|
| `Mint instruction failed after JIT: mint_authority mismatch` | The on-chain mint's `mint_authority` is not the operator's admin pubkey. Two known triggers: (a) an existing mint on the private channel is owned by a different authority and `mint_to` produced one of the three classified `InstructionError` variants (rare — the most common rotated-admin case produces `OwnerMismatch` → `Custom(3)`, which routes to `Failed` instead of here); (b) post-`InitializeMint` race where another party initialized the same mint with a different authority during our send window. |
| `Mint instruction failed after JIT: corrupt mint state` | The mint's account data does not decode as a valid SPL Mint (`COption` discriminant invalid, length mismatched). Defensive — should not occur in normal operation; investigate before any recovery. |

### Processor-side allowlist surface (`processor.rs::process_deposit_funds`)

| `error_message` contains | Cause |
|---|---|
| `is not in the allow-listed mints table` | The deposit's `mint` has no row in `mints`. The operator refused to issue private channel tokens because no indexed `AllowMint` event authorizes this mint. Row data is fine; no on-chain mint attempted. See **Path E**. |

Pull the row:

```sql
SELECT id, signature, recipient, mint, amount, slot, updated_at
  FROM transactions
 WHERE id = :transaction_id;
```

`signature` is the originating Solana deposit signature (immutable
reference). Use it to inspect the on-chain deposit if you need to confirm
what was actually deposited:

```bash
solana confirm -v <signature> --url <solana-rpc-url>
```

## Recovery

Deposits do not need on-chain mint verification before recovery - the
quarantine triggers on row-data validation, before any RPC call. The
idempotency memo (`private_channel:mint-idempotency:<transaction_id>`) prevents
double-mint on retry even if the mint somehow did land.

That said: **if `error_message` is `program_error`** the trigger is less
specific and may indicate a real on-chain rejection. In that case run
[`_verify_onchain_mint.md`](_verify_onchain_mint.md) before deciding.

### Path A - bad data, unrecoverable

The row's `mint` or `recipient` is malformed beyond fixing (e.g. the
indexer captured corrupt input). Mark `failed`; refund out-of-band.

```sql
UPDATE transactions SET status = 'failed', updated_at = NOW()
 WHERE id = :transaction_id;
```

The user's tokens are locked in escrow on Solana but no private channel side
mint will be issued. [Escalate](_escalation.md) (Tier 1) for refund
coordination -
typically a manual `release_funds` back to the depositor.

### Path B - data correctable

Rare; happens when `mint` or `recipient` was canonically wrong but the
underlying intent is recoverable from the originating Solana transaction.
Correct the columns and re-arm:

```sql
UPDATE transactions
   SET status = 'pending',
       mint = :corrected_mint,
       recipient = :corrected_recipient,
       updated_at = NOW()
 WHERE id = :transaction_id;
```

No operator restart required. The fetcher will pick the row up on its
next tick.

### Path C - conservative classification

If `error_message` describes a transient condition (RPC error, DB error
surfaced as `OperatorError::Program`), the classifier in
`classify_processor_error` quarantined on the side of caution rather
than retrying. This is the intended behavior: misclassifying a
deterministic error as transient could put the operator into a tight
retry loop. The asymmetric cost favors a noisy quarantine over a silent
retry.

Re-arm to `pending` (safe - idempotency memo prevents duplicate mint),
then [escalate](_escalation.md) (Tier 3) so the taxonomy can be
extended to classify this error variant explicitly. Do not patch
in-place.

```sql
UPDATE transactions SET status = 'pending', updated_at = NOW()
 WHERE id = :transaction_id;
```

### Path D - sender-side post-JIT failure

Triggered by the sender-side surface above (`error_message` starting
with `Mint instruction failed after JIT:`). The operator's JIT helper
examined on-chain reality and found the mint structurally unusable.
Treat as a **configuration alarm**, not a transient.

**Why this differs from `deposit_failed`.** The post-JIT path landed
in `manual_review` (not `failed`) specifically because JIT's structural
check distinguished it from generic program errors — the on-chain mint
state needs human investigation before any retry can succeed.

#### Step 1 - verify on-chain

Run [`_verify_onchain_mint.md`](_verify_onchain_mint.md). Like Paths
A / B / C but mandatory here: structural mint problems can mask a
prior `mint_to` that landed.

#### Step 2 - branch on verdict

##### `LANDED <signature>` — mint already succeeded

Mark `completed` with the observed signature (same SQL as
[`deposit_failed.md`](deposit_failed.md) Path B). The unique partial
index on `counterpart_signature` still applies; if it rejects the
update, escalate before retrying.

##### `NOT_LANDED` — inspect mint authority on the private channel

```bash
spl-token display <mint> --url <private-channel-rpc>
```

Compare the displayed `Mint authority` to the operator's current
admin pubkey.

- **Authority mismatch — current authority still accessible.**
  Treasury / admin signs

  ```bash
  spl-token authorize <mint> mint-authority <new-authority>
  ```

  to point the mint at the current operator admin. Then re-arm to
  `pending` (SQL below). The idempotency memo prevents double-mint.
- **Authority mismatch — current authority lost** (e.g. old admin key
  destroyed during rotation). The mint is permanently unusable.
  **Escalate (Tier 1).** Recovery is a manual coordinated mint
  replacement: deploy a fresh mint, migrate any existing balances
  out-of-band, mark the deposit `failed`, and refund the depositor
  through the same Tier 1 channel `deposit_failed` Path A uses.
  **Do not re-arm to `pending`** — the next mint attempt will fail
  the same way.
- **`error_message` contains `corrupt mint state`.** Escalate (Tier
  2). Corrupt mint data on a deployed chain is a structural anomaly
  engineering must investigate before any recovery. Do not re-arm.

##### `AMBIGUOUS`

Stop. Escalate (Tier 2). Do not act.

#### Re-arm SQL

Only for the recoverable authority case after the underlying authority
correction has been performed:

```sql
UPDATE transactions SET status = 'pending', updated_at = NOW()
 WHERE id = :transaction_id;
```

### Path E - mint not in `AllowMint` allowlist

`error_message`: `is not in the allow-listed mints table`. The deposit's
mint has no row in `mints`; the operator refused to mint on the private
channel. **No `MintTo` was built** therefore `_verify_onchain_mint.md` does not
apply. Steps 1–2 diagnose *why* the allowlist row is missing; Steps 3a–3c
are the recovery branches.

#### Step 1 - check whether the gap closed on its own

A race is possible: the gate fired because `mints` was empty at process
time, but the indexer may have caught up since. Confirm the current state:

```sql
SELECT id, signature, mint, slot FROM transactions WHERE id = :transaction_id;
SELECT * FROM mints WHERE mint_address = :mint;
```

If the second query now returns a row, the gap closed on its own. Skip to
**Step 3a** re-arm — no backfill needed.

#### Step 2 - was this mint ever authorized on-chain?

If the gap is real, find out whether an `AllowMint` instruction was ever
emitted. `mints` is populated by the indexer from `AllowMint` on the
escrow program, scoped to the configured `escrow_instance_id`. Search
the operator-admin's mainnet signature history:

```bash
solana transaction-history <operator-admin-pubkey> \
  --url <solana-mainnet-rpc-url> --limit 1000
```

Decode candidates against the escrow program IDL. The verdict tells you
which recovery branch to take:

| Finding | Branch |
|---|---|
| Found, slot < deposit slot. | **3a — indexer gap.** |
| Found, slot ≥ deposit slot. | **Escalate (Tier 1).** Retroactive allowlist; treasury policy call. |
| Not found after a full pass. | **3b — terminal.** |
| Found but bound to a different `instance`. | **3c — Tier 3 defect.** |

#### Step 3a - backfill the missing `mints` row, then re-arm

The mint *is* authorized on-chain; the indexer simply missed the event.
Replay the indexer over the `AllowMint`'s slot (preferred — same code path
as production), or insert directly. The gate (`assert_mint_allowed_at_slot`)
reads **`mint_status_history`**, so backfilling only `mints` loops
`pending` → `manual_review` forever — both rows are required. Values must
match the on-chain mint and `AllowMint` flags:

```sql
INSERT INTO mints
  (mint_address, decimals, token_program, is_pausable, has_permanent_delegate, created_at)
VALUES
  (:mint, :decimals, :token_program, :is_pausable, :has_permanent_delegate, NOW());

-- Clears the slot-aware gate. effective_slot/signature come from the AllowMint.
INSERT INTO mint_status_history
  (mint_address, status, effective_slot, signature, created_at)
VALUES
  (:mint, 'allowed', :allow_mint_slot, :allow_mint_signature, NOW())
ON CONFLICT (mint_address, effective_slot) DO NOTHING;

UPDATE transactions SET status = 'pending', updated_at = NOW()
 WHERE id = :transaction_id;
```

#### Step 3b - investigate why this happened, refund if needed, then delete

The indexer wrote a deposit row for a mint that has no `AllowMint` —
**this should not happen**. The deposit-side gate caught the symptom, but
something upstream wrote the row in the first place. Identify the cause
before clearing it:

```bash
solana confirm -v <signature> --url <solana-mainnet-rpc-url>
```

Decode the instruction against the escrow program IDL. Classify:

- **Indexer / instance-filtering defect** (row written from a
  foreign-instance instruction or a non-`Deposit` instruction).
  [Escalate](_escalation.md) (Tier 3) **first**; do not delete until
  engineering confirms root cause and can reproduce against the live row.
- **Manual DB insert.** [Escalate](_escalation.md) (Tier 3), capture
  the offender, then delete.

Once root cause is known, capture the row's full content in the
incident record and delete it. The
reconciliation orphan check has no status filter and runs with in-memory
per-id dedup, so a `failed` row stays silent in steady state but
re-appears in the orphan log — and re-posts a webhook alert via
`reconciliation_webhook_url` (payload: `orphan_ids`, `row_count`,
`timestamp`) — on every operator restart. Deleting the row is the only
way to remove that recurring boot-time noise:

```sql
DELETE FROM transactions WHERE id = :transaction_id;
```

**Do not re-arm** — the next tick fails the gate identically.

#### Step 3c - foreign-instance `AllowMint`, stop and escalate

The `AllowMint` exists but binds to a different escrow instance than
ours. The operator should never see foreign-instance rows — this is the
SOLA2-27 attack surface, not an operations recovery.
[Escalate](_escalation.md) (Tier 3). No SQL.

## Post-incident artifacts

- Transaction id, originating Solana `signature`, `recipient`, `mint`.
- Full webhook `error_message`.
- Recovery action taken.
- For Path A: refund tracking ticket.
- For Path D: on-chain `mint_authority` before/after, the
  `spl-token authorize` signature (if applicable), and the engineering
  ticket for any mint-replacement coordination.
- For Path E: which branch (3a/3b/3c), the `AllowMint` signature if
  found, any backfill SQL, refund ticket, and — for 3b — the full
  deleted row contents and the classified root cause (user error /
  indexer defect / manual insert).
