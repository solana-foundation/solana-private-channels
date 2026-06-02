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

There are **two trigger surfaces** that can land a deposit in
`manual_review`:

1. **Processor-side row-data validation** — deterministic per-row error
   raised before any RPC call (Paths A / B / C below).
2. **Sender-side post-JIT mint failure** — the operator's just-in-time
   mint-init helper found the on-chain mint in a state it cannot fix by
   re-issuing `mint_to` (wrong authority on the private channel mint, or
   corrupt mint data). See **Path D** for recovery.

## Symptom

- Webhook with `status=manual_review`, `transaction_type=deposit`.
- ERROR-level log line `Transaction <id> ManualReview`.

## Triage

`error_message` distinguishes the two trigger surfaces and dispatches to
the right Path below.

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

## Post-incident artifacts

- Transaction id, originating Solana `signature`, `recipient`, `mint`.
- Full webhook `error_message`.
- Recovery action taken.
- For Path A: refund tracking ticket.
- For Path D: on-chain `mint_authority` before/after, the
  `spl-token authorize` signature (if applicable), and the engineering
  ticket for any mint-replacement coordination.

