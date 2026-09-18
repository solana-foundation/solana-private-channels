# Procedure - Verify On-Chain Release

**Scope:** withdrawal operator only. This procedure verifies whether a
`release_funds` instruction landed on Solana mainnet. It does **not**
apply to deposits.

Shared across every withdrawal recovery runbook. Run it before taking any
action. **Do not skip - this is the gate that prevents double-crediting
users.**

## Inputs

You need:
- `transaction_id` - DB primary key of the withdrawal row.
- Operator pubkey - the signer that submits `release_funds`. Available via
  the operator container env.
- A working Solana RPC endpoint pointed at the same cluster the operator is
  running against.

## Output

Exactly one of:
- `LANDED <signature>` - release confirmed on-chain. Funds moved.
- `NOT_LANDED` - no release in operator history for this nonce.
- `AMBIGUOUS` - RPC unreachable, response inconclusive, or a sig appears
  but cannot be confirmed finalized.

**If output is `AMBIGUOUS`, stop. [Escalate](_escalation.md) (Tier 2).
Do not proceed to recovery.**

## Procedure

### Step 1 - pull row state

Run against the indexer Postgres primary:

```sql
SELECT id,
       withdrawal_nonce,
       status,
       counterpart_signature,
       remint_signatures,
       updated_at
  FROM transactions
 WHERE id = :transaction_id;
```

Record the values. `remint_signatures` is the array of withdrawal signatures
that were stashed before the failure; if non-NULL it is the fastest path.

### Step 2 - if `remint_signatures` is non-empty, check those first

For each signature in `remint_signatures`:

```bash
solana confirm -v <signature> --url <rpc-url>
```

Possible outcomes:
- One reports `Finalized` and **no error** → output `LANDED <signature>`.
  Use this signature in the recovery step.
- All report `Failed` or `not found` → continue to Step 3.
- RPC errors or returns inconclusive → output `AMBIGUOUS`.
  [Escalate](_escalation.md) (Tier 2).

### Step 3 - operator signature history scan

Required for sites that never stashed a signature (build error, pre-flight
bail, "no signatures to verify"). Also a backstop when Step 2 was empty.

```bash
solana transaction-history <operator-pubkey> --limit 1000 --url <rpc-url>
```

For each candidate signature returned, decode the transaction and check
whether it is a `release_funds` carrying `transaction_nonce =
<withdrawal_nonce>`:

```bash
solana confirm -v <signature> --url <rpc-url>
```

The instruction data for `release_funds` includes the nonce as a `u64` -
match against `withdrawal_nonce` from Step 1.

Outcomes:
- A signature matches the nonce and is `Finalized` → `LANDED <signature>`.
- No matching signature in the recent window and the row is older than the
  RPC's history window (typically ~1k–2k recent sigs per pubkey) →
  `NOT_LANDED`.
- RPC unavailable or window doesn't cover the row's `updated_at` →
  `AMBIGUOUS`.

## What "AMBIGUOUS" means in practice

Funds are stranded until the verdict resolves. Do not retry, do not remint,
do not mark Completed. Wait for RPC to recover and re-run the procedure, or
[escalate](_escalation.md) (Tier 2) for a deeper on-chain audit.

The operator code itself implements the same fence: when the sender cannot
verify whether a release landed, it routes the row to `manual_review` with
`error_message` ending in `| no signatures to verify, remint unsafe`
(`indexer/src/operator/sender/transaction.rs`). Honor that fence.

## After running this procedure

Capture the verdict, the signature(s) checked, and the RPC endpoint used
in the incident record. Without this trail, a future user dispute or
reconciliation mismatch cannot tell whether a row's
`counterpart_signature` was actually verified or hand-picked, and a
postmortem cannot reproduce the recovery decision.
