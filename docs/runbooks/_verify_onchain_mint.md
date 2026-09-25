# Procedure - Verify On-Chain Mint

**Scope:** deposit operator only. This procedure verifies whether a
`MintTo` instruction landed on the **private channel chain** (the channel side, not
Solana mainnet). For withdrawals, see
[`_verify_onchain_release.md`](_verify_onchain_release.md).

Run this before taking any deposit recovery action. **Do not skip - this
is the gate that prevents double-minting private channel tokens to the user.**

## Inputs

You need:
- `transaction_id` - DB primary key of the deposit row.
- The recipient ATA - derivable from `recipient` and `mint` columns of the
  row, or already in `counterpart_signature`'s associated transaction.
- A working RPC endpoint for the **private channel chain** (the channel, served by
  the gateway / read-node, not the Solana mainnet RPC). The operator's
  `COMMON_RPC_URL` env var is the same endpoint.

## Output

Exactly one of:
- `LANDED <signature>` - mint confirmed on the private channel; tokens were minted.
- `NOT_LANDED` - no mint in operator history for this deposit's source event.
- `AMBIGUOUS` - RPC unreachable, no decisive evidence, or the lookback
  window does not cover `processed_at`.

**If output is `AMBIGUOUS`, stop. [Escalate](_escalation.md) (Tier 2).
Do not retry.** A blind retry
risks double-minting if the original mint actually landed but is outside
the RPC's signature lookback window.

## Procedure

### Step 1 - pull row state

```sql
SELECT id,
       signature,
       instruction_index,
       inner_index,
       recipient,
       mint,
       amount,
       counterpart_signature,
       status,
       updated_at
  FROM transactions
 WHERE id = :transaction_id;
```

If `counterpart_signature` is set, the operator already recorded a mint
sig - verify it directly in Step 2 and skip Step 3.

### Step 2 - confirm a known signature

```bash
solana confirm -v <counterpart_signature> --url <private-channel-rpc-url>
```

- `Finalized`, no error → output `LANDED <counterpart_signature>`.
- `Failed` or `not found` → continue to Step 3 (the recorded sig may have
  been speculative; the actual mint may differ).
- RPC error → output `AMBIGUOUS`.
  [Escalate](_escalation.md) (Tier 2).

### Step 3 - search by idempotency memo

The operator attaches a deterministic memo to every mint:
`private_channel:mint-idempotency:<source_event_id>`
(`indexer/src/operator/constants.rs::MINT_IDEMPOTENCY_MEMO_PREFIX`).

`source_event_id` is base58 of sha256 over the row's `signature` bytes, then
`instruction_index` and `inner_index` (`-1` when NULL), each as i32
little-endian. Compute the memo from the Step 1 values (omit the last
argument when `inner_index` is NULL):

```bash
python3 - <signature> <instruction_index> [<inner_index>] <<'EOF'
import hashlib, struct, sys

signature, instruction_index = sys.argv[1], int(sys.argv[2])
inner_index = int(sys.argv[3]) if len(sys.argv) > 3 else -1
digest = hashlib.sha256(
    signature.encode() + struct.pack("<i", instruction_index) + struct.pack("<i", inner_index)
).digest()
alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
number, encoded = int.from_bytes(digest, "big"), ""
while number:
    number, remainder = divmod(number, 58)
    encoded = alphabet[remainder] + encoded
encoded = "1" * (len(digest) - len(digest.lstrip(b"\0"))) + encoded
print("private_channel:mint-idempotency:" + encoded)
EOF
```

The derivation is pinned by `source_event_id_matches_documented_vector` in
`indexer/src/operator/utils/instruction_util.rs`.

Derive the recipient ATA:

```bash
spl-token address \
  --token <mint> \
  --owner <recipient> \
  --url <private-channel-rpc-url> \
  --verbose
```

Scan recent signatures on that ATA:

```bash
solana transaction-history <recipient-ata> --limit 1000 --url <private-channel-rpc-url>
```

For each candidate, fetch and inspect:

```bash
solana confirm -v <signature> --url <private-channel-rpc-url>
```

A match has all of:
- A `MintTo` instruction for the row's `amount` of the same mint into the
  recipient ATA.
- A memo instruction whose data is exactly the memo computed above.
- `Finalized` commitment, no error.

Outcomes:
- One match → output `LANDED <signature>`. Use this signature in
  recovery.
- No match within the lookback window AND `processed_at` is more recent
  than the oldest signature returned → output `NOT_LANDED`.
- No match BUT `processed_at` predates the oldest signature returned →
  output `AMBIGUOUS` (the original mint may have rotated out of the RPC's
  history window). [Escalate](_escalation.md) (Tier 2).
- No match, `processed_at` is older than `getFirstAvailableBlock`, and the
  history is empty or starts at that block: output `AMBIGUOUS`. Truncation
  pruned that history, and pruned entries are simply not returned, so a
  short history here is not evidence the mint never landed.
  [Escalate](_escalation.md) (Tier 2).
- RPC unreachable → output `AMBIGUOUS`.

## Idempotency safety net

The operator does not scan for the memo before minting. Its guard is the
write-ahead journal: every mint signature is stored in
`pending_release_signatures` before broadcast, and a re-picked deposit is
classified against those signatures on the channel before any new mint
(`gate_reopened_deposit` in `indexer/src/operator/processor.rs`).

The recovery sweep deletes the journal once the row is `completed`, `failed`
or `failed_reminted`. **Re-arming a terminal row has no automatic guard: this
procedure is the only thing standing between a landed mint and a second
one.** The memo is a forensic marker and what resync uses to rebuild
serviced rows.

## After running this procedure

Capture the verdict, the signature(s) checked, and the RPC endpoint used
in the incident record. Without this trail, a future user dispute or
reconciliation mismatch cannot tell whether a row's
`counterpart_signature` was actually verified on the private channel or hand-picked,
and a postmortem cannot reproduce the recovery decision.
