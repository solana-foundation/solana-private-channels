# Runbook - Indexer Instruction Will Not Decode

The **`parse_failed`** indexer wedge: a slot holds an instruction whose discriminator
the indexer supports but whose payload it could not decode. It refuses to checkpoint
past a slot it could not fully read, so ingestion for that program stops.

Not routed by the webhook dispatch table in [`README.md`](README.md) (no transaction
row changes status); paged by the Grafana alert `indexer-parse-failed`, same shape as
[`indexer_block_unavailable.md`](indexer_block_unavailable.md).

**Retrying cannot clear this.** Replaying the slot re-parses the same bytes and fails
the same way. Only a fuller data source or a code fix will.

When `COMMON_FALLBACK_RPC_URL` is set, the indexer has already re-fetched the slot from it
and that copy was rejected too, so this alert firing means the fallback is unset, serves
the same thin metadata, or is on a different chain or fork (grep for `wrong cluster or
divergent fork`). Yellowstone reaches the fallback through the gap-fill, not the stream.

An unrecognized discriminator is ignored on purpose, so a program gaining a new
instruction never causes this. Two causes only:

1. **Thin metadata** (common, fixed by a different endpoint). No `innerInstructions`,
   or inner instructions with no `stackHeight`. An escrow `Deposit` reads its amount
   from a `DepositEvent` self-CPI and scopes it by stack height, so either omission
   makes every CPI deposit undecodable while the block still looks well-formed. Only
   escrow reads inner instructions, so a channel node returning a null list cannot
   trip the withdraw indexer.
2. **Layout drift** (a code problem). A program's instruction changed on chain and
   the parser was not updated.

### Detection

Keys off the error counter, not `private_channel_indexer_checkpoint_frontier_lag`
(that gauge reads zero while an ungated RPC-polling indexer is wedged):

```
rate(private_channel_indexer_rpc_errors_total{error_type="parse_failed"}[5m]) > 0
```

Log lines name the slot, transaction and instruction position (`inner` prints
`Some(<J>)` for a CPI instruction, `None` for a top-level one):

```
# live polling
Slot <N> transaction <SIG> instruction <I> (inner <J>) will not decode: <reason>; refusing to checkpoint past unknown contents

# backfill or reconnect gap-fill
Backfill slot <N> transaction <SIG> instruction <I> (inner <J>) will not decode: <reason>; aborting before checkpoint

# Yellowstone live stream
Slot <N> transaction <SIG> instruction <I> (inner <J>) will not decode: <reason>; refusing to complete the slot
```

Confirm the checkpoint is parked below the named slot, then check what the endpoint
actually served:

```sql
SELECT program_type, last_committed_slot, updated_at FROM indexer_state;
```

```sh
curl -s "$COMMON_RPC_URL" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[<N>,{"encoding":"json","maxSupportedTransactionVersion":0,"commitment":"finalized"}]}' | jq '.result.transactions[].meta | {inner: (.innerInstructions | length), heights: [.innerInstructions[]?.instructions[]?.stackHeight]}'
```

Zero inner instructions or `null` stack heights is cause 1. Real stack heights point
at cause 2, so read `<reason>`: a borsh or account-count error against a recently
upgraded program means the parser needs updating.

### Recovery

1. Repoint at an endpoint serving complete metadata (archival Solana RPC for
   `indexer-solana`; a read node still holding the slot for
   `indexer-private-channel`): `COMMON_RPC_URL`, and `COMMON_FALLBACK_RPC_URL` if
   configured. Both are read once at startup, so restart the process.
2. On Yellowstone the slot is replayed by the reconnect gap-fill over RPC, so the
   endpoint to fix is the backfill RPC URL. The gRPC provider must also emit
   `stackHeight` for live escrow deposits to decode.
3. Watch the counter go quiet and the checkpoint resume. The indexer re-reads from
   its stored checkpoint, so nothing is replayed by hand and nothing is skipped.

If every reachable endpoint returns the same undecodable instruction, it is cause 2:
the fix is a parser change and a deploy. The indexer stays wedged meanwhile, which is
intended. Escalate per [`_escalation.md`](_escalation.md) with the slot, program type,
signature and `<reason>`. A decision to skip the slot is a deliberate, human, recorded
one; there is no operator command that marks a slot skipped, and there should not be.

### What NOT to do

- Do not raise `start_slot` past the wedged slot. That is the silent data loss the
  refusal prevents, and it refuses to boot anyway, see
  [`indexer_start_slot_ahead_of_checkpoint.md`](indexer_start_slot_ahead_of_checkpoint.md).
- Do not hand-edit `indexer_state`. Same data loss, with nothing to stop you.
- Do not just restart. Re-parsing identical bytes fails identically.
- Do not repoint at a non-archival peer hoping it differs. It can serialize metadata
  the same way, turning the restart into a no-op.
