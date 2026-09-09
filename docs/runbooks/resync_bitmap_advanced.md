# Runbook - Resync Refused: Withdrawal Bitmap Has Advanced

**Status: untested guidance.** The sequence below is derived from the code paths
named in it, not from a rehearsed drill. Have someone who has run a withdraw
resync review it before following it under pressure.

## Symptom

A withdraw resync aborts during its pre-flight. The log carries
`Withdrawal bitmap has advanced; aborting resync before drop` with the
`generation` and `set_bits` fields, and the command exits with:

```
withdrawal bitmap has advanced (generation <G>, <N> set bit(s)); a resync would
restart the nonce sequence at 0 against it. Aborted before drop, the database is
intact. See docs/runbooks/resync_bitmap_advanced.md
```

The error surfaces as `ReconciliationError::WithdrawalBitmapAdvanced`.

A sibling refusal fires when the database itself records released nonces, before
any RPC is consulted:

```
the database records <N> completed withdrawal(s), so the chain's bitmap has issued
their nonces; a resync would restart the nonce sequence at 0 under them. Aborted
before drop, the database is intact. See docs/runbooks/resync_bitmap_advanced.md
```

That one is `ReconciliationError::WithdrawalNoncesReleased`. It is the same
refusal as an advanced bitmap, reached from local evidence rather than from the
chain, and no RPC answer can clear it.

A third refusal fires when the bitmap could not be read at all:

```
withdrawal bitmap could not be verified, resync aborted before drop: <reason>. An
unreadable bitmap is not evidence that resyncing is safe. See
docs/runbooks/resync_bitmap_advanced.md
```

That one is `ReconciliationError::WithdrawalBitmapUnverified`. It usually means
`common.escrow_instance_id` or `--escrow-rpc-url` is missing, or the Solana RPC
did not answer. Fix the input and rerun; the check itself has not made a
judgement about the chain yet.

Two of its reasons are about the RPC endpoint rather than the bitmap account:

- `finalized tip ... is <N>s behind wall clock` means the node is lagging or
  replaying a snapshot. Its answers describe an older chain, so an empty bitmap
  from it proves nothing. Point `--escrow-rpc-url` at a node at the live tip.
- `finalized tip slot ... has no block time` means the node could not date its
  own tip, so its freshness could not be shown either way.

**Nothing has been destroyed.** The bitmap is read in the same fail-closed
pre-flight block as the genesis-slot, channel-reachability and memo-scheme
checks, all of which run before the resync drops anything. The database is
exactly as it was. There is no data loss to recover from and no rush.

## Why the resync refuses rather than continuing

A resync drops every indexer table and recreates the schema, which recreates
`withdrawal_nonce_seq` starting at 0. The on-chain withdrawal bitmap is not
touched: it keeps its generation and every bit already set. Rebuilt withdrawals
would then be numbered against nonces the chain has already spent, and two
things break:

- **Every release is rejected.** The program's `validate_generation` requires
  `nonce / NONCES_PER_GENERATION == bitmap.generation`. A sequence restarted at
  0 issues nonces in generation 0, so while the bitmap sits at generation `G`
  every release fails with `NonceOutsideCurrentGeneration` until the sequence
  has climbed past `G * NONCES_PER_GENERATION`. With the production window at
  65,536 nonces per generation, that is not a wait anyone rides out.
- **The boot check compares the wrong things.** `validate_bitmap_consistency`
  diffs `completed` rows against the set bits at operator startup. After a
  resync the rows carry nonces from the new numbering and the bits were set by
  the old one, so a set bit with no row, and a row whose nonce now names a
  different withdrawal, are compared as though they were the same thing. Its
  verdict, whichever way it falls, is about the wrong withdrawals.

The refusal is the only pre-flight that reads chain-side withdrawal state, and it
fires on any non-zero generation or any set bit, because either one proves the
chain has issued a nonce.

It takes two independent proofs, in that order, so a wrong answer from one cannot
open the gate on its own:

1. **The database's own completed withdrawals.** A `completed` withdrawal row is
   this indexer's record that the chain released that nonce. No RPC answer can
   contradict it, so this runs first and needs no network at all.
2. **The bitmap, read at a tip proven fresh.** The check reads the node's
   finalized tip, refuses if that tip's block time is more than 120 seconds
   behind wall clock, and then reads the bitmap bound to that exact slot with
   `minContextSlot`. A load balancer routing the account read to an older backend
   returns an error rather than a staler bitmap, and a node replaying a snapshot
   is rejected at the tip check before the bitmap is read at all.

## Why renumbering is not offered

Aligning the rebuilt rows with the chain would mean assigning each rebuilt
withdrawal the nonce the chain already released it under. Nothing in the source
history records that mapping: the nonce was assigned by the database at the
time, and the database is what the resync is about to destroy. Any renumbering
scheme would be guessing which withdrawal a set bit belongs to, and a wrong
guess is silent. A withdrawal attributed to the wrong nonce looks fully
reconciled while paying, or refusing to pay, the wrong user.

Refusing is correct and matches how the other pre-flight checks behave.

## What to do instead

### 1. Confirm the refusal is real

Read the bitmap the error is describing. Derive the PDA from
`common.escrow_instance_id` and check its generation and bit count:

```
solana account <BITMAP_PDA> --url <ESCROW_RPC> --output json
```

If the account is at generation 0 with no set bits and the resync still refused,
either the RPC is serving stale or wrong state, or the database holds completed
withdrawals whose bits a rotation has since cleared. Check which refusal fired:
`WithdrawalNoncesReleased` is the local one and names a row count, and no change
of endpoint will clear it. For the others, point `--escrow-rpc-url` at a node at
the live tip and rerun before doing anything else.

### 2. Do not resync; stand up a fresh instance

The supported path is a **fresh escrow instance**, not a resync against a live
one. A new instance starts with a new bitmap at generation 0 and no set bits,
which is the only chain state a withdraw rebuild can be numbered against. The
existing instance and its history stay where they are, and the existing
database is not rebuilt.

Whatever prompted the resync should be resolved on the live instance by its own
runbook. A single divergent row is
[`withdrawal_pipeline_halt_runbook.md`](withdrawal_pipeline_halt_runbook.md); a
skipped slot range is
[`indexer_start_slot_ahead_of_checkpoint.md`](indexer_start_slot_ahead_of_checkpoint.md).

### 3. If a resync is genuinely required, escalate

A withdraw resync against an instance that has released anything is an
escalation, not a runbook step. It means someone has decided to rebuild the
withdrawal history of a live instance, which needs a plan for every nonce the
chain has already set. Raise it through [`_escalation.md`](_escalation.md) with
the generation and set-bit count from the error, and do not run the resync until
that plan exists.

## What NOT to do

- **Do not clear or reinitialise the bitmap** to make the check pass. The set
  bits are the chain's record of which nonces were paid. Clearing them lets the
  same nonce be released twice.
- **Do not fast-forward `withdrawal_nonce_seq` by hand** after a resync to skip
  past the current generation. The releases would then land, but the boot
  consistency check would still be comparing rows against bits set under the
  old numbering, and the rows in between would be unattributable.
- **Do not remove or bypass the pre-flight.** It is the only check that reads
  chain-side withdrawal state before the drop. Without it the resync destroys the
  database and discovers the problem at the first release.
- **Do not resync without `--escrow-rpc-url` configured** hoping the check is
  skipped. It is not; an unreadable bitmap refuses the same way.
- **Do not point `--escrow-rpc-url` at a lagging node** to get an empty bitmap
  past the check. The tip freshness check exists for exactly that, and deleting
  the completed rows to clear the local check destroys the only record of which
  nonces were paid.
