# Withdrawing Tokens from Solana Private Channels

This guide explains how to withdraw tokens from the Solana Private Channels payment channel back to Solana Mainnet, and how the on-chain withdrawal bitmap stops a withdrawal from being released twice.

Want to jump to the code example? [Jump to the TypeScript example](#initiate-a-withdrawal-on-private_channel)

## Overview

Withdrawals move tokens from the Solana Private Channels payment channel to Solana Mainnet through a three-step process:

1. **Burn on Solana Private Channels**: User calls `WithdrawFunds` instruction to burn tokens on the Solana Private Channels payment channel
2. **Backend Processing**: Indexer detects the burn event and submits the release to Mainnet
3. **Release on Mainnet**: Operator calls `ReleaseFunds`, which consumes the withdrawal's nonce in the instance's bitmap and unlocks the escrowed tokens

The [Indexer/Operator](../indexer/src/operator/) handles steps 2 and 3 automatically. This guide explains how the withdraw process works and how to manually initiate a withdrawal on Solana Private Channels.

## Understanding the withdrawal bitmap

Solana Private Channels prevents a withdrawal from being released twice with an
on-chain **withdrawal bitmap**: one bit per withdrawal nonce. Each withdrawal is
assigned a unique `transaction_nonce`, and releasing it sets that nonce's bit.
The mainnet escrow program refuses any release whose bit is already set.

### Account layout

The bitmap lives in its own PDA, one per instance, derived from
`[b"withdrawal_bitmap", instance_pda]` and created alongside the instance:

| Offset | Field | Size |
|---|---|---|
| 0 | account discriminator | 1 byte |
| 1 | PDA bump | 1 byte |
| 2 | `generation` (u64, little-endian) | 8 bytes |
| 10 | bits, one per nonce | 8,192 bytes |

8,192 bytes cover 65,536 nonces. A nonce's bit lives at byte
`10 + (nonce % 65_536) / 8`, position `(nonce % 65_536) % 8`.

### Why a bitmap

The bit is a direct, constant-cost answer to the only question that matters:
has this nonce been released? There is no proof to construct off-chain, nothing
to keep in sync, and no way for an operator's view to disagree with the chain's.
Setting a bit costs one byte write; checking one costs one byte read.

### Generations

To stay bounded, the bitmap covers a **generation** of nonces at a time and is
rotated once no withdrawal in that window still owes a release. `generation` is
stored in the account:

```rust
let nonce_generation = transaction_nonce / NONCES_PER_GENERATION; // 65_536
```

A release is accepted only when `nonce_generation` equals the bitmap's stored
`generation`; otherwise the program returns `NonceOutsideCurrentGeneration`.

**Examples:**

| Transaction nonce | Generation | Bit position in window |
|---|---|---|
| 0 | 0 | 0 |
| 1 | 0 | 1 |
| 65,535 | 0 | 65,535 |
| 65,536 | 1 | 0 |
| 65,537 | 1 | 1 |
| 131,071 | 1 | 65,535 |
| 131,072 | 2 | 0 |

### Rotation

The operator sends `RotateBitmap`, which clears every bit and advances
`generation` by one:

```rust
// Operator-only instruction (dispatched automatically, see below)
RotateBitmap {
    expected_generation: 0, // must equal the stored generation
}
```

Rotation is driven by state, not by any particular withdrawal. On a timer the
sender compares the bitmap's generation against the lowest withdrawal nonce that
still owes a release. When that nonce belongs to a later generation, the current
window is finished with and a rotation is armed. The same comparison also
withholds the rotation while a lower nonce is still unresolved, since rotating
past it would close the only window its release could ever land in.

`expected_generation` makes rotation non-idempotent: a replayed rotation is
rejected with `UnexpectedGeneration` rather than skipping a whole generation of
nonces.

**Key properties:**
- **No replay across generations**: a nonce from generation 0 is rejected once
  the bitmap is on generation 1, even though its bit was cleared.
- **Unbounded withdrawals**: rotate indefinitely (generation 0, 1, 2, ...).
- **Constant verification cost**: one bit read and one bit write per release,
  independent of how many nonces have already been consumed.

### What gates a rotation

The program checks two things before it rotates: the signer is a registered
operator for the instance, and `expected_generation` equals the stored
generation. It does not check that the window is full or that every withdrawal
in it was released. A window is not guaranteed to fill: a withdrawal that ends
`failed` or `failed_reminted` never sets its bit.

When to rotate is the operator's responsibility (see the
[trust model](ESCROW_PROGRAM.md#trust-model)), and the indexer's sender enforces
it:

- It arms a rotation only when the lowest withdrawal nonce that still owes a
  release belongs to a later generation than the bitmap is on. Rows that are
  `completed`, `failed` or `failed_reminted` do not count; `manual_review` does.
- It holds an armed rotation while any release is in flight, or while a pending
  remint still depends on a bit in the current generation. The database is not
  read again before sending, so a terminal row re-armed to `pending` after the
  rotation is armed is not waited for.
- If a lower nonce keeps a rotation withheld for five minutes, it reports
  `rotation_blocked_by_lower_nonce`.

A nonce whose generation has been rotated past can never be released. The
sender does not retry it as a release. It routes the withdrawal to the
compensating remint instead. A row with earlier broadcast signatures enters the
remint flow, which remints on the channel only once it proves none of them
landed; if the evidence stays inconclusive the row goes to `manual_review`. A
row with no signatures goes to `manual_review` directly, where a human confirms
nothing landed and restores the user's tokens (see Path H in
[`withdrawal_manual_review.md`](runbooks/withdrawal_manual_review.md)).

### Visual example

```
Generation 0 (nonces 0-65,535)              Generation 1 (nonces 65,536-131,071)
+----------------------------+             +----------------------------+
| generation: 0              |             | generation: 1              |
| Nonces used: 61,204/65,536 |   Rotate    | Nonces used: 0/65,536      |
| Still owed: 0              |   ------>   | Status: ACTIVE             |
+----------------------------+             +----------------------------+
 (no nonce still owes a release)                  (all bits cleared)
```

Bits can stay clear in a rotated window. When the sender rotates, those nonces
belong to withdrawals that ended without a release, so nothing is waiting on them.

### Rejections you may see

| Error | Meaning |
|---|---|
| `NonceAlreadyUsed` | The bit is already set: this nonce was released. |
| `NonceOutsideCurrentGeneration` | The nonce belongs to a different generation than the bitmap covers. |
| `UnexpectedGeneration` | A rotation was submitted against a stale generation. |
| `InvalidWithdrawalBitmap` | The passed account is not this instance's bitmap. |

## Initiate a Withdrawal on Solana Private Channels

Users initiate withdrawals by burning tokens on the Solana Private Channels payment channel using the Withdrawal Program. This will burn tokens from Solana Private Channels. The Solana Private Channels Indexer/Operator will monitor for these transactions and then process the `ReleaseFunds` instruction on Mainnet.

### TypeScript Example

```typescript
import {
  getWithdrawFundsInstructionAsync,
  PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS
} from 'private-channel-withdraw-program';
import { address, generateKeyPairSigner, none } from '@solana/kit';

const user = await generateKeyPairSigner();
const withdrawAmount = 1_000_000n; // 1 USDC (6 decimals)
const USDC_MINT = address('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');

// Optional: Specify destination address on Mainnet (defaults to user if null)
const destinationOnMainnet = address('DestinationAddressOnMainnet...');

// Build withdraw instruction
const withdrawIx = await getWithdrawFundsInstructionAsync({
  user,
  mint: USDC_MINT,
  amount: withdrawAmount,
  destination: none(), // Optionally pass a destination address on Mainnet
});

// Send to Solana Private Channels RPC.
// Replace the URL placeholder with your real RPC endpoint.
const private_channelRpc = createSolanaRpc(createDefaultRpcTransport({ url: 'https://private-channel-rpc.example.com' }));
// ... sign and send transaction
```

**Key Points:**
- **Permissionless**: Any user can burn their tokens on Solana Private Channels
- **Destination Field**:
  - If `null`: Tokens released to `user` address on Mainnet
  - If specified: Tokens released to `destination` address on Mainnet (associated token account must already exist for this user's address on Mainnet)
- Executing the `WithdrawFunds` instruction will burn tokens from the Solana Private Channels payment channel immediately.

### Related Documentation
- [Escrow Interaction Guide](ESCROW_INTERACTION_GUIDE.md)
- [Architecture Overview](ARCHITECTURE.md)
- [Escrow Program Technical Reference](ESCROW_PROGRAM.md)
- [Withdrawal Program Technical Reference](WITHDRAW_PROGRAM.md)
- [Indexer Architecture](INDEXER.md)
