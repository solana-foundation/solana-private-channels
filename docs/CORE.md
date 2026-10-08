
# Solana Private Channels Core

Solana Private Channels processes transactions through five sequential stages, each optimized for a specific concern.

```
Transaction → [1:SigVerify] → [2:Dedup] → [3:Sequencer] → [4:Executor] → [5:Settler] → Database
```

### Stage 1: SigVerify

Parallelizes Ed25519 signature verification across configurable workers. Each worker independently validates transaction signatures before forwarding to dedup. Invalid signatures are dropped with error logging. Verification runs first so that only fully-verified transactions ever reach the dedup cache.

**Location**: [`core/src/stages/sigverify.rs`](../core/src/stages/sigverify.rs)

### Stage 2: Dedup

Filter replayed transactions after signature verification:
- Validates that a transaction's blockhash is in the set of live blockhashes (populated from settled blocks). Transactions referencing unknown or expired blockhashes are rejected.
- Maintains a cache of recently seen transaction message hashes keyed by blockhash.
- The replay identity is the message hash, not the first signature. The first signature is the fee payer's and is malleable: a signer can emit many valid signatures over one fixed message, so keying on it would let a sponsor replay a single victim authorization. The message hash commits to everything the victim signed and is invariant across those signature variants.
- Dedup runs after sigverify, and the stage is a single task, so its check-and-insert is atomic with no lock: only verified transactions are cached, and two concurrently-verified variants are serialized so the first inserts and the second is dropped.
- Invalidates blockhashes after a configurable duration, e.g., 15 seconds (150 blockhashes × 100ms block time).

**Location**: [`core/src/stages/dedup.rs`](../core/src/stages/dedup.rs)

**Code Snippet**:
```rust
// Check for duplicate; the message hash is the replay identity.
let is_duplicate = dedup_cache // HashMap<Hash, HashSet<Hash>>
    .get(&blockhash)
    .map(|hashes| hashes.contains(&message_hash))
    .unwrap_or(false);

if is_duplicate {
    continue; // Drop replay
}

// Add to cache
dedup_cache
    .entry(blockhash)
    .or_default()
    .insert(message_hash);
```

### Stage 3: Sequencer

Builds dependency directed acyclic graph (DAG) and produces conflict-free transaction batches:
- Analyzes each transaction's read/write account set to form a DAG.
- Uses a greedy scheduler to produce conflict-free batches (max 64 transactions).
- Transactions touching overlapping writable accounts are placed in separate batches to enable parallel execution.
- Emits batches to the executor.

**Location**: [`core/src/stages/sequencer.rs`](../core/src/stages/sequencer.rs), [`core/src/scheduler/dag.rs`](../core/src/scheduler/dag.rs)

1. **Dependency Analysis**:
   - Read-Read: No conflict (parallel execution allowed)
   - Read-Write: Conflict (must serialize)
   - Write-Write: Conflict (must serialize)

2. **Batch Formation**:
   - Start with empty batch
   - For each transaction in dependency order:
     - If no conflict with current batch → add to batch
     - If conflict → start new batch
   - Emit batches to executor

### Stage 4: Executor

Execute transaction batches through the SVM with custom execution modes.

**Location**: [`core/src/stages/execution.rs`](../core/src/stages/execution.rs), [`core/src/vm/`](../core/src/vm/)

**Account loading**: the executor preloads every account a batch references
before running it. An account that is not in the store is loaded as absent, but a
store that *could not answer* aborts the batch and stops the executor, which the
node supervisor turns into a restart. Nothing in an aborted batch is executed or
settled, so its transactions stay resubmittable. Treating an unreadable account
as absent would let the SVM run against state it wrongly believes is empty and
settle the result over the real row. Transient query failures are retried under a
hard total time budget, so a dead database reaches its verdict in seconds rather
than blocking on connection acquisition; an error the database itself returned
and a row that will not deserialize are both fatal on sight, since neither can
change on a second ask. The same split reaches RPC: `getAccountInfo` returns a
null account only when the account is genuinely absent, and a server error when
the store could not be read.

**Account size gating**: before any of those bytes are fetched, the executor asks
the store for the *lengths* of the accounts a batch references. Resident accounts
answer from memory and only genuine misses reach the database, where
`octet_length` reads a row's TOAST pointer without pulling the blob, so a warm
batch pays nothing and a cold one pays a metadata read. Two limits are then
applied to those lengths. A transaction referencing more than 64 MiB of account
data fails with `MaxLoadedAccountsDataSizeExceeded` without its accounts being
loaded at all; the SVM enforces the same ceiling, but only while loading, which
is after the store has already been read. A batch is then packed against a
256 MiB preload budget, counting each account once however many transactions
name it, and whatever does not fit runs as a follow-on sub-batch once the current
one has been handed to the settler. Per-transaction limits alone would not bound
anything here: a batch of individually legal transactions still adds up to
gigabytes, and a small transaction can name a large amount of account data
through readonly keys no instruction uses. A store that cannot answer the size
query aborts the batch on the same terms as an unreadable preload. The fetch then
counts the account data it actually reads against the larger of the two limits
and stops as soon as it passes it, before anything reaches the cache. On the write
node the settler is the only writer, so the fetch always matches its sizes; more
bytes than that mean the accounts table changed under the writer, and the batch
aborts like an unreadable store. A simulation's fetch is limited to 64 MiB, and at
most 8 simulations run at once, so they hold at most about 512 MiB of account data
between them. On a read node with a Redis cache the reply for the cached accounts
arrives whole before it can be counted, so that bound covers what a fetch returns,
not that brief reply.

**Resident account memory**: the preload budget bounds one fetch, but the cache
keeps what it loads, so BOB is capped by entry count (1,000,000) and by bytes
(1 GiB). Both caps run after each preload lands: the entry cap evicts the oldest
clean entries first and the byte cap the largest, each until the cache is back
under 90% of its cap. The accounts the preload was asked for are never evicted,
and neither are unsettled writes, so every account the batch reads reaches the
SVM. Resident entries and account data each stay within the cap plus one preload
plus the writes the settler has not yet acknowledged, since only clean entries
can be evicted. Rows are decoded as they stream in, so a preload briefly costs about
1.07 times the bytes it fetches. Results leaving the executor
keep writable account data and every account's lamports but drop readonly data,
which nothing downstream reads; otherwise a result would keep an evicted account
alive until the settler had finished with it.

**Blockhash expiry**: a transaction commits in a block whose height is at most
`lastValidBlockHeight + 1` of its blockhash, or it never commits. The live window
keeps advancing while a batch waits on its account load, so validity is checked
twice. The check on arrival only avoids loading accounts for a transaction that
is already dead. The check after the load decides whether a transaction may run.
Under the same read lock it counts the batch as an *admission* if anything in it
is still live, and the last results message of that batch carries the admission's
number. Neither VM would catch a stale transaction later, since Core supplies
successful transaction prechecks and a default processing blockhash.

Before it cuts each block, the settler retires the hash that block expires under
the window's write lock, and counts the block as announced in the same step. If
it retired anything, it keeps receiving results, past its buffer caps if need be,
until the last message of every admission made before the retire has arrived,
and puts them all in this block. An admission either came first and lands in this
block, or came after and found the hash gone. So once `getBlockHeight` is above a
hash's `lastValidBlockHeight`, every transaction built on it that will ever
commit is already readable, because a block's statuses and its height commit in
one Postgres transaction. From the retire until dedup takes in the new block's
hash, which is after that block commits, `isBlockhashValid` answers the retryable
"catching up" error for the retired hash rather than `false`. So `false` means
the original can no longer land, not that it did not, and it comes from the write
node while `getSignatureStatuses` is served by a read replica that can lag. A
client may re-sign only after `getBlockHeight` is above the old
`lastValidBlockHeight` and a `getSignatureStatuses` read made after that height
read, on the same endpoint, is still `null`; otherwise the original and its
replacement can both execute. A transaction dropped by either
check is never settled, so it stays absent from `getSignatureStatuses` exactly as
one dropped on arrival does.


**Execution Modes**:

#### AdminVM

Privileged execution for token mint operations (bypasses BPF execution). This enables consistent mint addresses across Mainnet and the Solana Private Channels payment channel. This is achieved by intercepting `InitializeMint` instructions and synthesizing mint accounts without executing BPF code.

**Location**: [`core/src/vm/admin.rs`](../core/src/vm/admin.rs)
**Security**: Transactions are gated by admin key validation in the SigVerify stage (`PRIVATE_CHANNEL_ADMIN_KEYS`). Only transactions signed by an admin key are routed to AdminVM for execution.

#### GaslessCallback

GaslessCallback intercepts SVM account lookups to synthesize fee payer accounts on-demand (fixed lamports, owned by system program):

```rust
fn account(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
    self.bob.get_account_shared_data(pubkey).or_else(|| {
        // Synthesize fee payer with minimal lamports
        self.fee_payers.contains(pubkey).then(|| {
            AccountSharedData::new(
                DEFAULT_FEE_PAYER_LAMPORTS,
                0,
                &solana_sdk_ids::system_program::ID,
            )
        })
    })
}

fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
    self.account(pubkey).map(|account| (account, CHANNEL_SLOT))
}
```

This eliminates the operational overhead of funding user accounts for off-chain execution and results in zero gas fees for all user transactions.

**Location**: [`core/src/vm/gasless_callback.rs`](../core/src/vm/gasless_callback.rs)

##### Lamport conservation

These synthesized lamports are the only lamports in the channel that were never deposited, so the execution stage treats them as a loan the transaction must repay. After a successful regular transaction, every writable account the SVM loaded that BOB had never seen is examined:

- A synthesized fee payer must end holding the same amount it was handed. Whatever it is short by is the unrepaid part of the loan.
- Each account the transaction created may keep one lamport of that shortfall, because the SVM requires a live account to hold at least one and, with rent at zero, every creation path funds exactly one. A new System-owned account with no data does not count as a creation, and at most 4 accounts per transaction may be created while the float is short (CreateDvp's 4 is the most any flow needs).
- While any of the float is missing, no account that already existed may end richer than it started. Counting creations does not prove the float paid for them, so without this a creation funded by real money could licence a fabricated lamport landing somewhere durable. With the float intact, pre-existing accounts move real lamports freely.
- If the shortfall exceeds the allowance, a pre-existing balance grew while the float was short, or more than 4 accounts were created while it was short, the transaction is failed with `UnbalancedTransaction` and nothing it wrote is persisted. Otherwise the synthesized payers are erased and every other account is persisted exactly as executed.

Lamports sent *to* a synthesized payer are burned with it. Persisting the payer would graduate an address the channel invented into durable state, and returning them would mean rewriting the sender, so neither is safe. This is not a loss of deposited value: deposits mint tokens, never lamports, so every lamport in the channel began as a float or an admin mint's existence floor. It is also load-bearing for `CancelDvp`, where the settlement authority signs, pays, and receives the closed escrows' rent, ending above its float.

Lamports that would bring a new System account into being are burned the same way, in every transaction, so no sequence of transactions can turn the float into a plain wallet. Ingress refuses every top-level System instruction, but a token `CloseAccount` or a DvP close can still pay lamports to an address the channel has never seen. That address is never created: if the float paid for it, the transaction fails with `UnbalancedTransaction`; if existing lamports paid for it, the transaction succeeds and they are burned.

Because the SVM already enforces per-instruction lamport conservation, blocking the fabricated lamports at their source means every other balance is made of lamports that already existed, so no other account needs inspecting. Accounts a transaction merely carries as writable keys are never rewritten.

**Location**: [`enforce_lamport_conservation` in `core/src/stages/execution.rs`](../core/src/stages/execution.rs)

### Stage 5: Settler

Batches execution results every 100ms (configurable) and commits to PostgreSQL, the source of truth, mirroring each batch to the Redis cache if one is configured. The settler writes:
- Modified accounts
- Transaction records
- Block metadata (slot, blockhash, timestamp)

The mirror is best-effort and covers only what the cache can serve: point lookups by pubkey, signature and slot, plus the chain tip. Ranges, history and counters are read from PostgreSQL, because a short answer from a partial mirror is indistinguishable from a complete one. A failed cache write drops the keys it would have updated so reads miss and resolve against PostgreSQL, and leaves the cached tip behind, which makes the next batch rebuild the cache. It is also bounded: a cache that has not answered within the budget is abandoned for that block, and one that keeps failing is left alone for a cooldown, then probed with a PING off the block path and rebuilt before it is mirrored to again.

The settler also caps what it buffers between ticks: the settled account bytes, and the address-index rows the block will write, one per account key per transaction. Once a tick's buffer reaches either budget it stops draining the executor queue, so the executor's bounded send applies backpressure upstream rather than letting one commit grow without limit. A block that waits on an admission (see blockhash expiry above) drains the queue past the budget, so the queue itself is bounded too: the executor's in-flight budget weighs each message by its bytes or its address rows, whichever is more, and a full buffer plus that whole budget still fits the address-index writer. The rest of the admitted sub-batch also lands in that block; its rows fit at the default batch size, and in bytes the block can reach about 512 MiB, which a compile-time check keeps under Postgres' 1 GB limit for one bound value. Blocks are still produced only on the tick, never early, and the executor splits an oversized batch into byte-bounded messages so a single already-executed batch cannot overshoot the budget. The commit itself binds each column as one array parameter, so bounding the buffer is what bounds the bind; the driver is held at sqlx 0.8 or later, where an oversized bind fails loudly instead of being truncated by the binary protocol's length prefix.

Both queues either side of the settler are bounded by the account bytes and index rows their messages carry, not only by how many messages are queued. The executor holds a budget for the results it has sent but the settler has not yet received, sized at two of its own chunks and counting a message's address rows as well as its bytes, and the settler holds a row budget for the address-index rows it has handed to the background writer but the writer has not yet folded into a flush. In each case the budget travels inside the message and comes back when the message is consumed or dropped, so no consumer has to account for it.

Finally, the settler notifies the executor's in-memory cache (BOB) of settled accounts, completing the feedback loop. That acknowledgement is merged into a per-account inbox rather than sent on a queue, so it never blocks settlement: BOB drains it only while the executor runs and the executor runs only while the settler drains, so a blocking send would deadlock the three stages. Each account keeps only its newest settlement, which is all BOB can act on, so the inbox never holds more entries than BOB has dirty accounts. A settlement carries the generation of the executor write it made durable, not a mark for the whole block, so when the executor splits one batch across two messages and they land in different blocks, each account is still acknowledged for exactly the write that committed. It shares the account buffers BOB is already pinning rather than copying them, so its size costs metadata, not account data. The settler retires the hash each block expires from the dedup window itself, before it cuts the block, so the window keeps moving during a shutdown drain after dedup has stopped. The settled blockhash goes to dedup on a queue as deep as the blockhash window; dedup drains it even while its own forward to the sequencer is parked, so the settler only ever waits on it briefly.

**Location**: [`core/src/stages/settle.rs`](../core/src/stages/settle.rs)


## Supported Programs

Solana Private Channels restricts which programs can execute in the payment channel. Transactions referencing unsupported programs are rejected at the RPC layer.

| Program | Status | Notes |
|---------|--------|-------|
| **SPL Token** | Supported | Token-2022 is **not** admitted at ingress; the escrow program on Mainnet accepts it, the channel does not |
| **SPL Associated Token Account** | Supported | ATA creation and lookup |
| **SPL Memo** | Supported | Memo attachments |
| **System Program** | Not supported at top level | Every System instruction is refused at ingress, `Transfer` included; still reachable as a CPI target of the allowlisted programs |
| **Solana Private Channels Withdraw Program** | Supported | Token burns for withdrawal flow |
| **DvP Swap Program** | Supported | Delivery-versus-payment swaps |

A transaction that lists the spl-token native mint (`So11111111111111111111111111111111111111112`) in its account keys is also rejected. spl-token builds a native (wSOL) account without loading the mint, so refusing the key is what keeps fabricated gasless lamports from becoming wSOL.

**Source**: [`is_allowed_program_instruction` in `core/src/transactions.rs`](../core/src/transactions.rs) (predicate), [`core/src/rpc/send_transaction_impl.rs`](../core/src/rpc/send_transaction_impl.rs) (enforcement)

### AdminVM Program Support

The AdminVM (used for operator mint operations) only supports SPL Token `InitializeMint`. All other instruction types are rejected.

An `InitializeMint` or `InitializeMint2` that targets the spl-token native mint (`So11111111111111111111111111111111111111112`) is rejected with `InvalidArgument`, a second guard behind the ingress check that refuses any transaction listing the native mint.

An `InitializeMint` over a System-owned account with no data, which anyone can create by sending the address lamports, succeeds and keeps the account's lamports.

**Source**: [`core/src/vm/admin.rs`](../core/src/vm/admin.rs)

## Limitations

### No Custom Program Deployment

Solana Private Channels does not support deploying arbitrary BPF programs. The supported program set is fixed at compile time. The instruction allowlist is currently hardcoded to SPL Token instructions.

### No Address Lookup Tables

The address lookup table program is not in the instruction allowlist, so no lookup
table account can be created or read here. A v0 transaction whose message declares
`address_table_lookups` is therefore rejected at RPC admission with `-32602`, by
both `sendTransaction` and `simulateTransaction`. Resolving such a message is
impossible without the table it names, and admitting it unresolved would produce a
transaction whose instruction account indices point past its own account key list.
Legacy transactions and v0 transactions that declare no lookups are unaffected.

**Source**: [`core/src/rpc/send_transaction_impl.rs`](../core/src/rpc/send_transaction_impl.rs), [`core/src/rpc/simulate_transaction_impl.rs`](../core/src/rpc/simulate_transaction_impl.rs) (enforcement); [`core/src/transactions.rs`](../core/src/transactions.rs) (predicate)

### No Precompiles

Solana precompile programs (Ed25519, Secp256k1, Secp256r1) are not available. Transactions that reference precompile addresses will fail.

### Hardcoded Constraints

| Constraint | Value | Source |
|------------|-------|--------|
| Max transaction size | 1,232 bytes | Solana's `PACKET_DATA_SIZE` |
| Max transactions per batch | 64 (configurable) | `PRIVATE_CHANNEL_MAX_TX_PER_BATCH` |
| Max loaded accounts data | 64 MB | [`core/src/processor.rs`](../core/src/processor.rs) |
| Max signatures per `getSignatureStatuses` | 256 | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
| Max slot range for `getBlocks`, max limit for `getBlocksWithLimit` | 500,000 | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
| Max concurrent `getBlocks`/`getBlocksWithLimit` spanning more than 10,000 (a span counts only up to the newest block) | pool size / 8 (at least 1), the excess returns `-32003` | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
| Max addresses per `simulateTransaction` | the transaction's own account count (matches Agave) | [`core/src/rpc/simulate_transaction_impl.rs`](../core/src/rpc/simulate_transaction_impl.rs) |
| Max encoded bytes for `simulateTransaction` accounts | 5 MB | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
| Max concurrent `simulateTransaction` calls | 8, the excess returns `-32003` | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
| Max RPC response size | 10 MB, **declared but not enforced** (see below) | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
| Gateway max request body | 64 KB | [`gateway/src/lib.rs`](../gateway/src/lib.rs) |

`MAX_RESPONSE_SIZE` does not currently limit anything. It is passed to
`RpcModule::raw_json_request`, whose second parameter is jsonrpsee's subscription buffer size, and
jsonrpsee's own `inner_call` hardcodes `max_response_size = usize::MAX`. Core drives hyper directly
rather than using jsonrpsee's server, and the gateway streams upstream bodies through without a cap,
so no read method has an enforced response ceiling. `simulateTransaction` is the exception: its
accounts array is bounded explicitly by the 5 MB budget above. Treat the 10 MB row as intent, not
protection, when reasoning about memory.

### No Fork Choice

Solana Private Channels does not implement forks. The fork graph is stubbed — all blocks are final on write. There is no rollback mechanism. Slots do advance at the ledger level: they tick every `blocktime_ms` whether or not a block is produced, and are what `getSlot` answers. The SVM executes every transaction at a fixed slot, so the program cache never reasons across slots or forks.