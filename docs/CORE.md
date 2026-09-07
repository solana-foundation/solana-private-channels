
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


**Execution Modes**:

#### AdminVM

Privileged execution for token mint operations (bypasses BPF execution). This enables consistent mint addresses across Mainnet and the Solana Private Channels payment channel. This is achieved by intercepting `InitializeMint` instructions and synthesizing mint accounts without executing BPF code.

**Location**: [`core/src/vm/admin.rs`](../core/src/vm/admin.rs)
**Security**: Transactions are gated by admin key validation in the SigVerify stage (`PRIVATE_CHANNEL_ADMIN_KEYS`). Only transactions signed by an admin key are routed to AdminVM for execution.

#### GaslessCallback

GaslessCallback intercepts SVM account lookups to synthesize fee payer accounts on-demand (fixed lamports, owned by system program):

```rust
fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
    if let Some(account) = self.bob.get_account_shared_data(pubkey) {
        return Some(account);
    } else if self.fee_payers.contains(pubkey) {
        // Synthesize fee payer with minimal lamports
        return Some(AccountSharedData::new(
            DEFAULT_FEE_PAYER_LAMPORTS,
            0,
            &solana_sdk_ids::system_program::ID,
        ));
    }
    None
}
```

This eliminates the operational overhead of funding user accounts for off-chain execution and results in zero gas fees for all user transactions.

**Location**: [`core/src/vm/gasless_callback.rs`](../core/src/vm/gasless_callback.rs)

##### Lamport conservation

These synthesized lamports are the only lamports in the channel that were never deposited, so the execution stage treats them as a loan the transaction must repay. After a successful regular transaction, every writable account the SVM loaded that BOB had never seen is examined:

- A synthesized fee payer must end holding the same amount it was handed. Whatever it is short by is the unrepaid part of the loan.
- Each account the transaction created may keep one lamport of that shortfall, because the SVM requires a live account to hold at least one and, with rent at zero, every creation path funds exactly one.
- While any of the float is missing, no account that already existed may end richer than it started. Counting creations does not prove the float paid for them, so without this a creation funded by real money could licence a fabricated lamport landing somewhere durable. With the float intact, pre-existing accounts move real lamports freely.
- If the shortfall exceeds the allowance, or a pre-existing balance grew while the float was short, the transaction is failed with `UnbalancedTransaction` and nothing it wrote is persisted. Otherwise the synthesized payers are erased and every other account is persisted exactly as executed.

Lamports sent *to* a synthesized payer are burned with it. Persisting the payer would graduate an address the channel invented into durable state, and returning them would mean rewriting the sender, so neither is safe. This is not a loss of deposited value: deposits mint tokens, never lamports, so every lamport in the channel began as a float or an admin mint's existence floor. It is also load-bearing for `CancelDvp`, where the settlement authority signs, pays, and receives the closed escrows' rent, ending above its float.

Because the SVM already enforces per-instruction lamport conservation, blocking the fabricated lamports at their source means every other balance is made of lamports that already existed, so no other account needs inspecting. Accounts a transaction merely carries as writable keys are never rewritten.

**Location**: [`enforce_lamport_conservation` in `core/src/stages/execution.rs`](../core/src/stages/execution.rs)

#### GaslessRentCollector

Intercepts rent collection to prevent the runtime from debiting lamports from synthesized fee payer accounts. Works alongside GaslessCallback to maintain the zero-fee model.

**Location**: [`core/src/vm/gasless_rent_collector.rs`](../core/src/vm/gasless_rent_collector.rs)


### Stage 5: Settler

Batches execution results every 100ms (configurable) and commits to PostgreSQL, the source of truth, mirroring each batch to the Redis cache if one is configured. The settler writes:
- Modified accounts
- Transaction records
- Block metadata (slot, blockhash, timestamp)

The mirror is best-effort and covers only what the cache can serve: point lookups by pubkey, signature and slot, plus the chain tip. Ranges, history and counters are read from PostgreSQL, because a short answer from a partial mirror is indistinguishable from a complete one. A failed cache write drops the keys it would have updated so reads miss and resolve against PostgreSQL, and leaves the cached tip behind, which makes the next batch rebuild the cache. It is also bounded: a cache that has not answered within the budget is abandoned for that block, and one that keeps failing is left alone for a cooldown, then probed with a PING off the block path and rebuilt before it is mirrored to again.

The settler also caps the settled account bytes it buffers between ticks. Once a tick's buffer reaches that budget it stops draining the executor queue, so the executor's bounded send applies backpressure upstream rather than letting one commit grow without limit. Blocks are still produced only on the tick, never early, and the executor splits an oversized batch into byte-bounded messages so a single already-executed batch cannot overshoot the budget. The commit itself binds each column as one array parameter, so bounding the buffer is what bounds the bind; the driver is held at sqlx 0.8 or later, where an oversized bind fails loudly instead of being truncated by the binary protocol's length prefix.

Finally, the settler notifies the executor's in-memory cache (BOB) of settled accounts, completing the feedback loop.

**Location**: [`core/src/stages/settle.rs`](../core/src/stages/settle.rs)


## Supported Programs

Solana Private Channels restricts which programs can execute in the payment channel. Transactions referencing unsupported programs are rejected at the RPC layer.

| Program | Status | Notes |
|---------|--------|-------|
| **SPL Token** | Supported | Full support including Token-2022 |
| **SPL Associated Token Account** | Supported | ATA creation and lookup |
| **SPL Memo** | Supported | Memo attachments |
| **System Program** | Supported | Native transfers and account creation |
| **Solana Private Channels Withdraw Program** | Supported | Token burns for withdrawal flow |

**Source**: [`core/src/rpc/send_transaction_impl.rs`](../core/src/rpc/send_transaction_impl.rs)

### AdminVM Program Support

The AdminVM (used for operator mint operations) only supports SPL Token `InitializeMint`. All other instruction types are rejected.

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
| Max addresses per `simulateTransaction` | the transaction's own account count (matches Agave) | [`core/src/rpc/simulate_transaction_impl.rs`](../core/src/rpc/simulate_transaction_impl.rs) |
| Max encoded bytes for `simulateTransaction` accounts | 5 MB | [`core/src/rpc/constants.rs`](../core/src/rpc/constants.rs) |
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

Solana Private Channels does not implement slots or forks. The fork graph is stubbed — all blocks are final on write. There is no rollback mechanism.