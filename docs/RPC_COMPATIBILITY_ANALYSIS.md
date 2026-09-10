# Solana Private Channels (SPC) JSON-RPC Divergence

## 1. Summary

The JSON-RPC *API surface* SPC exposes is consistent with Solana's. Every implemented method re-uses Solana's method name, request/response types, and config structs (imported directly from `solana-rpc-client-api`), so a Solana-shaped client gets a Solana-shaped reply back. Where SPC's universe is smaller - no validators, no native token supply, no leader rotation, one infinite epoch - the response *values* reflect that, but the wire contract still matches.

That said, divergence shows up in three distinct buckets:

1. **Coverage gaps.** SPC implements 24 of Solana's ~50 methods. See [section 5](#5-appendix-solana-rpc-methods-missing-from-spc) for the full list.
2. **Internal functionality gaps in implemented methods.** Same wire shape, less behavior - `minContextSlot` accepted-and-ignored, `searchTransactionHistory` accepted-and-ignored, `simulateTransaction` not decoding base58 input etc.
3. **Semantic contract divergences** `getRecentBlockhash` returns the Solana-legacy constant `lamports_per_signature = 5000` that doesn't reflect SPC's gasless model.

## 2. Auth Model

### SPC gateway, with auth disabled (`JWT_SECRET` unset)

The gateway acts as a pure HTTP reverse proxy: it inspects the request body to find the `method` field, routes `sendTransaction` to the write upstream and everything else to the read upstream, adds CORS headers, and forwards. Liveness via GET `/health` (gateway-only) and readiness via GET `/ready` (probes both upstreams) are gateway-only additions.

### SPC gateway, with auth enabled (`JWT_SECRET` + `AUTH_DATABASE_URL` both set)

The gateway enforces RBAC on a fixed list of methods (`gateway/src/auth.rs`):

- **Operator-only methods:** `getBlock`, `getTransaction`, `simulateTransaction`. Missing/invalid JWT → 401 with JSON-RPC body `{"error":{"code":-32001,"message":"Unauthorized: valid JWT required"}}`. User-role JWT → 403 with `-32003` ("operator role required").
- **Account-gated methods:** `getAccountInfo`, `getTokenAccountBalance`, `getSignaturesForAddress`. JWT required. Operator role → pass through. User role → the gateway fetches the target account via an internal `getAccountInfo` and inspects bytes for the SPL Token owner field and looks the pubkey up in the auth service's Postgres `verified_wallets` table. A token account's delegate is accepted for `getAccountInfo` and `getTokenAccountBalance`, which read current state it can already spend, but not for `getSignaturesForAddress`: a delegation is a current spend authority and says nothing about who controlled the address when past transactions landed. The owner check is still current-state: an `AccountOwner` handoff via SPL `SetAuthority` hands the new owner every signature from before the transfer. Mismatch → 403, `-32002` ("account not owned by caller"). DB error → 500, `-32603`. Missing pubkey in params → 400, `-32602`. Read node unreachable, slow, or answering with an error → 503, `-32004`, which is retryable where 403 is not.
- **Error-bearing methods:** `getSignaturesForAddress` and `getSignatureStatuses` also have their responses rewritten for every caller but an Operator. A stored transaction is indexed under every account it touched and its status is readable by anyone holding the signature, so neither method's authorization proves the caller may see *why* execution failed. Every non-null `err` is replaced with `InstructionError(0, GenericError)`, as is the legacy `status` field `getSignatureStatuses` repeats it in. This applies to the caller's own transactions too: a wallet polling the signature it just submitted sees the marker, so `confirmTransaction` reports that instead of the real reason and clients cannot distinguish insufficient funds from any other failure. Operators keep the raw error for diagnostics.
- **All other methods (18 of them) are unauthenticated** even with auth on - they pass straight through to the read or write node.

None of this applies on the gateway's internal listener (`GATEWAY_INTERNAL_PORT`), which serves the operator's own services: no RBAC, raw errors. It is never published to the host, so no external caller can reach it.

---

## 3. Method-by-Method Comparison Table

> **Note on `commitment`:** SPC has a single linear timeline - one sequencer, no fork choice - so the Solana commitment levels `processed`/`confirmed`/`finalized` have no meaning here. The parameter is accepted (and serde-validated) on every handler that takes a Solana config, then discarded. Per-row mentions of this are omitted below; assume every method accepts `commitment` and ignores it.
>
> **`[auth]` marker:** when gateway auth is enabled (`JWT_SECRET` set), the method requires an `Authorization: Bearer <JWT>` header. See section 2 for the operator-only vs account-gated split. Otherwise the method conforms to Solana's wire contract.

### 3.1 Methods SPC implements

Ordered from most divergent → closest match.

| Method | Notes |
|---|---|
| `sendTransaction` | Program allowlist: SPL Token, ATA, Memo, Withdraw, Swap. System is admitted for `Transfer` only - the allocating variants (`CreateAccount`, `Allocate`, and their seeded forms) would let a caller write permanent account data that the gasless model never charges for, and no flow here needs the rest. This covers top-level instructions only; allowlisted programs still allocate at their own fixed sizes via CPI. Any other program, or a non-`Transfer` System instruction, returns `-32602`. `RpcSendTransactionConfig` accepted but ignored - no preflight (so failures surface at submit without sim logs / CU info), no `maxRetries`. Only `base64` input. A v0 message declaring address table lookups is rejected with `-32602`: no ALT program is admitted, so the lookup could never be resolved (lookup-free v0 and legacy are unaffected). |
| `simulateTransaction` **[auth]** | `sigVerify`, `accounts`, `accounts.encoding` honoured. `replaceRecentBlockhash`, `minContextSlot`, `innerInstructions` ignored. Only base64 for the transaction itself. `accounts.addresses` is capped at the transaction's own account count and `accounts.encoding` rejects `base58`/`binary`, both matching Agave. SPC-specific: the encoded `accounts` array is also capped at 5 MB, checked before anything is encoded, so a request repeating a large account is refused rather than served. Operator-only under auth - wallets can't preview tx effects. Rejects a v0 message declaring address table lookups with `-32602`, matching `sendTransaction`. |
| `getRecentBlockhash` | Solana-deprecated. Always returns `lamports_per_signature = 5000` - Solana-legacy constant, not SPC's gasless reality. |
| `getTokenAccountBalance` **[auth]** | Only SPL Token; Token-2022 rejected with `"Account is not a token account"`. Missing-mint/missing-account errors use `-32602` where Solana uses other codes. |
| `getSignaturesForAddress` **[auth]** | `limit`, `before`, `until` honoured. `minContextSlot` ignored. Default/max 1000 (matches Solana). Under auth, User role sees one uniform `err` per failed transaction instead of the real `TransactionError`; operators see the real one. |
| `getAccountInfo` **[auth]** | `encoding`, `dataSlice` honoured. `minContextSlot` ignored. |
| `getBlocks` | Max range 500_000 (matches Solana). When `end_slot` is omitted, SPC defaults to `start_slot + 500_000`; Solana defaults to latest slot. |
| `getBlocksWithLimit` | Max limit 500_000 (matches Solana). `limit = 0` returns `[]`. Unauthenticated, like `getBlocks`: it discloses only which slots produced a block, never transaction contents. |
| `getSlot` | `minContextSlot` ignored. Returns the live slot, or 0 on a fresh node. The settler ticks it every `blocktime_ms` and publishes it whether or not that tick produced a block, so it advances continuously while the node is idle and stays monotonic across a restart. **Most slots carry no block.** An idle node produces one block per heartbeat, so about nine in ten slots are empty and `getBlock` on one answers `-32007` (`SlotSkipped`), never a `null`; see the `getBlock` row for the full contract. Use `getBlocks`/`getBlocksWithLimit` to find slots that do hold a block, and `getBlockHeight` for the count of blocks produced. |
| `getBlockHeight` | `minContextSlot` ignored. Returns the count of blocks actually produced, or 0. It is not the slot: an idle node ticks about ten slots per produced block, so the two diverge. This is the clock a client compares a stored `lastValidBlockHeight` against to prove a status-less signature can no longer land. Feeding it to `getBlock` gets nothing; use `getSlot` for that. |
| `getEpochSchedule` | SPC's actual schedule (one infinite epoch): `slotsPerEpoch = u64::MAX`, `leaderScheduleSlotOffset = 0`, `warmup = false`, `firstNormalEpoch = 0`, `firstNormalSlot = 0`. Same wire shape as Solana; explorers doing epoch math will overflow. |
| `getEpochInfo` | Reflects SPC's schedule faithfully - epoch always 0, `slotsInEpoch = u64::MAX`, `slotIndex` = current slot. `absoluteSlot` reads the same counter as `getSlot` and `blockHeight` the same one as `getBlockHeight`, so the tip RPCs cannot disagree. |
| `getSupply` | All zeros - SPC has no native token supply. Block-explorers will render "0 SOL". |
| `getVoteAccounts` | `{current: [], delinquent: []}` - SPC has no validators. |
| `getSlotLeaders` | `[]` - SPC has no leader rotation. Jito-style "predict next leader" lookups get nothing. |
| `isBlockhashValid` | Checks the Dedup stage's in-memory live-blockhash window via linear scan. Identical contract to Solana but the window is `max_blockhashes` blocks, which an operator may configure below 150; older hashes return `false` indistinguishably from "never existed". |
| `getRecentPerformanceSamples` | Real data from SPC's pipeline; default/max 720 (matches Solana). Numbers reflect SPC, not mainnet - by design. |
| `getLatestBlockhash` | `lastValidBlockHeight = block_height + max_blockhashes - 1`, the last height at which the hash is still in the window, tracking the node's configured window rather than Solana's fixed 150. Both sides of a client's confirmation loop are block heights, and the dedup window evicts one entry per produced block, so the published deadline and the eviction rule are the same quantity. The response context stays a slot, as Solana reports it. The wall-clock duration of the window moves with load: roughly 15s under continuous load and 2.5min fully idle at the default 150. |
| `getSignatureStatuses` | `confirmation_status = Finalized`, `confirmations = None` on every found tx (correct under SPC's single timeline). `searchTransactionHistory` accepted but ignored. A storage or decode failure returns a `-32000` server error, never a `null` element, so a `null` means the signature is genuinely absent. A malformed signature fails the whole call with `-32602` invalid params, matching Solana, rather than nulling that one element. Max 256 sigs. |
| `getBlock` **[auth]** | `maxSupportedTransactionVersion`, `transactionDetails`, `rewards`, `encoding` honoured. `rewards` always `[]`; `numPartitions` always `None` - both SPC-faithful. A transaction the node cannot read fails the whole call with `-32000` rather than returning a block that silently omits it. **A slot with no block is an error, not a `null`**, matching Solana: `-32007` (`SlotSkipped`) for a slot the chain has passed, `-32004` (`BlockNotAvailable`) for one it has not reached. At idle roughly nine slots in ten are skipped, so this is the common answer, not the exception. One divergence: a slot pruned by `truncate` also answers `-32007` where Solana answers `-32001` (`BlockCleanedUp`), because `-32001` is already taken here by "Write operations not enabled"; use `getFirstAvailableBlock` to tell the two apart. |
| `getTransaction` **[auth]** | Real lookup. A storage or decode failure returns `-32000`, never a `null`. Only other difference from Solana is the JWT requirement. |
| `getTransactionCount` | Backed by SPC's own counter. |
| `getFirstAvailableBlock` | Returns the earliest slot SPC has stored. |
| `getBlockTime` | Returns `Option<i64>` from SPC's stored block data. Reads the same row as `getBlock`, so a storage or decode failure returns `-32000`, never a `null`. |

---

## 3a. Two Things That Will Bite a Stock Client

### Do not cache a blockhash. Fetch a fresh one per transaction.

The blockhash window is a block count (`max_blockhashes`, default 150), so its
wall-clock length moves with load: **about 15 seconds under continuous load, up
to about 150 seconds fully idle.** Quote clients the range, never a single
figure.

The loaded end of that range is shorter than what stock clients assume.
`@solana/web3.js` v1 caches a fetched blockhash for `BLOCKHASH_CACHE_TIMEOUT_MS`
(30 seconds) and reuses it for unsigned legacy transactions, so under load its
cache routinely hands out a hash older than this node's entire window. Any
hand-rolled "reuse the blockhash for about 30 seconds" heuristic has the same
problem.

What happens then is quiet and easy to misread: `sendTransaction` returns a
signature, and the transaction is dropped afterwards at the dedup stage because
its `recent_blockhash` has already left the window. The client is left polling a
signature that will never appear.

The guidance is: call `getLatestBlockhash` for every transaction, do not reuse
the result, and poll `getBlockHeight` against the `lastValidBlockHeight` that
came back with it.

### `getSlot` no longer implies a fetchable block.

`getSlot` reports a live slot height that advances every `blocktime_ms` whether
or not that tick produced a block. This is correct Solana processed-commitment
behaviour, and it means the slot it returns usually carries no block at all: at
idle a block is produced about once a second against a 100 ms tick.

So `getBlock(getSlot())` is no longer a safe pairing. On such a slot `getBlock`
returns `-32007`, as Solana does for a skipped slot. To walk blocks, use
`getBlocks` or `getBlocksWithLimit` to list the slots that actually produced one,
and use `getBlockHeight` when what you want is a count of blocks rather than a
slot number.

---

## 4. Client Integration With the Auth Gateway (No Solana SDK Fork Required)

### Premise

The SPC gateway adds an HTTP-layer auth check (`Authorization: Bearer <JWT>`) in front of a Solana-shaped JSON-RPC surface. The question for client integrators is: *do we need to fork or wrap `solana-rpc-client` to add this header?* The answer is **no**. The standard `RpcClient` exposes a constructor that takes a custom `HttpSender`, and `HttpSender` accepts a pre-configured `reqwest::Client`. Setting the `Authorization` header as a default header on that client makes every JSON-RPC call carry the token without changing a line of Solana SDK code.

### How the Flow Works

1. The client obtains a JWT out-of-band from the auth service (issued for an Operator or User role, with `iss=private-channel-auth`, `aud=private-channel-gateway`, finite `exp`).
2. The client builds a `reqwest::Client` whose `default_headers` include `Authorization: Bearer <JWT>`, wraps it in `HttpSender::new_with_client(url, client)`, and hands that to `RpcClient::new_sender(...)`.
3. Every `RpcClient` call (`get_account_info`, `send_transaction`, etc.) POSTs to the gateway with the header attached.
4. The gateway extracts the header (`hyper::header::AUTHORIZATION`), verifies the JWT, applies operator-only or account-gated rules per method (see §3), then forwards the body to the read or write upstream.
5. From the application's perspective the `RpcClient` looks and behaves like a normal Solana client; only construction differs.

### Working Snippet (Rust)

```rust
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use solana_rpc_client::{http_sender::HttpSender, rpc_client::RpcClient};
use solana_rpc_client_api::config::RpcClientConfig;
use solana_sdk::commitment_config::CommitmentConfig;

fn make_authed_client(gateway_url: &str, jwt: &str) -> anyhow::Result<RpcClient> {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {jwt}"))?);

    // NOTE: this reqwest must be 0.12.x (the version solana-rpc-client 3.1.x links).
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;

    let sender = HttpSender::new_with_client(gateway_url, http);
    Ok(RpcClient::new_sender(
        sender,
        RpcClientConfig::with_commitment(CommitmentConfig::confirmed()),
    ))
}
```

Usage is then identical to any other `RpcClient`:

```rust
let client = make_authed_client("https://gateway.example/", &jwt)?;
let account = client.get_account(&pubkey).await?;       // carries Authorization
let sig     = client.send_transaction(&tx).await?;      // carries Authorization
```

---

## 5. Appendix: Solana RPC methods missing from SPC

The list below enumerates the Solana JSON-RPC methods that have no SPC implementation. Calling them against an SPC node returns `-32601 Method not found`.

**Account / balance reads**

- `getBalance`
- `getMultipleAccounts`
- `getProgramAccounts`
- `getMinimumBalanceForRentExemption`
- `getLargestAccounts`
- `getTokenLargestAccounts`
- `getTokenAccountsByOwner`
- `getTokenAccountsByDelegate`
- `getTokenSupply`

**Cluster / node state**

- `getHealth`
- `getVersion`
- `getGenesisHash`
- `getIdentity`
- `getClusterNodes`
- `getStakeMinimumDelegation`
- `getStakeActivation`
- `getInflationGovernor`
- `getInflationRate`
- `getInflationReward`
- `getLeaderSchedule`
- `getMaxRetransmitSlot`
- `getMaxShredInsertSlot`
- `getHighestSnapshotSlot`
- `getSnapshotSlot`
- `minimumLedgerSlot`

**Slot / block / signature lookup**

- `getBlockProduction`
- `getBlockCommitment`
- `getSlotLeader` (singular; SPC has `getSlotLeaders` plural)
- `getConfirmedBlock` (deprecated alias of `getBlock`)
- `getConfirmedBlocks`
- `getConfirmedBlocksWithLimit`
- `getConfirmedSignaturesForAddress2`
- `getConfirmedTransaction`

**Transaction lifecycle**

- `getFeeForMessage`
- `getFees` (deprecated)
- `getFeeCalculatorForBlockhash` (deprecated)
- `requestAirdrop`

**WebSocket subscriptions (entire family)**

- `accountSubscribe` / `accountUnsubscribe`
- `blockSubscribe` / `blockUnsubscribe`
- `logsSubscribe` / `logsUnsubscribe`
- `programSubscribe` / `programUnsubscribe`
- `rootSubscribe` / `rootUnsubscribe`
- `signatureSubscribe` / `signatureUnsubscribe`
- `slotSubscribe` / `slotUnsubscribe`
- `slotsUpdatesSubscribe` / `slotsUpdatesUnsubscribe`
- `voteSubscribe` / `voteUnsubscribe`
