# Solana Private Channels (SPC) JSON-RPC Divergence

## 1. Summary

The JSON-RPC *API surface* SPC exposes is consistent with Solana's. Every implemented method re-uses Solana's method name, request/response types, and config structs (imported directly from `solana-rpc-client-api`), so a Solana-shaped client gets a Solana-shaped reply back. Where SPC's universe is smaller - no validators, no native token supply, no leader rotation, one infinite epoch - the response *values* reflect that, but the wire contract still matches.

That said, divergence shows up in three distinct buckets:

1. **Coverage gaps.** SPC implements 22 of Solana's ~50 methods. See [section 5](#5-appendix-solana-rpc-methods-missing-from-spc) for the full list.
2. **Internal functionality gaps in implemented methods.** Same wire shape, less behavior - `minContextSlot` accepted-and-ignored, `searchTransactionHistory` accepted-and-ignored, `simulateTransaction` not decoding base58 input etc.
3. **Semantic contract divergences** `getRecentBlockhash` returns the Solana-legacy constant `lamports_per_signature = 5000` that doesn't reflect SPC's gasless model.

## 2. Auth Model

### SPC gateway, with auth disabled (`JWT_SECRET` unset)

The gateway acts as a pure HTTP reverse proxy: it inspects the request body to find the `method` field, routes `sendTransaction` to the write upstream and everything else to the read upstream, adds CORS headers, and forwards. Liveness via GET `/health` (gateway-only) and readiness via GET `/ready` (probes both upstreams) are gateway-only additions.

### SPC gateway, with auth enabled (`JWT_SECRET` + `AUTH_DATABASE_URL` both set)

The gateway enforces RBAC on a fixed list of methods (`gateway/src/auth.rs`):

- **Operator-only methods:** `getBlock`, `getTransaction`, `simulateTransaction`. Missing/invalid JWT → 401 with JSON-RPC body `{"error":{"code":-32001,"message":"Unauthorized: valid JWT required"}}`. User-role JWT → 403 with `-32003` ("operator role required").
- **Account-gated methods:** `getAccountInfo`, `getTokenAccountBalance`, `getSignaturesForAddress`. JWT required. Operator role → pass through. User role → the gateway fetches the target account via an internal `getAccountInfo` and inspects bytes for the SPL Token owner field and looks the pubkey up in the auth service's Postgres `verified_wallets` table. Mismatch → 403, `-32002` ("account not owned by caller"). DB error → 500, `-32603`. Missing pubkey in params → 400, `-32602`.
- **All other methods (16 of them) are unauthenticated** even with auth on - they pass straight through to the read or write node.

---

## 3. Method-by-Method Comparison Table

> **Note on `commitment`:** SPC has a single linear timeline - one sequencer, no fork choice - so the Solana commitment levels `processed`/`confirmed`/`finalized` have no meaning here. The parameter is accepted (and serde-validated) on every handler that takes a Solana config, then discarded. Per-row mentions of this are omitted below; assume every method accepts `commitment` and ignores it.
>
> **`[auth]` marker:** when gateway auth is enabled (`JWT_SECRET` set), the method requires an `Authorization: Bearer <JWT>` header. See section 2 for the operator-only vs account-gated split. Otherwise the method conforms to Solana's wire contract.

### 3.1 Methods SPC implements

Ordered from most divergent → closest match.

| Method | Notes |
|---|---|
| `sendTransaction` | Program allowlist: SPL Token, ATA, Memo, System, Withdraw. Any other program will return `-32602`. `RpcSendTransactionConfig` accepted but ignored - no preflight (so failures surface at submit without sim logs / CU info), no `maxRetries`. Only `base64` input. |
| `simulateTransaction` **[auth]** | `sigVerify`, `accounts`, `accounts.encoding` honoured. `replaceRecentBlockhash`, `minContextSlot`, `innerInstructions` ignored. Only base64. Operator-only under auth - wallets can't preview tx effects. |
| `getRecentBlockhash` | Solana-deprecated. Always returns `lamports_per_signature = 5000` - Solana-legacy constant, not SPC's gasless reality. |
| `getTokenAccountBalance` **[auth]** | Only SPL Token; Token-2022 rejected with `"Account is not a token account"`. Missing-mint/missing-account errors use `-32602` where Solana uses other codes. |
| `getSignaturesForAddress` **[auth]** | `limit`, `before`, `until` honoured. `minContextSlot` ignored. Default/max 1000 (matches Solana). |
| `getAccountInfo` **[auth]** | `encoding`, `dataSlice` honoured. `minContextSlot` ignored. |
| `getBlocks` | Max range 500_000 (matches Solana). When `end_slot` is omitted, SPC defaults to `start_slot + 500_000`; Solana defaults to latest slot. |
| `getSlot` | `minContextSlot` ignored. Returns latest stored slot or 0. |
| `getEpochSchedule` | SPC's actual schedule (one infinite epoch): `slotsPerEpoch = u64::MAX`, `leaderScheduleSlotOffset = 0`, `warmup = false`, `firstNormalEpoch = 0`, `firstNormalSlot = 0`. Same wire shape as Solana; explorers doing epoch math will overflow. |
| `getEpochInfo` | Reflects SPC's schedule faithfully - epoch always 0, `slotsInEpoch = u64::MAX`, `slotIndex` = current slot. |
| `getSupply` | All zeros - SPC has no native token supply. Block-explorers will render "0 SOL". |
| `getVoteAccounts` | `{current: [], delinquent: []}` - SPC has no validators. |
| `getSlotLeaders` | `[]` - SPC has no leader rotation. Jito-style "predict next leader" lookups get nothing. |
| `isBlockhashValid` | Checks the Dedup stage's in-memory live-blockhash window via linear scan. Identical contract to Solana but window can be shorter than 150; older hashes return `false` indistinguishably from "never existed". |
| `getRecentPerformanceSamples` | Real data from SPC's pipeline; default/max 720 (matches Solana). Numbers reflect SPC, not mainnet - by design. |
| `getLatestBlockhash` | `lastValidBlockHeight = slot + 150` (literal, not derived from `max_blockhashes`). SPC keeps slot and block height equal so the value is internally consistent. No `getBlockHeight` - clients call `getSlot` instead. |
| `getSignatureStatuses` | `confirmation_status = Finalized`, `confirmations = None` on every found tx (correct under SPC's single timeline). `searchTransactionHistory` accepted but ignored. Malformed signatures return `None` rather than Solana's `INVALID_PARAMS` error. Max 256 sigs. |
| `getBlock` **[auth]** | `maxSupportedTransactionVersion`, `transactionDetails`, `rewards`, `encoding` honoured. `rewards` always `[]`; `numPartitions` always `None` - both SPC-faithful. |
| `getTransaction` **[auth]** | Real lookup. Only difference from Solana is the JWT requirement. |
| `getTransactionCount` | Backed by SPC's own counter. |
| `getFirstAvailableBlock` | Returns the earliest slot SPC has stored. |
| `getBlockTime` | Returns `Option<i64>` from SPC's stored block data. |

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

- `getBlockHeight`
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
- `getBlocksWithLimit`
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
