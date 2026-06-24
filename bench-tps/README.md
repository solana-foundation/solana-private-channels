# private-channel-bench-tps

Load testing binary for the Solana Private Channels payment channel. Supports three flows:

| Flow | What it stresses |
|------|-----------------|
| \`transfer\` | Solana Private Channels SPL token transfer pipeline (dedup → sigverify → sequencer → executor → settler) |
| `deposit` | Solana escrow deposits (Solana → Solana Private Channels, measured end-to-end via operator-solana) |
| `withdraw` | Solana Private Channels burn + Solana release (Solana Private Channels → Solana, measured end-to-end via operator-private-channel) |

---

## Quick start

```bash
# 1. Copy env file and set values (defaults work for local runs)
cp bench-tps/.env.sample bench-tps/.env

# 2. Build (from repo root)
cargo build --release -p private-channel-bench-tps
# Binary: target/release/private-channel-bench-tps

# 3. Run (from repo root)
./bench-tps/scripts/run.sh                        # transfer flow, defaults
./bench-tps/scripts/run.sh deposit                # deposit flow
./bench-tps/scripts/run.sh withdraw               # withdraw flow
./bench-tps/scripts/run.sh --rebuild              # force-rebuild images + keypair
./bench-tps/scripts/run.sh --clean                # wipe volumes, start fresh
```

`run.sh` handles everything: generates the admin keypair, starts the full Docker
stack, waits for services to stabilise, runs the bench, then tears down all
containers on exit.

> **Shares the `private-channel` compose stack.** `run.sh` brings up the same
> `docker-compose.yml` (project `private-channel`) as `make docker-up`, using
> `bench-tps/.env` and its own generated DB passwords + admin keypair. It
> initializes the shared Postgres volumes with those credentials. So if you switch
> back to the regular stack afterward, run `make docker-clean` first — otherwise
> `make docker-up` (which reads `.env.local`) hits the bench-initialized volumes
> with mismatched passwords and the nodes fail with `password authentication
> failed` / the replica hangs at *Waiting for primary*. See the README's
> "Resetting after a credential change" note.

---

## Transfer flow

### What it does

Generates sustained Solana Private Channels SPL token transfers against the Solana Private Channels write-node
(via the gateway).  Each sender thread cycles through funded source accounts,
signing a unique transfer + memo instruction per transaction.

### How to run

```bash
./bench-tps/scripts/run.sh \
    --accounts 500 \
    --threads 8 \
    --duration 120

# Maximum sequencer contention (all senders target one destination)
./bench-tps/scripts/run.sh --num-conflict-groups 1 --threads 20
```

### What it measures

| Field | Source | Meaning |
|-------|--------|---------|
| `sent` | `AtomicU64` counter | Transactions dispatched by sender threads |
| `landed` | `getTransactionCount` delta | Transactions confirmed by the node |
| `dropped` | `sent - landed` | Rejected by dedup / sigverify / sequencer / network |
| `tps` | `landed / duration` | Effective pipeline throughput |

### Config parameters

| Flag | Env var | Default | Notes |
|------|---------|---------|-------|
| `--rpc-url` | `BENCH_RPC_URL` | `http://localhost:8898` | Solana Private Channels gateway endpoint |
| `--accounts` | `BENCH_ACCOUNTS` | `50` | Source keypairs; must be ≥ `--threads` |
| `--duration` | `BENCH_DURATION` | `60` | Load phase seconds |
| `--threads` | `BENCH_THREADS` | `4` | Concurrent sender threads |
| `--num-conflict-groups` | `BENCH_NUM_CONFLICT_GROUPS` | `== accounts` | Distinct destination ATAs (1 = max contention) |
| `--initial-balance` | `BENCH_INITIAL_BALANCE` | `1_000_000` | Raw token units per account |
| `--sender-sleep-ms` | `BENCH_SENDER_SLEEP_MS` | `5` | Throttle per-thread (0 = max throughput) |

---

## Deposit flow

### What it does

Sends `Deposit` instructions to the Solana validator's escrow program.
Each transaction transfers tokens from a depositor's Solana ATA into the shared
escrow instance ATA.  The full e2e path ends when `operator-solana` picks up
the indexed deposit and mints an equivalent amount on Solana Private Channels.

```
bench → Solana Deposit → indexer-solana indexes event
      → operator-solana mints on Solana Private Channels
```

### How to run

```bash
./bench-tps/scripts/run.sh deposit \
    --accounts 500 \
    --threads 8 \
    --duration 120
```

### What it measures

| Field | Source | Meaning |
|-------|--------|---------|
| `sent` | `AtomicU64` | Deposit txs dispatched |
| `solana_landed` | `getTransactionCount` delta on Solana | Confirmed by validator |
| `private_channel_minted` | `private_channel_operator_mints_sent_total{escrow}` delta | Solana Private Channels mints confirmed by operator |
| `drop` | `solana_landed - private_channel_minted` | Deposits landed but not yet minted (indexer/operator lag) |

`private_channel_minted` requires `BENCH_OPERATOR_METRICS_URL` to be set (operator-solana
exposes metrics on port 9102).

### Config parameters

| Flag | Env var | Default | Notes |
|------|---------|---------|-------|
| `--solana-rpc-url` | `BENCH_SOLANA_RPC_URL` | `http://localhost:18899` | Solana validator endpoint |
| `--accounts` | `BENCH_DEPOSIT_ACCOUNTS` | `20` | Depositor keypairs |
| `--duration` | `BENCH_DURATION` | `60` | Load phase seconds |
| `--threads` | `BENCH_THREADS` | `4` | Concurrent sender threads |
| `--initial-balance` | `BENCH_INITIAL_BALANCE` | `1_000_000` | Solana token units per account |
| `--operator-metrics-url` | `BENCH_OPERATOR_METRICS_URL` | — | `http://localhost:9102/metrics` for e2e tracking |
| `--instance-seed-keypair` | `BENCH_INSTANCE_SEED_KEYPAIR` | — | Reuse persistent escrow instance across runs |

---

## Withdraw flow

### What it does

The most complex flow — exercises the full cross-chain withdrawal path:

```
bench → Solana Private Channels WithdrawFunds (burn) → indexer-private-channel indexes event
      → operator-private-channel sends Solana ReleaseFunds → funds released on Solana
```

Setup creates both Solana and Solana Private Channels state: an escrow instance on Solana, Solana Private Channels ATAs
funded with tokens, and Solana ATAs so that `ReleaseFunds` can transfer to them.

### How to run

```bash
./bench-tps/scripts/run.sh withdraw \
    --accounts 500 \
    --threads 8 \
    --duration 120
```

### What it measures

| Field | Source | Meaning |
|-------|--------|---------|
| `sent` | `AtomicU64` | WithdrawFunds txs dispatched |
| `private_channel_burned` | `getTransactionCount` delta on Solana Private Channels | Burns confirmed by the write-node |
| `solana_released` | `private_channel_operator_mints_sent_total{withdraw}` delta | Solana ReleaseFunds confirmed by operator |
| `drop` | `private_channel_burned - solana_released` | Burns not yet released (indexer/operator lag) |

`solana_released` requires `BENCH_WITHDRAW_OPERATOR_METRICS_URL` (operator-private-channel
on port 9103).

### Config parameters

| Flag | Env var | Default | Notes |
|------|---------|---------|-------|
| `--rpc-url` | `BENCH_RPC_URL` | `http://localhost:8898` | Solana Private Channels gateway endpoint |
| `--solana-rpc-url` | `BENCH_SOLANA_RPC_URL` | `http://localhost:18899` | Solana validator endpoint |
| `--accounts` | `BENCH_WITHDRAW_ACCOUNTS` | `20` | Withdrawer keypairs |
| `--duration` | `BENCH_DURATION` | `60` | Load phase seconds |
| `--threads` | `BENCH_THREADS` | `4` | Concurrent sender threads |
| `--initial-balance` | `BENCH_INITIAL_BALANCE` | `1_000_000` | Solana Private Channels token units per account |
| `--operator-metrics-url` | `BENCH_WITHDRAW_OPERATOR_METRICS_URL` | — | `http://localhost:9103/metrics` for e2e tracking |
| `--instance-seed-keypair` | `BENCH_INSTANCE_SEED_KEYPAIR` | — | Must match `COMMON_ESCROW_INSTANCE_ID` in docker-compose |

---

## Log interpretation

### Setup phase

```
INFO Loaded admin keypair pubkey=... path=...
INFO Generated account keypairs count=500 elapsed_ms=12
INFO Mint initialized mint=... elapsed_ms=1840
INFO ATAs confirmed confirmed=500 elapsed_ms=2100
INFO Mint-to confirmed confirmed=500 elapsed_ms=1950
INFO Initial blockhash seeded — setup complete
```

Long `elapsed_ms` on confirmations is normal — the Solana Private Channels pipeline settles
asynchronously (1–3 s per batch).

### Load phase (logged every second)

```
INFO metrics tps=312 total_tx=18720 remaining_secs=47
INFO operator confirmed/s confirmed_per_sec=8 total_confirmed=240 program_type=withdraw
INFO blockhash_poller avg fetch latency fetches=25 avg_fetch_us=840
```

### Final summary

**Transfer:**
```
INFO Final summary duration_secs=60 sent=18900 landed=18540 dropped=360 drop_rate=1.9% tps=309.0
```

**Deposit:**
```
INFO Final summary duration_secs=60 sent=30000 solana_landed=28500 private_channel_minted=280 drop=28220 drop_rate=99.0% solana_tps=475.0 private_channel_tps=4.7
```
> High `drop` between `solana_landed` and `private_channel_minted` is expected during the run — the operator
> pipeline has latency. Re-run with a longer `--duration` to reach steady state.

**Withdraw:**
```
INFO Final summary duration_secs=60 sent=3000 private_channel_burned=2940 solana_released=21 drop=2919 drop_rate=99.3% private_channel_tps=49.0 solana_tps=0.4
```

### Common warnings

```
WARN initialize_mint send failed, retrying in 2s attempt=0 err=...502...
```
Write-node not ready — retry loop handles this automatically.

```
WARN blockhash_poller: get_latest_blockhash failed, keeping cached hash
```
Transient RPC error; safe to ignore if infrequent (cached hash valid ~15 s).

```
WARN sender: send_transaction failed err=...
```
Individual transaction rejected — increments `dropped`. Occasional failures are
expected; a high rate points to dedup (stale blockhash) or the node being
overloaded.


## Architecture

```
bench-tps/src/
├── main.rs            Entry point — dispatches to run_transfer / run_deposit / run_withdraw
├── args.rs            CLI argument definitions (clap + env vars)
├── types.rs           Shared constants, BenchConfig, BenchState, BatchQueue
├── setup.rs           Transfer setup — mint, ATAs, balances
├── setup_deposit.rs   Deposit setup — escrow instance, depositor accounts
├── setup_withdraw.rs  Withdraw setup — escrow instance, Solana Private Channels accounts, Solana ATAs
├── background.rs      Blockhash poller, metrics sampler, operator mints sampler
├── load.rs            Transfer generator + sender threads
├── load_deposit.rs    Deposit generator + sender threads
├── load_withdraw.rs   Withdraw generator + sender threads
└── rpc.rs             Helpers — send_parallel, poll_confirmations
```

### Three-phase structure (all flows)

**Phase 1 — Setup**: creates all on-chain state before load begins.

**Phase 2 — Background tasks** (concurrent with Phase 3):
- **Blockhash poller** — refreshes `BenchState::current_blockhash` every 80 ms.
  The dedup stage rejects transactions with a blockhash older than ~15 s.
- **Metrics sampler** — polls `getTransactionCount` every second to compute
  landed TPS and returns `(start, end)` counts for the final summary.
- **Operator mints sampler** (deposit/withdraw only) — scrapes
  `private_channel_operator_mints_sent_total` from the operator Prometheus endpoint
  every second for e2e confirmation tracking.

**Phase 3 — Load generation**:
```
Generator (async tokio task)
  reads current_blockhash → signs batch → push to BatchQueue
  yields if queue depth ≥ 32 (backpressure)
        │
        └─ BatchQueue (Mutex<VecDeque> + Condvar)
              │
        ┌─────┴──────┐
    Sender 0      Sender N   (OS threads, --threads count)
    pop batch → send_transaction (blocking RpcClient)
    sent_count += batch.len()
    sleep --sender-sleep-ms
```

### Uniqueness guarantee

Every transaction includes a **memo instruction** encoding the monotonically
increasing `tx_seq` counter as its data.  This ensures every transaction has a
unique signature regardless of account or blockhash reuse, preventing the
dedup stage from dropping them as duplicates.

### Binary location

`bench-tps` is a Cargo workspace member.  Cargo always writes the binary to
the workspace root target directory:

```
target/release/private-channel-bench-tps   ← correct
bench-tps/target/                  ← does not exist
```

Build command:
```bash
cargo build --release -p private-channel-bench-tps
```

---

## CPU pinning verification

```bash
# Container CPU sets
docker ps --filter "name=private-channel-" --format "{{.Names}}" \
  | xargs -I{} docker inspect --format '{{.Name}} {{.HostConfig.CpusetCpus}}' {}

# Bench process CPU set
taskset -pc $(pgrep -f private-channel-bench-tps)
```
