# Technical Requirements

This document specifies the hardware, software, and network requirements for
running the Solana Private Channels stack.

## Reference machine specs

A single recommended floor for running the full stack on one host. Grow from
here along the [Scalability dimensions](#scalability-dimensions) when this
floor stops being enough.

| vCPUs | RAM | Disk | Network |
|---|---|---|---|
| **24** | **32 GB** | **300 GB NVMe** (state + 1–3 days hot history) | **1 Gbps** |

This colocates the full stack on a single host: one write node, one read
node, both Postgres instances, indexers, operators, and the gateway. To
grow beyond it, see [Scalability dimensions](#scalability-dimensions).

### Per-component sizing (single host or split deployment)

The dominant CPU consumer at high TPS is the write node's pipeline; the
gateway and Postgres primary are the next two. Other services are cheap and
mostly I/O-bound.

| Component | vCPUs (~1k TPS) | RAM |
|---|---|---|
| Write node | 3.7–4.3 | 3.2–3.7 GB |
| Postgres replica | 0.8–0.9 | 2.9–5.2 GB |
| Postgres primary | 0.9 | 0.5–1.0 GB † |
| Gateway  | 1.3 | 110–125 MB |
| Read node | 0.4–0.5 | 180–665 MB |
| Indexer (private channel) | 0.2 | 0.3–1.4 GB † |
| Postgres indexer | 0.1 | 230 MB |

† Postgres primary RAM scales with its in-memory cache size; the indexer's
RAM grows with the size of the indexed dataset.

Only the three Postgres containers need disk; size them per
[Storage sizing](#storage-sizing) and use NVMe for the primary.

*Streamer, Solana indexer, and Solana/private channel operators each take <0.1 core
and ~30 MB RAM, and are not material to sizing.*

*Measured 2026-05-05 on a 16-core / 62 GB host.*

### Storage sizing

Storage has two independent drivers.

**1. Working state, token accounts.** ~165 bytes per token account.

| Token accounts | Disk |
|---|---|
| 100k | 17 MB |
| 1M | 165 MB |
| 10M | 1.65 GB |

**2. Block + transaction history.** Linear in TPS × retention.

| TPS | 1 day | 3 days | 7 days |
|---|---|---|---|
| 100 | 4.5 GB | 13.5 GB | 31.5 GB |
| 1k | 45 GB | 135 GB | 315 GB |
| 10k | 450 GB | 1.35 TB | 3.15 TB |

**Recommendation:** retain 1–3 days hot on NVMe; archive older blocks to
S3-compatible object storage. The PITR setup (see [PITR.md](PITR.md))
streams WAL to archive volumes, wire those volumes to S3 lifecycle policies.

---

## Software requirements

| Software | Minimum | Purpose |
|---|---|---|
| [Rust](https://rust-lang.org/tools/install/) | 1.91+ | Build (`rust-toolchain.toml` is authoritative) |
| [Solana CLI](https://solana.com/docs/intro/installation) | 3.1.13 (see [`versions.env`](../versions.env)) | Program deployment; `make install-toolchain` |
| [Docker](https://docs.docker.com/get-docker/) | 26.0+ | Container runtime |
| [Docker Compose](https://docs.docker.com/compose/install/) | 2.20+ | Stack orchestration |
| [PostgreSQL](https://www.postgresql.org/download/) | 16+ | Database (skip if using bundled containers) |
| [pnpm](https://pnpm.io/installation) | 10.0+ | TypeScript client builds |


---

## Network requirements

### Ports (defaults)

| Service | Port | Exposure |
|---|---|---|
| Gateway (RPC) | 8899 | Public, clients |
| Streamer (WebSocket) | 8902 | Public, clients |
| Grafana | 37429 | Restricted, operators only |

All other services bind to internal ports on the Docker network and don't
need to be exposed at the host firewall; see `docker-compose.yml` for defaults.

### Firewall

- **Public ingress:** 8899 (gateway), 8902 (streamer).
- **Outbound:** 443 to Solana mainnet RPC, 10000 to Yellowstone gRPC (if used).
- **Internal:** allow all between stack services on the private network.

---

## Scalability dimensions

When a single instance is no longer enough, scale along these dimensions
independently.

### 1. Write-node throughput (vertical)

Tune the write node's 5-stage pipeline via environment variables:

| Knob | Default | Effect |
|---|---|---|
| `PRIVATE_CHANNEL_SIGVERIFY_WORKERS` | 4 | Parallel Ed25519 verification workers. Primary CPU lever. |
| `PRIVATE_CHANNEL_SIGVERIFY_QUEUE_SIZE` | 1000 | Bounded backlog before sigverify. Trade burst tolerance for memory. |
| `PRIVATE_CHANNEL_MAX_TX_PER_BATCH` | 256 | Sequencer batch size; larger amortises executor overhead. |
| `PRIVATE_CHANNEL_BATCH_DEADLINE_MS` | 10 | Force-flush timer for partial batches. |
| `PRIVATE_CHANNEL_MAX_SVM_WORKERS` | 8 | Executor's parallel SVM threads per batch. |
| `PRIVATE_CHANNEL_BLOCKTIME_MS` | 100 | Settlement interval. |

See [`CONFIG.md`](CONFIG.md) for the full reference and restart procedure.

### 2. Read replicas (horizontal)

- Add Postgres read replicas behind additional read nodes.
- Front them with a load balancer, or use the gateway's built-in fanout.

### 3. Indexer parallelism (horizontal)

- Run multiple indexer instances over disjoint slot ranges.
- Prefer Yellowstone gRPC over RPC polling for real-time ingestion.
- Allocate 2–4 vCPUs per indexer instance.

See [`INDEXER.md`](INDEXER.md) for datasource strategies and Yellowstone setup.

### 4. Storage tiering

- **Hot:** NVMe for working state and 1–3 days of recent blocks.
- **Warm:** SSD for replicas and PITR archive volumes.
- **Cold:** S3-compatible object storage for older blocks; configure WAL
  archive volumes with lifecycle rules.

See [`PITR.md`](PITR.md) for the WAL archive setup.

---

## Support

For questions about technical requirements or deployment assistance:

- **GitHub Issues**: https://github.com/solana-foundation/solana-private-channels/issues
- **Stack Exchange**: Ask on https://solana.stackexchange.com/ (use the `private_channel` tag)
- **Documentation**: See [ARCHITECTURE.md](ARCHITECTURE.md) and [DEVNET_QUICKSTART.md](DEVNET_QUICKSTART.md)


