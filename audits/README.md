# Audits

Third-party security assessments of Solana Private Channels.

## OtterSec (Otter Audits, LLC) — September 2nd, 2026

The engagement was split across three reports, one per component. All three were
performed against source delivered from this repository.

| Report | Components in scope | Audited commit |
| --- | --- | --- |
| [Onchain](ottersec/solana-private-channels-onchain-2026-09-02.pdf) | escrow program, withdraw program | `977327b` |
| [Offchain](ottersec/solana-private-channels-offchain-2026-09-02.pdf) | core, indexer | `977327b`, `d2a25b3` |
| [Web2](ottersec/solana-private-channels-web2-2026-09-02.pdf) | gateway, auth, infrastructure (deployment, devnet scripts, docker-compose, environment configuration) | `977327b` |

Each report carries its own findings and per-finding status.
