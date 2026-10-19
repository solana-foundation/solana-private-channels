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

## Cantina (Cantina Managed) — October 19th, 2026

A re-review of the full codebase, conducted September 21st to October 9th, 2026.
It identified 81 issues (7 High, 41 Medium, 33 Low); 79 are fixed and 2 Medium
findings are acknowledged.

| Report | Components in scope | Audited commit |
| --- | --- | --- |
| [Re-review](cantina/solana-private-channels-re-review-2026-10-19.pdf) | escrow program, withdraw program, core, indexer, gateway, auth, devnet scripts | `641a5f1b` |
