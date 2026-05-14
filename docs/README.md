# Documentation

## Architecture

- **[Architecture Overview](ARCHITECTURE.md)** — Core components, transaction pipeline, and design decisions
- **[Solana Private Channels Core](CORE.md)** — Transaction pipeline, supported programs, and limitations
- **[Indexer & Operator](INDEXER.md)** — Datasource strategies, backfill, reconciliation, and operator pipeline

## Programs

- **[Escrow Program](ESCROW_PROGRAM.md)** — On-chain escrow: instructions, accounts, PDAs, and error codes
- **[Withdraw Program](WITHDRAW_PROGRAM.md)** — Channel-side burn mechanics, events, and error codes

## Guides

- **[Devnet Quickstart](DEVNET_QUICKSTART.md)** — End-to-end setup: escrow instance, deposit, transfer, withdraw
- **[Escrow Interaction Guide](ESCROW_INTERACTION_GUIDE.md)** — TypeScript examples for all escrow instructions
- **[Withdrawing Guide](WITHDRAWING_GUIDE.md)** — Withdrawal flow from channel burn to Mainnet release

## Operations

- **[System Invariants](INVARIANTS.md)** — Safety and correctness invariants with implementation status
- **[Technical Requirements](TECHNICAL_REQUIREMENTS.md)** — Hardware specs, software versions, ports, and firewall rules
- **[Configuration & Operations](CONFIG.md)** — Service configuration, tuning guidelines, restart/recovery, and operational tools
