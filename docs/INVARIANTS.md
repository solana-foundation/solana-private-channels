# System Invariants

This document defines the safety and correctness invariants that Solana Private Channels must uphold. Requirement levels follow [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119) (MUST, SHOULD, etc.).

---

## Solana Private Channels Core

| ID | Invariant | Level | Status | Ref |
|----|-----------|-------|--------|-----|
| C1 | A slot and all of its transactions + account changes MUST be written to DB as a single transaction | MUST | Done | #16 |
| C2 | The in-memory accounts DB MUST be in sync or ahead of the accounts DB on disk | MUST | Done | #20, #277 |
| C3 | Solana Private Channels MUST NOT allow two transactions with the same signature to both execute | MUST | Done | #15 |
| C4 | Solana Private Channels MUST NOT allow a transaction with an expired blockhash to execute | MUST | Done | #22 |
| C5 | Solana Private Channels MUST require all transactions to be signed | MUST | Done | #22 |
| C6 | Solana Private Channels MUST enforce the instructions allowlist | MUST | Done | #22 |
| C7 | Solana Private Channels MUST require admin signatures for admin instructions | MUST | Done | #22 |
| C8 | Solana Private Channels MUST reject all transactions that mix admin and non-admin instructions | MUST | Done | #22 |
| C9 | Solana Private Channels MUST reject all transactions with no instructions | MUST | Done | #22 |
| C10 | Finalized state MUST be based on DB state | MUST | Done | #36 |
| C11 | Solana Private Channels SHOULD support transaction/slot truncation | SHOULD | Done | #51 |
| C12 | If truncation is supported, Solana Private Channels MUST have a valid cold storage backup before truncating DB rows | MUST | Done | #60 |
| C13 | Solana Private Channels DB SHOULD use database backup and recovery | SHOULD | Done | #60 |
| C14 | A regular transaction MUST NOT increase the durable lamport supply beyond one lamport per account it creates, and MUST NOT alter any account it did not touch | MUST | Done | #115, #131 |
| C15 | Every account key an admitted transaction can reference MUST be resolvable at admission | MUST | Done | #182 |
| C16 | A slot MUST commit only if it extends the stored ledger, and a write-capable node MUST hold the writer lease | MUST | Done | #134 |

## On-chain Programs

| ID | Invariant | Level | Status | Ref |
|----|-----------|-------|--------|-----|
| P1 | Escrow program MUST require SPL transfers on escrow | MUST | Done | #18 |
| P2 | Escrow program MUST reject SPL transfers for unauthorized mints | MUST | Done | #10 |
| P3 | Withdrawal program MUST require admin transaction to release funds AND a valid withdrawal proof | MUST | Done | #9, #10, #29 |

## Indexer

| ID | Invariant | Level | Status | Ref |
|----|-----------|-------|--------|-----|
| I1 | Solana Private Channels indexer SHOULD NOT fall behind by more than 10 seconds relative to the most recent Solana Private Channels block | SHOULD | Done | #55 |
| I2 | Mainnet indexer SHOULD NOT fall behind by more than 10 seconds relative to the most recent mainnet block | SHOULD | Done | #55 |
| I3 | After downtime, indexers MUST backfill missed slots | MUST | Done | #48 |

## Operator

| ID | Invariant | Level | Status | Ref |
|----|-----------|-------|--------|-----|
| O1 | Withdrawals from escrow MUST NOT withdraw more than once | MUST | Done | #29, #9 |
| O2 | Issuances on Solana Private Channels MUST NOT issue more than once | MUST | Done | #26 |
| O3 | Failed withdrawals/issuances MUST fire an alert | MUST | Done | #40 |

## Global

| ID | Invariant | Level | Status | Ref |
|----|-----------|-------|--------|-----|
| G1 | On-chain escrow holdings MUST equal total user liabilities in Solana Private Channels | MUST | Done | #12, #39, #14 |
