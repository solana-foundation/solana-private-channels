# contra-swap-program

Atomic delivery-versus-payment (DvP) escrow for P2P token swaps inside a Contra channel.

**Program ID:** `DzG1qJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC`

## What it does

Two parties — **`user_a`** (seller) and **`user_b`** (buyer) — agree to exchange `amount_a` of `mint_a` (the *asset* leg) for `amount_b` of `mint_b` (the *cash* leg). A third party, the **`settlement_authority`**, is the only address allowed to atomically settle the trade. Either party (or, via Cancel, the authority) can abort before settlement and recover their funded leg.

The trade lives as a single `SwapDvp` PDA with two associated escrow ATAs (one per leg). Each side funds its own leg by sending tokens to the corresponding escrow ATA via a plain SPL Transfer — there's no custom funding instruction, so custodian integrations need no special program call. Settlement transfers both legs in a single transaction, refunds any over-deposit to the depositor, and closes the PDA + escrows.

## State

```rust
SwapDvp {
    bump: u8,
    user_a: Pubkey,                              // seller
    user_b: Pubkey,                              // buyer
    mint_a: Pubkey,                              // asset
    mint_b: Pubkey,                              // cash
    settlement_authority: Pubkey,
    amount_a: u64,
    amount_b: u64,
    expiry_timestamp: i64,                       // settlement rejected after this
    nonce: u64,                                  // disambiguates DvPs sharing other seeds
    earliest_settlement_timestamp: Option<i64>,  // optional lower bound on settlement
}
```

PDA seeds: `[b"dvp", settlement_authority, user_a, user_b, mint_a, mint_b, nonce.to_le_bytes(), bump]`.

## Lifecycle

```
                      ┌─────────────────────┐
                      │       Create        │  permissionless
                      └──────────┬──────────┘
                                 │
                ┌────────────────┴────────────────┐
                ▼                                 ▼
         ┌─────────────┐                  ┌─────────────┐
         │ SPL Transfer│                  │ SPL Transfer│  user_a / user_b
         │ → escrow A  │                  │ → escrow B  │  (raw SPL — no
         └──────┬──────┘                  └──────┬──────┘   program call)
                │                                │
                └────────────────┬───────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
 ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
 │  Reclaim X  │          │   Settle    │          │ Cancel /    │
 │  (re-fund   │          │ (cash→A,    │          │ Reject      │
 │   allowed)  │          │  asset→B,   │          │ (refund all,│
 │             │          │  close all) │          │  close all) │
 └─────────────┘          └─────────────┘          └─────────────┘
```

## Instructions

| # | Name        | Signer                     | Effect                                                                          |
|---|-------------|----------------------------|---------------------------------------------------------------------------------|
| 0 | CreateDvp   | any                        | Allocates the SwapDvp PDA + both escrow ATAs. No funding.                       |
| 1 | ReclaimDvp  | `user_a` or `user_b`       | Drains signer's leg back to them. DvP stays open.                               |
| 2 | SettleDvp   | `settlement_authority`     | Transfers `amount_x` of each leg to recipients (cross), refunds any over-deposit to the depositor, closes SwapDvp + escrows. |
| 3 | CancelDvp   | `settlement_authority`     | Refunds any funded legs to depositors, closes SwapDvp + escrows.                |
| 4 | RejectDvp   | `user_a` or `user_b`       | Refunds any funded legs to depositors, closes SwapDvp + escrows.                |

### Notes

- **Funding via raw SPL Transfer.** There is no on-program funding instruction. Each side deposits its leg by issuing a plain SPL Transfer to the leg's escrow ATA (escrow ATAs are derivable from public state). This keeps custodian integrations free of any custom program call.
- **Settle clamps to `amount_x`.** Because anyone holding the leg mint can transfer tokens into an escrow ATA, the escrow may hold more than `amount_x`. Settle transfers exactly `amount_x` to the counterparty and refunds the surplus to the depositor's own-mint ATA, so over-deposits don't leak.
- **Reclaim/Cancel/Reject drain the escrow.** These instructions transfer the escrow's *actual* balance back to the depositor, leaving a 0-balance escrow that `CloseAccount` accepts.
- **Expiry.** `Reclaim` and `Settle` reject after `expiry_timestamp`. `Cancel` and `Reject` always work — otherwise an expired-but-funded DvP would strand funds. Funding via raw SPL Transfer is unauthenticated by the program; clients must avoid funding past expiry, but any tokens that do land are still recoverable via Cancel/Reject.
- **Earliest settlement.** If `earliest_settlement_timestamp` is set, `Settle` additionally rejects when `now < earliest`.

## Errors

| Code | Variant                       | When                                                                  |
|------|-------------------------------|-----------------------------------------------------------------------|
| 0    | `SignerNotParty`              | Reclaim/Reject signer is not `user_a` or `user_b`                     |
| 1    | `DvpExpired`                  | Reclaim/Settle after `expiry_timestamp`                               |
| 2    | `SettlementAuthorityMismatch` | Settle/Cancel signer is not `settlement_authority`                    |
| 3    | `SettlementTooEarly`          | Settle when `now < earliest_settlement_timestamp`                     |
| 4    | `LegNotFunded`                | Settle when an escrow holds less than its target amount               |
| 5    | `ExpiryNotInFuture`           | Create with `expiry_timestamp <= now`                                 |
| 6    | `EarliestAfterExpiry`         | Create with `earliest > expiry`                                       |
| 7    | `SelfDvp`                     | Create with `user_a == user_b`                                        |
| 8    | `SameMint`                    | Create with `mint_a == mint_b`                                        |
| 9    | `ZeroAmount`                  | Create with `amount_a == 0` or `amount_b == 0`                        |

## Build & test

```sh
make build              # generate clients + cargo-build-sbf
make unit-test          # program crate's #[cfg(test)] modules + JS client tests
make integration-test   # build + LiteSVM integration tests
make fmt                # cargo fmt + clippy + pnpm format
```

The integration tests live in `tests/integration-tests/` and run against the compiled `.so` via [LiteSVM](https://github.com/LiteSVM/litesvm). See `tests/integration-tests/src/` for one directory per instruction.

## Layout

```
program/         on-chain program (no_std, pinocchio-based)
clients/rust/    Codama-generated Rust client
clients/typescript/  Codama-generated TypeScript client
idl/             generated Codama IDL
tests/integration-tests/   LiteSVM integration tests
```
