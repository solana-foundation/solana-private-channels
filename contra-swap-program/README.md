# contra-swap-program

Atomic delivery-versus-payment (DvP) escrow for P2P token swaps inside a Contra channel.

**Program ID:** `DzG1qJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC`

## What it does

Two parties — **`user_a`** (seller) and **`user_b`** (buyer) — agree to exchange `amount_a` of `mint_a` (the *asset* leg) for `amount_b` of `mint_b` (the *cash* leg). A third party, the **`settlement_authority`**, is the only address allowed to atomically settle the trade. Either party (or, via Cancel, the authority) can abort before settlement and recover their funded leg.

The trade lives as a single `SwapDvp` PDA with two associated escrow ATAs (one per leg). Each side funds its own leg into the corresponding escrow; settlement transfers both legs in a single transaction and closes the PDA + escrows.

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
         │   Fund A    │                  │   Fund B    │  user_a / user_b
         └──────┬──────┘                  └──────┬──────┘
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
| 1 | FundDvp     | `user_a` or `user_b`       | Tops up signer's leg escrow to `amount_x`.                                      |
| 2 | ReclaimDvp  | `user_a` or `user_b`       | Drains signer's leg back to them. DvP stays open.                               |
| 3 | SettleDvp   | `settlement_authority`     | Transfers both legs to recipients (cross), closes SwapDvp + escrows.            |
| 4 | CancelDvp   | `settlement_authority`     | Refunds any funded legs to depositors, closes SwapDvp + escrows.                |
| 5 | RejectDvp   | `user_a` or `user_b`       | Refunds any funded legs to depositors, closes SwapDvp + escrows.                |

### Notes

- **Top-up funding.** `FundDvp` transfers `amount_x − escrow_balance`, not a fixed `amount_x`. This is robust to anyone dropping tokens into the escrow ATA via a raw SPL Transfer (escrow PDAs are derivable from public state).
- **Drain semantics.** `Settle`, `Cancel`, `Reject`, and `Reclaim` all transfer the escrow's *actual* balance, not `dvp.amount_x`. Any dust above target rides along with the leg, and the escrow is left empty so `CloseAccount` accepts it.
- **Expiry.** `Fund`, `Reclaim`, and `Settle` reject after `expiry_timestamp`. `Cancel` and `Reject` always work — otherwise an expired-but-funded DvP would strand funds.
- **Earliest settlement.** If `earliest_settlement_timestamp` is set, `Settle` additionally rejects when `now < earliest`.

## Errors

| Code | Variant                       | When                                                                  |
|------|-------------------------------|-----------------------------------------------------------------------|
| 0    | `SignerNotParty`              | Fund/Reclaim/Reject signer is not `user_a` or `user_b`                |
| 1    | `DvpExpired`                  | Fund/Reclaim/Settle after `expiry_timestamp`                          |
| 2    | `LegAlreadyFunded`            | Fund called when escrow ≥ leg amount                                  |
| 3    | `SettlementAuthorityMismatch` | Settle/Cancel signer is not `settlement_authority`                    |
| 4    | `SettlementTooEarly`          | Settle when `now < earliest_settlement_timestamp`                     |
| 5    | `LegNotFunded`                | Settle when an escrow holds less than its target amount               |
| 6    | `ExpiryNotInFuture`           | Create with `expiry_timestamp <= now`                                 |
| 7    | `EarliestAfterExpiry`         | Create with `earliest > expiry`                                       |
| 8    | `SelfDvp`                     | Create with `user_a == user_b`                                        |
| 9    | `SameMint`                    | Create with `mint_a == mint_b`                                        |
| 10   | `ZeroAmount`                  | Create with `amount_a == 0` or `amount_b == 0`                        |

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
