# dvp-swap-program

Atomic delivery-versus-payment (DvP) escrow for P2P token swaps.

**Program ID:** `DzG1qJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC`

## What it does

Two parties — **`user_a`** (seller) and **`user_b`** (buyer) — agree to exchange `amount_a` of `mint_a` (the _asset_ leg) for `amount_b` of `mint_b` (the _cash_ leg). A third party, the **`settlement_authority`**, is the only address allowed to atomically settle the trade. Either party (or, via Cancel, the authority) can abort before settlement and recover their funded leg.

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

| #   | Name       | Signer                 | Effect                                                                                                                       |
| --- | ---------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 0   | CreateDvp  | any                    | Allocates the SwapDvp PDA, its nonce tombstone, and both escrow ATAs. No funding.                                           |
| 1   | ReclaimDvp | `user_a` or `user_b`   | Drains signer's leg back to them. DvP stays open.                                                                            |
| 2   | SettleDvp  | `settlement_authority` | Transfers `amount_x` of each leg to recipients (cross), refunds any over-deposit to the depositor, closes SwapDvp + escrows. |
| 3   | CancelDvp  | `settlement_authority` | Refunds any funded legs to depositors, closes SwapDvp + escrows.                                                             |
| 4   | RejectDvp  | `user_a` or `user_b`   | Refunds any funded legs to depositors, closes SwapDvp + escrows.                                                             |

### Notes

- **Funding via raw token transfer.** There is no on-program funding instruction. Each side deposits its leg by transferring tokens to the leg's escrow ATA (escrow ATAs are derivable from public state), so custodian integrations need no custom program call. Use `TransferChecked`, not an unchecked `Transfer`: Token-2022 mints with extension-aware transfer behavior reject unchecked transfers because they need the mint account. A `Pausable` mint needs the mint account supplied so Token-2022 can enforce the current pause state, and a `TransferHook` mint needs the same hook account resolution as terminal transfers (hook program, validation PDA, and the resolved `ExtraAccountMetaList` accounts). A plain unchecked transfer can fail for these mints even though `CreateDvp` accepts them.
- **Settle clamps to `amount_x`.** Because anyone holding the leg mint can transfer tokens into an escrow ATA, the escrow may hold more than `amount_x`. Settle transfers exactly `amount_x` to the counterparty and refunds the surplus to the depositor's own-mint ATA, so over-deposits don't leak.
- **Reclaim/Cancel/Reject drain the escrow.** These instructions transfer the escrow's _actual_ balance back to the depositor, leaving a 0-balance escrow that `CloseAccount` accepts.
- **Closed-account rent goes to the closer, not the create payer.** `CreateDvp`'s payer funds the SwapDvp PDA and both escrow ATAs but isn't recorded as a rent recipient. On close those lamports go to the closing party: the settlement authority on `Settle`/`Cancel`, the rejecting `user_a`/`user_b` on `Reject`. If payer and closer differ, the payer subsidizes the closer.
- **Expiry.** Only `Settle` rejects after `expiry_timestamp`. `Reclaim`, `Cancel`, and `Reject` always work — otherwise an expired-but-funded DvP would strand funds. Reclaim is per-leg recovery: each party can pull their own leg back at any time, so a problem with the counterparty's leg can't lock a healthy leg behind the all-or-nothing Cancel/Reject path. Funding via raw SPL Transfer is unauthenticated by the program; clients must avoid funding past expiry, but any tokens that do land are still recoverable via Reclaim/Cancel/Reject.
- **Earliest settlement.** If `earliest_settlement_timestamp` is set, `Settle` additionally rejects when `now < earliest`.
- **Time is cluster time, not wall-clock.** All time gates use `Clock::unix_timestamp` (validator-vote median), which lags real time ~10 to 30s and can jump forward. Budget ~60s of drift: set `expiry` well past the real deadline and don't fund/settle/reclaim right at a boundary.
- **Nonces are single-use forever.** `CreateDvp` also creates a small nonce-tombstone PDA (seeds `[b"nonce", swap_dvp]`) that is never closed. A `(seeds, nonce)` combination can therefore only ever map to one trade: after a DvP closes, its PDA address can't be re-instantiated with new terms, so a deposit queued against the old escrow can't be captured by a recreated instance. Use a fresh nonce for each new DvP between the same parties + mints.
- **`CreateDvp` is permissionless — a record is not proof of agreement.** Only the payer signs; `user_a`, `user_b`, and `settlement_authority` do not. So anyone can create a `SwapDvp` for any parties with arbitrary terms (any non-zero amounts, any future expiry). The economic terms are stored in the account but are **not** part of the PDA seeds, so the address doesn't bind them. Two consequences clients must handle:
  - **Use a cryptographically random 64-bit `nonce` per trade.** A predictable nonce lets a third party squat the intended slot (and, after the real parties reject it, the tombstone burns that nonce — so they'd need a fresh one anyway). Random nonces make squatting/DoS impractical.
  - **Verify stored terms before acting.** Funders must read the on-chain `SwapDvp` and check `amount_a`/`amount_b`/`expiry`/`earliest`/`user_a`/`user_b`/`settlement_authority` against the agreed deal *before* depositing (escrow addresses derive only from PDA+mint, never from terms, so a raw transfer can land against terms you never agreed to). The `settlement_authority` must re-validate before `Settle`. The SDK should expose this as a create-and-verify / verify-before-fund helper rather than leaving it to integrators.
- **Token-2022.** Each leg carries its own `token_program` account, so a single DvP can mix legacy SPL and Token-2022 mints. `CreateDvp` rejects mints carrying amount-mutating extensions (`TransferFee`, `InterestBearing`, `ScaledUiAmount`, `ConfidentialTransfer`, `ConfidentialTransferFeeConfig`), and `NonTransferable` (a balance reaching the escrow could never be drained, stranding both legs) — only checked at Create, so funds remain recoverable if a mint's extensions change later. `PermanentDelegate`, `Pausable`, `MintCloseAuthority`, `DefaultAccountState`, and a mint freeze authority are accepted as-is. These are all trusted-authority risks the program cannot defend against: a permanent delegate can move escrowed tokens, a freeze authority can freeze the escrow ATA (or a frozen `DefaultAccountState` can make new ATAs start frozen), a pause authority can halt transfers, and a close authority can close a zero-supply mint and recreate it at the same address with a different extension set, changing transfer behavior mid-trade. Any of these can stall settlement or refunds until the authority cooperates. Treat the mint, freeze, and close authorities as trusted parties for the DvP's lifetime, and surface them in settlement UIs rather than presenting such mints as ordinary SPL tokens.
- **TransferHook.** Settle/Cancel/Reject/Reclaim issue `TransferChecked` CPIs and forward any trailing accounts to the token program as transfer-hook extras. Settle/Cancel/Reject split the trailing slice between the two legs via the `leg_a_extras_count: u8` data field (first `leg_a_extras_count` accounts go to leg A, rest to leg B); Reclaim has a single leg so all trailing accounts feed its one CPI. The client must resolve the hook's `ExtraAccountMetaList` off-chain. Hook extras are capped at 32 accounts per leg (the CPI account arrays are stack-allocated). A mutable hook is a trusted-authority risk: the hook authority can update the `ExtraAccountMetaList` after funding to require more than the cap, which bricks every terminal transfer on that leg until the authority relents. Treat mutable-hook mints as trusted and surface this in client risk checks before funding. Extras are also forwarded to the hook with the signer and writable flags the client set, so clients must never include a transaction signer or an unrelated writable account as a hook extra, or the hook program gains access to it. The DvP PDA transfer authority is safe (Token-2022 passes it to the hook read-only).
- **User ATAs are caller-managed.** The program creates escrow ATAs at `CreateDvp` but never creates user-side ATAs. Settle/Cancel/Reject/Reclaim assume the user destination ATAs already exist for any leg whose balance will be transferred; uninitialized destinations cause the `TransferChecked` CPI to fail and revert the instruction. Callers should add `CreateIdempotent` pre-instructions as needed. For `SettleDvp` this means **all four** user ATAs: the two that receive the agreed amounts (`user_a_ata_b`, `user_b_ata_a`) and the two surplus-refund ATAs (`user_a_ata_a`, `user_b_ata_b`). The surplus ATAs are not optional — anyone can dust an escrow, which forces a surplus refund, so a missing one reverts the whole settlement.
- **TransferHook + over-deposit caveat.** When Settle refunds a surplus, it issues a second `TransferChecked` CPI on the same mint with a different destination (the depositor's own ATA) and amount, but reuses the leg's extras slice. For hooks whose `ExtraAccountMetaList` resolves any account from the transfer's destination or amount (a valid but uncommon pattern), the surplus CPI will fail and revert Settle. Recovery: the depositor calls `Reclaim` to drain the leg, re-funds exactly `amount_x`, then Settle succeeds. Funds are never lost; only the over-deposit path is affected. Clients should fund hook-bearing escrows at exactly `amount_x` and warn on any surplus.

## Errors

| Code | Variant                       | When                                                                                                                                 |
| ---- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| 0    | `SignerNotParty`              | Reclaim/Reject signer is not `user_a` or `user_b`                                                                                    |
| 1    | `DvpExpired`                  | Settle after `expiry_timestamp`                                                                                                      |
| 2    | `SettlementAuthorityMismatch` | Settle/Cancel signer is not `settlement_authority`                                                                                   |
| 3    | `SettlementTooEarly`          | Settle when `now < earliest_settlement_timestamp`                                                                                    |
| 4    | `LegNotFunded`                | Settle when an escrow holds less than its target amount                                                                              |
| 5    | `ExpiryNotInFuture`           | Create with `expiry_timestamp <= now`                                                                                                |
| 6    | `EarliestAfterExpiry`         | Create with `earliest > expiry`                                                                                                      |
| 7    | `SelfDvp`                     | Create with `user_a == user_b`                                                                                                       |
| 8    | `SameMint`                    | Create with `mint_a == mint_b`                                                                                                       |
| 9    | `ZeroAmount`                  | Create with `amount_a == 0` or `amount_b == 0`                                                                                       |
| 10   | `BlockedMintExtension`        | Create with a Token-2022 mint carrying an unsupported extension (TransferFee, InterestBearing, ScaledUiAmount, ConfidentialTransfer, ConfidentialTransferFeeConfig, NonTransferable) |
| 11   | `SettlementAuthorityIsParty`  | Create with `settlement_authority` equal to `user_a` or `user_b`                                                                    |
| 12   | `SettlementAuthorityExecutable` | Create with an executable `settlement_authority` (can't be credited closed-account rent)                                          |
| 13   | `NonceAlreadyUsed`            | Create reusing a `(seeds, nonce)` that already has a nonce tombstone (the address was used by a prior DvP)                          |

## Build & test

```sh
make build              # generate clients + cargo-build-sbf
make unit-test          # program crate's #[cfg(test)] modules + JS client tests
make integration-test   # build + LiteSVM integration tests
make fmt                # cargo fmt + clippy + pnpm format
make verify-program-id  # pre-deploy: deploy keypair matches the declared program ID
```

The integration tests live in `tests/integration-tests/` and run against the compiled `.so` via [LiteSVM](https://github.com/LiteSVM/litesvm). See `tests/integration-tests/src/` for one directory per instruction.

### Deploying

The address in `declare_id!` (and therefore the IDL and generated clients) is a temporary placeholder. Its keypair is not available, so the program has not been deployed to it and that ID is not the program's real address. A real deployment must:

1. Generate a fresh program keypair (`solana-keygen new -o target/deploy/dvp_swap_program-keypair.json`).
2. Set `declare_id!` to its pubkey and regenerate the IDL and clients (`make build`).
3. Run `make verify-program-id` to confirm the deploy keypair matches the declared ID.
4. `solana program deploy --program-id target/deploy/dvp_swap_program-keypair.json target/deploy/dvp_swap_program.so`.

`cargo-build-sbf` writes a random deploy keypair that does not match `declare_id!`, so deploying without these steps (or without an explicit `--program-id`) publishes to the wrong address.

## Layout

```
program/         on-chain program (no_std, pinocchio-based)
clients/rust/    Codama-generated Rust client
clients/typescript/  Codama-generated TypeScript client
idl/             generated Codama IDL
tests/integration-tests/   LiteSVM integration tests
```
