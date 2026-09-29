# Solana Private Channels Withdraw Program Overview

## Program ID

```
J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi
```

- [Instruction Details](#instruction-details)
- [Accounts](#accounts)
- [Treasury](#treasury)
- [Errors](#errors)

## Instructions

| Instruction | Description | Discriminator |
|-------------|-------------|---------------|
| [`WithdrawFunds`](#withdrawfunds) | Burns tokens from the user's token account and emits a withdrawal event with an optional destination | 0 |
| [`SetWithdrawFeeConfig`](#setwithdrawfeeconfig) | Creates or overwrites the mint's withdraw fee config (mint authority only) | 1 |

### Instruction Details

#### WithdrawFunds
Burns tokens from the user's token account and emits a `WithdrawFundsEvent` containing the amount and destination. The `destination` is metadata only — it does not route tokens. The indexer decodes the instruction data (not the log) and triggers the corresponding `ReleaseFunds` on Mainnet.

Before burning, it transfers the mint's fee from the same token account to the treasury's, so the balance must cover `amount + fee` or nothing happens. The fee is never reminted when a Solana release fails, so each retry of a withdrawal that cannot settle costs the user the fee. The treasury itself withdraws without paying it. A mint whose fee is `0` charges nothing and never reads `treasury_token_account` (see the [zero-fee warning](ESCROW_INTERACTION_GUIDE.md#allowmint)). Read `treasury_token_account` from the [fee config](#withdrawfeeconfig); a mint with no config cannot be withdrawn, whatever its fee.

Discriminator: `0`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `amount` | u64 | Amount of tokens to burn |
| `destination` | Option&lt;Pubkey&gt; | Optional destination address recorded in the withdrawal event (defaults to user if omitted) |

**Events:**

The instruction emits a `WithdrawFundsEvent` via program log:

| Field | Type | Description |
|-------|------|-------------|
| `amount` | u64 | Amount of tokens burned |
| `destination` | Pubkey | Destination address for the withdrawal (user or specified destination) |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `user` | ✓ | | User initiating the withdrawal |
| 1 | `mint` | | ✓ | Token mint |
| 2 | `token_account` | | ✓ | Source token account |
| 3 | `token_program` | | | Token program |
| 4 | `associated_token_program` | | | Associated token program |
| 5 | `withdraw_fee_config` | | | Fee config PDA for the mint |
| 6 | `treasury_token_account` | | ✓ | Treasury token account the fee is paid to |

#### SetWithdrawFeeConfig
Creates the mint's fee config on first use and overwrites it on later calls whose `fee_slot` is at or after the stored one. Only the mint authority can sign it. The operator sends it in every deposit mint transaction with the fee and slot of the escrow's latest `AllowMint` and its own admin as the treasury, so re-allowing a mint with a new fee reprices it from the next deposit on. Deposits land in any order, so a deposit built before a reprice carries an older `fee_slot`; its write succeeds without changing the config, and the deposit still mints.

Every failure is a custom error. The deposit transaction carries this instruction, and the operator treats `InvalidAccountData`, `UninitializedAccount` or `IncorrectProgramId` from it as a missing mint and retries forever.

Discriminator: `1`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `fee` | u64 | Fee in base units charged on top of each withdrawal. `0` is allowed and means no fee; see the [zero-fee warning](ESCROW_INTERACTION_GUIDE.md#allowmint) |
| `fee_slot` | u64 | Slot of the `AllowMint` the fee came from. A value older than the stored one leaves the config unchanged |
| `treasury` | Pubkey | Owner of the token account fees are paid to. Its ATA is derived and stored; the account itself may not exist yet |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `authority` | ✓ | ✓ | Mint authority, also pays for the config |
| 1 | `mint` | | | Token mint |
| 2 | `withdraw_fee_config` | | ✓ | Fee config PDA for the mint |
| 3 | `system_program` | | | System program |

## Accounts

#### WithdrawFeeConfig
Seeds: `["withdraw_fee_config", mint]`. 81 bytes, no discriminator (it is the program's only account type).

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump |
| `fee` | u64 | Fee in base units charged on top of each withdrawal |
| `fee_slot` | u64 | Slot of the `AllowMint` the fee came from; orders later writes |
| `treasury` | Pubkey | Withdraws without paying the fee |
| `treasury_token_account` | Pubkey | The treasury's ATA for this mint; fees are credited here |

## Treasury

The treasury is the operator admin (`ADMIN_SIGNER`), the same key that pays Solana fees for `ReleaseFunds`. Collected fees sit in its channel ATA. It moves them out with an ordinary `WithdrawFunds`, for example `scripts/devnet/src/bin/withdraw.rs`, and pays no fee doing so. To reprice a mint, run the escrow's `AllowMint` again with the new fee.

## Errors

The program defines the following custom errors:

| Error Code | Error Name | Description |
|------------|------------|-------------|
| 0 | `InvalidMint` | Invalid mint provided |
| 1 | `ZeroAmount` | Withdrawal amount must be greater than zero |
| 2 | `InvalidFeeConfig` | Fee config is not the mint's PDA or its data is malformed |
| 3 | `FeeConfigNotInitialized` | No fee config exists for this mint yet |
| 4 | `InvalidMintAuthority` | Signer is not the mint authority |
| 5 | `InvalidTreasuryAccount` | Fee destination is not the configured treasury token account |
| 6 | `InvalidSystemProgram` | System program account is not the system program |
