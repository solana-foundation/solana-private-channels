# Solana Private Channels Withdraw Program Overview

## Program ID

```
J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi
```

- [Instruction Details](#instruction-details)
- [Errors](#errors)

## Instructions

| Instruction | Description | Discriminator |
|-------------|-------------|---------------|
| [`WithdrawFunds`](#withdrawfunds) | Burns tokens from the user's token account and emits a withdrawal event with an optional destination | 0 |

### Instruction Details

#### WithdrawFunds
Burns tokens from the user's token account and emits a `WithdrawFundsEvent` containing the amount and destination. The `destination` is metadata only — it does not route tokens. The indexer decodes the instruction data (not the log) and triggers the corresponding `ReleaseFunds` on Mainnet.

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

## Errors

The program defines the following custom errors:

| Error Code | Error Name | Description |
|------------|------------|-------------|
| 0 | `InvalidMint` | Invalid mint provided |
| 1 | `ZeroAmount` | Withdrawal amount must be greater than zero |
