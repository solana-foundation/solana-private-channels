# Solana Private Channels Escrow Program Overview

## Program ID

```
9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU
```

- [Instruction Details](#instruction-details)
- [Accounts](#accounts)
- [Errors](#errors)
- [Other Constants](#other-constants)

## Instructions

| Instruction | Description | Discriminator |
|-------------|-------------|---------------|
| [`CreateInstance`](#createinstance) | Create a new escrow instance with the specified admin | 0 |
| [`AllowMint`](#allowmint) | Allow new token mints for the instance (admin-only) | 1 |
| [`BlockMint`](#blockmint) | Set the deposit and withdrawal gates on an allowed mint (admin-only) | 2 |
| [`AddOperator`](#addoperator) | Add an operator to the instance (admin-only) | 3 |
| [`RemoveOperator`](#removeoperator) | Remove an operator from the instance (admin-only) | 4 |
| [`SetNewAdmin`](#setnewadmin) | Set a new admin for the instance (current admin only) | 5 |
| [`Deposit`](#deposit) | Deposit tokens from user ATA to instance escrow ATA (permissionless unless deposits are whitelisted) | 6 |
| [`ReleaseFunds`](#releasefunds) | Release funds from escrow to user (operator-only) | 7 |
| [`RotateBitmap`](#rotatebitmap) | Rotate the withdrawal bitmap to the next generation (operator-only) | 8 |
| [`AddOrUpdateAllowedWallet`](#addorupdateallowedwallet) | Add a wallet to the instance whitelist, or rewrite its flags (admin-only) | 9 |
| [`RemoveAllowedWallet`](#removeallowedwallet) | Remove a wallet from the instance whitelist (admin-only) | 10 |
| [`SetWhitelistEnabled`](#setwhitelistenabled) | Set which directions the instance whitelist gates (admin-only) | 11 |
| [`EmitEvent`](#emitevent) | Emit event via CPI | 228 |

### Instruction Details

#### CreateInstance
Creates a new escrow instance with the specified admin.

Discriminator: `0`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `bump` | u8 | PDA bump seed for instance account |
| `bitmap_bump` | u8 | PDA bump seed for withdrawal bitmap account |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance_seed` | ✓ | | Instance seed signer for PDA derivation |
| 3 | `instance` | | ✓ | Instance PDA to be created |
| 4 | `withdrawal_bitmap` | | ✓ | Withdrawal bitmap PDA to be created |
| 5 | `system_program` | | | System program |
| 6 | `event_authority` | | | Event authority PDA for emitting events |
| 7 | `private_channel_escrow_program` | | | Current program for CPI |

The bitmap is created here, so every instance has one by construction. It is
8202 bytes, costing the payer roughly 0.058 SOL in rent.

#### AllowMint
Allows new token mints for the instance (admin-only).

The spl-token native mint (`So11111111111111111111111111111111111111112`) is rejected with `InvalidMint`, because the channel cannot create it. Passing it with a token program that does not own it fails the owner check first with `InvalidAccountOwner`.

Discriminator: `1`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `bump` | u8 | PDA bump seed for allowed mint account |
| `withdraw_fee` | u64 | Fee in base units charged on each channel withdrawal of this mint. `0` is allowed but leaves the operator paying every release; see the [zero-fee warning](ESCROW_INTERACTION_GUIDE.md#allowmint). The escrow records it in `AllowMintEvent`; the indexer reads it from the instruction data and the operator writes it to the channel's withdraw config on every deposit, so re-allowing with a new value (to or from `0`) reprices from the next deposit on. A re-allow also re-opens both gates and re-pins the profile; see the [reprice procedure](ESCROW_INTERACTION_GUIDE.md#allowmint) |
| `min_withdraw_amount` | u64 | Smallest amount one channel withdrawal of this mint may move, `0` for none. Recorded, read and repriced the same way as `withdraw_fee` |
| `min_deposit_amount` | u64 | Smallest amount one deposit of this mint must land in the escrow, net of any transfer fee, `0` for none. Unlike the two above it never reaches the channel: it is stored on `AllowedMint` and enforced by `Deposit`. The depositor keeps the tokens, so it does not pay for the recipient ATA and channel mint the operator funds; it limits how much of that one balance triggers at once, and only the withdraw fee bounds the total. A re-allow reprices it |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `mint` | | | Token mint to be allowed |
| 4 | `allowed_mint` | | ✓ | PDA of the Allowed Mint |
| 5 | `instance_ata` | | ✓ | Instance Escrow account for specified mint |
| 6 | `system_program` | | | System program |
| 7 | `token_program` | | | Token program |
| 8 | `associated_token_program` | | | Associated Token program |
| 9 | `event_authority` | | | Event authority PDA for emitting events |
| 10 | `private_channel_escrow_program` | | | Current program for CPI |

#### BlockMint
Sets the deposit and withdrawal gates on an allowed mint (admin-only).

The two gates are independent, and both flags are absolute: passing `false` for one
re-opens that gate. The AllowedMint PDA is not closed, so blocking deposits leaves
already-escrowed balances withdrawable. `AllowMint` also re-opens both gates.

Discriminator: `2`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `block_deposits` | bool | Reject new deposits for this mint |
| `block_withdrawals` | bool | Reject fund releases for this mint |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `mint` | | | Token mint whose gates are being set |
| 4 | `allowed_mint` | | ✓ | Existing Allowed Mint PDA |
| 5 | `event_authority` | | | Event authority PDA for emitting events |
| 6 | `private_channel_escrow_program` | | | Current program for CPI |

#### AddOperator
Adds an operator to the instance (admin-only).

Discriminator: `3`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `bump` | u8 | PDA bump seed for operator account |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `operator` | | | Operator public key to be added |
| 4 | `operator_pda` | | ✓ | Operator PDA to be created |
| 5 | `system_program` | | | System program |
| 6 | `event_authority` | | | Event authority PDA for emitting events |
| 7 | `private_channel_escrow_program` | | | Current program for CPI |

#### RemoveOperator
Removes an operator from the instance (admin-only).

Discriminator: `4`

**Parameters:** None

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `operator` | | | Operator public key to be removed |
| 4 | `operator_pda` | | ✓ | Existing Operator PDA |
| 5 | `system_program` | | | System program |
| 6 | `event_authority` | | | Event authority PDA for emitting events |
| 7 | `private_channel_escrow_program` | | | Current program for CPI |

#### SetNewAdmin
Sets a new admin for the instance (current admin only).

Discriminator: `5`

**Parameters:** None

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `current_admin` | ✓ | | Current admin of Instance |
| 2 | `instance` | | ✓ | Instance PDA to update admin |
| 3 | `new_admin` | ✓ | | New admin public key |
| 4 | `event_authority` | | | Event authority PDA for emitting events |
| 5 | `private_channel_escrow_program` | | | Current program for CPI |

#### Deposit
Deposits tokens from user ATA to instance escrow ATA (permissionless unless deposits are whitelisted).

Fails with `ZeroAmount` when `amount` is 0 or the escrow receives nothing, and with `BelowMinimumDeposit` when the received amount is under the mint's `min_deposit_amount`. While deposits are whitelisted, fails with `WalletNotAllowed` unless `user` and the credited wallet (`recipient`, or `user`) are allowed to deposit.

Discriminator: `6`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `amount` | u64 | Amount of tokens to deposit |
| `recipient` | Option&lt;Pubkey&gt; | Optional recipient for Solana Private Channels tracking (wallet address, not the ATA; if None, defaults to user) |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `user` | ✓ | | User depositing tokens |
| 2 | `instance` | | | Instance PDA to validate |
| 3 | `mint` | | | Token mint being deposited |
| 4 | `allowed_mint` | | | AllowedMint PDA to validate mint is allowed |
| 5 | `user_ata` | | ✓ | User's Associated Token Account for this mint |
| 6 | `instance_ata` | | ✓ | Instance's Associated Token Account (escrow) for this mint |
| 7 | `system_program` | | | System program |
| 8 | `token_program` | | | Token program for the mint |
| 9 | `associated_token_program` | | | Associated Token program |
| 10 | `event_authority` | | | Event authority PDA for emitting events |
| 11 | `private_channel_escrow_program` | | | Current program for CPI |
| 12 | `user_allowed_wallet_pda` | | | AllowedWallet PDA of `user`; read only while deposits are whitelisted |
| 13 | `recipient_allowed_wallet_pda` | | | AllowedWallet PDA of the credited wallet; read only while deposits are whitelisted |

#### ReleaseFunds
Releases funds from escrow to user (operator-only). While withdrawals are whitelisted, fails with `WalletNotAllowed` unless `withdrawer` and `user` are allowed to withdraw.

Discriminator: `7`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `amount` | u64 | Amount of tokens to release |
| `user` | Pubkey | User receiving the funds (wallet address, not the ATA) |
| `transaction_nonce` | u64 | Transaction nonce to consume from the withdrawal bitmap |
| `withdrawer` | Pubkey | Wallet whose channel burn this release settles; may differ from `user` |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `operator` | ✓ | | Operator releasing the funds |
| 2 | `instance` | | | Instance PDA to validate and sign the transfer |
| 3 | `withdrawal_bitmap` | | ✓ | Withdrawal bitmap PDA to consume the nonce |
| 4 | `operator_pda` | | | Operator PDA to validate operator permissions |
| 5 | `mint` | | | Token mint being released |
| 6 | `allowed_mint` | | | AllowedMint PDA to validate mint is allowed |
| 7 | `user_ata` | | ✓ | User's Associated Token Account for this mint |
| 8 | `instance_ata` | | ✓ | Instance's Associated Token Account (escrow) for this mint |
| 9 | `token_program` | | | Token program for the mint |
| 10 | `associated_token_program` | | | Associated Token program |
| 11 | `event_authority` | | | Event authority PDA for emitting events |
| 12 | `private_channel_escrow_program` | | | Current program for CPI |
| 13 | `memo_program` | | | SPL Memo program; always required, but validated and invoked only when `user_ata` requires a memo |
| 14 | `user_allowed_wallet_pda` | | | AllowedWallet PDA of `user`; read only while withdrawals are whitelisted |
| 15 | `withdrawer_allowed_wallet_pda` | | | AllowedWallet PDA of `withdrawer`; read only while withdrawals are whitelisted |

Replay protection is the bitmap alone: the nonce's bit must be clear, and the
nonce must fall in the generation the bitmap currently covers. The instance is
read-only here, it only signs the transfer.

#### RotateBitmap
Rotates the withdrawal bitmap to the next generation (operator-only).

Clears every bit and increments the generation, so the next 65,536 nonces can be
released. `expected_generation` must match the bitmap's current generation, which
makes the instruction non-idempotent: a replayed rotation cannot skip a whole
generation of nonces.

The program does not require the current window to be full. When to rotate is
the operator's responsibility and is gated in the indexer, see
[Trust model](#trust-model).

Discriminator: `8`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `expected_generation` | u64 | Generation the caller expects the bitmap to be at |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `operator` | ✓ | | Operator rotating the bitmap |
| 2 | `instance` | | | Instance PDA the bitmap belongs to |
| 3 | `withdrawal_bitmap` | | ✓ | Withdrawal bitmap PDA to rotate |
| 4 | `operator_pda` | | | Operator PDA to validate operator permissions |
| 5 | `event_authority` | | | Event authority PDA for emitting events |
| 6 | `private_channel_escrow_program` | | | Current program for CPI |

#### AddOrUpdateAllowedWallet
Adds a wallet to the instance whitelist, or rewrites the flags of an existing entry (admin-only). Blocking both directions fails with `InvalidInstructionData`, since that entry would equal no entry; use `RemoveAllowedWallet` instead.

Discriminator: `9`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `bump` | u8 | PDA bump seed for the allowed wallet account |
| `block_deposits` | bool | Refuse this wallet on `Deposit` while deposits are whitelisted |
| `block_withdrawals` | bool | Refuse this wallet on `ReleaseFunds` while withdrawals are whitelisted |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `wallet` | | | Wallet to be added or updated |
| 4 | `allowed_wallet_pda` | | ✓ | AllowedWallet PDA to be created or updated |
| 5 | `system_program` | | | System program |
| 6 | `event_authority` | | | Event authority PDA for emitting events |
| 7 | `private_channel_escrow_program` | | | Current program for CPI |

#### RemoveAllowedWallet
Removes a wallet from the instance whitelist (admin-only). Closes the AllowedWallet PDA and returns its rent to the payer.

Discriminator: `10`

**Parameters:** None

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `wallet` | | | Wallet to be removed |
| 4 | `allowed_wallet_pda` | | ✓ | Existing AllowedWallet PDA |
| 5 | `system_program` | | | System program |
| 6 | `event_authority` | | | Event authority PDA for emitting events |
| 7 | `private_channel_escrow_program` | | | Current program for CPI |

#### SetWhitelistEnabled
Sets which directions the instance whitelist gates (admin-only). Both flags are absolute, and entries are kept either way. A 0.1.0 instance grows to the current layout, with the payer topping up its rent.

Discriminator: `11`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `whitelist_deposits` | bool | Require allowed wallets on `Deposit` |
| `whitelist_withdrawals` | bool | Require allowed wallets on `ReleaseFunds` |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | ✓ | Instance PDA to update |
| 3 | `system_program` | | | System program, for the rent top-up |
| 4 | `event_authority` | | | Event authority PDA for emitting events |
| 5 | `private_channel_escrow_program` | | | Current program for CPI |

#### EmitEvent
Invoked via CPI from another program to log event via instruction data.

Discriminator: `228`

**Parameters:** None (event data passed via instruction data)

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `event_authority` | ✓ | | Event authority PDA for emitting events |

## Trust model

The program trusts each kind of signer for a fixed set of decisions.

| Principal | Trusted for | Instructions |
|-----------|-------------|--------------|
| Admin | Instance configuration: the mint allowlist, the operator set, the wallet whitelist, and handing over the admin role | `AllowMint`, `BlockMint`, `AddOperator`, `RemoveOperator`, `AddOrUpdateAllowedWallet`, `RemoveAllowedWallet`, `SetWhitelistEnabled`, `SetNewAdmin` |
| Operator | Every release parameter (amount, recipient, withdrawer, allowlisted mint, nonce) and when to rotate the withdrawal bitmap | `ReleaseFunds`, `RotateBitmap` |
| Anyone | Creating an instance with a specified admin, who must also sign, and depositing their own tokens (allowed wallets only while deposits are whitelisted) | `CreateInstance`, `Deposit` |

Beyond the mint allowlist, the program constrains an operator in three ways.
Each nonce is released at most once, and only while the bitmap covers its
generation. A rotation must name the current generation, so a replayed one
cannot skip a generation. While withdrawals are whitelisted, a release's
withdrawer and destination must both be allowed to withdraw.

The program does not check a release's amount, recipient, withdrawer or mint
against the burn on the channel, and it does not check that a generation is
finished before it is rotated. The operator in the indexer makes those decisions
from its database, see [Rotation](WITHDRAWING_GUIDE.md#rotation) and the operator
invariants in [INVARIANTS.md](INVARIANTS.md#operator). The whitelist's withdrawer
check therefore trusts the operator to name the wallet that burned. A rotation that lands
early makes every unreleased nonce in the closed generation unreleasable. The
indexer then routes those withdrawals to its remint path. A row with earlier
broadcast signatures enters the automatic remint flow, which remints only once
it proves no release landed and otherwise sends the row to manual review. A row
with no signatures goes straight to manual review for an out-of-band remint.

The admin's control over an operator key is `RemoveOperator`. That separation is
on chain only. The indexer's operator process always loads the admin signer, uses
it to pay for rotations, and signs as the operator with it when `OPERATOR_SIGNER`
is unset (a set `OPERATOR_SIGNER` that fails to load stops the process instead),
so whoever holds that process holds both roles and
there is no one left to revoke anything. Containment requires an operator key
that is distinct from the admin key and kept outside that process.

## Accounts

| Account | Description | Discriminator |
|-------------|-------------|---------------|
| Instance | Escrow instance that holds token funds and manages operators | 0 |
| Operator | Authorized operator for an instance that can release funds | 1 |
| AllowedMint | Token mint allowed in an instance, holding its deposit and withdrawal gates | 2 |
| WithdrawalBitmap | Withdrawal nonce replay protection for an instance | 3 |
| AllowedWallet | A wallet on the instance whitelist, with per-direction blocks | 4 |

### Instance
Represents an escrow instance that holds token funds and manages operators.

**PDA Derivation**: `["instance", instance_seed]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `version` | u8 | Layout version: 1 for a 0.1.0 instance, which ends at `admin`, 2 for the current one |
| `instance_seed` | Pubkey | Unique seed for this instance |
| `admin` | Pubkey | Authority that controls the instance |
| `deposits_whitelisted` | bool | `Deposit` requires allowed wallets; set by `SetWhitelistEnabled`. Off on a 0.1.0 instance |
| `withdrawals_whitelisted` | bool | `ReleaseFunds` requires allowed wallets; set by `SetWhitelistEnabled`. Off on a 0.1.0 instance |
| `reserved` | [u8; 64] | Zeroed, for future fields |

### WithdrawalBitmap
Withdrawal nonce replay protection: one bit per nonce in the current generation.
Created alongside the instance and reused forever, so rent is a fixed one-time
cost regardless of withdrawal volume.

**PDA Derivation**: `["withdrawal_bitmap", instance_pda]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `generation` | u64 | Nonce window this bitmap covers: `nonce / 65536` |
| `bits` | [u8; 8192] | One bit per nonce; bit `nonce % 65536` is set on release |

`ReleaseFunds` rejects a nonce whose bit is already set, and rejects any nonce
outside the current generation. `RotateBitmap` clears the bits and advances the
generation, which is what keeps the account a fixed size as volume grows. It does
not check that the window is full, so a nonce still unreleased when its
generation is rotated past can never be released; rotation timing is the
operator's responsibility and is gated in the indexer. Only
`bump` and `generation` appear in the IDL: the bits are read by slicing at
offset 10, since a fixed 8192-byte field does not fit on the BPF stack and its
length varies with the `test-tree` feature.

### Operator
Represents an authorized operator for an instance that can release funds.

**PDA Derivation**: `["operator", instance_pda, wallet_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |

### AllowedWallet
A wallet on the instance whitelist. A wallet with no entry is refused in every whitelisted direction.

**PDA Derivation**: `["allowed_wallet", instance_pda, wallet_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `deposits_blocked` | bool | Refused on `Deposit` while deposits are whitelisted |
| `withdrawals_blocked` | bool | Refused on `ReleaseFunds` while withdrawals are whitelisted |
| `reserved` | [u8; 32] | Zeroed, for future fields |

### AllowedMint
Represents a token mint that is allowed in an instance, its two gates, and the mint
profile recorded when it was allowed.

**PDA Derivation**: `["allowed_mint", instance_pda, mint_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `deposits_blocked` | bool | `Deposit` rejects this mint |
| `withdrawals_blocked` | bool | `ReleaseFunds` rejects this mint |
| `decimals` | u8 | Mint decimals at `AllowMint`; `Deposit` rejects a mismatch |
| `token_program` | Pubkey | Token program at `AllowMint`; `Deposit` rejects a mismatch |
| `extensions` | u64 | Bitmask of the mint's Token-2022 `ExtensionType` discriminants (bit N = type N), 0 for a legacy mint; `Deposit` rejects a mismatch outside the metadata and group bits |
| `has_freeze_authority` | bool | Whether the mint had a freeze authority at `AllowMint`; `Deposit` rejects *gaining* one |
| `min_deposit_amount` | u64 | Smallest amount a deposit must land, set by `AllowMint`; `Deposit` rejects less, `0` for none |

A mint carrying `MintCloseAuthority` can be closed and recreated at the same address
with any of these changed. Closing requires zero supply, so that window is while the
escrow holds none of the mint — in practice between `AllowMint` and the first deposit,
which is also when the channel-side mint is initialized from the allow-time decimals.
`Deposit` compares them and fails with `MintProfileChanged`; an admin blocks and
re-allows to re-pin. A decimals change also needs the channel mint re-created,
since re-allow leaves it on the old decimals. `ReleaseFunds` does not compare them, since the escrow can only
hold a balance while the profile is unchangeable.

Freeze authority is compared in one direction only: it can be revoked but never
re-added, so losing one is the same mint behaving more safely while gaining one
means a recreate. The `extensions` mask pins *which* extensions exist, not their
contents — a transfer fee raised, a hook program swapped or a permanent delegate
rotated leaves it unchanged, and those stay issuer-trust decisions.

The metadata, group and group-member bits are recorded but not compared. An issuer
adds them to a live mint as a routine step, so comparing them would stop deposits on
a mint that never changed. They carry no power over transfers, and a recreate that
hides behind them still has to match decimals, token program, freeze authority and
every other extension bit.

## Errors

The program defines the following custom errors:

| Error Code | Error Name | Description |
|------------|------------|-------------|
| 0 | `InvalidEventAuthority` | Invalid event authority provided |
| 1 | `InvalidAta` | Invalid ATA provided |
| 2 | `InvalidMint` | Invalid mint provided |
| 3 | `InvalidInstanceId` | Instance ID invalid or does not respect rules |
| 4 | `InvalidInstance` | Invalid instance provided |
| 5 | `InvalidAdmin` | Invalid admin provided |
| 6 | `TransferHookNotAllowed` | Retired. Transfer-hook mints are supported; the code is kept so the ones after it do not shift |
| 7 | `InvalidOperatorPda` | Invalid operator PDA provided |
| 8 | `InvalidTokenAccount` | Invalid token account provided |
| 9 | `InvalidEscrowBalance` | Invalid escrow balance |
| 10 | `InvalidAllowedMint` | Invalid allowed mint |
| 11 | `InvalidWithdrawalBitmap` | Withdrawal bitmap account is malformed or not the expected PDA |
| 12 | `NonceAlreadyUsed` | Withdrawal nonce has already been released |
| 13 | `NonceOutsideCurrentGeneration` | Withdrawal nonce belongs to a different bitmap generation |
| 14 | `UnexpectedGeneration` | Bitmap rotation pre-state mismatch; blocks replaying a landed rotation |
| 15 | `DepositsBlockedForMint` | Deposits are blocked for this mint |
| 16 | `WithdrawalsBlockedForMint` | Withdrawals are blocked for this mint |
| 17 | `MintProfileChanged` | Mint no longer matches the profile recorded at AllowMint |
| 18 | `ZeroAmount` | Deposit requested or received zero tokens |
| 19 | `BelowMinimumDeposit` | Deposit received less than the mint's minimum deposit |
| 20 | `WalletNotAllowed` | The direction is whitelisted and the wallet has no entry, or its entry blocks it |
| 21 | `InvalidAllowedWalletPda` | AllowedWallet PDA does not belong to the wallet being checked |

## Other Constants

- **Instance Version**: 1
- **Nonces Per Generation**: 65536
- **Bitmap Bytes**: 8192 (one bit per nonce)
- **Withdrawal Bitmap Account Size**: 8202 bytes (1 discriminator + 1 bump + 8 generation + 8192 bits)

Under the `test-tree` feature these shrink to 8 nonces in 1 byte, so integration
tests can cross a generation boundary without 65,536 withdrawals.
