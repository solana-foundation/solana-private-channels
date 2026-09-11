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
| [`Deposit`](#deposit) | Deposit tokens from user ATA to instance escrow ATA (permissionless) | 6 |
| [`ReleaseFunds`](#releasefunds) | Release funds from escrow to user (operator-only) | 7 |
| [`RotateBitmap`](#rotatebitmap) | Rotate the withdrawal bitmap to the next generation (operator-only) | 8 |
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

Discriminator: `1`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `bump` | u8 | PDA bump seed for allowed mint account |

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
Deposits tokens from user ATA to instance escrow ATA (permissionless).

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

#### ReleaseFunds
Releases funds from escrow to user (operator-only).

Discriminator: `7`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `amount` | u64 | Amount of tokens to release |
| `user` | Pubkey | User receiving the funds (wallet address, not the ATA) |
| `transaction_nonce` | u64 | Transaction nonce to consume from the withdrawal bitmap |

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

Replay protection is the bitmap alone: the nonce's bit must be clear, and the
nonce must fall in the generation the bitmap currently covers. The instance is
read-only here, it only signs the transfer.

#### RotateBitmap
Rotates the withdrawal bitmap to the next generation (operator-only).

Clears every bit and increments the generation, so the next 65,536 nonces can be
released. `expected_generation` must match the bitmap's current generation, which
makes the instruction non-idempotent: a replayed rotation cannot skip a whole
generation of nonces.

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

#### EmitEvent
Invoked via CPI from another program to log event via instruction data.

Discriminator: `228`

**Parameters:** None (event data passed via instruction data)

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `event_authority` | ✓ | | Event authority PDA for emitting events |

## Accounts

| Account | Description | Discriminator |
|-------------|-------------|---------------|
| Instance | Escrow instance that holds token funds and manages operators | 0 |
| Operator | Authorized operator for an instance that can release funds | 1 |
| AllowedMint | Token mint allowed in an instance, holding its deposit and withdrawal gates | 2 |
| WithdrawalBitmap | Withdrawal nonce replay protection for an instance | 3 |

### Instance
Represents an escrow instance that holds token funds and manages operators.

**PDA Derivation**: `["instance", instance_seed]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `version` | u8 | Instance version |
| `instance_seed` | Pubkey | Unique seed for this instance |
| `admin` | Pubkey | Authority that controls the instance |

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
generation, which is what keeps the account a fixed size as volume grows. Only
`bump` and `generation` appear in the IDL: the bits are read by slicing at
offset 10, since a fixed 8192-byte field does not fit on the BPF stack and its
length varies with the `test-tree` feature.

### Operator
Represents an authorized operator for an instance that can release funds.

**PDA Derivation**: `["operator", instance_pda, wallet_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |

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
| `extensions` | u64 | Bitmask of the mint's Token-2022 `ExtensionType` discriminants (bit N = type N), 0 for a legacy mint; `Deposit` rejects a mismatch |
| `has_freeze_authority` | bool | Whether the mint had a freeze authority at `AllowMint`; `Deposit` rejects *gaining* one |

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

## Other Constants

- **Instance Version**: 1
- **Nonces Per Generation**: 65536
- **Bitmap Bytes**: 8192 (one bit per nonce)
- **Withdrawal Bitmap Account Size**: 8202 bytes (1 discriminator + 1 bump + 8 generation + 8192 bits)

Under the `test-tree` feature these shrink to 8 nonces in 1 byte, so integration
tests can cross a generation boundary without 65,536 withdrawals.
- **Non-Empty Leaf Hash**: SHA256 hash of `[1u8; 32]`
