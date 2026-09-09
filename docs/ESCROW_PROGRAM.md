# Solana Private Channels Escrow Program Overview

## Program ID

```
8msahYFvfeiiz3C2NzAorhfhAes5GaEThzmLWSLUHNkK
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
| [`BlockMint`](#blockmint) | Block previously allowed mints for the instance (admin-only) | 2 |
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
Blocks previously allowed mints for the instance (admin-only).

Discriminator: `2`

**Parameters:** None

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance` | | | Instance PDA to validate admin authority |
| 3 | `mint` | | | Token mint to be blocked |
| 4 | `allowed_mint` | | ✓ | Existing Allowed Mint PDA |
| 5 | `system_program` | | | System program for account creation |
| 6 | `event_authority` | | | Event authority PDA for emitting events |
| 7 | `private_channel_escrow_program` | | | Current program for CPI |

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
| AllowedMint | Token mint that is allowed for deposits in an instance | 2 |
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
Represents a token mint that is allowed for deposits in an instance.

**PDA Derivation**: `["allowed_mint", instance_pda, mint_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |

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
| 6 | `TransferHookNotAllowed` | Transfer hook extension not allowed |
| 7 | `InvalidOperatorPda` | Invalid operator PDA provided |
| 8 | `InvalidTokenAccount` | Invalid token account provided |
| 9 | `InvalidEscrowBalance` | Invalid escrow balance |
| 10 | `InvalidAllowedMint` | Invalid allowed mint |
| 11 | `InvalidWithdrawalBitmap` | Withdrawal bitmap account is malformed or not the expected PDA |
| 12 | `NonceAlreadyUsed` | Withdrawal nonce has already been released |
| 13 | `NonceOutsideCurrentGeneration` | Withdrawal nonce belongs to a different bitmap generation |
| 14 | `UnexpectedGeneration` | Bitmap rotation pre-state mismatch; blocks replaying a landed rotation |

## Other Constants

- **Instance Version**: 1
- **Nonces Per Generation**: 65536
- **Bitmap Bytes**: 8192 (one bit per nonce)
- **Withdrawal Bitmap Account Size**: 8202 bytes (1 discriminator + 1 bump + 8 generation + 8192 bits)

Under the `test-tree` feature these shrink to 8 nonces in 1 byte, so integration
tests can cross a generation boundary without 65,536 withdrawals.
- **Non-Empty Leaf Hash**: SHA256 hash of `[1u8; 32]`
