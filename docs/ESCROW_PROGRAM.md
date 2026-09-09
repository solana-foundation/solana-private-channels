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
| [`ResetSmtRoot`](#resetsmtroot) | Reset the SMT root for the instance (operator-only) | 8 |
| [`EmitEvent`](#emitevent) | Emit event via CPI | 228 |

### Instruction Details

#### CreateInstance
Creates a new escrow instance with the specified admin.

Discriminator: `0`

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `bump` | u8 | PDA bump seed for instance account |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `admin` | ✓ | | Admin of Instance |
| 2 | `instance_seed` | ✓ | | Instance seed signer for PDA derivation |
| 3 | `instance` | | ✓ | Instance PDA to be created |
| 4 | `system_program` | | | System program |
| 5 | `event_authority` | | | Event authority PDA for emitting events |
| 6 | `private_channel_escrow_program` | | | Current program for CPI |

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
| `new_withdrawal_root` | [u8; 32] | New withdrawal transactions root |
| `transaction_nonce` | u64 | Transaction nonce |
| `sibling_proofs` | [u8; 512] | Sibling proofs (flattened as 512 bytes: 16 proofs × 32 bytes each) |

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `operator` | ✓ | | Operator releasing the funds |
| 2 | `instance` | | ✓ | Instance PDA to validate and update |
| 3 | `operator_pda` | | | Operator PDA to validate operator permissions |
| 4 | `mint` | | | Token mint being released |
| 5 | `allowed_mint` | | | AllowedMint PDA to validate mint is allowed |
| 6 | `user_ata` | | ✓ | User's Associated Token Account for this mint |
| 7 | `instance_ata` | | ✓ | Instance's Associated Token Account (escrow) for this mint |
| 8 | `token_program` | | | Token program for the mint |
| 9 | `associated_token_program` | | | Associated Token program |
| 10 | `event_authority` | | | Event authority PDA for emitting events |
| 11 | `private_channel_escrow_program` | | | Current program for CPI |

#### ResetSmtRoot
Resets the SMT root for the instance (operator-only).

Discriminator: `8`

**Parameters:** None

**Accounts:**
| Account | Name | Signer | Writable | Description |
|---------|------|--------|----------|-------------|
| 0 | `payer` | ✓ | ✓ | Transaction fee payer |
| 1 | `operator` | ✓ | | Operator resetting the SMT root |
| 2 | `instance` | | ✓ | Instance PDA to reset |
| 3 | `operator_pda` | | | Operator PDA to validate operator permissions |
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

## Accounts

| Account | Description | Discriminator |
|-------------|-------------|---------------|
| Instance | Escrow instance that holds token funds and manages operators | 0 |
| Operator | Authorized operator for an instance that can release funds | 1 |
| AllowedMint | Token mint allowed in an instance, holding its deposit and withdrawal gates | 2 |

### Instance
Represents an escrow instance that holds token funds and manages operators.

**PDA Derivation**: `["instance", instance_seed]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `version` | u8 | Instance version |
| `instance_seed` | Pubkey | Unique seed for this instance |
| `admin` | Pubkey | Authority that controls the instance |
| `withdrawal_transactions_root` | [u8; 32] | Sparse Merkle Tree root for withdrawal transactions |
| `current_tree_index` | u64 | Current tree index to prevent double spending |

### Operator
Represents an authorized operator for an instance that can release funds.

**PDA Derivation**: `["operator", instance_pda, wallet_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |

### AllowedMint
Represents a token mint that is allowed in an instance, and its two gates.

**PDA Derivation**: `["allowed_mint", instance_pda, mint_pubkey]`

| Field | Type | Description |
|-------|------|-------------|
| `bump` | u8 | PDA bump seed |
| `deposits_blocked` | bool | `Deposit` rejects this mint |
| `withdrawals_blocked` | bool | `ReleaseFunds` rejects this mint |

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
| 11 | `InvalidSmtProof` | Invalid SMT proof provided |
| 12 | `InvalidTransactionNonceForCurrentTreeIndex` | Invalid transaction nonce for current tree index |
| 13 | `UnexpectedTreeIndex` | Unexpected current tree index for SMT root reset |
| 14 | `DepositsBlockedForMint` | Deposits are blocked for this mint |
| 15 | `WithdrawalsBlockedForMint` | Withdrawals are blocked for this mint |

## Other Constants

- **Instance Version**: 1
- **Tree Height**: 16
- **Max Tree Leaves**: 65536
- **Empty Tree Root** (Pre-computed root hash for empty Sparse Merkle Tree): `[143, 230, 177, 104, 146, 86, 192, 211, 133, 244, 47, 91, 190, 32, 39, 162, 44, 25, 150, 225, 16, 186, 151, 193, 113, 211, 229, 148, 141, 233, 43, 235]`
- **Empty Leaf**: `[0u8; 32]`
- **Non-Empty Leaf Hash**: SHA256 hash of `[1u8; 32]`
