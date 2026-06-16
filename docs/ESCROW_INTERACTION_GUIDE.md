# Interacting with the Solana Private Channels Escrow Program

This guide provides a reference for all client-side actions for calling Solana Private Channels Escrow Program instructions.

## Overview

The Solana Private Channels Escrow Program manages token deposits to and withdrawals from the Solana Private Channels payment channel. The program supports:

- **Instance Management**: Create and configure escrow instances
- **Access Control**: Manage admins and operators
- **Token Whitelisting**: Control which tokens can be deposited
- **Deposits**: Lock tokens on Mainnet for minting on the Solana Private Channels payment channel
- **Withdrawals**: Release funds from escrow to user (note: this is handled by the Solana Private Channels Indexer/Operator and is not covered in this guide)


### Program Address
```
GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83
```

### Installation
```bash
pnpm add private-channel-escrow-program @solana/kit
```

## Table of Contents

1. [CreateInstance](#createinstance) - Create a new escrow instance
2. [AllowMint](#allowmint) - Whitelist a token mint for deposits
3. [Mint Risk Considerations](#mint-risk-considerations) - Authorities/extensions to vet before whitelisting
4. [BlockMint](#blockmint) - Revoke deposit permissions for a mint
5. [AddOperator](#addoperator) - Authorize a withdrawal operator
6. [RemoveOperator](#removeoperator) - Remove an operator
7. [SetNewAdmin](#setnewadmin) - Transfer admin control
8. [Deposit](#deposit) - Deposit tokens to Solana Private Channels
9. [PDA Reference](#pda-reference) - Reference of all PDA's used in the program

## CreateInstance

Creates a new escrow instance with a dedicated admin. Each Solana Private Channels deployment requires its own instance. Instance PDA's are seeded with the string literal "instance" and a unique pubkey (the instance seed).

Anyone can create an instance--the `admin` signer will have authority for managing subsequent instructions.

### TypeScript Example

```typescript
import {
  getCreateInstanceInstructionAsync,
  findInstancePda,
} from 'private-channel-escrow-program';
import { generateKeyPairSigner } from '@solana/kit';

// Generate unique instance seed (save securely for future Instance retrieval)
const instanceSeed = await generateKeyPairSigner();
const admin = await generateKeyPairSigner();
const payer = await generateKeyPairSigner();

// Build instruction with payer, admin, and instanceSeed as signers
const createInstanceIx = await getCreateInstanceInstructionAsync({
  payer,
  admin,
  instanceSeed,
});

// Sign and send transaction
```

For deriving your instance PDA, refer to the [PDA Reference](#pda-reference) section.


## AllowMint

Whitelists an SPL token mint for deposits. At least one allowed mint is required for a functional instance. The instruction: 
1. Creates the instance's associated token account (ATA) for holding escrowed tokens.
2. Creates the allowed mint PDA.

### TypeScript Example

```typescript
import {
  getAllowMintInstructionAsync,
  findInstancePda,
} from 'private-channel-escrow-program';
import { address } from '@solana/kit';

const USDC_MINT = address('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');

const instanceSeed = address(process.env.INSTANCE_SEED!);

const [instanceAddress] = await findInstancePda({
  instanceSeed,
});

const allowMintIx = await getAllowMintInstructionAsync({
  payer,
  admin, // Must be instance admin
  instance: instanceAddress,
  mint: USDC_MINT,
});

// Sign and send transaction with payer and admin as signers
```

**Security Notes:**
- Supports Token Program and Token-2022
- Token-2022 mints with the `TransferHook` extension are rejected (the program's `TransferChecked` CPI does not resolve extra-account metas, so every deposit/release would fail on-chain)
- Token-2022 mints with `PermanentDelegate` or `PausableConfig` extensions are accepted; the operator enforces drain detection and pause state off-chain via a withdrawal pre-flight
- Only the instance admin can allow mints
- **Token-2022 usage**: The `tokenProgram` parameter defaults to the legacy Token Program. For Token-2022 mints, you must explicitly pass the Token-2022 program ID (e.g., `tokenProgram: TOKEN_2022_PROGRAM_ADDRESS`). This also applies to the `Deposit` instruction.

> **Before whitelisting any mint, review [Mint Risk Considerations](#mint-risk-considerations).** The escrow program accepts several mint authorities/extensions that a third party can use to strand or drain pooled escrow. The on-chain program does not block them, so whitelisting one is a trust decision about the mint's issuer.

To derive your allowed mint PDA, refer to the [PDA Reference](#pda-reference) section.

## Mint Risk Considerations

The escrow program only rejects the `TransferHook` extension on-chain. Several other mint authorities and Token-2022 extensions are accepted but hand an external party control over the pooled `instance_ata`. There is no on-chain guard and the off-chain preflight only *detects* the aftermath — it cannot prevent or revert it. Whitelisting a mint that carries any of these is a trust decision about the mint's issuer (and whoever holds its authorities), not just about its current state.

| Authority / extension | Risk | Operator guidance |
|---|---|---|
| `freeze_authority` (SPL & Token-2022) | The authority holder can `FreezeAccount` the pooled escrow ATA directly. Because the ATA is pooled per mint, one freeze strands *every* depositor's withdrawals for that mint until the holder thaws it. | Only whitelist mints whose `freeze_authority` is `None` (or held by a party you trust to never freeze escrow). |
| `PermanentDelegate` (Token-2022) | The delegate can transfer or burn directly from the escrow ATA, outside `ReleaseFunds` and every escrow access control, draining escrow for that mint. The withdrawal preflight only notices the missing balance afterward and routes victims to manual review. | Only whitelist if you trust the permanent-delegate holder; treat the off-chain check as detection, not a mitigation. |
| `MintCloseAuthority` (Token-2022) | Once supply reaches zero the mint can be closed and re-created at the *same pubkey* with a different extension set (e.g. adding `PermanentDelegate` or `PausableConfig`). The allowlist and the operator's safety cache are keyed only by pubkey, so a previously-reviewed "clean" mint can be swapped underneath that trust decision with no new allowlist review. | Prefer rejecting mints that carry `MintCloseAuthority`; if accepted, treat the original review as void after any close/recreate. |
| `PausableConfig` (Token-2022) | The pause authority can pause the mint, halting all transfers; while paused, `ReleaseFunds` fails and withdrawals for that mint cannot settle until it is unpaused. The operator checks pause state in the withdrawal preflight, but cannot prevent or override a pause. | Only whitelist if you trust the pause authority; treat the off-chain check as detection, not a mitigation. |

In every case the safe posture is the same: only whitelist mints whose close/freeze/delegate authorities are disabled or held by a party you trust, since the protocol cannot retain custody once an external authority can act on the escrow ATA.

## BlockMint

Revokes deposit permissions for a previously whitelisted mint. Closes the AllowedMint PDA and reclaims rent to the payer.

### TypeScript Example

```typescript
import {
  getBlockMintInstructionAsync,
} from 'private-channel-escrow-program';

const blockMintIx = await getBlockMintInstructionAsync({
  payer,
  admin, // Must be instance admin
  instance: process.env.INSTANCE_ADDRESS,
  mint: USDC_MINT,
});

// Sign and send transaction with payer and admin as signers
```

**Notes:**
- New deposits for this mint fail immediately with `InvalidAllowedMint` error
- Existing Solana Private Channels balances are NOT affected
- Withdraws still work for existing balances
- Reversible: Admin can call AllowMint again to re-enable

## AddOperator

Authorizes an operator to sign withdrawal transactions. At least one operator is required for a functional instance.

### TypeScript Example

```typescript
import {
  getAddOperatorInstructionAsync,
} from 'private-channel-escrow-program';

const operatorKeypair = await generateKeyPairSigner();

const addOperatorIx = await getAddOperatorInstructionAsync({
  payer,
  admin, // Must be instance admin
  instance: process.env.INSTANCE_ADDRESS,
  operator: operatorKeypair.address,
});

// Send transaction with payer and admin as signers
```

**Notes:**
- Multiple operators can be added for redundancy
- Operators sign withdrawal transactions to release escrowed funds

To derive your operator PDA, refer to the [PDA Reference](#pda-reference) section.

## RemoveOperator

Removes an operator's authorization. Closes the Operator PDA and reclaims rent to the payer.

### TypeScript Example

```typescript
import { getRemoveOperatorInstructionAsync } from 'private-channel-escrow-program';

const removeOperatorIx = await getRemoveOperatorInstructionAsync({
  payer,
  admin, // Must be instance admin
  instance: process.env.INSTANCE_ADDRESS,
  operator: process.env.OPERATOR_ADDRESS,
});

// Sign and send transaction with payer and admin as signers
```



## SetNewAdmin

Transfers admin control to a new address. Useful for key rotation, organizational changes, or upgrading to multisig governance.

### TypeScript Example

```typescript
import { getSetNewAdminInstruction } from 'private-channel-escrow-program';

const newAdmin = await generateKeyPairSigner();

const setAdminIx = getSetNewAdminInstruction({
  payer,
  currentAdmin, // Current admin authorizes as signer
  instance: process.env.INSTANCE_ADDRESS,
  newAdmin,     // New admin accepts as signer
});

// Sign and send transaction with payer, currentAdmin, AND newAdmin as signers
```

**Important:**
- Both current admin and new admin must sign the transaction
- Change is immediate and irreversible
- Old admin loses all privileges once confirmed
- Verify new admin address carefully (typos = permanent loss of control)



## Deposit

Locks tokens in the Mainnet escrow for minting on the Solana Private Channels payment channel. Permissionless instruction — any user can deposit to any instance with allowed mints.

### TypeScript Example

```typescript
import {
  getDepositInstructionAsync,
  findAllowedMintPda,
} from 'private-channel-escrow-program';
import { findAssociatedTokenPda } from '@solana-program/token';
import { address, none } from '@solana/kit';

const user = await generateKeyPairSigner();
const depositAmount = 100_000_000n; // 100 USDC (6 decimals)


const depositIx = await getDepositInstructionAsync({
  payer,
  user, // User authorizing the deposit
  instance: process.env.INSTANCE_ADDRESS,
  mint: process.env.ALLOWED_MINT_ADDRESS,
  amount: depositAmount,
  recipient: none(), // or address('RecipientAddressOnSolana Private Channels...') if you want to credit to a different address
});

// Send and sign transaction with payer and user as signers
```

### Recipient Field Behavior

| Recipient Value | Tokens Credited To |
|----------------|-------------------|
| `null` or `none()` | User's address on Solana Private Channels |
| Specified address | Recipient address on Solana Private Channels |

**Use Case**: Third-party deposits (e.g., CEX depositing on behalf of end users OR user's depositing to CEX managed-wallet)

Check out the [Architecture Overview](./ARCHITECTURE.md) for more details on how deposits are processed on Solana Private Channels.

## PDA Reference

### Instance PDA

The Instance PDA is your unique instance of the Solana Private Channels Escrow Program. It isolates your governance (allowed mints, operators, etc.) and escrowed deposits from other instances.

Seeds: `["instance", instance_seed]`

Derivation:

```typescript
const [instancePda] = await findInstancePda({
  instanceSeed: instanceSeed.address,
});
```

### Allowed Mint PDA

The Allowed Mint PDA is used to whitelist tokens that can be deposited into the instance.

Seeds: `["allowed_mint", instance_pda, mint]`

Derivation:

```typescript
const [allowedMintPda] = await findAllowedMintPda({
  instance: instancePda,
  mint: mintAddress,
});
```

### Operator PDA

The Operator PDA is used to authorize operators to release funds from the instance.

Seeds: `["operator", instance_pda, wallet_pubkey]`

Derivation:

```typescript
const [operatorPda] = await findOperatorPda({
  instance: instancePda,
  wallet: walletAddress,
});
```

### Event Authority PDA

The Event Authority PDA is used to emit events from the instance.

Seeds: `["event_authority"]`

Derivation:

```typescript
const [eventAuthorityPda] = await findEventAuthorityPda();
```


## Related Documentation

- [Withdraw Program](WITHDRAW_PROGRAM.md) - Technical reference
- [Withdraw Guide](WITHDRAWING_GUIDE.md) - Processing Solana Private Channels → Mainnet withdrawals
- [Architecture Overview](ARCHITECTURE.md) - System design
