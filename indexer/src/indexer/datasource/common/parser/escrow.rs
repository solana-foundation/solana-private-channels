use crate::{
    error::{account::AccountError, ParserError},
    indexer::datasource::common::parser::resolve_account,
    indexer::datasource::common::types::*,
    indexer::datasource::rpc_polling::types::{InnerInstruction, InnerInstructions},
    operator::utils::account_util::find_instance_pda,
};

use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

// PrivateChannel Escrow Program ID
pub const PRIVATE_CHANNEL_ESCROW_PROGRAM_ID: &str = "9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU";

// Instruction discriminators (from IDL)
const CREATE_INSTANCE: u8 = 0;
const ALLOW_MINT: u8 = 1;
const BLOCK_MINT: u8 = 2;
// pub(crate) so shared test fixtures can build valid Deposit and ReleaseFunds data.
pub(crate) const DEPOSIT: u8 = 6;
pub(crate) const RELEASE_FUNDS: u8 = 7;
const ROTATE_BITMAP: u8 = 8;

// Only the post-bitmap layouts are decoded. A pre-bitmap release can only name
// the instance this design abandons, and every escrow instruction whose instance
// is not the configured one is dropped before it reaches storage.
const CREATE_INSTANCE_ACCOUNTS: usize = 8;
const BLOCK_MINT_ACCOUNTS: usize = 7;
// ReleaseFunds reads only the 13 accounts every bitmap-era version shares; later ones
// (memo program, hook extras) are version-specific and ignored. A new account the
// indexer must read means parsing both layouts, as BlockMint does, not raising this.
const RELEASE_FUNDS_MIN_ACCOUNTS: usize = 13;
const ROTATE_BITMAP_ACCOUNTS: usize = 7;

// BlockMint before the gates: no args, and a system_program at index 5. Removing
// it pulled event_authority and the program id back one slot, so reading a legacy
// instruction means adding this to those two indices and nothing else.
const LEGACY_BLOCK_MINT_ACCOUNTS: usize = 8;
const LEGACY_BLOCK_MINT_SYSTEM_PROGRAM_SHIFT: usize = 1;

// Event related constants
pub(crate) const EVENT_IX_TAG_LE: &[u8] = &[0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];
const ALLOW_MINT_EVENT_DISCRIMINATOR: u8 = 1;
pub(crate) const DEPOSIT_EVENT_DISCRIMINATOR: u8 = 6;
const EVENT_DISCRIMINATOR_INDEX: usize = 8;
// AllowMintEvent: tag(8)+disc(1)+instance_seed(32) = 41
const ALLOW_MINT_EVENT_MINT_INDEX: usize = 41;
// AllowMintEvent: tag(8)+disc(1)+instance_seed(32)+mint(32) = 73
const EVENT_DECIMALS_INDEX: usize = 73;
// DepositEvent: tag(8)+disc(1)+instance_seed(32)+user(32) = 73
// (same offset, different event)
const EVENT_AMOUNT_INDEX: usize = 73;
// DepositEvent identity fields: instance_seed(32) after disc, user(32) before amount(8),
// then recipient(32) and mint(32).
const DEPOSIT_EVENT_INSTANCE_SEED_INDEX: usize = 9;
const DEPOSIT_EVENT_USER_INDEX: usize = 41;
const DEPOSIT_EVENT_RECIPIENT_INDEX: usize = 81;
const DEPOSIT_EVENT_MINT_INDEX: usize = 113;

// ******************************************************************************************
// Instruction types
// ******************************************************************************************

/// Escrow program instructions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EscrowInstruction {
    CreateInstance {
        accounts: CreateInstanceAccounts,
        data: CreateInstanceData,
    },
    AllowMint {
        accounts: AllowMintAccounts,
        data: AllowMintData,
        event: AllowMintEvent,
    },
    BlockMint {
        accounts: BlockMintAccounts,
        data: BlockMintData,
    },
    Deposit {
        accounts: DepositAccounts,
        data: DepositData,
        event: DepositEvent,
    },
    ReleaseFunds {
        accounts: ReleaseFundsAccounts,
        data: ReleaseFundsData,
    },
    RotateBitmap {
        accounts: RotateBitmapAccounts,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateInstanceAccounts {
    pub payer: Pubkey,
    pub admin: Pubkey,
    pub instance_seed: Pubkey,
    pub instance: Pubkey,
    pub withdrawal_bitmap: Pubkey,
    pub system_program: Pubkey,
    pub event_authority: Pubkey,
    pub private_channel_escrow_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowMintAccounts {
    pub payer: Pubkey,
    pub admin: Pubkey,
    pub instance: Pubkey,
    pub mint: Pubkey,
    pub allowed_mint: Pubkey,
    pub instance_ata: Pubkey,
    pub system_program: Pubkey,
    pub token_program: Pubkey,
    pub associated_token_program: Pubkey,
    pub event_authority: Pubkey,
    pub private_channel_escrow_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMintAccounts {
    pub payer: Pubkey,
    pub admin: Pubkey,
    pub instance: Pubkey,
    pub mint: Pubkey,
    pub allowed_mint: Pubkey,
    pub event_authority: Pubkey,
    pub private_channel_escrow_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositAccounts {
    pub payer: Pubkey,
    pub user: Pubkey,
    pub instance: Pubkey,
    pub mint: Pubkey,
    pub allowed_mint: Pubkey,
    pub user_ata: Pubkey,
    pub instance_ata: Pubkey,
    pub system_program: Pubkey,
    pub token_program: Pubkey,
    pub associated_token_program: Pubkey,
    pub event_authority: Pubkey,
    pub private_channel_escrow_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseFundsAccounts {
    pub payer: Pubkey,
    pub operator: Pubkey,
    pub instance: Pubkey,
    pub withdrawal_bitmap: Pubkey,
    pub operator_pda: Pubkey,
    pub mint: Pubkey,
    pub allowed_mint: Pubkey,
    pub user_ata: Pubkey,
    pub instance_ata: Pubkey,
    pub token_program: Pubkey,
    pub associated_token_program: Pubkey,
    pub event_authority: Pubkey,
    pub private_channel_escrow_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateBitmapAccounts {
    pub payer: Pubkey,
    pub operator: Pubkey,
    pub instance: Pubkey,
    pub withdrawal_bitmap: Pubkey,
    pub operator_pda: Pubkey,
    pub event_authority: Pubkey,
    pub private_channel_escrow_program: Pubkey,
}

// ******************************************************************************************
// Data types for instructions
// ******************************************************************************************
#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct CreateInstanceData {
    bump: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct AllowMintData {
    pub bump: u8,
    /// Per-withdrawal fee on the channel. Zero is allowed and means no fee.
    pub withdraw_fee: u64,
    /// Smallest channel withdrawal amount. Zero means no minimum.
    pub min_withdraw_amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct BlockMintData {
    pub block_deposits: bool,
    pub block_withdrawals: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct DepositData {
    pub amount: u64,
    pub recipient: Option<Pubkey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseFundsData {
    pub amount: u64,
    pub user: Pubkey,
    pub transaction_nonce: u64,
}

impl ReleaseFundsData {
    /// Parse ReleaseFundsData from raw bytes after the discriminator:
    /// amount (8) + user (32) + transaction_nonce (8).
    pub fn from_bytes(data: &[u8]) -> Result<Self, ParserError> {
        let min_len = 8 + 32 + 8;
        if data.len() < min_len {
            return Err(ParserError::InstructionParseFailed {
                reason: format!("ReleaseFundsData too short: {} < {}", data.len(), min_len),
            });
        }

        let mut offset = 0;

        let amount = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let user = Pubkey::try_from(&data[offset..offset + 32]).map_err(|e| {
            ParserError::InvalidPubkey {
                reason: format!("Invalid user pubkey: {}", e),
            }
        })?;
        offset += 32;

        let transaction_nonce = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

        Ok(Self {
            amount,
            user,
            transaction_nonce,
        })
    }
}

// ******************************************************************************************
// Event types
// ******************************************************************************************
#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct AllowMintEvent {
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct DepositEvent {
    pub amount: u64,
}

// ******************************************************************************************
// Parse instructions
// ******************************************************************************************
/// Inner (CPI) escrow discriminators the indexer skips: operator-gated
/// `ReleaseFunds`/`RotateBitmap` (already tracked as top-level) and admin
/// `CreateInstance`/`AllowMint`/`BlockMint` (a foreign CPI of them is
/// implausible). Only the user-initiated `Deposit` is indexed via CPI. Kept next
/// to the discriminator constants as the one source of truth both decoders share.
pub fn escrow_inner_discriminator_excluded(discriminator: u8) -> bool {
    matches!(
        discriminator,
        CREATE_INSTANCE | ALLOW_MINT | BLOCK_MINT | RELEASE_FUNDS | ROTATE_BITMAP
    )
}

/// Return the `accounts.instance` carried by any escrow instruction variant.
pub fn escrow_instance_of(ix: &EscrowInstruction) -> Pubkey {
    match ix {
        EscrowInstruction::CreateInstance { accounts, .. } => accounts.instance,
        EscrowInstruction::AllowMint { accounts, .. } => accounts.instance,
        EscrowInstruction::BlockMint { accounts, .. } => accounts.instance,
        EscrowInstruction::Deposit { accounts, .. } => accounts.instance,
        EscrowInstruction::ReleaseFunds { accounts, .. } => accounts.instance,
        EscrowInstruction::RotateBitmap { accounts, .. } => accounts.instance,
    }
}

/// Parse a single PrivateChannel Escrow instruction
pub fn parse_escrow_instruction(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
    location: InstructionLocation,
) -> Result<Option<EscrowInstruction>, ParserError> {
    // Decode base58 instruction data
    let data = bs58::decode(&instruction.data).into_vec()?;

    if data.is_empty() {
        return Ok(None);
    }

    let discriminator = data[0];
    let ix_data = &data[1..];

    match discriminator {
        CREATE_INSTANCE => parse_create_instance(ix_data, instruction, account_keys),
        ALLOW_MINT => parse_allow_mint(
            ix_data,
            instruction,
            account_keys,
            inner_instructions,
            location,
        ),
        BLOCK_MINT => parse_block_mint(ix_data, instruction, account_keys),
        DEPOSIT => parse_deposit(
            ix_data,
            instruction,
            account_keys,
            inner_instructions,
            location,
        ),
        RELEASE_FUNDS => parse_release_funds(ix_data, instruction, account_keys),
        ROTATE_BITMAP => parse_rotate_bitmap(instruction, account_keys),
        _ => Ok(None), // Unsupported instruction type
    }
}

/// Every field of an escrow DepositEvent self-CPI.
struct DecodedDepositEvent {
    instance_seed: Pubkey,
    user: Pubkey,
    amount: u64,
    recipient: Pubkey,
    mint: Pubkey,
}

fn is_escrow_program(inner: &InnerInstruction, account_keys: &[Pubkey]) -> bool {
    account_keys
        .get(inner.instruction.program_id_index as usize)
        .is_some_and(|program_id| program_id.to_string() == PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

/// Decode an inner instruction only if it is the escrow program's DepositEvent self-CPI; the program-id check stops a foreign instruction whose data merely starts with the event tag from being read as the event.
fn decode_deposit_event(
    inner: &InnerInstruction,
    account_keys: &[Pubkey],
) -> Option<DecodedDepositEvent> {
    if !is_escrow_program(inner, account_keys) {
        return None;
    }
    let event_data = bs58::decode(&inner.instruction.data).into_vec().ok()?;
    if event_data.len() < 145
        || !event_data.starts_with(EVENT_IX_TAG_LE)
        || event_data[EVENT_DISCRIMINATOR_INDEX] != DEPOSIT_EVENT_DISCRIMINATOR
    {
        return None;
    }
    // The `>= 145` length guard keeps every slice below in bounds.
    let key_at = |index: usize| Pubkey::try_from(&event_data[index..index + 32]).ok();
    let amount_bytes: [u8; 8] = event_data[EVENT_AMOUNT_INDEX..EVENT_AMOUNT_INDEX + 8]
        .try_into()
        .ok()?;
    Some(DecodedDepositEvent {
        instance_seed: key_at(DEPOSIT_EVENT_INSTANCE_SEED_INDEX)?,
        user: key_at(DEPOSIT_EVENT_USER_INDEX)?,
        amount: u64::from_le_bytes(amount_bytes),
        recipient: key_at(DEPOSIT_EVENT_RECIPIENT_INDEX)?,
        mint: key_at(DEPOSIT_EVENT_MINT_INDEX)?,
    })
}

fn deposit_event_amount(inner: &InnerInstruction, account_keys: &[Pubkey]) -> Option<u64> {
    decode_deposit_event(inner, account_keys).map(|event| event.amount)
}

fn stackless_deposit_error(reason: &str) -> ParserError {
    ParserError::InstructionParseFailed {
        reason: format!("CPI deposit without stack height: {reason}"),
    }
}

/// Errors unless the event names the deposit's own user, mint, recipient and instance.
fn check_event_matches_deposit(
    event: &DecodedDepositEvent,
    accounts: &DepositAccounts,
    data: &DepositData,
) -> Result<(), ParserError> {
    let mismatched = if event.user != accounts.user {
        Some("user")
    } else if event.mint != accounts.mint {
        Some("mint")
    } else if event.recipient != data.recipient.unwrap_or(accounts.user) {
        Some("recipient")
    } else if find_instance_pda(&event.instance_seed) != accounts.instance {
        Some("instance")
    } else {
        None
    };
    match mismatched {
        Some(field) => Err(stackless_deposit_error(&format!(
            "DepositEvent {field} does not match the deposit"
        ))),
        None => Ok(()),
    }
}

/// Decode an inner instruction and return its decimals only if it is the escrow program's AllowMintEvent self-CPI for `mint`; the mint check stops another AllowMint's event from supplying the decimals.
fn allow_mint_event_decimals(
    inner: &InnerInstruction,
    account_keys: &[Pubkey],
    mint: &Pubkey,
) -> Option<u8> {
    let program_id = account_keys.get(inner.instruction.program_id_index as usize)?;
    if program_id.to_string() != PRIVATE_CHANNEL_ESCROW_PROGRAM_ID {
        return None;
    }
    let event_data = bs58::decode(&inner.instruction.data).into_vec().ok()?;
    if event_data.len() >= 74
        && event_data.starts_with(EVENT_IX_TAG_LE)
        && event_data[EVENT_DISCRIMINATOR_INDEX] == ALLOW_MINT_EVENT_DISCRIMINATOR
        && event_data[ALLOW_MINT_EVENT_MINT_INDEX..EVENT_DECIMALS_INDEX] == *mint.as_ref()
    {
        Some(event_data[EVENT_DECIMALS_INDEX])
    } else {
        None
    }
}

/// Parse CreateInstance instruction
fn parse_create_instance(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    let ix_data = <CreateInstanceData as borsh::BorshDeserialize>::deserialize(&mut &data[..])?;

    // The bitmap redesign added `withdrawalBitmap` at index 4, so every account
    // after `instance` sits one slot later than in the pre-bitmap layout.
    if instruction.accounts.len() < CREATE_INSTANCE_ACCOUNTS {
        return Err(AccountError::InsufficientAccounts {
            required: CREATE_INSTANCE_ACCOUNTS,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = CreateInstanceAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        admin: resolve_account(instruction, account_keys, 1)?,
        instance_seed: resolve_account(instruction, account_keys, 2)?,
        instance: resolve_account(instruction, account_keys, 3)?,
        withdrawal_bitmap: resolve_account(instruction, account_keys, 4)?,
        system_program: resolve_account(instruction, account_keys, 5)?,
        event_authority: resolve_account(instruction, account_keys, 6)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 7)?,
    };

    Ok(Some(EscrowInstruction::CreateInstance {
        accounts,
        data: ix_data,
    }))
}

/// Parse AllowMint instruction
fn parse_allow_mint(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
    location: InstructionLocation,
) -> Result<Option<EscrowInstruction>, ParserError> {
    let ix_data = <AllowMintData as borsh::BorshDeserialize>::deserialize(&mut &data[..])?;

    // Expected 11 accounts
    if instruction.accounts.len() < 11 {
        return Err(AccountError::InsufficientAccounts {
            required: 11,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = AllowMintAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        admin: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        mint: resolve_account(instruction, account_keys, 3)?,
        allowed_mint: resolve_account(instruction, account_keys, 4)?,
        instance_ata: resolve_account(instruction, account_keys, 5)?,
        system_program: resolve_account(instruction, account_keys, 6)?,
        token_program: resolve_account(instruction, account_keys, 7)?,
        associated_token_program: resolve_account(instruction, account_keys, 8)?,
        event_authority: resolve_account(instruction, account_keys, 9)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 10)?,
    };

    // AllowMint is only indexed top-level (see `escrow_inner_discriminator_excluded`),
    // so the inner set at its own index is its whole subtree.
    let decimals = inner_instructions
        .iter()
        .find(|set| set.index as u32 == location.top_level_index)
        .and_then(|set| {
            set.instructions
                .iter()
                .find_map(|inner| allow_mint_event_decimals(inner, account_keys, &accounts.mint))
        });

    match decimals {
        Some(decimals) => Ok(Some(EscrowInstruction::AllowMint {
            accounts,
            data: ix_data,
            event: AllowMintEvent { decimals },
        })),
        None => Err(ParserError::InstructionParseFailed {
            reason: "No allow mint event found".to_string(),
        }),
    }
}

/// Parse BlockMint instruction.
///
/// The gate flags are read from the instruction args. The inner BlockMintEvent
/// carries them too, but the args need no CPI scan. Borsh rejects a bool byte
/// outside 0/1, matching the on-chain parser.
///
/// Empty args mean the pre-gates instruction, which closed the AllowedMint PDA
/// and so shut deposits and releases alike. It decodes as both gates set rather
/// than erroring, since a parse failure is fatal for the slot and would wedge
/// any resync or gap-fill that crosses one.
fn parse_block_mint(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    let legacy = data.is_empty();

    let (required, shift) = if legacy {
        (
            LEGACY_BLOCK_MINT_ACCOUNTS,
            LEGACY_BLOCK_MINT_SYSTEM_PROGRAM_SHIFT,
        )
    } else {
        (BLOCK_MINT_ACCOUNTS, 0)
    };

    if instruction.accounts.len() < required {
        return Err(AccountError::InsufficientAccounts {
            required,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let ix_data = if legacy {
        BlockMintData {
            block_deposits: true,
            block_withdrawals: true,
        }
    } else {
        <BlockMintData as borsh::BorshDeserialize>::deserialize(&mut &data[..])?
    };

    let accounts = BlockMintAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        admin: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        mint: resolve_account(instruction, account_keys, 3)?,
        allowed_mint: resolve_account(instruction, account_keys, 4)?,
        event_authority: resolve_account(instruction, account_keys, 5 + shift)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 6 + shift)?,
    };

    Ok(Some(EscrowInstruction::BlockMint {
        accounts,
        data: ix_data,
    }))
}

/// Parse Deposit instruction
fn parse_deposit(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
    location: InstructionLocation,
) -> Result<Option<EscrowInstruction>, ParserError> {
    // Errs when the instruction payload is truncated or not valid Deposit borsh.
    let ix_data = <DepositData as borsh::BorshDeserialize>::deserialize(&mut &data[..])?;

    // Errs when a malformed tx supplies fewer than the 12 accounts Deposit needs.
    if instruction.accounts.len() < 12 {
        return Err(AccountError::InsufficientAccounts {
            required: 12,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = DepositAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        user: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        mint: resolve_account(instruction, account_keys, 3)?,
        allowed_mint: resolve_account(instruction, account_keys, 4)?,
        user_ata: resolve_account(instruction, account_keys, 5)?,
        instance_ata: resolve_account(instruction, account_keys, 6)?,
        system_program: resolve_account(instruction, account_keys, 7)?,
        token_program: resolve_account(instruction, account_keys, 8)?,
        associated_token_program: resolve_account(instruction, account_keys, 9)?,
        event_authority: resolve_account(instruction, account_keys, 10)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 11)?,
    };

    // Scope the DepositEvent to this deposit's own self-CPI subtree.
    //
    // Stage 1: pick the inner set whose `index` equals this deposit's top-level
    // position. That alone separates multiple top-level deposits in one tx.
    //
    // Stage 2: a CPI deposit may share its set with other deposits, so we can't
    // just take the first event. Entries are listed in call order, and a
    // deposit's own event sits among the entries right after it that are nested
    // deeper (a higher stack height). Take that deeper run, which ends as soon as
    // the height drops back to this deposit's level, and read the event from it.
    //
    // A CPI deposit with no stack height falls back to call order and binds the
    // event to the deposit's accounts. A top-level deposit needs no
    // stage 2: its whole set is its own subtree.
    let scoped_set = inner_instructions
        .iter()
        .find(|set| set.index as u32 == location.top_level_index);

    let amount = match (scoped_set, location.inner) {
        // CPI deposit with a known depth: read the event from its own subtree.
        (Some(set), Some(inner_loc)) if inner_loc.stack_height.is_some() => {
            let own_height = inner_loc.stack_height.unwrap();
            let start = inner_loc.inner_index as usize + 1;
            set.instructions
                .iter()
                .skip(start)
                .take_while(|inner| inner.stack_height.is_some_and(|h| h > own_height))
                .find_map(|inner| deposit_event_amount(inner, account_keys))
        }
        // No depth: entries are in call order and nothing inside a Deposit can call the
        // escrow except its own EmitEvent, so the first escrow entry after it is its event.
        // Never skip past that entry, so a source that dropped the event fails closed.
        (Some(set), Some(inner_loc)) => {
            let start = inner_loc.inner_index as usize + 1;
            let first_escrow = set
                .instructions
                .iter()
                .skip(start)
                .find(|inner| is_escrow_program(inner, account_keys))
                .ok_or_else(|| stackless_deposit_error("no escrow entry after it"))?;
            let event = decode_deposit_event(first_escrow, account_keys).ok_or_else(|| {
                stackless_deposit_error("first escrow entry after it is not a DepositEvent")
            })?;
            check_event_matches_deposit(&event, &accounts, &ix_data)?;
            tracing::debug!(
                top_level_index = location.top_level_index,
                inner_index = inner_loc.inner_index,
                "CPI deposit without stack height read its event by call order"
            );
            Some(event.amount)
        }
        // Top-level deposit: the whole set is its subtree, so scan it directly.
        (Some(set), None) => set
            .instructions
            .iter()
            .find_map(|inner| deposit_event_amount(inner, account_keys)),
        // No matching inner set for this deposit: no event to read.
        (None, _) => None,
    };

    match amount {
        Some(amount) => Ok(Some(EscrowInstruction::Deposit {
            accounts,
            data: ix_data,
            event: DepositEvent { amount },
        })),
        // Errs when no escrow DepositEvent self-CPI exists in scope: the inner set
        // is missing, or the scoped subtree held no event (a non-deposit tx, or an
        // event emitted by a non-escrow program that the program-id check rejected).
        None => Err(ParserError::InstructionParseFailed {
            reason: "No deposit event found".to_string(),
        }),
    }
}

/// Parse ReleaseFunds in the bitmap-era layout. 13 accounts is safe to accept: the
/// current program rejects fewer than 14, and failed transactions never reach here,
/// so a successful 13-account release can only be a pre-memo one.
fn parse_release_funds(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    let account_count = instruction.accounts.len();
    if account_count < RELEASE_FUNDS_MIN_ACCOUNTS {
        return Err(AccountError::InsufficientAccounts {
            required: RELEASE_FUNDS_MIN_ACCOUNTS,
            actual: account_count,
        }
        .into());
    }

    let ix_data = ReleaseFundsData::from_bytes(data)?;

    let accounts = ReleaseFundsAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        operator: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        withdrawal_bitmap: resolve_account(instruction, account_keys, 3)?,
        operator_pda: resolve_account(instruction, account_keys, 4)?,
        mint: resolve_account(instruction, account_keys, 5)?,
        allowed_mint: resolve_account(instruction, account_keys, 6)?,
        user_ata: resolve_account(instruction, account_keys, 7)?,
        instance_ata: resolve_account(instruction, account_keys, 8)?,
        token_program: resolve_account(instruction, account_keys, 9)?,
        associated_token_program: resolve_account(instruction, account_keys, 10)?,
        event_authority: resolve_account(instruction, account_keys, 11)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 12)?,
    };

    Ok(Some(EscrowInstruction::ReleaseFunds {
        accounts,
        data: ix_data,
    }))
}

/// Parse a rotation. Only the accounts are read; the data carries nothing this
/// indexer needs.
fn parse_rotate_bitmap(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    let account_count = instruction.accounts.len();
    if account_count < ROTATE_BITMAP_ACCOUNTS {
        return Err(AccountError::InsufficientAccounts {
            required: ROTATE_BITMAP_ACCOUNTS,
            actual: account_count,
        }
        .into());
    }

    let accounts = RotateBitmapAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        operator: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        withdrawal_bitmap: resolve_account(instruction, account_keys, 3)?,
        operator_pda: resolve_account(instruction, account_keys, 4)?,
        event_authority: resolve_account(instruction, account_keys, 5)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 6)?,
    };

    Ok(Some(EscrowInstruction::RotateBitmap { accounts }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::escrow_fixtures::release_funds_borsh;
    use std::str::FromStr;

    // ============================================================================
    // Test Helper Functions
    // ============================================================================

    /// Create minimal valid Borsh-encoded data for CreateInstance instruction
    /// CreateInstanceIxData { bump: u8 }
    fn create_create_instance_borsh_data() -> Vec<u8> {
        vec![42] // Just one byte for bump
    }

    const ALLOW_MINT_WITHDRAW_FEE: u64 = 1_234_567;
    const ALLOW_MINT_MIN_WITHDRAW_AMOUNT: u64 = 7_654_321;
    const ALLOW_MINT_MIN_DEPOSIT_AMOUNT: u64 = 2_345_678;

    /// Create minimal valid Borsh-encoded data for AllowMint instruction
    /// AllowMintIxData { bump: u8, withdraw_fee: u64, min_withdraw_amount: u64,
    /// min_deposit_amount: u64 }. The indexer does not read the deposit
    /// minimum, so this pins that the trailing field still parses.
    fn create_allow_mint_borsh_data() -> Vec<u8> {
        let mut data = vec![123]; // bump
        data.extend_from_slice(&ALLOW_MINT_WITHDRAW_FEE.to_le_bytes());
        data.extend_from_slice(&ALLOW_MINT_MIN_WITHDRAW_AMOUNT.to_le_bytes());
        data.extend_from_slice(&ALLOW_MINT_MIN_DEPOSIT_AMOUNT.to_le_bytes());
        data
    }

    /// An AllowMintEvent self-CPI for `mint` with `decimals`, emitted by the program at `program_id_index`.
    fn allow_mint_event_inner(
        program_id_index: u8,
        mint: Pubkey,
        decimals: u8,
    ) -> InnerInstruction {
        let mut data = vec![];
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(ALLOW_MINT_EVENT_DISCRIMINATOR);
        data.extend_from_slice(&[0u8; 32]); // instance_seed
        data.extend_from_slice(mint.as_ref());
        data.push(decimals);
        InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index,
                accounts: vec![],
                data: bs58::encode(&data).into_string(),
            },
            stack_height: Some(2),
        }
    }

    /// Create minimal valid Borsh-encoded data for Deposit instruction
    /// DepositIxData { amount: u64, recipient: Option<[u8; 32]> }
    fn create_deposit_borsh_data() -> Vec<u8> {
        crate::test_utils::escrow_fixtures::deposit_borsh(1000, None)
    }

    /// Build inner instructions carrying a valid DepositEvent CPI with the given received `amount`.
    fn create_deposit_inner_instructions(amount: u64) -> Vec<InnerInstructions> {
        vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: ESCROW_PROGRAM_KEY_INDEX,
                    accounts: vec![],
                    data: bs58::encode(crate::test_utils::escrow_fixtures::deposit_event_bytes(
                        amount,
                    ))
                    .into_string(),
                },
                stack_height: Some(2),
            }],
        }]
    }

    /// Encode instruction data with discriminator and Borsh data as base58
    fn encode_instruction_data(discriminator: u8, borsh_data: Vec<u8>) -> String {
        let mut full = vec![discriminator];
        full.extend(borsh_data);
        bs58::encode(full).into_string()
    }

    /// Account slot the inner-instruction builders point program_id at; the escrow program sits here so event inner instructions resolve to it.
    const ESCROW_PROGRAM_KEY_INDEX: u8 = 20;

    /// N test account keys with the escrow program placed at `ESCROW_PROGRAM_KEY_INDEX` (padding the list) so helper-built event CPIs resolve to it.
    fn create_n_account_keys(n: usize) -> Vec<Pubkey> {
        let len = n.max(ESCROW_PROGRAM_KEY_INDEX as usize + 1);
        let mut keys: Vec<Pubkey> = (0..len)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = i as u8;
                Pubkey::new_from_array(bytes)
            })
            .collect();
        keys[ESCROW_PROGRAM_KEY_INDEX as usize] =
            Pubkey::from_str(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).unwrap();
        keys
    }

    /// Create a CompiledInstruction with N accounts
    fn create_instruction_with_accounts(n_accounts: usize, data: String) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: 0,
            accounts: (0..n_accounts as u8).collect(),
            data,
        }
    }

    // ============================================================================
    // parse_create_instance Tests
    // ============================================================================

    #[test]
    fn test_create_instance_valid_accounts() {
        let data = encode_instruction_data(CREATE_INSTANCE, create_create_instance_borsh_data());
        let instruction = create_instruction_with_accounts(8, data);
        let account_keys = create_n_account_keys(8);

        let result = parse_create_instance(&[42], &instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
    }

    #[test]
    fn test_create_instance_insufficient_accounts() {
        let data = encode_instruction_data(CREATE_INSTANCE, create_create_instance_borsh_data());
        let instruction = create_instruction_with_accounts(7, data); // Only 7 accounts (need 8)
        let account_keys = create_n_account_keys(7);

        let result = parse_create_instance(&[42], &instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    #[test]
    fn test_create_instance_maps_bitmap_era_account_layout() {
        let data = encode_instruction_data(CREATE_INSTANCE, create_create_instance_borsh_data());
        let instruction = create_instruction_with_accounts(8, data);
        let account_keys = create_n_account_keys(8);

        let parsed = parse_create_instance(&[42], &instruction, &account_keys)
            .expect("parse succeeds")
            .expect("instruction recognised");
        let EscrowInstruction::CreateInstance { accounts, .. } = parsed else {
            panic!("expected CreateInstance");
        };

        assert_eq!(accounts.payer, account_keys[0]);
        assert_eq!(accounts.admin, account_keys[1]);
        assert_eq!(accounts.instance_seed, account_keys[2]);
        assert_eq!(accounts.instance, account_keys[3]);
        assert_eq!(accounts.withdrawal_bitmap, account_keys[4]);
        assert_eq!(accounts.system_program, account_keys[5]);
        assert_eq!(accounts.event_authority, account_keys[6]);
        assert_eq!(accounts.private_channel_escrow_program, account_keys[7]);
    }

    // ============================================================================
    // parse_allow_mint Tests
    // ============================================================================

    #[test]
    fn test_allow_mint_valid_accounts() {
        let borsh_data = create_allow_mint_borsh_data();
        let instruction = create_instruction_with_accounts(11, "dummy".to_string());
        let account_keys = create_n_account_keys(11);

        let inner_sets = vec![InnerInstructions {
            index: 0,
            instructions: vec![allow_mint_event_inner(
                ESCROW_PROGRAM_KEY_INDEX,
                account_keys[3],
                2,
            )],
        }];

        let result = parse_allow_mint(
            &borsh_data,
            &instruction,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(0),
        );

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
        if let Some(EscrowInstruction::AllowMint { data, .. }) = parsed {
            assert_eq!(data.bump, 123);
            assert_eq!(data.withdraw_fee, ALLOW_MINT_WITHDRAW_FEE);
            assert_eq!(data.min_withdraw_amount, ALLOW_MINT_MIN_WITHDRAW_AMOUNT);
        } else {
            panic!("Expected AllowMint instruction");
        }
    }

    #[test]
    fn test_allow_mint_insufficient_accounts() {
        let borsh_data = create_allow_mint_borsh_data();
        let instruction = create_instruction_with_accounts(10, "dummy".to_string()); // Only 10 accounts (need 11)
        let account_keys = create_n_account_keys(10);
        let inner_sets = vec![InnerInstructions {
            index: 0,
            instructions: vec![allow_mint_event_inner(
                ESCROW_PROGRAM_KEY_INDEX,
                account_keys[3],
                2,
            )],
        }];

        let result = parse_allow_mint(
            &borsh_data,
            &instruction,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(0),
        );

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    #[test]
    fn test_allow_mint_decimals_not_found() {
        let borsh_data = create_allow_mint_borsh_data();
        let instruction = create_instruction_with_accounts(11, "dummy".to_string());
        let account_keys = create_n_account_keys(11);

        let result = parse_allow_mint(
            &borsh_data,
            &instruction,
            &account_keys,
            &[],
            InstructionLocation::top_level(0),
        );

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No allow mint event found"), "Error: {}", err);
    }

    /// Two top-level AllowMints in one tx each read their own inner set's event, not the first one in the tx.
    #[test]
    fn two_allow_mints_in_one_tx_read_their_own_decimals() {
        let account_keys = create_n_account_keys(11);
        // Keys pad to 21, so 12 is a spare key outside the instruction's 11 accounts.
        let mint_b_key_index = 12;
        let mint_a = account_keys[3];
        let mint_b = account_keys[mint_b_key_index as usize];
        let mint_a_decimals = 6;
        let mint_b_decimals = 9;

        let data = encode_instruction_data(ALLOW_MINT, create_allow_mint_borsh_data());
        let instruction_a = create_instruction_with_accounts(11, data.clone());
        let mut instruction_b = create_instruction_with_accounts(11, data);
        instruction_b.accounts[3] = mint_b_key_index;

        let inner_sets = vec![
            InnerInstructions {
                index: 0,
                instructions: vec![allow_mint_event_inner(
                    ESCROW_PROGRAM_KEY_INDEX,
                    mint_a,
                    mint_a_decimals,
                )],
            },
            InnerInstructions {
                index: 1,
                instructions: vec![allow_mint_event_inner(
                    ESCROW_PROGRAM_KEY_INDEX,
                    mint_b,
                    mint_b_decimals,
                )],
            },
        ];

        let parsed_a = parse_escrow_instruction(
            &instruction_a,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(0),
        )
        .unwrap()
        .unwrap();
        let parsed_b = parse_escrow_instruction(
            &instruction_b,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(1),
        )
        .unwrap()
        .unwrap();

        let EscrowInstruction::AllowMint {
            accounts: accounts_a,
            event: event_a,
            ..
        } = parsed_a
        else {
            panic!("expected AllowMint");
        };
        let EscrowInstruction::AllowMint {
            accounts: accounts_b,
            event: event_b,
            ..
        } = parsed_b
        else {
            panic!("expected AllowMint");
        };
        assert_eq!(accounts_a.mint, mint_a);
        assert_eq!(event_a.decimals, mint_a_decimals);
        assert_eq!(accounts_b.mint, mint_b);
        assert_eq!(
            event_b.decimals, mint_b_decimals,
            "mint B reads its own event, not mint A's"
        );
    }

    /// An AllowMintEvent sitting in another top-level instruction's inner set is not read, even for the same mint.
    #[test]
    fn allow_mint_event_in_another_instructions_set_is_ignored() {
        let account_keys = create_n_account_keys(11);
        let instruction = create_instruction_with_accounts(
            11,
            encode_instruction_data(ALLOW_MINT, create_allow_mint_borsh_data()),
        );
        // The only matching event belongs to top-level instruction 1, not 0.
        let inner_sets = vec![InnerInstructions {
            index: 1,
            instructions: vec![allow_mint_event_inner(
                ESCROW_PROGRAM_KEY_INDEX,
                account_keys[3],
                6,
            )],
        }];

        let err = parse_escrow_instruction(
            &instruction,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(0),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("No allow mint event found"),
            "event outside the instruction's own inner set must be ignored: {err}"
        );
    }

    /// An AllowMintEvent for a different mint than the instruction's is not read as its event.
    #[test]
    fn allow_mint_event_for_other_mint_is_ignored() {
        let account_keys = create_n_account_keys(11);
        // Keys pad to 21, so 12 is a spare key outside the instruction's 11 accounts.
        let other_mint = account_keys[12];
        let instruction = create_instruction_with_accounts(
            11,
            encode_instruction_data(ALLOW_MINT, create_allow_mint_borsh_data()),
        );
        let inner_sets = vec![InnerInstructions {
            index: 0,
            instructions: vec![allow_mint_event_inner(
                ESCROW_PROGRAM_KEY_INDEX,
                other_mint,
                6,
            )],
        }];

        let err = parse_escrow_instruction(
            &instruction,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(0),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("No allow mint event found"),
            "event for another mint must be ignored: {err}"
        );
    }

    /// An AllowMintEvent lookalike on a non-escrow program is not read as the event.
    #[test]
    fn allow_mint_foreign_program_event_lookalike_is_ignored() {
        let account_keys = create_n_account_keys(11);
        let instruction = create_instruction_with_accounts(
            11,
            encode_instruction_data(ALLOW_MINT, create_allow_mint_borsh_data()),
        );
        let inner_sets = vec![InnerInstructions {
            index: 0,
            // Key index 0 is not the escrow program.
            instructions: vec![allow_mint_event_inner(0, account_keys[3], 6)],
        }];

        let err = parse_escrow_instruction(
            &instruction,
            &account_keys,
            &inner_sets,
            InstructionLocation::top_level(0),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("No allow mint event found"),
            "foreign-program event lookalike must be ignored: {err}"
        );
    }

    // ============================================================================
    // parse_block_mint Tests
    // ============================================================================

    #[test]
    fn test_block_mint_valid_accounts() {
        let instruction = create_instruction_with_accounts(7, "dummy".to_string());
        let account_keys = create_n_account_keys(7);

        // Opposite gate values so a swapped byte would fail here.
        let result = parse_block_mint(&[1, 0], &instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        if let Some(EscrowInstruction::BlockMint { accounts, data }) = parsed {
            // instance @ index 2, mint @ index 3 — read straight from accounts.
            assert_eq!(accounts.instance, account_keys[2]);
            assert_eq!(accounts.mint, account_keys[3]);
            assert!(data.block_deposits);
            assert!(!data.block_withdrawals);
        } else {
            panic!("Expected BlockMint instruction");
        }
    }

    #[test]
    fn test_block_mint_insufficient_accounts() {
        let instruction = create_instruction_with_accounts(6, "dummy".to_string()); // Only 6 accounts (need 7)
        let account_keys = create_n_account_keys(6);

        let result = parse_block_mint(&[1, 1], &instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // A gate byte outside 0/1 must be refused, matching the on-chain parser.
    #[test]
    fn test_block_mint_non_canonical_gate() {
        let instruction = create_instruction_with_accounts(7, "dummy".to_string());
        let account_keys = create_n_account_keys(7);

        let result = parse_block_mint(&[2, 0], &instruction, &account_keys);

        assert!(result.is_err());
    }

    // The pre-gates instruction took no args and closed the AllowedMint PDA,
    // which shut deposits and releases alike. Erroring on it wedges any resync
    // that crosses one, so it must decode as both gates shut.
    #[test]
    fn test_block_mint_legacy_no_args_shuts_both_gates() {
        let instruction = create_instruction_with_accounts(8, "dummy".to_string());
        let account_keys = create_n_account_keys(8);

        let result = parse_block_mint(&[], &instruction, &account_keys);

        let parsed = result.expect("Legacy BlockMint should decode");
        if let Some(EscrowInstruction::BlockMint { accounts, data }) = parsed {
            // Same positions in both layouts, so a historical block still lands
            // on the right mint row.
            assert_eq!(accounts.instance, account_keys[2]);
            assert_eq!(accounts.mint, account_keys[3]);
            assert!(data.block_deposits);
            assert!(data.block_withdrawals);
        } else {
            panic!("Expected BlockMint instruction");
        }
    }

    // The legacy layout carried a system_program at index 5, so its trailing two
    // accounts sit one slot later than the current layout's.
    #[test]
    fn test_block_mint_legacy_trailing_accounts_skip_system_program() {
        let instruction = create_instruction_with_accounts(8, "dummy".to_string());
        let account_keys = create_n_account_keys(8);

        let result = parse_block_mint(&[], &instruction, &account_keys);

        let parsed = result.expect("Legacy BlockMint should decode");
        if let Some(EscrowInstruction::BlockMint { accounts, .. }) = parsed {
            assert_eq!(accounts.event_authority, account_keys[6]);
            assert_eq!(accounts.private_channel_escrow_program, account_keys[7]);
        } else {
            panic!("Expected BlockMint instruction");
        }
    }

    // Empty args alone do not make an instruction legacy; without the 8 accounts
    // it is malformed, and applying the shift would run past the account list.
    #[test]
    fn test_block_mint_legacy_insufficient_accounts() {
        let instruction = create_instruction_with_accounts(7, "dummy".to_string());
        let account_keys = create_n_account_keys(7);

        let result = parse_block_mint(&[], &instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // ============================================================================
    // parse_deposit Tests
    // ============================================================================

    // A deposit of a transfer-hook mint carries the hook's accounts after the
    // fixed 12. Tightening the account check to an equality would stop indexing
    // those deposits while the tokens still land in escrow, so pin that the
    // trailing accounts are tolerated and the fixed slots still resolve.
    #[test]
    fn test_deposit_tolerates_trailing_hook_accounts() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(15, "dummy".to_string());
        let account_keys = create_n_account_keys(15);

        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &create_deposit_inner_instructions(990),
            InstructionLocation::top_level(0),
        );

        let parsed = result.unwrap().expect("Some");
        if let EscrowInstruction::Deposit {
            accounts, event, ..
        } = parsed
        {
            assert_eq!(event.amount, 990);
            assert_eq!(accounts.mint, account_keys[3]);
            assert_eq!(accounts.private_channel_escrow_program, account_keys[11]);
        } else {
            panic!("Expected Deposit instruction");
        }
    }

    #[test]
    fn test_deposit_valid_accounts() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        // data.amount = 1000 (caller-requested), event.amount = 990 (net received).
        // Asserting on 990 proves the parser uses the event, not the instruction args.
        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &create_deposit_inner_instructions(990),
            InstructionLocation::top_level(0),
        );

        let parsed = result.unwrap().expect("Some");
        if let EscrowInstruction::Deposit { event, .. } = parsed {
            assert_eq!(event.amount, 990);
        } else {
            panic!("Expected Deposit instruction");
        }
    }

    #[test]
    fn test_deposit_insufficient_accounts() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(11, "dummy".to_string()); // Only 11 accounts (need 12)
        let account_keys = create_n_account_keys(11);

        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &[],
            InstructionLocation::top_level(0),
        );

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    #[test]
    fn test_deposit_no_event_errs() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &[],
            InstructionLocation::top_level(0),
        );

        let err = result.unwrap_err().to_string();
        assert!(err.contains("No deposit event found"), "Error: {}", err);
    }

    // ============================================================================
    // parse_release_funds Tests
    // ============================================================================

    /// The bitmap sits at index 3 and pushes every later account along by one,
    /// so a wrong offset table would put the wrong nonce and amount in the
    /// database rather than fail. 13 is pre-memo, 14 current, 17 with hook extras.
    #[test]
    fn test_release_funds_account_offsets() {
        for n in [13, 14, 17] {
            let user = Pubkey::new_unique();
            let data = release_funds_borsh(1_000, user, 42);
            let instruction = create_instruction_with_accounts(n, "dummy".to_string());
            let keys = create_n_account_keys(n);

            let parsed = parse_release_funds(&data, &instruction, &keys)
                .unwrap_or_else(|e| panic!("{n} accounts must parse: {e}"))
                .expect("must yield an instruction");

            let EscrowInstruction::ReleaseFunds { accounts: a, data } = parsed else {
                panic!("must decode as ReleaseFunds");
            };

            assert_eq!(data.amount, 1_000, "{n} accounts");
            assert_eq!(data.transaction_nonce, 42, "{n} accounts");
            assert_eq!(data.user, user, "{n} accounts");
            let resolved = [
                a.payer,
                a.operator,
                a.instance,
                a.withdrawal_bitmap,
                a.operator_pda,
                a.mint,
                a.allowed_mint,
                a.user_ata,
                a.instance_ata,
                a.token_program,
                a.associated_token_program,
                a.event_authority,
                a.private_channel_escrow_program,
            ];
            assert_eq!(resolved, keys[..13], "{n} accounts");
        }
    }

    /// Data shorter than the layout needs must error rather than read past the
    /// end or silently mis-slice.
    #[test]
    fn test_release_funds_malformed_data_errors() {
        let mut data = release_funds_borsh(1_000, Pubkey::new_unique(), 42);
        data.truncate(40);
        let instruction = create_instruction_with_accounts(13, "dummy".to_string());
        let account_keys = create_n_account_keys(13);

        let err = parse_release_funds(&data, &instruction, &account_keys)
            .expect_err("short data must not parse")
            .to_string();
        assert!(err.contains("too short"), "Error: {err}");
    }

    #[test]
    fn test_release_funds_insufficient_accounts() {
        let data = release_funds_borsh(1_000, Pubkey::new_unique(), 1);
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        let err = parse_release_funds(&data, &instruction, &account_keys)
            .expect_err("12 accounts must not parse")
            .to_string();
        assert!(
            err.contains("Insufficient accounts: required 13, actual 12"),
            "Error: {err}"
        );
    }

    // ============================================================================
    // parse_rotate_bitmap Tests
    // ============================================================================

    /// The rotation carries the bitmap at index 3 as well, so the same offset
    /// mistake is possible here.
    #[test]
    fn test_rotate_bitmap_account_offsets() {
        let instruction = create_instruction_with_accounts(7, "dummy".to_string());
        let account_keys = create_n_account_keys(7);

        let parsed = parse_rotate_bitmap(&instruction, &account_keys)
            .expect("must parse")
            .expect("must yield an instruction");

        let EscrowInstruction::RotateBitmap { accounts } = parsed else {
            panic!("must decode as RotateBitmap");
        };

        assert_eq!(accounts.withdrawal_bitmap, account_keys[3]);
        assert_eq!(accounts.operator_pda, account_keys[4]);
        assert_eq!(accounts.private_channel_escrow_program, account_keys[6]);
    }

    #[test]
    fn test_rotate_bitmap_insufficient_accounts() {
        let instruction = create_instruction_with_accounts(5, "dummy".to_string());
        let account_keys = create_n_account_keys(5);

        let result = parse_rotate_bitmap(&instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // ============================================================================
    // CPI hardening + event-scoping tests
    // ============================================================================

    /// A DepositEvent inner instruction on the escrow program at the given amount and stack height.
    fn deposit_event_inner(amount: u64, stack_height: u32) -> InnerInstruction {
        InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: ESCROW_PROGRAM_KEY_INDEX,
                accounts: vec![],
                data: bs58::encode(crate::test_utils::escrow_fixtures::deposit_event_bytes(
                    amount,
                ))
                .into_string(),
            },
            stack_height: Some(stack_height),
        }
    }

    /// An inner escrow Deposit instruction (the CPI'd deposit, not its event) at the given stack height.
    fn deposit_ix_inner(stack_height: u32) -> InnerInstruction {
        InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: ESCROW_PROGRAM_KEY_INDEX,
                accounts: (0..12).collect(),
                data: bs58::encode(crate::test_utils::escrow_fixtures::deposit_ix_bytes(
                    1000, None,
                ))
                .into_string(),
            },
            stack_height: Some(stack_height),
        }
    }

    /// An out-of-range account index returns an error instead of panicking the parse path.
    #[test]
    fn deposit_out_of_range_account_index_returns_err_not_panic() {
        let borsh_data = create_deposit_borsh_data();
        // 12 account entries (passes the count check) that index past the 5-key list.
        let instruction = CompiledInstruction {
            program_id_index: 0,
            accounts: (50..62).collect(),
            data: "dummy".to_string(),
        };
        let account_keys = create_n_account_keys(5);

        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &create_deposit_inner_instructions(990),
            InstructionLocation::top_level(0),
        );

        assert!(result.is_err(), "out-of-range index must be an Err");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("out of bounds"), "Error: {}", err);
    }

    /// Two escrow deposits sharing one inner set each read their own stack-height subtree, so they get distinct amounts.
    #[test]
    fn two_cpi_deposits_in_one_set_get_distinct_event_amounts() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        // Pre-order CPI walk under one foreign top-level instruction (index 4):
        //   [0] deposit A    height 2
        //   [1]   event 300  height 3
        //   [2] deposit B    height 2
        //   [3]   event 400  height 3
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![
                deposit_ix_inner(2),
                deposit_event_inner(300, 3),
                deposit_ix_inner(2),
                deposit_event_inner(400, 3),
            ],
        }];

        let deposit_a = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &inner_set,
            InstructionLocation {
                top_level_index: 4,
                inner: Some(InnerLocation {
                    inner_index: 0,
                    stack_height: Some(2),
                }),
            },
        )
        .unwrap()
        .unwrap();
        let deposit_b = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &inner_set,
            InstructionLocation {
                top_level_index: 4,
                inner: Some(InnerLocation {
                    inner_index: 2,
                    stack_height: Some(2),
                }),
            },
        )
        .unwrap()
        .unwrap();

        let amount = |ix: EscrowInstruction| match ix {
            EscrowInstruction::Deposit { event, .. } => event.amount,
            _ => panic!("expected Deposit"),
        };
        assert_eq!(amount(deposit_a), 300, "deposit A reads its own event");
        assert_eq!(amount(deposit_b), 400, "deposit B reads its own event");
    }

    /// A deposit nested two CPI hops deep (stack_height 3, beyond the one-level
    /// case) still reads its own DepositEvent: the validator flattens every CPI
    /// depth into one inner list, so `inner_index` stays a unique position and the
    /// stack-height subtree scan is depth-agnostic.
    #[test]
    fn cpi_deposit_nested_two_levels_reads_own_event() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        // Flattened inner set under one foreign top-level (index 4). Deposit A is
        // two CPI hops deep (height 3); its event is a hop deeper (height 4). A
        // deeper non-event entry sits in A's subtree before the event, proving the
        // scan walks the whole subtree and skips non-events. Sibling deposit B
        // (also height 3) bounds A's subtree.
        //   [0] deposit A     height 3
        //   [1]   nested ix   height 4   (in A's subtree, not an event)
        //   [2]   event 700   height 4   (A's event)
        //   [3] deposit B     height 3   (ends A's subtree)
        //   [4]   event 800   height 4   (B's event)
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![
                deposit_ix_inner(3),
                deposit_ix_inner(4),
                deposit_event_inner(700, 4),
                deposit_ix_inner(3),
                deposit_event_inner(800, 4),
            ],
        }];

        let parse_at = |inner_index: u32| {
            parse_deposit(
                &borsh_data,
                &instruction,
                &account_keys,
                &inner_set,
                InstructionLocation {
                    top_level_index: 4,
                    inner: Some(InnerLocation {
                        inner_index,
                        stack_height: Some(3),
                    }),
                },
            )
            .unwrap()
            .unwrap()
        };

        let amount = |ix: EscrowInstruction| match ix {
            EscrowInstruction::Deposit { event, .. } => event.amount,
            _ => panic!("expected Deposit"),
        };
        assert_eq!(
            amount(parse_at(0)),
            700,
            "depth-3 deposit A reads its own event past a deeper non-event entry"
        );
        assert_eq!(
            amount(parse_at(3)),
            800,
            "sibling deposit B at the same depth reads its own event, not A's"
        );
    }

    // ============================================================================
    // CPI deposit without stack height: positional fallback
    // ============================================================================

    /// Instance seed whose PDA sits at key 2, the Deposit's instance account.
    fn stackless_seed() -> Pubkey {
        Pubkey::new_from_array([7u8; 32])
    }

    /// Keys where user is key 1, instance is the seed's PDA (key 2) and mint is key 3.
    fn stackless_keys() -> Vec<Pubkey> {
        let mut keys = create_n_account_keys(12);
        keys[2] = crate::operator::utils::account_util::find_instance_pda(&stackless_seed());
        keys
    }

    /// An escrow DepositEvent with every field set and no stack height.
    fn stackless_event(
        instance_seed: Pubkey,
        user: Pubkey,
        amount: u64,
        recipient: Pubkey,
        mint: Pubkey,
    ) -> InnerInstruction {
        InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: ESCROW_PROGRAM_KEY_INDEX,
                accounts: vec![],
                data: bs58::encode(
                    crate::test_utils::escrow_fixtures::deposit_event_bytes_full(
                        instance_seed,
                        user,
                        amount,
                        recipient,
                        mint,
                    ),
                )
                .into_string(),
            },
            stack_height: None,
        }
    }

    /// The event a Deposit with no recipient emits over `stackless_keys`.
    fn bound_event(keys: &[Pubkey], amount: u64) -> InnerInstruction {
        stackless_event(stackless_seed(), keys[1], amount, keys[1], keys[3])
    }

    /// An inner escrow Deposit instruction with no stack height.
    fn stackless_deposit_ix() -> InnerInstruction {
        InnerInstruction {
            stack_height: None,
            ..deposit_ix_inner(2)
        }
    }

    /// A non-escrow entry (key 0) with no stack height, such as the token transfer.
    fn stackless_foreign_ix() -> InnerInstruction {
        InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: "transfer".to_string(),
            },
            stack_height: None,
        }
    }

    /// Parse the CPI deposit at `inner_index` of top-level set 4, with no stack height.
    fn parse_stackless(
        borsh_data: &[u8],
        keys: &[Pubkey],
        inner_set: &[InnerInstructions],
        inner_index: u32,
    ) -> Result<Option<EscrowInstruction>, ParserError> {
        parse_deposit(
            borsh_data,
            &create_instruction_with_accounts(12, "dummy".to_string()),
            keys,
            inner_set,
            InstructionLocation {
                top_level_index: 4,
                inner: Some(InnerLocation {
                    inner_index,
                    stack_height: None,
                }),
            },
        )
    }

    fn deposit_amount_of(ix: EscrowInstruction) -> u64 {
        match ix {
            EscrowInstruction::Deposit { event, .. } => event.amount,
            _ => panic!("expected Deposit"),
        }
    }

    /// Two CPI deposits in one set, no stack heights: each reads the first escrow
    /// entry after it, skipping the non-escrow token transfer in between.
    #[test]
    fn stackless_cpi_deposits_in_one_set_read_their_own_events() {
        let keys = stackless_keys();
        //   [0] deposit A  [1] token transfer  [2] event 300
        //   [3] deposit B  [4] token transfer  [5] event 400
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![
                stackless_deposit_ix(),
                stackless_foreign_ix(),
                bound_event(&keys, 300),
                stackless_deposit_ix(),
                stackless_foreign_ix(),
                bound_event(&keys, 400),
            ],
        }];
        let data = create_deposit_borsh_data();

        let a = parse_stackless(&data, &keys, &inner_set, 0)
            .unwrap()
            .unwrap();
        let b = parse_stackless(&data, &keys, &inner_set, 3)
            .unwrap()
            .unwrap();

        assert_eq!(deposit_amount_of(a), 300, "deposit A reads its own event");
        assert_eq!(deposit_amount_of(b), 400, "deposit B reads its own event");
    }

    /// An explicit recipient must match the event's recipient field.
    #[test]
    fn stackless_cpi_deposit_with_explicit_recipient_binds_it() {
        let keys = stackless_keys();
        let recipient = Pubkey::new_unique();
        let data = crate::test_utils::escrow_fixtures::deposit_borsh(1000, Some(recipient));
        let event = |r| stackless_event(stackless_seed(), keys[1], 250, r, keys[3]);
        let set = |r| {
            vec![InnerInstructions {
                index: 4,
                instructions: vec![stackless_deposit_ix(), event(r)],
            }]
        };

        let ok = parse_stackless(&data, &keys, &set(recipient), 0)
            .unwrap()
            .unwrap();
        assert_eq!(deposit_amount_of(ok), 250);

        // The event names the user, but the deposit asked for `recipient`.
        let err = parse_stackless(&data, &keys, &set(keys[1]), 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("recipient does not match"),
            "explicit recipient mismatch must error: {err}"
        );
    }

    /// Each identity field of the event must match the deposit, or the parse errors.
    #[test]
    fn stackless_cpi_deposit_rejects_event_that_does_not_match() {
        let keys = stackless_keys();
        let other = Pubkey::new_unique();
        let (seed, user, mint) = (stackless_seed(), keys[1], keys[3]);
        let cases = [
            ("user", stackless_event(seed, other, 1, user, mint)),
            ("mint", stackless_event(seed, user, 1, user, other)),
            // No recipient in the deposit, so the event must name the user.
            ("recipient", stackless_event(seed, user, 1, other, mint)),
            ("instance", stackless_event(other, user, 1, user, mint)),
        ];

        for (field, event) in cases {
            let inner_set = vec![InnerInstructions {
                index: 4,
                instructions: vec![stackless_deposit_ix(), event],
            }];
            let err = parse_stackless(&create_deposit_borsh_data(), &keys, &inner_set, 0)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("{field} does not match")),
                "{field} mismatch must error: {err}"
            );
        }
    }

    /// No escrow entry after the deposit means its event is missing, so the parse errors.
    #[test]
    fn stackless_cpi_deposit_without_escrow_entry_after_it_errors() {
        let keys = stackless_keys();
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![stackless_deposit_ix(), stackless_foreign_ix()],
        }];

        let err = parse_stackless(&create_deposit_borsh_data(), &keys, &inner_set, 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no escrow entry after it"),
            "missing event must error: {err}"
        );
    }

    /// A source that dropped deposit A's event shows deposit B as A's next escrow
    /// entry. A must error, never borrow B's amount.
    #[test]
    fn stackless_cpi_deposit_with_dropped_event_errors_not_borrows() {
        let keys = stackless_keys();
        //   [0] deposit A  (event dropped)  [1] deposit B  [2] event 400
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![
                stackless_deposit_ix(),
                stackless_deposit_ix(),
                bound_event(&keys, 400),
            ],
        }];

        let err = parse_stackless(&create_deposit_borsh_data(), &keys, &inner_set, 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not a DepositEvent"),
            "dropped event must error, not borrow the next deposit's: {err}"
        );
    }

    /// A CPI deposit whose own subtree holds deeper entries but no DepositEvent
    /// errors with "No deposit event found" rather than borrowing a sibling's.
    #[test]
    fn cpi_deposit_with_eventless_subtree_errs() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        // Pre-order walk: deposit A's subtree (height 3) is a nested deposit ix,
        // not its event; deposit B at height 2 owns the only event.
        //   [0] deposit A    height 2
        //   [1]   deposit    height 3   (deeper, but not an event)
        //   [2] deposit B    height 2
        //   [3]   event 400  height 3
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![
                deposit_ix_inner(2),
                deposit_ix_inner(3),
                deposit_ix_inner(2),
                deposit_event_inner(400, 3),
            ],
        }];

        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &inner_set,
            InstructionLocation {
                top_level_index: 4,
                inner: Some(InnerLocation {
                    inner_index: 0,
                    stack_height: Some(2),
                }),
            },
        );

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No deposit event found"),
            "eventless subtree must not borrow a sibling's event: {err}"
        );
    }

    /// Handing the DepositEvent self-CPI instruction to the parser yields Ok(None), not a second Deposit row (double-mint guard).
    #[test]
    fn self_cpi_event_instruction_parses_to_none() {
        let account_keys = create_n_account_keys(12);
        let event = deposit_event_inner(123, 3);

        let result = parse_escrow_instruction(
            &event.instruction,
            &account_keys,
            &[],
            InstructionLocation {
                top_level_index: 0,
                inner: Some(InnerLocation {
                    inner_index: 1,
                    stack_height: Some(3),
                }),
            },
        );

        assert!(
            matches!(result, Ok(None)),
            "event self-CPI must parse to Ok(None), got {result:?}"
        );
    }

    /// An event-tag lookalike on a non-escrow program is not read as the deposit's event.
    #[test]
    fn foreign_program_event_lookalike_is_not_read_as_event() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        // Same event bytes, but on a non-escrow program (key index 0).
        let mut data = vec![];
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(DEPOSIT_EVENT_DISCRIMINATOR);
        data.extend_from_slice(&[0u8; 32]);
        data.extend_from_slice(&[0u8; 32]);
        data.extend_from_slice(&999u64.to_le_bytes());
        data.extend_from_slice(&[0u8; 32]);
        data.extend_from_slice(&[0u8; 32]);
        let inner_set = vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: 0, // not the escrow program
                    accounts: vec![],
                    data: bs58::encode(&data).into_string(),
                },
                stack_height: Some(2),
            }],
        }];

        let result = parse_deposit(
            &borsh_data,
            &instruction,
            &account_keys,
            &inner_set,
            InstructionLocation::top_level(0),
        );

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No deposit event found"),
            "foreign-program event lookalike must be ignored: {err}"
        );
    }
}
