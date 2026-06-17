use crate::{
    error::{account::AccountError, ParserError},
    indexer::datasource::common::parser::resolve_account,
    indexer::datasource::common::types::*,
    indexer::datasource::rpc_polling::types::{InnerInstruction, InnerInstructions},
};

use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

// PrivateChannel Escrow Program ID
pub const PRIVATE_CHANNEL_ESCROW_PROGRAM_ID: &str = "GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83";

// Instruction discriminators (from IDL)
const CREATE_INSTANCE: u8 = 0;
const ALLOW_MINT: u8 = 1;
const BLOCK_MINT: u8 = 2;
// pub(crate) so shared test fixtures can build valid Deposit instruction data.
pub(crate) const DEPOSIT: u8 = 6;
const RELEASE_FUNDS: u8 = 7;
const RESET_SMT_ROOT: u8 = 8;

// Event related constants
pub(crate) const EVENT_IX_TAG_LE: &[u8] = &[0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];
const ALLOW_MINT_EVENT_DISCRIMINATOR: u8 = 1;
pub(crate) const DEPOSIT_EVENT_DISCRIMINATOR: u8 = 6;
const EVENT_DISCRIMINATOR_INDEX: usize = 8;
// AllowMintEvent: tag(8)+disc(1)+instance_seed(32)+mint(32) = 73
const EVENT_DECIMALS_INDEX: usize = 73;
// DepositEvent: tag(8)+disc(1)+instance_seed(32)+user(32) = 73
// (same offset, different event)
const EVENT_AMOUNT_INDEX: usize = 73;

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
    ResetSmtRoot {
        accounts: ResetSmtRootAccounts,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateInstanceAccounts {
    pub payer: Pubkey,
    pub admin: Pubkey,
    pub instance_seed: Pubkey,
    pub instance: Pubkey,
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
    pub system_program: Pubkey,
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
pub struct ResetSmtRootAccounts {
    pub payer: Pubkey,
    pub operator: Pubkey,
    pub instance: Pubkey,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize)]
pub struct DepositData {
    pub amount: u64,
    pub recipient: Option<Pubkey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseFundsData {
    amount: u64,
    user: Pubkey,
    new_withdrawal_root: [u8; 32],
    transaction_nonce: u64,
    // Skipping sibling_proofs, since we don't need it
}

impl ReleaseFundsData {
    /// Parse ReleaseFundsData from raw bytes (after discriminator)
    /// Layout: amount (8) + user (32) + new_withdrawal_root (32) + transaction_nonce (8)
    pub fn from_bytes(data: &[u8]) -> Result<Self, ParserError> {
        let min_len = 8 + 32 + 32 + 8;
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

        let mut new_withdrawal_root = [0u8; 32];
        new_withdrawal_root.copy_from_slice(&data[offset..offset + 32]);
        offset += 32;

        let transaction_nonce = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

        Ok(Self {
            amount,
            user,
            new_withdrawal_root,
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
/// `ReleaseFunds`/`ResetSmtRoot` (already tracked as top-level) and admin
/// `CreateInstance`/`AllowMint`/`BlockMint` (a foreign CPI of them is
/// implausible). Only the user-initiated `Deposit` is indexed via CPI. Kept next
/// to the discriminator constants as the one source of truth both decoders share.
pub fn escrow_inner_discriminator_excluded(discriminator: u8) -> bool {
    matches!(
        discriminator,
        CREATE_INSTANCE | ALLOW_MINT | BLOCK_MINT | RELEASE_FUNDS | RESET_SMT_ROOT
    )
}

/// Return the `accounts.instance` carried by any escrow instruction variant.
pub fn escrow_instance_of(ix: &EscrowInstruction) -> Pubkey {
    match ix {
        EscrowInstruction::CreateInstance { accounts, .. } => accounts.instance,
        EscrowInstruction::AllowMint { accounts, .. } => accounts.instance,
        EscrowInstruction::BlockMint { accounts } => accounts.instance,
        EscrowInstruction::Deposit { accounts, .. } => accounts.instance,
        EscrowInstruction::ReleaseFunds { accounts, .. } => accounts.instance,
        EscrowInstruction::ResetSmtRoot { accounts, .. } => accounts.instance,
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
        ALLOW_MINT => parse_allow_mint(ix_data, instruction, account_keys, inner_instructions),
        BLOCK_MINT => parse_block_mint(instruction, account_keys),
        DEPOSIT => parse_deposit(
            ix_data,
            instruction,
            account_keys,
            inner_instructions,
            location,
        ),
        RELEASE_FUNDS => parse_release_funds(ix_data, instruction, account_keys),
        RESET_SMT_ROOT => parse_reset_smt_root(instruction, account_keys),
        _ => Ok(None), // Unsupported instruction type
    }
}

/// Decode an inner instruction and return its amount only if it is the escrow program's DepositEvent self-CPI; the program-id check stops a foreign instruction whose data merely starts with the event tag from being read as the event.
fn deposit_event_amount(inner: &InnerInstruction, account_keys: &[Pubkey]) -> Option<u64> {
    let program_id = account_keys.get(inner.instruction.program_id_index as usize)?;
    if program_id.to_string() != PRIVATE_CHANNEL_ESCROW_PROGRAM_ID {
        return None;
    }
    let event_data = bs58::decode(&inner.instruction.data).into_vec().ok()?;
    if event_data.len() >= 145
        && event_data.starts_with(EVENT_IX_TAG_LE)
        && event_data[EVENT_DISCRIMINATOR_INDEX] == DEPOSIT_EVENT_DISCRIMINATOR
    {
        // The `>= 145` length guard guarantees this slice is exactly 8 bytes.
        let amount_bytes: [u8; 8] = event_data[EVENT_AMOUNT_INDEX..EVENT_AMOUNT_INDEX + 8]
            .try_into()
            .ok()?;
        Some(u64::from_le_bytes(amount_bytes))
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

    // Expected 7 accounts
    if instruction.accounts.len() < 7 {
        return Err(AccountError::InsufficientAccounts {
            required: 7,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = CreateInstanceAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        admin: resolve_account(instruction, account_keys, 1)?,
        instance_seed: resolve_account(instruction, account_keys, 2)?,
        instance: resolve_account(instruction, account_keys, 3)?,
        system_program: resolve_account(instruction, account_keys, 4)?,
        event_authority: resolve_account(instruction, account_keys, 5)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 6)?,
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

    for inner_instruction_set in inner_instructions {
        for inner_instruction in &inner_instruction_set.instructions {
            let Ok(event_data) = bs58::decode(&inner_instruction.instruction.data).into_vec()
            else {
                continue;
            };

            if event_data.len() >= 74
                && event_data.starts_with(EVENT_IX_TAG_LE)
                && event_data[EVENT_DISCRIMINATOR_INDEX] == ALLOW_MINT_EVENT_DISCRIMINATOR
            {
                return Ok(Some(EscrowInstruction::AllowMint {
                    accounts,
                    data: ix_data,
                    event: AllowMintEvent {
                        decimals: event_data[EVENT_DECIMALS_INDEX],
                    },
                }));
            }
        }
    }

    Err(ParserError::InstructionParseFailed {
        reason: "No allow mint event found".to_string(),
    })
}

/// Parse BlockMint instruction from its accounts.
///
/// BlockMint carries no instruction arguments. The two fields the downstream
/// status row needs — the instance and the mint — are both present in the
/// instruction accounts, so there is no
/// need to scan the inner BlockMintEvent.
fn parse_block_mint(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    // Expected 8 accounts
    if instruction.accounts.len() < 8 {
        return Err(AccountError::InsufficientAccounts {
            required: 8,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = BlockMintAccounts {
        payer: account_keys[instruction.accounts[0] as usize],
        admin: account_keys[instruction.accounts[1] as usize],
        instance: account_keys[instruction.accounts[2] as usize],
        mint: account_keys[instruction.accounts[3] as usize],
        allowed_mint: account_keys[instruction.accounts[4] as usize],
        system_program: account_keys[instruction.accounts[5] as usize],
        event_authority: account_keys[instruction.accounts[6] as usize],
        private_channel_escrow_program: account_keys[instruction.accounts[7] as usize],
    };

    Ok(Some(EscrowInstruction::BlockMint { accounts }))
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
    // A CPI deposit with no stack height can't be scoped to its subtree, so a
    // set holding several deposits would attribute the wrong amount. Rather than
    // guess, error out. A top-level deposit needs no
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
        // CPI deposit with no depth: can't isolate the subtree, so refuse to guess.
        // Occurs only when the data source omits stack height (e.g. an old RPC node
        // or a Geyser feed that doesn't emit stackHeight).
        (Some(_), Some(_)) => {
            return Err(ParserError::InstructionParseFailed {
                reason: "CPI deposit without stack height: cannot scope DepositEvent".to_string(),
            });
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

/// Parse ReleaseFunds instruction
fn parse_release_funds(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    let ix_data = ReleaseFundsData::from_bytes(data)?;

    // Expected 12 accounts
    if instruction.accounts.len() < 12 {
        return Err(AccountError::InsufficientAccounts {
            required: 12,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = ReleaseFundsAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        operator: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        operator_pda: resolve_account(instruction, account_keys, 3)?,
        mint: resolve_account(instruction, account_keys, 4)?,
        allowed_mint: resolve_account(instruction, account_keys, 5)?,
        user_ata: resolve_account(instruction, account_keys, 6)?,
        instance_ata: resolve_account(instruction, account_keys, 7)?,
        token_program: resolve_account(instruction, account_keys, 8)?,
        associated_token_program: resolve_account(instruction, account_keys, 9)?,
        event_authority: resolve_account(instruction, account_keys, 10)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 11)?,
    };

    Ok(Some(EscrowInstruction::ReleaseFunds {
        accounts,
        data: ix_data,
    }))
}

/// Parse ResetSmtRoot instruction (no data, just accounts)
fn parse_reset_smt_root(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<EscrowInstruction>, ParserError> {
    // Expected 6 accounts
    if instruction.accounts.len() < 6 {
        return Err(AccountError::InsufficientAccounts {
            required: 6,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = ResetSmtRootAccounts {
        payer: resolve_account(instruction, account_keys, 0)?,
        operator: resolve_account(instruction, account_keys, 1)?,
        instance: resolve_account(instruction, account_keys, 2)?,
        operator_pda: resolve_account(instruction, account_keys, 3)?,
        event_authority: resolve_account(instruction, account_keys, 4)?,
        private_channel_escrow_program: resolve_account(instruction, account_keys, 5)?,
    };

    Ok(Some(EscrowInstruction::ResetSmtRoot { accounts }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // ============================================================================
    // Test Helper Functions
    // ============================================================================

    /// Create minimal valid Borsh-encoded data for CreateInstance instruction
    /// CreateInstanceIxData { bump: u8 }
    fn create_create_instance_borsh_data() -> Vec<u8> {
        vec![42] // Just one byte for bump
    }

    /// Create minimal valid Borsh-encoded data for AllowMint instruction
    /// AllowMintIxData { bump: u8 }
    fn create_allow_mint_borsh_data() -> Vec<u8> {
        vec![123] // Just one byte for bump
    }

    /// Create valid inner instruction data for AllowMint event matching the actual program format
    fn create_allow_mint_inner_instructions() -> Vec<InnerInstructions> {
        let mut data = vec![];

        // Event IX Tag (8 bytes)
        data.extend_from_slice(&[0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d]);

        // AllowMint discriminator (1 byte)
        data.push(1);

        // Instance seed (32 bytes) - dummy pubkey
        data.extend_from_slice(&[0u8; 32]);

        // Mint (32 bytes) - dummy pubkey
        data.extend_from_slice(&[0u8; 32]);

        // Decimals (1 byte)
        data.push(2);

        vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: ESCROW_PROGRAM_KEY_INDEX,
                    accounts: vec![],
                    data: bs58::encode(&data).into_string(),
                },
                stack_height: Some(2),
            }],
        }]
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

    /// Create minimal valid Borsh-encoded data for ReleaseFunds instruction
    /// ReleaseFundsIxData { amount: u64, user: [u8; 32], new_withdrawal_root: [u8; 32], transaction_nonce: u64 }
    fn create_release_funds_borsh_data() -> Vec<u8> {
        let mut data = vec![];
        data.extend_from_slice(&1000u64.to_le_bytes()); // amount
        data.extend_from_slice(&[0u8; 32]); // user pubkey
        data.extend_from_slice(&[1u8; 32]); // new_withdrawal_root
        data.extend_from_slice(&1u64.to_le_bytes()); // transaction_nonce
        data
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
        let instruction = create_instruction_with_accounts(7, data);
        let account_keys = create_n_account_keys(7);

        let result = parse_create_instance(&[42], &instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
    }

    #[test]
    fn test_create_instance_insufficient_accounts() {
        let data = encode_instruction_data(CREATE_INSTANCE, create_create_instance_borsh_data());
        let instruction = create_instruction_with_accounts(6, data); // Only 6 accounts (need 7)
        let account_keys = create_n_account_keys(6);

        let result = parse_create_instance(&[42], &instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // ============================================================================
    // parse_allow_mint Tests
    // ============================================================================

    #[test]
    fn test_allow_mint_valid_accounts() {
        let borsh_data = create_allow_mint_borsh_data();
        let instruction = create_instruction_with_accounts(11, "dummy".to_string());
        let account_keys = create_n_account_keys(11);

        let result = parse_allow_mint(
            &borsh_data,
            &instruction,
            &account_keys,
            &create_allow_mint_inner_instructions(),
        );

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
        if let Some(EscrowInstruction::AllowMint { data, .. }) = parsed {
            assert_eq!(data.bump, 123);
        } else {
            panic!("Expected AllowMint instruction");
        }
    }

    #[test]
    fn test_allow_mint_insufficient_accounts() {
        let borsh_data = create_allow_mint_borsh_data();
        let instruction = create_instruction_with_accounts(10, "dummy".to_string()); // Only 10 accounts (need 11)
        let account_keys = create_n_account_keys(10);

        let result = parse_allow_mint(
            &borsh_data,
            &instruction,
            &account_keys,
            &create_allow_mint_inner_instructions(),
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

        let result = parse_allow_mint(&borsh_data, &instruction, &account_keys, &[]);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No allow mint event found"), "Error: {}", err);
    }

    // ============================================================================
    // parse_block_mint Tests
    // ============================================================================

    #[test]
    fn test_block_mint_valid_accounts() {
        let instruction = create_instruction_with_accounts(8, "dummy".to_string());
        let account_keys = create_n_account_keys(8);

        let result = parse_block_mint(&instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        if let Some(EscrowInstruction::BlockMint { accounts }) = parsed {
            // instance @ index 2, mint @ index 3 — read straight from accounts.
            assert_eq!(accounts.instance, account_keys[2]);
            assert_eq!(accounts.mint, account_keys[3]);
        } else {
            panic!("Expected BlockMint instruction");
        }
    }

    #[test]
    fn test_block_mint_insufficient_accounts() {
        let instruction = create_instruction_with_accounts(7, "dummy".to_string()); // Only 7 accounts (need 8)
        let account_keys = create_n_account_keys(7);

        let result = parse_block_mint(&instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // ============================================================================
    // parse_deposit Tests
    // ============================================================================

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

    #[test]
    fn test_release_funds_valid_accounts() {
        let data = create_release_funds_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        let result = parse_release_funds(&data, &instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
    }

    #[test]
    fn test_release_funds_insufficient_accounts() {
        let data = create_release_funds_borsh_data();
        let instruction = create_instruction_with_accounts(11, "dummy".to_string()); // Only 11 accounts (need 12)
        let account_keys = create_n_account_keys(11);

        let result = parse_release_funds(&data, &instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // ============================================================================
    // parse_reset_smt_root Tests
    // ============================================================================

    #[test]
    fn test_reset_smt_root_valid_accounts() {
        let instruction = create_instruction_with_accounts(6, "dummy".to_string());
        let account_keys = create_n_account_keys(6);

        let result = parse_reset_smt_root(&instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
    }

    #[test]
    fn test_reset_smt_root_insufficient_accounts() {
        let instruction = create_instruction_with_accounts(5, "dummy".to_string()); // Only 5 accounts (need 6)
        let account_keys = create_n_account_keys(5);

        let result = parse_reset_smt_root(&instruction, &account_keys);

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

    /// A CPI deposit whose location carries no stack height can't be scoped to
    /// its own subtree, so the parser errors (drops it) rather than risk reading
    /// a neighbouring deposit's event amount.
    #[test]
    fn cpi_deposit_without_stack_height_errors_rather_than_guess() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        // Two deposits share one set, so guessing would be ambiguous.
        let inner_set = vec![InnerInstructions {
            index: 4,
            instructions: vec![
                deposit_ix_inner(2),
                deposit_event_inner(300, 3),
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
                    stack_height: None,
                }),
            },
        );

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("cannot scope DepositEvent"),
            "CPI deposit without stack height must error: {err}"
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
