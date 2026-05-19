use crate::{
    error::{account::AccountError, ParserError},
    indexer::datasource::common::types::*,
    indexer::datasource::rpc_polling::types::InnerInstructions,
};

use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

// PrivateChannel Escrow Program ID
pub const PRIVATE_CHANNEL_ESCROW_PROGRAM_ID: &str = "GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83";

// Instruction discriminators (from IDL)
const CREATE_INSTANCE: u8 = 0;
const ALLOW_MINT: u8 = 1;
const DEPOSIT: u8 = 6;
const RELEASE_FUNDS: u8 = 7;
const RESET_SMT_ROOT: u8 = 8;

// Event related constants
const EVENT_IX_TAG_LE: &[u8] = &[0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d];
const ALLOW_MINT_EVENT_DISCRIMINATOR: u8 = 1;
const DEPOSIT_EVENT_DISCRIMINATOR: u8 = 6;
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
/// Return the `accounts.instance` carried by any escrow instruction variant.
pub fn escrow_instance_of(ix: &EscrowInstruction) -> Pubkey {
    match ix {
        EscrowInstruction::CreateInstance { accounts, .. } => accounts.instance,
        EscrowInstruction::AllowMint { accounts, .. } => accounts.instance,
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
        DEPOSIT => parse_deposit(ix_data, instruction, account_keys, inner_instructions),
        RELEASE_FUNDS => parse_release_funds(ix_data, instruction, account_keys),
        RESET_SMT_ROOT => parse_reset_smt_root(instruction, account_keys),
        _ => Ok(None), // Unsupported instruction type
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
        payer: account_keys[instruction.accounts[0] as usize],
        admin: account_keys[instruction.accounts[1] as usize],
        instance_seed: account_keys[instruction.accounts[2] as usize],
        instance: account_keys[instruction.accounts[3] as usize],
        system_program: account_keys[instruction.accounts[4] as usize],
        event_authority: account_keys[instruction.accounts[5] as usize],
        private_channel_escrow_program: account_keys[instruction.accounts[6] as usize],
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
        payer: account_keys[instruction.accounts[0] as usize],
        admin: account_keys[instruction.accounts[1] as usize],
        instance: account_keys[instruction.accounts[2] as usize],
        mint: account_keys[instruction.accounts[3] as usize],
        allowed_mint: account_keys[instruction.accounts[4] as usize],
        instance_ata: account_keys[instruction.accounts[5] as usize],
        system_program: account_keys[instruction.accounts[6] as usize],
        token_program: account_keys[instruction.accounts[7] as usize],
        associated_token_program: account_keys[instruction.accounts[8] as usize],
        event_authority: account_keys[instruction.accounts[9] as usize],
        private_channel_escrow_program: account_keys[instruction.accounts[10] as usize],
    };

    for inner_instruction_set in inner_instructions {
        for inner_instruction in &inner_instruction_set.instructions {
            let Ok(event_data) = bs58::decode(&inner_instruction.data).into_vec() else {
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

/// Parse Deposit instruction
fn parse_deposit(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
) -> Result<Option<EscrowInstruction>, ParserError> {
    let ix_data = <DepositData as borsh::BorshDeserialize>::deserialize(&mut &data[..])?;

    // Expected 12 accounts
    if instruction.accounts.len() < 12 {
        return Err(AccountError::InsufficientAccounts {
            required: 12,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let accounts = DepositAccounts {
        payer: account_keys[instruction.accounts[0] as usize],
        user: account_keys[instruction.accounts[1] as usize],
        instance: account_keys[instruction.accounts[2] as usize],
        mint: account_keys[instruction.accounts[3] as usize],
        allowed_mint: account_keys[instruction.accounts[4] as usize],
        user_ata: account_keys[instruction.accounts[5] as usize],
        instance_ata: account_keys[instruction.accounts[6] as usize],
        system_program: account_keys[instruction.accounts[7] as usize],
        token_program: account_keys[instruction.accounts[8] as usize],
        associated_token_program: account_keys[instruction.accounts[9] as usize],
        event_authority: account_keys[instruction.accounts[10] as usize],
        private_channel_escrow_program: account_keys[instruction.accounts[11] as usize],
    };

    for inner_instruction_set in inner_instructions {
        for inner_instruction in &inner_instruction_set.instructions {
            let Ok(event_data) = bs58::decode(&inner_instruction.data).into_vec() else {
                continue;
            };

            if event_data.len() >= 145
                && event_data.starts_with(EVENT_IX_TAG_LE)
                && event_data[EVENT_DISCRIMINATOR_INDEX] == DEPOSIT_EVENT_DISCRIMINATOR
            {
                // Safety: the `>= 145` guard above guarantees this slice is
                // always exactly 8 bytes; the unwrap cannot panic.
                let amount_bytes: [u8; 8] = event_data[EVENT_AMOUNT_INDEX..EVENT_AMOUNT_INDEX + 8]
                    .try_into()
                    .unwrap();

                let amount = u64::from_le_bytes(amount_bytes);

                return Ok(Some(EscrowInstruction::Deposit {
                    accounts,
                    data: ix_data,
                    event: DepositEvent { amount },
                }));
            }
        }
    }

    Err(ParserError::InstructionParseFailed {
        reason: "No deposit event found".to_string(),
    })
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
        payer: account_keys[instruction.accounts[0] as usize],
        operator: account_keys[instruction.accounts[1] as usize],
        instance: account_keys[instruction.accounts[2] as usize],
        operator_pda: account_keys[instruction.accounts[3] as usize],
        mint: account_keys[instruction.accounts[4] as usize],
        allowed_mint: account_keys[instruction.accounts[5] as usize],
        user_ata: account_keys[instruction.accounts[6] as usize],
        instance_ata: account_keys[instruction.accounts[7] as usize],
        token_program: account_keys[instruction.accounts[8] as usize],
        associated_token_program: account_keys[instruction.accounts[9] as usize],
        event_authority: account_keys[instruction.accounts[10] as usize],
        private_channel_escrow_program: account_keys[instruction.accounts[11] as usize],
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
        payer: account_keys[instruction.accounts[0] as usize],
        operator: account_keys[instruction.accounts[1] as usize],
        instance: account_keys[instruction.accounts[2] as usize],
        operator_pda: account_keys[instruction.accounts[3] as usize],
        event_authority: account_keys[instruction.accounts[4] as usize],
        private_channel_escrow_program: account_keys[instruction.accounts[5] as usize],
    };

    Ok(Some(EscrowInstruction::ResetSmtRoot { accounts }))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: bs58::encode(&data).into_string(),
            }],
        }]
    }

    /// Create minimal valid Borsh-encoded data for Deposit instruction
    /// DepositIxData { amount: u64, recipient: Option<[u8; 32]> }
    fn create_deposit_borsh_data() -> Vec<u8> {
        let mut data = vec![];
        data.extend_from_slice(&1000u64.to_le_bytes()); // amount
        data.push(0); // None for recipient (Option discriminator = 0)
        data
    }

    /// Build inner instructions carrying a valid DepositEvent CPI with the
    /// given received `amount`. Layout: tag(8) + disc(1) + instance_seed(32)
    /// + user(32) + amount(8 LE) + recipient(32) + mint(32) = 145 bytes.
    fn create_deposit_inner_instructions(amount: u64) -> Vec<InnerInstructions> {
        let mut data = vec![];
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(DEPOSIT_EVENT_DISCRIMINATOR);
        data.extend_from_slice(&[0u8; 32]); // instance_seed
        data.extend_from_slice(&[0u8; 32]); // user
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&[0u8; 32]); // recipient
        data.extend_from_slice(&[0u8; 32]); // mint

        vec![InnerInstructions {
            index: 0,
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: bs58::encode(&data).into_string(),
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

    /// Create N account keys for testing
    fn create_n_account_keys(n: usize) -> Vec<Pubkey> {
        (0..n)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = i as u8;
                Pubkey::new_from_array(bytes)
            })
            .collect()
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

        let result = parse_deposit(&borsh_data, &instruction, &account_keys, &[]);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    #[test]
    fn test_deposit_no_event_errs() {
        let borsh_data = create_deposit_borsh_data();
        let instruction = create_instruction_with_accounts(12, "dummy".to_string());
        let account_keys = create_n_account_keys(12);

        let result = parse_deposit(&borsh_data, &instruction, &account_keys, &[]);

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
}
