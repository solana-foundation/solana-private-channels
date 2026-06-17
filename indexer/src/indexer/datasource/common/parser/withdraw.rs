use crate::{
    error::{account::AccountError, ParserError},
    indexer::datasource::common::parser::resolve_account,
    indexer::datasource::common::types::{CompiledInstruction, InstructionLocation},
    indexer::datasource::rpc_polling::types::InnerInstructions,
};
use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

// PrivateChannel Withdraw Program ID
pub const PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID: &str =
    "J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi";

// Instruction discriminators
const WITHDRAW_FUNDS: u8 = 0;

/// Withdraw exposes only the user-initiated `WithdrawFunds`, so no inner (CPI) discriminator is excluded; mirrors the escrow predicate so both decoders share one per-program source of truth.
pub fn withdraw_inner_discriminator_excluded(_discriminator: u8) -> bool {
    false
}

// ******************************************************************************************
// Data types for instructions
// ******************************************************************************************
#[derive(BorshDeserialize)]
struct WithdrawFundsIxData {
    amount: u64,
    destination: Option<[u8; 32]>,
}

// ******************************************************************************************
// Instruction types
// ******************************************************************************************
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WithdrawInstruction {
    WithdrawFunds {
        accounts: WithdrawFundsAccounts,
        data: WithdrawFundsData,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawFundsAccounts {
    pub user: Pubkey,
    pub mint: Pubkey,
    pub token_account: Pubkey,
    pub token_program: Pubkey,
    pub associated_token_program: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawFundsData {
    pub amount: u64,
    pub destination: Pubkey,
}

// ******************************************************************************************
// Parse instructions
// ******************************************************************************************
pub fn parse_withdraw_instruction(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    _inner_instructions: &[InnerInstructions],
    // WithdrawFunds emits no event; the unused location keeps a uniform parser signature across both programs.
    _location: InstructionLocation,
) -> Result<Option<WithdrawInstruction>, ParserError> {
    // Decode base58 instruction data
    let data = bs58::decode(&instruction.data).into_vec()?;

    if data.is_empty() {
        return Ok(None);
    }

    let discriminator = data[0];
    let ix_data = &data[1..];

    match discriminator {
        WITHDRAW_FUNDS => parse_withdraw_funds(ix_data, instruction, account_keys),
        _ => Ok(None), // Unsupported instruction type
    }
}

/// Parse WithdrawFunds instruction
fn parse_withdraw_funds(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<WithdrawInstruction>, ParserError> {
    let ix_data = WithdrawFundsIxData::deserialize(&mut &data[..])?;

    // Expected 5 accounts
    if instruction.accounts.len() < 5 {
        return Err(AccountError::InsufficientAccounts {
            required: 5,
            actual: instruction.accounts.len(),
        }
        .into());
    }

    let user = resolve_account(instruction, account_keys, 0)?;

    let accounts = WithdrawFundsAccounts {
        user,
        mint: resolve_account(instruction, account_keys, 1)?,
        token_account: resolve_account(instruction, account_keys, 2)?,
        token_program: resolve_account(instruction, account_keys, 3)?,
        associated_token_program: resolve_account(instruction, account_keys, 4)?,
    };

    let destination = ix_data
        .destination
        .map(Pubkey::new_from_array)
        .unwrap_or(user);

    Ok(Some(WithdrawInstruction::WithdrawFunds {
        accounts,
        data: WithdrawFundsData {
            amount: ix_data.amount,
            destination,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Test Helper Functions
    // ============================================================================

    /// Create minimal valid Borsh-encoded data for WithdrawFunds instruction
    /// WithdrawFundsIxData { amount: u64, destination: Option<[u8; 32]> }
    fn create_withdraw_funds_borsh_data() -> Vec<u8> {
        let mut data = vec![];
        data.extend_from_slice(&1000u64.to_le_bytes()); // amount
        data.push(0); // None for destination (Option discriminator = 0)
        data
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
    // parse_withdraw_funds Tests
    // ============================================================================

    #[test]
    fn test_withdraw_funds_valid_accounts() {
        let borsh_data = create_withdraw_funds_borsh_data();
        let instruction = create_instruction_with_accounts(5, "dummy".to_string());
        let account_keys = create_n_account_keys(5);

        let result = parse_withdraw_funds(&borsh_data, &instruction, &account_keys);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
    }

    #[test]
    fn test_withdraw_funds_insufficient_accounts() {
        let borsh_data = create_withdraw_funds_borsh_data();
        let instruction = create_instruction_with_accounts(4, "dummy".to_string()); // Only 4 accounts (need 5)
        let account_keys = create_n_account_keys(4);

        let result = parse_withdraw_funds(&borsh_data, &instruction, &account_keys);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }
}
