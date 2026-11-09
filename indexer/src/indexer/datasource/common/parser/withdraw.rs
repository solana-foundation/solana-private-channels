use crate::{
    error::{account::AccountError, ParserError},
    indexer::datasource::common::parser::resolve_account,
    indexer::datasource::common::types::{
        CompiledInstruction, InnerInstructions, InstructionLocation,
    },
};
use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

// PrivateChannel Withdraw Program ID
pub const PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID: &str =
    "J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi";

// Instruction discriminators
const WITHDRAW_FUNDS: u8 = 0;
const SET_WITHDRAW_CONFIG: u8 = 1;
// Same data and accounts as WithdrawFunds; the release goes to the confidential balance.
pub(crate) const CONFIDENTIAL_WITHDRAW_FUNDS: u8 = 2;

/// Only the user-initiated withdrawals are indexed. `SetWithdrawConfig` rides in the operator's deposit mint transactions and parses to `Ok(None)`, so no inner (CPI) discriminator needs excluding; mirrors the escrow predicate so both decoders share one per-program source of truth.
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
    /// Set for `ConfidentialWithdrawFunds`.
    pub confidential: bool,
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

    // Allow-list: an instruction this parser does not know could be a burn it would skip,
    // so it fails the slot instead.
    let Some((&discriminator, ix_data)) = data.split_first() else {
        return Err(not_allowed("empty instruction data".to_string()));
    };

    match discriminator {
        WITHDRAW_FUNDS => parse_withdraw_funds(ix_data, instruction, account_keys, false),
        CONFIDENTIAL_WITHDRAW_FUNDS => {
            parse_withdraw_funds(ix_data, instruction, account_keys, true)
        }
        // Rides in the operator's deposit mint transactions and moves no funds.
        SET_WITHDRAW_CONFIG => Ok(None),
        other => Err(not_allowed(format!("discriminator {other}"))),
    }
}

fn not_allowed(what: String) -> ParserError {
    ParserError::InstructionParseFailed {
        reason: format!("withdraw instruction not on the allow-list: {what}"),
    }
}

/// Parse WithdrawFunds or ConfidentialWithdrawFunds, which share data and accounts.
fn parse_withdraw_funds(
    data: &[u8],
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    confidential: bool,
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
            confidential,
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

        let result = parse_withdraw_funds(&borsh_data, &instruction, &account_keys, false);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
    }

    #[test]
    fn test_withdraw_funds_insufficient_accounts() {
        let borsh_data = create_withdraw_funds_borsh_data();
        let instruction = create_instruction_with_accounts(4, "dummy".to_string()); // Only 4 accounts (need 5)
        let account_keys = create_n_account_keys(4);

        let result = parse_withdraw_funds(&borsh_data, &instruction, &account_keys, false);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Insufficient accounts"), "Error: {}", err);
    }

    // ============================================================================
    // Allow-list Tests
    // ============================================================================

    fn parse_with_discriminator(
        discriminator: u8,
    ) -> Result<Option<WithdrawInstruction>, ParserError> {
        let mut data = vec![discriminator];
        data.extend(create_withdraw_funds_borsh_data());
        let instruction = create_instruction_with_accounts(5, bs58::encode(data).into_string());
        parse_withdraw_instruction(
            &instruction,
            &create_n_account_keys(5),
            &[],
            InstructionLocation {
                top_level_index: 0,
                inner: None,
            },
        )
    }

    #[test]
    fn withdraw_funds_is_public() {
        let Some(WithdrawInstruction::WithdrawFunds { data, .. }) =
            parse_with_discriminator(WITHDRAW_FUNDS).unwrap()
        else {
            panic!("WithdrawFunds must parse");
        };
        assert_eq!(data.amount, 1000);
        assert!(!data.confidential);
    }

    #[test]
    fn set_withdraw_config_is_accepted_and_skipped() {
        assert!(parse_with_discriminator(SET_WITHDRAW_CONFIG)
            .unwrap()
            .is_none());
    }

    #[test]
    fn confidential_withdraw_funds_parses_as_a_confidential_withdrawal() {
        let Some(WithdrawInstruction::WithdrawFunds { accounts, data }) =
            parse_with_discriminator(CONFIDENTIAL_WITHDRAW_FUNDS).unwrap()
        else {
            panic!("ConfidentialWithdrawFunds must parse");
        };
        assert_eq!(data.amount, 1000);
        assert_eq!(data.destination, accounts.user);
        assert!(data.confidential);
    }

    #[test]
    fn any_other_instruction_fails_the_slot() {
        for discriminator in [3u8, 4, 7, 228, 255] {
            let err = parse_with_discriminator(discriminator).unwrap_err();
            assert!(err.to_string().contains("not on the allow-list"), "{err}");
        }
    }

    // The parser's discriminators must stay those of the deployed program's client.
    #[test]
    fn discriminators_match_the_withdraw_program_client() {
        use private_channel_withdraw_program_client::instructions::{
            CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR, SET_WITHDRAW_CONFIG_DISCRIMINATOR,
            WITHDRAW_FUNDS_DISCRIMINATOR,
        };
        assert_eq!(WITHDRAW_FUNDS, WITHDRAW_FUNDS_DISCRIMINATOR);
        assert_eq!(SET_WITHDRAW_CONFIG, SET_WITHDRAW_CONFIG_DISCRIMINATOR);
        assert_eq!(
            CONFIDENTIAL_WITHDRAW_FUNDS,
            CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR
        );
    }

    /// Parses an instruction the generated client built, with its real account order.
    fn parse_client_instruction(
        instruction: solana_sdk::instruction::Instruction,
    ) -> WithdrawInstruction {
        let account_keys: Vec<Pubkey> = instruction.accounts.iter().map(|m| m.pubkey).collect();
        let compiled = CompiledInstruction {
            program_id_index: 0,
            accounts: (0..account_keys.len() as u8).collect(),
            data: bs58::encode(&instruction.data).into_string(),
        };
        parse_withdraw_instruction(
            &compiled,
            &account_keys,
            &[],
            InstructionLocation {
                top_level_index: 0,
                inner: None,
            },
        )
        .expect("client instruction parses")
        .expect("client instruction is a withdrawal")
    }

    #[test]
    fn confidential_withdraw_funds_from_the_client_parses_as_confidential() {
        use private_channel_withdraw_program_client::instructions::ConfidentialWithdrawFundsBuilder;
        let (user, mint, token_account) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let destination = Pubkey::new_unique();
        let max = (1u64 << 48) - 1;
        let mut builder = ConfidentialWithdrawFundsBuilder::new();
        builder
            .user(user)
            .mint(mint)
            .token_account(token_account)
            .withdraw_config(Pubkey::new_unique())
            .treasury_token_account(Pubkey::new_unique())
            .amount(max);

        let WithdrawInstruction::WithdrawFunds { accounts, data } =
            parse_client_instruction(builder.instruction());
        assert_eq!(
            (accounts.user, accounts.mint, accounts.token_account),
            (user, mint, token_account)
        );
        assert_eq!((data.amount, data.destination), (max, user));
        assert!(data.confidential);

        builder.destination(destination);
        let WithdrawInstruction::WithdrawFunds { data, .. } =
            parse_client_instruction(builder.instruction());
        assert_eq!(data.destination, destination);
        assert!(data.confidential);
    }

    #[test]
    fn withdraw_funds_from_the_client_stays_public() {
        use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;
        let instruction = WithdrawFundsBuilder::new()
            .user(Pubkey::new_unique())
            .mint(Pubkey::new_unique())
            .token_account(Pubkey::new_unique())
            .withdraw_config(Pubkey::new_unique())
            .treasury_token_account(Pubkey::new_unique())
            .amount(1u64 << 48)
            .instruction();

        let WithdrawInstruction::WithdrawFunds { data, .. } = parse_client_instruction(instruction);
        assert_eq!(data.amount, 1u64 << 48);
        assert!(!data.confidential);
    }

    #[test]
    fn empty_instruction_data_fails_the_slot() {
        let instruction = create_instruction_with_accounts(5, String::new());
        let result = parse_withdraw_instruction(
            &instruction,
            &create_n_account_keys(5),
            &[],
            InstructionLocation {
                top_level_index: 0,
                inner: None,
            },
        );
        assert!(result.is_err());
    }
}
