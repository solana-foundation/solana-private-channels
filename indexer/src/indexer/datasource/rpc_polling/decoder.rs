use std::str::FromStr;

use crate::config::ProgramType;
use crate::error::ParserError;
use crate::indexer::datasource::common::parser::escrow::{
    parse_escrow_instruction, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use crate::indexer::datasource::common::parser::withdraw::{
    parse_withdraw_instruction, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
};
use crate::indexer::datasource::common::parser::{EscrowInstruction, WithdrawInstruction};
use crate::indexer::datasource::common::types::CompiledInstruction;
use crate::indexer::datasource::common::types::*;
use crate::indexer::datasource::rpc_polling::types::{InnerInstructions, RpcBlock};
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, warn};

type ParseInstructionFn<T> = fn(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
) -> Result<Option<T>, ParserError>;

/// Parse a block and extract program-specific instructions with metadata
pub fn parse_block(
    block: &RpcBlock,
    slot: u64,
    program_type: ProgramType,
    escrow_instance_id: Option<&Pubkey>,
) -> Vec<InstructionWithMetadata> {
    match program_type {
        ProgramType::Escrow => parse_block_for_program::<EscrowInstruction>(
            block,
            PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            parse_escrow_instruction,
            escrow_instance_id,
        )
        .into_iter()
        .map(
            |(signature, instruction_index, ix)| InstructionWithMetadata {
                instruction: ProgramInstruction::Escrow(Box::new(ix)),
                slot,
                program_type,
                signature: Some(signature),
                instruction_index,
            },
        )
        .collect(),
        ProgramType::Withdraw => parse_block_for_program::<WithdrawInstruction>(
            block,
            PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            parse_withdraw_instruction,
            None,
        )
        .into_iter()
        .map(
            |(signature, instruction_index, ix)| InstructionWithMetadata {
                instruction: ProgramInstruction::Withdraw(Box::new(ix)),
                slot,
                program_type,
                signature: Some(signature),
                instruction_index,
            },
        )
        .collect(),
    }
}

/// Parse a block and extract all instructions for a given program
/// Returns tuples of (signature, instruction_index, instruction) for each parsed
/// instruction, where instruction_index is the absolute position in the transaction.
pub fn parse_block_for_program<T>(
    block: &RpcBlock,
    filter_program_id: &str,
    parse_instruction: ParseInstructionFn<T>,
    escrow_instance_id: Option<&Pubkey>,
) -> Vec<(String, u32, T)>
where
    T: std::fmt::Debug,
{
    let mut instructions = Vec::new();

    for tx_with_meta in &block.transactions {
        let mut inner_instructions_list: &[InnerInstructions] = &[];

        // Skip failed transactions
        if let Some(meta) = &tx_with_meta.meta {
            if meta.err.is_some() {
                continue;
            }

            if let Some(inner_ix) = &meta.inner_instructions {
                inner_instructions_list = inner_ix;
            }
        }

        let tx = &tx_with_meta.transaction;
        let account_keys = &tx.message.account_keys;

        // Get transaction signature (first signature is the tx signature)
        // Skip transactions without valid signatures
        let Some(signature) = tx.signatures.first().cloned() else {
            warn!("Skipping transaction with no signature");
            continue;
        };

        let account_pubkeys: Vec<Pubkey> = account_keys
            .iter()
            .map(|s| Pubkey::from_str(s).expect("Invalid pubkey"))
            .collect();

        // Filter transactions by instance ID if provided
        // Check if any account in the transaction matches the instance ID
        if let Some(instance_id) = escrow_instance_id {
            if !account_pubkeys.contains(instance_id) {
                continue; // Skip this transaction entirely
            }
        }

        // Enumerate before the program-id filter so the index is the instruction's
        // absolute position in the transaction, independent of how many are relevant.
        for (ix_index, instruction) in tx.message.instructions.iter().enumerate() {
            let program_id = account_keys.get(instruction.program_id_index as usize);

            // Only parse program filtered instructions
            if program_id == Some(&filter_program_id.to_string()) {
                match parse_instruction(instruction, &account_pubkeys, inner_instructions_list) {
                    Ok(Some(ix)) => {
                        instructions.push((signature.clone(), ix_index as u32, ix));
                    }
                    Ok(None) => {
                        debug!("Skipped unsupported instruction");
                    }
                    Err(e) => {
                        warn!("Failed to parse instruction: {}", e);
                    }
                }
            }
        }
    }

    instructions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{error::ParserError, test_utils::rpc_blocks::*};

    const TEST_PROGRAM_ID: &str = "TestProgram11111111111111111111111111111111";

    // ============================================================================
    // Mock Parsers for Testing
    // ============================================================================

    /// Mock parser that returns the instruction data as-is
    fn mock_parser(
        instruction: &CompiledInstruction,
        _account_keys: &[Pubkey],
        _inner_instructions: &[InnerInstructions],
    ) -> Result<Option<String>, ParserError> {
        Ok(Some(instruction.data.clone()))
    }

    /// Mock parser that always returns None
    fn mock_parser_returns_none(
        _instruction: &CompiledInstruction,
        _account_keys: &[Pubkey],
        _inner_instructions: &[InnerInstructions],
    ) -> Result<Option<String>, ParserError> {
        Ok(None)
    }

    /// Mock parser that always returns an error
    fn mock_parser_returns_error(
        _instruction: &CompiledInstruction,
        _account_keys: &[Pubkey],
        _inner_instructions: &[InnerInstructions],
    ) -> Result<Option<String>, ParserError> {
        Err(ParserError::InstructionParseFailed {
            reason: "Parse error".to_string(),
        })
    }

    // ============================================================================
    // parse_block_for_program Tests
    // ============================================================================

    #[test]
    fn test_empty_block() {
        let block = create_test_block();

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert!(result.is_empty());
    }

    #[test]
    fn test_no_matching_program() {
        let mut block = create_test_block();
        let account_keys =
            create_account_keys_with_program("DifferentProgram1111111111111111111111111111", 0);
        let instruction = create_instruction(0, vec![], "test_data".to_string());

        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys,
            vec![instruction],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert!(result.is_empty());
    }

    #[test]
    fn test_skip_failed_transactions() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let instruction = create_instruction(0, vec![], "test_data".to_string());

        // Add a failed transaction
        block.transactions.push(create_failed_transaction(
            "sig_failed".to_string(),
            account_keys,
            vec![instruction],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert!(result.is_empty());
    }

    #[test]
    fn test_multiple_instructions_same_tx() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);

        let ix1 = create_instruction(0, vec![], "data1".to_string());
        let ix2 = create_instruction(0, vec![], "data2".to_string());
        let ix3 = create_instruction(0, vec![], "data3".to_string());

        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys,
            vec![ix1, ix2, ix3],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("sig1".to_string(), 0u32, "data1".to_string()));
        assert_eq!(result[1], ("sig1".to_string(), 1u32, "data2".to_string()));
        assert_eq!(result[2], ("sig1".to_string(), 2u32, "data3".to_string()));
    }

    #[test]
    fn test_index_is_absolute_position_across_filtered_instructions() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);

        let ix0 = create_instruction(0, vec![], "data0".to_string());
        // Middle instruction targets a different program and is filtered out.
        let ix1 = create_instruction(1, vec![], "data1".to_string());
        let ix2 = create_instruction(0, vec![], "data2".to_string());

        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys,
            vec![ix0, ix1, ix2],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("sig1".to_string(), 0u32, "data0".to_string()));
        assert_eq!(result[1], ("sig1".to_string(), 2u32, "data2".to_string()));
    }

    #[test]
    fn test_multiple_transactions() {
        let mut block = create_test_block();

        // Transaction 1
        let account_keys1 = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix1 = create_instruction(0, vec![], "data1".to_string());
        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys1,
            vec![ix1],
        ));

        // Transaction 2
        let account_keys2 = create_account_keys_with_program(TEST_PROGRAM_ID, 1);
        let ix2 = create_instruction(1, vec![], "data2".to_string());
        block.transactions.push(create_successful_transaction(
            "sig2".to_string(),
            account_keys2,
            vec![ix2],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("sig1".to_string(), 0u32, "data1".to_string()));
        assert_eq!(result[1], ("sig2".to_string(), 0u32, "data2".to_string()));
    }

    #[test]
    fn test_mixed_success_failure() {
        let mut block = create_test_block();

        // Successful transaction
        let account_keys1 = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix1 = create_instruction(0, vec![], "success".to_string());
        block.transactions.push(create_successful_transaction(
            "sig_success".to_string(),
            account_keys1,
            vec![ix1],
        ));

        // Failed transaction
        let account_keys2 = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix2 = create_instruction(0, vec![], "failed".to_string());
        block.transactions.push(create_failed_transaction(
            "sig_failed".to_string(),
            account_keys2,
            vec![ix2],
        ));

        // Another successful transaction
        let account_keys3 = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix3 = create_instruction(0, vec![], "success2".to_string());
        block.transactions.push(create_successful_transaction(
            "sig_success2".to_string(),
            account_keys3,
            vec![ix3],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            ("sig_success".to_string(), 0u32, "success".to_string())
        );
        assert_eq!(
            result[1],
            ("sig_success2".to_string(), 0u32, "success2".to_string())
        );
    }

    #[test]
    fn test_missing_signature_skips_transaction() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let instruction = create_instruction(0, vec![], "test_data".to_string());

        // Create transaction with empty signatures
        let mut tx =
            create_successful_transaction("dummy".to_string(), account_keys, vec![instruction]);
        tx.transaction.signatures = vec![]; // Clear signatures

        block.transactions.push(tx);

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        // Transaction without signature should be skipped entirely
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_returns_none() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let instruction = create_instruction(0, vec![], "test_data".to_string());

        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys,
            vec![instruction],
        ));

        let result =
            parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser_returns_none, None);

        // Should be empty because parser returned None (unsupported instruction)
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_returns_error() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let instruction = create_instruction(0, vec![], "test_data".to_string());

        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys,
            vec![instruction],
        ));

        let result =
            parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser_returns_error, None);

        // Should be empty because parser returned error (logged but continued)
        assert!(result.is_empty());
    }

    #[test]
    fn test_program_id_different_indices() {
        let mut block = create_test_block();

        // Program at index 0
        let account_keys1 = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix1 = create_instruction(0, vec![], "at_index_0".to_string());
        block.transactions.push(create_successful_transaction(
            "sig1".to_string(),
            account_keys1,
            vec![ix1],
        ));

        // Program at index 5
        let account_keys2 = create_account_keys_with_program(TEST_PROGRAM_ID, 5);
        let ix2 = create_instruction(5, vec![], "at_index_5".to_string());
        block.transactions.push(create_successful_transaction(
            "sig2".to_string(),
            account_keys2,
            vec![ix2],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            ("sig1".to_string(), 0u32, "at_index_0".to_string())
        );
        assert_eq!(
            result[1],
            ("sig2".to_string(), 0u32, "at_index_5".to_string())
        );
    }

    #[test]
    fn test_transaction_no_meta_treated_as_successful() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let instruction = create_instruction(0, vec![], "no_meta".to_string());

        block.transactions.push(create_transaction_no_meta(
            "sig_no_meta".to_string(),
            account_keys,
            vec![instruction],
        ));

        let result = parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            ("sig_no_meta".to_string(), 0u32, "no_meta".to_string())
        );
    }
}
