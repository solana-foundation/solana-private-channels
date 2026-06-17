use std::str::FromStr;

use crate::config::ProgramType;
use crate::error::ParserError;
use crate::indexer::datasource::common::parser::escrow::{
    escrow_inner_discriminator_excluded, parse_escrow_instruction,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use crate::indexer::datasource::common::parser::withdraw::{
    parse_withdraw_instruction, withdraw_inner_discriminator_excluded,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
};
use crate::indexer::datasource::common::parser::{EscrowInstruction, WithdrawInstruction};
use crate::indexer::datasource::common::types::CompiledInstruction;
use crate::indexer::datasource::common::types::*;
use crate::indexer::datasource::rpc_polling::types::{InnerInstructions, RpcBlock};
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, error, warn};

type ParseInstructionFn<T> = fn(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
    location: InstructionLocation,
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
            escrow_inner_discriminator_excluded,
            escrow_instance_id,
        )
        .into_iter()
        .map(|(signature, location, ix)| InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(ix)),
            slot,
            program_type,
            signature: Some(signature),
            instruction_index: location.top_level_index,
            inner_index: location.inner.map(|i| i.inner_index),
        })
        .collect(),
        ProgramType::Withdraw => parse_block_for_program::<WithdrawInstruction>(
            block,
            PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            parse_withdraw_instruction,
            withdraw_inner_discriminator_excluded,
            None,
        )
        .into_iter()
        .map(|(signature, location, ix)| InstructionWithMetadata {
            instruction: ProgramInstruction::Withdraw(Box::new(ix)),
            slot,
            program_type,
            signature: Some(signature),
            instruction_index: location.top_level_index,
            inner_index: location.inner.map(|i| i.inner_index),
        })
        .collect(),
    }
}

/// First byte (Anchor-style discriminator) of an instruction's base58 data, or `None` if empty/undecodable.
fn instruction_discriminator(instruction: &CompiledInstruction) -> Option<u8> {
    bs58::decode(&instruction.data)
        .into_vec()
        .ok()
        .and_then(|d| d.first().copied())
}

/// Parse a block and return (signature, location, instruction) for every
/// instruction of the given program.
pub fn parse_block_for_program<T>(
    block: &RpcBlock,
    filter_program_id: &str,
    parse_instruction: ParseInstructionFn<T>,
    inner_discriminator_excluded: fn(u8) -> bool,
    escrow_instance_id: Option<&Pubkey>,
) -> Vec<(String, InstructionLocation, T)>
where
    T: std::fmt::Debug,
{
    let mut instructions = Vec::new();

    for tx_with_meta in &block.transactions {
        let mut inner_instructions_list: &[InnerInstructions] = &[];
        let mut loaded_writable: &[String] = &[];
        let mut loaded_readonly: &[String] = &[];

        // Skip failed transactions
        if let Some(meta) = &tx_with_meta.meta {
            if meta.err.is_some() {
                continue;
            }

            if let Some(inner_ix) = &meta.inner_instructions {
                inner_instructions_list = inner_ix;
            }

            if let Some(loaded) = &meta.loaded_addresses {
                loaded_writable = &loaded.writable;
                loaded_readonly = &loaded.readonly;
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

        // Inner (and v0 top-level) account indices reference the full key list:
        // static message keys, then loaded writable, then readonly. Append the
        // already-resolved loaded keys so those indices resolve.
        // A malformed key makes every account index untrustworthy (dropping one
        // would shift the rest), so skip the whole transaction rather than panic.
        let account_pubkeys: Vec<Pubkey> = match account_keys
            .iter()
            .chain(loaded_writable.iter())
            .chain(loaded_readonly.iter())
            .map(|s| Pubkey::from_str(s))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(keys) => keys,
            Err(e) => {
                warn!("Skipping transaction {signature}: invalid account key: {e}");
                continue;
            }
        };

        // Filter transactions by instance ID if provided
        // Check if any account in the transaction matches the instance ID
        if let Some(instance_id) = escrow_instance_id {
            if !account_pubkeys.contains(instance_id) {
                continue; // Skip this transaction entirely
            }
        }

        // The filter program id is a hardcoded constant; a parse failure is a
        // programming error, not a per-transaction condition.
        let Ok(filter_pubkey) = Pubkey::from_str(filter_program_id) else {
            error!("Invalid filter program id: {filter_program_id}");
            return instructions;
        };

        // Enumerate before the program-id filter so the index is the instruction's
        // absolute position in the transaction, independent of how many are relevant.
        for (ix_index, instruction) in tx.message.instructions.iter().enumerate() {
            // Resolve against the full key list
            let program_id = account_pubkeys.get(instruction.program_id_index as usize);

            // Only parse program filtered instructions
            if program_id == Some(&filter_pubkey) {
                let location = InstructionLocation::top_level(ix_index as u32);
                match parse_instruction(
                    instruction,
                    &account_pubkeys,
                    inner_instructions_list,
                    location,
                ) {
                    Ok(Some(ix)) => {
                        instructions.push((signature.clone(), location, ix));
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

        // Inner (CPI) instructions: parse the filtered program's user-initiated
        // instructions.
        for inner_set in inner_instructions_list {
            for (inner_ix_index, inner) in inner_set.instructions.iter().enumerate() {
                let program_id = account_pubkeys.get(inner.instruction.program_id_index as usize);
                if program_id != Some(&filter_pubkey) {
                    continue;
                }
                if instruction_discriminator(&inner.instruction)
                    .is_some_and(inner_discriminator_excluded)
                {
                    continue;
                }

                let location = InstructionLocation {
                    top_level_index: inner_set.index as u32,
                    inner: Some(InnerLocation {
                        inner_index: inner_ix_index as u32,
                        stack_height: inner.stack_height,
                    }),
                };
                match parse_instruction(
                    &inner.instruction,
                    &account_pubkeys,
                    inner_instructions_list,
                    location,
                ) {
                    Ok(Some(ix)) => {
                        instructions.push((signature.clone(), location, ix));
                    }
                    Ok(None) => {
                        debug!("Skipped unsupported inner instruction");
                    }
                    Err(e) => {
                        warn!("Failed to parse inner instruction: {}", e);
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
        _location: InstructionLocation,
    ) -> Result<Option<String>, ParserError> {
        Ok(Some(instruction.data.clone()))
    }

    /// Mock parser that always returns None
    fn mock_parser_returns_none(
        _instruction: &CompiledInstruction,
        _account_keys: &[Pubkey],
        _inner_instructions: &[InnerInstructions],
        _location: InstructionLocation,
    ) -> Result<Option<String>, ParserError> {
        Ok(None)
    }

    /// Mock parser that always returns an error
    fn mock_parser_returns_error(
        _instruction: &CompiledInstruction,
        _account_keys: &[Pubkey],
        _inner_instructions: &[InnerInstructions],
        _location: InstructionLocation,
    ) -> Result<Option<String>, ParserError> {
        Err(ParserError::InstructionParseFailed {
            reason: "Parse error".to_string(),
        })
    }

    /// Never excludes: top-level mock parser tests are unaffected by inner skips.
    fn never_excluded(_discriminator: u8) -> bool {
        false
    }

    /// Wrapper so mock-parser tests keep their shape over the (signature, location, value) result tuple.
    fn parse_for_test(
        block: &RpcBlock,
        program_id: &str,
        parse_instruction: ParseInstructionFn<String>,
        escrow_instance_id: Option<&Pubkey>,
    ) -> Vec<(String, u32, String)> {
        parse_block_for_program(
            block,
            program_id,
            parse_instruction,
            never_excluded,
            escrow_instance_id,
        )
        .into_iter()
        .map(|(sig, location, value)| (sig, location.top_level_index, value))
        .collect()
    }

    // ============================================================================
    // parse_block_for_program Tests
    // ============================================================================

    #[test]
    fn test_empty_block() {
        let block = create_test_block();

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser_returns_none, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser_returns_error, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

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

        let result = parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None);

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            ("sig_no_meta".to_string(), 0u32, "no_meta".to_string())
        );
    }

    // ============================================================================
    // CPI (inner instruction) Tests
    // ============================================================================

    use crate::indexer::datasource::rpc_polling::types::{InnerInstruction, InnerInstructions};
    use solana_transaction_status::UiLoadedAddresses;

    fn inner(program_id_index: u8, data: &str, stack_height: u32) -> InnerInstruction {
        InnerInstruction {
            instruction: create_instruction(program_id_index, vec![], data.to_string()),
            stack_height: Some(stack_height),
        }
    }

    /// A program appearing only as a CPI yields a row with the parent's top-level index and the inner position.
    #[test]
    fn inner_only_instruction_yields_top_and_inner_index() {
        let mut block = create_test_block();
        // Top-level targets a foreign program (index 1); our program (key index 0) is only invoked via CPI.
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let foreign = create_instruction(1, vec![], "foreign".to_string());

        let mut tx =
            create_successful_transaction("sig_cpi".to_string(), account_keys, vec![foreign]);
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![
                inner(1, "skip", 2),             // foreign inner, filtered out
                inner(0, "cpi_deposit_data", 2), // our program, indexed
            ],
        }]);
        block.transactions.push(tx);

        let result =
            parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, never_excluded, None);

        assert_eq!(result.len(), 1);
        let (sig, location, data) = &result[0];
        assert_eq!(sig, "sig_cpi");
        assert_eq!(location.top_level_index, 0);
        assert_eq!(location.inner.unwrap().inner_index, 1);
        assert_eq!(data, "cpi_deposit_data");
    }

    /// Two inner sets with different `index` values each map their CPI deposit to
    /// the correct top-level ancestor, so `top_level_index` tracks `inner_set.index`.
    #[test]
    fn distinct_inner_sets_map_to_their_own_top_level_index() {
        let mut block = create_test_block();
        // Our program at key index 0; the two top-level instructions both target
        // a foreign program (index 1), so neither is indexed at top level.
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let foreign_0 = create_instruction(1, vec![], "foreign_0".to_string());
        let foreign_1 = create_instruction(1, vec![], "foreign_1".to_string());

        let mut tx = create_successful_transaction(
            "sig_two_sets".to_string(),
            account_keys,
            vec![foreign_0, foreign_1],
        );
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![
            InnerInstructions {
                index: 0,
                instructions: vec![inner(0, "deposit_a", 2)],
            },
            InnerInstructions {
                index: 1,
                instructions: vec![inner(0, "deposit_b", 2)],
            },
        ]);
        block.transactions.push(tx);

        let result =
            parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, never_excluded, None);

        assert_eq!(result.len(), 2);
        // Each CPI deposit is attributed to the top-level instruction it ran under.
        assert_eq!(result[0].1.top_level_index, 0);
        assert_eq!(result[0].2, "deposit_a");
        assert_eq!(result[1].1.top_level_index, 1);
        assert_eq!(result[1].2, "deposit_b");
    }

    /// Excluded inner discriminators (operator/admin) are skipped even when they belong to our program.
    #[test]
    fn excluded_inner_discriminator_is_skipped() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let top = create_instruction(1, vec![], "foreign".to_string());
        let mut tx = create_successful_transaction("sig_excl".to_string(), account_keys, vec![top]);
        // Discriminator 7 (ReleaseFunds) is excluded; 6 (Deposit) is indexed.
        let release_data = bs58::encode([7u8]).into_string();
        let deposit_data = bs58::encode([6u8]).into_string();
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![
                InnerInstruction {
                    instruction: create_instruction(0, vec![], release_data),
                    stack_height: Some(2),
                },
                InnerInstruction {
                    instruction: create_instruction(0, vec![], deposit_data.clone()),
                    stack_height: Some(2),
                },
            ],
        }]);
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            escrow_inner_discriminator_excluded,
            None,
        );

        assert_eq!(result.len(), 1, "only the non-excluded inner is indexed");
        assert_eq!(result[0].2, deposit_data);
    }

    /// An inner instruction referencing a meta.loadedAddresses account resolves to the loaded key.
    #[test]
    fn alt_loaded_addresses_resolve_inner_accounts() {
        // Parser that records how many account keys it was handed.
        fn count_keys_parser(
            instruction: &CompiledInstruction,
            account_keys: &[Pubkey],
            _inner: &[InnerInstructions],
            _location: InstructionLocation,
        ) -> Result<Option<String>, ParserError> {
            // The account index is only valid once loaded addresses are appended.
            let idx = instruction.accounts[0] as usize;
            Ok(account_keys.get(idx).map(|k| k.to_string()))
        }

        let mut block = create_test_block();
        // Static keys: [0]=foreign program (top-level target), [1]=our program (CPI-only).
        let account_keys = vec![
            crate::test_utils::pubkey::test_pubkey(1).to_string(),
            TEST_PROGRAM_ID.to_string(),
        ];
        let loaded_writable = crate::test_utils::pubkey::test_pubkey(50).to_string();
        let loaded_readonly = crate::test_utils::pubkey::test_pubkey(60).to_string();

        // Top-level targets the foreign program at index 0; filtered out.
        let top = create_instruction(0, vec![], "top".to_string());
        let mut tx = create_successful_transaction("sig_alt".to_string(), account_keys, vec![top]);
        // Inner account[0] = 3 points past the 2 static keys into the readonly loaded slot (static 0,1 + writable 2 + readonly 3).
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: 1, // our program (CPI'd)
                    accounts: vec![3],
                    data: "inner".to_string(),
                },
                stack_height: Some(2),
            }],
        }]);
        tx.meta.as_mut().unwrap().loaded_addresses = Some(UiLoadedAddresses {
            writable: vec![loaded_writable.clone()],
            readonly: vec![loaded_readonly.clone()],
        });
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            count_keys_parser,
            never_excluded,
            None,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].2, loaded_readonly,
            "inner account index resolved into the readonly loaded addresses"
        );
    }

    /// A CPI to our ALT-loaded program (program_id_index past the static keys) is still resolved by the inner filter.
    #[test]
    fn inner_filter_resolves_alt_loaded_program_id() {
        let mut block = create_test_block();
        // One static key: a foreign program targeted by the top-level instruction.
        let account_keys = vec![crate::test_utils::pubkey::test_pubkey(1).to_string()];
        let top = create_instruction(0, vec![], "foreign".to_string());
        let mut tx =
            create_successful_transaction("sig_alt_prog".to_string(), account_keys, vec![top]);

        // Our program is ALT-loaded (writable slot), so its full-list index is 1 (static 0 + writable 1).
        tx.meta.as_mut().unwrap().loaded_addresses = Some(UiLoadedAddresses {
            writable: vec![TEST_PROGRAM_ID.to_string()],
            readonly: vec![],
        });
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: 1, // points into loaded addresses
                    accounts: vec![],
                    data: "cpi".to_string(),
                },
                stack_height: Some(2),
            }],
        }]);
        block.transactions.push(tx);

        let result =
            parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, never_excluded, None);

        assert_eq!(
            result.len(),
            1,
            "a CPI to our ALT-loaded program must still be indexed"
        );
        assert_eq!(result[0].1.inner.unwrap().inner_index, 0);
    }

    /// A top-level instruction whose program id is ALT-loaded (program_id_index
    /// past the static keys) must still be indexed, not dropped.
    #[test]
    fn top_level_resolves_alt_loaded_program_id() {
        let mut block = create_test_block();
        // One static key: an unrelated account. Our program is not static.
        let account_keys = vec![crate::test_utils::pubkey::test_pubkey(1).to_string()];
        // Top-level instruction targets program_id_index 1, which lands in the
        // loaded (writable) slot holding our program.
        let top = create_instruction(1, vec![], "top_alt".to_string());
        let mut tx =
            create_successful_transaction("sig_top_alt".to_string(), account_keys, vec![top]);
        tx.meta.as_mut().unwrap().loaded_addresses = Some(UiLoadedAddresses {
            writable: vec![TEST_PROGRAM_ID.to_string()],
            readonly: vec![],
        });
        block.transactions.push(tx);

        let result =
            parse_block_for_program(&block, TEST_PROGRAM_ID, mock_parser, never_excluded, None);

        assert_eq!(
            result.len(),
            1,
            "a top-level call to our ALT-loaded program must still be indexed"
        );
        assert_eq!(result[0].1.top_level_index, 0);
        assert!(
            result[0].1.inner.is_none(),
            "must be recorded as a top-level instruction"
        );
    }

    // ============================================================================
    // Real escrow parser end-to-end (parse_block, not the mock parser)
    // ============================================================================

    /// Read the DepositEvent amount out of a parsed escrow Deposit row.
    fn deposit_amount(meta: &InstructionWithMetadata) -> u64 {
        match &meta.instruction {
            ProgramInstruction::Escrow(ix) => match ix.as_ref() {
                EscrowInstruction::Deposit { event, .. } => event.amount,
                _ => panic!("expected a Deposit instruction"),
            },
            _ => panic!("expected an Escrow instruction"),
        }
    }

    /// `parse_block` over the real escrow parser: two CPI deposits sharing one
    /// transaction each resolve their *own* DepositEvent amount by stack height,
    /// landing as two rows with distinct inner indices. The instruction (borsh)
    /// amount is left at the default 1000 so the asserted amounts can only come
    /// from the scoped event, proving the scoping reads the right event.
    #[test]
    fn real_parser_scopes_two_cpi_deposits_by_stack_height() {
        use crate::test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes};

        // Escrow program at key index 0; indices 1..12 fill the deposits' accounts.
        let mut account_keys: Vec<String> = (0u8..12)
            .map(|i| crate::test_utils::pubkey::test_pubkey(i).to_string())
            .collect();
        account_keys[0] = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string();

        // One foreign top-level instruction (program index 1) that CPIs two deposits.
        let top = create_instruction(1, vec![], "foreign".to_string());
        let mut tx =
            create_successful_transaction("sig_cpi_real".to_string(), account_keys, vec![top]);

        let deposit = || InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: 0, // escrow
                accounts: (0u8..12).collect(),
                data: bs58::encode(deposit_ix_bytes(1000, None)).into_string(),
            },
            stack_height: Some(2),
        };
        let event = |amount: u64| InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: 0, // escrow
                accounts: vec![],
                data: bs58::encode(deposit_event_bytes(amount)).into_string(),
            },
            stack_height: Some(3),
        };

        // Pre-order CPI walk under top-level index 0:
        //   [0] deposit A   height 2
        //   [1]   event 300 height 3   (A's subtree)
        //   [2] deposit B   height 2
        //   [3]   event 480 height 3   (B's subtree)
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![deposit(), event(300), deposit(), event(480)],
        }]);

        let mut block = create_test_block();
        block.transactions.push(tx);

        let result = parse_block(&block, 7, ProgramType::Escrow, None);

        assert_eq!(
            result.len(),
            2,
            "two CPI deposits indexed; the event self-CPIs are not counted as rows"
        );
        assert_eq!(result[0].inner_index, Some(0));
        assert_eq!(
            deposit_amount(&result[0]),
            300,
            "deposit A reads its own event amount"
        );
        assert_eq!(result[1].inner_index, Some(2));
        assert_eq!(
            deposit_amount(&result[1]),
            480,
            "deposit B reads its own event amount, not A's"
        );
    }

    /// `parse_block` over the real escrow parser: a top-level deposit whose escrow
    /// program id is ALT-loaded is still parsed, resolves its event, and is
    /// recorded with a NULL inner index.
    #[test]
    fn real_parser_top_level_deposit_with_alt_loaded_program() {
        use crate::test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes};

        // 12 static keys, none of which is escrow; escrow arrives via a lookup table.
        let account_keys: Vec<String> = (0u8..12)
            .map(|i| crate::test_utils::pubkey::test_pubkey(i).to_string())
            .collect();

        // Top-level deposit targets program index 12: the first loaded (writable) key.
        let top = create_instruction(
            12,
            (0u8..12).collect(),
            bs58::encode(deposit_ix_bytes(1000, None)).into_string(),
        );
        let mut tx =
            create_successful_transaction("sig_top_lut_real".to_string(), account_keys, vec![top]);
        tx.meta.as_mut().unwrap().loaded_addresses = Some(UiLoadedAddresses {
            writable: vec![PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string()],
            readonly: vec![],
        });
        // The deposit's event self-CPI, emitted by the ALT-loaded escrow program.
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: 12, // escrow via ALT
                    accounts: vec![],
                    data: bs58::encode(deposit_event_bytes(555)).into_string(),
                },
                stack_height: Some(2),
            }],
        }]);

        let mut block = create_test_block();
        block.transactions.push(tx);

        let result = parse_block(&block, 7, ProgramType::Escrow, None);

        assert_eq!(
            result.len(),
            1,
            "top-level ALT-loaded deposit must be indexed"
        );
        assert_eq!(result[0].instruction_index, 0);
        assert!(
            result[0].inner_index.is_none(),
            "a top-level deposit has a NULL inner_index"
        );
        assert_eq!(deposit_amount(&result[0]), 555);
    }

    /// End-to-end depth guarantee: a transaction whose flattened inner list mixes
    /// escrow deposits at 1, 2, and 4 CPI hops deep (with intermediate foreign
    /// CPIs and self-CPI events between them) is indexed so that every deposit
    /// gets a UNIQUE `inner_index` (its flat position) and reads its OWN event.
    /// A one-level-only scheme or mis-scoped subtree walk would drop deposits,
    /// collide indices, or cross amounts — all caught here.
    #[test]
    fn real_parser_indexes_mixed_depth_cpi_deposits_uniquely() {
        use crate::test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes};

        // escrow at key index 0; index 1 is a foreign program; 2..12 pad accounts.
        let mut account_keys: Vec<String> = (0u8..12)
            .map(|i| crate::test_utils::pubkey::test_pubkey(i).to_string())
            .collect();
        account_keys[0] = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string();

        // Top-level targets the foreign router (index 1); it is not indexed.
        let top = create_instruction(1, vec![], "router".to_string());
        let mut tx = create_successful_transaction("sig_deep".to_string(), account_keys, vec![top]);

        let foreign = |h: u32| InnerInstruction {
            instruction: create_instruction(1, vec![], "foreign".to_string()),
            stack_height: Some(h),
        };
        let deposit = |h: u32| InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: 0, // escrow
                accounts: (0u8..12).collect(),
                data: bs58::encode(deposit_ix_bytes(1000, None)).into_string(),
            },
            stack_height: Some(h),
        };
        let event = |amount: u64, h: u32| InnerInstruction {
            instruction: CompiledInstruction {
                program_id_index: 0, // escrow (its self-CPI event)
                accounts: vec![],
                data: bs58::encode(deposit_event_bytes(amount)).into_string(),
            },
            stack_height: Some(h),
        };

        // Pre-order flatten of the CPI tree under top-level 0 (height = depth):
        //   [0] foreign A      h2
        //   [1] deposit D1     h3   (2 hops)  -> event 111
        //   [2]   event 111    h4
        //   [3] foreign C      h3
        //   [4]   foreign C2   h4
        //   [5]   deposit D2   h5   (4 hops)  -> event 222
        //   [6]     event 222  h6
        //   [7] deposit D3     h2   (1 hop)   -> event 333
        //   [8]   event 333    h3
        tx.meta.as_mut().unwrap().inner_instructions = Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![
                foreign(2),
                deposit(3),
                event(111, 4),
                foreign(3),
                foreign(4),
                deposit(5),
                event(222, 6),
                deposit(2),
                event(333, 3),
            ],
        }]);

        let mut block = create_test_block();
        block.transactions.push(tx);

        let result = parse_block(&block, 9, ProgramType::Escrow, None);

        // Only the three deposits surface (events parse to Ok(None); foreign skipped).
        assert_eq!(
            result.len(),
            3,
            "three escrow deposits across mixed CPI depths"
        );

        // Each deposit keeps its flat position as inner_index and reads its own event.
        let got: Vec<(u32, Option<u32>, u64)> = result
            .iter()
            .map(|m| (m.instruction_index, m.inner_index, deposit_amount(m)))
            .collect();
        assert_eq!(
            got,
            vec![(0, Some(1), 111), (0, Some(5), 222), (0, Some(7), 333)],
            "depth 2/4/1 deposits: unique flat inner_index, each reads its own event"
        );

        // The core guarantee: (instruction_index, inner_index) is unique at every depth.
        let mut ids: Vec<(u32, Option<u32>)> = result
            .iter()
            .map(|m| (m.instruction_index, m.inner_index))
            .collect();
        let total = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            total,
            "(instruction_index, inner_index) must be unique regardless of CPI depth"
        );
    }
}
