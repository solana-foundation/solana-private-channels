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
use crate::indexer::datasource::common::tx_validation::{
    check_inner_set_index, check_instruction, check_loaded_counts, check_signature,
};
use crate::indexer::datasource::common::types::CompiledInstruction;
use crate::indexer::datasource::common::types::*;
use crate::indexer::datasource::rpc_polling::types::{
    InnerInstructions, Reported, RpcBlock, RpcTransactionWithMeta,
};
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, error};

type ParseInstructionFn<T> = fn(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    inner_instructions: &[InnerInstructions],
    location: InstructionLocation,
) -> Result<Option<T>, ParserError>;

/// An instruction of the indexed program whose discriminator is supported but whose
/// payload would not decode, with the position an operator needs to find it. Unknown
/// discriminators parse to `Ok(None)` and never land here, so this always means a slot
/// holds contents the indexer claims to cover but could not read.
#[derive(Debug)]
pub struct UndecodableInstruction {
    pub signature: String,
    pub instruction_index: u32,
    pub inner_index: Option<u32>,
    pub source: ParserError,
}

/// Why a fetched block cannot be turned into rows. Both variants mean the slot's contents
/// are unknown, so no caller may complete the slot; they differ in which endpoint problem
/// an operator has to chase, which is why they keep separate metric labels.
#[derive(Debug)]
pub enum SlotRejection {
    /// A transaction carries no `meta`, so it cannot be proven successful or in scope.
    MissingMeta { signature: String },
    /// A transaction's meta omits a key the decoder reads, so a missing `err` cannot be
    /// told apart from a success.
    MissingMetaField {
        signature: String,
        field: &'static str,
    },
    /// An instruction the indexer supports would not decode.
    Undecodable(UndecodableInstruction),
    /// A successful transaction breaks a rule the runtime enforces, so the provider corrupted it.
    Malformed { signature: String, reason: String },
    /// An escrow block came back with no transactions and its signatures view did not confirm it.
    EmptyUnconfirmed { reason: String },
}

impl SlotRejection {
    /// Label for `INDEXER_RPC_ERRORS`, kept distinct because each condition has its own
    /// alert and runbook. A missing meta key shares `missing_meta`: both are an
    /// endpoint serving incomplete meta.
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::MissingMeta { .. } | Self::MissingMetaField { .. } => "missing_meta",
            Self::Undecodable(_) => "parse_failed",
            Self::Malformed { .. } => "malformed_tx",
            Self::EmptyUnconfirmed { .. } => "empty_block_unconfirmed",
        }
    }
}

impl std::fmt::Display for SlotRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMeta { signature } => write!(f, "transaction {signature} is missing meta"),
            Self::MissingMetaField { signature, field } => {
                write!(f, "transaction {signature} meta is missing `{field}`")
            }
            Self::Undecodable(failure) => write!(
                f,
                "transaction {} instruction {} (inner {:?}) will not decode: {}",
                failure.signature, failure.instruction_index, failure.inner_index, failure.source
            ),
            Self::Malformed { signature, reason } => {
                write!(f, "transaction {signature} is malformed: {reason}")
            }
            Self::EmptyUnconfirmed { reason } => {
                write!(
                    f,
                    "came back with no transactions and could not be confirmed empty: {reason}"
                )
            }
        }
    }
}

/// Returns a label for the first `meta`-less
/// transaction in `block`, or `None` if all carry metadata; a single `meta: null`
/// tx makes the slot unverifiable, so callers MUST fail closed on `Some(_)`.
///
/// A null `innerInstructions` list is deliberately NOT rejected here: the private-channel
/// node records no inner instructions and returns null for every transaction. The escrow
/// parse rejects null on its own transactions instead, since Solana always records them.
fn first_missing_meta(block: &RpcBlock) -> Option<String> {
    for (index, tx_with_meta) in block.transactions.iter().enumerate() {
        if tx_with_meta.meta.is_none() {
            return Some(
                tx_with_meta
                    .transaction
                    .signatures
                    .first()
                    .cloned()
                    .unwrap_or_else(|| format!("tx#{index}")),
            );
        }
    }
    None
}

/// Returns the signature and key of the first transaction whose meta omits a key the
/// decoder reads, or nulls `loadedAddresses`, which a node never sends null. Callers MUST
/// fail closed on `Some(_)`, since a missing `err` would otherwise pass for a success.
fn first_missing_meta_field(block: &RpcBlock) -> Option<(String, &'static str)> {
    for (index, tx_with_meta) in block.transactions.iter().enumerate() {
        let Some(meta) = &tx_with_meta.meta else {
            continue;
        };
        let missing_field = if matches!(meta.err, Reported::Missing) {
            "err"
        } else if matches!(meta.inner_instructions, Reported::Missing) {
            "innerInstructions"
        } else if meta.loaded_addresses.is_none() {
            "loadedAddresses"
        } else {
            continue;
        };
        let signature = tx_with_meta
            .transaction
            .signatures
            .first()
            .cloned()
            .unwrap_or_else(|| format!("tx#{index}"));
        return Some((signature, missing_field));
    }
    None
}

/// Parse a block and extract program-specific instructions with metadata.
///
/// Precondition: every transaction in `block` must carry `meta` with every key the
/// decoder reads. Errs when a supported instruction will not decode, leaving the slot's
/// contents unknown.
fn parse_block(
    block: &RpcBlock,
    slot: u64,
    program_type: ProgramType,
    escrow_instance_id: Option<&Pubkey>,
) -> Result<Vec<InstructionWithMetadata>, SlotRejection> {
    match program_type {
        ProgramType::Escrow => Ok(parse_block_for_program::<EscrowInstruction>(
            block,
            PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            parse_escrow_instruction,
            escrow_inner_discriminator_excluded,
            escrow_instance_id,
            true,
        )?
        .into_iter()
        .map(|(signature, location, ix)| InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(ix)),
            slot,
            program_type,
            signature: Some(signature),
            instruction_index: location.top_level_index,
            inner_index: location.inner.map(|i| i.inner_index),
        })
        .collect()),
        ProgramType::Withdraw => Ok(parse_block_for_program::<WithdrawInstruction>(
            block,
            PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            parse_withdraw_instruction,
            withdraw_inner_discriminator_excluded,
            None,
            false,
        )?
        .into_iter()
        .map(|(signature, location, ix)| InstructionWithMetadata {
            instruction: ProgramInstruction::Withdraw(Box::new(ix)),
            slot,
            program_type,
            signature: Some(signature),
            instruction_index: location.top_level_index,
            inner_index: location.inner.map(|i| i.inner_index),
        })
        .collect()),
    }
}

/// Decode a whole slot, or reject it. Applies both guards in order, so a block re-fetched
/// from another endpoint is judged exactly as the first one was: a fallback can only be
/// accepted if it actually yields rows, never merely because its meta is present.
pub fn decode_slot(
    block: &RpcBlock,
    slot: u64,
    program_type: ProgramType,
    escrow_instance_id: Option<&Pubkey>,
) -> Result<Vec<InstructionWithMetadata>, SlotRejection> {
    if let Some(signature) = first_missing_meta(block) {
        return Err(SlotRejection::MissingMeta { signature });
    }

    if let Some((signature, field)) = first_missing_meta_field(block) {
        return Err(SlotRejection::MissingMetaField { signature, field });
    }

    parse_block(block, slot, program_type, escrow_instance_id)
}

/// Whether this instruction names the configured escrow instance among its own accounts.
/// Instance admission is transaction-wide, so a transaction of ours also carries other
/// instances' instructions, and only ours are worth failing a slot on. `None` (withdraw) and
/// an unresolvable index count as in scope, so an unreadable instruction still fails closed.
pub(crate) fn targets_configured_instance(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
    escrow_instance_id: Option<&Pubkey>,
) -> bool {
    let Some(instance_id) = escrow_instance_id else {
        return true;
    };

    instruction.accounts.iter().any(|index| {
        account_keys
            .get(*index as usize)
            .is_none_or(|key| key == instance_id)
    })
}

/// First byte (Anchor-style discriminator) of an instruction's base58 data, or `None` if empty/undecodable.
fn instruction_discriminator(instruction: &CompiledInstruction) -> Option<u8> {
    bs58::decode(&instruction.data)
        .into_vec()
        .ok()
        .and_then(|d| d.first().copied())
}

/// Runs the shared structural checks on one successful transaction and returns its full
/// key list (static, then loaded writable, then loaded readonly) and its signature.
fn validate_transaction(
    tx_with_meta: &RpcTransactionWithMeta,
) -> Result<(Vec<Pubkey>, String), String> {
    let tx = &tx_with_meta.transaction;
    let meta = tx_with_meta.meta.as_ref();

    let (loaded_writable, loaded_readonly): (&[String], &[String]) = meta
        .and_then(|meta| meta.loaded_addresses.as_ref())
        .map_or((&[], &[]), |loaded| (&loaded.writable, &loaded.readonly));
    let expected = tx.message.address_table_lookups.iter().flatten().fold(
        (0, 0),
        |(writable, readonly), lookup| {
            (
                writable + lookup.writable_indexes.len(),
                readonly + lookup.readonly_indexes.len(),
            )
        },
    );
    check_loaded_counts(expected, loaded_writable.len(), loaded_readonly.len())?;

    let account_pubkeys = tx
        .message
        .account_keys
        .iter()
        .chain(loaded_writable)
        .chain(loaded_readonly)
        .map(|key| Pubkey::from_str(key).map_err(|e| format!("invalid account key {key}: {e}")))
        .collect::<Result<Vec<_>, _>>()?;

    let signature = tx
        .signatures
        .first()
        .ok_or_else(|| "transaction has no signature".to_string())?;
    let signature_bytes = bs58::decode(signature)
        .into_vec()
        .map_err(|e| format!("signature is not base58: {e}"))?;
    check_signature(&signature_bytes)?;

    let num_keys = account_pubkeys.len();
    for instruction in &tx.message.instructions {
        check_instruction(
            num_keys,
            instruction.program_id_index.into(),
            &instruction.accounts,
        )?;
    }
    let inner_sets = meta.and_then(|meta| meta.inner_instructions.present().map(Vec::as_slice));
    for inner_set in inner_sets.unwrap_or_default() {
        check_inner_set_index(inner_set.index.into(), tx.message.instructions.len())?;
        for inner in &inner_set.instructions {
            check_instruction(
                num_keys,
                inner.instruction.program_id_index.into(),
                &inner.instruction.accounts,
            )?;
        }
    }

    Ok((account_pubkeys, signature.clone()))
}

/// Parse a block and return (signature, location, instruction) for every
/// instruction of the given program.
fn parse_block_for_program<T>(
    block: &RpcBlock,
    filter_program_id: &str,
    parse_instruction: ParseInstructionFn<T>,
    inner_discriminator_excluded: fn(u8) -> bool,
    escrow_instance_id: Option<&Pubkey>,
    require_inner_instructions: bool,
) -> Result<Vec<(String, InstructionLocation, T)>, SlotRejection>
where
    T: std::fmt::Debug,
{
    let mut instructions = Vec::new();

    // The filter program id is a hardcoded constant; a parse failure is a
    // programming error, not a per-transaction condition.
    let Ok(filter_pubkey) = Pubkey::from_str(filter_program_id) else {
        error!("Invalid filter program id: {filter_program_id}");
        return Ok(instructions);
    };

    for (tx_position, tx_with_meta) in block.transactions.iter().enumerate() {
        // Only an explicit `err: null` is a success; a failed or unreported one is skipped.
        if tx_with_meta
            .meta
            .as_ref()
            .is_some_and(|meta| !matches!(meta.err, Reported::Present(None)))
        {
            continue;
        }

        let tx = &tx_with_meta.transaction;
        let malformed = |reason: String| SlotRejection::Malformed {
            signature: tx
                .signatures
                .first()
                .cloned()
                .unwrap_or_else(|| format!("tx#{tx_position}")),
            reason,
        };

        // Checked on every successful transaction before scoping: a corrupt key list could
        // otherwise hide our program or instance and make the transaction look unrelated.
        let (account_pubkeys, signature) = validate_transaction(tx_with_meta).map_err(malformed)?;

        if !account_pubkeys.contains(&filter_pubkey) {
            continue;
        }

        // Filter transactions by instance ID if provided
        // Check if any account in the transaction matches the instance ID
        if let Some(instance_id) = escrow_instance_id {
            if !account_pubkeys.contains(instance_id) {
                continue; // Skip this transaction entirely
            }
        }

        let inner_instructions = tx_with_meta
            .meta
            .as_ref()
            .and_then(|meta| meta.inner_instructions.present().map(Vec::as_slice));
        if require_inner_instructions && inner_instructions.is_none() {
            return Err(malformed(
                "innerInstructions is null, so a CPI into our program would be invisible"
                    .to_string(),
            ));
        }
        let inner_instructions_list: &[InnerInstructions] = inner_instructions.unwrap_or(&[]);

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
                    // A discriminator we support that will not decode leaves this slot's
                    // contents unknown, so fail the whole slot rather than drop the row.
                    Err(source) => {
                        if !targets_configured_instance(
                            instruction,
                            &account_pubkeys,
                            escrow_instance_id,
                        ) {
                            debug!("Skipped undecodable instruction for a foreign instance");
                            continue;
                        }
                        return Err(SlotRejection::Undecodable(UndecodableInstruction {
                            signature: signature.clone(),
                            instruction_index: location.top_level_index,
                            inner_index: location.inner.map(|inner| inner.inner_index),
                            source,
                        }));
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
                    Err(source) => {
                        if !targets_configured_instance(
                            &inner.instruction,
                            &account_pubkeys,
                            escrow_instance_id,
                        ) {
                            debug!("Skipped undecodable inner instruction for a foreign instance");
                            continue;
                        }
                        return Err(SlotRejection::Undecodable(UndecodableInstruction {
                            signature: signature.clone(),
                            instruction_index: location.top_level_index,
                            inner_index: location.inner.map(|inner| inner.inner_index),
                            source,
                        }));
                    }
                }
            }
        }
    }

    Ok(instructions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::ParserError,
        test_utils::{
            escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes},
            pubkey::{test_pubkey, test_sig},
            rpc_blocks::*,
        },
    };
    use serde_json::json;

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

    /// Appends a foreign program key, so a fixture can target it with an in-range index.
    fn with_foreign_program(mut account_keys: Vec<String>) -> Vec<String> {
        account_keys.push(test_pubkey(99).to_string());
        account_keys
    }

    /// Declares one lookup table that loads `writable` and `readonly` addresses, matching meta.loadedAddresses.
    fn declare_lookups(
        tx: &mut crate::indexer::datasource::rpc_polling::types::RpcTransactionWithMeta,
        writable: u8,
        readonly: u8,
    ) {
        tx.transaction.message.address_table_lookups = Some(vec![UiAddressTableLookup {
            account_key: test_pubkey(77).to_string(),
            writable_indexes: (0..writable).collect(),
            readonly_indexes: (0..readonly).collect(),
        }]);
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
            false,
        )
        .expect("the mock parsers used here decode every instruction")
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
        assert_eq!(result[0], (test_sig("sig1"), 0u32, "data1".to_string()));
        assert_eq!(result[1], (test_sig("sig1"), 1u32, "data2".to_string()));
        assert_eq!(result[2], (test_sig("sig1"), 2u32, "data3".to_string()));
    }

    #[test]
    fn test_index_is_absolute_position_across_filtered_instructions() {
        let mut block = create_test_block();
        // Key 1 is a foreign program, so instructions naming it resolve but are not ours.
        let account_keys =
            with_foreign_program(create_account_keys_with_program(TEST_PROGRAM_ID, 0));

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
        assert_eq!(result[0], (test_sig("sig1"), 0u32, "data0".to_string()));
        assert_eq!(result[1], (test_sig("sig1"), 2u32, "data2".to_string()));
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
        assert_eq!(result[0], (test_sig("sig1"), 0u32, "data1".to_string()));
        assert_eq!(result[1], (test_sig("sig2"), 0u32, "data2".to_string()));
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
            (test_sig("sig_success"), 0u32, "success".to_string())
        );
        assert_eq!(
            result[1],
            (test_sig("sig_success2"), 0u32, "success2".to_string())
        );
    }

    /// Parses `tx` alone and returns the malformed-transaction reason, failing if it was accepted.
    fn malformed_reason(
        tx: crate::indexer::datasource::rpc_polling::types::RpcTransactionWithMeta,
    ) -> String {
        let mut block = create_test_block();
        block.transactions.push(tx);
        match parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            never_excluded,
            None,
            false,
        ) {
            Err(SlotRejection::Malformed { reason, .. }) => reason,
            Err(other) => panic!("expected a malformed rejection, got {other}"),
            Ok(rows) => panic!(
                "a malformed transaction was accepted with {} rows",
                rows.len()
            ),
        }
    }

    /// Every structural corruption of a successful transaction fails the whole slot instead
    /// of dropping the transaction, whether or not it touches our program.
    #[test]
    fn malformed_transaction_fails_the_slot() {
        type Corrupt =
            fn(&mut crate::indexer::datasource::rpc_polling::types::RpcTransactionWithMeta);
        // (case, the rule the reason must name, corruption)
        let cases: Vec<(&str, &str, Corrupt)> = vec![
            ("no signature", "has no signature", |tx| {
                tx.transaction.signatures.clear()
            }),
            ("63-byte signature", "signature is 63 bytes", |tx| {
                tx.transaction.signatures = vec![bs58::encode([1u8; 63]).into_string()]
            }),
            ("non-base58 signature", "not base58", |tx| {
                tx.transaction.signatures = vec!["0OIl".to_string()]
            }),
            ("bad account key", "invalid account key", |tx| {
                tx.transaction.message.account_keys[0] = "not a key".to_string()
            }),
            (
                "loaded addresses without a lookup",
                "lookups load (0, 0)",
                |tx| {
                    tx.meta.as_mut().unwrap().loaded_addresses = Some(UiLoadedAddresses {
                        writable: vec![test_pubkey(50).to_string()],
                        readonly: vec![],
                    })
                },
            ),
            (
                "lookup without its loaded address",
                "lookups load (0, 1)",
                |tx| declare_lookups(tx, 0, 1),
            ),
            (
                "top-level program index out of range",
                "program index 9 is outside",
                |tx| tx.transaction.message.instructions[0].program_id_index = 9,
            ),
            (
                "top-level account index out of range",
                "account index 9 is outside",
                |tx| tx.transaction.message.instructions[0].accounts = vec![9],
            ),
            (
                "inner set names no top-level instruction",
                "names no top-level instruction",
                |tx| {
                    tx.meta.as_mut().unwrap().inner_instructions =
                        Reported::Present(Some(vec![InnerInstructions {
                            index: 3,
                            instructions: vec![],
                        }]))
                },
            ),
            (
                "inner program index out of range",
                "program index 9 is outside",
                |tx| {
                    tx.meta.as_mut().unwrap().inner_instructions =
                        Reported::Present(Some(vec![InnerInstructions {
                            index: 0,
                            instructions: vec![inner(9, "x", 2)],
                        }]))
                },
            ),
            (
                "inner account index out of range",
                "account index 9 is outside",
                |tx| {
                    let mut ix = inner(0, "x", 2);
                    ix.instruction.accounts = vec![9];
                    tx.meta.as_mut().unwrap().inner_instructions =
                        Reported::Present(Some(vec![InnerInstructions {
                            index: 0,
                            instructions: vec![ix],
                        }]))
                },
            ),
        ];

        // One transaction of ours and one that never names our program.
        let ours = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let foreign =
            create_account_keys_with_program("DifferentProgram1111111111111111111111111111", 0);
        for keys in [ours, foreign] {
            for (name, rule, corrupt) in &cases {
                let mut tx = create_successful_transaction(
                    "sig1".to_string(),
                    keys.clone(),
                    vec![create_instruction(0, vec![0], "data".to_string())],
                );
                corrupt(&mut tx);
                let reason = malformed_reason(tx);
                assert!(reason.contains(rule), "{name}: {reason}");
            }
        }
    }

    /// The same checks leave a failed transaction alone: it is skipped whatever it holds.
    #[test]
    fn malformed_failed_transaction_is_still_skipped() {
        let mut block = create_test_block();
        let mut tx = create_failed_transaction(
            "sig_failed".to_string(),
            create_account_keys_with_program(TEST_PROGRAM_ID, 0),
            vec![create_instruction(9, vec![9], "data".to_string())],
        );
        tx.transaction.signatures.clear();
        block.transactions.push(tx);

        assert!(parse_for_test(&block, TEST_PROGRAM_ID, mock_parser, None).is_empty());
    }

    /// Escrow requires inner metadata on its own transactions: without it a CPI-only deposit
    /// is invisible. Withdraw keeps accepting null, since the channel node records none.
    #[test]
    fn null_inner_instructions_fail_escrow_but_not_withdraw() {
        use crate::indexer::datasource::common::parser::withdraw::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID;

        let block_with = |program: &str| {
            // A foreign top-level call (key 1); the watched program at key 0 could only run as a CPI.
            let mut tx = create_transaction_incomplete_meta(
                "sig_cpi_hidden".to_string(),
                with_foreign_program(create_account_keys_with_program(program, 0)),
                vec![create_instruction(1, vec![], "foreign".to_string())],
            );
            tx.meta.as_mut().unwrap().inner_instructions = Reported::Present(None);
            let mut block = create_test_block();
            block.transactions.push(tx);
            block
        };

        let escrow = block_with(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);
        assert!(matches!(
            decode_slot(&escrow, 7, ProgramType::Escrow, None),
            Err(SlotRejection::Malformed { .. })
        ));

        let withdraw = block_with(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID);
        assert!(decode_slot(&withdraw, 7, ProgramType::Withdraw, None)
            .expect("the channel node returns null inner lists for every transaction")
            .is_empty());

        // An escrow block whose null-inner transaction never names the escrow program is fine.
        let unrelated = block_with("DifferentProgram1111111111111111111111111111");
        assert!(decode_slot(&unrelated, 7, ProgramType::Escrow, None)
            .expect("a transaction that cannot touch escrow needs no inner metadata")
            .is_empty());
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

    /// A supported instruction that will not decode must fail the whole slot rather than
    /// vanish from the output: dropping it would let the caller complete and checkpoint a
    /// slot whose contents it never read. The reported location is what the caller logs.
    #[test]
    fn parse_error_fails_the_slot_with_the_offending_location() {
        let mut block = create_test_block();
        let account_keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let instruction = create_instruction(0, vec![], "test_data".to_string());
        let signature = "sig1";

        block.transactions.push(create_successful_transaction(
            signature.to_string(),
            account_keys,
            vec![instruction],
        ));

        let failure = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser_returns_error,
            never_excluded,
            None,
            false,
        )
        .expect_err("an undecodable supported instruction must fail the slot");
        let SlotRejection::Undecodable(failure) = failure else {
            panic!("expected an undecodable rejection, got {failure}");
        };

        assert_eq!(failure.signature, test_sig(signature));
        assert_eq!(failure.instruction_index, 0);
        assert!(
            failure.inner_index.is_none(),
            "a top-level instruction has no inner index"
        );
    }

    /// Instance admission is transaction-wide, so a transaction of ours also carries other
    /// instances' instructions. Only our own may fail the slot: rejecting on a foreign one
    /// would wedge a slot whose own rows are complete, and the processor discards those rows.
    #[test]
    fn undecodable_instruction_for_a_foreign_instance_does_not_fail_the_slot() {
        let our_instance = crate::test_utils::pubkey::test_pubkey(200);
        // Discriminator 6 (Deposit) with no borsh body: recognized, undecodable.
        let undecodable_deposit = bs58::encode([6u8]).into_string();

        // Key 0 is the escrow program, key 1 is our instance, keys 2..=13 are the twelve
        // accounts of a deposit that belongs to a different instance.
        let mut account_keys = vec![
            PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string(),
            our_instance.to_string(),
        ];
        for seed in 2u8..14 {
            account_keys.push(crate::test_utils::pubkey::test_pubkey(seed).to_string());
        }

        let mut block = create_test_block();
        block.transactions.push(create_successful_transaction(
            "sig_foreign".to_string(),
            account_keys.clone(),
            vec![create_instruction(
                0,
                (2u8..14).collect(),
                undecodable_deposit.clone(),
            )],
        ));

        let result = parse_block(&block, 7, ProgramType::Escrow, Some(&our_instance))
            .expect("a foreign instance's undecodable deposit must not fail the slot");
        assert!(
            result.is_empty(),
            "no row belongs to our instance in this transaction"
        );

        // The same undecodable deposit naming our instance still fails the slot.
        let mut block = create_test_block();
        block.transactions.push(create_successful_transaction(
            "sig_ours".to_string(),
            account_keys,
            vec![create_instruction(
                0,
                (1u8..13).collect(),
                undecodable_deposit,
            )],
        ));

        assert!(
            parse_block(&block, 7, ProgramType::Escrow, Some(&our_instance)).is_err(),
            "our own undecodable deposit must fail the slot"
        );
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
            (test_sig("sig1"), 0u32, "at_index_0".to_string())
        );
        assert_eq!(
            result[1],
            (test_sig("sig2"), 0u32, "at_index_5".to_string())
        );
    }

    // ============================================================================
    // first_missing_meta guard Tests
    // ============================================================================

    /// Every transaction carries meta, so the block is complete.
    #[test]
    fn first_missing_meta_all_present_returns_none() {
        let mut block = create_test_block();
        let keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix = create_instruction(0, vec![], "data".to_string());
        block.transactions.push(create_successful_transaction(
            "sig_ok".to_string(),
            keys.clone(),
            vec![ix.clone()],
        ));
        block.transactions.push(create_failed_transaction(
            "sig_failed".to_string(),
            keys,
            vec![ix],
        ));

        assert_eq!(first_missing_meta(&block), None);
    }

    /// One meta-less transaction among successful ones is reported by its signature.
    #[test]
    fn first_missing_meta_reports_signature_of_missing_tx() {
        let mut block = create_test_block();
        let keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix = create_instruction(0, vec![], "data".to_string());
        block.transactions.push(create_successful_transaction(
            "sig_ok".to_string(),
            keys.clone(),
            vec![ix.clone()],
        ));
        block.transactions.push(create_transaction_no_meta(
            "sig_no_meta".to_string(),
            keys,
            vec![ix],
        ));

        assert_eq!(first_missing_meta(&block), Some(test_sig("sig_no_meta")));
    }

    /// A meta-less transaction with no signature falls back to `tx#<index>` (no panic).
    #[test]
    fn first_missing_meta_no_signature_falls_back_to_index() {
        let mut block = create_test_block();
        let keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix = create_instruction(0, vec![], "data".to_string());
        // First a normal tx so the missing-meta tx lands at index 1.
        block.transactions.push(create_successful_transaction(
            "sig_ok".to_string(),
            keys.clone(),
            vec![ix.clone()],
        ));
        let mut no_meta = create_transaction_no_meta("dummy".to_string(), keys, vec![ix]);
        no_meta.transaction.signatures = vec![];
        block.transactions.push(no_meta);

        assert_eq!(first_missing_meta(&block), Some("tx#1".to_string()));
    }

    /// A chain that records no inner instructions returns a null list on every
    /// transaction. That is complete, not incomplete: rejecting it here would wedge
    /// the private-channel indexer on its very first block.
    #[test]
    fn first_missing_meta_null_inner_instructions_is_not_missing_meta() {
        let mut block = create_test_block();
        let keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix = create_instruction(0, vec![], "data".to_string());
        block.transactions.push(create_transaction_incomplete_meta(
            "sig_no_inner".to_string(),
            keys,
            vec![ix],
        ));

        assert_eq!(first_missing_meta(&block), None);
    }

    /// An empty block carries no missing-meta transaction.
    #[test]
    fn first_missing_meta_empty_block_returns_none() {
        let block = create_test_block();
        assert_eq!(first_missing_meta(&block), None);
    }

    /// A failed transaction (meta present, `err = Some`) is not missing meta;
    /// the meta-less transaction is the one reported.
    #[test]
    fn first_missing_meta_failed_tx_is_not_missing_meta() {
        let mut block = create_test_block();
        let keys = create_account_keys_with_program(TEST_PROGRAM_ID, 0);
        let ix = create_instruction(0, vec![], "data".to_string());
        block.transactions.push(create_failed_transaction(
            "sig_failed".to_string(),
            keys.clone(),
            vec![ix.clone()],
        ));
        block.transactions.push(create_transaction_no_meta(
            "sig_no_meta".to_string(),
            keys,
            vec![ix],
        ));

        assert_eq!(first_missing_meta(&block), Some(test_sig("sig_no_meta")));
    }

    /// Omitting `err` is not a success: a failed WithdrawFunds read as one would release
    /// escrow for a burn that never happened. Every other meta key the decoder reads is
    /// held to the same rule, so omitting any of them must reject the slot, never yield
    /// a row. Explicit nulls stay valid where a node may send them.
    #[test]
    fn decode_slot_rejects_meta_missing_a_required_key() {
        let slot = 100;
        let signature = &test_sig("sig_withdraw");
        // WithdrawFunds: discriminator 0, then borsh amount (u64 LE) + None destination.
        let mut withdraw_data = vec![0u8];
        withdraw_data.extend_from_slice(&1000u64.to_le_bytes());
        withdraw_data.push(0);
        let mut account_keys = vec![PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID.to_string()];
        for seed in 1u8..=5 {
            account_keys.push(test_pubkey(seed).to_string());
        }
        let complete_block = json!({
            "blockhash": "TestBlockHash11111111111111111111111111111",
            "parentSlot": slot - 1,
            "transactions": [{
                "transaction": {
                    "signatures": [signature],
                    "message": {
                        "accountKeys": account_keys,
                        "instructions": [{
                            "programIdIndex": 0,
                            "accounts": [1, 2, 3, 4, 5],
                            "data": bs58::encode(withdraw_data).into_string()
                        }]
                    }
                },
                "meta": {
                    "err": null,
                    "logMessages": null,
                    "innerInstructions": null,
                    "loadedAddresses": { "writable": [], "readonly": [] }
                }
            }]
        });

        let block: RpcBlock = serde_json::from_value(complete_block.clone())
            .expect("the complete block deserializes");
        let rows =
            decode_slot(&block, slot, ProgramType::Withdraw, None).expect("complete meta decodes");
        assert_eq!(rows.len(), 1, "the fixture holds one valid WithdrawFunds");

        for required_key in ["err", "innerInstructions", "loadedAddresses"] {
            let mut stripped_block = complete_block.clone();
            stripped_block["transactions"][0]["meta"]
                .as_object_mut()
                .expect("meta is an object")
                .remove(required_key);
            let block: RpcBlock = serde_json::from_value(stripped_block)
                .expect("a meta missing a key still deserializes");

            let rejection = decode_slot(&block, slot, ProgramType::Withdraw, None)
                .expect_err("a meta missing a required key must reject the slot");

            assert_eq!(rejection.metric_label(), "missing_meta");
            let message = rejection.to_string();
            assert!(
                message.contains(signature) && message.contains(&format!("`{required_key}`")),
                "rejection must name the transaction and the missing key: {message}"
            );
        }
    }

    /// A null `loadedAddresses` hides every key a lookup table supplied, so a CPI to our
    /// ALT-loaded program resolves to nothing and would drop silently while the slot
    /// completes. Unlike `innerInstructions`, a node never sends it null, so reject it.
    #[test]
    fn decode_slot_rejects_null_loaded_addresses() {
        let slot = 100;
        let signature = &test_sig("sig_alt_withdraw");
        // WithdrawFunds: discriminator 0, then borsh amount (u64 LE) + None destination.
        let mut withdraw_data = vec![0u8];
        withdraw_data.extend_from_slice(&1000u64.to_le_bytes());
        withdraw_data.push(0);
        // Static keys: a foreign program at 0, the withdraw accounts at 1..=5. Our program
        // is only in the lookup table, so its full-list index is 6.
        let mut account_keys = vec![test_pubkey(100).to_string()];
        for seed in 1u8..=5 {
            account_keys.push(test_pubkey(seed).to_string());
        }
        let loaded_block = json!({
            "blockhash": "TestBlockHash11111111111111111111111111111",
            "parentSlot": slot - 1,
            "transactions": [{
                "transaction": {
                    "signatures": [signature],
                    "message": {
                        "accountKeys": account_keys,
                        "instructions": [{
                            "programIdIndex": 0,
                            "accounts": [],
                            "data": ""
                        }],
                        "addressTableLookups": [{
                            "accountKey": test_pubkey(77).to_string(),
                            "writableIndexes": [0],
                            "readonlyIndexes": []
                        }]
                    }
                },
                "meta": {
                    "err": null,
                    "logMessages": null,
                    "loadedAddresses": {
                        "writable": [PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID],
                        "readonly": []
                    },
                    "innerInstructions": [{
                        "index": 0,
                        "instructions": [{
                            "programIdIndex": 6,
                            "accounts": [1, 2, 3, 4, 5],
                            "data": bs58::encode(withdraw_data).into_string(),
                            "stackHeight": 2
                        }]
                    }]
                }
            }]
        });

        let block: RpcBlock =
            serde_json::from_value(loaded_block.clone()).expect("the loaded block deserializes");
        let rows = decode_slot(&block, slot, ProgramType::Withdraw, None)
            .expect("a CPI to the ALT-loaded program decodes");
        assert_eq!(
            rows.len(),
            1,
            "the fixture holds one ALT-loaded WithdrawFunds"
        );

        let mut null_block = loaded_block;
        null_block["transactions"][0]["meta"]["loadedAddresses"] = serde_json::Value::Null;
        let block: RpcBlock =
            serde_json::from_value(null_block).expect("a null loadedAddresses deserializes");

        let rejection = decode_slot(&block, slot, ProgramType::Withdraw, None)
            .expect_err("a null loadedAddresses must reject the slot, not drop the row");

        assert_eq!(rejection.metric_label(), "missing_meta");
        let message = rejection.to_string();
        assert!(
            message.contains(signature) && message.contains("`loadedAddresses`"),
            "rejection must name the transaction and the missing key: {message}"
        );
    }

    // ============================================================================
    // CPI (inner instruction) Tests
    // ============================================================================

    use crate::indexer::datasource::rpc_polling::types::{InnerInstruction, InnerInstructions};
    use solana_transaction_status::{UiAddressTableLookup, UiLoadedAddresses};

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
        // Key 1 is a foreign program, so instructions naming it resolve but are not ours.
        let account_keys =
            with_foreign_program(create_account_keys_with_program(TEST_PROGRAM_ID, 0));
        let foreign = create_instruction(1, vec![], "foreign".to_string());

        let mut tx =
            create_successful_transaction("sig_cpi".to_string(), account_keys, vec![foreign]);
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
                index: 0,
                instructions: vec![
                    inner(1, "skip", 2),             // foreign inner, filtered out
                    inner(0, "cpi_deposit_data", 2), // our program, indexed
                ],
            }]));
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            never_excluded,
            None,
            false,
        )
        .expect("the mock parser decodes every instruction");

        assert_eq!(result.len(), 1);
        let (sig, location, data) = &result[0];
        assert_eq!(sig, &test_sig("sig_cpi"));
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
        // Key 1 is a foreign program, so instructions naming it resolve but are not ours.
        let account_keys =
            with_foreign_program(create_account_keys_with_program(TEST_PROGRAM_ID, 0));
        let foreign_0 = create_instruction(1, vec![], "foreign_0".to_string());
        let foreign_1 = create_instruction(1, vec![], "foreign_1".to_string());

        let mut tx = create_successful_transaction(
            "sig_two_sets".to_string(),
            account_keys,
            vec![foreign_0, foreign_1],
        );
        tx.meta.as_mut().unwrap().inner_instructions = Reported::Present(Some(vec![
            InnerInstructions {
                index: 0,
                instructions: vec![inner(0, "deposit_a", 2)],
            },
            InnerInstructions {
                index: 1,
                instructions: vec![inner(0, "deposit_b", 2)],
            },
        ]));
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            never_excluded,
            None,
            false,
        )
        .expect("the mock parser decodes every instruction");

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
        // Key 1 is a foreign program, so instructions naming it resolve but are not ours.
        let account_keys =
            with_foreign_program(create_account_keys_with_program(TEST_PROGRAM_ID, 0));
        let top = create_instruction(1, vec![], "foreign".to_string());
        let mut tx = create_successful_transaction("sig_excl".to_string(), account_keys, vec![top]);
        // Discriminator 7 (ReleaseFunds) is excluded; 6 (Deposit) is indexed.
        let release_data = bs58::encode([7u8]).into_string();
        let deposit_data = bs58::encode([6u8]).into_string();
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
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
            }]));
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            escrow_inner_discriminator_excluded,
            None,
            false,
        )
        .expect("the mock parser decodes every instruction");

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
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    instruction: CompiledInstruction {
                        program_id_index: 1, // our program (CPI'd)
                        accounts: vec![3],
                        data: "inner".to_string(),
                    },
                    stack_height: Some(2),
                }],
            }]));
        tx.meta.as_mut().unwrap().loaded_addresses = Some(UiLoadedAddresses {
            writable: vec![loaded_writable.clone()],
            readonly: vec![loaded_readonly.clone()],
        });
        declare_lookups(&mut tx, 1, 1);
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            count_keys_parser,
            never_excluded,
            None,
            false,
        )
        .expect("the key-counting parser decodes every instruction");

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
        declare_lookups(&mut tx, 1, 0);
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    instruction: CompiledInstruction {
                        program_id_index: 1, // points into loaded addresses
                        accounts: vec![],
                        data: "cpi".to_string(),
                    },
                    stack_height: Some(2),
                }],
            }]));
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            never_excluded,
            None,
            false,
        )
        .expect("the mock parser decodes every instruction");

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
        declare_lookups(&mut tx, 1, 0);
        block.transactions.push(tx);

        let result = parse_block_for_program(
            &block,
            TEST_PROGRAM_ID,
            mock_parser,
            never_excluded,
            None,
            false,
        )
        .expect("the mock parser decodes every instruction");

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
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
                index: 0,
                instructions: vec![deposit(), event(300), deposit(), event(480)],
            }]));

        let mut block = create_test_block();
        block.transactions.push(tx);

        let result = parse_block(&block, 7, ProgramType::Escrow, None)
            .expect("every deposit in this block decodes");

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
        declare_lookups(&mut tx, 1, 0);
        // The deposit's event self-CPI, emitted by the ALT-loaded escrow program.
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    instruction: CompiledInstruction {
                        program_id_index: 12, // escrow via ALT
                        accounts: vec![],
                        data: bs58::encode(deposit_event_bytes(555)).into_string(),
                    },
                    stack_height: Some(2),
                }],
            }]));

        let mut block = create_test_block();
        block.transactions.push(tx);

        let result = parse_block(&block, 7, ProgramType::Escrow, None)
            .expect("every deposit in this block decodes");

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
        tx.meta.as_mut().unwrap().inner_instructions =
            Reported::Present(Some(vec![InnerInstructions {
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
            }]));

        let mut block = create_test_block();
        block.transactions.push(tx);

        let result = parse_block(&block, 9, ProgramType::Escrow, None)
            .expect("every deposit in this block decodes");

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

    /// A v1 transaction survives `RpcBlock` and reaches the parser intact.
    ///
    /// The envelope is copied from what a node actually served: devnet slot
    /// 495752744, `getBlock` with `maxSupportedTransactionVersion: 1`. Its v1
    /// markers are `version: 1` beside `transaction`, `transactionConfig` nested
    /// inside `message`, and no `addressTableLookups` at all, because v1 dropped
    /// lookup tables (SIMD-0385). None of the three are declared on our structs,
    /// so serde has to drop them; this pins that. Tightening `RpcBlock` with
    /// `deny_unknown_fields` breaks v1 here rather than in production.
    ///
    /// The captured transaction belonged to an unrelated devnet program, so the
    /// instruction payload and its event CPI are swapped for escrow fixtures to
    /// give the parser a row to find. The envelope is the real one; the payload
    /// is version-independent and covered by the deposit tests above.
    ///
    /// v1 format: https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0385-transaction-v1.md
    #[test]
    fn v1_transaction_captured_from_devnet_parses_into_a_deposit_row() {
        let amount = 4_242;
        // Escrow program at key index 0; the rest pad the deposit's 12 accounts.
        let mut account_keys: Vec<String> = (0u8..12)
            .map(|index| test_pubkey(index).to_string())
            .collect();
        account_keys[0] = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string();

        let response = json!({
            "blockhash": "xbRm5shPwQECtyLGoxKKERD7vRqPeNHYqB6vt6hzjMb",
            "parentSlot": 495_752_743u64,
            "transactions": [{
                "version": 1,
                "transaction": {
                    "signatures": ["42dtdM7KqUDEuPZPH8ic7KXoe6e14TrLCAKLVefGPKWhQgWBZkmDomRze9zv9KHyDjHMWSHTGWqwLfKevq6wzQDq"],
                    "message": {
                        "header": {
                            "numRequiredSignatures": 1,
                            "numReadonlySignedAccounts": 0,
                            "numReadonlyUnsignedAccounts": 1
                        },
                        "accountKeys": account_keys,
                        "recentBlockhash": "Fu11pcSvhJBX3sNaE1FzsXC6jPx5wJPTaWrdsfmMPnos",
                        "instructions": [{
                            "programIdIndex": 0,
                            "accounts": (0u8..12).collect::<Vec<u8>>(),
                            "data": bs58::encode(deposit_ix_bytes(amount, None)).into_string(),
                            "stackHeight": 1
                        }],
                        "transactionConfig": {
                            "computeUnitLimit": 1_400_000,
                            "heapSize": null,
                            "loadedAccountsDataSizeLimit": 1_048_576,
                            "priorityFee": null
                        }
                    }
                },
                "meta": {
                    "err": null,
                    "status": { "Ok": null },
                    "fee": 5000,
                    "computeUnitsConsumed": 37_638,
                    "logMessages": [],
                    "loadedAddresses": { "writable": [], "readonly": [] },
                    "innerInstructions": [{
                        "index": 0,
                        "instructions": [{
                            "programIdIndex": 0,
                            "accounts": [],
                            "data": bs58::encode(deposit_event_bytes(amount)).into_string(),
                            "stackHeight": 2
                        }]
                    }]
                }
            }]
        });

        let block: RpcBlock = serde_json::from_value(response)
            .expect("a v1 getBlock response must deserialize into RpcBlock");

        let result = parse_block(&block, 495_752_744, ProgramType::Escrow, None)
            .expect("the v1 deposit decodes");

        assert_eq!(result.len(), 1, "the v1 deposit is indexed");
        assert_eq!(deposit_amount(&result[0]), amount);
        assert_eq!(result[0].instruction_index, 0);
        assert_eq!(result[0].inner_index, None);
    }
}
