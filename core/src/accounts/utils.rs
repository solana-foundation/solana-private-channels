use {
    super::types::StoredTransaction,
    base64::{engine::general_purpose::STANDARD, Engine},
    solana_sdk::{
        account::ReadableAccount, clock::UnixTimestamp, message::v0::LoadedAddresses,
        transaction::SanitizedTransaction,
    },
    solana_svm::transaction_processing_result::ProcessedTransaction,
    solana_transaction_status::{
        TransactionStatusMeta, UiTransactionEncoding, UiTransactionStatusMeta,
    },
    solana_transaction_status_client_types::InnerInstructions,
    tracing::debug,
};

pub fn get_stored_transaction(
    transaction: &SanitizedTransaction,
    slot: u64,
    block_time: UnixTimestamp,
    processed: &ProcessedTransaction,
) -> StoredTransaction {
    debug!("Stored transaction: {:?}", processed);

    let meta = match processed {
        ProcessedTransaction::Executed(executed) => {
            let details = &executed.execution_details;
            // A failed executed tx commits no account changes; its loaded accounts hold rolled-back intermediate state, so report no balances rather than values that disagree with committed state.
            let balances: Vec<u64> = if details.status.is_ok() {
                executed
                    .loaded_transaction
                    .accounts
                    .iter()
                    .map(|(_, account)| account.lamports())
                    .collect()
            } else {
                Vec::new()
            };
            TransactionStatusMeta {
                status: details.status.clone(),
                fee: executed.loaded_transaction.fee_details.total_fee(),
                pre_balances: balances.clone(),
                post_balances: balances,
                inner_instructions: details.inner_instructions.as_ref().map(|inner| {
                    inner
                        .iter()
                        .enumerate()
                        .map(|(index, instructions)| InnerInstructions {
                            index: index as u8,
                            instructions: instructions
                                .iter()
                                .map(|ii| {
                                    solana_transaction_status_client_types::InnerInstruction {
                                        instruction: ii.instruction.clone(),
                                        stack_height: Some(ii.stack_height as u32),
                                    }
                                })
                                .collect(),
                        })
                        .collect()
                }),
                log_messages: details.log_messages.clone(),
                pre_token_balances: None,
                post_token_balances: None,
                rewards: None,
                loaded_addresses: LoadedAddresses::default(),
                return_data: details.return_data.clone(),
                compute_units_consumed: Some(details.executed_units),
                cost_units: Some(executed.loaded_transaction.loaded_accounts_data_size as u64),
            }
        }
        ProcessedTransaction::FeesOnly(fees_only) => TransactionStatusMeta {
            status: Err(fees_only.load_error.clone()),
            fee: fees_only.fee_details.total_fee(),
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        },
    };

    StoredTransaction {
        slot,
        block_time,
        transaction: transaction.to_versioned_transaction(),
        meta: UiTransactionStatusMeta::from(meta),
    }
}

pub fn encode_transaction_data(data: &[u8], encoding: UiTransactionEncoding) -> String {
    match encoding {
        UiTransactionEncoding::Base58 => bs58::encode(data).into_string(),
        _ => STANDARD.encode(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_base58() {
        let data = b"hello";
        let encoded = encode_transaction_data(data, UiTransactionEncoding::Base58);
        assert_eq!(encoded, bs58::encode(b"hello").into_string());
        // Verify roundtrip
        let decoded = bs58::decode(&encoded).into_vec().unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_encode_base64() {
        let data = b"hello";
        let encoded = encode_transaction_data(data, UiTransactionEncoding::Base64);
        assert_eq!(encoded, STANDARD.encode(b"hello"));
        // Verify roundtrip
        let decoded = STANDARD.decode(&encoded).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn test_encode_binary_same_as_base64() {
        let data = b"test data";
        let base64 = encode_transaction_data(data, UiTransactionEncoding::Base64);
        let binary = encode_transaction_data(data, UiTransactionEncoding::Binary);
        assert_eq!(base64, binary);
    }

    #[test]
    fn test_encode_json_uses_base64() {
        let data = b"json data";
        let json = encode_transaction_data(data, UiTransactionEncoding::Json);
        let base64 = encode_transaction_data(data, UiTransactionEncoding::Base64);
        assert_eq!(json, base64);
    }

    #[test]
    fn test_encode_json_parsed_uses_base64() {
        let data = b"parsed data";
        let parsed = encode_transaction_data(data, UiTransactionEncoding::JsonParsed);
        let base64 = encode_transaction_data(data, UiTransactionEncoding::Base64);
        assert_eq!(parsed, base64);
    }

    use solana_sdk::{
        account::AccountSharedData, instruction::InstructionError, pubkey::Pubkey,
        signature::Keypair, transaction::TransactionError,
    };
    use solana_svm::account_loader::LoadedTransaction;
    use solana_svm::transaction_execution_result::{
        ExecutedTransaction, TransactionExecutionDetails,
    };

    fn executed_processed(
        status: Result<(), TransactionError>,
        accounts: Vec<(Pubkey, AccountSharedData)>,
    ) -> ProcessedTransaction {
        ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts,
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status,
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: std::collections::HashMap::new(),
        }))
    }

    /// A successful executed tx records its loaded-account lamports as the stored meta balances.
    #[test]
    fn stored_meta_records_balances_for_successful_executed() {
        let tx = crate::test_helpers::create_test_sanitized_transaction(
            &Keypair::new(),
            &Pubkey::new_unique(),
            0,
        );
        let acct = AccountSharedData::new(7, 0, &Pubkey::new_unique());
        let processed = executed_processed(Ok(()), vec![(Pubkey::new_unique(), acct)]);

        let stored = get_stored_transaction(&tx, 1, 0, &processed);

        assert_eq!(stored.meta.pre_balances, vec![7]);
        assert_eq!(stored.meta.post_balances, vec![7]);
    }

    /// A failed executed tx commits nothing, so its stored meta must not report the rolled-back intermediate balances that would disagree with committed state.
    #[test]
    fn stored_meta_omits_balances_for_failed_executed() {
        let tx = crate::test_helpers::create_test_sanitized_transaction(
            &Keypair::new(),
            &Pubkey::new_unique(),
            0,
        );
        let intermediate = AccountSharedData::new(6, 0, &Pubkey::new_unique());
        let processed = executed_processed(
            Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(0),
            )),
            vec![(Pubkey::new_unique(), intermediate)],
        );

        let stored = get_stored_transaction(&tx, 1, 0, &processed);

        assert!(
            stored.meta.pre_balances.is_empty(),
            "failed executed tx must not report intermediate balances"
        );
        assert!(stored.meta.post_balances.is_empty());
        assert!(
            stored.meta.err.is_some(),
            "failed executed tx must still record its error status"
        );
    }
}
