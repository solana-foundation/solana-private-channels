use {
    base64::Engine,
    serde::{Deserialize, Serialize},
    solana_sdk::{
        clock::UnixTimestamp, instruction::CompiledInstruction, message::v0::LoadedAddresses,
        pubkey::Pubkey, transaction::VersionedTransaction,
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        ConfirmedTransactionWithStatusMeta, EncodeError, EncodedConfirmedTransactionWithStatusMeta,
        TransactionStatusMeta, TransactionTokenBalance, TransactionWithStatusMeta, UiInstruction,
        UiTransactionEncoding, UiTransactionStatusMeta, VersionedTransactionWithStatusMeta,
    },
    solana_transaction_status_client_types::InnerInstruction,
};

/// Stored transaction with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTransaction {
    pub slot: u64,
    pub block_time: UnixTimestamp,
    pub transaction: VersionedTransaction,
    // Store as UiTransactionStatusMeta because TransactionStatusMeta does not
    // implement Serialize/Deserialize
    pub meta: UiTransactionStatusMeta,
}

impl StoredTransaction {
    pub fn transaction_with_status_meta(&self) -> TransactionWithStatusMeta {
        TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
            transaction: self.transaction.clone(),
            meta: self.ui_to_transaction_status_meta(),
        })
    }

    pub fn encoded_transaction(
        &self,
        encoding: &UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, EncodeError> {
        let confirmed_tx_with_meta = ConfirmedTransactionWithStatusMeta {
            slot: self.slot,
            tx_with_meta: self.transaction_with_status_meta(),
            block_time: Some(self.block_time),
        };
        confirmed_tx_with_meta.encode(*encoding, max_supported_transaction_version)
    }

    fn convert_token_balances(
        balances: Vec<solana_transaction_status::UiTransactionTokenBalance>,
    ) -> Vec<TransactionTokenBalance> {
        use solana_transaction_status_client_types::option_serializer::OptionSerializer;
        balances
            .into_iter()
            .map(|balance| TransactionTokenBalance {
                account_index: balance.account_index,
                mint: balance.mint,
                ui_token_amount: balance.ui_token_amount,
                owner: match balance.owner {
                    OptionSerializer::Some(s) => s,
                    _ => String::new(),
                },
                program_id: match balance.program_id {
                    OptionSerializer::Some(s) => s,
                    _ => String::new(),
                },
            })
            .collect()
    }

    fn ui_to_transaction_status_meta(&self) -> TransactionStatusMeta {
        TransactionStatusMeta {
            status: self.meta.status.clone(),
            fee: self.meta.fee,
            pre_balances: self.meta.pre_balances.clone(),
            post_balances: self.meta.post_balances.clone(),
            inner_instructions: self.meta.inner_instructions.clone().map(|inner| {
                inner
                    .into_iter()
                    .map(
                        |ui_inner| solana_transaction_status_client_types::InnerInstructions {
                            index: ui_inner.index,
                            instructions: ui_inner
                                .instructions
                                .into_iter()
                                .map(|ui_inst| InnerInstruction {
                                    instruction: match ui_inst {
                                        UiInstruction::Compiled(compiled) => CompiledInstruction {
                                            program_id_index: compiled.program_id_index,
                                            accounts: compiled.accounts,
                                            data: bs58::decode(&compiled.data)
                                                .into_vec()
                                                .unwrap_or_default(),
                                        },
                                        // `inner_instructions` is constructed solely from
                                        // `UiInstruction::Compiled` by `StoredTransaction`'s
                                        // own builders (see `UiInstruction::Compiled(...)`
                                        // construction further down in this file). The
                                        // `Parsed` variant is never produced or accepted.
                                        _ => unreachable!(
                                            "inner_instructions only contains Compiled variants"
                                        ),
                                    },
                                    stack_height: None,
                                })
                                .collect(),
                        },
                    )
                    .collect()
            }),
            log_messages: self.meta.log_messages.clone().into(),
            pre_token_balances: self
                .meta
                .pre_token_balances
                .clone()
                .map(Self::convert_token_balances),
            post_token_balances: self
                .meta
                .post_token_balances
                .clone()
                .map(Self::convert_token_balances),
            rewards: self.meta.rewards.clone().into(),
            loaded_addresses: self
                .meta
                .loaded_addresses
                .clone()
                .map(|addresses| LoadedAddresses {
                    writable: addresses
                        .writable
                        .into_iter()
                        .filter_map(|s| Pubkey::try_from(s.as_str()).ok())
                        .collect(),
                    readonly: addresses
                        .readonly
                        .into_iter()
                        .filter_map(|s| Pubkey::try_from(s.as_str()).ok())
                        .collect(),
                })
                .unwrap_or_default(),
            return_data: self
                .meta
                .return_data
                .clone()
                .map(|return_data| TransactionReturnData {
                    program_id: Pubkey::try_from(return_data.program_id.as_str())
                        .unwrap_or_default(),
                    data: base64::engine::general_purpose::STANDARD
                        .decode(&return_data.data.0)
                        .unwrap_or_default(),
                }),
            compute_units_consumed: self.meta.compute_units_consumed.clone().into(),
            cost_units: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        message::Message,
        signature::{Keypair, Signer},
        transaction::Transaction,
    };
    use solana_system_interface::instruction as system_instruction;

    fn make_stored_transaction() -> StoredTransaction {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from.pubkey(), &to, 100);
        let msg = Message::new(&[ix], Some(&from.pubkey()));
        let tx = Transaction::new(&[&from], msg, solana_sdk::hash::Hash::default());

        StoredTransaction {
            slot: 42,
            block_time: 1_700_000_000,
            transaction: tx.into(),
            meta: UiTransactionStatusMeta {
                err: None,
                status: Ok(()),
                fee: 5000,
                pre_balances: vec![100_000, 0],
                post_balances: vec![94_900, 100],
                inner_instructions: None.into(),
                log_messages: None.into(),
                pre_token_balances: None.into(),
                post_token_balances: None.into(),
                rewards: None.into(),
                loaded_addresses: None.into(),
                return_data: None.into(),
                compute_units_consumed: Some(200).into(),
                cost_units: None.into(),
            },
        }
    }

    #[test]
    fn test_transaction_with_status_meta_complete() {
        let stored = make_stored_transaction();
        let tx_with_meta = stored.transaction_with_status_meta();

        match tx_with_meta {
            TransactionWithStatusMeta::Complete(versioned) => {
                assert_eq!(versioned.meta.fee, 5000);
                assert_eq!(versioned.meta.pre_balances, vec![100_000, 0]);
                assert_eq!(versioned.meta.post_balances, vec![94_900, 100]);
            }
            _ => panic!("Expected Complete variant"),
        }
    }

    #[test]
    fn test_ui_to_meta_basic() {
        let stored = make_stored_transaction();
        let meta = stored.ui_to_transaction_status_meta();

        assert!(meta.status.is_ok());
        assert_eq!(meta.fee, 5000);
        assert_eq!(meta.pre_balances.len(), 2);
        assert_eq!(meta.post_balances.len(), 2);
        assert_eq!(meta.compute_units_consumed, Some(200));
    }

    #[test]
    fn test_ui_to_meta_with_inner_instructions() {
        use solana_transaction_status::{UiCompiledInstruction, UiInnerInstructions};

        let mut stored = make_stored_transaction();
        stored.meta.inner_instructions = Some(vec![UiInnerInstructions {
            index: 0,
            instructions: vec![UiInstruction::Compiled(UiCompiledInstruction {
                program_id_index: 1,
                accounts: vec![0, 2],
                data: bs58::encode(&[1, 2, 3]).into_string(),
                stack_height: None,
            })],
        }])
        .into();

        let meta = stored.ui_to_transaction_status_meta();
        let inner = meta.inner_instructions.unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].index, 0);
        assert_eq!(inner[0].instructions[0].instruction.program_id_index, 1);
        assert_eq!(inner[0].instructions[0].instruction.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_ui_to_meta_with_token_balances() {
        use solana_account_decoder_client_types::token::UiTokenAmount;
        use solana_transaction_status::UiTransactionTokenBalance;
        use solana_transaction_status_client_types::option_serializer::OptionSerializer;

        let owner_pubkey = Pubkey::new_unique().to_string();
        let program_id = Pubkey::new_unique().to_string();

        let mut stored = make_stored_transaction();
        stored.meta.pre_token_balances = Some(vec![UiTransactionTokenBalance {
            account_index: 1,
            mint: Pubkey::new_unique().to_string(),
            ui_token_amount: UiTokenAmount {
                ui_amount: Some(100.0),
                decimals: 6,
                amount: "100000000".to_string(),
                ui_amount_string: "100.0".to_string(),
            },
            owner: OptionSerializer::Some(owner_pubkey.clone()),
            program_id: OptionSerializer::Some(program_id.clone()),
        }])
        .into();
        stored.meta.post_token_balances = Some(vec![]).into();

        let meta = stored.ui_to_transaction_status_meta();
        let pre = meta.pre_token_balances.unwrap();
        assert_eq!(pre.len(), 1);
        assert_eq!(pre[0].account_index, 1);
        assert_eq!(pre[0].owner, owner_pubkey);
        assert_eq!(pre[0].program_id, program_id);

        let post = meta.post_token_balances.unwrap();
        assert!(post.is_empty());
    }

    #[test]
    fn test_ui_to_meta_with_loaded_addresses() {
        use solana_transaction_status::UiLoadedAddresses;

        let writable_key = Pubkey::new_unique();
        let readonly_key = Pubkey::new_unique();

        let mut stored = make_stored_transaction();
        stored.meta.loaded_addresses = Some(UiLoadedAddresses {
            writable: vec![writable_key.to_string()],
            readonly: vec![readonly_key.to_string()],
        })
        .into();

        let meta = stored.ui_to_transaction_status_meta();
        assert_eq!(meta.loaded_addresses.writable, vec![writable_key]);
        assert_eq!(meta.loaded_addresses.readonly, vec![readonly_key]);
    }

    #[test]
    fn test_ui_to_meta_with_return_data() {
        use solana_transaction_status::UiTransactionReturnData;
        use solana_transaction_status_client_types::UiReturnDataEncoding;

        let program = Pubkey::new_unique();
        let data_bytes = vec![10, 20, 30];
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data_bytes);

        let mut stored = make_stored_transaction();
        stored.meta.return_data = Some(UiTransactionReturnData {
            program_id: program.to_string(),
            data: (encoded, UiReturnDataEncoding::Base64),
        })
        .into();

        let meta = stored.ui_to_transaction_status_meta();
        let ret = meta.return_data.unwrap();
        assert_eq!(ret.program_id, program);
        assert_eq!(ret.data, data_bytes);
    }

    #[test]
    fn test_encoded_transaction_json() {
        let stored = make_stored_transaction();
        let result = stored.encoded_transaction(&UiTransactionEncoding::Json, Some(0));
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert_eq!(encoded.slot, 42);
        assert_eq!(encoded.block_time, Some(1_700_000_000));
    }
}
