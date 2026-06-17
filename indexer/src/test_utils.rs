//! Shared test utilities for unit tests across the indexer crate

#[cfg(test)]
pub mod pubkey {
    use solana_sdk::pubkey::Pubkey;
    /// Generate a deterministic test pubkey from a seed
    pub fn test_pubkey(seed: u8) -> Pubkey {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        Pubkey::new_from_array(bytes)
    }
}

#[cfg(feature = "datasource-rpc")]
#[cfg(test)]
pub mod rpc_blocks {
    use crate::indexer::datasource::common::types::CompiledInstruction;
    use crate::indexer::datasource::rpc_polling::types::{
        EncodedMessage, EncodedTransaction, RpcBlock, RpcTransactionWithMeta, TransactionMeta,
    };
    use crate::test_utils::pubkey;

    /// Create an empty test block with default values
    pub fn create_test_block() -> RpcBlock {
        RpcBlock {
            blockhash: "TestBlockHash11111111111111111111111111111".to_string(),
            parent_slot: 0,
            transactions: vec![],
        }
    }

    /// Create a test transaction with the given signature and instructions
    pub fn create_transaction(
        signature: String,
        account_keys: Vec<String>,
        instructions: Vec<CompiledInstruction>,
        is_failed: bool,
    ) -> RpcTransactionWithMeta {
        let meta = if is_failed {
            Some(TransactionMeta {
                err: Some(serde_json::json!({"InstructionError": [0, "Custom(1)"]})),
                log_messages: None,
                inner_instructions: None,
                loaded_addresses: None,
            })
        } else {
            Some(TransactionMeta {
                err: None,
                log_messages: None,
                inner_instructions: None,
                loaded_addresses: None,
            })
        };

        RpcTransactionWithMeta {
            transaction: EncodedTransaction {
                signatures: vec![signature],
                message: EncodedMessage {
                    account_keys,
                    instructions,
                },
            },
            meta,
        }
    }

    /// Create a successful transaction (no error)
    pub fn create_successful_transaction(
        signature: String,
        account_keys: Vec<String>,
        instructions: Vec<CompiledInstruction>,
    ) -> RpcTransactionWithMeta {
        create_transaction(signature, account_keys, instructions, false)
    }

    /// Create a failed transaction
    pub fn create_failed_transaction(
        signature: String,
        account_keys: Vec<String>,
        instructions: Vec<CompiledInstruction>,
    ) -> RpcTransactionWithMeta {
        create_transaction(signature, account_keys, instructions, true)
    }

    /// Create a transaction with no meta (should be treated as successful)
    pub fn create_transaction_no_meta(
        signature: String,
        account_keys: Vec<String>,
        instructions: Vec<CompiledInstruction>,
    ) -> RpcTransactionWithMeta {
        RpcTransactionWithMeta {
            transaction: EncodedTransaction {
                signatures: vec![signature],
                message: EncodedMessage {
                    account_keys,
                    instructions,
                },
            },
            meta: None,
        }
    }

    /// Create a compiled instruction
    pub fn create_instruction(
        program_id_index: u8,
        accounts: Vec<u8>,
        data: String,
    ) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index,
            accounts,
            data,
        }
    }

    pub fn create_account_keys_with_program(program_id: &str, program_index: usize) -> Vec<String> {
        (0..program_index)
            .map(|i| pubkey::test_pubkey(i as u8).to_string())
            .chain(std::iter::once(program_id.to_string()))
            .collect()
    }
}

/// Byte-layout builders for escrow Deposit instructions and their DepositEvent
/// self-CPI. Centralised here so the escrow parser tests and the decoder tests
/// build the exact same bytes against one source of truth (the `pub(crate)`
/// escrow constants), rather than each re-encoding the layout.
#[cfg(test)]
pub mod escrow_fixtures {
    use crate::indexer::datasource::common::parser::escrow::{
        DEPOSIT, DEPOSIT_EVENT_DISCRIMINATOR, EVENT_IX_TAG_LE,
    };
    use solana_sdk::pubkey::Pubkey;

    /// Borsh body of a Deposit instruction (after the discriminator):
    /// amount (u64 LE) + `Option<recipient>`.
    pub fn deposit_borsh(amount: u64, recipient: Option<Pubkey>) -> Vec<u8> {
        let mut data = amount.to_le_bytes().to_vec();
        match recipient {
            Some(r) => {
                data.push(1);
                data.extend_from_slice(r.as_ref());
            }
            None => data.push(0),
        }
        data
    }

    /// Full Deposit *instruction* bytes: discriminator + borsh body. Pre-base58.
    pub fn deposit_ix_bytes(amount: u64, recipient: Option<Pubkey>) -> Vec<u8> {
        let mut data = vec![DEPOSIT];
        data.extend(deposit_borsh(amount, recipient));
        data
    }

    /// DepositEvent self-CPI bytes (145B): tag(8) + disc(1) + instance_seed(32)
    /// + user(32) + amount(8 LE) + recipient(32) + mint(32).
    pub fn deposit_event_bytes(amount: u64) -> Vec<u8> {
        let mut data = Vec::with_capacity(145);
        data.extend_from_slice(EVENT_IX_TAG_LE);
        data.push(DEPOSIT_EVENT_DISCRIMINATOR);
        data.extend_from_slice(&[0u8; 32]); // instance_seed
        data.extend_from_slice(&[0u8; 32]); // user
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&[0u8; 32]); // recipient
        data.extend_from_slice(&[0u8; 32]); // mint
        data
    }
}

#[cfg(test)]
pub mod rpc_mocks {
    use mockito::{Mock, Server};

    /// Create a mock JSON-RPC response with a successful result
    pub async fn mock_rpc_success(server: &mut Server, result: &str) -> Mock {
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{
                "jsonrpc": "2.0",
                "result": {},
                "id": 1
            }}"#,
                result
            ))
            .create_async()
            .await
    }

    /// Create a mock JSON-RPC response with an error
    pub async fn mock_rpc_error(server: &mut Server, code: i32, message: &str) -> Mock {
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{
                "jsonrpc": "2.0",
                "error": {{
                    "code": {},
                    "message": "{}"
                }},
                "id": 1
            }}"#,
                code, message
            ))
            .create_async()
            .await
    }
}
