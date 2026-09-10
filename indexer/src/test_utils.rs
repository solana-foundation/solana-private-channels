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

    /// Create a transaction whose meta is present but carries a null
    /// innerInstructions list, the shape a chain that records none returns.
    pub fn create_transaction_incomplete_meta(
        signature: String,
        account_keys: Vec<String>,
        instructions: Vec<CompiledInstruction>,
    ) -> RpcTransactionWithMeta {
        let mut tx = create_successful_transaction(signature, account_keys, instructions);
        if let Some(meta) = tx.meta.as_mut() {
            meta.inner_instructions = None;
        }
        tx
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
#[cfg(any(test, feature = "test-mock-storage"))]
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
    use serde_json::json;

    /// Mock `getSlot` replying with the chain tip.
    pub fn mock_get_slot(server: &mut Server, slot: u64) -> Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                json!({ "method": "getSlot" }),
            ))
            .with_status(200)
            .with_body(json!({ "jsonrpc": "2.0", "result": slot, "id": 1 }).to_string())
            .create()
    }

    /// Mock `getBlocks(start, end)` replying with the slots that produced a block.
    /// Body-matched on method and range so it coexists with the getBlock mocks.
    pub fn mock_get_blocks(server: &mut Server, start: u64, end: u64, produced: &[u64]) -> Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlocks",
                "params": [start, end]
            })))
            .with_status(200)
            .with_body(json!({ "jsonrpc": "2.0", "result": produced, "id": 1 }).to_string())
            .create()
    }

    /// Mock `getBlocks` failing with a JSON-RPC error, for the enumeration-failure path.
    pub fn mock_get_blocks_error(server: &mut Server, code: i32, message: &str) -> Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlocks"
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "error": { "code": code, "message": message },
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    /// Mock `getBlocksWithLimit(start, ..)`, the lookup that finds the tail witness.
    /// An empty `produced` is the "no witness listed" case.
    pub fn mock_get_blocks_with_limit(server: &mut Server, start: u64, produced: &[u64]) -> Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlocksWithLimit",
                "params": [start]
            })))
            .with_status(200)
            .with_body(json!({ "jsonrpc": "2.0", "result": produced, "id": 1 }).to_string())
            .create()
    }

    /// Mock `getBlock(slot)` returning an empty block whose header names `parent_slot`.
    /// The parent link is what the classifier proves absence with.
    pub fn mock_get_block_at(server: &mut Server, slot: u64, parent_slot: u64) -> Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlock",
                "params": [slot]
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "blockhash": format!("TestBlockHash{slot}"),
                        "parentSlot": parent_slot,
                        "transactions": []
                    },
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    /// Escrow instance the `mock_get_block_with_deposit` fixture parses to.
    ///
    /// A Deposit reads its instance from account index 2, and the fixture passes
    /// accounts `0..12` straight through, so the instance is whatever sits at
    /// account key 2. Callers configure the indexer with this value; anything
    /// else makes the processor drop the deposit as out of scope.
    pub fn deposit_fixture_instance() -> solana_sdk::pubkey::Pubkey {
        crate::test_utils::pubkey::test_pubkey(2)
    }

    /// Mock `getBlock(slot)` returning a block with one top-level escrow Deposit.
    ///
    /// The deposit's amount is carried by its DepositEvent self-CPI, which is the
    /// value the parser records, so both halves are built from `amount`.
    pub fn mock_get_block_with_deposit(
        server: &mut Server,
        slot: u64,
        parent_slot: u64,
        amount: u64,
    ) -> Mock {
        use crate::indexer::datasource::common::parser::escrow::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
        use crate::test_utils::escrow_fixtures::{deposit_event_bytes, deposit_ix_bytes};

        // Escrow program at key index 0; the rest pad the deposit's 12 accounts.
        let mut account_keys: Vec<String> = (0u8..12)
            .map(|i| crate::test_utils::pubkey::test_pubkey(i).to_string())
            .collect();
        account_keys[0] = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string();

        let deposit_data = bs58::encode(deposit_ix_bytes(amount, None)).into_string();
        let event_data = bs58::encode(deposit_event_bytes(amount)).into_string();

        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlock",
                "params": [slot]
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "blockhash": format!("TestBlockHash{slot}"),
                        "parentSlot": parent_slot,
                        "transactions": [{
                            "transaction": {
                                "signatures": [format!("sig_deposit_slot_{slot}")],
                                "message": {
                                    "accountKeys": account_keys,
                                    "instructions": [{
                                        "programIdIndex": 0,
                                        "accounts": (0u8..12).collect::<Vec<u8>>(),
                                        "data": deposit_data
                                    }]
                                }
                            },
                            "meta": {
                                "err": null,
                                "logMessages": null,
                                "loadedAddresses": null,
                                "innerInstructions": [{
                                    "index": 0,
                                    "instructions": [{
                                        "programIdIndex": 0,
                                        "accounts": [],
                                        "data": event_data,
                                        "stackHeight": 2
                                    }]
                                }]
                            }
                        }]
                    },
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    /// Mock `getBlock(slot)` answering a JSON-RPC error with the given code.
    pub fn mock_get_block_error(server: &mut Server, slot: u64, code: i32, message: &str) -> Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(json!({
                "method": "getBlock",
                "params": [slot]
            })))
            .with_status(200)
            .with_body(
                json!({
                    "jsonrpc": "2.0",
                    "error": { "code": code, "message": message },
                    "id": 1
                })
                .to_string(),
            )
            .create()
    }

    /// Mock `getBlock(slot)` answering one of the skipped-or-missing error codes
    /// (-32004 / -32007 / -32009) that a node returns when it cannot serve a slot.
    pub fn mock_get_block_absent(server: &mut Server, slot: u64, code: i32) -> Mock {
        mock_get_block_error(server, slot, code, "Slot skipped or missing")
    }

    /// Register a whole scenario in one line: the `getBlocks` enumeration over
    /// `[start, end]` plus one `getBlock` per `(slot, parent_slot)` producer.
    pub fn chain(server: &mut Server, start: u64, end: u64, producers: &[(u64, u64)]) -> Vec<Mock> {
        let slots: Vec<u64> = producers.iter().map(|(slot, _)| *slot).collect();
        // The backfill floor is exclusive, so the anchor lookup asks from one slot
        // below the range. Same producers answer it.
        let mut mocks = vec![
            mock_get_blocks(server, start, end, &slots),
            mock_get_blocks(server, start.saturating_sub(1), end, &slots),
        ];
        for (slot, parent) in producers {
            mocks.push(mock_get_block_at(server, *slot, *parent));
        }
        mocks
    }

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
