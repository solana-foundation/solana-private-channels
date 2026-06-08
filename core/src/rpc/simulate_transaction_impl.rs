use crate::{
    accounts::utils::encode_transaction_data,
    rpc::{
        error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
        ReadDeps,
    },
    scheduler::{ConflictFreeBatch, TransactionWithIndex},
    stage_metrics::{NoopMetrics, SharedMetrics},
    stages::{execute_batch, get_execution_deps, sigverify_transaction, SigverifyResult},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use bincode::Options;
use jsonrpsee::core::RpcResult;
use solana_account_decoder::encode_ui_account;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_rpc_client_types::{
    config::RpcSimulateTransactionConfig,
    response::{Response, RpcResponseContext, RpcSimulateTransactionResult},
};
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::{
    message::{v0::LoadedAddresses, SimpleAddressLoader},
    pubkey::Pubkey,
    transaction::{MessageHash, VersionedTransaction},
};
use solana_svm::transaction_processing_result::ProcessedTransaction;
use solana_svm_callback::TransactionProcessingCallback;
use solana_transaction_status::{
    UiCompiledInstruction, UiInnerInstructions, UiInstruction, UiReturnDataEncoding,
    UiTransactionEncoding, UiTransactionReturnData,
};
use std::{collections::HashSet, str::FromStr, sync::Arc};
use tokio::sync::mpsc;
use tracing::{info, warn};

// TODO: We should reuse the stages for sigverify and execution so we're not
// duplicating code
pub async fn simulate_transaction(
    read_deps: &ReadDeps,
    transaction: String,
    config: Option<RpcSimulateTransactionConfig>,
) -> RpcResult<Response<RpcSimulateTransactionResult>> {
    let config = config.unwrap_or_default();
    let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);

    // Decode the base64 transaction
    let tx_data = STANDARD.decode(&transaction).map_err(|e| {
        custom_error(
            INVALID_PARAMS_CODE,
            format!("Invalid base64 encoding: {}", e),
        )
    })?;

    // Check packet size limit (1232 bytes is Solana's PACKET_DATA_SIZE)
    const PACKET_DATA_SIZE: usize = 1232;
    if tx_data.len() > PACKET_DATA_SIZE {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!(
                "Transaction too large: {} bytes (max: {} bytes)",
                tx_data.len(),
                PACKET_DATA_SIZE
            ),
        ));
    }

    // Use bincode options matching Agave's decode_and_deserialize
    let bincode_options = bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes();

    // Try to deserialize as VersionedTransaction first (standard format)
    let versioned_tx = bincode_options
        .deserialize::<VersionedTransaction>(&tx_data)
        .map_err(|e| {
            custom_error(
                INVALID_PARAMS_CODE,
                format!("Failed to deserialize transaction: {}", e),
            )
        })?;

    let runtime_tx = RuntimeTransaction::try_create(
        versioned_tx,
        MessageHash::Compute,
        None,
        SimpleAddressLoader::Enabled(LoadedAddresses {
            writable: vec![],
            readonly: vec![],
        }),
        &HashSet::new(),
    )
    .map_err(|err| custom_error(INVALID_PARAMS_CODE, format!("invalid transaction: {err}")))?;
    let sanitized_tx = runtime_tx.into_inner_transaction();

    if config.sig_verify {
        let sigverify_result = sigverify_transaction(&sanitized_tx, &read_deps.admin_keys).await;
        match sigverify_result {
            SigverifyResult::InvalidTransaction(transaction_type) => {
                return Err(custom_error(
                    INVALID_PARAMS_CODE,
                    format!("Invalid transaction: {:?}", transaction_type),
                ));
            }
            SigverifyResult::NotSignedByAdmin => {
                return Err(custom_error(
                    INVALID_PARAMS_CODE,
                    "Transaction not signed by admin".to_string(),
                ));
            }
            SigverifyResult::SigverifyFailed(e) => {
                return Err(custom_error(
                    INVALID_PARAMS_CODE,
                    format!("Sigverify failed: {}", e),
                ));
            }
            SigverifyResult::Valid(_) => (),
        }
    };

    info!("Simulating transaction: {}", sanitized_tx.signature());

    // Get the current slot for context
    let slot = read_deps
        .accounts_db
        .get_latest_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);

    // Simulation must never drop the caller's tx for blockhash expiry; build
    // a synthetic single-entry window containing the tx's own recent blockhash.
    let sim_live_blockhashes = std::sync::Arc::new(std::sync::RwLock::new(
        std::collections::LinkedList::from([*sanitized_tx.message().recent_blockhash()]),
    ));

    let mut batch = ConflictFreeBatch::new();
    batch.add_transaction(TransactionWithIndex {
        transaction: Arc::new(sanitized_tx),
        index: 0,
    });
    let (_settled_accounts_tx, settled_accounts_rx) = mpsc::unbounded_channel();
    // Simulation runs a single transaction; intra-batch parallelism is
    // unnecessary, so disable it (max_svm_workers=1 forces sequential path).
    let mut execution_deps = get_execution_deps(
        read_deps.accounts_db.clone(),
        settled_accounts_rx,
        1,
        sim_live_blockhashes,
    )
    .await;
    let noop: SharedMetrics = std::sync::Arc::new(NoopMetrics);
    let execution_result = execute_batch(batch, &mut execution_deps, &noop).await;

    let result = if let Some(regular_results) = execution_result.regular_results {
        regular_results
    } else if let Some(admin_results) = execution_result.admin_results {
        admin_results
    } else {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            "No execution result found",
        ));
    };

    // Extract execution results
    let value = if let Some(tx_result) = result.processing_results.first() {
        match tx_result {
            Ok(tx_result) => {
                match tx_result {
                    ProcessedTransaction::Executed(executed) => {
                        let logs = executed.execution_details.log_messages.clone();
                        let units_consumed = Some(executed.execution_details.executed_units);

                        // Get account keys and their final states after execution
                        // The loaded_transaction contains accounts with their post-execution state
                        let accounts = config.accounts.map(|accounts_config| {
                            let encoding = accounts_config
                                .encoding
                                .unwrap_or(UiAccountEncoding::Base64);
                            accounts_config
                                .addresses
                                .iter()
                                .map(|address| {
                                    let pubkey = Pubkey::from_str(address);
                                    match pubkey {
                                        Ok(pubkey) => execution_deps
                                            .bob
                                            .get_account_shared_data(&pubkey)
                                            .map(|account_shared_data| {
                                                encode_ui_account(
                                                    &pubkey,
                                                    &account_shared_data,
                                                    encoding,
                                                    None,
                                                    None,
                                                )
                                            }),
                                        Err(e) => {
                                            warn!(
                                                "Failed to get account shared data for {}: {}",
                                                address, e
                                            );
                                            None
                                        }
                                    }
                                })
                                .collect::<Vec<_>>()
                        });
                        let return_data =
                            executed
                                .execution_details
                                .return_data
                                .clone()
                                .map(|return_data| UiTransactionReturnData {
                                    program_id: return_data.program_id.to_string(),
                                    data: (
                                        STANDARD.encode(return_data.data),
                                        UiReturnDataEncoding::Base64,
                                    ),
                                });
                        let inner_instructions =
                            executed.execution_details.inner_instructions.clone().map(
                                |inner_instructions| {
                                    inner_instructions
                                        .iter()
                                        .enumerate()
                                        .map(|(i, inner_instructions)| UiInnerInstructions {
                                            index: i as u8,
                                            instructions: inner_instructions
                                                .iter()
                                                .map(|inner_instruction| {
                                                    let data = encode_transaction_data(
                                                        &inner_instruction.instruction.data,
                                                        encoding,
                                                    );
                                                    UiInstruction::Compiled(UiCompiledInstruction {
                                                        program_id_index: inner_instruction
                                                            .instruction
                                                            .program_id_index,
                                                        accounts: inner_instruction
                                                            .instruction
                                                            .accounts
                                                            .clone(),
                                                        data,
                                                        stack_height: None,
                                                    })
                                                })
                                                .collect(),
                                        })
                                        .collect()
                                },
                            );
                        RpcSimulateTransactionResult {
                            err: executed.execution_details.status.clone().err(),
                            logs,
                            accounts,
                            units_consumed,
                            loaded_accounts_data_size: Some(
                                executed.loaded_transaction.loaded_accounts_data_size,
                            ),
                            return_data,
                            inner_instructions,
                            replacement_blockhash: None,
                        }
                    }
                    ProcessedTransaction::FeesOnly(fees_only) => RpcSimulateTransactionResult {
                        err: Some(fees_only.load_error.clone()),
                        logs: None,
                        accounts: None,
                        units_consumed: None,
                        loaded_accounts_data_size: None,
                        return_data: None,
                        inner_instructions: None,
                        replacement_blockhash: None,
                    },
                }
            }
            Err(e) => {
                return Err(custom_error(
                    INVALID_PARAMS_CODE,
                    format!("Transaction processing error: {:?}", e),
                ));
            }
        }
    } else {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            "No execution result found",
        ));
    };

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value,
    })
}
