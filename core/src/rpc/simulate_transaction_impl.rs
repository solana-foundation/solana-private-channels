use crate::{
    accounts::utils::encode_transaction_data,
    rpc::{
        constants::MAX_SIMULATION_ACCOUNTS_BYTES,
        error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
        ReadDeps,
    },
    scheduler::{ConflictFreeBatch, TransactionWithIndex},
    stage_metrics::{NoopMetrics, SharedMetrics},
    stages::{execute_batch, get_execution_deps, sigverify_transaction, SigverifyResult},
    transactions::{has_address_table_lookups, ADDRESS_LOOKUP_UNSUPPORTED},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use bincode::Options;
use jsonrpsee::core::RpcResult;
use solana_account_decoder::encode_ui_account;
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_rpc_client_types::{
    config::{RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig},
    response::{Response, RpcResponseContext, RpcSimulateTransactionResult},
};
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_sdk::{
    account::ReadableAccount,
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

/// Rejects an unusable `accounts` request before the transaction is executed.
/// Caps addresses at the transaction's key count and refuses bs58, both like Agave.
fn validate_accounts_config(
    accounts_config: &RpcSimulateTransactionAccountsConfig,
    num_account_keys: usize,
) -> RpcResult<()> {
    let encoding = accounts_config
        .encoding
        .unwrap_or(UiAccountEncoding::Base64);
    if matches!(
        encoding,
        UiAccountEncoding::Binary | UiAccountEncoding::Base58
    ) {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            "base58 encoding not supported",
        ));
    }

    if accounts_config.addresses.len() > num_account_keys {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!("Too many accounts provided; max {num_account_keys}"),
        ));
    }

    Ok(())
}

/// Allowance per account for its metadata fields and the JSON punctuation.
const PER_ACCOUNT_JSON_OVERHEAD: usize = 256;

/// Estimated JSON bytes one encoded account contributes.
fn estimated_encoded_bytes(data_len: usize) -> usize {
    data_len.div_ceil(3) * 4 + PER_ACCOUNT_JSON_OVERHEAD
}

/// Resolves every requested address, then encodes only if the whole reply fits.
/// Resolving is a cache lookup, so nothing is encoded when the request is refused.
fn encode_simulation_accounts<C: TransactionProcessingCallback>(
    callbacks: &C,
    accounts_config: RpcSimulateTransactionAccountsConfig,
) -> RpcResult<Vec<Option<UiAccount>>> {
    let encoding = accounts_config
        .encoding
        .unwrap_or(UiAccountEncoding::Base64);

    let mut resolved = Vec::with_capacity(accounts_config.addresses.len());
    let mut estimated = 0usize;
    for address in &accounts_config.addresses {
        let account = match Pubkey::from_str(address) {
            Ok(pubkey) => callbacks
                .get_account_shared_data(&pubkey)
                .map(|account| (pubkey, account)),
            Err(e) => {
                warn!("Failed to get account shared data for {}: {}", address, e);
                None
            }
        };
        if let Some((_, account)) = &account {
            estimated = estimated.saturating_add(estimated_encoded_bytes(account.data().len()));
        }
        resolved.push(account);
    }

    if estimated > MAX_SIMULATION_ACCOUNTS_BYTES {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!(
                "Requested accounts encode to about {estimated} bytes (max: {MAX_SIMULATION_ACCOUNTS_BYTES})"
            ),
        ));
    }

    Ok(resolved
        .into_iter()
        .map(|entry| {
            entry
                .map(|(pubkey, account)| encode_ui_account(&pubkey, &account, encoding, None, None))
        })
        .collect())
}

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

    if has_address_table_lookups(&versioned_tx.message) {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            ADDRESS_LOOKUP_UNSUPPORTED,
        ));
    }

    // Every remaining v0 message declares no lookups, so an empty loaded set is
    // the true resolution rather than a stand-in for one.
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

    // Checked here so a bad address list never pays for a simulation.
    if let Some(accounts_config) = config.accounts.as_ref() {
        validate_accounts_config(accounts_config, sanitized_tx.message().account_keys().len())?;
    }

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
        .get_current_slot()
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
    let execution_result = execute_batch(batch, &mut execution_deps, &noop)
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, e.to_string()))?;

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

                        // Requested accounts are read back post-execution, so BOB holds
                        // this transaction's final state for them.
                        let accounts = match config.accounts {
                            Some(accounts_config) => Some(encode_simulation_accounts(
                                &execution_deps.bob,
                                accounts_config,
                            )?),
                            None => None,
                        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use solana_account_decoder_client_types::UiAccountData;
    use solana_sdk::account::AccountSharedData;
    use solana_svm_callback::InvokeContextCallback;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    /// Serves prebuilt accounts for the configured pubkeys and counts every lookup.
    /// Handing them out by clone mirrors the real cache, where a clone is a refcount bump.
    struct StubAccounts {
        accounts: HashMap<Pubkey, AccountSharedData>,
        lookups: AtomicUsize,
    }

    impl StubAccounts {
        fn new(entries: &[(Pubkey, usize)]) -> Self {
            let accounts = entries
                .iter()
                .map(|(key, data_len)| {
                    (
                        *key,
                        AccountSharedData::new(1, *data_len, &solana_sdk::bpf_loader::id()),
                    )
                })
                .collect();
            Self {
                accounts,
                lookups: AtomicUsize::new(0),
            }
        }

        fn lookups(&self) -> usize {
            self.lookups.load(Ordering::Relaxed)
        }
    }

    impl InvokeContextCallback for StubAccounts {}

    impl TransactionProcessingCallback for StubAccounts {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            self.accounts.get(pubkey).cloned()
        }

        fn account_matches_owners(&self, _account: &Pubkey, _owners: &[Pubkey]) -> Option<usize> {
            None
        }
    }

    // Derived, not hardcoded, so retuning either constant cannot move the boundary silently.
    const EXACT_BUDGET_DATA_LEN: usize =
        ((MAX_SIMULATION_ACCOUNTS_BYTES - PER_ACCOUNT_JSON_OVERHEAD) / 4) * 3;

    fn accounts_cfg(
        encoding: Option<UiAccountEncoding>,
        addresses: &[Pubkey],
    ) -> RpcSimulateTransactionAccountsConfig {
        RpcSimulateTransactionAccountsConfig {
            encoding,
            addresses: addresses.iter().map(|key| key.to_string()).collect(),
        }
    }

    fn distinct_cfg(
        encoding: UiAccountEncoding,
        len: usize,
    ) -> RpcSimulateTransactionAccountsConfig {
        let addresses: Vec<Pubkey> = (0..len).map(|_| Pubkey::new_unique()).collect();
        accounts_cfg(Some(encoding), &addresses)
    }

    // Asked for from both sides, since the cap is what bounds repeated addresses.
    #[test]
    fn cardinality_table() {
        for (addresses, keys, expect_ok) in
            [(0, 3, true), (3, 3, true), (4, 3, false), (1, 0, false)]
        {
            let cfg = distinct_cfg(UiAccountEncoding::Base64, addresses);
            let result = validate_accounts_config(&cfg, keys);
            if expect_ok {
                assert!(
                    result.is_ok(),
                    "{addresses} addresses against {keys} account keys must be allowed: {result:?}"
                );
            } else {
                assert!(
                    result.is_err(),
                    "{addresses} addresses against {keys} account keys must be rejected"
                );
                let err = result.unwrap_err();
                assert_eq!(err.code(), INVALID_PARAMS_CODE);
                assert!(
                    err.message().contains(&keys.to_string()),
                    "rejection must name the max, got: {}",
                    err.message()
                );
            }
        }
    }

    #[test]
    fn encoding_table() {
        for (encoding, expect_ok) in [
            (Some(UiAccountEncoding::Binary), false),
            (Some(UiAccountEncoding::Base58), false),
            (Some(UiAccountEncoding::Base64), true),
            (Some(UiAccountEncoding::Base64Zstd), true),
            (Some(UiAccountEncoding::JsonParsed), true),
            (None, true),
        ] {
            let cfg = accounts_cfg(encoding, &[Pubkey::new_unique()]);
            let result = validate_accounts_config(&cfg, 3);
            if expect_ok {
                assert!(
                    result.is_ok(),
                    "{encoding:?} must be accepted for account results: {result:?}"
                );
            } else {
                let err = result.expect_err("capped encodings must be rejected");
                assert_eq!(err.code(), INVALID_PARAMS_CODE, "encoding {encoding:?}");
            }
        }
    }

    #[test]
    fn budget_boundary_table() {
        assert_eq!(
            estimated_encoded_bytes(EXACT_BUDGET_DATA_LEN),
            MAX_SIMULATION_ACCOUNTS_BYTES,
            "the derived length must sit exactly on the budget"
        );

        for (data_len, expect_ok) in [
            (EXACT_BUDGET_DATA_LEN, true),
            (EXACT_BUDGET_DATA_LEN + 3, false),
        ] {
            let key = Pubkey::new_unique();
            let stub = StubAccounts::new(&[(key, data_len)]);
            let result = encode_simulation_accounts(
                &stub,
                accounts_cfg(Some(UiAccountEncoding::Base64), &[key]),
            );
            if expect_ok {
                assert!(
                    result.is_ok(),
                    "a request landing exactly on the budget must be served: {result:?}"
                );
            } else {
                let err = result.expect_err("an over-budget request must be rejected");
                assert_eq!(err.code(), INVALID_PARAMS_CODE);
            }
        }
    }

    #[test]
    fn budget_rejection_does_no_encoding() {
        let key = Pubkey::new_unique();
        let stub = StubAccounts::new(&[(key, EXACT_BUDGET_DATA_LEN + 3)]);
        let addresses = [key, key];

        let result = encode_simulation_accounts(
            &stub,
            accounts_cfg(Some(UiAccountEncoding::Base64), &addresses),
        );

        assert_eq!(
            result
                .expect_err("over-budget request must be rejected")
                .code(),
            INVALID_PARAMS_CODE
        );
        assert_eq!(
            stub.lookups(),
            addresses.len(),
            "a rejected request must resolve each address once and encode nothing"
        );
    }

    #[test]
    fn repeats_are_counted_and_returned_positionally() {
        // The largest account any caller can reach without creating one.
        let big = Pubkey::new_unique();
        let stub = StubAccounts::new(&[(big, 134_080)]);
        assert!(
            encode_simulation_accounts(
                &stub,
                accounts_cfg(Some(UiAccountEncoding::Base64), &[big])
            )
            .is_ok(),
            "one copy of a large account is well inside the budget"
        );

        let thirty = vec![big; 30];
        let err = encode_simulation_accounts(
            &stub,
            accounts_cfg(Some(UiAccountEncoding::Base64), &thirty),
        )
        .expect_err("every repeat is charged for, so thirty copies must be rejected");
        assert_eq!(err.code(), INVALID_PARAMS_CODE);

        // Small accounts repeat freely, and each requested slot keeps its own entry.
        let small = Pubkey::new_unique();
        let stub = StubAccounts::new(&[(small, 165)]);
        let served = encode_simulation_accounts(
            &stub,
            accounts_cfg(Some(UiAccountEncoding::Base64), &vec![small; 30]),
        )
        .expect("thirty small accounts are inside the budget");
        assert_eq!(served.len(), 30, "one slot per requested address");
        assert!(
            served.iter().all(|slot| slot.is_some()),
            "each repeat must be served, not collapsed"
        );
    }

    #[test]
    fn absent_and_malformed_addresses_are_null_and_free() {
        // This account sits exactly on the budget, so any charge for the other two would tip it over.
        let known = Pubkey::new_unique();
        let stub = StubAccounts::new(&[(known, EXACT_BUDGET_DATA_LEN)]);
        let cfg = RpcSimulateTransactionAccountsConfig {
            encoding: Some(UiAccountEncoding::Base64),
            addresses: vec![
                "!!not-a-pubkey!!".to_string(),
                Pubkey::new_unique().to_string(),
                known.to_string(),
            ],
        };

        let served =
            encode_simulation_accounts(&stub, cfg).expect("absent slots must cost nothing");

        assert_eq!(served.len(), 3);
        assert!(served[0].is_none(), "a malformed address must map to null");
        assert!(served[1].is_none(), "an unknown address must map to null");
        assert!(served[2].is_some(), "a known account must still be served");
    }

    #[test]
    fn requested_encoding_is_honoured() {
        let key = Pubkey::new_unique();
        let stub = StubAccounts::new(&[(key, 165)]);

        for encoding in [UiAccountEncoding::Base64, UiAccountEncoding::Base64Zstd] {
            let served = encode_simulation_accounts(&stub, accounts_cfg(Some(encoding), &[key]))
                .expect("a small account is inside the budget");
            let account = served[0].as_ref().expect("known account must resolve");
            match &account.data {
                UiAccountData::Binary(_, returned) => assert_eq!(*returned, encoding),
                other => panic!("expected binary account data, got {other:?}"),
            }
            assert_eq!(
                account.space,
                Some(165),
                "space reports the raw data length"
            );
        }
    }
}
