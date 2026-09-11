//! A helper to initialize Solana SVM API's `TransactionBatchProcessor`.

use {
    anyhow::Result,
    solana_compute_budget::compute_budget::SVMTransactionExecutionBudget,
    solana_program_runtime::{
        execution_budget::SVMTransactionExecutionAndFeeBudgetLimits,
        loaded_programs::{BlockRelation, ForkGraph, ProgramRuntimeEnvironments},
        program_cache_entry::ProgramCacheEntry,
        program_metrics::LoadProgramMetrics,
        // Brings the `register` associated function onto each builtin entrypoint.
        solana_sbpf::program::BuiltinFunctionDefinition,
    },
    solana_sdk::{
        account::ReadableAccount,
        clock::{Epoch, Slot},
        fee::FeeDetails,
        transaction,
    },
    solana_svm::{
        account_loader::CheckedTransactionDetails,
        transaction_processing_callback::TransactionProcessingCallback,
        transaction_processor::TransactionBatchProcessor,
    },
    solana_svm_feature_set::SVMFeatureSet,
    solana_syscalls::create_program_runtime_environment,
    solana_system_program::system_processor,
    std::sync::{Arc, RwLock},
};

/// The slot every account and program is reported at.
///
/// There are no slots or forks here, so the processor, the cache entries and the
/// accounts handed to the SVM all agree on zero.
pub const CHANNEL_SLOT: Slot = 0;

/// The epoch the processor runs at, for the same reason as the slot.
const CHANNEL_EPOCH: Epoch = 0;

/// In order to use the `TransactionBatchProcessor`, another trait - Solana
/// Program Runtime's `ForkGraph` - must be implemented, to tell the batch
/// processor how to work across forks.
///
/// Since PrivateChannel doesn't use slots or forks, this implementation is mocked.
pub struct PrivateChannelForkGraph {}

impl ForkGraph for PrivateChannelForkGraph {
    fn relationship(&self, _a: Slot, _b: Slot) -> BlockRelation {
        BlockRelation::Unknown
    }
}

/// Everything a caller needs to keep alive to execute against the processor.
pub struct BatchProcessor {
    pub processor: TransactionBatchProcessor<PrivateChannelForkGraph>,
    /// The cache holds only a weak reference, so the owner must outlive it.
    pub fork_graph: Arc<RwLock<PrivateChannelForkGraph>>,
    /// The environments every batch must be framed with.
    ///
    /// The cache matches a compiled program to an environment by pointer, so a
    /// batch framed with a freshly built environment matches nothing and
    /// recompiles every program on every batch. Handing the caller the exact
    /// environment the entries were compiled with keeps that from happening.
    pub environments: ProgramRuntimeEnvironments,
}

/// This function encapsulates some initial setup required to tweak the
/// `TransactionBatchProcessor` for use within PrivateChannel.
///
/// We're simply configuring the mocked fork graph on the SVM API's program
/// cache, then adding the System program to the processor's builtins.
pub fn create_transaction_batch_processor<AccountsDB: TransactionProcessingCallback>(
    accounts_db: &AccountsDB,
    feature_set: &SVMFeatureSet,
    compute_budget: &SVMTransactionExecutionBudget,
) -> Result<BatchProcessor> {
    // Create and keep the fork graph alive
    let fork_graph = Arc::new(RwLock::new(PrivateChannelForkGraph {}));

    let environment = create_program_runtime_environment(feature_set, compute_budget, false, false)
        .map_err(|e| anyhow::anyhow!("failed to build the program runtime environment: {e}"))?;

    let processor = TransactionBatchProcessor::<PrivateChannelForkGraph>::new(
        CHANNEL_SLOT,
        CHANNEL_EPOCH,
        Arc::downgrade(&fork_graph),
        Some(environment.clone()),
    );

    {
        let mut cache = processor.global_program_cache.write().unwrap();

        // List of BPF programs to load into the cache
        // These should match the precompiles loaded in BOB
        let bpf_programs = [
            spl_token::id(),
            spl_associated_token_account::id(),
            spl_memo::id(),
            private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            dvp_swap_program_client::DVP_SWAP_PROGRAM_ID,
        ];

        // Loop over all BPF programs and add them to the cache
        for program_id in bpf_programs {
            // The slot beside the account is where it was last modified, which
            // this store does not track; the processor runs at slot zero.
            if let Some((program_account, _slot)) = accounts_db.get_account_shared_data(&program_id)
            {
                let elf_bytes = program_account.data();
                cache.assign_program(
                    &environment,
                    program_id,
                    CHANNEL_SLOT,
                    Arc::new(
                        ProgramCacheEntry::new(
                            &solana_sdk::bpf_loader::id(),
                            environment.clone(),
                            CHANNEL_SLOT,
                            CHANNEL_SLOT,
                            elf_bytes,
                            elf_bytes.len(),
                            // Load timings are collected upstream but nothing
                            // here reads them, so they go to a scratch value.
                            &mut LoadProgramMetrics::default(),
                        )
                        .map_err(|e| {
                            anyhow::anyhow!("failed to load BPF program {program_id}: {e}")
                        })?,
                    ),
                );
            } else {
                return Err(anyhow::anyhow!("BPF program {} not found", program_id));
            }
        }
    }

    // Add the system program builtin.
    processor.add_builtin(
        solana_system_program::id(),
        ProgramCacheEntry::new_builtin(
            CHANNEL_SLOT,
            b"system_program".len(),
            system_processor::Entrypoint::register,
        ),
    );

    // Add the BPF Loader v2 builtin, for the SPL Token program.
    processor.add_builtin(
        solana_sdk::bpf_loader::id(),
        ProgramCacheEntry::new_builtin(
            CHANNEL_SLOT,
            b"solana_bpf_loader_program".len(),
            solana_bpf_loader_program::Entrypoint::register,
        ),
    );

    // Fill the sysvar cache with the accounts from the accounts DB
    processor.fill_missing_sysvar_cache_entries(accounts_db);

    // Programs execute and deploy under the same environment because this has no
    // epoch boundary to carry a recompiled one across.
    let environments = ProgramRuntimeEnvironments::new(environment.clone(), environment);

    Ok(BatchProcessor {
        processor,
        fork_graph,
        environments,
    })
}

/// This functions is also a mock. In the Agave validator, the bank pre-checks
/// transactions before providing them to the SVM API. We mock this step in
/// PrivateChannel, since we don't need to perform such pre-checks.
///
/// Every transaction is framed with the same budget regardless of what it asked
/// for. A v1 transaction carries its own compute unit limit, loaded data limit,
/// heap size and priority fee in the message, and all four are ignored here on
/// purpose: the channel is gasless, and compute budget instructions are already
/// refused at admission, so honouring the message fields would be the only place
/// a sender could influence its own budget.
pub fn get_transaction_check_results(
    len: usize,
) -> Vec<transaction::Result<CheckedTransactionDetails>> {
    vec![
        transaction::Result::Ok(CheckedTransactionDetails::new(
            None,
            SVMTransactionExecutionAndFeeBudgetLimits {
                budget: SVMTransactionExecutionBudget::default(),
                loaded_accounts_data_size_limit: MAX_LOADED_ACCOUNTS_DATA_SIZE,
                fee_details: FeeDetails::default(),
            },
        ));
        len
    ]
}

/// Ceiling on the bytes one transaction may load, matching the SVM's own cap.
const MAX_LOADED_ACCOUNTS_DATA_SIZE: u32 = 64 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};
    use solana_svm::transaction_processing_callback::TransactionProcessingCallback;
    use solana_svm_callback::InvokeContextCallback;

    #[test]
    fn test_get_transaction_check_results_constructed_with_expected_values() {
        // The limit fields are private, so the assertion compares against an
        // independently built value rather than reading them back.
        let expected = CheckedTransactionDetails::new(
            None, // no nonce
            SVMTransactionExecutionAndFeeBudgetLimits {
                budget: SVMTransactionExecutionBudget::default(),
                loaded_accounts_data_size_limit: MAX_LOADED_ACCOUNTS_DATA_SIZE,
                fee_details: FeeDetails::default(),
            },
        );
        let results = get_transaction_check_results(1);
        let actual = results[0].as_ref().unwrap();
        assert_eq!(
            actual, &expected,
            "check result should use None nonce, default budget, 64 MiB limit, default fees"
        );
    }

    /// Every check result in a batch carries the same budget, so a batch of
    /// several must not differ from a batch of one.
    #[test]
    fn check_results_are_uniform_across_a_batch() {
        let results = get_transaction_check_results(4);
        assert_eq!(results.len(), 4);
        let first = results[0].as_ref().expect("first result");
        for (index, result) in results.iter().enumerate() {
            assert_eq!(
                result.as_ref().expect("result"),
                first,
                "result {index} should carry the same budget as the first"
            );
        }
    }

    /// Minimal mock that returns None for all accounts, which triggers the
    /// "BPF program not found" error path.
    struct EmptyAccountsDB;
    impl InvokeContextCallback for EmptyAccountsDB {}
    impl TransactionProcessingCallback for EmptyAccountsDB {
        fn get_account_shared_data(&self, _pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
            None
        }
    }

    /// Serves the built-in program accounts, which is all the processor needs
    /// to compile and cache the BPF programs it loads at startup.
    struct PrecompiledAccountsDB;
    impl InvokeContextCallback for PrecompiledAccountsDB {}
    impl TransactionProcessingCallback for PrecompiledAccountsDB {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
            crate::accounts::precompiles::PRECOMPILES
                .get(pubkey)
                .cloned()
                .map(|account| (account, CHANNEL_SLOT))
        }
    }

    /// The environments handed to the caller must be the ones the cached
    /// programs were compiled against.
    ///
    /// A batch framed with a freshly built environment still executes correctly,
    /// so no assertion elsewhere would notice, but the cache matches entries by
    /// pointer and would recompile every program on every batch. Equality on
    /// this type is defined upstream as pointer identity, so comparing the
    /// values is the pointer check.
    #[test]
    fn the_returned_environments_are_the_ones_the_cache_compiled_against() {
        let batch = create_transaction_batch_processor(
            &PrecompiledAccountsDB,
            &SVMFeatureSet::all_enabled(),
            &SVMTransactionExecutionBudget::default(),
        )
        .expect("the precompiled accounts cover every program the processor loads");

        assert_eq!(
            batch.environments.get_env_for_execution(),
            &batch.processor.program_runtime_environment,
            "execution environment must be the processor's own, or the cache misses every entry"
        );
        assert_eq!(
            batch.environments.get_env_for_deployment(),
            &batch.processor.program_runtime_environment,
            "deployment environment must be the processor's own for the same reason"
        );
    }

    #[test]
    fn test_create_processor_fails_when_bpf_program_missing() {
        let db = EmptyAccountsDB;
        let feature_set = SVMFeatureSet::default();
        let budget = SVMTransactionExecutionBudget::default();

        let result = create_transaction_batch_processor(&db, &feature_set, &budget);

        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error when BPF programs are missing"),
        };
        // The first BPF program looked up is spl_token
        assert!(
            err.contains("BPF program") && err.contains("not found"),
            "expected 'BPF program ... not found' error, got: {err}"
        );
        assert!(
            err.contains(&spl_token::id().to_string()),
            "error should mention the first missing program (spl_token), got: {err}"
        );
    }
}
