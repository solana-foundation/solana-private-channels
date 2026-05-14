//! A helper to initialize Solana SVM API's `TransactionBatchProcessor`.

use {
    anyhow::Result,
    solana_bpf_loader_program::syscalls::create_program_runtime_environment_v1,
    solana_compute_budget::compute_budget::SVMTransactionExecutionBudget,
    solana_program_runtime::{
        execution_budget::SVMTransactionExecutionAndFeeBudgetLimits,
        loaded_programs::{BlockRelation, ForkGraph, LoadProgramMetrics, ProgramCacheEntry},
    },
    solana_sdk::{account::ReadableAccount, clock::Slot, fee::FeeDetails, transaction},
    solana_svm::{
        account_loader::CheckedTransactionDetails,
        transaction_processing_callback::TransactionProcessingCallback,
        transaction_processor::TransactionBatchProcessor,
    },
    solana_svm_feature_set::SVMFeatureSet,
    solana_system_program::system_processor,
    std::{
        num::NonZeroU32,
        sync::{Arc, RwLock},
    },
};

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

/// This function encapsulates some initial setup required to tweak the
/// `TransactionBatchProcessor` for use within PrivateChannel.
///
/// We're simply configuring the mocked fork graph on the SVM API's program
/// cache, then adding the System program to the processor's builtins.
pub fn create_transaction_batch_processor<AccountsDB: TransactionProcessingCallback>(
    accounts_db: &AccountsDB,
    feature_set: &SVMFeatureSet,
    compute_budget: &SVMTransactionExecutionBudget,
) -> Result<(
    TransactionBatchProcessor<PrivateChannelForkGraph>,
    Arc<RwLock<PrivateChannelForkGraph>>,
)> {
    let processor = TransactionBatchProcessor::<PrivateChannelForkGraph>::default();

    // Create and keep the fork graph alive
    let fork_graph = Arc::new(RwLock::new(PrivateChannelForkGraph {}));

    {
        let mut cache = processor.program_cache.write().unwrap();

        // Initialize the mocked fork graph with a weak reference
        cache.fork_graph = Some(Arc::downgrade(&fork_graph));

        // Initialize a proper cache environment.
        // (Use Loader v4 program to initialize runtime v2 if desired)
        cache.environments.program_runtime_v1 = Arc::new(
            create_program_runtime_environment_v1(feature_set, compute_budget, false, false)
                .unwrap(),
        );

        // List of BPF programs to load into the cache
        // These should match the precompiles loaded in BOB
        let bpf_programs = [
            spl_token::id(),
            spl_associated_token_account::id(),
            spl_memo::id(),
            private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        ];

        // Loop over all BPF programs and add them to the cache
        for program_id in bpf_programs {
            if let Some(program_account) = accounts_db.get_account_shared_data(&program_id) {
                let elf_bytes = program_account.data();
                let program_runtime_environment = cache.environments.program_runtime_v1.clone();
                cache.assign_program(
                    program_id,
                    Arc::new(
                        ProgramCacheEntry::new(
                            &solana_sdk::bpf_loader::id(),
                            program_runtime_environment,
                            0,
                            0,
                            elf_bytes,
                            elf_bytes.len(),
                            &mut LoadProgramMetrics::default(),
                        )
                        .unwrap(),
                    ),
                );
            } else {
                return Err(anyhow::anyhow!("BPF program {} not found", program_id));
            }
        }
    }

    // Add the system program builtin.
    processor.add_builtin(
        accounts_db,
        solana_system_program::id(),
        "system_program",
        ProgramCacheEntry::new_builtin(
            0,
            b"system_program".len(),
            system_processor::Entrypoint::vm,
        ),
    );

    // Add the BPF Loader v2 builtin, for the SPL Token program.
    processor.add_builtin(
        accounts_db,
        solana_sdk::bpf_loader::id(),
        "solana_bpf_loader_program",
        ProgramCacheEntry::new_builtin(
            0,
            b"solana_bpf_loader_program".len(),
            solana_bpf_loader_program::Entrypoint::vm,
        ),
    );

    // Fill the sysvar cache with the accounts from the accounts DB
    processor.fill_missing_sysvar_cache_entries(accounts_db);

    Ok((processor, fork_graph))
}

/// This functions is also a mock. In the Agave validator, the bank pre-checks
/// transactions before providing them to the SVM API. We mock this step in
/// PrivateChannel, since we don't need to perform such pre-checks.
pub fn get_transaction_check_results(
    len: usize,
) -> Vec<transaction::Result<CheckedTransactionDetails>> {
    vec![
        transaction::Result::Ok(CheckedTransactionDetails::new(
            None,
            Ok(SVMTransactionExecutionAndFeeBudgetLimits {
                budget: SVMTransactionExecutionBudget::default(),
                loaded_accounts_data_size_limit: NonZeroU32::new(64 * 1024 * 1024)
                    .expect("Failed to set loaded_accounts_bytes"),
                fee_details: FeeDetails::default(),
            }),
        ));
        len
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{account::AccountSharedData, pubkey::Pubkey};
    use solana_svm::transaction_processing_callback::TransactionProcessingCallback;
    use solana_svm_callback::InvokeContextCallback;

    #[test]
    fn test_get_transaction_check_results_constructed_with_expected_values() {
        // Verify the hardcoded configuration values by constructing
        // the expected details and comparing with Debug output.
        // Fields are pub(crate) so we can't access them directly,
        // but we can verify the construction matches our expectation.
        let expected = CheckedTransactionDetails::new(
            None, // no nonce
            Ok(SVMTransactionExecutionAndFeeBudgetLimits {
                budget: SVMTransactionExecutionBudget::default(),
                loaded_accounts_data_size_limit: NonZeroU32::new(64 * 1024 * 1024).expect("64 MiB"),
                fee_details: FeeDetails::default(),
            }),
        );
        let results = get_transaction_check_results(1);
        let actual = results[0].as_ref().unwrap();
        assert_eq!(
            actual, &expected,
            "check result should use None nonce, default budget, 64 MiB limit, default fees"
        );
    }

    /// Minimal mock that returns None for all accounts — triggers the
    /// "BPF program not found" error path.
    struct EmptyAccountsDB;
    impl InvokeContextCallback for EmptyAccountsDB {}
    impl TransactionProcessingCallback for EmptyAccountsDB {
        fn get_account_shared_data(&self, _pubkey: &Pubkey) -> Option<AccountSharedData> {
            None
        }
        fn account_matches_owners(&self, _account: &Pubkey, _owners: &[Pubkey]) -> Option<usize> {
            None
        }
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
