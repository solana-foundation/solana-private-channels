use std::collections::HashMap;

use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    instruction::InstructionError,
    pubkey::Pubkey,
    transaction::TransactionError,
};
use solana_svm::{
    account_loader::{LoadedTransaction, TransactionCheckResult},
    transaction_error_metrics::TransactionErrorMetrics,
    transaction_execution_result::{ExecutedTransaction, TransactionExecutionDetails},
    transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
    transaction_processor::{
        LoadAndExecuteSanitizedTransactionsOutput, TransactionProcessingConfig,
        TransactionProcessingEnvironment,
    },
};
use solana_svm_callback::TransactionProcessingCallback;
use solana_svm_transaction::svm_transaction::SVMTransaction;
use solana_timings::ExecuteTimings;
use spl_token::solana_program::program_option::COption;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Mint;
use tracing::{debug, warn};

const SPL_TOKEN_ID: Pubkey = spl_token::id();

// SPL Token instruction types
const INSTRUCTION_INITIALIZE_MINT: u8 = 0;

/// This VM is used to execute admin transactions
#[derive(Default)]
pub struct AdminVm {}

impl AdminVm {
    /// Creates a new SPL Token Mint account with the given parameters
    fn create_mint_account(
        decimals: u8,
        mint_authority: &[u8],
        freeze_authority: Option<&[u8]>,
    ) -> AccountSharedData {
        // Parse mint authority pubkey
        let mint_auth_pubkey =
            Pubkey::new_from_array(mint_authority.try_into().expect("Invalid mint authority"));

        // Parse freeze authority if provided
        let freeze_auth_pubkey = freeze_authority
            .map(|auth| Pubkey::new_from_array(auth.try_into().expect("Invalid freeze authority")));

        // Create the Mint struct using official SPL Token types
        let mint = Mint {
            mint_authority: COption::Some(mint_auth_pubkey),
            supply: 0,
            decimals,
            is_initialized: true,
            freeze_authority: freeze_auth_pubkey
                .map(COption::Some)
                .unwrap_or(COption::None),
        };

        // Pack the mint data using the official Pack trait
        let mut mint_data = vec![0u8; Mint::LEN];
        Mint::pack(mint, &mut mint_data).expect("Failed to pack mint");

        // lamports=1 so the SVM's AccountLoader cache doesn't treat the mint
        // as deallocated on subsequent loads within the same batch. See the
        // equivalent comment on system_program in bob.rs::BOB::new.
        let mut account = AccountSharedData::new(1, Mint::LEN, &spl_token::id());
        account.set_data_from_slice(&mint_data);
        account
    }

    /// Creates an ExecutedTransaction result carrying the given status.
    /// On failure (`status.is_err()`), callers pass `vec![]` so nothing persists —
    /// this matches real-SVM atomicity.
    fn create_executed_transaction(
        status: Result<(), TransactionError>,
        accounts: Vec<(Pubkey, AccountSharedData)>,
    ) -> ExecutedTransaction {
        ExecutedTransaction {
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
            programs_modified_by_tx: HashMap::new(),
        }
    }

    /// Process each tx's instructions in order. On the first failing instruction,
    /// short-circuit via `break 'tx` with a failed `ExecutedTransaction`
    ///
    /// Failure mapping:
    /// - non-`spl_token` program id             -> InvalidInstructionData
    /// - empty instruction data                 -> InvalidInstructionData
    /// - unsupported SPL instruction type       -> InvalidInstructionData
    /// - InitializeMint data < 35 bytes         -> InvalidAccountData
    /// - InitializeMint empty accounts          -> InvalidAccountData
    /// - freeze authority tag=1, <32 trailing   -> InvalidAccountData
    /// - mint index out of `account_keys`       -> NotEnoughAccountKeys
    /// - mint already initialized               -> AccountAlreadyInitialized
    pub fn load_and_execute_sanitized_transactions<CB: TransactionProcessingCallback>(
        &self,
        callbacks: &CB,
        sanitized_txs: &[impl SVMTransaction],
        _check_results: Vec<TransactionCheckResult>,
        _environment: &TransactionProcessingEnvironment,
        _config: &TransactionProcessingConfig,
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        let mut processing_results: Vec<TransactionProcessingResult> = vec![];
        for tx in sanitized_txs {
            let mut accounts = vec![];
            let executed: ExecutedTransaction = 'tx: {
                for (ix_index, (program_id, instruction)) in
                    tx.program_instructions_iter().enumerate()
                {
                    // Solana's InstructionError index is a u8;
                    let ix_idx_u8 = u8::try_from(ix_index).unwrap_or(u8::MAX);

                    if *program_id != SPL_TOKEN_ID {
                        warn!("[admin-vm] Unsupported program ID: {}", program_id);
                        break 'tx Self::create_executed_transaction(
                            Err(TransactionError::InstructionError(
                                ix_idx_u8,
                                InstructionError::InvalidInstructionData,
                            )),
                            vec![],
                        );
                    }

                    let Some(&instruction_type) = instruction.data.first() else {
                        warn!("[admin-vm] SPL Token instruction has empty data");
                        break 'tx Self::create_executed_transaction(
                            Err(TransactionError::InstructionError(
                                ix_idx_u8,
                                InstructionError::InvalidInstructionData,
                            )),
                            vec![],
                        );
                    };

                    match instruction_type {
                        INSTRUCTION_INITIALIZE_MINT => {
                            match Self::process_initialize_mint(callbacks, tx, instruction) {
                                Ok((pubkey, account)) => accounts.push((pubkey, account)),
                                Err(err) => {
                                    break 'tx Self::create_executed_transaction(
                                        Err(TransactionError::InstructionError(ix_idx_u8, err)),
                                        vec![],
                                    );
                                }
                            }
                        }
                        _ => {
                            warn!(
                                "[admin-vm] Unsupported SPL token instruction type: {}",
                                instruction_type
                            );
                            break 'tx Self::create_executed_transaction(
                                Err(TransactionError::InstructionError(
                                    ix_idx_u8,
                                    InstructionError::InvalidInstructionData,
                                )),
                                vec![],
                            );
                        }
                    }
                }
                Self::create_executed_transaction(Ok(()), accounts)
            };
            processing_results.push(Ok(ProcessedTransaction::Executed(Box::new(executed))));
        }

        // All three of these fields are intentional no-ops on the admin path:
        //  - error_metrics / execute_timings: defaulting to zero contributes
        //    nothing to the merged output (see execution.rs::merge_svm_outputs).
        //  - balance_collector: gasless execution does not record balance
        //    changes (see execution.rs:250).
        LoadAndExecuteSanitizedTransactionsOutput {
            error_metrics: TransactionErrorMetrics::default(),
            execute_timings: ExecuteTimings::default(),
            balance_collector: None,
            processing_results,
        }
    }

    /// Validate and process a single SPL Token `InitializeMint` instruction.
    /// Returns the (pubkey, Mint account) on success, or the
    /// appropriate `InstructionError` on any validation failure.
    ///
    /// SPL Token `InitializeMint` wire layout
    /// (see `spl_token::instruction::TokenInstruction::pack`):
    ///
    /// ```text
    /// byte  0       : discriminator = 0   (already checked by caller)
    /// byte  1       : decimals (u8)
    /// bytes 2..34   : mint_authority      (Pubkey, 32 bytes)
    /// byte  34      : freeze_authority COption tag: 0 = None, 1 = Some
    /// bytes 35..67  : freeze_authority    (present only when tag = 1)
    /// ```
    ///
    /// All byte indices below refer to this layout.
    fn process_initialize_mint<CB: TransactionProcessingCallback>(
        callbacks: &CB,
        tx: &impl SVMTransaction,
        instruction: solana_svm_transaction::instruction::SVMInstruction,
    ) -> Result<(Pubkey, AccountSharedData), InstructionError> {
        // Minimum payload: instruction must reference the mint as an account,
        // and data must span discriminator + decimals + mint_authority + the
        // freeze_authority COption tag at byte 34. `TokenInstruction::pack`
        // always emits the tag, so any shorter payload is malformed.
        if instruction.accounts.is_empty() || instruction.data.len() < 35 {
            debug!("[admin-vm] InitializeMint: malformed (len or accounts)");
            return Err(InstructionError::InvalidAccountData);
        }

        // Freeze-authority COption: byte 34 is the tag (0 = None, 1 = Some).
        // When Some, bytes 35..67 carry the 32-byte pubkey; reject if truncated.
        let freeze_authority = if instruction.data[34] == 1 {
            if instruction.data.len() < 67 {
                debug!("[admin-vm] InitializeMint: truncated freeze authority");
                return Err(InstructionError::InvalidAccountData);
            }
            Some(&instruction.data[35..67])
        } else {
            None
        };

        // Resolve the mint pubkey: InitializeMint places the mint at
        // `instruction.accounts[0]`
        let account_keys = tx.account_keys();
        let mint_index = instruction.accounts[0] as usize;
        let Some(mint_pubkey) = account_keys.get(mint_index).copied() else {
            return Err(InstructionError::NotEnoughAccountKeys);
        };

        // Refuse re-init on a live mint: if an account already exists at
        // `mint_pubkey` and unpacks as an initialized Mint, a second
        // InitializeMint would silently overwrite supply / decimals / authority.
        // Zero-initialized allocations (is_initialized = false) still proceed.
        if let Some(existing) = callbacks.get_account_shared_data(&mint_pubkey) {
            if Mint::unpack(existing.data())
                .map(|m| m.is_initialized)
                .unwrap_or(false)
            {
                debug!(
                    "[admin-vm] InitializeMint: {} already initialized",
                    mint_pubkey
                );
                return Err(InstructionError::AccountAlreadyInitialized);
            }
        }

        let decimals = instruction.data[1];
        let mint_authority = &instruction.data[2..34];
        let mint_account = Self::create_mint_account(decimals, mint_authority, freeze_authority);
        Ok((mint_pubkey, mint_account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::account::ReadableAccount;
    use spl_token::solana_program::program_pack::Pack;
    use spl_token::state::Mint;

    /// `create_mint_account` packs a Mint with the given decimals + authority
    /// and no freeze authority; the packed bytes round-trip through
    /// `Mint::unpack` to the same values.
    #[test]
    fn test_create_mint_account_roundtrip() {
        let authority = Pubkey::new_unique();
        let account = AdminVm::create_mint_account(6, &authority.to_bytes(), None);

        let mint = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint.decimals, 6);
        assert!(mint.is_initialized);
        assert_eq!(mint.supply, 0);
        assert_eq!(mint.mint_authority, COption::Some(authority));
        assert_eq!(mint.freeze_authority, COption::None);
    }

    /// `create_mint_account` sets `freeze_authority` to `Some` when one is
    /// supplied, and the packed bytes round-trip to the same pubkey.
    #[test]
    fn test_initialize_mint_with_freeze_authority() {
        let authority = Pubkey::new_unique();
        let freeze = Pubkey::new_unique();
        let account =
            AdminVm::create_mint_account(9, &authority.to_bytes(), Some(&freeze.to_bytes()));

        let mint = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint.decimals, 9);
        assert_eq!(mint.freeze_authority, COption::Some(freeze));
    }

    // ─── Test callbacks ─────────────────────────────────────────────────────
    //
    // DummyCb: account lookups always return None → "fresh" state, good for the
    // happy path and most malformed-input cases.
    //
    // StubCbWithInitializedMint / StubCbWithUninitialized: return a specific
    // account for a specific pubkey so the AlreadyInUse + "pre-existing but
    // uninitialized account" paths are exercisable.
    struct DummyCb;
    impl solana_svm_callback::TransactionProcessingCallback for DummyCb {
        fn get_account_shared_data(&self, _pubkey: &Pubkey) -> Option<AccountSharedData> {
            None
        }
        fn account_matches_owners(&self, _account: &Pubkey, _owners: &[Pubkey]) -> Option<usize> {
            None
        }
    }
    impl solana_svm_callback::InvokeContextCallback for DummyCb {}

    /// Returns a pre-initialized SPL Mint for the configured pubkey, None for anything else.
    struct StubCbWithInitializedMint {
        mint: Pubkey,
    }
    impl solana_svm_callback::TransactionProcessingCallback for StubCbWithInitializedMint {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
            if *pubkey == self.mint {
                Some(AdminVm::create_mint_account(
                    6,
                    &Pubkey::new_unique().to_bytes(),
                    None,
                ))
            } else {
                None
            }
        }
        fn account_matches_owners(&self, _account: &Pubkey, _owners: &[Pubkey]) -> Option<usize> {
            None
        }
    }
    impl solana_svm_callback::InvokeContextCallback for StubCbWithInitializedMint {}

    /// Returns an allocated but NOT-initialized account for the configured pubkey.
    /// Simulates the real-Solana case where `create_account` ran but `initialize_mint`
    /// has not — we should still let InitializeMint succeed in that case.
    struct StubCbWithUninitialized {
        mint: Pubkey,
    }
    impl solana_svm_callback::TransactionProcessingCallback for StubCbWithUninitialized {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
            if *pubkey == self.mint {
                // Zeroed data of Mint::LEN → is_initialized = false
                let mut acct = AccountSharedData::new(0, Mint::LEN, &spl_token::id());
                acct.set_data_from_slice(&[0u8; Mint::LEN]);
                Some(acct)
            } else {
                None
            }
        }
        fn account_matches_owners(&self, _account: &Pubkey, _owners: &[Pubkey]) -> Option<usize> {
            None
        }
    }
    impl solana_svm_callback::InvokeContextCallback for StubCbWithUninitialized {}

    // ─── Helpers ────────────────────────────────────────────────────────────

    fn run_admin_vm(
        txs: &[solana_sdk::transaction::SanitizedTransaction],
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        run_admin_vm_with_cb(txs, &DummyCb)
    }

    fn run_admin_vm_with_cb<CB: solana_svm_callback::TransactionProcessingCallback>(
        txs: &[solana_sdk::transaction::SanitizedTransaction],
        cb: &CB,
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        let vm = AdminVm::default();
        let check_results = crate::processor::get_transaction_check_results(txs.len());
        let env = solana_svm::transaction_processor::TransactionProcessingEnvironment::default();
        let config = solana_svm::transaction_processor::TransactionProcessingConfig::default();
        vm.load_and_execute_sanitized_transactions(cb, txs, check_results, &env, &config)
    }

    /// Unwrap a single `ProcessedTransaction::Executed` from the VM output and
    /// assert that its `execution_details.status` matches `expected`. Returns
    /// the `ExecutedTransaction` so callers can inspect `accounts` afterwards.
    ///
    /// Every test in this module asserts on `execution_details.status` via
    /// this helper; asserting only on `accounts` contents is not sufficient.
    fn assert_executed_with_status(
        output: LoadAndExecuteSanitizedTransactionsOutput,
        expected: Result<(), TransactionError>,
    ) -> Box<ExecutedTransaction> {
        assert_eq!(output.processing_results.len(), 1);
        let result = output
            .processing_results
            .into_iter()
            .next()
            .unwrap()
            .unwrap();
        match result {
            ProcessedTransaction::Executed(executed) => {
                assert_eq!(
                    executed.execution_details.status, expected,
                    "status mismatch"
                );
                executed
            }
            _ => panic!("Expected Executed variant"),
        }
    }

    /// Build a SanitizedTransaction with a single instruction targeting the given
    /// program_id, with the mint as the FIRST account meta (as SPL Token
    /// `InitializeMint` expects). Returns both the tx and the mint pubkey so
    /// tests can stub the callback keyed on the same pubkey the VM will query.
    fn make_spl_tx_with_mint(
        program_id: Pubkey,
        data: Vec<u8>,
    ) -> (solana_sdk::transaction::SanitizedTransaction, Pubkey) {
        use solana_sdk::{
            instruction::{AccountMeta, Instruction},
            message::Message,
            signature::{Keypair, Signer},
            transaction::Transaction,
        };
        use std::collections::HashSet;

        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        // Mint first — SPL Token InitializeMint semantics: accounts[0] is the mint.
        let account_metas = vec![
            AccountMeta::new(mint, false),
            AccountMeta::new(payer.pubkey(), true),
        ];
        let ix = Instruction {
            program_id,
            accounts: account_metas,
            data,
        };
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        let sanitized = solana_sdk::transaction::SanitizedTransaction::try_from_legacy_transaction(
            tx,
            &HashSet::new(),
        )
        .unwrap();
        (sanitized, mint)
    }

    fn make_spl_tx(
        program_id: Pubkey,
        _accounts_indices: &[u8],
        data: Vec<u8>,
    ) -> solana_sdk::transaction::SanitizedTransaction {
        make_spl_tx_with_mint(program_id, data).0
    }

    /// Build a valid-looking 35-byte InitializeMint instruction data blob.
    fn valid_init_mint_data(decimals: u8, authority: Pubkey) -> Vec<u8> {
        let mut data = vec![0u8; 35];
        data[1] = decimals;
        data[2..34].copy_from_slice(&authority.to_bytes());
        data[34] = 0; // COption::None for freeze authority
        data
    }

    /// Build a SanitizedTransaction carrying TWO instructions so we can prove
    /// multi-instruction atomicity (real-SVM semantics: fails at first bad ix).
    fn make_two_instruction_spl_tx(
        ix1_data: Vec<u8>,
        ix2_data: Vec<u8>,
    ) -> solana_sdk::transaction::SanitizedTransaction {
        use solana_sdk::{
            instruction::{AccountMeta, Instruction},
            message::Message,
            signature::{Keypair, Signer},
            transaction::Transaction,
        };
        use std::collections::HashSet;

        let payer = Keypair::new();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let ix1 = Instruction {
            program_id: spl_token::id(),
            accounts: vec![
                AccountMeta::new(mint_a, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data: ix1_data,
        };
        let ix2 = Instruction {
            program_id: spl_token::id(),
            accounts: vec![
                AccountMeta::new(mint_b, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data: ix2_data,
        };
        let msg = Message::new(&[ix1, ix2], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        solana_sdk::transaction::SanitizedTransaction::try_from_legacy_transaction(
            tx,
            &HashSet::new(),
        )
        .unwrap()
    }

    // ─── Happy path ─────────────────────────────────────────────────────────

    /// A well-formed InitializeMint tx succeeds and produces a Mint account
    /// whose packed bytes carry the declared decimals and mint authority.
    #[test]
    fn test_spl_valid_initialize_mint() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(9, authority);
        let tx = make_spl_tx(spl_token::id(), &[1], data);

        let executed = assert_executed_with_status(run_admin_vm(&[tx]), Ok(()));

        assert_eq!(executed.loaded_transaction.accounts.len(), 1);
        let (_, account) = &executed.loaded_transaction.accounts[0];
        let mint = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint.decimals, 9);
        assert_eq!(mint.mint_authority, COption::Some(authority));
    }

    // ─── Failure paths ──────────────────────────────────────────────────────

    /// An admin-routed tx whose program is not spl_token surfaces
    /// `InstructionError(0, InvalidInstructionData)` and persists no accounts.
    #[test]
    fn test_load_and_execute_unsupported_program_returns_invalid_instruction_data() {
        let from = solana_sdk::signature::Keypair::new();
        let to = Pubkey::new_unique();
        let tx = crate::test_helpers::create_test_sanitized_transaction(&from, &to, 100);

        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// An spl_token instruction with empty `data` surfaces
    /// `InvalidInstructionData` at ix index 0.
    #[test]
    fn test_spl_empty_data_returns_invalid_instruction_data() {
        let tx = make_spl_tx(spl_token::id(), &[1], vec![]);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// InitializeMint (type byte 0) with data shorter than the minimum
    /// required payload surfaces `InvalidAccountData`.
    #[test]
    fn test_spl_short_data_returns_invalid_account_data() {
        let data = vec![0u8; 10];
        let tx = make_spl_tx(spl_token::id(), &[1], data);

        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidAccountData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// An spl_token instruction whose first byte is a non-InitializeMint
    /// discriminator (here Transfer = 3) surfaces `InvalidInstructionData`.
    #[test]
    fn test_spl_unsupported_instruction_type_returns_invalid_instruction_data() {
        let data = vec![3u8; 10];
        let tx = make_spl_tx(spl_token::id(), &[1], data);

        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// When the callback reports an already-initialized Mint at the target
    /// pubkey, InitializeMint is rejected with `AccountAlreadyInitialized`
    /// and no accounts are persisted (preventing overwrite of the live mint).
    #[test]
    fn test_already_initialized_mint_returns_already_in_use() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, mint_pubkey) = make_spl_tx_with_mint(spl_token::id(), data);
        let cb = StubCbWithInitializedMint { mint: mint_pubkey };

        let executed = assert_executed_with_status(
            run_admin_vm_with_cb(&[tx], &cb),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::AccountAlreadyInitialized,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// A pre-existing but zero-initialized account (allocated, never
    /// initialized) does not block InitializeMint — the VM still succeeds
    /// and produces the fresh Mint.
    #[test]
    fn test_uninitialized_existing_account_still_initializes() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, mint_pubkey) = make_spl_tx_with_mint(spl_token::id(), data);
        let cb = StubCbWithUninitialized { mint: mint_pubkey };

        let executed = assert_executed_with_status(run_admin_vm_with_cb(&[tx], &cb), Ok(()));
        assert_eq!(executed.loaded_transaction.accounts.len(), 1);
    }

    /// InitializeMint data with the freeze-authority COption tag set to 1
    /// (Some) but missing the trailing 32-byte pubkey surfaces
    /// `InvalidAccountData` rather than panicking on the slice.
    #[test]
    fn test_short_freeze_authority_data_errors() {
        let mut data = vec![0u8; 35];
        data[1] = 6;
        data[2..34].copy_from_slice(&Pubkey::new_unique().to_bytes());
        data[34] = 1; // COption::Some but no trailing 32 bytes

        let tx = make_spl_tx(spl_token::id(), &[1], data);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidAccountData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    // ─── Multi-instruction atomicity ───────────────────────────────────────

    /// A two-instruction tx with a valid ix followed by a bad ix fails at
    /// the bad ix's index, and no accounts from the earlier valid ix are
    /// persisted (atomicity: all-or-nothing).
    #[test]
    fn test_multi_instruction_first_bad_fails_at_correct_index() {
        let authority = Pubkey::new_unique();
        let valid = valid_init_mint_data(6, authority);
        // Second instruction: unsupported SPL Token type 3 (Transfer).
        let bad = vec![3u8; 10];

        let tx = make_two_instruction_spl_tx(valid, bad);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(
            executed.loaded_transaction.accounts.is_empty(),
            "multi-instruction atomicity violated: partial accounts leaked"
        );
    }

    /// If the first instruction fails, subsequent instructions are not
    /// processed — the reported error index is 0 and no accounts persist.
    #[test]
    fn test_multi_instruction_second_valid_not_reached() {
        let authority = Pubkey::new_unique();
        let bad = vec![3u8; 10];
        let valid = valid_init_mint_data(6, authority);

        let tx = make_two_instruction_spl_tx(bad, valid);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    // ─── Low-level invariants ───────────────────────────────────────────────

    /// `SanitizedTransaction` guarantees that compiled
    /// `instruction.accounts` indices are in-range for `account_keys`, so
    /// the VM's index lookups are safe. This test exercises a minimal valid
    /// InitializeMint that relies on that invariant.
    #[test]
    fn test_spl_compiled_indices_prevent_oob() {
        let mut data = vec![0u8; 35];
        data[1] = 6;
        data[2..34].copy_from_slice(&Pubkey::new_unique().to_bytes());
        data[34] = 0; // COption::None for freeze authority

        use solana_sdk::{
            instruction::{AccountMeta, Instruction},
            message::Message,
            signature::{Keypair, Signer},
            transaction::Transaction,
        };
        use std::collections::HashSet;

        let payer = Keypair::new();
        let ix = Instruction {
            program_id: spl_token::id(),
            accounts: vec![AccountMeta::new(payer.pubkey(), true)],
            data,
        };
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        let sanitized = solana_sdk::transaction::SanitizedTransaction::try_from_legacy_transaction(
            tx,
            &HashSet::new(),
        )
        .unwrap();

        // accounts[0] is the payer (valid index). The ix is a valid InitializeMint
        // targeting the payer pubkey as the mint → VM succeeds.
        let output = run_admin_vm(&[sanitized]);
        assert_eq!(output.processing_results.len(), 1);
    }
}
