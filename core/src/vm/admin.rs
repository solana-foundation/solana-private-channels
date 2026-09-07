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
const INSTRUCTION_INITIALIZE_MINT2: u8 = 20;

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

    /// Preconditions canonical SPL Token execution would enforce before an
    /// `InitializeMint` could write the mint: the SVM's account rules
    /// (writable, non-executable, token-owned) and the processor's own
    /// (exactly `Mint::LEN` bytes, not already initialized). Synthesizing the
    /// post-state skips both, so without these the VM would replace any
    /// account an admin transaction happens to reference.
    ///
    /// A missing target stays legal: the private channel allocates and
    /// initializes in one step so mint addresses mirror mainnet.
    ///
    /// Rent exemption is deliberately not checked. Execution is gasless
    /// (`GaslessRentCollector` reports zero rent) and `create_mint_account`
    /// holds 1 lamport by design.
    fn check_initialize_mint_target(
        existing: Option<&AccountSharedData>,
        is_writable: bool,
    ) -> Result<(), InstructionError> {
        if !is_writable {
            return Err(InstructionError::ReadonlyDataModified);
        }

        let Some(existing) = existing else {
            return Ok(());
        };

        if existing.executable() {
            return Err(InstructionError::ExecutableDataModified);
        }
        if *existing.owner() != SPL_TOKEN_ID {
            return Err(InstructionError::ExternalAccountDataModified);
        }
        // `unpack_unchecked` rejects any length but `Mint::LEN`, and unlike
        // `unpack` it still decodes an uninitialized allocation.
        let mint = Mint::unpack_unchecked(existing.data())
            .map_err(|_| InstructionError::InvalidAccountData)?;
        if mint.is_initialized {
            return Err(InstructionError::AccountAlreadyInitialized);
        }
        Ok(())
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
    /// - mint read-only in the message          -> ReadonlyDataModified
    /// - existing target executable             -> ExecutableDataModified
    /// - existing target not SPL-Token-owned    -> ExternalAccountDataModified
    /// - existing target not `Mint::LEN` bytes  -> InvalidAccountData
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
            let mut created_mints: Vec<(usize, Pubkey, AccountSharedData)> = vec![];
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
                        // InitializeMint2 shares InitializeMint's data layout;
                        // only its account list differs (no rent sysvar), which
                        // process_initialize_mint does not read.
                        INSTRUCTION_INITIALIZE_MINT | INSTRUCTION_INITIALIZE_MINT2 => {
                            match Self::process_initialize_mint(callbacks, tx, instruction) {
                                Ok((mint_index, pubkey, account)) => {
                                    // Reject a second init of the same mint in one tx,
                                    // like real SVM, instead of overwriting the first.
                                    if created_mints.iter().any(|(i, _, _)| *i == mint_index) {
                                        break 'tx Self::create_executed_transaction(
                                            Err(TransactionError::InstructionError(
                                                ix_idx_u8,
                                                InstructionError::AccountAlreadyInitialized,
                                            )),
                                            vec![],
                                        );
                                    }
                                    created_mints.push((mint_index, pubkey, account))
                                }
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
                // Mirror account_keys() in order so downstream is_writable(index)
                // lines up. Fill slots from the callback, then place the mint(s).
                let account_keys = tx.account_keys();
                let mut mirror: Vec<(Pubkey, AccountSharedData)> = (0..account_keys.len())
                    .map(|i| {
                        // i is always in range; expect so a future refactor panics
                        // instead of silently seating a zero pubkey at a writable slot.
                        let key = account_keys
                            .get(i)
                            .copied()
                            .expect("index is within account_keys length");
                        // Mint slots are overwritten below
                        let account = if created_mints.iter().any(|(mi, _, _)| *mi == i) {
                            AccountSharedData::default()
                        } else {
                            callbacks.get_account_shared_data(&key).unwrap_or_default()
                        };
                        (key, account)
                    })
                    .collect();
                for (mint_index, pubkey, account) in created_mints {
                    mirror[mint_index] = (pubkey, account);
                }
                Self::create_executed_transaction(Ok(()), mirror)
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
    /// Returns the (account_keys index, pubkey, Mint account) on success, so the
    /// caller can place the mint at its true slot in the account_keys mirror, or
    /// the appropriate `InstructionError` on any validation failure.
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
    ) -> Result<(usize, Pubkey, AccountSharedData), InstructionError> {
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

        let existing = callbacks.get_account_shared_data(&mint_pubkey);
        if let Err(err) =
            Self::check_initialize_mint_target(existing.as_ref(), tx.is_writable(mint_index))
        {
            debug!(
                "[admin-vm] InitializeMint: target {} rejected: {:?}",
                mint_pubkey, err
            );
            return Err(err);
        }

        let decimals = instruction.data[1];
        let mint_authority = &instruction.data[2..34];
        let mint_account = Self::create_mint_account(decimals, mint_authority, freeze_authority);
        Ok((mint_index, mint_pubkey, mint_account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::account::{Account, ReadableAccount, WritableAccount};
    use solana_sdk::instruction::{AccountMeta, Instruction};
    use solana_sdk::message::Message;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::transaction::{SanitizedTransaction, Transaction};
    use solana_svm_transaction::svm_message::SVMMessage;
    use spl_token::solana_program::program_pack::Pack;
    use spl_token::state::Mint;
    use std::collections::HashSet;

    /// Replicates the downstream consumer gate: keep only the account slots the
    /// transaction marks writable at their positional index (see bob.rs and
    /// settle.rs, which both persist exactly `accounts[i]` where `is_writable(i)`).
    fn persisted_by_consumer(
        executed: &ExecutedTransaction,
        tx: &solana_sdk::transaction::SanitizedTransaction,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        executed
            .loaded_transaction
            .accounts
            .iter()
            .enumerate()
            .filter(|(i, _)| tx.is_writable(*i))
            .map(|(_, kv)| kv.clone())
            .collect()
    }

    /// Positional index of `key` inside the transaction's account_keys.
    fn index_of(tx: &solana_sdk::transaction::SanitizedTransaction, key: &Pubkey) -> usize {
        let account_keys = tx.account_keys();
        (0..account_keys.len())
            .find(|i| account_keys.get(*i).copied() == Some(*key))
            .expect("key present in account_keys")
    }

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
                Some(mint_allocation())
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
    ) -> (
        solana_sdk::transaction::SanitizedTransaction,
        Pubkey,
        Pubkey,
    ) {
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
        let sanitized = solana_sdk::transaction::SanitizedTransaction::try_from_legacy_transaction(
            tx,
            &HashSet::new(),
        )
        .unwrap();
        (sanitized, mint_a, mint_b)
    }

    // ─── Happy path ─────────────────────────────────────────────────────────

    /// A well-formed InitializeMint tx succeeds and produces a Mint account
    /// whose packed bytes carry the declared decimals and mint authority.
    #[test]
    fn test_spl_valid_initialize_mint() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(9, authority);
        let (tx, mint) = make_spl_tx_with_mint(spl_token::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        // Accounts mirror account_keys, and the mint lands at its own index.
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            tx.account_keys().len()
        );
        let mint_index = index_of(&tx, &mint);
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint);
        let mint_state = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint_state.decimals, 9);
        assert_eq!(mint_state.mint_authority, COption::Some(authority));
    }

    /// Returns the configured account for the configured pubkey, None otherwise.
    /// Used to prove non-mint mirror slots are filled from the callback.
    struct StubCbForPubkey {
        key: Pubkey,
        account: AccountSharedData,
    }
    impl solana_svm_callback::TransactionProcessingCallback for StubCbForPubkey {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
            if *pubkey == self.key {
                Some(self.account.clone())
            } else {
                None
            }
        }
        fn account_matches_owners(&self, _account: &Pubkey, _owners: &[Pubkey]) -> Option<usize> {
            None
        }
    }
    impl solana_svm_callback::InvokeContextCallback for StubCbForPubkey {}

    /// On the happy path `accounts` is a positional mirror of `account_keys()`:
    /// same length and same pubkey at every index.
    #[test]
    fn test_admin_accounts_mirror_account_keys() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, _mint) = make_spl_tx_with_mint(spl_token::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let account_keys = tx.account_keys();
        let accounts = &executed.loaded_transaction.accounts;
        assert_eq!(accounts.len(), account_keys.len());
        for (index, (pubkey, _)) in accounts.iter().enumerate() {
            assert_eq!(*pubkey, account_keys.get(index).copied().unwrap());
        }
    }

    /// The created mint sits at the same index its pubkey occupies in
    /// `account_keys()`, that slot is writable, and it unpacks to the declared
    /// decimals and authority.
    #[test]
    fn test_admin_mint_at_account_keys_index() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(4, authority);
        let (tx, mint) = make_spl_tx_with_mint(spl_token::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let mint_index = index_of(&tx, &mint);
        assert!(tx.is_writable(mint_index), "mint slot must be writable");
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint);
        let mint_state = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint_state.decimals, 4);
        assert_eq!(mint_state.mint_authority, COption::Some(authority));
    }

    /// Consumer writable-gate persists the mint and skips the read-only spl_token slot.
    #[test]
    fn test_admin_writable_gate_persists_only_mint_not_program() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, mint) = make_spl_tx_with_mint(spl_token::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let persisted = persisted_by_consumer(&executed, &tx);
        assert!(
            persisted.iter().any(|(k, _)| *k == mint),
            "writable mint must be persisted"
        );
        assert!(
            !persisted.iter().any(|(k, _)| *k == spl_token::id()),
            "read-only spl_token program must NOT be persisted"
        );
    }

    /// Two InitializeMint instructions in one tx: each mint lands at its own
    /// account_keys index, the mirror stays aligned, and the consumer gate
    /// persists both mints.
    #[test]
    fn test_admin_multi_mint_each_at_correct_index() {
        let authority = Pubkey::new_unique();
        let ix1 = valid_init_mint_data(2, authority);
        let ix2 = valid_init_mint_data(8, authority);
        let (tx, mint_a, mint_b) = make_two_instruction_spl_tx(ix1, ix2);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let account_keys = tx.account_keys();
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            account_keys.len()
        );

        let idx_a = index_of(&tx, &mint_a);
        let idx_b = index_of(&tx, &mint_b);
        assert_eq!(executed.loaded_transaction.accounts[idx_a].0, mint_a);
        assert_eq!(executed.loaded_transaction.accounts[idx_b].0, mint_b);
        assert_eq!(
            Mint::unpack(executed.loaded_transaction.accounts[idx_a].1.data())
                .unwrap()
                .decimals,
            2
        );
        assert_eq!(
            Mint::unpack(executed.loaded_transaction.accounts[idx_b].1.data())
                .unwrap()
                .decimals,
            8
        );

        let persisted = persisted_by_consumer(&executed, &tx);
        assert!(persisted.iter().any(|(k, _)| *k == mint_a));
        assert!(persisted.iter().any(|(k, _)| *k == mint_b));
    }

    /// Two InitializeMint instructions on the SAME mint in one tx: the second
    /// is rejected with AccountAlreadyInitialized, matching real SVM, and no
    /// accounts persist.
    #[test]
    fn test_admin_duplicate_mint_same_tx_rejected() {
        use solana_sdk::{
            instruction::{AccountMeta, Instruction},
            message::Message,
            signature::{Keypair, Signer},
            transaction::Transaction,
        };
        use std::collections::HashSet;

        let authority = Pubkey::new_unique();
        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        let ix = |data: Vec<u8>| Instruction {
            program_id: spl_token::id(),
            accounts: vec![
                AccountMeta::new(mint, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data,
        };
        let msg = Message::new(
            &[
                ix(valid_init_mint_data(6, authority)),
                ix(valid_init_mint_data(6, authority)),
            ],
            Some(&payer.pubkey()),
        );
        let legacy = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        let tx = solana_sdk::transaction::SanitizedTransaction::try_from_legacy_transaction(
            legacy,
            &HashSet::new(),
        )
        .unwrap();

        let executed = assert_executed_with_status(
            run_admin_vm(std::slice::from_ref(&tx)),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::AccountAlreadyInitialized,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// A non-mint slot (the fee payer) is filled from the callback: the mirror
    /// carries the exact account the callback returned, and it is persisted
    /// unchanged (proves slots are filled from the callback, not defaulted).
    #[test]
    fn test_admin_non_mint_slot_filled_from_callback() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, _mint) = make_spl_tx_with_mint(spl_token::id(), data);

        // The fee payer is account_keys[0]; stub the callback to return a
        // populated account for it.
        let fee_payer = tx.account_keys().get(0).copied().unwrap();
        let mut payer_account = AccountSharedData::new(777, 3, &Pubkey::new_unique());
        payer_account.set_data_from_slice(&[1, 2, 3]);
        let cb = StubCbForPubkey {
            key: fee_payer,
            account: payer_account.clone(),
        };

        let executed = assert_executed_with_status(
            run_admin_vm_with_cb(std::slice::from_ref(&tx), &cb),
            Ok(()),
        );

        let (pubkey, account) = &executed.loaded_transaction.accounts[0];
        assert_eq!(*pubkey, fee_payer);
        assert_eq!(
            *account, payer_account,
            "fee-payer slot must carry callback account"
        );

        let persisted = persisted_by_consumer(&executed, &tx);
        assert!(
            persisted
                .iter()
                .any(|(k, a)| *k == fee_payer && *a == payer_account),
            "fee-payer must be persisted unchanged"
        );
    }

    /// With an all-None callback, the read-only program slot defaults and is
    /// never persisted; no live account is tombstoned by the gate.
    #[test]
    fn test_admin_absent_non_mint_slot_defaults() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, _mint) = make_spl_tx_with_mint(spl_token::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let program_index = index_of(&tx, &spl_token::id());
        assert_eq!(
            executed.loaded_transaction.accounts[program_index].1,
            AccountSharedData::default(),
            "absent read-only slot must default"
        );
        assert!(
            !tx.is_writable(program_index),
            "spl_token program slot is read-only"
        );
        let persisted = persisted_by_consumer(&executed, &tx);
        assert!(
            !persisted.iter().any(|(k, _)| *k == spl_token::id()),
            "defaulted read-only slot must never be persisted"
        );
    }

    /// Documents the accepted admin side effect: when the fee payer is absent
    /// from the callback its writable slot defaults, so the consumer would
    /// tombstone it. This is a no-op in practice because every account key is
    /// preloaded before execution, so an absent key is absent in the DB too;
    /// the delete targets nothing and GC reaps the tombstone.
    #[test]
    fn test_admin_absent_fee_payer_slot_would_tombstone() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, _mint) = make_spl_tx_with_mint(spl_token::id(), data);

        // All-None callback: the writable fee payer at index 0 has no state.
        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let fee_payer = tx.account_keys().get(0).copied().unwrap();
        assert!(tx.is_writable(0), "fee payer slot is writable");
        let (pubkey, account) = &executed.loaded_transaction.accounts[0];
        assert_eq!(*pubkey, fee_payer);
        // lamports == 0 and empty data is exactly how the consumer detects a delete.
        assert!(account.lamports() == 0 && account.data().is_empty());
    }

    /// InitializeMint2 (type byte 20) shares InitializeMint's data layout and
    /// routes to the same handler: a well-formed tx succeeds and produces a
    /// Mint with the declared decimals and mint authority.
    #[test]
    fn test_spl_valid_initialize_mint2() {
        let authority = Pubkey::new_unique();
        let mut data = valid_init_mint_data(9, authority);
        data[0] = INSTRUCTION_INITIALIZE_MINT2;
        let (tx, mint) = make_spl_tx_with_mint(spl_token::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        // Accounts mirror account_keys, and the mint lands at its own index.
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            tx.account_keys().len()
        );
        let mint_index = index_of(&tx, &mint);
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint);
        let mint_state = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint_state.decimals, 9);
        assert_eq!(mint_state.mint_authority, COption::Some(authority));
    }

    /// The operator's JIT mint path, built exactly as
    /// `InitializeMintBuilder::instruction` does: no account at the target yet,
    /// admin as fee payer. Pins that flow against the target preconditions.
    #[test]
    fn test_operator_built_initialize_mint_for_missing_target_succeeds() {
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let decimals = 6;
        let ix = spl_token::instruction::initialize_mint(
            &spl_token::id(),
            &mint,
            &admin.pubkey(),
            Some(&admin.pubkey()),
            decimals,
        )
        .unwrap();
        let msg = Message::new(&[ix], Some(&admin.pubkey()));
        let tx = Transaction::new(&[&admin], msg, solana_sdk::hash::Hash::default());
        let sanitized =
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();

        let executed =
            assert_executed_with_status(run_admin_vm(std::slice::from_ref(&sanitized)), Ok(()));

        // Accounts mirror account_keys, so the mint lands at its own index.
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            sanitized.account_keys().len()
        );
        let mint_index = index_of(&sanitized, &mint);
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint, "the mint meta must resolve to the mint key");
        let mint_state = Mint::unpack(account.data()).unwrap();
        assert_eq!(mint_state.decimals, decimals);
        assert_eq!(mint_state.mint_authority, COption::Some(admin.pubkey()));
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

    /// A read-only mint meta is rejected even with no account at the target.
    /// BOB gates on the account's position in the returned vec, not the mint's
    /// message index, so this has to be the VM's own check.
    #[test]
    fn test_readonly_mint_meta_returns_readonly_data_modified() {
        let payer = Keypair::new();
        let target = Pubkey::new_unique();
        let ix = Instruction {
            program_id: spl_token::id(),
            accounts: vec![AccountMeta::new_readonly(target, false)],
            data: valid_init_mint_data(6, Pubkey::new_unique()),
        };
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        let sanitized =
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();

        let executed = assert_executed_with_status(
            run_admin_vm(&[sanitized]),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::ReadonlyDataModified,
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

        let executed = assert_executed_with_status(
            run_admin_vm_with_cb(std::slice::from_ref(&tx), &cb),
            Ok(()),
        );
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            tx.account_keys().len()
        );
        let mint_index = index_of(&tx, &mint_pubkey);
        assert_eq!(
            executed.loaded_transaction.accounts[mint_index].0,
            mint_pubkey
        );
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

        let (tx, _mint_a, _mint_b) = make_two_instruction_spl_tx(valid, bad);
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

        let (tx, _mint_a, _mint_b) = make_two_instruction_spl_tx(bad, valid);
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

    // ─── Target preconditions ───────────────────────────────────────────────

    /// Token-owned, Mint::LEN, zeroed: the only pre-existing state
    /// InitializeMint may write over.
    fn mint_allocation() -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports: 1,
            data: vec![0u8; Mint::LEN],
            owner: spl_token::id(),
            executable: false,
            rent_epoch: 0,
        })
    }

    /// No account at the target is legal: the private channel allocates and
    /// initializes in one step, which the operator's JIT path relies on.
    #[test]
    fn test_check_target_missing_is_allowed() {
        assert_eq!(AdminVm::check_initialize_mint_target(None, true), Ok(()));
    }

    #[test]
    fn test_check_target_uninitialized_allocation_is_allowed() {
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&mint_allocation()), true),
            Ok(())
        );
    }

    #[test]
    fn test_check_target_not_writable_rejected() {
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&mint_allocation()), false),
            Err(InstructionError::ReadonlyDataModified)
        );
    }

    #[test]
    fn test_check_target_executable_rejected() {
        let mut account = mint_allocation();
        account.set_executable(true);
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::ExecutableDataModified)
        );
    }

    #[test]
    fn test_check_target_foreign_owner_rejected() {
        let mut account = mint_allocation();
        account.set_owner(solana_sdk::system_program::id());
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::ExternalAccountDataModified)
        );
    }

    /// A token-owned account of the wrong size, e.g. an SPL token account.
    #[test]
    fn test_check_target_wrong_size_rejected() {
        let account = AccountSharedData::new(1, spl_token::state::Account::LEN, &spl_token::id());
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::InvalidAccountData)
        );
    }

    #[test]
    fn test_check_target_live_mint_rejected() {
        let account = AdminVm::create_mint_account(6, &Pubkey::new_unique().to_bytes(), None);
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::AccountAlreadyInitialized)
        );
    }
}
