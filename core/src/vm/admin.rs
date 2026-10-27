use std::collections::HashMap;

use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    instruction::InstructionError,
    program_error::ProgramError,
    pubkey::Pubkey,
    transaction::TransactionError,
};
use solana_svm::{
    account_loader::{LoadedTransaction, TransactionCheckResult},
    transaction_error_metrics::TransactionErrorMetrics,
    transaction_execution_result::{
        AccountsDeltas, ExecutedTransaction, TransactionExecutionDetails,
    },
    transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
    transaction_processor::{
        LoadAndExecuteSanitizedTransactionsOutput, TransactionProcessingConfig,
        TransactionProcessingEnvironment,
    },
};
use solana_svm_callback::TransactionProcessingCallback;
use solana_svm_timings::ExecuteTimings;
use solana_svm_transaction::svm_transaction::SVMTransaction;
use spl_token_2022::{
    extension::{
        permanent_delegate::PermanentDelegate, BaseStateWithExtensionsMut, ExtensionType,
        StateWithExtensions, StateWithExtensionsMut,
    },
    state::Mint,
};
use tracing::{debug, warn};

const TOKEN_2022_ID: Pubkey = spl_token_2022::ID;

// Token-2022 instruction types
const INSTRUCTION_INITIALIZE_MINT: u8 = 0;
const INSTRUCTION_INITIALIZE_MINT2: u8 = 20;
const INSTRUCTION_INITIALIZE_PERMANENT_DELEGATE: u8 = 35;

/// Size of a channel mint: base mint, account type and the PermanentDelegate extension.
pub(crate) const CHANNEL_MINT_LEN: usize = 202;

/// Executes admin transactions: it creates Token-2022 channel mints, each carrying
/// the configured permanent delegate. With no delegate configured it creates nothing.
#[derive(Default)]
pub struct AdminVm {
    delegate: Option<Pubkey>,
}

impl AdminVm {
    pub fn new(delegate: Option<Pubkey>) -> Self {
        Self { delegate }
    }

    /// Builds the account the real program leaves after `[InitializePermanentDelegate,
    /// InitializeMint2]`, using the library's own layout code.
    fn create_mint_account(
        decimals: u8,
        mint_authority: &Pubkey,
        freeze_authority: Option<Pubkey>,
        delegate: &Pubkey,
    ) -> AccountSharedData {
        let len =
            ExtensionType::try_calculate_account_len::<Mint>(&[ExtensionType::PermanentDelegate])
                .expect("mint length with one extension");
        debug_assert_eq!(len, CHANNEL_MINT_LEN);
        let mut mint_data = vec![0u8; len];
        let mut mint = StateWithExtensionsMut::<Mint>::unpack_uninitialized(&mut mint_data)
            .expect("zeroed mint data unpacks");
        mint.init_extension::<PermanentDelegate>(true)
            .expect("extension fits the allocation")
            .delegate = Some(*delegate)
            .try_into()
            .expect("delegate is not the zero key");
        mint.base = Mint {
            mint_authority: Some(*mint_authority).into(),
            supply: 0,
            decimals,
            is_initialized: true,
            freeze_authority: freeze_authority.into(),
        };
        mint.pack_base();
        mint.init_account_type().expect("account type fits");

        // lamports=1 so the SVM's AccountLoader cache doesn't treat the mint
        // as deallocated on subsequent loads within the same batch. See the
        // equivalent comment on system_program in bob.rs::BOB::new.
        let mut account = AccountSharedData::new(1, len, &TOKEN_2022_ID);
        account.set_data_from_slice(&mint_data);
        account
    }

    /// Preconditions canonical Token-2022 execution would enforce before an
    /// `InitializeMint` could write the mint: the SVM's account rules
    /// (writable, non-executable, token-owned) and the processor's own
    /// (exactly `CHANNEL_MINT_LEN` bytes, not already initialized). Synthesizing the
    /// post-state skips both, so without these the VM would replace any
    /// account an admin transaction happens to reference.
    ///
    /// A missing target stays legal: the private channel allocates and
    /// initializes in one step so mint addresses mirror mainnet.
    ///
    /// Rent exemption is deliberately not checked. Execution is gasless
    /// (the rent rate is zero) and `create_mint_account`
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
        if *existing.owner() != TOKEN_2022_ID {
            return Err(InstructionError::ExternalAccountDataModified);
        }
        if existing.data().len() != CHANNEL_MINT_LEN {
            return Err(InstructionError::InvalidAccountData);
        }
        // `unpack` succeeds only on an initialized base and reports an allocation as uninitialized.
        match StateWithExtensions::<Mint>::unpack(existing.data()) {
            Ok(_) => Err(InstructionError::AccountAlreadyInitialized),
            Err(ProgramError::UninitializedAccount) => Ok(()),
            Err(_) => Err(InstructionError::InvalidAccountData),
        }
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
                // Upstream keeps the deltas only for a transaction that
                // succeeded. This VM only ever creates mints at their exact
                // size and uninitialises nothing, so both deltas are zero.
                accounts_deltas: status.is_ok().then_some(AccountsDeltas {
                    accounts_resize_delta: 0,
                    accounts_uninitialized_size: 0,
                }),
                status,
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }
    }

    /// Process each tx's instructions in order. On the first failing instruction,
    /// short-circuit via `break 'tx` with a failed `ExecutedTransaction`
    ///
    /// Each `InitializeMint`/`InitializeMint2` must directly follow an
    /// `InitializePermanentDelegate` on the same mint naming the configured delegate,
    /// so every mint this VM creates carries the seize authority.
    ///
    /// Failure mapping:
    /// - non-Token-2022 program id                  -> InvalidInstructionData
    /// - empty instruction data                     -> InvalidInstructionData
    /// - unsupported Token-2022 instruction type    -> InvalidInstructionData
    /// - InitializePermanentDelegate data < 33 bytes -> InvalidInstructionData
    /// - delegate unset or not the configured key   -> InvalidArgument
    /// - InitializeMint without the delegate ix before it on the same mint -> InvalidArgument
    /// - delegate ix not followed by its InitializeMint -> InvalidArgument
    /// - either native mint                         -> InvalidArgument
    /// - InitializeMint data < 35 bytes             -> InvalidAccountData
    /// - InitializeMint empty accounts              -> InvalidAccountData
    /// - freeze authority tag=1, <32 trailing       -> InvalidAccountData
    /// - freeze authority tag not 0 or 1            -> InvalidInstructionData
    /// - mint index out of `account_keys`           -> NotEnoughAccountKeys
    /// - mint read-only in the message              -> ReadonlyDataModified
    /// - existing target executable                 -> ExecutableDataModified
    /// - existing target not Token-2022-owned       -> ExternalAccountDataModified
    /// - existing target not `CHANNEL_MINT_LEN` bytes -> InvalidAccountData
    /// - mint already initialized                   -> AccountAlreadyInitialized
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
                let fail = |ix_index: u8, err: InstructionError| {
                    Self::create_executed_transaction(
                        Err(TransactionError::InstructionError(ix_index, err)),
                        vec![],
                    )
                };
                // Mint account index and ix index of a delegate ix still waiting for its mint init.
                let mut pending_delegate: Option<(usize, u8)> = None;
                for (ix_index, (program_id, instruction)) in
                    tx.program_instructions_iter().enumerate()
                {
                    // Solana's InstructionError index is a u8;
                    let ix_idx_u8 = u8::try_from(ix_index).unwrap_or(u8::MAX);

                    if *program_id != TOKEN_2022_ID {
                        warn!("[admin-vm] Unsupported program ID: {}", program_id);
                        break 'tx fail(ix_idx_u8, InstructionError::InvalidInstructionData);
                    }

                    let Some(&instruction_type) = instruction.data.first() else {
                        warn!("[admin-vm] Token-2022 instruction has empty data");
                        break 'tx fail(ix_idx_u8, InstructionError::InvalidInstructionData);
                    };

                    match instruction_type {
                        INSTRUCTION_INITIALIZE_PERMANENT_DELEGATE => {
                            if let Some((_, delegate_ix)) = pending_delegate {
                                debug!("[admin-vm] delegate ix not followed by its mint init");
                                break 'tx fail(delegate_ix, InstructionError::InvalidArgument);
                            }
                            match self.check_permanent_delegate(instruction) {
                                Ok(mint_index) => pending_delegate = Some((mint_index, ix_idx_u8)),
                                Err(err) => break 'tx fail(ix_idx_u8, err),
                            }
                        }
                        // InitializeMint2 shares InitializeMint's data layout;
                        // only its account list differs (no rent sysvar), which
                        // process_initialize_mint does not read.
                        INSTRUCTION_INITIALIZE_MINT | INSTRUCTION_INITIALIZE_MINT2 => {
                            let delegated_mint = pending_delegate.take().map(|(index, _)| index);
                            if delegated_mint.is_none()
                                || delegated_mint
                                    != instruction.accounts.first().map(|&i| i as usize)
                            {
                                debug!(
                                    "[admin-vm] InitializeMint without the delegate ix on its mint"
                                );
                                break 'tx fail(ix_idx_u8, InstructionError::InvalidArgument);
                            }
                            // Checked above: the delegate ix stored the configured key.
                            let Some(delegate) = self.delegate else {
                                break 'tx fail(ix_idx_u8, InstructionError::InvalidArgument);
                            };
                            match Self::process_initialize_mint(
                                callbacks,
                                tx,
                                instruction,
                                &delegate,
                            ) {
                                Ok((mint_index, pubkey, account)) => {
                                    // Reject a second init of the same mint in one tx,
                                    // like real SVM, instead of overwriting the first.
                                    if created_mints.iter().any(|(i, _, _)| *i == mint_index) {
                                        break 'tx fail(
                                            ix_idx_u8,
                                            InstructionError::AccountAlreadyInitialized,
                                        );
                                    }
                                    created_mints.push((mint_index, pubkey, account))
                                }
                                Err(err) => break 'tx fail(ix_idx_u8, err),
                            }
                        }
                        _ => {
                            warn!(
                                "[admin-vm] Unsupported Token-2022 instruction type: {}",
                                instruction_type
                            );
                            break 'tx fail(ix_idx_u8, InstructionError::InvalidInstructionData);
                        }
                    }
                }
                if let Some((_, delegate_ix)) = pending_delegate {
                    debug!("[admin-vm] delegate ix not followed by its mint init");
                    break 'tx fail(delegate_ix, InstructionError::InvalidArgument);
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
                            // The slot beside the account is not tracked here.
                            callbacks
                                .get_account_shared_data(&key)
                                .map(|(account, _slot)| account)
                                .unwrap_or_default()
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

    /// Validates `InitializePermanentDelegate` (`[35, delegate(32)]`, mint at
    /// `accounts[0]`) against the configured delegate and returns the mint's index.
    fn check_permanent_delegate(
        &self,
        instruction: solana_svm_transaction::instruction::SVMInstruction,
    ) -> Result<usize, InstructionError> {
        let (Some(&mint_index), Some(delegate)) =
            (instruction.accounts.first(), instruction.data.get(1..33))
        else {
            debug!("[admin-vm] InitializePermanentDelegate: malformed");
            return Err(InstructionError::InvalidInstructionData);
        };
        if self.delegate.is_none_or(|d| d.as_ref() != delegate) {
            warn!("[admin-vm] InitializePermanentDelegate: delegate is not the configured key");
            return Err(InstructionError::InvalidArgument);
        }
        Ok(mint_index as usize)
    }

    /// Validate and process a single Token-2022 `InitializeMint` instruction.
    /// Returns the (account_keys index, pubkey, Mint account) on success, so the
    /// caller can place the mint at its true slot in the account_keys mirror, or
    /// the appropriate `InstructionError` on any validation failure.
    ///
    /// Token-2022 `InitializeMint` wire layout, the same as SPL Token's
    /// (see `spl_token_2022::instruction::TokenInstruction::pack`):
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
        delegate: &Pubkey,
    ) -> Result<(usize, Pubkey, AccountSharedData), InstructionError> {
        // Minimum payload: instruction must reference the mint as an account,
        // and data must span discriminator + decimals + mint_authority + the
        // freeze_authority COption tag at byte 34. `TokenInstruction::pack`
        // always emits the tag, so any shorter payload is malformed.
        if instruction.accounts.is_empty() || instruction.data.len() < 35 {
            debug!("[admin-vm] InitializeMint: malformed (len or accounts)");
            return Err(InstructionError::InvalidAccountData);
        }

        // Freeze-authority COption: byte 34 is the tag (0 = None, 1 = Some), and
        // Token-2022 rejects any other. When Some, bytes 35..67 carry the 32-byte pubkey;
        // reject if truncated.
        let freeze_authority = match instruction.data[34] {
            0 => None,
            1 => {
                if instruction.data.len() < 67 {
                    debug!("[admin-vm] InitializeMint: truncated freeze authority");
                    return Err(InstructionError::InvalidAccountData);
                }
                Some(Pubkey::new_from_array(
                    instruction.data[35..67]
                        .try_into()
                        .expect("slice is 32 bytes"),
                ))
            }
            _ => {
                debug!("[admin-vm] InitializeMint: invalid freeze authority tag");
                return Err(InstructionError::InvalidInstructionData);
            }
        };

        // Resolve the mint pubkey: InitializeMint places the mint at
        // `instruction.accounts[0]`
        let account_keys = tx.account_keys();
        let mint_index = instruction.accounts[0] as usize;
        let Some(mint_pubkey) = account_keys.get(mint_index).copied() else {
            // Deprecated upstream, kept so existing rows and clients see the same error.
            #[allow(deprecated)]
            return Err(InstructionError::NotEnoughAccountKeys);
        };

        // Neither native mint may become a channel mint. Checked before the account
        // read so no target state can bypass it. InvalidArgument is an error the
        // operator gives no special meaning.
        if mint_pubkey == spl_token::native_mint::id()
            || mint_pubkey == spl_token_2022::native_mint::id()
        {
            debug!(
                "[admin-vm] InitializeMint: native mint {} refused",
                mint_pubkey
            );
            return Err(InstructionError::InvalidArgument);
        }

        // The slot beside the account is not tracked here.
        let existing = callbacks
            .get_account_shared_data(&mint_pubkey)
            .map(|(account, _slot)| account);
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
        let mint_authority = Pubkey::new_from_array(
            instruction.data[2..34]
                .try_into()
                .expect("slice is 32 bytes"),
        );
        let mint_account =
            Self::create_mint_account(decimals, &mint_authority, freeze_authority, delegate);
        Ok((mint_index, mint_pubkey, mint_account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::CHANNEL_SLOT;
    use crate::t22_fixtures::{
        golden_authority, golden_mint, GOLDEN_DECIMALS, GOLDEN_DELEGATE, GOLDEN_T22_MINT,
    };
    use solana_sdk::account::{Account, ReadableAccount, WritableAccount};
    use solana_sdk::clock::Slot;
    use solana_sdk::instruction::{AccountMeta, Instruction};
    use solana_sdk::message::Message;
    use solana_sdk::program_error::ProgramError;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::transaction::{SanitizedTransaction, Transaction};
    use solana_svm_transaction::svm_message::SVMMessage;
    use spl_token_2022::extension::permanent_delegate::get_permanent_delegate;
    use std::collections::HashSet;

    /// Delegate the test VM is configured with.
    const TEST_DELEGATE: Pubkey = GOLDEN_DELEGATE;

    /// Decodes a channel mint's base state.
    fn unpack_mint(data: &[u8]) -> Mint {
        StateWithExtensions::<Mint>::unpack(data).unwrap().base
    }

    /// `[35, delegate]` for `mint`, as `spl_token_2022::instruction::initialize_permanent_delegate` packs it.
    fn delegate_ix(mint: Pubkey, delegate: Pubkey) -> Instruction {
        spl_token_2022::instruction::initialize_permanent_delegate(
            &spl_token_2022::id(),
            &mint,
            &delegate,
        )
        .unwrap()
    }

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

    /// `create_mint_account` packs a Token-2022 mint with the given decimals,
    /// authority and delegate and no freeze authority.
    #[test]
    fn test_create_mint_account_roundtrip() {
        let authority = Pubkey::new_unique();
        let account = AdminVm::create_mint_account(6, &authority, None, &TEST_DELEGATE);

        assert_eq!(*account.owner(), spl_token_2022::id());
        assert_eq!(account.data().len(), CHANNEL_MINT_LEN);
        let state = StateWithExtensions::<Mint>::unpack(account.data()).unwrap();
        assert_eq!(state.base.decimals, 6);
        assert!(state.base.is_initialized);
        assert_eq!(state.base.supply, 0);
        assert_eq!(state.base.mint_authority, Some(authority).into());
        assert_eq!(state.base.freeze_authority, None.into());
        assert_eq!(get_permanent_delegate(&state), Some(TEST_DELEGATE));
    }

    /// `create_mint_account` sets `freeze_authority` to `Some` when one is supplied.
    #[test]
    fn test_initialize_mint_with_freeze_authority() {
        let authority = Pubkey::new_unique();
        let freeze = Pubkey::new_unique();
        let account = AdminVm::create_mint_account(9, &authority, Some(freeze), &TEST_DELEGATE);

        let mint = unpack_mint(account.data());
        assert_eq!(mint.decimals, 9);
        assert_eq!(mint.freeze_authority, Some(freeze).into());
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
        fn get_account_shared_data(&self, _pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
            None
        }
    }
    impl solana_svm_callback::InvokeContextCallback for DummyCb {}

    /// Returns a pre-initialized channel mint for the configured pubkey, None for anything else.
    struct StubCbWithInitializedMint {
        mint: Pubkey,
    }
    impl solana_svm_callback::TransactionProcessingCallback for StubCbWithInitializedMint {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
            if *pubkey == self.mint {
                Some((
                    AdminVm::create_mint_account(6, &Pubkey::new_unique(), None, &TEST_DELEGATE),
                    CHANNEL_SLOT,
                ))
            } else {
                None
            }
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
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
            if *pubkey == self.mint {
                Some((mint_allocation(), CHANNEL_SLOT))
            } else {
                None
            }
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
        run_admin_vm_as(AdminVm::new(Some(TEST_DELEGATE)), txs, cb)
    }

    fn run_admin_vm_as<CB: solana_svm_callback::TransactionProcessingCallback>(
        vm: AdminVm,
        txs: &[solana_sdk::transaction::SanitizedTransaction],
        cb: &CB,
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        let check_results = crate::processor::get_transaction_check_results(txs.len());
        // The admin path ignores the environment, so a mock one is enough.
        let env = solana_svm::transaction_processor::TransactionProcessingEnvironment {
            blockhash: solana_sdk::hash::Hash::default(),
            blockhash_lamports_per_signature: 0,
            alpenglow_migration_succeeded: false,
            epoch_total_stake: 0,
            feature_set: solana_svm_feature_set::SVMFeatureSet::all_enabled(),
            program_runtime_environments:
                solana_program_runtime::loaded_programs::ProgramRuntimeEnvironments::mock(),
            rent: solana_sdk::rent::Rent::free(),
        };
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

    /// Build a SanitizedTransaction with `[InitializePermanentDelegate(TEST_DELEGATE), data]`
    /// on one mint under the given program_id, with the mint as the FIRST account
    /// meta of the second ix (as `InitializeMint` expects). The ix under test is at
    /// index 1. Returns the tx and the mint pubkey so tests can stub the callback.
    fn make_spl_tx_with_mint(
        program_id: Pubkey,
        data: Vec<u8>,
    ) -> (solana_sdk::transaction::SanitizedTransaction, Pubkey) {
        let payer = Keypair::new();
        let mint = Pubkey::new_unique();
        let mut delegate = delegate_ix(mint, TEST_DELEGATE);
        delegate.program_id = program_id;
        // Mint first: InitializeMint semantics, accounts[0] is the mint.
        let account_metas = vec![
            AccountMeta::new(mint, false),
            AccountMeta::new(payer.pubkey(), true),
        ];
        let ix = Instruction {
            program_id,
            accounts: account_metas,
            data,
        };
        let msg = Message::new(&[delegate, ix], Some(&payer.pubkey()));
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

    /// Build a SanitizedTransaction carrying TWO delegate + init pairs so we can prove
    /// multi-instruction atomicity (real-SVM semantics: fails at first bad ix).
    /// The inits sit at ix indexes 1 and 3.
    fn make_two_instruction_spl_tx(
        ix1_data: Vec<u8>,
        ix2_data: Vec<u8>,
    ) -> (
        solana_sdk::transaction::SanitizedTransaction,
        Pubkey,
        Pubkey,
    ) {
        make_two_instruction_spl_tx_with(ix1_data, ix2_data, Pubkey::new_unique())
    }

    /// Same as `make_two_instruction_spl_tx`, with the second mint chosen by the caller.
    fn make_two_instruction_spl_tx_with(
        ix1_data: Vec<u8>,
        ix2_data: Vec<u8>,
        mint_b: Pubkey,
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
        let ix1 = Instruction {
            program_id: spl_token_2022::id(),
            accounts: vec![
                AccountMeta::new(mint_a, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data: ix1_data,
        };
        let ix2 = Instruction {
            program_id: spl_token_2022::id(),
            accounts: vec![
                AccountMeta::new(mint_b, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data: ix2_data,
        };
        let msg = Message::new(
            &[
                delegate_ix(mint_a, TEST_DELEGATE),
                ix1,
                delegate_ix(mint_b, TEST_DELEGATE),
                ix2,
            ],
            Some(&payer.pubkey()),
        );
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
        let (tx, mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        // Accounts mirror account_keys, and the mint lands at its own index.
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            tx.account_keys().len()
        );
        let mint_index = index_of(&tx, &mint);
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint);
        let mint_state = unpack_mint(account.data());
        assert_eq!(mint_state.decimals, 9);
        assert_eq!(mint_state.mint_authority, Some(authority).into());
    }

    /// Returns the configured account for the configured pubkey, None otherwise.
    /// Used to prove non-mint mirror slots are filled from the callback.
    struct StubCbForPubkey {
        key: Pubkey,
        account: AccountSharedData,
    }
    impl solana_svm_callback::TransactionProcessingCallback for StubCbForPubkey {
        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
            if *pubkey == self.key {
                Some((self.account.clone(), CHANNEL_SLOT))
            } else {
                None
            }
        }
    }
    impl solana_svm_callback::InvokeContextCallback for StubCbForPubkey {}

    /// On the happy path `accounts` is a positional mirror of `account_keys()`:
    /// same length and same pubkey at every index.
    #[test]
    fn test_admin_accounts_mirror_account_keys() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, _mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

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
        let (tx, mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let mint_index = index_of(&tx, &mint);
        assert!(tx.is_writable(mint_index), "mint slot must be writable");
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint);
        let mint_state = unpack_mint(account.data());
        assert_eq!(mint_state.decimals, 4);
        assert_eq!(mint_state.mint_authority, Some(authority).into());
    }

    /// Consumer writable-gate persists the mint and skips the read-only spl_token slot.
    #[test]
    fn test_admin_writable_gate_persists_only_mint_not_program() {
        let authority = Pubkey::new_unique();
        let data = valid_init_mint_data(6, authority);
        let (tx, mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let persisted = persisted_by_consumer(&executed, &tx);
        assert!(
            persisted.iter().any(|(k, _)| *k == mint),
            "writable mint must be persisted"
        );
        assert!(
            !persisted.iter().any(|(k, _)| *k == spl_token_2022::id()),
            "read-only Token-2022 program must NOT be persisted"
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
            unpack_mint(executed.loaded_transaction.accounts[idx_a].1.data()).decimals,
            2
        );
        assert_eq!(
            unpack_mint(executed.loaded_transaction.accounts[idx_b].1.data()).decimals,
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
            program_id: spl_token_2022::id(),
            accounts: vec![
                AccountMeta::new(mint, false),
                AccountMeta::new(payer.pubkey(), true),
            ],
            data,
        };
        let msg = Message::new(
            &[
                delegate_ix(mint, TEST_DELEGATE),
                ix(valid_init_mint_data(6, authority)),
                delegate_ix(mint, TEST_DELEGATE),
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
                3,
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
        let (tx, _mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

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
        let (tx, _mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        let program_index = index_of(&tx, &spl_token_2022::id());
        assert_eq!(
            executed.loaded_transaction.accounts[program_index].1,
            AccountSharedData::default(),
            "absent read-only slot must default"
        );
        assert!(
            !tx.is_writable(program_index),
            "Token-2022 program slot is read-only"
        );
        let persisted = persisted_by_consumer(&executed, &tx);
        assert!(
            !persisted.iter().any(|(k, _)| *k == spl_token_2022::id()),
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
        let (tx, _mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

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
        let (tx, mint) = make_spl_tx_with_mint(spl_token_2022::id(), data);

        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));

        // Accounts mirror account_keys, and the mint lands at its own index.
        assert_eq!(
            executed.loaded_transaction.accounts.len(),
            tx.account_keys().len()
        );
        let mint_index = index_of(&tx, &mint);
        let (pubkey, account) = &executed.loaded_transaction.accounts[mint_index];
        assert_eq!(*pubkey, mint);
        let mint_state = unpack_mint(account.data());
        assert_eq!(mint_state.decimals, 9);
        assert_eq!(mint_state.mint_authority, Some(authority).into());
    }

    /// The operator's JIT mint path, built exactly as
    /// `InitializeMintBuilder::instruction` does: no account at the target yet,
    /// admin as fee payer. Pins that flow against the target preconditions.
    #[test]
    fn test_operator_built_initialize_mint_for_missing_target_succeeds() {
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let decimals = 6;
        let sanitized = operator_init_mint_tx(
            &admin,
            &mint,
            decimals,
            spl_token_2022::instruction::initialize_mint,
        );

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
        let mint_state = unpack_mint(account.data());
        assert_eq!(mint_state.decimals, decimals);
        assert_eq!(mint_state.mint_authority, Some(admin.pubkey()).into());
        assert_eq!(mint_state.freeze_authority, Some(admin.pubkey()).into());
    }

    /// Signature shared by Token-2022's `initialize_mint` and `initialize_mint2`.
    type InitMintIx =
        fn(&Pubkey, &Pubkey, &Pubkey, Option<&Pubkey>, u8) -> Result<Instruction, ProgramError>;

    /// Admin-signed `[InitializePermanentDelegate, InitializeMint]` tx built with
    /// Token-2022's own instruction builders, as the operator does. `init` picks the variant.
    fn operator_init_mint_tx(
        admin: &Keypair,
        mint: &Pubkey,
        decimals: u8,
        init: InitMintIx,
    ) -> SanitizedTransaction {
        let ix = init(
            &spl_token_2022::id(),
            mint,
            &admin.pubkey(),
            Some(&admin.pubkey()),
            decimals,
        )
        .unwrap();
        let msg = Message::new(
            &[delegate_ix(*mint, TEST_DELEGATE), ix],
            Some(&admin.pubkey()),
        );
        let tx = Transaction::new(&[admin], msg, solana_sdk::hash::Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap()
    }

    /// Both native mints are refused for both variants whatever the callback
    /// holds at their address, and nothing persists.
    #[test]
    fn native_mint_initialize_is_rejected() {
        for native in [
            spl_token::native_mint::id(),
            spl_token_2022::native_mint::id(),
        ] {
            assert_native_mint_refused(native);
        }
    }

    fn assert_native_mint_refused(native: Pubkey) {
        let variants: [(&str, InitMintIx); 2] = [
            (
                "initialize_mint",
                spl_token_2022::instruction::initialize_mint,
            ),
            (
                "initialize_mint2",
                spl_token_2022::instruction::initialize_mint2,
            ),
        ];
        let states: [(&str, Option<AccountSharedData>); 4] = [
            ("absent", None),
            ("allocated", Some(mint_allocation())),
            (
                "live mint",
                Some(AdminVm::create_mint_account(
                    9,
                    &Pubkey::new_unique(),
                    None,
                    &TEST_DELEGATE,
                )),
            ),
            (
                "system-owned lamports",
                Some(AccountSharedData::new(
                    1,
                    0,
                    &solana_sdk_ids::system_program::id(),
                )),
            ),
        ];

        for (variant, init) in variants {
            for (state, account) in &states {
                let tx = operator_init_mint_tx(&Keypair::new(), &native, 9, init);
                let output = match account {
                    None => run_admin_vm(std::slice::from_ref(&tx)),
                    Some(account) => run_admin_vm_with_cb(
                        std::slice::from_ref(&tx),
                        &StubCbForPubkey {
                            key: native,
                            account: account.clone(),
                        },
                    ),
                };
                let result = output.processing_results.into_iter().next().unwrap();
                let Ok(ProcessedTransaction::Executed(executed)) = result else {
                    panic!("{variant}/{state}: expected Executed");
                };
                assert_eq!(
                    executed.execution_details.status,
                    Err(TransactionError::InstructionError(
                        1,
                        InstructionError::InvalidArgument
                    )),
                    "{variant}/{state}"
                );
                assert!(
                    executed.loaded_transaction.accounts.is_empty(),
                    "{variant}/{state}: nothing may persist"
                );
            }
        }
    }

    /// A valid mint followed by the native mint fails the whole tx at the
    /// second instruction, so the valid mint does not persist either.
    #[test]
    fn valid_mint_then_native_fails_whole_tx() {
        let valid = valid_init_mint_data(6, Pubkey::new_unique());
        let native = valid_init_mint_data(9, Pubkey::new_unique());

        let (tx, _mint_a, _native) =
            make_two_instruction_spl_tx_with(valid, native, spl_token::native_mint::id());
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                3,
                InstructionError::InvalidArgument,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    // ─── Failure paths ──────────────────────────────────────────────────────

    /// An admin-routed tx whose program is not Token-2022 surfaces
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

    /// A Token-2022 instruction with empty `data` surfaces
    /// `InvalidInstructionData` at its own index.
    #[test]
    fn test_spl_empty_data_returns_invalid_instruction_data() {
        let tx = make_spl_tx(spl_token_2022::id(), &[1], vec![]);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
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
        let tx = make_spl_tx(spl_token_2022::id(), &[1], data);

        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::InvalidAccountData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// A Token-2022 instruction whose first byte is a non-InitializeMint
    /// discriminator (here Transfer = 3) surfaces `InvalidInstructionData`.
    #[test]
    fn test_spl_unsupported_instruction_type_returns_invalid_instruction_data() {
        let data = vec![3u8; 10];
        let tx = make_spl_tx(spl_token_2022::id(), &[1], data);

        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
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
        let (tx, mint_pubkey) = make_spl_tx_with_mint(spl_token_2022::id(), data);
        let cb = StubCbWithInitializedMint { mint: mint_pubkey };

        let executed = assert_executed_with_status(
            run_admin_vm_with_cb(&[tx], &cb),
            Err(TransactionError::InstructionError(
                1,
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
            program_id: spl_token_2022::id(),
            accounts: vec![AccountMeta::new_readonly(target, false)],
            data: valid_init_mint_data(6, Pubkey::new_unique()),
        };
        let mut delegate = delegate_ix(target, TEST_DELEGATE);
        delegate.accounts[0].is_writable = false;
        let msg = Message::new(&[delegate, ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        let sanitized =
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();

        let executed = assert_executed_with_status(
            run_admin_vm(&[sanitized]),
            Err(TransactionError::InstructionError(
                1,
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
        let (tx, mint_pubkey) = make_spl_tx_with_mint(spl_token_2022::id(), data);
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

        let tx = make_spl_tx(spl_token_2022::id(), &[1], data);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::InvalidAccountData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// SPL Token accepts only 0 and 1 as the freeze-authority tag. Any other
    /// value must be rejected, not read as None and the mint created.
    #[test]
    fn test_invalid_freeze_authority_tag_returns_invalid_instruction_data() {
        let mut data = valid_init_mint_data(6, Pubkey::new_unique());
        data[34] = 2;

        let tx = make_spl_tx(spl_token_2022::id(), &[1], data);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
                InstructionError::InvalidInstructionData,
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
                3,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(
            executed.loaded_transaction.accounts.is_empty(),
            "multi-instruction atomicity violated: partial accounts leaked"
        );
    }

    /// If the first init fails, subsequent instructions are not processed:
    /// the reported error index is its own (1) and no accounts persist.
    #[test]
    fn test_multi_instruction_second_valid_not_reached() {
        let authority = Pubkey::new_unique();
        let bad = vec![3u8; 10];
        let valid = valid_init_mint_data(6, authority);

        let (tx, _mint_a, _mint_b) = make_two_instruction_spl_tx(bad, valid);
        let executed = assert_executed_with_status(
            run_admin_vm(&[tx]),
            Err(TransactionError::InstructionError(
                1,
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
            program_id: spl_token_2022::id(),
            accounts: vec![AccountMeta::new(payer.pubkey(), true)],
            data,
        };
        let msg = Message::new(
            &[delegate_ix(payer.pubkey(), TEST_DELEGATE), ix],
            Some(&payer.pubkey()),
        );
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

    /// Token-2022-owned, CHANNEL_MINT_LEN, zeroed: the only pre-existing state
    /// InitializeMint may write over.
    fn mint_allocation() -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports: 1,
            data: vec![0u8; CHANNEL_MINT_LEN],
            owner: spl_token_2022::id(),
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
        account.set_owner(solana_sdk_ids::system_program::id());
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::ExternalAccountDataModified)
        );
    }

    /// A token-owned account of the wrong size, e.g. a token account or a bare mint.
    #[test]
    fn test_check_target_wrong_size_rejected() {
        for len in [82, 165, 170] {
            let account = AccountSharedData::new(1, len, &spl_token_2022::id());
            assert_eq!(
                AdminVm::check_initialize_mint_target(Some(&account), true),
                Err(InstructionError::InvalidAccountData),
                "{len} bytes"
            );
        }
        let account = AccountSharedData::new(1, 165, &spl_token_2022::id());
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::InvalidAccountData)
        );
    }

    #[test]
    fn test_check_target_live_mint_rejected() {
        let account = AdminVm::create_mint_account(6, &Pubkey::new_unique(), None, &TEST_DELEGATE);
        assert_eq!(
            AdminVm::check_initialize_mint_target(Some(&account), true),
            Err(InstructionError::AccountAlreadyInitialized)
        );
    }

    // ─── Permanent delegate ─────────────────────────────────────────────────

    /// One admin-signed tx of the given instructions, fee payer first.
    fn admin_tx(ixs: &[Instruction]) -> SanitizedTransaction {
        let payer = Keypair::new();
        let msg = Message::new(ixs, Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], msg, solana_sdk::hash::Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap()
    }

    fn init_mint2_ix(mint: Pubkey) -> Instruction {
        let authority = golden_authority().pubkey();
        spl_token_2022::instruction::initialize_mint2(
            &spl_token_2022::id(),
            &mint,
            &authority,
            Some(&authority),
            GOLDEN_DECIMALS,
        )
        .unwrap()
    }

    /// The mint the VM writes is byte for byte what the real Token-2022 program
    /// writes for the same instructions, for both InitializeMint variants.
    #[test]
    fn created_mint_matches_the_real_program_byte_for_byte() {
        let mint = golden_mint().pubkey();
        let authority = golden_authority().pubkey();
        let init_mint = spl_token_2022::instruction::initialize_mint(
            &spl_token_2022::id(),
            &mint,
            &authority,
            Some(&authority),
            GOLDEN_DECIMALS,
        )
        .unwrap();
        for init in [init_mint2_ix(mint), init_mint] {
            let tx = admin_tx(&[delegate_ix(mint, GOLDEN_DELEGATE), init]);
            let executed =
                assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));
            let (key, account) = &executed.loaded_transaction.accounts[index_of(&tx, &mint)];
            assert_eq!(*key, mint);
            assert_eq!(*account.owner(), spl_token_2022::id());
            assert_eq!(account.data(), &GOLDEN_T22_MINT[..]);
        }
    }

    /// Two delegate + init pairs create two mints, each with the delegate.
    #[test]
    fn two_pairs_create_two_delegated_mints() {
        let (a, b) = (Pubkey::new_unique(), Pubkey::new_unique());
        let tx = admin_tx(&[
            delegate_ix(a, TEST_DELEGATE),
            init_mint2_ix(a),
            delegate_ix(b, TEST_DELEGATE),
            init_mint2_ix(b),
        ]);
        let executed = assert_executed_with_status(run_admin_vm(std::slice::from_ref(&tx)), Ok(()));
        for mint in [a, b] {
            let account = &executed.loaded_transaction.accounts[index_of(&tx, &mint)].1;
            let state = StateWithExtensions::<Mint>::unpack(account.data()).unwrap();
            assert_eq!(get_permanent_delegate(&state), Some(TEST_DELEGATE));
        }
    }

    /// Every way to create a mint without the configured delegate fails the whole
    /// tx with InvalidArgument at the offending instruction.
    #[test]
    fn mint_without_the_configured_delegate_is_refused() {
        let (a, b) = (Pubkey::new_unique(), Pubkey::new_unique());
        let cases: [(&str, Vec<Instruction>, u8); 6] = [
            ("init alone", vec![init_mint2_ix(a)], 0),
            ("delegate alone", vec![delegate_ix(a, TEST_DELEGATE)], 0),
            (
                "delegate on another mint",
                vec![delegate_ix(b, TEST_DELEGATE), init_mint2_ix(a)],
                1,
            ),
            (
                "foreign delegate",
                vec![delegate_ix(a, Pubkey::new_unique()), init_mint2_ix(a)],
                0,
            ),
            (
                "delegate twice",
                vec![
                    delegate_ix(a, TEST_DELEGATE),
                    delegate_ix(a, TEST_DELEGATE),
                    init_mint2_ix(a),
                ],
                0,
            ),
            (
                "valid pair then a stray delegate",
                vec![
                    delegate_ix(a, TEST_DELEGATE),
                    init_mint2_ix(a),
                    delegate_ix(b, TEST_DELEGATE),
                ],
                2,
            ),
        ];
        for (label, ixs, index) in cases {
            let tx = admin_tx(&ixs);
            let output = run_admin_vm(std::slice::from_ref(&tx));
            let result = output.processing_results.into_iter().next().unwrap();
            let Ok(ProcessedTransaction::Executed(executed)) = result else {
                panic!("{label}: expected Executed");
            };
            assert_eq!(
                executed.execution_details.status,
                Err(TransactionError::InstructionError(
                    index,
                    InstructionError::InvalidArgument
                )),
                "{label}"
            );
            assert!(executed.loaded_transaction.accounts.is_empty(), "{label}");
        }
    }

    /// With no delegate configured the VM creates no mint at all.
    #[test]
    fn unconfigured_delegate_refuses_every_mint() {
        let mint = Pubkey::new_unique();
        let tx = admin_tx(&[delegate_ix(mint, TEST_DELEGATE), init_mint2_ix(mint)]);
        let executed = assert_executed_with_status(
            run_admin_vm_as(AdminVm::default(), std::slice::from_ref(&tx), &DummyCb),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidArgument,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// Legacy SPL Token mint creation is no longer an admin instruction here.
    #[test]
    fn spl_token_program_is_refused() {
        let mint = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let init =
            spl_token::instruction::initialize_mint2(&spl_token::id(), &mint, &authority, None, 6)
                .unwrap();
        let tx = admin_tx(&[init]);
        let executed = assert_executed_with_status(
            run_admin_vm(std::slice::from_ref(&tx)),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidInstructionData,
            )),
        );
        assert!(executed.loaded_transaction.accounts.is_empty());
    }

    /// A truncated delegate ix is malformed, not a foreign delegate.
    #[test]
    fn truncated_delegate_ix_is_invalid_instruction_data() {
        let mint = Pubkey::new_unique();
        let mut delegate = delegate_ix(mint, TEST_DELEGATE);
        delegate.data.truncate(20);
        let tx = admin_tx(&[delegate, init_mint2_ix(mint)]);
        assert_executed_with_status(
            run_admin_vm(std::slice::from_ref(&tx)),
            Err(TransactionError::InstructionError(
                0,
                InstructionError::InvalidInstructionData,
            )),
        );
    }
}
