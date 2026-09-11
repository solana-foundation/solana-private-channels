use litesvm::{types::TransactionMetadata, LiteSVM};
use private_channel_escrow_program_client::PrivateChannelEscrowProgramError;
use solana_program::pubkey;
use solana_program_pack::Pack;
use solana_sdk::{
    account::Account,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    program_option::COption,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use spl_pod::optional_keys::OptionalNonZeroPubkey;
use spl_tlv_account_resolution::{account::ExtraAccountMeta, state::ExtraAccountMetaList};
use spl_token::{
    state::{Account as TokenAccount, Mint},
    ID as TOKEN_PROGRAM_ID,
};
use spl_token_2022::{
    extension::{
        pausable::PausableConfig,
        permanent_delegate::PermanentDelegate,
        transfer_fee::instruction::initialize_transfer_fee_config,
        transfer_hook::{TransferHook, TransferHookAccount},
        BaseStateWithExtensionsMut, ExtensionType,
    },
    state::Mint as Token2022Mint,
};
use spl_transfer_hook_interface::{
    get_extra_account_metas_address, instruction::ExecuteInstruction,
};

use solana_program::clock::Clock;
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};
use spl_token_2022::extension::PodStateWithExtensionsMut;
use spl_token_2022::pod::{PodAccount, PodCOption, PodMint};
use spl_token_2022::state::{Account as Token2022Account, AccountState};

const MIN_LAMPORTS: u64 = 500_000_000;

pub const ATA_PROGRAM_ID: Pubkey = pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const PRIVATE_CHANNEL_ESCROW_PROGRAM_ID: Pubkey =
    pubkey!("9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU");
pub const TOKEN_2022_PROGRAM_ID: Pubkey = spl_token_2022::ID;
/// Transfer-hook program loaded into LiteSVM for hook-bearing Token-2022
/// mints. Matches `declare_id!` in `tests/transfer-hook-fixture`.
pub const HOOK_FIXTURE_PROGRAM_ID: Pubkey = pubkey!("hookEjHJAu757hfesyLchyGLxH6BeNuEbcztVEFT4K4");

// PrivateChannel Escrow Program Error Codes (using generated error enum)
pub const INVALID_EVENT_AUTHORITY_ERROR: u32 =
    PrivateChannelEscrowProgramError::InvalidEventAuthority as u32;
pub const INVALID_ATA_ERROR: u32 = PrivateChannelEscrowProgramError::InvalidAta as u32;
pub const INVALID_MINT_ERROR: u32 = PrivateChannelEscrowProgramError::InvalidMint as u32;
pub const INVALID_INSTANCE_ERROR: u32 = PrivateChannelEscrowProgramError::InvalidInstance as u32;
pub const INVALID_ADMIN_ERROR: u32 = PrivateChannelEscrowProgramError::InvalidAdmin as u32;
pub const INVALID_ALLOWED_MINT_ERROR: u32 =
    PrivateChannelEscrowProgramError::InvalidAllowedMint as u32;
pub const INVALID_OPERATOR_ERROR: u32 = PrivateChannelEscrowProgramError::InvalidOperatorPda as u32;
pub const INVALID_WITHDRAWAL_BITMAP_ERROR: u32 =
    PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap as u32;
pub const NONCE_ALREADY_USED_ERROR: u32 = PrivateChannelEscrowProgramError::NonceAlreadyUsed as u32;
pub const TRANSFER_HOOK_NOT_ALLOWED_ERROR: u32 =
    PrivateChannelEscrowProgramError::TransferHookNotAllowed as u32;
pub const NONCE_OUTSIDE_CURRENT_GENERATION_ERROR: u32 =
    PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration as u32;
pub const UNEXPECTED_GENERATION_ERROR: u32 =
    PrivateChannelEscrowProgramError::UnexpectedGeneration as u32;
pub const DEPOSITS_BLOCKED_FOR_MINT_ERROR: u32 =
    PrivateChannelEscrowProgramError::DepositsBlockedForMint as u32;
pub const WITHDRAWALS_BLOCKED_FOR_MINT_ERROR: u32 =
    PrivateChannelEscrowProgramError::WithdrawalsBlockedForMint as u32;
pub const MINT_PROFILE_CHANGED_ERROR: u32 =
    PrivateChannelEscrowProgramError::MintProfileChanged as u32;

/// Nonces covered by one bitmap generation. Must match the on-chain constant.
pub const NONCES_PER_GENERATION: u64 = 65_536;

// Standard Solana Program Error Codes
pub const INVALID_ARGUMENT_ERROR: u32 = 5; // ProgramError::InvalidArgument
pub const INVALID_ACCOUNT_DATA_ERROR: u32 = 6; // ProgramError::InvalidAccountData
pub const NOT_ENOUGH_ACCOUNT_KEYS_ERROR: u32 = 2; // ProgramError::NotEnoughAccountKeys
pub const INVALID_INSTRUCTION_DATA_ERROR: u32 = 3; // ProgramError::InvalidInstructionData
pub const INVALID_ACCOUNT_OWNER_ERROR: u32 = 23; // ProgramError::InvalidAccountOwner
pub const INVALID_SEEDS_ERROR: u32 = 14; // ProgramError::InvalidSeeds
pub const MISSING_REQUIRED_SIGNATURE_ERROR: u32 = 0; // ProgramError::MissingRequiredSignature

// Standard Solana Program Error Codes (continued)
pub const INCORRECT_PROGRAM_ID_ERROR: u32 = 100; // ProgramError::IncorrectProgramId (string match)

// SPL Token Program Error Codes
pub const TOKEN_INSUFFICIENT_FUNDS_ERROR: u32 = 1; // TokenError::InsufficientFunds (from spl-token)

pub struct TestContext {
    pub svm: LiteSVM,
    pub payer: Keypair,
}

impl TestContext {
    pub fn new() -> Self {
        let mut svm = LiteSVM::new().with_sysvars().with_default_programs();

        // Override clock to start at current time instead of Unix epoch 0
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        svm.set_sysvar(&Clock {
            slot: 1,
            epoch_start_timestamp: current_time,
            epoch: 0,
            leader_schedule_epoch: 0,
            unix_timestamp: current_time,
        });

        let program_data =
            include_bytes!("../../../../target/deploy/private_channel_escrow_program.so");
        let _ = svm.add_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, program_data);

        let hook_fixture_data =
            include_bytes!("../../../../target/deploy/transfer_hook_fixture.so");
        let _ = svm.add_program(HOOK_FIXTURE_PROGRAM_ID, hook_fixture_data);

        let payer = Keypair::new();

        svm.airdrop(&payer.pubkey(), 1_000_000_000).unwrap();

        Self { svm, payer }
    }

    pub fn airdrop_if_required(
        &mut self,
        pubkey: &Pubkey,
        lamports: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let needs_airdrop = match self.svm.get_account(pubkey) {
            Some(account) => account.lamports < MIN_LAMPORTS,
            None => true,
        };

        if needs_airdrop {
            match self.svm.airdrop(pubkey, lamports) {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("Airdrop failed: {:?}", e).into()),
            }
        } else {
            Ok(())
        }
    }

    pub fn create_account(
        &mut self,
        pubkey: &Pubkey,
        owner: &Pubkey,
        data: Vec<u8>,
        lamports: u64,
    ) {
        let account = Account {
            lamports,
            data,
            owner: *owner,
            executable: false,
            rent_epoch: 0,
        };
        let _ = self.svm.set_account(*pubkey, account);
    }

    pub fn send_transaction(
        &mut self,
        instruction: Instruction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let transaction = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&self.payer.pubkey()),
            &[&self.payer],
            self.svm.latest_blockhash(),
        );

        let result = self.svm.send_transaction(transaction);
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Transaction failed: {:?}", e).into()),
        }
    }

    pub fn send_transaction_with_signers(
        &mut self,
        instruction: Instruction,
        signers: &[&Keypair],
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.send_transaction_with_signers_with_transaction_result(
            instruction,
            signers,
            false,
            None,
        )
        .map(|_| ())
    }

    pub fn send_transaction_with_signers_with_transaction_result(
        &mut self,
        instruction: Instruction,
        signers: &[&Keypair],
        enable_profiling: bool,
        cu_limit: Option<u32>,
    ) -> Result<TransactionMetadata, Box<dyn std::error::Error>> {
        let mut all_signers = vec![&self.payer];
        all_signers.extend(signers);

        let mut instructions = vec![instruction.clone()];

        if let Some(cu_limit) = cu_limit {
            instructions.insert(
                0,
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            );
        }

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.payer.pubkey()),
            &all_signers,
            self.svm.latest_blockhash(),
        );

        // Simulate first to get CU consumption for profiling (only if enabled)
        if enable_profiling && instruction.program_id == PRIVATE_CHANNEL_ESCROW_PROGRAM_ID {
            let simulation_result = self.svm.simulate_transaction(transaction.clone());
            if let Ok(sim_metadata) = simulation_result {
                let cu_consumed = sim_metadata.meta.compute_units_consumed;
                let operation = get_operation_name(&instruction);
                eprintln!(
                    r#"{{"type":"profiling","operation":"{}","cu_consumed":{}}}"#,
                    operation, cu_consumed
                );
            }
        }

        let result = self.svm.send_transaction(transaction);
        match result {
            Ok(logs) => Ok(logs),
            Err(e) => Err(format!("Transaction failed: {:?}", e).into()),
        }
    }

    pub fn get_account(&mut self, pubkey: &Pubkey) -> Option<Account> {
        self.svm.get_account(pubkey)
    }

    pub fn get_account_data(&mut self, pubkey: &Pubkey) -> Option<Vec<u8>> {
        self.get_account(pubkey).map(|account| account.data)
    }

    pub fn advance_clock(&mut self, seconds: i64) {
        let current_clock = self.svm.get_sysvar::<Clock>();
        self.svm.set_sysvar(&Clock {
            slot: current_clock.slot + seconds as u64,
            epoch_start_timestamp: current_clock.epoch_start_timestamp,
            epoch: current_clock.epoch,
            leader_schedule_epoch: current_clock.leader_schedule_epoch,
            unix_timestamp: current_clock.unix_timestamp + seconds,
        });
    }

    pub fn warp_to_slot(&mut self, slot: u64) {
        let clock = self.svm.get_sysvar::<Clock>();
        self.svm.set_sysvar(&Clock { slot, ..clock });
        self.svm.expire_blockhash();
    }

    pub fn warp_to_timestamp(&mut self, unix_timestamp: i64) {
        self.svm.set_sysvar(&Clock {
            slot: 1,
            epoch_start_timestamp: unix_timestamp,
            epoch: 0,
            leader_schedule_epoch: 0,
            unix_timestamp,
        });
    }

    pub fn get_current_timestamp(&self) -> i64 {
        self.svm.get_sysvar::<Clock>().unix_timestamp
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

pub fn get_token_balance(context: &mut TestContext, ata: &Pubkey) -> u64 {
    let account = context.get_account(ata);
    match account {
        Some(account) => {
            if account.owner == TOKEN_PROGRAM_ID {
                let token_account =
                    TokenAccount::unpack(&account.data).expect("Should deserialize token account");
                token_account.amount
            } else if account.owner == TOKEN_2022_PROGRAM_ID {
                let token_account = spl_token_2022::extension::StateWithExtensions::<
                    Token2022Account,
                >::unpack(&account.data)
                .expect("Should deserialize Token2022 account");
                token_account.base.amount
            } else {
                0
            }
        }
        None => 0,
    }
}

pub fn get_or_create_associated_token_account(
    context: &mut TestContext,
    wallet: &Pubkey,
    mint: &Pubkey,
) -> Pubkey {
    let ata_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        wallet,
        mint,
        &TOKEN_PROGRAM_ID,
    );

    context
        .send_transaction(ata_ix)
        .expect("Failed to create associated token account");

    get_associated_token_address(wallet, mint)
}

pub fn set_token_balance(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) {
    let token_account = TokenAccount {
        mint: *mint,
        owner: *owner,
        amount,
        delegate: COption::None,
        state: spl_token::state::AccountState::Initialized,
        is_native: COption::None,
        delegated_amount: 0,
        close_authority: COption::None,
    };

    let mut data = vec![0u8; TokenAccount::LEN];
    TokenAccount::pack(token_account, &mut data).expect("Failed to pack token account");

    context
        .svm
        .set_account(
            *ata,
            Account {
                lamports: 2039280, // Rent-exempt minimum for token account
                data,
                owner: TOKEN_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set token account");
}

pub fn set_token_balance_2022(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) {
    let token_account = Token2022Account {
        mint: *mint,
        owner: *owner,
        amount,
        delegate: COption::None,
        state: AccountState::Initialized,
        is_native: COption::None,
        delegated_amount: 0,
        close_authority: COption::None,
    };

    let mut data = vec![0u8; Token2022Account::LEN];
    spl_token_2022::state::Account::pack(token_account, &mut data)
        .expect("Failed to pack Token2022 account");

    context
        .svm
        .set_account(
            *ata,
            Account {
                lamports: 2039280, // Rent-exempt minimum for token account
                data,
                owner: TOKEN_2022_PROGRAM_ID, // Use Token2022 program as owner
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set Token2022 account");
}

pub fn set_mint(context: &mut TestContext, mint: &Pubkey) {
    set_mint_with_decimals(context, mint, 6);
}

/// Same as [`set_mint`] but carrying a freeze authority, for the one-directional
/// profile check: gaining one means a recreate, losing one does not.
pub fn set_mint_with_freeze_authority(
    context: &mut TestContext,
    mint: &Pubkey,
    freeze_authority: &Pubkey,
) {
    let mint_account = Mint {
        decimals: 6,
        is_initialized: true,
        freeze_authority: COption::Some(*freeze_authority),
        mint_authority: COption::None,
        supply: 1_000_000,
    };

    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint_account, &mut data).expect("Failed to pack mint account");

    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set mint account");
}

pub fn set_mint_with_decimals(context: &mut TestContext, mint: &Pubkey, decimals: u8) {
    let mint_account = Mint {
        decimals,
        is_initialized: true,
        freeze_authority: COption::None,
        mint_authority: COption::None,
        supply: 1_000_000,
    };

    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint_account, &mut data).expect("Failed to pack mint account");

    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set mint account");
}

// Helper function to check if error contains specific program error code
pub fn assert_program_error(
    result: Result<(), Box<dyn std::error::Error>>,
    expected_error_code: u32,
) {
    match result {
        Err(e) => {
            let error_string = format!("{:?}", e);

            let expected_custom_error = format!("Custom({})", expected_error_code);

            // Check for standard Solana program errors based on error code mapping
            let standard_error_patterns = match expected_error_code {
                0 => vec!["MissingRequiredSignature"],
                1 => vec!["insufficient funds"], // Token program error
                2 => vec!["NotEnoughAccountKeys"],
                3 => vec!["InvalidInstructionData"],
                5 => vec!["InvalidArgument"],
                6 => vec!["InvalidAccountData"],
                14 => vec!["InvalidSeeds"],
                23 => vec!["InvalidAccountOwner"],
                100 => vec!["IncorrectProgramId"],
                _ => vec![],
            };

            let mut found_match = false;

            if error_string.contains(&expected_custom_error) {
                found_match = true;
            }

            // Then check for standard error patterns
            for pattern in &standard_error_patterns {
                if error_string.contains(pattern) {
                    found_match = true;
                    break;
                }
            }

            assert!(
                found_match,
                "Expected error code {} (Custom({}) or standard patterns {:?}) but got: {}",
                expected_error_code, expected_error_code, standard_error_patterns, error_string
            );
        }
        Ok(_) => panic!(
            "Expected transaction to fail with error code {}",
            expected_error_code
        ),
    }
}

pub fn assert_event_discriminator_present(
    transaction_metadata: &TransactionMetadata,
    discriminator: u8,
) {
    // Simple check: just verify that any inner instruction contains the event tag + discriminator
    let event_tag = [228, 69, 165, 46, 81, 203, 154, 29]; // EVENT_IX_TAG_LE
    let mut event_found = false;

    for inner_instruction_set in &transaction_metadata.inner_instructions {
        for inner_instruction in inner_instruction_set {
            let data = &inner_instruction.instruction.data;
            if data.len() >= 9 && data[0..8] == event_tag && data[8] == discriminator {
                event_found = true;
                break;
            }
        }
        if event_found {
            break;
        }
    }

    assert!(
        event_found,
        "Expected event with discriminator {} not found in transaction logs",
        discriminator
    );
}

pub fn set_mint_2022_basic(context: &mut TestContext, mint: &Pubkey) {
    let mint_data = Token2022Mint {
        mint_authority: COption::None,
        supply: 1_000_000,
        decimals: 6,
        is_initialized: true,
        freeze_authority: COption::None,
    };

    let mut data = vec![0u8; Token2022Mint::LEN];
    Token2022Mint::pack_into_slice(&mint_data, &mut data);

    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_2022_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set basic Token 2022 mint account");
}

pub fn set_mint_2022_with_permanent_delegate(context: &mut TestContext, mint: &Pubkey) {
    let extensions = [ExtensionType::PermanentDelegate];
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions).unwrap();
    let mut data = vec![0u8; space];

    let mut state = PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();

    // Initialize the extension first, then the base mint
    let permanent_delegate = state.init_extension::<PermanentDelegate>(true).unwrap();
    *permanent_delegate = PermanentDelegate {
        delegate: OptionalNonZeroPubkey::try_from(Some(context.payer.pubkey())).unwrap(),
    };

    let pod_mint = PodMint {
        mint_authority: COption::Some(context.payer.pubkey()).into(),
        supply: 1_000_000u64.into(),
        decimals: 6,
        is_initialized: true.into(),
        freeze_authority: COption::None.into(),
    };
    *state.base = pod_mint;

    state
        .init_account_type()
        .expect("Failed to init account type");

    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_2022_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set Token 2022 mint account with PermanentDelegate");
}

pub fn set_mint_2022_with_pausable(context: &mut TestContext, mint: &Pubkey, authority: &Pubkey) {
    let extensions = [ExtensionType::Pausable];
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions).unwrap();
    let mut data = vec![0u8; space];

    let mut state = PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();

    let pausable_config = state.init_extension::<PausableConfig>(true).unwrap();
    *pausable_config = PausableConfig {
        paused: false.into(),
        authority: OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap(),
    };

    let pod_mint = PodMint {
        mint_authority: COption::None.into(),
        supply: 1_000_000u64.into(),
        decimals: 6,
        is_initialized: true.into(),
        freeze_authority: COption::None.into(),
    };
    *state.base = pod_mint;

    state
        .init_account_type()
        .expect("Failed to init account type");

    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_2022_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set Token 2022 mint account with Pausable");
}

pub fn set_mint_2022_with_transfer_hook(
    context: &mut TestContext,
    mint: &Pubkey,
    hook_program_id: &Pubkey,
) {
    let extensions = [ExtensionType::TransferHook];
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions).unwrap();
    let mut data = vec![0u8; space];

    let mut state = PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();

    let transfer_hook = state.init_extension::<TransferHook>(true).unwrap();
    *transfer_hook = TransferHook {
        authority: OptionalNonZeroPubkey::try_from(Some(context.payer.pubkey())).unwrap(),
        program_id: OptionalNonZeroPubkey::try_from(Some(*hook_program_id)).unwrap(),
    };

    let pod_mint = PodMint {
        mint_authority: COption::Some(context.payer.pubkey()).into(),
        supply: 1_000_000u64.into(),
        decimals: 6,
        is_initialized: true.into(),
        freeze_authority: COption::None.into(),
    };
    *state.base = pod_mint;

    state
        .init_account_type()
        .expect("Failed to init account type");

    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_2022_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set Token 2022 mint account with TransferHook");
}

/// Writes the `ExtraAccountMetaList` validation PDA the hook fixture reads.
/// Token-2022 resolves the transfer's hook accounts from it.
fn set_extra_account_meta_list(
    context: &mut TestContext,
    mint: &Pubkey,
    extras: &[ExtraAccountMeta],
) {
    let validation_pda = get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID);

    let size = ExtraAccountMetaList::size_of(extras.len()).unwrap();
    let mut data = vec![0u8; size];
    ExtraAccountMetaList::init::<ExecuteInstruction>(&mut data, extras)
        .expect("Failed to init ExtraAccountMetaList");

    context
        .svm
        .set_account(
            validation_pda,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: HOOK_FIXTURE_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set ExtraAccountMetaList account");
}

/// Token-2022 mint whose transfers run the hook fixture, with an
/// `ExtraAccountMetaList` declaring one extra (the system program).
///
/// The fixture logs how many accounts it receives, so tests assert 6:
/// source, mint, destination, authority, validation PDA, and that extra.
pub fn setup_hook_mint(context: &mut TestContext, mint: &Pubkey) {
    set_mint_2022_with_transfer_hook(context, mint, &HOOK_FIXTURE_PROGRAM_ID);

    let extras =
        [
            ExtraAccountMeta::new_with_pubkey(&solana_program::system_program::ID, false, false)
                .unwrap(),
        ];
    set_extra_account_meta_list(context, mint, &extras);
}

/// Trailing accounts to append when transferring a mint from
/// [`setup_hook_mint`]. Order matches the offchain resolver: declared
/// extras, hook program, validation PDA.
pub fn hook_extras_for_mint(mint: &Pubkey) -> Vec<AccountMeta> {
    vec![
        AccountMeta::new_readonly(solana_program::system_program::ID, false),
        AccountMeta::new_readonly(HOOK_FIXTURE_PROGRAM_ID, false),
        AccountMeta::new_readonly(
            get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID),
            false,
        ),
    ]
}

/// Like [`setup_hook_mint`], but the `ExtraAccountMetaList` names `victim`
/// as a signer-bearing extra and `attacker` as a writable one, arming the
/// fixture's drain. A client resolver faithfully marks victim a signer; the
/// escrow strips that bit before forwarding.
pub fn setup_malicious_hook_mint(
    context: &mut TestContext,
    mint: &Pubkey,
    victim: &Pubkey,
    attacker: &Pubkey,
) {
    set_mint_2022_with_transfer_hook(context, mint, &HOOK_FIXTURE_PROGRAM_ID);

    // victim: signer + writable, since a System transfer's source needs
    // both. attacker: the writable sink. system program: so the hook can
    // CPI it.
    let extras = [
        ExtraAccountMeta::new_with_pubkey(victim, true, true).unwrap(),
        ExtraAccountMeta::new_with_pubkey(attacker, false, true).unwrap(),
        ExtraAccountMeta::new_with_pubkey(&solana_program::system_program::ID, false, false)
            .unwrap(),
    ];
    set_extra_account_meta_list(context, mint, &extras);
}

/// Trailing accounts for a mint from [`setup_malicious_hook_mint`]. Victim
/// is passed writable so the runtime carries its signer bit into the
/// instruction, which is what the escrow has to strip.
pub fn malicious_hook_extras(
    mint: &Pubkey,
    victim: &Pubkey,
    attacker: &Pubkey,
) -> Vec<AccountMeta> {
    vec![
        AccountMeta::new(*victim, false),
        AccountMeta::new(*attacker, false),
        AccountMeta::new_readonly(solana_program::system_program::ID, false),
        AccountMeta::new_readonly(HOOK_FIXTURE_PROGRAM_ID, false),
        AccountMeta::new_readonly(
            get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID),
            false,
        ),
    ]
}

/// Token-2022 account carrying the `TransferHookAccount` extension.
///
/// `process_transfer` flips `transferring` on both sides of a hook mint's
/// transfer, so a bare 165-byte account fails with `InvalidAccountData`.
/// ATAs the program creates after the mint has its hook get the extension
/// automatically; this is for accounts written by hand.
pub fn set_token_2022_with_hook_account(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) {
    let space = ExtensionType::try_calculate_account_len::<Token2022Account>(&[
        ExtensionType::TransferHookAccount,
    ])
    .unwrap();
    let mut data = vec![0u8; space];

    let mut state =
        PodStateWithExtensionsMut::<PodAccount>::unpack_uninitialized(&mut data).unwrap();
    state.init_extension::<TransferHookAccount>(true).unwrap();
    *state.base = PodAccount {
        mint: *mint,
        owner: *owner,
        amount: amount.into(),
        delegate: PodCOption::none(),
        state: AccountState::Initialized as u8,
        is_native: PodCOption::none(),
        delegated_amount: 0u64.into(),
        close_authority: PodCOption::none(),
    };
    state
        .init_account_type()
        .expect("Failed to init account type");

    context
        .svm
        .set_account(
            *ata,
            Account {
                lamports: 2_039_280,
                data,
                owner: TOKEN_2022_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .expect("Failed to set Token2022 account with TransferHookAccount");
}

pub fn create_mint_2022_with_transfer_fee(
    context: &mut TestContext,
    mint: &Keypair,
    transfer_fee_basis_points: u16,
    maximum_fee: u64,
) {
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&[
        ExtensionType::TransferFeeConfig,
    ])
    .unwrap();
    let rent = context.svm.minimum_balance_for_rent_exemption(space);

    let create_account_ix = solana_sdk::system_instruction::create_account(
        &context.payer.pubkey(),
        &mint.pubkey(),
        rent,
        space as u64,
        &TOKEN_2022_PROGRAM_ID,
    );

    let init_transfer_fee_ix = initialize_transfer_fee_config(
        &TOKEN_2022_PROGRAM_ID,
        &mint.pubkey(),
        Some(&context.payer.pubkey()),
        Some(&context.payer.pubkey()),
        transfer_fee_basis_points,
        maximum_fee,
    )
    .unwrap();

    let init_mint_ix = spl_token_2022::instruction::initialize_mint(
        &TOKEN_2022_PROGRAM_ID,
        &mint.pubkey(),
        &context.payer.pubkey(),
        None,
        6,
    )
    .unwrap();

    let transaction = Transaction::new_signed_with_payer(
        &[create_account_ix, init_transfer_fee_ix, init_mint_ix],
        Some(&context.payer.pubkey()),
        &[&context.payer, mint],
        context.svm.latest_blockhash(),
    );

    context
        .svm
        .send_transaction(transaction)
        .expect("Failed to create mint with transfer fee");
}

pub fn get_or_create_associated_token_account_2022(
    context: &mut TestContext,
    wallet: &Pubkey,
    mint: &Pubkey,
) -> Pubkey {
    let ata_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        wallet,
        mint,
        &TOKEN_2022_PROGRAM_ID,
    );

    context
        .send_transaction(ata_ix)
        .expect("Failed to create associated token account for Token 2022");

    spl_associated_token_account::get_associated_token_address_with_program_id(
        wallet,
        mint,
        &TOKEN_2022_PROGRAM_ID,
    )
}

/// Helper function to setup test ATAs and balances
pub fn setup_test_balances(
    context: &mut TestContext,
    user: &Keypair,
    instance_pda: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    user_balance: u64,
    instance_balance: u64,
) -> (Pubkey, Pubkey) {
    let (user_ata, instance_ata) = if token_program == &TOKEN_2022_PROGRAM_ID {
        let user_ata = get_or_create_associated_token_account_2022(context, &user.pubkey(), mint);
        let instance_ata = get_or_create_associated_token_account_2022(context, instance_pda, mint);
        (user_ata, instance_ata)
    } else {
        let user_ata = get_or_create_associated_token_account(context, &user.pubkey(), mint);
        let instance_ata = get_or_create_associated_token_account(context, instance_pda, mint);
        (user_ata, instance_ata)
    };

    if token_program == &TOKEN_2022_PROGRAM_ID {
        set_token_balance_2022(context, &user_ata, mint, &user.pubkey(), user_balance);
        set_token_balance_2022(context, &instance_ata, mint, instance_pda, instance_balance);
    } else {
        set_token_balance(context, &user_ata, mint, &user.pubkey(), user_balance);
        set_token_balance(context, &instance_ata, mint, instance_pda, instance_balance);
    }

    (user_ata, instance_ata)
}

/// Map instruction discriminator to operation name for profiling
fn get_operation_name(instruction: &Instruction) -> &'static str {
    if instruction.data.is_empty() {
        return "Unknown";
    }

    match instruction.data[0] {
        0 => "CreateInstance",
        1 => "AllowMint",
        2 => "BlockMint",
        3 => "AddOperator",
        4 => "RemoveOperator",
        5 => "SetNewAdmin",
        6 => "Deposit",
        7 => "ReleaseFunds",
        8 => "RotateBitmap",
        _ => "Unknown",
    }
}
