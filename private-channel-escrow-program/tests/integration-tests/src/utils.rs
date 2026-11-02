use bytemuck::Pod;
use litesvm::{types::TransactionMetadata, LiteSVM};
use private_channel_escrow_program_client::PrivateChannelEscrowProgramError;
pub use private_channel_escrow_program_client::{INSTANCE_LEN, INSTANCE_V1_LEN};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_nullable::MaybeNull;
use solana_program::pubkey;
use solana_program_pack::Pack;
use solana_sdk::{
    account::Account,
    instruction::{AccountMeta, Instruction},
    message::{v1, VersionedMessage},
    program_option::COption,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::{Transaction, VersionedTransaction},
};
use solana_zk_elgamal_proof_interface::{
    instruction::{ContextStateInfo, ProofInstruction},
    proof_data::ZkProofData,
    state::ProofContextState,
};
use solana_zk_sdk::{
    encryption::{
        auth_encryption::{AeCiphertext, AeKey},
        elgamal::{ElGamalCiphertext, ElGamalKeypair, ElGamalPubkey},
    },
    zk_elgamal_proof_program::build_pubkey_validity_proof_data,
};
use spl_tlv_account_resolution::{account::ExtraAccountMeta, state::ExtraAccountMetaList};
use spl_token::{
    state::{Account as TokenAccount, Mint},
    ID as TOKEN_PROGRAM_ID,
};
use spl_token_2022::error::TokenError;
use spl_token_2022::{
    extension::{
        confidential_transfer::{
            ConfidentialTransferAccount, DecryptableBalance,
            DEFAULT_MAXIMUM_PENDING_BALANCE_CREDIT_COUNTER,
        },
        confidential_transfer_fee::instruction::initialize_confidential_transfer_fee_config,
        memo_transfer::MemoTransfer,
        metadata_pointer::MetadataPointer,
        pausable::PausableConfig,
        permanent_delegate::PermanentDelegate,
        transfer_fee::instruction::initialize_transfer_fee_config,
        transfer_hook::{
            instruction::initialize as initialize_transfer_hook, TransferHook, TransferHookAccount,
        },
        BaseStateWithExtensions, BaseStateWithExtensionsMut, ExtensionType, StateWithExtensions,
    },
    state::Mint as Token2022Mint,
};
use spl_token_2022_interface::extension::confidential_transfer::instruction as confidential_instruction;
use spl_token_confidential_transfer_proof_extraction::instruction::ProofLocation;
use spl_token_confidential_transfer_proof_generation::{
    transfer::transfer_split_proof_data, transfer_with_fee::transfer_with_fee_split_proof_data,
    withdraw::withdraw_proof_data,
};
use spl_token_metadata_interface::state::TokenMetadata;
use spl_transfer_hook_interface::{
    get_extra_account_metas_address, instruction::ExecuteInstruction,
};

use solana_program::clock::Clock;
use spl_associated_token_account::{
    get_associated_token_address, get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
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
pub const ZERO_AMOUNT_ERROR: u32 = PrivateChannelEscrowProgramError::ZeroAmount as u32;
pub const BELOW_MINIMUM_DEPOSIT_ERROR: u32 =
    PrivateChannelEscrowProgramError::BelowMinimumDeposit as u32;
pub const WALLET_NOT_ALLOWED_ERROR: u32 = PrivateChannelEscrowProgramError::WalletNotAllowed as u32;
pub const INVALID_ALLOWED_WALLET_PDA_ERROR: u32 =
    PrivateChannelEscrowProgramError::InvalidAllowedWalletPda as u32;
pub const CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR: u32 =
    PrivateChannelEscrowProgramError::ConfidentialAccountNotConfigured as u32;

// Token-2022 errors the confidential tests expect
pub const TOKEN_CONFIDENTIAL_CREDITS_DISABLED_ERROR: u32 =
    TokenError::ConfidentialTransferDepositsAndTransfersDisabled as u32;
pub const TOKEN_CONFIDENTIAL_BALANCE_MISMATCH_ERROR: u32 =
    TokenError::ConfidentialTransferBalanceMismatch as u32;

/// Withdraw fee every test AllowMint sets. The escrow records it but does not
/// act on it.
pub const TEST_WITHDRAW_FEE: u64 = 1_000;

/// Minimum withdrawal every test AllowMint sets. Recorded like the fee, never
/// acted on by the escrow.
pub const TEST_MIN_WITHDRAW_AMOUNT: u64 = 100;

/// Minimum deposit every test AllowMint sets. 0 so existing deposit amounts
/// are unaffected; the minimum tests set their own.
pub const TEST_MIN_DEPOSIT_AMOUNT: u64 = 0;

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

        // Mainnet's Token-2022 build, the one confidential transfers run against.
        let token_2022 = include_bytes!("../../../../core/precompiles/spl_token_2022-11.1.0.so");
        svm.add_program(TOKEN_2022_PROGRAM_ID, token_2022)
            .expect("Failed to load the pinned Token-2022 program");

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

    /// Sends `instructions` as one v1 transaction, the format that carries
    /// confidential transfer proofs inline. LiteSVM does not enforce the v1 size
    /// limit, so this does.
    pub fn send_v1(
        &mut self,
        instructions: &[Instruction],
        signers: &[&Keypair],
    ) -> Result<TransactionMetadata, Box<dyn std::error::Error>> {
        // A fresh blockhash keeps a repeated transaction from being dropped as a replay.
        self.svm.expire_blockhash();
        // v1 needs both limits set; Token-2022 alone loads 1.3 MB.
        let config = v1::TransactionConfig::empty()
            .with_compute_unit_limit(1_400_000)
            .with_loaded_accounts_data_size_limit(4_000_000);
        let message = v1::Message::try_compile_with_config(
            &self.payer.pubkey(),
            instructions,
            self.svm.latest_blockhash(),
            config,
        )?;

        let mut all_signers = vec![&self.payer];
        all_signers.extend(signers);
        let transaction =
            VersionedTransaction::try_new(VersionedMessage::V1(message), &all_signers)?;

        // Message plus signatures: the wire size, give or take a length byte.
        let size = transaction.message.serialize().len() + 64 * transaction.signatures.len();
        if size > v1::MAX_TRANSACTION_SIZE {
            return Err(format!(
                "Transaction size {size} exceeds the v1 limit {}",
                v1::MAX_TRANSACTION_SIZE
            )
            .into());
        }

        self.svm
            .send_transaction(transaction)
            .map_err(|error| format!("Transaction failed: {:?}", error).into())
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

pub const INVALID_ACCOUNT_DATA_LOG: &str = "invalid account data for instruction";
pub const INCORRECT_PROGRAM_ID_LOG: &str = "incorrect program id for instruction";

/// How the runtime logs a program's custom error.
pub fn custom_error(code: u32) -> String {
    format!("custom program error: {code:#x}")
}

/// Asserts that `program_id` failed first, with `reason` in its log. Escrow and
/// Token-2022 error numbers overlap, and a failed CPI fails its caller with the
/// same error, so only the first failure names the program that refused.
pub fn assert_failed_first(
    result: Result<TransactionMetadata, Box<dyn std::error::Error>>,
    program_id: &Pubkey,
    reason: &str,
) {
    let message = result.expect_err("Transaction should fail").to_string();
    // Skip the "Transaction failed: ..." text before the first program log.
    let first_failure = message
        .split("Program ")
        .skip(1)
        .find(|log| log.contains(" failed: "))
        .expect("A program should have failed");
    let (failed_program, failed_reason) = first_failure.split_once(" failed: ").unwrap();
    // The log line ends at its closing quote.
    let failed_reason = failed_reason.split('"').next().unwrap();
    assert_eq!(
        (failed_program, failed_reason),
        (program_id.to_string().as_str(), reason)
    );
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
        delegate: MaybeNull::try_from(Some(context.payer.pubkey())).unwrap(),
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
        authority: MaybeNull::try_from(Some(*authority)).unwrap(),
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

/// A mint whose issuer has reserved the right to attach metadata later. The
/// pointer is init-time; the metadata it names is not there yet.
pub fn set_mint_2022_with_metadata_pointer(context: &mut TestContext, mint: &Pubkey) {
    let extensions = [ExtensionType::MetadataPointer];
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions).unwrap();
    let mut data = vec![0u8; space];

    let mut state = PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();

    let metadata_pointer = state.init_extension::<MetadataPointer>(true).unwrap();
    *metadata_pointer = MetadataPointer {
        authority: MaybeNull::try_from(Some(context.payer.pubkey())).unwrap(),
        metadata_address: MaybeNull::try_from(Some(*mint)).unwrap(),
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
        .expect("Failed to set Token 2022 mint account with MetadataPointer");
}

/// The same mint after the issuer wrote its metadata. Identical to
/// [`set_mint_2022_with_metadata_pointer`] but for the added `TokenMetadata`.
pub fn set_mint_2022_with_metadata(context: &mut TestContext, mint: &Pubkey) {
    let metadata = TokenMetadata {
        update_authority: MaybeNull::try_from(Some(context.payer.pubkey())).unwrap(),
        mint: *mint,
        name: "Test Token".to_string(),
        symbol: "TEST".to_string(),
        uri: "https://example.com/test.json".to_string(),
        additional_metadata: vec![],
    };

    let extensions = [ExtensionType::MetadataPointer];
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions).unwrap()
        + metadata.tlv_size_of().unwrap();
    let mut data = vec![0u8; space];

    let mut state = PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();

    let metadata_pointer = state.init_extension::<MetadataPointer>(true).unwrap();
    *metadata_pointer = MetadataPointer {
        authority: MaybeNull::try_from(Some(context.payer.pubkey())).unwrap(),
        metadata_address: MaybeNull::try_from(Some(*mint)).unwrap(),
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
        .init_variable_len_extension::<TokenMetadata>(&metadata, false)
        .expect("Failed to init TokenMetadata");

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
        .expect("Failed to set Token 2022 mint account with TokenMetadata");
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
        authority: MaybeNull::try_from(Some(context.payer.pubkey())).unwrap(),
        program_id: MaybeNull::try_from(Some(*hook_program_id)).unwrap(),
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
pub fn set_extra_account_meta_list(
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
            ExtraAccountMeta::new_with_pubkey(&solana_sdk_ids::system_program::ID, false, false)
                .unwrap(),
        ];
    set_extra_account_meta_list(context, mint, &extras);
}

/// Trailing accounts to append when transferring a mint from
/// [`setup_hook_mint`]. Order matches the offchain resolver: declared
/// extras, hook program, validation PDA.
pub fn hook_extras_for_mint(mint: &Pubkey) -> Vec<AccountMeta> {
    vec![
        AccountMeta::new_readonly(solana_sdk_ids::system_program::ID, false),
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
    set_malicious_extra_account_meta_list(context, mint, victim, attacker);
}

/// The `ExtraAccountMetaList` of [`setup_malicious_hook_mint`], for a hook mint
/// that already exists.
pub fn set_malicious_extra_account_meta_list(
    context: &mut TestContext,
    mint: &Pubkey,
    victim: &Pubkey,
    attacker: &Pubkey,
) {
    // victim: signer + writable, since a System transfer's source needs
    // both. attacker: the writable sink. system program: so the hook can
    // CPI it.
    let extras = [
        ExtraAccountMeta::new_with_pubkey(victim, true, true).unwrap(),
        ExtraAccountMeta::new_with_pubkey(attacker, false, true).unwrap(),
        ExtraAccountMeta::new_with_pubkey(&solana_sdk_ids::system_program::ID, false, false)
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
        AccountMeta::new_readonly(solana_sdk_ids::system_program::ID, false),
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

/// Token-2022 account with `MemoTransfer` enabled, so Token-2022 rejects an
/// incoming transfer unless a Memo instruction is its preceding sibling. Only
/// the account owner can turn this on, which is why it reaches user ATAs and
/// never the escrow's.
///
/// `with_transfer_hook` adds `TransferHookAccount`, which a hook mint's
/// transfer requires on both sides regardless of the memo.
pub fn set_token_2022_with_memo_account(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
    with_transfer_hook: bool,
) {
    let extensions: &[ExtensionType] = if with_transfer_hook {
        &[
            ExtensionType::MemoTransfer,
            ExtensionType::TransferHookAccount,
        ]
    } else {
        &[ExtensionType::MemoTransfer]
    };
    let space = ExtensionType::try_calculate_account_len::<Token2022Account>(extensions).unwrap();
    let mut data = vec![0u8; space];

    let mut state =
        PodStateWithExtensionsMut::<PodAccount>::unpack_uninitialized(&mut data).unwrap();
    state
        .init_extension::<MemoTransfer>(true)
        .unwrap()
        .require_incoming_transfer_memos = true.into();
    if with_transfer_hook {
        state.init_extension::<TransferHookAccount>(true).unwrap();
    }
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
        .expect("Failed to set Token2022 account with MemoTransfer");
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

    let create_account_ix = solana_system_interface::instruction::create_account(
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

/// Decimals of the mints from [`create_confidential_mint`].
pub const CONFIDENTIAL_MINT_DECIMALS: u8 = 6;

/// The ElGamal and AES keys behind one confidential account.
pub struct ConfidentialKeys {
    pub elgamal: ElGamalKeypair,
    pub aes: AeKey,
}

impl ConfidentialKeys {
    pub fn new() -> Self {
        Self {
            elgamal: ElGamalKeypair::new_rand(),
            aes: AeKey::new_rand(),
        }
    }

    /// `amount` encrypted the way Token-2022 stores a decryptable balance.
    pub fn decryptable(&self, amount: u64) -> DecryptableBalance {
        self.aes.encrypt(amount).into()
    }
}

impl Default for ConfidentialKeys {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct ConfidentialMintOptions {
    /// Basis points and maximum fee.
    pub transfer_fee: Option<(u16, u64)>,
    pub transfer_hook: bool,
    pub manual_approval: bool,
}

/// A confidential mint and the keys its transfer proofs name. The fee
/// authority's key matters only when the mint has a transfer fee.
pub struct ConfidentialMint {
    pub address: Pubkey,
    pub auditor: ElGamalKeypair,
    pub transfer_fee: Option<(u16, u64)>,
    pub withdraw_withheld_authority: ElGamalKeypair,
}

/// Creates a confidential Token-2022 mint with an auditor and the payer as mint
/// authority. A hook mint runs the hook fixture with the extras of
/// [`setup_hook_mint`].
pub fn create_confidential_mint(
    context: &mut TestContext,
    options: ConfidentialMintOptions,
) -> ConfidentialMint {
    let mint = Keypair::new();
    let auditor = ElGamalKeypair::new_rand();
    let withdraw_withheld_authority = ElGamalKeypair::new_rand();
    let payer = context.payer.pubkey();

    let mut extensions = vec![ExtensionType::ConfidentialTransferMint];
    if options.transfer_fee.is_some() {
        extensions.push(ExtensionType::TransferFeeConfig);
        extensions.push(ExtensionType::ConfidentialTransferFeeConfig);
    }
    if options.transfer_hook {
        extensions.push(ExtensionType::TransferHook);
    }
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&extensions).unwrap();

    let mut instructions = vec![
        solana_system_interface::instruction::create_account(
            &payer,
            &mint.pubkey(),
            context.svm.minimum_balance_for_rent_exemption(space),
            space as u64,
            &TOKEN_2022_PROGRAM_ID,
        ),
        confidential_instruction::initialize_mint(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            Some(payer),
            !options.manual_approval,
            Some((*auditor.pubkey()).into()),
        )
        .unwrap(),
    ];
    if let Some((basis_points, maximum_fee)) = options.transfer_fee {
        instructions.push(
            initialize_transfer_fee_config(
                &TOKEN_2022_PROGRAM_ID,
                &mint.pubkey(),
                Some(&payer),
                Some(&payer),
                basis_points,
                maximum_fee,
            )
            .unwrap(),
        );
        instructions.push(
            initialize_confidential_transfer_fee_config(
                &TOKEN_2022_PROGRAM_ID,
                &mint.pubkey(),
                Some(payer),
                &(*withdraw_withheld_authority.pubkey()).into(),
            )
            .unwrap(),
        );
    }
    if options.transfer_hook {
        instructions.push(
            initialize_transfer_hook(
                &TOKEN_2022_PROGRAM_ID,
                &mint.pubkey(),
                Some(payer),
                Some(HOOK_FIXTURE_PROGRAM_ID),
            )
            .unwrap(),
        );
    }
    instructions.push(
        spl_token_2022::instruction::initialize_mint2(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            &payer,
            None,
            CONFIDENTIAL_MINT_DECIMALS,
        )
        .unwrap(),
    );
    context
        .send_v1(&instructions, &[&mint])
        .expect("Confidential mint should be created");

    if options.transfer_hook {
        let extras =
            [
                ExtraAccountMeta::new_with_pubkey(
                    &solana_sdk_ids::system_program::ID,
                    false,
                    false,
                )
                .unwrap(),
            ];
        set_extra_account_meta_list(context, &mint.pubkey(), &extras);
    }

    ConfidentialMint {
        address: mint.pubkey(),
        auditor,
        transfer_fee: options.transfer_fee,
        withdraw_withheld_authority,
    }
}

/// Creates `owner`'s Token-2022 ATA for `mint` and configures it for
/// confidential transfers under `keys`.
pub fn create_confidential_account(
    context: &mut TestContext,
    owner: &Keypair,
    mint: &ConfidentialMint,
    keys: &ConfidentialKeys,
) -> Pubkey {
    let payer = context.payer.pubkey();
    let account = get_associated_token_address_with_program_id(
        &owner.pubkey(),
        &mint.address,
        &TOKEN_2022_PROGRAM_ID,
    );

    let mut extensions = vec![ExtensionType::ConfidentialTransferAccount];
    if mint.transfer_fee.is_some() {
        extensions.push(ExtensionType::ConfidentialTransferFeeAmount);
    }

    let proof = build_pubkey_validity_proof_data(&keys.elgamal).unwrap();
    let mut instructions = vec![
        create_associated_token_account_idempotent(
            &payer,
            &owner.pubkey(),
            &mint.address,
            &TOKEN_2022_PROGRAM_ID,
        ),
        spl_token_2022::instruction::reallocate(
            &TOKEN_2022_PROGRAM_ID,
            &account,
            &payer,
            &owner.pubkey(),
            &[],
            &extensions,
        )
        .unwrap(),
    ];
    instructions.extend(
        confidential_instruction::configure_account(
            &TOKEN_2022_PROGRAM_ID,
            &account,
            &mint.address,
            &keys.decryptable(0),
            DEFAULT_MAXIMUM_PENDING_BALANCE_CREDIT_COUNTER,
            &owner.pubkey(),
            &[],
            ProofLocation::InstructionOffset(1.try_into().unwrap(), &proof),
        )
        .unwrap(),
    );
    context
        .send_v1(&instructions, &[owner])
        .expect("Confidential account should be configured");

    account
}

/// Mints `amount` to `account` and moves it into the available confidential
/// balance, Token-2022's deposit and apply.
pub fn fund_confidential_balance(
    context: &mut TestContext,
    owner: &Keypair,
    account: &Pubkey,
    mint: &Pubkey,
    keys: &ConfidentialKeys,
    amount: u64,
) {
    let available = decrypt_available(context, account, keys);
    let instructions = [
        spl_token_2022::instruction::mint_to(
            &TOKEN_2022_PROGRAM_ID,
            mint,
            account,
            &context.payer.pubkey(),
            &[],
            amount,
        )
        .unwrap(),
        confidential_instruction::deposit(
            &TOKEN_2022_PROGRAM_ID,
            account,
            mint,
            amount,
            CONFIDENTIAL_MINT_DECIMALS,
            &owner.pubkey(),
            &[],
        )
        .unwrap(),
        confidential_instruction::apply_pending_balance(
            &TOKEN_2022_PROGRAM_ID,
            account,
            1,
            &keys.decryptable(available + amount),
            &owner.pubkey(),
            &[],
        )
        .unwrap(),
    ];
    context
        .send_v1(&instructions, &[owner])
        .expect("Confidential balance should be funded");
}

pub fn confidential_state(
    context: &mut TestContext,
    account: &Pubkey,
) -> ConfidentialTransferAccount {
    let data = context.get_account_data(account).unwrap();
    *StateWithExtensions::<Token2022Account>::unpack(&data)
        .unwrap()
        .get_extension::<ConfidentialTransferAccount>()
        .unwrap()
}

/// Decrypts the change from `before` to `after`, two ciphertexts of one part of
/// an amount Token-2022 splits into low and high bits. The change can be
/// negative: a fee taken from a pending balance can shrink its low part.
pub fn decrypt_change(keys: &ConfidentialKeys, before: &[u8; 64], after: &[u8; 64]) -> i64 {
    let before = ElGamalCiphertext::from_bytes(before).unwrap();
    let after = ElGamalCiphertext::from_bytes(after).unwrap();
    let secret = keys.elgamal.secret();
    match (after - before).decrypt_u32(secret) {
        Some(gain) => gain as i64,
        None => -((before - after).decrypt_u32(secret).unwrap() as i64),
    }
}

/// Low part plus high part shifted past the low 16 bits.
pub fn combine_lo_hi(lo: i64, hi: i64) -> u64 {
    (lo + (hi << 16)) as u64
}

pub fn decrypt_pending(
    context: &mut TestContext,
    account: &Pubkey,
    keys: &ConfidentialKeys,
) -> u64 {
    let state = confidential_state(context, account);
    combine_lo_hi(
        decrypt_change(keys, &[0; 64], &state.pending_balance_lo.0),
        decrypt_change(keys, &[0; 64], &state.pending_balance_hi.0),
    )
}

/// The available balance as Token-2022 holds it, not the AES copy.
pub fn decrypt_available(
    context: &mut TestContext,
    account: &Pubkey,
    keys: &ConfidentialKeys,
) -> u64 {
    let state = confidential_state(context, account);
    decrypt_change(keys, &[0; 64], &state.available_balance.0) as u64
}

/// The data of the escrow event with `discriminator`, after its tag and discriminator.
pub fn event_data(transaction_metadata: &TransactionMetadata, discriminator: u8) -> Vec<u8> {
    let event_tag = [228, 69, 165, 46, 81, 203, 154, 29]; // EVENT_IX_TAG_LE
    transaction_metadata
        .inner_instructions
        .iter()
        .flatten()
        .map(|inner_instruction| &inner_instruction.instruction.data)
        .find(|data| data.len() >= 9 && data[0..8] == event_tag && data[8] == discriminator)
        .map(|data| data[9..].to_vec())
        .expect("event should be emitted")
}

/// Where a test puts confidential transfer proofs.
#[derive(Clone, Copy)]
pub enum ProofPlacement {
    /// Verify instructions right after the escrow instruction, in one v1 transaction.
    Inline,
    /// Context accounts, verified in earlier transactions.
    ContextAccounts,
}

/// The proofs of one escrow instruction, in Token-2022's proof order.
pub struct PlacedProofs {
    placement: ProofPlacement,
    instructions: Vec<Instruction>,
    pub context_accounts: Vec<AccountMeta>,
}

impl PlacedProofs {
    pub fn new(placement: ProofPlacement) -> Self {
        Self {
            placement,
            instructions: Vec::new(),
            context_accounts: Vec::new(),
        }
    }

    /// Places one proof and returns the offset the escrow instruction passes for it.
    pub fn place<T: Pod + ZkProofData<U>, U: Pod>(
        &mut self,
        context: &mut TestContext,
        instruction: ProofInstruction,
        proof: &T,
    ) -> i8 {
        match self.placement {
            ProofPlacement::Inline => {
                self.instructions
                    .push(instruction.encode_verify_proof(None, proof));
                self.instructions.len() as i8
            }
            ProofPlacement::ContextAccounts => {
                let account = create_context_account(context, instruction, proof);
                self.context_accounts
                    .push(AccountMeta::new_readonly(account, false));
                0
            }
        }
    }

    /// The escrow instruction followed by the inline proofs, if any.
    pub fn with(&self, escrow_instruction: Instruction) -> Vec<Instruction> {
        let mut instructions = vec![escrow_instruction];
        instructions.extend(self.instructions.iter().cloned());
        instructions
    }
}

/// Verifies `proof` into a new context account, with the payer as its authority.
fn create_context_account<T: Pod + ZkProofData<U>, U: Pod>(
    context: &mut TestContext,
    instruction: ProofInstruction,
    proof: &T,
) -> Pubkey {
    let account = Keypair::new();
    let space = size_of::<ProofContextState<U>>();
    let instructions = [
        solana_system_interface::instruction::create_account(
            &context.payer.pubkey(),
            &account.pubkey(),
            context.svm.minimum_balance_for_rent_exemption(space),
            space as u64,
            &solana_zk_elgamal_proof_interface::id(),
        ),
        instruction.encode_verify_proof(
            Some(ContextStateInfo {
                context_state_account: &account.pubkey(),
                context_state_authority: &context.payer.pubkey(),
            }),
            proof,
        ),
    ];
    context
        .send_v1(&instructions, &[&account])
        .expect("Proof should verify into a context account");
    account.pubkey()
}

/// A confidential transfer's proofs and the sender fields the escrow forwards.
pub struct PreparedTransfer {
    pub proofs: PlacedProofs,
    pub new_source_decryptable_available_balance: [u8; 36],
    pub auditor_ciphertext_lo: [u8; 64],
    pub auditor_ciphertext_hi: [u8; 64],
    pub equality_proof_instruction_offset: i8,
    pub ciphertext_validity_proof_instruction_offset: i8,
    pub range_proof_instruction_offset: i8,
    pub fee_sigma_proof_instruction_offset: i8,
    pub fee_ciphertext_validity_proof_instruction_offset: i8,
}

/// Proves a transfer of `amount` from `source`'s available balance to an account
/// under `destination`, for the mint's auditor, and its fee when it has one.
pub fn prepare_transfer(
    context: &mut TestContext,
    source: &Pubkey,
    source_keys: &ConfidentialKeys,
    destination: &ElGamalPubkey,
    mint: &ConfidentialMint,
    amount: u64,
    placement: ProofPlacement,
) -> PreparedTransfer {
    let state = confidential_state(context, source);
    let decryptable: AeCiphertext = state.decryptable_available_balance.try_into().unwrap();
    let balance = source_keys.aes.decrypt(&decryptable).unwrap();
    prepare_transfer_from_balance(
        context,
        source,
        source_keys,
        balance,
        destination,
        mint,
        amount,
        placement,
    )
}

/// `prepare_transfer` from a known available balance instead of the AES copy, which
/// can lag after an apply.
#[allow(clippy::too_many_arguments)]
pub fn prepare_transfer_from_balance(
    context: &mut TestContext,
    source: &Pubkey,
    source_keys: &ConfidentialKeys,
    balance: u64,
    destination: &ElGamalPubkey,
    mint: &ConfidentialMint,
    amount: u64,
    placement: ProofPlacement,
) -> PreparedTransfer {
    let state = confidential_state(context, source);
    let available: ElGamalCiphertext = state.available_balance.try_into().unwrap();
    // The proof generators read the balance from an AES ciphertext.
    let decryptable = source_keys.aes.encrypt(balance);
    let new_balance = balance - amount;
    let mut proofs = PlacedProofs::new(placement);

    // Placed in Token-2022's order, so the context accounts line up with it.
    let Some((basis_points, maximum_fee)) = mint.transfer_fee else {
        let data = transfer_split_proof_data(
            &available,
            &decryptable,
            amount,
            &source_keys.elgamal,
            &source_keys.aes,
            destination,
            Some(mint.auditor.pubkey()),
        )
        .unwrap();
        let validity = &data.ciphertext_validity_proof_data_with_ciphertext;
        let equality_proof_instruction_offset = proofs.place(
            context,
            ProofInstruction::VerifyCiphertextCommitmentEquality,
            &data.equality_proof_data,
        );
        let ciphertext_validity_proof_instruction_offset = proofs.place(
            context,
            ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
            &validity.proof_data,
        );
        let range_proof_instruction_offset = proofs.place(
            context,
            ProofInstruction::VerifyBatchedRangeProofU128,
            &data.range_proof_data,
        );
        return PreparedTransfer {
            equality_proof_instruction_offset,
            ciphertext_validity_proof_instruction_offset,
            range_proof_instruction_offset,
            fee_sigma_proof_instruction_offset: 0,
            fee_ciphertext_validity_proof_instruction_offset: 0,
            new_source_decryptable_available_balance: source_keys.decryptable(new_balance).0,
            auditor_ciphertext_lo: validity.ciphertext_lo.0,
            auditor_ciphertext_hi: validity.ciphertext_hi.0,
            proofs,
        };
    };

    let data = transfer_with_fee_split_proof_data(
        &available,
        &decryptable,
        amount,
        &source_keys.elgamal,
        &source_keys.aes,
        destination,
        Some(mint.auditor.pubkey()),
        mint.withdraw_withheld_authority.pubkey(),
        basis_points,
        maximum_fee,
    )
    .unwrap();
    let validity = &data.transfer_amount_ciphertext_validity_proof_data_with_ciphertext;
    let equality_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyCiphertextCommitmentEquality,
        &data.equality_proof_data,
    );
    let ciphertext_validity_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
        &validity.proof_data,
    );
    let fee_sigma_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyPercentageWithCap,
        &data.percentage_with_cap_proof_data,
    );
    let fee_ciphertext_validity_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity,
        &data.fee_ciphertext_validity_proof_data,
    );
    let range_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyBatchedRangeProofU256,
        &data.range_proof_data,
    );
    PreparedTransfer {
        equality_proof_instruction_offset,
        ciphertext_validity_proof_instruction_offset,
        range_proof_instruction_offset,
        fee_sigma_proof_instruction_offset,
        fee_ciphertext_validity_proof_instruction_offset,
        new_source_decryptable_available_balance: source_keys.decryptable(new_balance).0,
        auditor_ciphertext_lo: validity.ciphertext_lo.0,
        auditor_ciphertext_hi: validity.ciphertext_hi.0,
        proofs,
    }
}

/// The proofs for moving `amount` from `account`'s available balance to its
/// public one, and the fields the escrow forwards.
pub struct PreparedWithdraw {
    pub proofs: PlacedProofs,
    pub new_decryptable_available_balance: [u8; 36],
    pub equality_proof_instruction_offset: i8,
    pub range_proof_instruction_offset: i8,
}

pub fn prepare_withdraw(
    context: &mut TestContext,
    account: &Pubkey,
    keys: &ConfidentialKeys,
    amount: u64,
    placement: ProofPlacement,
) -> PreparedWithdraw {
    let state = confidential_state(context, account);
    let available: ElGamalCiphertext = state.available_balance.try_into().unwrap();
    let balance = keys
        .aes
        .decrypt(&state.decryptable_available_balance.try_into().unwrap())
        .unwrap();
    let data = withdraw_proof_data(&available, balance, amount, &keys.elgamal).unwrap();

    let mut proofs = PlacedProofs::new(placement);
    let equality_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyCiphertextCommitmentEquality,
        &data.equality_proof_data,
    );
    let range_proof_instruction_offset = proofs.place(
        context,
        ProofInstruction::VerifyBatchedRangeProofU64,
        &data.range_proof_data,
    );
    PreparedWithdraw {
        proofs,
        new_decryptable_available_balance: keys.decryptable(balance - amount).0,
        equality_proof_instruction_offset,
        range_proof_instruction_offset,
    }
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
        9 => "AddOrUpdateAllowedWallet",
        10 => "RemoveAllowedWallet",
        11 => "SetWhitelistEnabled",
        12 => "ConfigureConfidentialAccount",
        13 => "ConfidentialDeposit",
        14 => "ConfidentialReleaseFunds",
        15 => "ApplyPendingBalance",
        16 => "MoveToConfidential",
        17 => "MoveToPublic",
        _ => "Unknown",
    }
}
