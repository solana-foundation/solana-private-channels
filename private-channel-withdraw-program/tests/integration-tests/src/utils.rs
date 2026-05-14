use litesvm::{types::TransactionMetadata, LiteSVM};
use private_channel_withdraw_program_client::PrivateChannelWithdrawProgramError;
use solana_program::pubkey;
use solana_program_pack::Pack;
use solana_sdk::{
    account::Account,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    program_option::COption,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use spl_token::{
    state::{Account as TokenAccount, Mint},
    ID as TOKEN_PROGRAM_ID,
};

use solana_program::clock::Clock;
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};

const MIN_LAMPORTS: u64 = 500_000_000;

pub const ATA_PROGRAM_ID: Pubkey = pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID: Pubkey =
    pubkey!("J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi");

// PrivateChannel Withdraw Program Error Codes (using generated error enum)
pub const INVALID_MINT_ERROR: u32 = PrivateChannelWithdrawProgramError::InvalidMint as u32;
pub const ZERO_AMOUNT_ERROR: u32 = PrivateChannelWithdrawProgramError::ZeroAmount as u32;

// Standard Solana Program Error Codes
pub const INVALID_ARGUMENT_ERROR: u32 = 5; // ProgramError::InvalidArgument
pub const INVALID_ACCOUNT_DATA_ERROR: u32 = 6; // ProgramError::InvalidAccountData
pub const NOT_ENOUGH_ACCOUNT_KEYS_ERROR: u32 = 2; // ProgramError::NotEnoughAccountKeys
pub const INVALID_INSTRUCTION_DATA_ERROR: u32 = 3; // ProgramError::InvalidInstructionData
pub const INVALID_ACCOUNT_OWNER_ERROR: u32 = 23; // ProgramError::InvalidAccountOwner
pub const INCORRECT_PROGRAM_ID_ERROR: u32 = 4; // ProgramError::IncorrectProgramId
pub const INVALID_SEEDS_ERROR: u32 = 14; // ProgramError::InvalidSeeds
pub const MISSING_REQUIRED_SIGNATURE_ERROR: u32 = 0; // ProgramError::MissingRequiredSignature

// SPL Token Program Error Codes
pub const TOKEN_INSUFFICIENT_FUNDS_ERROR: u32 = 1; // TokenError::InsufficientFunds

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
            include_bytes!("../../../../target/deploy/private_channel_withdraw_program.so");
        let _ = svm.add_program(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID, program_data);

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
        if enable_profiling && instruction.program_id == PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID {
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

pub fn set_mint(context: &mut TestContext, mint: &Pubkey) {
    let mint_account = Mint {
        decimals: 6,
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

            // Check for custom program errors (Custom(N))
            let expected_custom_error = format!("Custom({})", expected_error_code);

            // Check for standard Solana program errors based on error code mapping
            let standard_error_patterns = match expected_error_code {
                0 => vec!["MissingRequiredSignature"],
                1 => vec!["insufficient funds"], // Token program error
                2 => vec!["NotEnoughAccountKeys"],
                3 => vec!["InvalidInstructionData"],
                4 => vec!["IncorrectProgramId"],
                5 => vec!["InvalidArgument"],
                6 => vec!["InvalidAccountData"],
                14 => vec!["InvalidSeeds"],
                23 => vec!["InvalidAccountOwner"],
                _ => vec![],
            };

            let mut found_match = false;

            // First check for custom errors
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

pub fn setup_test_balances(
    context: &mut TestContext,
    user: &Keypair,
    mint: &Pubkey,
    user_balance: u64,
) {
    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .expect("Failed to airdrop to user");

    let user_ata = get_associated_token_address(&user.pubkey(), mint);

    let create_ata_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        &user.pubkey(),
        mint,
        &TOKEN_PROGRAM_ID, // Use TOKEN_PROGRAM_ID for now
    );

    context
        .send_transaction(create_ata_ix)
        .expect("Failed to create user ATA");

    set_token_balance(context, &user_ata, mint, &user.pubkey(), user_balance);
}

/// Map instruction discriminator to operation name for profiling
fn get_operation_name(instruction: &Instruction) -> &'static str {
    if instruction.data.is_empty() {
        return "Unknown";
    }

    match instruction.data[0] {
        0 => "WithdrawFunds",
        _ => "Unknown",
    }
}
