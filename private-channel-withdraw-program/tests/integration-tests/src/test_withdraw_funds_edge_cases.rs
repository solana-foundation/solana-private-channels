use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::ID as TOKEN_PROGRAM_ID;

use crate::utils::{
    assert_program_error, set_mint, setup_test_balances, TestContext, ATA_PROGRAM_ID,
    INCORRECT_PROGRAM_ID_ERROR, INVALID_INSTRUCTION_DATA_ERROR, INVALID_MINT_ERROR,
    MISSING_REQUIRED_SIGNATURE_ERROR, NOT_ENOUGH_ACCOUNT_KEYS_ERROR,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
};

const INITIAL_BALANCE: u64 = 1_000_000;
const WITHDRAW_AMOUNT: u64 = 500_000;

/// Wrong mint account should fail with InvalidMint.
#[test]
fn test_withdraw_funds_wrong_mint() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let wrong_mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(wrong_mint.pubkey()) // Wrong mint — no valid Mint data in SVM
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_MINT_ERROR);
}

/// Non-signer user should fail with MissingRequiredSignature.
#[test]
fn test_withdraw_funds_non_signer_user() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    // Build canonical instruction, then strip the signer flag from user account
    let mut instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    instruction.accounts[0] = AccountMeta::new_readonly(user.pubkey(), false);

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

/// Wrong associated token program address should fail with IncorrectProgramId.
#[test]
fn test_withdraw_funds_wrong_ata_program() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let fake_ata_program = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    // Build instruction with wrong ATA program
    let mut instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(fake_ata_program)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    // Override account 4 (associated_token_program) with a fake address
    instruction.accounts[4] = AccountMeta::new_readonly(fake_ata_program, false);

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INCORRECT_PROGRAM_ID_ERROR);
}

/// Wrong token program address should fail with IncorrectProgramId.
#[test]
fn test_withdraw_funds_wrong_token_program() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let fake_token_program = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    // Build instruction with wrong token program
    let mut instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(fake_token_program)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    instruction.accounts[3] = AccountMeta::new_readonly(fake_token_program, false);

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INCORRECT_PROGRAM_ID_ERROR);
}

/// ATA that doesn't match the expected PDA derivation should fail.
#[test]
fn test_withdraw_funds_wrong_ata_address() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let wrong_ata = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    // Use a random address instead of the correct ATA
    let mut instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(wrong_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    instruction.accounts[2] = AccountMeta::new(wrong_ata, false);

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

/// Invalid discriminator byte should fail with InvalidInstructionData.
#[test]
fn test_withdraw_funds_invalid_discriminator() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    // Build a raw instruction with invalid discriminator (byte 255)
    let mut data = vec![255u8]; // Invalid discriminator
    data.extend_from_slice(&WITHDRAW_AMOUNT.to_le_bytes());
    data.push(0); // No destination

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        accounts: vec![
            AccountMeta::new_readonly(user.pubkey(), true),
            AccountMeta::new(mint.pubkey(), false),
            AccountMeta::new(user_ata, false),
            AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
            AccountMeta::new_readonly(ATA_PROGRAM_ID, false),
        ],
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

/// Not enough accounts should fail with NotEnoughAccountKeys.
#[test]
fn test_withdraw_funds_not_enough_accounts() {
    let mut context = TestContext::new();
    let user = Keypair::new();

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    // Build a raw instruction with only 3 accounts (need 5)
    let mut data = vec![0u8]; // Valid discriminator
    data.extend_from_slice(&WITHDRAW_AMOUNT.to_le_bytes());
    data.push(0); // No destination

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        accounts: vec![
            AccountMeta::new_readonly(user.pubkey(), true),
            AccountMeta::new_readonly(Pubkey::new_unique(), false),
            AccountMeta::new(Pubkey::new_unique(), false),
        ],
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, NOT_ENOUGH_ACCOUNT_KEYS_ERROR);
}
