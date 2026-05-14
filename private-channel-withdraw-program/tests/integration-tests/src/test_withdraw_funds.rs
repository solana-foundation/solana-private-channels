use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::ID as TOKEN_PROGRAM_ID;

use crate::{
    state_utils::assert_get_or_withdraw_funds,
    utils::{
        assert_program_error, set_mint, setup_test_balances, TestContext, ATA_PROGRAM_ID,
        INVALID_INSTRUCTION_DATA_ERROR, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        TOKEN_INSUFFICIENT_FUNDS_ERROR, ZERO_AMOUNT_ERROR,
    },
};

const WITHDRAW_AMOUNT: u64 = 500_000; // 0.5 tokens with 6 decimals
const INITIAL_BALANCE: u64 = 1_000_000; // 1 token with 6 decimals

#[test]
fn test_withdraw_funds_success() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    assert_get_or_withdraw_funds(
        &mut context,
        &user,
        &mint.pubkey(),
        WITHDRAW_AMOUNT,
        None,
        true,
    )
    .expect("Withdraw funds should succeed");
}

#[test]
fn test_withdraw_funds_with_destination() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let destination = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    assert_get_or_withdraw_funds(
        &mut context,
        &user,
        &mint.pubkey(),
        WITHDRAW_AMOUNT,
        Some(destination.pubkey()),
        true,
    )
    .expect("Withdraw funds with destination should succeed");
}

#[test]
fn test_withdraw_funds_insufficient_funds() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    // Set balance less than withdraw amount
    setup_test_balances(&mut context, &user, &mint.pubkey(), WITHDRAW_AMOUNT / 2);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, TOKEN_INSUFFICIENT_FUNDS_ERROR);
}

#[test]
fn test_withdraw_funds_zero_amount() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(0)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, ZERO_AMOUNT_ERROR);
}

#[test]
fn test_withdraw_funds_invalid_instruction_data_too_short() {
    let mut context = TestContext::new();

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        accounts: vec![],
        data: vec![0, 1, 2], // Too short instruction data
    };

    let result = context.send_transaction(instruction);
    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

// Verifies that a successful withdrawal actually emits a WithdrawFundsEvent log.
// pinocchio_log formats &[u8] as "[b0, b1, ..., b39]" (decimal bytes, comma-space
// separated) and sol_log_ prepends "Program log: " in the transaction logs.
#[test]
fn test_withdraw_funds_event_emission() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let destination = Pubkey::new_from_array([42u8; 32]);
    let mint = Keypair::new();
    let amount: u64 = 500_000;

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .amount(amount)
        .destination(destination)
        .instruction();

    let meta = context
        .send_transaction_with_signers_with_transaction_result(instruction, &[&user], false, None)
        .expect("Withdraw funds should succeed");

    // Build the expected log string: amount as LE bytes followed by destination bytes,
    // formatted as pinocchio_log renders a &[u8].
    let mut event_bytes = [0u8; 40];
    event_bytes[..8].copy_from_slice(&amount.to_le_bytes());
    event_bytes[8..].copy_from_slice(destination.as_ref());

    let parts: Vec<String> = event_bytes.iter().map(|b| b.to_string()).collect();
    let expected_log = format!("Program log: [{}]", parts.join(", "));

    assert!(
        meta.logs.iter().any(|log| log == &expected_log),
        "WithdrawFundsEvent not found in transaction logs.\nExpected: {}\nGot: {:#?}",
        expected_log,
        meta.logs,
    );
}
