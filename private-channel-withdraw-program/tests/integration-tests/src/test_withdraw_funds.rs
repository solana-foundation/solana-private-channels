use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;
use solana_program_pack::Pack;
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::{state::Mint, ID as TOKEN_PROGRAM_ID};

use crate::{
    state_utils::assert_get_or_withdraw_funds,
    utils::{
        assert_program_error, find_withdraw_config_pda, get_token_balance, set_mint,
        set_token_balance, set_withdraw_config, set_withdraw_config_with_minimum,
        setup_test_balances, TestContext, AMOUNT_BELOW_MINIMUM_ERROR, ATA_PROGRAM_ID,
        INVALID_INSTRUCTION_DATA_ERROR, INVALID_TREASURY_ACCOUNT_ERROR,
        INVALID_WITHDRAW_CONFIG_ERROR, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID, TEST_WITHDRAW_FEE,
        TOKEN_INSUFFICIENT_FUNDS_ERROR, WITHDRAW_CONFIG_NOT_INITIALIZED_ERROR, ZERO_AMOUNT_ERROR,
    },
};

const WITHDRAW_AMOUNT: u64 = 500_000; // 0.5 tokens with 6 decimals
const INITIAL_BALANCE: u64 = 1_000_000; // 1 token with 6 decimals

// `admin` in these tests is the treasury the config stores. In production the
// operator writes its admin key there on every deposit.

#[test]
fn test_withdraw_funds_success() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let supply_before = Mint::unpack(&context.get_account_data(&mint.pubkey()).unwrap())
        .unwrap()
        .supply;

    assert_get_or_withdraw_funds(
        &mut context,
        &user,
        &mint.pubkey(),
        WITHDRAW_AMOUNT,
        None,
        true,
    )
    .expect("Withdraw funds should succeed");

    // Only the amount is burned. The fee moves to the treasury and stays in supply.
    let supply_after = Mint::unpack(&context.get_account_data(&mint.pubkey()).unwrap())
        .unwrap()
        .supply;
    assert_eq!(supply_after, supply_before - WITHDRAW_AMOUNT);
}

#[test]
fn test_withdraw_funds_with_destination() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let destination = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);
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

// The admin moves out the fees it collected, so charging it would only
// shuffle tokens between its own account and itself. That self-transfer is a
// no-op, so the balance alone cannot tell the exemption apart. Only the exempt
// path leaves the treasury account unread, which a wrong one here proves.
#[test]
fn test_withdraw_funds_treasury_pays_no_fee() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let unread_treasury_token_account = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    // Before the balance: the config setup resets the treasury ATA to zero.
    let (withdraw_config, _) = set_withdraw_config(
        &mut context,
        &mint.pubkey(),
        TEST_WITHDRAW_FEE,
        &admin.pubkey(),
    );
    setup_test_balances(&mut context, &admin, &mint.pubkey(), INITIAL_BALANCE);

    let admin_ata = get_associated_token_address(&admin.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(admin.pubkey())
        .mint(mint.pubkey())
        .token_account(admin_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(unread_treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("Treasury withdrawal should succeed without a fee");

    assert_eq!(
        get_token_balance(&mut context, &admin_ata),
        INITIAL_BALANCE - WITHDRAW_AMOUNT
    );
}

// A zero-fee mint charges nothing and never reads the treasury account, so a
// withdrawal works even when that account does not exist.
#[test]
fn test_withdraw_funds_zero_fee() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();
    let missing_treasury_token_account = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, _) = set_withdraw_config(&mut context, &mint.pubkey(), 0, &admin);
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(missing_treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("a zero-fee withdrawal should succeed");

    assert_eq!(
        get_token_balance(&mut context, &user_ata),
        INITIAL_BALANCE - WITHDRAW_AMOUNT
    );
}

#[test]
fn test_withdraw_funds_insufficient_funds() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, treasury_token_account) =
        set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);

    // Set balance less than withdraw amount
    setup_test_balances(&mut context, &user, &mint.pubkey(), WITHDRAW_AMOUNT / 2);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, TOKEN_INSUFFICIENT_FUNDS_ERROR);
}

// The fee is charged on top of the amount. A balance that covers only the
// amount fails as a whole, so neither the fee moves nor anything is burned.
#[test]
fn test_withdraw_funds_balance_covers_amount_but_not_fee() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, treasury_token_account) =
        set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);
    setup_test_balances(&mut context, &user, &mint.pubkey(), WITHDRAW_AMOUNT);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, TOKEN_INSUFFICIENT_FUNDS_ERROR);
    assert_eq!(get_token_balance(&mut context, &user_ata), WITHDRAW_AMOUNT);
    assert_eq!(get_token_balance(&mut context, &treasury_token_account), 0);
}

// Every accepted withdrawal is its own Solana release, so one below the mint's
// minimum is refused before the fee moves or anything is burned.
#[test]
fn test_withdraw_funds_below_minimum() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();
    let min_withdraw_amount = WITHDRAW_AMOUNT;

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, treasury_token_account) = set_withdraw_config_with_minimum(
        &mut context,
        &mint.pubkey(),
        TEST_WITHDRAW_FEE,
        &admin,
        min_withdraw_amount,
    );
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(min_withdraw_amount - 1)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, AMOUNT_BELOW_MINIMUM_ERROR);
    assert_eq!(get_token_balance(&mut context, &user_ata), INITIAL_BALANCE);
    assert_eq!(get_token_balance(&mut context, &treasury_token_account), 0);
}

// The minimum itself is a valid amount, and the fee is still charged on top.
#[test]
fn test_withdraw_funds_at_minimum() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();
    let min_withdraw_amount = WITHDRAW_AMOUNT;

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, treasury_token_account) = set_withdraw_config_with_minimum(
        &mut context,
        &mint.pubkey(),
        TEST_WITHDRAW_FEE,
        &admin,
        min_withdraw_amount,
    );
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(min_withdraw_amount)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("a withdrawal of exactly the minimum should succeed");

    assert_eq!(
        get_token_balance(&mut context, &user_ata),
        INITIAL_BALANCE - min_withdraw_amount - TEST_WITHDRAW_FEE
    );
    assert_eq!(
        get_token_balance(&mut context, &treasury_token_account),
        TEST_WITHDRAW_FEE
    );
}

// Collected fees can add up to less than the minimum, and the treasury still
// has to be able to move them out.
#[test]
fn test_withdraw_funds_treasury_has_no_minimum() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let below_minimum = 1;

    set_mint(&mut context, &mint.pubkey());
    // Before the balance: the config setup resets the treasury ATA to zero.
    let (withdraw_config, treasury_token_account) = set_withdraw_config_with_minimum(
        &mut context,
        &mint.pubkey(),
        TEST_WITHDRAW_FEE,
        &admin.pubkey(),
        WITHDRAW_AMOUNT,
    );
    setup_test_balances(&mut context, &admin, &mint.pubkey(), INITIAL_BALANCE);

    let instruction = WithdrawFundsBuilder::new()
        .user(admin.pubkey())
        .mint(mint.pubkey())
        .token_account(treasury_token_account)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(below_minimum)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("the treasury should withdraw below the minimum");

    assert_eq!(
        get_token_balance(&mut context, &treasury_token_account),
        INITIAL_BALANCE - below_minimum
    );
}

#[test]
fn test_withdraw_funds_zero_amount() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, treasury_token_account) =
        set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(0)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, ZERO_AMOUNT_ERROR);
}

// A mint with no config cannot be withdrawn at all, so a fee can never be
// skipped by withdrawing before the operator has written one.
#[test]
fn test_withdraw_funds_withdraw_config_not_initialized() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());
    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());
    let treasury_token_account = get_associated_token_address(&admin, &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, WITHDRAW_CONFIG_NOT_INITIALIZED_ERROR);
}

// A program-owned config at any address but the mint's PDA is not that mint's
// config, even with a well-formed layout.
#[test]
fn test_withdraw_funds_withdraw_config_wrong_address() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let other_mint = Keypair::new();
    let admin = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    set_mint(&mut context, &other_mint.pubkey());
    // A real config, but for another mint.
    let (other_withdraw_config, treasury_token_account) = set_withdraw_config(
        &mut context,
        &other_mint.pubkey(),
        TEST_WITHDRAW_FEE,
        &admin,
    );
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(other_withdraw_config)
        .treasury_token_account(treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_WITHDRAW_CONFIG_ERROR);
}

#[test]
fn test_withdraw_funds_wrong_treasury_account() {
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let admin = Pubkey::new_unique();
    let attacker = Pubkey::new_unique();

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, _) =
        set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());
    // A valid token account of the same mint, just not the admin's.
    let attacker_ata = get_associated_token_address(&attacker, &mint.pubkey());
    set_token_balance(&mut context, &attacker_ata, &mint.pubkey(), &attacker, 0);

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(attacker_ata)
        .amount(WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_TREASURY_ACCOUNT_ERROR);
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
    let admin = Pubkey::new_unique();
    let amount: u64 = 500_000;

    set_mint(&mut context, &mint.pubkey());
    let (withdraw_config, treasury_token_account) =
        set_withdraw_config(&mut context, &mint.pubkey(), TEST_WITHDRAW_FEE, &admin);
    setup_test_balances(&mut context, &user, &mint.pubkey(), INITIAL_BALANCE);

    let user_ata = get_associated_token_address(&user.pubkey(), &mint.pubkey());

    let instruction = WithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint.pubkey())
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config)
        .treasury_token_account(treasury_token_account)
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
