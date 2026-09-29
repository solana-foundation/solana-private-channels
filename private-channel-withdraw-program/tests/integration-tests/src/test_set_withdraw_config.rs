use private_channel_withdraw_program_client::instructions::SetWithdrawConfigBuilder;
use solana_sdk::{
    program_option::COption,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address;

use crate::utils::{
    assert_program_error, find_withdraw_config_pda, get_withdraw_config, set_mint_with_authority,
    TestContext, INVALID_MINT_AUTHORITY_ERROR, INVALID_SYSTEM_PROGRAM_ERROR,
    INVALID_WITHDRAW_CONFIG_ERROR, SYSTEM_PROGRAM_ID, TEST_MIN_WITHDRAW_AMOUNT, TEST_WITHDRAW_FEE,
};

// `admin` is the mint authority, the signer and the treasury, as the operator
// admin is in production.

#[test]
fn test_set_withdraw_config_creates_config() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, bump) = find_withdraw_config_pda(&mint.pubkey());

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .allow_mint_slot(0)
        .treasury(admin.pubkey())
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .instruction();

    context
        .send_transaction_with_signers_with_transaction_result(instruction, &[&admin], true, None)
        .expect("SetWithdrawConfig should succeed");

    let config = get_withdraw_config(&mut context, &withdraw_config);
    assert_eq!(config.bump, bump);
    assert_eq!(config.fee, TEST_WITHDRAW_FEE);
    assert_eq!(config.min_withdraw_amount, TEST_MIN_WITHDRAW_AMOUNT);
    assert_eq!(config.treasury, admin.pubkey());
    assert_eq!(
        config.treasury_token_account,
        get_associated_token_address(&admin.pubkey(), &mint.pubkey())
    );
}

// The operator sends this on every deposit, so the latest AllowMint values and
// the current admin always win over what an earlier deposit wrote.
#[test]
fn test_set_withdraw_config_overwrites() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let new_treasury = Pubkey::new_unique();
    let new_fee = TEST_WITHDRAW_FEE * 3;
    let new_min_withdraw_amount = TEST_MIN_WITHDRAW_AMOUNT * 5;

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .allow_mint_slot(0)
        .treasury(admin.pubkey())
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("First SetWithdrawConfig should succeed");

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(new_fee)
        .allow_mint_slot(0)
        .treasury(new_treasury)
        .min_withdraw_amount(new_min_withdraw_amount)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("Second SetWithdrawConfig should overwrite");

    let config = get_withdraw_config(&mut context, &withdraw_config);
    assert_eq!(config.fee, new_fee);
    assert_eq!(config.min_withdraw_amount, new_min_withdraw_amount);
    assert_eq!(config.treasury, new_treasury);
    assert_eq!(
        config.treasury_token_account,
        get_associated_token_address(&new_treasury, &mint.pubkey())
    );
}

// Deposits land in any order, so one built before a reprice can arrive after
// one built after it. Its older values must not win, and it must still succeed
// so the deposit carrying it mints.
#[test]
fn test_set_withdraw_config_ignores_an_older_allow_mint_slot() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let repriced_fee = 10_000;
    let repriced_min_withdraw_amount = 50_000;
    let later_fee = 20_000;
    let later_min_withdraw_amount = 70_000;
    let stale_treasury = Pubkey::new_unique();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(repriced_fee)
        .allow_mint_slot(100)
        .treasury(admin.pubkey())
        .min_withdraw_amount(repriced_min_withdraw_amount)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("the repriced write should succeed");

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(1)
        .allow_mint_slot(90)
        .treasury(stale_treasury)
        .min_withdraw_amount(1)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("a stale write must succeed so its deposit still mints");

    let config = get_withdraw_config(&mut context, &withdraw_config);
    assert_eq!(config.fee, repriced_fee);
    assert_eq!(config.min_withdraw_amount, repriced_min_withdraw_amount);
    assert_eq!(config.allow_mint_slot, 100);
    assert_eq!(config.treasury, admin.pubkey());
    assert_eq!(
        config.treasury_token_account,
        get_associated_token_address(&admin.pubkey(), &mint.pubkey())
    );

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(later_fee)
        .allow_mint_slot(110)
        .treasury(admin.pubkey())
        .min_withdraw_amount(later_min_withdraw_amount)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("a newer write should succeed");

    let config = get_withdraw_config(&mut context, &withdraw_config);
    assert_eq!(config.fee, later_fee);
    assert_eq!(config.min_withdraw_amount, later_min_withdraw_amount);
    assert_eq!(config.allow_mint_slot, 110);
}

// A zero fee and no minimum are supported for permissioned deployments, so
// they are stored rather than rejected.
#[test]
fn test_set_withdraw_config_zero_fee_and_minimum() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(0)
        .allow_mint_slot(0)
        .treasury(admin.pubkey())
        .min_withdraw_amount(0)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("SetWithdrawConfig with a zero fee and no minimum should succeed");

    let config = get_withdraw_config(&mut context, &withdraw_config);
    assert_eq!(config.fee, 0);
    assert_eq!(config.min_withdraw_amount, 0);
}

// Anyone can send lamports to the PDA before it exists, which would make a
// plain CreateAccount fail and wedge every deposit of the mint.
#[test]
fn test_set_withdraw_config_prefunded_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());
    context.create_account(&withdraw_config, &SYSTEM_PROGRAM_ID, vec![], 1_000);

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .allow_mint_slot(0)
        .treasury(admin.pubkey())
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("SetWithdrawConfig should succeed on a pre-funded PDA");

    let config = get_withdraw_config(&mut context, &withdraw_config);
    assert_eq!(config.fee, TEST_WITHDRAW_FEE);
}

// Anyone else could otherwise drop the fee to 1 or redirect it to themselves.
#[test]
fn test_set_withdraw_config_not_mint_authority() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let attacker = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&attacker.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(attacker.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(1)
        .allow_mint_slot(0)
        .treasury(attacker.pubkey())
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&attacker]);

    assert_program_error(result, INVALID_MINT_AUTHORITY_ERROR);
}

// Custom, not a builtin: the deposit transaction carries this instruction, and
// the operator retries a mint forever on InvalidAccountData.
#[test]
fn test_set_withdraw_config_wrong_address() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let wrong_withdraw_config = Pubkey::new_unique();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(wrong_withdraw_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .allow_mint_slot(0)
        .treasury(admin.pubkey())
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_WITHDRAW_CONFIG_ERROR);
}

// Checked by address so the error is custom. Letting the CPI find out would
// surface IncorrectProgramId, which the operator also retries forever.
#[test]
fn test_set_withdraw_config_wrong_system_program() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let fake_system_program = Pubkey::new_unique();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_config, _) = find_withdraw_config_pda(&mint.pubkey());

    let instruction = SetWithdrawConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_config(withdraw_config)
        .system_program(fake_system_program)
        .fee(TEST_WITHDRAW_FEE)
        .allow_mint_slot(0)
        .treasury(admin.pubkey())
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_SYSTEM_PROGRAM_ERROR);
}
