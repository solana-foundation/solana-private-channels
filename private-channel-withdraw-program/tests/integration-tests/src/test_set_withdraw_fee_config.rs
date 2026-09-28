use private_channel_withdraw_program_client::instructions::SetWithdrawFeeConfigBuilder;
use solana_sdk::{
    program_option::COption,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address;

use crate::utils::{
    assert_program_error, find_withdraw_fee_config_pda, get_withdraw_fee_config,
    set_mint_with_authority, TestContext, INVALID_FEE_CONFIG_ERROR, INVALID_MINT_AUTHORITY_ERROR,
    INVALID_SYSTEM_PROGRAM_ERROR, SYSTEM_PROGRAM_ID, TEST_WITHDRAW_FEE,
};

// `admin` is the mint authority, the signer and the treasury, as the operator
// admin is in production.

#[test]
fn test_set_withdraw_fee_config_creates_config() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_fee_config, bump) = find_withdraw_fee_config_pda(&mint.pubkey());

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(withdraw_fee_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .treasury(admin.pubkey())
        .instruction();

    context
        .send_transaction_with_signers_with_transaction_result(instruction, &[&admin], true, None)
        .expect("SetWithdrawFeeConfig should succeed");

    let config = get_withdraw_fee_config(&mut context, &withdraw_fee_config);
    assert_eq!(config.bump, bump);
    assert_eq!(config.fee, TEST_WITHDRAW_FEE);
    assert_eq!(config.treasury, admin.pubkey());
    assert_eq!(
        config.treasury_token_account,
        get_associated_token_address(&admin.pubkey(), &mint.pubkey())
    );
}

// The operator sends this on every deposit, so the latest AllowMint fee and the
// current admin always win over what an earlier deposit wrote.
#[test]
fn test_set_withdraw_fee_config_overwrites() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let new_treasury = Pubkey::new_unique();
    let new_fee = TEST_WITHDRAW_FEE * 3;

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_fee_config, _) = find_withdraw_fee_config_pda(&mint.pubkey());

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(withdraw_fee_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .treasury(admin.pubkey())
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("First SetWithdrawFeeConfig should succeed");

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(withdraw_fee_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(new_fee)
        .treasury(new_treasury)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("Second SetWithdrawFeeConfig should overwrite");

    let config = get_withdraw_fee_config(&mut context, &withdraw_fee_config);
    assert_eq!(config.fee, new_fee);
    assert_eq!(config.treasury, new_treasury);
    assert_eq!(
        config.treasury_token_account,
        get_associated_token_address(&new_treasury, &mint.pubkey())
    );
}

// Anyone can send lamports to the PDA before it exists, which would make a
// plain CreateAccount fail and wedge every deposit of the mint.
#[test]
fn test_set_withdraw_fee_config_prefunded_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_fee_config, _) = find_withdraw_fee_config_pda(&mint.pubkey());
    context.create_account(&withdraw_fee_config, &SYSTEM_PROGRAM_ID, vec![], 1_000);

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(withdraw_fee_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .treasury(admin.pubkey())
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("SetWithdrawFeeConfig should succeed on a pre-funded PDA");

    let config = get_withdraw_fee_config(&mut context, &withdraw_fee_config);
    assert_eq!(config.fee, TEST_WITHDRAW_FEE);
}

// Anyone else could otherwise drop the fee to 1 or redirect it to themselves.
#[test]
fn test_set_withdraw_fee_config_not_mint_authority() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let attacker = Keypair::new();
    let mint = Keypair::new();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&attacker.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_fee_config, _) = find_withdraw_fee_config_pda(&mint.pubkey());

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(attacker.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(withdraw_fee_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(1)
        .treasury(attacker.pubkey())
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&attacker]);

    assert_program_error(result, INVALID_MINT_AUTHORITY_ERROR);
}

// Custom, not a builtin: the deposit transaction carries this instruction, and
// the operator retries a mint forever on InvalidAccountData.
#[test]
fn test_set_withdraw_fee_config_wrong_address() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let wrong_withdraw_fee_config = Pubkey::new_unique();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(wrong_withdraw_fee_config)
        .system_program(SYSTEM_PROGRAM_ID)
        .fee(TEST_WITHDRAW_FEE)
        .treasury(admin.pubkey())
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_FEE_CONFIG_ERROR);
}

// Checked by address so the error is custom. Letting the CPI find out would
// surface IncorrectProgramId, which the operator also retries forever.
#[test]
fn test_set_withdraw_fee_config_wrong_system_program() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let fake_system_program = Pubkey::new_unique();

    set_mint_with_authority(&mut context, &mint.pubkey(), COption::Some(admin.pubkey()));
    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (withdraw_fee_config, _) = find_withdraw_fee_config_pda(&mint.pubkey());

    let instruction = SetWithdrawFeeConfigBuilder::new()
        .authority(admin.pubkey())
        .mint(mint.pubkey())
        .withdraw_fee_config(withdraw_fee_config)
        .system_program(fake_system_program)
        .fee(TEST_WITHDRAW_FEE)
        .treasury(admin.pubkey())
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_SYSTEM_PROGRAM_ERROR);
}
