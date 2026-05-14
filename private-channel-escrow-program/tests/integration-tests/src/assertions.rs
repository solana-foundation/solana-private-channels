use crate::smt_utils::EMPTY_TREE_ROOT;
use crate::utils::get_token_balance;
use crate::utils::{TestContext, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID};
use private_channel_escrow_program_client::{accounts::Instance, AllowedMint, Operator};
use solana_program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use spl_token::state::Account as TokenAccount;

pub fn assert_instance_account(
    context: &mut TestContext,
    instance_pda: &Pubkey,
    expected_admin: &Pubkey,
    expected_bump: u8,
    expected_instance_seed: &Pubkey,
    expected_current_tree_index: u64,
) {
    let account = context
        .get_account(instance_pda)
        .expect("Instance account should exist");

    assert_eq!(account.owner, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);

    let instance =
        Instance::from_bytes(&account.data).expect("Should deserialize instance account");

    assert_eq!(instance.admin, *expected_admin);
    assert_eq!(instance.bump, expected_bump);
    assert_eq!(instance.instance_seed, *expected_instance_seed);
    assert_eq!(instance.current_tree_index, expected_current_tree_index);
}

pub fn assert_allow_mint_account(
    context: &mut TestContext,
    allowed_mint_pda: &Pubkey,
    expected_bump: u8,
) {
    let account = context
        .get_account(allowed_mint_pda)
        .expect("Allowed mint account should exist");

    assert_eq!(account.owner, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);

    let allowed_mint =
        AllowedMint::from_bytes(&account.data).expect("Should deserialize allowed mint account");

    assert_eq!(allowed_mint.bump, expected_bump);
}

pub fn assert_block_mint_account(
    context: &mut TestContext,
    allowed_mint_pda: &Pubkey,
    payer: &Pubkey,
    previous_lamports_balance: u64,
) {
    assert_account_not_exists(context, allowed_mint_pda);
    assert_account_lamports_gt(context, payer, previous_lamports_balance);
}

pub fn assert_account_exists(context: &mut TestContext, pubkey: &Pubkey) {
    let account = context.get_account(pubkey);
    assert!(account.is_some(), "Account should exist");
}

pub fn assert_account_not_exists(context: &mut TestContext, pubkey: &Pubkey) {
    let account = context.get_account(pubkey);
    assert!(account.is_none(), "Account should not exist");
}

pub fn assert_account_lamports(context: &mut TestContext, pubkey: &Pubkey, expected_lamports: u64) {
    let account = context.get_account(pubkey).expect("Account should exist");
    assert_eq!(account.lamports, expected_lamports);
}

pub fn assert_account_lamports_gt(
    context: &mut TestContext,
    pubkey: &Pubkey,
    expected_lamports: u64,
) {
    let account = context.get_account(pubkey).expect("Account should exist");
    assert!(account.lamports > expected_lamports);
}

pub fn assert_account_owner(context: &mut TestContext, pubkey: &Pubkey, expected_owner: &Pubkey) {
    let account = context.get_account(pubkey).expect("Account should exist");
    assert_eq!(account.owner, *expected_owner);
}

pub fn assert_token_account(
    context: &mut TestContext,
    token_account: &Pubkey,
    expected_mint: &Pubkey,
    expected_owner: &Pubkey,
) {
    let account = context
        .get_account(token_account)
        .expect("Account should exist");
    let token_account = TokenAccount::unpack(&account.data).unwrap();
    assert_eq!(token_account.mint, *expected_mint);
    assert_eq!(token_account.owner, *expected_owner);
}

pub fn assert_add_operator_account(
    context: &mut TestContext,
    operator_pda: &Pubkey,
    expected_bump: u8,
) {
    let account = context
        .get_account(operator_pda)
        .expect("Operator account should exist");

    assert_eq!(account.owner, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);

    let operator =
        Operator::from_bytes(&account.data).expect("Should deserialize operator account");

    assert_eq!(operator.bump, expected_bump);
}

pub fn assert_remove_operator_account(
    context: &mut TestContext,
    operator_pda: &Pubkey,
    payer: &Pubkey,
    previous_lamports_balance: u64,
) {
    assert_account_not_exists(context, operator_pda);
    assert_account_lamports_gt(context, payer, previous_lamports_balance);
}

pub fn assert_instance_admin_updated(
    context: &mut TestContext,
    instance_pda: &Pubkey,
    expected_new_admin: &Pubkey,
) {
    let account = context
        .get_account(instance_pda)
        .expect("Instance account should exist");

    assert_eq!(account.owner, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);

    let instance =
        Instance::from_bytes(&account.data).expect("Should deserialize instance account");

    assert_eq!(instance.admin, *expected_new_admin);
}

pub fn assert_deposit_balances(
    context: &mut TestContext,
    user_ata: &Pubkey,
    instance_ata: &Pubkey,
    user_balance_before: u64,
    instance_balance_before: u64,
    deposit_amount: u64,
) {
    let user_balance_after = get_token_balance(context, user_ata);
    let instance_balance_after = get_token_balance(context, instance_ata);

    assert_eq!(
        user_balance_after,
        user_balance_before - deposit_amount,
        "User balance should decrease by deposit amount"
    );
    assert_eq!(
        instance_balance_after,
        instance_balance_before + deposit_amount,
        "Instance balance should increase by deposit amount"
    );
}

pub fn assert_release_funds_balances(
    context: &mut TestContext,
    user_ata: &Pubkey,
    instance_ata: &Pubkey,
    user_balance_before: u64,
    instance_balance_before: u64,
    release_amount: u64,
) {
    let user_balance_after = get_token_balance(context, user_ata);
    let instance_balance_after = get_token_balance(context, instance_ata);

    assert_eq!(
        user_balance_after,
        user_balance_before + release_amount,
        "User balance should increase by release amount"
    );
    assert_eq!(
        instance_balance_after,
        instance_balance_before - release_amount,
        "Instance balance should decrease by deposit amount"
    );
}

pub fn assert_instance_smt_reset(
    context: &mut TestContext,
    instance_pda: &Pubkey,
    previous_tree_index: u64,
) {
    let account = context
        .get_account(instance_pda)
        .expect("Instance account should exist");

    assert_eq!(account.owner, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID);

    let instance =
        Instance::from_bytes(&account.data).expect("Should deserialize instance account");

    assert_eq!(
        instance.withdrawal_transactions_root, EMPTY_TREE_ROOT,
        "SMT root should be reset to EMPTY_TREE_ROOT"
    );

    assert_eq!(
        instance.current_tree_index,
        previous_tree_index + 1,
        "Tree index should be incremented after reset"
    );
}
