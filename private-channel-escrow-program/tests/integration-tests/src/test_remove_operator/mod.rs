use crate::{
    pda_utils::{find_event_authority_pda, find_operator_pda},
    smt_utils::ProcessorSMT,
    state_utils::{
        assert_get_or_add_operator, assert_get_or_allow_mint, assert_get_or_create_instance,
        assert_get_or_deposit, assert_get_or_release_funds, assert_get_or_remove_operator,
    },
    utils::{
        assert_program_error, set_mint, setup_test_balances, TestContext,
        INVALID_ACCOUNT_DATA_ERROR, INVALID_ADMIN_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::RemoveOperatorBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
    system_program::ID as SYSTEM_PROGRAM_ID,
};
use spl_token::ID as TOKEN_PROGRAM_ID;

#[test]
fn test_remove_operator_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator_wallet = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator_wallet.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    assert_get_or_remove_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator_wallet.pubkey(),
        &operator_pda,
        true,
    )
    .expect("RemoveOperator should succeed");
}

#[test]
fn test_remove_operator_nonexistent() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator_wallet = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (operator_pda, _) = find_operator_pda(&instance_pda, &operator_wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveOperatorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .operator(operator_wallet.pubkey())
        .operator_pda(operator_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    // Should fail because operator account doesn't exist
    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_remove_operator_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator_wallet = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator_wallet.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new_readonly(operator_wallet.pubkey(), false), // operator
        AccountMeta::new(operator_pda, false),          // operator_pda (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let data = vec![4]; // discriminator for RemoveOperator

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_remove_operator_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let operator_wallet = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator_wallet.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveOperatorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .operator(operator_wallet.pubkey())
        .operator_pda(operator_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_remove_operator_invalid_instance_account_owner() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator_wallet = Keypair::new();

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    // Will be system account so will have invalid account owner
    // We don't even need to create an instance or an operator, as this check is at the beginning of the instruction
    let fake_instance = Keypair::new();
    context
        .airdrop_if_required(&fake_instance.pubkey(), 1_000_000_000)
        .unwrap();

    let (operator_pda, _) = find_operator_pda(&fake_instance.pubkey(), &operator_wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveOperatorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(fake_instance.pubkey())
        .operator(operator_wallet.pubkey())
        .operator_pda(operator_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_remove_operator_prevents_release_funds() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        1_000_000,
        1_000_000,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        1_000_000,
        None,
        false,
    )
    .expect("Deposit should succeed");

    assert_get_or_remove_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        &operator_pda,
        false,
    )
    .expect("RemoveOperator should succeed");

    // Operator PDA is now closed — release_funds must fail
    let mut smt = ProcessorSMT::new();
    let (_, sibling_proofs) = smt.generate_exclusion_proof_for_verification(1);
    smt.insert(1);
    let new_root = smt.current_root();

    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        500_000,
        &user.pubkey(),
        new_root,
        1,
        sibling_proofs,
        false,
    );

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}
