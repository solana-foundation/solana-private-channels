use crate::{
    pda_utils::find_event_authority_pda,
    state_utils::{
        assert_get_or_add_operator, assert_get_or_create_instance, assert_get_or_reset_smt_root,
    },
    utils::{
        assert_program_error, TestContext, INVALID_OPERATOR_ERROR,
        MISSING_REQUIRED_SIGNATURE_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        UNEXPECTED_TREE_INDEX_ERROR,
    },
};

use private_channel_escrow_program_client::{instructions::ResetSmtRootBuilder, Instance};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};

#[test]
fn test_reset_smt_root_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    assert_get_or_reset_smt_root(&mut context, &operator, &instance_pda, &operator_pda, true)
        .expect("ResetSmtRoot should succeed");
}

#[test]
fn test_reset_smt_root_not_operator() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let fake_operator = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (_operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Create another instance for fake operator
    let instance_seed_2 = Keypair::new();
    let (instance_pda_2, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed_2, false, false)
            .expect("CreateInstance should succeed");
    let (fake_operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda_2,
        &fake_operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Try to reset SMT root with fake operator (wrong instance) - this should fail
    // We need to manually create the instruction since our helper expects success
    let (event_authority_pda, _) = find_event_authority_pda();

    context
        .airdrop_if_required(&fake_operator.pubkey(), 1_000_000_000)
        .unwrap();

    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(fake_operator.pubkey(), true), // operator (signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(fake_operator_pda, false), // operator_pda (wrong one)
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![8]; // discriminator for ResetSmtRoot
    data.extend_from_slice(&0u64.to_le_bytes()); // expected_current_tree_index

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&fake_operator]);
    assert_program_error(result, INVALID_OPERATOR_ERROR);
}

#[test]
fn test_reset_smt_root_operator_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where operator is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(operator.pubkey(), false), // operator (NOT signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(operator_pda, false), // operator_pda
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![8]; // discriminator for ResetSmtRoot
    data.extend_from_slice(&0u64.to_le_bytes()); // expected_current_tree_index

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_reset_smt_root_updates_nonce() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator1 = Keypair::new();
    let operator2 = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda1, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator1.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    let (operator_pda2, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator2.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // First reset with operator1
    assert_get_or_reset_smt_root(
        &mut context,
        &operator1,
        &instance_pda,
        &operator_pda1,
        false,
    )
    .expect("ResetSmtRoot should succeed");

    // Second reset with operator2 should increment nonce
    assert_get_or_reset_smt_root(
        &mut context,
        &operator2,
        &instance_pda,
        &operator_pda2,
        false,
    )
    .expect("Second ResetSmtRoot should succeed");
}

/// A reset is not idempotent: every success advances current_tree_index. A
/// replay carrying the now-stale expected index must be rejected so an
/// ambiguously-confirmed rotation cannot skip a whole tree generation.
#[test]
fn test_reset_smt_root_replay_with_stale_index_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // First reset lands: current_tree_index 0 -> 1.
    assert_get_or_reset_smt_root(&mut context, &operator, &instance_pda, &operator_pda, false)
        .expect("first ResetSmtRoot should succeed");

    context.svm.expire_blockhash();

    // Replay the same reset, still carrying expected_current_tree_index = 0.
    // The instance is already at 1, so the precondition must reject it.
    let (event_authority_pda, _) = find_event_authority_pda();
    let replay = ResetSmtRootBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .operator_pda(operator_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .expected_current_tree_index(0)
        .instruction();

    let result = context.send_transaction_with_signers(replay, &[&operator]);
    assert_program_error(result, UNEXPECTED_TREE_INDEX_ERROR);

    // The rejected replay must not have advanced the tree: still 1.
    let account = context
        .get_account(&instance_pda)
        .expect("Instance account should exist");
    let instance = Instance::from_bytes(&account.data).expect("Should deserialize instance");
    assert_eq!(instance.current_tree_index, 1);
}
