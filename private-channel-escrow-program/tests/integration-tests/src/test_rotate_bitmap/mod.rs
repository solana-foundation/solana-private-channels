use crate::{
    pda_utils::{find_event_authority_pda, find_withdrawal_bitmap_pda},
    state_utils::{
        assert_get_or_add_operator, assert_get_or_create_instance, assert_get_or_rotate_bitmap,
    },
    utils::{
        assert_program_error, TestContext, INVALID_OPERATOR_ERROR, INVALID_WITHDRAWAL_BITMAP_ERROR,
        MISSING_REQUIRED_SIGNATURE_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        UNEXPECTED_GENERATION_ERROR,
    },
};

use private_channel_escrow_program_client::{instructions::RotateBitmapBuilder, WithdrawalBitmap};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};

#[test]
fn test_rotate_bitmap_success() {
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

    assert_get_or_rotate_bitmap(&mut context, &operator, &instance_pda, &operator_pda, true)
        .expect("RotateBitmap should succeed");
}

#[test]
fn test_rotate_bitmap_not_operator() {
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

    // Try to rotate with fake operator (wrong instance) - this should fail
    // We need to manually create the instruction since our helper expects success
    let (event_authority_pda, _) = find_event_authority_pda();
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);

    context
        .airdrop_if_required(&fake_operator.pubkey(), 1_000_000_000)
        .unwrap();

    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(fake_operator.pubkey(), true), // operator (signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new(withdrawal_bitmap_pda, false), // withdrawal_bitmap (writable)
        AccountMeta::new_readonly(fake_operator_pda, false), // operator_pda (wrong one)
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![8]; // discriminator for RotateBitmap
    data.extend_from_slice(&0u64.to_le_bytes()); // expected_generation

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&fake_operator]);
    assert_program_error(result, INVALID_OPERATOR_ERROR);
}

#[test]
fn test_rotate_bitmap_operator_not_signer() {
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
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);

    // Create instruction where operator is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(operator.pubkey(), false), // operator (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new(withdrawal_bitmap_pda, false), // withdrawal_bitmap (writable)
        AccountMeta::new_readonly(operator_pda, false), // operator_pda
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![8]; // discriminator for RotateBitmap
    data.extend_from_slice(&0u64.to_le_bytes()); // expected_generation

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_rotate_bitmap_advances_generation() {
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

    // First rotation with operator1
    assert_get_or_rotate_bitmap(
        &mut context,
        &operator1,
        &instance_pda,
        &operator_pda1,
        false,
    )
    .expect("RotateBitmap should succeed");

    // Second rotation with operator2 should advance the generation again
    assert_get_or_rotate_bitmap(
        &mut context,
        &operator2,
        &instance_pda,
        &operator_pda2,
        false,
    )
    .expect("Second RotateBitmap should succeed");
}

/// A rotation is not idempotent: every success advances the generation. A
/// replay carrying the now-stale expected generation must be rejected so an
/// ambiguously-confirmed rotation cannot skip a whole generation of nonces.
#[test]
fn test_rotate_bitmap_replay_with_stale_generation_rejected() {
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

    // First rotation lands: generation 0 -> 1.
    assert_get_or_rotate_bitmap(&mut context, &operator, &instance_pda, &operator_pda, false)
        .expect("first RotateBitmap should succeed");

    context.svm.expire_blockhash();

    // Replay the same rotation, still carrying expected_generation = 0.
    // The bitmap is already at 1, so the precondition must reject it.
    let (event_authority_pda, _) = find_event_authority_pda();
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);
    let replay = RotateBitmapBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .operator_pda(operator_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .expected_generation(0)
        .instruction();

    let result = context.send_transaction_with_signers(replay, &[&operator]);
    assert_program_error(result, UNEXPECTED_GENERATION_ERROR);

    // The rejected replay must not have advanced the generation: still 1.
    let account = context
        .get_account(&withdrawal_bitmap_pda)
        .expect("Withdrawal bitmap account should exist");
    let bitmap = WithdrawalBitmap::from_bytes(&account.data).expect("Should deserialize bitmap");
    assert_eq!(bitmap.generation, 1);
}

/// Rotating with another instance's bitmap would clear its consumed nonces and
/// advance its generation, stranding every nonce that instance still owes.
/// The PDA is re-derived from the instance, so the substitution must be refused.
#[test]
fn test_rotate_bitmap_foreign_bitmap_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let instance_seed = Keypair::new();
    let other_instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (other_instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &other_instance_seed, false, false)
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

    context
        .airdrop_if_required(&operator.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();
    let (other_bitmap_pda, _) = find_withdrawal_bitmap_pda(&other_instance_pda);

    let instruction = RotateBitmapBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(other_bitmap_pda)
        .operator_pda(operator_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .expected_generation(0)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&operator]);
    assert_program_error(result, INVALID_WITHDRAWAL_BITMAP_ERROR);

    // The other instance's bitmap must still be on generation 0.
    let account = context
        .get_account(&other_bitmap_pda)
        .expect("Withdrawal bitmap account should exist");
    let bitmap = WithdrawalBitmap::from_bytes(&account.data).expect("Should deserialize bitmap");
    assert_eq!(bitmap.generation, 0);
}
