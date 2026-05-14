use crate::{
    pda_utils::{find_event_authority_pda, find_instance_pda},
    state_utils::assert_get_or_create_instance,
    utils::{
        assert_program_error, TestContext, INCORRECT_PROGRAM_ID_ERROR,
        INVALID_EVENT_AUTHORITY_ERROR, INVALID_INSTRUCTION_DATA_ERROR,
        MISSING_REQUIRED_SIGNATURE_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::CreateInstanceBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_program::ID as SYSTEM_PROGRAM_ID,
};

#[test]
fn test_create_instance_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();

    let instance_seed = Keypair::new();

    assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, true)
        .expect("CreateInstance should succeed");
}

#[test]
fn test_create_instance_duplicate() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
        .expect("First CreateInstance should succeed");

    // Second creation with same instance_seed should fail
    let admin2 = Keypair::new();
    context
        .airdrop_if_required(&admin2.pubkey(), 1_000_000_000)
        .unwrap();

    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = CreateInstanceBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin2.pubkey())
        .instance_seed(instance_seed.pubkey())
        .instance(instance_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin2, &instance_seed]);

    // Should fail because account already exists
    assert!(result.is_err(), "Duplicate instance creation should fail");
}

#[test]
fn test_create_instance_invalid_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let wrong_pda = Pubkey::new_unique();
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = CreateInstanceBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance_seed(instance_seed.pubkey())
        .instance(wrong_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(1) // Wrong bump
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin, &instance_seed]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

#[test]
fn test_create_instance_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where admin is NOT marked as signer to test program validation
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new_readonly(instance_seed.pubkey(), true), // instance_seed (signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    // Create instruction data in Borsh format: discriminator(1) + bump(1)
    let mut data = vec![0]; // discriminator for CreateInstance
    data.push(bump); // bump

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&instance_seed]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_create_instance_invalid_event_authority() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());
    let wrong_event_authority = Pubkey::new_unique(); // Not the real event authority PDA

    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), true), // admin (signer)
        AccountMeta::new_readonly(instance_seed.pubkey(), true), // instance_seed (signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(wrong_event_authority, false), // event_authority (WRONG)
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![0]; // discriminator for CreateInstance
    data.push(bump); // bump

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&admin, &instance_seed]);

    assert_program_error(result, INVALID_EVENT_AUTHORITY_ERROR);
}

#[test]
fn test_create_instance_invalid_system_program() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let wrong_system_program = Pubkey::new_unique();

    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), true), // admin (signer)
        AccountMeta::new_readonly(instance_seed.pubkey(), true), // instance_seed (signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(wrong_system_program, false), // system_program (WRONG)
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![0]; // discriminator for CreateInstance
    data.push(bump); // bump

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&admin, &instance_seed]);

    assert_program_error(result, INCORRECT_PROGRAM_ID_ERROR);
}
