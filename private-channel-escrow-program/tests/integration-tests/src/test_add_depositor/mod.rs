use crate::{
    pda_utils::{find_depositor_pda, find_event_authority_pda},
    state_utils::{assert_get_or_add_depositor, assert_get_or_create_instance},
    utils::{
        assert_program_error, TestContext, INVALID_ACCOUNT_OWNER_ERROR, INVALID_ADMIN_ERROR,
        INVALID_DEPOSITOR_PDA_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::AddDepositorBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;

#[test]
fn test_add_depositor_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_add_depositor(
        &mut context,
        &admin,
        &instance_pda,
        &depositor_wallet.pubkey(),
        true,
    )
    .expect("AddDepositor should succeed");
}

#[test]
fn test_add_depositor_duplicate() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (depositor_pda, bump) = assert_get_or_add_depositor(
        &mut context,
        &admin,
        &instance_pda,
        &depositor_wallet.pubkey(),
        false,
    )
    .expect("First AddDepositor should succeed");

    // Without this the second transaction is byte-identical to the first and
    // gets dropped as an already-processed signature, so the program would
    // never run and this would pass whatever the handler does.
    context.svm.expire_blockhash();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddDepositorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .depositor(depositor_wallet.pubkey())
        .depositor_pda(depositor_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    // The entry already exists, so the PDA is no longer a system account.
    assert_program_error(result, INVALID_ACCOUNT_OWNER_ERROR);
}

#[test]
fn test_add_depositor_invalid_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let wrong_pda = Pubkey::new_unique();
    let (_, bump) = find_depositor_pda(&instance_pda, &depositor_wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddDepositorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .depositor(depositor_wallet.pubkey())
        .depositor_pda(wrong_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_DEPOSITOR_PDA_ERROR);
}

#[test]
fn test_add_depositor_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (depositor_pda, bump) = find_depositor_pda(&instance_pda, &depositor_wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new_readonly(depositor_wallet.pubkey(), false), // depositor
        AccountMeta::new(depositor_pda, false),         // depositor_pda (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let data = vec![9, bump]; // discriminator for AddDepositor, then bump

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_add_depositor_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (depositor_pda, bump) = find_depositor_pda(&instance_pda, &depositor_wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddDepositorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .depositor(depositor_wallet.pubkey())
        .depositor_pda(depositor_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}
