use crate::{
    pda_utils::{find_depositor_pda, find_event_authority_pda},
    state_utils::{
        assert_get_or_add_depositor, assert_get_or_create_instance, assert_get_or_remove_depositor,
    },
    utils::{
        assert_program_error, TestContext, INVALID_ACCOUNT_DATA_ERROR, INVALID_ADMIN_ERROR,
        INVALID_DEPOSITOR_PDA_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::RemoveDepositorBuilder;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;

#[test]
fn test_remove_depositor_success() {
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
        false,
    )
    .expect("AddDepositor should succeed");

    assert_get_or_remove_depositor(
        &mut context,
        &admin,
        &instance_pda,
        &depositor_wallet.pubkey(),
        true,
    )
    .expect("RemoveDepositor should succeed");
}

#[test]
fn test_remove_depositor_nonexistent() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (depositor_pda, _) = find_depositor_pda(&instance_pda, &depositor_wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveDepositorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .depositor(depositor_wallet.pubkey())
        .depositor_pda(depositor_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_remove_depositor_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (depositor_pda, _) = assert_get_or_add_depositor(
        &mut context,
        &admin,
        &instance_pda,
        &depositor_wallet.pubkey(),
        false,
    )
    .expect("AddDepositor should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveDepositorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .depositor(depositor_wallet.pubkey())
        .depositor_pda(depositor_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

// Removing one wallet must not be able to close another wallet's entry.
#[test]
fn test_remove_depositor_other_wallets_pda_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let depositor_wallet = Keypair::new();
    let other_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_add_depositor(
        &mut context,
        &admin,
        &instance_pda,
        &depositor_wallet.pubkey(),
        false,
    )
    .expect("AddDepositor should succeed");

    let (other_depositor_pda, _) = assert_get_or_add_depositor(
        &mut context,
        &admin,
        &instance_pda,
        &other_wallet.pubkey(),
        false,
    )
    .expect("AddDepositor should succeed");

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveDepositorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .depositor(depositor_wallet.pubkey())
        .depositor_pda(other_depositor_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_DEPOSITOR_PDA_ERROR);
}
