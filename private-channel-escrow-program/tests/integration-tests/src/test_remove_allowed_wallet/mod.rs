use crate::{
    pda_utils::{find_allowed_wallet_pda, find_event_authority_pda},
    state_utils::{
        assert_get_or_add_allowed_wallet, assert_get_or_create_instance,
        assert_get_or_remove_allowed_wallet,
    },
    utils::{
        assert_program_error, TestContext, INVALID_ACCOUNT_DATA_ERROR, INVALID_ADMIN_ERROR,
        INVALID_ALLOWED_WALLET_PDA_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::RemoveAllowedWalletBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;

#[test]
fn test_remove_allowed_wallet_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_add_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &wallet.pubkey(),
        false,
        false,
        false,
    )
    .expect("AddOrUpdateAllowedWallet should succeed");

    assert_get_or_remove_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &wallet.pubkey(),
        true,
    )
    .expect("RemoveAllowedWallet should succeed");
}

#[test]
fn test_remove_allowed_wallet_nonexistent() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_wallet_pda, _) = find_allowed_wallet_pda(&instance_pda, &wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_remove_allowed_wallet_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_wallet_pda, _) = assert_get_or_add_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &wallet.pubkey(),
        false,
        false,
        false,
    )
    .expect("AddOrUpdateAllowedWallet should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_remove_allowed_wallet_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_wallet_pda, _) = assert_get_or_add_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &wallet.pubkey(),
        false,
        false,
        false,
    )
    .expect("AddOrUpdateAllowedWallet should succeed");

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new_readonly(wallet.pubkey(), false), // wallet
        AccountMeta::new(allowed_wallet_pda, false),    // allowed_wallet_pda (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let data = vec![10]; // discriminator for RemoveAllowedWallet

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

// Removing one wallet must not be able to close another wallet's entry.
#[test]
fn test_remove_allowed_wallet_other_wallets_pda_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let other_wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_add_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &wallet.pubkey(),
        false,
        false,
        false,
    )
    .expect("AddOrUpdateAllowedWallet should succeed");

    let (other_allowed_wallet_pda, _) = assert_get_or_add_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &other_wallet.pubkey(),
        false,
        false,
        false,
    )
    .expect("AddOrUpdateAllowedWallet should succeed");

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = RemoveAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(other_allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ALLOWED_WALLET_PDA_ERROR);
}
