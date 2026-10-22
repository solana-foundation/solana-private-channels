use crate::{
    assertions::{
        assert_account_lamports, assert_account_not_exists, assert_allowed_wallet_account,
    },
    pda_utils::{find_allowed_wallet_pda, find_event_authority_pda},
    state_utils::{assert_get_or_add_allowed_wallet, assert_get_or_create_instance},
    utils::{
        assert_program_error, TestContext, INVALID_ADMIN_ERROR, INVALID_ALLOWED_WALLET_PDA_ERROR,
        INVALID_INSTRUCTION_DATA_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::AddOrUpdateAllowedWalletBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;

#[test]
fn test_add_or_update_allowed_wallet_success() {
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
        true,
    )
    .expect("AddOrUpdateAllowedWallet should succeed");
}

// A second call on a listed wallet rewrites its flags in place, keeping the
// account and its rent.
#[test]
fn test_add_or_update_allowed_wallet_updates_existing() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_wallet_pda, bump) = assert_get_or_add_allowed_wallet(
        &mut context,
        &admin,
        &instance_pda,
        &wallet.pubkey(),
        false,
        false,
        false,
    )
    .expect("Adding the wallet should succeed");

    let lamports_before = context.get_account(&allowed_wallet_pda).unwrap().lamports;
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddOrUpdateAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .block_deposits(true)
        .block_withdrawals(false)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("Updating the wallet should succeed");

    assert_allowed_wallet_account(&mut context, &allowed_wallet_pda, bump, true, false);
    assert_account_lamports(&mut context, &allowed_wallet_pda, lamports_before);
}

// An entry blocked both ways is the same as no entry, so RemoveAllowedWallet is
// the way to get there.
#[test]
fn test_add_or_update_allowed_wallet_both_blocked_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_wallet_pda, bump) = find_allowed_wallet_pda(&instance_pda, &wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddOrUpdateAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .block_deposits(true)
        .block_withdrawals(true)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
    assert_account_not_exists(&mut context, &allowed_wallet_pda);
}

#[test]
fn test_add_or_update_allowed_wallet_invalid_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let wrong_pda = Pubkey::new_unique();
    let (_, bump) = find_allowed_wallet_pda(&instance_pda, &wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddOrUpdateAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(wrong_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .block_deposits(false)
        .block_withdrawals(false)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ALLOWED_WALLET_PDA_ERROR);
}

#[test]
fn test_add_or_update_allowed_wallet_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_wallet_pda, bump) = find_allowed_wallet_pda(&instance_pda, &wallet.pubkey());
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

    // Discriminator for AddOrUpdateAllowedWallet, then bump, block_deposits, block_withdrawals
    let data = vec![9, bump, 0, 0];

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_add_or_update_allowed_wallet_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let wallet = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_wallet_pda, bump) = find_allowed_wallet_pda(&instance_pda, &wallet.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddOrUpdateAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .wallet(wallet.pubkey())
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .block_deposits(false)
        .block_withdrawals(false)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}
