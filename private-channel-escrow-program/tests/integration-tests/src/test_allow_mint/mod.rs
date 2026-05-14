use crate::{
    pda_utils::{find_allowed_mint_pda, find_event_authority_pda},
    state_utils::{assert_get_or_allow_mint, assert_get_or_create_instance},
    utils::{
        assert_program_error, set_mint, set_mint_2022_basic, set_mint_2022_with_pausable,
        set_mint_2022_with_permanent_delegate, set_mint_2022_with_transfer_hook, TestContext,
        INVALID_ACCOUNT_DATA_ERROR, INVALID_ADMIN_ERROR, INVALID_ALLOWED_MINT_ERROR,
        MISSING_REQUIRED_SIGNATURE_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
        TRANSFER_HOOK_NOT_ALLOWED_ERROR,
    },
};
use private_channel_escrow_program_client::instructions::AllowMintBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_program::ID as SYSTEM_PROGRAM_ID,
};
use spl_associated_token_account::ID as ATA_PROGRAM_ID;
use spl_token::ID as TOKEN_PROGRAM_ID;

#[test]
fn test_allow_mint_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        true,
    )
    .expect("AllowMint should succeed");
}

#[test]
fn test_allow_mint_duplicate() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
    .expect("First AllowMint should succeed");

    // Second allow mint with same mint should fail
    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = AllowMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    // Should fail because account already exists
    assert!(result.is_err(), "Duplicate allow mint should fail");
}

#[test]
fn test_allow_mint_invalid_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let wrong_pda = Pubkey::new_unique();
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = AllowMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(wrong_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(1) // Wrong bump
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ALLOWED_MINT_ERROR);
}

#[test]
fn test_allow_mint_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    // Create instruction where admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new_readonly(mint.pubkey(), false), // mint
        AccountMeta::new(allowed_mint_pda, false),      // allowed_mint (writable)
        AccountMeta::new(instance_ata, false),          // instance_ata (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false), // token_program
        AccountMeta::new_readonly(ATA_PROGRAM_ID, false), // associated_token_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![1]; // discriminator for AllowMint
    data.push(bump);

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_allow_mint_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = AllowMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_allow_mint_invalid_instance_account_owner() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let fake_instance = Keypair::new();
    context
        .airdrop_if_required(&fake_instance.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&fake_instance.pubkey(), &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &fake_instance.pubkey(),
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = AllowMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(fake_instance.pubkey())
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

// ============================================================================
// Token 2022 Tests
// ============================================================================

#[test]
fn test_allow_mint_token_2022_basic_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint_2022_basic(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        true,
    )
    .expect("AllowMint with basic Token 2022 should succeed");
}

#[test]
fn test_allow_mint_token_2022_permanent_delegate_accepted() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint_2022_with_permanent_delegate(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        true,
    )
    .expect("AllowMint should succeed for a Token-2022 mint with a permanent delegate");
}

#[test]
fn test_allow_mint_token_2022_pausable_accepted() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    let authority = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint_2022_with_pausable(&mut context, &mint.pubkey(), &authority.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        true,
    )
    .expect("AllowMint should succeed for a pausable Token-2022 mint");
}

#[test]
fn test_allow_mint_token_2022_transfer_hook_blocked() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();
    // Hook program id is arbitrary — the check fires on the extension's
    // presence, not on whether the hook program exists on-chain.
    let hook_program_id = Pubkey::new_unique();

    let instance_seed = Keypair::new();

    set_mint_2022_with_transfer_hook(&mut context, &mint.pubkey(), &hook_program_id);

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_2022_PROGRAM_ID,
    );

    let instruction = AllowMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_2022_PROGRAM_ID)
        .associated_token_program(spl_associated_token_account::ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, TRANSFER_HOOK_NOT_ALLOWED_ERROR);
}
