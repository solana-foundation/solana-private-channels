use crate::{
    pda_utils::{find_allowed_mint_pda, find_event_authority_pda},
    state_utils::{
        assert_get_or_allow_mint, assert_get_or_block_mint, assert_get_or_create_instance,
        assert_get_or_deposit,
    },
    utils::{
        assert_program_error, set_mint, setup_test_balances, TestContext, ATA_PROGRAM_ID,
        INVALID_ACCOUNT_DATA_ERROR, INVALID_ADMIN_ERROR, INVALID_ALLOWED_MINT_ERROR,
        MISSING_REQUIRED_SIGNATURE_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::{BlockMintBuilder, DepositBuilder};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_program::ID as SYSTEM_PROGRAM_ID,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;

#[test]
fn test_block_mint_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    assert_get_or_block_mint(
        &mut context,
        &admin,
        &instance_pda,
        &allowed_mint_pda,
        &mint.pubkey(),
        true,
    )
    .expect("BlockMint should succeed");
}

#[test]
fn test_block_mint_allowed_mint_not_found() {
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

    // Try to block a mint that was never allowed
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .allowed_mint(allowed_mint_pda)
        .mint(mint.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_block_mint_invalid_pda() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    // Setup mint
    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (_, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let wrong_allowed_mint_pda = Pubkey::new_unique();
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .allowed_mint(wrong_allowed_mint_pda)
        .mint(mint.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_block_mint_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new_readonly(mint.pubkey(), false), // mint
        AccountMeta::new(allowed_mint_pda, false),      // allowed_mint (writable)
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // system_program (not used but kept)
        AccountMeta::new_readonly(event_authority_pda, false),               // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let data = vec![2]; // discriminator for BlockMint

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_block_mint_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .allowed_mint(allowed_mint_pda)
        .mint(mint.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_block_mint_invalid_instance_account_owner() {
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

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&fake_instance.pubkey(), &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(fake_instance.pubkey())
        .allowed_mint(allowed_mint_pda)
        .mint(mint.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_block_mint_mismatched_mint() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::from_base58_string(
        "ejVzK9CfpYDjs24J1DysZCL2jGqvLRACBE8fLTE39K1y8rnJQDCaPpkaG9Wfu7mPf9P4C4d7Xno1X7JWx19XavE",
    );
    let other_mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());
    set_mint(&mut context, &other_mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    // Try to block with a different mint than what was allowed
    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .allowed_mint(allowed_mint_pda)
        .mint(other_mint.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ALLOWED_MINT_ERROR);
}

#[test]
fn test_block_mint_prevents_deposit() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        1_000_000,
        0,
    );

    assert_get_or_block_mint(
        &mut context,
        &admin,
        &instance_pda,
        &allowed_mint_pda,
        &mint.pubkey(),
        false,
    )
    .expect("BlockMint should succeed");

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();
    let user_ata = get_associated_token_address_with_program_id(
        &user.pubkey(),
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );
    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(1_000_000)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_allow_block_allow_cycle() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed");

    assert_get_or_block_mint(
        &mut context,
        &admin,
        &instance_pda,
        &allowed_mint_pda,
        &mint.pubkey(),
        false,
    )
    .expect("BlockMint should succeed");

    context.warp_to_slot(100);

    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.pubkey(),
        false,
        false,
    )
    .expect("Re-AllowMint after block should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        1_000_000,
        0,
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
    .expect("Deposit after re-allow should succeed");
}
