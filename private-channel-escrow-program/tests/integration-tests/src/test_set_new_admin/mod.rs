use crate::{
    pda_utils::find_event_authority_pda,
    smt_utils::ProcessorSMT,
    state_utils::{
        assert_get_or_add_operator, assert_get_or_allow_mint, assert_get_or_create_instance,
        assert_get_or_deposit, assert_get_or_release_funds, assert_get_or_set_new_admin,
    },
    utils::{
        assert_program_error, set_mint, setup_test_balances, TestContext, ATA_PROGRAM_ID,
        INVALID_ACCOUNT_DATA_ERROR, INVALID_ADMIN_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::{AllowMintBuilder, SetNewAdminBuilder};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
    system_program::ID as SYSTEM_PROGRAM_ID,
};
use spl_token::ID as TOKEN_PROGRAM_ID;

#[test]
fn test_set_new_admin_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let new_admin = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_set_new_admin(&mut context, &admin, &instance_pda, &new_admin, true)
        .expect("SetNewAdmin should succeed");
}

#[test]
fn test_set_new_admin_invalid_current_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let new_admin = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where current_admin is NOT marked as signer (but new_admin is)
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // current_admin (NOT signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(new_admin.pubkey(), true), // new_admin (signer)
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let data = vec![5]; // discriminator for SetNewAdmin

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&new_admin]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_set_new_admin_invalid_current_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let new_admin = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = SetNewAdminBuilder::new()
        .payer(context.payer.pubkey())
        .current_admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .new_admin(new_admin.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin, &new_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_set_new_admin_invalid_instance_account_owner() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let new_admin = Keypair::new();

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let fake_instance = Keypair::new();
    context
        .airdrop_if_required(&fake_instance.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = SetNewAdminBuilder::new()
        .payer(context.payer.pubkey())
        .current_admin(admin.pubkey())
        .instance(fake_instance.pubkey())
        .new_admin(new_admin.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&admin, &new_admin]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_set_new_admin_invalid_new_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let new_admin = Keypair::new();

    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();
    context
        .airdrop_if_required(&new_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where new_admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), true), // current_admin (signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(new_admin.pubkey(), false), // new_admin (NOT signer)
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let data = vec![5]; // discriminator for SetNewAdmin

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_set_new_admin_old_admin_locked_out() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let new_admin = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_set_new_admin(&mut context, &admin, &instance_pda, &new_admin, false)
        .expect("SetNewAdmin should succeed");

    // Old admin tries to allow a mint — must be rejected
    let (event_authority_pda, _) = find_event_authority_pda();
    let (allowed_mint_pda, bump) =
        crate::pda_utils::find_allowed_mint_pda(&instance_pda, &mint.pubkey());
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

    context
        .airdrop_if_required(&admin.pubkey(), 1_000_000_000)
        .unwrap();

    let result = context.send_transaction_with_signers(instruction, &[&admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_set_new_admin_existing_operators_still_valid() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let new_admin = Keypair::new();
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
        500_000,
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

    // Transfer admin — operator PDA is keyed to instance, not admin, so it survives
    assert_get_or_set_new_admin(&mut context, &admin, &instance_pda, &new_admin, false)
        .expect("SetNewAdmin should succeed");

    let mut smt = ProcessorSMT::new();
    let (_, sibling_proofs) = smt.generate_exclusion_proof_for_verification(1);
    smt.insert(1);
    let new_root = smt.current_root();

    assert_get_or_release_funds(
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
    )
    .expect("Operator should still be valid after admin change");
}
