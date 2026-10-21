use crate::{
    pda_utils::find_event_authority_pda,
    state_utils::{assert_get_or_create_instance, assert_get_or_set_whitelist_enabled},
    utils::{
        assert_program_error, TestContext, INSTANCE_LEN, INSTANCE_V1_LEN, INVALID_ADMIN_ERROR,
        MISSING_REQUIRED_SIGNATURE_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::{
    decode_instance, instructions::SetWhitelistEnabledBuilder,
};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;

#[test]
fn test_set_whitelist_enabled_is_absolute() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_set_whitelist_enabled(&mut context, &admin, &instance_pda, true, true, true)
        .expect("Enabling both whitelists should succeed");

    // Enabling again must leave them on; a toggle would turn them off. Without a fresh
    // blockhash the repeat is byte-identical and dropped before the program runs.
    context.svm.expire_blockhash();
    assert_get_or_set_whitelist_enabled(&mut context, &admin, &instance_pda, true, true, false)
        .expect("Enabling enabled whitelists should succeed");

    assert_get_or_set_whitelist_enabled(&mut context, &admin, &instance_pda, false, false, false)
        .expect("Disabling both whitelists should succeed");
}

// Either flag can be on while the other is off.
#[test]
fn test_set_whitelist_enabled_flags_are_independent() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    assert_get_or_set_whitelist_enabled(&mut context, &admin, &instance_pda, true, false, false)
        .expect("Whitelisting deposits only should succeed");

    assert_get_or_set_whitelist_enabled(&mut context, &admin, &instance_pda, false, true, false)
        .expect("Whitelisting withdrawals only should succeed");
}

// A 0.1.0 instance grows to the current layout, with the payer topping up its rent.
#[test]
fn test_set_whitelist_enabled_grows_a_0_1_0_instance() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    // Cut the instance down to its 0.1.0 bytes, as the program upgrade finds it.
    let mut instance_data = context.get_account_data(&instance_pda).unwrap();
    instance_data.truncate(INSTANCE_V1_LEN);
    instance_data[2] = 1; // version
    let v1_lamports = context
        .svm
        .minimum_balance_for_rent_exemption(INSTANCE_V1_LEN);
    context.create_account(
        &instance_pda,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        instance_data,
        v1_lamports,
    );

    assert_get_or_set_whitelist_enabled(&mut context, &admin, &instance_pda, true, false, false)
        .expect("SetWhitelistEnabled should grow a 0.1.0 instance");

    let instance_account = context.get_account(&instance_pda).unwrap();
    assert_eq!(instance_account.data.len(), INSTANCE_LEN);
    assert_eq!(
        instance_account.lamports,
        context.svm.minimum_balance_for_rent_exemption(INSTANCE_LEN)
    );
    let instance = decode_instance(&instance_account.data).unwrap();
    assert_eq!(instance.version, 2);
    assert_eq!(instance.admin, admin.pubkey());
    assert_eq!(instance.instance_seed, instance_seed.pubkey());
}

#[test]
fn test_set_whitelist_enabled_invalid_admin() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let wrong_admin = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    context
        .airdrop_if_required(&wrong_admin.pubkey(), 1_000_000_000)
        .unwrap();

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = SetWhitelistEnabledBuilder::new()
        .payer(context.payer.pubkey())
        .admin(wrong_admin.pubkey())
        .instance(instance_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .whitelist_deposits(true)
        .whitelist_withdrawals(false)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&wrong_admin]);

    assert_program_error(result, INVALID_ADMIN_ERROR);
}

#[test]
fn test_set_whitelist_enabled_invalid_admin_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (event_authority_pda, _) = find_event_authority_pda();

    // Create instruction where admin is NOT marked as signer
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(admin.pubkey(), false), // admin (NOT signer)
        AccountMeta::new(instance_pda, false),          // instance (writable)
        AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false), // system_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    // Discriminator for SetWhitelistEnabled, then whitelist_deposits, whitelist_withdrawals
    let data = vec![11, 1, 0];

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}
