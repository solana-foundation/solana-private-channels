use crate::{
    pda_utils::find_event_authority_pda,
    state_utils::{assert_get_or_create_instance, assert_get_or_set_whitelist_enabled},
    utils::{
        assert_program_error, TestContext, INVALID_ADMIN_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::SetWhitelistEnabledBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};

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
