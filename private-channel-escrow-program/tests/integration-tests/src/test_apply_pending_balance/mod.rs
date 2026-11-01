use crate::{
    state_utils::{
        assert_get_or_add_operator, assert_get_or_allow_mint, assert_get_or_create_instance,
        setup_confidential_escrow,
    },
    utils::{
        assert_event_discriminator_present, assert_program_error, confidential_state,
        create_confidential_mint, decrypt_available, decrypt_pending, ConfidentialKeys,
        ConfidentialMintOptions, TestContext, CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR,
        INVALID_OPERATOR_ERROR, PENDING_COUNTER_MISMATCH_ERROR, TOKEN_2022_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::{
    ApplyPendingBalanceBuilder, MoveToConfidentialBuilder,
};
use solana_sdk::signature::{Keypair, Signer};
use spl_associated_token_account::get_associated_token_address_with_program_id;

const PENDING_AMOUNT: u64 = 1_000_000;

#[test]
fn test_apply_pending_balance_success() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let mint_to = spl_token_2022::instruction::mint_to(
        &TOKEN_2022_PROGRAM_ID,
        &escrow.mint.address,
        &escrow.escrow_ata,
        &context.payer.pubkey(),
        &[],
        PENDING_AMOUNT,
    )
    .unwrap();
    let move_to_confidential = MoveToConfidentialBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(PENDING_AMOUNT)
        .instruction();
    context
        .send_v1(&[mint_to, move_to_confidential], &[&escrow.operator])
        .expect("MoveToConfidential should succeed");

    let instruction = ApplyPendingBalanceBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .expected_credit_counter(1)
        .new_decryptable_available_balance(escrow.keys.decryptable(PENDING_AMOUNT).0)
        .instruction();

    let transaction_metadata = context
        .send_v1(&[instruction], &[&escrow.operator])
        .expect("ApplyPendingBalance should succeed");

    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        PENDING_AMOUNT
    );
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        0
    );
    let state = confidential_state(&mut context, &escrow.escrow_ata);
    assert_eq!(
        escrow
            .keys
            .aes
            .decrypt(&state.decryptable_available_balance.try_into().unwrap()),
        Some(PENDING_AMOUNT)
    );
    assert_event_discriminator_present(&transaction_metadata, 15);
}

// A credit the operator did not see would be missing from the AES balance it writes.
#[test]
fn test_apply_pending_balance_stale_counter_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let mint_to = spl_token_2022::instruction::mint_to(
        &TOKEN_2022_PROGRAM_ID,
        &escrow.mint.address,
        &escrow.escrow_ata,
        &context.payer.pubkey(),
        &[],
        PENDING_AMOUNT,
    )
    .unwrap();
    let move_to_confidential = MoveToConfidentialBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(PENDING_AMOUNT)
        .instruction();
    context
        .send_v1(&[mint_to, move_to_confidential], &[&escrow.operator])
        .expect("MoveToConfidential should succeed");

    let instruction = ApplyPendingBalanceBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .expected_credit_counter(0)
        .new_decryptable_available_balance(escrow.keys.decryptable(0).0)
        .instruction();

    let result = context.send_v1(&[instruction], &[&escrow.operator]);

    assert_program_error(result.map(|_| ()), PENDING_COUNTER_MISMATCH_ERROR);
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        PENDING_AMOUNT
    );
}

#[test]
fn test_apply_pending_balance_not_operator_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    // An operator of another instance.
    let other = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());

    let instruction = ApplyPendingBalanceBuilder::new()
        .payer(context.payer.pubkey())
        .operator(other.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(other.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .expected_credit_counter(0)
        .new_decryptable_available_balance(escrow.keys.decryptable(0).0)
        .instruction();

    let result = context.send_v1(&[instruction], &[&other.operator]);

    assert_program_error(result.map(|_| ()), INVALID_OPERATOR_ERROR);
}

#[test]
fn test_apply_pending_balance_unconfigured_escrow_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let instance_seed = Keypair::new();
    let mint = create_confidential_mint(&mut context, ConfidentialMintOptions::default());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.address,
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

    let instruction = ApplyPendingBalanceBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .operator_pda(operator_pda)
        .mint(mint.address)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.address,
            &TOKEN_2022_PROGRAM_ID,
        ))
        .expected_credit_counter(0)
        .new_decryptable_available_balance(ConfidentialKeys::new().decryptable(0).0)
        .instruction();

    let result = context.send_v1(&[instruction], &[&operator]);

    assert_program_error(
        result.map(|_| ()),
        CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR,
    );
}
