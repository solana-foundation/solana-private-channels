use crate::{
    pda_utils::{find_allowed_mint_pda, find_allowed_wallet_pda, find_withdrawal_bitmap_pda},
    state_utils::{
        assert_get_or_add_operator, assert_get_or_allow_mint, assert_get_or_create_instance,
        setup_confidential_escrow,
    },
    utils::{
        assert_event_discriminator_present, assert_failed_first, combine_lo_hi, confidential_state,
        create_confidential_account, create_confidential_mint, custom_error, decrypt_available,
        decrypt_change, decrypt_pending, event_data, prepare_transfer_from_balance,
        ConfidentialKeys, ConfidentialMintOptions, ProofPlacement, TestContext,
        CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR, INVALID_OPERATOR_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::{
    ApplyPendingBalanceBuilder, ConfidentialReleaseFundsBuilder, MoveToConfidentialBuilder,
};
use solana_sdk::signature::{Keypair, Signer};
use spl_associated_token_account::get_associated_token_address_with_program_id;

const PENDING_AMOUNT: u64 = 1_000_000;
const RELEASE_AMOUNT: u64 = 400_000;
const TRANSACTION_NONCE: u64 = 42;

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

// A credit the operator did not see still lands, so dust deposits cannot starve the
// apply. The AES copy it wrote misses that credit, but the event carries everything
// applied, and a release proved from that count writes a correct copy again.
#[test]
fn test_apply_pending_balance_late_credit_recovered_by_release() {
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

    let transaction_metadata = context
        .send_v1(&[instruction], &[&escrow.operator])
        .expect("ApplyPendingBalance should succeed");

    let event = event_data(&transaction_metadata, 15);
    // After the instance seed, operator and mint: applied pending lo, then hi,
    // then the credit count.
    let applied = combine_lo_hi(
        decrypt_change(&escrow.keys, &[0; 64], event[96..160].try_into().unwrap()),
        decrypt_change(&escrow.keys, &[0; 64], event[160..224].try_into().unwrap()),
    );
    assert_eq!(applied, PENDING_AMOUNT);
    assert_eq!(
        u64::from_le_bytes(event[224..232].try_into().unwrap()),
        1,
        "the event counts the credit the operator missed"
    );
    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        PENDING_AMOUNT
    );
    let state = confidential_state(&mut context, &escrow.escrow_ata);
    assert_eq!(
        escrow
            .keys
            .aes
            .decrypt(&state.decryptable_available_balance.try_into().unwrap()),
        Some(0),
        "the AES copy misses the late credit"
    );

    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    let transfer = prepare_transfer_from_balance(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        applied,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let release = ConfidentialReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .withdrawal_bitmap(find_withdrawal_bitmap_pda(&escrow.instance_pda).0)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .withdrawer_allowed_wallet_pda(user_allowed_wallet_pda)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .withdrawer(user.pubkey())
        .expected_available_balance(transfer.expected_available_balance)
        .new_source_decryptable_available_balance(transfer.new_source_decryptable_available_balance)
        .auditor_ciphertext_lo(transfer.auditor_ciphertext_lo)
        .auditor_ciphertext_hi(transfer.auditor_ciphertext_hi)
        .equality_proof_instruction_offset(transfer.equality_proof_instruction_offset)
        .ciphertext_validity_proof_instruction_offset(
            transfer.ciphertext_validity_proof_instruction_offset,
        )
        .range_proof_instruction_offset(transfer.range_proof_instruction_offset)
        .fee_sigma_proof_instruction_offset(transfer.fee_sigma_proof_instruction_offset)
        .fee_ciphertext_validity_proof_instruction_offset(
            transfer.fee_ciphertext_validity_proof_instruction_offset,
        )
        .instruction();
    context
        .send_v1(&transfer.proofs.with(release), &[&escrow.operator])
        .expect("ConfidentialReleaseFunds should succeed");

    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        applied - RELEASE_AMOUNT
    );
    let state = confidential_state(&mut context, &escrow.escrow_ata);
    assert_eq!(
        escrow
            .keys
            .aes
            .decrypt(&state.decryptable_available_balance.try_into().unwrap()),
        Some(applied - RELEASE_AMOUNT),
        "the release wrote a correct AES copy"
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

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(INVALID_OPERATOR_ERROR),
    );
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

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR),
    );
}
