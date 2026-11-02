use crate::{
    state_utils::{fund_escrow_available, setup_confidential_escrow},
    utils::{
        assert_failed_first, custom_error, decrypt_available, event_data, get_token_balance,
        prepare_withdraw, ConfidentialMintOptions, ProofPlacement, TestContext,
        INVALID_OPERATOR_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::MoveToPublicBuilder;
use solana_sdk::signature::Signer;

const ESCROW_BALANCE: u64 = 3_000_000;
const MOVE_AMOUNT: u64 = 1_000_000;

#[test]
fn test_move_to_public_inline_proofs() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);

    let withdraw = prepare_withdraw(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        MOVE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = MoveToPublicBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(MOVE_AMOUNT)
        .new_decryptable_available_balance(withdraw.new_decryptable_available_balance)
        .equality_proof_instruction_offset(withdraw.equality_proof_instruction_offset)
        .range_proof_instruction_offset(withdraw.range_proof_instruction_offset)
        .instruction();

    let transaction_metadata = context
        .send_v1(&withdraw.proofs.with(instruction), &[&escrow.operator])
        .expect("MoveToPublic should succeed");

    assert_eq!(
        get_token_balance(&mut context, &escrow.escrow_ata),
        MOVE_AMOUNT
    );
    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        ESCROW_BALANCE - MOVE_AMOUNT
    );
    let event = event_data(&transaction_metadata, 17);
    assert_eq!(&event[96..104], &MOVE_AMOUNT.to_le_bytes());
}

#[test]
fn test_move_to_public_context_account_proofs() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);

    let withdraw = prepare_withdraw(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        MOVE_AMOUNT,
        ProofPlacement::ContextAccounts,
    );
    let instruction = MoveToPublicBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(MOVE_AMOUNT)
        .new_decryptable_available_balance(withdraw.new_decryptable_available_balance)
        .equality_proof_instruction_offset(withdraw.equality_proof_instruction_offset)
        .range_proof_instruction_offset(withdraw.range_proof_instruction_offset)
        .add_remaining_accounts(&withdraw.proofs.context_accounts)
        .instruction();

    context
        .send_v1(&withdraw.proofs.with(instruction), &[&escrow.operator])
        .expect("MoveToPublic should succeed");

    assert_eq!(
        get_token_balance(&mut context, &escrow.escrow_ata),
        MOVE_AMOUNT
    );
    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        ESCROW_BALANCE - MOVE_AMOUNT
    );
}

#[test]
fn test_move_to_public_not_operator_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    // An operator of another instance.
    let other = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);

    let withdraw = prepare_withdraw(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        MOVE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = MoveToPublicBuilder::new()
        .payer(context.payer.pubkey())
        .operator(other.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(other.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(MOVE_AMOUNT)
        .new_decryptable_available_balance(withdraw.new_decryptable_available_balance)
        .equality_proof_instruction_offset(withdraw.equality_proof_instruction_offset)
        .range_proof_instruction_offset(withdraw.range_proof_instruction_offset)
        .instruction();

    let result = context.send_v1(&withdraw.proofs.with(instruction), &[&other.operator]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(INVALID_OPERATOR_ERROR),
    );
}
