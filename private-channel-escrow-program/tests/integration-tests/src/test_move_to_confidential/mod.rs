use crate::{
    state_utils::setup_confidential_escrow,
    utils::{
        assert_program_error, confidential_state, decrypt_pending, event_data, get_token_balance,
        ConfidentialMintOptions, TestContext, INVALID_OPERATOR_ERROR, TOKEN_2022_PROGRAM_ID,
    },
};
use private_channel_escrow_program_client::instructions::MoveToConfidentialBuilder;
use solana_sdk::signature::Signer;

const PUBLIC_BALANCE: u64 = 3_000_000;
const MOVE_AMOUNT: u64 = 1_000_000;

#[test]
fn test_move_to_confidential_success() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let mint_to = spl_token_2022::instruction::mint_to(
        &TOKEN_2022_PROGRAM_ID,
        &escrow.mint.address,
        &escrow.escrow_ata,
        &context.payer.pubkey(),
        &[],
        PUBLIC_BALANCE,
    )
    .unwrap();
    context
        .send_v1(&[mint_to], &[])
        .expect("MintTo should succeed");

    let instruction = MoveToConfidentialBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(MOVE_AMOUNT)
        .instruction();

    let transaction_metadata = context
        .send_v1(&[instruction], &[&escrow.operator])
        .expect("MoveToConfidential should succeed");

    assert_eq!(
        get_token_balance(&mut context, &escrow.escrow_ata),
        PUBLIC_BALANCE - MOVE_AMOUNT
    );
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        MOVE_AMOUNT
    );
    assert!(!bool::from(
        confidential_state(&mut context, &escrow.escrow_ata).allow_confidential_credits
    ));
    let event = event_data(&transaction_metadata, 16);
    assert_eq!(&event[96..104], &MOVE_AMOUNT.to_le_bytes());
}

#[test]
fn test_move_to_confidential_not_operator_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    // An operator of another instance.
    let other = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());

    let instruction = MoveToConfidentialBuilder::new()
        .payer(context.payer.pubkey())
        .operator(other.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(other.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(MOVE_AMOUNT)
        .instruction();

    let result = context.send_v1(&[instruction], &[&other.operator]);

    assert_program_error(result.map(|_| ()), INVALID_OPERATOR_ERROR);
}
