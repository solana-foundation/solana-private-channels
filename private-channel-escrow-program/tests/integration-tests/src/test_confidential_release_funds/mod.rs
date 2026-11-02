use crate::{
    assertions::assert_nonce_consumed,
    pda_utils::{find_allowed_mint_pda, find_allowed_wallet_pda, find_withdrawal_bitmap_pda},
    state_utils::{assert_get_or_block_mint, fund_escrow_available, setup_confidential_escrow},
    utils::{
        assert_failed_first, combine_lo_hi, create_confidential_account, custom_error,
        decrypt_available, decrypt_change, decrypt_pending, event_data, hook_extras_for_mint,
        prepare_transfer, ConfidentialKeys, ConfidentialMintOptions, ProofPlacement, TestContext,
        INVALID_OPERATOR_ERROR, NONCE_ALREADY_USED_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        TOKEN_2022_PROGRAM_ID, TOKEN_CONFIDENTIAL_BALANCE_MISMATCH_ERROR,
        TOKEN_CONFIDENTIAL_CREDITS_DISABLED_ERROR, WITHDRAWALS_BLOCKED_FOR_MINT_ERROR,
    },
};
use private_channel_escrow_program_client::instructions::ConfidentialReleaseFundsBuilder;
use solana_sdk::signature::{Keypair, Signer};
use spl_token_2022::extension::{
    memo_transfer::instruction::enable_required_transfer_memos, ExtensionType,
};
use spl_token_2022_interface::extension::confidential_transfer::instruction as confidential_instruction;

const ESCROW_BALANCE: u64 = 3_000_000;
const RELEASE_AMOUNT: u64 = 1_000_000;
const TRANSACTION_NONCE: u64 = 42;
const HOOK_LOG: &str = "hook accounts: 6";

/// The amount in a ConfidentialReleaseFundsEvent, decrypted with the instance key.
fn released_amount(keys: &ConfidentialKeys, event: &[u8]) -> u64 {
    // After the instance seed, operator, user and mint: amount lo, then hi.
    combine_lo_hi(
        decrypt_change(keys, &[0; 64], event[128..192].try_into().unwrap()),
        decrypt_change(keys, &[0; 64], event[192..256].try_into().unwrap()),
    )
}

#[test]
fn test_confidential_release_funds_inline_proofs() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&escrow.instance_pda);

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
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

    let transaction_metadata = context
        .send_v1(&transfer.proofs.with(instruction), &[&escrow.operator])
        .expect("ConfidentialReleaseFunds should succeed");

    let event = event_data(&transaction_metadata, 14);
    assert_eq!(&event[64..96], user.pubkey().as_ref());
    assert_eq!(released_amount(&escrow.keys, &event), RELEASE_AMOUNT);
    assert_eq!(
        decrypt_pending(&mut context, &user_ata, &user_keys),
        RELEASE_AMOUNT
    );
    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        ESCROW_BALANCE - RELEASE_AMOUNT
    );
    assert_nonce_consumed(
        &mut context,
        &withdrawal_bitmap_pda,
        TRANSACTION_NONCE,
        true,
    );
}

#[test]
fn test_confidential_release_funds_context_account_proofs() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::ContextAccounts,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
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
        .add_remaining_accounts(&transfer.proofs.context_accounts)
        .instruction();

    let transaction_metadata = context
        .send_v1(&transfer.proofs.with(instruction), &[&escrow.operator])
        .expect("ConfidentialReleaseFunds should succeed");

    let event = event_data(&transaction_metadata, 14);
    assert_eq!(released_amount(&escrow.keys, &event), RELEASE_AMOUNT);
    assert_eq!(
        decrypt_pending(&mut context, &user_ata, &user_keys),
        RELEASE_AMOUNT
    );
}

#[test]
fn test_confidential_release_funds_nonce_replay_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    // The second release gets fresh proofs, so only the nonce can stop it.
    let mut results = Vec::new();
    for _ in 0..2 {
        let transfer = prepare_transfer(
            &mut context,
            &escrow.escrow_ata,
            &escrow.keys,
            user_keys.elgamal.pubkey(),
            &escrow.mint,
            RELEASE_AMOUNT,
            ProofPlacement::Inline,
        );
        let instruction = ConfidentialReleaseFundsBuilder::new()
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
            .new_source_decryptable_available_balance(
                transfer.new_source_decryptable_available_balance,
            )
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
        results.push(context.send_v1(&transfer.proofs.with(instruction), &[&escrow.operator]));
    }

    let mut results = results.into_iter();
    results
        .next()
        .unwrap()
        .expect("first release should succeed");
    assert_failed_first(
        results.next().unwrap(),
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(NONCE_ALREADY_USED_ERROR),
    );
}

// Token-2022 binds the proofs to the escrow's available balance, so anything that
// changes it first, here an apply, turns them stale. The operator rebuilds them.
#[test]
fn test_confidential_release_funds_stale_proof_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let instruction = ConfidentialReleaseFundsBuilder::new()
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&escrow.operator]);

    assert_failed_first(
        result,
        &TOKEN_2022_PROGRAM_ID,
        &custom_error(TOKEN_CONFIDENTIAL_BALANCE_MISMATCH_ERROR),
    );
}

#[test]
fn test_confidential_release_funds_destination_credits_disabled_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    let disable_credits = confidential_instruction::disable_confidential_credits(
        &TOKEN_2022_PROGRAM_ID,
        &user_ata,
        &user.pubkey(),
        &[],
    )
    .unwrap();
    context
        .send_v1(&[disable_credits], &[&user])
        .expect("DisableConfidentialCredits should succeed");

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&escrow.operator]);

    assert_failed_first(
        result,
        &TOKEN_2022_PROGRAM_ID,
        &custom_error(TOKEN_CONFIDENTIAL_CREDITS_DISABLED_ERROR),
    );
}

// The escrow puts the memo right before its transfer CPI, as for public releases.
#[test]
fn test_confidential_release_funds_memo_required_destination() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    let require_memos = [
        spl_token_2022::instruction::reallocate(
            &TOKEN_2022_PROGRAM_ID,
            &user_ata,
            &context.payer.pubkey(),
            &user.pubkey(),
            &[],
            &[ExtensionType::MemoTransfer],
        )
        .unwrap(),
        enable_required_transfer_memos(&TOKEN_2022_PROGRAM_ID, &user_ata, &user.pubkey(), &[])
            .unwrap(),
    ];
    context
        .send_v1(&require_memos, &[&user])
        .expect("EnableRequiredTransferMemos should succeed");

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
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
        .send_v1(&transfer.proofs.with(instruction), &[&escrow.operator])
        .expect("ConfidentialReleaseFunds should succeed");

    assert_eq!(
        decrypt_pending(&mut context, &user_ata, &user_keys),
        RELEASE_AMOUNT
    );
}

#[test]
fn test_confidential_release_funds_not_operator_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    // An operator of another instance.
    let other = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(other.operator.pubkey())
        .instance(escrow.instance_pda)
        .withdrawal_bitmap(find_withdrawal_bitmap_pda(&escrow.instance_pda).0)
        .operator_pda(other.operator_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .withdrawer_allowed_wallet_pda(user_allowed_wallet_pda)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .withdrawer(user.pubkey())
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&other.operator]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(INVALID_OPERATOR_ERROR),
    );
}

#[test]
fn test_confidential_release_funds_blocked_withdrawals_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    assert_get_or_block_mint(
        &mut context,
        &escrow.admin,
        &escrow.instance_pda,
        &allowed_mint_pda,
        &escrow.mint.address,
        false,
        true,
        false,
    )
    .expect("BlockMint should succeed");

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .withdrawal_bitmap(find_withdrawal_bitmap_pda(&escrow.instance_pda).0)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .withdrawer_allowed_wallet_pda(user_allowed_wallet_pda)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .withdrawer(user.pubkey())
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&escrow.operator]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(WITHDRAWALS_BLOCKED_FOR_MINT_ERROR),
    );
}

#[test]
fn test_confidential_release_funds_transfer_hook_forwards_extras() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            transfer_hook: true,
            ..Default::default()
        },
    );
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
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
        .add_remaining_accounts(&hook_extras_for_mint(&escrow.mint.address))
        .instruction();

    let transaction_metadata = context
        .send_v1(&transfer.proofs.with(instruction), &[&escrow.operator])
        .expect("ConfidentialReleaseFunds should succeed");

    assert!(
        transaction_metadata
            .logs
            .iter()
            .any(|log| log.contains(HOOK_LOG)),
        "the hook should see every declared extra"
    );
    assert_eq!(
        decrypt_pending(&mut context, &user_ata, &user_keys),
        RELEASE_AMOUNT
    );
}

// The escrow is debited the whole amount and the fee comes off the user's side,
// so the event's amount is what left the escrow.
#[test]
fn test_confidential_release_funds_transfer_fee() {
    const FEE_BASIS_POINTS: u16 = 100;
    const MAXIMUM_FEE: u64 = 1_000_000;

    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            transfer_fee: Some((FEE_BASIS_POINTS, MAXIMUM_FEE)),
            ..Default::default()
        },
    );
    fund_escrow_available(&mut context, &escrow, ESCROW_BALANCE);
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &escrow.escrow_ata,
        &escrow.keys,
        user_keys.elgamal.pubkey(),
        &escrow.mint,
        RELEASE_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialReleaseFundsBuilder::new()
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

    let transaction_metadata = context
        .send_v1(&transfer.proofs.with(instruction), &[&escrow.operator])
        .expect("ConfidentialReleaseFunds should succeed");

    let fee = (RELEASE_AMOUNT * FEE_BASIS_POINTS as u64).div_ceil(10_000);
    let event = event_data(&transaction_metadata, 14);
    assert_eq!(released_amount(&escrow.keys, &event), RELEASE_AMOUNT);
    assert_eq!(
        decrypt_available(&mut context, &escrow.escrow_ata, &escrow.keys),
        ESCROW_BALANCE - RELEASE_AMOUNT
    );
    assert_eq!(
        decrypt_pending(&mut context, &user_ata, &user_keys),
        RELEASE_AMOUNT - fee
    );
}
