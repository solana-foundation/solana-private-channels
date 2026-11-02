use crate::{
    pda_utils::{find_allowed_mint_pda, find_allowed_wallet_pda},
    state_utils::{
        assert_get_or_allow_mint, assert_get_or_block_mint, assert_get_or_create_instance,
        assert_get_or_set_whitelist_enabled, setup_confidential_escrow,
    },
    utils::{
        assert_failed_first, combine_lo_hi, confidential_state, create_confidential_account,
        create_confidential_mint, custom_error, decrypt_available, decrypt_change, decrypt_pending,
        event_data, fund_confidential_balance, hook_extras_for_mint, malicious_hook_extras,
        prepare_transfer, set_malicious_extra_account_meta_list, ConfidentialKeys,
        ConfidentialMintOptions, ProofPlacement, TestContext,
        CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR, DEPOSITS_BLOCKED_FOR_MINT_ERROR,
        HOOK_FIXTURE_PROGRAM_ID, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
        TOKEN_CONFIDENTIAL_CREDITS_DISABLED_ERROR, WALLET_NOT_ALLOWED_ERROR,
    },
};
use private_channel_escrow_program_client::instructions::ConfidentialDepositBuilder;
use solana_sdk::signature::{Keypair, Signer};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022_interface::extension::confidential_transfer::instruction as confidential_instruction;
use spl_token_confidential_transfer_proof_extraction::instruction::ProofLocation;

const USER_BALANCE: u64 = 3_000_000;
const DEPOSIT_AMOUNT: u64 = 1_000_000;
const HOOK_LOG: &str = "hook accounts: 6";

/// The amount in a ConfidentialDepositEvent: the growth of the escrow's pending
/// balance, decrypted with the instance key.
fn deposited_amount(keys: &ConfidentialKeys, event: &[u8]) -> u64 {
    // After the instance seed, user, recipient and mint: lo and hi before, then after.
    combine_lo_hi(
        decrypt_change(
            keys,
            event[128..192].try_into().unwrap(),
            event[256..320].try_into().unwrap(),
        ),
        decrypt_change(
            keys,
            event[192..256].try_into().unwrap(),
            event[320..384].try_into().unwrap(),
        ),
    )
}

#[test]
fn test_confidential_deposit_inline_proofs() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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
        .send_v1(&transfer.proofs.with(instruction), &[&user])
        .expect("ConfidentialDeposit should succeed");

    let event = event_data(&transaction_metadata, 13);
    assert_eq!(&event[32..64], user.pubkey().as_ref());
    assert_eq!(
        &event[64..96],
        user.pubkey().as_ref(),
        "recipient defaults to the user"
    );
    assert_eq!(deposited_amount(&escrow.keys, &event), DEPOSIT_AMOUNT);
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        DEPOSIT_AMOUNT
    );
    assert_eq!(
        decrypt_available(&mut context, &user_ata, &user_keys),
        USER_BALANCE - DEPOSIT_AMOUNT
    );
    assert!(!bool::from(
        confidential_state(&mut context, &escrow.escrow_ata).allow_confidential_credits
    ));
}

#[test]
fn test_confidential_deposit_context_account_proofs() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::ContextAccounts,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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
        .send_v1(&transfer.proofs.with(instruction), &[&user])
        .expect("ConfidentialDeposit should succeed");

    let event = event_data(&transaction_metadata, 13);
    assert_eq!(deposited_amount(&escrow.keys, &event), DEPOSIT_AMOUNT);
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        DEPOSIT_AMOUNT
    );
}

// The indexer mints the change in pending, so a deposit onto an unapplied one must
// report only its own amount.
#[test]
fn test_confidential_deposit_onto_a_non_empty_pending_balance() {
    const SECOND_DEPOSIT_AMOUNT: u64 = 250_000;

    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let mut deposited = Vec::new();
    for amount in [DEPOSIT_AMOUNT, SECOND_DEPOSIT_AMOUNT] {
        let transfer = prepare_transfer(
            &mut context,
            &user_ata,
            &user_keys,
            escrow.keys.elgamal.pubkey(),
            &escrow.mint,
            amount,
            ProofPlacement::Inline,
        );
        let instruction = ConfidentialDepositBuilder::new()
            .payer(context.payer.pubkey())
            .user(user.pubkey())
            .instance(escrow.instance_pda)
            .mint(escrow.mint.address)
            .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
            .user_ata(user_ata)
            .instance_ata(escrow.escrow_ata)
            .user_allowed_wallet_pda(user_allowed_wallet_pda)
            .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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
        let transaction_metadata = context
            .send_v1(&transfer.proofs.with(instruction), &[&user])
            .expect("ConfidentialDeposit should succeed");
        deposited.push(deposited_amount(
            &escrow.keys,
            &event_data(&transaction_metadata, 13),
        ));
    }

    assert_eq!(deposited, [DEPOSIT_AMOUNT, SECOND_DEPOSIT_AMOUNT]);
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        DEPOSIT_AMOUNT + SECOND_DEPOSIT_AMOUNT
    );
}

#[test]
fn test_confidential_deposit_with_recipient() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let recipient = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey()).0)
        .recipient_allowed_wallet_pda(
            find_allowed_wallet_pda(&escrow.instance_pda, &recipient.pubkey()).0,
        )
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
        .recipient(recipient.pubkey())
        .instruction();

    let transaction_metadata = context
        .send_v1(&transfer.proofs.with(instruction), &[&user])
        .expect("ConfidentialDeposit should succeed");

    let event = event_data(&transaction_metadata, 13);
    assert_eq!(&event[64..96], recipient.pubkey().as_ref());
}

// Credits stay off outside ConfidentialDeposit, so a plain confidential transfer
// into the escrow, which would skip the whitelist and the event, is refused.
#[test]
fn test_confidential_deposit_direct_transfer_to_escrow_refused() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::ContextAccounts,
    );
    let [equality, validity, range] =
        [0, 1, 2].map(|index| transfer.proofs.context_accounts[index].pubkey);
    let instructions = confidential_instruction::transfer(
        &TOKEN_2022_PROGRAM_ID,
        &user_ata,
        &escrow.mint.address,
        &escrow.escrow_ata,
        &bytemuck::cast(transfer.new_source_decryptable_available_balance),
        &bytemuck::cast(transfer.auditor_ciphertext_lo),
        &bytemuck::cast(transfer.auditor_ciphertext_hi),
        &user.pubkey(),
        &[],
        ProofLocation::ContextStateAccount(&equality),
        ProofLocation::ContextStateAccount(&validity),
        ProofLocation::ContextStateAccount(&range),
    )
    .unwrap();

    let result = context.send_v1(&instructions, &[&user]);

    assert_failed_first(
        result,
        &TOKEN_2022_PROGRAM_ID,
        &custom_error(TOKEN_CONFIDENTIAL_CREDITS_DISABLED_ERROR),
    );
}

#[test]
fn test_confidential_deposit_unconfigured_escrow_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let mint = create_confidential_mint(&mut context, ConfidentialMintOptions::default());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &mint.address,
        false,
        false,
    )
    .expect("AllowMint should succeed");
    let user_ata = create_confidential_account(&mut context, &user, &mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) = find_allowed_wallet_pda(&instance_pda, &user.pubkey());

    // The escrow has no key yet, so prove against some other one.
    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        ConfidentialKeys::new().elgamal.pubkey(),
        &mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.address)
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(get_associated_token_address_with_program_id(
            &instance_pda,
            &mint.address,
            &TOKEN_2022_PROGRAM_ID,
        ))
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&user]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(CONFIDENTIAL_ACCOUNT_NOT_CONFIGURED_ERROR),
    );
}

#[test]
fn test_confidential_deposit_blocked_mint_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address);
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    assert_get_or_block_mint(
        &mut context,
        &escrow.admin,
        &escrow.instance_pda,
        &allowed_mint_pda,
        &escrow.mint.address,
        true,
        false,
        false,
    )
    .expect("BlockMint should succeed");

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&user]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(DEPOSITS_BLOCKED_FOR_MINT_ERROR),
    );
}

#[test]
fn test_confidential_deposit_whitelist_unlisted_user_rejected() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(&mut context, ConfidentialMintOptions::default());
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());
    assert_get_or_set_whitelist_enabled(
        &mut context,
        &escrow.admin,
        &escrow.instance_pda,
        true,
        false,
        false,
    )
    .expect("SetWhitelistEnabled should succeed");

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&user]);

    assert_failed_first(
        result,
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        &custom_error(WALLET_NOT_ALLOWED_ERROR),
    );
}

// Token-2022 runs the hook on confidential transfers too, with the extras the
// escrow forwards after the proofs.
#[test]
fn test_confidential_deposit_transfer_hook_forwards_extras() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            transfer_hook: true,
            ..Default::default()
        },
    );
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::ContextAccounts,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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
        .add_remaining_accounts(&hook_extras_for_mint(&escrow.mint.address))
        .instruction();

    let transaction_metadata = context
        .send_v1(&transfer.proofs.with(instruction), &[&user])
        .expect("ConfidentialDeposit should succeed");

    assert!(
        transaction_metadata
            .logs
            .iter()
            .any(|log| log.contains(HOOK_LOG)),
        "the hook should see every declared extra"
    );
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        DEPOSIT_AMOUNT
    );
}

// A hook whose account list names the payer as a signer must not drain it. The
// payer is the victim because it is the one account here that is both writable
// and signing, as in the public deposit test. Token-2022's confidential path also
// withholds signer bits from the hook, so this pins the outcome, not which layer
// stops it.
#[test]
fn test_confidential_deposit_rejects_signer_bearing_hook_extra() {
    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            transfer_hook: true,
            ..Default::default()
        },
    );
    let user = Keypair::new();
    let attacker = Keypair::new();
    let victim = context.payer.pubkey();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    set_malicious_extra_account_meta_list(
        &mut context,
        &escrow.mint.address,
        &victim,
        &attacker.pubkey(),
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::Inline,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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
        .add_remaining_accounts(&malicious_hook_extras(
            &escrow.mint.address,
            &victim,
            &attacker.pubkey(),
        ))
        .instruction();

    let result = context.send_v1(&transfer.proofs.with(instruction), &[&user]);

    // The hook runs, but its drain lacks the payer's signature.
    assert_failed_first(
        result,
        &HOOK_FIXTURE_PROGRAM_ID,
        "Cross-program invocation with unauthorized signer or writable account",
    );
    assert!(context.get_account(&attacker.pubkey()).is_none());
}

// The fee comes off the escrow's pending balance, so the event carries the net
// amount, the one the channel mints, as public deposits do.
#[test]
fn test_confidential_deposit_transfer_fee_net_amount() {
    const FEE_BASIS_POINTS: u16 = 100;
    const MAXIMUM_FEE: u64 = 1_000_000;
    // Low 16 bits of 100 against the fee's 9_832, so the low part of the pending
    // change is negative.
    const DEPOSIT_AMOUNT: u64 = 15 * 65_536 + 100;

    let mut context = TestContext::new();
    let escrow = setup_confidential_escrow(
        &mut context,
        ConfidentialMintOptions {
            transfer_fee: Some((FEE_BASIS_POINTS, MAXIMUM_FEE)),
            ..Default::default()
        },
    );
    let user = Keypair::new();
    let user_keys = ConfidentialKeys::new();
    let user_ata = create_confidential_account(&mut context, &user, &escrow.mint, &user_keys);
    fund_confidential_balance(
        &mut context,
        &user,
        &user_ata,
        &escrow.mint.address,
        &user_keys,
        USER_BALANCE,
    );
    let (user_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(&escrow.instance_pda, &user.pubkey());

    let transfer = prepare_transfer(
        &mut context,
        &user_ata,
        &user_keys,
        escrow.keys.elgamal.pubkey(),
        &escrow.mint,
        DEPOSIT_AMOUNT,
        ProofPlacement::ContextAccounts,
    );
    let instruction = ConfidentialDepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(escrow.instance_pda)
        .mint(escrow.mint.address)
        .allowed_mint(find_allowed_mint_pda(&escrow.instance_pda, &escrow.mint.address).0)
        .user_ata(user_ata)
        .instance_ata(escrow.escrow_ata)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(user_allowed_wallet_pda)
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
        .send_v1(&transfer.proofs.with(instruction), &[&user])
        .expect("ConfidentialDeposit should succeed");

    let fee = (DEPOSIT_AMOUNT * FEE_BASIS_POINTS as u64).div_ceil(10_000);
    let event = event_data(&transaction_metadata, 13);
    assert_eq!(deposited_amount(&escrow.keys, &event), DEPOSIT_AMOUNT - fee);
    assert_eq!(
        decrypt_pending(&mut context, &escrow.escrow_ata, &escrow.keys),
        DEPOSIT_AMOUNT - fee
    );
}
