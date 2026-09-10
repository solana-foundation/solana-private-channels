mod double_spend;

use crate::{
    assertions::assert_nonce_consumed,
    pda_utils::{
        find_allowed_mint_pda, find_event_authority_pda, find_operator_pda,
        find_withdrawal_bitmap_pda,
    },
    state_utils::{
        assert_get_or_add_operator, assert_get_or_allow_mint, assert_get_or_create_instance,
        assert_get_or_deposit, assert_get_or_release_funds, assert_get_or_rotate_bitmap,
    },
    utils::{
        assert_program_error, create_mint_2022_with_transfer_fee,
        get_or_create_associated_token_account_2022, get_token_balance, set_mint,
        setup_test_balances, TestContext, ATA_PROGRAM_ID, INVALID_INSTRUCTION_DATA_ERROR,
        INVALID_OPERATOR_ERROR, INVALID_WITHDRAWAL_BITMAP_ERROR, MISSING_REQUIRED_SIGNATURE_ERROR,
        NONCES_PER_GENERATION, NONCE_ALREADY_USED_ERROR, NONCE_OUTSIDE_CURRENT_GENERATION_ERROR,
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, TOKEN_INSUFFICIENT_FUNDS_ERROR,
    },
};

use private_channel_escrow_program_client::instructions::ReleaseFundsBuilder;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;

const DEPOSIT_AMOUNT: u64 = 1_000_000; // 1 token with 6 decimals
const RELEASE_AMOUNT: u64 = 500_000; // 0.5 tokens with 6 decimals
const TRANSACTION_NONCE: u64 = 42; // Withdrawal nonce consumed from the bitmap

#[test]
fn test_release_funds_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    // Add operator
    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Setup and perform deposit
    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        RELEASE_AMOUNT,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit should succeed");

    // Release funds using utility function
    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        TRANSACTION_NONCE,
        true,
    )
    .expect("ReleaseFunds should succeed");
}

#[test]
fn test_release_funds_insufficient_funds() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    // Add operator
    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        true,
    )
    .expect("AddOperator should succeed");

    // Setup deposit test but don't perform deposit - this means the instance ATA will have 0 balance
    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        0, // Set instance balance to 0 to create insufficient funds scenario
    );

    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        TRANSACTION_NONCE,
        false,
    );

    assert_program_error(result, TOKEN_INSUFFICIENT_FUNDS_ERROR);
}

#[test]
fn test_release_funds_not_operator() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let fake_operator = Keypair::new(); // Not added as operator
    let user = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    // Another instance for fake operator
    let instance_seed_2 = Keypair::new();
    let (instance_pda_2, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed_2, false, false)
            .expect("CreateInstance should succeed");
    assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda_2,
        &fake_operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Real valid instance
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

    // Add legitimate operator
    assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Setup and perform deposit
    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        RELEASE_AMOUNT,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit should succeed");

    // Try to release funds with fake operator
    context
        .airdrop_if_required(&fake_operator.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let (fake_operator_pda, _) = find_operator_pda(&instance_pda_2, &fake_operator.pubkey());

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

    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);

    let instruction = ReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(fake_operator.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .operator_pda(fake_operator_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(RELEASE_AMOUNT)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&fake_operator]);

    assert_program_error(result, INVALID_OPERATOR_ERROR);
}

#[test]
fn test_release_funds_invalid_instruction_data_too_short() {
    let mut context = TestContext::new();

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts: vec![],
        data: vec![7, 1, 2, 3], // Too short instruction data (discriminator + partial data)
    };

    let result = context.send_transaction(instruction);
    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

#[test]
fn test_release_funds_operator_not_signer() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    // Add operator
    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Setup and perform deposit
    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        RELEASE_AMOUNT,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit should succeed");

    // Try to release funds with operator not marked as signer
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);

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

    // Create instruction where operator is NOT marked as signer (13 accounts)
    let accounts = vec![
        AccountMeta::new(context.payer.pubkey(), true), // payer (signer, writable)
        AccountMeta::new_readonly(operator.pubkey(), false), // operator (NOT signer)
        AccountMeta::new_readonly(instance_pda, false), // instance
        AccountMeta::new(withdrawal_bitmap_pda, false), // withdrawal_bitmap (writable)
        AccountMeta::new_readonly(operator_pda, false), // operator_pda
        AccountMeta::new_readonly(mint.pubkey(), false), // mint
        AccountMeta::new_readonly(allowed_mint_pda, false), // allowed_mint
        AccountMeta::new(user_ata, false),              // user_ata (writable)
        AccountMeta::new(instance_ata, false),          // instance_ata (writable)
        AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false), // token_program
        AccountMeta::new_readonly(ATA_PROGRAM_ID, false), // associated_token_program
        AccountMeta::new_readonly(event_authority_pda, false), // event_authority
        AccountMeta::new_readonly(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, false), // private_channel_escrow_program
    ];

    let mut data = vec![7]; // discriminator for ReleaseFunds
    data.extend_from_slice(&RELEASE_AMOUNT.to_le_bytes()); // amount (8 bytes)
    data.extend_from_slice(user.pubkey().as_ref()); // user (32 bytes)
    data.extend_from_slice(&TRANSACTION_NONCE.to_le_bytes()); // transaction_nonce (8 bytes)

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts,
        data,
    };

    let result = context.send_transaction_with_signers(instruction, &[]);

    assert_program_error(result, MISSING_REQUIRED_SIGNATURE_ERROR);
}

#[test]
fn test_release_funds_bitmap_tracks_many_nonces() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    // Add operator
    let (operator_pda, _) = assert_get_or_add_operator(
        &mut context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    // Setup test with a large deposit to support multiple releases
    let large_deposit = 10_000_000; // 10 tokens with 6 decimals
    let release_amount = 100_000; // 0.1 tokens per release

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        0,
        large_deposit, // Give escrow full amount
    );

    let mut used_nonces = std::collections::HashSet::new();

    // Nonces spread across many bytes of the bitmap, interleaved with replays of
    // ones already consumed. Setting a later bit must not free an earlier one.
    let test_nonces = [
        1, 2, 3, 5, 8, 13, 21, 34, 55, 89, // Valid unique nonces
        144, 233, 377, 610, 987, 1597, // More valid nonces
        1, 2, 3, // Duplicates (should fail)
        999, 1000, 1001, 1002, // More unique valid nonces
        5, 8, // More duplicates (should fail)
        2000, 2001, 2002, 2003, 2004, // Final batch of unique nonces
    ];

    for &nonce in test_nonces.iter() {
        // A replay carries the same instruction bytes as its first attempt, so
        // without a fresh blockhash it would be rejected as a duplicate
        // signature before ever reaching the program.
        context.svm.expire_blockhash();

        let result = assert_get_or_release_funds(
            &mut context,
            &operator,
            &instance_pda,
            &operator_pda,
            &mint.pubkey(),
            &TOKEN_PROGRAM_ID,
            release_amount,
            &user.pubkey(),
            nonce,
            false,
        );

        if used_nonces.contains(&nonce) {
            assert_program_error(result, NONCE_ALREADY_USED_ERROR);
        } else {
            assert!(result.is_ok(), "New nonce {} should succeed", nonce);
            used_nonces.insert(nonce);
        }
    }
}

#[test]
fn test_release_funds_with_bitmap_rotation() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    // Setup balances for multiple releases (large deposit to support multiple releases)
    let large_deposit = 10_000_000; // 10 tokens with 6 decimals
    let release_amount = 100_000; // 0.1 tokens per release

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        0,             // User doesn't need initial balance for this test
        large_deposit, // Give escrow the full amount
    );

    // === FIRST RELEASE (generation 0) ===
    let first_nonce = 42u64; // Nonce in range 0..65535 for generation 0

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        release_amount,
        &user.pubkey(),
        first_nonce,
        false,
    )
    .expect("First release should succeed");

    // === ROTATE (generation 0 -> 1) ===
    assert_get_or_rotate_bitmap(&mut context, &operator, &instance_pda, &operator_pda, false)
        .expect("RotateBitmap should succeed");

    // === SECOND RELEASE (generation 1) ===
    let second_nonce = NONCES_PER_GENERATION; // First nonce of generation 1

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        release_amount,
        &user.pubkey(),
        second_nonce,
        false,
    )
    .expect("Second release with the first nonce of generation 1 should succeed");

    // A never-used nonce from the previous generation is still refused: rotation
    // clears the bits, so only the generation check keeps the old range closed.
    let old_range_nonce = 123u64;
    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        release_amount,
        &user.pubkey(),
        old_range_nonce,
        false,
    );

    assert_program_error(result, NONCE_OUTSIDE_CURRENT_GENERATION_ERROR);

    // A nonce from a far future generation is refused for the same reason.
    let future_nonce = NONCES_PER_GENERATION * 10;
    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        release_amount,
        &user.pubkey(),
        future_nonce,
        false,
    );

    assert_program_error(result, NONCE_OUTSIDE_CURRENT_GENERATION_ERROR);
}

#[test]
fn test_release_funds_nonce_zero_boundary() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        0,
        DEPOSIT_AMOUNT,
    );

    // Use nonce = 0 (boundary value: first bit of the first byte)
    let nonce: u64 = 0;

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    )
    .expect("Release with nonce=0 should succeed");
}

#[test]
fn test_release_funds_last_nonce_in_generation() {
    // Nonce 65535 is the last bit of the last byte of the bitmap, so this pins
    // the byte index arithmetic at the far end of the account.
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        0,
        DEPOSIT_AMOUNT,
    );

    // Highest nonce the generation covers: the last bit of the last bitmap byte.
    let last_nonce: u64 = NONCES_PER_GENERATION - 1;

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        last_nonce,
        false,
    )
    .expect("Release with the last nonce of the generation should succeed");
}

#[test]
fn test_release_funds_wrong_user_ata() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let other_user = Keypair::new();
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
        DEPOSIT_AMOUNT,
        RELEASE_AMOUNT,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit should succeed");

    // Create an ATA for other_user so the account exists on-chain
    let other_user_ata = get_associated_token_address_with_program_id(
        &other_user.pubkey(),
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );
    context
        .airdrop_if_required(&other_user.pubkey(), 1_000_000_000)
        .unwrap();
    crate::utils::get_or_create_associated_token_account(
        &mut context,
        &other_user.pubkey(),
        &mint.pubkey(),
    );

    context
        .airdrop_if_required(&operator.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);

    // Pass other_user's ATA but user's pubkey in instruction data — mismatch
    let instruction = ReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .operator_pda(operator_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(other_user_ata)
        .instance_ata(instance_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(RELEASE_AMOUNT)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&operator]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

#[test]
fn test_release_funds_full_balance() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        DEPOSIT_AMOUNT,
        0,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit should succeed");

    // Release the entire balance — instance ATA should land at zero
    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        &user.pubkey(),
        TRANSACTION_NONCE,
        false,
    )
    .expect("Full balance release should succeed");

    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );
    let balance = crate::utils::get_token_balance(&mut context, &instance_ata);
    assert_eq!(
        balance, 0,
        "Instance ATA should be empty after full release"
    );
}

// Transfer fee mints require TransferChecked for the SPL Token 2022 runtime to accept
// the transfer. On release, the escrow sends `amount` and the user receives `amount - fee`
// (the fee is withheld at the destination). The escrow is debited the full `amount`, so
// the existing balance check (`escrow_after == escrow_before - amount`) stays correct.
//
// Mint config: 100 basis points (1%), max fee 1_000_000.
// The escrow is seeded directly via mint_to (no deposit flow), so it starts with exactly
// DEPOSIT_AMOUNT tokens — no fee is applied on mint_to.
// Release: operator releases 500_000 from escrow; user receives 495_000 (fee withheld at
// user ATA on release); escrow decreases by exactly 500_000.
#[test]
fn test_release_funds_token_2022_transfer_fee_success() {
    const TRANSFER_FEE_BASIS_POINTS: u16 = 100; // 1%
    const TRANSFER_FEE_MAX: u64 = 1_000_000;

    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();

    // Initialize the mint through SPL Token 2022 so the fee extension is properly
    // recognized by the runtime during transfers.
    create_mint_2022_with_transfer_fee(
        &mut context,
        &mint,
        TRANSFER_FEE_BASIS_POINTS,
        TRANSFER_FEE_MAX,
    );

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

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    // Create ATAs through SPL Token 2022 so they get the TransferFeeAmount extension,
    // which is required for fee tracking on fee-bearing mints.
    let user_ata =
        get_or_create_associated_token_account_2022(&mut context, &user.pubkey(), &mint.pubkey());
    let instance_ata =
        get_or_create_associated_token_account_2022(&mut context, &instance_pda, &mint.pubkey());

    // Fund the escrow directly via mint_to to simulate a prior deposit already being
    // in the escrow (avoids a full deposit flow in this test).
    let mint_to_ix = spl_token_2022::instruction::mint_to(
        &TOKEN_2022_PROGRAM_ID,
        &mint.pubkey(),
        &instance_ata,
        &context.payer.pubkey(),
        &[],
        DEPOSIT_AMOUNT,
    )
    .unwrap();
    context
        .send_transaction(mint_to_ix)
        .expect("mint_to should succeed");

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);

    let user_balance_before = get_token_balance(&mut context, &user_ata);
    let instance_balance_before = get_token_balance(&mut context, &instance_ata);

    let instruction = ReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .operator_pda(operator_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .token_program(TOKEN_2022_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(RELEASE_AMOUNT)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .instruction();

    context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[&operator],
            false,
            Some(1_200_000),
        )
        .expect("Release with transfer fee mint should succeed");

    let user_balance_after = get_token_balance(&mut context, &user_ata);
    let instance_balance_after = get_token_balance(&mut context, &instance_ata);

    // The escrow is debited the full release amount — the fee is withheld at the
    // destination (user ATA), not the source.
    assert_eq!(
        instance_balance_after,
        instance_balance_before - RELEASE_AMOUNT,
        "Escrow should be debited the full release amount"
    );

    // The user receives release amount minus the transfer fee.
    // SPL Token 2022 uses ceiling division for fee calculation.
    let expected_fee =
        (RELEASE_AMOUNT as u128 * TRANSFER_FEE_BASIS_POINTS as u128).div_ceil(10_000) as u64;
    let expected_received = RELEASE_AMOUNT - expected_fee;
    assert_eq!(
        user_balance_after,
        user_balance_before + expected_received,
        "User should receive release amount minus transfer fee"
    );
}

/// A fresh instance sits on generation 0, so a nonce from any later generation
/// must be refused before a rotation ever happens. Without this the operator
/// could consume a far-future nonce's bit and strand it for its real generation.
#[test]
fn test_release_funds_nonce_from_future_generation_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        0,
        DEPOSIT_AMOUNT,
    );

    // First nonce of generation 1, on an instance still at generation 0.
    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        NONCES_PER_GENERATION,
        false,
    );

    assert_program_error(result, NONCE_OUTSIDE_CURRENT_GENERATION_ERROR);

    context.svm.expire_blockhash();

    // A nonce far beyond the current generation must be refused the same way.
    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        NONCES_PER_GENERATION * 100,
        false,
    );

    assert_program_error(result, NONCE_OUTSIDE_CURRENT_GENERATION_ERROR);
}

/// A zero-amount release still consumes its nonce. Letting it through without
/// setting the bit would leave the nonce replayable for a real amount.
#[test]
fn test_release_funds_zero_amount_consumes_nonce() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        0,
        DEPOSIT_AMOUNT,
    );

    let nonce: u64 = 7;

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        0,
        &user.pubkey(),
        nonce,
        false,
    )
    .expect("Zero amount release should succeed");

    context.svm.expire_blockhash();

    let result = assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    );

    assert_program_error(result, NONCE_ALREADY_USED_ERROR);
}

/// The bitmap is a separate account, so an operator could hand this instance's
/// ReleaseFunds another instance's bitmap: the nonce would burn over there while
/// the funds leave here, making it replayable against this instance forever.
/// `WithdrawalBitmap::validate` re-derives the PDA from the instance to stop it.
#[test]
fn test_release_funds_foreign_bitmap_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let operator = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let instance_seed = Keypair::new();
    let other_instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    // A second instance, whose bitmap we will try to substitute.
    let (other_instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &other_instance_seed, false, false)
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
        0,
        DEPOSIT_AMOUNT,
    );

    context
        .airdrop_if_required(&operator.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();
    let (other_bitmap_pda, _) = find_withdrawal_bitmap_pda(&other_instance_pda);

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

    let instruction = ReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(other_bitmap_pda)
        .operator_pda(operator_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(RELEASE_AMOUNT)
        .user(user.pubkey())
        .transaction_nonce(TRANSACTION_NONCE)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&operator]);

    assert_program_error(result, INVALID_WITHDRAWAL_BITMAP_ERROR);

    // Neither bitmap may record the nonce, and no funds may move.
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(&instance_pda);
    assert_nonce_consumed(
        &mut context,
        &withdrawal_bitmap_pda,
        TRANSACTION_NONCE,
        false,
    );
    assert_nonce_consumed(&mut context, &other_bitmap_pda, TRANSACTION_NONCE, false);
    assert_eq!(
        get_token_balance(&mut context, &instance_ata),
        DEPOSIT_AMOUNT,
        "escrow balance must be untouched"
    );
}

/// Rotation must free the exact bit position it cleared. Releasing nonce N in one
/// generation and N + NONCES_PER_GENERATION in the next targets the same bit, so
/// a rotation that failed to clear it would surface here as NonceAlreadyUsed.
#[test]
fn test_release_funds_rotation_frees_same_bit_position() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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
        0,
        DEPOSIT_AMOUNT,
    );

    let nonce = 42u64;

    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce,
        false,
    )
    .expect("Release in generation 0 should succeed");

    assert_get_or_rotate_bitmap(&mut context, &operator, &instance_pda, &operator_pda, false)
        .expect("RotateBitmap should succeed");

    // Same bit position, next generation.
    assert_get_or_release_funds(
        &mut context,
        &operator,
        &instance_pda,
        &operator_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        RELEASE_AMOUNT,
        &user.pubkey(),
        nonce + NONCES_PER_GENERATION,
        false,
    )
    .expect("Same bit position must be reusable after rotation");
}
