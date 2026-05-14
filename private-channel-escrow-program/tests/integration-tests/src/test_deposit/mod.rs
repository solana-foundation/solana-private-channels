use crate::{
    pda_utils::{find_allowed_mint_pda, find_event_authority_pda},
    state_utils::{assert_get_or_allow_mint, assert_get_or_create_instance, assert_get_or_deposit},
    utils::{
        assert_program_error, create_mint_2022_with_transfer_fee,
        get_or_create_associated_token_account, get_or_create_associated_token_account_2022,
        get_token_balance, set_mint, set_mint_2022_basic, set_mint_2022_with_transfer_hook,
        set_token_balance, setup_test_balances, TestContext, ATA_PROGRAM_ID,
        INCORRECT_PROGRAM_ID_ERROR, INVALID_ACCOUNT_DATA_ERROR, INVALID_INSTRUCTION_DATA_ERROR,
        NOT_ENOUGH_ACCOUNT_KEYS_ERROR, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
        TOKEN_INSUFFICIENT_FUNDS_ERROR, TRANSFER_HOOK_NOT_ALLOWED_ERROR,
    },
};

use private_channel_escrow_program_client::instructions::DepositBuilder;
use solana_sdk::{
    instruction::Instruction,
    signature::{Keypair, Signer},
    system_program::ID as SYSTEM_PROGRAM_ID,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;

const DEPOSIT_AMOUNT: u64 = 1_000_000; // 1 token with 6 decimals

#[test]
fn test_deposit_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT * 2,
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
        true,
    )
    .expect("Deposit should succeed");
}

#[test]
fn test_deposit_with_recipient() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let recipient = Keypair::new();
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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT * 2,
        0,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        Some(recipient.pubkey()),
        false,
    )
    .expect("Deposit with recipient should succeed");
}

#[test]
fn test_deposit_insufficient_funds() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT / 2, // Not enough tokens
        0,
    );

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

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

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, TOKEN_INSUFFICIENT_FUNDS_ERROR);
}

#[test]
fn test_deposit_mint_not_allowed() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        0,
    );

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

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

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_ACCOUNT_DATA_ERROR);
}

#[test]
fn test_deposit_invalid_instruction_data_too_short() {
    let mut context = TestContext::new();

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts: vec![],
        data: vec![6, 1, 2], // Too short instruction data
    };

    let result = context.send_transaction(instruction);
    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

#[test]
fn test_deposit_not_enough_accounts() {
    let mut context = TestContext::new();

    let instruction = Instruction {
        program_id: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
        accounts: vec![], // No accounts
        // 1 discriminator + 8 amount + 1 recipient option
        data: vec![6, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    };

    let result = context.send_transaction(instruction);
    assert_program_error(result, NOT_ENOUGH_ACCOUNT_KEYS_ERROR);
}

// Token 2022 Tests

#[test]
fn test_deposit_token_2022_basic_success() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();

    let instance_seed = Keypair::new();

    set_mint_2022_basic(&mut context, &mint.pubkey());

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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_2022_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        0,
    );

    assert_get_or_deposit(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_2022_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Token2022 deposit should succeed");
}

// Transfer fee mints require special handling: when SPL Token 2022 executes a transfer,
// it withholds a fee from the destination. The sender is debited the full `amount`, but
// the escrow receives `amount - fee`. We set up the mint via real SPL Token 2022
// instructions (not raw account writes) so the fee mechanism is properly exercised.
//
// Mint config: 100 basis points (1%), max fee 1_000_000.
// On a deposit of 1_000_000: fee = ceil(1_000_000 * 100 / 10_000) = 10_000,
// so the escrow receives 990_000.
#[test]
fn test_deposit_token_2022_transfer_fee_success() {
    const TRANSFER_FEE_BASIS_POINTS: u16 = 100; // 1%
    const TRANSFER_FEE_MAX: u64 = 1_000_000;

    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    // Create ATAs through SPL Token 2022 so they get the TransferFeeAmount extension,
    // which is required for fee tracking on fee-bearing mints.
    let user_ata =
        get_or_create_associated_token_account_2022(&mut context, &user.pubkey(), &mint.pubkey());
    let instance_ata =
        get_or_create_associated_token_account_2022(&mut context, &instance_pda, &mint.pubkey());

    // Fund the user via mint_to so balances are set without overwriting ATA extensions.
    let mint_to_ix = spl_token_2022::instruction::mint_to(
        &TOKEN_2022_PROGRAM_ID,
        &mint.pubkey(),
        &user_ata,
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

    let user_balance_before = get_token_balance(&mut context, &user_ata);
    let instance_balance_before = get_token_balance(&mut context, &instance_ata);

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_2022_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("Deposit with transfer fee mint should succeed");

    let user_balance_after = get_token_balance(&mut context, &user_ata);
    let instance_balance_after = get_token_balance(&mut context, &instance_ata);

    // The full deposit amount is debited from the user.
    assert_eq!(
        user_balance_after,
        user_balance_before - DEPOSIT_AMOUNT,
        "User should be debited the full deposit amount"
    );

    // The escrow receives less than the deposit amount because the transfer fee is
    // withheld at the destination. The received amount is deposit - fee.
    // SPL Token 2022 uses ceiling division for fee calculation.
    let expected_fee =
        (DEPOSIT_AMOUNT as u128 * TRANSFER_FEE_BASIS_POINTS as u128).div_ceil(10_000) as u64;
    let expected_received = DEPOSIT_AMOUNT - expected_fee;
    assert_eq!(
        instance_balance_after,
        instance_balance_before + expected_received,
        "Escrow should receive deposit minus transfer fee"
    );
}

// `validate_token2022_extensions` runs on the deposit path as well as AllowMint
// (deposit.rs:100, allow_mint.rs:70). Without a deposit-side test, a future
// refactor that moves the check out of `validate_token2022_extensions` for
// just one path would pass CI. Test strategy mirrors the old
// `test_deposit_token_2022_permanent_delegate_rejected`: stand up a clean
// Token-2022 mint, AllowMint it (check passes), prime the user's balance,
// then swap the mint account data for a TransferHook mint via the litesvm
// cheat code. The deposit must then fail with TransferHookNotAllowed — proving
// the check is live on the deposit path, independent of AllowMint.
#[test]
fn test_deposit_token_2022_transfer_hook_rejected() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let good_mint = Keypair::new();
    let bad_mint = Keypair::new();

    let instance_seed = Keypair::new();

    // 1. Clean Token-2022 mint (no extensions) — passes AllowMint.
    set_mint_2022_basic(&mut context, &good_mint.pubkey());

    let (instance_pda, _) =
        assert_get_or_create_instance(&mut context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");

    let (allowed_mint_pda, _) = assert_get_or_allow_mint(
        &mut context,
        &admin,
        &instance_pda,
        &good_mint.pubkey(),
        false,
        false,
    )
    .expect("AllowMint should succeed for normal mint");

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &good_mint.pubkey(),
        &TOKEN_2022_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        0,
    );

    // 2. Build a separate mint account with the TransferHook extension
    //    initialized — we only need its account data.
    let hook_program_id = Keypair::new().pubkey();
    set_mint_2022_with_transfer_hook(&mut context, &bad_mint.pubkey(), &hook_program_id);

    // 3. litesvm cheat: overwrite the good mint's account with the bad mint's
    //    data. AllowMint has already landed, but the deposit handler re-runs
    //    `validate_token2022_extensions` against the live mint account.
    let bad_mint_account = context
        .get_account(&bad_mint.pubkey())
        .expect("Bad mint account should exist");
    context
        .svm
        .set_account(good_mint.pubkey(), bad_mint_account)
        .expect("Failed to overwrite good mint with TransferHook mint data");

    // 4. Attempt to deposit — the deposit-side validation must reject.
    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();
    let (event_authority_pda, _) = find_event_authority_pda();

    let user_ata = get_associated_token_address_with_program_id(
        &user.pubkey(),
        &good_mint.pubkey(),
        &TOKEN_2022_PROGRAM_ID,
    );
    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &good_mint.pubkey(),
        &TOKEN_2022_PROGRAM_ID,
    );

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(good_mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_2022_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, TRANSFER_HOOK_NOT_ALLOWED_ERROR);
}

#[test]
fn test_deposit_invalid_associated_token_program() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT * 2,
        0,
    );

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

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

    let wrong_ata_program = solana_sdk::pubkey::Pubkey::new_unique();

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(wrong_ata_program) // Wrong ATA program
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INCORRECT_PROGRAM_ID_ERROR);
}

#[test]
fn test_multiple_depositors_same_instance() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user1 = Keypair::new();
    let user2 = Keypair::new();
    let user3 = Keypair::new();
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

    // Setup balances for first user (also creates instance ATA)
    setup_test_balances(
        &mut context,
        &user1,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        0,
    );

    // Setup remaining users — airdrop SOL, create user ATAs, set token balances
    // without re-creating the instance ATA (avoids AlreadyProcessed in LiteSVM)
    for user in [&user2, &user3] {
        context
            .airdrop_if_required(&user.pubkey(), 1_000_000_000)
            .unwrap();
        let user_ata =
            get_or_create_associated_token_account(&mut context, &user.pubkey(), &mint.pubkey());
        set_token_balance(
            &mut context,
            &user_ata,
            &mint.pubkey(),
            &user.pubkey(),
            DEPOSIT_AMOUNT,
        );
    }

    // Get instance ATA balance before any deposits
    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    // Each user deposits
    assert_get_or_deposit(
        &mut context,
        &user1,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit from user1 should succeed");

    assert_get_or_deposit(
        &mut context,
        &user2,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit from user2 should succeed");

    assert_get_or_deposit(
        &mut context,
        &user3,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT,
        None,
        false,
    )
    .expect("Deposit from user3 should succeed");

    // Verify instance ATA received all deposits
    let instance_balance = get_token_balance(&mut context, &instance_ata);
    assert_eq!(
        instance_balance,
        DEPOSIT_AMOUNT * 3,
        "Instance should hold deposits from all three users"
    );
}

#[test]
fn test_deposit_wrong_user_ata() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT * 2,
        0,
    );

    // Also create an ATA for other_user so the account exists
    let other_user_ata =
        get_or_create_associated_token_account(&mut context, &other_user.pubkey(), &mint.pubkey());
    set_token_balance(
        &mut context,
        &other_user_ata,
        &mint.pubkey(),
        &other_user.pubkey(),
        DEPOSIT_AMOUNT,
    );

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    // user_ata belongs to other_user, not the signing user
    let wrong_user_ata = get_associated_token_address_with_program_id(
        &other_user.pubkey(),
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );
    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(wrong_user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}

#[test]
fn test_deposit_wrong_instance_ata() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let user = Keypair::new();
    let mint = Keypair::new();
    let other_mint = Keypair::new();
    let instance_seed = Keypair::new();

    set_mint(&mut context, &mint.pubkey());
    set_mint(&mut context, &other_mint.pubkey());

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

    setup_test_balances(
        &mut context,
        &user,
        &instance_pda,
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
        DEPOSIT_AMOUNT * 2,
        0,
    );

    // Create a valid ATA for the instance but for a different mint
    let wrong_instance_ata =
        get_or_create_associated_token_account(&mut context, &instance_pda, &other_mint.pubkey());

    context
        .airdrop_if_required(&user.pubkey(), 1_000_000_000)
        .unwrap();

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_pda, &mint.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    let user_ata = get_associated_token_address_with_program_id(
        &user.pubkey(),
        &mint.pubkey(),
        &TOKEN_PROGRAM_ID,
    );

    let instruction = DepositBuilder::new()
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(mint.pubkey())
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(wrong_instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(DEPOSIT_AMOUNT)
        .instruction();

    let result = context.send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, INVALID_INSTRUCTION_DATA_ERROR);
}
