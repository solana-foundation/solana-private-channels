use crate::assertions::{
    assert_account_lamports_gt, assert_allowed_wallet_account, assert_deposit_balances,
    assert_release_funds_balances, assert_whitelist_enabled,
};
use crate::pda_utils::{find_allowed_mint_pda, find_allowed_wallet_pda, find_event_authority_pda};
use crate::utils::{
    assert_event_discriminator_present, confidential_state, create_confidential_mint,
    decrypt_available, decrypt_pending, get_token_balance, ConfidentialKeys, ConfidentialMint,
    ConfidentialMintOptions, ATA_PROGRAM_ID, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    TEST_MIN_DEPOSIT_AMOUNT, TEST_MIN_WITHDRAW_AMOUNT, TEST_WITHDRAW_FEE,
};
use crate::{
    assertions::{
        assert_account_exists, assert_account_not_exists, assert_add_operator_account,
        assert_allow_mint_account, assert_bitmap_bits_cleared, assert_bitmap_rotated,
        assert_block_mint_account, assert_instance_account, assert_instance_admin_updated,
        assert_nonce_consumed, assert_remove_operator_account, assert_withdrawal_bitmap_account,
    },
    pda_utils::{find_instance_pda, find_operator_pda, find_withdrawal_bitmap_pda},
    utils::{TestContext, TOKEN_2022_PROGRAM_ID},
};
use litesvm::types::TransactionMetadata;
use private_channel_escrow_program_client::instructions::{
    AddOperatorBuilder, AddOrUpdateAllowedWalletBuilder, AllowMintBuilder,
    ApplyPendingBalanceBuilder, BlockMintBuilder, ConfigureConfidentialAccountBuilder,
    CreateInstanceBuilder, MoveToConfidentialBuilder, ReleaseFundsBuilder,
    RemoveAllowedWalletBuilder, RemoveOperatorBuilder, SetNewAdminBuilder,
    SetWhitelistEnabledBuilder,
};
use private_channel_escrow_program_client::instructions::{DepositBuilder, RotateBitmapBuilder};
use private_channel_escrow_program_client::WithdrawalBitmap;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use solana_sdk_ids::system_program::ID as SYSTEM_PROGRAM_ID;
use solana_zk_elgamal_proof_interface::instruction::ProofInstruction;
use solana_zk_sdk::zk_elgamal_proof_program::build_pubkey_validity_proof_data;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;

pub fn assert_get_or_create_instance(
    context: &mut TestContext,
    admin: &Keypair,
    instance_seed: &Keypair,
    fail_if_exists: bool,
    with_profiling: bool,
) -> Result<(Pubkey, u8), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    // Calculate expected Instance PDA
    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());

    if fail_if_exists {
        assert_account_not_exists(context, &instance_pda);
    }

    let (event_authority_pda, _) = find_event_authority_pda();
    let (withdrawal_bitmap_pda, bitmap_bump) = find_withdrawal_bitmap_pda(&instance_pda);

    // Use the generated client (this works in test_create_instance_invalid_pda)
    let instruction = CreateInstanceBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance_seed(instance_seed.pubkey())
        .instance(instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .bitmap_bump(bitmap_bump)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin, instance_seed],
            with_profiling,
            None,
        )
        .expect("CreateInstance should succeed");

    assert_instance_account(
        context,
        &instance_pda,
        &admin.pubkey(),
        bump,
        &instance_seed.pubkey(),
    );

    // A fresh instance must start on generation 0 with no nonce consumed.
    assert_withdrawal_bitmap_account(context, &withdrawal_bitmap_pda, bitmap_bump, 0);
    assert_bitmap_bits_cleared(context, &withdrawal_bitmap_pda);

    // Assert CreateInstance event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        0, // CreateInstance discriminator
    );

    Ok((instance_pda, bump))
}

pub fn assert_get_or_allow_mint(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    mint: &Pubkey,
    fail_if_exists: bool,
    with_profiling: bool,
) -> Result<(Pubkey, u8), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(instance_pda, mint);

    if fail_if_exists {
        assert_account_not_exists(context, &allowed_mint_pda);
    }

    // Auto-detect token program by checking mint account owner
    let mint_account = context
        .get_account(mint)
        .expect("Mint account should exist");

    let token_program_id = if mint_account.owner == TOKEN_2022_PROGRAM_ID {
        TOKEN_2022_PROGRAM_ID
    } else {
        TOKEN_PROGRAM_ID
    };

    // Use the appropriate ATA generation function
    let instance_ata =
        get_associated_token_address_with_program_id(instance_pda, mint, &token_program_id);

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AllowMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .mint(*mint)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(token_program_id) // Use detected token program
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .withdraw_fee(TEST_WITHDRAW_FEE)
        .min_withdraw_amount(TEST_MIN_WITHDRAW_AMOUNT)
        .min_deposit_amount(TEST_MIN_DEPOSIT_AMOUNT)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("AllowMint should succeed");

    assert_allow_mint_account(context, &allowed_mint_pda, mint, bump, &token_program_id);

    // Assert AllowMint event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        1, // AllowMint discriminator
    );

    Ok((allowed_mint_pda, bump))
}

#[allow(clippy::too_many_arguments)]
pub fn assert_get_or_block_mint(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    allowed_mint_pda: &Pubkey,
    mint: &Pubkey,
    block_deposits: bool,
    block_withdrawals: bool,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    assert_account_exists(context, allowed_mint_pda);

    let (_, bump) = find_allowed_mint_pda(instance_pda, mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .mint(*mint)
        .allowed_mint(*allowed_mint_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .block_deposits(block_deposits)
        .block_withdrawals(block_withdrawals)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("BlockMint should succeed");

    assert_block_mint_account(
        context,
        allowed_mint_pda,
        bump,
        block_deposits,
        block_withdrawals,
    );

    // Assert BlockMint event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        2, // BlockMint discriminator
    );

    Ok(())
}

pub fn assert_get_or_add_operator(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    wallet: &Pubkey,
    fail_if_exists: bool,
    with_profiling: bool,
) -> Result<(Pubkey, u8), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    let (operator_pda, bump) = find_operator_pda(instance_pda, wallet);

    if fail_if_exists {
        assert_account_not_exists(context, &operator_pda);
    }

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddOperatorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .operator(*wallet)
        .operator_pda(operator_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("AddOperator should succeed");

    assert_add_operator_account(context, &operator_pda, bump);

    // Assert AddOperator event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        3, // AddOperator discriminator
    );

    Ok((operator_pda, bump))
}

pub fn assert_get_or_remove_operator(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    wallet: &Pubkey,
    operator_pda: &Pubkey,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    assert_account_exists(context, operator_pda);

    let (event_authority_pda, _) = find_event_authority_pda();

    let _previous_lamports_balance = context
        .get_account(&context.payer.pubkey())
        .unwrap()
        .lamports;

    let instruction = RemoveOperatorBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .operator(*wallet)
        .operator_pda(*operator_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("RemoveOperator should succeed");

    assert_remove_operator_account(
        context,
        operator_pda,
        &context.payer.pubkey(),
        _previous_lamports_balance,
    );

    // Assert RemoveOperator event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        4, // RemoveOperator discriminator
    );

    Ok(())
}

pub fn assert_get_or_add_allowed_wallet(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    wallet: &Pubkey,
    block_deposits: bool,
    block_withdrawals: bool,
    with_profiling: bool,
) -> Result<(Pubkey, u8), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    let (allowed_wallet_pda, bump) = find_allowed_wallet_pda(instance_pda, wallet);

    assert_account_not_exists(context, &allowed_wallet_pda);

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = AddOrUpdateAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .wallet(*wallet)
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .block_deposits(block_deposits)
        .block_withdrawals(block_withdrawals)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("AddOrUpdateAllowedWallet should succeed");

    assert_allowed_wallet_account(
        context,
        &allowed_wallet_pda,
        bump,
        block_deposits,
        block_withdrawals,
    );

    // Assert AddOrUpdateAllowedWallet event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        9, // AddOrUpdateAllowedWallet discriminator
    );

    Ok((allowed_wallet_pda, bump))
}

pub fn assert_get_or_remove_allowed_wallet(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    wallet: &Pubkey,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    let (allowed_wallet_pda, _) = find_allowed_wallet_pda(instance_pda, wallet);

    assert_account_exists(context, &allowed_wallet_pda);

    let (event_authority_pda, _) = find_event_authority_pda();

    let previous_lamports_balance = context
        .get_account(&context.payer.pubkey())
        .unwrap()
        .lamports;

    let instruction = RemoveAllowedWalletBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .wallet(*wallet)
        .allowed_wallet_pda(allowed_wallet_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("RemoveAllowedWallet should succeed");

    // The rent comes back to the payer, net of the transaction fee.
    assert_account_not_exists(context, &allowed_wallet_pda);
    let payer = context.payer.pubkey();
    assert_account_lamports_gt(context, &payer, previous_lamports_balance);

    // Assert RemoveAllowedWallet event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        10, // RemoveAllowedWallet discriminator
    );

    Ok(())
}

pub fn assert_get_or_set_whitelist_enabled(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    whitelist_deposits: bool,
    whitelist_withdrawals: bool,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = SetWhitelistEnabledBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .whitelist_deposits(whitelist_deposits)
        .whitelist_withdrawals(whitelist_withdrawals)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("SetWhitelistEnabled should succeed");

    assert_whitelist_enabled(
        context,
        instance_pda,
        whitelist_deposits,
        whitelist_withdrawals,
    );

    // Assert SetWhitelistEnabled event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        11, // SetWhitelistEnabled discriminator
    );

    Ok(())
}

pub fn assert_get_or_set_new_admin(
    context: &mut TestContext,
    current_admin: &Keypair,
    instance_pda: &Pubkey,
    new_admin: &Keypair,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&current_admin.pubkey(), 1_000_000_000)?;
    context.airdrop_if_required(&new_admin.pubkey(), 1_000_000_000)?;

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = SetNewAdminBuilder::new()
        .payer(context.payer.pubkey())
        .current_admin(current_admin.pubkey())
        .instance(*instance_pda)
        .new_admin(new_admin.pubkey())
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[current_admin, new_admin],
            with_profiling,
            None,
        )
        .expect("SetNewAdmin should succeed");

    assert_instance_admin_updated(context, instance_pda, &new_admin.pubkey());

    // Assert SetNewAdmin event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        5, // SetNewAdmin discriminator
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn assert_get_or_deposit(
    context: &mut TestContext,
    user: &Keypair,
    instance_pda: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    amount: u64,
    recipient: Option<Pubkey>,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&user.pubkey(), 1_000_000_000)?;

    let (allowed_mint_pda, _) = find_allowed_mint_pda(instance_pda, mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let (user_allowed_wallet_pda, _) = find_allowed_wallet_pda(instance_pda, &user.pubkey());
    let (recipient_allowed_wallet_pda, _) =
        find_allowed_wallet_pda(instance_pda, &recipient.unwrap_or(user.pubkey()));

    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), mint, token_program);
    let instance_ata =
        get_associated_token_address_with_program_id(instance_pda, mint, token_program);

    let user_balance_before = get_token_balance(context, &user_ata);
    let instance_balance_before = get_token_balance(context, &instance_ata);

    let mut binding = DepositBuilder::new();
    let builder = binding
        .payer(context.payer.pubkey())
        .user(user.pubkey())
        .instance(*instance_pda)
        .mint(*mint)
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(*token_program)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        .recipient_allowed_wallet_pda(recipient_allowed_wallet_pda)
        .amount(amount);

    let instruction = if let Some(recipient) = recipient {
        builder.recipient(recipient).instruction()
    } else {
        builder.instruction()
    };

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[user],
            with_profiling,
            None,
        )
        .expect("Deposit should succeed");

    assert_deposit_balances(
        context,
        &user_ata,
        &instance_ata,
        user_balance_before,
        instance_balance_before,
        amount,
    );

    // Assert Deposit event was emitted
    assert_event_discriminator_present(&transaction_metadata, 6); // Deposit discriminator

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn assert_get_or_release_funds(
    context: &mut TestContext,
    operator: &Keypair,
    instance_pda: &Pubkey,
    operator_pda: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
    amount: u64,
    user: &Pubkey,
    transaction_nonce: u64,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&operator.pubkey(), 1_000_000_000)?;

    let (allowed_mint_pda, _) = find_allowed_mint_pda(instance_pda, mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(instance_pda);
    let (user_allowed_wallet_pda, _) = find_allowed_wallet_pda(instance_pda, user);

    let user_ata = get_associated_token_address_with_program_id(user, mint, token_program);
    let instance_ata =
        get_associated_token_address_with_program_id(instance_pda, mint, token_program);

    // Get balances before release
    let user_balance_before = get_token_balance(context, &user_ata);
    let instance_balance_before = get_token_balance(context, &instance_ata);

    let instruction = ReleaseFundsBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(*instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .operator_pda(*operator_pda)
        .mint(*mint)
        .allowed_mint(allowed_mint_pda)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .token_program(*token_program)
        .associated_token_program(ATA_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .user_allowed_wallet_pda(user_allowed_wallet_pda)
        // The helper releases to the wallet that withdrew; tests that split the
        // two build the instruction themselves.
        .withdrawer_allowed_wallet_pda(user_allowed_wallet_pda)
        .amount(amount)
        .user(*user)
        .transaction_nonce(transaction_nonce)
        .withdrawer(*user)
        .instruction();

    let transaction_metadata = context.send_transaction_with_signers_with_transaction_result(
        instruction,
        &[operator],
        with_profiling,
        Some(1_200_000),
    )?;

    assert_release_funds_balances(
        context,
        &user_ata,
        &instance_ata,
        user_balance_before,
        instance_balance_before,
        amount,
    );

    // The nonce must be consumed so a replay of the same withdrawal is refused.
    assert_nonce_consumed(context, &withdrawal_bitmap_pda, transaction_nonce, true);

    // Assert ReleaseFunds event was emitted (discriminator 7)
    assert_event_discriminator_present(&transaction_metadata, 7);

    Ok(())
}

pub fn assert_get_or_rotate_bitmap(
    context: &mut TestContext,
    operator: &Keypair,
    instance_pda: &Pubkey,
    operator_pda: &Pubkey,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&operator.pubkey(), 1_000_000_000)?;

    let (event_authority_pda, _) = find_event_authority_pda();
    let (withdrawal_bitmap_pda, _) = find_withdrawal_bitmap_pda(instance_pda);

    let current_bitmap = context
        .get_account(&withdrawal_bitmap_pda)
        .expect("Withdrawal bitmap account should exist")
        .data
        .clone();

    let current_bitmap = WithdrawalBitmap::from_bytes(&current_bitmap)
        .expect("Should deserialize withdrawal bitmap account");

    let previous_generation = current_bitmap.generation;

    let instruction = RotateBitmapBuilder::new()
        .payer(context.payer.pubkey())
        .operator(operator.pubkey())
        .instance(*instance_pda)
        .withdrawal_bitmap(withdrawal_bitmap_pda)
        .operator_pda(*operator_pda)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .expected_generation(previous_generation)
        .instruction();

    let transaction_metadata = context.send_transaction_with_signers_with_transaction_result(
        instruction,
        &[operator],
        with_profiling,
        None,
    )?;

    // Assert the bitmap was cleared and the generation incremented
    assert_bitmap_rotated(context, &withdrawal_bitmap_pda, previous_generation);

    // Assert RotateBitmap event was emitted (discriminator 8)
    assert_event_discriminator_present(&transaction_metadata, 8);

    Ok(())
}

/// An instance with a confidential mint allowed, its escrow account configured
/// under `keys`, and an operator.
pub struct ConfidentialEscrow {
    pub admin: Keypair,
    pub instance_pda: Pubkey,
    pub mint: ConfidentialMint,
    pub keys: ConfidentialKeys,
    pub escrow_ata: Pubkey,
    pub operator: Keypair,
    pub operator_pda: Pubkey,
}

pub fn setup_confidential_escrow(
    context: &mut TestContext,
    options: ConfidentialMintOptions,
) -> ConfidentialEscrow {
    let admin = Keypair::new();
    let instance_seed = Keypair::new();
    let operator = Keypair::new();
    let keys = ConfidentialKeys::new();
    let mint = create_confidential_mint(context, options);

    let (instance_pda, _) =
        assert_get_or_create_instance(context, &admin, &instance_seed, false, false)
            .expect("CreateInstance should succeed");
    let (allowed_mint_pda, _) =
        assert_get_or_allow_mint(context, &admin, &instance_pda, &mint.address, false, false)
            .expect("AllowMint should succeed");
    let (operator_pda, _) = assert_get_or_add_operator(
        context,
        &admin,
        &instance_pda,
        &operator.pubkey(),
        false,
        false,
    )
    .expect("AddOperator should succeed");

    let escrow_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint.address,
        &TOKEN_2022_PROGRAM_ID,
    );
    let configure = ConfigureConfidentialAccountBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(instance_pda)
        .mint(mint.address)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(escrow_ata)
        .decryptable_zero_balance(keys.decryptable(0).0)
        .proof_instruction_offset(1)
        .instruction();
    let proof = ProofInstruction::VerifyPubkeyValidity.encode_verify_proof(
        None,
        &build_pubkey_validity_proof_data(&keys.elgamal).unwrap(),
    );
    context
        .send_v1(&[configure, proof], &[&admin])
        .expect("ConfigureConfidentialAccount should succeed");

    ConfidentialEscrow {
        admin,
        instance_pda,
        mint,
        keys,
        escrow_ata,
        operator,
        operator_pda,
    }
}

/// ApplyPendingBalance on the escrow, with the counter and new AES balance taken
/// from its account.
pub fn apply_escrow_pending(
    context: &mut TestContext,
    escrow: &ConfidentialEscrow,
) -> Result<TransactionMetadata, Box<dyn std::error::Error>> {
    let credit_counter = confidential_state(context, &escrow.escrow_ata)
        .pending_balance_credit_counter
        .into();
    let available = decrypt_available(context, &escrow.escrow_ata, &escrow.keys);
    let pending = decrypt_pending(context, &escrow.escrow_ata, &escrow.keys);

    let apply = ApplyPendingBalanceBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .expected_credit_counter(credit_counter)
        .new_decryptable_available_balance(escrow.keys.decryptable(available + pending).0)
        .instruction();
    context.send_v1(&[apply], &[&escrow.operator])
}

/// Mints `amount` to the escrow's public balance and makes it available for
/// confidential releases.
pub fn fund_escrow_available(context: &mut TestContext, escrow: &ConfidentialEscrow, amount: u64) {
    let mint_to = spl_token_2022::instruction::mint_to(
        &TOKEN_2022_PROGRAM_ID,
        &escrow.mint.address,
        &escrow.escrow_ata,
        &context.payer.pubkey(),
        &[],
        amount,
    )
    .unwrap();
    let move_to_confidential = MoveToConfidentialBuilder::new()
        .payer(context.payer.pubkey())
        .operator(escrow.operator.pubkey())
        .instance(escrow.instance_pda)
        .operator_pda(escrow.operator_pda)
        .mint(escrow.mint.address)
        .instance_ata(escrow.escrow_ata)
        .amount(amount)
        .instruction();
    context
        .send_v1(&[mint_to, move_to_confidential], &[&escrow.operator])
        .expect("MoveToConfidential should succeed");
    apply_escrow_pending(context, escrow).expect("ApplyPendingBalance should succeed");
}
