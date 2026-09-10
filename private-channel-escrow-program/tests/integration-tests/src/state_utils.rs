use crate::assertions::{assert_deposit_balances, assert_release_funds_balances};
use crate::pda_utils::{find_allowed_mint_pda, find_event_authority_pda};
use crate::utils::{
    assert_event_discriminator_present, get_token_balance, ATA_PROGRAM_ID,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
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
use private_channel_escrow_program_client::instructions::{
    AddOperatorBuilder, AllowMintBuilder, BlockMintBuilder, CreateInstanceBuilder,
    ReleaseFundsBuilder, RemoveOperatorBuilder, SetNewAdminBuilder,
};
use private_channel_escrow_program_client::instructions::{DepositBuilder, RotateBitmapBuilder};
use private_channel_escrow_program_client::WithdrawalBitmap;
use solana_sdk::system_program::ID as SYSTEM_PROGRAM_ID;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
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
        .instruction();

    let transaction_metadata = context
        .send_transaction_with_signers_with_transaction_result(
            instruction,
            &[admin],
            with_profiling,
            None,
        )
        .expect("AllowMint should succeed");

    assert_allow_mint_account(context, &allowed_mint_pda, bump);

    // Assert AllowMint event was emitted
    assert_event_discriminator_present(
        &transaction_metadata,
        1, // AllowMint discriminator
    );

    Ok((allowed_mint_pda, bump))
}

pub fn assert_get_or_block_mint(
    context: &mut TestContext,
    admin: &Keypair,
    instance_pda: &Pubkey,
    allowed_mint_pda: &Pubkey,
    mint: &Pubkey,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&admin.pubkey(), 1_000_000_000)?;

    assert_account_exists(context, allowed_mint_pda);

    let (event_authority_pda, _) = find_event_authority_pda();

    let previous_lamports_balance = context
        .get_account(&context.payer.pubkey())
        .unwrap()
        .lamports;

    let instruction = BlockMintBuilder::new()
        .payer(context.payer.pubkey())
        .admin(admin.pubkey())
        .instance(*instance_pda)
        .mint(*mint)
        .allowed_mint(*allowed_mint_pda)
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
        .expect("BlockMint should succeed");

    assert_block_mint_account(
        context,
        allowed_mint_pda,
        &context.payer.pubkey(),
        previous_lamports_balance,
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
        .amount(amount)
        .user(*user)
        .transaction_nonce(transaction_nonce)
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
