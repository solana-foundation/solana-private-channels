use crate::{
    assertions::assert_balance_changed,
    utils::{
        find_withdraw_config_pda, get_token_balance, get_withdraw_config, TestContext,
        ATA_PROGRAM_ID,
    },
};
use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_associated_token_account::get_associated_token_address;
use spl_token::ID as TOKEN_PROGRAM_ID;

pub fn assert_get_or_withdraw_funds(
    context: &mut TestContext,
    user: &Keypair,
    mint: &Pubkey,
    amount: u64,
    destination: Option<Pubkey>,
    with_profiling: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    context.airdrop_if_required(&user.pubkey(), 1_000_000_000)?;

    let user_ata = get_associated_token_address(&user.pubkey(), mint);
    let (withdraw_config_pda, _) = find_withdraw_config_pda(mint);
    let withdraw_config = get_withdraw_config(context, &withdraw_config_pda);
    let treasury_token_account = withdraw_config.treasury_token_account;

    // The treasury withdraws without paying the fee.
    let fee = if user.pubkey() == withdraw_config.treasury {
        0
    } else {
        withdraw_config.fee
    };

    let user_balance_before = get_token_balance(context, &user_ata);
    let treasury_balance_before = get_token_balance(context, &treasury_token_account);

    let mut binding = WithdrawFundsBuilder::new();
    let builder = binding
        .user(user.pubkey())
        .mint(*mint)
        .token_account(user_ata)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(ATA_PROGRAM_ID)
        .withdraw_config(withdraw_config_pda)
        .treasury_token_account(treasury_token_account)
        .amount(amount);

    if let Some(destination) = destination {
        builder.destination(destination);
    }

    let instruction = builder.instruction();

    context.send_transaction_with_signers_with_transaction_result(
        instruction,
        &[user],
        with_profiling,
        None,
    )?;

    // The treasury's own ATA is the user ATA in its exempt withdrawal, so its
    // change is already the one asserted above.
    assert_balance_changed(
        context,
        &user_ata,
        user_balance_before,
        -((amount + fee) as i64),
    );
    if fee > 0 {
        assert_balance_changed(
            context,
            &treasury_token_account,
            treasury_balance_before,
            fee as i64,
        );
    }

    Ok(())
}
