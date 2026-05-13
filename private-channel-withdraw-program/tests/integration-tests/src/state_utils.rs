use crate::{
    assertions::assert_balance_changed,
    utils::{get_token_balance, to_addr, TestContext, ATA_PROGRAM_ID},
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

    let user_balance_before = get_token_balance(context, &user_ata);

    let mut binding = WithdrawFundsBuilder::new();
    let builder = binding
        .user(to_addr(user.pubkey()))
        .mint(to_addr(*mint))
        .token_account(to_addr(user_ata))
        .token_program(to_addr(TOKEN_PROGRAM_ID))
        .associated_token_program(to_addr(ATA_PROGRAM_ID))
        .amount(amount);

    if let Some(destination) = destination {
        builder.destination(to_addr(destination));
    }

    let instruction = builder.instruction();

    context.send_transaction_with_signers_with_transaction_result(
        instruction,
        &[user],
        with_profiling,
        None,
    )?;

    assert_balance_changed(context, &user_ata, user_balance_before, -(amount as i64));

    Ok(())
}
