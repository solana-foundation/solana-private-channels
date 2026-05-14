#![allow(dead_code)]

use super::send_and_confirm_instructions;
use super::test_types::{TransactionType, UserTransaction, BASE_AMOUNT, DEPOSITS_PER_USER};

use private_channel_escrow_program_client::instructions::DepositBuilder;
use private_channel_withdraw_program_client::instructions::{
    WithdrawFunds, WithdrawFundsInstructionArgs,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signer};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;

pub async fn execute_user_deposits(
    client: &RpcClient,
    user_id: usize,
    user: solana_sdk::signer::keypair::Keypair,
    instance: Pubkey,
    mint: Pubkey,
    allowed_mint_pda: Pubkey,
    event_authority_pda: Pubkey,
) -> Result<Vec<UserTransaction>, String> {
    let mut transactions = Vec::new();

    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_PROGRAM_ID);
    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_PROGRAM_ID);

    // Execute deposits
    for deposit_num in 0..DEPOSITS_PER_USER {
        let amount = BASE_AMOUNT + (user_id as u64 * 1000) + deposit_num as u64;

        let deposit_ix = DepositBuilder::new()
            .payer(user.pubkey())
            .user(user.pubkey())
            .instance(instance)
            .mint(mint)
            .allowed_mint(allowed_mint_pda)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .system_program(SYSTEM_PROGRAM_ID)
            .token_program(TOKEN_PROGRAM_ID)
            .associated_token_program(spl_associated_token_account::ID)
            .event_authority(event_authority_pda)
            .private_channel_escrow_program(
                private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            )
            .amount(amount)
            .instruction();

        let signature =
            send_and_confirm_instructions(client, &[deposit_ix], &user, &[&user], "Deposit")
                .await
                .map_err(|e| e.to_string())?;

        // Get slot from signature status
        let statuses = client
            .get_signature_statuses(&[signature])
            .await
            .map_err(|e| e.to_string())?;

        let slot = statuses
            .value
            .first()
            .and_then(|s| s.as_ref())
            .map(|s| s.slot)
            .ok_or("Failed to get slot from signature status")?;

        transactions.push(UserTransaction {
            user_pubkey: user.pubkey(),
            amount,
            signature: signature.to_string(),
            slot,
            tx_type: TransactionType::Deposit,
        });
    }

    Ok(transactions)
}

pub async fn execute_user_withdrawal(
    client: &RpcClient,
    user: &solana_sdk::signer::keypair::Keypair,
    mint: Pubkey,
    total_deposited: u64,
) -> Result<UserTransaction, String> {
    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_PROGRAM_ID);

    let withdraw_ix = WithdrawFunds {
        user: user.pubkey(),
        mint,
        token_account: user_ata,
        token_program: TOKEN_PROGRAM_ID,
        associated_token_program: spl_associated_token_account::ID,
    }
    .instruction(WithdrawFundsInstructionArgs {
        amount: total_deposited,
        destination: Some(user.pubkey()),
    });

    let signature =
        send_and_confirm_instructions(client, &[withdraw_ix], user, &[user], "Withdraw")
            .await
            .map_err(|e| e.to_string())?;

    // Get slot from signature status
    let statuses = client
        .get_signature_statuses(&[signature])
        .await
        .map_err(|e| e.to_string())?;
    let slot = statuses
        .value
        .first()
        .and_then(|s| s.as_ref())
        .map(|s| s.slot)
        .ok_or("Failed to get slot from signature status")?;

    Ok(UserTransaction {
        user_pubkey: user.pubkey(),
        amount: total_deposited,
        signature: signature.to_string(),
        slot,
        tx_type: TransactionType::Withdrawal,
    })
}

pub fn calculate_user_total_deposited(user_id: usize) -> u64 {
    (0..DEPOSITS_PER_USER)
        .map(|deposit_num| BASE_AMOUNT + (user_id as u64 * 1000) + deposit_num as u64)
        .sum()
}
