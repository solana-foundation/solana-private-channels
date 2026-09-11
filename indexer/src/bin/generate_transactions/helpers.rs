//! Minimal helpers for generate_transactions_test

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;

pub async fn setup_wallets(
    client: &RpcClient,
    wallets: &[&Keypair],
) -> Result<(), Box<dyn std::error::Error>> {
    for wallet in wallets {
        let airdrop_signature = client
            .request_airdrop(&wallet.pubkey(), 100_000_000_000)
            .await?;

        loop {
            let confirmed = client
                .confirm_transaction_with_commitment(
                    &airdrop_signature,
                    CommitmentConfig::confirmed(),
                )
                .await?;
            if confirmed.value {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    }
    Ok(())
}

pub async fn send_and_confirm_instructions(
    client: &RpcClient,
    instructions: &[Instruction],
    payer: &Keypair,
    signers: &[&Keypair],
    _label: &str,
) -> Result<Signature, Box<dyn std::error::Error>> {
    let blockhash = client.get_latest_blockhash().await?;
    let transaction =
        Transaction::new_signed_with_payer(instructions, Some(&payer.pubkey()), signers, blockhash);

    let signature = client
        .send_and_confirm_transaction_with_spinner(&transaction)
        .await?;

    Ok(signature)
}

pub async fn generate_mint(
    client: &RpcClient,
    payer: &Keypair,
    mint_authority: &Keypair,
    mint_keypair: &Keypair,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let mint_pubkey = mint_keypair.pubkey();

    let init_mint_ix = spl_token::instruction::initialize_mint(
        &TOKEN_PROGRAM_ID,
        &mint_pubkey,
        &mint_authority.pubkey(),
        None,
        6,
    )?;

    send_and_confirm_instructions(
        client,
        &[init_mint_ix],
        payer,
        &[payer, mint_keypair],
        "Initialize Mint",
    )
    .await?;

    Ok(mint_pubkey)
}

pub async fn mint_to_owner(
    client: &RpcClient,
    payer: &Keypair,
    mint: Pubkey,
    owner: Pubkey,
    mint_authority: &Keypair,
    amount: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner_ata = get_associated_token_address_with_program_id(&owner, &mint, &TOKEN_PROGRAM_ID);

    let mint_to_ix = spl_token::instruction::mint_to(
        &TOKEN_PROGRAM_ID,
        &mint,
        &owner_ata,
        &mint_authority.pubkey(),
        &[],
        amount,
    )?;

    send_and_confirm_instructions(
        client,
        &[mint_to_ix],
        payer,
        &[payer, mint_authority],
        "Mint To",
    )
    .await?;

    Ok(())
}
