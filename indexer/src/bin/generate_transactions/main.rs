/// Binary tool that generates transactions to test the indexer
///
/// Run with: cargo run --bin generate_transactions
use private_channel_escrow_program_client::{
    instructions::{AllowMintBuilder, CreateInstanceBuilder, DepositBuilder},
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::ID as TOKEN_PROGRAM_ID;
use std::time::Duration;

// Import helpers module
mod helpers;
use helpers::{generate_mint, mint_to_owner, send_and_confirm_instructions, setup_wallets};

const INSTANCE_SEED: &[u8] = b"instance";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";
const ALLOWED_MINT_SEED: &[u8] = b"allowed_mint";

fn find_instance_pda(instance_seed: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[INSTANCE_SEED, instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

fn find_allowed_mint_pda(instance: &Pubkey, mint: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[ALLOWED_MINT_SEED, instance.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

async fn send_create_instance(
    client: &RpcClient,
    my_wallet: &Keypair,
) -> Result<(Pubkey, Signature), Box<dyn std::error::Error>> {
    let instance_seed = Keypair::new();
    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());

    let (event_authority_pda, _) = find_event_authority_pda();

    let instruction = CreateInstanceBuilder::new()
        .payer(my_wallet.pubkey())
        .admin(my_wallet.pubkey())
        .instance_seed(instance_seed.pubkey())
        .instance(instance_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let signature = send_and_confirm_instructions(
        client,
        &[instruction],
        my_wallet,
        &[my_wallet, &instance_seed],
        "Create Instance",
    )
    .await?;

    Ok((instance_pda, signature))
}

async fn send_allow_mint(
    client: &RpcClient,
    admin: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
) -> Result<Signature, Box<dyn std::error::Error>> {
    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_PROGRAM_ID);

    let instruction = AllowMintBuilder::new()
        .payer(admin.pubkey())
        .admin(admin.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_PROGRAM_ID)
        .associated_token_program(spl_associated_token_account::ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let signature =
        send_and_confirm_instructions(client, &[instruction], admin, &[admin], "Allow Mint")
            .await?;

    Ok(signature)
}

async fn send_deposit(
    client: &RpcClient,
    user: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    amount: u64,
    recipient: Option<Pubkey>,
) -> Result<Signature, Box<dyn std::error::Error>> {
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), &mint, &TOKEN_PROGRAM_ID);
    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_PROGRAM_ID);

    let mut builder = DepositBuilder::new();
    builder
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
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(amount);

    let instruction = if let Some(recipient) = recipient {
        builder.recipient(recipient).instruction()
    } else {
        builder.instruction()
    };

    let signature =
        send_and_confirm_instructions(client, &[instruction], user, &[user], "Deposit").await?;

    Ok(signature)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting transaction generator test...");

    let client = RpcClient::new_with_commitment(
        "http://localhost:18899".to_string(),
        CommitmentConfig::confirmed(),
    );

    let my_wallet = Keypair::new();
    println!("My wallet: {}", my_wallet.pubkey());

    setup_wallets(&client, &[&my_wallet]).await?;

    let mint_keypair = Keypair::new();

    let mint = generate_mint(&client, &my_wallet, &my_wallet, &mint_keypair).await?;
    tracing::info!("Mint created: {}", mint);

    mint_to_owner(
        &client,
        &my_wallet,
        mint,
        my_wallet.pubkey(),
        &my_wallet,
        1_000_000 * 10u64.pow(6),
    )
    .await?;

    // Create instance
    let (instance, sig) = send_create_instance(&client, &my_wallet).await?;
    println!("Instance created: {} (sig: {})", instance, sig);

    // Allow mint
    let sig = send_allow_mint(&client, &my_wallet, instance, mint).await?;
    println!("Mint allowed: {} (sig: {})", mint, sig);

    println!("\nStarting deposit transaction loop (every 2 seconds)...");

    let mut counter = 0;
    loop {
        std::thread::sleep(Duration::from_secs(2));

        counter += 1;
        let amount = 1000 * counter; // Increasing amounts
        let recipient = if counter % 2 == 0 {
            Some(Keypair::new().pubkey()) // Alternate with/without recipient
        } else {
            None
        };

        let signature =
            send_deposit(&client, &my_wallet, instance, mint, amount, recipient).await?;

        let recipient_str = if let Some(recipient) = recipient {
            format!("to {}", recipient)
        } else {
            "to self".to_string()
        };

        println!(
            "Deposit #{}: {} tokens {} (sig: {})",
            counter, amount, recipient_str, signature
        );
    }
}
