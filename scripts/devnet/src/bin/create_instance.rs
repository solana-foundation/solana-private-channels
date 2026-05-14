use private_channel_escrow_program_client::{
    instructions::CreateInstanceBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signer},
    transaction::Transaction,
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use std::{env, error::Error};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const INSTANCE_SEED: &[u8] = b"instance";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

fn find_instance_pda(instance_seed: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[INSTANCE_SEED, instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <rpc-url> <admin-keypair-path>", args[0]);
        eprintln!(
            "Example: {} https://api.devnet.solana.com ./keypairs/admin.json",
            args[0]
        );
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    // Create new instance seed
    let instance_seed = Keypair::new();
    let (instance_pda, bump) = find_instance_pda(&instance_seed.pubkey());
    let (event_authority_pda, _) = find_event_authority_pda();

    println!("\nCreating escrow instance...");
    println!("Instance seed: {}", instance_seed.pubkey());
    println!("Instance PDA: {}", instance_pda);

    let instruction = CreateInstanceBuilder::new()
        .payer(admin_keypair.pubkey())
        .admin(admin_keypair.pubkey())
        .instance_seed(instance_seed.pubkey())
        .instance(instance_pda)
        .system_program(SYSTEM_PROGRAM_ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let recent_blockhash = client.get_latest_blockhash()?;
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&admin_keypair.pubkey()),
        &[&admin_keypair, &instance_seed],
        recent_blockhash,
    );

    println!("Sending transaction...");
    let signature = client.send_and_confirm_transaction(&transaction)?;

    println!("\n✅ Success!");
    println!("Transaction signature: {}", signature);
    println!("\n📝 Use this instance ID in your config:");
    println!("escrow_instance_id = \"{}\"", instance_pda);

    Ok(())
}
