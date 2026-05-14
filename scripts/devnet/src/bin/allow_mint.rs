use private_channel_escrow_program_client::{
    instructions::{AllowMint, AllowMintInstructionArgs},
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
    transaction::Transaction,
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use spl_associated_token_account::get_associated_token_address;
use std::{env, error::Error, str::FromStr};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const ALLOWED_MINT_SEED: &[u8] = b"allowed_mint";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

fn find_allowed_mint_pda(instance: &Pubkey, mint: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[ALLOWED_MINT_SEED, instance.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        eprintln!(
            "Usage: {} <rpc-url> <admin-keypair-path> <instance-id> <mint-address>",
            args[0]
        );
        eprintln!("Example: {} https://api.devnet.solana.com ./keypairs/admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv So11111111111111111111111111111111111111112", args[0]);
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let instance_id = Pubkey::from_str(&args[3])?;
    let mint = Pubkey::from_str(&args[4])?;

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);
    println!("Instance: {}", instance_id);
    println!("Mint: {}", mint);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_id, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata = get_associated_token_address(&instance_id, &mint);

    println!("\nAllowing mint for instance...");
    println!("Allowed Mint PDA: {}", allowed_mint_pda);
    println!("Instance ATA: {}", instance_ata);

    let instruction = AllowMint {
        payer: admin_keypair.pubkey(),
        admin: admin_keypair.pubkey(),
        instance: instance_id,
        mint,
        allowed_mint: allowed_mint_pda,
        instance_ata,
        system_program: SYSTEM_PROGRAM_ID,
        token_program: spl_token::ID,
        associated_token_program: spl_associated_token_account::ID,
        event_authority: event_authority_pda,
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction(AllowMintInstructionArgs { bump });

    let recent_blockhash = client.get_latest_blockhash()?;
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&admin_keypair.pubkey()),
        &[&admin_keypair],
        recent_blockhash,
    );

    println!("Sending transaction...");
    let signature = client.send_and_confirm_transaction(&transaction)?;

    println!("\n✅ Success!");
    println!("Transaction signature: {}", signature);
    println!("Mint {} allowed for instance {}", mint, instance_id);

    Ok(())
}
