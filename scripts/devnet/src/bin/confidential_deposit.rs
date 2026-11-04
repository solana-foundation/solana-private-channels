//! Deposit confidentially into the escrow (`ConfidentialDeposit`). Configures the user's
//! Token-2022 account for confidential transfers if needed and tops its available balance up
//! from pending, then from public, before proving the transfer to the escrow's key.
use devnet_scripts::{confidential::read_keys_file, confidential_deposit::deposit};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::{env, error::Error, path::Path, str::FromStr};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 7 {
        eprintln!(
            "Usage: {} <rpc-url> <user-keypair> <user-keys-file> <instance-id> <mint-address> <amount> [recipient]",
            args[0]
        );
        eprintln!(
            "\nThe user keys file is the user's own, from confidential_keygen; not the escrow's."
        );
        eprintln!("The escrow account must already be configured (configure_confidential).");
        eprintln!("Transfer hook mints are not supported by this tool.");
        std::process::exit(1);
    }
    let client = RpcClient::new(args[1].clone());
    let user = read_keypair_file(&args[2]).map_err(|e| format!("Failed to read keypair: {e}"))?;
    let keys = read_keys_file(Path::new(&args[3]))?;
    let instance = Pubkey::from_str(&args[4])?;
    let mint = Pubkey::from_str(&args[5])?;
    let amount: u64 = args[6].parse()?;
    let recipient = args.get(7).map(|r| Pubkey::from_str(r)).transpose()?;

    let escrow_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &spl_token_2022::ID);
    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), &mint, &spl_token_2022::ID);
    println!("User: {}", user.pubkey());
    println!("User account: {user_ata}");
    println!("Escrow account: {escrow_ata}");
    println!("Amount: {amount}");

    let signature = deposit(
        &client,
        &user,
        &keys,
        instance,
        mint,
        amount,
        recipient,
        &[],
        None,
    )?;
    println!("\nConfidentialDeposit sent. Transaction signature: {signature}");
    println!("The channel credits the net amount once indexer-solana sees it.");
    Ok(())
}
