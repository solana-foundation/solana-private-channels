//! Configure the escrow account of a Token-2022 mint for confidential transfers under the
//! instance's keys (`ConfigureConfidentialAccount`, signed by the escrow instance admin).
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use devnet_scripts::confidential::{
    configure_escrow, indexer_configure_log, read_keys_file, Configured,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::read_keypair_file};
use std::{env, error::Error, path::Path, str::FromStr};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        eprintln!(
            "Usage: {} <rpc-url> <escrow-admin-keypair> <instance-id> <mint-address> <instance-keys-file>",
            args[0]
        );
        eprintln!("\nThe keys file is the one confidential_keygen wrote and ESCROW_CONFIDENTIAL_KEYS holds.");
        eprintln!("Ceremony: block_mint, set ESCROW_CONFIDENTIAL_KEYS and restart indexer-solana and both");
        eprintln!(
            "operators, run this, check indexer-solana logged the line it prints, then allow_mint."
        );
        std::process::exit(1);
    }
    let client = RpcClient::new(args[1].clone());
    let admin = read_keypair_file(&args[2]).map_err(|e| format!("Failed to read keypair: {e}"))?;
    let instance = Pubkey::from_str(&args[3])?;
    let mint = Pubkey::from_str(&args[4])?;
    let keys = read_keys_file(Path::new(&args[5]))?;
    let pubkey = BASE64.encode(keys.keys.pubkey());
    println!("Instance: {instance}");
    println!("Mint: {mint}");
    println!("ElGamal pubkey: {pubkey}");
    match configure_escrow(&client, &admin, &instance, &mint, &keys)? {
        Configured::Already => println!("The escrow account is already configured under {pubkey}"),
        Configured::Sent(signature) => {
            println!("\nConfigured. Transaction signature: {signature}");
            println!(
                "Before allowing deposits, check indexer-solana logged at INFO: {}",
                indexer_configure_log(&mint, &pubkey)
            );
        }
    }
    Ok(())
}
