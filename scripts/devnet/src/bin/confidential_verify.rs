//! Check that a copy of the escrow instance's confidential keys is the instance key, before
//! funds depend on it: against the pinned pubkey (ElGamal half), or against a configured
//! escrow account on chain (ElGamal and AES halves).
use devnet_scripts::confidential::{read_keys_file, verify_against_account, verify_against_pin};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::{env, error::Error, path::Path, str::FromStr};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    match args.as_slice() {
        [_, keys_file, pin] => {
            let keys = read_keys_file(Path::new(keys_file))?;
            verify_against_pin(&keys, pin)?;
            println!("OK: {keys_file} matches the pinned ElGamal pubkey.");
            println!("Its AES key is checked only against a configured escrow account.");
        }
        [_, keys_file, rpc_url, instance, mint] => {
            let keys = read_keys_file(Path::new(keys_file))?;
            let instance = Pubkey::from_str(instance)?;
            let mint = Pubkey::from_str(mint)?;
            let escrow_ata =
                get_associated_token_address_with_program_id(&instance, &mint, &spl_token_2022::ID);
            let data = RpcClient::new(rpc_url.clone())
                .get_account_data(&escrow_ata)
                .map_err(|e| format!("could not read escrow account {escrow_ata}: {e}"))?;
            verify_against_account(&keys, &escrow_ata, &data)?;
            println!(
                "OK: {keys_file} is the key escrow account {escrow_ata} is configured under \
                 (ElGamal and AES)."
            );
        }
        _ => {
            let name = args.first().map_or("confidential_verify", String::as_str);
            eprintln!("Usage: {name} <instance-keys-file> <pinned-pubkey>");
            eprintln!("       {name} <instance-keys-file> <rpc-url> <instance-id> <mint-address>");
            eprintln!("\nThe pin is the ESCROW_CONFIDENTIAL_PUBKEY=... line keygen printed.");
            eprintln!("The pin proves the ElGamal half; the chain form proves both halves,");
            eprintln!("against the mint's configured escrow account. Run it on every copy.");
            std::process::exit(1);
        }
    }
    Ok(())
}
