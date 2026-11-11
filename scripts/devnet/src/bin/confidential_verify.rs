//! Check that a copy of the escrow instance's confidential keys is the instance key, before
//! funds depend on it: against the pinned pubkey (ElGamal half), or against a configured
//! escrow account's data from `escrow_account_data` (ElGamal and AES halves). It never uses
//! the network, so it runs on the offline machine that holds the backups.
use devnet_scripts::confidential::{read_keys_file, verify_against_account, verify_against_pin};
use solana_sdk::pubkey::Pubkey;
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
        [_, keys_file, instance, mint, account_file] => {
            let keys = read_keys_file(Path::new(keys_file))?;
            let instance = Pubkey::from_str(instance)?;
            let mint = Pubkey::from_str(mint)?;
            let data = std::fs::read(account_file)
                .map_err(|e| format!("could not read account file {account_file}: {e}"))?;
            verify_against_account(&keys, &instance, &mint, &data)?;
            println!(
                "OK: {keys_file} is the key mint {mint}'s escrow account is configured under \
                 (ElGamal and AES)."
            );
        }
        _ => {
            let name = args.first().map_or("confidential_verify", String::as_str);
            eprintln!("Usage: {name} <instance-keys-file> <pinned-pubkey>");
            eprintln!(
                "       {name} <instance-keys-file> <instance-id> <mint-address> <account-file>"
            );
            eprintln!("\nThe pin is the ESCROW_CONFIDENTIAL_PUBKEY=... line keygen printed.");
            eprintln!("The account file is what escrow_account_data wrote, online. The pin");
            eprintln!("proves the ElGamal half, the account file both. Run it on every copy.");
            std::process::exit(1);
        }
    }
    Ok(())
}
