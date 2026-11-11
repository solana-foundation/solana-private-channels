//! Write a mint's escrow account data to a file, for `confidential_verify` to check the
//! instance keys against on the offline machine. The data is public; this tool reads no key.
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::{env, error::Error, str::FromStr};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let [_, rpc_url, instance, mint, out_file] = args.as_slice() else {
        let name = args.first().map_or("escrow_account_data", String::as_str);
        eprintln!("Usage: {name} <rpc-url> <instance-id> <mint-address> <out-file>");
        eprintln!("\nWrites the raw data of the mint's escrow account, which is public. Carry the");
        eprintln!(
            "file to the offline machine and run confidential_verify on each copy against it."
        );
        std::process::exit(1);
    };
    let instance = Pubkey::from_str(instance)?;
    let mint = Pubkey::from_str(mint)?;
    let escrow_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &spl_token_2022::ID);
    let data = RpcClient::new(rpc_url.clone())
        .get_account_data(&escrow_ata)
        .map_err(|e| format!("could not read escrow account {escrow_ata}: {e}"))?;
    std::fs::write(out_file, &data).map_err(|e| format!("could not write {out_file}: {e}"))?;
    println!("Escrow account: {escrow_ata}");
    println!("Wrote {} bytes to {out_file}", data.len());
    Ok(())
}
