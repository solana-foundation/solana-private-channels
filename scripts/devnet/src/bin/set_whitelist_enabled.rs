use private_channel_escrow_program_client::{
    accounts::Instance,
    instructions::{SetWhitelistEnabled, SetWhitelistEnabledInstructionArgs},
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
    transaction::Transaction,
};
use std::{env, error::Error, str::FromStr};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 6 {
        eprintln!(
            "Usage: {} <rpc-url> <escrow-admin-keypair-path> <instance-id> <whitelist-deposits> <whitelist-withdrawals>",
            args[0]
        );
        eprintln!("Both flags are absolute: passing false for one turns it off.");
        eprintln!("Example (whitelist deposits only): {} https://api.devnet.solana.com ./keypairs/escrow-admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv true false", args[0]);
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let instance_id = Pubkey::from_str(&args[3])?;
    let whitelist_deposits = bool::from_str(&args[4]).map_err(|_| {
        format!(
            "whitelist-deposits must be true or false, got '{}'",
            args[4]
        )
    })?;
    let whitelist_withdrawals = bool::from_str(&args[5]).map_err(|_| {
        format!(
            "whitelist-withdrawals must be true or false, got '{}'",
            args[5]
        )
    })?;

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);
    println!("Instance: {}", instance_id);
    println!("Whitelist deposits: {}", whitelist_deposits);
    println!("Whitelist withdrawals: {}", whitelist_withdrawals);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    let (event_authority_pda, _) = find_event_authority_pda();

    let instance_account = client.get_account(&instance_id)?;
    let current = Instance::from_bytes(&instance_account.data)
        .map_err(|e| format!("Failed to decode Instance: {}", e))?;

    println!("\nSetting whitelist flags...");
    println!(
        "Current on-chain: deposits_whitelisted={}, withdrawals_whitelisted={}",
        current.deposits_whitelisted, current.withdrawals_whitelisted
    );

    // Both flags are absolute, so a flag the caller did not set is turned off
    // whether or not they knew it was on.
    if current.deposits_whitelisted && !whitelist_deposits {
        println!("WARNING: this turns off the deposit whitelist.");
    }
    if current.withdrawals_whitelisted && !whitelist_withdrawals {
        println!("WARNING: this turns off the withdrawal whitelist.");
    }

    let instruction = SetWhitelistEnabled {
        payer: admin_keypair.pubkey(),
        admin: admin_keypair.pubkey(),
        instance: instance_id,
        event_authority: event_authority_pda,
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction(SetWhitelistEnabledInstructionArgs {
        whitelist_deposits,
        whitelist_withdrawals,
    });

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
    println!(
        "Instance {} whitelist set (deposits_whitelisted={}, withdrawals_whitelisted={})",
        instance_id, whitelist_deposits, whitelist_withdrawals
    );

    Ok(())
}
