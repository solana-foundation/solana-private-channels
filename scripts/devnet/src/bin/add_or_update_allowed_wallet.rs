use private_channel_escrow_program_client::{
    instructions::{AddOrUpdateAllowedWallet, AddOrUpdateAllowedWalletInstructionArgs},
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
    transaction::Transaction,
};
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use std::{env, error::Error, str::FromStr};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const ALLOWED_WALLET_SEED: &[u8] = b"allowed_wallet";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

fn find_allowed_wallet_pda(instance: &Pubkey, wallet: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[ALLOWED_WALLET_SEED, instance.as_ref(), wallet.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 7 {
        eprintln!(
            "Usage: {} <rpc-url> <escrow-admin-keypair-path> <instance-id> <wallet-pubkey> <block-deposits> <block-withdrawals>",
            args[0]
        );
        eprintln!("Adds the wallet, or rewrites the flags of its existing entry.");
        eprintln!("Example (allow both ways): {} https://api.devnet.solana.com ./keypairs/escrow-admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv 5s3eemaoNZ4mW39w6ACBuy84EGo4UKMvXTTZxBkur6ma false false", args[0]);
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let instance_id = Pubkey::from_str(&args[3])?;
    let wallet = Pubkey::from_str(&args[4])?;
    let block_deposits = bool::from_str(&args[5])
        .map_err(|_| format!("block-deposits must be true or false, got '{}'", args[5]))?;
    let block_withdrawals = bool::from_str(&args[6])
        .map_err(|_| format!("block-withdrawals must be true or false, got '{}'", args[6]))?;

    // The program rejects an entry blocked both ways; it would equal no entry.
    if block_deposits && block_withdrawals {
        return Err("blocking both directions is rejected; remove the wallet instead".into());
    }

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);
    println!("Instance: {}", instance_id);
    println!("Wallet: {}", wallet);
    println!("Block deposits: {}", block_deposits);
    println!("Block withdrawals: {}", block_withdrawals);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    let (allowed_wallet_pda, bump) = find_allowed_wallet_pda(&instance_id, &wallet);
    let (event_authority_pda, _) = find_event_authority_pda();

    println!("\nWriting whitelist entry...");
    println!("Allowed Wallet PDA: {}", allowed_wallet_pda);

    let instruction = AddOrUpdateAllowedWallet {
        payer: admin_keypair.pubkey(),
        admin: admin_keypair.pubkey(),
        instance: instance_id,
        wallet,
        allowed_wallet_pda,
        system_program: SYSTEM_PROGRAM_ID,
        event_authority: event_authority_pda,
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction(AddOrUpdateAllowedWalletInstructionArgs {
        bump,
        block_deposits,
        block_withdrawals,
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
        "Wallet {} on instance {} (deposits_blocked={}, withdrawals_blocked={})",
        wallet, instance_id, block_deposits, block_withdrawals
    );

    Ok(())
}
