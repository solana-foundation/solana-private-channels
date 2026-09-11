use private_channel_escrow_program_client::{
    instructions::{BlockMint, BlockMintInstructionArgs},
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

    if args.len() < 7 {
        eprintln!(
            "Usage: {} <rpc-url> <escrow-admin-keypair-path> <instance-id> <mint-address> <block-deposits> <block-withdrawals>",
            args[0]
        );
        eprintln!("Both gates are absolute: passing false for one re-opens it.");
        eprintln!("Example (stop deposits, keep withdrawals open): {} https://api.devnet.solana.com ./keypairs/escrow-admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv So11111111111111111111111111111111111111112 true false", args[0]);
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let instance_id = Pubkey::from_str(&args[3])?;
    let mint = Pubkey::from_str(&args[4])?;
    let block_deposits = bool::from_str(&args[5])
        .map_err(|_| format!("block-deposits must be true or false, got '{}'", args[5]))?;
    let block_withdrawals = bool::from_str(&args[6])
        .map_err(|_| format!("block-withdrawals must be true or false, got '{}'", args[6]))?;

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);
    println!("Instance: {}", instance_id);
    println!("Mint: {}", mint);
    println!("Block deposits: {}", block_deposits);
    println!("Block withdrawals: {}", block_withdrawals);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&instance_id, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    // BlockMint only updates the existing PDA, so a mint that was never allowed
    // has nothing to set. Fail here rather than on-chain.
    if client.get_account(&allowed_mint_pda).is_err() {
        return Err(format!(
            "No AllowedMint account at {} — allow the mint first",
            allowed_mint_pda
        )
        .into());
    }

    println!("\nSetting mint gates...");
    println!("Allowed Mint PDA: {}", allowed_mint_pda);

    let instruction = BlockMint {
        payer: admin_keypair.pubkey(),
        admin: admin_keypair.pubkey(),
        instance: instance_id,
        mint,
        allowed_mint: allowed_mint_pda,
        event_authority: event_authority_pda,
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction(BlockMintInstructionArgs {
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
        "Mint {} gates set for instance {} (deposits_blocked={}, withdrawals_blocked={})",
        mint, instance_id, block_deposits, block_withdrawals
    );

    Ok(())
}
