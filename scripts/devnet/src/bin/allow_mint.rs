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
use spl_associated_token_account::get_associated_token_address_with_program_id;
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
            "Usage: {} <rpc-url> <escrow-admin-keypair-path> <instance-id> <mint-address> <withdraw-fee> <min-withdraw-amount>",
            args[0]
        );
        eprintln!("Example: {} https://api.devnet.solana.com ./keypairs/escrow-admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv So11111111111111111111111111111111111111112 10000 1000000", args[0]);
        eprintln!("\n<withdraw-fee> is in the mint's base units, charged on top of every PrivateChannel withdrawal.");
        eprintln!("<min-withdraw-amount> is the smallest amount a PrivateChannel withdrawal may move, in base units.");
        eprintln!("0 is allowed for either but removes its bound on release costs; use it only where every participant is known.");
        eprintln!(
            "Run again with new values, to or from 0, to reprice; they apply from the next deposit."
        );
        eprintln!("Re-running also re-opens both gates and re-pins the mint profile, accepting any change since the last allow.");
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let instance_id = Pubkey::from_str(&args[3])?;
    let mint = Pubkey::from_str(&args[4])?;
    let withdraw_fee: u64 = args[5].parse()?;
    let min_withdraw_amount: u64 = args[6].parse()?;

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);
    println!("Instance: {}", instance_id);
    println!("Mint: {}", mint);
    println!("Withdraw fee: {}", withdraw_fee);
    println!("Minimum withdraw amount: {}", min_withdraw_amount);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    // Detect the mint's token program (legacy SPL Token vs Token-2022) so this
    // works for either; the escrow verifies the mint is owned by the program we pass.
    let token_program = client
        .get_account(&mint)
        .map_err(|e| format!("Failed to fetch mint {}: {}", mint, e))?
        .owner;
    println!("Token program: {}", token_program);

    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance_id, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata =
        get_associated_token_address_with_program_id(&instance_id, &mint, &token_program);

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
        token_program,
        associated_token_program: spl_associated_token_account::ID,
        event_authority: event_authority_pda,
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction(AllowMintInstructionArgs {
        bump,
        withdraw_fee,
        min_withdraw_amount,
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
    println!("Mint {} allowed for instance {}", mint, instance_id);
    println!(
        "Withdraw fee {} and minimum {} reach PrivateChannel with the mint's next deposit",
        withdraw_fee, min_withdraw_amount
    );

    Ok(())
}
