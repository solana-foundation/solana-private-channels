use devnet_scripts::{ix_v3_to_sdk, to_addr};
use private_channel_escrow_program_client::{
    instructions::{AddOperator, AddOperatorInstructionArgs},
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

const OPERATOR_SEED: &[u8] = b"operator";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";

fn program_id_as_pubkey() -> Pubkey {
    Pubkey::new_from_array(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_bytes())
}

fn find_operator_pda(instance: &Pubkey, operator: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[OPERATOR_SEED, instance.as_ref(), operator.as_ref()],
        &program_id_as_pubkey(),
    )
}

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &program_id_as_pubkey())
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        eprintln!(
            "Usage: {} <rpc-url> <admin-keypair-path> <instance-id> <operator-pubkey>",
            args[0]
        );
        eprintln!("Example: {} https://api.devnet.solana.com ./keypairs/admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv 5s3eemaoNZ4mW39w6ACBuy84EGo4UKMvXTTZxBkur6ma", args[0]);
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let instance_id = Pubkey::from_str(&args[3])?;
    let operator_pubkey = Pubkey::from_str(&args[4])?;

    println!("Connecting to: {}", rpc_url);
    println!("Using admin keypair: {}", keypair_path);
    println!("Instance: {}", instance_id);
    println!("Operator: {}", operator_pubkey);

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    let (operator_pda, bump) = find_operator_pda(&instance_id, &operator_pubkey);
    let (event_authority_pda, _) = find_event_authority_pda();

    println!("\nAdding operator to instance...");
    println!("Operator PDA: {}", operator_pda);

    let instruction = AddOperator {
        payer: to_addr(admin_keypair.pubkey()),
        admin: to_addr(admin_keypair.pubkey()),
        instance: to_addr(instance_id),
        operator: to_addr(operator_pubkey),
        operator_pda: to_addr(operator_pda),
        system_program: to_addr(SYSTEM_PROGRAM_ID),
        event_authority: to_addr(event_authority_pda),
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction(AddOperatorInstructionArgs { bump });

    let recent_blockhash = client.get_latest_blockhash()?;
    let transaction = Transaction::new_signed_with_payer(
        &[ix_v3_to_sdk(instruction)],
        Some(&admin_keypair.pubkey()),
        &[&admin_keypair],
        recent_blockhash,
    );

    println!("Sending transaction...");
    let signature = client.send_and_confirm_transaction(&transaction)?;

    println!("\n✅ Success!");
    println!("Transaction signature: {}", signature);
    println!(
        "Operator {} added to instance {}",
        operator_pubkey, instance_id
    );

    Ok(())
}
