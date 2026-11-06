use devnet_scripts::pending_withdrawals::{gate, pending_query, Pending};
use private_channel_escrow_program_client::{
    instructions::RemoveOperator, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
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

fn find_operator_pda(instance: &Pubkey, operator: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[OPERATOR_SEED, instance.as_ref(), operator.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
}

fn find_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
}

/// The oldest withdrawal not yet settled, if any.
fn read_pending(db_url: &str) -> Result<Option<Pending>> {
    let row: Option<(i64, i64, String, Option<i64>, String, String)> =
        tokio::runtime::Runtime::new()?.block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(db_url)
                .await?;
            let row = sqlx::query_as(&pending_query()).fetch_optional(&pool).await;
            pool.close().await;
            row
        })?;
    Ok(row.map(
        |(count, id, status, nonce, signature, created_at)| Pending {
            count,
            id,
            status,
            nonce,
            signature,
            created_at,
        },
    ))
}

fn main() -> Result<()> {
    let mut args: Vec<String> = Vec::new();
    let mut db_url = env::var("DATABASE_URL").ok();
    let mut force = false;
    let mut all = env::args();
    while let Some(arg) = all.next() {
        match arg.as_str() {
            "--force" => force = true,
            "--indexer-db-url" => {
                db_url = Some(all.next().ok_or("--indexer-db-url needs a value")?)
            }
            flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}").into()),
            _ => args.push(arg),
        }
    }

    if args.len() != 5 {
        eprintln!(
            "Usage: {} <rpc-url> <escrow-admin-keypair-path> <instance-id> <operator-pubkey> \
             [--indexer-db-url <url>] [--force]",
            args[0]
        );
        eprintln!("Example: {} https://api.devnet.solana.com ./keypairs/escrow-admin.json 9F2CJEevdBVaPJwr1iCayZMT9Acvg7twG4JnjYf9G2zv 5s3eemaoNZ4mW39w6ACBuy84EGo4UKMvXTTZxBkur6ma", args[0]);
        eprintln!("\nRefuses while the indexer DB holds a withdrawal that is not settled.");
        eprintln!("The DB URL falls back to DATABASE_URL, keeping its password off argv.");
        eprintln!("--force skips that refusal with a warning.");
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

    match &db_url {
        Some(url) => {
            // A compromised key must stay removable even when the DB cannot be read.
            match read_pending(url) {
                Err(e) if force => eprintln!(
                    "WARNING: --force: indexer DB not readable ({e}), in-flight withdrawals not checked"
                ),
                pending => match gate(pending?.as_ref(), force)? {
                    Some(warning) => eprintln!("{warning}"),
                    None => println!("No withdrawal in flight"),
                },
            }
        }
        None if force => {
            eprintln!("WARNING: --force: no indexer DB given, in-flight withdrawals not checked")
        }
        None => {
            return Err(
                "pass --indexer-db-url or set DATABASE_URL to check in-flight \
                        withdrawals (or --force to skip the check)"
                    .into(),
            )
        }
    }

    let client = RpcClient::new(rpc_url.to_string());
    let admin_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("Admin pubkey: {}", admin_keypair.pubkey());

    let (operator_pda, _) = find_operator_pda(&instance_id, &operator_pubkey);
    let (event_authority_pda, _) = find_event_authority_pda();

    println!("\nRemoving operator from instance...");
    println!("Operator PDA: {}", operator_pda);

    // The closed PDA's rent goes to the payer, here the admin.
    let instruction = RemoveOperator {
        payer: admin_keypair.pubkey(),
        admin: admin_keypair.pubkey(),
        instance: instance_id,
        operator: operator_pubkey,
        operator_pda,
        system_program: SYSTEM_PROGRAM_ID,
        event_authority: event_authority_pda,
        private_channel_escrow_program: PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    }
    .instruction();

    let recent_blockhash = client.get_latest_blockhash()?;
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&admin_keypair.pubkey()),
        &[&admin_keypair],
        recent_blockhash,
    );

    println!("Sending transaction...");
    let signature = client.send_and_confirm_transaction(&transaction)?;

    println!("\nSuccess!");
    println!("Transaction signature: {}", signature);
    println!(
        "Operator {} removed from instance {}",
        operator_pubkey, instance_id
    );

    Ok(())
}
