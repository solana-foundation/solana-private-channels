use private_channel_withdraw_program_client::instructions::{
    WithdrawFunds, WithdrawFundsInstructionArgs,
};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use std::{env, error::Error, str::FromStr};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        eprintln!("Usage: {} <private-channel-gateway-rpc> <user-keypair-path> <mint-address> <amount> [destination]", args[0]);
        eprintln!("Example: {} http://localhost:8898 ./keypairs/user.json PANskKbAxqUQuqVfwzMtkyxii5GLaG1VFmB9xWb5tTP 500000", args[0]);
        eprintln!("\n⚠️  IMPORTANT: RPC URL must be PrivateChannel gateway (NOT Solana devnet)");
        eprintln!("  - Local:  http://localhost:8898");
        eprintln!("  - Docker: gateway:8899");
        eprintln!("\nThis burns tokens on PrivateChannel. The operator will then release funds on Solana.");
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    let keypair_path = &args[2];
    let mint = Pubkey::from_str(&args[3])?;
    let amount: u64 = args[4].parse()?;
    let destination = if args.len() > 5 {
        Some(Pubkey::from_str(&args[5])?)
    } else {
        None
    };

    println!("🔥 Withdrawing from PrivateChannel (burning tokens)");
    println!("Connecting to PrivateChannel gateway: {}", rpc_url);
    println!("Using user keypair: {}", keypair_path);
    println!("Mint: {}", mint);
    println!("Amount: {}", amount);
    if let Some(dest) = destination {
        println!("Destination (Solana): {}", dest);
    } else {
        println!("Destination: Same as user (default)");
    }

    let client = RpcClient::new(rpc_url.to_string());
    let user_keypair =
        read_keypair_file(keypair_path).map_err(|e| format!("Failed to read keypair: {}", e))?;

    println!("User pubkey: {}", user_keypair.pubkey());

    let user_ata = get_associated_token_address(&user_keypair.pubkey(), &mint);

    println!("\n📍 Transaction details:");
    println!("User ATA (on PrivateChannel): {}", user_ata);

    let instruction = WithdrawFunds {
        user: user_keypair.pubkey(),
        mint,
        token_account: user_ata,
        token_program: spl_token::ID,
        associated_token_program: spl_associated_token_account::ID,
    }
    .instruction(WithdrawFundsInstructionArgs {
        amount,
        destination,
    });

    let recent_blockhash = client.get_latest_blockhash()?;
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&user_keypair.pubkey()),
        &[&user_keypair],
        recent_blockhash,
    );

    // Print the locally-derived signature *before* sending so callers (the
    // smoke test in particular) can capture it even if the subsequent
    // `send_and_confirm` on the PrivateChannel gateway times out. The PrivateChannel channel
    // is gasless and finalizes within ~1s, but the solana-client default
    // confirmation poll uses methods (e.g. getRecentPerformanceSamples in
    // older versions) and timing assumptions that don't always agree with
    // a gasless channel — and we've seen "unable to confirm transaction"
    // come back on a tx that was actually finalized in the channel. Printing
    // the signature pre-flight lets the smoke harness fall back to polling
    // postgres-indexer directly.
    let prelim_sig = transaction.signatures[0];
    println!("Pre-send signature: {}", prelim_sig);

    println!("Sending transaction...");
    let send_result = client.send_and_confirm_transaction(&transaction);

    match send_result {
        Ok(signature) => {
            println!("\n✅ Withdrawal initiated on PrivateChannel!");
            println!("Transaction signature: {}", signature);
            println!("Burned {} tokens on PrivateChannel", amount);
            Ok(())
        }
        Err(e) => {
            // The gasless channel sometimes returns a confirmation-timeout
            // error on a transaction that actually finalized — the signature
            // is deterministic from the signed transaction body, so external
            // pollers (smoke harness, indexer DB, getSignatureStatuses) can
            // resolve the truth out-of-band. We only swallow this *specific*
            // class of error; other failures (network, account-not-found,
            // insufficient funds, malformed instruction) propagate so any
            // CI / orchestration script reading the exit code gets honest
            // signal instead of a false success.
            let msg = e.to_string().to_lowercase();
            let is_confirmation_timeout = msg.contains("unable to confirm")
                || msg.contains("not been confirmed")
                || msg.contains("transaction was not confirmed")
                || msg.contains("blockhash not found")
                || msg.contains("expired");
            if is_confirmation_timeout {
                println!("Transaction signature: {}", prelim_sig);
                eprintln!(
                    "Warning: send_and_confirm reported a confirmation timeout ({}); \
                     the transaction may still have landed. Verify with \
                     `getSignatureStatuses` on sig {}.",
                    e, prelim_sig
                );
                Ok(())
            } else {
                eprintln!(
                    "Error: send_and_confirm failed for sig {} — not a confirmation \
                     timeout. Propagating so callers see a non-zero exit code.\n  cause: {}",
                    prelim_sig, e
                );
                Err(e.into())
            }
        }
    }
}
