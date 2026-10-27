use devnet_scripts::seize::{compliance_ata, seize_instructions, SeizePlan};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSimulateTransactionConfig};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022::{
    extension::StateWithExtensions,
    state::{Account, AccountState, Mint},
};
use std::{env, error::Error, str::FromStr};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const USAGE: &str = "Usage: seize <private-channel-rpc> <delegate-keypair> <mint> <source-owner> \
<compliance-owner> [--amount <base-units>] [--freeze-authority <keypair>]";

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 5 {
        eprintln!("{USAGE}");
        eprintln!("\nMoves <source-owner>'s channel balance into the compliance ATA in one tx.");
        eprintln!("All of it unless --amount is given. A frozen account is thawed, moved and");
        eprintln!("frozen again; the freeze authority defaults to the delegate keypair.");
        eprintln!(
            "Set PRIVATE_CHANNEL_READ_RPC_URL to the read node when the gateway enforces JWT."
        );
        std::process::exit(1);
    }
    let rpc_url = &args[0];
    let delegate = read_keypair_file(&args[1]).map_err(|e| format!("delegate keypair: {e}"))?;
    let mint = Pubkey::from_str(&args[2])?;
    let source_owner = Pubkey::from_str(&args[3])?;
    let compliance_owner = Pubkey::from_str(&args[4])?;
    let mut amount: Option<u64> = None;
    let mut freeze_authority: Option<Keypair> = None;
    let mut rest = args[5..].iter();
    while let Some(flag) = rest.next() {
        let value = rest
            .next()
            .ok_or(format!("{flag} needs a value\n{USAGE}"))?;
        match flag.as_str() {
            "--amount" => amount = Some(value.parse()?),
            "--freeze-authority" => {
                freeze_authority =
                    Some(read_keypair_file(value).map_err(|e| format!("freeze authority: {e}"))?)
            }
            _ => return Err(format!("unknown flag {flag}\n{USAGE}").into()),
        }
    }

    let client = RpcClient::new(rpc_url.to_string());
    // With JWT on, the gateway refuses account reads and simulate without a token, so those
    // can go to a read node; sending stays on the gateway.
    let read_client = match env::var("PRIVATE_CHANNEL_READ_RPC_URL") {
        Ok(url) if !url.trim().is_empty() => RpcClient::new(url),
        _ => RpcClient::new(rpc_url.to_string()),
    };
    let source =
        get_associated_token_address_with_program_id(&source_owner, &mint, &spl_token_2022::ID);

    let mint_data = read_client.get_account_data(&mint)?;
    let mint_state = StateWithExtensions::<Mint>::unpack(&mint_data)?;
    let source_data = read_client.get_account_data(&source)?;
    let source_state = StateWithExtensions::<Account>::unpack(&source_data)?.base;

    let amount = amount.unwrap_or(source_state.amount);
    if amount == 0 || amount > source_state.amount {
        return Err(format!(
            "amount {amount} must be between 1 and the source balance {}",
            source_state.amount
        )
        .into());
    }
    let source_frozen = source_state.state == AccountState::Frozen;
    let freeze_signer = freeze_authority.as_ref().unwrap_or(&delegate);
    if source_frozen
        && Option::<Pubkey>::from(mint_state.base.freeze_authority) != Some(freeze_signer.pubkey())
    {
        return Err(
            "the source is frozen and the given key is not the mint's freeze authority; \
                    pass --freeze-authority"
                .into(),
        );
    }

    let plan = SeizePlan {
        mint,
        decimals: mint_state.base.decimals,
        source,
        source_frozen,
        amount,
        delegate: delegate.pubkey(),
        freeze_authority: freeze_signer.pubkey(),
        compliance_owner,
    };
    let mut signers: Vec<&Keypair> = vec![&delegate];
    if source_frozen && freeze_signer.pubkey() != delegate.pubkey() {
        signers.push(freeze_signer);
    }
    let blockhash = client.get_latest_blockhash()?;
    let tx = Transaction::new_signed_with_payer(
        &seize_instructions(&plan),
        Some(&delegate.pubkey()),
        &signers,
        blockhash,
    );

    println!("Seizing {amount} of mint {mint}");
    println!("  from   {source} (owner {source_owner}, frozen: {source_frozen})");
    println!(
        "  to     {} (compliance owner {compliance_owner})",
        compliance_ata(&compliance_owner, &mint)
    );

    // The node drops a refused delegate move silently at ingress, so simulate with
    // sigVerify first to surface the reason.
    let simulation = read_client.simulate_transaction_with_config(
        &tx,
        RpcSimulateTransactionConfig {
            sig_verify: true,
            ..Default::default()
        },
    )?;
    if let Some(err) = simulation.value.err {
        return Err(format!(
            "simulation failed: {err:?}, logs: {:?}",
            simulation.value.logs
        )
        .into());
    }

    let signature = client.send_and_confirm_transaction(&tx)?;
    println!("Seized. Signature: {signature}");
    Ok(())
}
