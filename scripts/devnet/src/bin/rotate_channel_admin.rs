use devnet_scripts::{
    rotate_channel_admin::{
        decode_mint, leftover_check, refuse_delegate, refuse_empty, rotation_instructions,
        ChannelMint,
    },
    signer::{load_signer, parse_signer_arg, sign, signer_pubkey},
};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSimulateTransactionConfig};
use solana_keychain::Signer;
use solana_sdk::{pubkey::Pubkey, transaction::Transaction};
use std::{env, error::Error, str::FromStr};
use tokio::runtime::Runtime;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const USAGE: &str = "Usage: rotate_channel_admin <channel-rpc> <old-admin-keypair | env:PREFIX> \
<new-admin-pubkey> [--indexer-db-url <url>] [--permanent-delegate <pubkey>] [--allow-empty]";

// `mints` gets a row only from an indexed AllowMint, so an indexer started after one has
// no row; deposits and status history name those mints too.
const MINTS_QUERY: &str = "SELECT mint_address FROM mints \
     UNION SELECT mint_address FROM mint_status_history \
     UNION SELECT mint AS mint_address FROM transactions WHERE transaction_type = 'deposit'";

fn list_mints(rt: &Runtime, db_url: &str) -> Result<Vec<Pubkey>> {
    let rows: Vec<String> = rt.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(db_url)
            .await?;
        let rows = sqlx::query_scalar(MINTS_QUERY).fetch_all(&pool).await;
        pool.close().await;
        rows
    })?;
    let mut mints = rows
        .iter()
        .map(|m| Pubkey::from_str(m).map_err(|e| format!("bad mint address {m} in DB: {e}")))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    mints.sort();
    Ok(mints)
}

/// Reads every listed mint from the channel. A mint with no channel account was never
/// deposited into the channel and has nothing to rotate.
fn read_mints(client: &RpcClient, mints: &[Pubkey]) -> Result<Vec<ChannelMint>> {
    let mut found = Vec::new();
    for mint in mints {
        match client
            .get_account_with_commitment(mint, client.commitment())?
            .value
        {
            Some(account) => found.push(decode_mint(*mint, account.owner, &account.data)?),
            None => println!("  {mint}: not on the channel, skipped"),
        }
    }
    Ok(found)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("{USAGE}");
        eprintln!("\nMoves MintTokens and FreezeAccount of every channel receipt mint held by");
        eprintln!("the old admin to the new one, then re-reads them and fails on any left.");
        eprintln!("The DB URL falls back to DATABASE_URL, keeping its password off argv.");
        eprintln!(
            "Set PRIVATE_CHANNEL_READ_RPC_URL to the read node when the gateway enforces JWT."
        );
        eprintln!("env:PREFIX loads the old admin from PREFIX_SIGNER and its backend vars");
        eprintln!("(e.g. PREFIX_VAULT_ADDR), named like the operators' ADMIN_* vars.");
        eprintln!("--allow-empty: finish even when the indexer DB lists no mint.");
        std::process::exit(1);
    }
    let rpc_url = &args[0];
    let rt = Runtime::new()?;
    let old_admin = rt
        .block_on(load_signer(&parse_signer_arg(&args[1])?))
        .map_err(|e| format!("old admin: {e}"))?;
    let new_admin = Pubkey::from_str(&args[2])?;
    let mut db_url = env::var("DATABASE_URL").ok();
    let mut configured_delegate: Option<Pubkey> = None;
    let mut allow_empty = false;
    let mut rest = args[3..].iter();
    while let Some(flag) = rest.next() {
        if flag == "--allow-empty" {
            allow_empty = true;
            continue;
        }
        let value = rest
            .next()
            .ok_or(format!("{flag} needs a value\n{USAGE}"))?;
        match flag.as_str() {
            "--indexer-db-url" => db_url = Some(value.clone()),
            "--permanent-delegate" => configured_delegate = Some(Pubkey::from_str(value)?),
            _ => return Err(format!("unknown flag {flag}\n{USAGE}").into()),
        }
    }
    let db_url = db_url.ok_or(format!(
        "pass --indexer-db-url or set DATABASE_URL\n{USAGE}"
    ))?;
    let old = signer_pubkey(&old_admin);
    if old == new_admin {
        return Err("old and new admin are the same key".into());
    }
    refuse_delegate(&old, configured_delegate.iter())?;

    let client = RpcClient::new(rpc_url.to_string());
    let read_client = match env::var("PRIVATE_CHANNEL_READ_RPC_URL") {
        Ok(url) if !url.trim().is_empty() => RpcClient::new(url),
        _ => RpcClient::new(rpc_url.to_string()),
    };

    let listed = list_mints(&rt, &db_url)?;
    println!("Old admin {old}, new admin {new_admin}");
    println!("{} mint(s) listed by the indexer DB", listed.len());
    refuse_empty(listed.len(), allow_empty)?;
    let mints = read_mints(&read_client, &listed)?;
    // Each mint's own PermanentDelegate extension is authoritative for the refusal.
    refuse_delegate(
        &old,
        mints.iter().filter_map(|m| m.permanent_delegate.as_ref()),
    )?;

    for mint in &mints {
        let instructions = rotation_instructions(std::slice::from_ref(mint), &old, &new_admin)?;
        if instructions.is_empty() {
            println!("  {}: not held by old admin, skipped", mint.mint);
            continue;
        }
        send(&rt, &client, &read_client, &old_admin, &instructions)?;
        println!(
            "  {}: {} authority(ies) moved",
            mint.mint,
            instructions.len()
        );
    }

    let after = read_mints(&read_client, &listed)?;
    leftover_check(&after, &old)?;
    println!("Done: no mint has {old} as mint or freeze authority");
    Ok(())
}

fn send(
    rt: &Runtime,
    client: &RpcClient,
    read_client: &RpcClient,
    signer: &Signer,
    instructions: &[solana_sdk::instruction::Instruction],
) -> Result<()> {
    let mut tx = Transaction::new_with_payer(instructions, Some(&signer_pubkey(signer)));
    tx.message.recent_blockhash = client.get_latest_blockhash()?;
    rt.block_on(sign(signer, &mut tx))?;
    // Simulate first so a refusal shows its reason rather than a confirm timeout.
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
    println!("  signature {signature}");
    Ok(())
}
