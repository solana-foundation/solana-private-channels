use solana_client::nonblocking::rpc_client::RpcClient;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    instruction::Instruction,
    message::Message,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as system_instruction;

const COMPUTE_UNIT_LIMIT: u32 = 200_000;
const COMPUTE_UNIT_PRICE: u64 = 1;
#[allow(dead_code)]
const AIRDROP_AMOUNT: u64 = 10_000_000_000;

pub async fn send_and_confirm_instructions(
    client: &RpcClient,
    instructions: &[Instruction],
    payer: &Keypair,
    signers: &[&Keypair],
    description: &str,
) -> Result<Signature, Box<dyn std::error::Error>> {
    let mut all_instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT),
        ComputeBudgetInstruction::set_compute_unit_price(COMPUTE_UNIT_PRICE),
    ];
    all_instructions.extend_from_slice(instructions);

    let recent_blockhash = client.get_latest_blockhash().await?;
    let message = Message::new(&all_instructions, Some(&payer.pubkey()));
    let mut transaction = Transaction::new_unsigned(message);
    transaction.sign(signers, recent_blockhash);

    let signature = client
        .send_and_confirm_transaction(&transaction)
        .await
        .map_err(|e| format!("Failed to {}: {}", description.to_lowercase(), e))?;

    Ok(signature)
}

#[allow(dead_code)]
pub async fn setup_wallets(
    client: &RpcClient,
    faucet_keypair: &Keypair,
    wallets: &[&Keypair],
) -> Result<(), Box<dyn std::error::Error>> {
    if wallets.is_empty() {
        return Ok(());
    }

    // batch all SOL transfers into a single transaction instead of
    // one confirmation round-trip per wallet.  A single `send_and_confirm_transaction`
    // with N transfer instructions replaces N sequential confirmations, reducing
    // setup time from O(N × confirmation_latency) to O(1 × confirmation_latency).
    let transfer_ixs: Vec<Instruction> = wallets
        .iter()
        .map(|w| {
            system_instruction::transfer(&faucet_keypair.pubkey(), &w.pubkey(), AIRDROP_AMOUNT)
        })
        .collect();

    send_and_confirm_instructions(
        client,
        &transfer_ixs,
        faucet_keypair,
        &[faucet_keypair],
        "Fund Wallets",
    )
    .await?;

    for wallet in wallets {
        println!(
            "Airdropped {} SOL to {}. New balance: {} SOL",
            AIRDROP_AMOUNT,
            wallet.pubkey(),
            client.get_balance(&wallet.pubkey()).await?
        );
    }

    Ok(())
}
