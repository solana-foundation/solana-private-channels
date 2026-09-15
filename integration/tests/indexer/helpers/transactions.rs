use private_channel_escrow_program_client::{
    instructions::ReleaseFundsBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_indexer::operator::{
    find_allowed_mint_pda, find_event_authority_pda, find_operator_pda, find_withdrawal_bitmap_pda,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::Message,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as system_instruction;
use spl_associated_token_account::get_associated_token_address_with_program_id;

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

/// Consume a withdrawal nonce on-chain directly, without an operator. Sending the release
/// from the test sidesteps operator lifecycle: a shut-down operator keeps the sender's
/// advisory lock, so a second one against the same database exits immediately.
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub async fn release_funds_on_chain(
    client: &RpcClient,
    admin: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    user: Pubkey,
    amount: u64,
    nonce: u64,
) -> Result<Signature, Box<dyn std::error::Error>> {
    let token_program = spl_token::id();
    let release_ix = ReleaseFundsBuilder::new()
        .payer(admin.pubkey())
        .operator(admin.pubkey())
        .instance(instance)
        .withdrawal_bitmap(find_withdrawal_bitmap_pda(&instance))
        .operator_pda(find_operator_pda(&instance, &admin.pubkey()))
        .mint(mint)
        .allowed_mint(find_allowed_mint_pda(&instance, &mint))
        .user_ata(get_associated_token_address_with_program_id(
            &user,
            &mint,
            &token_program,
        ))
        .instance_ata(get_associated_token_address_with_program_id(
            &instance,
            &mint,
            &token_program,
        ))
        .token_program(token_program)
        .associated_token_program(spl_associated_token_account::id())
        .event_authority(find_event_authority_pda())
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .amount(amount)
        .user(user)
        .transaction_nonce(nonce)
        .instruction();

    send_and_confirm_instructions(client, &[release_ix], admin, &[admin], "Release Funds").await
}
