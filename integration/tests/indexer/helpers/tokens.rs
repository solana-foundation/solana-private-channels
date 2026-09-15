use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    program_pack::Pack,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use solana_system_interface::instruction::create_account;
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token::{
    instruction::{initialize_mint, mint_to},
    state::Mint as TokenMint,
};
use spl_token_2022::extension::ExtensionType;
use spl_token_2022::state::Mint as Token2022Mint;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;

use super::transactions::send_and_confirm_instructions;

#[allow(dead_code)]
pub async fn generate_mint(
    client: &RpcClient,
    payer: &Keypair,
    authority: &Keypair,
    mint: &Keypair,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let space = TokenMint::LEN;
    let rent = client.get_minimum_balance_for_rent_exemption(space).await?;

    let instructions = vec![
        create_account(
            &payer.pubkey(),
            &mint.pubkey(),
            rent,
            space as u64,
            &spl_token::id(),
        ),
        initialize_mint(
            &spl_token::id(),
            &mint.pubkey(),
            &authority.pubkey(),
            Some(&authority.pubkey()),
            6, // decimals
        )?,
    ];

    send_and_confirm_instructions(
        client,
        &instructions,
        payer,
        &[payer, mint],
        "Generate Mint",
    )
    .await?;

    Ok(mint.pubkey())
}

#[allow(dead_code)]
pub async fn mint_to_owner(
    client: &RpcClient,
    payer: &Keypair,
    mint: Pubkey,
    owner: Pubkey,
    authority: &Keypair,
    amount: u64,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(&owner, &mint, &spl_token::id());

    let instructions = vec![
        create_associated_token_account_idempotent(
            &payer.pubkey(),
            &owner,
            &mint,
            &spl_token::id(),
        ),
        mint_to(
            &spl_token::id(),
            &mint,
            &ata,
            &authority.pubkey(),
            &[],
            amount,
        )?,
    ];

    send_and_confirm_instructions(
        client,
        &instructions,
        payer,
        &[payer, authority],
        "Mint to Owner",
    )
    .await?;

    Ok(ata)
}

#[allow(dead_code)]
pub async fn get_token_balance(
    client: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(owner, mint, &spl_token::id());

    Ok(client
        .get_token_account_balance(&ata)
        .await?
        .amount
        .parse::<u64>()?)
}

/// Create a Token-2022 mint whose permanent delegate is `delegate`. The delegate can move
/// tokens out of any account for this mint without the owner signing.
#[allow(dead_code)]
pub async fn generate_permanent_delegate_mint_2022(
    client: &RpcClient,
    payer: &Keypair,
    authority: &Keypair,
    delegate: &Pubkey,
    mint: &Keypair,
    decimals: u8,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&[
        ExtensionType::PermanentDelegate,
    ])?;
    let rent = client.get_minimum_balance_for_rent_exemption(space).await?;

    // Extensions must be initialized before the mint itself.
    let instructions = vec![
        create_account(
            &payer.pubkey(),
            &mint.pubkey(),
            rent,
            space as u64,
            &TOKEN_2022_PROGRAM_ID,
        ),
        spl_token_2022::instruction::initialize_permanent_delegate(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            delegate,
        )?,
        spl_token_2022::instruction::initialize_mint2(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            &authority.pubkey(),
            Some(&authority.pubkey()),
            decimals,
        )?,
    ];

    send_and_confirm_instructions(
        client,
        &instructions,
        payer,
        &[payer, mint],
        "Generate Permanent-Delegate Mint (Token-2022)",
    )
    .await?;

    Ok(mint.pubkey())
}

/// Mint Token-2022 tokens to `owner`, creating the owner's associated account if needed.
#[allow(dead_code)]
pub async fn mint_2022_to_owner(
    client: &RpcClient,
    payer: &Keypair,
    mint: Pubkey,
    owner: Pubkey,
    authority: &Keypair,
    amount: u64,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(&owner, &mint, &TOKEN_2022_PROGRAM_ID);

    let instructions = vec![
        create_associated_token_account_idempotent(
            &payer.pubkey(),
            &owner,
            &mint,
            &TOKEN_2022_PROGRAM_ID,
        ),
        spl_token_2022::instruction::mint_to(
            &TOKEN_2022_PROGRAM_ID,
            &mint,
            &ata,
            &authority.pubkey(),
            &[],
            amount,
        )?,
    ];

    send_and_confirm_instructions(
        client,
        &instructions,
        payer,
        &[payer, authority],
        "Mint to Owner (Token-2022)",
    )
    .await?;

    Ok(ata)
}

/// Move tokens out of `source_ata` using the mint's permanent delegate. The escrow program
/// is never invoked, so nothing an indexer watches explains the balance that leaves.
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub async fn drain_via_permanent_delegate(
    client: &RpcClient,
    payer: &Keypair,
    mint: Pubkey,
    source_ata: Pubkey,
    delegate: &Keypair,
    drain_owner: Pubkey,
    amount: u64,
    decimals: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let drain_ata =
        get_associated_token_address_with_program_id(&drain_owner, &mint, &TOKEN_2022_PROGRAM_ID);

    let instructions = vec![
        create_associated_token_account_idempotent(
            &payer.pubkey(),
            &drain_owner,
            &mint,
            &TOKEN_2022_PROGRAM_ID,
        ),
        spl_token_2022::instruction::transfer_checked(
            &TOKEN_2022_PROGRAM_ID,
            &source_ata,
            &mint,
            &drain_ata,
            &delegate.pubkey(),
            &[],
            amount,
            decimals,
        )?,
    ];

    send_and_confirm_instructions(
        client,
        &instructions,
        payer,
        &[payer, delegate],
        "Drain via Permanent Delegate",
    )
    .await?;
    Ok(())
}

/// Token-2022 balance of `owner`'s associated account, zero when the account does not exist.
#[allow(dead_code)]
pub async fn get_token_2022_balance(
    client: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(owner, mint, &TOKEN_2022_PROGRAM_ID);
    match client.get_token_account_balance(&ata).await {
        Ok(balance) => Ok(balance.amount.parse::<u64>()?),
        Err(_) => Ok(0),
    }
}
