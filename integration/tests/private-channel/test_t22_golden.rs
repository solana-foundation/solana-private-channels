//! The golden Token-2022 fixtures are exactly what the real Token-2022 program writes.
//!
//! Runs `[InitializePermanentDelegate, InitializeMint2]` and an ATA create plus `MintTo`
//! on a test validator for the fixed fixture keys and compares the bytes.

use {
    private_channel_core::t22_fixtures::{
        golden_authority, golden_mint, GOLDEN_ATA_AMOUNT, GOLDEN_DECIMALS, GOLDEN_DELEGATE,
        GOLDEN_OWNER, GOLDEN_T22_ATA, GOLDEN_T22_MINT, T22_MINT_LEN,
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    solana_sdk::{
        instruction::Instruction,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    spl_associated_token_account::{
        get_associated_token_address_with_program_id,
        instruction::create_associated_token_account_idempotent,
    },
    test_utils::validator_helper::start_test_validator_no_geyser,
};

async fn send(client: &RpcClient, ixs: &[Instruction], payer: &Keypair, signers: &[&Keypair]) {
    let blockhash = client.get_latest_blockhash().await.unwrap();
    let tx = Transaction::new_signed_with_payer(ixs, Some(&payer.pubkey()), signers, blockhash);
    client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("transaction failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn golden_fixtures_match_the_token_2022_program() {
    let (validator, payer) = start_test_validator_no_geyser().await;
    let client = RpcClient::new_with_commitment(validator.rpc_url(), CommitmentConfig::confirmed());
    let t22 = spl_token_2022::id();
    let authority = golden_authority();
    let mint = golden_mint();

    let lamports = client
        .get_minimum_balance_for_rent_exemption(T22_MINT_LEN)
        .await
        .unwrap();
    let create = solana_system_interface::instruction::create_account(
        &payer.pubkey(),
        &mint.pubkey(),
        lamports,
        T22_MINT_LEN as u64,
        &t22,
    );
    let delegate = spl_token_2022::instruction::initialize_permanent_delegate(
        &t22,
        &mint.pubkey(),
        &GOLDEN_DELEGATE,
    )
    .unwrap();
    let init = spl_token_2022::instruction::initialize_mint2(
        &t22,
        &mint.pubkey(),
        &authority.pubkey(),
        Some(&authority.pubkey()),
        GOLDEN_DECIMALS,
    )
    .unwrap();
    send(&client, &[create, delegate, init], &payer, &[&payer, &mint]).await;

    let mint_data = client.get_account_data(&mint.pubkey()).await.unwrap();
    assert_eq!(
        mint_data,
        GOLDEN_T22_MINT.to_vec(),
        "golden mint drifted from the real program"
    );

    let ata = get_associated_token_address_with_program_id(&GOLDEN_OWNER, &mint.pubkey(), &t22);
    let create_ata = create_associated_token_account_idempotent(
        &payer.pubkey(),
        &GOLDEN_OWNER,
        &mint.pubkey(),
        &t22,
    );
    let mint_to = spl_token_2022::instruction::mint_to(
        &t22,
        &mint.pubkey(),
        &ata,
        &authority.pubkey(),
        &[],
        GOLDEN_ATA_AMOUNT,
    )
    .unwrap();
    send(
        &client,
        &[create_ata, mint_to],
        &payer,
        &[&payer, &authority],
    )
    .await;

    let ata_data = client.get_account_data(&ata).await.unwrap();
    assert_eq!(
        ata_data,
        GOLDEN_T22_ATA.to_vec(),
        "golden token account drifted from the real program"
    );
}
