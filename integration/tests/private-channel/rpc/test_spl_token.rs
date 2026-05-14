use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::rpc_config::{
    RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
};

use {
    anyhow::Result,
    base64::{engine::general_purpose::STANDARD, Engine},
    private_channel_escrow_program_client::{
        instructions::{
            AddOperatorBuilder, AllowMintBuilder, CreateInstanceBuilder, DepositBuilder,
        },
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
    private_channel_indexer::storage::TransactionType,
    solana_account_decoder_client_types::UiAccountData,
    solana_pubkey::Pubkey,
    solana_sdk::{
        program_pack::Pack, signature::Keypair, signer::Signer, transaction::Transaction,
    },
    spl_associated_token_account::get_associated_token_address_with_program_id,
    spl_token::state::Account as TokenAccount,
    std::time::Duration,
    tokio::time::sleep,
};

use super::{
    test_context::{PrivateChannelContext, SolanaContext},
    utils::{AIRDROP_LAMPORTS, LAMPORTS_PER_SOL, MINT_DECIMALS},
};
use crate::setup;

// Token amounts for this test (in token base units with 3 decimals)
const INITIAL_ALICE_TOKENS: u64 = 1_000_000; // 1000 tokens
const INITIAL_BOB_TOKENS: u64 = 500_000; // 500 tokens
const INITIAL_CHARLIE_TOKENS: u64 = 300_000; // 300 tokens

// Solana Escrow deposit amounts (these get minted on PrivateChannel by the operator)
const SOLANA_ALICE_DEPOSIT: u64 = 200_000; // 200 tokens
const SOLANA_BOB_DEPOSIT: u64 = 150_000; // 150 tokens
const SOLANA_CHARLIE_DEPOSIT: u64 = 100_000; // 100 tokens

/// Request SOL airdrops for all accounts that will need it
async fn request_airdrops(
    private_channel_ctx: &PrivateChannelContext,
    solana_ctx: &SolanaContext,
    user_keypairs: &[&Keypair],
) -> Result<()> {
    println!("\n=== Requesting SOL airdrops on test validator ===");

    // Fund user keypairs
    for keypair in user_keypairs {
        solana_ctx
            .fund_account(&keypair.pubkey(), AIRDROP_LAMPORTS)
            .await?;
        println!(
            "  {} received {} SOL",
            keypair.pubkey(),
            AIRDROP_LAMPORTS / LAMPORTS_PER_SOL
        );
        assert!(
            solana_ctx.get_balance(&keypair.pubkey()).await? > 0,
            "User should have non-zero SOL balance"
        );
    }

    // Fund admin
    solana_ctx
        .fund_account(&private_channel_ctx.operator_key.pubkey(), AIRDROP_LAMPORTS)
        .await?;
    println!(
        "  Admin received {} SOL",
        AIRDROP_LAMPORTS / LAMPORTS_PER_SOL
    );
    assert!(
        solana_ctx
            .get_balance(&private_channel_ctx.operator_key.pubkey())
            .await?
            > 0,
        "Admin should have non-zero SOL balance"
    );

    Ok(())
}

/// Initialize the escrow instance and allow the mint on Solana test validator
async fn setup_solana_escrow_instance(solana_ctx: &SolanaContext) -> Result<()> {
    println!("\n=== Setting up PrivateChannel escrow instance on test validator ===");

    // Derive instance PDA
    let (instance_pda, instance_bump) = Pubkey::find_program_address(
        &[b"instance", solana_ctx.escrow_instance.pubkey().as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    println!("Instance seed: {}", solana_ctx.escrow_instance.pubkey());
    println!("Instance PDA: {} (bump: {})", instance_pda, instance_bump);

    if let Ok(_pda_account) = solana_ctx.client.get_account(&instance_pda).await {
        println!("Instance PDA account found, skipping creation");
        return Ok(());
    }
    println!("Instance PDA account not found, creating...");

    let create_instance_ix = CreateInstanceBuilder::new()
        .payer(solana_ctx.operator_key.pubkey())
        .admin(solana_ctx.operator_key.pubkey())
        .instance_seed(solana_ctx.escrow_instance.pubkey())
        .instance(instance_pda)
        .bump(instance_bump)
        .instruction();

    let blockhash = solana_ctx.get_latest_blockhash().await?;
    let create_instance_tx = Transaction::new_signed_with_payer(
        &[create_instance_ix],
        Some(&solana_ctx.operator_key.pubkey()),
        &[&solana_ctx.operator_key, &solana_ctx.escrow_instance],
        blockhash,
    );

    let sig = solana_ctx
        .send_transaction(&create_instance_tx)
        .await
        .unwrap();
    println!("Escrow instance created: {}", sig);

    println!(
        "Escrow instance PDA: {} {}",
        instance_pda,
        solana_ctx.get_balance(&instance_pda).await?
    );

    // Add the operator to the escrow instance
    println!("\nAdding operator to escrow instance...");
    let (operator_pda, operator_bump) = Pubkey::find_program_address(
        &[
            b"operator",
            instance_pda.as_ref(),
            solana_ctx.operator_key.pubkey().as_ref(),
        ],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );

    let add_operator_ix = AddOperatorBuilder::new()
        .payer(solana_ctx.operator_key.pubkey())
        .admin(solana_ctx.operator_key.pubkey())
        .instance(instance_pda)
        .operator(solana_ctx.operator_key.pubkey())
        .operator_pda(operator_pda)
        .bump(operator_bump)
        .instruction();

    let blockhash = solana_ctx.get_latest_blockhash().await.unwrap();
    let add_operator_tx = Transaction::new_signed_with_payer(
        &[add_operator_ix],
        Some(&solana_ctx.operator_key.pubkey()),
        &[&solana_ctx.operator_key],
        blockhash,
    );

    let sig = solana_ctx.send_transaction(&add_operator_tx).await.unwrap();
    println!("Operator added to escrow instance: {}", sig);
    println!("Operator PDA: {} (bump: {})", operator_pda, operator_bump);

    Ok(())
}

async fn allow_mint_on_escrow_instance(
    solana_ctx: &SolanaContext,
    mint_keypair: &Keypair,
    token_program_id: &Pubkey,
) -> Result<()> {
    println!("\nAllowing mint in escrow program...");

    // Derive instance PDA
    let (instance_pda, _instance_bump) = Pubkey::find_program_address(
        &[b"instance", solana_ctx.escrow_instance.pubkey().as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );

    // Derive allowed_mint PDA
    let mint_pubkey = Pubkey::from(mint_keypair.pubkey().to_bytes());
    let (allowed_mint_pda, allowed_mint_bump) = Pubkey::find_program_address(
        &[b"allowed_mint", instance_pda.as_ref(), mint_pubkey.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );

    // Derive instance ATA (escrow account for this mint)
    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint_keypair.pubkey(),
        token_program_id,
    );
    let instance_ata_pubkey = Pubkey::from(instance_ata.to_bytes());

    let allow_mint_ix = AllowMintBuilder::new()
        .payer(solana_ctx.operator_key.pubkey())
        .admin(solana_ctx.operator_key.pubkey())
        .instance(instance_pda)
        .mint(mint_pubkey)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata_pubkey)
        .token_program(*token_program_id)
        .bump(allowed_mint_bump)
        .instruction();

    let blockhash = solana_ctx.get_latest_blockhash().await.unwrap();
    let allow_mint_tx = Transaction::new_signed_with_payer(
        &[allow_mint_ix],
        Some(&solana_ctx.operator_key.pubkey()),
        &[&solana_ctx.operator_key],
        blockhash,
    );

    let sig = solana_ctx.send_transaction(&allow_mint_tx).await.unwrap();
    println!("Mint allowed in escrow: {}", sig);
    println!(
        "Allowed mint PDA: {} (bump: {})",
        allowed_mint_pda, allowed_mint_bump
    );
    println!("Instance ATA: {}", instance_ata_pubkey);

    Ok(())
}

/// Setup token accounts and mint tokens to users on Solana test validator
async fn setup_solana_token_accounts(
    solana_ctx: &SolanaContext,
    mint_keypair: &Keypair,
    user_keypairs: &[&Keypair],
    token_amounts: &[u64],
    token_program_id: &Pubkey,
) -> Result<()> {
    println!("\n=== Setting up token accounts on test validator ===");

    assert_eq!(
        user_keypairs.len(),
        token_amounts.len(),
        "Number of user keypairs must match number of token amounts"
    );

    // Create token accounts
    println!("\nCreating token accounts on test validator...");
    solana_ctx
        .create_token_accounts(&mint_keypair.pubkey(), user_keypairs, token_program_id)
        .await
        .unwrap();

    // Mint tokens to each user
    println!("\nMinting tokens on test validator...");
    for (keypair, &amount) in user_keypairs.iter().zip(token_amounts.iter()) {
        let token_account = get_associated_token_address_with_program_id(
            &keypair.pubkey(),
            &mint_keypair.pubkey(),
            token_program_id,
        );
        let sig = solana_ctx
            .mint_to(
                &mint_keypair.pubkey(),
                &token_account,
                amount,
                token_program_id,
            )
            .await
            .unwrap();

        println!(
            "  Minted {} tokens to {}: {}",
            amount,
            keypair.pubkey(),
            sig
        );
    }

    // Verify balances on test validator
    println!("\nVerifying balances on test validator:");
    for (keypair, &expected) in user_keypairs.iter().zip(token_amounts.iter()) {
        let token_account = get_associated_token_address_with_program_id(
            &keypair.pubkey(),
            &mint_keypair.pubkey(),
            token_program_id,
        );
        let balance = solana_ctx.get_token_balance(&token_account).await.unwrap();
        println!("  {}: {} tokens", keypair.pubkey(), balance);
        assert_eq!(
            balance,
            expected,
            "{} should have {} tokens on test validator",
            keypair.pubkey(),
            expected
        );
    }

    Ok(())
}

/// Step 1: Setup Solana test validator with funded accounts, mint, and token accounts
async fn setup_solana_accounts(
    private_channel_ctx: &PrivateChannelContext,
    solana_ctx: &SolanaContext,
    mint_keypair: &Keypair,
    user_keypairs: &[&Keypair],
    token_amounts: &[u64],
    token_program_id: &Pubkey,
) -> Result<()> {
    println!("\n=== Step 1: Setup Solana Environment ===");
    println!("Mint: {}", mint_keypair.pubkey());

    // Request SOL airdrops first
    request_airdrops(private_channel_ctx, solana_ctx, user_keypairs)
        .await
        .unwrap();

    // Create and initialize mint
    println!("\nCreating mint on test validator...");
    let sig = if token_program_id == &spl_token_2022::ID {
        solana_ctx
            .create_t22_mint(
                mint_keypair,
                &private_channel_ctx.operator_key.pubkey(),
                MINT_DECIMALS,
            )
            .await
            .unwrap()
    } else if token_program_id == &spl_token::ID {
        solana_ctx
            .create_mint(
                mint_keypair,
                &private_channel_ctx.operator_key.pubkey(),
                MINT_DECIMALS,
            )
            .await
            .unwrap()
    } else {
        panic!("Unsupported token program ID: {}", token_program_id);
    };
    println!("Mint created: {}", sig);

    setup_solana_escrow_instance(solana_ctx).await?;

    allow_mint_on_escrow_instance(solana_ctx, mint_keypair, token_program_id).await?;

    // Setup token accounts and mint tokens to users
    setup_solana_token_accounts(
        solana_ctx,
        mint_keypair,
        user_keypairs,
        token_amounts,
        token_program_id,
    )
    .await
    .unwrap();

    Ok(())
}

/// Step 2: Make deposits to Solana Escrow
async fn solana_deposit(
    solana_ctx: &SolanaContext,
    mint_keypair: &Keypair,
    alice: &Keypair,
    bob: &Keypair,
    charlie: &Keypair,
    token_program_id: &Pubkey,
) {
    println!("\n=== Step 2: Solana Escrow Deposits ===");

    let solana_mint = &mint_keypair.pubkey();

    // Reuse the existing escrow instance from solana_ctx (created in setup_solana_accounts)
    // This is the SAME instance that the PrivateChannel->Solana operator is configured to use
    let instance_seed_pubkey = Pubkey::from(solana_ctx.escrow_instance.pubkey().to_bytes());

    // Derive instance PDA
    let (instance_pda, _instance_bump) = Pubkey::find_program_address(
        &[b"instance", instance_seed_pubkey.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );

    println!(
        "Using existing Escrow instance seed: {}",
        instance_seed_pubkey
    );
    println!("Using existing Escrow instance PDA: {}", instance_pda);

    // Derive the allowed_mint PDA and instance_ata for this mint
    // These should already exist from the setup_solana_escrow_instance call
    let mint_pubkey = Pubkey::from(solana_mint.to_bytes());
    let (allowed_mint_pda, _) = Pubkey::find_program_address(
        &[b"allowed_mint", instance_pda.as_ref(), mint_pubkey.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    let instance_ata =
        get_associated_token_address_with_program_id(&instance_pda, solana_mint, token_program_id);
    let instance_ata_pubkey = Pubkey::from(instance_ata.to_bytes());

    // Now have Alice, Bob, and Charlie deposit into the escrow on Solana
    println!("\n=== Making Solana Escrow deposits ===");
    let mut deposit_signatures = Vec::new();

    for (name, keypair, amount) in [
        ("Alice", alice, SOLANA_ALICE_DEPOSIT),
        ("Bob", bob, SOLANA_BOB_DEPOSIT),
        ("Charlie", charlie, SOLANA_CHARLIE_DEPOSIT),
    ] {
        let user_pubkey = Pubkey::from(keypair.pubkey().to_bytes());
        let user_solana_ata = get_associated_token_address_with_program_id(
            &keypair.pubkey(),
            solana_mint,
            token_program_id,
        );
        let user_ata_pubkey = Pubkey::from(user_solana_ata.to_bytes());

        let deposit_ix = DepositBuilder::new()
            .payer(user_pubkey)
            .user(user_pubkey)
            .instance(instance_pda)
            .mint(mint_pubkey)
            .allowed_mint(allowed_mint_pda)
            .token_program(*token_program_id)
            .user_ata(user_ata_pubkey)
            .instance_ata(instance_ata_pubkey)
            .amount(amount)
            .instruction();

        let blockhash = solana_ctx.get_latest_blockhash().await.unwrap();
        let deposit_tx = Transaction::new_signed_with_payer(
            &[deposit_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            blockhash,
        );

        let sig = solana_ctx.send_transaction(&deposit_tx).await.unwrap();
        println!(
            "  {} deposited {} tokens to Solana Escrow: {}",
            name, amount, sig
        );
        deposit_signatures.push((name, sig, amount));
    }

    // Verify the Solana indexer captured these deposits
    println!("\n=== Verifying Solana Escrow deposits in Solana indexer database ===");

    let poll_start = std::time::Instant::now();
    let max_poll_duration = Duration::from_secs(20);
    let mut found_deposits = Vec::new();

    while poll_start.elapsed() < max_poll_duration {
        let deposits = solana_ctx
            .indexer_storage
            .get_all_db_transactions(TransactionType::Deposit, 100)
            .await
            .expect("Failed to query deposits from Solana indexer database");

        for (name, sig, amount) in &deposit_signatures {
            if let Some(deposit_tx) = deposits.iter().find(|tx| tx.signature == sig.to_string()) {
                if !found_deposits.iter().any(|s| s == name) {
                    println!(
                        "  ✓ Found {} Solana Escrow deposit: {} tokens (sig: {})",
                        name, amount, sig
                    );
                    found_deposits.push(name.to_string());

                    assert_eq!(deposit_tx.amount, *amount as i64);
                    assert_eq!(deposit_tx.transaction_type, TransactionType::Deposit);
                }
            }
        }

        if found_deposits.len() == deposit_signatures.len() {
            println!(
                "\n  ✓ All {} Solana Escrow deposits verified after {:?}",
                deposit_signatures.len(),
                poll_start.elapsed()
            );
            break;
        }

        println!(
            "  ✗ Found {} Solana Escrow deposits, expected {}. Trying again...",
            found_deposits.len(),
            deposit_signatures.len()
        );
        sleep(Duration::from_millis(500)).await;
    }

    assert_eq!(
        found_deposits.len(),
        deposit_signatures.len(),
        "Should have found all {} Solana Escrow deposits in database",
        deposit_signatures.len()
    );
}

// TODO: Rename function
/// Step 3: Setup PrivateChannel accounts and perform token operations
async fn setup_private_channel_accounts(
    private_channel_ctx: &PrivateChannelContext,
    mint_pubkey: &Pubkey,
    alice: &Keypair,
    bob: &Keypair,
    charlie: &Keypair,
) {
    println!("\n=== Step 3: PrivateChannel Token Operations ===");
    println!("Using mint: {}", mint_pubkey);

    // We only have SPL token on PrivateChannel
    let alice_token_account = get_associated_token_address_with_program_id(
        &alice.pubkey(),
        mint_pubkey,
        &spl_token::id(),
    );
    let bob_token_account =
        get_associated_token_address_with_program_id(&bob.pubkey(), mint_pubkey, &spl_token::id());
    let charlie_token_account = get_associated_token_address_with_program_id(
        &charlie.pubkey(),
        mint_pubkey,
        &spl_token::id(),
    );

    // Wait for the operator to mint tokens based on Solana deposits
    // Note: The operator automatically creates token accounts (ATAs) when minting
    println!("\n=== Waiting for Operator to Mint Tokens from Solana Deposits ===");
    println!("The operator should automatically mint tokens based on Solana deposit events");
    println!(
        "Expected deposits: Alice={}, Bob={}, Charlie={}",
        SOLANA_ALICE_DEPOSIT, SOLANA_BOB_DEPOSIT, SOLANA_CHARLIE_DEPOSIT
    );

    // Poll for balances to be minted by operator
    let poll_start = std::time::Instant::now();
    let max_poll_duration = Duration::from_secs(30);
    let mut alice_balance = 0u64;
    let mut bob_balance = 0u64;
    let mut charlie_balance = 0u64;

    while poll_start.elapsed() < max_poll_duration {
        alice_balance = private_channel_ctx
            .get_token_balance(&alice_token_account)
            .await
            .unwrap_or(0);
        bob_balance = private_channel_ctx
            .get_token_balance(&bob_token_account)
            .await
            .unwrap_or(0);
        charlie_balance = private_channel_ctx
            .get_token_balance(&charlie_token_account)
            .await
            .unwrap_or(0);

        if alice_balance >= SOLANA_ALICE_DEPOSIT
            && bob_balance >= SOLANA_BOB_DEPOSIT
            && charlie_balance >= SOLANA_CHARLIE_DEPOSIT
        {
            println!("✓ Operator minted tokens after {:?}", poll_start.elapsed());
            println!("  Alice: {} tokens", alice_balance / 1000);
            println!("  Bob: {} tokens", bob_balance / 1000);
            println!("  Charlie: {} tokens", charlie_balance / 1000);
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    // Verify balances match Solana deposits
    assert_eq!(
        alice_balance,
        SOLANA_ALICE_DEPOSIT,
        "Alice should have {} tokens minted by operator from Solana deposit",
        SOLANA_ALICE_DEPOSIT / 1000
    );
    assert_eq!(
        bob_balance,
        SOLANA_BOB_DEPOSIT,
        "Bob should have {} tokens minted by operator from Solana deposit",
        SOLANA_BOB_DEPOSIT / 1000
    );
    assert_eq!(
        charlie_balance,
        SOLANA_CHARLIE_DEPOSIT,
        "Charlie should have {} tokens minted by operator from Solana deposit",
        SOLANA_CHARLIE_DEPOSIT / 1000
    );

    // Transfer tokens from Alice to Charlie (Alice has 200, send 100 to Charlie)
    let transfer_amount = 100_000; // 100 tokens

    // Test simulation with both Legacy and V0 transaction types
    println!("\n=== Testing Simulation with Legacy Transaction ===");
    test_simulate_transaction(
        private_channel_ctx,
        mint_pubkey,
        alice,
        charlie,
        transfer_amount,
        setup::TransactionType::Legacy,
    )
    .await
    .unwrap();

    println!("\n=== Testing Simulation with V0 Transaction ===");
    test_simulate_transaction(
        private_channel_ctx,
        mint_pubkey,
        alice,
        charlie,
        transfer_amount,
        setup::TransactionType::V0,
    )
    .await
    .unwrap();

    println!(
        "\n=== Transferring {} tokens from Alice to Charlie ===",
        transfer_amount / 1000
    );
    let blockhash = private_channel_ctx.get_blockhash().await.unwrap();
    let transfer_tx = setup::transfer_tokens_transaction(
        alice,
        &charlie.pubkey(),
        mint_pubkey,
        transfer_amount,
        blockhash,
    );

    // Send the transaction
    let sig = private_channel_ctx
        .send_transaction(&transfer_tx)
        .await
        .unwrap();
    private_channel_ctx.check_transaction_exists(sig).await;
    println!("Transfer transaction sent: {}", sig);

    // Give time for the transfer to be processed
    sleep(Duration::from_millis(500)).await;

    // Get final slot
    let slot_after_transfers = private_channel_ctx.get_slot().await.unwrap();
    println!("\n=== Final State ===");
    println!("Slot after transfers: {}", slot_after_transfers);

    // Verify final balances
    println!("\n=== Verifying Final Balances ===");
    let alice_final = SOLANA_ALICE_DEPOSIT - transfer_amount; // 200 - 100 = 100
    let bob_final = SOLANA_BOB_DEPOSIT; // 150 (unchanged)
    let charlie_final = SOLANA_CHARLIE_DEPOSIT + transfer_amount; // 100 + 100 = 200

    println!("Alice token account {}", alice_token_account);
    println!("Bob token account {}", bob_token_account);
    println!("Charlie token account {}", charlie_token_account);

    assert_eq!(
        private_channel_ctx
            .get_token_balance(&alice_token_account)
            .await
            .unwrap(),
        alice_final,
        "Alice should have {} tokens",
        alice_final / 1000
    );
    assert_eq!(
        private_channel_ctx
            .get_token_balance(&bob_token_account)
            .await
            .unwrap(),
        bob_final,
        "Bob should have {} tokens",
        bob_final / 1000
    );
    assert_eq!(
        private_channel_ctx
            .get_token_balance(&charlie_token_account)
            .await
            .unwrap(),
        charlie_final,
        "Charlie should have {} tokens",
        charlie_final / 1000
    );

    println!("\n✓ PrivateChannel token operations completed successfully!");
}

/// Step 4: Withdraw from PrivateChannel and verify in indexer
async fn private_channel_burn(
    private_channel_ctx: &PrivateChannelContext,
    solana_ctx: &SolanaContext,
    mint_pubkey: &Pubkey,
    alice: &Keypair,
    token_program_id: &Pubkey,
) {
    println!("\n=== Step 4: PrivateChannel Withdrawals ===");

    // We only have SPL token on PrivateChannel
    let alice_token_account = get_associated_token_address_with_program_id(
        &alice.pubkey(),
        mint_pubkey,
        &spl_token::id(),
    );

    // Calculate expected balance before withdrawal
    // Alice received 200 from Solana deposit, sent 100 to Charlie, so has 100
    let transfer_amount = 100_000; // Same as used in setup_private_channel_accounts
    let alice_balance_before = SOLANA_ALICE_DEPOSIT - transfer_amount;

    // Alice withdraws 50 tokens (half of her remaining balance)
    let withdrawal_amount = 50_000; // 50 tokens
    println!(
        "\n=== Withdrawing {} tokens from Alice ===",
        withdrawal_amount / 1000
    );
    let blockhash = private_channel_ctx.get_blockhash().await.unwrap();
    let withdraw_tx =
        setup::withdraw_funds_transaction(alice, mint_pubkey, withdrawal_amount, blockhash);
    let sig = private_channel_ctx
        .send_transaction(&withdraw_tx)
        .await
        .unwrap();
    private_channel_ctx.check_transaction_exists(sig).await;
    println!(
        "Withdrew {} tokens from Alice: {}",
        withdrawal_amount / 1000,
        sig
    );

    // Verify balance after withdrawal
    let alice_after_withdrawal = alice_balance_before - withdrawal_amount;
    assert_eq!(
        private_channel_ctx
            .get_token_balance(&alice_token_account)
            .await
            .unwrap(),
        alice_after_withdrawal,
        "Alice should have {} tokens after withdrawal",
        alice_after_withdrawal / 1000
    );

    // Verify the withdrawal was recorded in the PrivateChannel indexer database
    println!("\n=== Verifying Withdrawal in PrivateChannel Indexer Database ===");

    // Poll for the withdrawal with timeout (max 10 seconds)
    let poll_start = std::time::Instant::now();
    let max_poll_duration = Duration::from_secs(10);
    let mut our_withdrawal = None;

    while poll_start.elapsed() < max_poll_duration {
        // Query for withdrawal transactions
        let withdrawals = private_channel_ctx
            .indexer_storage
            .get_all_db_transactions(TransactionType::Withdrawal, 100)
            .await
            .expect("Failed to query withdrawals from database");

        // Find our withdrawal by signature
        our_withdrawal = withdrawals
            .iter()
            .find(|tx| tx.signature == sig.to_string())
            .cloned();

        if our_withdrawal.is_some() {
            println!(
                "Found withdrawal in database after {:?}",
                poll_start.elapsed()
            );
            break;
        }

        // Wait a bit before polling again
        sleep(Duration::from_millis(200)).await;
    }

    assert!(
        our_withdrawal.is_some(),
        "Withdrawal transaction {} should be recorded in indexer database after {:?}",
        sig,
        poll_start.elapsed()
    );

    let withdrawal_tx = our_withdrawal.unwrap();
    println!("Withdrawal found in database:");
    println!("  ID: {}", withdrawal_tx.id);
    println!("  Signature: {}", withdrawal_tx.signature);
    println!("  Initiator: {}", withdrawal_tx.initiator);
    println!("  Amount: {}", withdrawal_tx.amount);
    println!("  Status: {:?}", withdrawal_tx.status);

    let withdrawal_amount = 50_000; // Same as used above
    assert_eq!(withdrawal_tx.initiator, alice.pubkey().to_string());
    assert_eq!(withdrawal_tx.amount, withdrawal_amount as i64);
    assert_eq!(withdrawal_tx.transaction_type, TransactionType::Withdrawal);

    println!("\n✓ Withdrawal successfully recorded in PrivateChannel indexer database");

    // Poll for Alice's balance to be 850_000 (1_000_000 start, 200_000 escrowed on Solana, 50_000 withdrawn)
    println!("\n=== Polling for Alice's updated Solana balance after withdrawal ===");
    let expected_alice_balance = INITIAL_ALICE_TOKENS - SOLANA_ALICE_DEPOSIT + withdrawal_amount;
    let poll_start = std::time::Instant::now();
    let max_poll_duration = Duration::from_secs(10);
    let alice_solana_ata = get_associated_token_address_with_program_id(
        &alice.pubkey(),
        mint_pubkey,
        token_program_id,
    );

    loop {
        let balance = solana_ctx
            .get_token_balance(&alice_solana_ata)
            .await
            .unwrap_or(0);
        if balance == expected_alice_balance {
            println!(
                "✓ Alice's Solana balance updated to {} after withdrawal (after {:?})",
                balance,
                poll_start.elapsed()
            );
            break;
        }
        if poll_start.elapsed() >= max_poll_duration {
            panic!(
                "Timeout waiting for Alice's Solana balance to reach {} (got {})",
                expected_alice_balance, balance
            );
        }
        sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(
        solana_ctx
            .get_token_balance(&alice_solana_ata)
            .await
            .unwrap(),
        expected_alice_balance,
        "Alice's Solana token balance should update to {} after withdrawal",
        expected_alice_balance
    );
}

/// Main test orchestration function
/// Runs all 4 steps in order with a single mint for the entire test
pub async fn run_spl_token_test(
    private_channel_ctx: &PrivateChannelContext,
    solana_ctx: &SolanaContext,
    token_program_id: Pubkey,
) {
    if token_program_id == spl_token::ID {
        println!("\n=== SPL Token Integration Test ===");
    } else if token_program_id == spl_token_2022::ID {
        println!("\n=== SPL Token2022 Integration Test ===");
    } else {
        panic!("Unsupported token program ID: {}", token_program_id);
    }

    // Generate user keypairs
    let alice = Keypair::new();
    let bob = Keypair::new();
    let charlie = Keypair::new();

    // Generate a SINGLE mint keypair for the entire test
    // This mint will be created on Solana and its pubkey will be used on PrivateChannel
    let mint_keypair = Keypair::new();

    println!("\n=== Test Participants ===");
    println!("  Mint: {}", mint_keypair.pubkey());
    println!("  Alice: {}", alice.pubkey());
    println!("  Bob: {}", bob.pubkey());
    println!("  Charlie: {}", charlie.pubkey());

    // Step 1: Setup accounts in Solana
    setup_solana_accounts(
        private_channel_ctx,
        solana_ctx,
        &mint_keypair,
        &[&alice, &bob, &charlie],
        &[
            INITIAL_ALICE_TOKENS,
            INITIAL_BOB_TOKENS,
            INITIAL_CHARLIE_TOKENS,
        ],
        &token_program_id,
    )
    .await
    .expect("Solana environment setup failed");

    // Step 2: Deposit in Solana
    solana_deposit(
        solana_ctx,
        &mint_keypair,
        &alice,
        &bob,
        &charlie,
        &token_program_id,
    )
    .await;

    // Step 3: Setup accounts in PrivateChannel and perform token operations
    setup_private_channel_accounts(
        private_channel_ctx,
        &mint_keypair.pubkey(),
        &alice,
        &bob,
        &charlie,
    )
    .await;

    // Step 4: Withdraw from PrivateChannel
    private_channel_burn(
        private_channel_ctx,
        solana_ctx,
        &mint_keypair.pubkey(),
        &alice,
        &token_program_id,
    )
    .await;

    if token_program_id == spl_token::ID {
        println!("\n✓ SPL Token Integration Test Passed!");
    } else if token_program_id == spl_token_2022::ID {
        println!("\n✓ SPL Token2022 Integration Test Passed!");
    } else {
        panic!("Unsupported token program ID: {}", token_program_id);
    }
}

async fn test_simulate_transaction(
    private_channel_ctx: &PrivateChannelContext,
    mint_pubkey: &Pubkey,
    from: &Keypair,
    to: &Keypair,
    amount: u64,
    tx_type: setup::TransactionType,
) -> Result<()> {
    let from_ata =
        get_associated_token_address_with_program_id(&from.pubkey(), mint_pubkey, &spl_token::id());
    let to_ata =
        get_associated_token_address_with_program_id(&to.pubkey(), mint_pubkey, &spl_token::id());
    let from_balance_before = private_channel_ctx
        .get_token_balance(&from_ata)
        .await
        .unwrap();
    let to_balance_before = private_channel_ctx
        .get_token_balance(&to_ata)
        .await
        .unwrap();
    let blockhash = private_channel_ctx.get_blockhash().await.unwrap();
    let transfer_tx = setup::transfer_tokens_versioned_transaction(
        from,
        &to.pubkey(),
        mint_pubkey,
        amount,
        blockhash,
        tx_type,
    );

    println!("Testing simulation with {:?} transaction type", tx_type);

    // Check the simulation
    let sim = private_channel_ctx
        .read_client
        .simulate_transaction_with_config(
            &transfer_tx,
            RpcSimulateTransactionConfig {
                accounts: Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![from_ata.to_string(), to_ata.to_string()],
                }),
                ..RpcSimulateTransactionConfig::default()
            },
        )
        .await
        .unwrap();

    println!(
        "Simulation result: err={:?}, logs={:?}",
        sim.value.err, sim.value.logs
    );
    let accounts = sim.value.accounts.as_ref().unwrap();
    println!("Number of accounts returned: {}", accounts.len());
    for (i, acc) in accounts.iter().enumerate() {
        if let Some(acc) = acc {
            println!(
                "Account {}: lamports={}, owner={}",
                i, acc.lamports, acc.owner
            );
        } else {
            println!("Account {}: None", i);
        }
    }

    let decode_token_account =
        |account: &solana_account_decoder_client_types::UiAccount, label: &str| -> TokenAccount {
            let data_str = match &account.data {
                UiAccountData::Binary(data, _) | UiAccountData::LegacyBinary(data) => data,
                UiAccountData::Json(parsed) => {
                    panic!("Unexpected JSON account data for {}: {:?}", label, parsed);
                }
            };
            let bytes = STANDARD.decode(data_str).unwrap_or_else(|e| {
                panic!("Failed to decode {} account data: {}", label, e);
            });
            TokenAccount::unpack(&bytes).unwrap_or_else(|e| {
                panic!("Failed to unpack {} token account data: {}", label, e);
            })
        };

    let from_account = accounts
        .first()
        .and_then(|opt| opt.as_ref())
        .expect("Sender's token account not found in simulation response");
    let from_token_data = decode_token_account(from_account, "sender");
    let expected_from_balance = from_balance_before - amount;
    assert_eq!(
        from_token_data.amount, expected_from_balance,
        "Sender's simulated balance should be {} after transfer",
        expected_from_balance
    );
    println!(
        "✓ Simulation shows sender's balance will be: {}",
        from_token_data.amount
    );

    let to_account = accounts
        .get(1)
        .and_then(|opt| opt.as_ref())
        .expect("Recipient's token account not found in simulation response");
    let to_token_data = decode_token_account(to_account, "recipient");
    let expected_to_balance = to_balance_before + amount;
    assert_eq!(
        to_token_data.amount, expected_to_balance,
        "Recipient's simulated balance should be {} after transfer",
        expected_to_balance
    );
    println!(
        "✓ Simulation shows recipient's balance will be: {}",
        to_token_data.amount
    );

    Ok(())
}
