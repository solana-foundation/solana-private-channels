// Refactored integration test using helper modules

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

// Parser malformation: pure-function tests of `parse_escrow_instruction`
// against deliberately-malformed payloads.
#[path = "parser_malformation.rs"]
mod parser_malformation;

use helpers::{
    calculate_user_total_deposited, db, execute_user_deposits, execute_user_withdrawal,
    get_token_balance, operator_util, test_types::*, verify_database,
};
use private_channel_indexer::operator::bitmap_constants::NONCES_PER_GENERATION;
use private_channel_indexer::operator::find_withdrawal_bitmap_pda;
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::PostgresConfig;
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment, TEST_ADMIN_KEYPAIR};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::SeedDerivable;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signer};
use std::sync::{Arc, Once};
use test_utils::indexer_helper::{start_private_channel_indexer, start_solana_indexer};
use test_utils::operator_helper::{
    start_private_channel_to_solana_operator, start_solana_to_private_channel_operator,
};
use test_utils::validator_helper::start_test_validator;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

static INIT: Once = Once::new();

// ANSI color codes for terminal output
const RESET: &str = "\x1b[0m";
const CYAN: &str = "\x1b[36m";
const ORANGE: &str = "\x1b[38;5;208m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const MAGENTA: &str = "\x1b[35m";
const RED: &str = "\x1b[31m";
const BOLD: &str = "\x1b[1m";

fn init_tracing() {
    use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"))
            .add_directive("solana=off".parse().unwrap())
            .add_directive("agave=off".parse().unwrap());

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .init();
    });
}

// ============================================================================
// Phase Orchestration Functions
// ============================================================================
async fn setup_test_environments(
    client: &Arc<RpcClient>,
    faucet_keypair: &Keypair,
    pool: &sqlx::PgPool,
) -> Result<
    (
        TestEnvironment,
        TestEnvironment,
        i64,
        u64,
        Vec<UserTransaction>,
    ),
    Box<dyn std::error::Error>,
> {
    println!("## Setup Phase");
    println!("Creating {} users and funding accounts...", NUM_USERS);
    let instance_seeds = Keypair::from_seed(&ESCROW_INSTANCE_SEEDS_PRIVATE_KEY).unwrap();
    let env = TestEnvironment::setup_multi_user(
        client.as_ref(),
        faucet_keypair,
        NUM_USERS,
        Some(instance_seeds),
    )
    .await?;

    println!("✓ Environment setup complete");
    println!("  Instance: {}", env.instance);
    println!("  Mint: {}", env.mint);

    // Create second instance (filtered out)
    println!("\nCreating second instance (should be filtered out)...");
    let env_filtered =
        TestEnvironment::setup_multi_user(client.as_ref(), faucet_keypair, 2, None).await?;
    println!("✓ Second instance created");
    println!("  Instance: {}", env_filtered.instance);
    println!("  Mint: {}", env_filtered.mint);
    println!();

    // Get initial balance BEFORE pre-indexer deposits for later verification
    let initial_balance_per_user =
        get_token_balance(client.as_ref(), &env.users[0].pubkey(), &env.mint).await?;
    println!("Initial balance per user: {}\n", initial_balance_per_user);

    // Create 3 pre-indexer deposits for backfill testing
    println!("Creating 3 pre-indexer deposits for backfill testing...");
    let (allowed_mint_pda, _) = find_allowed_mint_pda(&env.instance, &env.mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let mut pre_indexer_transactions = Vec::new();

    for user_id in 0..3 {
        let user = &env.users[user_id];
        let amount = BASE_AMOUNT + (user_id as u64 * 1000);

        let user_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &user.pubkey(),
            &env.mint,
            &spl_token::ID,
        );
        let instance_ata =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &env.instance,
                &env.mint,
                &spl_token::ID,
            );

        let deposit_ix = private_channel_escrow_program_client::instructions::DepositBuilder::new()
            .payer(user.pubkey())
            .user(user.pubkey())
            .instance(env.instance)
            .mint(env.mint)
            .allowed_mint(allowed_mint_pda)
            .user_ata(user_ata)
            .instance_ata(instance_ata)
            .system_program(solana_system_interface::program::ID)
            .token_program(spl_token::ID)
            .associated_token_program(spl_associated_token_account::ID)
            .event_authority(event_authority_pda)
            .private_channel_escrow_program(
                private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            )
            .amount(amount)
            .instruction();

        let signature = helpers::send_and_confirm_instructions(
            client.as_ref(),
            &[deposit_ix],
            user,
            &[user],
            "Pre-indexer deposit",
        )
        .await?;

        let statuses = client.get_signature_statuses(&[signature]).await?;
        let slot = statuses
            .value
            .first()
            .and_then(|s| s.as_ref())
            .map(|s| s.slot)
            .ok_or("Failed to get slot from signature status")?;

        pre_indexer_transactions.push(UserTransaction {
            user_pubkey: user.pubkey(),
            amount,
            signature: signature.to_string(),
            slot,
            tx_type: helpers::test_types::TransactionType::Deposit,
        });

        println!(
            "  ✓ Pre-indexer deposit #{} for user #{} confirmed: {}",
            user_id + 1,
            user_id,
            signature
        );
    }

    println!(
        "✓ Created {} pre-indexer deposits\n",
        pre_indexer_transactions.len()
    );

    // Get initial count
    let count_before = db::count_transactions(pool).await?;
    println!("Initial transaction count: {}", count_before);

    Ok((
        env,
        env_filtered,
        count_before,
        initial_balance_per_user,
        pre_indexer_transactions,
    ))
}

async fn execute_deposit_phase(
    client: &Arc<RpcClient>,
    env: &TestEnvironment,
    env_filtered: &TestEnvironment,
) -> Result<(Vec<UserTransaction>, Vec<UserTransaction>), Box<dyn std::error::Error>> {
    println!(
        "## Execution Phase - Spawning {} concurrent deposit tasks",
        NUM_USERS
    );

    let (allowed_mint_pda, _) = find_allowed_mint_pda(&env.instance, &env.mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    // Execute deposits for main instance (concurrently)
    let mut tasks = Vec::new();
    for (user_id, user) in env.users.iter().enumerate() {
        let client = client.clone();
        let user = user.insecure_clone();
        let instance = env.instance;
        let mint = env.mint;
        let allowed_mint = allowed_mint_pda;
        let event_authority = event_authority_pda;

        let task = tokio::spawn(async move {
            execute_user_deposits(
                client.as_ref(),
                user_id,
                user,
                instance,
                mint,
                allowed_mint,
                event_authority,
            )
            .await
        });

        tasks.push(task);
    }

    // Execute deposits for filtered instance
    let (filtered_allowed_mint_pda, _) =
        find_allowed_mint_pda(&env_filtered.instance, &env_filtered.mint);
    let mut filtered_tasks = Vec::new();
    for (user_id, user) in env_filtered.users.iter().enumerate() {
        let client = client.clone();
        let user = user.insecure_clone();
        let instance = env_filtered.instance;
        let mint = env_filtered.mint;
        let allowed_mint = filtered_allowed_mint_pda;
        let event_authority = event_authority_pda;

        let task = tokio::spawn(async move {
            execute_user_deposits(
                client.as_ref(),
                user_id,
                user,
                instance,
                mint,
                allowed_mint,
                event_authority,
            )
            .await
        });

        filtered_tasks.push(task);
    }

    println!(
        "✓ All {} tasks spawned (+ {} filtered), waiting for completion...",
        NUM_USERS,
        env_filtered.users.len()
    );

    // Collect results
    let mut all_transactions = Vec::new();
    for (i, task) in tasks.into_iter().enumerate() {
        let txs = task.await.map_err(|e| format!("Task panicked: {}", e))??;
        all_transactions.extend(txs);
        if (i + 1) % 5 == 0 {
            println!("  {} / {} users completed", i + 1, NUM_USERS);
        }
    }

    let mut filtered_transactions = Vec::new();
    for task in filtered_tasks {
        let txs = task
            .await
            .map_err(|e| format!("Filtered task panicked: {}", e))??;
        filtered_transactions.extend(txs);
    }

    println!("✓ All {} users completed", NUM_USERS);
    println!(
        "✓ Sent {} main instance transactions on-chain",
        all_transactions.len()
    );
    println!(
        "✓ Sent {} filtered instance transactions (should be ignored)\n",
        filtered_transactions.len()
    );

    Ok((all_transactions, filtered_transactions))
}

async fn verify_backfill_phase(
    pool: &sqlx::PgPool,
    pre_indexer_transactions: &[UserTransaction],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("## Backfill Verification Phase");
    println!(
        "Waiting for indexer to backfill {} pre-indexer deposits...",
        pre_indexer_transactions.len()
    );

    let expected_count = pre_indexer_transactions.len() as i64;
    let ready = db::wait_for_count(pool, expected_count, *WAIT_TIMEOUT_SECS).await?;

    assert!(
        ready,
        "Indexer did not backfill pre-indexer transactions within timeout"
    );
    println!(
        "✓ Indexer backfilled {} transactions",
        pre_indexer_transactions.len()
    );

    println!("Waiting for operator to process backfilled deposits...");
    operator_util::wait_for_operator_completion(
        pool,
        expected_count as usize,
        "backfilled deposits",
    )
    .await?;

    println!("Verifying backfilled transactions in database...");
    for (idx, expected_tx) in pre_indexer_transactions.iter().enumerate() {
        let db_tx = db::get_transaction(pool, &expected_tx.signature)
            .await?
            .ok_or_else(|| {
                format!(
                    "Pre-indexer transaction {} not found in database: {}",
                    idx + 1,
                    expected_tx.signature
                )
            })?;

        assert_eq!(
            db_tx.signature,
            expected_tx.signature,
            "Signature mismatch for transaction {}",
            idx + 1
        );
        assert_eq!(
            db_tx.slot as u64,
            expected_tx.slot,
            "Slot mismatch for transaction {}",
            idx + 1
        );
        assert_eq!(
            db_tx.amount.value(),
            expected_tx.amount,
            "Amount mismatch for transaction {}",
            idx + 1
        );
        assert_eq!(
            db_tx.transaction_type,
            "deposit",
            "Transaction type mismatch for transaction {}",
            idx + 1
        );
        assert_eq!(
            db_tx.status,
            "completed",
            "Status should be completed for transaction {}",
            idx + 1
        );
        assert!(
            db_tx.counterpart_signature.is_some(),
            "Transaction {} missing counterpart_signature",
            expected_tx.signature
        );

        println!(
            "  ✓ Pre-indexer transaction {} verified and completed: {}",
            idx + 1,
            expected_tx.signature
        );
    }

    println!("✓ All pre-indexer transactions successfully backfilled, processed, and verified\n");

    Ok(())
}

async fn verify_deposit_indexing(
    client: &Arc<RpcClient>,
    pool: &sqlx::PgPool,
    count_before: i64,
    pre_indexer_transactions: &[UserTransaction],
    all_transactions: &[UserTransaction],
    filtered_transactions: &[UserTransaction],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("## Verification Phase");

    let expected_total = NUM_USERS * DEPOSITS_PER_USER;
    println!("Expected: {} deposits", expected_total);
    println!();

    let current_slot = client.get_slot().await?;
    println!(
        "Current validator slot: {}, waiting for indexers to catch up...",
        current_slot
    );

    let expected_count = count_before + expected_total as i64;
    let ready = db::wait_for_count(pool, expected_count, *WAIT_TIMEOUT_SECS).await?;

    assert!(
        ready,
        "Indexer did not process all transactions within timeout"
    );
    println!("✓ Indexer has correct transaction count");

    // Wait for checkpoint
    println!("Waiting for checkpoints to reach slot {}...", current_slot);
    let checkpoint_ready =
        db::wait_for_checkpoint(pool, "escrow", current_slot, *WAIT_TIMEOUT_SECS).await?;

    assert!(checkpoint_ready, "Checkpoint did not catch up");
    println!("✓ Indexer caught up to slot {}\n", current_slot);

    // Verify database (including pre-indexer transactions)
    println!("Verifying database...");
    let mut all_deposits = pre_indexer_transactions.to_vec();
    all_deposits.extend_from_slice(all_transactions);
    verify_database(pool, &all_deposits, filtered_transactions, "Indexer").await?;

    Ok(())
}

async fn verify_operator_processing(
    pool: &sqlx::PgPool,
    pre_indexer_transactions: &[UserTransaction],
    all_transactions: &[UserTransaction],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n## Operator Verification Phase");
    println!("Waiting for operator-solana to process deposits...");

    let mut all_deposits = pre_indexer_transactions.to_vec();
    all_deposits.extend_from_slice(all_transactions);

    let expected_total = all_deposits.len();
    operator_util::wait_for_operator_completion(pool, expected_total, "deposits").await?;

    // Verify all transactions marked as completed
    println!("\nVerifying operator updated database...");
    let processed_txs = db::get_processed_transactions(pool).await?;

    assert_eq!(
        processed_txs.len(),
        expected_total,
        "Processed transaction count mismatch"
    );

    for expected_tx in &all_deposits {
        let db_tx = db::get_transaction(pool, &expected_tx.signature)
            .await?
            .expect("Transaction not found");

        assert_eq!(
            db_tx.status, "completed",
            "Transaction {} not marked as completed",
            expected_tx.signature
        );
        assert!(
            db_tx.counterpart_signature.is_some(),
            "Transaction {} missing counterpart_signature",
            expected_tx.signature
        );
    }

    println!("✓ All {} deposits processed by operator", expected_total);

    Ok(())
}

async fn execute_withdrawal_phase(
    client: &Arc<RpcClient>,
    env: &TestEnvironment,
) -> Result<Vec<UserTransaction>, Box<dyn std::error::Error>> {
    println!("\n## Withdrawal Phase");
    println!("Users will now withdraw funds using Withdraw program...");

    let mut withdrawal_tasks = Vec::new();
    for (user_id, user) in env.users.iter().enumerate() {
        let client = client.clone();
        let user = user.insecure_clone();
        let mint = env.mint;
        // Withdraw only half the balance, leaving funds for boundary test
        let partial_amount = calculate_user_total_deposited(user_id) / 2;

        let task = tokio::spawn(async move {
            execute_user_withdrawal(client.as_ref(), &user, mint, partial_amount).await
        });

        withdrawal_tasks.push(task);
    }

    // Collect withdrawal transactions
    let mut withdrawal_transactions = Vec::new();
    for (i, task) in withdrawal_tasks.into_iter().enumerate() {
        let tx = task
            .await
            .map_err(|e| format!("Withdrawal task {} panicked: {}", i, e))??;
        withdrawal_transactions.push(tx);
    }

    println!(
        "✓ All {} users submitted withdrawal transactions",
        NUM_USERS
    );
    println!(
        "✓ Tracked {} withdrawal transactions\n",
        withdrawal_transactions.len()
    );

    Ok(withdrawal_transactions)
}

async fn verify_withdrawal_processing(
    pool: &sqlx::PgPool,
    count_before: i64,
    deposit_transactions: &[UserTransaction],
    withdrawal_transactions: &[UserTransaction],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nWaiting for Withdraw indexer to process withdrawals...");

    let expected_withdrawals = withdrawal_transactions.len();
    let total_expected_txs =
        count_before + deposit_transactions.len() as i64 + expected_withdrawals as i64;

    let withdraw_ready = db::wait_for_count(pool, total_expected_txs, *WAIT_TIMEOUT_SECS).await?;
    assert!(
        withdraw_ready,
        "Withdraw indexer did not process all withdrawals within timeout"
    );
    println!(
        "✓ Withdraw indexer processed all {} withdrawals",
        expected_withdrawals
    );

    println!("\nWaiting for operator-private_channel to process withdrawals...");
    let total_expected_completed = deposit_transactions.len() + expected_withdrawals;
    operator_util::wait_for_operator_completion(pool, total_expected_completed, "withdrawals")
        .await?;

    println!("✓ All withdrawals processed and completed");

    // Wait for ReleaseFunds to finalize
    println!("\nWaiting for ReleaseFunds transactions to be finalized on-chain...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}

async fn verify_final_balances(
    client: &Arc<RpcClient>,
    env: &TestEnvironment,
    initial_balance: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nVerifying final balances after complete round-trip...");

    for (user_id, user) in env.users.iter().enumerate() {
        let actual_balance = get_token_balance(client.as_ref(), &user.pubkey(), &env.mint).await?;

        assert_eq!(
            actual_balance, initial_balance,
            "User {} final balance mismatch after round-trip. Expected: {}, Actual: {}",
            user_id, initial_balance, actual_balance
        );
    }

    println!(
        "✓ All {} users returned to original balance after full round-trip",
        NUM_USERS
    );

    Ok(())
}

// ============================================================================
// Test Functions
// ============================================================================

#[allow(dead_code, unreachable_code, unused_variables)]
async fn execute_tree_rotation_boundary_phase(
    client: &Arc<RpcClient>,
    pool: &sqlx::PgPool,
    env: &TestEnvironment,
) -> Result<(), Box<dyn std::error::Error>> {
    // Phase 10 drives a full generation of withdrawals through the operator to
    // trigger a rotation. With the production window (65,536) this would need tens
    // of thousands of on-chain transactions and would never finish on a test
    // validator. Always compile with --features test-tree (window of 8).
    #[cfg(not(feature = "test-tree"))]
    panic!(
        "test_master_chaos_stress_test requires --features test-tree. \
         Without it NONCES_PER_GENERATION={} and Phase 10 would need ~65k on-chain transactions.",
        NONCES_PER_GENERATION
    );

    println!("\n## Bitmap Rotation Boundary Phase");
    println!(
        "Testing bitmap rotation at boundary (NONCES_PER_GENERATION = {})...",
        NONCES_PER_GENERATION
    );

    let opening = read_withdrawal_bitmap(client, &env.instance).await?;
    assert_eq!(
        opening.generation, 0,
        "the rotation phase must start on the first generation"
    );

    // Only withdrawals consume nonce space, one nonce per row.
    let next_nonce = db::next_withdrawal_nonce(pool).await?;
    println!(
        "Next withdrawal nonce: {} (boundary at {})",
        next_nonce, NONCES_PER_GENERATION
    );
    assert!(
        next_nonce < NONCES_PER_GENERATION,
        "earlier phases already crossed the boundary at nonce {}, so this phase cannot observe the rotation",
        NONCES_PER_GENERATION
    );

    // Fill the rest of the window one withdrawal at a time. Serialising them
    // keeps the nonce each release consumes predictable, which is what makes the
    // bitmap assertions below exact rather than a count.
    let fill_count = NONCES_PER_GENERATION - next_nonce;
    println!(
        "Releasing {} withdrawals to consume the rest of generation 0...",
        fill_count
    );
    for i in 0..fill_count {
        let user_idx = (i % env.users.len() as u64) as usize;
        // Small next to the user's balance, so no single withdrawal can drain it.
        let small_amount = calculate_user_total_deposited(user_idx) / 20;
        let withdrawal_tx = execute_user_withdrawal(
            client.as_ref(),
            &env.users[user_idx],
            env.mint,
            small_amount,
        )
        .await?;
        operator_util::wait_for_transaction_completion(pool, &withdrawal_tx.signature).await?;
        println!("  ✓ nonce {} released", next_nonce + i);
    }

    let filled = read_withdrawal_bitmap(client, &env.instance).await?;
    assert_eq!(
        filled.generation, 0,
        "the window must not rotate before a boundary nonce is released"
    );
    assert_eq!(
        filled.consumed,
        (0..NONCES_PER_GENERATION).collect::<Vec<u64>>(),
        "every nonce of generation 0 must be consumed before the boundary"
    );
    println!("✓ Generation 0 fully consumed, still on generation 0");

    // This nonce opens the next window, so the operator has to rotate before it
    // can release it.
    println!(
        "\nReleasing the boundary nonce {} (should trigger rotation)...",
        NONCES_PER_GENERATION
    );
    let boundary_tx = execute_user_withdrawal(
        client.as_ref(),
        &env.users[0],
        env.mint,
        calculate_user_total_deposited(0) / 20,
    )
    .await?;
    operator_util::wait_for_transaction_completion(pool, &boundary_tx.signature).await?;

    // Verify the on-chain generation advanced and the bits were cleared.
    println!("\nVerifying bitmap rotation occurred...");
    let bitmap = read_withdrawal_bitmap(client, &env.instance).await?;

    assert_eq!(
        bitmap.generation, 1,
        "the boundary release must rotate the bitmap to generation 1"
    );
    // The new window reuses the same bit positions, so a bit the rotation failed
    // to clear would resurface here as an already-consumed nonce. Only the
    // boundary nonce itself may be set.
    assert_eq!(
        bitmap.consumed,
        vec![NONCES_PER_GENERATION],
        "rotation must clear every bit of the previous generation"
    );

    println!("✓ Bitmap rotation successful! generation = 1");
    println!(
        "✓ Operator correctly handled the generation boundary at {}",
        NONCES_PER_GENERATION
    );

    Ok(())
}

/// Read and decode this instance's withdrawal bitmap.
async fn read_withdrawal_bitmap(
    client: &Arc<RpcClient>,
    instance: &solana_sdk::pubkey::Pubkey,
) -> Result<private_channel_indexer::operator::BitmapState, Box<dyn std::error::Error>> {
    let bitmap_pda = find_withdrawal_bitmap_pda(instance);
    let data = client.get_account_data(&bitmap_pda).await?;
    Ok(private_channel_indexer::operator::parse_withdrawal_bitmap(
        &data,
    )?)
}

#[allow(dead_code)]
async fn execute_post_rotation_verification_phase(
    client: &Arc<RpcClient>,
    pool: &sqlx::PgPool,
    env: &TestEnvironment,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n## Post-Rotation Verification Phase");
    println!("Creating 3 additional withdrawals to verify generation 1 works correctly...");

    let before = read_withdrawal_bitmap(client, &env.instance).await?;
    assert_eq!(
        before.generation, 1,
        "the post-rotation phase must start in generation 1"
    );
    let first_new_nonce = db::next_withdrawal_nonce(pool).await?;

    let small_amount = calculate_user_total_deposited(0) / 20;

    let post_rotation_tx_1 =
        execute_user_withdrawal(client.as_ref(), &env.users[0], env.mint, small_amount).await?;
    println!(
        "  Post-rotation withdrawal 1: {}",
        post_rotation_tx_1.signature
    );

    let post_rotation_tx_2 =
        execute_user_withdrawal(client.as_ref(), &env.users[1], env.mint, small_amount).await?;
    println!(
        "  Post-rotation withdrawal 2: {}",
        post_rotation_tx_2.signature
    );

    let post_rotation_tx_3 =
        execute_user_withdrawal(client.as_ref(), &env.users[2], env.mint, small_amount).await?;
    println!(
        "  Post-rotation withdrawal 3: {}",
        post_rotation_tx_3.signature
    );

    println!("\n✓ Created 3 post-rotation withdrawals");

    // Wait for all post-rotation withdrawals to complete
    println!("Waiting for post-rotation withdrawals to complete...");
    operator_util::wait_for_transaction_completion(pool, &post_rotation_tx_1.signature).await?;
    operator_util::wait_for_transaction_completion(pool, &post_rotation_tx_2.signature).await?;
    operator_util::wait_for_transaction_completion(pool, &post_rotation_tx_3.signature).await?;

    println!("✓ All 3 post-rotation withdrawals completed successfully");

    // These must consume bits in the new window without rotating again.
    let bitmap = read_withdrawal_bitmap(client, &env.instance).await?;
    assert_eq!(
        bitmap.generation, 1,
        "three withdrawals inside one window must not rotate again"
    );
    let mut expected = before.consumed.clone();
    expected.extend([first_new_nonce, first_new_nonce + 1, first_new_nonce + 2]);
    assert_eq!(
        bitmap.consumed, expected,
        "each post-rotation withdrawal must consume its own nonce in the new generation"
    );
    println!("✓ Verified generation = 1 (stable after rotation)");

    Ok(())
}

// ============================================================================
// Master Integration Test
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_master_chaos_stress_test() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("RUST_LOG").is_ok() {
        init_tracing();
    }

    println!("=== Master Chaos Stress Test ===");
    println!("Configuration:");
    println!("  Users: {}", NUM_USERS);
    println!("  Deposits per user: {}", DEPOSITS_PER_USER);
    println!("  Total transactions: {}", NUM_USERS * DEPOSITS_PER_USER);
    println!();

    let (test_validator, faucet_keypair, geyser_port) = start_test_validator().await;
    println!(
        "Solana test validator started on {}",
        test_validator.rpc_url()
    );
    let geyser_endpoint = format!("http://127.0.0.1:{}", geyser_port);
    println!("Geyser plugin running on port {}", geyser_port);

    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());
    let client_arc = Arc::new(client);

    // Start PostgreSQL container for indexer
    let indexer_postgres_container = Postgres::default()
        .with_db_name("indexer")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let indexer_host = indexer_postgres_container.get_host().await?;
    let indexer_port = indexer_postgres_container.get_host_port_ipv4(5432).await?;
    let indexer_db_url = format!(
        "postgres://postgres:password@{}:{}/indexer",
        indexer_host, indexer_port
    );

    println!("Indexer PostgreSQL container started: {}", indexer_db_url);
    let indexer_pool = db::connect(&indexer_db_url).await?;

    println!("Running database migrations...");
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: indexer_db_url.clone(),
            max_connections: 50,
        })
        .await?,
    );
    storage.init_schema().await?;

    println!("{}{}", CYAN, "=".repeat(40));
    println!("{}PHASE 0: Setup{}", BOLD, RESET);
    println!("{}{}", CYAN, "=".repeat(40));
    // Setup escrow instance
    let instance_seeds = Keypair::from_seed(&ESCROW_INSTANCE_SEEDS_PRIVATE_KEY).unwrap();
    let (_instance_seed, instance_pda) =
        TestEnvironment::setup_instance(&client_arc, &faucet_keypair, Some(instance_seeds)).await?;

    // Setup operator
    TestEnvironment::setup_operator(&client_arc, &faucet_keypair, instance_pda).await?;
    println!("✓ Operator added");

    let (env, env_filtered, count_before, initial_balance_per_user, pre_indexer_transactions) =
        setup_test_environments(&client_arc, &faucet_keypair, &indexer_pool).await?;

    println!("{}{}", CYAN, "=".repeat(40));
    println!("{}PHASE 1: Start Indexers and Operators{}", BOLD, ORANGE);
    println!("{}{}", CYAN, "=".repeat(40));
    // Start PrivateChannel indexer (Yellowstone) in background
    println!("\n=== Starting PrivateChannel Indexer (Yellowstone) ===");
    let (_private_channel_indexer_handle, _private_channel_indexer_storage) =
        start_private_channel_indexer(
            Some(geyser_endpoint.clone()),
            test_validator.rpc_url(),
            indexer_db_url.clone(),
        )
        .await
        .expect("Failed to start PrivateChannel indexer");

    println!("PrivateChannel Indexer started successfully");

    // Start Solana indexer (Yellowstone geyser) in background
    println!("\n=== Starting Solana Indexer (Yellowstone Geyser) ===");
    let geyser_endpoint = format!("http://127.0.0.1:{}", geyser_port);
    let (_solana_indexer_handle, _solana_indexer_storage) = start_solana_indexer(
        geyser_endpoint,
        test_validator.rpc_url(),
        indexer_db_url.clone(),
        Some(instance_pda),
    )
    .await
    .expect("Failed to start Solana indexer");

    println!("Solana Indexer started successfully");

    // Start Solana -> PrivateChannel operator
    let operator_key = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).unwrap();
    println!("\n=== Starting Solana -> PrivateChannel Operator ===");
    let operator_key_clone = Keypair::try_from(&operator_key.to_bytes()[..]).unwrap();
    let _solana_to_private_channel_operator_handle = start_solana_to_private_channel_operator(
        test_validator.rpc_url(),
        indexer_db_url.clone(),
        operator_key_clone,
        instance_pda,
    )
    .await
    .expect("Failed to start Solana -> PrivateChannel operator");
    println!("Solana -> PrivateChannel Operator started successfully");

    println!("\n=== Starting PrivateChannel -> Solana Operator ===");
    let operator_key_clone = Keypair::try_from(&operator_key.to_bytes()[..]).unwrap();
    let _private_channel_to_solana_operator_handle = start_private_channel_to_solana_operator(
        test_validator.rpc_url(),
        test_validator.rpc_url(),
        indexer_db_url.clone(),
        operator_key_clone,
        instance_pda,
    )
    .await
    .expect("Failed to start PrivateChannel -> Solana operator");
    println!("PrivateChannel -> Solana Operator started successfully");

    println!("\n{}{}", GREEN, "=".repeat(40));
    println!("{}PHASE 2: Verify Backfill{}", BOLD, RESET);
    println!("{}{}", GREEN, "=".repeat(40));
    verify_backfill_phase(&indexer_pool, &pre_indexer_transactions).await?;

    println!("\n{}{}", YELLOW, "=".repeat(40));
    println!("{}PHASE 3: Execute Deposits (Concurrent){}", BOLD, RESET);
    println!("{}{}", YELLOW, "=".repeat(40));
    let (all_transactions, filtered_transactions) =
        execute_deposit_phase(&client_arc, &env, &env_filtered).await?;

    println!("\n{}{}", BLUE, "=".repeat(40));
    println!("{}PHASE 4: Verify Deposits Indexed{}", BOLD, RESET);
    println!("{}{}", BLUE, "=".repeat(40));
    verify_deposit_indexing(
        &client_arc,
        &indexer_pool,
        count_before,
        &pre_indexer_transactions,
        &all_transactions,
        &filtered_transactions,
    )
    .await?;

    println!("\n{}{}", MAGENTA, "=".repeat(40));
    println!(
        "{}PHASE 5: Verify Operator Processes Deposits{}",
        BOLD, RESET
    );
    println!("{}{}", MAGENTA, "=".repeat(40));
    verify_operator_processing(&indexer_pool, &pre_indexer_transactions, &all_transactions).await?;

    println!("\n{}{}", RED, "=".repeat(40));
    println!("{}PHASE 6: Execute Withdrawals{}", BOLD, RESET);
    println!("{}{}", RED, "=".repeat(40));
    let withdrawal_transactions = execute_withdrawal_phase(&client_arc, &env).await?;

    println!("\n{}{}", CYAN, "=".repeat(40));
    println!(
        "{}PHASE 7: Verify Withdrawals & Operator Processing{}",
        BOLD, RESET
    );
    println!("{}{}", CYAN, "=".repeat(40));
    verify_withdrawal_processing(
        &indexer_pool,
        count_before,
        &all_transactions,
        &withdrawal_transactions,
    )
    .await?;

    println!("\n{}{}", GREEN, "=".repeat(40));
    println!("{}PHASE 8: Final Balance Verification{}", BOLD, RESET);
    println!("{}{}", GREEN, "=".repeat(40));
    verify_final_balances(&client_arc, &env, initial_balance_per_user).await?;

    println!("\n{}{}", YELLOW, "=".repeat(40));
    println!("{}PHASE 9: Complete Database Verification{}", BOLD, RESET);
    println!("{}{}", YELLOW, "=".repeat(40));
    println!("\n## Final Database Verification");
    println!("Verifying all deposits AND withdrawals in database...");

    let mut all_tracked_transactions = pre_indexer_transactions.clone();
    all_tracked_transactions.extend(all_transactions.clone());
    all_tracked_transactions.extend(withdrawal_transactions.clone());

    verify_database(
        &indexer_pool,
        &all_tracked_transactions,
        &filtered_transactions,
        "Indexer (Final)",
    )
    .await?;

    println!("\n{}{}", BLUE, "=".repeat(40));
    println!("{}PHASE 10: Tree Rotation Boundary Test{}", BOLD, RESET);
    println!("{}{}", BLUE, "=".repeat(40));
    execute_tree_rotation_boundary_phase(&client_arc, &indexer_pool, &env).await?;

    println!("\n{}{}", MAGENTA, "=".repeat(40));
    println!("{}PHASE 11: Post-Rotation Verification{}", BOLD, RESET);
    println!("{}{}", MAGENTA, "=".repeat(40));
    execute_post_rotation_verification_phase(&client_arc, &indexer_pool, &env).await?;

    println!("\n=== All Verifications Passed (Including Tree Rotation & Post-Rotation) ===");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Config-validation tests
//
// These two tests exercise `PrivateChannelIndexerConfig::validate()` and the
// startup-reconciliation skip branch logic. They are intentionally placed
// in the `indexer_integration` binary so they share its build artefacts
// with the main chaos test — but they require *no* fixtures (no Postgres,
// no validator, no Yellowstone) and run in well under a second each.
// Cargo executes tests in parallel by default, so they complete long
// before `test_master_chaos_stress_test`'s ~30 s fixture boot.
// ─────────────────────────────────────────────────────────────────────────────

/// Config validation rejects Escrow mode without an `escrow_instance_id`.
///
/// Targets `private_channel_indexer::config::PrivateChannelIndexerConfig::validate()`. The error
/// message contract is part of the CLI's public surface (operators rely on it
/// to diagnose startup failures), so we assert the exact substring.
#[test]
fn test_indexer_missing_escrow_instance_id_fails() {
    let bad = private_channel_indexer::PrivateChannelIndexerConfig {
        program_type: private_channel_indexer::ProgramType::Escrow,
        storage_type: private_channel_indexer::StorageType::Postgres,
        rpc_url: "http://localhost:0".to_string(),
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres: private_channel_indexer::PostgresConfig {
            database_url: "postgresql://unused".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: None, // ← the violation
    };

    let err = bad
        .validate()
        .expect_err("Escrow config without escrow_instance_id must fail validation");

    assert!(
        err.contains("--escrow-instance-id required"),
        "error message must name the missing CLI flag, got: {err:?}"
    );
}

/// Complement of the Escrow-validation test above: Withdraw mode must
/// reject an *unexpected* `escrow_instance_id` for symmetry. Exercises
/// the matching arm of `PrivateChannelIndexerConfig::validate()` that the config
/// unit tests also lock in.
#[test]
fn test_indexer_withdraw_with_instance_id_fails() {
    use std::str::FromStr;

    let bad = private_channel_indexer::PrivateChannelIndexerConfig {
        program_type: private_channel_indexer::ProgramType::Withdraw,
        storage_type: private_channel_indexer::StorageType::Postgres,
        rpc_url: "http://localhost:0".to_string(),
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres: private_channel_indexer::PostgresConfig {
            database_url: "postgresql://unused".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: Some(
            solana_sdk::pubkey::Pubkey::from_str("11111111111111111111111111111111").unwrap(),
        ),
    };

    let err = bad
        .validate()
        .expect_err("Withdraw config with escrow_instance_id must fail validation");

    assert!(
        err.contains("should not be set for Withdraw program"),
        "error message must explain why, got: {err:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// `validate_gap` boundary tests
//
// Pure-function unit-of-behaviour tests for `indexer::backfill::validate_gap`
// that lift integration coverage on the function's body without needing RPC
// or Postgres. Same shape and cost as the config-validation tests above.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_backfill_validate_gap_no_gap() {
    use private_channel_indexer::indexer::backfill::validate_gap;
    assert!(matches!(validate_gap(100, 100, 50), Ok(None)));
    assert!(matches!(validate_gap(99, 100, 50), Ok(None)));
}

#[test]
fn test_backfill_validate_gap_within_threshold() {
    use private_channel_indexer::indexer::backfill::validate_gap;
    let r = validate_gap(150, 100, 50).expect("gap within threshold must be Ok");
    assert_eq!(r, Some(50));

    let r = validate_gap(101, 100, 50).expect("minimal gap must be Ok");
    assert_eq!(r, Some(1));
}

#[test]
fn test_backfill_validate_gap_rejects_too_large() {
    use private_channel_indexer::error::BackfillError;
    use private_channel_indexer::indexer::backfill::validate_gap;
    let err = validate_gap(200, 100, 50).expect_err("gap > max must be rejected");
    match err {
        BackfillError::GapTooLarge { gap, max_gap } => {
            assert_eq!(gap, 100);
            assert_eq!(max_gap, 50);
        }
        other => panic!("expected GapTooLarge, got {other:?}"),
    }
}
