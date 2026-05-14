//! Integration test for indexer gap detection and restart recovery.
//!
//! Verifies that when the Solana indexer is stopped while on-chain deposits occur,
//! it correctly re-indexes the missed ("gap") slots when it is restarted, and
//! that the checkpoint advances past every recovered deposit's slot.
//!
//! Phases:
//! 1. Start validator + Postgres.
//! 2. Execute 2 deposits → start indexer → verify backfill.
//! 3. Abort indexer → execute 2 more deposits (the "gap").
//! 4. Restart indexer → verify all 4 deposits are in DB + checkpoint advanced.

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
#[allow(dead_code)]
mod setup;

use helpers::{db, send_and_confirm_instructions, test_types::*};
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::PostgresConfig;
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::SeedDerivable;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signer};
use std::sync::Arc;
use test_utils::indexer_helper::{start_solana_indexer, start_solana_indexer_rpc_polling};
use test_utils::validator_helper::{start_test_validator, start_test_validator_no_geyser};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

const GAP_DEPOSIT_AMOUNT: u64 = 50_000;

async fn execute_deposit(
    client: &RpcClient,
    user: &Keypair,
    instance: &solana_sdk::pubkey::Pubkey,
    mint: &solana_sdk::pubkey::Pubkey,
    amount: u64,
) -> Result<UserTransaction, Box<dyn std::error::Error>> {
    let (allowed_mint_pda, _) = find_allowed_mint_pda(instance, mint);
    let (event_authority_pda, _) = find_event_authority_pda();

    let user_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &user.pubkey(),
        mint,
        &spl_token::ID,
    );
    let instance_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        instance,
        mint,
        &spl_token::ID,
    );

    let deposit_ix = private_channel_escrow_program_client::instructions::DepositBuilder::new()
        .payer(user.pubkey())
        .user(user.pubkey())
        .instance(*instance)
        .mint(*mint)
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

    let signature =
        send_and_confirm_instructions(client, &[deposit_ix], user, &[user], "Deposit").await?;

    let statuses = client.get_signature_statuses(&[signature]).await?;
    let slot = statuses
        .value
        .first()
        .and_then(|s| s.as_ref())
        .map(|s| s.slot)
        .ok_or("Failed to get slot from signature status")?;

    Ok(UserTransaction {
        user_pubkey: user.pubkey(),
        amount,
        signature: signature.to_string(),
        slot,
        tx_type: TransactionType::Deposit,
    })
}

/// Full lifecycle: deposit → index → stop → deposit-while-down → restart → verify recovery.
///
/// Deposits 1 and 2 are indexed on the first run.  Deposits 3 and 4 land while
/// the indexer is offline.  After restart the indexer must backfill the gap and
/// the DB must contain all 4 deposits with correct slot and amount values.
/// The checkpoint stored in `indexer_state` must also advance past the highest
/// gap-deposit slot, proving the backfill reached and committed those slots.
#[tokio::test(flavor = "multi_thread")]
async fn test_gap_detection_restart_recovery() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Gap Detection: Restart Recovery Test ===\n");

    // ── Phase 0: Infrastructure ─────────────────────────────────────────
    println!("## Phase 0: Start validator + Postgres");
    let (test_validator, faucet_keypair, geyser_port) = start_test_validator().await;
    let geyser_endpoint = format!("http://127.0.0.1:{}", geyser_port);
    println!("  Validator: {}", test_validator.rpc_url());
    println!("  Geyser:    {}", geyser_endpoint);

    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());
    let client = Arc::new(client);

    let pg_container = Postgres::default()
        .with_db_name("gap_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/gap_test",
        pg_host, pg_port
    );
    println!("  Postgres:  {}\n", db_url);

    let pool = db::connect(&db_url).await?;

    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 50,
        })
        .await?,
    );
    storage.init_schema().await?;

    // ── Phase 1: On-chain setup ─────────────────────────────────────────
    println!("## Phase 1: On-chain setup (instance, operator, user, mint)");
    let instance_seeds = Keypair::from_seed(&ESCROW_INSTANCE_SEEDS_PRIVATE_KEY).unwrap();
    let (_instance_seed, instance_pda) =
        TestEnvironment::setup_instance(&client, &faucet_keypair, Some(instance_seeds)).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, instance_pda).await?;

    let env = TestEnvironment::setup(
        &client,
        &faucet_keypair,
        1,
        10_000_000 * 10u64.pow(6),
        Some(Keypair::from_seed(&ESCROW_INSTANCE_SEEDS_PRIVATE_KEY).unwrap()),
    )
    .await?;

    let user = &env.users[0];
    println!("  Instance: {}", env.instance);
    println!("  Mint:     {}", env.mint);
    println!("  User:     {}\n", user.pubkey());

    // ── Phase 2: Pre-indexer deposits ───────────────────────────────────
    println!("## Phase 2: Execute 2 deposits BEFORE indexer starts");
    let mut all_signatures: Vec<UserTransaction> = Vec::new();

    let tx1 = execute_deposit(&client, user, &env.instance, &env.mint, GAP_DEPOSIT_AMOUNT).await?;
    println!("  Deposit 1: {} (slot {})", tx1.signature, tx1.slot);
    all_signatures.push(tx1);

    let tx2 = execute_deposit(
        &client,
        user,
        &env.instance,
        &env.mint,
        GAP_DEPOSIT_AMOUNT + 1000,
    )
    .await?;
    println!("  Deposit 2: {} (slot {})", tx2.signature, tx2.slot);
    all_signatures.push(tx2);

    // ── Phase 3: Start indexer — should backfill the 2 deposits ─────────
    println!("\n## Phase 3: Start Solana indexer (first run — backfill expected)");
    let (indexer_handle, _storage) = start_solana_indexer(
        geyser_endpoint.clone(),
        test_validator.rpc_url(),
        db_url.clone(),
        Some(instance_pda),
    )
    .await?;
    println!("  Indexer started");

    // ── Phase 4: Verify initial backfill ────────────────────────────────
    println!("\n## Phase 4: Wait for 2 deposits in DB");
    let ready = db::wait_for_count(&pool, 2, *WAIT_TIMEOUT_SECS).await?;
    assert!(
        ready,
        "Indexer did not backfill 2 pre-indexer deposits within timeout"
    );

    for tx in &all_signatures {
        let db_tx = db::get_transaction(&pool, &tx.signature)
            .await?
            .unwrap_or_else(|| panic!("Deposit {} not found in DB", tx.signature));
        assert_eq!(
            db_tx.slot as u64, tx.slot,
            "Slot mismatch for {}",
            tx.signature
        );
        assert_eq!(
            db_tx.amount as u64, tx.amount,
            "Amount mismatch for {}",
            tx.signature
        );
        assert_eq!(db_tx.transaction_type, "deposit");
        println!("  Verified: {}", tx.signature);
    }

    let checkpoint_before_gap = db::get_checkpoint_slot(&pool, "escrow")
        .await?
        .expect("Checkpoint should exist after backfill");
    println!("  Checkpoint after first run: {}\n", checkpoint_before_gap);

    // ── Phase 5: Kill the indexer ───────────────────────────────────────
    println!("## Phase 5: Abort indexer");
    indexer_handle.abort();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("  Indexer aborted\n");

    // ── Phase 6: Execute 2 deposits while indexer is down ───────────────
    println!("## Phase 6: Execute 2 deposits while indexer is DOWN");
    let tx3 = execute_deposit(
        &client,
        user,
        &env.instance,
        &env.mint,
        GAP_DEPOSIT_AMOUNT + 2000,
    )
    .await?;
    println!("  Deposit 3: {} (slot {})", tx3.signature, tx3.slot);
    all_signatures.push(tx3);

    let tx4 = execute_deposit(
        &client,
        user,
        &env.instance,
        &env.mint,
        GAP_DEPOSIT_AMOUNT + 3000,
    )
    .await?;
    println!("  Deposit 4: {} (slot {})", tx4.signature, tx4.slot);
    all_signatures.push(tx4);

    // ── Phase 7: Restart indexer — backfill should recover the gap ──────
    println!("\n## Phase 7: Start NEW Solana indexer (second run — gap recovery expected)");
    let (indexer_handle_2, _storage_2) = start_solana_indexer(
        geyser_endpoint.clone(),
        test_validator.rpc_url(),
        db_url.clone(),
        Some(instance_pda),
    )
    .await?;
    println!("  Indexer restarted");

    // ── Phase 8: Verify all 4 deposits recovered ────────────────────────
    println!("\n## Phase 8: Wait for all 4 deposits in DB");
    let ready = db::wait_for_count(&pool, 4, *WAIT_TIMEOUT_SECS).await?;
    assert!(ready, "Indexer did not recover gap deposits within timeout");

    for tx in &all_signatures {
        let db_tx = db::get_transaction(&pool, &tx.signature)
            .await?
            .unwrap_or_else(|| {
                panic!(
                    "Deposit {} not found in DB after gap recovery",
                    tx.signature
                )
            });
        assert_eq!(
            db_tx.slot as u64, tx.slot,
            "Slot mismatch for {}",
            tx.signature
        );
        assert_eq!(
            db_tx.amount as u64, tx.amount,
            "Amount mismatch for {}",
            tx.signature
        );
        assert_eq!(db_tx.transaction_type, "deposit");
        println!("  Verified: {} (slot {})", tx.signature, tx.slot);
    }

    let gap_deposit_max_slot = all_signatures.iter().map(|t| t.slot).max().unwrap();

    // Wait for the checkpoint to advance past the gap deposits' max slot.
    // The deposits are in the DB, but the checkpoint update is asynchronous
    // so we must poll rather than read immediately.
    let checkpoint_ready =
        db::wait_for_checkpoint(&pool, "escrow", gap_deposit_max_slot, *WAIT_TIMEOUT_SECS).await?;
    assert!(
        checkpoint_ready,
        "Checkpoint did not advance past gap deposits' max slot ({}) within timeout",
        gap_deposit_max_slot
    );

    let checkpoint_after_gap = db::get_checkpoint_slot(&pool, "escrow")
        .await?
        .expect("Checkpoint should exist after gap recovery");
    assert!(
        checkpoint_after_gap >= gap_deposit_max_slot,
        "Checkpoint ({}) should have advanced past the gap deposits' max slot ({})",
        checkpoint_after_gap,
        gap_deposit_max_slot
    );
    println!(
        "\n  Checkpoint after gap recovery: {} (gap deposits max slot: {})",
        checkpoint_after_gap, gap_deposit_max_slot
    );

    // Cleanup
    indexer_handle_2.abort();

    println!("\n=== Gap Detection: Restart Recovery Test PASSED ===");
    Ok(())
}

/// Same stop/gap/restart scenario as [`test_gap_detection_restart_recovery`] but
/// uses the **RPC-polling datasource** instead of Yellowstone geyser.
///
/// This test exercises `indexer/datasource/rpc_polling/source.rs` which is
/// otherwise unreachable in the integration suite (all other tests use geyser).
/// A validator WITHOUT the geyser plugin is used — only the RPC port is needed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "polling source races ahead of test-validator block availability \
            (treats -32009 Ok(None) as terminal advance)"]
async fn test_gap_detection_rpc_polling_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Gap Detection: RPC-Polling Fallback Test ===\n");

    // ── Phase 0: Infrastructure (no geyser needed) ───────────────────────
    println!("## Phase 0: Start validator (no geyser) + Postgres");
    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let rpc_url = test_validator.rpc_url();
    println!("  Validator: {}", rpc_url);

    let client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let client = Arc::new(client);

    let pg_container = Postgres::default()
        .with_db_name("gap_rpc_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/gap_rpc_test",
        pg_host, pg_port
    );
    println!("  Postgres: {}\n", db_url);

    let pool = db::connect(&db_url).await?;

    let storage = Storage::Postgres(
        PostgresDb::new(&private_channel_indexer::PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 50,
        })
        .await?,
    );
    storage.init_schema().await?;

    // ── Phase 1: On-chain setup ──────────────────────────────────────────
    println!("## Phase 1: On-chain setup");
    let instance_seeds = Keypair::from_seed(&ESCROW_INSTANCE_SEEDS_PRIVATE_KEY).unwrap();
    let (_instance_seed, instance_pda) =
        TestEnvironment::setup_instance(&client, &faucet_keypair, Some(instance_seeds)).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, instance_pda).await?;

    let env = TestEnvironment::setup(
        &client,
        &faucet_keypair,
        1,
        10_000_000 * 10u64.pow(6),
        Some(Keypair::from_seed(&ESCROW_INSTANCE_SEEDS_PRIVATE_KEY).unwrap()),
    )
    .await?;

    let user = &env.users[0];
    println!("  Instance: {}", env.instance);
    println!("  Mint:     {}", env.mint);
    println!("  User:     {}\n", user.pubkey());

    // ── Phase 2: Pre-indexer deposits ────────────────────────────────────
    println!("## Phase 2: Execute 2 deposits BEFORE indexer starts");
    let mut all_txs: Vec<UserTransaction> = Vec::new();

    for i in 0..2 {
        let tx = execute_deposit(
            &client,
            user,
            &env.instance,
            &env.mint,
            GAP_DEPOSIT_AMOUNT + i * 500,
        )
        .await?;
        println!("  Deposit {}: {} (slot {})", i + 1, tx.signature, tx.slot);
        all_txs.push(tx);
    }

    // ── Phase 3: Start RPC-polling indexer — backfill expected ───────────
    println!("\n## Phase 3: Start RPC-polling Solana indexer");
    let (indexer_handle, _storage) =
        start_solana_indexer_rpc_polling(rpc_url.clone(), db_url.clone(), Some(instance_pda))
            .await?;
    println!("  Indexer started (RPC-polling mode)");

    // ── Phase 4: Verify backfill ─────────────────────────────────────────
    println!("\n## Phase 4: Wait for 2 deposits in DB");
    let ready = db::wait_for_count(&pool, 2, *WAIT_TIMEOUT_SECS).await?;
    assert!(
        ready,
        "RPC-polling indexer did not backfill 2 pre-indexer deposits within timeout"
    );
    println!("  2 deposits confirmed in DB");

    let checkpoint_before = db::get_checkpoint_slot(&pool, "escrow")
        .await?
        .expect("Checkpoint should exist after backfill");
    println!("  Checkpoint after first run: {}", checkpoint_before);

    // ── Phase 5: Kill the indexer ────────────────────────────────────────
    println!("\n## Phase 5: Abort indexer");
    indexer_handle.abort();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // ── Phase 6: Gap deposits ────────────────────────────────────────────
    println!("\n## Phase 6: Execute 2 deposits while indexer is DOWN");
    for i in 0..2 {
        let tx = execute_deposit(
            &client,
            user,
            &env.instance,
            &env.mint,
            GAP_DEPOSIT_AMOUNT + 1000 + i * 500,
        )
        .await?;
        println!("  Deposit {}: {} (slot {})", i + 3, tx.signature, tx.slot);
        all_txs.push(tx);
    }

    // ── Phase 7: Restart indexer ─────────────────────────────────────────
    println!("\n## Phase 7: Restart RPC-polling indexer (gap recovery)");
    let (indexer_handle_2, _storage_2) =
        start_solana_indexer_rpc_polling(rpc_url.clone(), db_url.clone(), Some(instance_pda))
            .await?;
    println!("  Indexer restarted");

    // ── Phase 8: Verify all 4 deposits ──────────────────────────────────
    println!("\n## Phase 8: Wait for all 4 deposits in DB");
    let ready = db::wait_for_count(&pool, 4, *WAIT_TIMEOUT_SECS).await?;
    assert!(
        ready,
        "RPC-polling indexer did not recover gap deposits within timeout"
    );

    let gap_max_slot = all_txs.iter().map(|t| t.slot).max().unwrap();
    let checkpoint_ready =
        db::wait_for_checkpoint(&pool, "escrow", gap_max_slot, *WAIT_TIMEOUT_SECS).await?;
    assert!(
        checkpoint_ready,
        "Checkpoint did not advance past gap deposits' max slot ({}) within timeout",
        gap_max_slot
    );

    let checkpoint_after = db::get_checkpoint_slot(&pool, "escrow")
        .await?
        .expect("Checkpoint should exist after gap recovery");
    assert!(
        checkpoint_after >= gap_max_slot,
        "Checkpoint ({}) should have advanced past gap max slot ({})",
        checkpoint_after,
        gap_max_slot
    );
    println!(
        "\n  Checkpoint after gap recovery: {} (gap max slot: {})",
        checkpoint_after, gap_max_slot
    );

    indexer_handle_2.abort();
    println!("\n=== Gap Detection: RPC-Polling Fallback Test PASSED ===");
    Ok(())
}
