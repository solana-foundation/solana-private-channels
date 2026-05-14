//! E2E integration test for reconciliation DB-corruption detection (G1).
//!
//! Scenario: normal operations produce matching DB ↔ on-chain data.
//! Then a row in the DB is manually inflated (simulating corruption or a
//! bug).  `run_startup_reconciliation` with a zero tolerance must return an
//! error, proving that the reconciliation check catches the divergence.

#[path = "helpers/mod.rs"]
mod helpers;

use helpers::{generate_mint, mint_to_owner, setup_wallets};
use private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_indexer::{
    config::{ProgramType, ReconciliationConfig},
    error::IndexerError,
    indexer::reconciliation::run_startup_reconciliation,
    storage::{PostgresDb, Storage},
    PostgresConfig,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use sqlx::PgPool;
use std::sync::Arc;
use test_utils::validator_helper::start_test_validator;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── helpers ───────────────────────────────────────────────────────────────────

fn instance_pda(seed: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"instance", seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

async fn seed_mint_and_deposit(
    pool: &PgPool,
    mint_address: &str,
    amount: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mints (mint_address, decimals, token_program, created_at)
         VALUES ($1, 6, $2, NOW())",
    )
    .bind(mint_address)
    .bind(spl_token::id().to_string())
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO transactions
         (signature, slot, initiator, recipient, mint, amount,
          transaction_type, status, created_at, updated_at)
         VALUES ($1, 1, 'e2e_test', 'e2e_test', $2, $3,
                 'deposit'::transaction_type, 'pending'::transaction_status,
                 NOW(), NOW())",
    )
    .bind(format!("e2e_sig_{}", mint_address))
    .bind(mint_address)
    .bind(amount)
    .execute(pool)
    .await?;

    Ok(())
}

async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("recon_e2e")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{}:{}/recon_e2e", host, port);

    let pool = PgPool::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url,
            max_connections: 5,
        })
        .await?,
    );
    storage.init_schema().await?;

    Ok((pool, storage, container))
}

// ── test ──────────────────────────────────────────────────────────────────────

/// Verifies that `run_startup_reconciliation` detects a corrupted DB row.
///
/// Steps:
/// 1. Mint real SPL tokens to the escrow instance ATA (on-chain balance = AMOUNT).
/// 2. Seed the DB with a matching deposit row (db_expected = AMOUNT).
/// 3. Assert reconciliation passes (sanity check).
/// 4. Inflate the DB row to AMOUNT * 2.
/// 5. Assert reconciliation fails with a mismatch error.
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_catches_corrupted_db() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Reconciliation E2E: Corruption Detection ===");

    let (test_validator, faucet_keypair, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        test_validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let (pool, storage, _pg) = start_postgres().await?;

    // Fund authority
    let authority = Keypair::new();
    setup_wallets(client.as_ref(), &faucet_keypair, &[&authority]).await?;

    // Create mint
    let mint_keypair = Keypair::new();
    let mint_pubkey = generate_mint(client.as_ref(), &authority, &authority, &mint_keypair).await?;

    // Derive escrow instance PDA
    let seed_keypair = Keypair::new();
    let pda = instance_pda(&seed_keypair.pubkey());

    const AMOUNT: u64 = 500_000;

    // Mint real tokens to the escrow instance ATA
    mint_to_owner(
        client.as_ref(),
        &authority,
        mint_pubkey,
        pda,
        &authority,
        AMOUNT,
    )
    .await?;

    // Seed DB with matching record
    seed_mint_and_deposit(&pool, &mint_pubkey.to_string(), AMOUNT as i64).await?;

    // Wait for finalized ATA balance
    {
        let fin_client =
            RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::finalized());
        let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &pda,
            &mint_pubkey,
            &spl_token::id(),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if let Ok(b) = fin_client.get_token_account_balance(&ata).await {
                if b.amount.parse::<u64>().unwrap_or(0) == AMOUNT {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "Timed out waiting for finalized ATA balance"
            );
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    let zero_threshold = ReconciliationConfig {
        mismatch_threshold_raw: 0,
    };

    // Step 3: reconciliation must PASS before corruption
    println!("  Step 3: Reconciliation before corruption — expecting Ok");
    let pre_result = run_startup_reconciliation(
        &zero_threshold,
        ProgramType::Escrow,
        &storage,
        &test_validator.rpc_url(),
        &pda,
    )
    .await;
    assert!(
        pre_result.is_ok(),
        "Reconciliation should pass when DB matches on-chain: {:?}",
        pre_result
    );
    println!("  ✓ Pre-corruption reconciliation passed");

    // Step 4: corrupt the DB — inflate the amount to 2×
    println!("  Step 4: Corrupting DB (inflating amount to 2×)");
    sqlx::query("UPDATE transactions SET amount = amount * 2")
        .execute(&pool)
        .await?;

    // Step 5: reconciliation must FAIL after corruption
    println!("  Step 5: Reconciliation after corruption — expecting Err");
    let post_result = run_startup_reconciliation(
        &zero_threshold,
        ProgramType::Escrow,
        &storage,
        &test_validator.rpc_url(),
        &pda,
    )
    .await;

    assert!(
        post_result.is_err(),
        "Reconciliation must detect the corrupted DB row and return Err"
    );

    // Verify it is a reconciliation mismatch error, not some unrelated failure
    match post_result.unwrap_err() {
        IndexerError::Reconciliation(_) => {
            println!("  ✓ Corruption correctly detected as ReconciliationError");
        }
        other => {
            panic!("Expected ReconciliationError, got: {:?}", other);
        }
    }

    println!("=== Reconciliation E2E: Corruption Detection PASSED ===");
    Ok(())
}
