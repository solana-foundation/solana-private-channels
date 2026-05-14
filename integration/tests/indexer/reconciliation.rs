//! Integration tests for startup reconciliation against on-chain escrow ATA balances.
//!
//! Each test starts an isolated Postgres container and a real Solana test validator.
//! `run_startup_reconciliation` is called directly so tests focus exclusively on
//! reconciliation behaviour without starting the full indexer stack.
//!
//! Scenarios covered:
//! 1. Empty DB (no mints) → passes trivially without hitting RPC.
//! 2. DB has a phantom deposit (mint registered, tokens never on-chain) → blocks startup.
//! 3. Same phantom deposit but threshold covers the gap → passes with warning.
//! 4. Real tokens minted to escrow ATA, DB matches → passes with strict threshold.

#[path = "helpers/mod.rs"]
mod helpers;

// Pure-function coverage of `compare_balances` branches that aren't hit
// by the end-to-end reconciliation tests below.
#[path = "reconciliation_compare.rs"]
mod compare_balances;

// DB migration idempotency + insert-race safety on PostgresDb.
#[path = "db_migration_race.rs"]
mod db_migration_race;

// Pending-remint storage round-trip (set/get public API).
#[path = "pending_remint_storage.rs"]
mod pending_remint_storage;

use helpers::{generate_mint, mint_to_owner, setup_wallets};
use private_channel_escrow_program_client::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_indexer::{
    config::{ProgramType, ReconciliationConfig},
    error::{IndexerError, ReconciliationError},
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

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Derive the escrow instance PDA from a seed pubkey.
fn instance_pda(seed: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"instance", seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

/// Register a mint in the `mints` table and insert a single `pending` deposit of
/// `amount` raw tokens. Uses `pending` status to exercise the all-statuses fix —
/// all indexed deposits count toward the DB-expected balance regardless of status.
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
         VALUES ($1, 1, 'recon_test', 'recon_test', $2, $3,
                 'deposit'::transaction_type, 'pending'::transaction_status, NOW(), NOW())",
    )
    .bind(format!("recon_test_sig_{}", mint_address))
    .bind(mint_address)
    .bind(amount)
    .execute(pool)
    .await?;

    Ok(())
}

/// Start a fresh Postgres container and return (pool, Storage, container).
/// The container must be kept alive for the duration of the test.
async fn start_postgres(
) -> Result<(PgPool, Storage, testcontainers::ContainerAsync<Postgres>), Box<dyn std::error::Error>>
{
    let container = Postgres::default()
        .with_db_name("recon_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{}:{}/recon_test", host, port);

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

// ── Tests ─────────────────────────────────────────────────────────────────────

/// No mints registered in the DB → reconciliation returns immediately without
/// hitting the RPC at all (empty-mints fast path).
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_empty_db_passes() -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet, _geyser_port) = start_test_validator().await;
    let (_pool, storage, _pg) = start_postgres().await?;

    let result = run_startup_reconciliation(
        &ReconciliationConfig::default(),
        ProgramType::Escrow,
        &storage,
        &test_validator.rpc_url(),
        &Keypair::new().pubkey(),
    )
    .await;

    assert!(result.is_ok(), "empty DB must pass: {:?}", result);
    Ok(())
}

/// DB has a deposit for a mint whose escrow ATA does not exist on-chain (balance 0).
/// With strict threshold (0) reconciliation must block startup.
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_blocks_on_phantom_deposit() -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet, _geyser_port) = start_test_validator().await;
    let (pool, storage, _pg) = start_postgres().await?;

    let mint = Pubkey::new_unique();
    seed_mint_and_deposit(&pool, &mint.to_string(), 1_000_000).await?;

    let result = run_startup_reconciliation(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
        ProgramType::Escrow,
        &storage,
        &test_validator.rpc_url(),
        &Keypair::new().pubkey(),
    )
    .await;

    match result {
        Err(IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
            count,
            threshold,
        })) => {
            assert_eq!(count, 1, "exactly one mint should exceed threshold");
            assert_eq!(threshold, 0);
        }
        other => panic!("expected MismatchExceedsThreshold, got: {:?}", other),
    }
    Ok(())
}

/// Same phantom deposit, but the configured threshold is larger than the mismatch →
/// reconciliation logs a warning and returns Ok.
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_passes_within_threshold() -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, _faucet, _geyser_port) = start_test_validator().await;
    let (pool, storage, _pg) = start_postgres().await?;

    // DB expects 500_000; ATA absent → on-chain = 0; mismatch = 500_000 ≤ threshold 1_000_000
    let mint = Pubkey::new_unique();
    seed_mint_and_deposit(&pool, &mint.to_string(), 500_000).await?;

    let result = run_startup_reconciliation(
        &ReconciliationConfig {
            mismatch_threshold_raw: 1_000_000,
        },
        ProgramType::Escrow,
        &storage,
        &test_validator.rpc_url(),
        &Keypair::new().pubkey(),
    )
    .await;

    assert!(
        result.is_ok(),
        "mismatch within threshold should pass: {:?}",
        result
    );
    Ok(())
}

/// Tokens are minted directly to the escrow ATA on-chain; the DB records the
/// matching pending deposit. With strict threshold (0) reconciliation must pass.
///
/// This also exercises the all-statuses deposit fix: the DB row has
/// `status = 'pending'` (operator has not yet acted on private_channel) but the tokens are
/// already reflected in the ATA, so the DB-expected balance should include it.
#[tokio::test(flavor = "multi_thread")]
async fn test_reconciliation_passes_with_matching_on_chain_balance(
) -> Result<(), Box<dyn std::error::Error>> {
    let (test_validator, faucet_keypair, _geyser_port) = start_test_validator().await;
    let client = Arc::new(RpcClient::new_with_commitment(
        test_validator.rpc_url(),
        CommitmentConfig::confirmed(),
    ));
    let (pool, storage, _pg) = start_postgres().await?;

    // Fund an authority keypair that will pay for transactions and sign minting.
    let authority = Keypair::new();
    setup_wallets(client.as_ref(), &faucet_keypair, &[&authority]).await?;

    // Create an SPL mint with `authority` as the mint authority.
    let mint_keypair = Keypair::new();
    let mint_pubkey = generate_mint(client.as_ref(), &authority, &authority, &mint_keypair).await?;

    // Derive the escrow instance PDA and mint tokens directly to its ATA.
    let seed_keypair = Keypair::new();
    let pda = instance_pda(&seed_keypair.pubkey());

    const AMOUNT: u64 = 1_000_000;
    mint_to_owner(
        client.as_ref(),
        &authority,
        mint_pubkey,
        pda,
        &authority,
        AMOUNT,
    )
    .await?;

    // Seed the DB with a matching pending deposit — pending status is intentional
    // to confirm that all deposit statuses contribute to the DB-expected balance.
    seed_mint_and_deposit(&pool, &mint_pubkey.to_string(), AMOUNT as i64).await?;

    // run_startup_reconciliation queries at `finalized` commitment.  Wait for the
    // ATA balance to become visible at that commitment level before proceeding so
    // the test doesn't race against the validator's finalization pipeline.
    {
        let finalized_client =
            RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::finalized());
        let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &pda,
            &mint_pubkey,
            &spl_token::id(),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let balance = finalized_client.get_token_account_balance(&ata).await;
            if let Ok(b) = balance {
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

    let result = run_startup_reconciliation(
        &ReconciliationConfig {
            mismatch_threshold_raw: 0,
        },
        ProgramType::Escrow,
        &storage,
        &test_validator.rpc_url(),
        &pda,
    )
    .await;

    assert!(
        result.is_ok(),
        "DB matching real on-chain balance must pass: {:?}",
        result
    );
    Ok(())
}
