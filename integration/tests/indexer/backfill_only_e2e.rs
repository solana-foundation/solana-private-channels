//! End-to-end coverage for backfill-only mode (`backfill.backfill_only = true`).
//!
//! This is the repair path an operator runs when finalized events are known to be
//! missing from the database. It is a one-shot: `private_channel_indexer::run` fills the
//! resolved slot range and exits, with no live datasource behind it.
//!
//! Two properties are pinned here, both of which need real Postgres:
//!
//! 1. A clean run records every deposit in the range and advances the durable
//!    checkpoint past the last of them, and re-running it changes nothing.
//! 2. A run whose slot writes fail exits non-zero carrying the storage error, and
//!    leaves the checkpoint parked below the slot it could not store, so the next
//!    attempt replays from there.

// Shared `#[path]` helper modules (helpers, setup) expose more than this binary
// uses; match the sibling integration test crates and allow the unused items.
#![allow(dead_code)]

#[path = "helpers/mod.rs"]
mod helpers;

#[path = "setup.rs"]
mod setup;

use helpers::{db, send_and_confirm_instructions};
use private_channel_indexer::{
    config::{BackfillConfig, ReconciliationConfig},
    error::{CheckpointError, IndexerError},
    indexer::run,
    storage::{PostgresDb, Storage},
    DatasourceType, IndexerConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType,
    RpcPollingConfig, StorageType,
};
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use solana_transaction_status::UiTransactionEncoding;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use test_utils::validator_helper::start_test_validator_no_geyser;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Upper bound for one backfill-only run (resolve, fill, drain, verify, close).
const BACKFILL_TIMEOUT_SECS: u64 = 180;
/// Per-user SPL balance minted at setup, large enough to fund the deposits below.
const USER_BALANCE: u64 = 1_000_000;
/// Deposit amounts, distinct so each row can be told apart in the assertions.
const DEPOSIT_AMOUNT_A: u64 = 31_000;
const DEPOSIT_AMOUNT_B: u64 = 42_000;

// ── harness ─────────────────────────────────────────────────────────────────

async fn start_postgres(
    db_name: &str,
) -> Result<
    (
        String,
        Arc<Storage>,
        testcontainers::ContainerAsync<Postgres>,
    ),
    Box<dyn std::error::Error>,
> {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);

    let storage = Arc::new(Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 5,
        })
        .await?,
    ));
    storage.init_schema().await?;

    Ok((db_url, storage, container))
}

/// Backfill-only config for the escrow indexer over `(start_slot - 1, tip]`.
///
/// `rpc_polling` is required even though no datasource is ever started: the backfill
/// branch reads its encoding and commitment before deciding the mode.
fn backfill_only_config(
    rpc_url: String,
    db_url: String,
    instance: Pubkey,
    start_slot: u64,
) -> (PrivateChannelIndexerConfig, IndexerConfig) {
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: rpc_url.clone(),
        // Only read by startup reconciliation, which backfill-only mode skips.
        source_rpc_url: Some(rpc_url.clone()),
        fallback_rpc_url: None,
        postgres: PostgresConfig {
            database_url: db_url,
            max_connections: 5,
        },
        escrow_instance_id: Some(instance),
    };

    let indexer = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: Some(RpcPollingConfig {
            poll_interval_ms: 200,
            error_retry_interval_ms: 1000,
            batch_size: 10,
            from_slot: Some(start_slot),
            encoding: UiTransactionEncoding::Json,
            commitment: CommitmentLevel::Finalized,
        }),
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: true,
            exit_after_backfill: true,
            rpc_url,
            batch_size: 50,
            // The range is bounded by the deposits below, not by a gap ceiling.
            max_gap_slots: u64::MAX,
            start_slot: Some(start_slot),
        },
        reconciliation: ReconciliationConfig::default(),
    };

    (common, indexer)
}

async fn run_backfill_only_once(
    common: PrivateChannelIndexerConfig,
    indexer: IndexerConfig,
) -> Result<(), IndexerError> {
    tokio::time::timeout(
        Duration::from_secs(BACKFILL_TIMEOUT_SECS),
        run(common, indexer, None),
    )
    .await
    .expect("backfill-only run timed out")
}

/// Wait for the finalized tip to clear `target`, so every deposit block is inside the
/// range and retrievable rather than raced at the bleeding edge.
async fn wait_for_finalized_slot(rpc_url: &str, target: u64) {
    let client = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::finalized());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if client
            .get_slot()
            .await
            .map(|s| s >= target)
            .unwrap_or(false)
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for finalized slot to reach {target}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Block until `slot`'s block can actually be fetched.
///
/// Finalization alone is not enough: the validator will list a recent slot before its block
/// is fully readable, and the fill refuses to checkpoint past a block it cannot read. Waiting
/// on the fetch itself is what makes the range below deterministic.
async fn wait_for_block_available(rpc_url: &str, slot: u64) {
    let client = RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::finalized());
    let config = solana_client::rpc_config::RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Json),
        transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
        rewards: Some(false),
        commitment: Some(CommitmentConfig::finalized()),
        max_supported_transaction_version: Some(0),
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if client.get_block_with_config(slot, config).await.is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for slot {slot} to become fetchable"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Submit one escrow deposit and return its signature and confirmed slot.
async fn execute_deposit(
    client: &RpcClient,
    user: &Keypair,
    instance: &Pubkey,
    mint: &Pubkey,
    amount: u64,
) -> Result<(String, u64), Box<dyn std::error::Error>> {
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
        .ok_or("deposit signature has no confirmed slot")?;

    Ok((signature.to_string(), slot))
}

// ── tests ───────────────────────────────────────────────────────────────────

/// A backfill-only repair must record the finalized deposits in its range and only
/// report success once the checkpoint covers every one of them. Re-running it must be
/// a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_only_records_finalized_deposits_and_exits_clean(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = Arc::new(RpcClient::new_with_commitment(
        rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));
    let (db_url, _storage, _pg) = start_postgres("backfill_only_records").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    let user = &env.users[0];

    let (sig_a, slot_a) =
        execute_deposit(&client, user, &env.instance, &env.mint, DEPOSIT_AMOUNT_A).await?;
    let (sig_b, slot_b) =
        execute_deposit(&client, user, &env.instance, &env.mint, DEPOSIT_AMOUNT_B).await?;

    // Headroom past the last deposit so its block is finalized and inside the range.
    wait_for_finalized_slot(&rpc_url, slot_b + 3).await;
    wait_for_block_available(&rpc_url, slot_a).await;
    wait_for_block_available(&rpc_url, slot_b).await;

    // Anchor the range at the first deposit rather than at whatever slot the validator
    // happened to be on at startup. A freshly started validator lists its earliest slots
    // without always serving their blocks, and the fill refuses to checkpoint past a block
    // it cannot read, so a range reaching back that far fails on the ledger rather than on
    // anything this test is about.
    let start_slot = slot_a;

    let (common, indexer) =
        backfill_only_config(rpc_url.clone(), db_url.clone(), env.instance, start_slot);
    run_backfill_only_once(common, indexer)
        .await
        .expect("a clean backfill-only run must succeed");

    // The run closes the pool it was given, so assertions need their own.
    let pool: PgPool = db::connect(&db_url).await?;

    let row_a = db::get_transaction(&pool, &sig_a)
        .await?
        .expect("deposit A must be recorded by the repair");
    let row_b = db::get_transaction(&pool, &sig_b)
        .await?
        .expect("deposit B must be recorded by the repair");
    assert_eq!(row_a.amount.value(), DEPOSIT_AMOUNT_A);
    assert_eq!(row_b.amount.value(), DEPOSIT_AMOUNT_B);
    assert_eq!(row_a.slot as u64, slot_a);
    assert_eq!(row_b.slot as u64, slot_b);
    assert_eq!(
        row_a.status, "pending",
        "a backfilled deposit is queued for the operator, not already serviced"
    );

    let checkpoint = db::get_checkpoint_slot(&pool, "escrow")
        .await?
        .expect("a successful repair must leave a durable checkpoint");
    assert!(
        checkpoint >= slot_b,
        "checkpoint {checkpoint} must cover the last recorded deposit at slot {slot_b}"
    );

    let count_after_first = db::count_transactions(&pool).await?;
    assert_eq!(
        count_after_first, 2,
        "exactly the two deposits are recorded"
    );

    // Second run: the range now starts at the committed checkpoint, so the deposits
    // are below it and are never re-emitted.
    let (common, indexer) =
        backfill_only_config(rpc_url.clone(), db_url.clone(), env.instance, start_slot);
    run_backfill_only_once(common, indexer)
        .await
        .expect("re-running a completed repair must still succeed");

    let pool: PgPool = db::connect(&db_url).await?;
    assert_eq!(
        db::count_transactions(&pool).await?,
        count_after_first,
        "a second repair must not duplicate rows"
    );
    let checkpoint_after = db::get_checkpoint_slot(&pool, "escrow").await?.unwrap();
    assert!(
        checkpoint_after >= checkpoint,
        "the checkpoint must never regress across repairs"
    );

    Ok(())
}

/// When a slot's rows cannot be written the repair must fail loudly with the storage
/// error, and must leave the checkpoint below that slot so the next run replays it.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_only_exits_nonzero_when_slot_writes_fail(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = Arc::new(RpcClient::new_with_commitment(
        rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));
    let (db_url, _storage, _pg) = start_postgres("backfill_only_write_fail").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    let user = &env.users[0];

    let (_sig, deposit_slot) =
        execute_deposit(&client, user, &env.instance, &env.mint, DEPOSIT_AMOUNT_A).await?;
    wait_for_finalized_slot(&rpc_url, deposit_slot + 3).await;
    wait_for_block_available(&rpc_url, deposit_slot).await;

    // Anchored at the deposit for the same reason as the test above: a range reaching back
    // to the validator's first slots fails on unreadable blocks before it reaches this one.
    let start_slot = deposit_slot;

    // Reject every insert. NOT VALID skips the scan of existing rows, and the schema
    // setup the run performs on boot leaves it in place because each statement is
    // guarded and its one data statement touches no rows on an empty table.
    let pool: PgPool = db::connect(&db_url).await?;
    sqlx::query(
        "ALTER TABLE transactions ADD CONSTRAINT reject_all_inserts CHECK (false) NOT VALID",
    )
    .execute(&pool)
    .await?;
    pool.close().await;

    let (common, indexer) =
        backfill_only_config(rpc_url.clone(), db_url.clone(), env.instance, start_slot);
    let result = run_backfill_only_once(common, indexer).await;

    match result {
        Err(IndexerError::Storage(_)) => {}
        other => panic!(
            "a slot whose rows cannot be written must surface the storage error rather \
             than a channel or completeness error, got: {other:?}"
        ),
    }

    let pool: PgPool = db::connect(&db_url).await?;
    let checkpoint = db::get_checkpoint_slot(&pool, "escrow").await?;
    assert!(
        checkpoint.is_none_or(|slot| slot < deposit_slot),
        "checkpoint {checkpoint:?} must stay below the unwritten deposit slot \
         {deposit_slot} so a retry replays it"
    );

    Ok(())
}

/// A repair whose start slot sits above the durable checkpoint must refuse.
///
/// The fill would cover only the range above that slot while the gated writer, seeded at
/// the same floor, walked its frontier to the target. The run would exit clean having
/// committed a checkpoint over slots nothing ever fetched, and because the next run reads
/// that higher checkpoint as its floor, the skipped band would never be offered again.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_only_refuses_a_start_slot_above_the_checkpoint(
) -> Result<(), Box<dyn std::error::Error>> {
    let (validator, faucet) = start_test_validator_no_geyser().await;
    let rpc_url = validator.rpc_url();
    let client = Arc::new(RpcClient::new_with_commitment(
        rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));
    let (db_url, _storage, _pg) = start_postgres("backfill_only_start_slot_guard").await?;

    let env = TestEnvironment::setup(&client, &faucet, 1, USER_BALANCE, None).await?;
    let user = &env.users[0];

    let (_sig, deposit_slot) =
        execute_deposit(&client, user, &env.instance, &env.mint, DEPOSIT_AMOUNT_A).await?;
    wait_for_finalized_slot(&rpc_url, deposit_slot + 3).await;
    wait_for_block_available(&rpc_url, deposit_slot).await;

    // Establish the checkpoint through the real repair path rather than seeding a row, so
    // the guard is tested against a checkpoint this pipeline actually wrote.
    let (common, indexer) =
        backfill_only_config(rpc_url.clone(), db_url.clone(), env.instance, deposit_slot);
    run_backfill_only_once(common, indexer)
        .await
        .expect("the first repair must succeed and leave a checkpoint");

    let pool: PgPool = db::connect(&db_url).await?;
    let committed = db::get_checkpoint_slot(&pool, "escrow")
        .await?
        .expect("the first run must leave a durable checkpoint");

    // Well clear of the tip, so the skipped band is unambiguous.
    let (common, indexer) = backfill_only_config(
        rpc_url.clone(),
        db_url.clone(),
        env.instance,
        committed + 10_000,
    );
    let err = run_backfill_only_once(common, indexer)
        .await
        .expect_err("a start slot above the checkpoint must refuse");

    match err {
        IndexerError::Checkpoint(CheckpointError::StartSlotAheadOfCheckpoint {
            setting,
            start_slot,
            checkpoint,
            ..
        }) => {
            assert_eq!(setting, "indexer.backfill.start_slot");
            assert_eq!(start_slot, committed + 10_000);
            assert_eq!(checkpoint, committed);
        }
        other => panic!("expected a start-slot refusal, got: {other:?}"),
    }

    // The refusal is only worth anything if it committed nothing on the way out.
    let after = db::get_checkpoint_slot(&pool, "escrow").await?;
    assert_eq!(
        after,
        Some(committed),
        "a refused run must leave the checkpoint untouched, got {after:?}"
    );

    Ok(())
}
