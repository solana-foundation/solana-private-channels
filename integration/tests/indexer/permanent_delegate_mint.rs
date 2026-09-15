//! Integration test for the Token-2022 PermanentDelegate pre-flight on the
//! withdrawal operator.
//!
//! Scenario — the attack the pre-flight exists to block:
//! 1. Create a Token-2022 mint with the PermanentDelegate extension; the
//!    delegate is a keypair we control. AllowMint it on the escrow instance.
//! 2. Fund the escrow ATA with 2x the withdrawal amount via `mint_to`. This
//!    is the only way the escrow balance gets bumped in this test — sidesteps
//!    the deposit path so the operator has no PrivateChannel-side event for the
//!    drain that follows.
//! 3. Use the permanent delegate to drain the escrow ATA below the withdrawal
//!    amount. The escrow program is never invoked, so the indexer sees
//!    nothing and the DB's implied balance (still 2x) diverges from on-chain.
//! 4. Seed the DB: `mints` row with `has_permanent_delegate = None` (what the
//!    indexer writes at AllowMint time) and a pending withdrawal for the full
//!    amount at nonce 0.
//! 5. Start the PrivateChannel→Solana withdrawal operator.
//! 6. Assert the operator routes the withdrawal to `manual_review`: the row
//!    status flips from `pending` to `manual_review`, `has_permanent_delegate`
//!    flips from `None` to `Some(true)` (lazy RPC resolution + write-back),
//!    and no tokens reach the recipient. Webhook firing is covered by the
//!    db_transaction_writer unit tests.

#[path = "helpers/mod.rs"]
mod helpers;

#[allow(dead_code)]
#[path = "setup.rs"]
mod setup;

use chrono::Utc;
use helpers::db;
use helpers::{
    drain_via_permanent_delegate, generate_permanent_delegate_mint_2022, get_token_2022_balance,
    mint_2022_to_owner,
};
use private_channel_indexer::storage::common::amount::TokenAmount;
use private_channel_indexer::storage::common::models::{
    DbMint, DbMintStatus, DbTransaction, TransactionStatus, TransactionType,
};
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::PostgresConfig;
use setup::{allow_mint_for_program, TestEnvironment, TEST_ADMIN_KEYPAIR};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::transaction::Transaction;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use std::time::Duration;
use test_utils::operator_helper::start_private_channel_to_solana_operator;
use test_utils::validator_helper::start_test_validator_no_geyser;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

const MINT_DECIMALS: u8 = 6;

fn make_withdrawal_transaction(
    signature: String,
    mint: String,
    recipient: String,
    amount: u64,
    nonce: i64,
) -> DbTransaction {
    let now = Utc::now();
    DbTransaction {
        id: 0,
        signature,
        trace_id: Uuid::new_v4().to_string(),
        slot: 1,
        initiator: recipient.clone(),
        recipient,
        mint,
        amount: TokenAmount(amount),
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(nonce),
        status: TransactionStatus::Pending,
        created_at: now,
        updated_at: now,
        processed_at: None,
        counterpart_signature: None,
        remint_signatures: None,
        remint_last_valid_block_heights: None,
        pending_remint_deadline_at: None,
        finality_check_attempts: 0,
        recovery_requeue_attempts: 0,
        instruction_index: 0,
        inner_index: None,
        landed_remint_signature: None,
        release_refused_on_chain: false,
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_routed_to_manual_review_when_permanent_delegate_drained_escrow(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Permanent Delegate: Withdrawal → ManualReview When Escrow Drained ===");

    set_operator_env_vars();

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("permanent_delegate_mint")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/permanent_delegate_mint",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    // Instance + operator (reuses shared setup — pure escrow state, no mint).
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let recipient = Keypair::new();
    let delegate = Keypair::new();
    let drainer = Keypair::new();

    let (_, instance_pda) = TestEnvironment::setup_instance(&client, &faucet_keypair, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, instance_pda).await?;

    // Fund the delegate so it can pay tx fees when draining.
    let fund_delegate_ix = solana_system_interface::instruction::transfer(
        &faucet_keypair.pubkey(),
        &delegate.pubkey(),
        1_000_000_000,
    );
    let bh = client.get_latest_blockhash().await?;
    let fund_tx = Transaction::new_signed_with_payer(
        &[fund_delegate_ix],
        Some(&faucet_keypair.pubkey()),
        &[&faucet_keypair],
        bh,
    );
    client.send_and_confirm_transaction(&fund_tx).await?;

    // Token-2022 mint with PermanentDelegate. Admin is mint authority; a
    // separate keypair holds the permanent-delegate authority.
    let mint_keypair = Keypair::new();
    let mint_pubkey = generate_permanent_delegate_mint_2022(
        &client,
        &admin,
        &admin,
        &delegate.pubkey(),
        &mint_keypair,
        MINT_DECIMALS,
    )
    .await?;
    println!("Created permanent-delegate Token-2022 mint {}", mint_pubkey);

    // AllowMint on the escrow instance — accepted post-change.
    allow_mint_for_program(
        &client,
        &admin,
        instance_pda,
        mint_pubkey,
        TOKEN_2022_PROGRAM_ID,
    )
    .await?;
    println!("AllowMint succeeded for permanent-delegate mint");

    // Fund the escrow ATA with 2x the withdrawal amount. Sidesteps the
    // deposit path so no PrivateChannel-side deposit event is ever produced.
    let withdraw_amount: u64 = 50_000;
    let escrow_ata = mint_2022_to_owner(
        &client,
        &admin,
        mint_pubkey,
        instance_pda,
        &admin,
        withdraw_amount * 2,
    )
    .await?;

    // release_funds requires the recipient ATA to already exist — the escrow
    // program's `validate_ata` rejects empty-data ATAs. Pre-create it here
    // by minting zero tokens to it.
    mint_2022_to_owner(&client, &admin, mint_pubkey, recipient.pubkey(), &admin, 0).await?;

    // Drain the escrow ATA below the withdrawal amount using the permanent
    // delegate. The escrow program is never invoked; the indexer sees
    // nothing; the DB's derived balance remains at 2x the amount.
    let drain_amount = withdraw_amount * 2 - (withdraw_amount / 2); // leave 25k, need 50k
    drain_via_permanent_delegate(
        &client,
        &admin,
        mint_pubkey,
        escrow_ata,
        &delegate,
        drainer.pubkey(),
        drain_amount,
        MINT_DECIMALS,
    )
    .await?;
    println!(
        "Permanent delegate drained {} tokens from the escrow ATA",
        drain_amount
    );

    // Seed DB: mints row with has_permanent_delegate=None (what the indexer
    // writes at AllowMint time), and a pending withdrawal at nonce 0.
    let mint_meta = DbMint::new(
        mint_pubkey.to_string(),
        MINT_DECIMALS as i16,
        TOKEN_2022_PROGRAM_ID.to_string(),
    );
    storage.upsert_mints_batch(&[mint_meta]).await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            mint_address: mint_pubkey.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{mint_pubkey}"),
            created_at: Utc::now(),
        }])
        .await?;
    let pre = storage
        .get_mint(&mint_pubkey.to_string())
        .await?
        .expect("mints row");
    assert!(
        pre.has_permanent_delegate.is_none(),
        "pre-condition: DB mints row should have has_permanent_delegate = None",
    );

    let withdrawal_sig = Signature::new_unique().to_string();
    let withdrawal_tx = make_withdrawal_transaction(
        withdrawal_sig.clone(),
        mint_pubkey.to_string(),
        recipient.pubkey().to_string(),
        withdraw_amount,
        0,
    );
    storage.insert_db_transaction(&withdrawal_tx).await?;

    // Start the withdraw operator.
    let operator_handle = start_private_channel_to_solana_operator(
        test_validator.rpc_url(),
        test_validator.rpc_url(),
        db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        instance_pda,
    )
    .await?;

    // On-chain balance < withdrawal amount → pre-flight must route to
    // manual_review (terminal; no self-recovery).
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(tx) = db::get_transaction(&pool, &withdrawal_sig).await? {
            if tx.status == "manual_review" {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "withdrawal {} did not reach manual_review within 60s",
                withdrawal_sig
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let row = db::get_transaction(&pool, &withdrawal_sig)
        .await?
        .expect("withdrawal row should still exist");
    assert_eq!(
        row.status, "manual_review",
        "drained escrow should route the withdrawal to manual_review",
    );

    let stored_mint = storage
        .get_mint(&mint_pubkey.to_string())
        .await?
        .expect("mints row");
    assert_eq!(
        stored_mint.has_permanent_delegate,
        Some(true),
        "operator should have resolved has_permanent_delegate via RPC and written it back",
    );

    let recipient_balance =
        get_token_2022_balance(&client, &recipient.pubkey(), &mint_pubkey).await?;
    assert_eq!(
        recipient_balance, 0,
        "recipient ATA should be empty — no release_funds should have landed",
    );

    operator_handle.shutdown().await;
    Ok(())
}

fn set_operator_env_vars() {
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).expect("valid admin keypair");
    let private_key_base58 = bs58::encode(admin.to_bytes()).into_string();
    std::env::set_var("ADMIN_SIGNER", "memory");
    std::env::set_var("ADMIN_PRIVATE_KEY", &private_key_base58);
    std::env::set_var("OPERATOR_SIGNER", "memory");
    std::env::set_var("OPERATOR_PRIVATE_KEY", &private_key_base58);
}

/// Coverage for the escrow-balance branch of the withdrawal pre-flight: an
/// allowlisted permanent-delegate mint whose escrow ATA holds less than the
/// withdrawal amount must route to ManualReview, not restart the operator.
///
/// `AllowMint` creates the escrow ATA alongside the AllowedMint account, so the
/// ATA starts empty and every withdrawal reads a zero balance. Skipping AllowMint
/// would instead park the row at the mint gate before this branch is reached.
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_routed_to_manual_review_when_escrow_ata_is_empty(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Permanent Delegate: Withdrawal to ManualReview When Escrow ATA Empty ===");

    set_operator_env_vars();

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("permanent_delegate_mint_missing_ata")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/permanent_delegate_mint_missing_ata",
        pg_host, pg_port
    );

    let pool = db::connect(&db_url).await?;
    let storage = Storage::Postgres(
        PostgresDb::new(&PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 10,
        })
        .await?,
    );
    storage.init_schema().await?;

    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;
    let recipient = Keypair::new();
    let delegate = Keypair::new();

    let (_, instance_pda) = TestEnvironment::setup_instance(&client, &faucet_keypair, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, instance_pda).await?;

    let mint_keypair = Keypair::new();
    let mint_pubkey = generate_permanent_delegate_mint_2022(
        &client,
        &admin,
        &admin,
        &delegate.pubkey(),
        &mint_keypair,
        MINT_DECIMALS,
    )
    .await?;

    // AllowMint creates the escrow ATA and the AllowedMint account the gate reads.
    // Nothing funds the ATA afterwards, so the pre-flight sees a zero balance.
    allow_mint_for_program(
        &client,
        &admin,
        instance_pda,
        mint_pubkey,
        TOKEN_2022_PROGRAM_ID,
    )
    .await?;

    let escrow_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint_pubkey,
        &TOKEN_2022_PROGRAM_ID,
    );
    let escrow_balance = client.get_token_account_balance(&escrow_ata).await?;
    assert_eq!(
        escrow_balance.amount, "0",
        "pre-condition: escrow ATA must exist and be empty",
    );

    // Seed DB: mints row with has_permanent_delegate=None, pending withdrawal.
    let mint_meta = DbMint::new(
        mint_pubkey.to_string(),
        MINT_DECIMALS as i16,
        TOKEN_2022_PROGRAM_ID.to_string(),
    );
    storage.upsert_mints_batch(&[mint_meta]).await?;
    storage
        .insert_mint_statuses_batch(&[DbMintStatus {
            mint_address: mint_pubkey.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{mint_pubkey}"),
            created_at: Utc::now(),
        }])
        .await?;

    let withdraw_amount: u64 = 50_000;
    let withdrawal_sig = Signature::new_unique().to_string();
    let withdrawal_tx = make_withdrawal_transaction(
        withdrawal_sig.clone(),
        mint_pubkey.to_string(),
        recipient.pubkey().to_string(),
        withdraw_amount,
        0,
    );
    storage.insert_db_transaction(&withdrawal_tx).await?;

    let operator_handle = start_private_channel_to_solana_operator(
        test_validator.rpc_url(),
        test_validator.rpc_url(),
        db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        instance_pda,
    )
    .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(tx) = db::get_transaction(&pool, &withdrawal_sig).await? {
            if tx.status == "manual_review" {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "withdrawal {} did not reach manual_review within 60s",
                withdrawal_sig
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let row = db::get_transaction(&pool, &withdrawal_sig)
        .await?
        .expect("withdrawal row should still exist");
    assert_eq!(
        row.status, "manual_review",
        "an empty escrow ATA should route the withdrawal to manual_review, not loop the operator",
    );

    let stored_mint = storage
        .get_mint(&mint_pubkey.to_string())
        .await?
        .expect("mints row");
    assert_eq!(
        stored_mint.has_permanent_delegate,
        Some(true),
        "operator should have resolved has_permanent_delegate via RPC and written it back",
    );

    let recipient_balance =
        get_token_2022_balance(&client, &recipient.pubkey(), &mint_pubkey).await?;
    assert_eq!(
        recipient_balance, 0,
        "recipient ATA should be empty — no release_funds should have landed",
    );

    operator_handle.shutdown().await;
    Ok(())
}
