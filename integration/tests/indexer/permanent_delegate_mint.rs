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
use private_channel_escrow_program_client::{
    instructions::AllowMintBuilder, PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
};
use private_channel_indexer::storage::common::models::{
    DbMint, DbTransaction, TransactionStatus, TransactionType,
};
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::PostgresConfig;
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment, TEST_ADMIN_KEYPAIR};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::transaction::Transaction;
use solana_system_interface::{instruction::create_account, program::ID as SYSTEM_PROGRAM_ID};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token_2022::extension::ExtensionType;
use spl_token_2022::state::Mint as Token2022Mint;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use std::time::Duration;
use test_utils::operator_helper::start_private_channel_to_solana_operator;
use test_utils::validator_helper::start_test_validator_no_geyser;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

const MINT_DECIMALS: u8 = 6;

// ---------------------------------------------------------------------------
// Local helpers — Token-2022 mint with PermanentDelegate + delegate-driven drain
// ---------------------------------------------------------------------------

/// Create a Token-2022 mint on-chain with the PermanentDelegate extension.
/// The returned mint has `delegate` as its permanent delegate; the delegate
/// can transfer out of any ATA for this mint without the owner's consent.
async fn generate_permanent_delegate_mint_2022(
    client: &RpcClient,
    payer: &Keypair,
    authority: &Keypair,
    delegate: &Pubkey,
    mint: &Keypair,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(&[
        ExtensionType::PermanentDelegate,
    ])?;
    let rent = client.get_minimum_balance_for_rent_exemption(space).await?;

    // Three-instruction sequence: allocate, init extension, init mint.
    // Extensions must be initialized BEFORE the mint itself.
    let ixs = vec![
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
            MINT_DECIMALS,
        )?,
    ];

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &ixs,
        Some(&payer.pubkey()),
        &[payer, mint],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;

    Ok(mint.pubkey())
}

/// Mint Token-2022 tokens to `owner`, creating their ATA if needed.
async fn mint_2022_to_owner(
    client: &RpcClient,
    payer: &Keypair,
    mint: Pubkey,
    owner: Pubkey,
    authority: &Keypair,
    amount: u64,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(&owner, &mint, &TOKEN_2022_PROGRAM_ID);

    let ixs = vec![
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

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &ixs,
        Some(&payer.pubkey()),
        &[payer, authority],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;

    Ok(ata)
}

/// Use the permanent delegate to move tokens out of `source_ata` into a
/// fresh ATA owned by `drain_owner`. Simulates the attack the pre-flight
/// is designed to catch: the escrow program is never invoked, so no
/// PrivateChannel-side event ever reaches the indexer.
async fn drain_via_permanent_delegate(
    client: &RpcClient,
    payer: &Keypair,
    mint: Pubkey,
    source_ata: Pubkey,
    delegate: &Keypair,
    drain_owner: Pubkey,
    amount: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let drain_ata =
        get_associated_token_address_with_program_id(&drain_owner, &mint, &TOKEN_2022_PROGRAM_ID);

    let ixs = vec![
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
            MINT_DECIMALS,
        )?,
    ];

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &ixs,
        Some(&payer.pubkey()),
        &[payer, delegate],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;
    Ok(())
}

/// Allow a mint on the escrow instance, binding it to `token_program`. The
/// shared `TestEnvironment::setup` hardcodes SPL Token, so we replicate the
/// AllowMint call here against Token-2022.
async fn allow_mint_for_program(
    client: &RpcClient,
    admin: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
    token_program: Pubkey,
) -> Result<(), Box<dyn std::error::Error>> {
    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &token_program);

    let ix = AllowMintBuilder::new()
        .payer(admin.pubkey())
        .admin(admin.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(token_program)
        .associated_token_program(spl_associated_token_account::ID)
        .event_authority(event_authority_pda)
        .private_channel_escrow_program(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        .bump(bump)
        .instruction();

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&admin.pubkey()),
        &[admin],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;
    Ok(())
}

fn make_withdrawal_transaction(
    signature: String,
    mint: String,
    recipient: String,
    amount: i64,
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
        amount,
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(nonce),
        status: TransactionStatus::Pending,
        created_at: now,
        updated_at: now,
        processed_at: None,
        counterpart_signature: None,
        remint_signatures: None,
        pending_remint_deadline_at: None,
    }
}

async fn get_token_2022_balance(
    client: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(owner, mint, &TOKEN_2022_PROGRAM_ID);
    match client.get_token_account_balance(&ata).await {
        Ok(bal) => Ok(bal.amount.parse::<u64>()?),
        // ATA may not exist yet before release-funds fires it into existence.
        Err(_) => Ok(0),
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
        withdraw_amount as i64,
        0,
    );
    storage.insert_db_transaction(&withdrawal_tx).await?;

    // Start the withdraw operator.
    let operator_handle = start_private_channel_to_solana_operator(
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

/// Defensive coverage for the missing-ATA branch in the withdrawal pre-flight:
/// when the escrow ATA does not exist on-chain, the operator must treat the
/// query as on-chain balance = 0 and route the withdrawal to ManualReview.
/// Mapping the not-found error to a transient RPC failure instead would
/// restart the operator in a loop on a condition that won't heal.
///
/// We skip `AllowMint` to set up the missing-ATA state — it's the simplest
/// way to leave the canonical escrow ATA address unfunded. The pre-flight
/// reads on-chain state at query time and doesn't care how we got there.
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_routed_to_manual_review_when_escrow_ata_does_not_exist(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Permanent Delegate: Withdrawal → ManualReview When Escrow ATA Missing ===");

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
    )
    .await?;

    // Skip AllowMint so the escrow ATA is never created on-chain.
    let escrow_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint_pubkey,
        &TOKEN_2022_PROGRAM_ID,
    );
    assert!(
        client.get_account(&escrow_ata).await.is_err(),
        "pre-condition: escrow ATA must not exist on-chain",
    );

    // Seed DB: mints row with has_permanent_delegate=None, pending withdrawal.
    let mint_meta = DbMint::new(
        mint_pubkey.to_string(),
        MINT_DECIMALS as i16,
        TOKEN_2022_PROGRAM_ID.to_string(),
    );
    storage.upsert_mints_batch(&[mint_meta]).await?;

    let withdraw_amount: u64 = 50_000;
    let withdrawal_sig = Signature::new_unique().to_string();
    let withdrawal_tx = make_withdrawal_transaction(
        withdrawal_sig.clone(),
        mint_pubkey.to_string(),
        recipient.pubkey().to_string(),
        withdraw_amount as i64,
        0,
    );
    storage.insert_db_transaction(&withdrawal_tx).await?;

    let operator_handle = start_private_channel_to_solana_operator(
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
        "missing escrow ATA should route the withdrawal to manual_review, not loop the operator",
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
