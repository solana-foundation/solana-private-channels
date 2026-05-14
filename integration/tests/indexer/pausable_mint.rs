//! Integration test for the Token-2022 PausableConfig pre-flight on the
//! withdrawal operator.
//!
//! Scenario:
//! 1. Create a Token-2022 mint with the PausableConfig extension initialized
//!    (unpaused at creation), AllowMint it on the escrow instance, and fund
//!    the escrow ATA so a withdrawal has tokens to release.
//! 2. Pause the mint on-chain.
//! 3. Seed a `DbMint` row with `is_pausable = None` (the state the indexer
//!    leaves behind at AllowMint time) and a pending `DbTransaction`
//!    withdrawal at nonce 0.
//! 4. Start the PrivateChannel→Solana withdrawal operator.
//! 5. Assert the operator routes the withdrawal to `manual_review`: the row
//!    status flips from `pending` to `manual_review`, and `mints.is_pausable`
//!    flips from `None` to `Some(true)` (lazy RPC resolution + write-back).
//!    No tokens are released to the recipient. The webhook alert payload is
//!    covered by unit tests in `db_transaction_writer`; here we just assert
//!    the terminal DB state the operator bailed into.

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
use spl_token_2022::extension::{pausable, ExtensionType};
use spl_token_2022::state::Mint as Token2022Mint;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use std::time::Duration;
use test_utils::operator_helper::start_private_channel_to_solana_operator;
use test_utils::validator_helper::start_test_validator_no_geyser;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Local helpers — Token-2022 mint with PausableConfig
// ---------------------------------------------------------------------------

/// Create a Token-2022 mint on-chain with the PausableConfig extension.
/// `authority` is the mint authority AND the pause authority.
async fn generate_pausable_mint_2022(
    client: &RpcClient,
    payer: &Keypair,
    authority: &Keypair,
    mint: &Keypair,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let space =
        ExtensionType::try_calculate_account_len::<Token2022Mint>(&[ExtensionType::Pausable])?;
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
        pausable::instruction::initialize(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            &authority.pubkey(),
        )?,
        spl_token_2022::instruction::initialize_mint2(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            &authority.pubkey(),
            Some(&authority.pubkey()),
            6,
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

async fn set_mint_paused(
    client: &RpcClient,
    payer: &Keypair,
    authority: &Keypair,
    mint: &Pubkey,
    paused: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let ix = if paused {
        pausable::instruction::pause(&TOKEN_2022_PROGRAM_ID, mint, &authority.pubkey(), &[])?
    } else {
        pausable::instruction::resume(&TOKEN_2022_PROGRAM_ID, mint, &authority.pubkey(), &[])?
    };

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&payer.pubkey()),
        &[payer, authority],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;
    Ok(())
}

/// Allow a mint on the escrow instance, binding it to `token_program`. Unlike
/// the shared `TestEnvironment::setup` which hardcodes SPL Token, this one
/// accepts any token program — needed for Token-2022.
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
async fn test_withdrawal_routed_to_manual_review_when_pausable_mint_is_paused(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Pausable Mint: Withdrawal → ManualReview While Paused ===");

    set_operator_env_vars();

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name("pausable_mint")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!(
        "postgres://postgres:password@{}:{}/pausable_mint",
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

    let (_, instance_pda) = TestEnvironment::setup_instance(&client, &faucet_keypair, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, instance_pda).await?;

    // Pausable Token-2022 mint. Admin is both mint and pause authority.
    let mint_keypair = Keypair::new();
    let mint_pubkey = generate_pausable_mint_2022(&client, &admin, &admin, &mint_keypair).await?;
    println!("Created pausable Token-2022 mint {}", mint_pubkey);

    // AllowMint on the escrow — this would fail before our program-side change.
    allow_mint_for_program(
        &client,
        &admin,
        instance_pda,
        mint_pubkey,
        TOKEN_2022_PROGRAM_ID,
    )
    .await?;
    println!("AllowMint succeeded for pausable mint");

    // Fund the escrow ATA directly. Sidesteps the deposit path — only the
    // escrow ATA balance matters for release_funds.
    let withdraw_amount: u64 = 50_000;
    mint_2022_to_owner(
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
    // by minting zero tokens (ATA idempotent creation + mint_to amount=0).
    mint_2022_to_owner(&client, &admin, mint_pubkey, recipient.pubkey(), &admin, 0).await?;

    // Pause the mint BEFORE the operator sees the withdrawal.
    set_mint_paused(&client, &admin, &admin, &mint_pubkey, true).await?;
    println!("Mint paused on-chain");

    // Seed DB: mints row with is_pausable=None (what the indexer would write
    // at AllowMint time), and a pending withdrawal at nonce 0.
    let mint_meta = DbMint::new(
        mint_pubkey.to_string(),
        6,
        TOKEN_2022_PROGRAM_ID.to_string(),
    );
    storage.upsert_mints_batch(&[mint_meta]).await?;
    assert!(
        storage
            .get_mint(&mint_pubkey.to_string())
            .await?
            .expect("mints row")
            .is_pausable
            .is_none(),
        "pre-condition: DB mints row should have is_pausable = None",
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

    // Mint is paused → pre-flight must route the withdrawal to manual_review
    // (terminal status; no self-recovery on unpause).
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
        "paused mint should route the withdrawal to manual_review",
    );

    let stored_mint = storage
        .get_mint(&mint_pubkey.to_string())
        .await?
        .expect("mints row");
    assert_eq!(
        stored_mint.is_pausable,
        Some(true),
        "operator should have resolved is_pausable via RPC and written it back",
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
