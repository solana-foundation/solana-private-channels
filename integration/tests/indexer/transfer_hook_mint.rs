//! Integration test for transfer-hook mints on the withdrawal operator.
//!
//! The escrow program forwards a release's trailing accounts to Token-2022,
//! which resolves the mint's `ExtraAccountMetaList` and invokes the hook. The
//! operator is the client for those releases, so it has to resolve those
//! accounts itself. LiteSVM covers the program side; only a real node exercises
//! the operator's resolution against real account data.
//!
//! Two scenarios:
//! 1. A hook mint with a valid `ExtraAccountMetaList` releases normally. The
//!    release can only land if the operator resolved and attached the hook
//!    accounts, since Token-2022 rejects the transfer without them.
//! 2. A hook mint whose validation account was never created parks the row.
//!    Nothing can ever resolve it, so the operator must not spin on it.

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
use private_channel_indexer::storage::common::amount::TokenAmount;
use private_channel_indexer::storage::common::models::{
    DbMint, DbMintStatus, DbTransaction, TransactionStatus, TransactionType,
};
use private_channel_indexer::storage::{PostgresDb, Storage};
use private_channel_indexer::PostgresConfig;
use setup::{find_allowed_mint_pda, find_event_authority_pda, TestEnvironment, TEST_ADMIN_KEYPAIR};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::transaction::Transaction;
use solana_system_interface::{instruction::create_account, program::ID as SYSTEM_PROGRAM_ID};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_tlv_account_resolution::{account::ExtraAccountMeta, state::ExtraAccountMetaList};
use spl_token_2022::extension::{transfer_hook, ExtensionType};
use spl_token_2022::state::Mint as Token2022Mint;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use spl_transfer_hook_interface::{
    get_extra_account_metas_address, instruction::ExecuteInstruction,
};
use std::time::Duration;
use test_utils::operator_helper::start_private_channel_to_solana_operator;
use test_utils::validator_helper::{start_test_validator_no_geyser, HOOK_FIXTURE_PROGRAM_ID};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

/// Selects the fixture's `ExtraAccountMetaList` initializer. Must match
/// `INIT_EXTRA_ACCOUNT_METAS_TAG` in the fixture.
const INIT_EXTRA_ACCOUNT_METAS_TAG: u8 = 0;

const WITHDRAW_AMOUNT: u64 = 50_000;

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// Create a Token-2022 mint whose transfers run the hook fixture.
async fn generate_hook_mint_2022(
    client: &RpcClient,
    payer: &Keypair,
    authority: &Keypair,
    mint: &Keypair,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let space =
        ExtensionType::try_calculate_account_len::<Token2022Mint>(&[ExtensionType::TransferHook])?;
    let rent = client.get_minimum_balance_for_rent_exemption(space).await?;

    // Extensions must be initialized before the mint itself.
    let ixs = vec![
        create_account(
            &payer.pubkey(),
            &mint.pubkey(),
            rent,
            space as u64,
            &TOKEN_2022_PROGRAM_ID,
        ),
        transfer_hook::instruction::initialize(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            Some(authority.pubkey()),
            Some(HOOK_FIXTURE_PROGRAM_ID),
        )?,
        spl_token_2022::instruction::initialize_mint2(
            &TOKEN_2022_PROGRAM_ID,
            &mint.pubkey(),
            &authority.pubkey(),
            None,
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

/// Create the mint's validation account, declaring one extra account (the
/// system program). Serialized here and copied in by the fixture, because the
/// address is a PDA of the fixture and nothing else can create it.
async fn init_extra_account_meta_list(
    client: &RpcClient,
    payer: &Keypair,
    mint: &Pubkey,
) -> Result<Pubkey, Box<dyn std::error::Error>> {
    let extras = [ExtraAccountMeta::new_with_pubkey(
        &SYSTEM_PROGRAM_ID,
        false,
        false,
    )?];
    let mut metas = vec![0u8; ExtraAccountMetaList::size_of(extras.len())?];
    ExtraAccountMetaList::init::<ExecuteInstruction>(&mut metas, &extras)?;

    let mut data = vec![INIT_EXTRA_ACCOUNT_METAS_TAG];
    data.extend_from_slice(&metas);

    let validation_pda = get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID);
    let ix = Instruction {
        program_id: HOOK_FIXTURE_PROGRAM_ID,
        accounts: vec![
            AccountMeta::new(payer.pubkey(), true),
            AccountMeta::new(validation_pda, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(SYSTEM_PROGRAM_ID, false),
        ],
        data,
    };

    let recent_blockhash = client.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    );
    client.send_and_confirm_transaction(&tx).await?;

    Ok(validation_pda)
}

/// Mint Token-2022 tokens to `owner`, creating their ATA if needed. `MintTo` is
/// not a transfer, so it runs no hook and needs no extra accounts.
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

/// Allow a mint bound to Token-2022. The shared setup hardcodes SPL Token.
async fn allow_mint_2022(
    client: &RpcClient,
    admin: &Keypair,
    instance: Pubkey,
    mint: Pubkey,
) -> Result<(), Box<dyn std::error::Error>> {
    let (allowed_mint_pda, bump) = find_allowed_mint_pda(&instance, &mint);
    let (event_authority_pda, _) = find_event_authority_pda();
    let instance_ata =
        get_associated_token_address_with_program_id(&instance, &mint, &TOKEN_2022_PROGRAM_ID);

    let ix = AllowMintBuilder::new()
        .payer(admin.pubkey())
        .admin(admin.pubkey())
        .instance(instance)
        .mint(mint)
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .system_program(SYSTEM_PROGRAM_ID)
        .token_program(TOKEN_2022_PROGRAM_ID)
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

async fn get_token_2022_balance(
    client: &RpcClient,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<u64, Box<dyn std::error::Error>> {
    let ata = get_associated_token_address_with_program_id(owner, mint, &TOKEN_2022_PROGRAM_ID);
    match client.get_token_account_balance(&ata).await {
        Ok(balance) => Ok(balance.amount.parse::<u64>()?),
        Err(_) => Ok(0),
    }
}

fn set_operator_env_vars() {
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..]).expect("valid admin keypair");
    let private_key_base58 = bs58::encode(admin.to_bytes()).into_string();
    std::env::set_var("ADMIN_SIGNER", "memory");
    std::env::set_var("ADMIN_PRIVATE_KEY", &private_key_base58);
    std::env::set_var("OPERATOR_SIGNER", "memory");
    std::env::set_var("OPERATOR_PRIVATE_KEY", &private_key_base58);
}

/// Everything both scenarios share: validator, Postgres, escrow instance,
/// operator, a hook mint, a funded escrow ATA and a pre-created recipient ATA.
/// Returns what the assertions need.
struct HookMintEnv {
    test_validator: solana_test_validator::TestValidator,
    client: RpcClient,
    pool: sqlx::PgPool,
    db_url: String,
    storage: Storage,
    instance_pda: Pubkey,
    mint_pubkey: Pubkey,
    recipient: Keypair,
    _pg_container: testcontainers::ContainerAsync<Postgres>,
}

async fn setup_hook_mint_env(db_name: &str) -> Result<HookMintEnv, Box<dyn std::error::Error>> {
    set_operator_env_vars();

    let (test_validator, faucet_keypair) = start_test_validator_no_geyser().await;
    let client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::confirmed());

    let pg_container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await?;
    let pg_host = pg_container.get_host().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let db_url = format!("postgres://postgres:password@{pg_host}:{pg_port}/{db_name}");

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

    let (_, instance_pda) = TestEnvironment::setup_instance(&client, &faucet_keypair, None).await?;
    TestEnvironment::setup_operator(&client, &faucet_keypair, instance_pda).await?;

    let mint_keypair = Keypair::new();
    let mint_pubkey = generate_hook_mint_2022(&client, &admin, &admin, &mint_keypair).await?;
    println!("Created transfer-hook Token-2022 mint {mint_pubkey}");

    allow_mint_2022(&client, &admin, instance_pda, mint_pubkey).await?;

    // Fund the escrow directly: only its balance matters to release_funds.
    mint_2022_to_owner(
        &client,
        &admin,
        mint_pubkey,
        instance_pda,
        &admin,
        WITHDRAW_AMOUNT * 2,
    )
    .await?;

    // release_funds requires an initialized recipient ATA.
    mint_2022_to_owner(&client, &admin, mint_pubkey, recipient.pubkey(), &admin, 0).await?;

    let mint_meta = DbMint::new(
        mint_pubkey.to_string(),
        6,
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

    Ok(HookMintEnv {
        test_validator,
        client,
        pool,
        db_url,
        storage,
        instance_pda,
        mint_pubkey,
        recipient,
        _pg_container: pg_container,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The release lands only if the operator resolved the mint's
/// `ExtraAccountMetaList` and appended the hook accounts: Token-2022 rejects the
/// transfer when any of them is missing.
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_of_transfer_hook_mint_releases() -> Result<(), Box<dyn std::error::Error>>
{
    println!("=== Transfer-hook Mint: Withdrawal Releases ===");

    let env = setup_hook_mint_env("transfer_hook_mint").await?;
    let admin = Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?;

    let validation_pda =
        init_extra_account_meta_list(&env.client, &admin, &env.mint_pubkey).await?;
    println!("Initialized ExtraAccountMetaList at {validation_pda}");

    let withdrawal_sig = Signature::new_unique().to_string();
    env.storage
        .insert_db_transaction(&make_withdrawal_transaction(
            withdrawal_sig.clone(),
            env.mint_pubkey.to_string(),
            env.recipient.pubkey().to_string(),
            WITHDRAW_AMOUNT,
            0,
        ))
        .await?;

    let operator_handle = start_private_channel_to_solana_operator(
        env.test_validator.rpc_url(),
        env.test_validator.rpc_url(),
        env.db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance_pda,
    )
    .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let balance =
            get_token_2022_balance(&env.client, &env.recipient.pubkey(), &env.mint_pubkey).await?;
        if balance == WITHDRAW_AMOUNT {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let status = db::get_transaction(&env.pool, &withdrawal_sig)
                .await?
                .map(|row| row.status)
                .unwrap_or_else(|| "missing".to_string());
            return Err(format!(
                "hook-mint withdrawal did not release within 90s (row status: {status})"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    operator_handle.shutdown().await;
    Ok(())
}

/// A hook mint whose validation account was never created can never resolve, so
/// the row parks instead of the operator restarting on it forever.
#[tokio::test(flavor = "multi_thread")]
async fn test_withdrawal_parks_when_validation_account_is_missing(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Transfer-hook Mint: Missing Validation Account → ManualReview ===");

    let env = setup_hook_mint_env("transfer_hook_missing_eaml").await?;

    let withdrawal_sig = Signature::new_unique().to_string();
    env.storage
        .insert_db_transaction(&make_withdrawal_transaction(
            withdrawal_sig.clone(),
            env.mint_pubkey.to_string(),
            env.recipient.pubkey().to_string(),
            WITHDRAW_AMOUNT,
            0,
        ))
        .await?;

    let operator_handle = start_private_channel_to_solana_operator(
        env.test_validator.rpc_url(),
        env.test_validator.rpc_url(),
        env.db_url.clone(),
        Keypair::try_from(&TEST_ADMIN_KEYPAIR[..])?,
        env.instance_pda,
    )
    .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(row) = db::get_transaction(&env.pool, &withdrawal_sig).await? {
            if row.status == "manual_review" {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err("withdrawal did not reach manual_review within 90s".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(
        get_token_2022_balance(&env.client, &env.recipient.pubkey(), &env.mint_pubkey).await?,
        0,
        "nothing may be released for an unresolvable hook",
    );

    operator_handle.shutdown().await;
    Ok(())
}
