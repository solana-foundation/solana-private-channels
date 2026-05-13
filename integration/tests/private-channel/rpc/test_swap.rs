//! Channel-side test that the Clock sysvar injected by `crate::vm::clock`
//! is actually visible to BPF programs running in the Contra SVM.
//!
//! Discriminator: `CreateDvp` with `expiry_timestamp = wall_now() - 60`.
//! `CreateDvp`'s validation rejects when `expiry <= now`. With Clock unset
//! (default = 0) the check evaluates `wall_now() - 60 <= 0` → false, so
//! CreateDvp would *succeed*. With `set_clock_now` populating the cache,
//! the check fires and CreateDvp errors with `ExpiryNotInFuture` (custom 5).
//!
//! A second case (future expiry → Create + Fund succeed, escrow holds
//! amount_a) is non-discriminating against the Clock=0 bug but catches
//! unrelated regressions in the Create→Fund path.

use {
    anyhow::Result,
    private_channel_escrow_program_client::{
        instructions::{AllowMintBuilder, DepositBuilder},
        PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    },
    private_channel_indexer::storage::TransactionType,
    solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction},
    solana_transaction_status::UiTransactionEncoding,
    spl_associated_token_account::get_associated_token_address_with_program_id,
    std::time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    tokio::time::sleep,
};

use super::{
    test_context::{PrivateChannelContext, SolanaContext},
    utils::{AIRDROP_LAMPORTS, MINT_DECIMALS},
};
use crate::setup;

const DEPOSIT_A: u64 = 100_000;
const DEPOSIT_B: u64 = 1;
const AMOUNT_A: u64 = 75_000;
const AMOUNT_B: u64 = 50_000;

/// `DvpSwapProgramError::ExpiryNotInFuture` discriminant.
const EXPIRY_NOT_IN_FUTURE_CODE: u32 = 5;

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const POLL_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run_swap_clock_tests(
    private_channel_ctx: &PrivateChannelContext,
    solana_ctx: &SolanaContext,
) {
    println!("\n=== Swap: Clock injection via DvP expiry checks ===");

    let user_a = Keypair::new();
    let user_b = Keypair::new();
    let mint_a_kp = Keypair::new();
    let mint_b_kp = Keypair::new();
    let mint_a = mint_a_kp.pubkey();
    let mint_b = mint_b_kp.pubkey();
    let settlement_authority = private_channel_ctx.operator_key.pubkey();

    bootstrap_user_a(
        private_channel_ctx,
        solana_ctx,
        &user_a,
        &mint_a_kp,
        &mint_b_kp,
    )
    .await
    .unwrap();

    case_past_expiry_blocks_create(
        private_channel_ctx,
        &user_a,
        &user_b,
        &mint_a,
        &mint_b,
        settlement_authority,
    )
    .await;

    case_future_expiry_funds_ok(
        private_channel_ctx,
        &user_a,
        &user_b,
        &mint_a,
        &mint_b,
        settlement_authority,
    )
    .await;

    println!("✓ Clock injection verified end-to-end");
}

async fn case_past_expiry_blocks_create(
    private_channel_ctx: &PrivateChannelContext,
    user_a: &Keypair,
    user_b: &Keypair,
    mint_a: &Pubkey,
    mint_b: &Pubkey,
    settlement_authority: Pubkey,
) {
    let nonce = 1;
    let past_expiry = wall_now_secs() - 60;

    let bh = private_channel_ctx.get_blockhash().await.unwrap();
    let create_tx = setup::create_dvp_transaction(
        user_a,
        user_a.pubkey(),
        user_b.pubkey(),
        mint_a,
        mint_b,
        settlement_authority,
        AMOUNT_A,
        AMOUNT_B,
        past_expiry,
        None,
        nonce,
        bh,
    );
    let create_sig = private_channel_ctx
        .send_transaction(&create_tx)
        .await
        .unwrap();
    assert_tx_failed_with_custom(
        private_channel_ctx,
        &create_sig.to_string(),
        EXPIRY_NOT_IN_FUTURE_CODE,
    )
    .await;
    println!("  ✓ past-expiry CreateDvp failed with ExpiryNotInFuture");
}

async fn case_future_expiry_funds_ok(
    private_channel_ctx: &PrivateChannelContext,
    user_a: &Keypair,
    user_b: &Keypair,
    mint_a: &Pubkey,
    mint_b: &Pubkey,
    settlement_authority: Pubkey,
) {
    let nonce = 2;
    let future_expiry = wall_now_secs() + 3600;
    let (swap_dvp, _) = setup::swap_dvp_pda(
        &settlement_authority,
        &user_a.pubkey(),
        &user_b.pubkey(),
        mint_a,
        mint_b,
        nonce,
    );
    let dvp_ata_a = get_associated_token_address_with_program_id(&swap_dvp, mint_a, &spl_token::ID);

    let bh = private_channel_ctx.get_blockhash().await.unwrap();
    let create_tx = setup::create_dvp_transaction(
        user_a,
        user_a.pubkey(),
        user_b.pubkey(),
        mint_a,
        mint_b,
        settlement_authority,
        AMOUNT_A,
        AMOUNT_B,
        future_expiry,
        None,
        nonce,
        bh,
    );
    let create_sig = private_channel_ctx
        .send_transaction(&create_tx)
        .await
        .unwrap();
    assert_tx_succeeded(private_channel_ctx, &create_sig.to_string()).await;

    let bh = private_channel_ctx.get_blockhash().await.unwrap();
    let fund_tx = setup::fund_dvp_transaction(user_a, swap_dvp, mint_a, AMOUNT_A, bh);
    let fund_sig = private_channel_ctx
        .send_transaction(&fund_tx)
        .await
        .unwrap();
    assert_tx_succeeded(private_channel_ctx, &fund_sig.to_string()).await;

    let escrow = private_channel_ctx
        .get_token_balance(&dvp_ata_a)
        .await
        .unwrap();
    assert_eq!(
        escrow, AMOUNT_A,
        "asset escrow should hold AMOUNT_A after future-expiry FundDvp"
    );
    println!("  ✓ future-expiry FundDvp succeeded, escrow funded");
}

fn wall_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Polls `getTransaction` until landed; panics on timeout. `meta.err` must
/// be `null`. Contra's `sendTransaction` skips preflight, so on-chain
/// failures only surface here.
async fn assert_tx_succeeded(private_channel_ctx: &PrivateChannelContext, sig: &str) {
    let meta = wait_for_tx_meta(private_channel_ctx, sig).await;
    let err = meta.get("err").unwrap_or(&serde_json::Value::Null);
    assert!(
        err.is_null(),
        "tx {sig} expected to succeed, got err: {err}"
    );
}

/// Polls `getTransaction` until landed; asserts `meta.err` matches
/// `{"InstructionError":[<idx>, {"Custom": <code>}]}`.
async fn assert_tx_failed_with_custom(
    private_channel_ctx: &PrivateChannelContext,
    sig: &str,
    code: u32,
) {
    let meta = wait_for_tx_meta(private_channel_ctx, sig).await;
    let err = meta
        .get("err")
        .and_then(|v| if v.is_null() { None } else { Some(v) })
        .unwrap_or_else(|| panic!("tx {sig} expected to fail with Custom({code}), got success"));

    let custom = err
        .pointer("/InstructionError/1/Custom")
        .and_then(|c| c.as_u64())
        .unwrap_or_else(|| panic!("tx {sig} err is not InstructionError(_, Custom(_)): {err}"));
    assert_eq!(
        custom, code as u64,
        "tx {sig} failed with Custom({custom}); expected Custom({code}). Full err: {err}"
    );
}

async fn wait_for_tx_meta(
    private_channel_ctx: &PrivateChannelContext,
    sig: &str,
) -> serde_json::Value {
    let parsed_sig = sig.parse().expect("valid signature");
    let started = Instant::now();
    while started.elapsed() < POLL_TIMEOUT {
        if let Ok(Some(value)) = private_channel_ctx
            .get_transaction_with_encoding(&parsed_sig, UiTransactionEncoding::Json)
            .await
        {
            if let Some(meta) = value.pointer("/meta").cloned() {
                return meta;
            }
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("tx {sig} did not land within {POLL_TIMEOUT:?}");
}

async fn bootstrap_user_a(
    private_channel_ctx: &PrivateChannelContext,
    solana_ctx: &SolanaContext,
    user_a: &Keypair,
    mint_a_kp: &Keypair,
    mint_b_kp: &Keypair,
) -> Result<()> {
    let mint_a = mint_a_kp.pubkey();
    let mint_b = mint_b_kp.pubkey();

    solana_ctx
        .fund_account(&user_a.pubkey(), AIRDROP_LAMPORTS)
        .await?;
    solana_ctx
        .fund_account(&private_channel_ctx.operator_key.pubkey(), AIRDROP_LAMPORTS)
        .await?;

    println!("Creating mint_a on test validator: {mint_a}");
    solana_ctx
        .create_mint(
            mint_a_kp,
            &private_channel_ctx.operator_key.pubkey(),
            MINT_DECIMALS,
        )
        .await?;
    println!("Creating mint_b on test validator: {mint_b}");
    solana_ctx
        .create_mint(
            mint_b_kp,
            &private_channel_ctx.operator_key.pubkey(),
            MINT_DECIMALS,
        )
        .await?;

    allow_mint_in_escrow(solana_ctx, mint_a_kp).await?;
    allow_mint_in_escrow(solana_ctx, mint_b_kp).await?;

    solana_ctx
        .create_token_accounts(&mint_a, &[user_a], &spl_token::ID)
        .await?;
    solana_ctx
        .create_token_accounts(&mint_b, &[user_a], &spl_token::ID)
        .await?;

    let user_a_solana_ata_a =
        get_associated_token_address_with_program_id(&user_a.pubkey(), &mint_a, &spl_token::ID);
    let user_a_solana_ata_b =
        get_associated_token_address_with_program_id(&user_a.pubkey(), &mint_b, &spl_token::ID);
    solana_ctx
        .mint_to(&mint_a, &user_a_solana_ata_a, DEPOSIT_A, &spl_token::ID)
        .await?;
    solana_ctx
        .mint_to(&mint_b, &user_a_solana_ata_b, DEPOSIT_B, &spl_token::ID)
        .await?;

    deposit_to_escrow(solana_ctx, user_a, &mint_a, DEPOSIT_A).await?;
    deposit_to_escrow(solana_ctx, user_a, &mint_b, DEPOSIT_B).await?;

    let user_a_contra_ata_a =
        get_associated_token_address_with_program_id(&user_a.pubkey(), &mint_a, &spl_token::ID);
    let user_a_contra_ata_b =
        get_associated_token_address_with_program_id(&user_a.pubkey(), &mint_b, &spl_token::ID);
    wait_for_private_channel_balance(private_channel_ctx, &user_a_contra_ata_a, DEPOSIT_A).await;
    wait_for_private_channel_balance(private_channel_ctx, &user_a_contra_ata_b, DEPOSIT_B).await;

    private_channel_ctx
        .read_client
        .get_account(&mint_b)
        .await
        .expect("mint_b must exist on the channel before swap tests");

    Ok(())
}

async fn allow_mint_in_escrow(solana_ctx: &SolanaContext, mint_keypair: &Keypair) -> Result<()> {
    let (instance_pda, _) = Pubkey::find_program_address(
        &[b"instance", solana_ctx.escrow_instance.pubkey().as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    let (allowed_mint_pda, allowed_mint_bump) = Pubkey::find_program_address(
        &[
            b"allowed_mint",
            instance_pda.as_ref(),
            mint_keypair.pubkey().as_ref(),
        ],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    let instance_ata = get_associated_token_address_with_program_id(
        &instance_pda,
        &mint_keypair.pubkey(),
        &spl_token::ID,
    );

    let allow_mint_ix = AllowMintBuilder::new()
        .payer(solana_ctx.operator_key.pubkey())
        .admin(solana_ctx.operator_key.pubkey())
        .instance(instance_pda)
        .mint(mint_keypair.pubkey())
        .allowed_mint(allowed_mint_pda)
        .instance_ata(instance_ata)
        .token_program(spl_token::ID)
        .bump(allowed_mint_bump)
        .instruction();

    let blockhash = solana_ctx.get_latest_blockhash().await?;
    let tx = Transaction::new_signed_with_payer(
        &[allow_mint_ix],
        Some(&solana_ctx.operator_key.pubkey()),
        &[&solana_ctx.operator_key],
        blockhash,
    );
    solana_ctx.send_transaction(&tx).await?;
    println!("  AllowMint OK: {}", mint_keypair.pubkey());
    Ok(())
}

async fn deposit_to_escrow(
    solana_ctx: &SolanaContext,
    user: &Keypair,
    mint: &Pubkey,
    amount: u64,
) -> Result<()> {
    let (instance_pda, _) = Pubkey::find_program_address(
        &[b"instance", solana_ctx.escrow_instance.pubkey().as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    let (allowed_mint_pda, _) = Pubkey::find_program_address(
        &[b"allowed_mint", instance_pda.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    );
    let user_ata =
        get_associated_token_address_with_program_id(&user.pubkey(), mint, &spl_token::ID);
    let instance_ata =
        get_associated_token_address_with_program_id(&instance_pda, mint, &spl_token::ID);

    let deposit_ix = DepositBuilder::new()
        .payer(user.pubkey())
        .user(user.pubkey())
        .instance(instance_pda)
        .mint(*mint)
        .allowed_mint(allowed_mint_pda)
        .token_program(spl_token::ID)
        .user_ata(user_ata)
        .instance_ata(instance_ata)
        .amount(amount)
        .instruction();

    let blockhash = solana_ctx.get_latest_blockhash().await?;
    let tx =
        Transaction::new_signed_with_payer(&[deposit_ix], Some(&user.pubkey()), &[user], blockhash);
    let sig = solana_ctx.send_transaction(&tx).await?;
    println!("  Deposit OK ({amount} of {mint}): {sig}");

    let started = Instant::now();
    while started.elapsed() < POLL_TIMEOUT {
        let deposits = solana_ctx
            .indexer_storage
            .get_all_db_transactions(TransactionType::Deposit, 100)
            .await
            .expect("query deposits from Solana indexer DB");
        if deposits.iter().any(|tx| tx.signature == sig.to_string()) {
            return Ok(());
        }
        sleep(POLL_INTERVAL).await;
    }
    Err(anyhow::anyhow!(
        "deposit {sig} not picked up by Solana indexer within {POLL_TIMEOUT:?}"
    ))
}

async fn wait_for_private_channel_balance(
    private_channel_ctx: &PrivateChannelContext,
    ata: &Pubkey,
    expected: u64,
) {
    let started = Instant::now();
    while started.elapsed() < POLL_TIMEOUT {
        if let Ok(balance) = private_channel_ctx.get_token_balance(ata).await {
            if balance >= expected {
                println!(
                    "  Contra ATA {ata} reached {balance} after {:?}",
                    started.elapsed()
                );
                return;
            }
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("Contra ATA {ata} did not reach expected balance {expected} within {POLL_TIMEOUT:?}");
}
