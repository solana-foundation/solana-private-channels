//! `test_admin_vm_initialize_mint_persists`
//!
//! Target file: `core/src/vm/admin.rs` — verifies that the AdminVm returns a
//! full `account_keys()` mirror so a well-formed `InitializeMint` persists the
//! mint and produces length-aligned stored-meta balance arrays.
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! Cases:
//!   1. A single well-formed admin InitializeMint lands successfully, its
//!      stored-meta balance arrays are length-aligned to accountKeys, the mint
//!      exists with the declared decimals/authority, and the spl_token program
//!      account is not clobbered.
//!   2. One admin tx with two InitializeMint instructions - both mints persist.
//!   3. Someone closes an ATA into a future mint address first. The operator's
//!      deposit mint fails there with IncorrectProgramId, InitializeMint still
//!      lands over the dataless System account, and the deposit mint then lands.

use {
    super::{
        test_context::PrivateChannelContext,
        utils::{MINT_DECIMALS, SEND_AND_CHECK_DURATION_SECONDS},
    },
    crate::setup,
    solana_sdk::{
        account::ReadableAccount,
        program_pack::Pack,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    solana_sdk_ids::system_program,
    spl_associated_token_account::{
        get_associated_token_address,
        instruction::{
            create_associated_token_account, create_associated_token_account_idempotent,
        },
    },
    spl_token::state::Mint,
    std::time::Duration,
};

pub async fn run_admin_vm_initialize_mint_persists_test(ctx: &PrivateChannelContext) {
    println!("\n=== AdminVm InitializeMint Persistence Coverage ===");

    case_single_mint_persists(ctx).await;
    case_two_mints_persist(ctx).await;
    case_reserved_target_initializes(ctx).await;

    println!("✓ AdminVm mint-persistence tests passed");
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Assert the getTransaction meta balance arrays are aligned to accountKeys,
/// which is exactly the invariant the AdminVm account_keys mirror restores.
fn assert_balances_aligned(tx_json: &serde_json::Value, label: &str) {
    let keys_len = tx_json
        .pointer("/transaction/message/accountKeys")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or_else(|| panic!("{label}: accountKeys missing (full response: {tx_json})"));
    let pre_len = tx_json
        .pointer("/meta/preBalances")
        .and_then(|v| v.as_array())
        .map(|a| a.len());
    let post_len = tx_json
        .pointer("/meta/postBalances")
        .and_then(|v| v.as_array())
        .map(|a| a.len());
    assert_eq!(
        pre_len,
        Some(keys_len),
        "{label}: preBalances must equal accountKeys len (full response: {tx_json})"
    );
    assert_eq!(
        post_len,
        Some(keys_len),
        "{label}: postBalances must equal accountKeys len (full response: {tx_json})"
    );
}

/// Fetch and unpack the on-chain Mint at `pubkey`, asserting its decimals.
async fn assert_mint_decimals(
    ctx: &PrivateChannelContext,
    pubkey: &solana_sdk::pubkey::Pubkey,
    label: &str,
) {
    let account = ctx
        .read_client
        .get_account(pubkey)
        .await
        .unwrap_or_else(|e| panic!("{label}: mint {pubkey} must exist after settle: {e:?}"));
    let mint = Mint::unpack(account.data())
        .unwrap_or_else(|e| panic!("{label}: mint {pubkey} must unpack as SPL Mint: {e:?}"));
    assert!(mint.is_initialized, "{label}: mint must be initialized");
    assert_eq!(
        mint.decimals, MINT_DECIMALS,
        "{label}: mint decimals must match declared value"
    );
}

// ── Cases ───────────────────────────────────────────────────────────────────

/// A well-formed single InitializeMint lands, has length-aligned balance meta,
/// persists the mint, and leaves the spl_token program account untouched.
async fn case_single_mint_persists(ctx: &PrivateChannelContext) {
    let mint = Keypair::new();

    // Baseline of the spl_token program account so we can prove it is not
    // clobbered by the mint write (best-effort: skipped if the RPC lacks it).
    let program_before = ctx.read_client.get_account(&spl_token::id()).await.ok();

    let blockhash = ctx.get_blockhash().await.unwrap();
    let init_tx = setup::create_mint_account_transaction(
        &ctx.operator_key,
        &mint,
        &ctx.operator_key.pubkey(),
        MINT_DECIMALS,
        blockhash,
    );
    let sig = ctx
        .send_and_check(
            &init_tx,
            Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS),
        )
        .await
        .expect("send_and_check should not error")
        .expect("InitializeMint should land");

    let response = ctx
        .get_transaction(&sig)
        .await
        .expect("get_transaction should not error")
        .expect("InitializeMint must be retrievable");

    let err = response
        .pointer("/meta/err")
        .unwrap_or(&serde_json::Value::Null);
    assert!(
        err.is_null(),
        "case 1: InitializeMint must succeed, got err: {err} (full: {response})"
    );
    assert_balances_aligned(&response, "case 1 (single mint)");

    assert_mint_decimals(ctx, &mint.pubkey(), "case 1").await;

    if let Some(before) = program_before {
        let after = ctx
            .read_client
            .get_account(&spl_token::id())
            .await
            .expect("spl_token program must still exist after mint write");
        assert_eq!(
            before.data(),
            after.data(),
            "case 1: spl_token program account must not be clobbered"
        );
    }
    println!("  ✓ case 1: single InitializeMint persists mint, balances aligned");
}

/// One admin tx carrying two InitializeMint instructions: both mints succeed
/// and persist at the correct state.
async fn case_two_mints_persist(ctx: &PrivateChannelContext) {
    let mint_a = Keypair::new();
    let mint_b = Keypair::new();
    let operator = ctx.operator_key.pubkey();

    let ix_a = spl_token::instruction::initialize_mint(
        &spl_token::id(),
        &mint_a.pubkey(),
        &operator,
        None,
        MINT_DECIMALS,
    )
    .unwrap();
    let ix_b = spl_token::instruction::initialize_mint(
        &spl_token::id(),
        &mint_b.pubkey(),
        &operator,
        None,
        MINT_DECIMALS,
    )
    .unwrap();

    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx = Transaction::new_signed_with_payer(
        &[ix_a, ix_b],
        Some(&operator),
        &[&ctx.operator_key],
        blockhash,
    );

    let sig = ctx
        .send_and_check(&tx, Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS))
        .await
        .expect("send_and_check should not error")
        .expect("two-mint InitializeMint should land");

    let response = ctx
        .get_transaction(&sig)
        .await
        .expect("get_transaction should not error")
        .expect("two-mint tx must be retrievable");
    let err = response
        .pointer("/meta/err")
        .unwrap_or(&serde_json::Value::Null);
    assert!(
        err.is_null(),
        "case 2: two-mint tx must succeed, got err: {err} (full: {response})"
    );
    assert_balances_aligned(&response, "case 2 (two mints)");

    assert_mint_decimals(ctx, &mint_a.pubkey(), "case 2 mint_a").await;
    assert_mint_decimals(ctx, &mint_b.pubkey(), "case 2 mint_b").await;
    println!("  ✓ case 2: two InitializeMint instructions both persist");
}

/// Someone squats a future mint address by closing an ATA into it. The
/// operator's deposit mint fails there until the admin initializes the mint,
/// then lands.
async fn case_reserved_target_initializes(ctx: &PrivateChannelContext) {
    let operator = ctx.operator_key.pubkey();
    let live_mint = Keypair::new();
    let mint = Keypair::new();
    let squatter = Keypair::new();
    let squatter_ata = get_associated_token_address(&squatter.pubkey(), &live_mint.pubkey());
    let recipient = Pubkey::new_unique();
    let recipient_ata = get_associated_token_address(&recipient, &mint.pubkey());
    let amount = 1_000;

    // Squat: open an ATA on a live mint, then close it into the future mint
    // address. The close destination does not have to sign.
    let live_mint_tx = setup::create_mint_account_transaction(
        &ctx.operator_key,
        &live_mint,
        &operator,
        MINT_DECIMALS,
        ctx.get_blockhash().await.unwrap(),
    );
    let err = send_and_get_err(ctx, &live_mint_tx).await;
    assert!(err.is_null(), "live mint init failed: {err}");
    let open_tx = Transaction::new_signed_with_payer(
        &[create_associated_token_account(
            &squatter.pubkey(),
            &squatter.pubkey(),
            &live_mint.pubkey(),
            &spl_token::id(),
        )],
        Some(&squatter.pubkey()),
        &[&squatter],
        ctx.get_blockhash().await.unwrap(),
    );
    let err = send_and_get_err(ctx, &open_tx).await;
    assert!(err.is_null(), "squatter ATA create failed: {err}");
    let close_tx = Transaction::new_signed_with_payer(
        &[spl_token::instruction::close_account(
            &spl_token::id(),
            &squatter_ata,
            &mint.pubkey(),
            &squatter.pubkey(),
            &[],
        )
        .unwrap()],
        Some(&squatter.pubkey()),
        &[&squatter],
        ctx.get_blockhash().await.unwrap(),
    );
    let err = send_and_get_err(ctx, &close_tx).await;
    assert!(err.is_null(), "squatter ATA close failed: {err}");
    let reserved = ctx
        .read_client
        .get_account(&mint.pubkey())
        .await
        .expect("case 3: the squat must leave an account at the mint address");
    assert_eq!(*reserved.owner(), system_program::ID);
    assert!(reserved.data().is_empty());

    // The operator's deposit mint: create the recipient ATA, then MintTo.
    let deposit_ixs = [
        create_associated_token_account_idempotent(
            &operator,
            &recipient,
            &mint.pubkey(),
            &spl_token::id(),
        ),
        spl_token::instruction::mint_to(
            &spl_token::id(),
            &mint.pubkey(),
            &recipient_ata,
            &operator,
            &[],
            amount,
        )
        .unwrap(),
    ];

    // Before the init it fails with IncorrectProgramId, the error that sends
    // the indexer to JIT mint initialization.
    let deposit_tx = Transaction::new_signed_with_payer(
        &deposit_ixs,
        Some(&operator),
        &[&ctx.operator_key],
        ctx.get_blockhash().await.unwrap(),
    );
    let err = send_and_get_err(ctx, &deposit_tx).await;
    assert!(
        err.to_string().contains("IncorrectProgramId"),
        "deposit before init must fail with IncorrectProgramId, got: {err}"
    );

    let init_tx = setup::create_mint_account_transaction(
        &ctx.operator_key,
        &mint,
        &operator,
        MINT_DECIMALS,
        ctx.get_blockhash().await.unwrap(),
    );
    let err = send_and_get_err(ctx, &init_tx).await;
    assert!(err.is_null(), "InitializeMint over the squat failed: {err}");
    assert_mint_decimals(ctx, &mint.pubkey(), "case 3").await;
    let account = ctx.read_client.get_account(&mint.pubkey()).await.unwrap();
    assert_eq!(*account.owner(), spl_token::id());
    assert_eq!(account.lamports(), 1, "the squatted lamport is kept");

    // After the init the same deposit mint lands.
    let deposit_tx = Transaction::new_signed_with_payer(
        &deposit_ixs,
        Some(&operator),
        &[&ctx.operator_key],
        ctx.get_blockhash().await.unwrap(),
    );
    let err = send_and_get_err(ctx, &deposit_tx).await;
    assert!(err.is_null(), "deposit after init failed: {err}");
    assert_eq!(ctx.get_token_balance(&recipient_ata).await.unwrap(), amount);
    println!("  ✓ case 3: a squatted mint address still initializes and mints");
}

/// Send `tx`, wait for it to land, and return its `meta.err` (null on success).
async fn send_and_get_err(ctx: &PrivateChannelContext, tx: &Transaction) -> serde_json::Value {
    let sig = ctx
        .send_and_check(tx, Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS))
        .await
        .expect("send_and_check should not error")
        .expect("transaction should land");
    let response = ctx
        .get_transaction(&sig)
        .await
        .expect("get_transaction should not error")
        .expect("a landed transaction must be retrievable");
    response
        .pointer("/meta/err")
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}
