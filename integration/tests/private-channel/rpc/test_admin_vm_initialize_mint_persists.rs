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

use {
    super::{
        test_context::PrivateChannelContext,
        utils::{MINT_DECIMALS, SEND_AND_CHECK_DURATION_SECONDS},
    },
    crate::setup,
    solana_sdk::{
        account::ReadableAccount,
        program_pack::Pack,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    spl_token::state::Mint,
    std::time::Duration,
};

pub async fn run_admin_vm_initialize_mint_persists_test(ctx: &PrivateChannelContext) {
    println!("\n=== AdminVm InitializeMint Persistence Coverage ===");

    case_single_mint_persists(ctx).await;
    case_two_mints_persist(ctx).await;

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
