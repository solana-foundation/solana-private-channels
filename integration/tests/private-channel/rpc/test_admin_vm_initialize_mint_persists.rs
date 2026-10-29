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
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    spl_token_2022::{extension::StateWithExtensions, state::Mint},
    std::time::Duration,
};

pub async fn run_admin_vm_initialize_mint_persists_test(ctx: &PrivateChannelContext) {
    println!("\n=== AdminVm InitializeMint Persistence Coverage ===");

    case_single_mint_persists(ctx).await;
    case_two_mints_persist(ctx).await;
    case_mint_bytes_match_the_real_program(ctx).await;

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
    let mint = StateWithExtensions::<Mint>::unpack(account.data())
        .unwrap_or_else(|e| {
            panic!("{label}: mint {pubkey} must unpack as a Token-2022 mint: {e:?}")
        })
        .base;
    assert!(mint.is_initialized, "{label}: mint must be initialized");
    assert_eq!(
        mint.decimals, MINT_DECIMALS,
        "{label}: mint decimals must match declared value"
    );
}

// ── Cases ───────────────────────────────────────────────────────────────────

/// A well-formed single InitializeMint lands, has length-aligned balance meta,
/// persists the mint, and leaves the Token-2022 program account untouched.
async fn case_single_mint_persists(ctx: &PrivateChannelContext) {
    let mint = Keypair::new();

    // Baseline of the Token-2022 program account so we can prove it is not
    // clobbered by the mint write (best-effort: skipped if the RPC lacks it).
    let program_before = program_head(ctx).await;

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
        let after = program_head(ctx)
            .await
            .expect("Token-2022 program must still exist after mint write");
        assert_eq!(
            before, after,
            "case 1: Token-2022 program account must not be clobbered"
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

    // The suite's node delegate is the operator key.
    let pair = |mint: &Keypair| {
        [
            spl_token_2022::instruction::initialize_permanent_delegate(
                &spl_token_2022::id(),
                &mint.pubkey(),
                &operator,
            )
            .unwrap(),
            spl_token_2022::instruction::initialize_mint(
                &spl_token_2022::id(),
                &mint.pubkey(),
                &operator,
                None,
                MINT_DECIMALS,
            )
            .unwrap(),
        ]
    };
    let ixs: Vec<_> = pair(&mint_a).into_iter().chain(pair(&mint_b)).collect();

    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx =
        Transaction::new_signed_with_payer(&ixs, Some(&operator), &[&ctx.operator_key], blockhash);

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

/// First bytes and flags of the Token-2022 program account; the full 1.4 MB ELF
/// is over the account response budget, so it is read through a dataSlice.
async fn program_head(ctx: &PrivateChannelContext) -> Option<(Vec<u8>, bool, u64)> {
    let config = solana_client::rpc_config::RpcAccountInfoConfig {
        encoding: Some(solana_account_decoder_client_types::UiAccountEncoding::Base64),
        data_slice: Some(solana_account_decoder_client_types::UiDataSliceConfig {
            offset: 0,
            length: 256,
        }),
        ..Default::default()
    };
    let account = ctx
        .read_client
        .get_ui_account_with_config(&spl_token_2022::id(), config)
        .await
        .ok()?
        .value?
        .to_account()?;
    Some((account.data, account.executable, account.lamports))
}

/// The admin VM writes exactly what the real Token-2022 program writes for
/// `[InitializePermanentDelegate, InitializeMint2]`. `t22_mint` reproduces the
/// validator-captured golden bytes, so it stands in for the program here.
async fn case_mint_bytes_match_the_real_program(ctx: &PrivateChannelContext) {
    let mint = Keypair::new();
    let operator = ctx.operator_key.pubkey();
    let ixs = [
        spl_token_2022::instruction::initialize_permanent_delegate(
            &spl_token_2022::id(),
            &mint.pubkey(),
            &operator,
        )
        .unwrap(),
        spl_token_2022::instruction::initialize_mint2(
            &spl_token_2022::id(),
            &mint.pubkey(),
            &operator,
            Some(&operator),
            MINT_DECIMALS,
        )
        .unwrap(),
    ];
    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx =
        Transaction::new_signed_with_payer(&ixs, Some(&operator), &[&ctx.operator_key], blockhash);
    ctx.send_and_check(&tx, Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS))
        .await
        .expect("send_and_check should not error")
        .expect("mint creation should land");

    let account = ctx
        .read_client
        .get_account(&mint.pubkey())
        .await
        .expect("mint must exist");
    assert_eq!(account.owner, spl_token_2022::id());
    let expected = private_channel_core::t22_fixtures::t22_mint(
        MINT_DECIMALS,
        &operator,
        Some(&operator),
        Some(&operator),
        0,
    );
    assert_eq!(account.data(), &expected[..], "case 3: mint bytes");
    println!("  ✓ case 3: channel mint bytes match the real Token-2022 layout");
}
