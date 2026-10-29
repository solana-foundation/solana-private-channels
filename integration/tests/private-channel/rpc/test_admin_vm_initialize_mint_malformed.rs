//! `test_admin_vm_initialize_mint_malformed`
//!
//! Target file: `core/src/vm/admin.rs` — drives `process_initialize_mint`'s
//! validation arms via the full sequencer → executor → AdminVm path.
//! Binary: `private_channel_integration` (existing).
//! Fixture: reuses `PrivateChannelContext`.
//!
//! Cases:
//!   A. InitializeMint data < 35 bytes              → InvalidAccountData
//!   B. Freeze-tag = 1 with truncated freeze auth   → InvalidAccountData
//!   C. Re-init on an already-initialized mint      → AccountAlreadyInitialized
//!
//! Why these three only:
//!   * The execution-stage partition routes a tx to AdminVm only when
//!     *every* instruction matches `is_admin_instruction` — i.e. spl-token
//!     discriminator 0 (`InitializeMint`). The non-SPL-program, empty-data,
//!     and unsupported-discriminator arms in `process_initialize_mint` all
//!     require an instruction the partition classifies as non-admin, so
//!     the tx goes through the regular SVM and those AdminVm branches are
//!     unreachable from any sanitized integration transaction. The unit
//!     tests in `admin.rs::tests` cover them by calling
//!     `load_and_execute_sanitized_transactions` directly.
//!   * The `mint_index` out-of-bounds arm is unreachable because
//!     `SanitizedTransaction::try_from_*` rejects account-index overflow
//!     before the AdminVm sees the instruction.

use {
    super::{
        test_context::PrivateChannelContext,
        utils::{MINT_DECIMALS, SEND_AND_CHECK_DURATION_SECONDS},
    },
    crate::setup,
    solana_client::rpc_config::RpcSendTransactionConfig,
    solana_sdk::{
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::time::Duration,
    tokio::time::{sleep, Instant},
};

const SPL_TOKEN_INITIALIZE_MINT: u8 = 0;

pub async fn run_admin_vm_initialize_mint_malformed_test(ctx: &PrivateChannelContext) {
    println!("\n=== AdminVm InitializeMint Malformed-Input Coverage ===");

    case_a_init_mint_short_data(ctx).await;
    case_b_init_mint_truncated_freeze_authority(ctx).await;
    case_c_init_mint_already_initialized(ctx).await;

    println!("✓ AdminVm malformed-input coverage tests passed");
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Build a tx signed by the admin (operator) key, carrying the permanent
/// delegate ix (the suite's node delegate is the operator) and then an
/// `InitializeMint` instruction with the supplied raw data. Bypasses
/// `spl_token_2022::instruction::initialize_mint` so the data layout can be
/// deliberately malformed.
fn build_admin_init_mint_tx_raw(
    admin: &Keypair,
    mint: &Pubkey,
    data: Vec<u8>,
    blockhash: solana_sdk::hash::Hash,
) -> Transaction {
    let ix = Instruction {
        program_id: spl_token_2022::id(),
        accounts: vec![
            AccountMeta::new(*mint, false),
            // Solana's legacy InitializeMint shape includes a rent sysvar;
            // the AdminVm validation aborts before any rent lookup so this
            // meta is harmless for the malformed-data arms.
            AccountMeta::new_readonly(solana_sdk::sysvar::rent::ID, false),
        ],
        data,
    };
    let delegate = spl_token_2022::instruction::initialize_permanent_delegate(
        &spl_token_2022::id(),
        mint,
        &admin.pubkey(),
    )
    .unwrap();
    Transaction::new_signed_with_payer(&[delegate, ix], Some(&admin.pubkey()), &[admin], blockhash)
}

/// Submit the tx with preflight disabled so a malformed AdminVm payload
/// reaches the settle stage instead of being rejected at simulate time.
/// Then poll `getTransaction` until it surfaces, or until the configured
/// settle window elapses.
async fn submit_skip_preflight_and_fetch(
    ctx: &PrivateChannelContext,
    tx: &Transaction,
) -> Option<serde_json::Value> {
    let sig = ctx
        .write_client
        .send_transaction_with_config(
            tx,
            RpcSendTransactionConfig {
                skip_preflight: true,
                ..Default::default()
            },
        )
        .await
        .expect("send_transaction_with_config(skip_preflight=true) failed");

    let deadline = Instant::now() + Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS);
    loop {
        if let Some(value) = ctx
            .get_transaction(&sig)
            .await
            .expect("get_transaction should not error")
        {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Locate the `meta.err` field in a `getTransaction` JSON response and assert
/// it stringifies to something containing every fragment in `expected_substrings`.
/// `EncodedConfirmedTransactionWithStatusMeta` flattens the meta to the top
/// level alongside `slot` / `transaction`, so the pointer is `/meta/err`.
fn assert_meta_err_contains(
    tx_json: &serde_json::Value,
    expected_substrings: &[&str],
    label: &str,
) {
    let err_val = tx_json
        .pointer("/meta/err")
        .unwrap_or(&serde_json::Value::Null);
    assert!(
        !err_val.is_null(),
        "{label}: meta.err must be populated for a malformed tx (full response: {tx_json})"
    );
    let err_str = err_val.to_string();
    for needle in expected_substrings {
        assert!(
            err_str.contains(needle),
            "{label}: meta.err {err_str} missing fragment {needle:?} (full response: {tx_json})"
        );
    }
}

// ── Cases ───────────────────────────────────────────────────────────────────

/// A. InitializeMint with data < 35 bytes: hits the
///    `data.len() < 35 || accounts.is_empty()` guard in
///    `process_initialize_mint`.
async fn case_a_init_mint_short_data(ctx: &PrivateChannelContext) {
    let mint = Keypair::new();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let mut data = vec![0u8; 20];
    data[0] = SPL_TOKEN_INITIALIZE_MINT;
    let tx = build_admin_init_mint_tx_raw(&ctx.operator_key, &mint.pubkey(), data, blockhash);

    let response = submit_skip_preflight_and_fetch(ctx, &tx).await;
    assert!(
        response.is_some(),
        "case A: short-data InitializeMint should land with err meta"
    );
    assert_meta_err_contains(
        &response.unwrap(),
        &["InstructionError", "InvalidAccountData"],
        "case A (short data)",
    );
    println!("  ✓ case A: InitializeMint data < 35 → InvalidAccountData");
}

/// B. InitializeMint with freeze_authority tag = 1 but data length 50
///    (only 16 bytes after the tag, not the required 32). Hits the
///    truncated-freeze-authority guard in `process_initialize_mint`.
async fn case_b_init_mint_truncated_freeze_authority(ctx: &PrivateChannelContext) {
    let mint = Keypair::new();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let mut data = vec![0u8; 50];
    data[0] = SPL_TOKEN_INITIALIZE_MINT;
    data[1] = MINT_DECIMALS;
    // bytes 2..34: mint_authority — leave zeros; the validator never reads
    // this far in the truncated case.
    data[34] = 1; // freeze-authority COption tag = Some — but only 16 trailing bytes.
    let tx = build_admin_init_mint_tx_raw(&ctx.operator_key, &mint.pubkey(), data, blockhash);

    let response = submit_skip_preflight_and_fetch(ctx, &tx).await;
    assert!(
        response.is_some(),
        "case B: truncated-freeze-auth tx should land with err meta"
    );
    assert_meta_err_contains(
        &response.unwrap(),
        &["InstructionError", "InvalidAccountData"],
        "case B (truncated freeze auth)",
    );
    println!("  ✓ case B: InitializeMint truncated freeze authority → InvalidAccountData");
}

/// C. Re-init on an already-initialized mint: first initialize a fresh mint
///    via the standard helper, then submit a second InitializeMint targeting
///    the same pubkey. The second must fail with AccountAlreadyInitialized
///    via the already-initialized guard in `process_initialize_mint`.
async fn case_c_init_mint_already_initialized(ctx: &PrivateChannelContext) {
    let mint = Keypair::new();

    // Step 1: legitimate initial mint creation.
    let blockhash = ctx.get_blockhash().await.unwrap();
    let init_tx = setup::create_mint_account_transaction(
        &ctx.operator_key,
        &mint,
        &ctx.operator_key.pubkey(),
        MINT_DECIMALS,
        blockhash,
    );
    let init_sig = ctx
        .send_and_check(
            &init_tx,
            Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS),
        )
        .await
        .expect("send_and_check should not error")
        .expect("first InitializeMint should land");
    let init_response = ctx
        .get_transaction(&init_sig)
        .await
        .expect("get_transaction should not error")
        .expect("first InitializeMint must be retrievable");
    let init_err = init_response
        .pointer("/meta/err")
        .unwrap_or(&serde_json::Value::Null);
    assert!(
        init_err.is_null(),
        "case C: first InitializeMint must succeed, got err: {init_err}"
    );

    // Step 2: second InitializeMint on the same pubkey.
    let blockhash2 = ctx.get_blockhash().await.unwrap();
    let reinit_tx = setup::create_mint_account_transaction(
        &ctx.operator_key,
        &mint,
        &ctx.operator_key.pubkey(),
        MINT_DECIMALS,
        blockhash2,
    );
    let response = submit_skip_preflight_and_fetch(ctx, &reinit_tx).await;
    assert!(
        response.is_some(),
        "case C: re-init tx should land with err meta"
    );
    assert_meta_err_contains(
        &response.unwrap(),
        &["InstructionError", "AccountAlreadyInitialized"],
        "case C (re-init)",
    );
    println!("  ✓ case C: re-init of initialized mint → AccountAlreadyInitialized");
}
