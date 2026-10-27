//! Seize on the channel: the permanent delegate may only move a balance with
//! TransferChecked into the compliance ATA, run through the real Token-2022 program.

use {
    super::{test_context::PrivateChannelContext, utils::SEND_AND_CHECK_DURATION_SECONDS},
    crate::setup,
    devnet_scripts::seize::{compliance_ata, seize_instructions, SeizePlan},
    dvp_swap_program_client::instructions::CancelDvpBuilder,
    solana_client::rpc_config::RpcSimulateTransactionConfig,
    solana_sdk::{
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    spl_token_2022::{
        extension::StateWithExtensions,
        state::{Account, AccountState, Mint},
    },
    std::time::Duration,
};

/// Compliance owner the suite's node is configured with.
pub const COMPLIANCE_OWNER: Pubkey = Pubkey::new_from_array([0xC0; 32]);

const DECIMALS: u8 = 6;
const BALANCE: u64 = 1_000;

pub async fn run_seize_test(ctx: &PrivateChannelContext) {
    println!("\n=== Seize (permanent delegate) ===");
    case_seize_frozen_account_into_compliance(ctx).await;
    case_delegate_cannot_move_elsewhere(ctx).await;
    case_seize_out_of_a_dvp_escrow(ctx).await;
    println!("✓ Seize tests passed");
}

/// Sends and returns the signature once settled, or None if it never lands.
async fn land(
    ctx: &PrivateChannelContext,
    ixs: &[Instruction],
    signers: &[&Keypair],
) -> Option<Signature> {
    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx =
        Transaction::new_signed_with_payer(ixs, Some(&signers[0].pubkey()), signers, blockhash);
    ctx.send_and_check(
        &tx,
        Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS * 3),
    )
    .await
    .expect("send must not error")
}

async fn meta_err(ctx: &PrivateChannelContext, sig: &Signature) -> serde_json::Value {
    let response = ctx
        .get_transaction(sig)
        .await
        .unwrap()
        .expect("landed tx is readable");
    response
        .pointer("/meta/err")
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

async fn token_account(ctx: &PrivateChannelContext, key: &Pubkey) -> Account {
    let account = ctx
        .read_client
        .get_account(key)
        .await
        .expect("token account exists");
    StateWithExtensions::<Account>::unpack(&account.data)
        .unwrap()
        .base
}

async fn supply(ctx: &PrivateChannelContext, mint: &Pubkey) -> u64 {
    let account = ctx
        .read_client
        .get_account(mint)
        .await
        .expect("mint exists");
    StateWithExtensions::<Mint>::unpack(&account.data)
        .unwrap()
        .base
        .supply
}

/// A channel mint with the operator as authority, freeze authority and delegate, and
/// `holder`'s ATA holding `BALANCE`.
async fn mint_with_balance(ctx: &PrivateChannelContext, holder: &Pubkey) -> (Pubkey, Pubkey) {
    let operator = &ctx.operator_key;
    let mint = Keypair::new().pubkey();
    let program = spl_token_2022::id();
    let create = [
        spl_token_2022::instruction::initialize_permanent_delegate(
            &program,
            &mint,
            &operator.pubkey(),
        )
        .unwrap(),
        spl_token_2022::instruction::initialize_mint2(
            &program,
            &mint,
            &operator.pubkey(),
            Some(&operator.pubkey()),
            DECIMALS,
        )
        .unwrap(),
    ];
    land(ctx, &create, &[operator])
        .await
        .expect("mint creation lands");
    let ata = setup::get_associated_token_address(holder, &mint);
    let fund = [
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &operator.pubkey(),
            holder,
            &mint,
            &program,
        ),
        spl_token_2022::instruction::mint_to(
            &program,
            &mint,
            &ata,
            &operator.pubkey(),
            &[],
            BALANCE,
        )
        .unwrap(),
    ];
    let sig = land(ctx, &fund, &[operator]).await.expect("funding lands");
    assert!(meta_err(ctx, &sig).await.is_null(), "funding must succeed");
    (mint, ata)
}

/// Freeze Alice, seize with the CLI's transaction: one tx thaws, moves all of it to
/// the compliance ATA and freezes again. Supply is unchanged.
async fn case_seize_frozen_account_into_compliance(ctx: &PrivateChannelContext) {
    let operator = &ctx.operator_key;
    let alice = Keypair::new().pubkey();
    let (mint, alice_ata) = mint_with_balance(ctx, &alice).await;
    let freeze = spl_token_2022::instruction::freeze_account(
        &spl_token_2022::id(),
        &alice_ata,
        &mint,
        &operator.pubkey(),
        &[],
    )
    .unwrap();
    land(ctx, &[freeze], &[operator])
        .await
        .expect("freeze lands");
    assert_eq!(
        token_account(ctx, &alice_ata).await.state,
        AccountState::Frozen
    );

    let plan = SeizePlan {
        mint,
        decimals: DECIMALS,
        source: alice_ata,
        source_frozen: true,
        amount: BALANCE,
        delegate: operator.pubkey(),
        freeze_authority: operator.pubkey(),
        compliance_owner: COMPLIANCE_OWNER,
    };
    let sig = land(ctx, &seize_instructions(&plan), &[operator])
        .await
        .expect("seize lands");
    assert!(meta_err(ctx, &sig).await.is_null(), "seize must succeed");

    let alice_after = token_account(ctx, &alice_ata).await;
    assert_eq!(alice_after.amount, 0);
    assert_eq!(
        alice_after.state,
        AccountState::Frozen,
        "re-frozen in the same tx"
    );
    let compliance = token_account(ctx, &compliance_ata(&COMPLIANCE_OWNER, &mint)).await;
    assert_eq!(compliance.amount, BALANCE);
    assert_eq!(
        supply(ctx, &mint).await,
        BALANCE,
        "seize moves, it never burns"
    );
    println!("  ✓ frozen balance seized into the compliance ATA, account re-frozen");
}

/// A delegate move anywhere else never lands, and the delegate signing an unchecked
/// Transfer is not honoured by Token-2022 as a permanent delegate.
async fn case_delegate_cannot_move_elsewhere(ctx: &PrivateChannelContext) {
    let operator = &ctx.operator_key;
    let alice = Keypair::new().pubkey();
    let (mint, alice_ata) = mint_with_balance(ctx, &alice).await;
    let program = spl_token_2022::id();
    let elsewhere = setup::get_associated_token_address(&operator.pubkey(), &mint);
    let create_elsewhere =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &operator.pubkey(),
            &operator.pubkey(),
            &mint,
            &program,
        );
    land(ctx, &[create_elsewhere], &[operator])
        .await
        .expect("ATA creation lands");

    let steal = spl_token_2022::instruction::transfer_checked(
        &program,
        &alice_ata,
        &mint,
        &elsewhere,
        &operator.pubkey(),
        &[],
        10,
        DECIMALS,
    )
    .unwrap();
    let blockhash = ctx.get_blockhash().await.unwrap();
    let tx = Transaction::new_signed_with_payer(
        std::slice::from_ref(&steal),
        Some(&operator.pubkey()),
        &[operator],
        blockhash,
    );
    let simulation = ctx
        .read_client
        .simulate_transaction_with_config(
            &tx,
            RpcSimulateTransactionConfig {
                sig_verify: true,
                ..Default::default()
            },
        )
        .await;
    let message = format!("{simulation:?}");
    assert!(
        message.contains("compliance account"),
        "simulate must name the rule: {message}"
    );
    assert_eq!(
        land(ctx, &[steal], &[operator]).await,
        None,
        "the move must never land"
    );

    #[allow(deprecated)]
    let unchecked = spl_token_2022::instruction::transfer(
        &program,
        &alice_ata,
        &elsewhere,
        &operator.pubkey(),
        &[],
        10,
    )
    .unwrap();
    let sig = land(ctx, &[unchecked], &[operator])
        .await
        .expect("lands as a failed tx");
    assert!(
        !meta_err(ctx, &sig).await.is_null(),
        "Transfer must not honour the delegate"
    );
    assert_eq!(token_account(ctx, &alice_ata).await.amount, BALANCE);
    println!("  ✓ delegate moves outside the compliance ATA are refused");
}

/// The delegate can seize out of a funded DvP escrow only into compliance; Cancel then
/// refunds what is left and closes the swap.
async fn case_seize_out_of_a_dvp_escrow(ctx: &PrivateChannelContext) {
    let operator = &ctx.operator_key;
    let user_a = Keypair::new();
    let user_b = Keypair::new();
    let settlement = Keypair::new();
    let (mint_a, user_a_ata) = mint_with_balance(ctx, &user_a.pubkey()).await;
    let (mint_b, user_b_ata) = mint_with_balance(ctx, &user_b.pubkey()).await;
    let program = spl_token_2022::id();

    let (swap_dvp, _) = setup::swap_dvp_pda(
        &settlement.pubkey(),
        &user_a.pubkey(),
        &user_b.pubkey(),
        &mint_a,
        &mint_b,
        7,
    );
    let blockhash = ctx.get_blockhash().await.unwrap();
    let create = setup::create_dvp_transaction(
        &user_a,
        user_a.pubkey(),
        user_b.pubkey(),
        &mint_a,
        &mint_b,
        settlement.pubkey(),
        BALANCE,
        BALANCE,
        chrono_now() + 3600,
        None,
        7,
        blockhash,
    );
    let sig = ctx
        .send_and_check(
            &create,
            Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS * 3),
        )
        .await
        .unwrap()
        .expect("CreateDvp lands");
    assert!(
        meta_err(ctx, &sig).await.is_null(),
        "CreateDvp must succeed"
    );
    let blockhash = ctx.get_blockhash().await.unwrap();
    let fund = setup::fund_dvp_transaction(&user_a, swap_dvp, &mint_a, BALANCE, blockhash);
    let sig = ctx
        .send_and_check(
            &fund,
            Duration::from_secs(SEND_AND_CHECK_DURATION_SECONDS * 3),
        )
        .await
        .unwrap()
        .expect("funding lands");
    assert!(meta_err(ctx, &sig).await.is_null(), "funding must succeed");

    let escrow_a = setup::get_associated_token_address(&swap_dvp, &mint_a);
    let own = setup::get_associated_token_address(&operator.pubkey(), &mint_a);
    let to_own = spl_token_2022::instruction::transfer_checked(
        &program,
        &escrow_a,
        &mint_a,
        &own,
        &operator.pubkey(),
        &[],
        100,
        DECIMALS,
    )
    .unwrap();
    assert_eq!(
        land(ctx, &[to_own], &[operator]).await,
        None,
        "escrow drain to own ATA refused"
    );

    let plan = SeizePlan {
        mint: mint_a,
        decimals: DECIMALS,
        source: escrow_a,
        source_frozen: false,
        amount: 100,
        delegate: operator.pubkey(),
        freeze_authority: operator.pubkey(),
        compliance_owner: COMPLIANCE_OWNER,
    };
    let sig = land(ctx, &seize_instructions(&plan), &[operator])
        .await
        .expect("escrow seize lands");
    assert!(
        meta_err(ctx, &sig).await.is_null(),
        "escrow seize must succeed"
    );
    assert_eq!(token_account(ctx, &escrow_a).await.amount, BALANCE - 100);

    let cancel = CancelDvpBuilder::new()
        .settlement_authority(settlement.pubkey())
        .swap_dvp(swap_dvp)
        .mint_a(mint_a)
        .mint_b(mint_b)
        .dvp_ata_a(escrow_a)
        .dvp_ata_b(setup::get_associated_token_address(&swap_dvp, &mint_b))
        .user_a_ata_a(user_a_ata)
        .user_b_ata_b(user_b_ata)
        .token_program_a(program)
        .token_program_b(program)
        .memo_program(spl_memo::id())
        .leg_a_extras_count(0)
        .instruction();
    let sig = land(ctx, &[cancel], &[&settlement])
        .await
        .expect("cancel lands");
    assert!(
        meta_err(ctx, &sig).await.is_null(),
        "Cancel must refund the remainder"
    );
    assert_eq!(token_account(ctx, &user_a_ata).await.amount, BALANCE - 100);
    println!("  ✓ DvP escrow seized only into compliance; Cancel refunds the rest");
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
