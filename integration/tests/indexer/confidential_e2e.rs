//! End to end confidential deposits and releases on one test validator (Token-2022 11.1.0,
//! the ZK ElGamal proof program, real proofs), with both indexers and both operators running.
//! Each test owns its validator and database; run them with few threads, proofs are costly.

#[path = "helpers/mod.rs"]
mod helpers;

#[allow(dead_code)]
#[path = "setup.rs"]
mod setup;

#[allow(dead_code)]
#[path = "confidential_harness.rs"]
mod confidential_harness;

use base64::Engine;
use confidential_harness::*;
use private_channel_indexer::operator::confidential_constants::CU_CONFIDENTIAL_RELEASE;
use private_channel_indexer::operator::confidential_custody::ConfidentialCustody;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionVersion;
use solana_transaction_status::{EncodedTransaction, UiMessage, UiTransactionEncoding};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Fee mint used across tests: 1% capped at 5,000, so a 1,000,000 deposit pays the cap.
const FEE: (u16, u64) = (100, 5_000);

/// Wait for a deposit row to be minted on the channel; returns its credited amount.
async fn wait_credited(env: &Env, signature: &Signature) -> TestResult<Row> {
    let row = env.wait_status(&signature.to_string(), "completed").await?;
    assert_eq!(row.kind, "deposit");
    Ok(row)
}

/// Wait for a withdrawal to be released on Solana; returns the row.
async fn wait_released(env: &Env, signature: &str) -> TestResult<Row> {
    env.wait_status(signature, "completed").await
}

/// The release transaction, its serialized size, account count and compute used.
async fn release_footprint(env: &Env, release: &str) -> TestResult<(usize, usize, u64, bool)> {
    let fetch = |encoding| {
        let client = env.client.clone();
        async move {
            client
                .get_transaction_with_config(
                    &Signature::from_str(release)?,
                    RpcTransactionConfig {
                        encoding: Some(encoding),
                        commitment: Some(CommitmentConfig::confirmed()),
                        max_supported_transaction_version: Some(1),
                    },
                )
                .await
                .map_err(Box::<dyn std::error::Error>::from)
        }
    };
    let raw = fetch(UiTransactionEncoding::Base64).await?;
    let EncodedTransaction::Binary(data, _) = &raw.transaction.transaction else {
        return Err("expected a binary transaction".into());
    };
    let size = base64::engine::general_purpose::STANDARD
        .decode(data)?
        .len();
    let tx = fetch(UiTransactionEncoding::Json).await?;
    let v1 = tx.transaction.version == Some(TransactionVersion::Number(1));
    let EncodedTransaction::Json(ui) = &tx.transaction.transaction else {
        return Err("expected a json transaction".into());
    };
    let UiMessage::Raw(message) = &ui.message else {
        return Err("expected a raw message".into());
    };
    let accounts = message.account_keys.len();
    let cu = tx
        .transaction
        .meta
        .as_ref()
        .and_then(|meta| Option::<u64>::from(meta.compute_units_consumed.clone()))
        .ok_or("release has no compute units")?;
    Ok((size, accounts, cu, v1))
}

/// 29. A confidential release of a fee and hook mint is a v1 transaction inside 4,096 bytes
/// and 64 accounts, and uses less compute than the limit the sender sets.
#[tokio::test(flavor = "multi_thread")]
async fn test_29_fee_hook_release_size_and_compute() -> TestResult {
    let env = Env::start("conf_29").await?;
    let mint = env
        .mint(MintSpec {
            fee: Some(FEE),
            hook: Hook::WithExtra,
            ..MintSpec::default()
        })
        .await?;
    let mut stack = Stack::start(&env).await;
    let user = env.user().await?;

    let deposit = env
        .deposit_confidential(&user, mint, 1_000_000, false)
        .await?;
    let credited = wait_credited(&env, &deposit).await?;
    assert_eq!(credited.amount, 1_000_000 - FEE.1, "net of the capped fee");

    let withdrawal = env.withdraw(&user, mint, 300_000, true).await?;
    let row = wait_released(&env, &withdrawal).await?;
    let release = row.counterpart.ok_or("no release signature")?;
    let (size, accounts, cu, v1) = release_footprint(&env, &release).await?;
    println!("MEASURED fee+hook confidential release: {size} bytes, {accounts} accounts, {cu} CU");
    assert!(v1, "confidential releases are v1 transactions");
    assert!(size <= 4_096, "{size} bytes");
    assert!(accounts <= 64, "{accounts} accounts");
    assert!(cu < u64::from(CU_CONFIDENTIAL_RELEASE), "{cu} CU");
    // Token-2022 charges the transfer fee on the release too, as on a public one.
    assert_eq!(env.user_pending(&user, &mint).await?, 300_000 - 3_000);
    env.reconcile(&mut stack).await?;

    stack.stop().await;
    Ok(())
}

/// 30. Confidential deposits (no fee, fee mint, sent through a CPI wrapper) are credited on
/// the channel with the net amount, and custody reconciles exactly.
#[tokio::test(flavor = "multi_thread")]
async fn test_30_confidential_deposits_are_credited() -> TestResult {
    let env = Env::start("conf_30").await?;
    let plain = env.mint(MintSpec::default()).await?;
    let fee = env
        .mint(MintSpec {
            fee: Some(FEE),
            ..MintSpec::default()
        })
        .await?;
    let mut stack = Stack::start(&env).await;
    let user = env.user().await?;

    let a = env
        .deposit_confidential(&user, plain, 1_000_000, false)
        .await?;
    let b = env
        .deposit_confidential(&user, fee, 1_000_000, false)
        .await?;
    let c = env
        .deposit_confidential(&user, plain, 300_000, true)
        .await?;

    for (signature, net) in [(a, 1_000_000), (b, 1_000_000 - FEE.1), (c, 300_000)] {
        let row = wait_credited(&env, &signature).await?;
        assert!(
            row.confidential,
            "{signature} is a confidential deposit row"
        );
        assert_eq!(row.amount, net, "{signature} credits the net amount");
    }
    assert_eq!(env.user_public(&user, &plain).await?, 1_300_000);
    assert_eq!(env.user_public(&user, &fee).await?, 1_000_000 - FEE.1);

    assert_eq!(
        env.custody_verdict(&plain).await?,
        ConfidentialCustody::Counted(1_300_000)
    );
    assert_eq!(
        env.custody_verdict(&fee).await?,
        ConfidentialCustody::Counted(1_000_000 - FEE.1)
    );
    env.reconcile(&mut stack).await?;

    stack.stop().await;
    Ok(())
}

/// 31. `ConfidentialWithdrawFunds` is released into the user's confidential balance after
/// the sender applies the escrow's pending deposits; the release is observed, back to back
/// releases both land, and custody reconciles.
#[tokio::test(flavor = "multi_thread")]
async fn test_31_confidential_withdrawals_are_released() -> TestResult {
    let env = Env::start("conf_31").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let user = env.user().await?;

    let deposit = env
        .deposit_confidential(&user, mint, 1_000_000, false)
        .await?;
    wait_credited(&env, &deposit).await?;

    let first = env.withdraw(&user, mint, 400_000, true).await?;
    let row = wait_released(&env, &first).await?;
    assert!(row.confidential);
    assert_eq!(env.user_pending(&user, &mint).await?, 400_000);
    let nonce = row.nonce.ok_or("no nonce")?;
    let observed = wait_until("the release to be observed", || async {
        env.observed_release(nonce).await
    })
    .await?;
    assert_eq!(Some(observed.0), row.counterpart);
    assert_eq!(observed.1, Some(400_000));

    // Back to back: both burns are in before the first release.
    let second = env.withdraw(&user, mint, 100_000, true).await?;
    let third = env.withdraw(&user, mint, 200_000, true).await?;
    wait_released(&env, &second).await?;
    wait_released(&env, &third).await?;
    assert_eq!(env.user_pending(&user, &mint).await?, 700_000);

    assert_eq!(
        env.custody_verdict(&mint).await?,
        ConfidentialCustody::Counted(300_000)
    );
    env.reconcile(&mut stack).await?;

    stack.stop().await;
    Ok(())
}

/// Applies the sender ran on `mint`, by trigger (release, background, repair).
fn applies(mint: &Pubkey, trigger: &str) -> f64 {
    private_channel_indexer::metrics::OPERATOR_CONFIDENTIAL_APPLIES
        .with_label_values(&[&mint.to_string(), trigger])
        .get()
}

/// Wait until the sender has counted an apply for `trigger`: it counts after its own
/// confirmation poll, which can trail the chain by a few seconds.
async fn wait_applied(mint: &Pubkey, trigger: &str) -> TestResult {
    wait_until(&format!("a {trigger} apply"), || async {
        Ok((applies(mint, trigger) >= 1.0).then_some(()))
    })
    .await
}

/// Sample reconciliation's verdict for `mint` until `done`; returns every hold label seen,
/// and fails on the first mismatch.
async fn watch_verdicts(env: &Env, mint: Pubkey, done: &AtomicBool) -> TestResult<Vec<String>> {
    let mut seen = Vec::new();
    while !done.load(Ordering::SeqCst) {
        match env.custody_verdict(&mint).await? {
            ConfidentialCustody::Mismatch(why) => {
                return Err(format!("reconciliation went dark: {why}").into())
            }
            ConfidentialCustody::Held(hold) => seen.push(hold.label().to_string()),
            ConfidentialCustody::Counted(_) => seen.push("counted".into()),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(seen)
}

/// Wait until the sender has repaired the escrow's AES copy and custody counts again.
async fn wait_counted(env: &Env, mint: &Pubkey) -> TestResult<u64> {
    wait_until("custody to be counted", || async {
        Ok(match env.custody_verdict(mint).await? {
            ConfidentialCustody::Counted(total) => Some(total),
            _ => None,
        })
    })
    .await
}

/// 32. Deposits keep landing while the sender applies and releases. A deposit that races
/// an apply leaves the AES copy stale: reconciliation counts custody exactly from the gap
/// (never held, never a mismatch) and the sender repairs the copy.
#[tokio::test(flavor = "multi_thread")]
async fn test_32_deposits_racing_applies_are_repaired_never_dark() -> TestResult {
    let env = Env::start("conf_32").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let (alice, bob) = (env.user().await?, env.user().await?);
    let seed = env
        .deposit_confidential(&alice, mint, 3_000_000, false)
        .await?;
    wait_credited(&env, &seed).await?;

    let done = AtomicBool::new(false);
    let watch = watch_verdicts(&env, mint, &done);
    let scenario = async {
        // Bob deposits nonstop while three releases each need an apply first.
        let racing = AtomicBool::new(true);
        let deposits = async {
            let mut sent = Vec::new();
            while racing.load(Ordering::SeqCst) {
                sent.push(env.deposit_confidential(&bob, mint, 25_000, false).await?);
            }
            TestResult::Ok(sent)
        };
        let releases = async {
            let mut total = 0;
            for amount in [700_000u64, 800_000, 900_000] {
                let withdrawal = env.withdraw(&alice, mint, amount, true).await?;
                wait_released(&env, &withdrawal).await?;
                total += amount;
            }
            racing.store(false, Ordering::SeqCst);
            TestResult::Ok(total)
        };
        let (sent, released) = tokio::try_join!(deposits, releases)?;
        for signature in &sent {
            wait_credited(&env, signature).await?;
        }
        assert_eq!(env.user_pending(&alice, &mint).await?, released);

        // A race forced on top, so the repair path always runs. The stale copy is counted
        // exactly from its gap, before and after the repair.
        env.raced_apply(mint, &bob, 40_000).await?;
        let deposited = 3_000_000 + 25_000 * sent.len() as u64 + 40_000;
        assert_eq!(
            env.custody_verdict(&mint).await?,
            ConfidentialCustody::Counted(deposited - released)
        );
        let counted = wait_counted(&env, &mint).await?;
        done.store(true, Ordering::SeqCst);
        TestResult::Ok((sent.len(), released, counted))
    };
    let (seen, (racing_deposits, released, counted)) = tokio::try_join!(watch, scenario)?;
    println!(
        "racing deposits {racing_deposits}, verdicts seen {:?}, repairs {}",
        seen.iter().collect::<std::collections::BTreeSet<_>>(),
        applies(&mint, "repair")
    );
    assert!(
        seen.iter().all(|label| label == "counted"),
        "a raced copy is never held: {seen:?}"
    );
    wait_applied(&mint, "repair").await?;
    let deposited = 3_000_000 + 25_000 * racing_deposits as u64 + 40_000;
    assert_eq!(counted, deposited - released);
    env.reconcile(&mut stack).await?;

    stack.stop().await;
    Ok(())
}

/// 33. Pending at or above 2^47 is applied by the sender's background check on its own,
/// before pending hi can leave the 32-bit search.
#[tokio::test(flavor = "multi_thread")]
async fn test_33_large_pending_is_applied_in_the_background() -> TestResult {
    let env = Env::start("conf_33").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let user = env.user().await?;

    let amount = (1u64 << 47) + 12_345;
    let deposit = env.deposit_confidential(&user, mint, amount, false).await?;
    assert_eq!(wait_credited(&env, &deposit).await?.amount, amount);

    // No release asks for it: only the background threshold applies it.
    let available = wait_until("the background apply", || async {
        let view = env.escrow_view(&mint).await?;
        Ok(if view.pending_credit_counter == 0 {
            env.escrow_available(&mint).await?
        } else {
            None
        })
    })
    .await?;
    assert_eq!(available, amount);
    wait_applied(&mint, "background").await?;
    assert_eq!(
        env.custody_verdict(&mint).await?,
        ConfidentialCustody::Counted(amount)
    );
    env.reconcile(&mut stack).await?;

    stack.stop().await;
    Ok(())
}

/// Compute the transaction `signature` used.
async fn compute_of(env: &Env, signature: &str) -> TestResult<u64> {
    let tx = env
        .client
        .get_transaction_with_config(
            &Signature::from_str(signature)?,
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Json),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(1),
            },
        )
        .await?;
    tx.transaction
        .meta
        .and_then(|meta| Option::<u64>::from(meta.compute_units_consumed))
        .ok_or_else(|| "no compute units".into())
}

/// 34. Rebalance both ways through request rows run by the sender, then public and
/// confidential releases are paid from the moved balances.
#[tokio::test(flavor = "multi_thread")]
async fn test_34_rebalance_both_ways() -> TestResult {
    let env = Env::start("conf_34").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let (alice, bob) = (env.user().await?, env.user().await?);

    let deposit = env
        .deposit_confidential(&alice, mint, 2_000_000, false)
        .await?;
    wait_credited(&env, &deposit).await?;
    let public = env.deposit_public(&bob, mint, 200_000).await?;
    wait_credited(&env, &public).await?;
    // The first release applies the pending deposit, so there is an available balance.
    let first = env.withdraw(&alice, mint, 100_000, true).await?;
    wait_released(&env, &first).await?;

    let to_public = env.request_rebalance(mint, true, 800_000).await?;
    env.wait_rebalance(to_public, "completed").await?;
    assert_eq!(env.escrow_view(&mint).await?.public_amount, 1_000_000);
    let moved: String =
        sqlx::query_scalar("SELECT signature FROM rebalance_requests WHERE id = $1")
            .bind(to_public)
            .fetch_one(&env.pool)
            .await?;
    println!(
        "MEASURED MoveToPublic: {} CU",
        compute_of(&env, &moved).await?
    );

    let public_release = env.withdraw(&alice, mint, 900_000, false).await?;
    assert!(!wait_released(&env, &public_release).await?.confidential);

    let to_confidential = env.request_rebalance(mint, false, 100_000).await?;
    env.wait_rebalance(to_confidential, "completed").await?;
    assert_eq!(env.escrow_view(&mint).await?.public_amount, 0);

    let last = env.withdraw(&alice, mint, 650_000, true).await?;
    wait_released(&env, &last).await?;
    assert_eq!(env.user_pending(&alice, &mint).await?, 100_000 + 650_000);

    assert_eq!(wait_counted(&env, &mint).await?, 550_000);
    env.reconcile(&mut stack).await?;
    stack.stop().await;
    Ok(())
}

/// The parked row of `signature`, once the operator alerted with `label`.
async fn wait_parked(
    stack: &mut Stack,
    env: &Env,
    signature: &str,
    label: &str,
) -> TestResult<Row> {
    stack.wait_alert(label).await?;
    env.wait_status(signature, "manual_review").await
}

/// 35. Each park and its recovery: a destination not ready (configure it, re-arm), public
/// balance low (rebalance to public, re-arm), confidential balance low (rebalance to
/// confidential, re-arm).
#[tokio::test(flavor = "multi_thread")]
async fn test_35_parks_destination_and_balances_recover() -> TestResult {
    let env = Env::start("conf_35a").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let (alice, carol) = (env.user().await?, env.user().await?);

    let deposit = env
        .deposit_confidential(&alice, mint, 600_000, false)
        .await?;
    wait_credited(&env, &deposit).await?;
    // Carol's Solana account is never configured for confidential transfers.
    let carols = env.deposit_public(&carol, mint, 100_000).await?;
    wait_credited(&env, &carols).await?;

    let not_ready = env.withdraw(&carol, mint, 50_000, true).await?;
    let row = wait_parked(&mut stack, &env, &not_ready, "destination_not_ready").await?;
    env.configure_user_account(&carol, mint).await?;
    env.rearm(row.id, false).await?;
    wait_released(&env, &not_ready).await?;
    assert_eq!(env.user_pending(&carol, &mint).await?, 50_000);

    let ok = env.withdraw(&alice, mint, 100_000, true).await?;
    wait_released(&env, &ok).await?;

    // Custody is 100,000 public: a 200,000 public release parks.
    let short_public = env.withdraw(&alice, mint, 200_000, false).await?;
    let row = wait_parked(&mut stack, &env, &short_public, "public_balance_low").await?;
    let to_public = env.request_rebalance(mint, true, 450_000).await?;
    env.wait_rebalance(to_public, "completed").await?;
    env.rearm(row.id, false).await?;
    wait_released(&env, &short_public).await?;

    // Nothing is left confidential: a confidential release parks.
    let short_confidential = env.withdraw(&alice, mint, 200_000, true).await?;
    let row = wait_parked(
        &mut stack,
        &env,
        &short_confidential,
        "confidential_balance_low",
    )
    .await?;
    let to_confidential = env.request_rebalance(mint, false, 300_000).await?;
    env.wait_rebalance(to_confidential, "completed").await?;
    env.rearm(row.id, false).await?;
    wait_released(&env, &short_confidential).await?;
    assert_eq!(env.user_pending(&alice, &mint).await?, 300_000);

    assert_eq!(wait_counted(&env, &mint).await?, 150_000);
    env.reconcile(&mut stack).await?;
    stack.stop().await;
    Ok(())
}

/// 35. Parks that end in a public release: a hook switched to refuse `u64::MAX`
/// (rebalance to public, release publicly) and a mint whose escrow account was never
/// configured (release publicly).
#[tokio::test(flavor = "multi_thread")]
async fn test_35_parks_hook_and_unconfigured_mint_release_publicly() -> TestResult {
    let env = Env::start("conf_35b").await?;
    let hooked = env
        .mint(MintSpec {
            hook: Hook::BaseOnly,
            ..MintSpec::default()
        })
        .await?;
    let unconfigured = env
        .mint(MintSpec {
            unconfigured: true,
            ..MintSpec::default()
        })
        .await?;
    let mut stack = Stack::start(&env).await;
    let (alice, bob) = (env.user().await?, env.user().await?);

    let deposit = env
        .deposit_confidential(&alice, hooked, 500_000, false)
        .await?;
    wait_credited(&env, &deposit).await?;
    // The issuer switches the hook: with an empty list the fixture refuses u64::MAX.
    helpers::init_extra_account_meta_list_with(&env.client, &admin(), &hooked, &[]).await?;
    let refused = env.withdraw(&alice, hooked, 100_000, true).await?;
    let row = wait_parked(&mut stack, &env, &refused, "hook_unresolvable").await?;
    let to_public = env.request_rebalance(hooked, true, 100_000).await?;
    env.wait_rebalance(to_public, "completed").await?;
    let before = env.user_public(&alice, &hooked).await?;
    env.rearm(row.id, true).await?;
    wait_released(&env, &refused).await?;
    assert_eq!(env.user_public(&alice, &hooked).await?, before + 100_000);

    let public = env.deposit_public(&bob, unconfigured, 100_000).await?;
    wait_credited(&env, &public).await?;
    let unconfigured_release = env.withdraw(&bob, unconfigured, 60_000, true).await?;
    let row = wait_parked(
        &mut stack,
        &env,
        &unconfigured_release,
        "confidential_mint_not_configured",
    )
    .await?;
    let before = env.user_public(&bob, &unconfigured).await?;
    env.rearm(row.id, true).await?;
    wait_released(&env, &unconfigured_release).await?;
    assert_eq!(env.user_public(&bob, &unconfigured).await?, before + 60_000);

    assert_eq!(wait_counted(&env, &hooked).await?, 400_000);
    env.reconcile(&mut stack).await?;
    stack.stop().await;
    Ok(())
}

/// A confidential withdrawal of `amount`, with the withdraw operator killed once `at` says
/// so and started again; returns the settled row.
async fn release_across_a_crash(
    env: &Env,
    stack: &mut Stack,
    user: &User,
    mint: Pubkey,
    amount: u64,
    at: CrashAt,
) -> TestResult<Row> {
    let pending_before = env.user_pending(user, &mint).await?;
    let withdrawal = env.withdraw(user, mint, amount, true).await?;
    let row = wait_until("the withdrawal row", || async {
        env.row(&withdrawal).await
    })
    .await?;
    match at {
        CrashAt::Journaled => {
            wait_until("a journaled attempt", || async {
                Ok((!env.journal(row.id).await?.is_empty()).then_some(()))
            })
            .await?;
        }
        CrashAt::Sent => {
            let signature = wait_until("a journaled attempt", || async {
                Ok(env.journal(row.id).await?.first().cloned())
            })
            .await?;
            let signature = Signature::from_str(&signature)?;
            wait_until("the attempt to reach the node", || async {
                let status = env.client.get_signature_statuses(&[signature]).await?;
                Ok(status.value.first().cloned().flatten().map(|_| ()))
            })
            .await?;
        }
        CrashAt::Confirmed => {
            wait_until("the release to land", || async {
                Ok((env.user_pending(user, &mint).await? > pending_before).then_some(()))
            })
            .await?;
        }
    }
    let status = env.row(&withdrawal).await?.map(|r| r.status);
    println!("killing the withdraw operator at {at:?}, row {status:?}");
    stack.restart_service(Service::WithdrawOperator).await;
    let row = wait_released(env, &withdrawal).await?;
    assert_eq!(
        env.user_pending(user, &mint).await?,
        pending_before + amount
    );
    Ok(row)
}

#[derive(Clone, Copy, Debug)]
enum CrashAt {
    Journaled,
    Sent,
    Confirmed,
}

/// 36. Kill the withdraw operator after the journal, after the send and after the confirm,
/// and between an apply and its repair; kill the escrow indexer with a deposit unindexed.
/// Every nonce is paid once, nothing is reminted, the AES copy is repaired and custody is
/// exact.
#[tokio::test(flavor = "multi_thread")]
async fn test_36_crash_points_recover_exactly() -> TestResult {
    let env = Env::start("conf_36").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let (alice, bob) = (env.user().await?, env.user().await?);
    let deposit = env
        .deposit_confidential(&alice, mint, 2_000_000, false)
        .await?;
    wait_credited(&env, &deposit).await?;

    let mut nonces = Vec::new();
    for (amount, at) in [
        (300_000, CrashAt::Journaled),
        (200_000, CrashAt::Sent),
        (100_000, CrashAt::Confirmed),
    ] {
        let row = release_across_a_crash(&env, &mut stack, &alice, mint, amount, at).await?;
        nonces.push(row.nonce.ok_or("no nonce")?);
    }

    // Between an apply and its repair: the race lands while the operator is down.
    stack.stop_service(Service::WithdrawOperator).await;
    env.raced_apply(mint, &bob, 50_000).await?;
    stack.start_service(Service::WithdrawOperator).await;
    // Custody counts from the gap at once; the restarted sender still repairs the copy.
    wait_counted(&env, &mint).await?;
    wait_until("the AES copy to be repaired", || async {
        Ok((!env.escrow_view(&mint).await?.aes_known_stale()).then_some(()))
    })
    .await?;

    // The escrow indexer dies with a deposit not yet indexed.
    stack.stop_service(Service::EscrowIndexer).await;
    let unindexed = env.deposit_confidential(&bob, mint, 70_000, false).await?;
    // Boot reads custody finalized at or past the confirmed checkpoint and gives up after a
    // few seconds, so let finalization catch up first.
    let checkpoint = helpers::db::get_checkpoint_slot(&env.pool, "escrow")
        .await?
        .unwrap_or(0);
    wait_until("finalization to pass the indexer checkpoint", || async {
        let finalized = env
            .client
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;
        Ok((finalized >= checkpoint).then_some(()))
    })
    .await?;
    stack.start_service(Service::EscrowIndexer).await;
    wait_credited(&env, &unindexed).await?;
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions WHERE signature = $1")
        .bind(unindexed.to_string())
        .fetch_one(&env.pool)
        .await?;
    assert_eq!(rows, 1, "the deposit is indexed once");

    for nonce in &nonces {
        wait_until("the release to be observed", || async {
            env.observed_release(*nonce).await
        })
        .await?;
        assert_eq!(env.landed_releases(*nonce).await?, 1);
    }
    assert_eq!(env.user_pending(&alice, &mint).await?, 600_000);
    assert_eq!(
        env.reminted_or_review().await?,
        0,
        "nothing reminted or parked"
    );
    // Channel: 2,000,000 credited, 600,000 burned, and no remint gave any back.
    assert_eq!(env.user_public(&alice, &mint).await?, 1_400_000);
    assert_eq!(
        wait_counted(&env, &mint).await?,
        2_000_000 + 50_000 + 70_000 - 600_000
    );
    env.reconcile(&mut stack).await?;
    stack.stop().await;
    Ok(())
}

/// 37. With the escrow indexer stopped, confidential releases and their applies go on: the
/// sender reads custody from the escrow account, not from the indexer. Custody still counts;
/// observed releases catch up once the indexer is back.
#[tokio::test(flavor = "multi_thread")]
async fn test_37_releases_continue_with_the_indexer_stopped() -> TestResult {
    let env = Env::start("conf_37").await?;
    let mint = env.mint(MintSpec::default()).await?;
    let mut stack = Stack::start(&env).await;
    let user = env.user().await?;
    let deposit = env
        .deposit_confidential(&user, mint, 1_000_000, false)
        .await?;
    wait_credited(&env, &deposit).await?;

    stack.stop_service(Service::EscrowIndexer).await;
    let first = env.withdraw(&user, mint, 300_000, true).await?;
    let second = env.withdraw(&user, mint, 200_000, true).await?;
    let a = wait_released(&env, &first).await?;
    let b = wait_released(&env, &second).await?;
    assert_eq!(env.user_pending(&user, &mint).await?, 500_000);
    wait_applied(&mint, "release").await?;
    assert_eq!(
        env.custody_verdict(&mint).await?,
        ConfidentialCustody::Counted(500_000)
    );

    stack.start_service(Service::EscrowIndexer).await;
    for row in [a, b] {
        let nonce = row.nonce.ok_or("no nonce")?;
        wait_until("the release to be observed", || async {
            env.observed_release(nonce).await
        })
        .await?;
    }
    env.reconcile(&mut stack).await?;
    stack.stop().await;
    Ok(())
}

/// 38. A mint configured under a key the instance does not hold: its confidential deposit
/// is kept undecrypted and never credited, the mint is held, its confidential withdrawals
/// park, and the other confidential mint and public deposits are unaffected. Recovery
/// refuses the mint, so nothing is ever credited from it.
#[tokio::test(flavor = "multi_thread")]
async fn test_38_wrong_key_poisons_one_mint_and_recovery_is_refused() -> TestResult {
    use private_channel_indexer::operator::confidential_custody::ConfidentialHold;
    let env = Env::start("conf_38").await?;
    let good = env.mint(MintSpec::default()).await?;
    let foreign = foreign_keys();
    let poisoned = env.mint_under(MintSpec::default(), foreign.clone()).await?;
    let mut stack = Stack::start(&env).await;
    let (alice, bob) = (env.user().await?, env.user().await?);

    let undecryptable = env
        .deposit_confidential(&alice, poisoned, 400_000, false)
        .await?;
    let row = wait_until("the undecrypted deposit row", || async {
        env.row(&undecryptable.to_string()).await
    })
    .await?;
    assert!(row.undecrypted && row.confidential);
    let public = env.deposit_public(&bob, poisoned, 100_000).await?;
    wait_credited(&env, &public).await?;
    let fine = env
        .deposit_confidential(&alice, good, 300_000, false)
        .await?;
    wait_credited(&env, &fine).await?;
    assert_eq!(
        env.row(&undecryptable.to_string()).await?.map(|r| r.status),
        Some("pending".to_string()),
        "an undecrypted deposit is never minted"
    );
    assert_eq!(
        env.custody_verdict(&poisoned).await?,
        ConfidentialCustody::Held(ConfidentialHold::Poisoned)
    );

    let blocked = env.withdraw(&bob, poisoned, 50_000, true).await?;
    wait_parked(&mut stack, &env, &blocked, "confidential_mint_poisoned").await?;
    let released = env.withdraw(&alice, good, 100_000, true).await?;
    wait_released(&env, &released).await?;

    // A foreign-key mint is never recovered into the channel: recovery with the instance key
    // refuses, the deposit stays undecrypted and uncredited, and the mint stays held.
    stack.stop_all().await;
    let source = private_channel_indexer::indexer::confidential::RpcEscrowKeySource {
        rpc: std::sync::Arc::new(
            private_channel_indexer::operator::utils::RpcClientWithRetry::with_retry_config(
                env.rpc_url.clone(),
                private_channel_indexer::operator::utils::RetryConfig::default(),
                CommitmentConfig::confirmed(),
            ),
        ),
    };
    let refused = private_channel_indexer::indexer::confidential::run_recover(
        &env.storage,
        &env.keys.keys,
        &source,
        &env.instance,
        &poisoned.to_string(),
        private_channel_indexer::storage::common::storage::live_lock::LIVE_LOCK_HEARTBEAT_INTERVAL,
    )
    .await;
    assert!(
        matches!(
            refused,
            Err(private_channel_indexer::error::IndexerError::ConfidentialKeys { .. })
        ),
        "{refused:?}"
    );
    stack.start_all().await;
    let row = env
        .row(&undecryptable.to_string())
        .await?
        .expect("row kept");
    assert!(row.undecrypted);
    assert_eq!(row.status, "pending");
    assert_eq!(
        env.custody_verdict(&poisoned).await?,
        ConfidentialCustody::Held(ConfidentialHold::Poisoned)
    );
    assert_eq!(wait_counted(&env, &good).await?, 200_000);
    env.reconcile(&mut stack).await?;
    stack.stop().await;
    Ok(())
}
