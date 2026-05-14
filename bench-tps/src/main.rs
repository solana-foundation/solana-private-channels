//! private-channel-bench-tps — PrivateChannel pipeline load testing binary
//!
//! # Overview
//!
//! The binary supports three subcommands for testing different parts of the PrivateChannel pipeline:
//!
//! **`transfer`** (default flow)
//! Tests the PrivateChannel SPL transfer pipeline.
//!
//! **`deposit`**
//! Tests the Solana escrow deposit flow.
//!
//! **`withdraw`**
//! Tests the PrivateChannel withdraw-burn flow.
//!
//! Each flow follows the same three-phase structure:
//!
//! **Phase 1 — Setup**
//! Creates all on-chain state required by the load phase.
//!
//! **Phase 2 — Background tasks**
//! Blockhash poller + metrics sampler run concurrently.
//!
//! **Phase 3 — Load**
//! Generator task signs batches; sender threads dispatch them.

mod args;
mod background;
mod bench_metrics;
mod load;
mod load_deposit;
mod load_withdraw;
mod rpc;
mod setup;
mod setup_deposit;
mod setup_withdraw;
mod types;

use {
    anyhow::Result,
    args::{Cli, DerivePdaArgs, SubCommand},
    background::{run_blockhash_poller, run_metrics_sampler, run_operator_mints_sampler},
    bench_metrics::{
        bench_metrics_init, {FLOW_DEPOSIT, FLOW_TRANSFER, FLOW_WITHDRAW},
    },
    clap::Parser,
    load::{build_destinations, run_generator, run_sender_task},
    load_deposit::{run_deposit_generator, run_deposit_sender_thread},
    load_withdraw::{run_withdraw_generator, run_withdraw_sender_thread},
    private_channel_core::client::load_keypair,
    setup_deposit::find_instance_pda,
    solana_sdk::{signature::Keypair, signer::Signer},
    std::{
        collections::VecDeque,
        fs::write,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Condvar, Mutex,
        },
    },
    tokio::time::Duration,
    tokio_util::sync::CancellationToken,
    tracing::info,
    types::{BatchQueue, BenchConfig},
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.subcommand {
        SubCommand::Transfer(args) => run_transfer(args).await,
        SubCommand::Deposit(args) => run_deposit(args).await,
        SubCommand::Withdraw(args) => run_withdraw(args).await,
        SubCommand::DerivePda(args) => run_derive_pda(args),
    }
}

// ---------------------------------------------------------------------------
// Transfer subcommand
// ---------------------------------------------------------------------------

async fn run_transfer(args: args::TransferArgs) -> Result<()> {
    init_logging(&args.log_level);
    bench_metrics_init();
    if let Some(port) = args.metrics_port {
        private_channel_metrics::start_metrics_server(port);
    }

    info!(
        rpc_url = %args.rpc_url,
        accounts = args.accounts,
        threads = args.threads,
        batch_size = args.batch_size,
        duration = args.duration,
        "Starting private-channel-bench-tps (transfer)",
    );

    // -------------------------------------------------------------------------
    // Phase 1 — Setup
    //
    // Creates the mint, ATAs, and token balances.  The function blocks until
    // all setup transactions are confirmed on-chain before returning.
    // -------------------------------------------------------------------------
    let setup_result = setup::run_setup_phase(
        &args.rpc_url,
        &args.admin_keypair,
        args.accounts,
        args.initial_balance,
    )
    .await?;

    // -------------------------------------------------------------------------
    // Phase 2 + 3 — Background tasks and load generation
    //
    // Build the shared config and queue, then spawn background tasks (blockhash
    // poller, metrics sampler) and the generator + sender threads concurrently.
    // The main task waits for `args.duration` seconds, then cancels everything.
    // -------------------------------------------------------------------------
    // Split accounts into sender (first half) and receiver (second half) pools.
    // num_conflict_groups controls how many distinct receivers are used;
    // defaults to half of accounts for zero sequencer contention.
    let num_conflict_groups = args.num_conflict_groups.unwrap_or(args.accounts / 2);
    let (senders, destinations) = build_destinations(&setup_result.keypairs, num_conflict_groups);
    let config = Arc::new(BenchConfig {
        mint: setup_result.mint,
        accounts: senders,
        destinations,
    });

    // Bounded async_channel replaces the old Mutex<VecDeque> + Condvar queue.
    // The channel capacity provides backpressure — the generator awaits when
    // all slots are occupied, preventing unbounded memory growth. The MPMC
    // fan-out lets every sender task hold its own cloned receiver, so
    // cancellation interrupts every task immediately without the shared-mutex
    // cascade that `tokio::sync::mpsc` would require.
    let (batch_tx, batch_rx) =
        async_channel::bounded::<Vec<solana_sdk::transaction::Transaction>>(types::MAX_QUEUE_DEPTH);

    let cancel = CancellationToken::new();

    // Blockhash poller: keeps BenchState::current_blockhash fresh.
    let bh_handle = tokio::spawn(run_blockhash_poller(
        args.rpc_url.clone(),
        Arc::clone(&setup_result.state),
        cancel.clone(),
    ));

    // Metrics sampler: logs instantaneous TPS every second, returns
    // (start_tx_count, end_tx_count) for the final drop-rate calculation.
    let load_end = tokio::time::Instant::now() + Duration::from_secs(args.duration);
    let metrics_handle = tokio::spawn(run_metrics_sampler(
        args.rpc_url.clone(),
        load_end,
        cancel.clone(),
        FLOW_TRANSFER,
    ));

    // Generator: signs batches of `batch_size` transactions and sends them
    // through the mpsc channel for sender tasks to consume.
    let gen_handle = tokio::spawn(run_generator(
        Arc::clone(&config),
        Arc::clone(&setup_result.state),
        batch_tx,
        args.batch_size,
        cancel.clone(),
    ));

    // Sender tasks: each pops one batch and sends all transactions concurrently
    // via `join_all` on the async RPC client.  Tokio tasks replace OS threads.
    let sent_count = Arc::new(AtomicU64::new(0));
    let mut sender_handles = Vec::with_capacity(args.threads);
    for _ in 0..args.threads {
        let rpc_url = args.rpc_url.clone();
        let rx = batch_rx.clone();
        let c = cancel.clone();
        let sc = Arc::clone(&sent_count);
        let sleep_ms = args.sender_sleep_ms;
        sender_handles.push(tokio::spawn(run_sender_task(rpc_url, rx, c, sc, sleep_ms)));
    }
    // Drop the main-task receiver clone; each sender task owns its own. When
    // the generator exits and drops batch_tx the channel closes and every
    // sender's recv() returns Err immediately.
    drop(batch_rx);

    info!(
        duration_secs = args.duration,
        threads = args.threads,
        batch_size = args.batch_size,
        "Transfer load phase started"
    );
    tokio::time::sleep(Duration::from_secs(args.duration)).await;

    info!("Transfer load phase complete — shutting down");
    cancel.cancel();

    // Await all async tasks.
    let _ = gen_handle.await;
    let _ = bh_handle.await;
    let (start_tx_count, end_tx_count) = metrics_handle.await.unwrap_or((0, 0));
    for h in sender_handles {
        let _ = h.await;
    }

    // -------------------------------------------------------------------------
    // Final summary
    //
    // `sent`    — transactions accepted by the RPC server (from AtomicU64)
    // `landed`  — transactions settled by the pipeline during the test window
    //             (`getTransactionCount` sampled at t=duration by the metrics
    //             sampler). Reflects steady-state capacity during the run.
    // `dropped` — sent - landed. Includes both true drops (rejected by dedup /
    //             sigverify / sequencer) and any in-flight tail still in the
    //             pipeline at shutdown.
    // -------------------------------------------------------------------------
    let sent = sent_count.load(Ordering::Relaxed);
    let landed = end_tx_count.saturating_sub(start_tx_count);
    let dropped = sent.saturating_sub(landed);
    let drop_rate = if sent > 0 {
        dropped as f64 / sent as f64 * 100.0
    } else {
        0.0
    };
    let tps = landed as f64 / args.duration as f64;
    info!(
        duration_secs = args.duration,
        sent,
        landed,
        dropped,
        drop_rate = format!("{drop_rate:.1}%"),
        tps = format!("{tps:.1}"),
        "Final summary (transfer)",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Deposit subcommand
// ---------------------------------------------------------------------------

async fn run_deposit(args: args::DepositArgs) -> Result<()> {
    init_logging(&args.log_level);
    bench_metrics_init();
    if let Some(port) = args.metrics_port {
        private_channel_metrics::start_metrics_server(port);
    }

    info!(
        solana_rpc_url = %args.solana_rpc_url,
        accounts = args.accounts,
        threads = args.threads,
        duration = args.duration,
        "Starting private-channel-bench-tps (deposit)",
    );

    let deposit_config = setup_deposit::run_setup_deposit_phase(
        &args.solana_rpc_url,
        &args.private_channel_rpc_url,
        &args.admin_keypair,
        args.instance_seed_keypair.as_deref(),
        args.accounts,
        args.initial_balance,
    )
    .await?;

    let deposit_config = Arc::new(deposit_config);

    let queue: BatchQueue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));

    let cancel = CancellationToken::new();

    let bh_handle = tokio::spawn(run_blockhash_poller(
        args.solana_rpc_url.clone(),
        Arc::clone(&deposit_config.state),
        cancel.clone(),
    ));

    let load_end = tokio::time::Instant::now() + Duration::from_secs(args.duration);
    let metrics_handle = tokio::spawn(run_metrics_sampler(
        args.solana_rpc_url.clone(),
        load_end,
        cancel.clone(),
        FLOW_DEPOSIT,
    ));

    let gen_handle = tokio::spawn(run_deposit_generator(
        Arc::clone(&deposit_config),
        Arc::clone(&deposit_config.state),
        Arc::clone(&queue),
        args.threads,
        cancel.clone(),
    ));

    let sent_count = Arc::new(AtomicU64::new(0));
    let mut sender_handles = Vec::with_capacity(args.threads);
    for _ in 0..args.threads {
        let rpc_url = args.solana_rpc_url.clone();
        let q = Arc::clone(&queue);
        let c = cancel.clone();
        let sc = Arc::clone(&sent_count);
        let ms = args.sender_sleep_ms;
        sender_handles.push(std::thread::spawn(move || {
            run_deposit_sender_thread(rpc_url, q, c, sc, ms)
        }));
    }

    let operator_handle = args.operator_metrics_url.clone().map(|url| {
        tokio::spawn(run_operator_mints_sampler(
            url,
            load_end,
            cancel.clone(),
            "escrow",
        ))
    });

    info!(
        duration_secs = args.duration,
        threads = args.threads,
        "Deposit load phase started"
    );
    tokio::time::sleep(Duration::from_secs(args.duration)).await;

    info!("Deposit load phase complete — shutting down");
    cancel.cancel();
    let (_, cvar) = queue.as_ref();
    cvar.notify_all();

    let _ = gen_handle.await;
    let _ = bh_handle.await;
    let (start_tx_count, end_tx_count) = metrics_handle.await.unwrap_or((0, 0));
    let (start_mints, end_mints) = if let Some(h) = operator_handle {
        h.await.unwrap_or((0, 0))
    } else {
        (0, 0)
    };
    for h in sender_handles {
        let _ = h.join();
    }

    let sent = sent_count.load(Ordering::Relaxed);
    let solana_landed = end_tx_count.saturating_sub(start_tx_count);
    let private_channel_minted = end_mints.saturating_sub(start_mints);
    let solana_tps = solana_landed as f64 / args.duration as f64;

    if args.operator_metrics_url.is_some() {
        let private_channel_tps = private_channel_minted as f64 / args.duration as f64;
        let drop = solana_landed.saturating_sub(private_channel_minted);
        let drop_rate = if solana_landed > 0 {
            drop as f64 / solana_landed as f64 * 100.0
        } else {
            0.0
        };
        info!(
            duration_secs = args.duration,
            sent,
            solana_landed,
            private_channel_minted,
            drop,
            drop_rate = format!("{drop_rate:.1}%"),
            solana_tps = format!("{solana_tps:.1}"),
            private_channel_tps = format!("{private_channel_tps:.1}"),
            "Final summary (deposit)",
        );
    } else {
        info!(
            duration_secs = args.duration,
            sent,
            solana_landed,
            solana_tps = format!("{solana_tps:.1}"),
            "Final summary (deposit)",
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Withdraw subcommand
// ---------------------------------------------------------------------------

async fn run_withdraw(args: args::WithdrawArgs) -> Result<()> {
    init_logging(&args.log_level);
    bench_metrics_init();
    if let Some(port) = args.metrics_port {
        private_channel_metrics::start_metrics_server(port);
    }

    info!(
        solana_rpc_url = %args.solana_rpc_url,
        rpc_url = %args.rpc_url,
        accounts = args.accounts,
        threads = args.threads,
        duration = args.duration,
        "Starting private-channel-bench-tps (withdraw)",
    );

    // Full e2e setup: initialise Solana escrow infrastructure + PrivateChannel mint and accounts.
    let withdraw_config = Arc::new(
        setup_withdraw::run_setup_withdraw_phase(
            &args.solana_rpc_url,
            &args.rpc_url,
            &args.admin_keypair,
            args.instance_seed_keypair.as_deref(),
            args.accounts,
            args.initial_balance,
        )
        .await?,
    );

    let queue: BatchQueue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));

    let cancel = CancellationToken::new();

    let bh_handle = tokio::spawn(run_blockhash_poller(
        args.rpc_url.clone(),
        Arc::clone(&withdraw_config.state),
        cancel.clone(),
    ));

    let load_end = tokio::time::Instant::now() + Duration::from_secs(args.duration);
    // Measures burn transactions confirmed on the PrivateChannel write-node
    let private_channel_metrics_handle = tokio::spawn(run_metrics_sampler(
        args.rpc_url.clone(),
        load_end,
        cancel.clone(),
        FLOW_WITHDRAW,
    ));
    // Samples private_channel_operator_mints_sent_total from operator-private_channel for e2e solana_released count
    let operator_handle = args.operator_metrics_url.clone().map(|url| {
        tokio::spawn(run_operator_mints_sampler(
            url,
            load_end,
            cancel.clone(),
            "withdraw",
        ))
    });

    let gen_handle = tokio::spawn(run_withdraw_generator(
        Arc::clone(&withdraw_config),
        Arc::clone(&withdraw_config.state),
        Arc::clone(&queue),
        args.threads,
        cancel.clone(),
    ));

    let sent_count = Arc::new(AtomicU64::new(0));
    let mut sender_handles = Vec::with_capacity(args.threads);
    for _ in 0..args.threads {
        let rpc_url = args.rpc_url.clone();
        let q = Arc::clone(&queue);
        let c = cancel.clone();
        let sc = Arc::clone(&sent_count);
        let ms = args.sender_sleep_ms;
        sender_handles.push(std::thread::spawn(move || {
            run_withdraw_sender_thread(rpc_url, q, c, sc, ms)
        }));
    }

    info!(
        duration_secs = args.duration,
        threads = args.threads,
        "Withdraw load phase started"
    );
    tokio::time::sleep(Duration::from_secs(args.duration)).await;

    info!("Withdraw load phase complete — shutting down");
    cancel.cancel();
    let (_, cvar) = queue.as_ref();
    cvar.notify_all();

    let _ = gen_handle.await;
    let _ = bh_handle.await;
    let (start_private_channel_count, end_private_channel_count) =
        private_channel_metrics_handle.await.unwrap_or((0, 0));
    let (start_mints, end_mints) = if let Some(h) = operator_handle {
        h.await.unwrap_or((0, 0))
    } else {
        (0, 0)
    };
    for h in sender_handles {
        let _ = h.join();
    }

    let sent = sent_count.load(Ordering::Relaxed);
    let private_channel_burned =
        end_private_channel_count.saturating_sub(start_private_channel_count);
    let private_channel_tps = private_channel_burned as f64 / args.duration as f64;

    if args.operator_metrics_url.is_some() {
        let solana_released = end_mints.saturating_sub(start_mints);
        let solana_tps = solana_released as f64 / args.duration as f64;
        let drop = private_channel_burned.saturating_sub(solana_released);
        let drop_rate = if private_channel_burned > 0 {
            drop as f64 / private_channel_burned as f64 * 100.0
        } else {
            0.0
        };
        info!(
            duration_secs = args.duration,
            sent,
            private_channel_burned,
            solana_released,
            drop,
            drop_rate = format!("{drop_rate:.1}%"),
            private_channel_tps = format!("{private_channel_tps:.1}"),
            solana_tps = format!("{solana_tps:.1}"),
            "Final summary (withdraw)",
        );
    } else {
        info!(
            duration_secs = args.duration,
            sent,
            private_channel_burned,
            private_channel_tps = format!("{private_channel_tps:.1}"),
            "Final summary (withdraw)",
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// DerivePda subcommand
// ---------------------------------------------------------------------------

/// Derives and prints the escrow instance PDA for a given instance-seed keypair.
///
/// If the keypair file does not exist, a new keypair is generated and saved
/// to the specified path before printing the PDA.  This lets run.sh use a
/// single command to both create the keypair and read the PDA.
fn run_derive_pda(args: DerivePdaArgs) -> Result<()> {
    let keypair: Keypair = if args.instance_seed_keypair.exists() {
        load_keypair(&args.instance_seed_keypair)
            .map_err(|e| anyhow::anyhow!("failed to load instance-seed keypair: {e}"))?
    } else {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let json = serde_json::to_string(&bytes.to_vec())?;
        write(&args.instance_seed_keypair, json)?;
        kp
    };

    let (pda, _) = find_instance_pda(&keypair.pubkey());
    println!("{pda}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn init_logging(log_level: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();
}
