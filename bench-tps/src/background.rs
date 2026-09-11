//! Phase 2 — Background tasks
//!
//! Two tasks run concurrently throughout the entire load phase:
//!
//! **Blockhash poller** (`run_blockhash_poller`)
//! Calls `getLatestBlockhash` every 80 ms and writes the result into
//! `BenchState::current_blockhash`.  The generator task reads this value
//! before signing each batch.  Keeping it fresh prevents the node's dedup
//! stage from rejecting transactions with a stale blockhash (>15 s old).
//! On RPC error the old hash is kept — it is still valid for up to ~15 s.
//!
//! **Metrics sampler** (`run_metrics_sampler`)
//! Calls `getTransactionCount` every second and diffs successive values to
//! compute instantaneous TPS as seen by the node.  This reflects actual
//! pipeline throughput, not just how fast the bench sends.  Returns
//! `(start_count, final_count)` so the caller can compute the total
//! transactions landed over the full duration.

use solana_commitment_config::CommitmentConfig;
use {
    crate::{
        bench_metrics::BENCH_LANDED_TOTAL,
        types::{
            BenchState, BLOCKHASH_LOG_INTERVAL, BLOCKHASH_POLL_INTERVAL, METRICS_SAMPLE_INTERVAL,
        },
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    std::{sync::Arc, time::Instant},
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
};

/// Scrape a single metric value from a Prometheus `/metrics` text endpoint.
///
/// Returns `Ok(value)` when the metric line is found, `Err(reason)` otherwise
/// so callers can log a precise failure message.
async fn scrape_prometheus_metric(
    metrics_url: &str,
    metric_name: &str,
    label_filter: &str,
) -> Result<f64, &'static str> {
    let body = match reqwest::get(metrics_url).await {
        Ok(resp) => match resp.text().await {
            Ok(t) => t,
            Err(_) => return Err("failed to read response body"),
        },
        Err(_) => return Err("connection refused or unreachable"),
    };

    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(metric_name) && line.contains(label_filter) {
            // Prometheus text format: `metric_name{labels} value [timestamp]`
            let parts: Vec<&str> = line.split_whitespace().collect();
            return parts
                .get(1)
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or("failed to parse metric value");
        }
    }
    Err("metric line not found in response")
}

/// Samples `private_channel_operator_mints_sent_total` every second from a Prometheus
/// `/metrics` endpoint, logging instantaneous confirmed/s and remaining test time.
///
/// Mirrors `run_metrics_sampler` but measures operator-confirmed transactions instead
/// of raw PrivateChannel transaction count — giving the true end-to-end completion rate.
///
/// `program_type` selects which operator to sample:
///   - `"escrow"`  → deposit operator (PrivateChannel mints confirmed)
///   - `"withdraw"` → withdraw operator (Solana releases confirmed)
///
/// Returns `(start_count, final_count)` for drop-rate calculation.
pub async fn run_operator_mints_sampler(
    metrics_url: String,
    load_end: tokio::time::Instant,
    cancel: CancellationToken,
    program_type: &'static str,
) -> (u64, u64) {
    info!(url = %metrics_url, program_type, "operator_mints_sampler: starting");

    let mut ticker = tokio::time::interval(METRICS_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut start_count: Option<u64> = None;
    let mut prev_count: Option<u64> = None;

    const METRIC: &str = "private_channel_operator_mints_sent_total";
    let label = format!(r#"program_type="{}""#, program_type);

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                let final_count = match scrape_prometheus_metric(&metrics_url, METRIC, &label).await {
                    Ok(v) => v as u64,
                    Err(reason) => {
                        warn!(reason, "operator_mints_sampler: final scrape failed");
                        prev_count.unwrap_or(0)
                    }
                };
                return (start_count.unwrap_or(final_count), final_count);
            }

            _ = ticker.tick() => {
                let count = match scrape_prometheus_metric(&metrics_url, METRIC, &label).await {
                    Ok(v) => v as u64,
                    Err(reason) => {
                        warn!(reason, url = %metrics_url, "operator_mints_sampler: scrape failed, will retry");
                        continue;
                    }
                };

                let sc = *start_count.get_or_insert(count);
                let confirmed_per_sec = count.saturating_sub(prev_count.unwrap_or(count));
                let remaining_secs = load_end
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_secs();
                info!(confirmed_per_sec, total_confirmed = count, baseline = sc, remaining_secs, program_type, "operator confirmed/s");
                prev_count = Some(count);
            }
        }
    }
}

/// Refreshes the cached blockhash at `BLOCKHASH_POLL_INTERVAL` (80 ms) and
/// logs average fetch latency at `BLOCKHASH_LOG_INTERVAL` (every 2 s).
///
/// Exits when `cancel` is triggered.
pub async fn run_blockhash_poller(
    rpc_url: String,
    state: Arc<BenchState>,
    cancel: CancellationToken,
) {
    let rpc = RpcClient::new(rpc_url);
    let mut poll_ticker = tokio::time::interval(BLOCKHASH_POLL_INTERVAL);
    let mut log_ticker = tokio::time::interval(BLOCKHASH_LOG_INTERVAL);

    // Delay mode: if the runtime falls behind, skip missed ticks rather than
    // firing a burst of catch-up ticks which would flood the RPC endpoint.
    poll_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    log_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Accumulate individual fetch durations between log ticks.
    let mut fetch_times_us: Vec<u64> = Vec::new();

    loop {
        tokio::select! {
            biased; // check cancel first so shutdown is prompt

            _ = cancel.cancelled() => break,

            _ = log_ticker.tick() => {
                if fetch_times_us.is_empty() {
                    info!("blockhash_poller: no fetches in last 2s");
                } else {
                    let avg_us: u64 =
                        fetch_times_us.iter().sum::<u64>() / fetch_times_us.len() as u64;
                    info!(
                        fetches = fetch_times_us.len(),
                        avg_fetch_us = avg_us,
                        "blockhash_poller avg fetch latency",
                    );
                    fetch_times_us.clear();
                }
            }

            _ = poll_ticker.tick() => {
                let t = Instant::now();
                match rpc.get_latest_blockhash().await {
                    Ok(hash) => {
                        let elapsed_us = t.elapsed().as_micros() as u64;
                        fetch_times_us.push(elapsed_us);
                        // Write the new hash into shared state so the generator
                        // picks it up on its next batch.
                        *state.current_blockhash.write().await = hash;
                    }
                    Err(e) => {
                        // Keep the existing hash — it stays valid for ~15 s.
                        warn!(err = %e, "blockhash_poller: get_latest_blockhash failed, keeping cached hash");
                    }
                }
            }
        }
    }
}

/// Samples `getTransactionCount` at `METRICS_SAMPLE_INTERVAL` (every 1 s),
/// logs instantaneous TPS and remaining test time, then returns
/// `(start_count, final_count)` on shutdown.
///
/// Using `CommitmentConfig::processed()` gives the most up-to-date view of
/// the validator's transaction count, reducing lag between actual landing and
/// the metric being reflected here.
pub async fn run_metrics_sampler(
    rpc_url: String,
    load_end: tokio::time::Instant,
    cancel: CancellationToken,
    flow: &'static str,
) -> (u64, u64) {
    let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed());
    let mut ticker = tokio::time::interval(METRICS_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // start_count is captured on the first successful sample so the final
    // summary can compute the delta over the exact load-phase window.
    let mut start_count: Option<u64> = None;
    let mut prev_count: Option<u64> = None;

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                // Take one final sample to minimise measurement lag at shutdown.
                let final_count = rpc
                    .get_transaction_count()
                    .await
                    .unwrap_or(prev_count.unwrap_or(0));
                if let Some(prev) = prev_count {
                    let delta = final_count.saturating_sub(prev);
                    if delta > 0 {
                        BENCH_LANDED_TOTAL
                            .with_label_values(&[flow])
                            .inc_by(delta as f64);
                    }
                }
                return (start_count.unwrap_or(final_count), final_count);
            }

            _ = ticker.tick() => {
                match rpc.get_transaction_count().await {
                    Ok(count) => {
                        let sc = *start_count.get_or_insert(count);
                        if let Some(prev) = prev_count {
                            let tps = count.saturating_sub(prev);
                            if tps > 0 {
                                BENCH_LANDED_TOTAL
                                    .with_label_values(&[flow])
                                    .inc_by(tps as f64);
                            }
                            let remaining_secs = load_end
                                .saturating_duration_since(tokio::time::Instant::now())
                                .as_secs();
                            info!(tps, total_tx = count, remaining_secs, "metrics");
                        }
                        prev_count = Some(count);
                        let _ = sc; // silence unused-variable lint
                    }
                    Err(e) => {
                        warn!(err = %e, "metrics_sampler: get_transaction_count failed");
                    }
                }
            }
        }
    }
}
