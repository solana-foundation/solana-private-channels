//! Escrow balance reconciliation module
//!
//! Per mint, freezes the pipelines after `HALT_CONFIRM_TICKS` finalized breaches of either invariant:
//! channel supply within custody plus the in-flight envelope (over-issuance), or ledger liabilities
//! at the custody slot within custody (a drain that unminted deposits hide from the supply check).

use crate::config::{OperatorConfig, ProgramType};
use crate::error::OperatorError;
use crate::indexer::checkpoint::program_key;
use crate::operator::escrow_sweep::{
    fetch_channel_supply, fetch_escrow_balances_by_mint, CustodySnapshot,
};
use crate::operator::RpcClientWithRetry;
use crate::storage::common::amount::{net_to_u64, NetBalance};
use crate::storage::common::models::MintDbBalance;
use crate::storage::Storage;
use private_channel_core::webhook::{WebhookClient, WebhookRetryConfig};
use private_channel_metrics::HealthState;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const WEBHOOK_MAX_ATTEMPTS: u32 = 3;
const WEBHOOK_BASE_DELAY: Duration = Duration::from_millis(500);
const WEBHOOK_MAX_DELAY: Duration = Duration::from_secs(5);
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// Consecutive beyond-envelope, insolvency-direction ticks required before the
/// pipelines are frozen. Each tick is a fresh finalized read, so a one-off
/// corrupt RPC read cannot halt while a persistent shortfall always will.
const HALT_CONFIRM_TICKS: u32 = 3;

/// Longest a tick waits for the escrow indexer's checkpoint to reach the custody slot.
#[cfg(not(test))]
const LEDGER_CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const LEDGER_CATCHUP_TIMEOUT: Duration = Duration::from_millis(500);

/// Checkpoint poll interval while waiting for the ledger to cover the custody slot.
#[cfg(not(test))]
const LEDGER_CATCHUP_POLL_MS: u64 = 500;
#[cfg(test)]
const LEDGER_CATCHUP_POLL_MS: u64 = 5;

/// Per-mint consecutive breach counts, one map per invariant.
/// Two maps so an unknown or clean reading of one invariant never resets the other's evidence.
#[derive(Debug, Default)]
struct BreachCounters {
    supply: HashMap<Pubkey, u32>,
    liability: HashMap<Pubkey, u32>,
}

/// Runs periodic escrow balance reconciliation checks
///
/// Each tick reads escrow custody, channel supply and the ledger per mint, then halts
/// (durable flag + quarantine + forced-unhealthy + webhook) when either invariant breaches
/// for `HALT_CONFIRM_TICKS` consecutive ticks. Reads never mutate state outside the halt path.
pub async fn run_reconciliation(
    storage: Arc<Storage>,
    config: OperatorConfig,
    rpc_client: Arc<RpcClientWithRetry>,
    channel_rpc: Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
    health: Option<Arc<HealthState>>,
    cancellation_token: CancellationToken,
) -> Result<(), OperatorError> {
    info!("Starting reconciliation");
    info!(
        "Reconciliation interval: {:?}",
        config.reconciliation_interval
    );
    info!(
        "Tolerance threshold: {} basis points",
        config.reconciliation_tolerance_bps
    );

    let webhook_client = WebhookClient::new(
        WEBHOOK_TIMEOUT,
        WebhookRetryConfig::new(WEBHOOK_MAX_ATTEMPTS, WEBHOOK_BASE_DELAY, WEBHOOK_MAX_DELAY),
    )
    .map_err(|e| OperatorError::WebhookError(format!("Failed to create HTTP client: {}", e)))?;

    // Orphan-mint dedup state
    let mut previously_alerted_orphans: Option<HashSet<i64>> = None;

    // A read-erroring tick returns early and leaves these untouched, so they hold rather than reset.
    let mut breach_counters = BreachCounters::default();

    // Seed the set-once guard from the durable flag so a restart into an active
    // halt does not re-quarantine or re-webhook; it stays frozen until cleared.
    let mut halted = storage
        .is_reconciliation_halted()
        .await
        .ok()
        .flatten()
        .is_some();

    loop {
        // Check for cancellation
        if cancellation_token.is_cancelled() {
            info!("Reconciliation received cancellation signal, stopping...");
            break;
        }

        // Perform reconciliation check
        match perform_reconciliation_check(
            &storage,
            &config,
            &rpc_client,
            &channel_rpc,
            escrow_instance_id,
            &webhook_client,
            &health,
            &mut previously_alerted_orphans,
            &mut breach_counters,
            &mut halted,
            &cancellation_token,
        )
        .await
        {
            Ok(_) => {
                // Reconciliation check completed successfully
            }
            Err(e) => {
                warn!("Failed to perform reconciliation check: {}", e);
            }
        }

        // Sleep between checks, but break immediately when cancellation is signaled.
        tokio::select! {
            _ = tokio::time::sleep(config.reconciliation_interval) => {},
            _ = cancellation_token.cancelled() => {
                info!("Reconciliation received cancellation signal during sleep, stopping...");
                break;
            }
        }
    }

    info!("Reconciliation stopped gracefully");
    Ok(())
}

/// A per-mint proven insolvency: channel supply exceeds escrow custody by more
/// than the in-flight envelope plus tolerance. `supply_gap` feeds the halt
/// reason string and the webhook payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsolvencyBreach {
    pub supply_gap: u64,
    pub envelope: u64,
    pub tolerance: u64,
}

/// Decide whether one mint's supply exceeds custody beyond the in-flight
/// envelope plus tolerance. Only `supply > custody` (custody short) is
/// insolvency; the benign direction (custody richer, normal in-flight activity)
/// is dropped via `saturating_sub`.
fn evaluate_insolvency(
    custody: u64,
    supply: u64,
    envelope: u64,
    tolerance: u64,
) -> Option<InsolvencyBreach> {
    let supply_gap = supply.saturating_sub(custody);
    let allowance = envelope.saturating_add(tolerance);
    if supply_gap > allowance {
        Some(InsolvencyBreach {
            supply_gap,
            envelope,
            tolerance,
        })
    } else {
        None
    }
}

/// A per-mint custody shortfall: ledger liabilities exceed custody by more than tolerance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiabilityBreach {
    pub gap: u64,
    pub liabilities: u64,
    pub tolerance: u64,
}

/// Decide whether custody falls short of ledger liabilities beyond tolerance.
/// No envelope: a pending deposit is already in custody, so nothing in flight explains a shortfall.
fn evaluate_liability_shortfall(
    custody: u64,
    liabilities: u64,
    tolerance: u64,
) -> Option<LiabilityBreach> {
    let gap = liabilities.saturating_sub(custody);
    (gap > tolerance).then_some(LiabilityBreach {
        gap,
        liabilities,
        tolerance,
    })
}

/// A ledger row's liabilities (deposits minus released withdrawals) as u64.
/// Negative means missing deposit history and reads as 0 (surplus); above u64::MAX fails closed.
fn ledger_liability(row: &MintDbBalance) -> u64 {
    let net = &row.total_deposits - &row.total_withdrawals;
    match net_to_u64(&net) {
        NetBalance::Exact(v) => v,
        NetBalance::Negative => {
            warn!(mint = %row.mint_address, net = %net, "Ledger withdrawals exceed deposits; liabilities read as 0");
            0
        }
        NetBalance::Overflow => {
            error!(mint = %row.mint_address, net = %net, "Ledger net exceeds u64::MAX; liabilities read as u64::MAX");
            u64::MAX
        }
    }
}

/// Convert an in-flight envelope BigDecimal into u64. The sum is non-negative by
/// construction; an over-u64 sum reports u64::MAX, which only ever enlarges the
/// envelope (delays a halt), never fabricates one.
fn envelope_to_u64(amount: &bigdecimal::BigDecimal) -> u64 {
    match net_to_u64(amount) {
        NetBalance::Exact(v) => v,
        NetBalance::Overflow => u64::MAX,
        NetBalance::Negative => 0,
    }
}

/// Raw insolvency tolerance derived from the bps knob, applied to custody so it
/// mirrors the alert layer's relative comparison. Envelope is the real bound;
/// this is only a small cushion, and a larger one just delays a real halt.
fn insolvency_tolerance_raw(custody: u64, tolerance_bps: u16) -> u64 {
    // Saturate rather than truncate: a >100% bps knob against a huge custody must
    // widen the cushion, never wrap it down to a tiny value that over-halts.
    ((custody as u128 * tolerance_bps as u128) / 10_000).min(u64::MAX as u128) as u64
}

/// Insolvency gap in basis points of custody for the alert payload. A tiny
/// custody against a huge gap can push the ratio past u64::MAX, so saturate
/// rather than let the u128->u64 cast wrap into a garbage value. Zero custody has
/// no ratio, so report the max, matching every other reconciliation alert.
fn insolvency_delta_bps(custody: u64, gap: u64) -> u64 {
    if custody == 0 {
        return u64::MAX;
    }
    u64::try_from((gap as u128 * 10_000) / custody as u128).unwrap_or(u64::MAX)
}

/// Performs a single reconciliation check.
///
/// Per mint, over finalized reads: supply against custody plus envelope, and ledger
/// liabilities at the custody slot against custody. A failed halt-input load warns and
/// returns Ok with the counters held, so a transient glitch keeps the evidence.
#[allow(clippy::too_many_arguments)]
async fn perform_reconciliation_check(
    storage: &Arc<Storage>,
    config: &OperatorConfig,
    rpc_client: &Arc<RpcClientWithRetry>,
    channel_rpc: &Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
    webhook_client: &WebhookClient,
    health: &Option<Arc<HealthState>>,
    previously_alerted_orphans: &mut Option<HashSet<i64>>,
    breach_counters: &mut BreachCounters,
    halted: &mut bool,
    cancellation_token: &CancellationToken,
) -> Result<(), OperatorError> {
    check_orphan_deposit_rows(
        storage,
        previously_alerted_orphans,
        webhook_client,
        &config.reconciliation_webhook_url,
    )
    .await;

    // Re-sync the set-once guard with the durable flag each tick: a manual clear
    // (runbook) must let a fresh insolvency re-trip, while a still-set flag keeps
    // re-firing suppressed. On a read error leave the guard as-is.
    if let Ok(flag) = storage.is_reconciliation_halted().await {
        *halted = flag.is_some();
    }

    let custody = fetch_on_chain_balances(rpc_client, escrow_instance_id).await?;
    let covered = match wait_for_ledger(storage, custody.slot, cancellation_token).await {
        LedgerWait::Covered => true,
        LedgerWait::Unknown => false,
        LedgerWait::Cancelled => return Ok(()),
    };

    // Rows are read whatever the wait outcome: they enumerate the mints even when
    // liabilities are unknown, keeping a zero-custody mint with supply in scope.
    let (ledger_mints, liabilities) = fetch_ledger(storage, custody.slot).await?;
    let mut mints: HashSet<Pubkey> = custody.balances.keys().copied().collect();
    mints.extend(ledger_mints);

    // Envelope (DB) and channel supply (PrivateChannel RPC) are the remaining
    // halt inputs. A failure of either holds the breach counters and skips the
    // tick, so a transient glitch cannot reset a building breach.
    match load_halt_inputs(storage, channel_rpc, &mints).await {
        Ok((supply, envelope)) => {
            evaluate_and_maybe_halt(
                storage,
                config,
                health,
                webhook_client,
                &custody.balances,
                custody.slot,
                &mints,
                &supply,
                &envelope,
                covered.then_some(&liabilities),
                breach_counters,
                halted,
            )
            .await;
        }
        Err(e) => warn!("Skipping halt evaluation this tick (counters held): {}", e),
    }

    Ok(())
}

/// Whether the ledger can be compared at the custody slot this tick.
#[derive(Debug, PartialEq, Eq)]
enum LedgerWait {
    Covered,
    Unknown,
    Cancelled,
}

/// Wait, bounded, for the escrow indexer's checkpoint to reach `slot`.
///
/// A checkpoint at or past `slot` means every deposit and release up to it is in the DB,
/// so the ledger is exact there. Giving up returns Unknown, which holds the liability counter.
async fn wait_for_ledger(
    storage: &Arc<Storage>,
    slot: u64,
    cancellation_token: &CancellationToken,
) -> LedgerWait {
    let key = program_key(ProgramType::Escrow);
    let started = tokio::time::Instant::now();
    let mut last: Option<u64> = None;
    loop {
        match storage.get_committed_checkpoint(&key).await {
            Ok(Some(committed)) if committed >= slot => return LedgerWait::Covered,
            Ok(Some(committed)) => last = Some(committed),
            // No escrow indexer has ever committed, so waiting cannot help.
            Ok(None) => {
                warn!(
                    slot,
                    "No escrow indexer checkpoint; ledger liabilities unknown this tick"
                );
                return LedgerWait::Unknown;
            }
            Err(e) => warn!("Checkpoint read failed while waiting for the ledger: {}", e),
        }
        if started.elapsed() >= LEDGER_CATCHUP_TIMEOUT {
            warn!(
                checkpoint = ?last,
                slot,
                "Escrow indexer checkpoint did not reach the custody slot in time; ledger liabilities unknown this tick"
            );
            return LedgerWait::Unknown;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(LEDGER_CATCHUP_POLL_MS)) => {}
            _ = cancellation_token.cancelled() => return LedgerWait::Cancelled,
        }
    }
}

/// Read the ledger at `slot`: every DB mint, plus its liabilities.
async fn fetch_ledger(
    storage: &Arc<Storage>,
    slot: u64,
) -> Result<(HashSet<Pubkey>, HashMap<Pubkey, u64>), OperatorError> {
    let rows = storage
        .get_mint_balances_for_reconciliation(slot)
        .await
        .map_err(OperatorError::Storage)?;
    let mut mints = HashSet::new();
    let mut liabilities = HashMap::new();
    for row in rows {
        let mint = parse_mint(&row.mint_address)?;
        mints.insert(mint);
        liabilities.insert(mint, ledger_liability(&row));
    }
    Ok((mints, liabilities))
}

/// Query the per-mint in-flight envelope (unsettled amount) as u64.
async fn fetch_in_flight_envelope(
    storage: &Arc<Storage>,
) -> Result<HashMap<Pubkey, u64>, OperatorError> {
    let rows = storage
        .get_in_flight_amounts_by_mint()
        .await
        .map_err(OperatorError::Storage)?;
    let mut out = HashMap::new();
    for row in rows {
        let mint = parse_mint(&row.mint_address)?;
        out.insert(mint, envelope_to_u64(&row.in_flight_amount));
    }
    Ok(out)
}

/// Load the halt inputs: the in-flight envelope (DB) and per-mint channel supply
/// (PrivateChannel RPC) for the given mint set. An envelope-query failure skips
/// the whole tick (one DB read), but a single mint's supply read failing must not
/// blind the others: that mint is omitted from the returned map and held in
/// `evaluate_and_maybe_halt`, so a flaky read on one mint cannot suppress
/// detection on a genuinely over-issued one.
async fn load_halt_inputs(
    storage: &Arc<Storage>,
    channel_rpc: &Arc<RpcClientWithRetry>,
    mints: &HashSet<Pubkey>,
) -> Result<(HashMap<Pubkey, u64>, HashMap<Pubkey, u64>), OperatorError> {
    let envelope = fetch_in_flight_envelope(storage).await?;
    let mut supply = HashMap::new();
    for mint in mints {
        match fetch_channel_supply(channel_rpc, mint).await {
            Ok(s) => {
                supply.insert(*mint, s);
            }
            Err(e) => warn!(
                mint = %mint,
                "Channel supply read failed; skipping this mint this tick: {}", e.reason
            ),
        }
    }
    Ok((supply, envelope))
}

fn parse_mint(mint_address: &str) -> Result<Pubkey, OperatorError> {
    mint_address
        .parse::<Pubkey>()
        .map_err(|e| OperatorError::InvalidPubkey {
            pubkey: mint_address.to_string(),
            reason: e.to_string(),
        })
}

/// Halt path: per mint, advance each invariant's own counter and freeze the pipelines once
/// (durable flag + quarantine + forced-unhealthy + webhook) on the `HALT_CONFIRM_TICKS`-th breach of either.
/// `liabilities` is `None` when the ledger could not be pinned to `slot`; that arm then holds.
#[allow(clippy::too_many_arguments)]
async fn evaluate_and_maybe_halt(
    storage: &Arc<Storage>,
    config: &OperatorConfig,
    health: &Option<Arc<HealthState>>,
    webhook_client: &WebhookClient,
    custody: &HashMap<Pubkey, u64>,
    slot: u64,
    mints: &HashSet<Pubkey>,
    supply: &HashMap<Pubkey, u64>,
    envelope: &HashMap<Pubkey, u64>,
    liabilities: Option<&HashMap<Pubkey, u64>>,
    breach_counters: &mut BreachCounters,
    halted: &mut bool,
) {
    // Rebuild counters from scratch each tick so a mint that stops breaching (or
    // disappears) resets to zero rather than lingering.
    let mut next_counters = BreachCounters::default();
    for &mint in mints {
        // A mint absent from `supply` had its read fail this tick (an absent
        // account reads as Ok(0), not a miss). Hold its counter rather than
        // resetting, so a transient per-mint glitch neither halts it nor erases
        // its evidence, and never blocks the other mints.
        let Some(&s) = supply.get(&mint) else {
            if let Some(&held) = breach_counters.supply.get(&mint) {
                next_counters.supply.insert(mint, held);
            }
            continue;
        };
        let c = *custody.get(&mint).unwrap_or(&0);
        let env = *envelope.get(&mint).unwrap_or(&0);
        let tolerance = insolvency_tolerance_raw(c, config.reconciliation_tolerance_bps);

        let Some(breach) = evaluate_insolvency(c, s, env, tolerance) else {
            continue;
        };

        let count = breach_counters.supply.get(&mint).copied().unwrap_or(0) + 1;
        next_counters.supply.insert(mint, count);

        if count < HALT_CONFIRM_TICKS || *halted {
            warn!(
                mint = %mint,
                supply_gap = breach.supply_gap,
                envelope = breach.envelope,
                tolerance = breach.tolerance,
                consecutive_ticks = count,
                "Supply beyond custody past envelope; halt pending confirmation"
            );
            continue;
        }

        // Confirmed insolvency: trip the halt exactly once.
        *halted = true;
        let reason = format!(
            "reconciliation halt: mint {} custody {} short of supply by {}, \
             envelope {} tolerance {} over {} consecutive finalized ticks",
            mint, c, breach.supply_gap, breach.envelope, breach.tolerance, count
        );
        error!(reason = %reason, "RECONCILIATION HALT tripped; freezing both pipelines");
        trip_halt(
            storage,
            health,
            webhook_client,
            config,
            &mint,
            c,
            breach.supply_gap,
            c.saturating_add(breach.supply_gap),
            &reason,
        )
        .await;
    }

    for &mint in mints {
        // Unknown liabilities hold the counter: this tick is no evidence either way.
        let Some(liabilities) = liabilities else {
            if let Some(&held) = breach_counters.liability.get(&mint) {
                next_counters.liability.insert(mint, held);
            }
            continue;
        };
        let c = *custody.get(&mint).unwrap_or(&0);
        let owed = *liabilities.get(&mint).unwrap_or(&0);
        let tolerance = insolvency_tolerance_raw(c, config.reconciliation_tolerance_bps);

        let Some(breach) = evaluate_liability_shortfall(c, owed, tolerance) else {
            continue;
        };

        let count = breach_counters.liability.get(&mint).copied().unwrap_or(0) + 1;
        next_counters.liability.insert(mint, count);

        if count < HALT_CONFIRM_TICKS || *halted {
            warn!(
                mint = %mint,
                gap = breach.gap,
                liabilities = breach.liabilities,
                tolerance = breach.tolerance,
                slot,
                consecutive_ticks = count,
                "Custody short of ledger liabilities; halt pending confirmation"
            );
            continue;
        }

        *halted = true;
        let reason = format!(
            "reconciliation halt: mint {} custody {} short of ledger liabilities {} by {}, \
             tolerance {} at slot {} over {} consecutive finalized ticks",
            mint, c, breach.liabilities, breach.gap, breach.tolerance, slot, count
        );
        error!(reason = %reason, "RECONCILIATION HALT tripped; freezing both pipelines");
        trip_halt(
            storage,
            health,
            webhook_client,
            config,
            &mint,
            c,
            breach.gap,
            breach.liabilities,
            &reason,
        )
        .await;
    }

    *breach_counters = next_counters;
}

/// Fire every fail-closed lever for a confirmed insolvency. Each is best-effort
/// and independently logged: the durable flag is the cross-process freeze,
/// quarantine flips rows active right now, forced-unhealthy pages orchestration,
/// and the webhook notifies operators. None of them gate the others.
#[allow(clippy::too_many_arguments)]
async fn trip_halt(
    storage: &Arc<Storage>,
    health: &Option<Arc<HealthState>>,
    webhook_client: &WebhookClient,
    config: &OperatorConfig,
    mint: &Pubkey,
    custody: u64,
    gap: u64,
    db_balance: u64,
    reason: &str,
) {
    if let Err(e) = storage.set_reconciliation_halt(reason).await {
        error!("Failed to set durable reconciliation halt flag: {}", e);
    }
    // Unbounded on purpose: an insolvency halt is not nonce-scoped.
    match storage.quarantine_active_withdrawals(None, None).await {
        Ok(n) => info!(rows = n, "Quarantined active withdrawals on halt"),
        Err(e) => error!("Failed to quarantine active withdrawals on halt: {}", e),
    }
    if let Some(h) = health {
        h.force_unhealthy(reason.to_string());
    }
    // Payload carries real custody and the amount the escrow should hold (supply it
    // could not honor, or ledger liabilities); delta_bps is u64::MAX when custody is 0.
    let alert = BalanceMismatch {
        mint: *mint,
        on_chain_balance: custody,
        db_balance,
        delta_bps: insolvency_delta_bps(custody, gap),
    };
    if let Err(e) =
        send_webhook_alert(&config.reconciliation_webhook_url, &[alert], webhook_client).await
    {
        error!("Failed to send reconciliation halt webhook: {}", e);
    }
}

/// Surface orphan deposit rows (deposits whose mint was not `allowed` at the
/// deposit's slot) via `error!` log and a webhook alert. Dedup is per
/// `transactions.id` (in `previously_alerted_orphans`): each id alerts once
/// then stays silent.
///
/// - `None` (baseline tick): empty set logs `info!`; non-empty logs `error!`
///   and posts the full set.
/// - `Some(seen)`: only ids not in `seen` are new, they log `error!` and post.
/// - Storage-query failure logs `warn!` and leaves dedup state untouched.
async fn check_orphan_deposit_rows(
    storage: &Arc<Storage>,
    previously_alerted_orphans: &mut Option<HashSet<i64>>,
    webhook_client: &WebhookClient,
    webhook_url: &Option<String>,
) {
    let orphans = match storage.get_orphan_deposit_ids().await {
        Ok(orphans) => orphans,
        Err(e) => {
            warn!("Failed to query orphan deposit ids: {}", e);
            return;
        }
    };

    match previously_alerted_orphans {
        None => {
            // Baseline pass
            if orphans.is_empty() {
                info!("Reconciliation baseline: no orphan deposit rows detected");
                // No alert needed; establish an empty baseline.
                *previously_alerted_orphans = Some(HashSet::new());
                return;
            }

            error!(
                row_count = orphans.len(),
                orphan_ids = ?orphans,
                "Reconciliation baseline: {} orphan deposit row(s) currently present \
                 (no allowed mint status at the deposit's slot); subsequent ticks will only log on new entries",
                orphans.len()
            );

            // Advance dedup state only after the alert is delivered; on failure
            // leave it `None` so the next tick re-runs the baseline and retries.
            match send_orphan_deposit_alert(webhook_url, &orphans, webhook_client).await {
                Ok(()) => {
                    *previously_alerted_orphans = Some(orphans.into_iter().collect());
                }
                Err(e) => {
                    error!("Failed to send orphan deposit webhook alert: {}", e);
                }
            }
        }
        Some(seen) => {
            // Delta pass, only NEW orphan rows (not in `seen`) get logged.
            let new_orphans: Vec<i64> = orphans
                .into_iter()
                .filter(|id| !seen.contains(id))
                .collect();

            if new_orphans.is_empty() {
                // Steady state, silent to avoid log spam on every tick.
                return;
            }

            error!(
                new_row_count = new_orphans.len(),
                new_orphan_ids = ?new_orphans,
                "Reconciliation found {} new orphan deposit row(s) (no allowed mint status at the deposit's slot)",
                new_orphans.len()
            );

            // Mark seen only after a successful alert; a failed send leaves
            // them unseen so the next tick re-alerts.
            match send_orphan_deposit_alert(webhook_url, &new_orphans, webhook_client).await {
                Ok(()) => {
                    seen.extend(new_orphans);
                }
                Err(e) => {
                    error!("Failed to send orphan deposit webhook alert: {}", e);
                }
            }
        }
    }
}

/// Represents a balance mismatch between on-chain and database balances for a specific mint
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceMismatch {
    pub mint: Pubkey,
    pub on_chain_balance: u64,
    pub db_balance: u64,
    pub delta_bps: u64,
}

/// Fetches on-chain token balances for all token accounts owned by the escrow
///
/// Queries the Solana RPC using `get_token_accounts_by_owner` to retrieve all SPL token accounts
/// (both Token and Token-2022 programs) owned by the escrow instance. Returns a mapping of mint
/// addresses to total balances, aggregating across multiple token accounts for the same mint if present.
///
/// # Arguments
/// * `rpc_client` - RPC client with retry logic for on-chain queries
/// * `escrow_instance_id` - Public key of the escrow account that owns the token accounts
///
/// # Returns
/// * `CustodySnapshot` - Per-mint balances (smallest token units) and the slot they are valid at
///
/// # Errors
/// Returns `OperatorError::RpcError` if the RPC call fails after retries or if token account data cannot be parsed
async fn fetch_on_chain_balances(
    rpc_client: &Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
) -> Result<CustodySnapshot, OperatorError> {
    fetch_escrow_balances_by_mint(rpc_client, escrow_instance_id)
        .await
        .map_err(|e| OperatorError::RpcError(e.to_string()))
}

/// Sends webhook alerts for balance mismatches with retry logic
///
/// Posts each mismatch to the configured webhook URL as a JSON payload with the format:
/// ```json
/// {
///   "mint": "<mint_pubkey>",
///   "on_chain_balance": 123,
///   "db_balance": 456,
///   "delta_bps": 789,
///   "timestamp": "2024-01-01T12:00:00Z"
/// }
/// ```
///
/// Implements exponential backoff retry logic (up to 3 attempts) for transient HTTP errors.
/// If the webhook URL is not configured (None), logs a warning and returns Ok without sending.
///
/// # Arguments
/// * `webhook_url` - Optional webhook URL to POST alerts to
/// * `mismatches` - Slice of balance mismatches to alert on
/// * `webhook_client` - Shared webhook client for HTTP delivery
///
/// # Returns
/// * `Ok(())` if all webhooks sent successfully (or no URL configured)
/// * `Err(OperatorError::WebhookError)` if webhook delivery fails after retries
pub async fn send_webhook_alert(
    webhook_url: &Option<String>,
    mismatches: &[BalanceMismatch],
    webhook_client: &WebhookClient,
) -> Result<(), OperatorError> {
    // If no webhook URL configured, log and return early
    let url = match webhook_url {
        Some(url) => url,
        None => {
            if !mismatches.is_empty() {
                warn!(
                    "Balance mismatch detected but no webhook URL configured (found {} mismatches)",
                    mismatches.len()
                );
            }
            return Ok(());
        }
    };

    // Send alert for each mismatch
    for mismatch in mismatches {
        let payload = serde_json::json!({
            "mint": mismatch.mint.to_string(),
            "on_chain_balance": mismatch.on_chain_balance,
            "db_balance": mismatch.db_balance,
            "delta_bps": mismatch.delta_bps,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        let context = format!("mint {} (delta {} bps)", mismatch.mint, mismatch.delta_bps);

        webhook_client
            .post_json(url, &payload, &context)
            .await
            .map_err(|error| {
                error!(
                    "Failed to send webhook alert after {} attempts for mint {}: {}",
                    error.attempts(),
                    mismatch.mint,
                    error.message()
                );
                OperatorError::WebhookError(format!(
                    "Failed to send webhook alert after {} attempts: {}",
                    error.attempts(),
                    error.message()
                ))
            })?;

        info!(
            "Webhook alert sent for mint {} (delta: {} bps)",
            mismatch.mint, mismatch.delta_bps
        );
    }

    Ok(())
}

/// Posts orphan deposit transaction ids to `webhook_url` as a single JSON
/// payload `{ orphan_ids, row_count, timestamp }` (ids only — operators resolve
/// mint/amount/signature via `docs/runbooks/deposit_manual_review.md`).
///
/// Retries up to 3 times with exponential backoff on transient HTTP errors.
/// `None` URL with ids present logs a `warn!` and returns `Ok`. Returns
/// `Err(OperatorError::WebhookError)` if delivery fails after retries.
pub async fn send_orphan_deposit_alert(
    webhook_url: &Option<String>,
    orphan_ids: &[i64],
    webhook_client: &WebhookClient,
) -> Result<(), OperatorError> {
    // If no webhook URL configured, log and return early
    let url = match webhook_url {
        Some(url) => url,
        None => {
            if !orphan_ids.is_empty() {
                warn!(
                    "Orphan deposit rows detected but no webhook URL configured (found {} row(s))",
                    orphan_ids.len()
                );
            }
            return Ok(());
        }
    };

    let payload = serde_json::json!({
        "row_count": orphan_ids.len(),
        "orphan_ids": orphan_ids,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    let context = format!("orphan deposits: {} row(s)", orphan_ids.len());

    webhook_client
        .post_json(url, &payload, &context)
        .await
        .map_err(|error| {
            error!(
                "Failed to send orphan deposit webhook alert after {} attempts: {}",
                error.attempts(),
                error.message()
            );
            OperatorError::WebhookError(format!(
                "Failed to send orphan deposit webhook alert after {} attempts: {}",
                error.attempts(),
                error.message()
            ))
        })?;

    info!(
        "Orphan deposit webhook alert sent for {} row(s)",
        orphan_ids.len()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::MintDbBalance;
    use crate::storage::common::storage::{mock::MockStorage, Storage};
    use solana_sdk::pubkey::Pubkey;

    // The sweep that `fetch_on_chain_balances` delegates to is tested directly in
    // `operator::escrow_sweep` (both encodings, multi-account summing, skip/error arms).

    fn make_operator_config() -> OperatorConfig {
        use solana_sdk::commitment_config::CommitmentLevel;
        OperatorConfig {
            db_poll_interval: std::time::Duration::from_secs(1),
            batch_size: 10,
            retry_max_attempts: 3,
            retry_base_delay: std::time::Duration::from_millis(100),
            channel_buffer_size: 100,
            rpc_commitment: CommitmentLevel::Confirmed,
            alert_webhook_url: None,
            reconciliation_interval: std::time::Duration::from_secs(60),
            reconciliation_tolerance_bps: 10,
            reconciliation_webhook_url: None,
            feepayer_monitor_interval: std::time::Duration::from_secs(60),
            confirmation_poll_interval_ms: 400,
        }
    }

    #[tokio::test]
    async fn run_reconciliation_returns_ok_when_precancelled() {
        use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
        use crate::storage::common::storage::{mock::MockStorage, Storage};
        use solana_sdk::commitment_config::CommitmentConfig;
        use std::sync::Arc;

        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig::confirmed(),
        ));
        let channel_rpc = rpc_client.clone();
        let config = make_operator_config();
        let ct = CancellationToken::new();
        ct.cancel(); // pre-cancel so the loop exits immediately

        let result = run_reconciliation(
            storage,
            config,
            rpc_client,
            channel_rpc,
            solana_sdk::pubkey::Pubkey::new_unique(),
            None,
            ct,
        )
        .await;
        assert!(
            result.is_ok(),
            "pre-cancelled reconciliation should return Ok"
        );
    }

    // ── evaluate_insolvency (pure) ────────────────────────────────────

    #[test]
    fn balanced_is_none() {
        assert!(evaluate_insolvency(1000, 1000, 0, 0).is_none());
    }

    #[test]
    fn over_custody_is_none() {
        // Custody richer than supply is the benign direction.
        assert!(evaluate_insolvency(2000, 1000, 0, 0).is_none());
    }

    #[test]
    fn supply_excess_within_envelope_is_none() {
        // supply - custody = 100, envelope 100 -> within.
        assert!(evaluate_insolvency(900, 1000, 100, 0).is_none());
    }

    #[test]
    fn supply_excess_beyond_envelope_breaches() {
        // The over-issuance case: supply exceeds custody beyond the envelope.
        let b = evaluate_insolvency(900, 1001, 100, 0).expect("must breach");
        assert_eq!(b.supply_gap, 101);
    }

    #[test]
    fn boundary_equal_to_envelope_plus_tolerance_is_none() {
        // gap exactly envelope+tolerance is within (strict greater-than).
        assert!(evaluate_insolvency(900, 1000, 90, 10).is_none());
        assert!(evaluate_insolvency(900, 1001, 90, 10).is_some());
    }

    #[test]
    fn zero_envelope_zero_tolerance_flags_one_unit() {
        assert!(evaluate_insolvency(1000, 1001, 0, 0).is_some());
    }

    #[test]
    fn envelope_plus_tolerance_saturates_no_wrap() {
        // A huge envelope plus tolerance must saturate to u64::MAX, never wrap to
        // a tiny allowance that would over-halt on a 1-unit gap.
        assert!(evaluate_insolvency(0, 1, u64::MAX, 1).is_none());
    }

    #[test]
    fn insolvency_tolerance_raw_scales_and_saturates() {
        assert_eq!(insolvency_tolerance_raw(10_000, 10), 10);
        assert_eq!(insolvency_tolerance_raw(1_000_000, 1), 100);
        assert_eq!(insolvency_tolerance_raw(0, 100), 0);
        // A large bps against max custody must saturate, not wrap down.
        assert_eq!(insolvency_tolerance_raw(u64::MAX, u16::MAX), u64::MAX);
    }

    #[test]
    fn insolvency_delta_bps_saturates_on_small_custody() {
        assert_eq!(insolvency_delta_bps(10_000, 100), 100);
        assert_eq!(insolvency_delta_bps(0, 5), u64::MAX);
        // A huge gap over a tiny custody overflows u64 in bps; saturate, don't wrap.
        assert_eq!(insolvency_delta_bps(1, u64::MAX), u64::MAX);
    }

    #[test]
    fn envelope_to_u64_maps_exact_negative_overflow() {
        use bigdecimal::BigDecimal;
        assert_eq!(envelope_to_u64(&BigDecimal::from(1500u64)), 1500);
        // A negative can't arise from a sum, but must clamp to 0 defensively.
        assert_eq!(envelope_to_u64(&BigDecimal::from(-5i64)), 0);
        // An over-u64 sum reports u64::MAX, which only ever enlarges the envelope.
        let over = BigDecimal::from(u64::MAX) + BigDecimal::from(1u64);
        assert_eq!(envelope_to_u64(&over), u64::MAX);
    }

    // -- evaluate_liability_shortfall and ledger mapping (pure) --

    #[test]
    fn evaluate_liability_shortfall_table() {
        let gap = |c, l, t| evaluate_liability_shortfall(c, l, t).map(|b| b.gap);
        assert_eq!(gap(100, 100, 0), None);
        assert_eq!(gap(200, 100, 0), None, "custody richer is benign");
        assert_eq!(gap(100, 110, 10), None, "gap equal to tolerance is within");
        assert_eq!(gap(100, 111, 10), Some(11));
        assert_eq!(gap(0, 1, 0), Some(1), "zero custody with liabilities flags");
        assert_eq!(gap(0, u64::MAX, u64::MAX), None);
        let b = evaluate_liability_shortfall(100, 111, 10).unwrap();
        assert_eq!((b.liabilities, b.tolerance), (111, 10));
    }

    #[test]
    fn ledger_net_maps_negative_and_overflow() {
        use bigdecimal::BigDecimal;
        assert_eq!(ledger_liability(&ledger_row("m", 500, 200)), 300);
        assert_eq!(ledger_liability(&ledger_row("m", 100, 200)), 0);
        let over = MintDbBalance {
            total_deposits: BigDecimal::from(u64::MAX) + BigDecimal::from(1u64),
            ..ledger_row("m", 0, 0)
        };
        assert_eq!(ledger_liability(&over), u64::MAX);
    }

    #[test]
    fn negative_net_never_breaches_overflow_net_always_breaches() {
        use bigdecimal::BigDecimal;
        let negative = ledger_liability(&ledger_row("m", 100, 200));
        assert!(evaluate_liability_shortfall(0, negative, 0).is_none());

        let over = MintDbBalance {
            total_deposits: BigDecimal::from(u64::MAX) * BigDecimal::from(2u64),
            ..ledger_row("m", 0, 0)
        };
        let custody = 1_000_000;
        let tolerance = insolvency_tolerance_raw(custody, 10);
        assert!(
            evaluate_liability_shortfall(custody, ledger_liability(&over), tolerance).is_some()
        );
    }

    // ── halt layer (persistence + actions), driven directly ───────────

    fn recon_config_zero_tolerance() -> OperatorConfig {
        OperatorConfig {
            reconciliation_tolerance_bps: 0,
            ..make_operator_config()
        }
    }

    fn seed_pending_withdrawal(mock: &MockStorage, id: i64, nonce: i64) {
        use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
        use chrono::Utc;
        let now = Utc::now();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id,
                signature: format!("wd_{id}"),
                trace_id: format!("trace_{id}"),
                slot: 1,
                initiator: "init".to_string(),
                recipient: "recip".to_string(),
                mint: "mint".to_string(),
                amount: TokenAmount(1),
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
            });
    }

    // (custody, db_mints, supply, envelope, mint) for the halt-path tests.
    type BreachMaps = (
        HashMap<Pubkey, u64>,
        HashSet<Pubkey>,
        HashMap<Pubkey, u64>,
        HashMap<Pubkey, u64>,
        Pubkey,
    );

    /// custody 900, supply 1200, envelope 100 -> supply-driven breach.
    fn breach_maps() -> BreachMaps {
        let mint = Pubkey::new_unique();
        let custody = HashMap::from([(mint, 900u64)]);
        let db_mints = HashSet::from([mint]);
        let supply = HashMap::from([(mint, 1200u64)]);
        let envelope = HashMap::from([(mint, 100u64)]);
        (custody, db_mints, supply, envelope, mint)
    }

    #[tokio::test]
    async fn single_beyond_envelope_tick_does_not_halt() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let (custody, db_mints, supply, envelope, _mint) = breach_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        evaluate_and_maybe_halt(
            &storage,
            &recon_config_zero_tolerance(),
            &None,
            &test_webhook_client(),
            &custody,
            1,
            &db_mints,
            &supply,
            &envelope,
            None,
            &mut counters,
            &mut halted,
        )
        .await;

        assert!(!halted, "one breach must not halt");
        assert!(storage.is_reconciliation_halted().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn three_consecutive_breaches_halt() {
        use private_channel_metrics::{HealthConfig, HealthState};
        let mock = MockStorage::new();
        seed_pending_withdrawal(&mock, 1, 1);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let health_state = HealthState::new(HealthConfig::operator());
        let health = Some(health_state.clone());
        let (custody, db_mints, supply, envelope, _mint) = breach_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;
        let config = recon_config_zero_tolerance();

        for _ in 0..3 {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &health,
                &test_webhook_client(),
                &custody,
                1,
                &db_mints,
                &supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(halted, "the 3rd consecutive breach must halt");
        assert!(storage.is_reconciliation_halted().await.unwrap().is_some());
        // Quarantine flipped the active withdrawal.
        let rows = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            rows[0].status,
            crate::storage::common::models::TransactionStatus::ManualReview
        );
        // Forced unhealthy fired.
        assert!(!health_state.is_healthy());
    }

    #[tokio::test]
    async fn within_envelope_tick_resets_counter() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let config = recon_config_zero_tolerance();
        let (custody, db_mints, supply, envelope, mint) = breach_maps();
        // A clean map: supply matches custody, no gap.
        let clean_supply = HashMap::from([(mint, 900u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;
        let webhook = test_webhook_client();

        // breach, breach, clean (reset), breach => no halt.
        for supply in [&supply, &supply, &clean_supply, &supply] {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &None,
                &webhook,
                &custody,
                1,
                &db_mints,
                supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(
            !halted,
            "a clean tick between breaches must reset the counter"
        );
    }

    #[tokio::test]
    async fn errored_read_tick_holds_counter() {
        // Model the loop's behavior: on a read error, perform_reconciliation_check
        // returns before evaluate runs, so the counters are simply not touched.
        // breach, [error tick: skip], breach, breach => halts on the 3rd breach.
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let config = recon_config_zero_tolerance();
        let (custody, db_mints, supply, envelope, _mint) = breach_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for _ in 0..2 {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &None,
                &test_webhook_client(),
                &custody,
                1,
                &db_mints,
                &supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }
        // "error tick" -> evaluate is not called; counters hold at 2.
        assert!(!halted);
        evaluate_and_maybe_halt(
            &storage,
            &config,
            &None,
            &test_webhook_client(),
            &custody,
            1,
            &db_mints,
            &supply,
            &envelope,
            None,
            &mut counters,
            &mut halted,
        )
        .await;
        assert!(halted, "held counter reaches 3 and halts");
    }

    #[tokio::test]
    async fn halt_is_idempotent() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let config = recon_config_zero_tolerance();
        let (custody, db_mints, supply, envelope, _mint) = breach_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for _ in 0..3 {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &None,
                &test_webhook_client(),
                &custody,
                1,
                &db_mints,
                &supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }
        assert!(halted);

        // A new active withdrawal arrives after the halt; a 4th breach tick must
        // NOT re-quarantine it (set-once guard).
        seed_pending_withdrawal(&mock, 99, 5);
        evaluate_and_maybe_halt(
            &storage,
            &config,
            &None,
            &test_webhook_client(),
            &custody,
            1,
            &db_mints,
            &supply,
            &envelope,
            None,
            &mut counters,
            &mut halted,
        )
        .await;

        let rows = mock.pending_transactions.lock().unwrap();
        let fresh = rows.iter().find(|t| t.id == 99).unwrap();
        assert_eq!(
            fresh.status,
            crate::storage::common::models::TransactionStatus::Pending,
            "a post-halt row must not be re-quarantined"
        );
    }

    #[tokio::test]
    async fn zero_custody_mint_enumerated_via_db_set_halts() {
        // A mint with outstanding supply but no custody entry (blocked, or the
        // escrow holds none of it) is enumerated from the DB mint set, evaluated
        // with custody defaulting to 0, and still trips the halt.
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let mint = Pubkey::new_unique();
        let custody: HashMap<Pubkey, u64> = HashMap::new();
        let db_mints = HashSet::from([mint]);
        let supply = HashMap::from([(mint, 500u64)]);
        let envelope = HashMap::from([(mint, 100u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;
        let config = recon_config_zero_tolerance();

        for _ in 0..3 {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &None,
                &test_webhook_client(),
                &custody,
                1,
                &db_mints,
                &supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(
            halted,
            "a zero-custody mint from the DB set must still halt"
        );
        assert!(storage.is_reconciliation_halted().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn breaches_are_counted_per_mint_not_globally() {
        // Three breach ticks spread across two mints must NOT halt: a global
        // counter would reach 3, but per-mint counters peak at 2 (mint A) and 1
        // (mint B), with A reset on its clean third tick.
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let config = recon_config_zero_tolerance();
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let custody = HashMap::from([(a, 900u64), (b, 900u64)]);
        let db_mints = HashSet::from([a, b]);
        let envelope = HashMap::from([(a, 100u64), (b, 100u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;
        let webhook = test_webhook_client();

        let t1 = HashMap::from([(a, 1200u64), (b, 900u64)]); // A breaches, B clean
        let t2 = HashMap::from([(a, 1200u64), (b, 900u64)]); // A breaches, B clean
        let t3 = HashMap::from([(a, 900u64), (b, 1200u64)]); // A clean (reset), B breaches
        for supply in [&t1, &t2, &t3] {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &None,
                &webhook,
                &custody,
                1,
                &db_mints,
                supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(!halted, "per-mint counters must not aggregate across mints");
        assert_eq!(counters.supply.get(&b).copied(), Some(1));
        assert_eq!(
            counters.supply.get(&a).copied(),
            None,
            "A reset on its clean tick"
        );
    }

    #[tokio::test]
    async fn one_mint_supply_read_failure_does_not_block_another_mints_halt() {
        // Mint A is omitted from the supply map (its read failed this tick); mint
        // B is over-issued. A's failure must not suppress B's halt.
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let config = recon_config_zero_tolerance();
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let custody = HashMap::from([(a, 900u64), (b, 900u64)]);
        let db_mints = HashSet::from([a, b]);
        let envelope = HashMap::from([(a, 100u64), (b, 100u64)]);
        let supply = HashMap::from([(b, 1200u64)]); // A absent: read failed
        let mut counters = BreachCounters::default();
        let mut halted = false;
        let webhook = test_webhook_client();

        for _ in 0..3 {
            evaluate_and_maybe_halt(
                &storage,
                &config,
                &None,
                &webhook,
                &custody,
                1,
                &db_mints,
                &supply,
                &envelope,
                None,
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(
            halted,
            "a failed supply read on one mint must not block another"
        );
        assert_eq!(counters.supply.get(&b).copied(), Some(3));
    }

    // -- liability arm at the halt layer --

    // (custody, mints, supply, envelope, liabilities, mint) for the liability tests.
    type LiabilityMaps = (
        HashMap<Pubkey, u64>,
        HashSet<Pubkey>,
        HashMap<Pubkey, u64>,
        HashMap<Pubkey, u64>,
        HashMap<Pubkey, u64>,
        Pubkey,
    );

    /// custody 100, supply 100, envelope 100, liabilities 200: supply clean, custody short.
    fn liability_maps() -> LiabilityMaps {
        let mint = Pubkey::new_unique();
        (
            HashMap::from([(mint, 100u64)]),
            HashSet::from([mint]),
            HashMap::from([(mint, 100u64)]),
            HashMap::from([(mint, 100u64)]),
            HashMap::from([(mint, 200u64)]),
            mint,
        )
    }

    /// One halt-layer tick at custody slot 1 with no health state.
    #[allow(clippy::too_many_arguments)]
    async fn halt_tick(
        storage: &Arc<Storage>,
        config: &OperatorConfig,
        custody: &HashMap<Pubkey, u64>,
        mints: &HashSet<Pubkey>,
        supply: &HashMap<Pubkey, u64>,
        envelope: &HashMap<Pubkey, u64>,
        liabilities: Option<&HashMap<Pubkey, u64>>,
        counters: &mut BreachCounters,
        halted: &mut bool,
    ) {
        evaluate_and_maybe_halt(
            storage,
            config,
            &None,
            &test_webhook_client(),
            custody,
            1,
            mints,
            supply,
            envelope,
            liabilities,
            counters,
            halted,
        )
        .await;
    }

    async fn halt_reason(storage: &Arc<Storage>) -> String {
        storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .expect("halt set")
            .reason
    }

    #[tokio::test]
    async fn pending_deposit_masked_drain_halts_on_liability_arm() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = recon_config_zero_tolerance();
        let (custody, mints, supply, envelope, liabilities, mint) = liability_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for _ in 0..3 {
            assert!(!halted);
            halt_tick(
                &storage,
                &config,
                &custody,
                &mints,
                &supply,
                &envelope,
                Some(&liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
            assert!(counters.supply.is_empty(), "supply arm stays clean");
        }

        assert!(halted);
        let reason = halt_reason(&storage).await;
        assert!(reason.contains("short of ledger liabilities"), "{reason}");
        assert!(!reason.contains("supply by"), "{reason}");
        assert!(reason.contains(&mint.to_string()), "{reason}");
        assert_eq!(counters.liability.get(&mint).copied(), Some(3));
    }

    #[tokio::test]
    async fn unknown_liabilities_hold_liability_counter() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = recon_config_zero_tolerance();
        let (custody, mints, supply, envelope, liabilities, mint) = liability_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for known in [Some(&liabilities), Some(&liabilities), None] {
            halt_tick(
                &storage,
                &config,
                &custody,
                &mints,
                &supply,
                &envelope,
                known,
                &mut counters,
                &mut halted,
            )
            .await;
        }
        assert!(!halted);
        assert_eq!(
            counters.liability.get(&mint).copied(),
            Some(2),
            "held, not reset"
        );

        halt_tick(
            &storage,
            &config,
            &custody,
            &mints,
            &supply,
            &envelope,
            Some(&liabilities),
            &mut counters,
            &mut halted,
        )
        .await;
        assert!(halted, "the held count reaches 3 on the next breach");
        assert!(counters.supply.is_empty());
    }

    #[tokio::test]
    async fn clean_liability_tick_resets_only_its_counter() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = recon_config_zero_tolerance();
        let (custody, db_mints, supply, envelope, mint) = breach_maps();
        let short = HashMap::from([(mint, 2000u64)]);
        let covered = HashMap::from([(mint, 900u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for liabilities in [&short, &short, &covered] {
            halt_tick(
                &storage,
                &config,
                &custody,
                &db_mints,
                &supply,
                &envelope,
                Some(liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(halted, "the supply arm reaches 3 on its own");
        assert!(halt_reason(&storage).await.contains("supply by"));
        assert_eq!(counters.supply.get(&mint).copied(), Some(3));
        assert_eq!(
            counters.liability.get(&mint).copied(),
            None,
            "reset by the clean tick"
        );
    }

    #[tokio::test]
    async fn liability_halt_webhook_reports_liabilities() {
        let mut server = mockito::Server::new_async().await;
        let hook = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "on_chain_balance": 100,
                "db_balance": 200,
                "delta_bps": 10_000,
            })))
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(server.url()),
            ..recon_config_zero_tolerance()
        };
        let (custody, mints, supply, envelope, liabilities, _mint) = liability_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for _ in 0..3 {
            halt_tick(
                &storage,
                &config,
                &custody,
                &mints,
                &supply,
                &envelope,
                Some(&liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(halted);
        hook.assert_async().await;
    }

    #[tokio::test]
    async fn liability_tolerance_boundary_with_nonzero_bps() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = OperatorConfig {
            reconciliation_tolerance_bps: 100,
            ..make_operator_config()
        };
        let mint = Pubkey::new_unique();
        let custody = HashMap::from([(mint, 10_000u64)]);
        let mints = HashSet::from([mint]);
        let supply = HashMap::from([(mint, 10_000u64)]);
        let envelope = HashMap::new();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        let at_tolerance = HashMap::from([(mint, 10_100u64)]);
        halt_tick(
            &storage,
            &config,
            &custody,
            &mints,
            &supply,
            &envelope,
            Some(&at_tolerance),
            &mut counters,
            &mut halted,
        )
        .await;
        assert!(
            counters.liability.is_empty(),
            "gap equal to tolerance never counts"
        );

        let past_tolerance = HashMap::from([(mint, 10_101u64)]);
        halt_tick(
            &storage,
            &config,
            &custody,
            &mints,
            &supply,
            &envelope,
            Some(&past_tolerance),
            &mut counters,
            &mut halted,
        )
        .await;
        assert_eq!(counters.liability.get(&mint).copied(), Some(1));
    }

    #[tokio::test]
    async fn liability_breach_on_one_mint_never_touches_another() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = recon_config_zero_tolerance();
        let (a, b) = (Pubkey::new_unique(), Pubkey::new_unique());
        let custody = HashMap::from([(a, 100u64), (b, 100u64)]);
        let mints = HashSet::from([a, b]);
        let supply = HashMap::from([(a, 100u64)]); // B's supply read failed
        let envelope = HashMap::new();
        let liabilities = HashMap::from([(a, 200u64), (b, 100u64)]);
        let mut counters = BreachCounters {
            supply: HashMap::from([(b, 2)]),
            ..Default::default()
        };
        let mut halted = false;

        for _ in 0..3 {
            halt_tick(
                &storage,
                &config,
                &custody,
                &mints,
                &supply,
                &envelope,
                Some(&liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(halted);
        let reason = halt_reason(&storage).await;
        assert!(
            reason.contains(&a.to_string()) && !reason.contains(&b.to_string()),
            "{reason}"
        );
        assert_eq!(counters.liability, HashMap::from([(a, 3)]));
        assert_eq!(
            counters.supply,
            HashMap::from([(b, 2)]),
            "B's held supply count is untouched"
        );
    }

    #[tokio::test]
    async fn both_arms_breaching_one_mint_halt_once_with_supply_reason() {
        let mut server = mockito::Server::new_async().await;
        let hook = server
            .mock("POST", "/")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let mock = MockStorage::new();
        seed_pending_withdrawal(&mock, 1, 1);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(server.url()),
            ..recon_config_zero_tolerance()
        };
        let (custody, db_mints, supply, envelope, mint) = breach_maps();
        let liabilities = HashMap::from([(mint, 2000u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for _ in 0..4 {
            halt_tick(
                &storage,
                &config,
                &custody,
                &db_mints,
                &supply,
                &envelope,
                Some(&liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert!(halted);
        assert_eq!(mock.calls("set_reconciliation_halt"), 1, "one flag write");
        assert_eq!(
            mock.calls("quarantine_active_withdrawals"),
            1,
            "one quarantine"
        );
        assert!(
            halt_reason(&storage).await.contains("supply by"),
            "supply arm is checked first"
        );
        assert_eq!(counters.supply.get(&mint).copied(), Some(4));
        assert_eq!(counters.liability.get(&mint).copied(), Some(4));
        hook.assert_async().await;
    }

    #[tokio::test]
    async fn durable_halt_suppresses_liability_trip() {
        let mut server = mockito::Server::new_async().await;
        let hook = server
            .mock("POST", "/")
            .with_status(200)
            .expect(0)
            .create_async()
            .await;
        let mock = MockStorage::new();
        seed_pending_withdrawal(&mock, 1, 1);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        storage.set_reconciliation_halt("prior halt").await.unwrap();
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(server.url()),
            ..recon_config_zero_tolerance()
        };
        let (custody, mints, supply, envelope, liabilities, mint) = liability_maps();
        let mut counters = BreachCounters::default();
        let mut halted = true;

        for _ in 0..3 {
            halt_tick(
                &storage,
                &config,
                &custody,
                &mints,
                &supply,
                &envelope,
                Some(&liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
        }

        assert_eq!(halt_reason(&storage).await, "prior halt");
        assert_eq!(
            mock.calls("set_reconciliation_halt"),
            1,
            "only the seed wrote the flag"
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            crate::storage::common::models::TransactionStatus::Pending
        );
        assert_eq!(
            counters.liability.get(&mint).copied(),
            Some(3),
            "evidence still counted"
        );
        hook.assert_async().await;
    }

    #[tokio::test]
    async fn custody_only_mint_has_zero_liabilities() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mint = Pubkey::new_unique();
        let custody = HashMap::from([(mint, 500u64)]);
        let mints = HashSet::from([mint]);
        let supply = HashMap::from([(mint, 500u64)]);
        let known = HashMap::new();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        halt_tick(
            &storage,
            &recon_config_zero_tolerance(),
            &custody,
            &mints,
            &supply,
            &HashMap::new(),
            Some(&known),
            &mut counters,
            &mut halted,
        )
        .await;

        assert!(counters.liability.is_empty());
        assert!(counters.supply.is_empty());
    }

    #[tokio::test]
    async fn ledger_only_mint_with_zero_custody_breaches() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = recon_config_zero_tolerance();
        let mint = Pubkey::new_unique();
        let custody = HashMap::new();
        let mints = HashSet::from([mint]);
        let supply = HashMap::from([(mint, 0u64)]);
        let liabilities = HashMap::from([(mint, 1u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for tick in 1..=3 {
            halt_tick(
                &storage,
                &config,
                &custody,
                &mints,
                &supply,
                &HashMap::new(),
                Some(&liabilities),
                &mut counters,
                &mut halted,
            )
            .await;
            assert_eq!(counters.liability.get(&mint).copied(), Some(tick));
        }

        assert!(halted);
        assert!(halt_reason(&storage)
            .await
            .contains("short of ledger liabilities"));
    }

    #[tokio::test]
    async fn liability_tolerance_is_taken_from_custody() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = OperatorConfig {
            reconciliation_tolerance_bps: 10_000,
            ..make_operator_config()
        };
        let mint = Pubkey::new_unique();
        let custody = HashMap::from([(mint, 1000u64)]);
        let mints = HashSet::from([mint]);
        let supply = HashMap::from([(mint, 1000u64)]);
        // Tolerance from custody is 1000 (gap 2000 breaches); from liabilities it would be 3000.
        let liabilities = HashMap::from([(mint, 3000u64)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        halt_tick(
            &storage,
            &config,
            &custody,
            &mints,
            &supply,
            &HashMap::new(),
            Some(&liabilities),
            &mut counters,
            &mut halted,
        )
        .await;

        assert_eq!(counters.liability.get(&mint).copied(), Some(1));
    }

    /// Mock the escrow custody sweep on a mockito server: the SPL Token program
    /// call returns one jsonParsed token account (mint, amount); Token-2022 empty.
    async fn mock_custody_sweep(server: &mut mockito::Server, mint: Pubkey, amount: u64) {
        let account = format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":2039280,"owner":"{prog}","executable":false,"rentEpoch":0,"space":165,"data":{{"program":"spl-token","space":165,"parsed":{{"type":"account","info":{{"mint":"{mint}","owner":"{owner}","tokenAmount":{{"amount":"{amount}","decimals":6,"uiAmount":null,"uiAmountString":"{amount}"}}}}}}}}}}}}"#,
            ata = Pubkey::new_unique(),
            prog = spl_token::id(),
            owner = Pubkey::new_unique(),
        );
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token::id().to_string()))
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":1}},"value":[{}]}},"id":1}}"#,
                account
            ))
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token_2022::id().to_string()))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":{"context":{"slot":1},"value":[]},"id":1}"#)
            .create_async()
            .await;
    }

    /// One ledger aggregate row as the balance query returns it.
    fn ledger_row(mint: &str, deposits: u64, withdrawals: u64) -> MintDbBalance {
        MintDbBalance {
            mint_address: mint.to_string(),
            token_program: spl_token::id().to_string(),
            total_deposits: bigdecimal::BigDecimal::from(deposits),
            total_withdrawals: bigdecimal::BigDecimal::from(withdrawals),
        }
    }

    /// Commit the escrow indexer's checkpoint under the key it really writes.
    fn seed_checkpoint(mock: &MockStorage, slot: u64) {
        mock.set_checkpoint(&program_key(ProgramType::Escrow), slot);
    }

    /// Answer every channel `getAccountInfo` with an SPL mint at `supply`.
    async fn mock_channel_supply(server: &mut mockito::Server, supply: u64) {
        use base64::Engine as _;
        use spl_token::solana_program::program_option::COption;
        use spl_token::solana_program::program_pack::Pack;
        let mint = spl_token::state::Mint {
            mint_authority: COption::None,
            supply,
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
        };
        let mut buf = vec![0u8; spl_token::state::Mint::LEN];
        mint.pack_into_slice(&mut buf);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"context":{{"slot":1}},"value":{{"owner":"{prog}","lamports":1000000,"data":["{b64}","base64"],"executable":false,"rentEpoch":0}}}}}}"#,
                prog = spl_token::id(),
            ))
            .create_async()
            .await;
    }

    /// One-attempt RPC client so a failing read ends the tick quickly.
    fn fast_rpc(url: String) -> Arc<RpcClientWithRetry> {
        use crate::operator::utils::rpc_util::RetryConfig;
        use solana_sdk::commitment_config::CommitmentConfig;
        Arc::new(RpcClientWithRetry::with_retry_config(
            url,
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::finalized(),
        ))
    }

    /// Mocked custody (one mint at slot 1), channel supply and storage for one tick.
    struct TickEnv {
        custody: mockito::ServerGuard,
        channel: mockito::ServerGuard,
        mock: MockStorage,
        storage: Arc<Storage>,
    }

    async fn tick_env(mint: Pubkey, custody: u64, supply: u64) -> TickEnv {
        let mut custody_server = mockito::Server::new_async().await;
        mock_custody_sweep(&mut custody_server, mint, custody).await;
        let mut channel = mockito::Server::new_async().await;
        mock_channel_supply(&mut channel, supply).await;
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        TickEnv {
            custody: custody_server,
            channel,
            mock,
            storage,
        }
    }

    async fn run_tick(
        env: &TickEnv,
        config: &OperatorConfig,
        counters: &mut BreachCounters,
        halted: &mut bool,
        token: &CancellationToken,
    ) -> Result<(), OperatorError> {
        perform_reconciliation_check(
            &env.storage,
            config,
            &fast_rpc(env.custody.url()),
            &fast_rpc(env.channel.url()),
            Pubkey::new_unique(),
            &test_webhook_client(),
            &None,
            &mut None,
            counters,
            halted,
            token,
        )
        .await
    }

    #[tokio::test]
    async fn ledger_rejects_unparseable_address() {
        let mock = MockStorage::new();
        mock.set_mint_balances(vec![ledger_row("not-a-pubkey", 0, 0)]);
        let storage = Arc::new(Storage::Mock(mock));

        let err = fetch_ledger(&storage, 1).await.unwrap_err();

        match err {
            OperatorError::InvalidPubkey { pubkey, .. } => assert_eq!(pubkey, "not-a-pubkey"),
            other => panic!("expected InvalidPubkey, got {other:?}"),
        }
    }

    /// A failed ledger read must skip the tick without halting or erasing either
    /// building breach counter, the same way a failed supply read does.
    #[tokio::test]
    async fn ledger_read_failure_returns_err_and_holds_counters() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 900, 1200).await;
        seed_checkpoint(&env.mock, 1);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 2000, 0)]);
        env.mock
            .set_should_fail("get_mint_balances_for_reconciliation", true);
        let mut counters = BreachCounters {
            supply: HashMap::from([(mint, 2)]),
            liability: HashMap::from([(mint, 2)]),
        };
        let mut halted = false;

        let res = run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await;

        assert!(res.is_err(), "a ledger read failure must fail the tick");
        assert_eq!(counters.supply, HashMap::from([(mint, 2)]));
        assert_eq!(counters.liability, HashMap::from([(mint, 2)]));
        assert!(!halted, "a failed ledger read must not halt");
        assert!(env
            .storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn ledger_is_read_at_the_custody_slot_when_checkpoint_covers_it() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(env.mock.last_reconciliation_slot(), Some(1));
        assert_eq!(counters.liability.get(&mint).copied(), Some(1));
        assert!(counters.supply.is_empty());
    }

    #[tokio::test]
    async fn absent_checkpoint_skips_wait_and_leaves_liabilities_unknown() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 1000).await;
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        // The wait alone, timed without the mocked RPC round trips around it.
        let started = std::time::Instant::now();
        let outcome = wait_for_ledger(&env.storage, 1, &CancellationToken::new()).await;
        assert_eq!(outcome, LedgerWait::Unknown);
        assert!(started.elapsed() < LEDGER_CATCHUP_TIMEOUT / 2, "no wait");
        env.mock.call_counts.lock().unwrap().clear();

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            env.mock.calls("get_committed_checkpoint"),
            1,
            "one read, no poll"
        );
        assert!(counters.liability.is_empty());
        assert_eq!(env.mock.last_reconciliation_slot(), Some(1));
        assert_eq!(counters.supply.get(&mint).copied(), Some(1));
    }

    #[tokio::test]
    async fn lagging_checkpoint_times_out_to_unknown() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 1000).await;
        seed_checkpoint(&env.mock, 0);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters {
            liability: HashMap::from([(mint, 2)]),
            ..Default::default()
        };
        let mut halted = false;

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(counters.liability.get(&mint).copied(), Some(2), "held");
        assert!(!halted);
        assert_eq!(counters.supply.get(&mint).copied(), Some(1));
        assert!(env.mock.calls("get_committed_checkpoint") > 1, "it polled");
    }

    #[tokio::test]
    async fn cancelled_token_ends_wait_immediately() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 1000).await;
        seed_checkpoint(&env.mock, 0);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters {
            liability: HashMap::from([(mint, 2)]),
            ..Default::default()
        };
        let mut halted = false;
        let token = CancellationToken::new();
        token.cancel();

        // The wait alone, timed without the mocked RPC round trips around it.
        let started = std::time::Instant::now();
        let outcome = wait_for_ledger(&env.storage, 1, &token).await;
        assert_eq!(outcome, LedgerWait::Cancelled);
        assert!(started.elapsed() < LEDGER_CATCHUP_TIMEOUT / 2);
        env.mock.call_counts.lock().unwrap().clear();

        let res = run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &token,
        )
        .await;

        assert!(
            res.is_ok(),
            "a cancelled wait ends the tick cleanly: {res:?}"
        );
        assert_eq!(
            env.mock.calls("get_committed_checkpoint"),
            1,
            "one poll, no wait"
        );
        assert_eq!(counters.liability, HashMap::from([(mint, 2)]));
        assert!(counters.supply.is_empty(), "nothing evaluated");
        assert_eq!(env.mock.last_reconciliation_slot(), None, "ledger not read");
        assert!(!halted);
    }

    #[tokio::test]
    async fn checkpoint_advanced_during_wait_is_covered() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 0);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        // The indexer commits the custody slot only after the tick's first poll.
        let writer = env.mock.clone();
        tokio::spawn(async move {
            while writer.calls("get_committed_checkpoint") == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            seed_checkpoint(&writer, 1);
        });
        let mut counters = BreachCounters::default();
        let mut halted = false;

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(counters.liability.get(&mint).copied(), Some(1));
        assert!(env.mock.calls("get_committed_checkpoint") >= 2);
    }

    #[tokio::test]
    async fn checkpoint_read_failing_every_poll_leaves_liabilities_unknown() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 1000).await;
        seed_checkpoint(&env.mock, 1);
        env.mock.set_should_fail("get_committed_checkpoint", true);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters {
            liability: HashMap::from([(mint, 2)]),
            ..Default::default()
        };
        let mut halted = false;

        let res = run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await;

        assert!(res.is_ok(), "{res:?}");
        assert_eq!(counters.liability.get(&mint).copied(), Some(2), "held");
        assert_eq!(counters.supply.get(&mint).copied(), Some(1));
        assert!(!halted);
    }

    #[tokio::test]
    async fn transient_checkpoint_read_failure_then_covered() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        env.mock.set_fail_times("get_committed_checkpoint", 2);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(counters.liability.get(&mint).copied(), Some(1));
    }

    #[tokio::test]
    async fn withdraw_checkpoint_does_not_cover_the_escrow_ledger() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        env.mock
            .set_checkpoint(&program_key(ProgramType::Withdraw), 1);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(counters.liability.is_empty());
        assert_eq!(env.mock.calls("get_committed_checkpoint"), 1);
    }

    /// Supply matches custody while a pending deposit pads the envelope, yet the
    /// escrow holds half of what it owes: three ticks halt on the liability arm.
    #[tokio::test]
    async fn reviewer_scenario_halts_over_three_ticks() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        let id = seed_orphan_deposit(&env.mock, &mint.to_string());
        env.mock
            .pending_transactions
            .lock()
            .unwrap()
            .iter_mut()
            .find(|t| t.id == id)
            .unwrap()
            .amount = TokenAmount(100);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 200, 0)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;
        let config = recon_config_zero_tolerance();

        for _ in 0..3 {
            assert!(!halted);
            run_tick(
                &env,
                &config,
                &mut counters,
                &mut halted,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        }

        assert!(halted);
        let reason = env
            .storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .expect("halt set")
            .reason;
        assert!(reason.contains("short of ledger liabilities"), "{reason}");
        assert!(counters.supply.is_empty(), "supply arm stayed clean");
    }

    #[tokio::test]
    async fn ledger_rows_enumerate_zero_and_withdrawal_rows() {
        let (a, b, c) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let env = tick_env(c, 100, 50).await;
        seed_checkpoint(&env.mock, 1);
        env.mock.set_mint_balances(vec![
            ledger_row(&a.to_string(), 0, 0),
            ledger_row(&b.to_string(), 300, 300),
        ]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(counters.supply, HashMap::from([(a, 1), (b, 1)]));
        assert!(counters.liability.is_empty());
    }

    /// A tick whose halt-input load fails (channel supply RPC down) must not
    /// halt, must not reset an existing breach counter, and must not set the
    /// flag: the evidence is held for the next finalized read rather than
    /// thrown away by a transient glitch.
    #[tokio::test]
    async fn supply_read_failure_holds_counters_and_does_not_halt() {
        use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
        use solana_sdk::commitment_config::CommitmentConfig;

        let fast = || RetryConfig {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        };

        let mint = Pubkey::new_unique();
        // Custody holds 900 on-chain; the ledger row only enumerates the mint.
        let mut custody_server = mockito::Server::new_async().await;
        mock_custody_sweep(&mut custody_server, mint, 900).await;

        let mock = MockStorage::new();
        mock.set_mint_balances(vec![ledger_row(&mint.to_string(), 0, 0)]);
        let storage = Arc::new(Storage::Mock(mock));

        let custody_rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            custody_server.url(),
            fast(),
            CommitmentConfig::finalized(),
        ));
        // Channel RPC returns 503 for the supply getAccountInfo, so the halt-input
        // read fails deterministically (a transient error, not AccountNotFound).
        let mut channel_server = mockito::Server::new_async().await;
        channel_server
            .mock("POST", "/")
            .with_status(503)
            .create_async()
            .await;
        let channel_rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            channel_server.url(),
            fast(),
            CommitmentConfig::finalized(),
        ));

        let config = OperatorConfig {
            reconciliation_tolerance_bps: 0,
            ..make_operator_config()
        };
        let webhook_client = test_webhook_client();
        let mut orphans = None;
        // A breach counter already building; a failed load must hold it intact.
        let mut counters = BreachCounters {
            supply: HashMap::from([(mint, 2)]),
            ..Default::default()
        };
        let mut halted = false;

        let res = perform_reconciliation_check(
            &storage,
            &config,
            &custody_rpc,
            &channel_rpc,
            Pubkey::new_unique(),
            &webhook_client,
            &None,
            &mut orphans,
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await;
        assert!(res.is_ok(), "tick should complete: {res:?}");

        // Load failed: no halt, flag unset, and the counter is HELD (not reset).
        assert!(!halted);
        assert_eq!(
            counters.supply.get(&mint).copied(),
            Some(2),
            "a read failure must hold the breach counter, not reset it"
        );
    }

    #[tokio::test]
    async fn halted_guard_is_resynced_from_durable_flag_each_tick() {
        // The in-memory guard is refreshed from the durable flag every tick, not
        // latched for the process lifetime: a manual clear (runbook) must let a
        // fresh insolvency re-trip, and a still-set flag keeps re-firing suppressed.
        use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
        use solana_sdk::commitment_config::CommitmentConfig;

        // Unreachable RPC: the custody fetch fails fast, but only after the guard
        // has already been resynced from the flag at the top of the check.
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://127.0.0.1:1".to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::finalized(),
        ));
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let config = recon_config_zero_tolerance();
        let webhook = test_webhook_client();
        let mut orphans = None;
        let mut counters = BreachCounters::default();

        // Flag set: the guard is raised even though it started false.
        storage.set_reconciliation_halt("prior halt").await.unwrap();
        let mut halted = false;
        let _ = perform_reconciliation_check(
            &storage,
            &config,
            &rpc,
            &rpc,
            Pubkey::new_unique(),
            &webhook,
            &None,
            &mut orphans,
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await;
        assert!(halted, "a set durable flag must raise the in-memory guard");

        // Flag cleared: the guard drops so a fresh insolvency can re-trip.
        storage.clear_reconciliation_halt().await.unwrap();
        let mut halted = true;
        let _ = perform_reconciliation_check(
            &storage,
            &config,
            &rpc,
            &rpc,
            Pubkey::new_unique(),
            &webhook,
            &None,
            &mut orphans,
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await;
        assert!(
            !halted,
            "a cleared durable flag must drop the in-memory guard"
        );
    }

    fn test_webhook_client() -> WebhookClient {
        WebhookClient::new(
            Duration::from_secs(10),
            WebhookRetryConfig::new(3, Duration::from_millis(500), Duration::from_secs(5)),
        )
        .expect("test webhook client")
    }

    #[tokio::test]
    async fn test_send_webhook_alert_no_url() {
        // Test with no webhook URL configured - should not fail
        let mint = Pubkey::new_unique();
        let mismatches = vec![BalanceMismatch {
            mint,
            on_chain_balance: 1000,
            db_balance: 900,
            delta_bps: 1000,
        }];

        let client = test_webhook_client();
        let result = send_webhook_alert(&None, &mismatches, &client).await;
        assert!(
            result.is_ok(),
            "Should succeed when no webhook URL configured"
        );
    }

    #[tokio::test]
    async fn test_send_webhook_alert_empty_mismatches() {
        // Test with empty mismatches - should succeed immediately
        let webhook_url = Some("http://example.com/webhook".to_string());
        let mismatches: Vec<BalanceMismatch> = vec![];

        let client = test_webhook_client();
        let result = send_webhook_alert(&webhook_url, &mismatches, &client).await;
        assert!(result.is_ok(), "Should succeed with empty mismatches");
    }

    #[tokio::test]
    async fn test_send_webhook_alert_success() {
        // Test successful webhook delivery with mockito
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let mint = Pubkey::new_unique();
        let mismatches = vec![BalanceMismatch {
            mint,
            on_chain_balance: 1000,
            db_balance: 900,
            delta_bps: 1000,
        }];

        let client = test_webhook_client();
        let result = send_webhook_alert(&webhook_url, &mismatches, &client).await;
        assert!(result.is_ok(), "Should successfully send webhook");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_send_webhook_alert_retry_then_success() {
        // Test webhook retry logic - fail once, then succeed
        let mut server = mockito::Server::new_async().await;

        // First request fails with 500
        let mock_fail = server
            .mock("POST", "/")
            .with_status(500)
            .expect(1)
            .create_async()
            .await;

        // Second request succeeds
        let mock_success = server
            .mock("POST", "/")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let mint = Pubkey::new_unique();
        let mismatches = vec![BalanceMismatch {
            mint,
            on_chain_balance: 1000,
            db_balance: 900,
            delta_bps: 1000,
        }];

        let client = test_webhook_client();
        let result = send_webhook_alert(&webhook_url, &mismatches, &client).await;
        assert!(result.is_ok(), "Should succeed after retry");

        mock_fail.assert_async().await;
        mock_success.assert_async().await;
    }

    #[tokio::test]
    async fn test_send_webhook_alert_max_retries_exceeded() {
        // Test webhook fails after max retries
        let mut server = mockito::Server::new_async().await;

        // All requests fail with 500
        let mock = server
            .mock("POST", "/")
            .with_status(500)
            .expect(3) // Should retry 3 times
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let mint = Pubkey::new_unique();
        let mismatches = vec![BalanceMismatch {
            mint,
            on_chain_balance: 1000,
            db_balance: 900,
            delta_bps: 1000,
        }];

        let client = test_webhook_client();
        let result = send_webhook_alert(&webhook_url, &mismatches, &client).await;
        assert!(result.is_err(), "Should fail after max retries");
        assert!(
            matches!(result.unwrap_err(), OperatorError::WebhookError(_)),
            "Should return WebhookError"
        );

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_send_webhook_alert_multiple_mismatches() {
        // Test sending multiple webhook alerts
        let mut server = mockito::Server::new_async().await;

        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .expect(2) // Should send 2 webhooks
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mismatches = vec![
            BalanceMismatch {
                mint: mint1,
                on_chain_balance: 1000,
                db_balance: 900,
                delta_bps: 1000,
            },
            BalanceMismatch {
                mint: mint2,
                on_chain_balance: 2000,
                db_balance: 1800,
                delta_bps: 1000,
            },
        ];

        let client = test_webhook_client();
        let result = send_webhook_alert(&webhook_url, &mismatches, &client).await;
        assert!(result.is_ok(), "Should successfully send all webhooks");

        mock.assert_async().await;
    }

    // ── orphan-deposit webhook alert tests ────────────────────────────────

    #[tokio::test]
    async fn test_send_orphan_deposit_alert_no_url() {
        // No webhook URL configured with orphan ids present -> no-op Ok(())
        let orphan_ids = vec![123_i64, 456];

        let client = test_webhook_client();
        let result = send_orphan_deposit_alert(&None, &orphan_ids, &client).await;
        assert!(
            result.is_ok(),
            "Should succeed when no webhook URL configured"
        );
    }

    #[tokio::test]
    async fn test_send_orphan_deposit_alert_success() {
        // Successful single POST of the orphan id payload
        let mut server = mockito::Server::new_async().await;
        // Assert the received alert carries the expected payload. The
        // timestamp is dynamic, so match only the stable fields via partial
        // JSON.
        let mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "row_count": 2,
                "orphan_ids": [123, 456],
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .expect(1)
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let orphan_ids = vec![123_i64, 456];

        let client = test_webhook_client();
        let result = send_orphan_deposit_alert(&webhook_url, &orphan_ids, &client).await;
        assert!(result.is_ok(), "Should successfully send orphan webhook");

        // Verifies the request was received AND its body matched the payload.
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_send_orphan_deposit_alert_max_retries_exceeded() {
        // All requests fail -> returns WebhookError after exhausting retries
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(500)
            .expect(3) // Should retry 3 times
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let orphan_ids = vec![123_i64];

        let client = test_webhook_client();
        let result = send_orphan_deposit_alert(&webhook_url, &orphan_ids, &client).await;
        assert!(result.is_err(), "Should fail after max retries");
        assert!(
            matches!(result.unwrap_err(), OperatorError::WebhookError(_)),
            "Should return WebhookError"
        );

        mock.assert_async().await;
    }

    /// Baseline tick posts the full orphan set once; a steady-state tick with the same orphans posts nothing more.
    #[tokio::test]
    async fn check_orphan_deposit_rows_webhook_posts_baseline_once_then_silent() {
        let mut server = mockito::Server::new_async().await;
        // Exactly one POST is expected across both ticks: the baseline.
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let client = test_webhook_client();

        let mock_storage = MockStorage::new();
        seed_orphan_deposit(&mock_storage, "mint_a");
        seed_orphan_deposit(&mock_storage, "mint_b");
        let storage = Arc::new(Storage::Mock(mock_storage));

        let mut state: Option<HashSet<i64>> = None;

        // Baseline tick: posts the full current orphan set.
        check_orphan_deposit_rows(&storage, &mut state, &client, &webhook_url).await;
        assert_eq!(
            state.as_ref().map(|s| s.len()),
            Some(2),
            "baseline should capture every current orphan"
        );

        // Steady-state tick: same orphans, no new ids -> no additional POST.
        check_orphan_deposit_rows(&storage, &mut state, &client, &webhook_url).await;
        assert_eq!(
            state.as_ref().map(|s| s.len()),
            Some(2),
            "steady-state tick must leave the dedup set unchanged"
        );

        mock.assert_async().await;
    }

    /// A newly-arrived orphan in a delta tick posts exactly once for the new
    /// id (not the already-seen ones).
    #[tokio::test]
    async fn check_orphan_deposit_rows_webhook_posts_new_orphan_once() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let client = test_webhook_client();

        let mock_storage = MockStorage::new();
        let id_a = seed_orphan_deposit(&mock_storage, "mint_a");
        let id_b = seed_orphan_deposit(&mock_storage, "mint_b");
        let storage = Arc::new(Storage::Mock(mock_storage));

        // Pre-seed dedup state as if `id_a` had already been alerted; only
        // `id_b` is new and should drive a single webhook POST.
        let mut state: Option<HashSet<i64>> = Some([id_a].into_iter().collect());

        check_orphan_deposit_rows(&storage, &mut state, &client, &webhook_url).await;

        let seen = state.expect("state should remain Some after delta tick");
        assert_eq!(
            seen.len(),
            2,
            "dedup set should now hold both the pre-seen and the new orphan"
        );
        assert!(
            seen.contains(&id_a),
            "already-seen orphan must stay in the dedup set"
        );
        assert!(
            seen.contains(&id_b),
            "newly-arrived orphan must be added to the dedup set"
        );

        // Exactly one POST: only the new id (`id_b`) is alerted; the
        // already-seen `id_a` does not re-post.
        mock.assert_async().await;
    }

    /// A failed baseline alert must leave state `None` so the next tick retries
    /// instead of suppressing the orphan forever.
    #[tokio::test]
    async fn check_orphan_deposit_rows_failed_webhook_does_not_seed_baseline_state() {
        let mut server = mockito::Server::new_async().await;
        // Endpoint is down: every attempt 500s, so the alert exhausts retries.
        let mock = server
            .mock("POST", "/")
            .with_status(500)
            .expect(3) // alert exhausts its 3 retries
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let client = test_webhook_client();

        let mock_storage = MockStorage::new();
        seed_orphan_deposit(&mock_storage, "mint_a");
        let storage = Arc::new(Storage::Mock(mock_storage));

        let mut state: Option<HashSet<i64>> = None;
        check_orphan_deposit_rows(&storage, &mut state, &client, &webhook_url).await;

        assert!(
            state.is_none(),
            "failed baseline alert must leave dedup state unset so the next tick retries"
        );
        mock.assert_async().await;
    }

    /// A failed delta alert must not mark the new orphan seen, so a later tick
    /// re-alerts.
    #[tokio::test]
    async fn check_orphan_deposit_rows_failed_webhook_does_not_mark_new_orphan_seen() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(500)
            .expect(3) // alert exhausts its 3 retries
            .create_async()
            .await;

        let webhook_url = Some(server.url());
        let client = test_webhook_client();

        let mock_storage = MockStorage::new();
        let id_a = seed_orphan_deposit(&mock_storage, "mint_a");
        let storage = Arc::new(Storage::Mock(mock_storage));

        // Already in delta mode (baseline established as empty); id_a is new.
        let mut state: Option<HashSet<i64>> = Some(HashSet::new());
        check_orphan_deposit_rows(&storage, &mut state, &client, &webhook_url).await;

        let seen = state.expect("state must remain Some after a delta tick");
        assert!(
            !seen.contains(&id_a),
            "a new orphan whose alert failed must not be marked seen (so it re-alerts)"
        );
        mock.assert_async().await;
    }

    // The dedup tests below cover the orphan-detection wiring at the
    // boundary we *can* exercise without a test validator: the
    // `check_orphan_deposit_rows` helper, driven through its three state
    // transitions (baseline / stable / new-orphan) plus the
    // storage-error path.

    /// Insert a deposit row whose mint has no `mints` entry. Used by the
    /// dedup tests below, the actual orphan-detection SQL is exercised by
    /// the storage-layer tests; here we only need MockStorage to return the
    /// mint as orphaned so `check_orphan_deposit_rows` is driven through
    /// its state transitions.
    fn seed_orphan_deposit(
        mock: &crate::storage::common::storage::mock::MockStorage,
        mint: &str,
    ) -> i64 {
        use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
        use chrono::Utc;
        let mut txs = mock.pending_transactions.lock().unwrap();
        let id = txs.len() as i64 + 1;
        txs.push(DbTransaction {
            id,
            signature: format!("sig_orphan_{}", mint),
            trace_id: format!("trace_orphan_{}", mint),
            slot: 1,
            initiator: "init".to_string(),
            recipient: "recip".to_string(),
            mint: mint.to_string(),
            amount: TokenAmount(1),
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Pending,
            created_at: Utc::now(),
            updated_at: Utc::now(),
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
        });
        id
    }

    /// Baseline tick (`None` in → `Some` out): full current orphan set is
    /// captured into the dedup cache, info-log only.
    #[tokio::test]
    async fn check_orphan_deposit_rows_baseline_seeds_state() {
        let mock = MockStorage::new();
        let id_a = seed_orphan_deposit(&mock, "mint_a");
        let id_b = seed_orphan_deposit(&mock, "mint_b");

        let storage = Arc::new(Storage::Mock(mock));
        let mut state: Option<HashSet<i64>> = None;

        check_orphan_deposit_rows(&storage, &mut state, &test_webhook_client(), &None).await;

        let seen = state.expect("baseline tick must populate dedup cache");
        assert_eq!(
            seen.len(),
            2,
            "baseline should capture every current orphan"
        );
        assert!(seen.contains(&id_a));
        assert!(seen.contains(&id_b));
    }

    /// Baseline with no orphans still flips state to `Some(empty)` so the
    /// next tick is in delta mode.
    #[tokio::test]
    async fn check_orphan_deposit_rows_baseline_with_no_orphans_still_flips_state() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut state: Option<HashSet<i64>> = None;

        check_orphan_deposit_rows(&storage, &mut state, &test_webhook_client(), &None).await;

        let seen = state.expect("baseline must seed state even when empty");
        assert!(seen.is_empty());
    }

    /// Delta tick where the orphan set hasn't changed: dedup cache must not
    /// grow and nothing should be logged (cache contents unchanged).
    #[tokio::test]
    async fn check_orphan_deposit_rows_stable_orphans_dont_realert() {
        let mock = MockStorage::new();
        let id_a = seed_orphan_deposit(&mock, "mint_a");

        let storage = Arc::new(Storage::Mock(mock));
        // Pre-seed dedup state as if a prior baseline tick had already
        // captured `id_a`. A second tick with the same row must be a no-op
        // as far as state is concerned.
        let mut state: Option<HashSet<i64>> = Some([id_a].into_iter().collect());

        check_orphan_deposit_rows(&storage, &mut state, &test_webhook_client(), &None).await;

        let seen = state.expect("state should remain Some after delta tick");
        assert_eq!(seen.len(), 1, "stable orphan must not grow the cache");
        assert!(seen.contains(&id_a));
    }

    /// Delta tick where a NEW orphan row has appeared, even sharing the same
    /// mint as a previously-seen orphan: the new id must extend the cache.
    /// This is the regression guard for the old mint-level dedup, which
    /// would have silently suppressed subsequent deposits on a known mint.
    #[tokio::test]
    async fn check_orphan_deposit_rows_new_orphan_extends_cache() {
        let mock = MockStorage::new();
        let id_a = seed_orphan_deposit(&mock, "shared_mint");
        let id_b = seed_orphan_deposit(&mock, "shared_mint");

        let storage = Arc::new(Storage::Mock(mock));
        // Cache already contains `id_a` from a prior tick. `id_b` is a
        // brand-new orphan row (same mint, different deposit) that must
        // trigger the error log even though the mint is already "known".
        let mut state: Option<HashSet<i64>> = Some([id_a].into_iter().collect());

        check_orphan_deposit_rows(&storage, &mut state, &test_webhook_client(), &None).await;

        let seen = state.expect("state should remain Some after delta tick");
        assert_eq!(seen.len(), 2, "cache should grow by exactly the new row");
        assert!(seen.contains(&id_a));
        assert!(seen.contains(&id_b));
    }

    /// Storage failure must leave the dedup state untouched so the next
    /// tick can retry the baseline (or delta) cleanly.
    #[tokio::test]
    async fn check_orphan_deposit_rows_storage_error_preserves_state() {
        let mock = MockStorage::new();
        mock.set_should_fail("get_orphan_deposit_ids", true);
        let storage = Arc::new(Storage::Mock(mock));
        let mut state: Option<HashSet<i64>> = None;

        check_orphan_deposit_rows(&storage, &mut state, &test_webhook_client(), &None).await;

        assert!(
            state.is_none(),
            "baseline must not be marked done if the storage query failed"
        );
    }
}
