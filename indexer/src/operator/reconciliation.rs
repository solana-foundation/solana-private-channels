//! Escrow balance reconciliation module
//!
//! Per mint, freezes the pipelines after `HALT_CONFIRM_TICKS` finalized breaches of either invariant:
//! channel supply within custody plus the in-flight envelope (over-issuance), or ledger liabilities
//! at the custody slot within custody (a drain that unminted deposits hide from the supply check).

use crate::config::{OperatorConfig, ProgramType};
use crate::error::OperatorError;
use crate::indexer::checkpoint::program_key;
use crate::metrics::{
    OPERATOR_RECONCILIATION_INPUT_DARK_TICKS, OPERATOR_RECONCILIATION_LIABILITY_DARK_TICKS,
    OPERATOR_RECONCILIATION_LIABILITY_SHORTFALL, OPERATOR_RECONCILIATION_LIABILITY_UNKNOWN,
};
use crate::operator::escrow_sweep::{
    channel_anchor, fetch_channel_supply_at, fetch_escrow_custody, EscrowCustody,
};
use crate::operator::RpcClientWithRetry;
use crate::storage::common::amount::{net_to_u64, NetBalance};
use crate::storage::common::models::MintDbBalance;
use crate::storage::Storage;
use private_channel_core::webhook::{WebhookClient, WebhookRetryConfig};
use private_channel_metrics::{HealthState, MetricLabel};
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

/// Tries at writing the halt flag within one tick; the guard stays down until one lands.
const HALT_WRITE_ATTEMPTS: u32 = 3;

/// Consecutive ticks with a required input unreadable before the pipelines are frozen.
const INPUT_DARK_HALT_TICKS: u32 = 3;

/// Longest a tick waits for the escrow indexer's checkpoint to reach the custody slot.
#[cfg(not(test))]
const LEDGER_CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const LEDGER_CATCHUP_TIMEOUT: Duration = Duration::from_millis(500);

/// Consecutive unpinnable ticks before the dark liability arm is alerted on, and the
/// cadence it re-alerts at afterwards. A frozen checkpoint keeps the arm off indefinitely,
/// so this has to keep firing rather than notify once.
const LIABILITY_DARK_ALERT_TICKS: u32 = 3;

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
    /// The most severe halt the current incident has paged for, so a failing write never re-pages.
    announced: Option<HaltKind>,
    /// An insolvency halt is in force; only that suppresses a new breach trip.
    insolvency_halted: bool,
}

/// Why the pipelines are frozen. An insolvency outranks an outage and is never replaced by one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HaltKind {
    /// Reconciliation inputs were unreadable; nothing is proven wrong.
    Outage,
    /// A breach was confirmed; active withdrawals need a human.
    Insolvency,
}

impl BreachCounters {
    /// Whether any mint has reached the breach count that trips a halt.
    fn any_confirmed(&self) -> bool {
        self.supply
            .values()
            .chain(self.liability.values())
            .any(|&count| count >= HALT_CONFIRM_TICKS)
    }
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

    // Consecutive ticks the liability arm could not be pinned for. Held across ticks so a
    // silently dark arm is alertable rather than just repeatedly warned about.
    let mut liability_dark_ticks: u32 = 0;
    let mut input_dark_ticks: u32 = 0;

    // Seed the set-once guard from the durable flag so a restart into an active
    // halt does not re-quarantine or re-webhook; it stays frozen until cleared.
    // A failed read seeds "not halted"; the flag write itself never lets an outage replace an insolvency.
    let seeded = storage.is_reconciliation_halted().await.ok().flatten();
    let mut halted = seeded.is_some();
    breach_counters.insolvency_halted = seeded.is_some_and(|flag| flag.insolvency);

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
            &mut liability_dark_ticks,
            &mut input_dark_ticks,
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
pub(crate) fn insolvency_tolerance_raw(custody: u64, tolerance_bps: u16) -> u64 {
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

/// Whether a tick read every input the invariants need.
#[derive(Debug, PartialEq, Eq)]
enum TickInputs {
    Complete,
    /// Something required could not be read; carries the reason.
    Missing(String),
}

/// Performs a single reconciliation check, then counts a tick that could not read
/// every required input toward the input-dark halt.
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
    liability_dark_ticks: &mut u32,
    input_dark_ticks: &mut u32,
    cancellation_token: &CancellationToken,
) -> Result<(), OperatorError> {
    let result = check_invariants(
        storage,
        config,
        rpc_client,
        channel_rpc,
        escrow_instance_id,
        webhook_client,
        health,
        previously_alerted_orphans,
        breach_counters,
        halted,
        liability_dark_ticks,
        cancellation_token,
    )
    .await;
    // A tick cut short by shutdown is evidence of nothing.
    if cancellation_token.is_cancelled() {
        return result.map(|_| ());
    }
    let missing = match &result {
        Ok(TickInputs::Complete) => None,
        Ok(TickInputs::Missing(reason)) => Some(reason.clone()),
        Err(e) => Some(e.to_string()),
    };
    track_input_state(
        storage,
        config,
        health,
        webhook_client,
        missing,
        input_dark_ticks,
        halted,
        breach_counters,
    )
    .await;
    // An incident whose flag never landed ends once nothing would trip it, so the next one pages.
    if !*halted && *input_dark_ticks < INPUT_DARK_HALT_TICKS && !breach_counters.any_confirmed() {
        breach_counters.announced = None;
    }
    result.map(|_| ())
}

/// Count a tick with a missing input and freeze once the streak reaches the limit. Unread
/// inputs mean the invariants are unchecked, so value stops rather than moves blind; `>=`
/// sets a flag cleared while the inputs are still dark again on the next dark tick.
#[allow(clippy::too_many_arguments)]
async fn track_input_state(
    storage: &Arc<Storage>,
    config: &OperatorConfig,
    health: &Option<Arc<HealthState>>,
    webhook_client: &WebhookClient,
    missing: Option<String>,
    input_dark_ticks: &mut u32,
    halted: &mut bool,
    breach_counters: &mut BreachCounters,
) {
    match missing {
        None => *input_dark_ticks = 0,
        Some(reason) => {
            *input_dark_ticks = input_dark_ticks.saturating_add(1);
            error!(
                dark_ticks = *input_dark_ticks,
                reason = %reason,
                "Reconciliation could not read a required input this tick"
            );
            if *input_dark_ticks >= INPUT_DARK_HALT_TICKS && !*halted {
                let halt_reason = format!(
                    "reconciliation halt: required inputs unavailable for {} consecutive ticks (last: {})",
                    input_dark_ticks, reason
                );
                error!(reason = %halt_reason, "RECONCILIATION HALT tripped; freezing both pipelines");
                let in_force =
                    freeze_pipelines(storage, health, &halt_reason, HaltKind::Outage).await;
                *halted = in_force.is_some();
                // The write found an insolvency halt already set, which has paged on its own.
                if in_force == Some(HaltKind::Insolvency) {
                    breach_counters.insolvency_halted = true;
                } else if breach_counters.announced.is_none() {
                    match send_inputs_dark_halt_alert(
                        &config.reconciliation_webhook_url,
                        *input_dark_ticks,
                        &halt_reason,
                        webhook_client,
                    )
                    .await
                    {
                        Ok(()) => breach_counters.announced = Some(HaltKind::Outage),
                        Err(e) => error!("Failed to send inputs-dark halt webhook: {}", e),
                    }
                }
            }
        }
    }
    OPERATOR_RECONCILIATION_INPUT_DARK_TICKS
        .with_label_values(&[ProgramType::Escrow.as_label()])
        .set(*input_dark_ticks as f64);
}

/// One pass per mint over finalized reads: supply against custody plus envelope, and ledger
/// liabilities at the custody slot against custody. A failed read holds the breach counters,
/// so a transient glitch keeps the evidence, and is reported as missing.
#[allow(clippy::too_many_arguments)]
async fn check_invariants(
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
    liability_dark_ticks: &mut u32,
    cancellation_token: &CancellationToken,
) -> Result<TickInputs, OperatorError> {
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
        // A flag cleared after it landed ends the incident, so a re-trip pages again.
        if *halted && flag.is_none() {
            breach_counters.announced = None;
        }
        breach_counters.insolvency_halted = flag.as_ref().is_some_and(|flag| flag.insolvency);
        *halted = flag.is_some();
    }

    // Custody is read at each known mint's ATA, so the mint set comes first.
    let db_mints = fetch_db_mint_set(storage).await?;
    let custody = fetch_on_chain_balances(rpc_client, escrow_instance_id, &db_mints).await?;

    // Envelope (DB) and channel supply (PrivateChannel RPC) are read here, next to
    // custody, because the supply invariant compares all three as one instant. The ledger
    // wait below costs seconds and must never sit between two readings being compared.
    let mut mints: HashSet<Pubkey> = db_mints.iter().map(|(mint, _)| *mint).collect();
    // A failure of either input holds the breach counters and skips the tick, so a
    // transient glitch cannot reset a building breach.
    let (supply, envelope, supply_missing) =
        match load_halt_inputs(storage, channel_rpc, &mints).await {
            Ok(inputs) => inputs,
            Err(e) => {
                warn!("Skipping halt evaluation this tick (counters held): {}", e);
                return Ok(TickInputs::Missing(format!("halt inputs unavailable: {e}")));
            }
        };

    let mut ledger_missing = None;
    let covered = match wait_for_ledger(storage, custody.slot, cancellation_token).await {
        LedgerWait::Covered => {
            *liability_dark_ticks = 0;
            true
        }
        LedgerWait::Unknown => {
            *liability_dark_ticks += 1;
            false
        }
        // Lag stays alert-only, but a checkpoint that cannot be read at all is a missing input.
        LedgerWait::Unreadable => {
            *liability_dark_ticks += 1;
            ledger_missing = Some("escrow checkpoint unreadable".to_string());
            false
        }
        // Never counted: the caller skips input accounting once cancelled.
        LedgerWait::Cancelled => return Ok(TickInputs::Complete),
    };
    report_liability_arm_state(*liability_dark_ticks, config, webhook_client).await;

    // Rows are read whatever the wait outcome. A mint that appeared since the enumeration
    // has no supply reading, so the supply arm holds it as it holds any unread mint, while
    // the liability arm still sees it.
    let (ledger_mints, liabilities) =
        fetch_ledger_at(storage, &custody.slots, custody.slot).await?;
    mints.extend(ledger_mints);

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

    Ok(match supply_missing.or(ledger_missing) {
        Some(reason) => TickInputs::Missing(reason),
        None => TickInputs::Complete,
    })
}

/// Every mint the DB knows with its token program, parsed. Read before the ledger wait so
/// enumeration never depends on whether the ledger can be pinned this tick.
async fn fetch_db_mint_set(storage: &Arc<Storage>) -> Result<Vec<(Pubkey, Pubkey)>, OperatorError> {
    storage
        .get_mint_addresses()
        .await
        .map_err(OperatorError::Storage)?
        .iter()
        .map(|(address, token_program)| Ok((parse_mint(address)?, parse_mint(token_program)?)))
        .collect()
}

/// Whether the ledger can be compared at the custody slot this tick.
#[derive(Debug, PartialEq, Eq)]
enum LedgerWait {
    Covered,
    Unknown,
    /// Every checkpoint read failed, so the ledger is unknown and the input is missing.
    Unreadable,
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
    let mut read_once = false;
    loop {
        match storage.get_committed_checkpoint(&key).await {
            Ok(Some(committed)) if committed >= slot => return LedgerWait::Covered,
            Ok(Some(committed)) => {
                read_once = true;
                last = Some(committed);
            }
            // No escrow indexer has ever committed, so waiting cannot help.
            Ok(None) => {
                warn!(
                    slot,
                    "No escrow indexer checkpoint; ledger liabilities unknown this tick"
                );
                count_unknown_ledger("no_checkpoint");
                return LedgerWait::Unknown;
            }
            Err(e) => warn!("Checkpoint read failed while waiting for the ledger: {}", e),
        }
        if started.elapsed() >= LEDGER_CATCHUP_TIMEOUT {
            if !read_once {
                warn!(
                    slot,
                    "Checkpoint never read within the wait; ledger input unreadable this tick"
                );
                count_unknown_ledger("checkpoint_unreadable");
                return LedgerWait::Unreadable;
            }
            warn!(
                checkpoint = ?last,
                slot,
                "Escrow indexer checkpoint did not reach the custody slot in time; ledger liabilities unknown this tick"
            );
            count_unknown_ledger("catchup_timeout");
            return LedgerWait::Unknown;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(LEDGER_CATCHUP_POLL_MS)) => {}
            _ = cancellation_token.cancelled() => return LedgerWait::Cancelled,
        }
    }
}

fn count_unknown_ledger(reason: &str) {
    OPERATOR_RECONCILIATION_LIABILITY_UNKNOWN
        .with_label_values(&[ProgramType::Escrow.as_label(), reason])
        .inc();
}

/// Export how long the liability arm has been unable to pin the ledger, and alert once it
/// has been dark for `LIABILITY_DARK_ALERT_TICKS` and on every multiple after that.
async fn report_liability_arm_state(
    dark_ticks: u32,
    config: &OperatorConfig,
    webhook_client: &WebhookClient,
) {
    OPERATOR_RECONCILIATION_LIABILITY_DARK_TICKS
        .with_label_values(&[ProgramType::Escrow.as_label()])
        .set(dark_ticks as f64);

    if dark_ticks == 0 || !dark_ticks.is_multiple_of(LIABILITY_DARK_ALERT_TICKS) {
        return;
    }
    error!(
        reconciliation_alert = true,
        dark_ticks,
        "RECONCILIATION ALERT: the liability invariant has been unchecked for {} consecutive ticks; \
         the escrow indexer's checkpoint is not reaching the custody slot",
        dark_ticks
    );
    if let Err(e) = send_liability_dark_alert(
        &config.reconciliation_webhook_url,
        dark_ticks,
        webhook_client,
    )
    .await
    {
        error!("Failed to send dark liability arm webhook: {}", e);
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

/// The ledger for each mint at the slot its custody was read at; a mint with no custody
/// slot uses `highest`.
async fn fetch_ledger_at(
    storage: &Arc<Storage>,
    slots: &HashMap<Pubkey, u64>,
    highest: u64,
) -> Result<(HashSet<Pubkey>, HashMap<Pubkey, u64>), OperatorError> {
    let mut by_slot = HashMap::new();
    for slot in slots.values().copied().chain([highest]) {
        if let std::collections::hash_map::Entry::Vacant(entry) = by_slot.entry(slot) {
            entry.insert(fetch_ledger(storage, slot).await?);
        }
    }
    let (mints, at_highest) = &by_slot[&highest];
    let mut all_mints = mints.clone();
    let mut liabilities = at_highest.clone();
    for (mint, slot) in slots {
        let (slot_mints, at_slot) = &by_slot[slot];
        all_mints.extend(slot_mints);
        if let Some(&owed) = at_slot.get(mint) {
            liabilities.insert(*mint, owed);
        }
    }
    Ok((all_mints, liabilities))
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

/// Per-mint channel supply, the in-flight envelope, and why any supply is missing.
type HaltInputs = (HashMap<Pubkey, u64>, HashMap<Pubkey, u64>, Option<String>);

/// Load the halt inputs: the in-flight envelope (DB) and per-mint channel supply
/// (PrivateChannel RPC) for the given mint set. An envelope-query failure skips
/// the whole tick (one DB read), but a single mint's supply read failing must not
/// blind the others: that mint is omitted from the returned map and held in
/// `evaluate_and_maybe_halt`, so a flaky read on one mint cannot suppress
/// detection on a genuinely over-issued one. The tick is still reported missing, and a
/// supply read older than the channel's newest recent block counts as failed.
async fn load_halt_inputs(
    storage: &Arc<Storage>,
    channel_rpc: &Arc<RpcClientWithRetry>,
    mints: &HashSet<Pubkey>,
) -> Result<HaltInputs, OperatorError> {
    let envelope = fetch_in_flight_envelope(storage).await?;
    // No mints means no supply to read, so the channel's freshness does not matter.
    if mints.is_empty() {
        return Ok((HashMap::new(), envelope, None));
    }
    let anchor = match channel_anchor(channel_rpc).await {
        Ok(anchor) => anchor,
        Err(e) => {
            warn!(
                "Channel freshness unknown; holding every supply counter this tick: {}",
                e.reason
            );
            return Ok((HashMap::new(), envelope, Some(e.reason)));
        }
    };
    let mut supply = HashMap::new();
    let mut missing = None;
    for mint in mints {
        let reason = match fetch_channel_supply_at(channel_rpc, mint).await {
            Ok((s, slot)) if slot >= anchor => {
                supply.insert(*mint, s);
                continue;
            }
            Ok((_, slot)) => {
                format!("answered at slot {slot}, behind the channel's block {anchor}")
            }
            Err(e) => e.reason,
        };
        warn!(
            mint = %mint,
            "Channel supply read unusable; skipping this mint this tick: {}", reason
        );
        missing = Some(format!("channel supply for mint {mint}: {reason}"));
    }
    Ok((supply, envelope, missing))
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
    let mut next_counters = BreachCounters {
        announced: breach_counters.announced,
        insolvency_halted: breach_counters.insolvency_halted,
        ..Default::default()
    };
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

        // An outage halt does not suppress this: a proven breach must still quarantine and page.
        if count < HALT_CONFIRM_TICKS || next_counters.insolvency_halted {
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

        // Confirmed insolvency: trip the halt once the flag lands; a failed write retries next tick.
        let reason = format!(
            "reconciliation halt: mint {} custody {} short of supply by {}, \
             envelope {} tolerance {} over {} consecutive finalized ticks",
            mint, c, breach.supply_gap, breach.envelope, breach.tolerance, count
        );
        error!(reason = %reason, "RECONCILIATION HALT tripped; freezing both pipelines");
        if trip_halt(
            storage,
            health,
            webhook_client,
            config,
            &mint,
            c,
            breach.supply_gap,
            c.saturating_add(breach.supply_gap),
            &reason,
            &mut next_counters.announced,
        )
        .await
        {
            *halted = true;
            next_counters.insolvency_halted = true;
        }
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
        OPERATOR_RECONCILIATION_LIABILITY_SHORTFALL
            .with_label_values(&[&mint.to_string()])
            .set(owed.saturating_sub(c) as f64);

        let Some(breach) = evaluate_liability_shortfall(c, owed, tolerance) else {
            // A shortfall the tolerance absorbs never halts, yet it is exactly what refuses
            // the next boot. Report it now rather than leaving it to surface at a restart.
            if owed > c {
                warn!(
                    mint = %mint,
                    shortfall = owed - c,
                    tolerance,
                    "Custody short of ledger liabilities but within tolerance"
                );
            }
            continue;
        };

        let count = breach_counters.liability.get(&mint).copied().unwrap_or(0) + 1;
        next_counters.liability.insert(mint, count);

        // An outage halt does not suppress this: a proven breach must still quarantine and page.
        if count < HALT_CONFIRM_TICKS || next_counters.insolvency_halted {
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

        let reason = format!(
            "reconciliation halt: mint {} custody {} short of ledger liabilities {} by {}, \
             tolerance {} at slot {} over {} consecutive finalized ticks",
            mint, c, breach.liabilities, breach.gap, breach.tolerance, slot, count
        );
        error!(reason = %reason, "RECONCILIATION HALT tripped; freezing both pipelines");
        if trip_halt(
            storage,
            health,
            webhook_client,
            config,
            &mint,
            c,
            breach.gap,
            breach.liabilities,
            &reason,
            &mut next_counters.announced,
        )
        .await
        {
            *halted = true;
            next_counters.insolvency_halted = true;
        }
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
    announced: &mut Option<HaltKind>,
) -> bool {
    let persisted = freeze_pipelines(storage, health, reason, HaltKind::Insolvency)
        .await
        .is_some();
    // An outage page does not cover this: an upgrade to insolvency pages again.
    if *announced == Some(HaltKind::Insolvency) {
        return persisted;
    }
    // Payload carries real custody and the amount the escrow should hold (supply it
    // could not honor, or ledger liabilities); delta_bps is u64::MAX when custody is 0.
    let alert = BalanceMismatch {
        mint: *mint,
        on_chain_balance: custody,
        db_balance,
        delta_bps: insolvency_delta_bps(custody, gap),
    };
    match send_webhook_alert(&config.reconciliation_webhook_url, &[alert], webhook_client).await {
        Ok(()) => *announced = Some(HaltKind::Insolvency),
        Err(e) => error!("Failed to send reconciliation halt webhook: {}", e),
    }
    persisted
}

/// The levers every halt pulls: the durable flag, quarantine for an insolvency, forced-unhealthy.
/// Returns the kind of halt now in force, or `None` when the flag never landed. An outage skips
/// quarantine because nothing is proven wrong and the flag already blocks every send.
async fn freeze_pipelines(
    storage: &Arc<Storage>,
    health: &Option<Arc<HealthState>>,
    reason: &str,
    kind: HaltKind,
) -> Option<HaltKind> {
    let mut in_force = None;
    for attempt in 1..=HALT_WRITE_ATTEMPTS {
        let written = match kind {
            HaltKind::Insolvency => storage
                .set_reconciliation_halt(reason)
                .await
                .map(|()| HaltKind::Insolvency),
            // The outage write leaves an insolvency halt in place and says so.
            HaltKind::Outage => storage.set_outage_halt(reason).await.map(|written| {
                if written {
                    HaltKind::Outage
                } else {
                    HaltKind::Insolvency
                }
            }),
        };
        match written {
            Ok(held) => {
                in_force = Some(held);
                break;
            }
            Err(e) => error!(
                attempt,
                "Failed to set durable reconciliation halt flag: {}", e
            ),
        }
    }
    if kind == HaltKind::Insolvency {
        // Unbounded on purpose: an insolvency halt is not nonce-scoped.
        match storage.quarantine_active_withdrawals(None, None).await {
            Ok(n) => info!(rows = n, "Quarantined active withdrawals on halt"),
            Err(e) => error!("Failed to quarantine active withdrawals on halt: {}", e),
        }
    }
    // The latch never clears, so it waits for a flag that is really set.
    if let (Some(h), Some(_)) = (health, in_force) {
        h.force_unhealthy(reason.to_string());
    }
    in_force
}

/// Posts the inputs-dark halt as `{ halt_reason, dark_ticks, timestamp }`, retrying
/// transient HTTP errors. With no webhook URL configured it only logs.
pub async fn send_inputs_dark_halt_alert(
    webhook_url: &Option<String>,
    dark_ticks: u32,
    reason: &str,
    webhook_client: &WebhookClient,
) -> Result<(), OperatorError> {
    let Some(url) = webhook_url else {
        warn!(dark_ticks, "Inputs-dark halt but no webhook URL configured");
        return Ok(());
    };
    let payload = serde_json::json!({
        "halt_reason": reason,
        "dark_ticks": dark_ticks,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    webhook_client
        .post_json(
            url,
            &payload,
            &format!("inputs dark for {dark_ticks} tick(s)"),
        )
        .await
        .map_err(|error| {
            OperatorError::WebhookError(format!(
                "Failed to send inputs-dark halt webhook after {} attempts: {}",
                error.attempts(),
                error.message()
            ))
        })?;
    Ok(())
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

/// Escrow custody for each known `(mint, token_program)`, read from the instance ATAs.
async fn fetch_on_chain_balances(
    rpc_client: &Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
    mints: &[(Pubkey, Pubkey)],
) -> Result<EscrowCustody, OperatorError> {
    fetch_escrow_custody(rpc_client, escrow_instance_id, mints)
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

/// Posts `{ dark_ticks, timestamp }` when the liability invariant has gone unchecked for
/// `dark_ticks` consecutive ticks. `None` URL logs a `warn!` and returns `Ok`.
pub async fn send_liability_dark_alert(
    webhook_url: &Option<String>,
    dark_ticks: u32,
    webhook_client: &WebhookClient,
) -> Result<(), OperatorError> {
    let url = match webhook_url {
        Some(url) => url,
        None => {
            warn!(
                dark_ticks,
                "Liability invariant unchecked but no webhook URL configured"
            );
            return Ok(());
        }
    };

    let payload = serde_json::json!({
        "dark_ticks": dark_ticks,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    let context = format!("liability arm dark for {dark_ticks} tick(s)");

    webhook_client
        .post_json(url, &payload, &context)
        .await
        .map_err(|error| {
            OperatorError::WebhookError(format!(
                "Failed to send dark liability arm webhook alert after {} attempts: {}",
                error.attempts(),
                error.message()
            ))
        })?;

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
        use solana_commitment_config::CommitmentLevel;
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
        use solana_commitment_config::CommitmentConfig;
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
        // The insolvency halt is in force, as the tick's resync would have found it.
        let mut counters = BreachCounters {
            insolvency_halted: true,
            ..Default::default()
        };
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

    /// Escrow instance every tick in these tests reads custody for.
    fn test_instance() -> Pubkey {
        Pubkey::new_from_array([7; 32])
    }

    /// Mock escrow custody: `getMultipleAccounts` answers the test instance's SPL ATA for
    /// `mint` with `amount`, and any other requested key as absent.
    async fn mock_custody_sweep(server: &mut mockito::Server, mint: Pubkey, amount: u64) {
        use base64::Engine as _;
        use spl_token::solana_program::program_option::COption;
        use spl_token::solana_program::program_pack::Pack;
        let account = spl_token::state::Account {
            mint,
            owner: test_instance(),
            amount,
            delegate: COption::None,
            state: spl_token::state::AccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        };
        let mut buf = vec![0u8; spl_token::state::Account::LEN];
        account.pack_into_slice(&mut buf);
        let held = serde_json::json!({
            "lamports": 2_039_280u64,
            "owner": spl_token::id().to_string(),
            "executable": false,
            "rentEpoch": 0,
            "space": buf.len(),
            "data": [base64::engine::general_purpose::STANDARD.encode(&buf), "base64"],
        });
        let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &test_instance(),
            &mint,
            &spl_token::id(),
        )
        .to_string();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getMultipleAccounts"}),
            ))
            .with_status(200)
            .with_body_from_request(move |req| {
                let body: serde_json::Value = serde_json::from_slice(req.body().unwrap()).unwrap();
                let value: Vec<serde_json::Value> = body["params"][0]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|k| {
                        if k.as_str() == Some(ata.as_str()) {
                            held.clone()
                        } else {
                            serde_json::Value::Null
                        }
                    })
                    .collect();
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"context": {"slot": 1}, "value": value}
                })
                .to_string()
                .into_bytes()
            })
            .create_async()
            .await;
    }

    /// Register mints in the `mints` table, the universe every ledger read groups over.
    async fn seed_mints(mock: &MockStorage, mints: &[Pubkey]) {
        let rows: Vec<_> = mints
            .iter()
            .map(|m| {
                crate::storage::common::storage::DbMint::new(
                    m.to_string(),
                    6,
                    spl_token::id().to_string(),
                )
            })
            .collect();
        mock.upsert_mints_batch(&rows).await.unwrap();
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

    /// Channel slot the test clock's newest block sits at, and supply reads answer at.
    const CHANNEL_TIP: u64 = 100;

    /// Answer every channel `getAccountInfo` with an SPL mint at `supply`, at `CHANNEL_TIP`.
    async fn mock_channel_supply(server: &mut mockito::Server, supply: u64) {
        mock_channel_supply_at(server, supply, CHANNEL_TIP).await;
    }

    /// Answer every channel `getAccountInfo` with an SPL mint at `supply`, read at `slot`.
    async fn mock_channel_supply_at(server: &mut mockito::Server, supply: u64, slot: u64) {
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
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getAccountInfo"}),
            ))
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"context":{{"slot":{slot}}},"value":{{"owner":"{prog}","lamports":1000000,"data":["{b64}","base64"],"executable":false,"rentEpoch":0}}}}}}"#,
                prog = spl_token::id(),
            ))
            .create_async()
            .await;
    }

    /// A live channel clock: its newest block is `CHANNEL_TIP`, produced a second ago.
    async fn mock_fresh_channel_clock(server: &mut mockito::Server) {
        crate::operator::escrow_sweep::tests::mock_channel_clock(
            server,
            CHANNEL_TIP,
            vec![CHANNEL_TIP],
            Some(1),
        )
        .await;
    }

    /// One-attempt RPC client so a failing read ends the tick quickly.
    fn fast_rpc(url: String) -> Arc<RpcClientWithRetry> {
        use crate::operator::utils::rpc_util::RetryConfig;
        use solana_commitment_config::CommitmentConfig;
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
        mock_fresh_channel_clock(&mut channel).await;
        let mock = MockStorage::new();
        seed_mints(&mock, &[mint]).await;
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
        run_tick_tracking_dark(env, config, counters, halted, token, &mut 0).await
    }

    async fn run_tick_tracking_dark(
        env: &TickEnv,
        config: &OperatorConfig,
        counters: &mut BreachCounters,
        halted: &mut bool,
        token: &CancellationToken,
        dark_ticks: &mut u32,
    ) -> Result<(), OperatorError> {
        run_tick_full(env, config, counters, halted, token, dark_ticks, &mut 0).await
    }

    /// One tick with both the liability-dark and the input-dark counters supplied.
    async fn run_tick_full(
        env: &TickEnv,
        config: &OperatorConfig,
        counters: &mut BreachCounters,
        halted: &mut bool,
        token: &CancellationToken,
        dark_ticks: &mut u32,
        input_dark_ticks: &mut u32,
    ) -> Result<(), OperatorError> {
        perform_reconciliation_check(
            &env.storage,
            config,
            &fast_rpc(env.custody.url()),
            &fast_rpc(env.channel.url()),
            test_instance(),
            &test_webhook_client(),
            &None,
            &mut None,
            counters,
            halted,
            dark_ticks,
            input_dark_ticks,
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

    /// Answer channel `getAccountInfo` with a mint at `supply`, recording how many
    /// checkpoint reads the storage had served by the time the request arrived.
    async fn mock_channel_supply_recording(
        server: &mut mockito::Server,
        supply: u64,
        mock: MockStorage,
        seen: Arc<std::sync::Mutex<Vec<usize>>>,
    ) {
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
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getAccountInfo"}),
            ))
            .with_status(200)
            .with_body_from_request(move |_| {
                seen.lock()
                    .unwrap()
                    .push(mock.calls("get_committed_checkpoint"));
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": CHANNEL_TIP},
                        "value": {
                            "owner": spl_token::id().to_string(),
                            "lamports": 1_000_000u64,
                            "data": [b64, "base64"],
                            "executable": false,
                            "rentEpoch": 0,
                        }
                    }
                })
                .to_string()
                .into_bytes()
            })
            .create_async()
            .await;
    }

    #[tokio::test]
    async fn a_sub_tolerance_shortfall_is_reported_without_halting() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 1000, 0).await;
        seed_checkpoint(&env.mock, 1);
        // Liabilities one unit over custody, against a tolerance of 10 bps of 1000.
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 1001, 0)]);
        let mut counters = BreachCounters::default();
        let mut halted = false;

        run_tick(
            &env,
            &make_operator_config(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(!halted);
        assert!(counters.liability.is_empty(), "within tolerance, no breach");
        assert_eq!(
            OPERATOR_RECONCILIATION_LIABILITY_SHORTFALL
                .with_label_values(&[&mint.to_string()])
                .get(),
            1.0,
            "a shortfall the tolerance absorbs still has to be visible"
        );
    }

    #[tokio::test]
    async fn dark_ticks_advance_on_unknown_and_reset_on_covered() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_mints(&env.mock, &[mint]).await;
        let config = recon_config_zero_tolerance();
        let mut dark = 0;

        // No checkpoint at all: the arm cannot be pinned, twice.
        for _ in 0..2 {
            run_tick_tracking_dark(
                &env,
                &config,
                &mut BreachCounters::default(),
                &mut false,
                &CancellationToken::new(),
                &mut dark,
            )
            .await
            .unwrap();
        }
        assert_eq!(dark, 2, "each unpinnable tick is one dark tick");

        seed_checkpoint(&env.mock, 1);
        run_tick_tracking_dark(
            &env,
            &config,
            &mut BreachCounters::default(),
            &mut false,
            &CancellationToken::new(),
            &mut dark,
        )
        .await
        .unwrap();
        assert_eq!(dark, 0, "a covered tick rearms the arm");
    }

    #[tokio::test]
    async fn a_persistently_dark_arm_alerts_on_every_third_tick() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_mints(&env.mock, &[mint]).await;
        let mut webhook = mockito::Server::new_async().await;
        let posted = webhook
            .mock("POST", "/")
            .with_status(200)
            .expect(2)
            .create_async()
            .await;
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(webhook.url()),
            ..recon_config_zero_tolerance()
        };
        let mut dark = 0;

        // Six dark ticks: one alert at the third, one at the sixth, silence between.
        for _ in 0..6 {
            run_tick_tracking_dark(
                &env,
                &config,
                &mut BreachCounters::default(),
                &mut false,
                &CancellationToken::new(),
                &mut dark,
            )
            .await
            .unwrap();
        }

        assert_eq!(dark, 6);
        posted.assert_async().await;
    }

    #[tokio::test]
    async fn supply_is_read_before_the_ledger_wait() {
        let mint = Pubkey::new_unique();
        let mut custody_server = mockito::Server::new_async().await;
        mock_custody_sweep(&mut custody_server, mint, 100).await;
        let mock = MockStorage::new();
        seed_mints(&mock, &[mint]).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut channel = mockito::Server::new_async().await;
        mock_channel_supply_recording(&mut channel, 100, mock.clone(), seen.clone()).await;
        mock_fresh_channel_clock(&mut channel).await;
        let storage = Arc::new(Storage::Mock(mock.clone()));
        // A checkpoint stuck below the custody slot makes the wait spend its whole budget.
        seed_checkpoint(&mock, 0);
        mock.set_mint_balances(vec![ledger_row(&mint.to_string(), 100, 0)]);
        let env = TickEnv {
            custody: custody_server,
            channel,
            mock,
            storage,
        };

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut BreachCounters::default(),
            &mut false,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        let seen = seen.lock().unwrap().clone();
        assert!(!seen.is_empty(), "the supply read must have happened");
        assert!(
            seen.iter().all(|&reads| reads == 0),
            "custody and supply must be read with no ledger wait between them, saw {seen:?}"
        );
        assert!(
            env.mock.calls("get_committed_checkpoint") > 1,
            "the wait must still have run, after the supply read"
        );
    }

    #[tokio::test]
    async fn mint_enumeration_survives_an_unknown_ledger() {
        let custody_mint = Pubkey::new_unique();
        let listed_only = Pubkey::new_unique();
        let env = tick_env(custody_mint, 100, 1000).await;
        // Known to the mints table only: no custody, no ledger row, no checkpoint.
        seed_mints(&env.mock, &[listed_only]).await;
        let mut counters = BreachCounters::default();

        run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut false,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(
            counters.supply.get(&listed_only).copied(),
            Some(1),
            "a mint the ledger read cannot enumerate must still be checked"
        );
    }

    #[tokio::test]
    async fn enumeration_read_failure_holds_counters_and_halt_flag() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 1000).await;
        seed_checkpoint(&env.mock, 1);
        env.mock.set_should_fail("get_mint_addresses", true);
        let mut counters = BreachCounters {
            supply: HashMap::from([(mint, 2)]),
            ..Default::default()
        };
        let mut halted = false;

        let result = run_tick(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
        )
        .await;

        assert!(result.is_err(), "the tick must surface the read failure");
        assert_eq!(counters.supply.get(&mint).copied(), Some(2), "held");
        assert!(!halted);
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
        let mut input_dark = 0;

        let res = run_tick_full(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
            &mut 0,
            &mut input_dark,
        )
        .await;

        assert!(res.is_ok(), "{res:?}");
        assert_eq!(counters.liability.get(&mint).copied(), Some(2), "held");
        assert_eq!(counters.supply.get(&mint).copied(), Some(1));
        assert!(!halted);
        assert_eq!(input_dark, 1, "an unreadable checkpoint is a missing input");
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
        let mut input_dark = 0;

        run_tick_full(
            &env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
            &mut 0,
            &mut input_dark,
        )
        .await
        .unwrap();

        assert_eq!(counters.liability.get(&mint).copied(), Some(1));
        assert_eq!(
            input_dark, 0,
            "a read that recovers within the wait is not missing"
        );
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
        // The aggregate's universe is the `mints` table, so a row there implies a row here.
        seed_mints(&env.mock, &[a, b]).await;
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
        use solana_commitment_config::CommitmentConfig;

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
        seed_mints(&mock, &[mint]).await;
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
            test_instance(),
            &webhook_client,
            &None,
            &mut orphans,
            &mut counters,
            &mut halted,
            &mut 0,
            &mut 0,
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
        use solana_commitment_config::CommitmentConfig;

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
            test_instance(),
            &webhook,
            &None,
            &mut orphans,
            &mut counters,
            &mut halted,
            &mut 0,
            &mut 0,
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
            test_instance(),
            &webhook,
            &None,
            &mut orphans,
            &mut counters,
            &mut halted,
            &mut 0,
            &mut 0,
            &CancellationToken::new(),
        )
        .await;
        assert!(
            !halted,
            "a cleared durable flag must drop the in-memory guard"
        );
    }

    // ── channel supply freshness ──────────────────────────────────────

    /// Point the env's channel at a clock whose newest block is `block`, `age_secs` old,
    /// answering supply reads with `supply` at `read_slot`.
    async fn set_channel(
        env: &mut TickEnv,
        supply: u64,
        read_slot: u64,
        block: u64,
        age_secs: i64,
    ) {
        env.channel.reset();
        mock_channel_supply_at(&mut env.channel, supply, read_slot).await;
        crate::operator::escrow_sweep::tests::mock_channel_clock(
            &mut env.channel,
            block,
            vec![block],
            Some(age_secs),
        )
        .await;
    }

    /// Runs one tick with a supply counter already at 2 and returns (counter, input-dark).
    async fn supply_tick(env: &TickEnv, mint: Pubkey) -> (Option<u32>, u32) {
        let mut counters = BreachCounters {
            supply: HashMap::from([(mint, 2)]),
            ..Default::default()
        };
        let mut input_dark = 0;
        run_tick_full(
            env,
            &recon_config_zero_tolerance(),
            &mut counters,
            &mut false,
            &CancellationToken::new(),
            &mut 0,
            &mut input_dark,
        )
        .await
        .unwrap();
        (counters.supply.get(&mint).copied(), input_dark)
    }

    /// A supply read answered below the channel's newest block is stale: it may not clear
    /// a building breach, and the tick counts as missing an input.
    #[tokio::test]
    async fn a_supply_read_behind_the_anchor_is_held() {
        let mint = Pubkey::new_unique();
        let mut env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        set_channel(&mut env, 100, 5, 10, 1).await;

        assert_eq!(supply_tick(&env, mint).await, (Some(2), 1));
        assert!(env
            .storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .is_none());
    }

    /// A read at or past the anchor is fresh and counts as a clean reading.
    #[tokio::test]
    async fn a_supply_read_at_the_anchor_is_accepted() {
        let mint = Pubkey::new_unique();
        let mut env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        set_channel(&mut env, 100, 10, 10, 1).await;

        assert_eq!(
            supply_tick(&env, mint).await,
            (None, 0),
            "a clean fresh read resets"
        );
    }

    /// A channel whose newest block is old cannot build or clear evidence either way.
    #[tokio::test]
    async fn a_stale_channel_holds_every_supply_counter() {
        let mint = Pubkey::new_unique();
        let mut env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        // Breaching supply, but the node's newest block is ten minutes old.
        set_channel(&mut env, 5_000, 10, 10, 600).await;

        assert_eq!(supply_tick(&env, mint).await, (Some(2), 1));
    }

    // ── missing inputs (input-dark ticks) ─────────────────────────────

    /// A required reconciliation input that a tick can fail to read.
    #[derive(Clone, Copy, Debug)]
    enum Input {
        MintSet,
        Custody,
        Envelope,
        ChannelSupply,
        Ledger,
    }

    const ALL_INPUTS: [Input; 5] = [
        Input::MintSet,
        Input::Custody,
        Input::Envelope,
        Input::ChannelSupply,
        Input::Ledger,
    ];

    /// Make `input` unreadable (or readable again) for the ticks that follow.
    async fn set_input_failing(env: &mut TickEnv, input: Input, failing: bool) {
        match input {
            Input::MintSet => env.mock.set_should_fail("get_mint_addresses", failing),
            Input::Envelope => env
                .mock
                .set_should_fail("get_in_flight_amounts_by_mint", failing),
            Input::Ledger => env
                .mock
                .set_should_fail("get_mint_balances_for_reconciliation", failing),
            Input::Custody | Input::ChannelSupply => {
                assert!(failing, "RPC inputs are only ever broken in these tests");
                let server = match input {
                    Input::Custody => &mut env.custody,
                    _ => &mut env.channel,
                };
                server.reset();
                server
                    .mock("POST", "/")
                    .with_status(503)
                    .create_async()
                    .await;
            }
        }
    }

    /// A tick env whose one mint also has an active withdrawal, so a quarantine would show.
    async fn dark_env() -> (TickEnv, Pubkey) {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 1);
        seed_pending_withdrawal(&env.mock, 1, 1);
        env.mock.pending_transactions.lock().unwrap()[0].mint = mint.to_string();
        (env, mint)
    }

    fn withdrawal_status(env: &TickEnv) -> crate::storage::common::models::TransactionStatus {
        env.mock.pending_transactions.lock().unwrap()[0].status
    }

    /// Every required input counts: three unreadable ticks in a row set the durable halt,
    /// page with the inputs-dark webhook, and leave active withdrawals untouched.
    #[tokio::test]
    async fn each_missing_input_halts_after_three_ticks_without_quarantine() {
        use crate::storage::common::models::TransactionStatus;
        for input in ALL_INPUTS {
            let (mut env, _) = dark_env().await;
            set_input_failing(&mut env, input, true).await;
            let mut hook = mockito::Server::new_async().await;
            let alert = hook
                .mock("POST", "/")
                .match_body(mockito::Matcher::PartialJson(
                    serde_json::json!({"dark_ticks": INPUT_DARK_HALT_TICKS}),
                ))
                .with_status(200)
                .expect(1)
                .create_async()
                .await;
            let config = OperatorConfig {
                reconciliation_webhook_url: Some(hook.url()),
                ..recon_config_zero_tolerance()
            };
            let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);

            for tick in 1..=INPUT_DARK_HALT_TICKS {
                let _ = run_tick_full(
                    &env,
                    &config,
                    &mut counters,
                    &mut halted,
                    &CancellationToken::new(),
                    &mut 0,
                    &mut input_dark,
                )
                .await;
                assert_eq!(
                    input_dark, tick,
                    "{input:?}: one dark tick per unreadable tick"
                );
                assert_eq!(
                    halted,
                    tick == INPUT_DARK_HALT_TICKS,
                    "{input:?} tick {tick}"
                );
            }

            let reason = env
                .storage
                .is_reconciliation_halted()
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{input:?}: halt must be durable"))
                .reason;
            assert!(
                reason.contains("required inputs unavailable"),
                "{input:?}: {reason}"
            );
            assert_eq!(
                withdrawal_status(&env),
                TransactionStatus::Pending,
                "{input:?}: an outage halt must not quarantine"
            );
            alert.assert_async().await;
        }
    }

    /// One tick that reads everything clears the streak, so scattered failures never halt.
    #[tokio::test]
    async fn a_complete_tick_resets_the_input_dark_streak() {
        let (mut env, _) = dark_env().await;
        let config = recon_config_zero_tolerance();
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for failing in [true, true, false, true, true] {
            set_input_failing(&mut env, Input::Envelope, failing).await;
            let _ = run_tick_full(
                &env,
                &config,
                &mut counters,
                &mut halted,
                &CancellationToken::new(),
                &mut 0,
                &mut input_dark,
            )
            .await;
            if !failing {
                assert_eq!(input_dark, 0, "a complete tick resets the streak");
            }
        }
        assert_eq!(input_dark, 2);
        assert!(!halted);
        assert!(env
            .storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .is_none());
    }

    /// Clearing the flag while the inputs are still down re-trips on the next dark tick.
    #[tokio::test]
    async fn clearing_while_still_dark_trips_again() {
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Ledger, true).await;
        let config = recon_config_zero_tolerance();
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for _ in 0..INPUT_DARK_HALT_TICKS {
            let _ = run_tick_full(
                &env,
                &config,
                &mut counters,
                &mut halted,
                &CancellationToken::new(),
                &mut 0,
                &mut input_dark,
            )
            .await;
        }
        assert!(halted);

        env.storage.clear_reconciliation_halt().await.unwrap();
        let _ = run_tick_full(
            &env,
            &config,
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
            &mut 0,
            &mut input_dark,
        )
        .await;

        assert!(halted);
        assert!(
            env.storage
                .is_reconciliation_halted()
                .await
                .unwrap()
                .is_some(),
            "the halt is set again while the inputs stay dark"
        );
    }

    /// A halt whose flag write failed is not treated as set, so a later tick writes it,
    /// even while the flag cannot be read back to resync the guard.
    #[tokio::test]
    async fn a_failed_halt_write_is_retried_on_the_next_tick() {
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Ledger, true).await;
        env.mock.set_should_fail("is_reconciliation_halted", true);
        env.mock.set_should_fail("set_outage_halt", true);
        let config = recon_config_zero_tolerance();
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for _ in 0..INPUT_DARK_HALT_TICKS {
            let _ = run_tick_full(
                &env,
                &config,
                &mut counters,
                &mut halted,
                &CancellationToken::new(),
                &mut 0,
                &mut input_dark,
            )
            .await;
        }
        assert!(!halted, "an unwritten flag must not latch the guard");
        assert!(env.mock.reconciliation_halt.lock().unwrap().is_none());

        env.mock.set_should_fail("set_outage_halt", false);
        let _ = run_tick_full(
            &env,
            &config,
            &mut counters,
            &mut halted,
            &CancellationToken::new(),
            &mut 0,
            &mut input_dark,
        )
        .await;

        assert!(halted);
        assert!(
            env.mock.reconciliation_halt.lock().unwrap().is_some(),
            "the next dark tick writes the flag"
        );
    }

    /// A webhook that counts every halt alert it receives.
    async fn counting_halt_hook() -> (mockito::ServerGuard, Arc<std::sync::atomic::AtomicUsize>) {
        let mut hook = mockito::Server::new_async().await;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = seen.clone();
        hook.mock("POST", "/")
            .with_status(200)
            .with_body_from_request(move |_| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Vec::new()
            })
            .create_async()
            .await;
        (hook, seen)
    }

    /// One tick whose result is ignored, for tests that only watch state across ticks.
    async fn one_tick(
        env: &TickEnv,
        config: &OperatorConfig,
        counters: &mut BreachCounters,
        halted: &mut bool,
        input_dark: &mut u32,
    ) {
        let _ = run_tick_full(
            env,
            config,
            counters,
            halted,
            &CancellationToken::new(),
            &mut 0,
            input_dark,
        )
        .await;
    }

    /// While the flag write keeps failing, every dark tick retries it but the incident pages
    /// once; a manual clear starts a new incident that pages again.
    #[tokio::test]
    async fn a_failing_halt_write_alerts_once_per_incident() {
        use std::sync::atomic::Ordering;
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Ledger, true).await;
        env.mock.set_should_fail("set_outage_halt", true);
        let (hook, alerts) = counting_halt_hook().await;
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(hook.url()),
            ..recon_config_zero_tolerance()
        };
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for _ in 0..INPUT_DARK_HALT_TICKS + 2 {
            one_tick(&env, &config, &mut counters, &mut halted, &mut input_dark).await;
        }
        assert!(!halted, "an unwritten flag must not latch the guard");
        assert_eq!(
            env.mock.calls("set_outage_halt"),
            3 * HALT_WRITE_ATTEMPTS as usize,
            "the write is retried on every dark tick past the limit"
        );
        assert_eq!(alerts.load(Ordering::SeqCst), 1, "one page per incident");

        env.mock.set_should_fail("set_outage_halt", false);
        one_tick(&env, &config, &mut counters, &mut halted, &mut input_dark).await;
        assert!(halted, "the guard latches once the write lands");
        assert_eq!(
            alerts.load(Ordering::SeqCst),
            1,
            "landing the flag does not page again"
        );

        env.storage.clear_reconciliation_halt().await.unwrap();
        one_tick(&env, &config, &mut counters, &mut halted, &mut input_dark).await;
        assert!(halted);
        assert_eq!(
            alerts.load(Ordering::SeqCst),
            2,
            "a cleared halt that re-trips is a new incident"
        );
    }

    /// An incident whose flag never landed ends when its inputs come back, so the next one pages.
    #[tokio::test]
    async fn an_unwritten_halt_rearms_its_alert_once_the_inputs_recover() {
        use std::sync::atomic::Ordering;
        let (mut env, _) = dark_env().await;
        env.mock.set_should_fail("set_outage_halt", true);
        let (hook, alerts) = counting_halt_hook().await;
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(hook.url()),
            ..recon_config_zero_tolerance()
        };
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        let dark = INPUT_DARK_HALT_TICKS as usize;
        let pattern = std::iter::repeat_n(true, dark)
            .chain([false])
            .chain(std::iter::repeat_n(true, dark));
        for failing in pattern {
            set_input_failing(&mut env, Input::Envelope, failing).await;
            let _ = run_tick_full(
                &env,
                &config,
                &mut counters,
                &mut halted,
                &CancellationToken::new(),
                &mut 0,
                &mut input_dark,
            )
            .await;
        }
        assert_eq!(
            alerts.load(Ordering::SeqCst),
            2,
            "two separate incidents page twice"
        );
    }

    /// A confirmed breach whose flag write keeps failing retries the write each tick and pages once.
    #[tokio::test]
    async fn a_breach_with_a_failing_halt_write_alerts_once() {
        use std::sync::atomic::Ordering;
        let (hook, alerts) = counting_halt_hook().await;
        let mock = MockStorage::new();
        mock.set_should_fail("set_reconciliation_halt", true);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(hook.url()),
            ..recon_config_zero_tolerance()
        };
        let (custody, db_mints, supply, envelope, _) = breach_maps();
        let mut counters = BreachCounters::default();
        let mut halted = false;

        for _ in 0..HALT_CONFIRM_TICKS + 2 {
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

        assert!(!halted);
        assert_eq!(
            mock.calls("set_reconciliation_halt"),
            3 * HALT_WRITE_ATTEMPTS as usize,
            "the write is retried on every confirmed tick"
        );
        assert_eq!(alerts.load(Ordering::SeqCst), 1, "one page per incident");
    }

    /// An existing halt is never overwritten or re-announced by a dark streak.
    #[tokio::test]
    async fn a_dark_streak_under_an_existing_halt_trips_nothing() {
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Envelope, true).await;
        env.storage
            .set_reconciliation_halt("prior halt")
            .await
            .unwrap();
        let mut hook = mockito::Server::new_async().await;
        let alert = hook.mock("POST", "/").expect(0).create_async().await;
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(hook.url()),
            ..recon_config_zero_tolerance()
        };
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for _ in 0..INPUT_DARK_HALT_TICKS {
            let _ = run_tick_full(
                &env,
                &config,
                &mut counters,
                &mut halted,
                &CancellationToken::new(),
                &mut 0,
                &mut input_dark,
            )
            .await;
        }

        let reason = env
            .storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .expect("still halted")
            .reason;
        assert_eq!(reason, "prior halt");
        assert_eq!(env.mock.calls("quarantine_active_withdrawals"), 0);
        alert.assert_async().await;
    }

    /// Each mint's liabilities come from the ledger at its own custody slot, one read per slot.
    #[tokio::test]
    async fn ledger_is_read_at_each_mints_custody_slot() {
        let (a, b, late) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let mock = MockStorage::new();
        let rows = |deposits: u64| {
            vec![
                ledger_row(&a.to_string(), deposits, 0),
                ledger_row(&b.to_string(), deposits, 0),
                ledger_row(&late.to_string(), deposits, 0),
            ]
        };
        mock.mint_balances_at
            .lock()
            .unwrap()
            .extend([(7, rows(100)), (9, rows(900))]);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let slots = HashMap::from([(a, 7), (b, 9)]);

        let (mints, liabilities) = fetch_ledger_at(&storage, &slots, 9).await.unwrap();

        assert_eq!(liabilities[&a], 100, "a is compared at its slot 7");
        assert_eq!(liabilities[&b], 900, "b is compared at its slot 9");
        assert_eq!(
            liabilities[&late], 900,
            "a mint with no custody slot uses the highest"
        );
        assert_eq!(mints, HashSet::from([a, b, late]));
        assert_eq!(
            mock.calls("get_mint_balances_for_reconciliation"),
            2,
            "one read per distinct slot"
        );
    }

    /// One tick like `run_tick_full`, but with a health handle so the forced-unhealthy latch shows.
    async fn tick_with_health(
        env: &TickEnv,
        config: &OperatorConfig,
        health: &Arc<HealthState>,
        counters: &mut BreachCounters,
        halted: &mut bool,
        input_dark: &mut u32,
    ) {
        let _ = perform_reconciliation_check(
            &env.storage,
            config,
            &fast_rpc(env.custody.url()),
            &fast_rpc(env.channel.url()),
            test_instance(),
            &test_webhook_client(),
            &Some(health.clone()),
            &mut None,
            counters,
            halted,
            &mut 0,
            input_dark,
            &CancellationToken::new(),
        )
        .await;
    }

    /// A breach confirmed while an outage halt holds still quarantines, pages as an insolvency,
    /// and replaces the stored reason, so clearing the outage cannot release a drained mint.
    #[tokio::test]
    async fn a_breach_during_an_outage_halt_still_trips_as_insolvency() {
        use std::sync::atomic::Ordering;
        let (hook, alerts) = counting_halt_hook().await;
        let mock = MockStorage::new();
        seed_pending_withdrawal(&mock, 1, 1);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        storage
            .set_outage_halt("reconciliation halt: required inputs unavailable")
            .await
            .unwrap();
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(hook.url()),
            ..recon_config_zero_tolerance()
        };
        let (custody, db_mints, supply, envelope, mint) = breach_maps();
        let mut counters = BreachCounters::default();
        // The outage halt is in force, as the tick's resync would have found it.
        let mut halted = true;

        for _ in 0..HALT_CONFIRM_TICKS + 2 {
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

        let halt = storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .expect("halted");
        assert!(halt.insolvency, "the breach upgrades the halt");
        assert!(halt.reason.contains(&mint.to_string()), "{}", halt.reason);
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            crate::storage::common::models::TransactionStatus::ManualReview,
            "an insolvency quarantines active withdrawals"
        );
        assert_eq!(alerts.load(Ordering::SeqCst), 1, "the upgrade pages once");
    }

    /// A dark streak never replaces an insolvency halt, even when the guard could not read the
    /// flag and so believes nothing is halted.
    #[tokio::test]
    async fn a_dark_streak_never_replaces_an_insolvency_halt() {
        use std::sync::atomic::Ordering;
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Envelope, true).await;
        env.storage
            .set_reconciliation_halt("mint X insolvent")
            .await
            .unwrap();
        env.mock.set_should_fail("is_reconciliation_halted", true);
        let (hook, alerts) = counting_halt_hook().await;
        let config = OperatorConfig {
            reconciliation_webhook_url: Some(hook.url()),
            ..recon_config_zero_tolerance()
        };
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for _ in 0..INPUT_DARK_HALT_TICKS + 1 {
            one_tick(&env, &config, &mut counters, &mut halted, &mut input_dark).await;
        }

        let halt = env
            .mock
            .reconciliation_halt
            .lock()
            .unwrap()
            .clone()
            .expect("halted");
        assert_eq!(halt.reason, "mint X insolvent");
        assert!(halt.insolvency);
        assert!(halted, "the guard learns a halt is in force");
        assert_eq!(
            alerts.load(Ordering::SeqCst),
            0,
            "no outage page over an insolvency"
        );
    }

    /// Health is forced unhealthy only once the flag has landed, since that latch never clears.
    #[tokio::test]
    async fn health_is_forced_only_once_the_halt_flag_lands() {
        use private_channel_metrics::{HealthConfig, HealthOutcome};
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Envelope, true).await;
        env.mock.set_should_fail("set_outage_halt", true);
        env.mock.set_should_fail("set_reconciliation_halt", true);
        let health = HealthState::new(HealthConfig::operator());
        let config = recon_config_zero_tolerance();
        let (mut counters, mut halted, mut input_dark) = (BreachCounters::default(), false, 0);
        for _ in 0..INPUT_DARK_HALT_TICKS + 1 {
            tick_with_health(
                &env,
                &config,
                &health,
                &mut counters,
                &mut halted,
                &mut input_dark,
            )
            .await;
        }
        assert!(
            !matches!(health.check(), HealthOutcome::ForcedUnhealthy { .. }),
            "an unwritten halt must not latch health"
        );

        env.mock.set_should_fail("set_outage_halt", false);
        env.mock.set_should_fail("set_reconciliation_halt", false);
        tick_with_health(
            &env,
            &config,
            &health,
            &mut counters,
            &mut halted,
            &mut input_dark,
        )
        .await;
        assert!(matches!(
            health.check(),
            HealthOutcome::ForcedUnhealthy { .. }
        ));
    }

    /// With no mints there is no supply to read, so an unreachable channel is not a dark tick.
    #[tokio::test]
    async fn no_mints_needs_no_channel() {
        let mut custody = mockito::Server::new_async().await;
        custody
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getSlot"}),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":1}"#)
            .create_async()
            .await;
        let mut channel = mockito::Server::new_async().await;
        channel
            .mock("POST", "/")
            .with_status(503)
            .create_async()
            .await;
        let mock = MockStorage::new();
        seed_checkpoint(&mock, 1);
        let env = TickEnv {
            custody,
            channel,
            storage: Arc::new(Storage::Mock(mock.clone())),
            mock,
        };
        let mut input_dark = 0;
        run_tick_full(
            &env,
            &recon_config_zero_tolerance(),
            &mut BreachCounters::default(),
            &mut false,
            &CancellationToken::new(),
            &mut 0,
            &mut input_dark,
        )
        .await
        .unwrap();
        assert_eq!(input_dark, 0);
    }

    /// A tick cut short by shutdown is not evidence of anything.
    #[tokio::test]
    async fn a_cancelled_tick_is_not_counted() {
        let (mut env, _) = dark_env().await;
        set_input_failing(&mut env, Input::Envelope, true).await;
        let token = CancellationToken::new();
        token.cancel();
        let mut input_dark = 0;
        let _ = run_tick_full(
            &env,
            &recon_config_zero_tolerance(),
            &mut BreachCounters::default(),
            &mut false,
            &token,
            &mut 0,
            &mut input_dark,
        )
        .await;
        assert_eq!(input_dark, 0);
    }

    /// A lagging escrow checkpoint is not a failed read: it stays on the liability-dark
    /// alert path and never feeds the input-dark halt.
    #[tokio::test]
    async fn checkpoint_lag_is_not_an_input_failure() {
        let mint = Pubkey::new_unique();
        let env = tick_env(mint, 100, 100).await;
        seed_checkpoint(&env.mock, 0);
        env.mock
            .set_mint_balances(vec![ledger_row(&mint.to_string(), 100, 0)]);
        let (mut liability_dark, mut input_dark) = (0, 0);
        run_tick_full(
            &env,
            &recon_config_zero_tolerance(),
            &mut BreachCounters::default(),
            &mut false,
            &CancellationToken::new(),
            &mut liability_dark,
            &mut input_dark,
        )
        .await
        .unwrap();
        assert_eq!((liability_dark, input_dark), (1, 0));
    }

    /// Both halt kinds pull the same levers, but only a proven insolvency quarantines.
    #[tokio::test]
    async fn freeze_pipelines_quarantines_only_when_asked() {
        use crate::storage::common::models::TransactionStatus;
        use private_channel_metrics::{HealthConfig, HealthOutcome, HealthState};
        for (kind, expected) in [
            (HaltKind::Insolvency, TransactionStatus::ManualReview),
            (HaltKind::Outage, TransactionStatus::Pending),
        ] {
            let mock = MockStorage::new();
            seed_pending_withdrawal(&mock, 1, 1);
            let storage = Arc::new(Storage::Mock(mock.clone()));
            let health = HealthState::new(HealthConfig::operator());

            let in_force =
                freeze_pipelines(&storage, &Some(health.clone()), "test freeze", kind).await;

            assert_eq!(in_force, Some(kind));
            assert!(storage.is_reconciliation_halted().await.unwrap().is_some());
            assert_eq!(
                mock.pending_transactions.lock().unwrap()[0].status,
                expected,
                "{kind:?}"
            );
            assert!(matches!(
                health.check(),
                HealthOutcome::ForcedUnhealthy { .. }
            ));
        }
    }

    /// The flag write is retried within the tick, and a write that never lands is reported.
    #[tokio::test]
    async fn freeze_pipelines_reports_whether_the_flag_landed() {
        for (failures, expected) in [(1, true), (HALT_WRITE_ATTEMPTS as usize, false)] {
            let mock = MockStorage::new();
            mock.set_fail_times("set_reconciliation_halt", failures);
            let storage = Arc::new(Storage::Mock(mock.clone()));

            let in_force =
                freeze_pipelines(&storage, &None, "test freeze", HaltKind::Insolvency).await;

            assert_eq!(in_force.is_some(), expected, "{failures} failed writes");
            assert_eq!(mock.reconciliation_halt.lock().unwrap().is_some(), expected);
        }
    }

    #[tokio::test]
    async fn inputs_dark_alert_without_url_is_ok() {
        let result = send_inputs_dark_halt_alert(&None, 3, "reason", &test_webhook_client()).await;
        assert!(result.is_ok());
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
