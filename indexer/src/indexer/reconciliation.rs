//! Startup reconciliation of DB state against on-chain escrow ATA balances.
//!
//! On startup, before processing any new data, the escrow indexer verifies that its
//! stored deposit/withdrawal totals match the actual token balances held in the escrow
//! instance's Associated Token Accounts (ATAs).
//!
//! The DB-side formula mirrors exactly what is on-chain:
//!   `db_expected = all_indexed_deposits − completed_withdrawals`
//!
//! Deposits increase the ATA balance on-chain the moment they are observed, regardless of
//! the operator's private_channel minting status (`pending`/`processing`/`completed`/`failed`).
//! Only completed withdrawals (`release_funds`) reduce the ATA balance.
//!
//! Flow:
//! 1. Sweep the escrow instance's on-chain token accounts, summed per mint, noting the
//!    slot the reading is valid as of.
//! 2. Query the DB for per-mint aggregate balances (all deposits − completed
//!    withdrawals), bounded by that slot so both sides describe the same instant.
//! 3. Compare the union of both mint sets; a mint on only one side compares against 0.
//! 4. If any mint's channel supply exceeds its custody by more than the threshold, log
//!    error, emit alert, abort startup. Checked first: it reads the chain on both sides,
//!    so it is unaffected by how far behind the DB is and can never be repaired by
//!    indexing more slots. A supply that cannot be read at all aborts too, since an
//!    unreadable channel and a solvent one look the same from here.
//! 5. A mint whose shortfall (db_expected minus on-chain) exceeds the threshold means the escrow may not cover its liabilities: log an error, emit an alert, and abort startup.
//! 6. A surplus (on-chain minus db_expected) is benign and attacker-inducible, so it only logs a warning and never blocks; a shortfall within the threshold also just warns.
//! 7. If all mints balance (or both sides are empty), log info and continue.

use crate::{
    config::{ProgramType, ReconciliationConfig},
    error::{IndexerError, ReconciliationError},
    operator::{
        escrow_sweep::{
            fetch_channel_supply, fetch_escrow_balances_by_mint, CustodySnapshot, SweepFailure,
        },
        rpc_util::RpcClientWithRetry,
        RetryConfig,
    },
    storage::common::amount::{net_to_u64, NetBalance},
    storage::common::models::MintDbBalance,
    storage::Storage,
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::{error, info, warn};

/// Per-mint result produced during reconciliation.
#[derive(Debug, Clone)]
pub struct MintReconciliation {
    pub mint: String,
    /// Expected balance according to DB: all indexed deposits − completed withdrawals.
    /// Unsigned because it mirrors the escrow ATA balance, itself a u64; a negative
    /// net is clamped to 0 at the call site so this value stays lossless across the
    /// full u64 range instead of truncating at i64::MAX.
    pub db_expected: u64,
    /// Actual raw token balance in the escrow ATA on-chain.
    pub on_chain_actual: u64,
}

impl MintReconciliation {
    pub fn new(mint: String, db_expected: u64, on_chain_actual: u64) -> Self {
        Self {
            mint,
            db_expected,
            on_chain_actual,
        }
    }

    /// Custody shortfall below DB-expected liabilities. This is the only
    /// solvency-relevant direction: a shortfall past the threshold means the
    /// escrow may not cover withdrawals, so it blocks startup.
    pub fn shortfall(&self) -> u64 {
        self.db_expected.saturating_sub(self.on_chain_actual)
    }

    /// Custody surplus above DB-expected liabilities. Benign and attacker-inducible
    /// because anyone can send tokens to an escrow-owned account, so it only warns
    /// and never blocks startup.
    pub fn surplus(&self) -> u64 {
        self.on_chain_actual.saturating_sub(self.db_expected)
    }
}

/// Run startup reconciliation for the escrow indexer.
///
/// Returns `Ok(())` if all mints are within tolerance.
/// Returns `Err(IndexerError::Reconciliation(_))` if any mint exceeds the
/// mismatch threshold – callers should treat this as a fatal startup error.
///
/// Does nothing when `program_type` is not `Escrow` (only the escrow program
/// has ATAs to check).
pub async fn run_startup_reconciliation(
    config: &ReconciliationConfig,
    program_type: ProgramType,
    storage: &Storage,
    rpc_url: &str,
    channel_rpc_url: Option<&str>,
    instance_pda: &Pubkey,
) -> Result<(), IndexerError> {
    if program_type != ProgramType::Escrow {
        info!("Startup reconciliation skipped (program_type is not Escrow)");
        return Ok(());
    }

    let snapshot = capture_custody_snapshot(rpc_url, instance_pda).await?;
    reconcile_against_snapshot(
        config,
        program_type,
        storage,
        rpc_url,
        channel_rpc_url,
        instance_pda,
        &snapshot,
    )
    .await
}

/// Read the escrow's on-chain custody, along with the slot the reading is valid as of.
///
/// Callers that can catch their ledger up take this first and compare against it after,
/// so the two sides of the comparison describe the same slot. Reading custody afterwards
/// instead would measure a chain that has moved on from the ledger it is judged against.
pub async fn capture_custody_snapshot(
    rpc_url: &str,
    instance_pda: &Pubkey,
) -> Result<CustodySnapshot, IndexerError> {
    let rpc_client = RpcClientWithRetry::with_retry_config(
        rpc_url.to_string(),
        RetryConfig::default(),
        CommitmentConfig::finalized(),
    );

    let snapshot = fetch_escrow_balances_by_mint(&rpc_client, *instance_pda)
        .await
        .map_err(|e| match e {
            // Kept distinct so the caller's retry can take another sweep: this one says the
            // node never held still, not that custody could not be read.
            SweepFailure::SlotUnsettled {
                attempts,
                low,
                high,
            } => ReconciliationError::CustodySlotUnsettled {
                attempts,
                low,
                high,
            },
            SweepFailure::Read(e) => ReconciliationError::Rpc {
                mint: instance_pda.to_string(),
                reason: e.reason,
            },
        })?;

    info!(
        instance_pda = %instance_pda,
        snapshot_slot = snapshot.slot,
        mint_count = snapshot.balances.len(),
        "Captured on-chain escrow custody"
    );
    Ok(snapshot)
}

/// Compare a captured custody snapshot against the ledger as of the snapshot's slot.
pub async fn reconcile_against_snapshot(
    config: &ReconciliationConfig,
    program_type: ProgramType,
    storage: &Storage,
    rpc_url: &str,
    channel_rpc_url: Option<&str>,
    instance_pda: &Pubkey,
    snapshot: &CustodySnapshot,
) -> Result<(), IndexerError> {
    if program_type != ProgramType::Escrow {
        info!("Startup reconciliation skipped (program_type is not Escrow)");
        return Ok(());
    }

    // The supply invariant must always run for an escrow indexer, so the channel
    // RPC is a hard config gate: a missing or blank one fails the boot even in empty state.
    let channel_rpc_url = channel_rpc_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or(IndexerError::Reconciliation(
            ReconciliationError::MissingChannelRpc,
        ))?;

    info!(
        instance_pda = %instance_pda,
        snapshot_slot = snapshot.slot,
        "Running startup reconciliation"
    );

    // Snapshot of any deposit rows whose mint never had an `AllowMint` row.
    // The log here gives a complete boot-time snapshot before runtime
    // dedup hides anything they haven't already seen.
    log_orphan_deposit_rows_at_startup(storage).await;

    // Bounded by the snapshot's slot so the ledger side answers for the same instant the
    // custody side does. Without it, rows indexed after the reading would be counted
    // against custody that never reflected them.
    let mint_balances = storage
        .get_mint_balances_for_reconciliation(snapshot.slot)
        .await
        .map_err(ReconciliationError::Storage)?;

    let results = build_reconciliation_set(&mint_balances, &snapshot.balances)?;

    if results.is_empty() {
        // Reached only when both the escrow sweep and the DB are genuinely empty, i.e. a truly-first deploy.
        info!("Both on-chain escrow and DB are empty; reconciliation passed (empty state)");
        return Ok(());
    }

    info!(
        mint_count = results.len(),
        "Comparing DB totals against on-chain escrow balances"
    );

    // The supply invariant runs first because it reads the chain on both sides and so
    // means the same thing however far behind the ledger is. Reporting the ledger
    // mismatch ahead of it would hide the graver finding whenever both are true, and
    // would send startup back for another catch-up that cannot change this answer.
    check_channel_supply_invariant(channel_rpc_url, rpc_url, instance_pda, config, &results)
        .await?;

    classify_and_report(config, &results)?;

    Ok(())
}

/// Readings a supply breach must appear in, consecutively, before it is believed.
const SUPPLY_BREACH_CONFIRMATIONS: u32 = 3;

/// Ceiling on reading rounds. A round whose supply read fails proves nothing either way,
/// so it buys a replacement round instead of counting: a flaky RPC must not shorten the
/// rule to fewer than SUPPLY_BREACH_CONFIRMATIONS real comparisons.
const SUPPLY_BREACH_MAX_ROUNDS: u32 = SUPPLY_BREACH_CONFIRMATIONS * 2;

/// Pause between those readings, so each one sees a genuinely later state of both chains.
/// Sized for a withdrawal in flight: the burn and the release finalize on separate chains
/// moments apart, and the readings have to span that gap or all of them land inside it.
#[cfg(not(test))]
const SUPPLY_BREACH_RECHECK_DELAY_MS: u64 = 2_000;
#[cfg(test)]
const SUPPLY_BREACH_RECHECK_DELAY_MS: u64 = 10;

/// One mint whose channel supply was not covered by custody in a single reading.
struct SupplyBreach {
    mint: String,
    key: Pubkey,
    supply: u64,
    custody: u64,
    custody_slot: u64,
    gap: u64,
}

/// What a single reading round learned: the mints it found breaching, and the mints it
/// could not read at all. The two are kept apart because a read that failed is not
/// evidence that a mint recovered, and only evidence of recovery may clear a breach.
struct SupplyReading {
    breaches: Vec<SupplyBreach>,
    unread: HashSet<Pubkey>,
}

/// Take one reading of every mint's channel supply and compare it against escrow custody.
///
/// Supplies are read first and custody after, so the custody reading cannot be missing a
/// deposit any supply read already saw. Custody is judged on this round's reading alone.
/// The frozen snapshot is never mixed in: it only ages, so using it as a floor would let a
/// mint that lost its backing after the snapshot keep answering with custody it no longer
/// holds. A release landing between the two reads can still make one round look short, and
/// the repeated readings above are what settle that. `only` restricts the reading to mints
/// an earlier round already suspected.
async fn measure_supply_breaches(
    channel_rpc: &RpcClientWithRetry,
    escrow_rpc_url: &str,
    instance_pda: &Pubkey,
    config: &ReconciliationConfig,
    results: &[MintReconciliation],
    only: Option<&HashSet<Pubkey>>,
) -> Result<SupplyReading, IndexerError> {
    // Same mint universe as the ledger comparison, so a mint is never dropped from the
    // invariant just because it holds nothing on chain.
    let mut supplies: Vec<(&MintReconciliation, Pubkey, u64)> = Vec::new();
    let mut unread: HashSet<Pubkey> = HashSet::new();
    for r in results {
        let mint = r
            .mint
            .parse::<Pubkey>()
            .map_err(|e| ReconciliationError::InvalidPubkey {
                pubkey: r.mint.clone(),
                reason: e.to_string(),
            })?;
        if only.is_some_and(|s| !s.contains(&mint)) {
            continue;
        }
        match fetch_channel_supply(channel_rpc, &mint).await {
            Ok(supply) => supplies.push((r, mint, supply)),
            Err(e) => {
                unread.insert(mint);
                warn!(
                    mint = %r.mint,
                    reason = %e.reason,
                    "Startup supply invariant: channel supply read failed"
                );
            }
        }
    }

    if supplies.is_empty() {
        return Ok(SupplyReading {
            breaches: Vec::new(),
            unread,
        });
    }

    let custody = capture_custody_snapshot(escrow_rpc_url, instance_pda).await?;

    let mut breaches = Vec::new();
    for (r, mint, supply) in supplies {
        let on_chain_custody = custody.balances.get(&mint).copied().unwrap_or(0);
        let gap = supply.saturating_sub(on_chain_custody);
        if gap > config.mismatch_threshold_raw {
            breaches.push(SupplyBreach {
                mint: r.mint.clone(),
                key: mint,
                supply,
                custody: on_chain_custody,
                custody_slot: custody.slot,
                gap,
            });
        }
    }
    Ok(SupplyReading { breaches, unread })
}

/// Fail startup if any mint's channel supply exceeds the custody backing it.
///
/// Supply and custody live on different chains and both keep moving, so no single pass can
/// read them at one instant: a deposit minted between the readings, or a burn whose release
/// has not landed yet, each make a healthy mint look short-changed for a moment. A breach
/// is therefore believed only if it survives several consecutive readings, which is the
/// same persistence rule the runtime invariant uses. A real insolvency does not heal
/// between reads, so this costs nothing on a healthy boot and nothing in detection.
///
/// A supply read that errors buys another round rather than being written off. The rounds
/// give a gateway that is still coming up time to answer, but a mint that stays unreadable
/// to the end stops the boot: an unreadable channel hides an existing breach exactly as
/// well as a healthy one does, and nothing here can tell the two apart.
async fn check_channel_supply_invariant(
    channel_rpc_url: &str,
    escrow_rpc_url: &str,
    instance_pda: &Pubkey,
    config: &ReconciliationConfig,
    results: &[MintReconciliation],
) -> Result<(), IndexerError> {
    let channel_rpc = RpcClientWithRetry::with_retry_config(
        channel_rpc_url.to_string(),
        RetryConfig::default(),
        CommitmentConfig::finalized(),
    );

    // Each round re-reads only what the previous one suspected, so a mint has to breach in
    // every reading to survive to the end.
    let mut suspects: Option<HashSet<Pubkey>> = None;
    // Per mint: its latest breach numbers, and how many readings have shown that breach.
    // Counted per mint because the rule is about one mint's own history; a shared counter
    // would let a mint seen breaching once ride out on rounds other mints supplied.
    let mut standing: BTreeMap<Pubkey, (SupplyBreach, u32)> = BTreeMap::new();

    for round in 1..=SUPPLY_BREACH_MAX_ROUNDS {
        let reading = measure_supply_breaches(
            &channel_rpc,
            escrow_rpc_url,
            instance_pda,
            config,
            results,
            suspects.as_ref(),
        )
        .await?;

        // A mint the node would not answer for proves nothing either way, so it stays a
        // suspect and buys another round. Later rounds only ask about suspects, so whatever
        // came back unread is already the set still owed an answer.
        let unresolved = &reading.unread;

        if reading.breaches.is_empty() && unresolved.is_empty() {
            return Ok(());
        }

        // Carry forward only what this round still holds against: a mint read cleanly here
        // is out, whatever an earlier round said about it.
        let mut still: HashSet<Pubkey> = reading.breaches.iter().map(|b| b.key).collect();
        still.extend(unresolved.iter().copied());
        let unresolved_count = unresolved.len();
        standing.retain(|mint, _| still.contains(mint));

        // A breach reading advances that mint's count; a round that could not read it
        // neither advances nor resets it, having shown nothing either way.
        for b in reading.breaches {
            match standing.get_mut(&b.key) {
                Some((detail, readings)) => {
                    *detail = b;
                    *readings += 1;
                }
                None => {
                    standing.insert(b.key, (b, 1));
                }
            }
        }
        suspects = Some(still);

        if standing
            .values()
            .any(|(_, readings)| *readings >= SUPPLY_BREACH_CONFIRMATIONS)
        {
            break;
        }

        if round < SUPPLY_BREACH_MAX_ROUNDS {
            warn!(
                mint_count = standing.len(),
                round,
                required = SUPPLY_BREACH_CONFIRMATIONS,
                unresolved = unresolved_count,
                "Startup supply invariant: possible breach, re-reading before acting on it"
            );
            tokio::time::sleep(std::time::Duration::from_millis(
                SUPPLY_BREACH_RECHECK_DELAY_MS,
            ))
            .await;
        }
    }

    // `readings` is per mint, so a mint that ran out of rounds while unreadable is visibly
    // weaker evidence than one that breached in every reading, though both stop the boot.
    for (b, readings) in standing.values() {
        error!(
            reconciliation_alert = true,
            mint = %b.mint,
            supply = b.supply,
            on_chain_custody = b.custody,
            custody_slot = b.custody_slot,
            gap = b.gap,
            threshold = config.mismatch_threshold_raw,
            readings,
            required = SUPPLY_BREACH_CONFIRMATIONS,
            "RECONCILIATION ALERT: channel token supply exceeds escrow custody"
        );
    }

    if !standing.is_empty() {
        return Err(IndexerError::Reconciliation(
            ReconciliationError::SupplyExceedsCustody {
                count: standing.len(),
                threshold: config.mismatch_threshold_raw,
            },
        ));
    }

    // Left over: mints that never breached but were never readable either. The invariant
    // did not run for them, and an unreadable gateway looks exactly like a healthy one from
    // here, so the boot stops instead of vouching for custody it never measured.
    let unverified = suspects.map(|s| s.len()).unwrap_or(0);
    error!(
        reconciliation_alert = true,
        mint_count = unverified,
        rounds = SUPPLY_BREACH_MAX_ROUNDS,
        "RECONCILIATION ALERT: channel supply unreadable, the supply invariant did not run"
    );
    Err(IndexerError::Reconciliation(
        ReconciliationError::SupplyInvariantUnverified { count: unverified },
    ))
}

/// Build the per-mint reconciliation set from the union of (DB mints) and
/// (on-chain escrow mints). A mint present on only one side compares against 0
/// on the other; an empty result means both sides are genuinely empty.
fn build_reconciliation_set(
    db_balances: &[MintDbBalance],
    on_chain_balances: &HashMap<Pubkey, u64>,
) -> Result<Vec<MintReconciliation>, ReconciliationError> {
    // Keyed by mint string so the DB side (String addresses) and on-chain side
    // (Pubkey) merge into one universe; BTreeMap keeps the order deterministic.
    let mut by_mint: BTreeMap<String, (u64, u64)> = BTreeMap::new();

    for balance in db_balances {
        let net = &balance.total_deposits - &balance.total_withdrawals;
        by_mint.entry(balance.mint_address.clone()).or_default().0 =
            net_db_expected(&net, &balance.mint_address)?;
    }

    for (mint, on_chain) in on_chain_balances {
        by_mint.entry(mint.to_string()).or_default().1 = *on_chain;
    }

    Ok(by_mint
        .into_iter()
        .map(|(mint, (db_expected, on_chain_actual))| {
            MintReconciliation::new(mint, db_expected, on_chain_actual)
        })
        .collect())
}

/// Log deposit rows whose mint was not allowed at the deposit's slot.
/// Diagnostic only, surfaced at boot, never fails startup, and a query
/// failure is logged at `warn` and swallowed rather than propagated.
async fn log_orphan_deposit_rows_at_startup(storage: &Storage) {
    match storage.get_orphan_deposit_ids().await {
        Ok(orphans) if !orphans.is_empty() => {
            error!(
                row_count = orphans.len(),
                orphan_ids = ?orphans,
                "Startup reconciliation: orphan deposit row(s) present (deposit rows with \
                 no allowed mint status at the deposit's slot) — surfaced for visibility, does not fail startup"
            );
        }
        Ok(_) => {
            info!("Startup reconciliation: no orphan deposit rows");
        }
        Err(e) => {
            warn!(
                "Startup reconciliation: failed to query orphan deposit ids: {}",
                e
            );
        }
    }
}

/// Turn a mint's net (deposits - withdrawals) into the expected escrow balance (a u64).
/// Three cases:
/// - In range: return it as-is.
/// - Negative (more withdrawals than deposits): the DB is missing deposit history, so
///   clamp to 0 and warn. Expected 0 against a real on-chain balance is just a surplus,
///   which warns but never blocks.
/// - Above u64::MAX: impossible for a real escrow (the ATA balance is a u64), so treat it
///   as DB corruption and abort startup.
fn net_db_expected(
    net: &bigdecimal::BigDecimal,
    mint_address: &str,
) -> Result<u64, ReconciliationError> {
    match net_to_u64(net) {
        NetBalance::Exact(v) => Ok(v),
        NetBalance::Negative => {
            warn!(
                mint = mint_address,
                net = %net,
                "Withdrawals exceed deposits; treating expected escrow balance as 0"
            );
            Ok(0)
        }
        NetBalance::Overflow => Err(ReconciliationError::DbBalanceOverflow {
            mint: mint_address.to_string(),
            net: net.to_string(),
        }),
    }
}

/// Log results and decide whether to allow or block startup.
fn classify_and_report(
    config: &ReconciliationConfig,
    results: &[MintReconciliation],
) -> Result<(), IndexerError> {
    // Only a shortfall past the threshold is fatal: custody below liabilities.
    let exceeding: Vec<&MintReconciliation> = results
        .iter()
        .filter(|r| r.shortfall() > config.mismatch_threshold_raw)
        .collect();

    let within_tolerance: Vec<&MintReconciliation> = results
        .iter()
        .filter(|r| r.shortfall() > 0 && r.shortfall() <= config.mismatch_threshold_raw)
        .collect();

    if !exceeding.is_empty() {
        for r in &exceeding {
            error!(
                reconciliation_alert = true,
                mint = %r.mint,
                db_expected = r.db_expected,
                on_chain_actual = r.on_chain_actual,
                shortfall = r.shortfall(),
                threshold = config.mismatch_threshold_raw,
                "RECONCILIATION ALERT: escrow custody shortfall below DB-expected liabilities exceeds threshold"
            );
        }

        return Err(IndexerError::Reconciliation(
            ReconciliationError::MismatchExceedsThreshold {
                count: exceeding.len(),
                threshold: config.mismatch_threshold_raw,
            },
        ));
    }

    // A surplus is benign and attacker-inducible, so it only warns and never blocks.
    for r in results.iter().filter(|r| r.surplus() > 0) {
        warn!(
            mint = %r.mint,
            db_expected = r.db_expected,
            on_chain_actual = r.on_chain_actual,
            surplus = r.surplus(),
            "Reconciliation: escrow holds more than DB expects (benign surplus, not a solvency issue), continuing startup"
        );
    }

    for r in &within_tolerance {
        warn!(
            mint = %r.mint,
            db_expected = r.db_expected,
            on_chain_actual = r.on_chain_actual,
            shortfall = r.shortfall(),
            threshold = config.mismatch_threshold_raw,
            "Reconciliation: custody shortfall within tolerance, continuing startup"
        );
    }

    // Every passing mint is exactly one of balanced, within-tolerance shortfall, or
    // surplus, so these three counts sum to total_mints.
    let surplus = results.iter().filter(|r| r.surplus() > 0).count();
    let balanced = results
        .iter()
        .filter(|r| r.shortfall() == 0 && r.surplus() == 0)
        .count();
    info!(
        total_mints = results.len(),
        balanced,
        within_tolerance = within_tolerance.len(),
        surplus,
        "Startup reconciliation passed"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // directional accessor tests
    // =========================================================================

    #[test]
    fn shortfall_when_db_exceeds_chain() {
        let r = make_result("mint", 1000, 900);
        assert_eq!(r.shortfall(), 100);
        assert_eq!(r.surplus(), 0);
    }

    #[test]
    fn surplus_when_chain_exceeds_db() {
        let r = make_result("mint", 900, 1000);
        assert_eq!(r.surplus(), 100);
        assert_eq!(r.shortfall(), 0);
    }

    #[test]
    fn balanced_is_zero_both_directions() {
        let r = make_result("mint", 1000, 1000);
        assert_eq!(r.shortfall(), 0);
        assert_eq!(r.surplus(), 0);
    }

    #[test]
    fn accessors_saturate_across_full_u64_range() {
        // Saturating subtraction keeps both directions lossless and panic-free at the extremes.
        let s = make_result("mint", 0, u64::MAX);
        assert_eq!(s.surplus(), u64::MAX);
        assert_eq!(s.shortfall(), 0);
        let d = make_result("mint", u64::MAX, 0);
        assert_eq!(d.shortfall(), u64::MAX);
        assert_eq!(d.surplus(), 0);
    }

    // =========================================================================
    // net_db_expected tests
    // =========================================================================

    #[test]
    fn net_db_expected_clamps_negative_to_zero() {
        // Withdrawals exceeding deposits is an impossible-in-a-healthy-system
        // state; the net is clamped to 0 so a corrupt over-withdrawn mint reads
        // as expected balance 0 and surfaces as a mismatch, not a wrap.
        let net = bigdecimal::BigDecimal::from(-50);
        assert_eq!(net_db_expected(&net, "mint").unwrap(), 0);
    }

    #[test]
    fn net_db_expected_passes_full_u64_range() {
        let net = bigdecimal::BigDecimal::from(u64::MAX);
        assert_eq!(net_db_expected(&net, "mint").unwrap(), u64::MAX);
    }

    #[test]
    fn net_db_expected_over_u64_is_a_hard_error() {
        // A net above u64::MAX cannot back a real escrow ATA (itself a u64), so it
        // is a corrupt-DB signal and must fail the startup gate, not return a value.
        let net = bigdecimal::BigDecimal::from(u64::MAX) + bigdecimal::BigDecimal::from(1);
        assert!(matches!(
            net_db_expected(&net, "mint"),
            Err(ReconciliationError::DbBalanceOverflow { .. })
        ));
    }

    // =========================================================================
    // classify_and_report tests
    // =========================================================================

    fn make_result(mint: &str, db_expected: u64, on_chain_actual: u64) -> MintReconciliation {
        MintReconciliation::new(mint.to_string(), db_expected, on_chain_actual)
    }

    #[test]
    fn test_classify_all_balanced() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let results = vec![
            make_result("mint1", 1000, 1000),
            make_result("mint2", 500, 500),
        ];
        assert!(classify_and_report(&config, &results).is_ok());
    }

    #[test]
    fn test_classify_shortfall_within_tolerance() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        // shortfall = 5, threshold = 10 => should pass with warning
        let results = vec![make_result("mint1", 1005, 1000)];
        assert!(classify_and_report(&config, &results).is_ok());
    }

    #[test]
    fn test_classify_shortfall_equals_threshold() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 5,
        };
        // shortfall == threshold => within tolerance (not exceeding)
        let results = vec![make_result("mint1", 1005, 1000)];
        assert!(classify_and_report(&config, &results).is_ok());
    }

    #[test]
    fn test_classify_shortfall_exceeds_threshold_blocks() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 4,
        };
        // shortfall = 5 > threshold = 4 => error
        let results = vec![make_result("mint1", 1005, 1000)];
        let err = classify_and_report(&config, &results).unwrap_err();
        match err {
            IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
                count,
                threshold,
            }) => {
                assert_eq!(count, 1);
                assert_eq!(threshold, 4);
            }
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_classify_strict_zero_threshold_any_shortfall_blocks() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let results = vec![make_result("mint1", 1001, 1000)];
        assert!(classify_and_report(&config, &results).is_err());
    }

    #[test]
    fn test_classify_surplus_never_blocks() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        // A large surplus at the strictest threshold must still pass: surplus is benign.
        let results = vec![make_result("mint1", 1000, 1_000_000)];
        assert!(classify_and_report(&config, &results).is_ok());
    }

    #[test]
    fn test_classify_surplus_does_not_inflate_block_count() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let results = vec![
            make_result("mint1", 1000, 1000),      // balanced
            make_result("mint2", 1000, 1_000_000), // large surplus => warn only
            make_result("mint3", 1005, 1000),      // shortfall 5 <= 10 => warn
            make_result("mint4", 1020, 1000),      // shortfall 20 > 10 => the only blocker
        ];
        let err = classify_and_report(&config, &results).unwrap_err();
        match err {
            IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
                count,
                threshold,
            }) => {
                assert_eq!(count, 1);
                assert_eq!(threshold, 10);
            }
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_classify_empty_results_passes() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        assert!(classify_and_report(&config, &[]).is_ok());
    }

    #[test]
    fn test_classify_pending_deposit_included_in_db_expected() {
        // Regression: total_deposits must include all statuses (pending/processing/failed),
        // not just completed. If the SQL is wrong, db_expected would be 0 (only completed=0),
        // and the on-chain balance of 500 would produce a false mismatch.
        // With the correct SQL, total_deposits = 500 (all indexed), so db_expected = 500
        // and there is no mismatch.
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        // Simulate: 500 tokens deposited (pending, not yet operator-completed),
        // db_expected = all_deposits(500) - completed_withdrawals(0) = 500
        // on_chain_actual = 500 → balanced
        let results = vec![make_result("mint1", 500, 500)];
        assert!(
            classify_and_report(&config, &results).is_ok(),
            "pending deposits should be included in db_expected; should not cause false mismatch"
        );
    }

    // =========================================================================
    // build_reconciliation_set (union) tests
    // =========================================================================

    fn db_balance(mint: &str, deposits: u64, withdrawals: u64) -> MintDbBalance {
        MintDbBalance {
            mint_address: mint.to_string(),
            token_program: spl_token::id().to_string(),
            total_deposits: bigdecimal::BigDecimal::from(deposits),
            total_withdrawals: bigdecimal::BigDecimal::from(withdrawals),
        }
    }

    #[test]
    fn union_empty_both_sides_is_empty() {
        assert!(build_reconciliation_set(&[], &HashMap::new())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn union_db_only_mint_compares_against_zero_on_chain() {
        let results =
            build_reconciliation_set(&[db_balance("MintAAAA", 1000, 200)], &HashMap::new())
                .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].mint, "MintAAAA");
        assert_eq!(results[0].db_expected, 800);
        assert_eq!(results[0].on_chain_actual, 0);
        assert_eq!(results[0].shortfall(), 800);
        assert_eq!(results[0].surplus(), 0);
    }

    #[test]
    fn union_on_chain_only_mint_compares_against_zero_db() {
        let mint = Pubkey::new_unique();
        let on_chain = HashMap::from([(mint, 1234u64)]);
        let results = build_reconciliation_set(&[], &on_chain).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].mint, mint.to_string());
        assert_eq!(results[0].db_expected, 0);
        assert_eq!(results[0].on_chain_actual, 1234);
        assert_eq!(results[0].surplus(), 1234);
        assert_eq!(results[0].shortfall(), 0);
    }

    #[test]
    fn union_merges_same_mint_on_both_sides() {
        let mint = Pubkey::new_unique();
        let on_chain = HashMap::from([(mint, 1000u64)]);
        let results =
            build_reconciliation_set(&[db_balance(&mint.to_string(), 1000, 0)], &on_chain).unwrap();
        assert_eq!(results.len(), 1, "same mint must merge to one entry");
        assert_eq!(results[0].shortfall(), 0);
        assert_eq!(results[0].surplus(), 0);
    }

    #[test]
    fn union_includes_disjoint_mints_from_both_sides() {
        let db_mint = Pubkey::new_unique();
        let chain_mint = Pubkey::new_unique();
        let on_chain = HashMap::from([(chain_mint, 700u64)]);
        let results =
            build_reconciliation_set(&[db_balance(&db_mint.to_string(), 500, 0)], &on_chain)
                .unwrap();
        assert_eq!(results.len(), 2, "union must contain both mints");
    }

    #[test]
    fn union_db_overflow_is_a_hard_error() {
        // A DB net above u64::MAX is corrupt accounting; building the set must fail
        // closed rather than emit a sentinel that could compare as balanced.
        let mut bal = db_balance("MintAAAA", 0, 0);
        bal.total_deposits =
            bigdecimal::BigDecimal::from(u64::MAX) + bigdecimal::BigDecimal::from(1);
        assert!(matches!(
            build_reconciliation_set(&[bal], &HashMap::new()),
            Err(ReconciliationError::DbBalanceOverflow { .. })
        ));
    }

    // =========================================================================
    // run_startup_reconciliation contract tests (mockito escrow sweep)
    // =========================================================================

    use crate::storage::common::storage::mock::MockStorage;

    fn make_mint_balance(
        mint_address: &str,
        total_deposits: u64,
        total_withdrawals: u64,
    ) -> MintDbBalance {
        MintDbBalance {
            mint_address: mint_address.to_string(),
            token_program: spl_token::id().to_string(),
            total_deposits: bigdecimal::BigDecimal::from(total_deposits),
            total_withdrawals: bigdecimal::BigDecimal::from(total_withdrawals),
        }
    }

    /// `get_token_accounts_by_owner` returns jsonParsed token accounts; the sweep
    /// sums them per mint. Build one such account entry for the mock RPC response.
    fn token_account_entry(mint: &str, amount: u64) -> String {
        format!(
            r#"{{"pubkey":"{ata}","account":{{"lamports":2039280,"owner":"{owner}",
                "executable":false,"rentEpoch":0,"space":165,
                "data":{{"program":"spl-token","space":165,
                    "parsed":{{"type":"account","info":{{"mint":"{mint}","owner":"{owner}",
                        "tokenAmount":{{"amount":"{amount}","decimals":6,"uiAmount":null,
                            "uiAmountString":"{amount}"}}}}}}}}}}}}"#,
            ata = Pubkey::new_unique(),
            owner = Pubkey::new_unique(),
            mint = mint,
            amount = amount,
        )
    }

    /// Mock both sweep calls (SPL Token and Token-2022). The SPL Token call (matched
    /// by its program id in the request body) returns `entries`; the Token-2022 call
    /// returns an empty list so balances are not double-counted.
    async fn mock_escrow_sweep(server: &mut mockito::Server, entries: &[(String, u64)]) {
        let value: Vec<String> = entries
            .iter()
            .map(|(mint, amount)| token_account_entry(mint, *amount))
            .collect();
        let token_body = format!(
            r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":100}},"value":[{}]}},"id":1}}"#,
            value.join(",")
        );
        let empty_body =
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":100},"value":[]},"id":1}"#.to_string();

        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token::id().to_string()))
            .with_status(200)
            .with_body(token_body)
            .create_async()
            .await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(spl_token_2022::id().to_string()))
            .with_status(200)
            .with_body(empty_body)
            .create_async()
            .await;
    }

    #[tokio::test]
    async fn test_reconciliation_skipped_for_withdraw_program() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let storage = Storage::Mock(MockStorage::new());
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Withdraw,
            &storage,
            "http://localhost:8899",
            None,
            &seed,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn escrow_without_channel_rpc_is_fatal() {
        // The supply invariant must always run, so a missing channel RPC fails the
        // escrow indexer boot rather than silently skipping the check.
        let mut server = mockito::Server::new_async().await;
        mock_escrow_sweep(&mut server, &[]).await;
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let storage = Storage::Mock(MockStorage::new());
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            None,
            &seed,
        )
        .await;
        assert!(matches!(
            result,
            Err(IndexerError::Reconciliation(
                ReconciliationError::MissingChannelRpc
            ))
        ));
    }

    #[tokio::test]
    async fn test_reconciliation_empty_db_and_empty_escrow_passes() {
        let mut server = mockito::Server::new_async().await;
        mock_escrow_sweep(&mut server, &[]).await;

        let storage = Storage::Mock(MockStorage::new());
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;
        assert!(
            result.is_ok(),
            "truly-empty state (no DB mints, no escrow balance) must pass: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_empty_db_with_nonempty_escrow_passes_as_surplus() {
        // A fresh or partial DB against a live escrow is a pure surplus: the escrow
        // holds more than the DB expects. Custody above liabilities is not a solvency
        // risk and is attacker-inducible, so startup continues with a warning instead
        // of failing closed.
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        // Healthy supply, so the ledger comparison stays the thing under test.
        mock_channel_supply(&mut server, 0).await;

        let storage = Storage::Mock(MockStorage::new());
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;

        assert!(
            result.is_ok(),
            "live escrow with empty DB is a benign surplus and must not block startup: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_surplus_does_not_block() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // DB expects 1000, on-chain has 1_000_000 => pure surplus, strict threshold 0.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000_000)]).await;
        // A readable supply below custody, so the invariant holds and the surplus
        // rule is what the test actually exercises.
        mock_channel_supply(&mut server, 1_000).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;
        assert!(
            result.is_ok(),
            "a surplus must never block startup: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_balanced_passes() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        // Healthy supply, so the ledger comparison stays the thing under test.
        mock_channel_supply(&mut server, 1_000).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;
        assert!(result.is_ok(), "balanced state should pass: {:?}", result);
    }

    #[tokio::test]
    async fn test_reconciliation_shortfall_within_threshold_passes() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // DB expects 1000, on-chain has 995 => shortfall 5 <= threshold 10 => ok
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 995)]).await;
        // Healthy supply, so the ledger comparison stays the thing under test.
        mock_channel_supply(&mut server, 1_000).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;
        assert!(
            result.is_ok(),
            "shortfall within threshold should pass: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_shortfall_exceeds_threshold_blocks() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // DB expects 1000, on-chain has 980 => shortfall 20 > threshold 10 => err
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 980)]).await;
        // Supply matches custody, so the supply invariant passes and the ledger
        // comparison is the only thing left to fail.
        mock_channel_supply(&mut server, 980).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;
        match result {
            Err(IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
                count,
                threshold,
            })) => {
                assert_eq!(count, 1);
                assert_eq!(threshold, 10);
            }
            other => panic!("Expected MismatchExceedsThreshold, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_reconciliation_with_nonzero_withdrawals_balanced() {
        // 1500 deposits, 500 withdrawals => db_expected 1000; on-chain 1000 => balanced.
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        // Healthy supply, so the ledger comparison stays the thing under test.
        mock_channel_supply(&mut server, 1_000).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1500, 500)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &server.url(),
            Some(&server.url()),
            &seed,
        )
        .await;
        assert!(
            result.is_ok(),
            "net (deposits - withdrawals) must match on-chain: {:?}",
            result
        );
    }

    /// A deposit that lands after the snapshot can be minted before the supply is read, so
    /// judging supply against the frozen snapshot would call a fully backed mint a breach
    /// and abort the boot. The snapshot is deliberately old; the supply check must read
    /// custody for itself.
    #[tokio::test]
    async fn supply_invariant_is_not_fooled_by_custody_that_arrived_after_the_snapshot() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // Custody now holds the deposit backing the mint, and the channel has minted it.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        mock_channel_supply(&mut server, 1_000).await;

        // The snapshot predates the deposit: at that slot the escrow held nothing.
        let snapshot = CustodySnapshot {
            balances: HashMap::from([(mint, 0)]),
            slot: 50,
        };

        let storage = Storage::Mock(MockStorage::new());
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let url = server.url();
        let result = reconcile_against_snapshot(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &Pubkey::new_unique(),
            &snapshot,
        )
        .await;

        assert!(
            result.is_ok(),
            "a mint backed by custody that arrived after the snapshot must not be a breach: {:?}",
            result
        );
    }

    /// The mirror case. A withdrawal burns on the channel and releases on Solana, and for
    /// a moment a reading can catch the release without the burn. Custody is short in that
    /// one reading only, so the re-read is what has to clear it.
    #[tokio::test]
    async fn custody_released_before_its_burn_is_visible_clears_on_a_re_read() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // The fresh sweep sees the escrow already emptied by the release.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 0)]).await;
        // First reading still predates the burn; the next one sees the supply gone too.
        mock_channel_supply_times(&mut server, 1_000, Some(1)).await;
        mock_channel_supply(&mut server, 0).await;

        // The snapshot predates the release: at that slot custody still backed the supply.
        let snapshot = CustodySnapshot {
            balances: HashMap::from([(mint, 1_000)]),
            slot: 50,
        };

        // The ledger matches the snapshot, so only the supply logic is under test here.
        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1_000, 0)]);
        let storage = Storage::Mock(mock_storage);
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let url = server.url();
        let result = reconcile_against_snapshot(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &Pubkey::new_unique(),
            &snapshot,
        )
        .await;

        assert!(
            result.is_ok(),
            "a release read ahead of its burn must not abort the boot: {:?}",
            result
        );
    }

    /// Custody that is gone and stays gone is the insolvency this gate exists to catch,
    /// and a snapshot taken while the backing was still there must not answer for it. The
    /// snapshot only ages, so letting it stand in for custody would excuse the breach on
    /// every reading and pass the boot for good.
    #[tokio::test]
    async fn a_healthy_snapshot_cannot_excuse_custody_that_is_gone() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // Escrow is empty on every reading, and the supply was never burned against it.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 0)]).await;
        mock_channel_supply(&mut server, 1_000).await;

        // Taken while custody still backed the supply, so the ledger comparison passes.
        let snapshot = CustodySnapshot {
            balances: HashMap::from([(mint, 1_000)]),
            slot: 50,
        };

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1_000, 0)]);
        let storage = Storage::Mock(mock_storage);
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let url = server.url();
        let result = reconcile_against_snapshot(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &Pubkey::new_unique(),
            &snapshot,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(IndexerError::Reconciliation(
                    ReconciliationError::SupplyExceedsCustody { .. }
                ))
            ),
            "supply standing over an empty escrow must stop the boot: {:?}",
            result
        );
    }

    /// getAccountInfo mock returning an SPL Mint blob with `supply`.
    async fn mock_channel_supply(server: &mut mockito::Server, supply: u64) {
        mock_channel_supply_times(server, supply, None).await;
    }

    /// `times` caps how many reads this supply is served for, so a test can script one
    /// reading followed by a different one.
    async fn mock_channel_supply_times(
        server: &mut mockito::Server,
        supply: u64,
        times: Option<usize>,
    ) {
        use base64::Engine as _;
        use spl_token::solana_program::program_option::COption;
        use spl_token::solana_program::program_pack::Pack;
        use spl_token::state::Mint;

        let mint_state = Mint {
            mint_authority: COption::Some(Pubkey::new_unique()),
            supply,
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
        };
        let mut buf = vec![0u8; Mint::LEN];
        mint_state.pack_into_slice(&mut buf);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        let mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex("getAccountInfo".to_string()))
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"context":{{"slot":100}},"value":{{"owner":"{prog}","lamports":1000000,"data":["{b64}","base64"],"executable":false,"rentEpoch":0}}}}}}"#,
                prog = spl_token::id(),
            ));
        match times {
            Some(n) => mock.expect(n).create_async().await,
            None => mock.create_async().await,
        };
    }

    /// A channel-supply read the node refuses outright. Uses -32601 so the retry wrapper
    /// treats it as permanent and answers immediately instead of backing off five times.
    async fn mock_channel_supply_failure(server: &mut mockito::Server) {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex("getAccountInfo".to_string()))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#,
            )
            .create_async()
            .await;
    }

    /// Supply and custody sit on different chains, so one reading can catch a mint mid
    /// flight: minted here, not yet released there. Startup must re-read before acting,
    /// or an operator working alongside it turns an ordinary boot into a fatal alert.
    #[tokio::test]
    async fn supply_breach_that_clears_on_a_re_read_is_not_fatal() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        // First reading catches supply above custody; the next one no longer does.
        mock_channel_supply_times(&mut server, 1_200, Some(1)).await;
        mock_channel_supply(&mut server, 1_000).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1_000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;

        assert!(
            result.is_ok(),
            "a breach that does not survive a re-read must not abort startup: {:?}",
            result
        );
    }

    /// A channel-supply read scoped to one mint, so a test can script the two mints of a
    /// round independently. `times` caps how many reads this answer is served for.
    async fn mock_supply_for_mint(
        server: &mut mockito::Server,
        mint: &Pubkey,
        supply: u64,
        times: Option<usize>,
    ) -> mockito::Mock {
        use base64::Engine as _;
        use spl_token::solana_program::program_option::COption;
        use spl_token::solana_program::program_pack::Pack;
        use spl_token::state::Mint;

        let mint_state = Mint {
            mint_authority: COption::Some(Pubkey::new_unique()),
            supply,
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
        };
        let mut buf = vec![0u8; Mint::LEN];
        mint_state.pack_into_slice(&mut buf);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
        let mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex("getAccountInfo".to_string()),
                mockito::Matcher::Regex(mint.to_string()),
            ]))
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"context":{{"slot":100}},"value":{{"owner":"{prog}","lamports":1000000,"data":["{b64}","base64"],"executable":false,"rentEpoch":0}}}}}}"#,
                prog = spl_token::id(),
            ));
        match times {
            Some(n) => mock.expect(n).create_async().await,
            None => mock.create_async().await,
        }
    }

    /// Same scoping for a read the node refuses outright.
    async fn mock_supply_failure_for_mint(
        server: &mut mockito::Server,
        mint: &Pubkey,
        times: Option<usize>,
    ) -> mockito::Mock {
        let mock = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex("getAccountInfo".to_string()),
                mockito::Matcher::Regex(mint.to_string()),
            ]))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#,
            );
        match times {
            Some(n) => mock.expect(n).create_async().await,
            None => mock.create_async().await,
        }
    }

    /// A confirmation round that cannot read the mint has learned nothing. Reading that
    /// silence as "the breach cleared" would let an insolvent channel boot whenever the
    /// channel RPC failed right after the first reading caught it.
    #[tokio::test]
    async fn supply_breach_is_not_cleared_by_a_failed_re_read() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        // First reading catches supply above custody; every re-read then fails outright.
        mock_channel_supply_times(&mut server, 1_200, Some(1)).await;
        mock_channel_supply_failure(&mut server).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1_000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(IndexerError::Reconciliation(
                    ReconciliationError::SupplyExceedsCustody { .. }
                ))
            ),
            "an unreadable suspect must not clear the breach: {:?}",
            result
        );
    }

    /// A gateway that answers nothing leaves the invariant unrun, and an unreadable channel
    /// looks exactly like a solvent one from here. Startup must stop rather than vouch for
    /// custody it never measured.
    #[tokio::test]
    async fn supply_that_is_never_readable_stops_startup() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // Custody and ledger agree, so only the unreadable supply can decide this.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        mock_channel_supply_failure(&mut server).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1_000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(IndexerError::Reconciliation(
                    ReconciliationError::SupplyInvariantUnverified { count: 1 }
                ))
            ),
            "an unreadable supply must stop the boot, not pass it: {:?}",
            result
        );
    }

    /// The rule is about one mint's own history. With a counter shared across mints, two
    /// suspects taking turns to breach reach three rounds between them, and a mint seen
    /// breaching once gets closed out as confirmed on readings that were never about it.
    #[tokio::test]
    async fn one_mint_cannot_confirm_a_breach_on_another_mints_readings() {
        let mut server = mockito::Server::new_async().await;
        let alpha = Pubkey::new_unique();
        let beta = Pubkey::new_unique();
        mock_escrow_sweep(
            &mut server,
            &[(alpha.to_string(), 1_000), (beta.to_string(), 1_000)],
        )
        .await;

        // Both breach on round 1, so both stay suspects. From there they take turns, and
        // three rounds pass with a breach in each while neither mint has three of its own.
        mock_supply_for_mint(&mut server, &alpha, 1_200, Some(2)).await;
        mock_supply_failure_for_mint(&mut server, &alpha, None).await;

        mock_supply_for_mint(&mut server, &beta, 1_200, Some(1)).await;
        mock_supply_failure_for_mint(&mut server, &beta, Some(1)).await;
        mock_supply_for_mint(&mut server, &beta, 1_200, Some(1)).await;
        // Only reached if the loop kept going past the third round.
        let beyond_round_three = mock_supply_for_mint(&mut server, &beta, 1_200, None).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![
            make_mint_balance(&alpha.to_string(), 1_000, 0),
            make_mint_balance(&beta.to_string(), 1_000, 0),
        ]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;

        beyond_round_three.assert_async().await;
        assert!(
            matches!(
                result,
                Err(IndexerError::Reconciliation(
                    ReconciliationError::SupplyExceedsCustody { .. }
                ))
            ),
            "a suspect that is never read clean still stops the boot: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_supply_exceeds_custody_is_fatal() {
        // Custody sweep and channel supply share one server, routed by method.
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // custody 1000, ledger 1000 (balanced); but minted supply 1200 -> gap 200.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        mock_channel_supply(&mut server, 1_200).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;
        match result {
            Err(IndexerError::Reconciliation(ReconciliationError::SupplyExceedsCustody {
                count,
                ..
            })) => assert_eq!(count, 1),
            other => panic!("supply over custody must block startup, got: {:?}", other),
        }
    }

    /// A stale ledger must not hide a supply breach. The breach is the graver finding and
    /// the only one of the two that no amount of further indexing can clear, so startup
    /// has to report it rather than the mismatch that happens to sit alongside it.
    #[tokio::test]
    async fn startup_reconciliation_reports_the_supply_breach_over_a_stale_ledger() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // Custody 1000 against a ledger at 900 is a mismatch of 100, and minted supply
        // 1200 is 200 above custody. Both exceed the threshold on the same mint.
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        mock_channel_supply(&mut server, 1_200).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 900, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;

        match result {
            Err(IndexerError::Reconciliation(ReconciliationError::SupplyExceedsCustody {
                count,
                ..
            })) => assert_eq!(count, 1),
            other => panic!(
                "the supply breach must surface ahead of the ledger mismatch, got: {:?}",
                other
            ),
        }
    }

    #[tokio::test]
    async fn startup_reconciliation_supply_within_custody_passes() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;
        // Supply equals custody: the invariant holds.
        mock_channel_supply(&mut server, 1_000).await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let storage = Storage::Mock(mock_storage);

        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();
        let url = server.url();
        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;
        assert!(result.is_ok(), "supply <= custody must pass: {:?}", result);
    }

    /// The two rules that survived the main merge point in opposite directions,
    /// and both directions have to hold in one place so neither is later
    /// "simplified" into the other. A custody surplus is benign and never
    /// blocks; a channel supply above custody is insolvency and always does.
    #[tokio::test]
    async fn startup_reconciliation_surplus_passes_but_supply_breach_halts() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let seed = Pubkey::new_unique();

        // Direction 1: custody far above the ledger, supply within custody.
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000_000)]).await;
        mock_channel_supply(&mut server, 1_000).await;
        let surplus_storage = MockStorage::new();
        surplus_storage.set_mint_balances(vec![make_mint_balance(&mint.to_string(), 1000, 0)]);
        let surplus_storage = Storage::Mock(surplus_storage);
        let url = server.url();
        let surplus = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &surplus_storage,
            &url,
            Some(&url),
            &seed,
        )
        .await;
        assert!(
            surplus.is_ok(),
            "a custody surplus must never block startup: {:?}",
            surplus
        );

        // Direction 2: same balanced ledger, but the channel minted past custody.
        let mut breach_server = mockito::Server::new_async().await;
        let breach_mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut breach_server, &[(breach_mint.to_string(), 1_000)]).await;
        mock_channel_supply(&mut breach_server, 1_200).await;
        let breach_storage = MockStorage::new();
        breach_storage.set_mint_balances(vec![make_mint_balance(
            &breach_mint.to_string(),
            1000,
            0,
        )]);
        let breach_storage = Storage::Mock(breach_storage);
        let breach_url = breach_server.url();
        let breach = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &breach_storage,
            &breach_url,
            Some(&breach_url),
            &seed,
        )
        .await;
        assert!(
            matches!(
                breach,
                Err(IndexerError::Reconciliation(
                    ReconciliationError::SupplyExceedsCustody { .. }
                ))
            ),
            "supply over custody must still halt: {:?}",
            breach
        );
    }
}
