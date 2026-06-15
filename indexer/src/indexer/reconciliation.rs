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
//! 1. Sweep the escrow instance's on-chain token accounts, summed per mint.
//! 2. Query the DB for per-mint aggregate balances (all deposits − completed withdrawals).
//! 3. Compare the union of both mint sets; a mint on only one side compares against 0.
//! 4. If any |on_chain - db_expected| > threshold → log error, emit alert, abort startup.
//! 5. If any mismatch ≤ threshold (but > 0) → log warning, continue.
//! 6. If all balanced (or both sides empty) → log info, continue.

use crate::{
    config::{ProgramType, ReconciliationConfig},
    error::{IndexerError, ReconciliationError},
    operator::{
        escrow_sweep::fetch_escrow_balances_by_mint, rpc_util::RpcClientWithRetry, RetryConfig,
    },
    storage::common::amount::{net_to_u64, NetBalance},
    storage::common::models::MintDbBalance,
    storage::Storage,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::collections::{BTreeMap, HashMap};
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
    /// Absolute difference: |on_chain_actual − db_expected|.  Derived from the
    /// two fields above — use `MintReconciliation::new` to ensure consistency.
    pub mismatch: u64,
}

impl MintReconciliation {
    pub fn new(mint: String, db_expected: u64, on_chain_actual: u64) -> Self {
        let mismatch = compute_mismatch(db_expected, on_chain_actual);
        Self {
            mint,
            db_expected,
            on_chain_actual,
            mismatch,
        }
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
    instance_pda: &Pubkey,
) -> Result<(), IndexerError> {
    if program_type != ProgramType::Escrow {
        info!("Startup reconciliation skipped (program_type is not Escrow)");
        return Ok(());
    }

    let instance_pda = *instance_pda;
    info!(
        instance_pda = %instance_pda,
        "Running startup reconciliation"
    );

    // Snapshot of any deposit rows whose mint never had an `AllowMint` row.
    // The log here gives a complete boot-time snapshot before runtime
    // dedup hides anything they haven't already seen.
    log_orphan_deposit_rows_at_startup(storage).await;

    let rpc_client = RpcClientWithRetry::with_retry_config(
        rpc_url.to_string(),
        RetryConfig::default(),
        CommitmentConfig::finalized(),
    );

    // The on-chain sweep is the authoritative custody view.
    let on_chain_balances = fetch_escrow_balances_by_mint(&rpc_client, instance_pda)
        .await
        .map_err(|e| ReconciliationError::Rpc {
            mint: instance_pda.to_string(),
            reason: e.reason,
        })?;

    let mint_balances = storage
        .get_mint_balances_for_reconciliation()
        .await
        .map_err(ReconciliationError::Storage)?;

    let results = build_reconciliation_set(&mint_balances, &on_chain_balances)?;

    if results.is_empty() {
        // Reached only when both the escrow sweep and the DB are genuinely empty, i.e. a truly-first deploy.
        info!("Both on-chain escrow and DB are empty; reconciliation passed (empty state)");
        return Ok(());
    }

    info!(
        mint_count = results.len(),
        "Comparing DB totals against on-chain escrow balances"
    );

    classify_and_report(config, &results)
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

/// Convert a per-mint net (deposits - withdrawals) into the unsigned expected
/// balance. A negative net (withdrawals exceed deposits) is clamped to 0 with a
/// warning. An over-u64 net is impossible for a real escrow (the ATA balance is
/// itself a u64), so it signals a corrupt DB and aborts the startup gate rather
/// than feeding a sentinel into the mismatch compare.
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

/// Compute the absolute difference between on-chain balance and DB expected value.
/// Both sides are u64; the diff cannot exceed u64::MAX.
pub fn compute_mismatch(db_expected: u64, on_chain_actual: u64) -> u64 {
    on_chain_actual.abs_diff(db_expected)
}

/// Log results and decide whether to allow or block startup.
fn classify_and_report(
    config: &ReconciliationConfig,
    results: &[MintReconciliation],
) -> Result<(), IndexerError> {
    let exceeding: Vec<&MintReconciliation> = results
        .iter()
        .filter(|r| r.mismatch > config.mismatch_threshold_raw)
        .collect();

    let within_tolerance: Vec<&MintReconciliation> = results
        .iter()
        .filter(|r| r.mismatch > 0 && r.mismatch <= config.mismatch_threshold_raw)
        .collect();

    if !exceeding.is_empty() {
        for r in &exceeding {
            error!(
                reconciliation_alert = true,
                mint = %r.mint,
                db_expected = r.db_expected,
                on_chain_actual = r.on_chain_actual,
                mismatch = r.mismatch,
                threshold = config.mismatch_threshold_raw,
                "RECONCILIATION ALERT: escrow ATA balance mismatch exceeds threshold"
            );
        }

        return Err(IndexerError::Reconciliation(
            ReconciliationError::MismatchExceedsThreshold {
                count: exceeding.len(),
                threshold: config.mismatch_threshold_raw,
            },
        ));
    }

    for r in &within_tolerance {
        warn!(
            mint = %r.mint,
            db_expected = r.db_expected,
            on_chain_actual = r.on_chain_actual,
            mismatch = r.mismatch,
            threshold = config.mismatch_threshold_raw,
            "Reconciliation: balance mismatch within tolerance, continuing startup"
        );
    }

    let balanced = results.iter().filter(|r| r.mismatch == 0).count();
    info!(
        total_mints = results.len(),
        balanced,
        within_tolerance = within_tolerance.len(),
        "Startup reconciliation passed"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // compute_mismatch tests
    // =========================================================================

    #[test]
    fn test_compute_mismatch_balanced() {
        assert_eq!(compute_mismatch(1000, 1000), 0);
    }

    #[test]
    fn test_compute_mismatch_on_chain_excess() {
        // on-chain has 100 more than DB expects (unlikely but defensively handled)
        assert_eq!(compute_mismatch(900, 1000), 100);
    }

    #[test]
    fn test_compute_mismatch_db_excess() {
        // DB expects 100 more than on-chain (tokens not yet settled or slippage)
        assert_eq!(compute_mismatch(1100, 1000), 100);
    }

    #[test]
    fn test_compute_mismatch_zero_both() {
        assert_eq!(compute_mismatch(0, 0), 0);
    }

    #[test]
    fn test_compute_mismatch_full_u64_range() {
        // A wiped DB (expected 0) against a u64::MAX escrow must not overflow.
        assert_eq!(compute_mismatch(0, u64::MAX), u64::MAX);
        assert_eq!(compute_mismatch(u64::MAX, 0), u64::MAX);
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
    fn test_classify_mismatch_within_tolerance() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        // mismatch = 5, threshold = 10 → should pass with warning
        let results = vec![make_result("mint1", 1000, 1005)];
        assert!(classify_and_report(&config, &results).is_ok());
    }

    #[test]
    fn test_classify_mismatch_equals_threshold() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 5,
        };
        // mismatch == threshold → within tolerance (not exceeding)
        let results = vec![make_result("mint1", 1000, 1005)];
        assert!(classify_and_report(&config, &results).is_ok());
    }

    #[test]
    fn test_classify_mismatch_exceeds_threshold_blocks() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 4,
        };
        // mismatch = 5 > threshold = 4 → error
        let results = vec![make_result("mint1", 1000, 1005)];
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
    fn test_classify_strict_zero_threshold_any_mismatch_blocks() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let results = vec![make_result("mint1", 1000, 1001)];
        assert!(classify_and_report(&config, &results).is_err());
    }

    #[test]
    fn test_classify_multiple_mints_one_exceeds() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 10,
        };
        let results = vec![
            make_result("mint1", 1000, 1000), // balanced
            make_result("mint2", 1000, 1005), // mismatch 5 ≤ 10 → warn
            make_result("mint3", 1000, 1020), // mismatch 20 > 10 → error
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
        assert_eq!(results[0].mismatch, 800);
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
        assert_eq!(results[0].mismatch, 1234);
    }

    #[test]
    fn union_merges_same_mint_on_both_sides() {
        let mint = Pubkey::new_unique();
        let on_chain = HashMap::from([(mint, 1000u64)]);
        let results =
            build_reconciliation_set(&[db_balance(&mint.to_string(), 1000, 0)], &on_chain).unwrap();
        assert_eq!(results.len(), 1, "same mint must merge to one entry");
        assert_eq!(results[0].mismatch, 0);
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
            &seed,
        )
        .await;
        assert!(result.is_ok());
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
    async fn test_reconciliation_empty_db_with_nonempty_escrow_blocks() {
        // The SOLA3-7 regression: a fresh/partial DB (no `mints` rows) against a
        // live escrow balance must fail closed instead of passing blind.
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;

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
            &seed,
        )
        .await;

        match result {
            Err(IndexerError::Reconciliation(ReconciliationError::MismatchExceedsThreshold {
                count,
                ..
            })) => assert_eq!(count, 1),
            other => panic!(
                "live escrow with empty DB must block startup, got: {:?}",
                other
            ),
        }
    }

    #[tokio::test]
    async fn test_reconciliation_balanced_passes() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_000)]).await;

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
            &seed,
        )
        .await;
        assert!(result.is_ok(), "balanced state should pass: {:?}", result);
    }

    #[tokio::test]
    async fn test_reconciliation_mismatch_within_threshold_passes() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // DB expects 1000, on-chain has 1005 => mismatch 5 <= threshold 10 => ok
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_005)]).await;

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
            &seed,
        )
        .await;
        assert!(
            result.is_ok(),
            "mismatch within threshold should pass: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_mismatch_exceeds_threshold_blocks() {
        let mut server = mockito::Server::new_async().await;
        let mint = Pubkey::new_unique();
        // DB expects 1000, on-chain has 1020 => mismatch 20 > threshold 10 => err
        mock_escrow_sweep(&mut server, &[(mint.to_string(), 1_020)]).await;

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
            &seed,
        )
        .await;
        assert!(
            result.is_ok(),
            "net (deposits - withdrawals) must match on-chain: {:?}",
            result
        );
    }
}
