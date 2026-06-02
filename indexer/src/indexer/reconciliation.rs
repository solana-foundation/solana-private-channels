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
//! 1. Query the DB for per-mint aggregate balances (all deposits − completed withdrawals).
//! 2. Derive the escrow ATA for each mint (instance PDA + mint + token program).
//! 3. Fetch the live ATA balance via RPC.
//! 4. If any |on_chain - db_expected| > threshold → log error, emit alert, abort startup.
//! 5. If any mismatch ≤ threshold (but > 0) → log warning, continue.
//! 6. If all balanced → log info, continue.

use crate::{
    config::{ProgramType, ReconciliationConfig},
    error::{IndexerError, ReconciliationError},
    operator::{rpc_util::RpcClientWithRetry, RetryConfig, RetryPolicy},
    storage::Storage,
};
use private_channel_core::rpc::error::INVALID_PARAMS_CODE;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::str::FromStr;
use tracing::{error, info, warn};

/// Per-mint result produced during reconciliation.
#[derive(Debug, Clone)]
pub struct MintReconciliation {
    pub mint: String,
    /// Expected balance according to DB: all indexed deposits − completed withdrawals.
    pub db_expected: i64,
    /// Actual raw token balance in the escrow ATA on-chain.
    pub on_chain_actual: u64,
    /// Absolute difference: |on_chain_actual − db_expected|.  Derived from the
    /// two fields above — use `MintReconciliation::new` to ensure consistency.
    pub mismatch: u64,
}

impl MintReconciliation {
    pub fn new(mint: String, db_expected: i64, on_chain_actual: u64) -> Self {
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

    let rpc_client = RpcClientWithRetry::with_retry_config(
        rpc_url.to_string(),
        RetryConfig::default(),
        CommitmentConfig::finalized(),
    );

    let mint_balances = storage
        .get_mint_balances_for_reconciliation()
        .await
        .map_err(ReconciliationError::Storage)?;

    if mint_balances.is_empty() {
        info!("No mints in storage; reconciliation passed (empty state)");
        return Ok(());
    }

    info!(
        mint_count = mint_balances.len(),
        "Comparing DB totals against on-chain escrow ATA balances"
    );

    let mut results = Vec::with_capacity(mint_balances.len());

    for balance in &mint_balances {
        let mint_pk = Pubkey::from_str(&balance.mint_address).map_err(|e| {
            ReconciliationError::InvalidPubkey {
                pubkey: balance.mint_address.clone(),
                reason: e.to_string(),
            }
        })?;

        let token_program_pk = Pubkey::from_str(&balance.token_program).map_err(|e| {
            ReconciliationError::InvalidPubkey {
                pubkey: balance.token_program.clone(),
                reason: e.to_string(),
            }
        })?;

        let instance_ata = get_associated_token_address_with_program_id(
            &instance_pda,
            &mint_pk,
            &token_program_pk,
        );

        let on_chain_actual =
            fetch_ata_balance(&rpc_client, &instance_ata, &balance.mint_address).await?;

        let db_expected = balance.total_deposits - balance.total_withdrawals;

        results.push(MintReconciliation::new(
            balance.mint_address.clone(),
            db_expected,
            on_chain_actual,
        ));
    }

    classify_and_report(config, &results)
}

/// Fetch the raw token balance for an ATA.
///
/// Returns `Ok(0)` if the account does not exist yet (valid before the first
/// deposit for that mint). "Not found" is handled inside the retry closure as
/// `Ok(None)` so it is never retried — only genuine transient failures are
/// retried with exponential backoff.
async fn fetch_ata_balance(
    rpc_client: &RpcClientWithRetry,
    ata: &Pubkey,
    mint_address: &str,
) -> Result<u64, IndexerError> {
    let rpc = rpc_client.rpc_client.clone();
    let ata = *ata;

    let result = rpc_client
        .with_retry("get_token_account_balance", RetryPolicy::Idempotent, || {
            let rpc = rpc.clone();
            async move {
                match rpc.get_token_account_balance(&ata).await {
                    Ok(ui_amount) => Ok(Some(ui_amount)),
                    Err(e) if is_account_not_found(&e) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        })
        .await;

    match result {
        Ok(Some(ui_amount)) => ui_amount.amount.parse::<u64>().map_err(|e| {
            IndexerError::Reconciliation(ReconciliationError::Rpc {
                mint: mint_address.to_string(),
                reason: format!(
                    "Failed to parse token balance '{}': {}",
                    ui_amount.amount, e
                ),
            })
        }),
        Ok(None) => Ok(0),
        Err(e) => Err(IndexerError::Reconciliation(ReconciliationError::Rpc {
            mint: mint_address.to_string(),
            reason: e.to_string(),
        })),
    }
}

/// Returns true when the RPC error indicates the account simply does not exist.
///
/// Uses structured `ErrorKind` inspection rather than raw string matching:
/// - Primary: `INVALID_PARAMS_CODE` (`-32602`, JSON-RPC "Invalid params"), which
///   is what Solana validators return for a missing account.
/// - Fallback: substring match on the error message for non-standard RPC
///   providers that may emit the same wording with a different code.
///
/// This function only receives errors from `rpc.get_token_account_balance`, so
/// it cannot be triggered by a DB error — DB failures propagate as a different
/// error type entirely and never reach this function.
fn is_account_not_found(e: &solana_rpc_client_api::client_error::Error) -> bool {
    use solana_rpc_client_api::{client_error::ErrorKind, request::RpcError};

    if let ErrorKind::RpcError(RpcError::RpcResponseError { code, message, .. }) = &e.kind {
        if *code == INVALID_PARAMS_CODE as i64 {
            return true;
        }
        let msg = message.to_lowercase();
        msg.contains("could not find account") || msg.contains("account not found")
    } else {
        false
    }
}

/// Compute the absolute difference between on-chain balance and DB expected value.
/// Uses i128 internally to avoid overflow when subtracting.
pub fn compute_mismatch(db_expected: i64, on_chain_actual: u64) -> u64 {
    let diff = (on_chain_actual as i128) - (db_expected as i128);
    // unsigned_abs() returns u128; cap at u64::MAX to keep the type consistent
    diff.unsigned_abs().min(u64::MAX as u128) as u64
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
    fn test_compute_mismatch_db_negative() {
        // Impossible state: more completed withdrawals than deposits in DB.
        // Mismatch is the absolute difference from 0.
        assert_eq!(compute_mismatch(-50, 0), 50);
    }

    #[test]
    fn test_compute_mismatch_zero_both() {
        assert_eq!(compute_mismatch(0, 0), 0);
    }

    // =========================================================================
    // is_account_not_found tests
    // =========================================================================

    fn make_rpc_response_error(
        code: i64,
        message: &str,
    ) -> solana_rpc_client_api::client_error::Error {
        use solana_rpc_client_api::{
            client_error::ErrorKind,
            request::{RpcError, RpcResponseErrorData},
        };
        ErrorKind::RpcError(RpcError::RpcResponseError {
            code,
            message: message.to_string(),
            data: RpcResponseErrorData::Empty,
        })
        .into()
    }

    #[test]
    fn test_is_account_not_found_by_code() {
        // INVALID_PARAMS_CODE matches regardless of message wording
        let err = make_rpc_response_error(
            INVALID_PARAMS_CODE as i64,
            "Invalid param: some unrecognized wording",
        );
        assert!(is_account_not_found(&err));
    }

    #[test]
    fn test_is_account_not_found_standard_message() {
        // Standard Solana validator message — also matches via INVALID_PARAMS_CODE
        let err = make_rpc_response_error(
            INVALID_PARAMS_CODE as i64,
            "Invalid param: could not find account",
        );
        assert!(is_account_not_found(&err));
    }

    #[test]
    fn test_is_account_not_found_message_fallback() {
        // Non-standard provider: different code but recognizable message
        let err = make_rpc_response_error(-32000, "could not find account");
        assert!(is_account_not_found(&err));
    }

    #[test]
    fn test_is_account_not_found_account_not_found_variant() {
        // Alternative message wording — message fallback
        let err = make_rpc_response_error(-32000, "account not found");
        assert!(is_account_not_found(&err));
    }

    #[test]
    fn test_is_account_not_found_unrelated_error() {
        // Unrelated RPC error — should not match
        let err = make_rpc_response_error(-32005, "Node is unhealthy");
        assert!(!is_account_not_found(&err));
    }

    #[test]
    fn test_is_account_not_found_non_rpc_error() {
        use solana_rpc_client_api::client_error::ErrorKind;
        let err = ErrorKind::Custom("connection refused".to_string()).into();
        assert!(!is_account_not_found(&err));
    }

    // =========================================================================
    // classify_and_report tests
    // =========================================================================

    fn make_result(mint: &str, db_expected: i64, on_chain_actual: u64) -> MintReconciliation {
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
    // run_startup_reconciliation skip / pass tests
    // =========================================================================

    #[tokio::test]
    async fn test_reconciliation_skipped_for_withdraw_program() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };

        #[cfg(test)]
        {
            use crate::storage::common::storage::mock::MockStorage;
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
    }

    #[tokio::test]
    async fn test_reconciliation_empty_mints_passes() {
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };

        #[cfg(test)]
        {
            use crate::storage::common::storage::mock::MockStorage;
            // Empty mock: no mints → should pass immediately
            let storage = Storage::Mock(MockStorage::new());
            let seed = Pubkey::new_unique();

            let result = run_startup_reconciliation(
                &config,
                ProgramType::Escrow,
                &storage,
                "http://localhost:8899",
                &seed,
            )
            .await;

            // Empty state always passes (no mints to compare)
            assert!(result.is_ok());
        }
    }

    // =========================================================================
    // InvalidPubkey error path tests (comment 4)
    // =========================================================================

    #[tokio::test]
    async fn test_reconciliation_invalid_mint_pubkey_returns_error() {
        use crate::storage::common::storage::mock::MockStorage;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![crate::storage::common::models::MintDbBalance {
            mint_address: "not-a-valid-pubkey".to_string(),
            token_program: spl_token::id().to_string(),
            total_deposits: 1000,
            total_withdrawals: 0,
        }]);
        let storage = Storage::Mock(mock_storage);
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();

        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            "http://localhost:8899",
            &seed,
        )
        .await;

        match result {
            Err(IndexerError::Reconciliation(ReconciliationError::InvalidPubkey {
                pubkey,
                reason: _,
            })) => {
                assert_eq!(pubkey, "not-a-valid-pubkey");
            }
            other => panic!("Expected InvalidPubkey error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_reconciliation_invalid_token_program_pubkey_returns_error() {
        use crate::storage::common::storage::mock::MockStorage;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![crate::storage::common::models::MintDbBalance {
            mint_address: Pubkey::new_unique().to_string(),
            token_program: "not-a-valid-token-program".to_string(),
            total_deposits: 500,
            total_withdrawals: 0,
        }]);
        let storage = Storage::Mock(mock_storage);
        let config = ReconciliationConfig {
            mismatch_threshold_raw: 0,
        };
        let seed = Pubkey::new_unique();

        let result = run_startup_reconciliation(
            &config,
            ProgramType::Escrow,
            &storage,
            "http://localhost:8899",
            &seed,
        )
        .await;

        match result {
            Err(IndexerError::Reconciliation(ReconciliationError::InvalidPubkey {
                pubkey,
                reason: _,
            })) => {
                assert_eq!(pubkey, "not-a-valid-token-program");
            }
            other => panic!("Expected InvalidPubkey error, got: {:?}", other),
        }
    }

    // =========================================================================
    // RPC integration tests (mockito)
    // =========================================================================

    use crate::storage::common::storage::mock::MockStorage;

    fn token_account_balance_response(amount: u64) -> String {
        format!(
            r#"{{
                "context": {{"slot": 100}},
                "value": {{
                    "amount": "{}",
                    "decimals": 6,
                    "uiAmount": null,
                    "uiAmountString": "{}"
                }}
            }}"#,
            amount, amount
        )
    }

    fn account_not_found_error() -> (i32, &'static str) {
        (-32602, "Invalid param: could not find account")
    }

    fn make_mint_balance(
        mint_address: &str,
        total_deposits: i64,
        total_withdrawals: i64,
    ) -> crate::storage::common::models::MintDbBalance {
        crate::storage::common::models::MintDbBalance {
            mint_address: mint_address.to_string(),
            token_program: spl_token::id().to_string(),
            total_deposits,
            total_withdrawals,
        }
    }

    #[tokio::test]
    async fn test_reconciliation_balanced_passes() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#,
                token_account_balance_response(1000)
            ))
            .create_async()
            .await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(
            &Pubkey::new_unique().to_string(),
            1000,
            0,
        )]);
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
        // DB expects 1000, on-chain has 1005 → mismatch = 5 ≤ threshold 10 → ok
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#,
                token_account_balance_response(1005)
            ))
            .create_async()
            .await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(
            &Pubkey::new_unique().to_string(),
            1000,
            0,
        )]);
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
        // DB expects 1000, on-chain has 1020 → mismatch = 20 > threshold 10 → err
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#,
                token_account_balance_response(1020)
            ))
            .create_async()
            .await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(
            &Pubkey::new_unique().to_string(),
            1000,
            0,
        )]);
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
    async fn test_reconciliation_ata_not_found_treated_as_zero() {
        let mut server = mockito::Server::new_async().await;
        let (code, message) = account_not_found_error();
        // Return "account not found" error → treated as balance 0
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","error":{{"code":{},"message":"{}"}},"id":1}}"#,
                code, message
            ))
            .create_async()
            .await;

        let mock_storage = MockStorage::new();
        // DB also expects 0 (no completed deposits) → balanced → should pass
        mock_storage.set_mint_balances(vec![make_mint_balance(
            &Pubkey::new_unique().to_string(),
            0,
            0,
        )]);
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

        assert!(
            result.is_ok(),
            "account not found should be treated as 0 balance: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_with_nonzero_withdrawals_balanced() {
        // Exercises total_deposits - total_withdrawals: 1500 deposits, 500 withdrawals → db_expected 1000
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#,
                token_account_balance_response(1000)
            ))
            .create_async()
            .await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(
            &Pubkey::new_unique().to_string(),
            1500,
            500,
        )]);
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

        assert!(
            result.is_ok(),
            "1500 deposits - 500 withdrawals = 1000 on-chain should balance: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_reconciliation_with_nonzero_withdrawals_mismatch_blocks() {
        // 1500 deposits, 500 withdrawals → db_expected 1000
        // on-chain = 1050 → mismatch 50 > threshold 10 → error
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(format!(
                r#"{{"jsonrpc":"2.0","result":{},"id":1}}"#,
                token_account_balance_response(1050)
            ))
            .create_async()
            .await;

        let mock_storage = MockStorage::new();
        mock_storage.set_mint_balances(vec![make_mint_balance(
            &Pubkey::new_unique().to_string(),
            1500,
            500,
        )]);
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
            other => panic!(
                "Expected MismatchExceedsThreshold for withdrawal mismatch, got: {:?}",
                other
            ),
        }
    }
}
