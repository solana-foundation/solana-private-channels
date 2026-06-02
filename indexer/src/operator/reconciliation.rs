//! Escrow balance reconciliation module
//!
//! Validates that on-chain escrow holdings equal total user liabilities in the database.
//! This module performs periodic reconciliation checks by comparing the escrow's Associated Token
//! Account (ATA) balances on-chain against the sum of completed deposits minus completed withdrawals
//! for each mint. Discrepancies exceeding the configured tolerance threshold trigger webhook alerts
//! to notify operators of potential security issues.
//!
//! This check is fundamental to the safety and correctness of the escrow system: if on-chain
//! balances fall short of database liabilities, users may be unable to withdraw their funds.

use crate::config::OperatorConfig;
use crate::error::OperatorError;
use crate::operator::utils::instruction_util::RetryPolicy;
use crate::operator::RpcClientWithRetry;
use crate::storage::Storage;
use private_channel_core::webhook::{WebhookClient, WebhookRetryConfig};
use solana_account_decoder_client_types::UiAccountData;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Account as TokenAccount;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const WEBHOOK_MAX_ATTEMPTS: u32 = 3;
const WEBHOOK_BASE_DELAY: Duration = Duration::from_millis(500);
const WEBHOOK_MAX_DELAY: Duration = Duration::from_secs(5);
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs periodic escrow balance reconciliation checks
///
/// Validates that on-chain escrow holdings equal total user liabilities in the database.
/// Compares the escrow's Associated Token Account (ATA) balance on-chain against
/// the sum of completed deposits minus completed withdrawals, alerting via webhook when discrepancies
/// exceed the configured tolerance threshold.
///
/// Uses row-level locking-free queries since reconciliation is read-only and doesn't modify transaction state.
pub async fn run_reconciliation(
    storage: Arc<Storage>,
    config: OperatorConfig,
    rpc_client: Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
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
            escrow_instance_id,
            &webhook_client,
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

/// Performs a single reconciliation check
///
/// This function orchestrates the complete reconciliation flow:
/// 1. Fetch on-chain balances for all mints held by the escrow
/// 2. Query database for sum of completed deposits minus withdrawals per mint
/// 3. Compare balances with tolerance threshold
/// 4. Send webhook alert if mismatch exceeds tolerance
async fn perform_reconciliation_check(
    storage: &Arc<Storage>,
    config: &OperatorConfig,
    rpc_client: &Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
    webhook_client: &WebhookClient,
) -> Result<(), OperatorError> {
    // Step 1: Fetch on-chain balances from Solana RPC
    let on_chain_balances = fetch_on_chain_balances(rpc_client, escrow_instance_id).await?;

    // Step 2: Query database for completed transaction balances per mint
    let db_balance_results = storage
        .get_escrow_balances_by_mint()
        .await
        .map_err(OperatorError::Storage)?;

    // Convert DB results to HashMap<Pubkey, u64> for comparison
    let mut db_balances = std::collections::HashMap::new();
    for balance_result in db_balance_results {
        let mint = balance_result.mint_address.parse::<Pubkey>().map_err(|e| {
            OperatorError::InvalidPubkey {
                pubkey: balance_result.mint_address.clone(),
                reason: e.to_string(),
            }
        })?;

        // Calculate net balance (deposits - withdrawals)
        let net_balance_i64 = if balance_result.total_deposits >= balance_result.total_withdrawals {
            balance_result.total_deposits - balance_result.total_withdrawals
        } else {
            // This shouldn't happen in a properly functioning system, but handle it gracefully
            warn!(
                "Withdrawals exceed deposits for mint {}: deposits={}, withdrawals={}",
                balance_result.mint_address,
                balance_result.total_deposits,
                balance_result.total_withdrawals
            );
            0 // Treat as zero balance for comparison
        };

        let net_balance = net_balance_i64 as u64;
        db_balances.insert(mint, net_balance);
    }

    // Step 3: Compare balances with tolerance threshold
    let mismatches = compare_balances(
        &on_chain_balances,
        &db_balances,
        config.reconciliation_tolerance_bps,
    );

    // Step 4: Send webhook alert if mismatches found
    if !mismatches.is_empty() {
        error!(
            "Balance reconciliation failed: found {} mismatch(es) exceeding tolerance of {} bps",
            mismatches.len(),
            config.reconciliation_tolerance_bps
        );

        for mismatch in &mismatches {
            error!(
                "Mismatch for mint {}: on-chain={}, db={}, delta={} bps",
                mismatch.mint, mismatch.on_chain_balance, mismatch.db_balance, mismatch.delta_bps
            );
        }

        send_webhook_alert(
            &config.reconciliation_webhook_url,
            &mismatches,
            webhook_client,
        )
        .await?;
    } else {
        info!("Balance reconciliation successful: all mints within tolerance");
    }

    Ok(())
}

/// Represents a balance mismatch between on-chain and database balances for a specific mint
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceMismatch {
    pub mint: Pubkey,
    pub on_chain_balance: u64,
    pub db_balance: u64,
    pub delta_bps: u64,
}

/// Compares on-chain and database balances for all mints and identifies mismatches exceeding tolerance
///
/// Calculates the delta in basis points for each mint using the formula:
/// `delta_bps = |(on_chain - db) / on_chain * 10000|`
///
/// Mismatches are detected when:
/// - A mint exists on-chain but not in DB (or vice versa) - always a critical mismatch
/// - The delta exceeds the tolerance threshold (e.g., 10 basis points = 0.1%)
///
/// # Arguments
/// * `on_chain_balances` - Map of mint pubkey to balance fetched from Solana RPC
/// * `db_balances` - Map of mint pubkey to net balance (deposits - withdrawals) from database
/// * `tolerance_bps` - Maximum acceptable delta in basis points (100 bps = 1%)
///
/// # Returns
/// * `Vec<BalanceMismatch>` - List of mismatches exceeding tolerance, empty if all balances reconcile
pub fn compare_balances(
    on_chain_balances: &HashMap<Pubkey, u64>,
    db_balances: &HashMap<Pubkey, u64>,
    tolerance_bps: u16,
) -> Vec<BalanceMismatch> {
    let mut mismatches = Vec::new();

    // Collect all unique mints from both sources
    let mut all_mints: std::collections::HashSet<Pubkey> =
        on_chain_balances.keys().copied().collect();
    all_mints.extend(db_balances.keys().copied());

    for mint in all_mints {
        let on_chain = *on_chain_balances.get(&mint).unwrap_or(&0);
        let db = *db_balances.get(&mint).unwrap_or(&0);

        // Both zero is considered a match (no alert needed)
        if on_chain == 0 && db == 0 {
            continue;
        }

        // Calculate delta in basis points
        // Formula: |(on_chain - db) / on_chain * 10000|
        // Special case: if on_chain is zero but db is not, this is a critical mismatch
        let delta_bps = if on_chain == 0 {
            // Critical: DB shows balance but on-chain is zero
            u64::MAX // Use max to ensure it exceeds any tolerance
        } else {
            // Calculate percentage difference in basis points
            let diff = on_chain.abs_diff(db);
            // Use u128 to avoid overflow during multiplication
            ((diff as u128 * 10000) / on_chain as u128) as u64
        };

        // Check if delta exceeds tolerance
        if delta_bps > tolerance_bps as u64 {
            mismatches.push(BalanceMismatch {
                mint,
                on_chain_balance: on_chain,
                db_balance: db,
                delta_bps,
            });
        }
    }

    mismatches
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
/// * `HashMap<Pubkey, u64>` - Map of mint pubkey to total balance (in smallest token units)
///
/// # Errors
/// Returns `OperatorError::RpcError` if the RPC call fails after retries or if token account data cannot be parsed
async fn fetch_on_chain_balances(
    rpc_client: &Arc<RpcClientWithRetry>,
    escrow_instance_id: Pubkey,
) -> Result<HashMap<Pubkey, u64>, OperatorError> {
    let mut balances = HashMap::new();
    let token_programs = [spl_token::id(), spl_token_2022::id()];

    for token_program_id in token_programs {
        // Fetch all token accounts owned by the escrow for each supported token program
        let accounts = rpc_client
            .with_retry(
                "get_token_accounts_by_owner",
                RetryPolicy::Idempotent,
                || async {
                    rpc_client
                        .rpc_client
                        .get_token_accounts_by_owner(
                            &escrow_instance_id,
                            TokenAccountsFilter::ProgramId(token_program_id),
                        )
                        .await
                },
            )
            .await
            .map_err(|e| {
                OperatorError::RpcError(format!(
                    "Failed to fetch token accounts for program {}: {}",
                    token_program_id, e
                ))
            })?;

        // Parse each token account and aggregate balances by mint.
        // The RPC may return accounts in either binary (base64) or JSON-parsed format
        // depending on the client's requested encoding. We handle both variants.
        for keyed_account in accounts {
            let (mint, amount) = match &keyed_account.account.data {
                // Binary encoding: decode base64, then unpack the SPL token layout
                data if data.decode().is_some() => {
                    let account_data = data.decode().unwrap();
                    let token_account = TokenAccount::unpack(&account_data).map_err(|e| {
                        OperatorError::RpcError(format!(
                            "Failed to parse token account for program {}: {}",
                            token_program_id, e
                        ))
                    })?;
                    (token_account.mint, token_account.amount)
                }
                // JSON-parsed encoding: the RPC has already decoded the account for us;
                // extract mint and amount from the nested `info` object.
                UiAccountData::Json(parsed) => {
                    let info = parsed.parsed.get("info").ok_or_else(|| {
                        OperatorError::RpcError(
                            "Missing 'info' in parsed token account".to_string(),
                        )
                    })?;
                    let mint_str = info.get("mint").and_then(|v| v.as_str()).ok_or_else(|| {
                        OperatorError::RpcError(
                            "Missing 'mint' in parsed token account info".to_string(),
                        )
                    })?;
                    let amount_str = info
                        .get("tokenAmount")
                        .and_then(|v| v.get("amount"))
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            OperatorError::RpcError(
                                "Missing 'tokenAmount.amount' in parsed token account".to_string(),
                            )
                        })?;
                    let mint = Pubkey::from_str(mint_str).map_err(|e| {
                        OperatorError::RpcError(format!(
                            "Invalid mint pubkey '{}': {}",
                            mint_str, e
                        ))
                    })?;
                    let amount = amount_str.parse::<u64>().map_err(|e| {
                        OperatorError::RpcError(format!(
                            "Invalid token amount '{}': {}",
                            amount_str, e
                        ))
                    })?;
                    (mint, amount)
                }
                // Unknown encoding variant — skip with a warning rather than hard-failing
                _ => {
                    warn!(
                        "Skipping token account with unrecognised data encoding for program {}",
                        token_program_id
                    );
                    continue;
                }
            };

            // Sum balances for each mint (handles multiple token accounts for the same mint)
            *balances.entry(mint).or_insert(0) += amount;
        }
    }

    Ok(balances)
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

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn test_fetch_on_chain_balances_exists() {
        // This test verifies that the fetch_on_chain_balances function exists and compiles
        // Integration testing with a real RPC client would require a test validator
        // and is better suited for integration tests rather than unit tests
        let _function = fetch_on_chain_balances;
    }

    #[test]
    fn test_pubkey_hashmap_initialization() {
        // Test that we can create a HashMap<Pubkey, u64> as expected by the function
        let mut balances: HashMap<Pubkey, u64> = HashMap::new();
        let mint = Pubkey::new_unique();
        balances.insert(mint, 1000);
        assert_eq!(*balances.get(&mint).unwrap(), 1000);
    }

    #[test]
    fn test_balance_aggregation_logic() {
        // Test the aggregation logic used in fetch_on_chain_balances
        let mut balances: HashMap<Pubkey, u64> = HashMap::new();
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();

        // Simulate multiple token accounts for the same mint
        *balances.entry(mint1).or_insert(0) += 100;
        *balances.entry(mint1).or_insert(0) += 200;
        *balances.entry(mint2).or_insert(0) += 500;

        assert_eq!(*balances.get(&mint1).unwrap(), 300);
        assert_eq!(*balances.get(&mint2).unwrap(), 500);
    }

    #[test]
    fn test_compare_balances_exact_match() {
        // Test exact balance match - should return no mismatches
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        on_chain.insert(mint, 1000);
        db.insert(mint, 1000);

        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(mismatches.len(), 0, "Exact match should have no mismatches");
    }

    #[test]
    fn test_compare_balances_within_tolerance() {
        // Test balance difference within tolerance - should return no mismatches
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 10000 on-chain, 9999 in DB = 1 basis point difference (0.01%)
        on_chain.insert(mint, 10000);
        db.insert(mint, 9999);

        let mismatches = compare_balances(&on_chain, &db, 10); // 10 bps tolerance
        assert_eq!(
            mismatches.len(),
            0,
            "Difference within tolerance should have no mismatches"
        );
    }

    #[test]
    fn test_compare_balances_exceeds_tolerance() {
        // Test balance difference exceeding tolerance - should return mismatch
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 10000 on-chain, 9900 in DB = 100 basis points difference (1%)
        on_chain.insert(mint, 10000);
        db.insert(mint, 9900);

        let mismatches = compare_balances(&on_chain, &db, 10); // 10 bps tolerance
        assert_eq!(
            mismatches.len(),
            1,
            "Difference exceeding tolerance should have mismatch"
        );

        let mismatch = &mismatches[0];
        assert_eq!(mismatch.mint, mint);
        assert_eq!(mismatch.on_chain_balance, 10000);
        assert_eq!(mismatch.db_balance, 9900);
        assert_eq!(mismatch.delta_bps, 100); // 1% = 100 basis points
    }

    #[test]
    fn test_compare_balances_both_zero() {
        // Test both balances zero - should return no mismatches
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        on_chain.insert(mint, 0);
        db.insert(mint, 0);

        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(mismatches.len(), 0, "Both zero should have no mismatches");
    }

    #[test]
    fn test_compare_balances_on_chain_only() {
        // Test mint exists on-chain but not in DB - critical mismatch
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let db = HashMap::new();

        on_chain.insert(mint, 1000);

        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(
            mismatches.len(),
            1,
            "On-chain balance without DB balance should be mismatch"
        );

        let mismatch = &mismatches[0];
        assert_eq!(mismatch.mint, mint);
        assert_eq!(mismatch.on_chain_balance, 1000);
        assert_eq!(mismatch.db_balance, 0);
        assert_eq!(mismatch.delta_bps, 10000); // 100% difference
    }

    #[test]
    fn test_compare_balances_db_only() {
        // Test mint exists in DB but not on-chain - critical mismatch
        let mint = Pubkey::new_unique();
        let on_chain = HashMap::new();
        let mut db = HashMap::new();

        db.insert(mint, 1000);

        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(
            mismatches.len(),
            1,
            "DB balance without on-chain balance should be critical mismatch"
        );

        let mismatch = &mismatches[0];
        assert_eq!(mismatch.mint, mint);
        assert_eq!(mismatch.on_chain_balance, 0);
        assert_eq!(mismatch.db_balance, 1000);
        assert_eq!(
            mismatch.delta_bps,
            u64::MAX,
            "On-chain zero with DB balance should have MAX delta"
        );
    }

    #[test]
    fn test_compare_balances_multiple_mints() {
        // Test multiple mints with mixed results
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mint3 = Pubkey::new_unique();

        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Mint1: exact match (no mismatch)
        on_chain.insert(mint1, 1000);
        db.insert(mint1, 1000);

        // Mint2: within tolerance (no mismatch)
        on_chain.insert(mint2, 10000);
        db.insert(mint2, 9999); // 1 bps difference

        // Mint3: exceeds tolerance (mismatch)
        on_chain.insert(mint3, 10000);
        db.insert(mint3, 9800); // 200 bps difference (2%)

        let mismatches = compare_balances(&on_chain, &db, 10); // 10 bps tolerance
        assert_eq!(
            mismatches.len(),
            1,
            "Should only have one mismatch for mint3"
        );

        let mismatch = &mismatches[0];
        assert_eq!(mismatch.mint, mint3);
        assert_eq!(mismatch.delta_bps, 200);
    }

    #[test]
    fn test_compare_balances_bps_calculation_accuracy() {
        // Test basis points calculation accuracy
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Test 0.1% difference (10 basis points)
        on_chain.insert(mint, 100000);
        db.insert(mint, 99900); // 0.1% = 10 bps

        let mismatches = compare_balances(&on_chain, &db, 9);
        assert_eq!(
            mismatches.len(),
            1,
            "Should detect 10 bps with 9 bps tolerance"
        );
        assert_eq!(mismatches[0].delta_bps, 10);

        // Test edge case: exactly at tolerance threshold
        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(
            mismatches.len(),
            0,
            "Should not detect 10 bps with 10 bps tolerance"
        );
    }

    #[test]
    fn test_compare_balances_db_greater_than_on_chain() {
        // Test when DB balance is greater than on-chain balance
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        on_chain.insert(mint, 9000);
        db.insert(mint, 10000); // DB has more than on-chain

        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(
            mismatches.len(),
            1,
            "DB > on-chain should be detected as mismatch"
        );

        let mismatch = &mismatches[0];
        assert_eq!(mismatch.on_chain_balance, 9000);
        assert_eq!(mismatch.db_balance, 10000);
        // Delta = |9000 - 10000| / 9000 * 10000 = 1000 / 9000 * 10000 ≈ 1111 bps
        assert!(
            mismatch.delta_bps > 1000,
            "Delta should be > 1000 bps for 1000 unit difference on 9000 base"
        );
    }

    #[test]
    fn test_compare_balances_large_values() {
        // Test with large token amounts to ensure no overflow
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Use large values (e.g., billions of tokens)
        let large_value = 1_000_000_000_000u64; // 1 trillion
        on_chain.insert(mint, large_value);
        db.insert(mint, large_value - 100_000_000); // 100 million difference

        let mismatches = compare_balances(&on_chain, &db, 10);
        // Delta is 1 bps (100M / 1T * 10000 = 1), which is within 10 bps tolerance.
        assert_eq!(mismatches.len(), 0);
    }

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
        let config = make_operator_config();
        let ct = CancellationToken::new();
        ct.cancel(); // pre-cancel so the loop exits immediately

        let result = run_reconciliation(
            storage,
            config,
            rpc_client,
            solana_sdk::pubkey::Pubkey::new_unique(),
            ct,
        )
        .await;
        assert!(
            result.is_ok(),
            "pre-cancelled reconciliation should return Ok"
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

    // Additional edge case tests for basis points calculation

    #[test]
    fn test_bps_calculation_very_small_difference() {
        // Test that differences less than 1 basis point are detected correctly
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 1,000,000 on-chain, 999,999 in DB
        // Delta = 1 / 1,000,000 * 10,000 = 0.01 bps (rounds to 0)
        on_chain.insert(mint, 1_000_000);
        db.insert(mint, 999_999);

        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(
            mismatches.len(),
            0,
            "Sub-basis-point differences should be within 0 bps tolerance"
        );
    }

    #[test]
    fn test_bps_calculation_exactly_one_basis_point() {
        // Test exactly 1 basis point difference
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 100,000 on-chain, 99,990 in DB = exactly 10 / 100,000 * 10,000 = 1 bps
        on_chain.insert(mint, 100_000);
        db.insert(mint, 99_990);

        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(mismatches.len(), 1, "1 bps should exceed 0 bps tolerance");
        assert_eq!(mismatches[0].delta_bps, 1);

        let mismatches = compare_balances(&on_chain, &db, 1);
        assert_eq!(
            mismatches.len(),
            0,
            "1 bps should be within 1 bps tolerance"
        );
    }

    #[test]
    fn test_bps_calculation_small_balances() {
        // Test with very small balances to ensure no division issues
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 10 on-chain, 9 in DB = 1 / 10 * 10,000 = 1,000 bps (10%)
        on_chain.insert(mint, 10);
        db.insert(mint, 9);

        let mismatches = compare_balances(&on_chain, &db, 999);
        assert_eq!(
            mismatches.len(),
            1,
            "10% difference should exceed 9.99% tolerance"
        );
        assert_eq!(mismatches[0].delta_bps, 1000);
    }

    #[test]
    fn test_bps_calculation_single_unit_difference() {
        // Test with single unit differences at various scales
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mint3 = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 1 unit difference on 1000 base = 10 bps
        on_chain.insert(mint1, 1000);
        db.insert(mint1, 999);

        // 1 unit difference on 10000 base = 1 bps
        on_chain.insert(mint2, 10000);
        db.insert(mint2, 9999);

        // 1 unit difference on 100 base = 100 bps
        on_chain.insert(mint3, 100);
        db.insert(mint3, 99);

        let mismatches = compare_balances(&on_chain, &db, 5);
        // Should detect mint1 (10 bps) and mint3 (100 bps), but not mint2 (1 bps)
        assert_eq!(
            mismatches.len(),
            2,
            "Should detect mismatches > 5 bps tolerance"
        );

        // Verify the detected mismatches are mint1 and mint3
        let mismatch_mints: Vec<Pubkey> = mismatches.iter().map(|m| m.mint).collect();
        assert!(mismatch_mints.contains(&mint1));
        assert!(mismatch_mints.contains(&mint3));
        assert!(!mismatch_mints.contains(&mint2));
    }

    #[test]
    fn test_bps_calculation_near_max_u64() {
        // Test with values near u64::MAX to ensure no overflow in intermediate calculations
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Use large values that would overflow if we didn't use u128 internally
        let large_value = u64::MAX / 2; // Half of u64::MAX
        let diff = (large_value / 10000) + 1; // ensure at least 1 bps after integer rounding

        on_chain.insert(mint, large_value);
        db.insert(mint, large_value - diff);

        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(
            mismatches.len(),
            1,
            "Should detect mismatch with large values"
        );

        // Delta should be approximately 1 bps
        let mismatch = &mismatches[0];
        assert!(
            mismatch.delta_bps >= 1 && mismatch.delta_bps <= 2,
            "Delta should be approximately 1 bps, got {}",
            mismatch.delta_bps
        );
    }

    #[test]
    fn test_bps_calculation_rounding_behavior() {
        // Test integer division rounding behavior
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 99,999 on-chain, 99,998 in DB
        // Delta = 1 / 99,999 * 10,000 = 0.100001... bps (rounds down to 0)
        on_chain.insert(mint, 99_999);
        db.insert(mint, 99_998);

        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(
            mismatches.len(),
            0,
            "Rounded-down sub-basis-point difference should be within 0 bps tolerance"
        );

        // Now test a case that rounds up to 1 bps
        on_chain.insert(mint, 10_001);
        db.insert(mint, 10_000);

        // Delta = 1 / 10,001 * 10,000 = 0.9999... bps (rounds down to 0)
        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(mismatches.len(), 0, "Should round down to 0 bps");
    }

    #[test]
    fn test_bps_calculation_exact_tolerance_boundaries() {
        // Test behavior at exact tolerance boundaries
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mint3 = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Exactly 10 bps difference
        on_chain.insert(mint1, 100_000);
        db.insert(mint1, 99_900);

        // Exactly 11 bps difference
        on_chain.insert(mint2, 100_000);
        db.insert(mint2, 99_890);

        // Exactly 9 bps difference
        on_chain.insert(mint3, 100_000);
        db.insert(mint3, 99_910);

        // Test with 10 bps tolerance - should only detect mint2 (11 bps)
        let mismatches = compare_balances(&on_chain, &db, 10);
        assert_eq!(
            mismatches.len(),
            1,
            "Should only detect mismatches > 10 bps tolerance"
        );
        assert_eq!(mismatches[0].mint, mint2);
        assert_eq!(mismatches[0].delta_bps, 11);
    }

    #[test]
    fn test_bps_calculation_symmetry() {
        // Test that delta calculation is symmetric (on_chain > db vs db > on_chain)
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Case 1: on_chain > db by 100
        on_chain.insert(mint1, 10_000);
        db.insert(mint1, 9_900);

        // Case 2: db > on_chain by 100 (same absolute difference)
        on_chain.insert(mint2, 10_000);
        db.insert(mint2, 10_100);

        let mismatches = compare_balances(&on_chain, &db, 50);
        assert_eq!(
            mismatches.len(),
            2,
            "Both cases should be detected as mismatches"
        );

        // Both should have 100 bps delta (1% of 10,000)
        for mismatch in mismatches {
            assert_eq!(
                mismatch.delta_bps, 100,
                "Delta should be 100 bps for both cases"
            );
        }
    }

    #[test]
    fn test_bps_calculation_zero_tolerance() {
        // Test with zero tolerance - only exact matches should pass
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // Exact match
        on_chain.insert(mint1, 1000);
        db.insert(mint1, 1000);

        // Tiny difference
        on_chain.insert(mint2, 1_000_000);
        db.insert(mint2, 999_999);

        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(
            mismatches.len(),
            0,
            "Sub-basis-point differences should pass with 0 tolerance due to rounding"
        );
    }

    #[test]
    fn test_bps_calculation_maximum_tolerance() {
        // Test with maximum u16 tolerance (65535 bps = 655.35%)
        let mint = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 100% difference (10000 bps)
        on_chain.insert(mint, 1000);
        db.insert(mint, 0);

        let mismatches = compare_balances(&on_chain, &db, u16::MAX);
        assert_eq!(
            mismatches.len(),
            0,
            "100% difference should be within 655% tolerance"
        );

        // But db-only case (on_chain = 0, db > 0) should still be detected
        let on_chain_empty = HashMap::new();
        let mut db_only = HashMap::new();
        db_only.insert(mint, 1000);

        let mismatches = compare_balances(&on_chain_empty, &db_only, u16::MAX);
        assert_eq!(
            mismatches.len(),
            1,
            "DB-only balance should always be detected (u64::MAX delta)"
        );
        assert_eq!(mismatches[0].delta_bps, u64::MAX);
    }

    #[test]
    fn test_bps_calculation_precision_with_decimal_percentages() {
        // Test precision for common decimal percentages
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        let mint3 = Pubkey::new_unique();
        let mut on_chain = HashMap::new();
        let mut db = HashMap::new();

        // 0.5% difference = 50 bps
        on_chain.insert(mint1, 100_000);
        db.insert(mint1, 99_500);

        // 0.25% difference = 25 bps
        on_chain.insert(mint2, 100_000);
        db.insert(mint2, 99_750);

        // 0.125% difference = 12.5 bps (rounds to 12)
        on_chain.insert(mint3, 100_000);
        db.insert(mint3, 99_875);

        let mismatches = compare_balances(&on_chain, &db, 0);
        assert_eq!(mismatches.len(), 3);

        // Verify delta values
        let deltas: HashMap<Pubkey, u64> =
            mismatches.iter().map(|m| (m.mint, m.delta_bps)).collect();

        assert_eq!(deltas[&mint1], 50, "0.5% should be 50 bps");
        assert_eq!(deltas[&mint2], 25, "0.25% should be 25 bps");
        assert_eq!(deltas[&mint3], 12, "0.125% should be 12 bps (rounded down)");
    }
}
